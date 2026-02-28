import logging
import random
import time
from typing import Any, Callable

from requests.exceptions import HTTPError


def _parse_retry_after(exc: Exception) -> float | None:
    """Extract Retry-After value from a 429 response, if present."""
    response = getattr(exc, "response", None)
    if response is None or getattr(response, "status_code", None) != 429:
        return None
    raw = response.headers.get("Retry-After")
    if raw is None:
        return 5.0  # Default wait for 429 without header
    try:
        return max(float(raw), 1.0)
    except (TypeError, ValueError):
        return 5.0


def api_call_with_retry(
    func: Callable[..., Any],
    *args: Any,
    max_attempts: int = 3,
    base_delay_seconds: float = 1.5,
    logger: logging.Logger | None = None,
    **kwargs: Any,
) -> Any:
    active_logger = logger or logging.getLogger("polymarket_pipeline")

    for attempt in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            is_last_attempt = attempt == max_attempts - 1
            active_logger.warning(
                "API call failed (%s/%s): %s - %s",
                attempt + 1,
                max_attempts,
                type(exc).__name__,
                exc,
            )
            if is_last_attempt:
                raise

            # Respect Retry-After header on 429 rate-limit responses
            retry_after = _parse_retry_after(exc)
            if retry_after is not None:
                sleep_seconds = retry_after + random.uniform(0.1, 1.0)
            else:
                # Exponential backoff with jitter
                sleep_seconds = (2 ** attempt) * base_delay_seconds + random.uniform(0, 0.5)

            active_logger.info("Retrying in %.1fs...", sleep_seconds)
            time.sleep(sleep_seconds)
    return None
