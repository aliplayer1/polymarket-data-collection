import logging
import random
import time
from typing import Any, Callable


def _parse_retry_after(exc: Exception) -> float | None:
    """Extract Retry-After value from a 429 or 503 response, if present."""
    response = getattr(exc, "response", None)
    if response is None:
        return None
    status = getattr(response, "status_code", None)
    if status not in (429, 503):
        return None
    raw = response.headers.get("Retry-After")
    if raw is None:
        return 5.0 if status == 429 else 3.0  # sensible defaults per status
    try:
        return max(float(raw), 1.0)
    except (TypeError, ValueError):
        return 5.0


def api_call_with_retry(
    func: Callable[..., Any],
    *args: Any,
    max_attempts: int = 3,
    base_delay_seconds: float = 1.5,
    min_retry_after: float = 0.0,
    logger: logging.Logger | None = None,
    **kwargs: Any,
) -> Any:
    """Call *func* with exponential backoff on failure.

    Parameters
    ----------
    min_retry_after:
        Floor for the retry-after delay when a rate-limit (429/503) response
        is detected.  Pyth Hermes enforces a 60-second ban on 429, so callers
        should pass ``min_retry_after=61`` for Hermes calls.
    """
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

            # Respect Retry-After header on 429/503 rate-limit responses
            retry_after = _parse_retry_after(exc)
            if retry_after is not None:
                sleep_seconds = max(retry_after, min_retry_after) + random.uniform(0.1, 1.0)
            else:
                # Exponential backoff with jitter
                sleep_seconds = (2 ** attempt) * base_delay_seconds + random.uniform(0, 0.5)

            active_logger.info("Retrying in %.1fs...", sleep_seconds)
            time.sleep(sleep_seconds)
    return None
