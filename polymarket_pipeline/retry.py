import logging
import random
import time
from email.utils import parsedate_to_datetime
from typing import Any, Callable


def _parse_retry_after(exc: Exception) -> float | None:
    """Extract Retry-After value from a 429 or 503 response, if present.

    RFC 7231 allows two formats:
      - delta-seconds (integer or float): ``Retry-After: 120``
      - HTTP-date:   ``Retry-After: Wed, 21 Oct 2026 07:28:00 GMT``

    Cloudflare-fronted services (and AWS WAF) frequently send the
    HTTP-date variant; without parsing it we'd silently fall back to
    the 5 s default and hammer the service well before the real ban
    window has elapsed.
    """
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
        pass
    # Fall back to HTTP-date parsing.  parsedate_to_datetime returns an
    # aware datetime when the input has a timezone (RFC 7231 mandates
    # GMT) and a naive one otherwise; coerce to UTC either way.
    try:
        dt = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return 5.0
    if dt is None:
        return 5.0
    epoch = dt.timestamp() if dt.tzinfo else dt.timestamp()  # naive treated as local
    delay = epoch - time.time()
    if delay <= 0:
        return 1.0  # date already passed; minimum back-off floor
    return delay


def _is_non_retryable_client_error(exc: Exception) -> bool:
    """Return True for 4xx HTTP statuses that won't succeed on retry.

    408 (Request Timeout) and 429 (Too Many Requests) are explicitly
    excluded — those usually do recover after a delay.  All other 4xx
    codes (400/401/403/404/422/etc.) indicate a malformed or
    rejected request; retrying just wastes attempts and adds latency
    before any caller-level fallback can fire.
    """
    response = getattr(exc, "response", None)
    if response is None:
        return False
    status = getattr(response, "status_code", None)
    if not isinstance(status, int):
        return False
    if 400 <= status < 500 and status not in (408, 429):
        return True
    return False


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
            # Skip retry for non-retryable 4xx client errors so callers'
            # own fallback paths (e.g. api.py's 422 → no-order rerun)
            # fire immediately instead of waiting for 3 attempts ×
            # backoff to exhaust.
            if _is_non_retryable_client_error(exc):
                raise
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
