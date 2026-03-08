import re
from datetime import datetime

from .config import CRYPTO_ALIASES


TIME_RANGE_PATTERN = re.compile(
    r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*-\s*(\d{1,2}):(\d{2})\s*(AM|PM)",
    flags=re.IGNORECASE,
)

# Matches single-hour format: "Bitcoin Up or Down - March 1, 10AM ET"
# No end time means the market covers exactly one hour.
SINGLE_HOUR_PATTERN = re.compile(
    r"\b\d{1,2}(?:AM|PM)\s+ET\b",
    flags=re.IGNORECASE,
)


def _minutes_from_time_range(question: str) -> int | None:
    match = TIME_RANGE_PATTERN.search(question)
    if not match:
        return None

    start_h, start_m, start_meridiem, end_h, end_m, end_meridiem = match.groups()
    start_dt = datetime.strptime(f"{start_h}:{start_m}{start_meridiem.upper()}", "%I:%M%p")
    end_dt = datetime.strptime(f"{end_h}:{end_m}{end_meridiem.upper()}", "%I:%M%p")
    delta_minutes = int((end_dt - start_dt).total_seconds() // 60)
    if delta_minutes <= 0:
        delta_minutes += 24 * 60
    return delta_minutes


def extract_timeframe(question: str) -> str | None:
    q = question.lower()
    # Check longer prefixes BEFORE shorter ones to prevent substring false matches:
    # "15 min" contains "5 min" as a trailing substring, so "5 min" must not be
    # checked first.  Same principle applies to "4 hour" vs "1 hour".
    if "15 min" in q or "15-minute" in q:
        return "15-minute"
    if "5 min" in q or "5-minute" in q:
        return "5-minute"
    if "4 hour" in q or "4-hour" in q:
        return "4-hour"
    if "1 hour" in q or "hourly" in q or "hour" in q:
        return "1-hour"

    range_minutes = _minutes_from_time_range(question)
    if range_minutes == 5:
        return "5-minute"
    if range_minutes == 15:
        return "15-minute"
    if range_minutes == 60:
        return "1-hour"
    if range_minutes == 240:
        return "4-hour"

    # Single-hour format: "Bitcoin Up or Down - March 1, 10AM ET" (no end time).
    if SINGLE_HOUR_PATTERN.search(question):
        return "1-hour"

    return None


def extract_crypto(question: str) -> str | None:
    q = question.lower()
    for symbol, aliases in CRYPTO_ALIASES.items():
        if any(alias in q for alias in aliases):
            return symbol
    return None


def parse_iso_timestamp(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except (TypeError, ValueError):
        return None
