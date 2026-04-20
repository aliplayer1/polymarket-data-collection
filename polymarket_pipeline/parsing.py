from __future__ import annotations

import re
from datetime import datetime
from typing import Sequence

from .markets import (
    BinaryMarketDefinition,
    DEFAULT_MARKET_DEFINITION,
    get_matching_market_definition,
    normalize_timeframe_value,
)

# Matches a bare 2-digit timezone offset like "+00" or "-05" at end of string.
# Python 3.10's ``datetime.fromisoformat`` requires "+HH:MM" — Gamma's
# closedTime field breaks that, so we normalise first.
_BARE_OFFSET_RE = re.compile(r"([+-])(\d{2})$")

# Matches fractional-second portions (e.g. ".65102" in "20:36:55.65102Z").
# Python 3.10's ``fromisoformat`` only accepts exactly 3 or 6 fractional
# digits; anything else (Gamma emits 5 or 7) is rejected.  We truncate
# or pad to 6 digits (microseconds).
_FRACTIONAL_RE = re.compile(r"\.(\d+)")


def _normalise_fraction(match: re.Match[str]) -> str:
    frac = match.group(1)
    if len(frac) > 6:
        frac = frac[:6]
    elif len(frac) < 6:
        frac = frac.ljust(6, "0")
    return f".{frac}"


def extract_timeframe(
    question: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> str | None:
    definition = get_matching_market_definition(question, definitions) or DEFAULT_MARKET_DEFINITION
    return definition.extract_timeframe(question)


def extract_asset(
    question: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> str | None:
    definition = get_matching_market_definition(question, definitions) or DEFAULT_MARKET_DEFINITION
    return definition.extract_asset(question)


def extract_crypto(
    question: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> str | None:
    return extract_asset(question, definitions)


def normalize_timeframe_input(
    value: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> str:
    return normalize_timeframe_value(value, definitions)


def parse_iso_timestamp(value: str | None) -> int | None:
    """Parse an ISO-8601-ish timestamp into an epoch second.

    Gamma API is inconsistent about format:
      - ``endDate``    : ``2026-04-20T20:35:00Z``            (standard ISO)
      - ``closedTime`` : ``2026-04-20 20:35:26+00``          (space + bare offset)
      - ``createdAt``  : ``2026-04-19T20:36:55.65102Z``      (fractional + Z)

    Python 3.10's ``datetime.fromisoformat`` rejects the space separator
    and bare 2-digit offsets (``+00`` vs the required ``+00:00``).  This
    helper normalises both quirks before parsing, so upstream callers can
    treat any Gamma time field uniformly.

    Returns None for empty / unparseable input (never raises).
    """
    if not value:
        return None
    v = value.strip()
    if " " in v and "T" not in v:
        v = v.replace(" ", "T", 1)
    v = v.replace("Z", "+00:00")
    v = _BARE_OFFSET_RE.sub(r"\1\2:00", v)
    v = _FRACTIONAL_RE.sub(_normalise_fraction, v)
    try:
        return int(datetime.fromisoformat(v).timestamp())
    except (TypeError, ValueError):
        return None
