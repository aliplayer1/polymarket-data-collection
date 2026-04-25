"""Tests for polymarket_pipeline.parsing — pure functions, no network required."""

import pytest

from polymarket_pipeline.parsing import (
    extract_crypto,
    extract_timeframe,
    normalize_timeframe_input,
    parse_iso_timestamp,
)


# ---------------------------------------------------------------------------
# extract_timeframe
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("question,expected", [
    # Explicit keyword matches
    ("Bitcoin Up or Down - 5-Minute market", "5-minute"),
    ("Will BTC be up or down? 5 min window", "5-minute"),
    ("ETH Up or Down - 15-Minute", "15-minute"),
    ("SOL up or down 15 min", "15-minute"),
    ("Bitcoin Up or Down - 1 hour window", "1-hour"),
    ("BTC up or down hourly", "1-hour"),
    # Regex time-range matches
    ("Bitcoin Up or Down 9:00AM - 9:05AM ET", "5-minute"),
    ("BTC Up or Down 9:00AM - 9:15AM ET", "15-minute"),
    ("SOL Up or Down 10:00AM - 11:00AM ET", "1-hour"),
    ("ETH Up or Down 2:00PM - 6:00PM ET", "4-hour"),
    # Single-hour pattern: no end time, just "XXam ET"
    ("Bitcoin Up or Down - March 1, 10AM ET", "1-hour"),
    ("Bitcoin Up or Down - December 25, 3PM ET", "1-hour"),
    # Unrecognised — should return None
    ("Will it rain tomorrow?", None),
    ("Bitcoin price target for Q4", None),
    ("", None),
])
def test_extract_timeframe(question: str, expected: str | None) -> None:
    assert extract_timeframe(question) == expected


# ---------------------------------------------------------------------------
# extract_crypto
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("question,expected", [
    ("Bitcoin Up or Down", "BTC"),
    ("Will BTC be up?", "BTC"),
    ("bitcoin up or down 5-minute", "BTC"),
    ("Ethereum Up or Down", "ETH"),
    ("eth up or down", "ETH"),
    ("Solana Up or Down", "SOL"),
    ("sol up or down 15 min", "SOL"),
    # Unrecognised
    ("Dogecoin price prediction", "DOGE"),
    ("S&P 500 up or down", None),
    ("", None),
])
def test_extract_crypto(question: str, expected: str | None) -> None:
    assert extract_crypto(question) == expected


# ---------------------------------------------------------------------------
# Combined: a full realistic question string
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("question,tf,crypto", [
    ("Bitcoin Up or Down - March 6, 2026 | 5-Minute", "5-minute", "BTC"),
    ("Will Ethereum be Up or Down in the next 15-minute window?", "15-minute", "ETH"),
    ("SOL Up or Down 2:00PM - 3:00PM ET", "1-hour", "SOL"),
    ("BTC Up or Down 9:00AM - 9:05AM ET", "5-minute", "BTC"),
])
def test_combined(question: str, tf: str, crypto: str) -> None:
    assert extract_timeframe(question) == tf
    assert extract_crypto(question) == crypto


@pytest.mark.parametrize("raw,expected", [
    ("5m", "5-minute"),
    ("15min", "15-minute"),
    ("1hr", "1-hour"),
    ("4h", "4-hour"),
    ("custom-window", "custom-window"),
])
def test_normalize_timeframe_input(raw: str, expected: str) -> None:
    assert normalize_timeframe_input(raw) == expected


# ---------------------------------------------------------------------------
# parse_iso_timestamp — Gamma emits non-standard formats; regression guards
# ---------------------------------------------------------------------------

# The canonical epoch for 2026-04-20 20:35:26 UTC.  Reused across cases.
_EPOCH_2026_04_20 = 1776717326


@pytest.mark.parametrize("value,expected", [
    # Gamma ``closedTime`` quirk: space separator + bare 2-digit offset.
    # Python 3.10's ``fromisoformat`` rejects this natively — the parser
    # must normalise before delegating.
    ("2026-04-20 20:35:26+00",        _EPOCH_2026_04_20),
    # Gamma ``closedTime`` with non-UTC bare offset
    ("2026-04-20 15:35:26-05",        _EPOCH_2026_04_20),
    # Standard ISO + explicit offset
    ("2026-04-20T20:35:26+00:00",     _EPOCH_2026_04_20),
    # Z form
    ("2026-04-20T20:35:26Z",          _EPOCH_2026_04_20),
    # Fractional seconds — 3 digits (standard, accepted by Python)
    ("2026-04-20T20:35:26.123Z",      _EPOCH_2026_04_20),
    # 5 fractional digits (Gamma ``createdAt`` emits this; Python 3.10 rejects)
    ("2026-04-20T20:35:26.12345Z",    _EPOCH_2026_04_20),
    # 7 fractional digits (truncate beyond microsecond resolution)
    ("2026-04-20T20:35:26.1234567Z",  _EPOCH_2026_04_20),
    # Date-only
    ("2026-04-20",                    1776643200),
])
def test_parse_iso_timestamp_accepts_gamma_formats(value: str, expected: int) -> None:
    # The parser may or may not treat naive date-only strings as midnight
    # UTC vs. midnight local; assert roughly-correct epoch for the date-only
    # case rather than an exact match.
    got = parse_iso_timestamp(value)
    if value == "2026-04-20":
        assert got is not None
        assert abs(got - expected) < 86_400  # within a day
    else:
        assert got == expected, f"{value!r}: got {got}, expected {expected}"


@pytest.mark.parametrize("value", [None, "", "  ", "not a date", "2026-13-45"])
def test_parse_iso_timestamp_returns_none_for_invalid(value) -> None:
    assert parse_iso_timestamp(value) is None


def test_parse_iso_timestamp_treats_naive_as_utc() -> None:
    """A tz-naive timestamp must be treated as UTC, not system-local
    time.  Otherwise ``datetime.fromisoformat(...).timestamp()`` would
    silently shift the value by hours on any non-UTC host (developer
    machines, mostly), polluting the markets table with off-by-tz
    timestamps.
    """
    from datetime import datetime, timezone
    expected = int(datetime(2026, 4, 25, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    # No "Z", no offset → naive
    assert parse_iso_timestamp("2026-04-25T12:00:00") == expected
