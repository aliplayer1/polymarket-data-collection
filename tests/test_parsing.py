"""Tests for polymarket_pipeline.parsing — pure functions, no network required."""

import pytest

from polymarket_pipeline.parsing import (
    extract_crypto,
    extract_timeframe,
    normalize_timeframe_input,
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
    ("Dogecoin price prediction", None),
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
