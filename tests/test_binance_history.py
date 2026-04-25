"""Tests for BinanceHistoryPhase and SpotPriceLookup — no network required."""

from polymarket_pipeline.phases.binance_history import (
    SpotPriceLookup,
    _merge_ranges,
)


# ---------------------------------------------------------------------------
# SpotPriceLookup
# ---------------------------------------------------------------------------

def test_spot_price_lookup_exact_match():
    lookup = SpotPriceLookup()
    lookup.add("BTC", 1000, 67000.0)
    lookup.add("BTC", 2000, 67100.0)
    lookup.add("BTC", 3000, 67200.0)
    lookup.finalize()

    result = lookup.get("BTC", 2000)
    assert result is not None
    assert result[0] == 67100.0
    assert result[1] == 2000


def test_spot_price_lookup_nearest_before():
    lookup = SpotPriceLookup()
    lookup.add("BTC", 1000, 67000.0)
    lookup.add("BTC", 3000, 67200.0)
    lookup.finalize()

    # 1500 is closer to 1000 than 3000
    result = lookup.get("BTC", 1500)
    assert result is not None
    assert result[0] == 67000.0


def test_spot_price_lookup_nearest_after():
    lookup = SpotPriceLookup()
    lookup.add("BTC", 1000, 67000.0)
    lookup.add("BTC", 3000, 67200.0)
    lookup.finalize()

    # 2500 is closer to 3000 than 1000
    result = lookup.get("BTC", 2500)
    assert result is not None
    assert result[0] == 67200.0


def test_spot_price_lookup_missing_crypto():
    lookup = SpotPriceLookup()
    lookup.add("BTC", 1000, 67000.0)
    lookup.finalize()

    assert lookup.get("ETH", 1000) is None


def test_spot_price_lookup_empty():
    lookup = SpotPriceLookup()
    lookup.finalize()
    assert lookup.get("BTC", 1000) is None


def test_spot_price_lookup_single_entry():
    lookup = SpotPriceLookup()
    lookup.add("ETH", 5000, 3200.0)
    lookup.finalize()

    result = lookup.get("ETH", 9999)
    assert result is not None
    assert result[0] == 3200.0


def test_spot_price_lookup_len():
    lookup = SpotPriceLookup()
    lookup.add("BTC", 1000, 67000.0)
    lookup.add("BTC", 2000, 67100.0)
    lookup.add("ETH", 1000, 3200.0)
    assert len(lookup) == 3


# ---------------------------------------------------------------------------
# _merge_ranges
# ---------------------------------------------------------------------------

def test_merge_ranges_non_overlapping():
    # Gaps > 60_000ms (1 minute adjacency threshold)
    ranges = [(100_000, 200_000), (300_000, 400_000), (500_000, 600_000)]
    assert _merge_ranges(ranges) == [(100_000, 200_000), (300_000, 400_000), (500_000, 600_000)]


def test_merge_ranges_overlapping():
    ranges = [(100, 300), (200, 400), (350, 500)]
    assert _merge_ranges(ranges) == [(100, 500)]


def test_merge_ranges_adjacent():
    # Within 60_000ms (1 minute) adjacency threshold
    ranges = [(100, 200), (200 + 60_000, 300 + 60_000)]
    assert _merge_ranges(ranges) == [(100, 300 + 60_000)]


def test_merge_ranges_unsorted():
    ranges = [(500_000, 600_000), (100_000, 200_000), (300_000, 400_000)]
    assert _merge_ranges(ranges) == [(100_000, 200_000), (300_000, 400_000), (500_000, 600_000)]


def test_merge_ranges_empty():
    assert _merge_ranges([]) == []


def test_merge_ranges_single():
    assert _merge_ranges([(100, 200)]) == [(100, 200)]


# ---------------------------------------------------------------------------
# HTTP 418 (IP ban) handling
# ---------------------------------------------------------------------------

def test_fetch_klines_handles_http_418_ip_ban(monkeypatch):
    """A 418 response (Binance IP ban) must trigger a long retry-after
    sleep instead of falling through to the broad ``except`` and
    silently breaking pagination for the requested range.  Retry must
    eventually succeed when the ban lifts.
    """
    import logging
    from polymarket_pipeline.phases.binance_history import BinanceHistoryPhase

    sleeps: list[float] = []
    monkeypatch.setattr(
        "polymarket_pipeline.phases.binance_history.time.sleep",
        lambda s: sleeps.append(s),
    )

    class FakeResp:
        def __init__(self, status: int, body: object, headers: dict | None = None):
            self.status_code = status
            self._body = body
            self.headers = headers or {}

        def json(self):
            return self._body

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")

    responses = [
        FakeResp(418, {}, headers={"Retry-After": "30"}),  # first call → ban
        FakeResp(200, []),  # second call (after ban "lifts") → empty kline page → break
    ]

    class FakeSession:
        def get(self, url, params=None, timeout=None):
            return responses.pop(0)

    phase = BinanceHistoryPhase(logger=logging.getLogger("test_b418"))
    phase._session = FakeSession()
    out = phase._fetch_klines("BTCUSDT", 0, 1_000_000)
    assert out == []
    # The 418 path must call time.sleep with the Retry-After value.
    assert 30.0 in sleeps
