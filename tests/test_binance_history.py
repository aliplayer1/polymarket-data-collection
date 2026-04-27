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


def test_spot_price_lookup_returns_largest_le_query():
    """``get(t)`` returns the entry with the largest ``ts_ms <= t`` —
    the most recent observation strictly in the past or AT ``t``.
    """
    lookup = SpotPriceLookup()
    lookup.add("BTC", 1000, 67000.0)
    lookup.add("BTC", 3000, 67200.0)
    lookup.finalize()

    # 1500 lies between two entries — must return the past one (1000),
    # never the future one (3000).
    result = lookup.get("BTC", 1500)
    assert result is not None
    assert result == (67000.0, 1000)


def test_spot_price_lookup_query_after_a_future_entry_still_returns_past():
    """Critical PiT case: even when a stored entry is closer to the
    query in absolute terms, ``get`` must NOT return it if its
    ``ts_ms`` is in the future.  Pre-PiT-fix this returned 3000
    (nearest); now it must return 1000 (latest observable).
    """
    lookup = SpotPriceLookup()
    lookup.add("BTC", 1000, 67000.0)
    lookup.add("BTC", 3000, 67200.0)
    lookup.finalize()

    # 2500 is closer to 3000 (Δ500) than 1000 (Δ1500), but at t=2500
    # the entry stored at ts_ms=3000 hasn't been observed yet.
    result = lookup.get("BTC", 2500)
    assert result is not None
    assert result == (67000.0, 1000), (
        "lookup must never return a future-timestamped price; "
        "regression of the SpotPriceLookup PiT contract"
    )


def test_spot_price_lookup_query_before_any_entry_returns_none():
    """When no observation has happened by ``t`` yet, ``get`` returns
    ``None`` (signals the caller to write NULL ``spot_price_usdt``)
    rather than fabricating a future-leaked value.
    """
    lookup = SpotPriceLookup()
    lookup.add("BTC", 1000, 67000.0)
    lookup.add("BTC", 3000, 67200.0)
    lookup.finalize()

    assert lookup.get("BTC", 999) is None
    assert lookup.get("BTC", 0) is None


def test_spot_price_lookup_missing_crypto():
    lookup = SpotPriceLookup()
    lookup.add("BTC", 1000, 67000.0)
    lookup.finalize()

    assert lookup.get("ETH", 1000) is None


def test_spot_price_lookup_empty():
    lookup = SpotPriceLookup()
    lookup.finalize()
    assert lookup.get("BTC", 1000) is None


def test_spot_price_lookup_single_entry_query_after_returns_it():
    """A single entry at ts=5000 satisfies any query at ts >= 5000."""
    lookup = SpotPriceLookup()
    lookup.add("ETH", 5000, 3200.0)
    lookup.finalize()

    result = lookup.get("ETH", 9999)
    assert result is not None
    assert result == (3200.0, 5000)


def test_spot_price_lookup_single_entry_query_before_returns_none():
    """A single entry at ts=5000 cannot satisfy a query at ts < 5000."""
    lookup = SpotPriceLookup()
    lookup.add("ETH", 5000, 3200.0)
    lookup.finalize()

    assert lookup.get("ETH", 4999) is None


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

def test_run_persists_close_price_keyed_at_close_time(monkeypatch, tmp_path):
    """PiT correctness: a 1-minute kline's close price is observable only at
    its close_time, never at its open_time.  Keying close_price on open_time
    leaks up to 60 s of future BTC into any consumer (subgraph_to_csv,
    hf_to_csv spot lookup), corrupting Optuna training data.

    This test asserts that ``BinanceHistoryPhase.run`` produces lookup
    entries and spot_prices rows whose ts_ms is the kline's close_time.
    """
    import logging

    from polymarket_pipeline.models import MarketRecord
    from polymarket_pipeline.phases.binance_history import BinanceHistoryPhase

    monkeypatch.setattr(
        "polymarket_pipeline.phases.binance_history.time.sleep",
        lambda s: None,
    )

    # Two consecutive 1-minute klines: opens at T and T+60s, closes 59999ms
    # later. Use deliberately different open and close prices so a wrong
    # implementation that keyed close_price on open_time can be detected
    # by inspecting either the lookup value or the spot_rows price column.
    open_t1, close_t1 = 1_700_000_000_000, 1_700_000_059_999
    open_t2, close_t2 = 1_700_000_060_000, 1_700_000_119_999
    klines = [
        [open_t1, "67000.0", "67100.0", "66900.0", "67050.0",
         "1.0", close_t1, 0, 0, 0, 0, 0],
        [open_t2, "67050.0", "67200.0", "67000.0", "67150.0",
         "2.0", close_t2, 0, 0, 0, 0, 0],
    ]

    class FakeResp:
        def __init__(self, body): self._body = body; self.status_code = 200; self.headers = {}
        def json(self): return self._body
        def raise_for_status(self): pass

    pages = [FakeResp(klines), FakeResp([])]

    class FakeSession:
        def get(self, url, params=None, timeout=None):
            return pages.pop(0)

    phase = BinanceHistoryPhase(logger=logging.getLogger("test_pit"))
    phase._session = FakeSession()

    market = MarketRecord(
        market_id="m1", market_type="binary", question="?",
        timeframe="5-minute", crypto="BTC", condition_id="0x",
        start_ts=open_t1 // 1000, end_ts=(close_t2 // 1000) + 60,
        volume=0.0, resolution=-1, is_active=False, closed_ts=0,
        up_token_id="1", down_token_id="2",
        slug="s", fee_rate_bps=2500,
    )
    spot_dir = tmp_path / "spot_prices"
    lookup = phase.run([market], spot_prices_dir=str(spot_dir))

    # Lookup must be keyed on close_time, not open_time.
    hit_close_t1 = lookup.get("BTC", close_t1)
    assert hit_close_t1 is not None
    assert hit_close_t1 == (67050.0, close_t1)
    hit_close_t2 = lookup.get("BTC", close_t2)
    assert hit_close_t2 is not None
    assert hit_close_t2 == (67150.0, close_t2)

    # Stored spot_prices parquet ts_ms must equal close_time, not open_time.
    import pandas as pd
    shards = list(spot_dir.glob("*.parquet"))
    assert shards, "expected at least one binance_history shard"
    df = pd.concat(pd.read_parquet(p) for p in shards)
    df = df.sort_values("ts_ms").reset_index(drop=True)
    binance_rows = df[df["source"] == "binance"]
    assert sorted(binance_rows["ts_ms"].tolist()) == [close_t1, close_t2], \
        "binance source rows must be keyed at close_time"
    chainlink_rows = df[df["source"] == "chainlink_proxy"]
    assert sorted(chainlink_rows["ts_ms"].tolist()) == [close_t1, close_t2], \
        "chainlink_proxy source rows must be keyed at close_time"


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
