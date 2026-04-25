"""Integration tests for WebSocketPhase private helpers.

These don't open a real WebSocket — they exercise the pure handlers
(``_apply_*``) and the buffer/flush plumbing with synthetic payloads.
"""
from __future__ import annotations


import pyarrow.parquet as pq

from polymarket_pipeline.models import MarketRecord
from polymarket_pipeline.phases.shared import PipelinePaths
from polymarket_pipeline.phases.websocket import WebSocketPhase
from polymarket_pipeline.phases.ws_messages import (
    parse_book_snapshot,
    parse_price_change,
)
from polymarket_pipeline.phases.ws_watchdog import DropOldestBuffer


def _make_phase(tmp_path) -> WebSocketPhase:
    paths = PipelinePaths.from_root(tmp_path / "data")
    paths.ensure_data_dir()
    # PriceHistoryPhase / LastTradePriceProvider are unused in these tests.
    phase = WebSocketPhase(
        last_trade_price_provider=None,
        price_history_phase=None,
        logger=__import__("logging").getLogger("test"),
        paths=paths,
    )
    return phase


def _make_market(market_id: str = "m1", timeframe: str = "1-hour", category: str = "crypto") -> MarketRecord:
    return MarketRecord(
        market_id=market_id,
        market_type="binary",
        question="Will BTC be up?",
        timeframe=timeframe,
        crypto="BTC",
        condition_id="cond",
        start_ts=1_700_000_000,
        end_ts=1_700_003_600,
        volume=1000.0,
        resolution=-1,
        is_active=True,
        closed_ts=0,
        up_token_id="0xUP",
        down_token_id="0xDOWN",
        category=category,
        tokens={"up": "0xUP", "down": "0xDOWN"},
    )


def test_bucket_by_timeframe_groups_rows():
    rows = [
        {"timeframe": "1-hour", "v": 1},
        {"timeframe": "1-hour", "v": 2},
        {"timeframe": "5-minute", "v": 3},
        {"missing": True},  # no timeframe — silently dropped
    ]
    out = WebSocketPhase._bucket_by_timeframe(rows)
    assert set(out.keys()) == {"1-hour", "5-minute"}
    assert len(out["1-hour"]) == 2


def test_spot_price_kwargs_drops_stale_entries(tmp_path):
    """If the RTDS feed is stalled, ``_spot_price_kwargs`` must not
    embed the last-known price into fresh ticks.  A stale spot price
    next to a current ``local_recv_ts_ns`` silently corrupts spread
    analysis downstream — better a NULL value than a wrong one.
    """
    import time
    phase = _make_phase(tmp_path)

    fresh_ts = int(time.time() * 1000) - 500  # 0.5 s old
    stale_ts = int(time.time() * 1000) - 60_000  # 60 s old (> 30 s cap)

    phase.spot_price_cache["btcusdt"] = (67000.5, fresh_ts)
    fresh = phase._spot_price_kwargs("BTC")
    assert fresh["spot_price_usdt"] == 67000.5
    assert fresh["spot_price_ts_ms"] == fresh_ts

    phase.spot_price_cache["btcusdt"] = (67000.5, stale_ts)
    stale = phase._spot_price_kwargs("BTC")
    assert stale == {"spot_price_usdt": None, "spot_price_ts_ms": None}

    # Unknown crypto (no RTDS mapping) — also returns None.
    unknown = phase._spot_price_kwargs("DOGECOIN")
    assert unknown == {"spot_price_usdt": None, "spot_price_ts_ms": None}


def test_book_snapshot_records_orderbook_row(tmp_path):
    phase = _make_phase(tmp_path)
    market = _make_market()
    token_to_market = {"0xUP": (market, "up")}
    token_bbo: dict = {}
    ob_buf = DropOldestBuffer(maxlen=100)

    book = parse_book_snapshot({
        "asset_id": "0xUP",
        "bids": [{"price": "0.49", "size": "100"}],
        "asks": [{"price": "0.51", "size": "200"}],
    })
    phase._apply_book_snapshot(book, token_to_market, token_bbo, ob_buf, local_recv_ts_ns=123)
    assert len(ob_buf) == 1
    row = list(ob_buf)[0]
    assert row["market_id"] == "m1"
    assert row["best_bid"] == 0.49
    assert row["local_recv_ts_ns"] == 123
    # per-token BBO state was updated
    assert token_bbo["0xUP"]["best_bid"] == 0.49


def test_price_change_preserves_bbo_state_across_partial_updates(tmp_path):
    """The classic bug: price_change sends only the changed side; we
    must merge into the existing per-token snapshot instead of zeroing."""
    phase = _make_phase(tmp_path)
    market = _make_market()
    token_to_market = {"0xUP": (market, "up")}
    token_bbo: dict = {}
    ob_buf = DropOldestBuffer(maxlen=100)

    # First: full book snapshot sets both sides
    book = parse_book_snapshot({
        "asset_id": "0xUP",
        "bids": [{"price": "0.49", "size": "100"}],
        "asks": [{"price": "0.51", "size": "200"}],
    })
    phase._apply_book_snapshot(book, token_to_market, token_bbo, ob_buf, local_recv_ts_ns=1)
    ob_buf.drain()  # ignore this row

    # Second: price_change with ONLY the bid changed.  The ask side in
    # the resulting orderbook row must still reflect the snapshot value.
    changes = parse_price_change({
        "price_changes": [{"asset_id": "0xUP", "best_bid": "0.495"}],
    })
    for change in changes:
        phase._apply_price_change(change, token_to_market, token_bbo, ob_buf, local_recv_ts_ns=2)
    rows = list(ob_buf)
    assert len(rows) == 1
    assert rows[0]["best_bid"] == 0.495
    assert rows[0]["best_ask"] == 0.51  # preserved
    assert token_bbo["0xUP"]["best_ask"] == 0.51


def test_price_change_drops_invalid_bbo(tmp_path):
    """Empty or inverted books shouldn't pollute storage.

    During a market's first ~170 s the CLOB often emits bids/asks with
    either side empty; those must not be recorded because downstream
    spread-based features would see spread = -bid or undefined values.
    """
    phase = _make_phase(tmp_path)
    market = _make_market()
    token_to_market = {"0xUP": (market, "up")}
    token_bbo: dict = {}
    ob_buf = DropOldestBuffer(maxlen=100)

    # Inverted book: bid >= ask
    book = parse_book_snapshot({
        "asset_id": "0xUP",
        "bids": [{"price": "0.51", "size": "100"}],
        "asks": [{"price": "0.49", "size": "200"}],
    })
    phase._apply_book_snapshot(book, token_to_market, token_bbo, ob_buf, local_recv_ts_ns=0)
    assert len(ob_buf) == 0  # rejected by is_valid_bbo


def test_flush_heartbeat_rows_reach_parquet(tmp_path):
    """End-to-end: registered heartbeats get persisted as ws_hb shard files."""
    phase = _make_phase(tmp_path)
    from polymarket_pipeline.phases.ws_watchdog import DataHeartbeat
    hb = DataHeartbeat({"price_change": 60.0}, grace_period_s=0.0)
    phase._heartbeats["clob_shard_0"] = hb
    hb.mark("price_change")

    rows = phase._build_heartbeat_rows()
    assert len(rows) == 1
    r = rows[0]
    assert r["source"] == "clob_ws"
    assert r["shard_key"] == "clob_shard_0"
    assert r["event_type"] == "price_change"
    assert r["last_event_age_ms"] >= 0

    # Flush path writes a shard file
    phase._flush_snapshot({}, {}, {}, [], rows)
    hb_dir = str(phase.paths.heartbeats_dir)
    files = [f for f in (__import__("os").listdir(hb_dir)) if f.endswith(".parquet")]
    assert files, f"expected a shard file in {hb_dir}"


def test_drop_alert_fires_after_threshold_jump(tmp_path, monkeypatch):
    phase = _make_phase(tmp_path)
    ws_buf = DropOldestBuffer(maxlen=2)
    tick_buf = DropOldestBuffer(maxlen=100)
    ob_buf = DropOldestBuffer(maxlen=100)

    fired = []
    # Patch the send_alert_async import that _check_drop_alerts uses.
    import polymarket_pipeline.phases.websocket as ws_mod
    monkeypatch.setattr(ws_mod, "send_alert_async", lambda *a, **k: fired.append((a, k)))
    # Lower the alert threshold so the test doesn't need 500 drops.
    monkeypatch.setattr(ws_mod, "WS_DROP_ALERT_THRESHOLD", 1)

    # Force a drop by over-filling the tiny ws_buf
    ws_buf.extend([1, 2, 3])
    assert ws_buf.dropped_count == 1

    phase._check_drop_alerts(ws_buf, tick_buf, ob_buf)
    assert fired, "drop alert did not fire"


def test_ws_prices_flush_produces_partitioned_parquet(tmp_path):
    phase = _make_phase(tmp_path)
    market = _make_market()

    ws_snapshot = {
        "1-hour": [
            {
                "market_id": market.market_id,
                "crypto": market.crypto,
                "timeframe": market.timeframe,
                "timestamp": 1_700_000_000,
                "up_price": 0.5,
                "down_price": 0.5,
                "volume": 100.0,
                "resolution": None,
                "question": market.question,
                "start_ts": market.start_ts,
                "end_ts": market.end_ts,
                "closed_ts": 0,
                "condition_id": market.condition_id,
                "up_token_id": market.up_token_id,
                "down_token_id": market.down_token_id,
                "slug": "",
                "fee_rate_bps": -1,
                "category": "crypto",
            }
        ]
    }
    phase._flush_snapshot(ws_snapshot, {}, {}, [], [])
    crypto_dir = phase.paths.prices_dir / "crypto=BTC" / "timeframe=1-hour"
    files = [f for f in crypto_dir.iterdir() if f.suffix == ".parquet"]
    assert files, f"no parquet shard under {crypto_dir}"
    df = pq.ParquetFile(str(files[0])).read().to_pandas()
    assert df.iloc[0]["up_price"] == 0.5
