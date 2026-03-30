"""Tests for polymarket_pipeline.storage — no network required.

Uses tmp_path fixture so all files are created in a temporary directory and
cleaned up automatically after each test.
"""

import os
import threading
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_pipeline.storage import (
    STORAGE_SCHEMA_VERSION,
    _check_disk_space,
    _write_lock,
    _write_parquet_atomic,
    _write_partitioned_atomic,
    append_ws_ticks_staged,
    append_ws_spot_prices_staged,
    append_ws_orderbook_staged,
    consolidate_ticks,
    consolidate_spot_prices,
    consolidate_orderbook,
    load_markets,
    load_prices,
    persist_normalized,
)


# ---------------------------------------------------------------------------
# _write_parquet_atomic
# ---------------------------------------------------------------------------

def test_write_parquet_atomic_creates_file(tmp_path):
    path = str(tmp_path / "test.parquet")
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    _write_parquet_atomic(table, path)
    assert os.path.exists(path)
    result = pq.read_table(path).to_pandas()
    assert len(result) == 3
    assert list(result["a"]) == [1, 2, 3]


def test_write_parquet_atomic_stamps_schema_version(tmp_path):
    path = str(tmp_path / "versioned.parquet")
    table = pa.table({"x": [1]})
    _write_parquet_atomic(table, path)
    t = pq.read_table(path)
    meta = t.schema.metadata or {}
    assert meta.get(b"schema_version") == str(STORAGE_SCHEMA_VERSION).encode()


def test_write_parquet_atomic_no_tmp_left_on_success(tmp_path):
    path = str(tmp_path / "clean.parquet")
    _write_parquet_atomic(pa.table({"v": [42]}), path)
    tmp_files = [f for f in os.listdir(tmp_path) if ".tmp" in f]
    assert tmp_files == []


# ---------------------------------------------------------------------------
# _write_partitioned_atomic
# ---------------------------------------------------------------------------

def test_write_partitioned_atomic_creates_hive_dirs(tmp_path):
    root = str(tmp_path / "prices")
    df = pd.DataFrame({
        "market_id": ["m1", "m1"],
        "timestamp": [100, 200],
        "up_price": [0.6, 0.7],
        "down_price": [0.4, 0.3],
        "crypto": ["BTC", "BTC"],
        "timeframe": ["5-minute", "5-minute"],
    })
    table = pa.Table.from_pandas(df, preserve_index=False)
    _write_partitioned_atomic(table, root, partition_cols=["crypto", "timeframe"])
    assert os.path.isdir(os.path.join(root, "crypto=BTC", "timeframe=5-minute"))


def test_write_partitioned_atomic_no_tmp_dir_left(tmp_path):
    root = str(tmp_path / "prices2")
    df = pd.DataFrame({
        "market_id": ["m1"], "timestamp": [1], "up_price": [0.5],
        "down_price": [0.5], "crypto": ["ETH"], "timeframe": ["1-hour"],
    })
    _write_partitioned_atomic(
        pa.Table.from_pandas(df, preserve_index=False),
        root, partition_cols=["crypto", "timeframe"],
    )
    tmp_dirs = [d for d in os.listdir(tmp_path) if ".tmp." in d]
    assert tmp_dirs == []


# ---------------------------------------------------------------------------
# _write_lock: single-process and thread-safety
# ---------------------------------------------------------------------------

def test_write_lock_creates_lock_file(tmp_path):
    root = str(tmp_path / "data")
    with _write_lock(root):
        pass
    assert os.path.exists(os.path.join(root, ".write.lock"))


def test_write_lock_is_reentrant_across_sequential_calls(tmp_path):
    root = str(tmp_path / "data2")
    with _write_lock(root):
        pass
    with _write_lock(root):
        pass  # second acquisition must succeed without deadlocking


def test_write_lock_thread_safety(tmp_path):
    """Two threads must not execute the critical section simultaneously."""
    root = str(tmp_path / "threaded")
    results: list[int] = []
    errors: list[Exception] = []

    def worker(val: int) -> None:
        try:
            with _write_lock(root):
                results.append(val)
                time.sleep(0.05)
                results.append(val + 10)
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors
    assert len(results) == 4
    # Each worker must have completed its pair before the other started.
    # i.e. [1,11,2,12] or [2,12,1,11] — not [1,2,11,12] or similar interleaving.
    idx1 = results.index(1)
    assert results[idx1 + 1] == 11, f"Thread interleaving detected: {results}"


# ---------------------------------------------------------------------------
# _check_disk_space
# ---------------------------------------------------------------------------

def test_check_disk_space_passes_on_existing_path(tmp_path):
    # Should not raise (unless the test machine has < 2 GB free, which is unlikely)
    _check_disk_space(str(tmp_path))


def test_check_disk_space_raises_on_low_threshold(tmp_path, monkeypatch):
    import polymarket_pipeline.storage as storage_mod
    original = storage_mod._MIN_FREE_DISK_GB
    storage_mod._MIN_FREE_DISK_GB = 1e12  # 1 TB — guaranteed to fail
    try:
        with pytest.raises(OSError, match="Insufficient disk space"):
            _check_disk_space(str(tmp_path))
    finally:
        storage_mod._MIN_FREE_DISK_GB = original


# ---------------------------------------------------------------------------
# persist_normalized round-trip
# ---------------------------------------------------------------------------

def test_persist_normalized_roundtrip(tmp_path):
    markets_path = str(tmp_path / "markets.parquet")
    prices_dir = str(tmp_path / "prices")

    markets_df = pd.DataFrame({
        "market_id": ["m1"],
        "question": ["BTC Up or Down - 5-Minute"],
        "crypto": ["BTC"],
        "timeframe": ["5-minute"],
        "volume": [1000.0],
        "resolution": [None],
        "start_ts": [0],
        "end_ts": [0],
        "condition_id": ["c1"],
        "up_token_id": ["u1"],
        "down_token_id": ["d1"],
    })
    prices_df = pd.DataFrame({
        "market_id": ["m1", "m1"],
        "timestamp": [1000, 2000],
        "up_price": [0.6, 0.65],
        "down_price": [0.4, 0.35],
        "crypto": ["BTC", "BTC"],
        "timeframe": ["5-minute", "5-minute"],
    })

    persist_normalized(
        markets_df, prices_df,
        markets_path=markets_path, prices_dir=prices_dir,
    )

    loaded_m = load_markets(markets_path)
    assert len(loaded_m) == 1
    assert loaded_m.iloc[0]["market_id"] == "m1"

    loaded_p = load_prices(prices_dir)
    assert len(loaded_p) == 2


def test_persist_normalized_deduplicates_on_rewrite(tmp_path):
    markets_path = str(tmp_path / "markets.parquet")
    prices_dir = str(tmp_path / "prices")

    def _write(ts_list):
        markets_df = pd.DataFrame({
            "market_id": ["m1"], "question": ["q"], "crypto": ["BTC"],
            "timeframe": ["5-minute"], "volume": [0.0], "resolution": [None],
            "start_ts": [0], "end_ts": [0], "condition_id": ["c1"],
            "up_token_id": ["u1"], "down_token_id": ["d1"],
        })
        prices_df = pd.DataFrame({
            "market_id": ["m1"] * len(ts_list),
            "timestamp": ts_list,
            "up_price": [0.5] * len(ts_list),
            "down_price": [0.5] * len(ts_list),
            "crypto": ["BTC"] * len(ts_list),
            "timeframe": ["5-minute"] * len(ts_list),
        })
        persist_normalized(markets_df, prices_df, markets_path=markets_path, prices_dir=prices_dir)

    _write([100, 200, 300])
    _write([200, 300, 400])  # overlap with previous

    loaded = load_prices(prices_dir)
    # Rows should be deduplicated: 100, 200, 300, 400
    assert len(loaded) == 4
    assert sorted(loaded["timestamp"].tolist()) == [100, 200, 300, 400]


# ---------------------------------------------------------------------------
# append_ws_ticks_staged
# ---------------------------------------------------------------------------

def test_append_ws_ticks_staged_creates_staging_file(tmp_path):
    ticks_dir = str(tmp_path / "ticks")
    df = pd.DataFrame({
        "market_id": ["m1"],
        "timestamp_ms": [1_000_000],
        "token_id": ["tok1"],
        "outcome": ["Up"],
        "side": ["BUY"],
        "price": [0.6],
        "size_usdc": [10.0],
        "tx_hash": ["0xabc"],
        "block_number": [0],
        "log_index": [0],
        "source": ["websocket"],
        "crypto": ["BTC"],
        "timeframe": ["5-minute"],
    })
    append_ws_ticks_staged(df, ticks_dir=ticks_dir)
    staging = os.path.join(ticks_dir, "crypto=BTC", "timeframe=5-minute", "ws_staging.parquet")
    assert os.path.exists(staging)
    t = pq.read_table(staging)
    assert len(t) == 1


def test_append_ws_ticks_staged_accumulates(tmp_path):
    ticks_dir = str(tmp_path / "ticks2")

    def _make_row(ts):
        return pd.DataFrame({
            "market_id": ["m1"], "timestamp_ms": [ts], "token_id": ["t1"],
            "outcome": ["Up"], "side": ["BUY"], "price": [0.5], "size_usdc": [1.0],
            "tx_hash": [""], "block_number": [0], "log_index": [0],
            "source": ["websocket"], "crypto": ["BTC"], "timeframe": ["5-minute"],
        })

    append_ws_ticks_staged(_make_row(1000), ticks_dir=ticks_dir)
    append_ws_ticks_staged(_make_row(2000), ticks_dir=ticks_dir)

    staging = os.path.join(ticks_dir, "crypto=BTC", "timeframe=5-minute", "ws_staging.parquet")
    t = pq.read_table(staging)
    assert len(t) == 2


def test_consolidate_ticks_merges_shards_and_deduplicates(tmp_path):
    ticks_dir = tmp_path / "ticks"
    shard_dir = ticks_dir / "crypto=BTC" / "timeframe=5-minute"
    shard_dir.mkdir(parents=True)

    shard_a = pd.DataFrame({
        "market_id": ["m1", "m1"],
        "timestamp_ms": [1_000, 2_000],
        "token_id": ["tok1", "tok1"],
        "outcome": ["Up", "Up"],
        "side": ["BUY", "BUY"],
        "price": [0.55, 0.60],
        "size_usdc": [10.0, 11.0],
        "tx_hash": ["0xabc", "0xdef"],
        "block_number": [100, 101],
        "log_index": [1, 2],
        "source": ["onchain", "onchain"],
    })
    shard_b = pd.DataFrame({
        "market_id": ["m1", "m2"],
        "timestamp_ms": [1_000, 3_000],
        "token_id": ["tok1", "tok9"],
        "outcome": ["Up", "Down"],
        "side": ["BUY", "SELL"],
        "price": [0.57, 0.42],
        "size_usdc": [12.0, 5.0],
        "tx_hash": ["0xabc", "0x999"],
        "block_number": [100, 102],
        "log_index": [1, 1],
        "source": ["websocket", "websocket"],
    })

    shard_a_path = shard_dir / "backfill_a.parquet"
    shard_b_path = shard_dir / "backfill_b.parquet"
    pq.write_table(pa.Table.from_pandas(shard_a, preserve_index=False), shard_a_path)
    pq.write_table(pa.Table.from_pandas(shard_b, preserve_index=False), shard_b_path)
    os.utime(shard_a_path, (1, 1))
    os.utime(shard_b_path, (2, 2))

    consolidate_ticks(ticks_dir=str(ticks_dir))

    assert sorted(os.listdir(shard_dir)) == ["part-0.parquet"]

    consolidated = pq.read_table(shard_dir / "part-0.parquet").to_pandas()
    dedup_keys = ["market_id", "timestamp_ms", "token_id", "tx_hash", "log_index"]
    assert len(consolidated) == 3
    assert len(consolidated.drop_duplicates(subset=dedup_keys)) == 3
    assert set(consolidated["market_id"]) == {"m1", "m2"}
    duplicate_key_row = consolidated[consolidated["tx_hash"] == "0xabc"].iloc[0]
    assert duplicate_key_row["price"] == pytest.approx(0.57)
    assert duplicate_key_row["source"] == "websocket"
    assert not os.path.exists(shard_dir / ".duckdb_tmp")


def test_consolidate_ticks_handles_legacy_shards_without_log_index(tmp_path):
    ticks_dir = tmp_path / "ticks"
    shard_dir = ticks_dir / "crypto=BTC" / "timeframe=5-minute"
    shard_dir.mkdir(parents=True)

    legacy_shard = pd.DataFrame({
        "market_id": ["m1"],
        "timestamp_ms": [1_000],
        "token_id": ["tok1"],
        "outcome": ["Up"],
        "side": ["BUY"],
        "price": [0.55],
        "size_usdc": [10.0],
        "tx_hash": ["0xabc"],
        "block_number": [100],
        "source": ["onchain"],
    })
    current_shard = pd.DataFrame({
        "market_id": ["m1"],
        "timestamp_ms": [1_000],
        "token_id": ["tok1"],
        "outcome": ["Up"],
        "side": ["BUY"],
        "price": [0.57],
        "size_usdc": [12.0],
        "tx_hash": ["0xabc"],
        "block_number": [100],
        "log_index": [0],
        "source": ["websocket"],
    })

    legacy_path = shard_dir / "legacy.parquet"
    current_path = shard_dir / "current.parquet"
    pq.write_table(pa.Table.from_pandas(legacy_shard, preserve_index=False), legacy_path)
    pq.write_table(pa.Table.from_pandas(current_shard, preserve_index=False), current_path)
    os.utime(legacy_path, (1, 1))
    os.utime(current_path, (2, 2))

    consolidate_ticks(ticks_dir=str(ticks_dir))

    consolidated = pq.read_table(shard_dir / "part-0.parquet").to_pandas()

    assert len(consolidated) == 1
    assert consolidated.iloc[0]["price"] == pytest.approx(0.57)
    assert int(consolidated.iloc[0]["log_index"]) == 0


# ---------------------------------------------------------------------------
# append_ws_spot_prices_staged
# ---------------------------------------------------------------------------

def test_append_ws_spot_prices_staged_creates_staging(tmp_path):
    spot_dir = str(tmp_path / "spot_prices")
    rows = [
        {"ts_ms": 1710000000000, "symbol": "btcusdt", "price": 67234.50, "source": "binance"},
        {"ts_ms": 1710000000100, "symbol": "btcusdt", "price": 67235.00, "source": "binance"},
        {"ts_ms": 1710000000050, "symbol": "btc/usd", "price": 67200.12, "source": "chainlink"},
    ]
    append_ws_spot_prices_staged(rows, spot_prices_dir=spot_dir)
    staging = os.path.join(spot_dir, "ws_staging.parquet")
    assert os.path.exists(staging)
    t = pq.read_table(staging).to_pandas()
    assert len(t) == 3
    assert set(t["source"].unique()) == {"binance", "chainlink"}
    assert t["price"].dtype == "float64"  # full precision


def test_append_ws_spot_prices_staged_accumulates(tmp_path):
    spot_dir = str(tmp_path / "spot_prices2")
    rows1 = [{"ts_ms": 1000, "symbol": "btcusdt", "price": 67000.0, "source": "binance"}]
    rows2 = [{"ts_ms": 2000, "symbol": "btcusdt", "price": 67001.0, "source": "binance"}]
    append_ws_spot_prices_staged(rows1, spot_prices_dir=spot_dir)
    append_ws_spot_prices_staged(rows2, spot_prices_dir=spot_dir)
    t = pq.read_table(os.path.join(spot_dir, "ws_staging.parquet")).to_pandas()
    assert len(t) == 2


def test_append_ws_spot_prices_staged_empty_noop(tmp_path):
    spot_dir = str(tmp_path / "spot_empty")
    append_ws_spot_prices_staged([], spot_prices_dir=spot_dir)
    assert not os.path.exists(spot_dir)


# ---------------------------------------------------------------------------
# consolidate_spot_prices
# ---------------------------------------------------------------------------

def test_consolidate_spot_prices_deduplicates(tmp_path):
    spot_dir = tmp_path / "spot_prices"
    spot_dir.mkdir()
    # Write two staging files with overlapping rows
    df1 = pd.DataFrame({
        "ts_ms": [1000, 2000],
        "symbol": ["btcusdt", "btcusdt"],
        "price": [67000.0, 67001.0],
        "source": ["binance", "binance"],
    })
    df2 = pd.DataFrame({
        "ts_ms": [2000, 3000],
        "symbol": ["btcusdt", "btcusdt"],
        "price": [67001.5, 67002.0],
        "source": ["binance", "binance"],
    })
    pq.write_table(pa.Table.from_pandas(df1, preserve_index=False), spot_dir / "ws_staging.parquet")
    pq.write_table(pa.Table.from_pandas(df2, preserve_index=False), spot_dir / "shard_2.parquet")

    consolidate_spot_prices(spot_prices_dir=str(spot_dir))

    assert sorted(os.listdir(spot_dir)) == ["part-0.parquet"]
    result = pq.read_table(spot_dir / "part-0.parquet").to_pandas()
    # ts_ms=2000 duplicated — should keep last (from shard_2)
    assert len(result) == 3
    assert result["ts_ms"].is_monotonic_increasing


# ---------------------------------------------------------------------------
# append_ws_orderbook_staged
# ---------------------------------------------------------------------------

def test_append_ws_orderbook_staged_creates_staging(tmp_path):
    ob_dir = str(tmp_path / "orderbook")
    df = pd.DataFrame({
        "ts_ms": [1710000000000],
        "market_id": ["m1"],
        "token_id": ["tok1"],
        "outcome": ["Up"],
        "best_bid": [0.48],
        "best_ask": [0.52],
        "best_bid_size": [150.0],
        "best_ask_size": [200.0],
        "crypto": ["BTC"],
        "timeframe": ["5-minute"],
    })
    append_ws_orderbook_staged(df, orderbook_dir=ob_dir)
    staging = os.path.join(ob_dir, "crypto=BTC", "timeframe=5-minute", "ws_staging.parquet")
    assert os.path.exists(staging)
    t = pq.read_table(staging).to_pandas()
    assert len(t) == 1
    assert t.iloc[0]["best_bid"] == pytest.approx(0.48)


def test_append_ws_orderbook_staged_accumulates(tmp_path):
    ob_dir = str(tmp_path / "orderbook2")
    def _make_row(ts):
        return pd.DataFrame({
            "ts_ms": [ts], "market_id": ["m1"], "token_id": ["t1"],
            "outcome": ["Up"], "best_bid": [0.5], "best_ask": [0.5],
            "best_bid_size": [100.0], "best_ask_size": [100.0],
            "crypto": ["BTC"], "timeframe": ["5-minute"],
        })
    append_ws_orderbook_staged(_make_row(1000), orderbook_dir=ob_dir)
    append_ws_orderbook_staged(_make_row(2000), orderbook_dir=ob_dir)
    staging = os.path.join(ob_dir, "crypto=BTC", "timeframe=5-minute", "ws_staging.parquet")
    t = pq.read_table(staging).to_pandas()
    assert len(t) == 2


# ---------------------------------------------------------------------------
# consolidate_orderbook
# ---------------------------------------------------------------------------

def test_consolidate_orderbook_merges_shards(tmp_path):
    ob_dir = tmp_path / "orderbook"
    shard_dir = ob_dir / "crypto=BTC" / "timeframe=5-minute"
    shard_dir.mkdir(parents=True)

    df1 = pd.DataFrame({
        "ts_ms": [1000, 2000],
        "market_id": ["m1", "m1"],
        "token_id": ["t1", "t1"],
        "outcome": ["Up", "Up"],
        "best_bid": [0.48, 0.49],
        "best_ask": [0.52, 0.51],
        "best_bid_size": [100.0, 110.0],
        "best_ask_size": [100.0, 90.0],
    })
    df2 = pd.DataFrame({
        "ts_ms": [3000],
        "market_id": ["m1"],
        "token_id": ["t1"],
        "outcome": ["Up"],
        "best_bid": [0.50],
        "best_ask": [0.50],
        "best_bid_size": [120.0],
        "best_ask_size": [80.0],
    })
    pq.write_table(pa.Table.from_pandas(df1, preserve_index=False), shard_dir / "ws_staging.parquet")
    pq.write_table(pa.Table.from_pandas(df2, preserve_index=False), shard_dir / "shard_2.parquet")

    consolidate_orderbook(orderbook_dir=str(ob_dir))

    assert sorted(os.listdir(shard_dir)) == ["part-0.parquet"]
    result = pq.read_table(shard_dir / "part-0.parquet").to_pandas()
    assert len(result) == 3
    assert result["ts_ms"].is_monotonic_increasing
