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
    append_ws_culture_prices_staged,
    append_ws_heartbeats_staged,
    append_ws_orderbook_staged,
    append_ws_prices_staged,
    append_ws_spot_prices_staged,
    append_ws_ticks_staged,
    consolidate_culture_prices,
    consolidate_heartbeats,
    consolidate_orderbook,
    consolidate_prices,
    consolidate_spot_prices,
    consolidate_ticks,
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


def test_write_partitioned_atomic_preserves_concurrent_shard_files(tmp_path):
    """_write_partitioned_atomic must NOT delete lock-free shard files
    written by concurrent processes (WS service, tick backfill)."""
    root = str(tmp_path / "prices")

    # First write creates the partition
    df1 = pd.DataFrame({
        "market_id": ["m1"], "timestamp": [100], "up_price": [0.5],
        "down_price": [0.5], "crypto": ["BTC"], "timeframe": ["1-hour"],
    })
    _write_partitioned_atomic(
        pa.Table.from_pandas(df1, preserve_index=False),
        root, partition_cols=["crypto", "timeframe"],
    )
    part_dir = os.path.join(root, "crypto=BTC", "timeframe=1-hour")

    # Simulate concurrent WS shard writes
    shard_names = [
        "ws_prices_12345_99999.parquet",
        "ws_ticks_12345_99999.parquet",
        "ws_culture_prices_12345_99999.parquet",
        "ws_ob_12345_99999.parquet",
        "backfill_12345.parquet",
    ]
    for name in shard_names:
        dummy = pa.table({"x": [1]})
        pq.write_table(dummy, os.path.join(part_dir, name))

    # Second write should replace part-0.parquet but NOT delete shard files
    df2 = pd.DataFrame({
        "market_id": ["m1"], "timestamp": [200], "up_price": [0.6],
        "down_price": [0.4], "crypto": ["BTC"], "timeframe": ["1-hour"],
    })
    _write_partitioned_atomic(
        pa.Table.from_pandas(df2, preserve_index=False),
        root, partition_cols=["crypto", "timeframe"],
    )

    remaining = set(os.listdir(part_dir))
    # All shard files must survive
    for name in shard_names:
        assert name in remaining, f"Shard file {name} was deleted by _write_partitioned_atomic"
    # part-0.parquet must exist with new content
    assert "part-0.parquet" in remaining
    result = pq.ParquetFile(os.path.join(part_dir, "part-0.parquet")).read().to_pandas()
    assert int(result["timestamp"].iloc[0]) == 200


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

def test_append_ws_ticks_staged_creates_shard_file(tmp_path):
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
    shard_dir = os.path.join(ticks_dir, "crypto=BTC", "timeframe=5-minute")
    shard_files = [f for f in os.listdir(shard_dir) if f.startswith("ws_ticks_")]
    assert len(shard_files) == 1
    t = pq.ParquetFile(os.path.join(shard_dir, shard_files[0])).read()
    assert len(t) == 1


def test_append_ws_ticks_staged_creates_independent_shards(tmp_path):
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

    shard_dir = os.path.join(ticks_dir, "crypto=BTC", "timeframe=5-minute")
    shard_files = [f for f in os.listdir(shard_dir) if f.startswith("ws_ticks_")]
    assert len(shard_files) == 2
    total_rows = sum(len(pq.ParquetFile(os.path.join(shard_dir, f)).read()) for f in shard_files)
    assert total_rows == 2


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

def test_append_ws_spot_prices_staged_creates_shard(tmp_path):
    spot_dir = str(tmp_path / "spot_prices")
    rows = [
        {"ts_ms": 1710000000000, "symbol": "btcusdt", "price": 67234.50, "source": "binance"},
        {"ts_ms": 1710000000100, "symbol": "btcusdt", "price": 67235.00, "source": "binance"},
        {"ts_ms": 1710000000050, "symbol": "btc/usd", "price": 67200.12, "source": "chainlink"},
    ]
    append_ws_spot_prices_staged(rows, spot_prices_dir=spot_dir)
    shard_files = [f for f in os.listdir(spot_dir) if f.startswith("ws_spot_")]
    assert len(shard_files) == 1
    t = pq.ParquetFile(os.path.join(spot_dir, shard_files[0])).read().to_pandas()
    assert len(t) == 3
    assert set(t["source"].unique()) == {"binance", "chainlink"}
    assert t["price"].dtype == "float64"  # full precision


def test_append_ws_spot_prices_staged_creates_independent_shards(tmp_path):
    spot_dir = str(tmp_path / "spot_prices2")
    rows1 = [{"ts_ms": 1000, "symbol": "btcusdt", "price": 67000.0, "source": "binance"}]
    rows2 = [{"ts_ms": 2000, "symbol": "btcusdt", "price": 67001.0, "source": "binance"}]
    append_ws_spot_prices_staged(rows1, spot_prices_dir=spot_dir)
    append_ws_spot_prices_staged(rows2, spot_prices_dir=spot_dir)
    shard_files = [f for f in os.listdir(spot_dir) if f.startswith("ws_spot_")]
    assert len(shard_files) == 2
    total_rows = sum(len(pq.ParquetFile(os.path.join(spot_dir, f)).read()) for f in shard_files)
    assert total_rows == 2


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

def test_append_ws_orderbook_staged_creates_shard(tmp_path):
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
    part_dir = os.path.join(ob_dir, "crypto=BTC", "timeframe=5-minute")
    shards = [f for f in os.listdir(part_dir) if f.startswith("ws_ob_")]
    assert len(shards) == 1
    t = pq.read_table(os.path.join(part_dir, shards[0])).to_pandas()
    assert len(t) == 1
    assert t.iloc[0]["best_bid"] == pytest.approx(0.48)


def test_append_ws_orderbook_staged_writes_separate_shards(tmp_path):
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
    part_dir = os.path.join(ob_dir, "crypto=BTC", "timeframe=5-minute")
    shards = [f for f in os.listdir(part_dir) if f.startswith("ws_ob_")]
    assert len(shards) == 2
    total_rows = sum(len(pq.read_table(os.path.join(part_dir, s))) for s in shards)
    assert total_rows == 2


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
    assert set(result["ts_ms"]) == {1000, 2000, 3000}


def test_orderbook_shard_write_then_consolidate(tmp_path):
    """End-to-end: append_ws_orderbook_staged writes shards, consolidate merges them."""
    ob_dir = str(tmp_path / "orderbook")

    def _make_batch(ts_values):
        return pd.DataFrame({
            "ts_ms": ts_values,
            "market_id": ["m1"] * len(ts_values),
            "token_id": ["t1"] * len(ts_values),
            "outcome": ["Up"] * len(ts_values),
            "best_bid": [0.48] * len(ts_values),
            "best_ask": [0.52] * len(ts_values),
            "best_bid_size": [100.0] * len(ts_values),
            "best_ask_size": [100.0] * len(ts_values),
            "crypto": ["BTC"] * len(ts_values),
            "timeframe": ["5-minute"] * len(ts_values),
        })

    # Simulate 3 flush cycles writing shard files
    append_ws_orderbook_staged(_make_batch([1000, 2000]), orderbook_dir=ob_dir)
    append_ws_orderbook_staged(_make_batch([3000, 4000]), orderbook_dir=ob_dir)
    append_ws_orderbook_staged(_make_batch([5000]), orderbook_dir=ob_dir)

    part_dir = os.path.join(ob_dir, "crypto=BTC", "timeframe=5-minute")
    shards = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
    assert len(shards) == 3  # one shard per flush

    # Consolidation merges all shards into part-0.parquet
    consolidate_orderbook(orderbook_dir=ob_dir)

    remaining = os.listdir(part_dir)
    assert remaining == ["part-0.parquet"]
    result = pq.read_table(os.path.join(part_dir, "part-0.parquet")).to_pandas()
    assert len(result) == 5
    assert sorted(result["ts_ms"].tolist()) == [1000, 2000, 3000, 4000, 5000]


def test_consolidate_orderbook_deduplicates_overlapping_rows(tmp_path):
    """Overlapping (ts_ms, market_id, token_id, outcome) rows from two shards
    must collapse to a single row, with the newer ``local_recv_ts_ns`` winning.

    Regression test for the missing-dedup bug: a process restart that re-emits
    buffered events, or two shards that wrote overlapping ``ts_ms`` rows, was
    leaving duplicates in part-0.parquet permanently.
    """
    ob_dir = tmp_path / "orderbook"
    shard_dir = ob_dir / "crypto=BTC" / "timeframe=5-minute"
    shard_dir.mkdir(parents=True)

    # Shard 1: older snapshot
    df_old = pd.DataFrame({
        "ts_ms": [1000, 2000],
        "market_id": ["m1", "m1"],
        "token_id": ["t1", "t1"],
        "outcome": ["Up", "Up"],
        "best_bid": [0.40, 0.41],
        "best_ask": [0.60, 0.59],
        "best_bid_size": [100.0, 110.0],
        "best_ask_size": [100.0, 90.0],
        "local_recv_ts_ns": [1_000_000_000, 2_000_000_000],
    })
    # Shard 2: NEWER snapshot, same key tuples ⇒ dedup must keep these values
    df_new = pd.DataFrame({
        "ts_ms": [1000, 2000],
        "market_id": ["m1", "m1"],
        "token_id": ["t1", "t1"],
        "outcome": ["Up", "Up"],
        "best_bid": [0.45, 0.46],
        "best_ask": [0.55, 0.54],
        "best_bid_size": [200.0, 210.0],
        "best_ask_size": [200.0, 190.0],
        "local_recv_ts_ns": [1_500_000_000, 2_500_000_000],
    })
    pq.write_table(pa.Table.from_pandas(df_old, preserve_index=False), shard_dir / "ws_ob_1.parquet")
    pq.write_table(pa.Table.from_pandas(df_new, preserve_index=False), shard_dir / "ws_ob_2.parquet")

    consolidate_orderbook(orderbook_dir=str(ob_dir))
    assert os.listdir(shard_dir) == ["part-0.parquet"]

    result = pq.read_table(shard_dir / "part-0.parquet").to_pandas()
    assert len(result) == 2  # two distinct ts_ms, no duplicates

    by_ts = {row["ts_ms"]: row for _, row in result.iterrows()}
    # Newer values (from df_new) must win because their local_recv_ts_ns is larger
    assert by_ts[1000]["best_bid"] == pytest.approx(0.45)
    assert by_ts[1000]["best_ask"] == pytest.approx(0.55)
    assert by_ts[1000]["best_bid_size"] == pytest.approx(200.0)
    assert by_ts[2000]["best_bid"] == pytest.approx(0.46)


# ---------------------------------------------------------------------------
# append_ws_prices_staged / consolidate_prices
# ---------------------------------------------------------------------------

def test_append_ws_prices_staged_creates_shards(tmp_path):
    prices_dir = str(tmp_path / "prices")
    df = pd.DataFrame({
        "market_id": ["m1", "m1"],
        "crypto": ["BTC", "BTC"],
        "timeframe": ["1-hour", "1-hour"],
        "timestamp": [100, 200],
        "up_price": [0.55, 0.60],
        "down_price": [0.45, 0.40],
        # Extra market-metadata cols that should be ignored by the shard writer
        "volume": [1000.0, 1000.0],
        "question": ["Will BTC go up?", "Will BTC go up?"],
        "category": ["crypto", "crypto"],
    })
    append_ws_prices_staged(df, prices_dir=prices_dir)

    part_dir = os.path.join(prices_dir, "crypto=BTC", "timeframe=1-hour")
    shards = [f for f in os.listdir(part_dir) if f.startswith("ws_prices_")]
    assert len(shards) == 1
    # Use ParquetFile to read without Hive-partition column inference
    result = pq.ParquetFile(os.path.join(part_dir, shards[0])).read().to_pandas()
    assert len(result) == 2
    assert set(result.columns) == {"market_id", "timestamp", "up_price", "down_price"}


def test_consolidate_prices_merges_shards(tmp_path):
    prices_dir = tmp_path / "prices"
    part_dir = prices_dir / "crypto=BTC" / "timeframe=1-hour"
    part_dir.mkdir(parents=True)

    # Existing consolidated data
    df_existing = pd.DataFrame({
        "market_id": ["m1"],
        "timestamp": [100],
        "up_price": [0.50],
        "down_price": [0.50],
    })
    pq.write_table(
        pa.Table.from_pandas(df_existing, preserve_index=False),
        part_dir / "part-0.parquet",
    )

    # Shard from WS
    df_shard = pd.DataFrame({
        "market_id": ["m1", "m1"],
        "timestamp": [100, 200],  # ts=100 is a duplicate
        "up_price": [0.55, 0.60],
        "down_price": [0.45, 0.40],
    })
    pq.write_table(
        pa.Table.from_pandas(df_shard, preserve_index=False),
        part_dir / "ws_prices_12345_99999.parquet",
    )

    consolidate_prices(prices_dir=str(prices_dir))

    assert sorted(os.listdir(part_dir)) == ["part-0.parquet"]
    result = pq.read_table(part_dir / "part-0.parquet").to_pandas()
    assert len(result) == 2  # deduped: ts=100 (keep last) + ts=200
    assert set(result["timestamp"]) == {100, 200}
    # ts=100 should have the shard's values (keep="last")
    row_100 = result[result["timestamp"] == 100].iloc[0]
    assert float(row_100["up_price"]) == pytest.approx(0.55, abs=0.01)


def test_prices_shard_write_then_consolidate_end_to_end(tmp_path):
    """End-to-end: staged writes then consolidation produces correct result."""
    prices_dir = str(tmp_path / "prices")

    def _make_batch(timestamps, up_prices):
        n = len(timestamps)
        return pd.DataFrame({
            "market_id": ["m1"] * n,
            "crypto": ["ETH"] * n,
            "timeframe": ["15-minute"] * n,
            "timestamp": timestamps,
            "up_price": up_prices,
            "down_price": [1.0 - p for p in up_prices],
        })

    append_ws_prices_staged(_make_batch([100, 200], [0.5, 0.6]), prices_dir=prices_dir)
    append_ws_prices_staged(_make_batch([300], [0.7]), prices_dir=prices_dir)

    part_dir = os.path.join(prices_dir, "crypto=ETH", "timeframe=15-minute")
    assert len([f for f in os.listdir(part_dir) if f.endswith(".parquet")]) == 2

    consolidate_prices(prices_dir=prices_dir)

    assert os.listdir(part_dir) == ["part-0.parquet"]
    result = pq.read_table(os.path.join(part_dir, "part-0.parquet")).to_pandas()
    assert len(result) == 3
    assert list(result.sort_values("timestamp")["timestamp"]) == [100, 200, 300]


# ---------------------------------------------------------------------------
# append_ws_culture_prices_staged / consolidate_culture_prices
# ---------------------------------------------------------------------------

def test_append_ws_culture_prices_staged_creates_shards(tmp_path):
    prices_dir = str(tmp_path / "prices")
    df = pd.DataFrame({
        "market_id": ["m1", "m1"],
        "crypto": ["ELON-TWEETS", "ELON-TWEETS"],
        "timeframe": ["7-day", "7-day"],
        "timestamp": [100, 200],
        "token_id": ["t1", "t2"],
        "outcome": ["Yes", "No"],
        "price": [0.70, 0.30],
        # Extra cols that should be ignored
        "volume": [500.0, 500.0],
        "category": ["culture", "culture"],
    })
    append_ws_culture_prices_staged(df, prices_dir=prices_dir)

    part_dir = os.path.join(prices_dir, "crypto=ELON-TWEETS", "timeframe=7-day")
    shards = [f for f in os.listdir(part_dir) if f.startswith("ws_culture_prices_")]
    assert len(shards) == 1
    # Use ParquetFile to read without Hive-partition column inference
    result = pq.ParquetFile(os.path.join(part_dir, shards[0])).read().to_pandas()
    assert len(result) == 2
    assert set(result.columns) == {"market_id", "timestamp", "token_id", "outcome", "price"}


def test_consolidate_culture_prices_deduplicates_on_three_columns(tmp_path):
    prices_dir = tmp_path / "prices"
    part_dir = prices_dir / "crypto=ELON-TWEETS" / "timeframe=7-day"
    part_dir.mkdir(parents=True)

    # Existing data
    df_existing = pd.DataFrame({
        "market_id": ["m1", "m1"],
        "timestamp": [100, 100],
        "token_id": ["t1", "t2"],
        "outcome": ["Yes", "No"],
        "price": [0.60, 0.40],
    })
    pq.write_table(
        pa.Table.from_pandas(df_existing, preserve_index=False),
        part_dir / "part-0.parquet",
    )

    # Shard with updated price for (m1, 100, "Yes") and a new row
    df_shard = pd.DataFrame({
        "market_id": ["m1", "m1"],
        "timestamp": [100, 200],
        "token_id": ["t1", "t3"],
        "outcome": ["Yes", "Maybe"],
        "price": [0.70, 0.15],
    })
    pq.write_table(
        pa.Table.from_pandas(df_shard, preserve_index=False),
        part_dir / "ws_culture_prices_12345_99999.parquet",
    )

    consolidate_culture_prices(prices_dir=str(prices_dir))

    assert sorted(os.listdir(part_dir)) == ["part-0.parquet"]
    result = pq.read_table(part_dir / "part-0.parquet").to_pandas()
    # 3 unique (market_id, timestamp, outcome) combos:
    # (m1, 100, Yes) — updated to 0.70
    # (m1, 100, No) — kept at 0.40
    # (m1, 200, Maybe) — new
    assert len(result) == 3
    yes_row = result[result["outcome"] == "Yes"].iloc[0]
    assert float(yes_row["price"]) == pytest.approx(0.70, abs=0.01)


def test_consolidate_prices_skips_partition_without_shards(tmp_path):
    """Consolidation is a no-op when only part-0.parquet exists (no shards)."""
    prices_dir = tmp_path / "prices"
    part_dir = prices_dir / "crypto=BTC" / "timeframe=1-hour"
    part_dir.mkdir(parents=True)

    df = pd.DataFrame({
        "market_id": ["m1"],
        "timestamp": [100],
        "up_price": [0.50],
        "down_price": [0.50],
    })
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        part_dir / "part-0.parquet",
    )

    consolidate_prices(prices_dir=str(prices_dir))

    # Still just part-0.parquet, unchanged
    assert os.listdir(part_dir) == ["part-0.parquet"]
    result = pq.read_table(part_dir / "part-0.parquet").to_pandas()
    assert len(result) == 1


# ---------------------------------------------------------------------------
# Culture markets: identity/grouping fields round-trip
# ---------------------------------------------------------------------------

def test_persist_culture_markets_roundtrip_new_fields(tmp_path):
    """Writing a culture market with slug/event_slug/bucket_index/bucket_label
    should round-trip through parquet (new schema)."""
    from polymarket_pipeline.storage import (
        persist_culture_normalized,
        load_culture_markets,
    )

    markets_path = str(tmp_path / "markets.parquet")
    prices_dir = str(tmp_path / "prices")

    markets_df = pd.DataFrame({
        "market_id": ["m-260-279"],
        "question": ["Will Elon Musk post 260-279 tweets from March 31 to April 7, 2026?"],
        "crypto": ["ELON-TWEETS"],
        "timeframe": ["7-day"],
        "volume": [12345.0],
        "resolution": [1],  # won
        "start_ts": [1743456000],
        "end_ts": [1744041600],
        "closed_ts": [1744041605],
        "condition_id": ["0xabc"],
        "tokens": ['{"Yes":"tok-yes","No":"tok-no"}'],
        "slug": ["elon-musk-of-tweets-march-31-april-7-260-279"],
        "event_slug": ["elon-musk-of-tweets-march-31-april-7"],
        "bucket_index": [13],
        "bucket_label": ["260-279"],
    })
    prices_df = pd.DataFrame({
        "market_id": ["m-260-279"],
        "timestamp": [1743500000],
        "token_id": ["tok-yes"],
        "outcome": ["Yes"],
        "price": [0.05],
        "crypto": ["ELON-TWEETS"],
        "timeframe": ["7-day"],
    })

    persist_culture_normalized(
        markets_df, prices_df,
        markets_path=markets_path, prices_dir=prices_dir,
    )

    loaded = load_culture_markets(markets_path)
    assert len(loaded) == 1
    row = loaded.iloc[0]
    assert row["market_id"] == "m-260-279"
    assert row["resolution"] == 1
    assert row["slug"] == "elon-musk-of-tweets-march-31-april-7-260-279"
    assert row["event_slug"] == "elon-musk-of-tweets-march-31-april-7"
    assert int(row["bucket_index"]) == 13
    assert row["bucket_label"] == "260-279"
    assert int(row["closed_ts"]) == 1744041605


def test_schema_version_is_v5() -> None:
    # Sentinel test: bumping the schema version is a deliberate act that
    # downstream consumers depend on, so fail loudly if it regresses.
    assert STORAGE_SCHEMA_VERSION == 5


def test_ticks_schema_has_order_hash_v5() -> None:
    from polymarket_pipeline.storage import TICKS_SCHEMA, _TICKS_EMPTY_COLS
    assert "order_hash" in TICKS_SCHEMA.names
    assert "order_hash" in _TICKS_EMPTY_COLS
    # Nullable string, not a dict — subgraph fills populate it, WS/legacy
    # rows leave it NULL.
    field = TICKS_SCHEMA.field("order_hash")
    assert pa.types.is_string(field.type)


def test_consolidate_ticks_preserves_order_hash_and_dedups_by_it(tmp_path):
    """v4 (no order_hash) + v5 (with order_hash) shards must coexist."""
    from polymarket_pipeline.storage import consolidate_ticks
    part = tmp_path / "crypto=BTC" / "timeframe=1-hour"
    part.mkdir(parents=True)

    # Legacy row: one fill in tx X at log_index 5, no order_hash.
    legacy = pd.DataFrame({
        "market_id": ["m1"], "timestamp_ms": [1700000000000], "token_id": ["0xA"],
        "outcome": ["Up"], "side": ["BUY"], "price": [0.5], "size_usdc": [10.0],
        "tx_hash": ["0xtx"], "block_number": [100], "log_index": [5],
        "source": ["onchain"], "spot_price_usdt": [67000.0],
        "spot_price_ts_ms": [1700000000000], "local_recv_ts_ns": [None],
    })
    _write_parquet_atomic(
        pa.Table.from_pandas(legacy, preserve_index=False),
        str(part / "backfill_legacy.parquet"),
    )
    # Two subgraph fills in SAME tx at SAME timestamp — would collide under
    # the old dedup (tx_hash + log_index=0 identical), but have distinct
    # order_hash values so the v5 key preserves them.
    new = pd.DataFrame({
        "market_id": ["m1", "m1"],
        "timestamp_ms": [1700000100000, 1700000100000],
        "token_id": ["0xA", "0xA"],
        "outcome": ["Up", "Up"], "side": ["BUY", "SELL"],
        "price": [0.5, 0.51], "size_usdc": [5.0, 5.0],
        "tx_hash": ["0xtx2", "0xtx2"],
        "block_number": [0, 0], "log_index": [0, 0],
        "source": ["onchain", "onchain"],
        "spot_price_usdt": [67000.0, 67000.0],
        "spot_price_ts_ms": [1700000100000, 1700000100000],
        "local_recv_ts_ns": [None, None],
        "order_hash": ["0xorder_A", "0xorder_B"],
    })
    _write_parquet_atomic(
        pa.Table.from_pandas(new, preserve_index=False),
        str(part / "backfill_new.parquet"),
    )

    consolidate_ticks(ticks_dir=str(tmp_path))
    out = pq.ParquetFile(str(part / "part-0.parquet")).read().to_pandas()
    assert len(out) == 3, f"expected 3 rows (1 legacy + 2 subgraph), got {len(out)}"
    # Legacy row NULL order_hash survives; subgraph rows carry their distinct hashes.
    order_hashes = set(str(x) if x is not None else "NULL" for x in out["order_hash"])
    assert order_hashes == {"NULL", "0xorder_A", "0xorder_B"}


def test_heartbeats_append_and_consolidate_roundtrip(tmp_path):
    hb_dir = str(tmp_path / "heartbeats")
    rows1 = [
        {"ts_ms": 1000, "source": "clob_ws", "shard_key": "0",
         "event_type": "price_change", "last_event_age_ms": 500},
        {"ts_ms": 1010, "source": "rtds_binance", "shard_key": "btcusdt",
         "event_type": "btcusdt", "last_event_age_ms": 100},
    ]
    rows2 = [
        {"ts_ms": 2000, "source": "clob_ws", "shard_key": "0",
         "event_type": "price_change", "last_event_age_ms": 700},
    ]
    append_ws_heartbeats_staged(rows1, heartbeats_dir=hb_dir)
    # Next shard has a later monotonic timestamp
    time.sleep(0.01)
    append_ws_heartbeats_staged(rows2, heartbeats_dir=hb_dir)

    shards = [f for f in os.listdir(hb_dir) if f.endswith(".parquet")]
    assert len(shards) == 2, shards
    consolidate_heartbeats(heartbeats_dir=hb_dir)
    after = [f for f in os.listdir(hb_dir) if f.endswith(".parquet")]
    assert after == ["part-0.parquet"]

    df = pq.ParquetFile(os.path.join(hb_dir, "part-0.parquet")).read().to_pandas()
    assert len(df) == 3
    assert set(df["source"]) == {"clob_ws", "rtds_binance"}


def test_heartbeats_empty_rows_noop(tmp_path):
    hb_dir = str(tmp_path / "heartbeats")
    append_ws_heartbeats_staged([], heartbeats_dir=hb_dir)
    # Directory should not exist (no rows, no side-effect)
    assert not os.path.exists(hb_dir)


def test_ticks_schema_has_local_recv_ts_ns():
    from polymarket_pipeline.storage import TICKS_SCHEMA
    assert "local_recv_ts_ns" in TICKS_SCHEMA.names


def test_orderbook_schema_has_local_recv_ts_ns():
    from polymarket_pipeline.storage import ORDERBOOK_SCHEMA
    assert "local_recv_ts_ns" in ORDERBOOK_SCHEMA.names


def test_persist_culture_markets_backcompat_missing_new_fields(tmp_path):
    """Old writers that didn't emit the new identity columns should still
    produce valid parquet (columns default to empty/-1)."""
    from polymarket_pipeline.storage import (
        persist_culture_normalized,
        load_culture_markets,
    )

    markets_path = str(tmp_path / "markets.parquet")
    prices_dir = str(tmp_path / "prices")

    # Minimal legacy columns — no slug / event_slug / bucket_* / closed_ts
    markets_df = pd.DataFrame({
        "market_id": ["legacy-1"],
        "question": ["legacy market"],
        "crypto": ["ELON-TWEETS"],
        "timeframe": ["7-day"],
        "volume": [0.0],
        "resolution": [None],
        "start_ts": [0],
        "end_ts": [0],
        "condition_id": ["c1"],
        "tokens": ['{}'],
    })
    prices_df = pd.DataFrame({
        "market_id": ["legacy-1"],
        "timestamp": [100],
        "token_id": ["tok-1"],
        "outcome": ["Yes"],
        "price": [0.5],
        "crypto": ["ELON-TWEETS"],
        "timeframe": ["7-day"],
    })

    persist_culture_normalized(
        markets_df, prices_df,
        markets_path=markets_path, prices_dir=prices_dir,
    )

    loaded = load_culture_markets(markets_path)
    assert len(loaded) == 1
    row = loaded.iloc[0]
    # Defaults for the new columns
    assert row["slug"] == ""
    assert row["event_slug"] == ""
    assert int(row["bucket_index"]) == -1
    assert row["bucket_label"] == ""
    assert int(row["closed_ts"]) == 0
    # Resolution still maps None → -1
    assert int(row["resolution"]) == -1
