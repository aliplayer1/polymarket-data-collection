"""Tests for the DuckDB query layer (``polymarket_pipeline.query``)."""
from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def query_root(tmp_path, monkeypatch):
    """Build a tiny self-contained data root with all 6 view sources
    populated, then point the query module at it."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # markets.parquet
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({
            "market_id": ["m1"], "question": ["q"],
            "crypto": ["BTC"], "timeframe": ["1-hour"],
            "volume": [100.0], "resolution": [-1],
            "start_ts": [1700000000], "end_ts": [1700003600],
            "closed_ts": [0], "condition_id": [""],
            "up_token_id": ["t1"], "down_token_id": ["t2"],
            "slug": [""], "fee_rate_bps": [-1],
        })),
        str(data_dir / "markets.parquet"),
    )

    # prices/ — Hive partitioned
    prices_dir = data_dir / "prices" / "crypto=BTC" / "timeframe=1-hour"
    prices_dir.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({
            "market_id": ["m1"], "timestamp": [100],
            "up_price": [0.5], "down_price": [0.5],
        })),
        str(prices_dir / "part-0.parquet"),
    )

    # ticks/ — Hive partitioned
    ticks_dir = data_dir / "ticks" / "crypto=BTC" / "timeframe=1-hour"
    ticks_dir.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({
            "market_id": ["m1"], "timestamp_ms": [1700000000000],
            "token_id": ["t1"], "outcome": ["Up"], "side": ["BUY"],
            "price": [0.5], "size_usdc": [10.0], "tx_hash": ["0xtx"],
            "block_number": [0], "log_index": [0], "source": ["onchain"],
        })),
        str(ticks_dir / "part-0.parquet"),
    )

    # orderbook/ — Hive partitioned
    ob_dir = data_dir / "orderbook" / "crypto=BTC" / "timeframe=5-minute"
    ob_dir.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({
            "ts_ms": [1700000000000], "market_id": ["m1"],
            "token_id": ["t1"], "outcome": ["Up"],
            "best_bid": [0.49], "best_ask": [0.51],
            "best_bid_size": [100.0], "best_ask_size": [100.0],
        })),
        str(ob_dir / "part-0.parquet"),
    )

    # spot_prices/ — flat dir
    spot_dir = data_dir / "spot_prices"
    spot_dir.mkdir()
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({
            "ts_ms": [1700000000000],
            "symbol": ["btcusdt"],
            "price": [67000.0],
            "source": ["binance"],
        })),
        str(spot_dir / "part-0.parquet"),
    )

    # heartbeats/ — flat dir
    hb_dir = data_dir / "heartbeats"
    hb_dir.mkdir()
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({
            "ts_ms": [1700000000000],
            "source": ["clob_ws"],
            "shard_key": ["0"],
            "event_type": ["price_change"],
            "last_event_age_ms": [500],
        })),
        str(hb_dir / "part-0.parquet"),
    )

    # Point the module's path constants at this temp data root.
    import polymarket_pipeline.query as q
    monkeypatch.setattr(q, "PARQUET_MARKETS_PATH", str(data_dir / "markets.parquet"))
    monkeypatch.setattr(q, "PARQUET_PRICES_DIR", str(data_dir / "prices"))
    monkeypatch.setattr(q, "PARQUET_TICKS_DIR", str(data_dir / "ticks"))
    return data_dir


def test_query_views_cover_all_six_datasets(query_root):
    """The query layer must expose markets/prices/ticks AND the three
    secondary tables (orderbook/spot_prices/heartbeats) so operators
    don't have to reach for ``read_parquet(...)`` directly.
    """
    from polymarket_pipeline.query import query

    for table in ("markets", "prices", "ticks", "orderbook", "spot_prices", "heartbeats"):
        df = query(f"SELECT COUNT(*) AS n FROM {table}")
        assert int(df.iloc[0]["n"]) == 1, f"view '{table}' returned no rows"


def test_query_views_omit_missing_datasets(tmp_path, monkeypatch):
    """When a dataset's parquet files don't exist, the corresponding
    view is silently omitted — querying it raises a clear DuckDB
    Catalog Error rather than crashing earlier.
    """
    data_dir = tmp_path / "empty"
    data_dir.mkdir()
    # Only markets.parquet is present.
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({
            "market_id": ["m1"], "question": ["q"],
            "crypto": ["BTC"], "timeframe": ["1-hour"],
            "volume": [100.0], "resolution": [-1],
            "start_ts": [0], "end_ts": [0], "closed_ts": [0],
            "condition_id": [""], "up_token_id": [""], "down_token_id": [""],
            "slug": [""], "fee_rate_bps": [-1],
        })),
        str(data_dir / "markets.parquet"),
    )

    import polymarket_pipeline.query as q
    monkeypatch.setattr(q, "PARQUET_MARKETS_PATH", str(data_dir / "markets.parquet"))
    monkeypatch.setattr(q, "PARQUET_PRICES_DIR", str(data_dir / "prices"))
    monkeypatch.setattr(q, "PARQUET_TICKS_DIR", str(data_dir / "ticks"))

    # markets works …
    assert int(q.query("SELECT COUNT(*) FROM markets").iloc[0, 0]) == 1
    # … but spot_prices / orderbook / heartbeats don't exist as views.
    import duckdb
    for missing in ("orderbook", "spot_prices", "heartbeats"):
        with pytest.raises(duckdb.CatalogException):
            q.query(f"SELECT * FROM {missing}")
