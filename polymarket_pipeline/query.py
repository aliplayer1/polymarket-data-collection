"""DuckDB convenience layer for querying local Parquet datasets.

Usage (CLI):
    python -m polymarket_pipeline.query "SELECT * FROM prices WHERE crypto='BTC' LIMIT 10"

Usage (Python):
    from polymarket_pipeline.query import query
    df = query("SELECT * FROM prices WHERE crypto='BTC' AND timeframe='1-hour' LIMIT 100")
"""

from __future__ import annotations

import argparse
import os

import duckdb
import pandas as pd

from .config import PARQUET_MARKETS_PATH, PARQUET_PRICES_DIR, PARQUET_TICKS_DIR


def _duckdb_escape(value: str) -> str:
    """Escape a string for safe interpolation into DuckDB SQL literals."""
    return value.replace("'", "''")


def _ensure_views(con: duckdb.DuckDBPyConnection) -> None:
    """Register every available Parquet dataset as a DuckDB view.

    Tables registered (subject to file presence on disk):

    * ``markets``      — top-level markets metadata
    * ``prices``       — Hive-partitioned binary prices
    * ``ticks``        — Hive-partitioned trade fills
    * ``orderbook``    — Hive-partitioned BBO snapshots
    * ``spot_prices``  — flat directory of RTDS / Binance spot prices
    * ``heartbeats``   — flat directory of WS heartbeats

    Datasets whose Parquet files are missing on disk are silently
    omitted; querying a missing one raises a DuckDB Catalog Error
    that names the table, which is clear enough for operators.
    """
    if os.path.exists(PARQUET_MARKETS_PATH):
        con.execute(
            f"CREATE OR REPLACE VIEW markets AS SELECT * FROM read_parquet('{_duckdb_escape(PARQUET_MARKETS_PATH)}')"
        )
    if os.path.exists(PARQUET_PRICES_DIR):
        con.execute(
            f"CREATE OR REPLACE VIEW prices AS SELECT * FROM read_parquet('{_duckdb_escape(PARQUET_PRICES_DIR)}/**/*.parquet', hive_partitioning=true)"
        )
    if os.path.exists(PARQUET_TICKS_DIR):
        con.execute(
            f"CREATE OR REPLACE VIEW ticks AS SELECT * FROM read_parquet('{_duckdb_escape(PARQUET_TICKS_DIR)}/**/*.parquet', hive_partitioning=true)"
        )

    # Derive sibling-dir paths from the markets path so the views
    # follow ``--data-dir`` overrides automatically.
    data_root = os.path.dirname(os.path.abspath(PARQUET_MARKETS_PATH))
    orderbook_dir = os.path.join(data_root, "orderbook")
    spot_prices_dir = os.path.join(data_root, "spot_prices")
    heartbeats_dir = os.path.join(data_root, "heartbeats")

    if os.path.exists(orderbook_dir) and any(
        f.endswith(".parquet") for _, _, files in os.walk(orderbook_dir) for f in files
    ):
        con.execute(
            f"CREATE OR REPLACE VIEW orderbook AS SELECT * FROM "
            f"read_parquet('{_duckdb_escape(orderbook_dir)}/**/*.parquet', hive_partitioning=true)"
        )
    if os.path.exists(spot_prices_dir) and any(
        f.endswith(".parquet") for f in os.listdir(spot_prices_dir)
    ):
        con.execute(
            f"CREATE OR REPLACE VIEW spot_prices AS SELECT * FROM "
            f"read_parquet('{_duckdb_escape(spot_prices_dir)}/*.parquet')"
        )
    if os.path.exists(heartbeats_dir) and any(
        f.endswith(".parquet") for f in os.listdir(heartbeats_dir)
    ):
        con.execute(
            f"CREATE OR REPLACE VIEW heartbeats AS SELECT * FROM "
            f"read_parquet('{_duckdb_escape(heartbeats_dir)}/*.parquet')"
        )


def connect() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with ``markets`` and ``prices`` views ready."""
    con = duckdb.connect()
    _ensure_views(con)
    return con


def query(sql: str) -> pd.DataFrame:
    """Run an arbitrary SQL query and return a pandas DataFrame."""
    con = connect()
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query local Polymarket Parquet data with SQL (via DuckDB)."
    )
    parser.add_argument(
        "sql",
        help=(
            "SQL query to execute.  Tables: 'markets', 'prices', 'ticks', "
            "'orderbook', 'spot_prices', 'heartbeats' (subject to availability)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Append LIMIT N to the query (convenience shortcut).",
    )
    args = parser.parse_args()

    sql = args.sql
    if args.limit is not None and "limit" not in sql.lower():
        sql = f"{sql.rstrip('; ')} LIMIT {args.limit}"

    df = query(sql)
    if df.empty:
        print("(no results)")
    else:
        print(df.to_markdown(index=False))


if __name__ == "__main__":
    main()
