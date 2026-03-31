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
    """Register the markets and prices Parquet sources as DuckDB views."""
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
    parser.add_argument("sql", help="SQL query to execute. Tables: 'markets', 'prices', 'ticks'.")
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
