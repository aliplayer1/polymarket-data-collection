#!/usr/bin/env python3
"""Profile a targeted-collection dataset — schema, stats, density.

Memory-capped DuckDB with per-query fresh connections so a single heavy
query can't cascade across the rest of the profile.  Uses approximate
quantiles to avoid OOM on tick sets > 100M rows.

Usage::

    python scripts/profile_dataset.py --data-dir collections/march_2026_btc_5min
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb


def new_conn(tmp: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{tmp}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def section(t: str) -> None:
    print(f"\n{'=' * 72}\n{t}\n{'=' * 72}", flush=True)


def run(tmp: Path, label: str, sql: str) -> None:
    t0 = time.monotonic()
    con = new_conn(tmp)
    try:
        rows = con.execute(sql).fetchall()
    except Exception as exc:
        print(f"  {label}: FAILED — {exc}", flush=True)
        con.close()
        return
    finally:
        con.close()
    dt = time.monotonic() - t0
    if rows and len(rows[0]) == 1 and len(rows) == 1:
        v = rows[0][0]
        formatted = f"{v:,}" if isinstance(v, int) else str(v)
        print(f"  {label}: {formatted}  ({dt:.1f}s)", flush=True)
    else:
        print(f"  {label}  ({dt:.1f}s)", flush=True)
        for r in rows:
            print(f"    {r}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, type=Path,
                        help="Collection dir containing ticks/, spot_prices/, markets.parquet")
    args = parser.parse_args()

    data = args.data_dir.resolve()
    ticks_glob = str(data / "ticks/**/*.parquet")
    spot_glob = str(data / "spot_prices/**/*.parquet")
    markets = str(data / "markets.parquet")
    tmp = Path(f"/tmp/duckdb_profile_{data.name}")
    tmp.mkdir(exist_ok=True)

    print(f"Profiling {data}", flush=True)

    section("ROW COUNTS")
    run(tmp, "raw tick rows",
        f"SELECT COUNT(*) FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)")
    run(tmp, "approx distinct (market_id, timestamp_ms, order_hash)",
        f"""SELECT approx_count_distinct(market_id || '|' || CAST(timestamp_ms AS VARCHAR) || '|' || COALESCE(order_hash,''))
            FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)""")
    run(tmp, "markets in markets.parquet",
        f"SELECT COUNT(*) FROM read_parquet('{markets}')")
    run(tmp, "markets with >=1 tick",
        f"SELECT COUNT(DISTINCT market_id) FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)")

    section("TIME RANGE")
    run(tmp, "start/end/span",
        f"""SELECT
                to_timestamp(MIN(timestamp_ms)/1000),
                to_timestamp(MAX(timestamp_ms)/1000),
                ROUND((MAX(timestamp_ms) - MIN(timestamp_ms)) / 86400000.0, 2) AS days
            FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)""")

    section("PRICE / SIZE")
    run(tmp, "price summary",
        f"""SELECT ROUND(MIN(price)::DOUBLE,4), ROUND(MAX(price)::DOUBLE,4),
                   ROUND(AVG(price)::DOUBLE,4), ROUND(stddev_pop(price)::DOUBLE,4)
            FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)""")
    run(tmp, "size summary (USDC)",
        f"""SELECT ROUND(MIN(size_usdc)::DOUBLE,6), ROUND(MAX(size_usdc)::DOUBLE,2),
                   ROUND(AVG(size_usdc)::DOUBLE,2), ROUND(stddev_pop(size_usdc)::DOUBLE,2),
                   ROUND(SUM(size_usdc)::DOUBLE,0)
            FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)""")
    run(tmp, "size approx-quantiles (5/25/50/75/95/99)",
        f"""SELECT ROUND(approx_quantile(size_usdc,0.05)::DOUBLE,4),
                   ROUND(approx_quantile(size_usdc,0.25)::DOUBLE,4),
                   ROUND(approx_quantile(size_usdc,0.50)::DOUBLE,4),
                   ROUND(approx_quantile(size_usdc,0.75)::DOUBLE,4),
                   ROUND(approx_quantile(size_usdc,0.95)::DOUBLE,4),
                   ROUND(approx_quantile(size_usdc,0.99)::DOUBLE,4)
            FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)""")
    run(tmp, "price approx-quantiles",
        f"""SELECT ROUND(approx_quantile(price,0.05)::DOUBLE,4),
                   ROUND(approx_quantile(price,0.25)::DOUBLE,4),
                   ROUND(approx_quantile(price,0.50)::DOUBLE,4),
                   ROUND(approx_quantile(price,0.75)::DOUBLE,4),
                   ROUND(approx_quantile(price,0.95)::DOUBLE,4)
            FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)""")

    section("SIDE / OUTCOME")
    run(tmp, "side",
        f"SELECT side, COUNT(*) FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true) GROUP BY side ORDER BY 2 DESC")
    run(tmp, "outcome",
        f"SELECT outcome, COUNT(*) FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true) GROUP BY outcome ORDER BY 2 DESC")

    section("DAILY ACTIVITY")
    run(tmp, "ticks per day",
        f"""SELECT CAST(to_timestamp(timestamp_ms/1000) AS DATE) AS day,
                   COUNT(*) AS n_ticks, COUNT(DISTINCT market_id) AS markets,
                   ROUND(SUM(size_usdc)::DOUBLE,0) AS notional
            FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)
            GROUP BY 1 ORDER BY 1""")

    section("PER-MARKET TICK DISTRIBUTION")
    run(tmp, "tick-count quantiles per market",
        f"""WITH pm AS (SELECT market_id, COUNT(*) AS n
                        FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)
                        GROUP BY market_id)
            SELECT MIN(n),
                   ROUND(approx_quantile(n,0.05)::DOUBLE,0),
                   ROUND(approx_quantile(n,0.25)::DOUBLE,0),
                   ROUND(approx_quantile(n,0.50)::DOUBLE,0),
                   ROUND(approx_quantile(n,0.75)::DOUBLE,0),
                   ROUND(approx_quantile(n,0.95)::DOUBLE,0),
                   ROUND(approx_quantile(n,0.99)::DOUBLE,0),
                   MAX(n), ROUND(AVG(n)::DOUBLE,0) FROM pm""")

    section("FREQUENCY / DENSITY")
    run(tmp, "overall + per-market rates",
        f"""WITH s AS (SELECT (MAX(timestamp_ms)-MIN(timestamp_ms))/1000.0 AS secs,
                              COUNT(*)::BIGINT AS n, COUNT(DISTINCT market_id)::BIGINT AS m
                       FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true))
            SELECT n, ROUND(secs/86400.0,2) AS days, ROUND(n/secs,3) AS ticks_per_sec, m,
                   ROUND(n::DOUBLE/m,0) AS ticks_per_mkt FROM s""")

    section("COVERAGE")
    run(tmp, "spot_price_usdt populated %",
        f"SELECT ROUND(100.0*COUNT(spot_price_usdt)/COUNT(*),2) FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)")
    run(tmp, "order_hash populated %",
        f"SELECT ROUND(100.0*COUNT(order_hash)/COUNT(*),2) FROM read_parquet('{ticks_glob}', union_by_name=true, hive_partitioning=true)")

    section("SPOT PRICE STREAM")
    run(tmp, "spot prices by symbol/source",
        f"""SELECT symbol, source, COUNT(*) AS n,
                to_timestamp(MIN(ts_ms)/1000), to_timestamp(MAX(ts_ms)/1000)
            FROM read_parquet('{spot_glob}', union_by_name=true)
            GROUP BY 1,2 ORDER BY n DESC""")

    print("\nProfile complete.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
