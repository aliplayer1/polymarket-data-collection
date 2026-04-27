#!/usr/bin/env python3
"""Mop-up pass: retry windows that failed all retries in the main run.

Parses ``collect.log`` for ``Window [X, Y] failed`` lines, reconstructs
which markets fall into each window, and fetches ticks for just those
windows using a single worker (no contention, more tolerant of slow
queries).  Appends to the same ticks/ dir — dedup at consolidation.

Usage::

    python scripts/retry_failed_windows.py \\
        --data-dir collections/march_2026_btc_5min \\
        --from-date 2026-03-01 --to-date 2026-04-01 \\
        --crypto BTC --timeframe 5-minute \\
        --max-window-hours 6
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd  # noqa: E402

from polymarket_pipeline.models import MarketRecord  # noqa: E402
from polymarket_pipeline.phases.binance_history import SpotPriceLookup  # noqa: E402
from polymarket_pipeline.phases.subgraph_ticks import SubgraphTickFetcher  # noqa: E402
from polymarket_pipeline.storage import append_ticks_only  # noqa: E402
from polymarket_pipeline.subgraph_client import SubgraphClient  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("retry_failed")

_FAILED_RE = re.compile(r"Window \[(\d+), (\d+)\] failed")


def parse_failed_windows(log_path: Path) -> list[tuple[int, int]]:
    seen = set()
    with log_path.open() as fh:
        for line in fh:
            m = _FAILED_RE.search(line)
            if m:
                seen.add((int(m.group(1)), int(m.group(2))))
    return sorted(seen)


def load_markets(path: Path, crypto: str, timeframe: str) -> list[MarketRecord]:
    import pyarrow.parquet as pq
    df = pq.ParquetFile(str(path)).read().to_pandas()
    df = df[(df["crypto"] == crypto) & (df["timeframe"] == timeframe)]
    out: list[MarketRecord] = []
    for _, row in df.iterrows():
        out.append(MarketRecord(
            market_id=str(row["market_id"]),
            market_type="crypto-up-down",
            question=str(row.get("question", "")),
            timeframe=str(row["timeframe"]),
            crypto=str(row["crypto"]),
            condition_id=str(row.get("condition_id") or "") or None,
            start_ts=int(row["start_ts"]),
            end_ts=int(row["end_ts"]),
            up_token_id=str(row.get("up_token_id") or ""),
            down_token_id=str(row.get("down_token_id") or ""),
            volume=float(row.get("volume") or 0.0),
            resolution=int(row.get("resolution") if row.get("resolution") is not None else -1),
            is_active=False,
            closed_ts=int(row.get("closed_ts") or 0) or None,
        ))
    return out


def load_spot_lookup(spot_dir: Path) -> SpotPriceLookup:
    """Build a per-crypto Binance-USDT spot lookup from spot_prices/.

    Filter to ``source == "binance"`` rows.  The schema contract
    (TICKS_SCHEMA.spot_price_usdt) is "BTC/USDT close from Binance" —
    Chainlink USD prices and chainlink_proxy entries are different
    series with different cadences and would silently violate the
    contract if mixed into the same per-crypto bucket.  Without this
    filter the ``<= t`` lookup returns whichever source happens to
    have the largest timestamp, producing inconsistent
    ``spot_price_usdt`` columns when this script re-collects ticks.
    """
    import pyarrow.parquet as pq
    lookup = SpotPriceLookup()
    for f in spot_dir.glob("*.parquet"):
        df = pq.ParquetFile(str(f)).read().to_pandas()
        if "source" in df.columns:
            df = df[df["source"] == "binance"]
        for _, row in df.iterrows():
            sym = str(row["symbol"]).lower()
            if not sym.endswith("usdt"):
                continue  # only Binance USDT pairs
            crypto = sym[:-4].upper()
            lookup.add(crypto, int(row["ts_ms"]), float(row["price"]))
    lookup.finalize()
    log.info("Loaded %d Binance spot price points into lookup", len(lookup))
    return lookup


def rebuild_window_map(
    markets: list[MarketRecord], max_window_seconds: int
) -> dict[tuple[int, int], list[MarketRecord]]:
    windows: dict[tuple[int, int], list[MarketRecord]] = defaultdict(list)
    for m in markets:
        s = m.start_ts
        while s < m.end_ts:
            e = min(s + max_window_seconds, m.end_ts)
            windows[(s, e)].append(m)
            s = e
    return dict(windows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--crypto", default="BTC")
    parser.add_argument("--timeframe", default="5-minute")
    parser.add_argument("--max-window-hours", type=float, default=6.0)
    parser.add_argument("--request-interval", type=float, default=0.3,
                        help="Gentler throttle for retries (default 0.3s = 3.3 req/s)")
    args = parser.parse_args()

    # 1. Parse failed windows from the original run's log
    log_path = args.data_dir / "collect.log"
    failed = parse_failed_windows(log_path)
    if not failed:
        log.info("No failed windows found — nothing to retry")
        return 0
    log.info("Found %d failed windows to retry", len(failed))

    # 2. Reconstruct market→window mapping from markets.parquet
    markets = load_markets(
        args.data_dir / "markets.parquet", args.crypto, args.timeframe,
    )
    log.info("Loaded %d markets", len(markets))
    window_map = rebuild_window_map(markets, int(args.max_window_hours * 3600))
    log.info("Rebuilt %d total windows", len(window_map))

    # 3. Load spot prices for enrichment
    spot_lookup = load_spot_lookup(args.data_dir / "spot_prices")

    # 4. Retry each failed window, one at a time, with a gentler throttle
    client = SubgraphClient(
        logger=log,
        request_interval_s=args.request_interval,
        max_retries=5,  # extra patience on retries
    )
    fetcher = SubgraphTickFetcher(client, logger=log, spot_price_lookup=spot_lookup)

    ticks_dir = args.data_dir / "ticks"
    pending: list[dict] = []
    total_ticks = 0
    still_failing: list[tuple[int, int]] = []
    t0 = time.monotonic()

    for idx, (ws, we) in enumerate(failed, start=1):
        ms = window_map.get((ws, we))
        if not ms:
            log.warning("  [%d/%d] window [%s, %s] — no markets mapped, skipping",
                        idx, len(failed), ws, we)
            continue
        try:
            batch = fetcher.get_ticks_for_markets_batch(ms, ws, we)
            rows: list[dict] = []
            for r in batch.values():
                rows.extend(r)
            pending.extend(rows)
            total_ticks += len(rows)
            log.info("  [%d/%d] window [%s, %s] (%d markets) → %d ticks",
                     idx, len(failed), ws, we, len(ms), len(rows))
        except Exception as exc:
            still_failing.append((ws, we))
            log.error("  [%d/%d] window [%s, %s] FAILED again: %s",
                      idx, len(failed), ws, we, exc)

        # Flush every few windows
        if pending and (idx % 5 == 0 or idx == len(failed)):
            df = pd.DataFrame(pending)
            if "category" in df.columns:
                df = df[df["category"] != "culture"].drop(columns=["category"], errors="ignore")
            if not df.empty:
                append_ticks_only(df, ticks_dir=str(ticks_dir), logger=log)
            pending = []

    elapsed = time.monotonic() - t0
    log.info("=== RETRY PASS COMPLETE ===")
    log.info("  attempted:      %d windows", len(failed))
    log.info("  recovered:      %d windows", len(failed) - len(still_failing))
    log.info("  still failing:  %d windows", len(still_failing))
    log.info("  new ticks:      %d", total_ticks)
    log.info("  elapsed:        %.1f s", elapsed)
    if still_failing:
        log.warning("Still-failing windows (data loss):")
        for ws, we in still_failing:
            log.warning("  [%s, %s]  (%s → %s UTC)", ws, we,
                        datetime.fromtimestamp(ws, tz=timezone.utc).isoformat(),
                        datetime.fromtimestamp(we, tz=timezone.utc).isoformat())
    return 0


if __name__ == "__main__":
    sys.exit(main())
