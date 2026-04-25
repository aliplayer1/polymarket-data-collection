#!/usr/bin/env python3
"""Recollect historical ticks via the subgraph for windows previously
covered by the Etherscan/RPC path.

The subgraph is now the sole tick source; legacy Etherscan/RPC rows
should be replaced by re-fetching their windows.  ``consolidate_ticks``
already handles the merge: when a subgraph row exists for the same
``(market_id, timestamp_ms, token_id, tx_hash)`` as a legacy row, the
legacy row is dropped at consolidation time.  This script:

1. Reads ``markets.parquet`` to enumerate previously-collected markets.
2. For each market, calls ``SubgraphTickFetcher`` for its full
   ``[start_ts, end_ts]`` window.
3. Writes the fills as ``backfill_*.parquet`` shards under
   ``data/ticks/`` (the same path the historical phase uses).
4. Runs ``consolidate_ticks`` once at the end so the legacy rows are
   replaced atomically.

WS shard files (``ws_ticks_*.parquet``) and rows (``tx_hash=""``) are
left strictly alone — the hybrid filter in ``consolidate_ticks`` only
fires when ``tx_hash <> ''``.

Usage::

    python scripts/recollect_legacy_ticks.py \\
        --data-dir data \\
        [--from-date 2026-01-01] [--to-date 2026-04-01] \\
        [--crypto BTC] [--timeframe 5-minute] \\
        [--max-window-hours 6] [--max-workers 4] \\
        [--dry-run]

``--dry-run`` lists the windows that WOULD be re-fetched and exits.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from polymarket_pipeline.config import (  # noqa: E402
    SUBGRAPH_URL_FALLBACK,
    SUBGRAPH_URL_PRIMARY,
)
from polymarket_pipeline.models import MarketRecord  # noqa: E402
from polymarket_pipeline.phases.binance_history import SpotPriceLookup  # noqa: E402
from polymarket_pipeline.phases.subgraph_ticks import SubgraphTickFetcher  # noqa: E402
from polymarket_pipeline.storage import (  # noqa: E402
    append_ticks_only,
    consolidate_ticks,
)
from polymarket_pipeline.subgraph_client import SubgraphClient  # noqa: E402

log = logging.getLogger("recollect_legacy_ticks")


def _parse_date(s: str) -> int:
    return int(datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def load_markets(markets_parquet: Path) -> list[MarketRecord]:
    df = pd.read_parquet(markets_parquet)
    out: list[MarketRecord] = []
    for _, row in df.iterrows():
        out.append(MarketRecord(
            market_id=str(row["market_id"]),
            market_type="crypto-up-down",
            question=str(row.get("question", "")),
            timeframe=str(row["timeframe"]),
            crypto=str(row["crypto"]),
            condition_id=str(row.get("condition_id") or ""),
            start_ts=int(row["start_ts"]),
            end_ts=int(row["end_ts"]),
            up_token_id=str(row.get("up_token_id") or ""),
            down_token_id=str(row.get("down_token_id") or ""),
            up_outcome="Up",
            down_outcome="Down",
            volume=float(row.get("volume", 0.0) or 0.0),
            resolution=int(row["resolution"]) if row.get("resolution") is not None else None,
            is_active=False,
            closed_ts=int(row.get("closed_ts", 0) or 0),
        ))
    return out


def split_into_windows(
    market: MarketRecord,
    max_window_seconds: int,
) -> list[tuple[int, int]]:
    """Split a market's [start_ts, end_ts] into chunks of at most
    ``max_window_seconds`` seconds (matches the historical phase's
    auto-chunking behaviour)."""
    if market.end_ts <= market.start_ts:
        return []
    windows: list[tuple[int, int]] = []
    cur = market.start_ts
    while cur < market.end_ts:
        end = min(cur + max_window_seconds, market.end_ts)
        windows.append((cur, end))
        cur = end
    return windows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", required=True, type=Path)
    ap.add_argument("--from-date", type=str, default=None, metavar="YYYY-MM-DD")
    ap.add_argument("--to-date", type=str, default=None, metavar="YYYY-MM-DD")
    ap.add_argument("--crypto", type=str, default=None)
    ap.add_argument("--timeframe", type=str, default=None)
    ap.add_argument("--max-window-hours", type=float, default=6.0)
    ap.add_argument("--max-workers", type=int, default=4)
    ap.add_argument("--request-interval", type=float, default=0.2,
                    help="Subgraph request interval in seconds (politeness throttle).")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.INFO,
    )

    data_dir: Path = args.data_dir
    markets_parquet = data_dir / "markets.parquet"
    ticks_dir = data_dir / "ticks"
    if not markets_parquet.exists():
        log.error("No markets.parquet at %s", markets_parquet)
        return 2

    from_ts = _parse_date(args.from_date) if args.from_date else None
    to_ts = _parse_date(args.to_date) if args.to_date else None

    log.info("Loading markets from %s", markets_parquet)
    markets = load_markets(markets_parquet)
    log.info("Loaded %d markets", len(markets))

    def _keep(m: MarketRecord) -> bool:
        if args.crypto and m.crypto != args.crypto:
            return False
        if args.timeframe and m.timeframe != args.timeframe:
            return False
        if from_ts is not None and m.end_ts < from_ts:
            return False
        if to_ts is not None and m.start_ts >= to_ts:
            return False
        return True

    markets = [m for m in markets if _keep(m)]
    log.info("After filtering: %d markets to recollect", len(markets))

    max_window_s = int(args.max_window_hours * 3600)
    work: list[tuple[MarketRecord, int, int]] = []
    for m in markets:
        for ws, we in split_into_windows(m, max_window_s):
            work.append((m, ws, we))
    log.info("Total windows: %d (max_window=%dh)", len(work), int(args.max_window_hours))

    if args.dry_run:
        for m, ws, we in work[:20]:
            log.info("  would fetch market=%s [%d, %d]", m.market_id, ws, we)
        if len(work) > 20:
            log.info("  ... and %d more", len(work) - 20)
        return 0

    client = SubgraphClient(
        primary_url=SUBGRAPH_URL_PRIMARY,
        fallback_url=SUBGRAPH_URL_FALLBACK,
        api_key=os.environ.get("SUBGRAPH_API_KEY"),
        request_interval_s=args.request_interval,
        logger=log,
    )
    # No spot-price embedding here — the existing rows already have
    # spot prices from the original collection, and the subgraph
    # fetcher will leave the field NULL on the new rows; consolidation
    # ``arg_max`` keeps the populated value.
    fetcher = SubgraphTickFetcher(client, logger=log, spot_price_lookup=SpotPriceLookup())

    pending_lock = threading.Lock()
    pending: list[dict] = []
    flushed_total = 0

    def _flush_locked() -> int:
        nonlocal flushed_total
        if not pending:
            return 0
        df = pd.DataFrame(pending)
        # SubgraphTickFetcher rows lack crypto/timeframe; restore them
        # from each row's market_id.
        market_lookup = {m.market_id: m for m in markets}
        df["crypto"] = df["market_id"].map(lambda mid: market_lookup[mid].crypto)
        df["timeframe"] = df["market_id"].map(lambda mid: market_lookup[mid].timeframe)
        n = len(df)
        append_ticks_only(df, ticks_dir=str(ticks_dir), logger=log)
        flushed_total += n
        pending.clear()
        return n

    def fetch_one(window: tuple[MarketRecord, int, int]) -> int:
        m, ws, we = window
        try:
            result = fetcher.get_ticks_for_markets_batch([m], start_ts=ws, end_ts=we)
        except Exception as exc:
            log.error("Window [%s, %s] for %s failed: %s", ws, we, m.market_id, exc)
            return 0
        rows = result.get(m.market_id, [])
        if not rows:
            return 0
        with pending_lock:
            pending.extend(rows)
            if len(pending) >= 50_000:
                _flush_locked()
        return len(rows)

    started = time.monotonic()
    total_rows = 0
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(fetch_one, w): w for w in work}
        for i, fut in enumerate(as_completed(futures), 1):
            total_rows += fut.result()
            if i % 100 == 0 or i == len(futures):
                with pending_lock:
                    _flush_locked()
                log.info(
                    "Progress: %d/%d windows | %d rows fetched | %.1fs elapsed",
                    i, len(futures), total_rows, time.monotonic() - started,
                )

    with pending_lock:
        _flush_locked()

    log.info("Fetched %d total subgraph rows; consolidating...", total_rows)
    consolidate_ticks(ticks_dir=str(ticks_dir), logger=log)
    log.info(
        "Recollection complete in %.1fs.  Run --upload-only next to push the "
        "new dataset to Hugging Face.",
        time.monotonic() - started,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
