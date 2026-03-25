#!/usr/bin/env python3
"""Standalone historical on-chain tick backfill.

Streams closed markets from the Polymarket API (newest → oldest), fetches their
on-chain trade fills from Polygon via the CTF Exchange OrderFilled event log,
and writes them to the ticks/ Parquet table.

Stops automatically when it encounters N consecutive already-covered markets
(default: 50), so re-running after an interruption picks up cleanly without
re-processing data you already have.  Use --stop-after 0 for the first full
backfill to disable this heuristic entirely.

This script is safe to run in parallel with the live polymarket-live.service —
it only writes to the ticks/ directory and does not touch prices/ or markets.parquet.

Usage
-----
    # Preferred: Polygonscan API key (free at polygonscan.com/myapikey, 3 req/s limit)
    python scripts/tick_backfill.py --polygonscan-key <KEY>

    # Alternative: direct Polygon JSON-RPC endpoint
    python scripts/tick_backfill.py --rpc-url https://polygon-mainnet.g.alchemy.com/v2/<KEY>

    # Limit how far back to scan (defaults to 2025-01-01 as a safety guard):
    python scripts/tick_backfill.py --polygonscan-key <KEY> --from-date 2025-01-01

    # All credentials can also come from .env / environment variables:
    #   POLYGONSCAN_API_KEY, POLYGON_RPC_URL, POLYMARKET_DATA_DIR

How it works
------------
1. Loads the set of market_ids already present in data/ticks/ (source=onchain).
2. Streams closed markets from the API, newest-first.
3. Skips markets already covered; once 5 consecutive relevant markets are all
   already covered, the script assumes it has reached the "covered boundary"
   and exits cleanly.
4. Batches uncovered markets into groups of --batch-size, fetches ticks for
   each window group, and persists them.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Allow running as a standalone script from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

from polymarket_pipeline.api import PolymarketApi  # noqa: E402
from polymarket_pipeline.config import TIMEFRAME_SECONDS  # noqa: E402
from polymarket_pipeline.models import MarketRecord  # noqa: E402
from polymarket_pipeline.storage import load_ticks, persist_ticks  # noqa: E402
from polymarket_pipeline.ticks import PolygonTickFetcher  # noqa: E402

import pandas as pd  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_FROM_DATE = "2025-01-01"   # Safety guard: don't scan before this date
_DEFAULT_BATCH_SIZE = 20             # Markets per fetch batch (keep small for Polygonscan 3 req/s limit)
_STOP_AFTER_N_COVERED = 50          # Stop after this many consecutive already-covered markets


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("tick_backfill")


def _covered_market_ids(ticks_dir: str) -> set[str]:
    """Return market_ids that already have on-chain ticks stored.

    Filters to source='onchain' so live WebSocket ticks don't falsely block
    the on-chain backfill for that market.  We filter in pandas rather than
    via ParquetDataset filters because 'source' is a data column, not a
    partition column, and legacy PyArrow may silently ignore such filters.
    """
    if not os.path.exists(ticks_dir):
        return set()
    try:
        df = load_ticks(ticks_dir)
        if df.empty:
            return set()
        # Only consider on-chain ticks as "covered"
        if "source" in df.columns:
            df = df[df["source"].astype(str) == "onchain"]
        if df.empty:
            return set()
        return set(df["market_id"].astype(str).unique())
    except Exception as exc:
        logging.getLogger("tick_backfill").warning("Could not read existing ticks: %s", exc)
        return set()


def _fetch_and_persist_batch(
    markets: list[MarketRecord],
    tick_fetcher: PolygonTickFetcher,
    ticks_dir: str,
    logger: logging.Logger,
) -> int:
    """Fetch on-chain fills for a batch of markets and persist to ticks/."""
    # Group by (win_start, win_end) — BTC/ETH/SOL at the same time slot share a
    # block range, so one eth_getLogs call covers all three.
    windows: dict[tuple[int, int], list[MarketRecord]] = defaultdict(list)
    for m in markets:
        window_s = TIMEFRAME_SECONDS.get(m.timeframe, 300)
        win_start = max(m.start_ts, m.end_ts - window_s)
        windows[(win_start, m.end_ts)].append(m)

    total_ticks = 0
    n_windows = len(windows)
    for i, ((win_start, win_end), window_markets) in enumerate(windows.items(), 1):
        logger.info(
            "[%d/%d] Window %s–%s  (%s)",
            i, n_windows, win_start, win_end,
            ", ".join(f"{m.crypto}/{m.timeframe}" for m in window_markets),
        )
        try:
            batch_result = tick_fetcher.get_ticks_for_markets_batch(
                window_markets, win_start, win_end,
            )
            for m in window_markets:
                ticks = batch_result.get(m.market_id, [])
                if not ticks:
                    continue
                ticks_df = pd.DataFrame(ticks)
                ticks_df["source"] = "onchain"
                persist_ticks(ticks_df, ticks_dir=ticks_dir, logger=logger)
                total_ticks += len(ticks)
                logger.info("  -> %s %s market %s: %d fills", m.crypto, m.timeframe, m.market_id, len(ticks))
        except Exception as exc:
            logger.error("Failed tick fetch for window %s–%s: %s", win_start, win_end, exc)

    return total_ticks


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Historical on-chain tick backfill for Polymarket crypto markets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--rpc-url", default=None, metavar="URL",
        help="Polygon JSON-RPC endpoint (e.g. Alchemy). Also reads POLYGON_RPC_URL env var.",
    )
    parser.add_argument(
        "--polygonscan-key", default=None, metavar="KEY",
        help="Free Polygonscan API key (polygonscan.com/myapikey). Also reads POLYGONSCAN_API_KEY env var.",
    )
    parser.add_argument(
        "--data-dir", default=None, metavar="PATH",
        help="Root data directory (contains ticks/). Defaults to 'data/'. Also reads POLYMARKET_DATA_DIR env var.",
    )
    parser.add_argument(
        "--from-date", default=_DEFAULT_FROM_DATE, metavar="YYYY-MM-DD",
        help=(
            f"Only backfill markets that closed on or after this date. "
            f"Default: {_DEFAULT_FROM_DATE}"
        ),
    )
    parser.add_argument(
        "--batch-size", type=int, default=_DEFAULT_BATCH_SIZE, metavar="N",
        help=f"Markets per RPC batch. Default: {_DEFAULT_BATCH_SIZE}.",
    )
    parser.add_argument(
        "--stop-after", type=int, default=_STOP_AFTER_N_COVERED, metavar="N",
        help=(
            f"Stop after N consecutive already-covered markets (resume detection). "
            f"Set to 0 to disable and scan the full date range. "
            f"Default: {_STOP_AFTER_N_COVERED}."
        ),
    )
    args = parser.parse_args()

    rpc_url         = args.rpc_url         or os.environ.get("POLYGON_RPC_URL")
    polygonscan_key = args.polygonscan_key  or os.environ.get("POLYGONSCAN_API_KEY")
    data_dir        = args.data_dir         or os.environ.get("POLYMARKET_DATA_DIR") or "data"
    ticks_dir       = os.path.join(data_dir, "ticks")

    logger = _configure_logging()

    # ── Validate credentials ─────────────────────────────────────────────────
    if not rpc_url and not polygonscan_key:
        logger.error(
            "No Polygon data source configured.\n"
            "Pass --polygonscan-key <KEY> or --rpc-url <URL>, "
            "or set POLYGONSCAN_API_KEY / POLYGON_RPC_URL in your .env file."
        )
        sys.exit(1)

    # ── Parse cutoff date ────────────────────────────────────────────────────
    scan_cutoff_ts = int(datetime.strptime(args.from_date, "%Y-%m-%d").timestamp())
    logger.info("Tick backfill starting — scanning closed markets from %s onwards", args.from_date)

    # ── Setup ────────────────────────────────────────────────────────────────
    tick_fetcher = PolygonTickFetcher(rpc_url=rpc_url, polygonscan_key=polygonscan_key, logger=logger)
    api = PolymarketApi(logger=logger)

    # Load market_ids that already have on-chain ticks stored
    logger.info("Loading existing tick coverage from %s …", ticks_dir)
    covered = _covered_market_ids(ticks_dir)
    logger.info("Already covered: %d unique markets", len(covered))

    # ── Main scan loop ───────────────────────────────────────────────────────
    total_ticks_written = 0
    total_markets_processed = 0
    total_markets_scanned = 0
    total_skipped_covered = 0
    pending_batch: list[MarketRecord] = []
    consecutive_covered = 0
    stop_after = args.stop_after  # 0 = disabled

    for m in api.fetch_markets(closed=True, end_ts_min=scan_cutoff_ts):
        total_markets_scanned += 1

        # The API returns newest→oldest. Stop once we pass the from-date cutoff.
        if m.end_ts < scan_cutoff_ts:
            logger.info("Reached --from-date cutoff (%s); stopping.", args.from_date)
            break

        if m.market_id in covered:
            consecutive_covered += 1
            total_skipped_covered += 1
            if stop_after > 0 and consecutive_covered >= stop_after:
                logger.info(
                    "Hit %d consecutive already-covered markets — "
                    "backfill is caught up to the covered boundary. Stopping.",
                    consecutive_covered,
                )
                break
            continue

        # This market needs ticks fetched.
        consecutive_covered = 0
        pending_batch.append(m)
        total_markets_processed += 1

        if total_markets_scanned % 200 == 0:
            logger.info(
                "Progress: scanned %d markets, %d need ticks, %d already covered",
                total_markets_scanned, total_markets_processed, total_skipped_covered,
            )

        if len(pending_batch) >= args.batch_size:
            logger.info("─── Flushing batch of %d markets ───", len(pending_batch))
            total_ticks_written += _fetch_and_persist_batch(
                pending_batch, tick_fetcher, ticks_dir, logger,
            )
            # Refresh coverage so idempotency check stays accurate
            covered = _covered_market_ids(ticks_dir)
            pending_batch = []

    # Flush any remaining markets
    if pending_batch:
        logger.info("─── Flushing final batch of %d markets ───", len(pending_batch))
        total_ticks_written += _fetch_and_persist_batch(
            pending_batch, tick_fetcher, ticks_dir, logger,
        )

    logger.info(
        "Tick backfill complete — %d fills written across %d markets.",
        total_ticks_written,
        total_markets_processed,
    )


if __name__ == "__main__":
    main()
