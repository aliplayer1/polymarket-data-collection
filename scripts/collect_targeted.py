#!/usr/bin/env python3
"""Targeted date-range collector — ticks + spot prices only.

Goes straight from Gamma → SubgraphTickFetcher + BinanceHistoryPhase,
skipping the slow CLOB ``/prices-history`` path.  Output layout matches
``polymarket_pipeline``'s TICKS_SCHEMA + spot_prices layout so the
collected Parquet can be joined with anything produced by the main
pipeline.

Usage::

    python scripts/collect_targeted.py \\
        --from-date 2026-03-01 \\
        --to-date 2026-04-01 \\
        --crypto BTC \\
        --timeframe 5-minute \\
        --data-dir collections/march_2026_btc_5min
"""
from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd  # noqa: E402

from polymarket_pipeline.api import PolymarketApi  # noqa: E402
from polymarket_pipeline.models import MarketRecord  # noqa: E402
from polymarket_pipeline.phases.binance_history import BinanceHistoryPhase  # noqa: E402
from polymarket_pipeline.phases.subgraph_ticks import SubgraphTickFetcher  # noqa: E402
from polymarket_pipeline.storage import (  # noqa: E402
    append_ticks_only,
    persist_normalized,
)
from polymarket_pipeline.subgraph_client import SubgraphClient  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("collect_targeted")


def _parse_date(s: str) -> int:
    return int(datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def fetch_markets(start_ts: int, end_ts: int, crypto: str, timeframe: str) -> list[MarketRecord]:
    api = PolymarketApi()
    log.info(
        "Querying Gamma for %s %s markets in [%s, %s]",
        crypto, timeframe,
        datetime.fromtimestamp(start_ts, tz=timezone.utc).date(),
        datetime.fromtimestamp(end_ts, tz=timezone.utc).date(),
    )
    out: list[MarketRecord] = []
    scanned = 0
    for m in api.fetch_markets(closed=True, end_ts_min=start_ts, end_ts_max=end_ts):
        scanned += 1
        if scanned % 500 == 0:
            log.info("  scanned %d markets, matched %d so far", scanned, len(out))
        if m.crypto != crypto or m.timeframe != timeframe:
            continue
        if m.closed_ts is None or m.closed_ts < start_ts or m.closed_ts >= end_ts:
            continue
        out.append(m)
    log.info("Market scan complete: scanned %d, matched %d %s %s markets", scanned, len(out), crypto, timeframe)
    return out


def load_markets_from_parquet(path: Path, crypto: str, timeframe: str) -> list[MarketRecord]:
    """Reconstruct MarketRecord objects from a previously-written markets.parquet.

    Used by --resume to skip the expensive Gamma scan on re-runs.
    """
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
            fee_rate_bps=int(row.get("fee_rate_bps") if row.get("fee_rate_bps") is not None else -1),
            slug=str(row.get("slug") or "") or None,
        ))
    log.info("Loaded %d %s %s markets from %s (resume)", len(out), crypto, timeframe, path)
    return out


def fetch_spot_prices(
    markets: list[MarketRecord],
    data_dir: Path,
) -> object:  # returns SpotPriceLookup
    phase = BinanceHistoryPhase(logger=log)
    spot_dir = data_dir / "spot_prices"
    spot_dir.mkdir(parents=True, exist_ok=True)
    lookup = phase.run(markets, spot_prices_dir=str(spot_dir))
    log.info("Binance klines fetched; spot lookup contains %d points", len(lookup))
    return lookup


def fetch_ticks(
    markets: list[MarketRecord],
    spot_lookup: object,
    data_dir: Path,
    *,
    max_window_seconds: int = 6 * 3600,
    flush_every: int = 50,
    request_interval_s: float = 0.05,
    progress_every: int = 500,
    workers: int = 4,
) -> int:
    """Fetch trade fills per time-window with bounded thread parallelism.

    Each worker independently calls ``SubgraphTickFetcher``.  The
    underlying ``SubgraphClient`` enforces a single global throttle lock
    so we never exceed the configured request rate regardless of
    worker count — the threads just keep multiple I/O-bound requests
    in flight to saturate bandwidth.  With ~300ms per-query latency
    and 0.05s throttle, 4 workers saturate the 20 req/s ceiling.
    """
    if not markets:
        return 0
    client = SubgraphClient(logger=log, request_interval_s=request_interval_s)
    fetcher = SubgraphTickFetcher(client, logger=log, spot_price_lookup=spot_lookup)

    windows: dict[tuple[int, int], list[MarketRecord]] = defaultdict(list)
    for m in markets:
        s = m.start_ts
        while s < m.end_ts:
            e = min(s + max_window_seconds, m.end_ts)
            windows[(s, e)].append(m)
            s = e
    sorted_windows = sorted(windows.items(), key=lambda x: x[0])
    total_windows = len(sorted_windows)
    log.info(
        "Chunked %d markets into %d time windows (≤%ds each); %d workers",
        len(markets), total_windows, max_window_seconds, workers,
    )

    ticks_dir = data_dir / "ticks"
    ticks_dir.mkdir(parents=True, exist_ok=True)
    pending: list[dict] = []
    pending_lock = threading.Lock()
    progress_lock = threading.Lock()
    completed = {"idx": 0, "ticks": 0}
    t0 = time.monotonic()

    def process_one(args):
        (ws, we), ms = args
        try:
            batch = fetcher.get_ticks_for_markets_batch(ms, ws, we)
        except Exception as exc:
            log.error("Window [%s, %s] failed: %s", ws, we, exc)
            return []
        rows: list[dict] = []
        for _mid, r in batch.items():
            rows.extend(r)
        return rows

    def maybe_flush(final: bool = False) -> None:
        """Flush pending rows to disk.  Caller may hold ``pending_lock``."""
        with pending_lock:
            if not pending:
                return
            if not final and len(pending) < flush_every * 100:
                return
            to_flush = pending[:]
            pending.clear()
        df = pd.DataFrame(to_flush)
        if "category" in df.columns:
            df = df[df["category"] != "culture"].drop(columns=["category"], errors="ignore")
        if not df.empty:
            append_ticks_only(df, ticks_dir=str(ticks_dir), logger=log)

    # Batched submission avoids a memory leak where completed futures
    # hold their large result lists indefinitely (Python 3.10's
    # ThreadPoolExecutor keeps a reference until the executor is closed).
    # We create a fresh executor per BATCH_SIZE windows so all futures
    # and their row payloads are garbage-collected at batch boundaries.
    BATCH_SIZE = 100
    for batch_start in range(0, total_windows, BATCH_SIZE):
        batch = sorted_windows[batch_start:batch_start + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sg") as ex:
            batch_futures = [ex.submit(process_one, w) for w in batch]
            for fut in as_completed(batch_futures):
                rows = fut.result()
                with pending_lock:
                    pending.extend(rows)
                with progress_lock:
                    completed["idx"] += 1
                    completed["ticks"] += len(rows)
                    idx = completed["idx"]
                    total_ticks = completed["ticks"]
                if idx % progress_every == 0 or idx == total_windows:
                    elapsed = time.monotonic() - t0
                    rate = idx / elapsed if elapsed > 0 else 0
                    remaining_min = (total_windows - idx) / rate / 60 if rate > 0 else float("inf")
                    log.info(
                        "[%d/%d] %.1f windows/s, total ticks: %d, ETA %.0f min",
                        idx, total_windows, rate, total_ticks, remaining_min,
                    )
                # Size-based flush: tick count per window is highly
                # variable (0 to 10k+), so we can't rely on a fixed
                # window-count trigger.
                if len(pending) >= flush_every * 100:
                    maybe_flush()
        # Executor exits here — all batch_futures drop their references.

    # Final flush
    maybe_flush(final=True)

    log.info("Tick backfill complete: %d total fills across %d windows", completed["ticks"], total_windows)
    return completed["ticks"]


def persist_markets_metadata(markets: list[MarketRecord], data_dir: Path) -> None:
    """Write a minimal markets.parquet so consumers can map market_id ↔ token_ids."""
    if not markets:
        return
    rows = []
    for m in markets:
        rows.append({
            "market_id": m.market_id,
            "question": m.question,
            "crypto": m.crypto,
            "timeframe": m.timeframe,
            "volume": m.volume or 0.0,
            "resolution": m.resolution if m.resolution is not None else -1,
            "start_ts": m.start_ts,
            "end_ts": m.end_ts,
            "closed_ts": m.closed_ts if m.closed_ts is not None else 0,
            "condition_id": m.condition_id or "",
            "up_token_id": m.up_token_id,
            "down_token_id": m.down_token_id,
            "slug": m.slug or "",
            "fee_rate_bps": m.fee_rate_bps if m.fee_rate_bps is not None else -1,
        })
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["market_id"], keep="last")
    persist_normalized(
        df,
        pd.DataFrame(),
        markets_path=str(data_dir / "markets.parquet"),
        prices_dir=str(data_dir / "prices"),
        skip_markets=False,
        logger=log,
    )
    log.info("markets.parquet persisted: %d unique markets", len(df))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="YYYY-MM-DD (exclusive)")
    parser.add_argument("--crypto", default="BTC")
    parser.add_argument("--timeframe", default="5-minute")
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reuse existing markets.parquet; skip Gamma scan. Fast restart.",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.05,
        help="Seconds between subgraph requests (default 0.05 = 20 req/s)",
    )
    parser.add_argument(
        "--max-window-hours",
        type=float,
        default=6.0,
        help="Max time-window per subgraph query.  Default 6h splits each "
             "market into ~4 chunks.  Set to 48+ to do each market in one "
             "shot — fewer queries at the cost of larger per-query responses.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Thread count for subgraph fetch parallelism (default 4).  The "
             "global throttle lock enforces the overall rate limit regardless "
             "of worker count; more workers just keep more I/O in flight.",
    )
    args = parser.parse_args()

    start_ts = _parse_date(args.from_date)
    end_ts = _parse_date(args.to_date)
    args.data_dir.mkdir(parents=True, exist_ok=True)

    log.info(
        "=== Targeted collector: %s %s from %s to %s ===",
        args.crypto, args.timeframe, args.from_date, args.to_date,
    )
    log.info("Output dir: %s", args.data_dir.resolve())

    markets_path = args.data_dir / "markets.parquet"
    if args.resume and markets_path.exists():
        markets = load_markets_from_parquet(markets_path, args.crypto, args.timeframe)
    else:
        markets = fetch_markets(start_ts, end_ts, args.crypto, args.timeframe)
    if not markets:
        log.warning("No markets found; nothing to do")
        return 0

    if not args.resume:
        persist_markets_metadata(markets, args.data_dir)
    spot_lookup = fetch_spot_prices(markets, args.data_dir)
    n_ticks = fetch_ticks(
        markets, spot_lookup, args.data_dir,
        request_interval_s=args.request_interval,
        max_window_seconds=int(args.max_window_hours * 3600),
        workers=args.workers,
    )

    log.info("=== COLLECTION COMPLETE ===")
    log.info("Markets: %d", len(markets))
    log.info("Ticks:   %d", n_ticks)
    log.info("Data:    %s", args.data_dir.resolve())
    return 0


if __name__ == "__main__":
    sys.exit(main())
