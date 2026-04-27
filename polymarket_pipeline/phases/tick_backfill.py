from __future__ import annotations

import logging
import os
from collections import defaultdict

import pandas as pd

from ..config import HF_CULTURE_REPO_ID
from ..models import MarketRecord
from ..providers import TickBatchProvider
from ..storage import append_ticks_only, upload_to_huggingface
from .shared import PipelinePaths


class TickBackfillPhase:
    def __init__(
        self,
        tick_provider: TickBatchProvider | None,
        *,
        logger: logging.Logger,
        paths: PipelinePaths,
    ) -> None:
        self.tick_provider = tick_provider
        self.logger = logger
        self.paths = paths

    @property
    def is_enabled(self) -> bool:
        return self.tick_provider is not None

    def update_paths(self, paths: PipelinePaths) -> None:
        self.paths = paths

    def run(self, markets: list[MarketRecord]) -> int:
        if self.tick_provider is None:
            self.logger.warning(
                "Tick provider unavailable; skipping on-chain tick backfill. "
                "Set PM_DISABLE_TICK_BACKFILL=0 (or unset) and ensure "
                "subgraph endpoints are reachable.",
            )
            return 0

        # Performance optimization: skip on-chain backfill for "culture" markets.
        # These markets (e.g. Elon Musk tweets) have many tokens and long durations,
        # making historical log scans extremely slow. We still collect their
        # share prices (OHLC) via the PriceHistoryPhase.
        original_count = len(markets)
        markets = [m for m in markets if m.category != "culture"]
        if len(markets) < original_count:
            self.logger.info(
                "Skipping on-chain tick backfill for %d 'culture' markets; "
                "share price (OHLC) history will still be collected.",
                original_count - len(markets),
            )

        if not markets:
            return 0

        # Max window size for a single on-chain log query (6 hours).
        # Larger windows (e.g. 4-day culture markets) will be chunked.
        MAX_WINDOW_SECONDS = 6 * 3600

        windows: dict[tuple[int, int], list[MarketRecord]] = defaultdict(list)
        for market in markets:
            win_start = market.start_ts

            # Chunk the window if it exceeds MAX_WINDOW_SECONDS.  Use
            # half-open chunks: the next chunk starts at ``current_end + 1``
            # (subgraph queries use inclusive ``timestamp_gte`` /
            # ``timestamp_lte``, so reusing ``current_end`` as the next
            # ``current_start`` would re-fetch every fill that lands
            # exactly on a chunk boundary, doubling our subgraph budget
            # at every 6 h boundary on busy markets).
            current_start = win_start
            while current_start < market.end_ts:
                current_end = min(current_start + MAX_WINDOW_SECONDS, market.end_ts)
                windows[(current_start, current_end)].append(market)
                current_start = current_end + 1

        tick_flush_every = 5
        total_ticks = 0
        pending_ticks: list[dict] = []
        n_windows = len(windows)
        
        # Sort windows by start time to keep log output chronological
        sorted_windows = sorted(windows.items(), key=lambda x: x[0][0])
        
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        total_ticks = 0
        total_ticks_lock = threading.Lock()
        
        def process_window(index: int, win_start: int, win_end: int, window_markets: list[MarketRecord]) -> list[dict]:
            self.logger.info(
                "[%d/%d] Fetching on-chain ticks for window %s–%s (%s)",
                index,
                n_windows,
                win_start,
                win_end,
                ", ".join(set(f"{market.crypto}/{market.timeframe}" for market in window_markets)),
            )
            try:
                batch = self.tick_provider.get_ticks_for_markets_batch(
                    window_markets,
                    win_start,
                    win_end,
                )
                window_ticks = []
                for market in window_markets:
                    ticks = batch.get(market.market_id, [])
                    if not ticks:
                        continue
                    window_ticks.extend(ticks)
                    with total_ticks_lock:
                        nonlocal total_ticks
                        total_ticks += len(ticks)
                    self.logger.info("  -> %s %s: %s fills", market.crypto, market.timeframe, len(ticks))
                return window_ticks
            except Exception as exc:
                self.logger.error(
                    "Failed on-chain tick fetch for window %s–%s: %s",
                    win_start,
                    win_end,
                    exc,
                )
                return []

        pending_ticks: list[dict] = []

        # The legacy Etherscan/RPC tick provider supported parallelism;
        # the subgraph provider that replaced it (commit 1ee7f9a)
        # serialises every request on a global 5 req/s polite throttle
        # (see ``subgraph_client._throttle``), so 4 worker threads
        # gained no throughput and just wasted thread overhead.  We
        # keep an executor here only to preserve the as-completed
        # iteration shape of the surrounding flush logic.
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = [
                executor.submit(process_window, i, ws, we, wm) 
                for i, ((ws, we), wm) in enumerate(sorted_windows, 1)
            ]
            
            for i, future in enumerate(as_completed(futures), 1):
                pending_ticks.extend(future.result())
                
                # Periodically flush to disk to keep memory usage low
                if pending_ticks and (i % tick_flush_every == 0 or i == n_windows):
                    ticks_df = pd.DataFrame(pending_ticks)
                    
                    if "category" in ticks_df.columns:
                        culture_mask = ticks_df["category"] == "culture"
                        crypto_df = ticks_df[~culture_mask].drop(columns=["category"], errors="ignore")
                        culture_df = ticks_df[culture_mask].drop(columns=["category"], errors="ignore")
                    else:
                        crypto_df = ticks_df
                        culture_df = pd.DataFrame()

                    if not crypto_df.empty:
                        append_ticks_only(
                            crypto_df,
                            ticks_dir=str(self.paths.ticks_dir),
                            logger=self.logger,
                        )
                    if not culture_df.empty:
                        data_culture_dir = self.paths.data_dir.parent / "data-culture"
                        append_ticks_only(
                            culture_df,
                            ticks_dir=str(data_culture_dir / "ticks"),
                            logger=self.logger,
                        )

                    self.logger.info(
                        "Flushed %d ticks to disk (batch %d/%d)",
                        len(pending_ticks),
                        i,
                        n_windows,
                    )
                    pending_ticks = []

                    # Periodic upload of culture data (every 500 windows) to prevent
                    # the dataset from appearing stale during massive backfills.
                    if i % 500 == 0:
                        culture_root = self.paths.data_dir.parent / "data-culture"
                        if culture_root.exists():
                            repo_id = os.environ.get("HF_CULTURE_REPO_ID", HF_CULTURE_REPO_ID)
                            self.logger.info(
                                "Periodic upload: pushing intermediate culture data to %s...",
                                repo_id,
                            )
                            try:
                                upload_to_huggingface(
                                    repo_id=repo_id,
                                    markets_path=str(culture_root / "markets.parquet"),
                                    prices_dir=str(culture_root / "prices"),
                                    ticks_dir=str(culture_root / "ticks"),
                                    logger=self.logger,
                                    skip_consolidate=False,
                                )
                            except Exception as exc:
                                self.logger.warning("Periodic culture upload failed: %s", exc)

        self.logger.info(
            "On-chain tick backfill complete: %s total ticks across %s markets",
            total_ticks,
            len(markets),
        )
        return total_ticks
