from __future__ import annotations

import logging
from collections import defaultdict

import pandas as pd

from ..config import TIMEFRAME_SECONDS
from ..models import MarketRecord
from ..providers import TickBatchProvider
from ..storage import append_ticks_only
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
                "No Polygon RPC or Polygonscan key configured; skipping on-chain tick backfill. "
                "Pass --rpc-url or --polygonscan-key to enable.",
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
            
            # Chunk the window if it exceeds MAX_WINDOW_SECONDS
            current_start = win_start
            while current_start < market.end_ts:
                current_end = min(current_start + MAX_WINDOW_SECONDS, market.end_ts)
                windows[(current_start, current_end)].append(market)
                current_start = current_end

        tick_flush_every = 5
        total_ticks = 0
        pending_ticks: list[dict] = []
        n_windows = len(windows)
        
        # Sort windows by start time to keep log output chronological
        sorted_windows = sorted(windows.items(), key=lambda x: x[0][0])

        for index, ((win_start, win_end), window_markets) in enumerate(sorted_windows, 1):
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
                for market in window_markets:
                    ticks = batch.get(market.market_id, [])
                    if not ticks:
                        continue
                    pending_ticks.extend(ticks)
                    total_ticks += len(ticks)
                    self.logger.info("  -> %s %s: %s fills", market.crypto, market.timeframe, len(ticks))
            except Exception as exc:
                self.logger.error(
                    "Failed on-chain tick fetch for window %s–%s: %s",
                    win_start,
                    win_end,
                    exc,
                )

            if pending_ticks and (index % tick_flush_every == 0 or index == n_windows):
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
                    "Flushed %d ticks to disk (window %d/%d)",
                    len(pending_ticks),
                    index,
                    n_windows,
                )
                pending_ticks = []

        self.logger.info(
            "On-chain tick backfill complete: %s total ticks across %s markets",
            total_ticks,
            len(markets),
        )
        return total_ticks
