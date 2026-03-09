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

        windows: dict[tuple[int, int], list[MarketRecord]] = defaultdict(list)
        for market in markets:
            window_seconds = TIMEFRAME_SECONDS.get(market.timeframe, 300)
            win_start = max(market.start_ts, market.end_ts - window_seconds)
            windows[(win_start, market.end_ts)].append(market)

        tick_flush_every = 5
        total_ticks = 0
        pending_ticks: list[dict] = []
        n_windows = len(windows)

        for index, ((win_start, win_end), window_markets) in enumerate(windows.items(), 1):
            self.logger.info(
                "[%d/%d] Fetching on-chain ticks for window %s–%s (%s)",
                index,
                n_windows,
                win_start,
                win_end,
                ", ".join(f"{market.crypto}/{market.timeframe}" for market in window_markets),
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
                append_ticks_only(
                    ticks_df,
                    ticks_dir=str(self.paths.ticks_dir),
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
