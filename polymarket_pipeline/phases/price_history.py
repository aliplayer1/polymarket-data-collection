from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from ..config import PRICE_SUM_TOLERANCE, TIME_FRAMES
from ..models import MarketRecord
from ..providers import PriceHistoryProvider
from ..storage import (
    append_ws_culture_prices_staged,
    append_ws_prices_staged,
    load_prices_for_timeframe,
    persist_culture_normalized,
    persist_normalized,
    split_culture_markets_prices,
    split_markets_prices,
)
from .shared import PipelinePaths, build_binary_price_frame, market_record_to_markets_df


class PriceHistoryPhase:
    _MIN_FETCH_WINDOW_SECONDS = 60

    def __init__(
        self,
        price_history_provider: PriceHistoryProvider,
        *,
        logger: logging.Logger,
        paths: PipelinePaths,
    ) -> None:
        self.price_history_provider = price_history_provider
        self.logger = logger
        self.paths = paths
        self.existing_dfs: dict[str, pd.DataFrame] = {}
        self.pending_dfs: dict[str, list[pd.DataFrame]] = {tf: [] for tf in TIME_FRAMES}
        self.processed_count = 0

    def update_paths(self, paths: PipelinePaths) -> None:
        self.paths = paths

    def reset_batch_state(self) -> None:
        self.pending_dfs = {tf: [] for tf in TIME_FRAMES}
        self.processed_count = 0

    def load_existing_data(self) -> None:
        for timeframe in TIME_FRAMES:
            self.existing_dfs[timeframe] = load_prices_for_timeframe(
                timeframe,
                str(self.paths.prices_dir),
            )

    def clear_cache(self) -> None:
        self.existing_dfs.clear()

    def last_cached_prices(self, market: MarketRecord) -> dict[str, float] | None:
        df = self.existing_dfs.get(market.timeframe, pd.DataFrame())
        market_df = df[df["market_id"] == market.market_id] if not df.empty else pd.DataFrame()
        if market_df.empty:
            return None
            
        if market.category == "crypto":
            row = market_df.iloc[-1]
            return {
                "up": float(row["up_price"]),
                "down": float(row["down_price"]),
            }
        else:
            # For culture markets, we need to return the latest price per token/outcome
            latest_prices = {}
            for outcome in market.tokens.keys():
                outcome_df = market_df[market_df["outcome"] == outcome]
                if not outcome_df.empty:
                    latest_prices[outcome] = float(outcome_df.iloc[-1]["price"])
                else:
                    latest_prices[outcome] = 0.5
            return latest_prices

    def _market_start_ts(self, market: MarketRecord) -> int:
        prediction_start = market.start_ts

        existing_df = self.existing_dfs.get(market.timeframe, pd.DataFrame())
        if existing_df.empty:
            return prediction_start

        market_df = existing_df[existing_df["market_id"] == market.market_id]
        if market_df.empty:
            return prediction_start

        try:
            last_ts = int(market_df["timestamp"].max())
            return max(prediction_start, last_ts + 1)
        except Exception:
            return prediction_start

    def build_market_dataframe(self, market: MarketRecord) -> pd.DataFrame | None:
        start_ts = self._market_start_ts(market)
        if start_ts >= market.end_ts:
            return None

        if (market.end_ts - start_ts) < self._MIN_FETCH_WINDOW_SECONDS:
            return None

        self.logger.info(
            "Fetching price history for market %s (start_ts=%s, end_ts=%s)",
            market.market_id,
            start_ts,
            market.end_ts,
        )
        
        histories = {}
        with ThreadPoolExecutor(max_workers=min(5, len(market.tokens))) as executor:
            future_to_outcome = {
                executor.submit(
                    self.price_history_provider.fetch_price_history,
                    token_id,
                    start_ts,
                    market.end_ts,
                ): outcome for outcome, token_id in market.tokens.items()
            }
            for future in as_completed(future_to_outcome):
                outcome = future_to_outcome[future]
                histories[outcome] = future.result()
                
        for outcome, history in histories.items():
            self.logger.info("  -> %s history rows: %s", outcome, len(history) if history else 0)

        if not any(histories.values()):
            return None
            
        if market.category == "culture":
            frames = []
            for outcome, history in histories.items():
                if history:
                    df = pd.DataFrame(
                        [
                            {"timestamp": int(item["t"]), "price": float(item["p"])}
                            for item in history
                            if "t" in item and "p" in item
                        ]
                    ).sort_values("timestamp")
                    if not df.empty:
                        df["market_id"] = market.market_id
                        df["crypto"] = market.crypto
                        df["timeframe"] = market.timeframe
                        df["token_id"] = market.tokens[outcome]
                        df["outcome"] = outcome
                        # Add market-level fields for the split to work correctly
                        df["question"] = market.question
                        df["volume"] = market.volume
                        df["resolution"] = market.resolution
                        df["start_ts"] = market.start_ts
                        df["end_ts"] = market.end_ts
                        df["closed_ts"] = market.closed_ts if market.closed_ts is not None else 0
                        df["condition_id"] = market.condition_id
                        df["tokens"] = json.dumps(market.tokens)
                        df["slug"] = market.slug or ""
                        df["event_slug"] = market.event_slug or ""
                        df["bucket_index"] = market.bucket_index if market.bucket_index is not None else -1
                        df["bucket_label"] = market.bucket_label or ""
                        df["category"] = "culture"
                        frames.append(df)
            if not frames:
                return None
            return pd.concat(frames, ignore_index=True)
            
        else:
            # Binary market processing
            up_history = histories.get(market.up_outcome, [])
            down_history = histories.get(market.down_outcome, [])
            
            if not up_history or not down_history:
                return None

            up_history_df = pd.DataFrame(
                [
                    {"timestamp": int(item["t"]), "up_price": float(item["p"])}
                    for item in up_history
                    if "t" in item and "p" in item
                ]
            ).sort_values("timestamp")
            down_history_df = pd.DataFrame(
                [
                    {"timestamp": int(item["t"]), "down_price": float(item["p"])}
                    for item in down_history
                    if "t" in item and "p" in item
                ]
            ).sort_values("timestamp")
            if up_history_df.empty or down_history_df.empty:
                return None

            union_ts = (
                pd.concat([up_history_df[["timestamp"]], down_history_df[["timestamp"]]])
                .drop_duplicates()
                .sort_values("timestamp")
                .reset_index(drop=True)
            )
            merged_prices = pd.merge_asof(
                union_ts,
                up_history_df,
                on="timestamp",
                direction="backward",
                allow_exact_matches=True,
            )
            merged_prices = pd.merge_asof(
                merged_prices,
                down_history_df,
                on="timestamp",
                direction="backward",
                allow_exact_matches=True,
            )
            merged_prices = merged_prices.fillna({"up_price": 0.5, "down_price": 0.5})
            if merged_prices.empty:
                return None

            price_sum = merged_prices["up_price"] + merged_prices["down_price"]
            outlier_count = int(((price_sum - 1.0).abs() > PRICE_SUM_TOLERANCE).sum())
            if outlier_count > 0:
                self.logger.debug(
                    "  -> %s/%s rows have price sum deviating >%.2f from 1.0 for market %s",
                    outlier_count,
                    len(merged_prices),
                    PRICE_SUM_TOLERANCE,
                    market.market_id,
                )

            df = build_binary_price_frame(
                market,
                timestamps=merged_prices["timestamp"],
                side_prices={
                    "up": merged_prices["up_price"],
                    "down": merged_prices["down_price"],
                },
                volume=market.volume,
                resolution=market.resolution,
                question=market.question,
            )
            df["category"] = "crypto"
            return df

    def persist_dataframe(
        self,
        timeframe: str,
        df: pd.DataFrame,
        *,
        update_cache: bool = True,
    ) -> pd.DataFrame:
        """Write price data to disk and optionally update the in-memory cache.

        Uses lock-free shard writes for prices (no cross-process lock needed)
        and a short lock only for market metadata updates.  This prevents the
        historical service from blocking on the upload service's consolidation
        lock, which previously caused 5-minute timeout crashes.

        Market metadata (``markets.parquet``) still uses ``persist_normalized``
        with the write lock, but the write is fast (small file, no merge with
        millions of price rows).  Price data goes to shard files that the
        upload service's consolidation phase merges later.
        """
        df = df.assign(market_id=lambda data: data["market_id"].astype(str))

        # Split by category
        if "category" not in df.columns:
            df["category"] = "crypto"

        is_culture = df["category"] == "culture"
        culture_df = df[is_culture]
        crypto_df = df[~is_culture]

        prices_frames = []

        if not crypto_df.empty:
            markets_df, prices_df = split_markets_prices(crypto_df)
            if update_cache:
                # Write market metadata under a short lock (small file, fast).
                # Prices go to lock-free shard files — no lock needed.
                persist_normalized(
                    markets_df,
                    pd.DataFrame(),  # empty — prices written as shards below
                    markets_path=str(self.paths.markets_path),
                    prices_dir=str(self.paths.prices_dir),
                    logger=self.logger,
                    skip_markets=False,
                )
                append_ws_prices_staged(
                    prices_df,  # already has crypto/timeframe from split_markets_prices
                    prices_dir=str(self.paths.prices_dir),
                    logger=self.logger,
                )
            prices_frames.append(prices_df)

        if not culture_df.empty:
            culture_markets_df, culture_prices_df = split_culture_markets_prices(culture_df)
            data_culture_dir = self.paths.data_dir.parent / "data-culture"
            data_culture_dir.mkdir(parents=True, exist_ok=True)
            if update_cache:
                persist_culture_normalized(
                    culture_markets_df,
                    pd.DataFrame(),  # empty — prices written as shards below
                    markets_path=str(data_culture_dir / "markets.parquet"),
                    prices_dir=str(data_culture_dir / "prices"),
                    logger=self.logger,
                    skip_markets=False,
                )
                append_ws_culture_prices_staged(
                    culture_prices_df,  # already has crypto/timeframe
                    prices_dir=str(data_culture_dir / "prices"),
                    logger=self.logger,
                )
            prices_frames.append(culture_prices_df)

        if not update_cache:
            return pd.concat(prices_frames) if prices_frames else pd.DataFrame()

        existing = self.existing_dfs.get(timeframe, pd.DataFrame())
        dfs_to_concat = [existing] + prices_frames
        dfs_to_concat = [d for d in dfs_to_concat if not d.empty]

        merged = (
            pd.concat(dfs_to_concat, ignore_index=True) if dfs_to_concat else existing
        )
        if not merged.empty:
            if "outcome" in merged.columns:
                merged = merged.drop_duplicates(subset=["market_id", "timestamp", "outcome"], keep="last")
            else:
                merged = merged.drop_duplicates(subset=["market_id", "timestamp"], keep="last")
            merged = merged.sort_values(["market_id", "timestamp"]).reset_index(drop=True)

        self.existing_dfs[timeframe] = merged
        return merged

    def _fetch_fee_rate(self, market: MarketRecord) -> None:
        """Best-effort fetch of fee_rate_bps for a crypto market."""
        if market.category != "crypto" or not market.up_token_id:
            return
        if market.fee_rate_bps is not None:
            return
        try:
            bps = self.price_history_provider.fetch_fee_rate_bps(market.up_token_id)
            if bps is not None:
                market.fee_rate_bps = bps
        except Exception:
            pass  # best-effort; will remain None → stored as -1

    def process_market_batch(self, batch: list[MarketRecord]) -> None:
        if not batch:
            return

        metadata_only_markets: list[MarketRecord] = []

        with ThreadPoolExecutor(max_workers=min(3, len(batch))) as executor:
            # Fetch fee rates in parallel (crypto only, best-effort)
            fee_futures = [
                executor.submit(self._fetch_fee_rate, market)
                for market in batch if market.category == "crypto"
            ]

            future_to_market = {
                executor.submit(self.build_market_dataframe, market): market for market in batch
            }

            # Wait for fee futures (fast, single API call each)
            for f in fee_futures:
                try:
                    f.result()
                except Exception:
                    pass

            for future in as_completed(future_to_market):
                market = future_to_market[future]
                self.processed_count += 1
                if self.processed_count % 10 == 0:
                    self.logger.info("Processed %d relevant markets so far...", self.processed_count)
                try:
                    market_df = future.result()
                    if market_df is not None and not market_df.empty:
                        self.pending_dfs[market.timeframe].append(market_df)
                        self.logger.info("  -> Fetched %s rows", len(market_df))
                    else:
                        # No new price history — but market metadata may still
                        # have changed (e.g. a closed event just got its
                        # resolution populated). Refresh metadata-only so
                        # markets.parquet eventually converges to the truth.
                        metadata_only_markets.append(market)
                except Exception as exc:
                    self.logger.error("Failed to process market %s: %s", market.market_id, exc)

        if metadata_only_markets:
            self._persist_metadata_only(metadata_only_markets)

        self.flush_if_needed()

    def _persist_metadata_only(self, markets: list[MarketRecord]) -> None:
        """Write updated market metadata (no prices) for markets whose
        price history is already fully cached.

        This backfills the ``resolution`` column for events that closed
        after their prices were first captured — without it, those rows
        stayed at -1 forever because the price-history path treated them
        as "nothing to do".
        """
        if not markets:
            return

        # Split crypto vs culture; persist via the corresponding function
        crypto_rows = [m for m in markets if m.category == "crypto"]
        culture_rows = [m for m in markets if m.category == "culture"]

        if crypto_rows:
            import pandas as pd
            df = pd.concat([market_record_to_markets_df(m) for m in crypto_rows], ignore_index=True)
            from ..storage import persist_normalized
            try:
                persist_normalized(
                    df, pd.DataFrame(),
                    markets_path=str(self.paths.markets_path),
                    prices_dir=str(self.paths.prices_dir),
                    logger=self.logger,
                    skip_markets=False,
                )
                self.logger.info("Metadata refresh (crypto): %d markets", len(crypto_rows))
            except Exception as exc:
                self.logger.debug("Metadata-only refresh failed (crypto): %s", exc)

        if culture_rows:
            import pandas as pd
            df = pd.concat([market_record_to_markets_df(m) for m in culture_rows], ignore_index=True)
            from ..storage import persist_culture_normalized
            data_culture_dir = self.paths.data_dir.parent / "data-culture"
            data_culture_dir.mkdir(parents=True, exist_ok=True)
            try:
                persist_culture_normalized(
                    df, pd.DataFrame(),
                    markets_path=str(data_culture_dir / "markets.parquet"),
                    prices_dir=str(data_culture_dir / "prices"),
                    logger=self.logger,
                    skip_markets=False,
                )
                self.logger.info("Metadata refresh (culture): %d markets", len(culture_rows))
            except Exception as exc:
                self.logger.debug("Metadata-only refresh failed (culture): %s", exc)

    def flush_if_needed(self, *, threshold: int = 10) -> None:
        for timeframe in list(self.pending_dfs.keys()):
            if len(self.pending_dfs[timeframe]) >= threshold:
                combined = pd.concat(self.pending_dfs[timeframe], ignore_index=True)
                merged = self.persist_dataframe(timeframe, combined)
                self.logger.info("Flushed batch for %s (%s total rows)", timeframe, len(merged))
                self.pending_dfs[timeframe] = []

    def flush_all(self) -> None:
        for timeframe, dataframes in self.pending_dfs.items():
            if dataframes:
                combined = pd.concat(dataframes, ignore_index=True)
                merged = self.persist_dataframe(timeframe, combined)
                self.logger.info("Final flush for %s (%s total rows)", timeframe, len(merged))
                self.pending_dfs[timeframe] = []
