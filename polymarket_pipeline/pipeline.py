import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import websockets
from py_clob_client.client import ClobClient

from .api import PolymarketApi
from .config import (
    CHAIN_ID,
    CLOB_HOST,
    HF_REPO_ID,
    MAX_WS_RECONNECT_DELAY_SECONDS,
    PARQUET_DATA_DIR,
    PARQUET_MARKETS_PATH,
    PARQUET_PRICES_DIR,
    PARQUET_TICKS_DIR,
    PARQUET_TEST_DIR,
    PARQUET_TEST_MARKETS_PATH,
    PARQUET_TEST_PRICES_DIR,
    PARQUET_TEST_TICKS_DIR,
    PRICE_SUM_TOLERANCE,
    TIME_FRAMES,
    TIMEFRAME_SECONDS,
    WS_FLUSH_BATCH_SIZE,
    WS_FLUSH_INTERVAL_SECONDS,
    WS_URL,
)
from .models import MarketRecord
from .retry import api_call_with_retry
from .storage import (
    load_markets,
    load_prices,
    load_prices_for_timeframe,
    load_ticks,
    persist_normalized,
    persist_ticks,
    split_markets_prices,
    upload_to_huggingface,
)
from .ticks import PolygonTickFetcher


class PolymarketDataPipeline:
    def __init__(
        self,
        api: PolymarketApi | None = None,
        client: ClobClient | None = None,
        logger: logging.Logger | None = None,
        rpc_url: str | None = None,
        polygonscan_key: str | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger("polymarket_pipeline")
        self.api = api or PolymarketApi(logger=self.logger)
        self.client = client or ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID)
        self.existing_dfs: dict[str, pd.DataFrame] = {}
        self._parquet_markets_path = PARQUET_MARKETS_PATH
        self._parquet_prices_dir = PARQUET_PRICES_DIR
        self._parquet_ticks_dir = PARQUET_TICKS_DIR

        # On-chain tick fetcher (None when no RPC or Polygonscan key is provided)
        if rpc_url or polygonscan_key:
            self.tick_fetcher: PolygonTickFetcher | None = PolygonTickFetcher(
                rpc_url=rpc_url,
                polygonscan_key=polygonscan_key,
                logger=self.logger,
            )
        else:
            self.tick_fetcher = None

    def load_existing_data(self) -> None:
        """Load existing price data from Parquet for incremental fetching."""
        for timeframe in TIME_FRAMES:
            self.existing_dfs[timeframe] = load_prices_for_timeframe(
                timeframe, self._parquet_prices_dir,
            )

    # ------------------------------------------------------------------
    # Scan checkpoint — records the max end_ts seen in the last full scan
    # so subsequent runs can skip already-scanned history.
    # ------------------------------------------------------------------

    def _scan_checkpoint_path(self) -> str:
        parent = os.path.dirname(self._parquet_markets_path) or "."
        return os.path.join(parent, ".scan_checkpoint")

    def _load_scan_checkpoint(self) -> int | None:
        try:
            with open(self._scan_checkpoint_path()) as f:
                return int(f.read().strip())
        except (OSError, ValueError):
            return None

    def _save_scan_checkpoint(self, max_end_ts: int) -> None:
        path = self._scan_checkpoint_path()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            f.write(str(max_end_ts))

    def _market_start_ts(self, market: MarketRecord) -> int:
        """Return the timestamp from which to start fetching price history.

        For closed historical markets we restrict the fetch window to the actual
        prediction period: [end_ts - timeframe_seconds, end_ts].  This avoids
        collecting the long pre-trading dormant phase where prices sit flat at
        the default ~0.5 opening value and no meaningful trading has occurred.
        """
        window_seconds = TIMEFRAME_SECONDS.get(market.timeframe, 300)
        # Start of the actual prediction window (never before market open)
        prediction_start = max(market.start_ts, market.end_ts - window_seconds)

        existing_df = self.existing_dfs.get(market.timeframe, pd.DataFrame())
        if existing_df.empty:
            return prediction_start

        market_df = existing_df[existing_df["market_id"] == market.market_id]
        if market_df.empty:
            return prediction_start

        try:
            last_ts = int(market_df["timestamp"].max())
            # Only advance past already-fetched data; never go before prediction window
            return max(prediction_start, last_ts + 1)
        except Exception:
            return prediction_start

    def build_market_dataframe(self, market: MarketRecord) -> pd.DataFrame | None:
        start_ts = self._market_start_ts(market)
        if start_ts >= market.end_ts:
            return None

        self.logger.info("Fetching price history for market %s (start_ts=%s, end_ts=%s)", market.market_id, start_ts, market.end_ts)
        # Fetch both token histories concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(self.api.fetch_price_history, market.token1_id, start_ts, market.end_ts)
            future2 = executor.submit(self.api.fetch_price_history, market.token2_id, start_ts, market.end_ts)
            history1 = future1.result()
            history2 = future2.result()
        
        len_h1 = len(history1) if history1 else 0
        len_h2 = len(history2) if history2 else 0
        self.logger.info("  -> Token1 history rows: %s", len_h1)
        self.logger.info("  -> Token2 history rows: %s", len_h2)

        if not history1 or not history2:
            return None

        history1_df = pd.DataFrame(
            [{"timestamp": int(item["t"]), "price_1": float(item["p"])} for item in history1 if "t" in item and "p" in item]
        ).sort_values("timestamp")
        history2_df = pd.DataFrame(
            [{"timestamp": int(item["t"]), "price_2": float(item["p"])} for item in history2 if "t" in item and "p" in item]
        ).sort_values("timestamp")
        if history1_df.empty or history2_df.empty:
            return None

        union_ts = (
            pd.concat([history1_df[["timestamp"]], history2_df[["timestamp"]]])
            .drop_duplicates()
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        merged_prices = pd.merge_asof(
            union_ts,
            history1_df,
            on="timestamp",
            direction="backward",
            allow_exact_matches=True,
        )
        merged_prices = pd.merge_asof(
            merged_prices,
            history2_df,
            on="timestamp",
            direction="backward",
            allow_exact_matches=True,
        )
        merged_prices = merged_prices.dropna(subset=["price_1", "price_2"])
        if merged_prices.empty:
            return None

        # Data quality validation: token prices should sum to ~1.0
        price_sum = merged_prices["price_1"] + merged_prices["price_2"]
        outlier_count = int(((price_sum - 1.0).abs() > PRICE_SUM_TOLERANCE).sum())
        if outlier_count > 0:
            self.logger.debug(
                "  -> %s/%s rows have price sum deviating >%.2f from 1.0 for market %s",
                outlier_count, len(merged_prices), PRICE_SUM_TOLERANCE, market.market_id,
            )

        # Vectorized column assignment (replaces slow iterrows loop)
        up_is_token1 = market.up_token_id == market.token1_id
        down_is_token2 = market.down_token_id == market.token2_id
        merged_prices = merged_prices.assign(
            market_id=market.market_id,
            crypto=market.crypto,
            timeframe=market.timeframe,
            up_price=merged_prices["price_1"] if up_is_token1 else merged_prices["price_2"],
            down_price=merged_prices["price_2"] if down_is_token2 else merged_prices["price_1"],
            volume=market.volume,
            resolution=market.resolution,
            question=market.question,
        )

        return merged_prices[["market_id", "crypto", "timeframe", "timestamp", "up_price", "down_price", "volume", "resolution", "question"]]

    def persist_dataframe(self, timeframe: str, df: pd.DataFrame) -> pd.DataFrame:
        """Persist a flat DataFrame to normalised Parquet (markets + prices)."""
        df = df.assign(market_id=lambda d: d["market_id"].astype(str))
        markets_df, prices_df = split_markets_prices(df)
        persist_normalized(
            markets_df,
            prices_df,
            markets_path=self._parquet_markets_path,
            prices_dir=self._parquet_prices_dir,
            logger=self.logger,
        )

        # Update in-memory cache for incremental fetch decisions
        existing = self.existing_dfs.get(timeframe, pd.DataFrame())
        merged = (
            pd.concat([existing, prices_df], ignore_index=True)
            .drop_duplicates(subset=["market_id", "timestamp"], keep="last")
            .sort_values(["market_id", "timestamp"])
            .reset_index(drop=True)
        )
        self.existing_dfs[timeframe] = merged
        return merged

    def run_historical_tick_backfill(self, markets: list[MarketRecord]) -> int:
        """Fetch on-chain trade fills for closed markets and persist to the ticks table.

        Requires ``self.tick_fetcher`` to be configured (pass --rpc-url or
        --polygonscan-key on the CLI).  Each market is fetched over the same
        prediction-window time range used for prices.

        Returns the total number of tick rows written.
        """
        if self.tick_fetcher is None:
            self.logger.warning(
                "No Polygon RPC or Polygonscan key configured; skipping on-chain tick backfill. "
                "Pass --rpc-url or --polygonscan-key to enable."
            )
            return 0

        total_ticks = 0
        for i, market in enumerate(markets, 1):
            window_seconds = TIMEFRAME_SECONDS.get(market.timeframe, 300)
            start_ts = max(market.start_ts, market.end_ts - window_seconds)
            end_ts   = market.end_ts

            self.logger.info(
                "[%s/%s] Fetching on-chain ticks for market %s (%s %s, %s–%s)",
                i, len(markets), market.market_id, market.crypto, market.timeframe,
                start_ts, end_ts,
            )
            try:
                ticks = self.tick_fetcher.get_ticks_for_market(market, start_ts, end_ts)
                if not ticks:
                    self.logger.info("  -> No on-chain fills found")
                    continue

                ticks_df = pd.DataFrame(ticks)
                ticks_df["source"] = "onchain"
                persist_ticks(
                    ticks_df,
                    ticks_dir=self._parquet_ticks_dir,
                    logger=self.logger,
                )
                total_ticks += len(ticks)
                self.logger.info("  -> %s on-chain ticks stored", len(ticks))
            except Exception as exc:
                self.logger.error("Failed on-chain tick fetch for market %s: %s", market.market_id, exc)

        self.logger.info("On-chain tick backfill complete: %s total ticks across %s markets", total_ticks, len(markets))
        return total_ticks

    def _initial_last_prices(self, active_markets: list[MarketRecord]) -> dict[str, dict[str, float]]:
        last_prices: dict[str, dict[str, float]] = {}
        for market in active_markets:
            df = self.existing_dfs.get(market.timeframe, pd.DataFrame())
            market_df = df[df["market_id"] == market.market_id] if not df.empty else pd.DataFrame()
            if not market_df.empty:
                row = market_df.iloc[-1]
                last_prices[market.market_id] = {
                    "up": float(row["up_price"]),
                    "down": float(row["down_price"]),
                }
                continue

            try:
                up_price = api_call_with_retry(self.client.get_last_trade_price, market.up_token_id, logger=self.logger) or 0.5
                down_price = api_call_with_retry(self.client.get_last_trade_price, market.down_token_id, logger=self.logger) or 0.5
                last_prices[market.market_id] = {
                    "up": float(up_price),
                    "down": float(down_price),
                }
            except Exception as exc:
                self.logger.warning("Falling back to 0.5 for market %s: %s", market.market_id, exc)
                last_prices[market.market_id] = {"up": 0.5, "down": 0.5}

        return last_prices

    def _flush_ws_buffer(
        self,
        buffer: dict[str, list[dict[str, Any]]],
        tick_buffer: dict[str, list[dict[str, Any]]],
    ) -> int:
        flushed_rows = 0
        for timeframe, rows in buffer.items():
            if not rows:
                continue
            new_df = pd.DataFrame(rows)
            self.persist_dataframe(timeframe, new_df)
            flushed_rows += len(rows)
            buffer[timeframe] = []

        # Flush tick data
        all_ticks: list[dict[str, Any]] = []
        for timeframe, rows in tick_buffer.items():
            if rows:
                all_ticks.extend(rows)
                tick_buffer[timeframe] = []
        if all_ticks:
            ticks_df = pd.DataFrame(all_ticks)
            persist_ticks(ticks_df, ticks_dir=self._parquet_ticks_dir, logger=self.logger)

        return flushed_rows

    async def run_websocket(self, active_markets: list[MarketRecord]) -> None:
        if not active_markets:
            return

        token_to_market: dict[str, tuple[MarketRecord, str]] = {}
        condition_ids: list[str] = []
        for market in active_markets:
            if market.condition_id:
                condition_ids.append(str(market.condition_id))
            token_to_market[market.up_token_id] = (market, "up")
            token_to_market[market.down_token_id] = (market, "down")

        if not condition_ids:
            self.logger.warning("No condition IDs available for active markets; skipping WebSocket stream")
            return

        last_prices = self._initial_last_prices(active_markets)
        ws_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
        tick_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
        pending_rows = 0
        last_flush = time.time()
        reconnect_attempts = 0

        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps({"action": "subscribe", "ids": condition_ids}))
                    self.logger.info("Subscribed to %s active markets over WebSocket", len(condition_ids))
                    reconnect_attempts = 0

                    while True:
                        message = await ws.recv()
                        payload = json.loads(message)
                        events = payload if isinstance(payload, list) else [payload]

                        for event in events:
                            if event.get("type") != "trade":
                                continue

                            token_id = str(event.get("token_id", ""))
                            if token_id not in token_to_market:
                                continue

                            market, outcome_side = token_to_market[token_id]
                            timestamp_ms = int(event.get("timestamp", 0) or 0)
                            if timestamp_ms <= 0:
                                continue
                            timestamp = timestamp_ms // 1000

                            price = float(event.get("price", 0) or 0)
                            if not (0.0 <= price <= 1.0):
                                continue

                            # Raw trade side from the event ("BUY" / "SELL")
                            trade_side = str(event.get("side") or event.get("type_side") or "").upper()
                            if trade_side not in ("BUY", "SELL"):
                                trade_side = "BUY"

                            # Size in shares; multiply by price to get USDC notional
                            size_shares = float(event.get("size") or event.get("amount") or 0)
                            size_usdc   = round(size_shares * price, 6) if size_shares > 0 else 0.0

                            # --- Update coarse price feed ---
                            if market.market_id not in last_prices:
                                last_prices[market.market_id] = {"up": 0.5, "down": 0.5}
                            last_prices[market.market_id][outcome_side] = price

                            ws_buffer[market.timeframe].append(
                                {
                                    "market_id":  market.market_id,
                                    "crypto":     market.crypto,
                                    "timeframe":  market.timeframe,
                                    "timestamp":  timestamp,
                                    "up_price":   last_prices[market.market_id]["up"],
                                    "down_price": last_prices[market.market_id]["down"],
                                    "volume":     market.volume,
                                    "resolution": None,
                                    "question":   market.question,
                                }
                            )

                            # --- Tick-level record (full fill details) ---
                            outcome = "Up" if outcome_side == "up" else "Down"
                            tick_buffer[market.timeframe].append(
                                {
                                    "timestamp_ms": timestamp_ms,
                                    "market_id":    market.market_id,
                                    "crypto":       market.crypto,
                                    "timeframe":    market.timeframe,
                                    "token_id":     token_id,
                                    "outcome":      outcome,
                                    "side":         trade_side,
                                    "price":        price,
                                    "size_usdc":    size_usdc,
                                    "tx_hash":      str(event.get("hash") or event.get("tx_hash") or ""),
                                    "block_number": 0,          # not available from WS
                                    "source":       "websocket",
                                }
                            )
                            pending_rows += 1

                        flush_due_to_size = pending_rows >= WS_FLUSH_BATCH_SIZE
                        flush_due_to_time = (time.time() - last_flush) >= WS_FLUSH_INTERVAL_SECONDS
                        if flush_due_to_size or flush_due_to_time:
                            flushed = self._flush_ws_buffer(ws_buffer, tick_buffer)
                            if flushed:
                                self.logger.info("Flushed %s WebSocket ticks to disk", flushed)
                            pending_rows = 0
                            last_flush = time.time()

            except KeyboardInterrupt:
                flushed = self._flush_ws_buffer(ws_buffer, tick_buffer)
                if flushed:
                    self.logger.info("Final flush on shutdown: %s rows", flushed)
                raise
            except Exception as exc:
                reconnect_delay = min(10 * (2 ** reconnect_attempts), MAX_WS_RECONNECT_DELAY_SECONDS)
                reconnect_attempts += 1
                self.logger.warning("WebSocket disconnected: %s. Reconnecting in %ss...", exc, reconnect_delay)
                flushed = self._flush_ws_buffer(ws_buffer, tick_buffer)
                if flushed:
                    self.logger.info("Flushed %s buffered rows before reconnect", flushed)
                pending_rows = 0
                await asyncio.sleep(reconnect_delay)

    def print_summary(self) -> None:
        self.logger.info("=== FINAL DATASETS ===")

        # Parquet summary
        markets_df = load_markets(self._parquet_markets_path)
        if not markets_df.empty:
            self.logger.info("Markets table: %s (%s unique markets)", self._parquet_markets_path, len(markets_df))

        for timeframe in TIME_FRAMES:
            prices_df = load_prices_for_timeframe(timeframe, self._parquet_prices_dir)
            if prices_df.empty:
                self.logger.info("No Parquet data for %s yet", timeframe)
                continue
            self.logger.info("%s (%s rows in Parquet)", timeframe.upper(), len(prices_df))
            print(prices_df.tail(10).to_markdown(index=False))

    def _print_test_report(self) -> None:
        """Print a validation report for test mode, reading from Parquet."""
        self.logger.info("")
        self.logger.info("=" * 60)
        self.logger.info("TEST MODE VALIDATION REPORT")
        self.logger.info("=" * 60)

        total_rows = 0
        total_markets = 0
        all_ok = True

        for timeframe in TIME_FRAMES:
            df = load_prices_for_timeframe(timeframe, self._parquet_prices_dir)
            if df.empty:
                continue

            n_rows = len(df)
            n_markets = df["market_id"].nunique()
            total_rows += n_rows
            total_markets += n_markets

            self.logger.info("")
            self.logger.info("--- %s ---", timeframe.upper())
            self.logger.info("  Markets: %s  |  Rows: %s", n_markets, n_rows)

            # Check 1: No NaN prices
            nan_up = int(df["up_price"].isna().sum())
            nan_down = int(df["down_price"].isna().sum())
            if nan_up or nan_down:
                self.logger.warning("  FAIL: NaN prices found (up=%s, down=%s)", nan_up, nan_down)
                all_ok = False
            else:
                self.logger.info("  OK: No NaN prices")

            # Check 2: Prices in [0, 1]
            out_of_range = int((
                (df["up_price"] < 0) | (df["up_price"] > 1) |
                (df["down_price"] < 0) | (df["down_price"] > 1)
            ).sum())
            if out_of_range:
                self.logger.warning("  FAIL: %s rows with prices outside [0, 1]", out_of_range)
                all_ok = False
            else:
                self.logger.info("  OK: All prices in [0, 1]")

            # Check 3: Price sum ≈ 1.0
            price_sum = df["up_price"] + df["down_price"]
            bad_sum = int(((price_sum - 1.0).abs() > PRICE_SUM_TOLERANCE).sum())
            mean_sum = float(price_sum.mean())
            if bad_sum:
                self.logger.warning("  WARN: %s/%s rows deviate >%.2f from sum=1.0 (mean=%.4f)", bad_sum, n_rows, PRICE_SUM_TOLERANCE, mean_sum)
            else:
                self.logger.info("  OK: Price sums healthy (mean=%.4f)", mean_sum)

            # Check 4: Timestamps monotonically increasing per market
            ts_issues = 0
            for mid, grp in df.groupby("market_id"):
                if not grp["timestamp"].is_monotonic_increasing:
                    ts_issues += 1
            if ts_issues:
                self.logger.warning("  FAIL: %s market(s) have non-monotonic timestamps", ts_issues)
                all_ok = False
            else:
                self.logger.info("  OK: Timestamps monotonically increasing per market")

            # Check 5: No duplicate (market_id, timestamp) pairs
            dup_count = int(df.duplicated(subset=["market_id", "timestamp"]).sum())
            if dup_count:
                self.logger.warning("  FAIL: %s duplicate (market_id, timestamp) rows", dup_count)
                all_ok = False
            else:
                self.logger.info("  OK: No duplicate (market_id, timestamp) rows")

            # Show sample rows
            self.logger.info("  Sample (last 3 rows):")
            print(df.tail(3).to_markdown(index=False))

        self.logger.info("")
        self.logger.info("=" * 60)
        if total_rows == 0:
            self.logger.warning("NO DATA COLLECTED — test found 0 matching markets.")
            self.logger.info("Tip: Try relaxing filters or increasing N.")
        elif all_ok:
            self.logger.info("ALL CHECKS PASSED  (%s rows across %s markets)", total_rows, total_markets)
        else:
            self.logger.warning("SOME CHECKS FAILED — review warnings above (%s rows across %s markets)", total_rows, total_markets)
        self.logger.info("Test output written to: %s/", self._parquet_prices_dir)
        self.logger.info("=" * 60)

    def run(
        self,
        historical_only: bool = False,
        market_ids: list[str] | None = None,
        cryptos: list[str] | None = None,
        timeframes: list[str] | None = None,
        test_limit: int | None = None,
        upload: bool = False,
        hf_repo: str | None = None,
        data_dir: str | None = None,
        from_date: str | None = None,
    ) -> None:
        is_test = test_limit is not None and test_limit > 0

        # Configure Parquet output paths.
        # Priority: test mode > explicit data_dir > config defaults.
        if is_test:
            self._parquet_markets_path = PARQUET_TEST_MARKETS_PATH
            self._parquet_prices_dir = PARQUET_TEST_PRICES_DIR
            self._parquet_ticks_dir = PARQUET_TEST_TICKS_DIR
        elif data_dir:
            self._parquet_markets_path = f"{data_dir}/markets.parquet"
            self._parquet_prices_dir = f"{data_dir}/prices"
            self._parquet_ticks_dir = f"{data_dir}/ticks"
        else:
            self._parquet_markets_path = PARQUET_MARKETS_PATH
            self._parquet_prices_dir = PARQUET_PRICES_DIR
            self._parquet_ticks_dir = PARQUET_TICKS_DIR

        if is_test:
            os.makedirs(PARQUET_TEST_DIR, exist_ok=True)
            self.logger.info("=== TEST MODE: collecting up to %s historical markets ===", test_limit)
            self.logger.info("Output directory: %s/", PARQUET_TEST_DIR)
        else:
            output_root = data_dir or PARQUET_DATA_DIR
            os.makedirs(output_root, exist_ok=True)

        self.load_existing_data()

        # Compute scan cutoff for incremental runs.
        # After a successful full scan the checkpoint file holds the max end_ts
        # seen.  Subsequent runs only need to scan markets that closed after
        # (checkpoint - 2 days), skipping the thousands of already-seen pages.
        scan_cutoff_ts: int | None = None
        if not is_test:
            if from_date:
                try:
                    scan_cutoff_ts = int(datetime.strptime(from_date, "%Y-%m-%d").timestamp())
                    self.logger.info("Scan limited to markets closing on or after %s", from_date)
                except ValueError:
                    self.logger.warning("Invalid --from-date value '%s'; scanning all markets", from_date)
            else:
                checkpoint = self._load_scan_checkpoint()
                if checkpoint is not None:
                    scan_cutoff_ts = checkpoint - 2 * 86400  # 2-day lookback buffer
                    cutoff_date = datetime.fromtimestamp(scan_cutoff_ts, tz=timezone.utc).strftime("%Y-%m-%d")
                    self.logger.info(
                        "Incremental scan: re-checking markets from %s onwards (checkpoint=%s)",
                        cutoff_date, checkpoint,
                    )

        # Normalize timeframe arguments (e.g. "5m" to "5-minute")
        normalized_timeframes = []
        if timeframes:
            for tf in timeframes:
                tf_lower = tf.lower()
                if tf_lower in ("5m", "5min", "5-minute", "5-min"):
                    normalized_timeframes.append("5-minute")
                elif tf_lower in ("15m", "15min", "15-minute", "15-min"):
                    normalized_timeframes.append("15-minute")
                elif tf_lower in ("1h", "1hr", "1-hour", "hourly"):
                    normalized_timeframes.append("1-hour")
                elif tf_lower in ("4h", "4hr", "4-hour"):
                    normalized_timeframes.append("4-hour")
                else:
                    normalized_timeframes.append(tf)

        # Helper to filter markets
        def is_market_relevant(m: MarketRecord) -> bool:
            if market_ids and m.market_id not in market_ids:
                return False
            if cryptos and m.crypto not in cryptos:
                return False
            if normalized_timeframes and m.timeframe not in normalized_timeframes:
                return False
            return True

        # Helper to process a batch of markets
        pending_dfs: dict[str, list[pd.DataFrame]] = {tf: [] for tf in TIME_FRAMES}
        self.processed_count = 0
        
        def process_market(m: MarketRecord) -> None:
            self.processed_count += 1
            if self.processed_count % 10 == 0:
                self.logger.info("Processed %d relevant markets so far...", self.processed_count)

            try:
                market_df = self.build_market_dataframe(m)
                if market_df is not None and not market_df.empty:
                    pending_dfs[m.timeframe].append(market_df)
                    self.logger.info("  -> Fetched %s rows", len(market_df))
                    
                    # Flush if batch gets big
                    if len(pending_dfs[m.timeframe]) >= 10:
                        combined = pd.concat(pending_dfs[m.timeframe], ignore_index=True)
                        merged = self.persist_dataframe(m.timeframe, combined)
                        self.logger.info("Flushed batch for %s (%s total rows)", m.timeframe, len(merged))
                        pending_dfs[m.timeframe] = []
            except Exception as exc:
                self.logger.error("Failed to process market %s: %s", m.market_id, exc)

        def flush_all():
            for tf, dfs in pending_dfs.items():
                if dfs:
                    combined = pd.concat(dfs, ignore_index=True)
                    merged = self.persist_dataframe(tf, combined)
                    self.logger.info("Final flush for %s (%s total rows)", tf, len(merged))
                    pending_dfs[tf] = []

        # 1. Process Closed Markets (Streamed)
        self.logger.info("Fetching and processing closed markets...")
        fetched_closed = 0
        test_collected = 0
        max_end_ts_seen = 0
        closed_markets_for_ticks: list[MarketRecord] = []
        for m in self.api.fetch_markets(closed=True, end_ts_min=scan_cutoff_ts if not is_test else None):
            # In test mode, stop once we have enough markets
            if is_test and test_collected >= test_limit:
                self.logger.info("Test limit reached (%s markets). Stopping market scan.", test_limit)
                break

            fetched_closed += 1
            if fetched_closed % 500 == 0:
                self.logger.info("Scanned %d closed markets...", fetched_closed)

            if m.end_ts > max_end_ts_seen:
                max_end_ts_seen = m.end_ts

            if is_market_relevant(m):
                process_market(m)
                test_collected += 1
                closed_markets_for_ticks.append(m)

        # Save checkpoint so the next run can skip already-scanned history.
        if max_end_ts_seen > 0 and not is_test:
            self._save_scan_checkpoint(max_end_ts_seen)
            self.logger.info("Scan checkpoint saved (max end_ts: %s)", max_end_ts_seen)

        # 2. Process Active Markets (Streamed & Collected) — skipped in test mode
        active_markets_for_ws = []
        if is_test:
            self.logger.info("Test mode: skipping active markets and WebSocket streaming.")
        elif not historical_only:
            self.logger.info("Fetching and processing active markets...")
            fetched_active = 0
            for m in self.api.fetch_markets(active=True):
                fetched_active += 1
                if fetched_active % 500 == 0:
                     self.logger.info("Scanned %d active markets...", fetched_active)

                if is_market_relevant(m):
                    process_market(m)
                    active_markets_for_ws.append(m)
        else:
            self.logger.info("Historical-only mode enabled. Skipping active markets.")

        # Flush any remaining historical data
        flush_all()

        # On-chain historical tick backfill
        if self.tick_fetcher is not None:
            all_markets_for_ticks = closed_markets_for_ticks + active_markets_for_ws
            if all_markets_for_ticks:
                self.logger.info(
                    "Starting on-chain tick backfill for %s markets...",
                    len(all_markets_for_ticks),
                )
                total_ticks = self.run_historical_tick_backfill(all_markets_for_ticks)
                self.logger.info("Tick backfill complete: %s fills stored.", total_ticks)
            else:
                self.logger.info("No markets matched for tick backfill.")

        if is_test:
            self._print_test_report()
        elif historical_only:
            self.logger.info("Historical-only mode enabled. Skipping WebSocket streaming.")
            self.print_summary()
        elif active_markets_for_ws:
            self.logger.info("Starting real-time WebSocket streaming for %s active markets", len(active_markets_for_ws))
            try:
                asyncio.run(self.run_websocket(active_markets_for_ws))
            except KeyboardInterrupt:
                self.logger.info("Stopped by user")
            self.print_summary()
        else:
            self.logger.info("No active markets matched the filters for real-time monitoring")
            self.print_summary()

        # Upload to Hugging Face Hub if requested (not in test mode)
        if upload and is_test:
            self.logger.warning(
                "--upload is ignored in test mode (test data is never pushed to Hugging Face). "
                "Re-run without --test to upload production data."
            )
        elif upload:
            self.logger.info("Uploading dataset to Hugging Face Hub...")
            upload_to_huggingface(
                repo_id=hf_repo,
                markets_path=self._parquet_markets_path,
                prices_dir=self._parquet_prices_dir,
                ticks_dir=self._parquet_ticks_dir,
                logger=self.logger,
            )
