from __future__ import annotations

import asyncio
import gc
import logging
from datetime import datetime, timezone

from py_clob_client.client import ClobClient

from .api import PolymarketApi
from .config import CHAIN_ID, CLOB_HOST, PRICE_SUM_TOLERANCE, TIME_FRAMES
from .models import MarketRecord
from .phases import PipelinePaths, PriceHistoryPhase, PythPricePhase, RTDSStreamPhase, TickBackfillPhase, WebSocketPhase
from .providers import (
    ClobLastTradePriceProvider,
    LastTradePriceProvider,
    MarketDataProvider,
    TickBatchProvider,
)
from .settings import PipelineRunOptions, RuntimeSettings
from .storage import consolidate_ticks, load_markets, load_prices_for_timeframe, upload_to_huggingface
from .ticks import PolygonTickFetcher


class PolymarketDataPipeline:
    _MARKET_BATCH_WORKERS = 5

    def __init__(
        self,
        api: MarketDataProvider | None = None,
        client: ClobClient | None = None,
        last_trade_price_provider: LastTradePriceProvider | None = None,
        tick_provider: TickBatchProvider | None = None,
        logger: logging.Logger | None = None,
        settings: RuntimeSettings | None = None,
        rpc_url: str | None = None,
        polygonscan_key: str | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger("polymarket_pipeline")
        self.settings = settings or RuntimeSettings(
            rpc_url=rpc_url,
            polygonscan_key=polygonscan_key,
        )
        self.api = api or PolymarketApi(logger=self.logger)
        self._clob_client = client or ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID)
        self.last_trade_price_provider = (
            last_trade_price_provider or ClobLastTradePriceProvider(self._clob_client)
        )
        self.tick_provider = tick_provider
        if self.tick_provider is None and (self.settings.rpc_url or self.settings.polygonscan_key):
            self.tick_provider = PolygonTickFetcher(
                rpc_url=self.settings.rpc_url,
                polygonscan_key=self.settings.polygonscan_key,
                logger=self.logger,
            )

        default_paths = self.settings.resolve_paths(PipelineRunOptions())
        self.paths = default_paths
        self._parquet_markets_path = str(default_paths.markets_path)
        self._parquet_prices_dir = str(default_paths.prices_dir)
        self._parquet_ticks_dir = str(default_paths.ticks_dir)

        self.price_history_phase = PriceHistoryPhase(
            self.api,
            logger=self.logger,
            paths=default_paths,
        )
        self.tick_backfill_phase = TickBackfillPhase(
            self.tick_provider,
            logger=self.logger,
            paths=default_paths,
        )
        self.pyth_phase = PythPricePhase(
            logger=self.logger,
            paths=default_paths,
        )

        # Shared in-memory cache: RTDS writes, WebSocket reads.
        # Both run in the same asyncio event loop — no lock needed.
        # Structure: {"btcusdt": (67234.50, 1710000000087), ...}
        self._spot_price_cache: dict[str, tuple[float, int]] = {}

        self.rtds_stream_phase = RTDSStreamPhase(
            self._spot_price_cache,
            logger=self.logger,
        )
        self.websocket_phase = WebSocketPhase(
            self.last_trade_price_provider,
            self.price_history_phase,
            logger=self.logger,
            paths=default_paths,
            spot_price_cache=self._spot_price_cache,
        )

    def _set_paths(self, paths: PipelinePaths) -> None:
        self.paths = paths
        self._parquet_markets_path = str(paths.markets_path)
        self._parquet_prices_dir = str(paths.prices_dir)
        self._parquet_ticks_dir = str(paths.ticks_dir)
        self.price_history_phase.update_paths(paths)
        self.tick_backfill_phase.update_paths(paths)
        self.websocket_phase.update_paths(paths)

    def load_existing_data(self) -> None:
        self.price_history_phase.load_existing_data()

    def run_historical_tick_backfill(self, markets: list[MarketRecord]) -> int:
        return self.tick_backfill_phase.run(markets)

    async def run_websocket(self, active_markets: list[MarketRecord]) -> None:
        rtds_task = asyncio.create_task(self.rtds_stream_phase.run())
        try:
            await self.websocket_phase.run(active_markets)
        finally:
            rtds_task.cancel()
            await asyncio.gather(rtds_task, return_exceptions=True)

    def _scan_checkpoint_path(self) -> str:
        return str(self.paths.scan_checkpoint_path())

    def _load_scan_checkpoint(self) -> int | None:
        try:
            with self.paths.scan_checkpoint_path().open() as handle:
                return int(handle.read().strip())
        except (OSError, ValueError):
            return None

    def _save_scan_checkpoint(self, max_end_ts: int) -> None:
        checkpoint_path = self.paths.scan_checkpoint_path()
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        with checkpoint_path.open("w") as handle:
            handle.write(str(max_end_ts))

    def _compute_scan_cutoff_ts(self, run_options: PipelineRunOptions) -> int | None:
        if run_options.is_test:
            return None

        checkpoint = self._load_scan_checkpoint()
        if checkpoint is not None:
            scan_cutoff_ts = checkpoint - 2 * 86400
            cutoff_date = datetime.fromtimestamp(scan_cutoff_ts, tz=timezone.utc).strftime("%Y-%m-%d")
            self.logger.info(
                "Incremental scan: re-checking markets from %s onwards (checkpoint=%s)",
                cutoff_date,
                checkpoint,
            )
            return scan_cutoff_ts

        if run_options.from_date:
            try:
                scan_cutoff_ts = int(datetime.strptime(run_options.from_date, "%Y-%m-%d").timestamp())
                self.logger.info(
                    "No checkpoint found; scan limited to markets closing on or after %s",
                    run_options.from_date,
                )
                return scan_cutoff_ts
            except ValueError:
                self.logger.warning(
                    "Invalid --from-date value '%s'; scanning all markets",
                    run_options.from_date,
                )
                return None

        self.logger.info("No checkpoint and no --from-date; scanning all closed markets")
        return None

    def _is_market_relevant(
        self,
        market: MarketRecord,
        run_options: PipelineRunOptions,
        scan_cutoff_ts: int | None,
    ) -> bool:
        if run_options.market_ids and market.market_id not in run_options.market_ids:
            return False
        if run_options.cryptos and market.crypto not in run_options.cryptos:
            return False
        if run_options.timeframes and market.timeframe not in run_options.timeframes:
            return False
        if scan_cutoff_ts is not None:
            market_ts = market.closed_ts if market.closed_ts is not None else market.end_ts
            if market_ts < scan_cutoff_ts:
                return False
        return True

    def _collect_closed_markets(
        self,
        run_options: PipelineRunOptions,
        scan_cutoff_ts: int | None,
    ) -> list[MarketRecord]:
        self.logger.info("Fetching and processing closed markets...")
        fetched_closed = 0
        test_collected = 0
        max_end_ts_seen = 0
        relevant_batch: list[MarketRecord] = []
        closed_markets_for_ticks: list[MarketRecord] = []

        for market in self.api.fetch_markets(
            closed=True,
            end_ts_min=scan_cutoff_ts if not run_options.is_test else None,
        ):
            if run_options.is_test and run_options.test_limit is not None and test_collected >= run_options.test_limit:
                self.logger.info(
                    "Test limit reached (%s markets). Stopping market scan.",
                    run_options.test_limit,
                )
                break

            fetched_closed += 1
            if fetched_closed % 500 == 0:
                self.logger.info("Scanned %d closed markets...", fetched_closed)

            if market.end_ts > max_end_ts_seen:
                max_end_ts_seen = market.end_ts

            if not run_options.is_test and fetched_closed % 500 == 0 and max_end_ts_seen > 0:
                self._save_scan_checkpoint(max_end_ts_seen)

            if self._is_market_relevant(market, run_options, scan_cutoff_ts):
                relevant_batch.append(market)
                test_collected += 1
                closed_markets_for_ticks.append(market)
                if len(relevant_batch) >= self._MARKET_BATCH_WORKERS:
                    self.price_history_phase.process_market_batch(relevant_batch)
                    relevant_batch = []

        if relevant_batch:
            self.price_history_phase.process_market_batch(relevant_batch)

        if max_end_ts_seen > 0 and not run_options.is_test:
            self._save_scan_checkpoint(max_end_ts_seen)
            self.logger.info("Scan checkpoint saved (max end_ts: %s)", max_end_ts_seen)

        return closed_markets_for_ticks

    def _collect_active_markets(
        self,
        run_options: PipelineRunOptions,
        scan_cutoff_ts: int | None,
    ) -> list[MarketRecord]:
        self.logger.info("Fetching and processing active markets...")
        fetched_active = 0
        active_batch: list[MarketRecord] = []
        active_markets_for_ws: list[MarketRecord] = []

        for market in self.api.fetch_markets(active=True):
            fetched_active += 1
            if fetched_active % 500 == 0:
                self.logger.info("Scanned %d active markets...", fetched_active)

            if self._is_market_relevant(market, run_options, scan_cutoff_ts):
                active_markets_for_ws.append(market)
                if not run_options.websocket_only:
                    active_batch.append(market)
                    if len(active_batch) >= self._MARKET_BATCH_WORKERS:
                        self.price_history_phase.process_market_batch(active_batch)
                        active_batch = []

        if not run_options.websocket_only and active_batch:
            self.price_history_phase.process_market_batch(active_batch)

        return active_markets_for_ws

    def print_summary(self) -> None:
        self.logger.info("=== FINAL DATASETS ===")

        markets_df = load_markets(self._parquet_markets_path)
        if not markets_df.empty:
            self.logger.info(
                "Markets table: %s (%s unique markets)",
                self._parquet_markets_path,
                len(markets_df),
            )

        for timeframe in TIME_FRAMES:
            prices_df = load_prices_for_timeframe(timeframe, self._parquet_prices_dir)
            if prices_df.empty:
                self.logger.info("No Parquet data for %s yet", timeframe)
                continue
            self.logger.info("%s (%s rows in Parquet)", timeframe.upper(), len(prices_df))
            print(prices_df.tail(10).to_markdown(index=False))

    def _print_test_report(self) -> None:
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

            nan_up = int(df["up_price"].isna().sum())
            nan_down = int(df["down_price"].isna().sum())
            if nan_up or nan_down:
                self.logger.warning("  FAIL: NaN prices found (up=%s, down=%s)", nan_up, nan_down)
                all_ok = False
            else:
                self.logger.info("  OK: No NaN prices")

            out_of_range = int(
                (
                    (df["up_price"] < 0)
                    | (df["up_price"] > 1)
                    | (df["down_price"] < 0)
                    | (df["down_price"] > 1)
                ).sum()
            )
            if out_of_range:
                self.logger.warning("  FAIL: %s rows with prices outside [0, 1]", out_of_range)
                all_ok = False
            else:
                self.logger.info("  OK: All prices in [0, 1]")

            price_sum = df["up_price"] + df["down_price"]
            bad_sum = int(((price_sum - 1.0).abs() > PRICE_SUM_TOLERANCE).sum())
            mean_sum = float(price_sum.mean())
            if bad_sum:
                self.logger.warning(
                    "  WARN: %s/%s rows deviate >%.2f from sum=1.0 (mean=%.4f)",
                    bad_sum,
                    n_rows,
                    PRICE_SUM_TOLERANCE,
                    mean_sum,
                )
            else:
                self.logger.info("  OK: Price sums healthy (mean=%.4f)", mean_sum)

            ts_issues = 0
            for _, group in df.groupby("market_id"):
                if not group["timestamp"].is_monotonic_increasing:
                    ts_issues += 1
            if ts_issues:
                self.logger.warning("  FAIL: %s market(s) have non-monotonic timestamps", ts_issues)
                all_ok = False
            else:
                self.logger.info("  OK: Timestamps monotonically increasing per market")

            dup_count = int(df.duplicated(subset=["market_id", "timestamp"]).sum())
            if dup_count:
                self.logger.warning("  FAIL: %s duplicate (market_id, timestamp) rows", dup_count)
                all_ok = False
            else:
                self.logger.info("  OK: No duplicate (market_id, timestamp) rows")

            self.logger.info("  Sample (last 3 rows):")
            print(df.tail(3).to_markdown(index=False))

        self.logger.info("")
        self.logger.info("=" * 60)
        if total_rows == 0:
            self.logger.warning("NO DATA COLLECTED — test found 0 matching markets.")
            self.logger.info("Tip: Try relaxing filters or increasing N.")
        elif all_ok:
            self.logger.info(
                "ALL CHECKS PASSED  (%s rows across %s markets)",
                total_rows,
                total_markets,
            )
        else:
            self.logger.warning(
                "SOME CHECKS FAILED — review warnings above (%s rows across %s markets)",
                total_rows,
                total_markets,
            )
        self.logger.info("Test output written to: %s/", self._parquet_prices_dir)
        self.logger.info("=" * 60)

    def run(
        self,
        historical_only: bool = False,
        websocket_only: bool = False,
        market_ids: list[str] | None = None,
        cryptos: list[str] | None = None,
        timeframes: list[str] | None = None,
        test_limit: int | None = None,
        upload: bool = False,
        hf_repo: str | None = None,
        data_dir: str | None = None,
        from_date: str | None = None,
        run_options: PipelineRunOptions | None = None,
    ) -> None:
        options = run_options or PipelineRunOptions.from_values(
            historical_only=historical_only,
            websocket_only=websocket_only,
            market_ids=market_ids,
            cryptos=cryptos,
            timeframes=timeframes,
            test_limit=test_limit,
            upload=upload,
            from_date=from_date,
        )
        self.settings = self.settings.with_overrides(data_dir=data_dir, hf_repo=hf_repo)
        self._set_paths(self.settings.resolve_paths(options))

        self.price_history_phase.reset_batch_state()
        self.price_history_phase.clear_cache()
        self.paths.ensure_data_dir()

        if options.is_test:
            self.logger.info("=== TEST MODE: collecting up to %s historical markets ===", options.test_limit)
            self.logger.info("Output directory: %s/", self.paths.data_dir)

        if not options.websocket_only:
            self.load_existing_data()

        scan_cutoff_ts = self._compute_scan_cutoff_ts(options)

        if options.websocket_only:
            self.logger.info("WebSocket-only mode: skipping historical scan.")
            closed_markets_for_ticks: list[MarketRecord] = []
        else:
            closed_markets_for_ticks = self._collect_closed_markets(options, scan_cutoff_ts)

        if options.is_test:
            self.logger.info("Test mode: skipping active markets and WebSocket streaming.")
            active_markets_for_ws: list[MarketRecord] = []
        elif options.historical_only:
            self.logger.info("Historical-only mode enabled. Skipping active markets.")
            active_markets_for_ws = []
        else:
            active_markets_for_ws = self._collect_active_markets(options, scan_cutoff_ts)

        if not options.websocket_only:
            self.price_history_phase.flush_all()

        if not options.websocket_only and self.tick_backfill_phase.is_enabled:
            self.price_history_phase.clear_cache()
            gc.collect()
            all_markets_for_ticks = closed_markets_for_ticks + active_markets_for_ws
            if all_markets_for_ticks:
                self.logger.info(
                    "Starting on-chain tick backfill for %s markets...",
                    len(all_markets_for_ticks),
                )
                total_ticks = self.run_historical_tick_backfill(all_markets_for_ticks)
                self.logger.info("Tick backfill complete: %s fills stored.", total_ticks)
                self.logger.info("Consolidating tick shard files...")
                consolidate_ticks(ticks_dir=self._parquet_ticks_dir, logger=self.logger)
            else:
                self.logger.info("No markets matched for tick backfill.")

        if not options.websocket_only and self.pyth_phase.is_enabled:
            self.pyth_phase.run(closed_markets_for_ticks)

        if options.upload and options.is_test:
            self.logger.warning(
                "--upload is ignored in test mode (test data is never pushed to Hugging Face). "
                "Re-run without --test to upload production data.",
            )
        elif options.upload and options.websocket_only:
            self.logger.info("WebSocket-only mode: skipping Hugging Face upload.")
        elif options.upload:
            self.logger.info("Uploading dataset to Hugging Face Hub...")
            try:
                upload_to_huggingface(
                    repo_id=self.settings.hf_repo,
                    markets_path=self._parquet_markets_path,
                    prices_dir=self._parquet_prices_dir,
                    ticks_dir=self._parquet_ticks_dir,
                    logger=self.logger,
                    skip_consolidate=True,
                )
            except Exception as exc:
                self.logger.error("Hugging Face upload failed (non-fatal): %s", exc)
                self.logger.info("Continuing to WebSocket streaming despite upload failure.")

        if options.is_test:
            self._print_test_report()
        elif options.historical_only:
            self.logger.info("Historical-only mode enabled. Skipping WebSocket streaming.")
            self.print_summary()
        elif active_markets_for_ws:
            self.logger.info(
                "Starting real-time WebSocket streaming for %s active markets",
                len(active_markets_for_ws),
            )
            try:
                asyncio.run(self.run_websocket(active_markets_for_ws))
            except KeyboardInterrupt:
                self.logger.info("Stopped by user")
            self.print_summary()
        else:
            self.logger.info("No active markets matched the filters for real-time monitoring")
            self.print_summary()
