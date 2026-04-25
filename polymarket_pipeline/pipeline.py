from __future__ import annotations

import asyncio
import gc
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from py_clob_client.client import ClobClient

from .api import PolymarketApi
from .config import CHAIN_ID, CLOB_HOST, PRICE_SUM_TOLERANCE, TIME_FRAMES, WS_MARKET_REFRESH_INTERVAL_S
from .models import MarketRecord
from .phases import PipelinePaths, PriceHistoryPhase, PythPricePhase, RTDSStreamPhase, TickBackfillPhase, WebSocketPhase
from .phases.binance_history import BinanceHistoryPhase
from .providers import (
    ClobLastTradePriceProvider,
    LastTradePriceProvider,
    MarketDataProvider,
    TickBatchProvider,
)
from .settings import PipelineRunOptions, RuntimeSettings
from .storage import consolidate_culture_prices, consolidate_ticks, load_markets, load_prices_for_timeframe, upload_to_huggingface


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
        rpc_urls: list[str] | tuple[str, ...] | None = None,
        polygonscan_key: str | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger("polymarket_pipeline")
        
        if settings is not None:
            self.settings = settings
        else:
            # Reconstruct settings from individual components if not provided
            final_urls = rpc_urls if rpc_urls else ((rpc_url,) if rpc_url else ())
            self.settings = RuntimeSettings(
                rpc_urls=tuple(final_urls),
                polygonscan_key=polygonscan_key,
            )

        self.api = api or PolymarketApi(logger=self.logger)
        self._clob_client = client or ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID)
        self.last_trade_price_provider = (
            last_trade_price_provider or ClobLastTradePriceProvider(self._clob_client)
        )
        self.tick_provider = tick_provider
        if self.tick_provider is None:
            self.tick_provider = self._build_tick_provider()

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
        self.binance_history_phase = BinanceHistoryPhase(
            logger=self.logger,
        )

        # Shared in-memory caches: RTDS writes, WebSocket reads.
        # Both run in the same asyncio event loop — no lock needed.
        # Structure: {"btcusdt": (67234.50, 1710000000087), ...}
        self._spot_price_cache: dict[str, tuple[float, int]] = {}
        self._chainlink_price_cache: dict[str, tuple[float, int]] = {}
        # Spot price buffer: RTDS appends rows, WS flush loop drains to
        # Parquet.  Bounded drop-oldest ring absorbs sustained RTDS rates
        # (~30 updates/s combined) without unbounded memory growth if the
        # flush loop stalls.
        from .config import WS_SPOT_BUFFER_MAX
        from .phases.ws_watchdog import DataHeartbeat, DropOldestBuffer  # local to avoid cycle
        self._spot_price_buffer: DropOldestBuffer = DropOldestBuffer(WS_SPOT_BUFFER_MAX)
        # Shared heartbeat registry: CLOB shards + RTDS feeds each register
        # their DataHeartbeat instances so the WS flush loop can emit
        # heartbeat rows for the full WebSocket surface area.
        self._heartbeat_registry: dict[str, DataHeartbeat] = {}

        self.rtds_stream_phase = RTDSStreamPhase(
            self._spot_price_cache,
            self._chainlink_price_cache,
            self._spot_price_buffer,
            heartbeat_registry=self._heartbeat_registry,
            logger=self.logger,
        )
        self.websocket_phase = WebSocketPhase(
            self.last_trade_price_provider,
            self.price_history_phase,
            logger=self.logger,
            paths=default_paths,
            spot_price_cache=self._spot_price_cache,
            spot_price_buffer=self._spot_price_buffer,
            heartbeat_registry=self._heartbeat_registry,
        )

    def _build_tick_provider(self):
        """Construct the subgraph tick provider.

        The legacy Etherscan/RPC ``eth_getLogs`` path was removed —
        Polymarket's orderbook subgraph (Goldsky, with optional Graph-
        network fallback via ``SUBGRAPH_API_KEY``) is the sole tick
        source.  Set the ``PM_DISABLE_TICK_BACKFILL=1`` env var if you
        need to run without tick backfill (e.g. in a unit-test
        environment that has no live network access).
        """
        if os.environ.get("PM_DISABLE_TICK_BACKFILL", "").strip() == "1":
            self.logger.info("Tick backfill disabled via PM_DISABLE_TICK_BACKFILL=1")
            return None
        from .config import SUBGRAPH_URL_FALLBACK, SUBGRAPH_URL_PRIMARY
        from .phases.subgraph_ticks import SubgraphTickFetcher
        from .subgraph_client import SubgraphClient
        api_key = os.environ.get("SUBGRAPH_API_KEY")
        client = SubgraphClient(
            primary_url=SUBGRAPH_URL_PRIMARY,
            fallback_url=SUBGRAPH_URL_FALLBACK,
            api_key=api_key,
            logger=self.logger,
        )
        self.logger.info(
            "Tick backfill mode: SUBGRAPH (fallback=%s)",
            "enabled" if api_key else "disabled",
        )
        return SubgraphTickFetcher(client, logger=self.logger)

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

    async def run_websocket(
        self,
        active_markets: list[MarketRecord],
        run_options: PipelineRunOptions | None = None,
    ) -> None:
        # Start RTDS first and let it warm up so the spot price cache is
        # populated before the first WS trade events arrive.
        rtds_task = asyncio.create_task(self.rtds_stream_phase.run())
        await asyncio.sleep(2)
        self.logger.info(
            "RTDS warm-up complete (%d symbols cached)",
            len(self._spot_price_cache),
        )
        loop = asyncio.get_running_loop()
        try:
            while True:
                if not active_markets:
                    self.logger.warning(
                        "No active markets for WebSocket — retrying in 60s"
                    )
                    await asyncio.sleep(60)
                    active_markets = await self._refresh_markets(loop, run_options)
                    continue

                ws_task = asyncio.create_task(
                    self.websocket_phase.run(active_markets)
                )
                # Run WS until the refresh interval elapses (or the task
                # exits early, which shouldn't happen in normal operation).
                done, _ = await asyncio.wait(
                    {ws_task}, timeout=WS_MARKET_REFRESH_INTERVAL_S,
                )
                if ws_task in done:
                    # WS phase exited on its own — propagate any exception
                    ws_task.result()
                    break

                # Timeout — cancel the current WS phase (triggers its
                # final-flush logic) and re-subscribe with fresh markets.
                ws_task.cancel()
                await asyncio.gather(ws_task, return_exceptions=True)

                self.logger.info("Refreshing active market subscriptions...")
                active_markets = await self._refresh_markets(loop, run_options)
                self.logger.info(
                    "Market refresh complete: %d active markets",
                    len(active_markets),
                )
        finally:
            rtds_task.cancel()
            await asyncio.gather(rtds_task, return_exceptions=True)

    async def _refresh_markets(
        self,
        loop: asyncio.AbstractEventLoop,
        run_options: PipelineRunOptions | None,
    ) -> list[MarketRecord]:
        """Re-fetch active markets from the Gamma API (runs in executor)."""
        if run_options is None:
            run_options = PipelineRunOptions(websocket_only=True)
        try:
            return await loop.run_in_executor(
                None, self._collect_active_markets, run_options, None,
            )
        except Exception as exc:
            self.logger.error(
                "Market refresh failed: %s — will retry next cycle", exc,
            )
            return []

    def _scan_checkpoint_path(self) -> str:
        return str(self.paths.scan_checkpoint_path())

    def _load_scan_checkpoint(self) -> int | None:
        path = self.paths.scan_checkpoint_path()
        try:
            with path.open() as handle:
                return int(handle.read().strip())
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            # Corrupt or unreadable checkpoint should not silently
            # trigger a 6-hour full historical re-scan.  Warn loudly so
            # operators see the cause when the next run takes longer
            # than expected.
            self.logger.warning(
                "Scan checkpoint at %s is unreadable (%s) — falling back to full scan",
                path, exc,
            )
            return None

    def _save_scan_checkpoint(self, max_end_ts: int) -> None:
        checkpoint_path = self.paths.scan_checkpoint_path()
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write: tmp file + os.replace.  A SIGKILL or power loss
        # mid-write used to truncate ``.scan_checkpoint`` to zero
        # length, which then made ``_load_scan_checkpoint`` silently
        # return None and force a full re-scan on the next run.
        import os as _os
        tmp_path = checkpoint_path.with_suffix(f".{_os.getpid()}.tmp")
        try:
            with tmp_path.open("w") as handle:
                handle.write(str(max_end_ts))
            _os.replace(tmp_path, checkpoint_path)
        except OSError:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            raise

    def _compute_scan_cutoff_ts(self, run_options: PipelineRunOptions) -> int | None:
        if run_options.is_test:
            return None

        checkpoint = self._load_scan_checkpoint()
        if checkpoint is not None:
            # Cap checkpoint to "now" — a corrupted checkpoint in the future
            # (e.g. from a market whose end_ts was months ahead) would make
            # the cutoff unreachable and suppress all backfill.
            now_ts = int(time.time())
            if checkpoint > now_ts:
                self.logger.warning(
                    "Scan checkpoint %s is in the future (now=%s) — capping to now",
                    checkpoint, now_ts,
                )
                checkpoint = now_ts
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
                scan_cutoff_ts = int(datetime.strptime(run_options.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
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
        max_closed_ts_seen = 0
        relevant_batch: list[MarketRecord] = []
        closed_markets_for_ticks: list[MarketRecord] = []

        # Optional upper bound from --to-date.  Paired with --from-date
        # this enables scanning a bounded historical window via Gamma's
        # server-side ``end_date_max`` filter, bypassing the 250K-offset
        # pagination cap.  Useful when restoring multi-month gaps.
        #
        # Treat ``--to-date 2026-04-25`` as midnight 2026-04-26 UTC
        # (i.e. *end* of the named day) so a market closing on the
        # named day is INCLUDED in the scan.  The earlier
        # ``midnight-of-the-day`` semantics excluded markets closing
        # exactly on the named day, which most operators do not expect.
        scan_cutoff_ts_max: int | None = None
        if run_options.to_date and not run_options.is_test:
            try:
                _to_date_dt = (
                    datetime.strptime(run_options.to_date, "%Y-%m-%d")
                    .replace(tzinfo=timezone.utc)
                    + timedelta(days=1)
                )
                scan_cutoff_ts_max = int(_to_date_dt.timestamp())
            except ValueError:
                self.logger.warning(
                    "Invalid --to-date value '%s'; ignoring upper bound",
                    run_options.to_date,
                )

        for market in self.api.fetch_markets(
            closed=True,
            end_ts_min=scan_cutoff_ts if not run_options.is_test else None,
            end_ts_max=scan_cutoff_ts_max,
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

            # Track closed_ts (not end_ts) for the checkpoint.  The scan
            # sorts by closedTime DESC and the relevance check compares
            # closed_ts against the cutoff.  Using end_ts was wrong because
            # early-resolved markets (e.g. culture markets) can have end_ts
            # months in the future, inflating the checkpoint and causing all
            # subsequent runs to find zero relevant markets.
            checkpoint_ts = market.closed_ts if market.closed_ts is not None else market.end_ts
            if checkpoint_ts > max_closed_ts_seen:
                max_closed_ts_seen = checkpoint_ts

            if not run_options.is_test and fetched_closed % 500 == 0 and max_closed_ts_seen > 0:
                self._save_scan_checkpoint(max_closed_ts_seen)

            if self._is_market_relevant(market, run_options, scan_cutoff_ts):
                # Respect --to-date upper bound client-side as well, in
                # case Gamma's ``end_date_max`` filter honours a slightly
                # different date field.  Compare against ``end_ts`` —
                # the same field Gamma's ``end_date_max`` filters on —
                # rather than ``closed_ts`` (which can drift for
                # early-resolved markets).
                if scan_cutoff_ts_max is not None and market.end_ts >= scan_cutoff_ts_max:
                    continue
                relevant_batch.append(market)
                test_collected += 1
                closed_markets_for_ticks.append(market)
                if len(relevant_batch) >= self._MARKET_BATCH_WORKERS:
                    self.price_history_phase.process_market_batch(relevant_batch)
                    relevant_batch = []

        if relevant_batch:
            self.price_history_phase.process_market_batch(relevant_batch)

        if max_closed_ts_seen > 0 and not run_options.is_test:
            self._save_scan_checkpoint(max_closed_ts_seen)
            self.logger.info("Scan checkpoint saved (max closed_ts: %s)", max_closed_ts_seen)

        return closed_markets_for_ticks

    def _collect_active_markets(
        self,
        run_options: PipelineRunOptions,
        scan_cutoff_ts: int | None,
    ) -> list[MarketRecord]:
        self.logger.info("Fetching and processing active markets...")
        fetched_active = 0
        skipped_expired = 0
        active_batch: list[MarketRecord] = []
        active_markets_for_ws: list[MarketRecord] = []
        now_ts = int(time.time())

        for market in self.api.fetch_markets(active=True):
            fetched_active += 1
            if fetched_active % 500 == 0:
                self.logger.info("Scanned %d active markets...", fetched_active)

            # Skip markets whose prediction window has already ended.
            # The Gamma API returns old markets as "active" long after expiry.
            if market.end_ts < now_ts:
                skipped_expired += 1
                continue

            if self._is_market_relevant(market, run_options, scan_cutoff_ts):
                active_markets_for_ws.append(market)
                if not run_options.websocket_only:
                    active_batch.append(market)
                    if len(active_batch) >= self._MARKET_BATCH_WORKERS:
                        self.price_history_phase.process_market_batch(active_batch)
                        active_batch = []

        if not run_options.websocket_only and active_batch:
            self.price_history_phase.process_market_batch(active_batch)

        if skipped_expired:
            self.logger.info(
                "Skipped %d expired markets (end_ts < now) still marked active by Gamma API",
                skipped_expired,
            )

        # Best-effort fee rate fetch: one call for all crypto markets (same rate).
        # ``fetch_fee_rate_bps`` already swallows errors internally (returns
        # None on failure), so the previous outer ``try/except: pass`` was
        # redundant AND silently masked any unexpected failure.  Drop it
        # and surface the missing-fee case as a warning instead.
        crypto_markets = [m for m in active_markets_for_ws if m.category == "crypto" and m.up_token_id]
        if crypto_markets:
            bps = self.api.fetch_fee_rate_bps(crypto_markets[0].up_token_id)
            if bps is not None:
                for m in crypto_markets:
                    m.fee_rate_bps = bps
                self.logger.info("Fee rate for crypto markets: %d bps", bps)
            else:
                self.logger.warning(
                    "Could not fetch fee rate for crypto markets (CLOB /fee-rate "
                    "returned None or failed); fee_rate_bps will remain unset."
                )

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
            # Never apply scan cutoff to active markets — the cutoff is for
            # incremental historical scans only.  Active markets have future
            # end_ts values but may have old closed_ts from prior resolution
            # cycles, which the cutoff would incorrectly filter out.
            active_markets_for_ws = self._collect_active_markets(options, None)

        if not options.websocket_only:
            self.price_history_phase.flush_all()

        if not options.websocket_only:
            # Fetch Binance historical spot prices for all processed markets.
            # This populates the spot_prices table AND provides a lookup for
            # embedding prices into historical on-chain ticks.
            all_markets_for_history = closed_markets_for_ticks + active_markets_for_ws
            if all_markets_for_history:
                self.logger.info(
                    "Fetching Binance historical spot prices for %s markets...",
                    len(all_markets_for_history),
                )
                spot_lookup = self.binance_history_phase.run(
                    all_markets_for_history,
                    spot_prices_dir=str(self.paths.spot_prices_dir),
                )
            else:
                spot_lookup = None

            if self.tick_backfill_phase.is_enabled:
                self.price_history_phase.clear_cache()
                gc.collect()
                if all_markets_for_history:
                    # Pass spot price lookup so on-chain ticks get spot_price_usdt embedded
                    if spot_lookup is not None and self.tick_provider is not None:
                        self.tick_provider.spot_price_lookup = spot_lookup
                    self.logger.info(
                        "Starting on-chain tick backfill for %s markets...",
                        len(all_markets_for_history),
                    )
                    total_ticks = self.run_historical_tick_backfill(all_markets_for_history)
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
                # 1. Upload main crypto dataset (including spot prices + orderbook)
                upload_to_huggingface(
                    repo_id=self.settings.hf_repo,
                    markets_path=self._parquet_markets_path,
                    prices_dir=self._parquet_prices_dir,
                    ticks_dir=self._parquet_ticks_dir,
                    spot_prices_dir=str(self.paths.spot_prices_dir),
                    orderbook_dir=str(self.paths.orderbook_dir),
                    heartbeats_dir=str(self.paths.heartbeats_dir),
                    logger=self.logger,
                    skip_consolidate=True,
                )
                
                # 2. Upload culture dataset (if it exists)
                from .config import HF_CULTURE_REPO_ID
                culture_root = self.paths.data_dir.parent / "data-culture"
                if culture_root.exists():
                    self.logger.info("Uploading culture dataset to Hugging Face...")
                    culture_prices_dir = str(culture_root / "prices")
                    if os.path.exists(culture_prices_dir):
                        consolidate_culture_prices(prices_dir=culture_prices_dir, logger=self.logger)
                    upload_to_huggingface(
                        repo_id=os.environ.get("HF_CULTURE_REPO_ID", HF_CULTURE_REPO_ID),
                        markets_path=str(culture_root / "markets.parquet"),
                        prices_dir=culture_prices_dir,
                        ticks_dir=str(culture_root / "ticks"),
                        logger=self.logger,
                        skip_consolidate=False,
                    )
            except Exception as exc:
                self.logger.error("Hugging Face upload failed (non-fatal): %s", exc)
                self.logger.info("Continuing despite upload failure.")

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
                asyncio.run(self.run_websocket(active_markets_for_ws, options))
            except KeyboardInterrupt:
                self.logger.info("Stopped by user")
            self.print_summary()
        else:
            self.logger.info("No active markets matched the filters for real-time monitoring")
            self.print_summary()
