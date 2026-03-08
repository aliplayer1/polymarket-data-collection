import asyncio
import json
import logging
import os
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    WS_BUFFER_MAX_ROWS,
    WS_FLUSH_BATCH_SIZE,
    WS_FLUSH_INTERVAL_SECONDS,
    WS_MAX_TOKENS_PER_SHARD,
    WS_URL,
)
from .models import MarketRecord
from .retry import api_call_with_retry
from .storage import (
    append_ticks_only,
    append_ws_ticks_staged,
    consolidate_ticks,
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

        Note: we do NOT clip by market.start_ts because Polymarket's Gamma API
        sets startDate unreliably close to endDate for recently-closed markets
        (sometimes only 30–100 s before), which would collapse the fetch window
        to near-zero and return 0 price rows from the CLOB API.
        """
        window_seconds = TIMEFRAME_SECONDS.get(market.timeframe, 300)
        # Always fetch the last [window_seconds] before resolution.
        prediction_start = market.end_ts - window_seconds

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

    # Minimum fetch window (seconds) worth making an API call for.
    # Windows shorter than this yield 0 rows from the CLOB API;
    # skipping them avoids thousands of pointless HTTP round-trips
    # during incremental re-scans of already-fetched markets.
    _MIN_FETCH_WINDOW_SECONDS = 60

    def build_market_dataframe(self, market: MarketRecord) -> pd.DataFrame | None:
        start_ts = self._market_start_ts(market)
        if start_ts >= market.end_ts:
            return None

        # Skip tiny fetch windows — they return 0 rows and waste API calls
        if (market.end_ts - start_ts) < self._MIN_FETCH_WINDOW_SECONDS:
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
        merged_prices = merged_prices.fillna({"price_1": 0.5, "price_2": 0.5})
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

    def persist_dataframe(self, timeframe: str, df: pd.DataFrame, *, update_cache: bool = True) -> pd.DataFrame:
        """Persist a flat DataFrame to normalised Parquet (markets + prices).

        Parameters
        ----------
        update_cache:
            When True (default) the in-memory ``existing_dfs`` cache is updated
            so incremental fetch decisions stay current.  Pass False from the
            WebSocket flush path — WebSocket streaming never calls
            ``_market_start_ts()``, so the cache is dead weight that would grow
            unboundedly with every 5-second flush.

        When ``update_cache=False`` (WebSocket flush path) the markets table
        write is also skipped (``skip_markets=True``): market metadata is
        already up-to-date from the startup historical scan, and re-writing
        ``markets.parquet`` on every 5-second flush cycle is wasteful I/O.
        """
        df = df.assign(market_id=lambda d: d["market_id"].astype(str))
        markets_df, prices_df = split_markets_prices(df)
        persist_normalized(
            markets_df,
            prices_df,
            markets_path=self._parquet_markets_path,
            prices_dir=self._parquet_prices_dir,
            logger=self.logger,
            skip_markets=not update_cache,
        )

        if not update_cache:
            return prices_df

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

        # Group markets by their prediction-window (start_ts, end_ts).
        # BTC/ETH/SOL markets at the same time slot share identical block ranges,
        # so one eth_getLogs fetch covers all three instead of three separate calls.
        windows: dict[tuple[int, int], list[MarketRecord]] = defaultdict(list)
        for m in markets:
            window_s = TIMEFRAME_SECONDS.get(m.timeframe, 300)
            win_start = max(m.start_ts, m.end_ts - window_s)
            windows[(win_start, m.end_ts)].append(m)

        # Flush accumulated ticks to Parquet every this many windows.
        # Keep low to limit peak memory: each pending window can hold
        # thousands of tick dicts, and persist_ticks() loads the existing
        # partition into RAM for merge.
        _TICK_FLUSH_EVERY = 5

        total_ticks = 0
        n_windows = len(windows)
        pending_ticks: list[dict] = []

        for i, ((win_start, win_end), window_markets) in enumerate(windows.items(), 1):
            self.logger.info(
                "[%d/%d] Fetching on-chain ticks for window %s–%s (%s)",
                i, n_windows, win_start, win_end,
                ", ".join(f"{m.crypto}/{m.timeframe}" for m in window_markets),
            )
            try:
                batch = self.tick_fetcher.get_ticks_for_markets_batch(
                    window_markets, win_start, win_end,
                )
                for m in window_markets:
                    ticks = batch.get(m.market_id, [])
                    if not ticks:
                        continue
                    for t in ticks:
                        t["source"] = "onchain"
                    pending_ticks.extend(ticks)
                    total_ticks += len(ticks)
                    self.logger.info("  -> %s %s: %s fills", m.crypto, m.timeframe, len(ticks))
            except Exception as exc:
                self.logger.error(
                    "Failed on-chain tick fetch for window %s–%s: %s", win_start, win_end, exc,
                )

            if pending_ticks and (i % _TICK_FLUSH_EVERY == 0 or i == n_windows):
                ticks_df = pd.DataFrame(pending_ticks)
                append_ticks_only(
                    ticks_df,
                    ticks_dir=self._parquet_ticks_dir,
                    logger=self.logger,
                )
                self.logger.info(
                    "Flushed %d ticks to disk (window %d/%d)", len(pending_ticks), i, n_windows,
                )
                pending_ticks = []

        self.logger.info("On-chain tick backfill complete: %s total ticks across %s markets", total_ticks, len(markets))
        return total_ticks

    def _initial_last_prices(self, active_markets: list[MarketRecord]) -> dict[str, dict[str, float]]:
        """Seed last-known prices for every active market.

        Uses the in-memory price cache as the fast path (populated by
        ``load_existing_data``).  Falls back to the CLOB API for markets not
        in the cache.  CLOB lookups are parallelised with a ThreadPoolExecutor
        to avoid the sequential ~3 000-call startup delay that previously made
        ``--websocket-only`` mode take 10–30 minutes to begin streaming.
        """
        last_prices: dict[str, dict[str, float]] = {}
        needs_api: list[MarketRecord] = []

        def _parse_price(raw: Any) -> float:
            if isinstance(raw, dict):
                return float(raw.get("price") or raw.get("value") or 0.5)
            return float(raw)

        # Fast path: populate from in-memory cache (populated by load_existing_data)
        for market in active_markets:
            df = self.existing_dfs.get(market.timeframe, pd.DataFrame())
            market_df = df[df["market_id"] == market.market_id] if not df.empty else pd.DataFrame()
            if not market_df.empty:
                row = market_df.iloc[-1]
                last_prices[market.market_id] = {
                    "up": float(row["up_price"]),
                    "down": float(row["down_price"]),
                }
            else:
                needs_api.append(market)

        if not needs_api:
            return last_prices

        self.logger.info(
            "Fetching initial prices for %d markets from CLOB API (parallel)...",
            len(needs_api),
        )

        def _fetch_one(market: MarketRecord) -> tuple[str, dict[str, float]]:
            try:
                up_raw   = api_call_with_retry(self.client.get_last_trade_price, market.up_token_id,   logger=self.logger) or 0.5
                down_raw = api_call_with_retry(self.client.get_last_trade_price, market.down_token_id, logger=self.logger) or 0.5
                return market.market_id, {"up": _parse_price(up_raw), "down": _parse_price(down_raw)}
            except Exception as exc:
                self.logger.warning("Falling back to 0.5 for market %s: %s", market.market_id, exc)
                return market.market_id, {"up": 0.5, "down": 0.5}

        # Cap concurrency at 20 to stay well within CLOB rate limits.
        with ThreadPoolExecutor(max_workers=min(20, len(needs_api))) as executor:
            for mid, prices in executor.map(_fetch_one, needs_api):
                last_prices[mid] = prices

        return last_prices

    def _flush_snapshot(
        self,
        ws_snapshot: dict[str, list[dict[str, Any]]],
        tck_snapshot: dict[str, list[dict[str, Any]]],
    ) -> int:
        """Flush pre-snapshotted buffer dicts to Parquet (runs in a thread).

        Callers must have already swapped the live buffers for fresh empty
        ones before invoking this, so shard tasks can keep appending without
        any lock contention.
        """
        flushed_rows = 0
        for timeframe, rows in ws_snapshot.items():
            if not rows:
                continue
            new_df = pd.DataFrame(rows)
            # update_cache=False → skip_markets=True inside persist_dataframe:
            #   Market metadata is already written during the startup historical
            #   scan; re-writing it on every flush cycle is wasteful I/O.
            self.persist_dataframe(timeframe, new_df, update_cache=False)
            flushed_rows += len(rows)

        all_ticks: list[dict[str, Any]] = []
        for rows in tck_snapshot.values():
            all_ticks.extend(rows)
        if all_ticks:
            ticks_df = pd.DataFrame(all_ticks)
            # Use the staging writer — never reads the main consolidated partition.
            # The next --historical-only pass absorbs the staging file via persist_ticks().
            append_ws_ticks_staged(ticks_df, ticks_dir=self._parquet_ticks_dir, logger=self.logger)

        return flushed_rows

    async def _run_ws_shard(
        self,
        shard_token_ids: list[str],
        shard_idx: int,
        n_shards: int,
        token_to_market: dict[str, tuple[MarketRecord, str]],
        last_prices: dict[str, dict[str, float]],
        ws_buffer: dict[str, list[dict[str, Any]]],
        tick_buffer: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Run one WebSocket connection for a slice of token IDs.

        Reconnects on any error with exponential back-off.  Raises
        CancelledError when the task is cancelled by run_websocket so the
        caller can do a final flush.

        Fix #6: tracks disconnect_time so the gap duration is logged on
        each reconnect, making data gaps visible in the logs.
        Fix #9: exception type and WebSocket close code are logged separately
        so shard disconnects can be diagnosed remotely without ambiguity.
        """
        reconnect_attempts = 0
        disconnect_time: float | None = None

        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_size=None) as ws:
                    await ws.send(json.dumps({
                        "assets_ids": shard_token_ids,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }))
                    connect_time = time.time()
                    if disconnect_time is not None:
                        gap_s = connect_time - disconnect_time
                        self.logger.info(
                            "WS shard %d/%d: reconnected after %.1fs gap — price data "
                            "for this shard's markets will be backfilled by the next "
                            "historical scan.",
                            shard_idx + 1, n_shards, gap_s,
                        )
                    self.logger.info(
                        "WS shard %d/%d: subscribed to %d tokens",
                        shard_idx + 1, n_shards, len(shard_token_ids),
                    )
                    reconnect_attempts = 0
                    disconnect_time = None

                    while True:
                        message = await ws.recv()
                        payload = json.loads(message)
                        events = payload if isinstance(payload, list) else [payload]

                        # Guard against unbounded buffer growth if the flush loop
                        # falls behind (slow disk / lock contention).  Check once
                        # per message batch, not per event, to avoid O(n) overhead.
                        total_buffered = (
                            sum(len(v) for v in ws_buffer.values())
                            + sum(len(v) for v in tick_buffer.values())
                        )
                        if total_buffered >= WS_BUFFER_MAX_ROWS:
                            for _buf in (ws_buffer, tick_buffer):
                                for _tf in list(_buf.keys()):
                                    _rows = _buf[_tf]
                                    if _rows:
                                        _buf[_tf] = _rows[len(_rows) // 2:]
                            self.logger.error(
                                "WS shard %d/%d: buffer at capacity (%d rows) — "
                                "evicted oldest half; flush loop may be stuck",
                                shard_idx + 1, n_shards, total_buffered,
                            )

                        for event in events:
                            if event.get("event_type") != "last_trade_price":
                                continue

                            token_id = str(event.get("asset_id", ""))
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

                            trade_side = str(event.get("side") or event.get("type_side") or "").upper()
                            if trade_side not in ("BUY", "SELL"):
                                trade_side = "BUY"

                            size_shares = float(event.get("size") or event.get("amount") or 0)
                            size_usdc   = round(size_shares * price, 6) if size_shares > 0 else 0.0

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
                                    "block_number": 0,
                                    "log_index":    0,
                                    "source":       "websocket",
                                }
                            )

            except (asyncio.CancelledError, KeyboardInterrupt):
                raise
            except websockets.exceptions.ConnectionClosed as exc:
                # Record disconnect time for gap tracking on next reconnect.
                disconnect_time = time.time()
                reconnect_delay = min(10 * (2 ** reconnect_attempts), MAX_WS_RECONNECT_DELAY_SECONDS)
                reconnect_attempts += 1
                self.logger.warning(
                    "WS shard %d/%d: connection closed (code=%s reason=%r). "
                    "Reconnecting in %ss...",
                    shard_idx + 1, n_shards,
                    getattr(exc, "code", "?"), getattr(exc, "reason", ""),
                    reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)
            except Exception as exc:
                disconnect_time = time.time()
                reconnect_delay = min(10 * (2 ** reconnect_attempts), MAX_WS_RECONNECT_DELAY_SECONDS)
                reconnect_attempts += 1
                self.logger.warning(
                    "WS shard %d/%d: %s — %s. Reconnecting in %ss...",
                    shard_idx + 1, n_shards, type(exc).__name__, exc, reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)

    async def _ws_flush_loop(
        self,
        ws_buffer: dict[str, list[dict[str, Any]]],
        tick_buffer: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Periodically flush WebSocket buffers to Parquet.

        Fixes #1 (blocking I/O) and #3 (unused WS_FLUSH_BATCH_SIZE):

        • Parquet writes run in a ThreadPoolExecutor via run_in_executor so
          the asyncio event loop is never blocked.  The ping/pong machinery
          and ws.recv() calls in every shard continue processing throughout.
        • Buffers are swapped atomically before the thread is dispatched, so
          shard tasks can append without any contention.
        • Flush fires on time (WS_FLUSH_INTERVAL_SECONDS) *or* size
          (WS_FLUSH_BATCH_SIZE rows), whichever comes first.
        """
        loop = asyncio.get_running_loop()
        _CHECK_INTERVAL = 1.0   # seconds between threshold checks
        last_flush_time = loop.time()

        while True:
            await asyncio.sleep(_CHECK_INTERVAL)

            total_ws_rows = sum(len(v) for v in ws_buffer.values())
            elapsed = loop.time() - last_flush_time

            # Skip if neither threshold is met yet
            if total_ws_rows == 0 and elapsed < WS_FLUSH_INTERVAL_SECONDS:
                continue
            if total_ws_rows < WS_FLUSH_BATCH_SIZE and elapsed < WS_FLUSH_INTERVAL_SECONDS:
                continue

            # Atomic buffer swap — no `await` between snapshot and clear so no
            # shard task can interleave.  defaultdict creates a fresh list on the
            # next append after the key is reset.
            ws_snap: dict[str, list] = {}
            tck_snap: dict[str, list] = {}
            for tf in list(ws_buffer.keys()):
                if ws_buffer[tf]:
                    ws_snap[tf] = ws_buffer[tf]
                    ws_buffer[tf] = []
            for tf in list(tick_buffer.keys()):
                if tick_buffer[tf]:
                    tck_snap[tf] = tick_buffer[tf]
                    tick_buffer[tf] = []

            if not ws_snap and not tck_snap:
                last_flush_time = loop.time()
                continue

            last_flush_time = loop.time()
            # Blocking Parquet I/O runs in a thread; event loop stays free.
            flushed = await loop.run_in_executor(None, self._flush_snapshot, ws_snap, tck_snap)
            if flushed:
                self.logger.info("Flushed %d WebSocket rows to disk", flushed)

    async def run_websocket(self, active_markets: list[MarketRecord]) -> None:
        if not active_markets:
            return

        token_to_market: dict[str, tuple[MarketRecord, str]] = {}
        token_ids: list[str] = []
        for market in active_markets:
            token_to_market[market.up_token_id] = (market, "up")
            token_to_market[market.down_token_id] = (market, "down")
            token_ids.append(market.up_token_id)
            token_ids.append(market.down_token_id)

        if not token_ids:
            self.logger.warning("No token IDs available for active markets; skipping WebSocket stream")
            return

        last_prices = self._initial_last_prices(active_markets)
        ws_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
        tick_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)

        # Fix #4: shuffle token IDs before sharding so that high-volume tokens
        # (which arrive first because fetch_markets sorts by volume24hr DESC)
        # are spread evenly across shards rather than concentrated in shards 0
        # and 1.  Without shuffling those shards receive far more events/second,
        # causing disproportionate server-side pressure and repeated disconnects.
        shuffled_token_ids = token_ids[:]
        random.shuffle(shuffled_token_ids)

        # Split tokens into shards so each subscription message stays well
        # below the ~200 KB threshold that causes Polymarket's server to drop
        # connections every 30-90 s.  Each shard runs as an independent asyncio
        # task with its own reconnect loop, so only 1/N shards go dark on any
        # single disconnect rather than all tokens simultaneously.
        shards = [
            shuffled_token_ids[i : i + WS_MAX_TOKENS_PER_SHARD]
            for i in range(0, len(shuffled_token_ids), WS_MAX_TOKENS_PER_SHARD)
        ]
        n_shards = len(shards)
        self.logger.info(
            "Starting WebSocket stream: %d active markets → %d tokens across %d shard(s) "
            "(≤%d tokens/shard)",
            len(active_markets), len(token_ids), n_shards, WS_MAX_TOKENS_PER_SHARD,
        )

        shard_tasks = [
            asyncio.create_task(
                self._run_ws_shard(shard, idx, n_shards, token_to_market, last_prices, ws_buffer, tick_buffer)
            )
            for idx, shard in enumerate(shards)
        ]
        flush_task = asyncio.create_task(self._ws_flush_loop(ws_buffer, tick_buffer))
        all_tasks = [flush_task, *shard_tasks]

        try:
            await asyncio.gather(*all_tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for task in all_tasks:
                task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)
            # Final synchronous flush — all tasks are done so there is no
            # concurrent appending; no need for run_in_executor here.
            final_ws  = {tf: rows for tf, rows in ws_buffer.items()   if rows}
            final_tck = {tf: rows for tf, rows in tick_buffer.items() if rows}
            if final_ws or final_tck:
                flushed = self._flush_snapshot(final_ws, final_tck)
                if flushed:
                    self.logger.info("Final flush on shutdown: %d rows", flushed)

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
        websocket_only: bool = False,
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

        # In websocket-only mode skip loading historical price data — we only
        # need last prices for the initial WS seed, which fall back to the CLOB
        # API.  Loading the full history wastes hundreds of MB of RAM.
        if not websocket_only:
            self.load_existing_data()

        # Compute scan cutoff for incremental runs.
        # After a successful scan the checkpoint file holds the max end_ts
        # seen.  Subsequent runs only need to scan markets that closed after
        # (checkpoint - 2 days), skipping the thousands of already-seen pages.
        # Priority: checkpoint > --from-date > full scan (no cutoff).
        scan_cutoff_ts: int | None = None
        if not is_test:
            checkpoint = self._load_scan_checkpoint()
            if checkpoint is not None:
                scan_cutoff_ts = checkpoint - 2 * 86400  # 2-day lookback buffer
                cutoff_date = datetime.fromtimestamp(scan_cutoff_ts, tz=timezone.utc).strftime("%Y-%m-%d")
                self.logger.info(
                    "Incremental scan: re-checking markets from %s onwards (checkpoint=%s)",
                    cutoff_date, checkpoint,
                )
            elif from_date:
                try:
                    scan_cutoff_ts = int(datetime.strptime(from_date, "%Y-%m-%d").timestamp())
                    self.logger.info("No checkpoint found; scan limited to markets closing on or after %s", from_date)
                except ValueError:
                    self.logger.warning("Invalid --from-date value '%s'; scanning all markets", from_date)
            else:
                self.logger.info("No checkpoint and no --from-date; scanning all closed markets")

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
            if scan_cutoff_ts:
                # Prefer closed_ts (what the scan checkpoint tracks) over end_ts
                # to keep the filter consistent with the pager stop-condition in
                # api.py, which also uses closedTime.  Fall back to end_ts when
                # closed_ts is unavailable (e.g. active markets).
                market_ts = m.closed_ts if m.closed_ts is not None else m.end_ts
                if market_ts < scan_cutoff_ts:
                    return False
            return True

        # Number of markets whose price histories are fetched concurrently.
        # Each market makes 2 CLOB API calls (one per token) so this results in
        # 2 * _WORKERS concurrent HTTP requests.  Keep conservative to avoid
        # overwhelming the CLOB endpoint.
        _WORKERS = 5

        pending_dfs: dict[str, list[pd.DataFrame]] = {tf: [] for tf in TIME_FRAMES}
        self.processed_count = 0

        def _flush_if_needed() -> None:
            for tf in list(pending_dfs.keys()):
                if len(pending_dfs[tf]) >= 10:
                    combined = pd.concat(pending_dfs[tf], ignore_index=True)
                    merged = self.persist_dataframe(tf, combined)
                    self.logger.info("Flushed batch for %s (%s total rows)", tf, len(merged))
                    pending_dfs[tf] = []

        def process_market_batch(batch: list[MarketRecord]) -> None:
            """Fetch price histories for *batch* concurrently, then persist serially."""
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                future_to_market = {
                    executor.submit(self.build_market_dataframe, m): m for m in batch
                }
                for future in as_completed(future_to_market):
                    m = future_to_market[future]
                    self.processed_count += 1
                    if self.processed_count % 10 == 0:
                        self.logger.info("Processed %d relevant markets so far...", self.processed_count)
                    try:
                        market_df = future.result()
                        if market_df is not None and not market_df.empty:
                            pending_dfs[m.timeframe].append(market_df)
                            self.logger.info("  -> Fetched %s rows", len(market_df))
                    except Exception as exc:
                        self.logger.error("Failed to process market %s: %s", m.market_id, exc)
            _flush_if_needed()

        def flush_all():
            for tf, dfs in pending_dfs.items():
                if dfs:
                    combined = pd.concat(dfs, ignore_index=True)
                    merged = self.persist_dataframe(tf, combined)
                    self.logger.info("Final flush for %s (%s total rows)", tf, len(merged))
                    pending_dfs[tf] = []

        # 1. Process Closed Markets (Streamed) — skipped in websocket-only mode
        closed_markets_for_ticks: list[MarketRecord] = []
        if websocket_only:
            self.logger.info("WebSocket-only mode: skipping historical scan.")
        else:
            self.logger.info("Fetching and processing closed markets...")
            fetched_closed = 0
            test_collected = 0
            max_end_ts_seen = 0
            relevant_batch: list[MarketRecord] = []

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

                # Save checkpoint periodically so progress survives SIGTERM.
                if not is_test and fetched_closed % 500 == 0 and max_end_ts_seen > 0:
                    self._save_scan_checkpoint(max_end_ts_seen)

                if is_market_relevant(m):
                    relevant_batch.append(m)
                    test_collected += 1
                    closed_markets_for_ticks.append(m)
                    if len(relevant_batch) >= _WORKERS:
                        process_market_batch(relevant_batch)
                        relevant_batch = []

            if relevant_batch:
                process_market_batch(relevant_batch)

            # Save checkpoint so the next run can skip already-scanned history.
            if max_end_ts_seen > 0 and not is_test:
                self._save_scan_checkpoint(max_end_ts_seen)
                self.logger.info("Scan checkpoint saved (max end_ts: %s)", max_end_ts_seen)

        # 2. Process Active Markets (Streamed & Collected) — skipped in test/historical-only mode
        active_markets_for_ws = []
        if is_test:
            self.logger.info("Test mode: skipping active markets and WebSocket streaming.")
        elif historical_only:
            self.logger.info("Historical-only mode enabled. Skipping active markets.")
        else:
            self.logger.info("Fetching and processing active markets...")
            fetched_active = 0
            active_batch: list[MarketRecord] = []
            for m in self.api.fetch_markets(active=True):
                fetched_active += 1
                if fetched_active % 500 == 0:
                    self.logger.info("Scanned %d active markets...", fetched_active)

                if is_market_relevant(m):
                    active_markets_for_ws.append(m)
                    if not websocket_only:
                        # In websocket-only mode skip price history fetch; initial
                        # prices come from existing Parquet or CLOB last-trade fallback.
                        active_batch.append(m)
                        if len(active_batch) >= _WORKERS:
                            process_market_batch(active_batch)
                            active_batch = []

            if not websocket_only and active_batch:
                process_market_batch(active_batch)

        # Flush any remaining historical data
        if not websocket_only:
            flush_all()

        # On-chain historical tick backfill — skipped in websocket-only mode
        if not websocket_only and self.tick_fetcher is not None:
            # Free cached price DataFrames before tick backfill — they are no
            # longer needed and can consume hundreds of MB of RAM.
            self.existing_dfs.clear()
            import gc; gc.collect()
            all_markets_for_ticks = closed_markets_for_ticks + active_markets_for_ws
            if all_markets_for_ticks:
                self.logger.info(
                    "Starting on-chain tick backfill for %s markets...",
                    len(all_markets_for_ticks),
                )
                total_ticks = self.run_historical_tick_backfill(all_markets_for_ticks)
                self.logger.info("Tick backfill complete: %s fills stored.", total_ticks)
                # Consolidate shard files one partition at a time (memory-safe)
                self.logger.info("Consolidating tick shard files...")
                consolidate_ticks(
                    ticks_dir=self._parquet_ticks_dir,
                    logger=self.logger,
                )
            else:
                self.logger.info("No markets matched for tick backfill.")

        # Upload to Hugging Face Hub after historical phase, before WebSocket.
        # Must happen here — the WebSocket loop below never exits, so any code
        # placed after it would never run.
        if upload and is_test:
            self.logger.warning(
                "--upload is ignored in test mode (test data is never pushed to Hugging Face). "
                "Re-run without --test to upload production data."
            )
        elif upload and websocket_only:
            self.logger.info("WebSocket-only mode: skipping Hugging Face upload.")
        elif upload:
            self.logger.info("Uploading dataset to Hugging Face Hub...")
            try:
                upload_to_huggingface(
                    repo_id=hf_repo,
                    markets_path=self._parquet_markets_path,
                    prices_dir=self._parquet_prices_dir,
                    ticks_dir=self._parquet_ticks_dir,
                    logger=self.logger,
                    # consolidate_ticks already ran above — skip redundant re-scan
                    skip_consolidate=True,
                )
            except Exception as exc:
                self.logger.error("Hugging Face upload failed (non-fatal): %s", exc)
                self.logger.info("Continuing to WebSocket streaming despite upload failure.")

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
