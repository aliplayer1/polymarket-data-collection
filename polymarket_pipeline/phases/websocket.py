from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd
import websockets

from ..alerts import fire_startup_once, send_alert_async
from ..config import (
    WS_DROP_ALERT_THRESHOLD,
    WS_FLUSH_BATCH_SIZE,
    WS_FLUSH_INTERVAL_SECONDS,
    WS_HEARTBEAT_INTERVAL_S,
    WS_MAX_TOKENS_PER_SHARD,
    WS_OB_BUFFER_MAX,
    WS_OB_FLUSH_INTERVAL_S,
    WS_PRICE_BUFFER_MAX,
    WS_RECONNECT_BURST_THRESHOLD,
    WS_RECONNECT_BURST_WINDOW_S,
    WS_SPOT_BUFFER_MAX,
    WS_STALENESS_CLOB_PRICE_CHANGE_S,
    WS_TICK_BUFFER_MAX,
    WS_URL,
    WS_WATCHDOG_CHECK_INTERVAL_S,
    WS_WATCHDOG_GRACE_PERIOD_S,
)
from ..models import MarketRecord
from ..providers import LastTradePriceProvider
from ..retry import api_call_with_retry
from ..storage import (
    append_ws_culture_prices_staged,
    append_ws_heartbeats_staged,
    append_ws_orderbook_staged,
    append_ws_prices_staged,
    append_ws_spot_prices_staged,
    append_ws_ticks_staged,
)
from .rtds_stream import CRYPTO_TO_RTDS_SYMBOL
from .shared import PipelinePaths, build_binary_price_row, build_binary_tick_row, build_orderbook_row
from .ws_messages import (
    clob_subscribe_payload,
    is_valid_bbo,
    parse_book_snapshot,
    parse_last_trade_price,
    parse_price_change,
)
from .ws_watchdog import (
    DataHeartbeat,
    DropOldestBuffer,
    ReconnectRateMonitor,
    configure_tcp_socket,
    jittered_backoff,
)


# Dedicated logger for reconnect events — Task 8 installs a RotatingFileHandler
# on this name to produce a queryable jsonl of reconnect history.
_RECONNECT_LOGGER = logging.getLogger("polymarket_pipeline.ws_reconnects")


class _WatchdogStale(Exception):
    """Raised by the per-session watchdog to force a reconnect."""


class WebSocketPhase:
    def __init__(
        self,
        last_trade_price_provider: LastTradePriceProvider,
        price_history_phase,
        *,
        logger: logging.Logger,
        paths: PipelinePaths,
        spot_price_cache: dict[str, tuple[float, int]] | None = None,
        spot_price_buffer: DropOldestBuffer | None = None,
        heartbeat_registry: dict[str, DataHeartbeat] | None = None,
    ) -> None:
        self.last_trade_price_provider = last_trade_price_provider
        self.price_history_phase = price_history_phase
        self.logger = logger
        self.paths = paths
        self.spot_price_cache = spot_price_cache if spot_price_cache is not None else {}
        self.spot_price_buffer: DropOldestBuffer = (
            spot_price_buffer if spot_price_buffer is not None
            else DropOldestBuffer(WS_SPOT_BUFFER_MAX)
        )

        # Shared with RTDSStreamPhase: flush loop emits heartbeat rows for
        # every registered key so downstream gap-scanning covers the full
        # WS surface area, not just CLOB shards.
        self._heartbeats: dict[str, DataHeartbeat] = (
            heartbeat_registry if heartbeat_registry is not None else {}
        )

        # Track drop counts observed at last flush so alert thresholds
        # measure "drops since last successful flush" not cumulative totals.
        self._last_drop_totals: dict[str, int] = {}
        # Burst-reconnect monitor fires a webhook when any shard rapidly
        # churns connections (Polymarket-side incident signature).
        self._reconnect_monitor = ReconnectRateMonitor(
            threshold=WS_RECONNECT_BURST_THRESHOLD,
            window_s=WS_RECONNECT_BURST_WINDOW_S,
        )
        # First-data flag used to fire the one-shot startup webhook.
        self._first_data_seen = False

    def update_paths(self, paths: PipelinePaths) -> None:
        self.paths = paths

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _spot_price_kwargs(self, crypto: str) -> dict[str, float | int | None]:
        rtds_symbol = CRYPTO_TO_RTDS_SYMBOL.get(crypto)
        if rtds_symbol is None:
            return {"spot_price_usdt": None, "spot_price_ts_ms": None}
        entry = self.spot_price_cache.get(rtds_symbol)
        if entry is None:
            return {"spot_price_usdt": None, "spot_price_ts_ms": None}
        return {"spot_price_usdt": entry[0], "spot_price_ts_ms": entry[1]}

    def _mark_first_data(self) -> None:
        if not self._first_data_seen:
            self._first_data_seen = True
            fire_startup_once("WS shard received first data")

    def _initial_last_prices(self, active_markets: list[MarketRecord]) -> dict[str, dict[str, float]]:
        last_prices: dict[str, dict[str, float]] = {}
        needs_api: list[MarketRecord] = []

        def _parse_price(raw: Any) -> float:
            if isinstance(raw, dict):
                return float(raw.get("price") or raw.get("value") or 0.5)
            return float(raw)

        for market in active_markets:
            cached_prices = self.price_history_phase.last_cached_prices(market)
            if cached_prices is not None:
                last_prices[market.market_id] = cached_prices
            else:
                needs_api.append(market)

        if not needs_api:
            return last_prices

        self.logger.info(
            "Fetching initial prices for %d markets from CLOB API (parallel)...",
            len(needs_api),
        )

        def _fetch_one(market: MarketRecord) -> tuple[str, dict[str, float]]:
            prices = {}
            for outcome, token_id in market.tokens.items():
                try:
                    raw = api_call_with_retry(
                        self.last_trade_price_provider.get_last_trade_price,
                        token_id,
                        logger=self.logger,
                    ) or 0.5
                    prices[outcome] = _parse_price(raw)
                except Exception as exc:
                    self.logger.warning(
                        "Falling back to 0.5 for market %s outcome %s: %s",
                        market.market_id, outcome, exc,
                    )
                    prices[outcome] = 0.5
            return market.market_id, prices

        with ThreadPoolExecutor(max_workers=min(5, len(needs_api))) as executor:
            for market_id, prices in executor.map(_fetch_one, needs_api):
                last_prices[market_id] = prices

        return last_prices

    def _flush_snapshot(
        self,
        ws_snapshot: dict[str, list[dict[str, Any]]],
        tick_snapshot: dict[str, list[dict[str, Any]]],
        orderbook_snapshot: dict[str, list[dict[str, Any]]],
        spot_price_snapshot: list[dict[str, Any]],
        heartbeat_snapshot: list[dict[str, Any]],
    ) -> int:
        """Flush all buffered data to disk using lock-free shard writes."""
        # --- Orderbook ---
        all_ob: list[dict[str, Any]] = []
        for rows in orderbook_snapshot.values():
            all_ob.extend(rows)
        if all_ob:
            ob_df = pd.DataFrame(all_ob)
            append_ws_orderbook_staged(
                ob_df,
                orderbook_dir=str(self.paths.orderbook_dir),
                logger=self.logger,
            )

        # --- Ticks ---
        all_ticks: list[dict[str, Any]] = []
        for rows in tick_snapshot.values():
            all_ticks.extend(rows)
        if all_ticks:
            ticks_df = pd.DataFrame(all_ticks)
            if "category" in ticks_df.columns:
                culture_mask = ticks_df["category"] == "culture"
                crypto_df = ticks_df[~culture_mask].drop(columns=["category"], errors="ignore")
                culture_df = ticks_df[culture_mask].drop(columns=["category"], errors="ignore")
            else:
                crypto_df = ticks_df
                culture_df = pd.DataFrame()

            if not crypto_df.empty:
                append_ws_ticks_staged(
                    crypto_df,
                    ticks_dir=str(self.paths.ticks_dir),
                    logger=self.logger,
                )
            if not culture_df.empty:
                data_culture_dir = self.paths.data_dir.parent / "data-culture"
                append_ws_ticks_staged(
                    culture_df,
                    ticks_dir=str(data_culture_dir / "ticks"),
                    logger=self.logger,
                )

        # --- Spot prices ---
        if spot_price_snapshot:
            append_ws_spot_prices_staged(
                spot_price_snapshot,
                spot_prices_dir=str(self.paths.spot_prices_dir),
                logger=self.logger,
            )

        # --- Heartbeats ---
        if heartbeat_snapshot:
            append_ws_heartbeats_staged(
                heartbeat_snapshot,
                heartbeats_dir=str(self.paths.heartbeats_dir),
                logger=self.logger,
            )

        # --- Prices (shard writes, no lock) ---
        flushed_rows = 0
        if ws_snapshot:
            all_price_rows: list[dict[str, Any]] = []
            for rows in ws_snapshot.values():
                all_price_rows.extend(rows)
            if all_price_rows:
                prices_df = pd.DataFrame(all_price_rows)
                if "category" not in prices_df.columns:
                    prices_df["category"] = "crypto"

                is_culture = prices_df["category"] == "culture"
                crypto_prices = prices_df[~is_culture]
                culture_prices = prices_df[is_culture]

                if not crypto_prices.empty:
                    append_ws_prices_staged(
                        crypto_prices,
                        prices_dir=str(self.paths.prices_dir),
                        logger=self.logger,
                    )
                    flushed_rows += len(crypto_prices)

                if not culture_prices.empty:
                    data_culture_dir = self.paths.data_dir.parent / "data-culture"
                    data_culture_dir.mkdir(parents=True, exist_ok=True)
                    append_ws_culture_prices_staged(
                        culture_prices,
                        prices_dir=str(data_culture_dir / "prices"),
                        logger=self.logger,
                    )
                    flushed_rows += len(culture_prices)

        return flushed_rows

    # ------------------------------------------------------------------
    # Per-shard session (runs inside the reconnect loop)
    # ------------------------------------------------------------------

    async def _run_ws_session(
        self,
        ws: Any,
        shard_token_ids: list[str],
        shard_idx: int,
        n_shards: int,
        token_to_market: dict[str, tuple[MarketRecord, str]],
        last_prices: dict[str, dict[str, float]],
        ws_buffer: DropOldestBuffer,
        tick_buffer: DropOldestBuffer,
        orderbook_buffer: DropOldestBuffer,
        hb: DataHeartbeat,
    ) -> dict[str, Any]:
        """Subscribe and process messages until the session ends.

        Returns ``{"data_received": bool, "clean": bool}`` so the outer
        reconnect loop can decide between zero-delay (healthy drop) and
        jittered backoff (unhealthy).  Any terminal failure is raised.
        """
        # Per-token BBO state survives ``price_change`` updates.  Reset on
        # every new session since the server won't carry it across
        # connections.
        token_bbo: dict[str, dict[str, float]] = {}
        data_received = False

        await ws.send(clob_subscribe_payload(shard_token_ids))
        self.logger.info(
            "WS shard %d/%d: subscribed to %d tokens",
            shard_idx + 1, n_shards, len(shard_token_ids),
        )

        async def _recv_loop() -> None:
            nonlocal data_received
            while True:
                message = await ws.recv()
                local_recv_ts_ns = time.time_ns()
                try:
                    payload = json.loads(message)
                except (ValueError, TypeError):
                    continue

                if not data_received:
                    data_received = True
                    self._mark_first_data()

                # Backpressure is now continuous drop-oldest at the buffer
                # layer (see DropOldestBuffer).  No emergency-eviction
                # cliff — the flush loop alerts on sustained drop counts.

                # --- Initial snapshot array ---
                if isinstance(payload, list):
                    for entry in payload:
                        if not isinstance(entry, dict):
                            continue
                        book = parse_book_snapshot(entry)
                        if book is None:
                            continue
                        self._apply_book_snapshot(
                            book, token_to_market, token_bbo,
                            orderbook_buffer, local_recv_ts_ns,
                        )
                    continue

                if not isinstance(payload, dict):
                    continue

                event_type = payload.get("event_type")

                # Dispatch ordered by frequency: price_change > last_trade_price > book.
                # Orderbook updates dominate at ~90%+ of message volume.
                if event_type == "price_change":
                    hb.mark("price_change")
                    for change in parse_price_change(payload):
                        self._apply_price_change(
                            change, token_to_market, token_bbo,
                            orderbook_buffer, local_recv_ts_ns,
                        )
                    continue

                if event_type == "last_trade_price":
                    hb.mark("last_trade_price")
                    trade = parse_last_trade_price(payload)
                    if trade is None:
                        continue
                    self._apply_last_trade(
                        trade, token_to_market, last_prices,
                        ws_buffer, tick_buffer, local_recv_ts_ns,
                    )
                    continue

                if event_type == "book":
                    hb.mark("book")
                    book = parse_book_snapshot(payload)
                    if book is None:
                        continue
                    self._apply_book_snapshot(
                        book, token_to_market, token_bbo,
                        orderbook_buffer, local_recv_ts_ns,
                    )
                    continue

        async def _watchdog_loop() -> None:
            while True:
                await asyncio.sleep(WS_WATCHDOG_CHECK_INTERVAL_S)
                stale = hb.stale_keys()
                if stale:
                    keys_str = ", ".join(f"{k}({age:.1f}s)" for k, age in stale)
                    self.logger.warning(
                        "WS shard %d/%d: stale data (%s) — forcing reconnect",
                        shard_idx + 1, n_shards, keys_str,
                    )
                    raise _WatchdogStale(keys_str)

        recv_task = asyncio.create_task(_recv_loop())
        watch_task = asyncio.create_task(_watchdog_loop())
        try:
            done, pending = await asyncio.wait(
                {recv_task, watch_task},
                return_when=asyncio.FIRST_EXCEPTION,
            )
        finally:
            for task in (recv_task, watch_task):
                if not task.done():
                    task.cancel()
            # Surface any cancellation / suppress asyncio.CancelledError noise.
            await asyncio.gather(recv_task, watch_task, return_exceptions=True)

        clean_close = False
        for task in done:
            exc = task.exception()
            if exc is None:
                continue
            if isinstance(exc, _WatchdogStale):
                raise exc
            if isinstance(exc, websockets.exceptions.ConnectionClosedOK):
                clean_close = True
                continue
            raise exc

        return {"data_received": data_received, "clean": clean_close}

    # ------------------------------------------------------------------
    # Event handlers (pure: no awaiting, no I/O)
    # ------------------------------------------------------------------

    def _apply_book_snapshot(
        self,
        book,
        token_to_market: dict[str, tuple[MarketRecord, str]],
        token_bbo: dict[str, dict[str, float]],
        orderbook_buffer: DropOldestBuffer,
        local_recv_ts_ns: int,
    ) -> None:
        entry = token_to_market.get(book.asset_id)
        if entry is None:
            return
        market, outcome_side = entry
        # Snapshots always reset the per-token BBO (they are full refreshes).
        token_bbo[book.asset_id] = {
            "best_bid": book.best_bid,
            "best_ask": book.best_ask,
            "best_bid_size": book.best_bid_size,
            "best_ask_size": book.best_ask_size,
        }
        if market.category != "crypto":
            return
        if not is_valid_bbo(
            book.best_bid, book.best_ask, book.best_bid_size, book.best_ask_size,
        ):
            return
        ts_ms = int(time.time() * 1000)
        orderbook_buffer.append(
            build_orderbook_row(
                market,
                ts_ms=ts_ms,
                token_id=book.asset_id,
                outcome_side=outcome_side,
                best_bid=book.best_bid,
                best_ask=book.best_ask,
                best_bid_size=book.best_bid_size,
                best_ask_size=book.best_ask_size,
                local_recv_ts_ns=local_recv_ts_ns,
            )
        )

    def _apply_price_change(
        self,
        change,
        token_to_market: dict[str, tuple[MarketRecord, str]],
        token_bbo: dict[str, dict[str, float]],
        orderbook_buffer: DropOldestBuffer,
        local_recv_ts_ns: int,
    ) -> None:
        entry = token_to_market.get(change.asset_id)
        if entry is None:
            return
        market, outcome_side = entry
        if market.category != "crypto":
            return
        bbo = token_bbo.get(change.asset_id, {
            "best_bid": 0.0, "best_ask": 0.0,
            "best_bid_size": 0.0, "best_ask_size": 0.0,
        })
        # Merge: partial updates omit fields the server didn't change.  We
        # must NOT zero them — that would inject fake state.
        if change.best_bid is not None:
            bbo["best_bid"] = change.best_bid
        if change.best_ask is not None:
            bbo["best_ask"] = change.best_ask
        if change.best_bid_size is not None:
            bbo["best_bid_size"] = change.best_bid_size
        if change.best_ask_size is not None:
            bbo["best_ask_size"] = change.best_ask_size
        token_bbo[change.asset_id] = bbo

        if not is_valid_bbo(
            bbo["best_bid"], bbo["best_ask"],
            bbo["best_bid_size"], bbo["best_ask_size"],
        ):
            # Empty/degenerate books during a market's first 170 s —
            # tracked in BBO state for future merges but not written.
            return
        ts_ms = int(time.time() * 1000)
        orderbook_buffer.append(
            build_orderbook_row(
                market,
                ts_ms=ts_ms,
                token_id=change.asset_id,
                outcome_side=outcome_side,
                best_bid=bbo["best_bid"],
                best_ask=bbo["best_ask"],
                best_bid_size=bbo["best_bid_size"],
                best_ask_size=bbo["best_ask_size"],
                local_recv_ts_ns=local_recv_ts_ns,
            )
        )

    def _apply_last_trade(
        self,
        trade,
        token_to_market: dict[str, tuple[MarketRecord, str]],
        last_prices: dict[str, dict[str, float]],
        ws_buffer: DropOldestBuffer,
        tick_buffer: DropOldestBuffer,
        local_recv_ts_ns: int,
    ) -> None:
        entry = token_to_market.get(trade.asset_id)
        if entry is None:
            return
        market, outcome_side = entry

        timestamp = trade.timestamp_ms // 1000
        size_usdc = round(trade.size_shares * trade.price, 6) if trade.size_shares > 0 else 0.0

        if market.market_id not in last_prices:
            last_prices[market.market_id] = {}
        last_prices[market.market_id][outcome_side] = trade.price

        if market.category == "crypto":
            if "up" not in last_prices[market.market_id]:
                last_prices[market.market_id]["up"] = 0.5
            if "down" not in last_prices[market.market_id]:
                last_prices[market.market_id]["down"] = 0.5
            ws_buffer.append(
                build_binary_price_row(
                    market,
                    timestamp=timestamp,
                    side_prices=last_prices[market.market_id],
                    resolution=None,
                )
            )
        else:
            ws_buffer.append({
                "market_id": market.market_id,
                "crypto": market.crypto,
                "timeframe": market.timeframe,
                "timestamp": timestamp,
                "token_id": trade.asset_id,
                "outcome": outcome_side,
                "price": float(trade.price),
                "question": market.question,
                "volume": market.volume,
                "resolution": market.resolution,
                "start_ts": market.start_ts,
                "end_ts": market.end_ts,
                "condition_id": market.condition_id,
                "tokens": json.dumps(market.tokens),
                "category": market.category,
            })

        # Culture markets skip live tick collection — matches historical
        # backfill policy (focus on share prices only).
        if market.category == "culture":
            return

        tick_buffer.append(
            build_binary_tick_row(
                market,
                timestamp_ms=trade.timestamp_ms,
                token_id=trade.asset_id,
                outcome_side=outcome_side,
                trade_side=trade.trade_side,
                price=trade.price,
                size_usdc=size_usdc,
                tx_hash=trade.tx_hash,
                block_number=0,
                log_index=0,
                source="websocket",
                local_recv_ts_ns=local_recv_ts_ns,
                **self._spot_price_kwargs(market.crypto),
            )
        )

    # ------------------------------------------------------------------
    # Outer reconnect loop
    # ------------------------------------------------------------------

    async def _run_ws_shard(
        self,
        shard_token_ids: list[str],
        shard_idx: int,
        n_shards: int,
        token_to_market: dict[str, tuple[MarketRecord, str]],
        last_prices: dict[str, dict[str, float]],
        ws_buffer: DropOldestBuffer,
        tick_buffer: DropOldestBuffer,
        orderbook_buffer: DropOldestBuffer,
    ) -> None:
        shard_key = f"clob_shard_{shard_idx}"
        hb = DataHeartbeat(
            stale_after={"price_change": WS_STALENESS_CLOB_PRICE_CHANGE_S},
            grace_period_s=WS_WATCHDOG_GRACE_PERIOD_S,
        )
        self._heartbeats[shard_key] = hb
        reconnect_attempts = 0
        disconnect_time: float | None = None

        while True:
            session_started = time.monotonic()
            fd: int | None = None
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=10,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    fd = configure_tcp_socket(ws, self.logger)
                    hb.reset()
                    connect_time = time.time()
                    if disconnect_time is not None:
                        gap_seconds = connect_time - disconnect_time
                        self.logger.info(
                            "WS shard %d/%d: reconnected after %.1fs gap (fd=%s) — data for this window will be backfilled by the next historical scan.",
                            shard_idx + 1, n_shards, gap_seconds, fd,
                        )
                    disconnect_time = None
                    result = await self._run_ws_session(
                        ws, shard_token_ids, shard_idx, n_shards,
                        token_to_market, last_prices,
                        ws_buffer, tick_buffer, orderbook_buffer, hb,
                    )
                # Exited cleanly (server-initiated close with no watchdog trip).
                session_duration = time.monotonic() - session_started
                self._log_reconnect_event(
                    shard_key, "clean_close", None,
                    fd=fd, session_duration=session_duration,
                )
                if result["data_received"]:
                    reconnect_attempts = 0
                    disconnect_time = time.time()
                    # Fast reconnect for healthy server-initiated drops.
                    self.logger.info(
                        "WS shard %d/%d: clean close after %.0fs session — immediate reconnect",
                        shard_idx + 1, n_shards, session_duration,
                    )
                    continue
                # Clean close with no data → likely server refusal; back off.
                delay = jittered_backoff(reconnect_attempts)
                reconnect_attempts += 1
                disconnect_time = time.time()
                self.logger.warning(
                    "WS shard %d/%d: clean close with no data (attempt %d) — sleeping %.1fs",
                    shard_idx + 1, n_shards, reconnect_attempts, delay,
                )
                self._record_reconnect_burst(shard_key)
                await asyncio.sleep(delay)
            except (asyncio.CancelledError, KeyboardInterrupt):
                raise
            except _WatchdogStale as exc:
                session_duration = time.monotonic() - session_started
                disconnect_time = time.time()
                self._log_reconnect_event(
                    shard_key, "watchdog_stale", str(exc),
                    fd=fd, session_duration=session_duration,
                )
                # Stale data implies the prior session WAS receiving data —
                # reset backoff for fast MTTR.  Still record for the burst
                # monitor so we detect repeated staleness.
                reconnect_attempts = 0
                self._record_reconnect_burst(shard_key)
                await asyncio.sleep(jittered_backoff(0))
            except websockets.exceptions.ConnectionClosed as exc:
                session_duration = time.monotonic() - session_started
                disconnect_time = time.time()
                delay = jittered_backoff(reconnect_attempts)
                reconnect_attempts += 1
                self._log_reconnect_event(
                    shard_key, "connection_closed",
                    f"code={getattr(exc, 'code', '?')} reason={getattr(exc, 'reason', '')}",
                    fd=fd, session_duration=session_duration,
                )
                self.logger.warning(
                    "WS shard %d/%d: connection closed (code=%s reason=%r). Reconnecting in %.1fs",
                    shard_idx + 1, n_shards,
                    getattr(exc, "code", "?"), getattr(exc, "reason", ""), delay,
                )
                self._record_reconnect_burst(shard_key)
                await asyncio.sleep(delay)
            except Exception as exc:
                session_duration = time.monotonic() - session_started
                disconnect_time = time.time()
                delay = jittered_backoff(reconnect_attempts)
                reconnect_attempts += 1
                self._log_reconnect_event(
                    shard_key, "exception",
                    f"{type(exc).__name__}: {exc}",
                    fd=fd, session_duration=session_duration,
                )
                self.logger.warning(
                    "WS shard %d/%d: %s — %s. Reconnecting in %.1fs",
                    shard_idx + 1, n_shards,
                    type(exc).__name__, exc, delay,
                )
                self._record_reconnect_burst(shard_key)
                await asyncio.sleep(delay)

    def _log_reconnect_event(
        self,
        shard_key: str,
        kind: str,
        detail: str | None,
        *,
        fd: int | None,
        session_duration: float,
    ) -> None:
        payload = {
            "ts": time.time(),
            "shard_key": shard_key,
            "kind": kind,
            "detail": detail or "",
            "fd": fd,
            "session_s": round(session_duration, 3),
        }
        _RECONNECT_LOGGER.info(json.dumps(payload))

    def _record_reconnect_burst(self, shard_key: str) -> None:
        count = self._reconnect_monitor.record(shard_key)
        if self._reconnect_monitor.reserve_alert(shard_key):
            send_alert_async(
                "reconnect_burst",
                f"{shard_key}: {count} reconnects in {WS_RECONNECT_BURST_WINDOW_S:.0f}s",
                extra={"shard": shard_key, "count": count, "window_s": WS_RECONNECT_BURST_WINDOW_S},
            )

    # ------------------------------------------------------------------
    # Flush loop
    # ------------------------------------------------------------------

    def _build_heartbeat_rows(self) -> list[dict[str, Any]]:
        """Snapshot all registered heartbeats into a row list.

        Rows go to ``data/heartbeats/`` via ``append_ws_heartbeats_staged``
        on the next flush cycle, so downstream gap scanning is a single
        file scan.
        """
        now_ms = int(time.time() * 1000)
        rows: list[dict[str, Any]] = []
        for shard_key, hb in self._heartbeats.items():
            if shard_key.startswith("rtds_binance"):
                source = "rtds_binance"
            elif shard_key.startswith("rtds_chainlink"):
                source = "rtds_chainlink"
            else:
                source = "clob_ws"
            for event_type, _threshold in hb.stale_after.items():
                age_s = hb.age(event_type)
                rows.append({
                    "ts_ms": now_ms,
                    "source": source,
                    "shard_key": shard_key,
                    "event_type": event_type,
                    "last_event_age_ms": int(age_s * 1000) if age_s is not None else -1,
                })
        return rows

    @staticmethod
    def _bucket_by_timeframe(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        """Group row dicts by their ``timeframe`` field for partitioned writes."""
        out: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            tf = row.get("timeframe")
            if tf is None:
                continue
            out.setdefault(tf, []).append(row)
        return out

    def _check_drop_alerts(
        self,
        ws_buffer: DropOldestBuffer,
        tick_buffer: DropOldestBuffer,
        orderbook_buffer: DropOldestBuffer,
    ) -> None:
        """Fire a webhook when any buffer's drop-count jumped past threshold.

        Drops are continuous under backpressure (drop-oldest), so we
        measure increments since the last check rather than total
        cumulative drops.
        """
        for name, buf in (
            ("ws", ws_buffer),
            ("tick", tick_buffer),
            ("ob", orderbook_buffer),
            ("spot", self.spot_price_buffer),
        ):
            prev = self._last_drop_totals.get(name, 0)
            now_total = buf.dropped_count
            delta = now_total - prev
            self._last_drop_totals[name] = now_total
            if delta >= WS_DROP_ALERT_THRESHOLD:
                self.logger.error(
                    "Buffer backpressure: %s buffer dropped %d rows since last flush",
                    name, delta,
                )
                send_alert_async(
                    "buffer_drop",
                    f"{name} buffer dropped {delta} rows since last flush",
                    extra={"buffer": name, "delta": delta, "total": now_total},
                )

    async def _ws_flush_loop(
        self,
        ws_buffer: DropOldestBuffer,
        tick_buffer: DropOldestBuffer,
        orderbook_buffer: DropOldestBuffer,
    ) -> None:
        loop = asyncio.get_running_loop()
        check_interval = 1.0
        last_flush_time = loop.time()
        last_ob_flush_time = loop.time()
        last_heartbeat_time = loop.time()

        while True:
            await asyncio.sleep(check_interval)

            now = loop.time()
            total_ws_rows = len(ws_buffer)
            total_tick_rows = len(tick_buffer)
            total_ob_rows = len(orderbook_buffer)
            total_spot_rows = len(self.spot_price_buffer)
            elapsed = now - last_flush_time
            ob_elapsed = now - last_ob_flush_time
            hb_elapsed = now - last_heartbeat_time

            flush_ob = ob_elapsed >= WS_OB_FLUSH_INTERVAL_S
            flush_hb = hb_elapsed >= WS_HEARTBEAT_INTERVAL_S

            total_non_ob = total_ws_rows + total_tick_rows + total_spot_rows
            total_all = total_non_ob + (total_ob_rows if flush_ob else 0)

            if total_all == 0 and not flush_hb and elapsed < WS_FLUSH_INTERVAL_SECONDS:
                continue
            if total_all < WS_FLUSH_BATCH_SIZE and not flush_hb and elapsed < WS_FLUSH_INTERVAL_SECONDS:
                continue

            # Atomic snapshot-then-clear inside the event loop (no await
            # between drains prevents producer races).
            ws_rows = ws_buffer.drain()
            tick_rows = tick_buffer.drain()
            orderbook_rows = orderbook_buffer.drain() if flush_ob else []
            spot_snapshot = self.spot_price_buffer.drain()
            if flush_ob:
                last_ob_flush_time = now

            ws_snapshot = self._bucket_by_timeframe(ws_rows)
            tick_snapshot = self._bucket_by_timeframe(tick_rows)
            orderbook_snapshot = self._bucket_by_timeframe(orderbook_rows)

            hb_snapshot: list[dict[str, Any]] = []
            if flush_hb:
                hb_snapshot = self._build_heartbeat_rows()
                last_heartbeat_time = now

            if not (ws_snapshot or tick_snapshot or orderbook_snapshot or spot_snapshot or hb_snapshot):
                last_flush_time = now
                continue

            last_flush_time = now
            try:
                flushed = await loop.run_in_executor(
                    None, self._flush_snapshot,
                    ws_snapshot, tick_snapshot, orderbook_snapshot, spot_snapshot, hb_snapshot,
                )
            except Exception as exc:
                self.logger.error("Flush failed (data will be retried next cycle): %s", exc)
                # Re-queue at the TAIL (not head) — DropOldestBuffer is
                # FIFO and we can't prepend without risking unbounded
                # growth.  Downstream dedup covers the reordering.
                ws_buffer.extend(ws_rows)
                tick_buffer.extend(tick_rows)
                orderbook_buffer.extend(orderbook_rows)
                self.spot_price_buffer.extend(spot_snapshot)
                # Heartbeat rows aren't critical; drop on failure.
                continue

            # After a successful flush, check whether any buffer dropped
            # rows during the last window — sustained drops indicate the
            # flush loop or disk is backing up.
            self._check_drop_alerts(ws_buffer, tick_buffer, orderbook_buffer)

            total_flushed = flushed + len(spot_snapshot) + sum(len(r) for r in orderbook_snapshot.values())
            if total_flushed or hb_snapshot:
                self.logger.info(
                    "Flushed %d price + %d orderbook + %d spot + %d heartbeat rows to disk",
                    flushed,
                    sum(len(r) for r in orderbook_snapshot.values()),
                    len(spot_snapshot),
                    len(hb_snapshot),
                )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self, active_markets: list[MarketRecord]) -> None:
        if not active_markets:
            return

        token_to_market: dict[str, tuple[MarketRecord, str]] = {}
        token_ids: list[str] = []
        for market in active_markets:
            for outcome, token_id in market.tokens.items():
                token_to_market[token_id] = (market, outcome)
                token_ids.append(token_id)

        if not token_ids:
            self.logger.warning("No token IDs available for active markets; skipping WebSocket stream")
            return

        last_prices = self._initial_last_prices(active_markets)
        ws_buffer = DropOldestBuffer(WS_PRICE_BUFFER_MAX)
        tick_buffer = DropOldestBuffer(WS_TICK_BUFFER_MAX)
        orderbook_buffer = DropOldestBuffer(WS_OB_BUFFER_MAX)

        shuffled_token_ids = token_ids[:]
        random.shuffle(shuffled_token_ids)
        shards = [
            shuffled_token_ids[index:index + WS_MAX_TOKENS_PER_SHARD]
            for index in range(0, len(shuffled_token_ids), WS_MAX_TOKENS_PER_SHARD)
        ]
        n_shards = len(shards)
        self.logger.info(
            "Starting WebSocket stream: %d active markets → %d tokens across %d shard(s) (≤%d tokens/shard)",
            len(active_markets), len(token_ids), n_shards, WS_MAX_TOKENS_PER_SHARD,
        )

        shard_tasks = [
            asyncio.create_task(
                self._run_ws_shard(
                    shard, idx, n_shards, token_to_market, last_prices,
                    ws_buffer, tick_buffer, orderbook_buffer,
                )
            )
            for idx, shard in enumerate(shards)
        ]
        flush_task = asyncio.create_task(self._ws_flush_loop(ws_buffer, tick_buffer, orderbook_buffer))
        all_tasks = [flush_task, *shard_tasks]

        try:
            await asyncio.gather(*all_tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for task in all_tasks:
                task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)
            final_ws = self._bucket_by_timeframe(ws_buffer.drain())
            final_ticks = self._bucket_by_timeframe(tick_buffer.drain())
            final_ob = self._bucket_by_timeframe(orderbook_buffer.drain())
            final_spot = self.spot_price_buffer.drain()
            final_hb = self._build_heartbeat_rows()
            if final_ws or final_ticks or final_ob or final_spot or final_hb:
                flushed = self._flush_snapshot(final_ws, final_ticks, final_ob, final_spot, final_hb)
                total = flushed + len(final_spot) + sum(len(r) for r in final_ob.values()) + len(final_hb)
                if total:
                    self.logger.info("Final flush on shutdown: %d rows", total)
