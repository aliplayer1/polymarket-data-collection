from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd
import websockets

from ..config import (
    MAX_WS_RECONNECT_DELAY_SECONDS,
    WS_BUFFER_MAX_ROWS,
    WS_FLUSH_BATCH_SIZE,
    WS_FLUSH_INTERVAL_SECONDS,
    WS_MAX_TOKENS_PER_SHARD,
    WS_URL,
)
from ..models import MarketRecord
from ..providers import LastTradePriceProvider
from ..retry import api_call_with_retry
from ..storage import append_ws_ticks_staged
from .rtds_stream import CRYPTO_TO_RTDS_SYMBOL
from .shared import PipelinePaths, build_binary_price_row, build_binary_tick_row


class WebSocketPhase:
    def __init__(
        self,
        last_trade_price_provider: LastTradePriceProvider,
        price_history_phase,
        *,
        logger: logging.Logger,
        paths: PipelinePaths,
        spot_price_cache: dict[str, tuple[float, int]] | None = None,
    ) -> None:
        self.last_trade_price_provider = last_trade_price_provider
        self.price_history_phase = price_history_phase
        self.logger = logger
        self.paths = paths
        self.spot_price_cache = spot_price_cache or {}

    def update_paths(self, paths: PipelinePaths) -> None:
        self.paths = paths

    def _spot_price_kwargs(self, crypto: str) -> dict[str, float | int | None]:
        """Look up the latest RTDS spot price for the given crypto.

        Returns a dict suitable for **-unpacking into ``build_binary_tick_row``.
        If no price has been received yet, returns None values (columns are nullable).
        """
        rtds_symbol = CRYPTO_TO_RTDS_SYMBOL.get(crypto)
        if rtds_symbol is None:
            return {"spot_price_usdt": None, "spot_price_ts_ms": None}
        entry = self.spot_price_cache.get(rtds_symbol)
        if entry is None:
            return {"spot_price_usdt": None, "spot_price_ts_ms": None}
        return {"spot_price_usdt": entry[0], "spot_price_ts_ms": entry[1]}


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
                    self.logger.warning("Falling back to 0.5 for market %s outcome %s: %s", market.market_id, outcome, exc)
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
    ) -> int:
        flushed_rows = 0
        for timeframe, rows in ws_snapshot.items():
            if not rows:
                continue
            new_df = pd.DataFrame(rows)
            self.price_history_phase.persist_dataframe(timeframe, new_df, update_cache=False)
            flushed_rows += len(rows)

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
                        gap_seconds = connect_time - disconnect_time
                        self.logger.info(
                            "WS shard %d/%d: reconnected after %.1fs gap — price data for this shard's markets will be backfilled by the next historical scan.",
                            shard_idx + 1,
                            n_shards,
                            gap_seconds,
                        )
                    self.logger.info(
                        "WS shard %d/%d: subscribed to %d tokens",
                        shard_idx + 1,
                        n_shards,
                        len(shard_token_ids),
                    )
                    reconnect_attempts = 0
                    disconnect_time = None

                    while True:
                        message = await ws.recv()
                        payload = json.loads(message)
                        events = payload if isinstance(payload, list) else [payload]

                        total_buffered = (
                            sum(len(values) for values in ws_buffer.values())
                            + sum(len(values) for values in tick_buffer.values())
                        )
                        if total_buffered >= WS_BUFFER_MAX_ROWS:
                            for buffer in (ws_buffer, tick_buffer):
                                for timeframe in list(buffer.keys()):
                                    rows = buffer[timeframe]
                                    if rows:
                                        buffer[timeframe] = rows[len(rows) // 2:]
                            self.logger.error(
                                "WS shard %d/%d: buffer at capacity (%d rows) — evicted oldest half; flush loop may be stuck",
                                shard_idx + 1,
                                n_shards,
                                total_buffered,
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
                            size_usdc = round(size_shares * price, 6) if size_shares > 0 else 0.0

                            if market.market_id not in last_prices:
                                last_prices[market.market_id] = {}
                            last_prices[market.market_id][outcome_side] = price

                            if market.category == "crypto":
                                # Ensure we have 'up' and 'down' in last_prices
                                if "up" not in last_prices[market.market_id]:
                                    last_prices[market.market_id]["up"] = 0.5
                                if "down" not in last_prices[market.market_id]:
                                    last_prices[market.market_id]["down"] = 0.5
                                ws_buffer[market.timeframe].append(
                                    build_binary_price_row(
                                        market,
                                        timestamp=timestamp,
                                        side_prices=last_prices[market.market_id],
                                        resolution=None,
                                    )
                                )
                            else:
                                ws_buffer[market.timeframe].append({
                                    "market_id": market.market_id,
                                    "crypto": market.crypto,
                                    "timeframe": market.timeframe,
                                    "timestamp": timestamp,
                                    "token_id": token_id,
                                    "outcome": outcome_side,
                                    "price": float(price),
                                    "question": market.question,
                                    "volume": market.volume,
                                    "resolution": market.resolution,
                                    "start_ts": market.start_ts,
                                    "end_ts": market.end_ts,
                                    "condition_id": market.condition_id,
                                    "tokens": json.dumps(market.tokens),
                                    "category": market.category,
                                })
                                
                            tick_buffer[market.timeframe].append(
                                build_binary_tick_row(
                                    market,
                                    timestamp_ms=timestamp_ms,
                                    token_id=token_id,
                                    outcome_side=outcome_side,
                                    trade_side=trade_side,
                                    price=price,
                                    size_usdc=size_usdc,
                                    tx_hash=str(event.get("hash") or event.get("tx_hash") or ""),
                                    block_number=0,
                                    log_index=0,
                                    source="websocket",
                                    **self._spot_price_kwargs(market.crypto),
                                )
                            )

            except (asyncio.CancelledError, KeyboardInterrupt):
                raise
            except websockets.exceptions.ConnectionClosed as exc:
                disconnect_time = time.time()
                reconnect_delay = min(10 * (2 ** reconnect_attempts), MAX_WS_RECONNECT_DELAY_SECONDS)
                reconnect_attempts += 1
                self.logger.warning(
                    "WS shard %d/%d: connection closed (code=%s reason=%r). Reconnecting in %ss...",
                    shard_idx + 1,
                    n_shards,
                    getattr(exc, "code", "?"),
                    getattr(exc, "reason", ""),
                    reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)
            except Exception as exc:
                disconnect_time = time.time()
                reconnect_delay = min(10 * (2 ** reconnect_attempts), MAX_WS_RECONNECT_DELAY_SECONDS)
                reconnect_attempts += 1
                self.logger.warning(
                    "WS shard %d/%d: %s — %s. Reconnecting in %ss...",
                    shard_idx + 1,
                    n_shards,
                    type(exc).__name__,
                    exc,
                    reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)

    async def _ws_flush_loop(
        self,
        ws_buffer: dict[str, list[dict[str, Any]]],
        tick_buffer: dict[str, list[dict[str, Any]]],
    ) -> None:
        loop = asyncio.get_running_loop()
        check_interval = 1.0
        last_flush_time = loop.time()

        while True:
            await asyncio.sleep(check_interval)

            total_ws_rows = sum(len(values) for values in ws_buffer.values())
            elapsed = loop.time() - last_flush_time

            if total_ws_rows == 0 and elapsed < WS_FLUSH_INTERVAL_SECONDS:
                continue
            if total_ws_rows < WS_FLUSH_BATCH_SIZE and elapsed < WS_FLUSH_INTERVAL_SECONDS:
                continue

            ws_snapshot: dict[str, list[dict[str, Any]]] = {}
            tick_snapshot: dict[str, list[dict[str, Any]]] = {}
            for timeframe in list(ws_buffer.keys()):
                if ws_buffer[timeframe]:
                    ws_snapshot[timeframe] = ws_buffer[timeframe]
                    ws_buffer[timeframe] = []
            for timeframe in list(tick_buffer.keys()):
                if tick_buffer[timeframe]:
                    tick_snapshot[timeframe] = tick_buffer[timeframe]
                    tick_buffer[timeframe] = []

            if not ws_snapshot and not tick_snapshot:
                last_flush_time = loop.time()
                continue

            last_flush_time = loop.time()
            flushed = await loop.run_in_executor(None, self._flush_snapshot, ws_snapshot, tick_snapshot)
            if flushed:
                self.logger.info("Flushed %d WebSocket rows to disk", flushed)

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
        ws_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)
        tick_buffer: dict[str, list[dict[str, Any]]] = defaultdict(list)

        shuffled_token_ids = token_ids[:]
        random.shuffle(shuffled_token_ids)
        shards = [
            shuffled_token_ids[index:index + WS_MAX_TOKENS_PER_SHARD]
            for index in range(0, len(shuffled_token_ids), WS_MAX_TOKENS_PER_SHARD)
        ]
        n_shards = len(shards)
        self.logger.info(
            "Starting WebSocket stream: %d active markets → %d tokens across %d shard(s) (≤%d tokens/shard)",
            len(active_markets),
            len(token_ids),
            n_shards,
            WS_MAX_TOKENS_PER_SHARD,
        )

        shard_tasks = [
            asyncio.create_task(
                self._run_ws_shard(
                    shard,
                    idx,
                    n_shards,
                    token_to_market,
                    last_prices,
                    ws_buffer,
                    tick_buffer,
                )
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
            final_ws = {timeframe: rows for timeframe, rows in ws_buffer.items() if rows}
            final_ticks = {timeframe: rows for timeframe, rows in tick_buffer.items() if rows}
            if final_ws or final_ticks:
                flushed = self._flush_snapshot(final_ws, final_ticks)
                if flushed:
                    self.logger.info("Final flush on shutdown: %d rows", flushed)
