"""RTDSStreamPhase — maintains a live spot-price cache from Polymarket RTDS.

Polymarket RTDS WebSocket: wss://ws-live-data.polymarket.com
Docs: https://docs.polymarket.com/market-data/websocket/rtds

The phase does NOT write to disk.  It populates a shared in-memory dict
``latest_prices`` that the WebSocketPhase reads synchronously when it builds
each tick row.  Because both run in the same asyncio event loop, no lock is
needed — updates and reads are atomic at the await boundary.

Cache structure::

    latest_prices: dict[str, tuple[float, int]]
    #  key   = RTDS symbol  ("btcusdt", "ethusdt", "solusdt")
    #  value = (spot_price_usdt, binance_timestamp_ms)

Subscription message (Binance source, filtered):
    {
        "action": "subscribe",
        "subscriptions": [
            {"topic": "crypto_prices", "type": "update",
             "filters": "btcusdt,ethusdt,solusdt"}
        ]
    }

Keep-alive: send plain "PING" text every 5 seconds.
No authentication required for the crypto_prices topic.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import websockets

from ..config import MAX_WS_RECONNECT_DELAY_SECONDS

_RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
_PING_INTERVAL_SECONDS = 5.0
_SUPPORTED_SYMBOLS = frozenset({"btcusdt", "ethusdt", "solusdt"})

# Mapping from MarketRecord.crypto → RTDS symbol
CRYPTO_TO_RTDS_SYMBOL: dict[str, str] = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
}


class RTDSStreamPhase:
    """Populates a shared price cache from Polymarket RTDS crypto_prices.

    Usage::

        cache: dict[str, tuple[float, int]] = {}
        phase = RTDSStreamPhase(cache, logger=log)
        task  = asyncio.create_task(phase.run())

    Other tasks can then read ``cache.get("btcusdt")`` at any time to
    obtain ``(spot_price_usdt, binance_ts_ms)`` or ``None``.
    """

    def __init__(
        self,
        latest_prices: dict[str, tuple[float, int]],
        *,
        symbols: frozenset[str] = _SUPPORTED_SYMBOLS,
        logger: logging.Logger | None = None,
    ) -> None:
        self.latest_prices = latest_prices
        self.symbols = symbols
        self.logger = logger or logging.getLogger("polymarket_pipeline.rtds")

    # ------------------------------------------------------------------
    # Internal: single connection attempt
    # ------------------------------------------------------------------

    async def _run_connection(self) -> bool:
        """Connect, subscribe, and update the cache until disconnected.

        Returns True if at least one valid price update was received during
        this connection, False otherwise (used to decide whether to reset the
        reconnect backoff counter).
        """
        received_data = False
        filters = ",".join(sorted(self.symbols))
        subscribe_msg = json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices", "type": "update", "filters": filters}
            ],
        })

        async with websockets.connect(
            _RTDS_WS_URL,
            max_size=None,
            open_timeout=10,          # fail fast on DNS/TCP hangs
            ping_interval=None,       # we manage pings manually per RTDS docs
        ) as ws:
            self.logger.info("RTDS: connected, subscribing to %s", filters)
            await ws.send(subscribe_msg)

            async def _ping_loop() -> None:
                while True:
                    await asyncio.sleep(_PING_INTERVAL_SECONDS)
                    try:
                        await ws.send("PING")
                    except Exception:
                        break  # outer recv will raise and trigger reconnect

            ping_task = asyncio.create_task(_ping_loop())
            try:
                while True:
                    raw = await ws.recv()

                    # RTDS responds to PING with "PONG"
                    if raw == "PONG":
                        continue

                    try:
                        msg: dict[str, Any] = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if msg.get("topic") != "crypto_prices" or msg.get("type") != "update":
                        continue

                    payload = msg.get("payload")
                    if not isinstance(payload, dict):
                        continue

                    symbol = str(payload.get("symbol", "")).lower()
                    if symbol not in self.symbols:
                        continue

                    value = payload.get("value")
                    timestamp_ms = payload.get("timestamp")
                    if value is None or timestamp_ms is None:
                        continue

                    # Atomic dict write — safe in single-threaded asyncio
                    self.latest_prices[symbol] = (float(value), int(timestamp_ms))
                    received_data = True

            finally:
                ping_task.cancel()
                await asyncio.gather(ping_task, return_exceptions=True)

        return received_data

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run the RTDS cache updater indefinitely (until task is cancelled)."""
        reconnect_attempts = 0

        while True:
            try:
                received_data = await self._run_connection()
                # Only reset backoff if we received real data — prevents tight
                # reconnect loops when the server keeps cleanly closing.
                if received_data:
                    reconnect_attempts = 0
                else:
                    reconnect_delay = min(
                        5 * (2 ** reconnect_attempts), MAX_WS_RECONNECT_DELAY_SECONDS
                    )
                    reconnect_attempts += 1
                    self.logger.warning(
                        "RTDS: clean close with no data. Reconnecting in %.1fs...",
                        reconnect_delay,
                    )
                    await asyncio.sleep(reconnect_delay)
            except (asyncio.CancelledError, KeyboardInterrupt):
                return
            except Exception as exc:
                reconnect_delay = min(
                    5 * (2 ** reconnect_attempts), MAX_WS_RECONNECT_DELAY_SECONDS
                )
                reconnect_attempts += 1
                self.logger.warning(
                    "RTDS: %s — %s. Reconnecting in %.1fs...",
                    type(exc).__name__, exc, reconnect_delay,
                )
                await asyncio.sleep(reconnect_delay)
