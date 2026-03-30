"""RTDSStreamPhase — maintains a live spot-price cache from Polymarket RTDS.

Polymarket RTDS WebSocket: wss://ws-live-data.polymarket.com
Docs: https://docs.polymarket.com/market-data/websocket/rtds

The phase populates two shared in-memory dicts:

- ``latest_prices`` — Binance spot prices used by WebSocketPhase to embed
  into tick rows.  Same asyncio event loop → no lock needed.
- ``chainlink_prices`` — Chainlink reference prices (resolution oracle).

It also buffers every received price update (Binance + Chainlink) into
``spot_price_buffer``, which the caller drains periodically and flushes
to the ``data/spot_prices/`` Parquet table.

Cache structures::

    latest_prices: dict[str, tuple[float, int]]
    #  key   = RTDS symbol  ("btcusdt", "ethusdt", "solusdt")
    #  value = (spot_price_usdt, binance_timestamp_ms)

    chainlink_prices: dict[str, tuple[float, int]]
    #  key   = Chainlink symbol  ("btc/usd", "eth/usd")
    #  value = (price_usd, chainlink_timestamp_ms)

Subscription message — subscribes to BOTH Binance and Chainlink feeds:
    {
        "action": "subscribe",
        "subscriptions": [
            {"topic": "crypto_prices", "type": "*"},
            {"topic": "crypto_prices_chainlink", "type": "*"}
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
_SUPPORTED_BINANCE_SYMBOLS = frozenset({"btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt", "dogeusdt", "hypeusdt"})
_SUPPORTED_CHAINLINK_SYMBOLS = frozenset({"btc/usd", "eth/usd", "sol/usd", "bnb/usd", "xrp/usd", "doge/usd", "hype/usd"})

# Mapping from MarketRecord.crypto → RTDS Binance symbol
CRYPTO_TO_RTDS_SYMBOL: dict[str, str] = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "BNB": "bnbusdt",
    "XRP": "xrpusdt",
    "DOGE": "dogeusdt",
    "HYPE": "hypeusdt",
}


class RTDSStreamPhase:
    """Populates shared price caches and a persistent buffer from Polymarket RTDS.

    Usage::

        binance_cache: dict[str, tuple[float, int]] = {}
        chainlink_cache: dict[str, tuple[float, int]] = {}
        spot_buffer: list[dict] = []
        phase = RTDSStreamPhase(binance_cache, chainlink_cache, spot_buffer, logger=log)
        task  = asyncio.create_task(phase.run())

    Other tasks can then read ``binance_cache.get("btcusdt")`` to obtain
    ``(spot_price_usdt, binance_ts_ms)`` or ``None``.

    The ``spot_price_buffer`` accumulates rows for the spot_prices Parquet table.
    The caller is responsible for draining and flushing it periodically.
    """

    def __init__(
        self,
        latest_prices: dict[str, tuple[float, int]],
        chainlink_prices: dict[str, tuple[float, int]] | None = None,
        spot_price_buffer: list[dict[str, Any]] | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.latest_prices = latest_prices
        self.chainlink_prices = chainlink_prices if chainlink_prices is not None else {}
        self.spot_price_buffer = spot_price_buffer if spot_price_buffer is not None else []
        self.logger = logger or logging.getLogger("polymarket_pipeline.rtds")

    # ------------------------------------------------------------------
    # Internal: single connection attempt
    # ------------------------------------------------------------------

    async def _run_connection(self) -> bool:
        """Connect, subscribe, and update caches until disconnected.

        Returns True if at least one valid price update was received during
        this connection, False otherwise (used to decide whether to reset the
        reconnect backoff counter).
        """
        received_data = False

        # Subscribe to both Binance and Chainlink feeds.
        # Using type "*" (not "update") ensures we receive both snapshot and
        # update messages — filtered mode returns only a snapshot with no
        # live updates for some topic configurations.
        subscribe_msg = json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices", "type": "*"},
                {"topic": "crypto_prices_chainlink", "type": "*"},
            ],
        })

        async with websockets.connect(
            _RTDS_WS_URL,
            max_size=None,
            open_timeout=10,          # fail fast on DNS/TCP hangs
            ping_interval=None,       # we manage pings manually per RTDS docs
        ) as ws:
            self.logger.info("RTDS: connected, subscribing to Binance + Chainlink feeds")
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

                    msg_type = msg.get("type")
                    if msg_type != "update":
                        continue

                    payload = msg.get("payload")
                    if not isinstance(payload, dict):
                        continue

                    topic = msg.get("topic", "")
                    symbol = str(payload.get("symbol", "")).lower()
                    value = payload.get("value")
                    timestamp_ms = payload.get("timestamp")

                    if value is None or timestamp_ms is None:
                        continue

                    try:
                        price = float(value)
                        ts_ms = int(timestamp_ms)
                    except (ValueError, TypeError):
                        continue

                    # Validate price
                    if price <= 0.0 or not (price == price):  # NaN check
                        continue

                    if topic == "crypto_prices" and symbol in _SUPPORTED_BINANCE_SYMBOLS:
                        # Binance spot price
                        self.latest_prices[symbol] = (price, ts_ms)
                        self.spot_price_buffer.append({
                            "ts_ms": ts_ms,
                            "symbol": symbol,
                            "price": price,
                            "source": "binance",
                        })
                        received_data = True

                    elif topic == "crypto_prices_chainlink" and symbol in _SUPPORTED_CHAINLINK_SYMBOLS:
                        # Chainlink reference price (resolution oracle)
                        self.chainlink_prices[symbol] = (price, ts_ms)
                        self.spot_price_buffer.append({
                            "ts_ms": ts_ms,
                            "symbol": symbol,
                            "price": price,
                            "source": "chainlink",
                        })
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
