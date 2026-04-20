"""RTDSStreamPhase — Polymarket RTDS live spot-price cache.

URL: wss://ws-live-data.polymarket.com
Docs: https://docs.polymarket.com/market-data/websocket/rtds

The phase maintains two live dict caches (``latest_prices`` for Binance,
``chainlink_prices`` for the Chainlink oracle) that other components read
without locking — both writers and readers run on the same asyncio event
loop.  Every price update is also appended to ``spot_price_buffer`` for
persistence to ``data/spot_prices/`` by the WS flush loop.

Connection architecture
-----------------------
Two independent WebSocket connections are maintained: one for
``crypto_prices`` (Binance feed, ~1–5 Hz per symbol) and one for
``crypto_prices_chainlink`` (~0.1 Hz per symbol).  A stall on either
feed no longer takes the other down — this directly addresses the
"btcusdt silent while Chainlink/ETH alive" failure class observed in
production.

Each connection carries a data-level watchdog (:class:`DataHeartbeat`)
that forces a reconnect when any subscribed symbol goes silent for
longer than its per-feed threshold.  TCP-level keepalive + NODELAY are
set directly on the socket after connect.

Backoff is exponential with multiplicative jitter and a 30 s cap; clean
closes with prior data trigger an immediate reconnect for fast MTTR on
Polymarket's routine ~2 h server-initiated drops.
"""
from __future__ import annotations

import asyncio
import logging
import time

import websockets

from ..config import (
    WS_STALENESS_RTDS_BINANCE_S,
    WS_STALENESS_RTDS_CHAINLINK_S,
    WS_WATCHDOG_CHECK_INTERVAL_S,
    WS_WATCHDOG_GRACE_PERIOD_S,
)
from .ws_messages import parse_rtds_update, rtds_subscribe_payload
from .ws_watchdog import (
    DataHeartbeat,
    DropOldestBuffer,
    configure_tcp_socket,
    jittered_backoff,
)

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


class _WatchdogStale(Exception):
    """Raised by the per-session watchdog to force reconnect."""


class RTDSStreamPhase:
    """Populate shared price caches + persistent buffer from Polymarket RTDS.

    Usage::

        binance_cache: dict[str, tuple[float, int]] = {}
        chainlink_cache: dict[str, tuple[float, int]] = {}
        spot_buffer: list[dict] = []
        heartbeats: dict[str, DataHeartbeat] = {}
        phase = RTDSStreamPhase(
            binance_cache, chainlink_cache, spot_buffer,
            heartbeat_registry=heartbeats, logger=log,
        )
        task = asyncio.create_task(phase.run())

    ``heartbeat_registry`` is optional.  When provided, the phase registers
    one :class:`DataHeartbeat` per feed under keys ``"rtds_binance"`` and
    ``"rtds_chainlink"`` so the WS flush loop can emit heartbeat rows.
    """

    def __init__(
        self,
        latest_prices: dict[str, tuple[float, int]],
        chainlink_prices: dict[str, tuple[float, int]] | None = None,
        spot_price_buffer: DropOldestBuffer | None = None,
        *,
        heartbeat_registry: dict[str, DataHeartbeat] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.latest_prices = latest_prices
        self.chainlink_prices = chainlink_prices if chainlink_prices is not None else {}
        # Default to a 10 000-row drop-oldest ring if the caller didn't
        # pass one — keeps stand-alone instantiation from the test suite
        # working without depending on WebSocketPhase config.
        self.spot_price_buffer: DropOldestBuffer = (
            spot_price_buffer if spot_price_buffer is not None else DropOldestBuffer(10_000)
        )
        self.logger = logger or logging.getLogger("polymarket_pipeline.rtds")
        self._heartbeat_registry = heartbeat_registry

        # Per-symbol heartbeats — one DataHeartbeat per feed, keyed by symbol.
        self._binance_hb = DataHeartbeat(
            stale_after={s: WS_STALENESS_RTDS_BINANCE_S for s in _SUPPORTED_BINANCE_SYMBOLS},
            grace_period_s=WS_WATCHDOG_GRACE_PERIOD_S,
        )
        self._chainlink_hb = DataHeartbeat(
            stale_after={s: WS_STALENESS_RTDS_CHAINLINK_S for s in _SUPPORTED_CHAINLINK_SYMBOLS},
            grace_period_s=WS_WATCHDOG_GRACE_PERIOD_S,
        )
        if self._heartbeat_registry is not None:
            self._heartbeat_registry["rtds_binance"] = self._binance_hb
            self._heartbeat_registry["rtds_chainlink"] = self._chainlink_hb

    # ------------------------------------------------------------------
    # Per-feed connection
    # ------------------------------------------------------------------

    async def _run_feed(
        self,
        feed_name: str,
        topic: str,
        supported_symbols: frozenset[str],
        hb: DataHeartbeat,
        cache: dict[str, tuple[float, int]],
        source_label: str,
    ) -> None:
        """One independent RTDS WebSocket connection for a single topic.

        ``feed_name`` is used for log prefixes.  ``source_label`` is the
        ``source`` column value written into ``spot_price_buffer``.
        """
        reconnect_attempts = 0

        while True:
            session_started = time.monotonic()
            prior_data = False
            try:
                async with websockets.connect(
                    _RTDS_WS_URL,
                    max_size=10 * 1024 * 1024,
                    open_timeout=10,
                    close_timeout=5,
                    ping_interval=None,  # we manage PINGs manually per RTDS docs
                ) as ws:
                    configure_tcp_socket(ws, self.logger)
                    hb.reset()
                    self.logger.info("RTDS[%s]: connected, subscribing", feed_name)
                    await ws.send(rtds_subscribe_payload(topic))

                    async def _ping_loop() -> None:
                        while True:
                            await asyncio.sleep(_PING_INTERVAL_SECONDS)
                            try:
                                await ws.send("PING")
                            except Exception:
                                return

                    async def _recv_loop() -> None:
                        nonlocal prior_data
                        while True:
                            raw = await ws.recv()
                            update = parse_rtds_update(raw)
                            if update is None:
                                continue
                            if update.topic != topic:
                                continue
                            if update.symbol not in supported_symbols:
                                continue
                            cache[update.symbol] = (update.price, update.ts_ms)
                            self.spot_price_buffer.append({
                                "ts_ms": update.ts_ms,
                                "symbol": update.symbol,
                                "price": update.price,
                                "source": source_label,
                            })
                            hb.mark(update.symbol)
                            prior_data = True

                    async def _watchdog_loop() -> None:
                        while True:
                            await asyncio.sleep(WS_WATCHDOG_CHECK_INTERVAL_S)
                            stale = hb.stale_keys()
                            if stale:
                                keys_str = ", ".join(f"{k}({age:.1f}s)" for k, age in stale)
                                self.logger.warning(
                                    "RTDS[%s]: stale symbols (%s) — forcing reconnect",
                                    feed_name, keys_str,
                                )
                                raise _WatchdogStale(keys_str)

                    ping_task = asyncio.create_task(_ping_loop())
                    recv_task = asyncio.create_task(_recv_loop())
                    watch_task = asyncio.create_task(_watchdog_loop())
                    try:
                        done, _ = await asyncio.wait(
                            {recv_task, watch_task},
                            return_when=asyncio.FIRST_EXCEPTION,
                        )
                    finally:
                        for t in (ping_task, recv_task, watch_task):
                            if not t.done():
                                t.cancel()
                        await asyncio.gather(
                            ping_task, recv_task, watch_task,
                            return_exceptions=True,
                        )
                    for task in done:
                        exc = task.exception()
                        if exc is None:
                            continue
                        if isinstance(exc, _WatchdogStale):
                            raise exc
                        if isinstance(exc, websockets.exceptions.ConnectionClosedOK):
                            break
                        raise exc

                # Clean exit from `async with`.  Fast reconnect when data
                # was flowing; back off only when the server closed us out
                # without ever sending anything.
                session_duration = time.monotonic() - session_started
                if prior_data:
                    reconnect_attempts = 0
                    self.logger.info(
                        "RTDS[%s]: clean close after %.0fs session — immediate reconnect",
                        feed_name, session_duration,
                    )
                    continue
                delay = jittered_backoff(reconnect_attempts)
                reconnect_attempts += 1
                self.logger.warning(
                    "RTDS[%s]: clean close with no data — sleeping %.1fs",
                    feed_name, delay,
                )
                await asyncio.sleep(delay)

            except (asyncio.CancelledError, KeyboardInterrupt):
                return
            except _WatchdogStale as exc:
                # Stale data means the prior session WAS healthy up to
                # ``stale_after`` seconds ago → reset backoff.
                reconnect_attempts = 0
                delay = jittered_backoff(0)
                self.logger.warning(
                    "RTDS[%s]: watchdog stale (%s). Reconnecting in %.1fs",
                    feed_name, exc, delay,
                )
                await asyncio.sleep(delay)
            except Exception as exc:
                delay = jittered_backoff(reconnect_attempts)
                reconnect_attempts += 1
                self.logger.warning(
                    "RTDS[%s]: %s — %s. Reconnecting in %.1fs",
                    feed_name, type(exc).__name__, exc, delay,
                )
                await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run both RTDS feeds concurrently until cancelled."""
        binance_task = asyncio.create_task(
            self._run_feed(
                feed_name="binance",
                topic="crypto_prices",
                supported_symbols=_SUPPORTED_BINANCE_SYMBOLS,
                hb=self._binance_hb,
                cache=self.latest_prices,
                source_label="binance",
            )
        )
        chainlink_task = asyncio.create_task(
            self._run_feed(
                feed_name="chainlink",
                topic="crypto_prices_chainlink",
                supported_symbols=_SUPPORTED_CHAINLINK_SYMBOLS,
                hb=self._chainlink_hb,
                cache=self.chainlink_prices,
                source_label="chainlink",
            )
        )
        try:
            await asyncio.gather(binance_task, chainlink_task)
        except asyncio.CancelledError:
            pass
        finally:
            for t in (binance_task, chainlink_task):
                if not t.done():
                    t.cancel()
            await asyncio.gather(binance_task, chainlink_task, return_exceptions=True)
