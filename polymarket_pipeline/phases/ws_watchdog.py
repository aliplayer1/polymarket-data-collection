"""DataHeartbeat — per-key last-seen tracker with per-key staleness thresholds.

TCP-alive does not imply data-alive.  Both Polymarket WebSockets (CLOB and
RTDS) have been observed to keep a connection open while silently dropping
a subset of expected messages — e.g. RTDS continues delivering Chainlink
but stops sending ``btcusdt``, or a CLOB shard stays subscribed but emits
no ``price_change`` for minutes.

``DataHeartbeat`` is the data-level side of that two-layer watchdog:

    hb = DataHeartbeat({"btcusdt": 30.0, "ethusdt": 30.0, "btc/usd": 120.0})

    async def watcher() -> None:
        while True:
            await asyncio.sleep(5.0)
            stale = hb.stale_keys()
            if stale:
                raise ConnectionResetError(f"stale: {stale}")

    # inside recv loop:
    hb.mark("btcusdt")

A short grace period after construction (default 30 s) prevents false
positives at connect time before any data has flowed.  ``reset()`` is
called by the outer reconnect loop so the grace period restarts on every
new session.
"""

from __future__ import annotations

import collections
import logging
import random
import socket
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from ..config import (
    WS_RECONNECT_BASE_S,
    WS_RECONNECT_CAP_S,
    WS_RECONNECT_JITTER_HI,
    WS_RECONNECT_JITTER_LO,
)


@dataclass
class DataHeartbeat:
    """Track last-seen timestamps per key; flag stale keys.

    Parameters
    ----------
    stale_after
        Map of ``key -> max_age_seconds``.  A key becomes stale when its
        last-seen age exceeds its threshold (after the grace period).
    grace_period_s
        Seconds after construction / reset during which no key is reported
        as stale.  Prevents "never-seen" false positives at connect.
    """

    stale_after: dict[str, float]
    grace_period_s: float = 30.0
    _last_seen: dict[str, float] = field(default_factory=dict)
    _installed_at: float = field(default_factory=time.monotonic)
    _grace_extended: bool = False

    def mark(self, key: str) -> None:
        """Record that ``key`` just delivered data.

        On the very first ``mark`` after a ``reset`` (or construction)
        we re-anchor the grace period to *now*.  This rolls the
        watchdog forward past any subscription-confirmation delay that
        consumed part of the original grace window, so the first valid
        message — not the connection moment — defines when keys must
        start delivering data.  Subsequent ``mark`` calls leave the
        grace anchor alone so the staleness check works normally.
        """
        now = time.monotonic()
        if not self._grace_extended:
            self._grace_extended = True
            self._installed_at = now
        self._last_seen[key] = now

    def age(self, key: str) -> float | None:
        """Return seconds since ``key`` was last seen, or None if never."""
        ts = self._last_seen.get(key)
        if ts is None:
            return None
        return time.monotonic() - ts

    def has_seen_any(self) -> bool:
        """Return True iff any registered key has been marked at least once.

        Useful as a "prior_data" signal for the outer reconnect loop —
        distinguishes "session was healthy then went stale" (reset
        backoff) from "session never delivered anything" (keep growing
        backoff to avoid hammering a misconfigured subscription).
        """
        return bool(self._last_seen)

    def stale_keys(self) -> list[tuple[str, float]]:
        """Return ``[(key, age_seconds)]`` for every key past its threshold.

        Returns ``[]`` during the grace period.  Keys that have never been
        seen are flagged only after ``max(threshold, grace_period_s)`` has
        elapsed since construction / reset; their reported "age" is
        ``inf`` so log consumers can distinguish "stale" (real staleness)
        from "never seen" (session uptime, often misleadingly large).
        """
        now = time.monotonic()
        elapsed = now - self._installed_at
        if elapsed < self.grace_period_s:
            return []
        stale: list[tuple[str, float]] = []
        for key, threshold in self.stale_after.items():
            last = self._last_seen.get(key)
            if last is None:
                if elapsed > max(threshold, self.grace_period_s):
                    stale.append((key, float("inf")))
            else:
                age = now - last
                if age > threshold:
                    stale.append((key, age))
        return stale

    def reset(self) -> None:
        """Forget all last-seen entries and restart the grace period."""
        self._last_seen.clear()
        self._installed_at = time.monotonic()
        self._grace_extended = False


# --- Reconnect helpers (shared by CLOB + RTDS) ---------------------------


def jittered_backoff(
    attempts: int,
    *,
    base: float = WS_RECONNECT_BASE_S,
    cap: float = WS_RECONNECT_CAP_S,
) -> float:
    """Exponential backoff capped at ``cap`` with multiplicative jitter.

    ``delay = random.uniform(jitter_lo, jitter_hi) * min(base * 2^attempts, cap)``

    Jitter prevents thundering-herd reconnects when many shards lose their
    connections simultaneously (common during Polymarket-side incidents).
    """
    raw = min(base * (2 ** max(0, attempts)), cap)
    return raw * random.uniform(WS_RECONNECT_JITTER_LO, WS_RECONNECT_JITTER_HI)


def configure_tcp_socket(ws: Any, logger: logging.Logger) -> int | None:
    """Enable TCP_NODELAY + aggressive keepalive on the underlying socket.

    The ``websockets`` library does not enable these by default.  Missing
    TCP_KEEPALIVE means a half-open connection (NAT timeout, peer power
    loss) only surfaces when the next application-level ping fails —
    which can be 40 s+ after the link actually died.

    Returns the socket's fd on success, None when the transport doesn't
    expose a raw socket (tests, in-memory transports).  Silently ignores
    platform-specific options that aren't available.
    """
    try:
        sock = ws.transport.get_extra_info("socket")
    except Exception:
        return None
    if sock is None:
        return None
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except OSError as exc:
        logger.debug("TCP_NODELAY/SO_KEEPALIVE setsockopt failed: %s", exc)
    for opt_name, value in (
        ("TCP_KEEPIDLE", 15),
        ("TCP_KEEPINTVL", 5),
        ("TCP_KEEPCNT", 3),
    ):
        opt = getattr(socket, opt_name, None)
        if opt is None:
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, opt, value)
        except OSError:
            pass
    try:
        return sock.fileno()
    except OSError:
        return None


@dataclass
class ReconnectRateMonitor:
    """Track reconnects per shard and flag bursts.

    A burst is ``>= threshold`` reconnects within the last ``window_s``
    seconds.  Clients call :meth:`record` on every reconnect and
    :meth:`reserve_alert` to get a one-shot alert per burst — the same
    burst will not re-fire until the shard's rate drops below the
    threshold and then exceeds it again.
    """

    threshold: int
    window_s: float
    _events: dict[str, list[float]] = field(default_factory=dict)
    _alert_active: dict[str, bool] = field(default_factory=dict)

    def _prune(self, shard_key: str) -> list[float]:
        now = time.monotonic()
        cutoff = now - self.window_s
        events = self._events.setdefault(shard_key, [])
        while events and events[0] < cutoff:
            events.pop(0)
        return events

    def record(self, shard_key: str) -> int:
        """Record a reconnect; return current burst count for this shard."""
        events = self._prune(shard_key)
        events.append(time.monotonic())
        return len(events)

    def over_threshold(self, shard_key: str) -> bool:
        return len(self._prune(shard_key)) >= self.threshold

    def reserve_alert(self, shard_key: str) -> bool:
        """Return True exactly once per ongoing burst for ``shard_key``.

        Resets when the shard drops below threshold, so a subsequent burst
        will alert again.
        """
        if not self.over_threshold(shard_key):
            self._alert_active.pop(shard_key, None)
            return False
        if self._alert_active.get(shard_key):
            return False
        self._alert_active[shard_key] = True
        return True


class DropOldestBuffer:
    """Bounded list-like buffer with drop-oldest overflow semantics.

    Backed by :class:`collections.deque` with ``maxlen`` — appending past
    capacity silently pops the leftmost (oldest) element, so producers
    never block and the buffer can never OOM.  Maintains a running
    ``dropped_count`` so the flush loop can alert on sustained
    backpressure (continuous drop-one-per-append, not cliff-style
    emergency eviction).

    Usage within a single asyncio event loop is safe — ``drain()`` is
    snapshot-then-clear with no intervening ``await`` so producer
    coroutines cannot race.  Not thread-safe.
    """

    __slots__ = ("_buf", "_maxlen", "dropped_count")

    def __init__(self, maxlen: int):
        if maxlen <= 0:
            raise ValueError(f"maxlen must be positive, got {maxlen}")
        self._buf: collections.deque = collections.deque(maxlen=maxlen)
        self._maxlen = maxlen
        self.dropped_count = 0

    def append(self, item: Any) -> None:
        if len(self._buf) == self._maxlen:
            self.dropped_count += 1
        self._buf.append(item)

    def extend(self, items: Iterable[Any]) -> None:
        for it in items:
            self.append(it)

    def requeue(self, items: Iterable[Any]) -> None:
        """Re-insert items previously drained by the flush loop.

        Identical to ``extend`` but does NOT update ``dropped_count`` on
        overflow.  These bytes were ours to begin with — counting them
        as producer drops produced phantom alerts when a flush failure
        re-queued more rows than the buffer could hold (sustained
        backpressure during transient disk errors).  Producer-side
        ``append`` still increments the counter normally.
        """
        for it in items:
            if len(self._buf) == self._maxlen:
                # Drop the oldest silently — same FIFO eviction as
                # ``append``, but the count stays as-is.
                pass
            self._buf.append(it)

    def __len__(self) -> int:
        return len(self._buf)

    def clear(self) -> None:
        self._buf.clear()

    def drain(self) -> list:
        """Snapshot the current contents and empty the buffer atomically."""
        snapshot = list(self._buf)
        self._buf.clear()
        return snapshot

    def __iter__(self):
        return iter(self._buf)
