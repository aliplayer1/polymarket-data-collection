"""Best-effort webhook alerts + signal/atexit handlers.

Every failure class in the WebSocket pipeline that systemd cannot recover
from should surface as a webhook:

- process startup (once per run, after first data)
- process exit (atexit)
- SIGTERM / SIGINT / SIGHUP
- custom runtime events: buffer eviction, reconnect burst, data staleness

The webhook URL is read from ``POLYMARKET_ALERT_WEBHOOK``.  When unset,
every entry point here is a no-op, so production can run without alerting
configured and development doesn't spam a test channel.

Payload shape (JSON)::

    {"kind": str, "msg": str, "host": str, "ts": float,
     "pid": int, "extra": {...}}

``send_alert`` is synchronous with a 3 s timeout; use ``send_alert_async``
when called from the WS recv loop so a slow receiver cannot stall trading.
``SIGKILL`` and hard crashes bypass this module — rely on systemd
``OnFailure=polymarket-websocket-alert.service`` for those.
"""

from __future__ import annotations

import atexit
import logging
import os
import signal
import socket
import threading
import time
from typing import Any

try:
    import requests
    _HAVE_REQUESTS = True
except ImportError:
    _HAVE_REQUESTS = False


_ENV_WEBHOOK = "POLYMARKET_ALERT_WEBHOOK"
_ENV_HOSTNAME = "POLYMARKET_ALERT_HOST"
_WEBHOOK_TIMEOUT_S = 3.0

_install_guard = threading.Lock()
_installed = False
_startup_fired = False

_logger = logging.getLogger("polymarket_pipeline.alerts")


def _hostname() -> str:
    override = os.environ.get(_ENV_HOSTNAME)
    if override:
        return override
    try:
        return socket.gethostname()
    except OSError:
        return "unknown"


def send_alert(kind: str, msg: str, *, extra: dict[str, Any] | None = None) -> None:
    """Fire-and-forget POST.  Never raises — all errors are swallowed."""
    url = os.environ.get(_ENV_WEBHOOK, "").strip()
    if not url or not _HAVE_REQUESTS:
        return
    payload = {
        "kind": kind,
        "msg": msg,
        "host": _hostname(),
        "ts": time.time(),
        "pid": os.getpid(),
        "extra": extra or {},
    }
    try:
        requests.post(url, json=payload, timeout=_WEBHOOK_TIMEOUT_S)
    except Exception as exc:
        _logger.debug("alert webhook failed (ignored): %s", exc)


def send_alert_async(kind: str, msg: str, *, extra: dict[str, Any] | None = None) -> None:
    """Run ``send_alert`` in a short-lived daemon thread for hot-path callers."""
    threading.Thread(
        target=send_alert,
        args=(kind, msg),
        kwargs={"extra": extra},
        daemon=True,
    ).start()


def _on_signal(signum: int, _frame: Any) -> None:
    try:
        name = signal.Signals(signum).name
    except ValueError:
        name = f"signal_{signum}"
    send_alert("signal", f"received {name}")
    raise SystemExit(128 + signum)


def _on_exit() -> None:
    send_alert("exit", "process exited")


def install_death_alerts() -> None:
    """Register atexit + SIGTERM/SIGINT/SIGHUP handlers once per process.

    Must be called on the main thread — ``signal.signal`` rejects other
    threads on POSIX.  Safe to call multiple times.
    """
    global _installed
    with _install_guard:
        if _installed:
            return
        _installed = True

    atexit.register(_on_exit)
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        try:
            signal.signal(sig, _on_signal)
        except (OSError, ValueError, AttributeError) as exc:
            _logger.debug("could not install handler for %s: %s", sig, exc)


def fire_startup_once(msg: str = "startup complete") -> None:
    """Fire a ``kind="startup"`` webhook exactly once per process."""
    global _startup_fired
    with _install_guard:
        if _startup_fired:
            return
        _startup_fired = True
    send_alert_async("startup", msg)
