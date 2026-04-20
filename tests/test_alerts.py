"""Unit tests for polymarket_pipeline.alerts."""
from __future__ import annotations

import os
import signal
import time
from typing import Any
from unittest import mock

import polymarket_pipeline.alerts as alerts


def test_send_alert_noop_when_webhook_unset(monkeypatch) -> None:
    """Module must be safe to call from production without a webhook configured."""
    monkeypatch.delenv("POLYMARKET_ALERT_WEBHOOK", raising=False)
    posted: list[Any] = []
    monkeypatch.setattr(alerts, "requests", mock.MagicMock(post=lambda *a, **kw: posted.append((a, kw))))
    alerts.send_alert("test", "hello")
    assert posted == []


def test_send_alert_posts_when_webhook_set(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_ALERT_WEBHOOK", "http://example.invalid/hook")
    posted: list[tuple[Any, Any]] = []

    def fake_post(url: str, **kw: Any) -> mock.MagicMock:
        posted.append((url, kw))
        return mock.MagicMock()

    monkeypatch.setattr(alerts, "requests", mock.MagicMock(post=fake_post))
    alerts.send_alert("test", "msg", extra={"k": 1})
    assert len(posted) == 1
    url, kw = posted[0]
    assert url == "http://example.invalid/hook"
    payload = kw["json"]
    assert payload["kind"] == "test"
    assert payload["msg"] == "msg"
    assert payload["extra"] == {"k": 1}
    assert payload["pid"] == os.getpid()
    assert kw["timeout"] == alerts._WEBHOOK_TIMEOUT_S


def test_send_alert_swallows_network_errors(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_ALERT_WEBHOOK", "http://example.invalid/hook")

    def raiser(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("DNS error")

    monkeypatch.setattr(alerts, "requests", mock.MagicMock(post=raiser))
    # Must not raise — alerting is best-effort.
    alerts.send_alert("test", "msg")


def test_send_alert_async_does_not_block(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_ALERT_WEBHOOK", "http://example.invalid/hook")
    started = time.monotonic()
    call_count = [0]

    def slow_post(*_a: Any, **_k: Any) -> None:
        time.sleep(0.5)
        call_count[0] += 1

    monkeypatch.setattr(alerts, "requests", mock.MagicMock(post=slow_post))
    alerts.send_alert_async("test", "msg")
    # If send_alert_async were synchronous, we'd block ~0.5s here.
    elapsed = time.monotonic() - started
    assert elapsed < 0.1, f"send_alert_async appears blocking: {elapsed:.3f}s"


def test_fire_startup_once_is_idempotent(monkeypatch) -> None:
    # Reset module state so each test run starts fresh.
    monkeypatch.setattr(alerts, "_startup_fired", False)
    monkeypatch.setenv("POLYMARKET_ALERT_WEBHOOK", "http://example.invalid/hook")
    posted: list[Any] = []

    def fake_post(url: str, **kw: Any) -> mock.MagicMock:
        posted.append((url, kw))
        return mock.MagicMock()

    monkeypatch.setattr(alerts, "requests", mock.MagicMock(post=fake_post))
    alerts.fire_startup_once("msg1")
    alerts.fire_startup_once("msg2")
    # The second call is suppressed by the module guard; give the threads
    # a moment to run, then check at most one POST happened.
    time.sleep(0.05)
    assert len(posted) == 1


def test_install_death_alerts_registers_signal_handlers(monkeypatch) -> None:
    # Reset the install guard.
    monkeypatch.setattr(alerts, "_installed", False)
    prev = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}
    try:
        alerts.install_death_alerts()
        # Our handler should be installed.  Default handlers are
        # ``signal.SIG_DFL`` or the original handler; our new one is not
        # equal to the captured previous value.
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            assert signal.getsignal(sig) is not prev[sig], f"{sig} handler not replaced"
    finally:
        for sig, h in prev.items():
            try:
                signal.signal(sig, h)
            except (OSError, ValueError):
                pass
