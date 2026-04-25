"""Unit tests for polymarket_pipeline.phases.ws_watchdog."""
from __future__ import annotations

import time

import pytest

from polymarket_pipeline.phases.ws_watchdog import (
    DataHeartbeat,
    DropOldestBuffer,
    ReconnectRateMonitor,
    jittered_backoff,
)


# --- DataHeartbeat ---------------------------------------------------------


def test_heartbeat_grace_period_suppresses_staleness() -> None:
    hb = DataHeartbeat({"k": 0.001}, grace_period_s=60.0)
    # Even though threshold is 1ms, we're within the grace period so
    # nothing should be stale yet.
    time.sleep(0.01)
    assert hb.stale_keys() == []


def test_heartbeat_flags_never_seen_past_threshold() -> None:
    # grace_period=0 but threshold=1ms so after 10ms it's stale.
    hb = DataHeartbeat({"k": 0.001}, grace_period_s=0.0)
    time.sleep(0.05)
    stale = hb.stale_keys()
    assert stale
    key, age = stale[0]
    assert key == "k"
    # Never-seen keys report age=inf so log consumers can distinguish
    # "key was never marked" from "key went stale after being seen".
    # Using session-uptime as the "age" for never-seen keys was
    # misleading and produced inflated outage durations in operator
    # logs.
    assert age == float("inf")


def test_heartbeat_has_seen_any() -> None:
    hb = DataHeartbeat({"a": 1.0, "b": 1.0})
    assert hb.has_seen_any() is False
    hb.mark("a")
    assert hb.has_seen_any() is True
    hb.reset()
    assert hb.has_seen_any() is False


def test_heartbeat_mark_clears_staleness() -> None:
    hb = DataHeartbeat({"k": 0.001}, grace_period_s=0.0)
    time.sleep(0.01)
    hb.mark("k")
    assert hb.stale_keys() == []


def test_heartbeat_age_is_none_before_first_mark() -> None:
    hb = DataHeartbeat({"k": 1.0}, grace_period_s=0.0)
    assert hb.age("k") is None


def test_heartbeat_reset_clears_and_restarts_grace() -> None:
    hb = DataHeartbeat({"k": 0.001}, grace_period_s=0.0)
    hb.mark("k")
    assert hb.age("k") is not None
    # simulate time passing so staleness triggers
    time.sleep(0.05)
    assert hb.stale_keys()
    # after reset, grace 0 still triggers for never-seen, so adjust
    hb2 = DataHeartbeat({"k": 1.0}, grace_period_s=1.0)
    hb2.mark("k")
    hb2.reset()
    assert hb2.age("k") is None
    assert hb2.stale_keys() == []  # within new grace period


# --- jittered_backoff ------------------------------------------------------


def test_backoff_respects_cap() -> None:
    # Very high attempt count — should be capped not exponential runaway.
    for attempts in range(0, 20):
        delay = jittered_backoff(attempts, base=1.0, cap=30.0)
        # jitter range is 0.5–1.5 ×
        assert 0.5 <= delay <= 45.0, f"delay {delay} out of bounds at {attempts}"


def test_backoff_first_attempt_is_small() -> None:
    # attempts=0 → base × 1 × jitter ∈ [0.5, 1.5]
    delays = [jittered_backoff(0, base=1.0, cap=30.0) for _ in range(50)]
    assert all(0.5 <= d <= 1.5 for d in delays)


# --- ReconnectRateMonitor --------------------------------------------------


def test_burst_monitor_fires_once_per_burst() -> None:
    m = ReconnectRateMonitor(threshold=3, window_s=60.0)
    assert not m.over_threshold("s0")
    m.record("s0")
    m.record("s0")
    assert not m.reserve_alert("s0")
    m.record("s0")
    assert m.over_threshold("s0")
    assert m.reserve_alert("s0")          # first observation
    assert not m.reserve_alert("s0")      # suppressed during same burst
    # Age out the events and record a fresh burst
    m._events["s0"] = []
    assert not m.reserve_alert("s0")
    for _ in range(3):
        m.record("s0")
    assert m.reserve_alert("s0")          # new burst alerts again


def test_burst_monitor_is_per_shard() -> None:
    m = ReconnectRateMonitor(threshold=2, window_s=60.0)
    m.record("s0")
    m.record("s0")
    assert m.reserve_alert("s0")
    # Other shard unaffected
    assert not m.reserve_alert("s1")


# --- DropOldestBuffer ------------------------------------------------------


def test_drop_oldest_buffer_drops_oldest_on_overflow() -> None:
    b = DropOldestBuffer(maxlen=3)
    for i in range(5):
        b.append(i)
    assert list(b) == [2, 3, 4]
    assert b.dropped_count == 2


def test_drop_oldest_buffer_drain_is_atomic_snapshot() -> None:
    b = DropOldestBuffer(maxlen=100)
    b.extend([1, 2, 3])
    snap = b.drain()
    assert snap == [1, 2, 3]
    assert len(b) == 0
    b.append(4)
    assert list(b) == [4]


def test_drop_oldest_buffer_extend_counts_drops() -> None:
    b = DropOldestBuffer(maxlen=2)
    b.extend([1, 2, 3, 4])
    assert list(b) == [3, 4]
    assert b.dropped_count == 2


def test_drop_oldest_buffer_rejects_nonpositive_maxlen() -> None:
    with pytest.raises(ValueError):
        DropOldestBuffer(maxlen=0)
    with pytest.raises(ValueError):
        DropOldestBuffer(maxlen=-1)


def test_drop_oldest_buffer_clear_resets_contents_not_drop_count() -> None:
    b = DropOldestBuffer(maxlen=2)
    b.extend([1, 2, 3])
    assert b.dropped_count == 1
    b.clear()
    assert len(b) == 0
    # drop count is cumulative across clears (observability aid)
    assert b.dropped_count == 1
