"""Unit tests for ``scripts/audit_ws_gaps.py`` exit-code policy.

The script's exit code controls whether systemd marks the daily audit
unit as failed (and fires the ``OnFailure=`` chain).  Routine
restart-induced heartbeat gaps must NOT trip this — only opt-in
thresholds (``--fail-on-gap-ms``, ``--fail-on-bursts``) should.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "audit_ws_gaps.py"

# Importing the script as a module lets us call ``main()`` and capture
# its return code directly, without spawning subprocesses.
sys.path.insert(0, str(ROOT / "scripts"))
import audit_ws_gaps as audit  # noqa: E402


def _write_heartbeats(data_dir: Path, *, gap_pairs: list[tuple[int, int]]) -> None:
    """Write a heartbeats parquet shard with the given (ts_ms, ts_ms) pairs.

    Each pair becomes two rows for the same (source, shard_key, event_type),
    so the spacing between them shows up as a gap.
    """
    rows = []
    for start_ms, end_ms in gap_pairs:
        rows.append({
            "ts_ms": start_ms, "source": "clob_ws", "shard_key": "0",
            "event_type": "price_change", "last_event_age_ms": 500,
        })
        rows.append({
            "ts_ms": end_ms, "source": "clob_ws", "shard_key": "0",
            "event_type": "price_change", "last_event_age_ms": 500,
        })
    df = pd.DataFrame(rows)
    hb_dir = data_dir / "heartbeats"
    hb_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), str(hb_dir / "test.parquet"))


def _run(monkeypatch, argv: list[str]) -> int:
    monkeypatch.setattr(sys, "argv", ["audit_ws_gaps.py", *argv])
    return audit.main()


def test_default_exit_is_zero_even_with_gaps(monkeypatch, tmp_path):
    """Routine restart-induced gaps must not fail the systemd unit.
    The default (``--fail-on-gap-ms 0``, ``--fail-on-bursts`` unset)
    must exit 0 even when the manifest contains findings.
    """
    _write_heartbeats(tmp_path, gap_pairs=[(1_700_000_000_000, 1_700_000_060_000)])
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    rc = _run(monkeypatch, [
        "--data-dir", str(tmp_path),
        "--log-dir", str(log_dir),
    ])
    assert rc == 0


def test_fail_on_gap_ms_below_threshold_exits_zero(monkeypatch, tmp_path):
    """Gaps smaller than ``--fail-on-gap-ms`` don't promote to failure."""
    # 60 s gap, threshold 90 s → exit 0
    _write_heartbeats(tmp_path, gap_pairs=[(1_700_000_000_000, 1_700_000_060_000)])
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    rc = _run(monkeypatch, [
        "--data-dir", str(tmp_path),
        "--log-dir", str(log_dir),
        "--fail-on-gap-ms", "90000",
    ])
    assert rc == 0


def test_fail_on_gap_ms_above_threshold_exits_one(monkeypatch, tmp_path):
    """A gap that exceeds ``--fail-on-gap-ms`` must exit 1 so systemd's
    ``OnFailure=`` fires.  This is the path that catches major outages
    (e.g. the 6-hour threshold the production unit uses).
    """
    # 60 s gap, threshold 30 s → exit 1
    _write_heartbeats(tmp_path, gap_pairs=[(1_700_000_000_000, 1_700_000_060_000)])
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    rc = _run(monkeypatch, [
        "--data-dir", str(tmp_path),
        "--log-dir", str(log_dir),
        "--fail-on-gap-ms", "30000",
    ])
    assert rc == 1


def test_no_heartbeats_no_findings_exits_zero(monkeypatch, tmp_path):
    """Empty heartbeats directory: no gaps, no failure."""
    (tmp_path / "heartbeats").mkdir()
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    rc = _run(monkeypatch, [
        "--data-dir", str(tmp_path),
        "--log-dir", str(log_dir),
        "--fail-on-gap-ms", "30000",
        "--fail-on-bursts",
    ])
    assert rc == 0


def test_fail_window_hours_excludes_historical_gap(monkeypatch, tmp_path):
    """A gap that closed before the fail-window must not promote to a
    non-zero exit.  This is the path that prevents a one-time outage
    from re-tripping OnFailure on every subsequent daily run.
    """
    import time as _t
    now_ms = int(_t.time() * 1000)
    # Historical gap: closed ~5 days ago, much larger than the
    # fail-on-gap-ms threshold.  With a 48 h fail-window the gap is
    # filtered out → exit 0.
    historical_start = now_ms - 6 * 86400 * 1000
    historical_end = now_ms - 5 * 86400 * 1000
    _write_heartbeats(tmp_path, gap_pairs=[(historical_start, historical_end)])
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    # Without the recency filter, a 30s threshold catches the giant gap.
    rc_strict = _run(monkeypatch, [
        "--data-dir", str(tmp_path),
        "--log-dir", str(log_dir),
        "--fail-on-gap-ms", "30000",
    ])
    assert rc_strict == 1, "without --fail-window-hours, historical gap fails"

    # With a 48 h recency window, the historical gap is excluded.
    rc_with_window = _run(monkeypatch, [
        "--data-dir", str(tmp_path),
        "--log-dir", str(log_dir),
        "--fail-on-gap-ms", "30000",
        "--fail-window-hours", "48",
    ])
    assert rc_with_window == 0, "48 h window must exclude a 5-day-old gap"


def test_fail_window_hours_includes_recent_gap(monkeypatch, tmp_path):
    """A gap that closed within the fail-window still trips the fail
    check.  This protects against silently ignoring a real recent
    outage just because the recency filter is in play.
    """
    import time as _t
    now_ms = int(_t.time() * 1000)
    # Recent gap that ended 1 hour ago, exceeds threshold.
    recent_start = now_ms - 2 * 3600 * 1000
    recent_end = now_ms - 1 * 3600 * 1000
    _write_heartbeats(tmp_path, gap_pairs=[(recent_start, recent_end)])
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    rc = _run(monkeypatch, [
        "--data-dir", str(tmp_path),
        "--log-dir", str(log_dir),
        "--fail-on-gap-ms", "30000",
        "--fail-window-hours", "48",
    ])
    assert rc == 1, "recent gap inside the window must still fail the check"


def test_diff_mode_alerts_once_then_quiets(monkeypatch, tmp_path):
    """With ``--diff-mode``, the FIRST run records the baseline and
    fires (one-shot) on findings.  The SECOND run sees the same
    findings → suppressed.  This is the path that prevents a
    historical outage from re-tripping OnFailure forever.
    """
    import time as _t
    now_ms = int(_t.time() * 1000)
    # A gap that exceeds threshold, ended 1 h ago (within window).
    _write_heartbeats(tmp_path, gap_pairs=[
        (now_ms - 2 * 3600 * 1000, now_ms - 1 * 3600 * 1000),
    ])
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    args = [
        "--data-dir", str(tmp_path),
        "--log-dir", str(log_dir),
        "--fail-on-gap-ms", "30000",
        "--fail-window-hours", "48",
        "--diff-mode",
    ]
    rc1 = _run(monkeypatch, args)
    assert rc1 == 1, "first run must fire on the new gap"

    rc2 = _run(monkeypatch, args)
    assert rc2 == 0, "second run with no new gaps must exit clean"


def test_diff_mode_fires_on_genuinely_new_finding(monkeypatch, tmp_path):
    """Diff-mode is permissive — once a NEW gap shows up that wasn't
    in the previous run, the fail check fires on it.
    """
    import time as _t
    now_ms = int(_t.time() * 1000)

    # First run: one historical gap (ends well within the window).
    _write_heartbeats(tmp_path, gap_pairs=[
        (now_ms - 4 * 3600 * 1000, now_ms - 3 * 3600 * 1000),
    ])
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    args = [
        "--data-dir", str(tmp_path),
        "--log-dir", str(log_dir),
        "--fail-on-gap-ms", "30000",
        "--fail-window-hours", "48",
        "--diff-mode",
    ]
    rc1 = _run(monkeypatch, args)
    assert rc1 == 1

    # Second run: the original gap PLUS a new one with a different
    # start_ms / end_ms.  The new one alone must trip the fail check.
    hb_dir = tmp_path / "heartbeats"
    for f in hb_dir.iterdir():
        f.unlink()
    _write_heartbeats(tmp_path, gap_pairs=[
        (now_ms - 4 * 3600 * 1000, now_ms - 3 * 3600 * 1000),  # same as run 1
        (now_ms - 90 * 60 * 1000, now_ms - 30 * 60 * 1000),    # new
    ])

    rc2 = _run(monkeypatch, args)
    assert rc2 == 1, "diff-mode must still fail on a genuinely new finding"
