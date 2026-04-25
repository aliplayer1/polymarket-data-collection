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
