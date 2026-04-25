#!/usr/bin/env python3
"""Scan WebSocket heartbeats + reconnect log for data gaps.

Reads:
- ``<data_root>/heartbeats/`` — consolidated + staging parquet files
- ``<log_dir>/ws_reconnects.jsonl`` (+ rotated backups) — reconnect events

Writes:
- ``<data_root>/.gap_manifest.json`` — structured summary of gaps found
- stdout — human-readable report

Optionally POSTs the manifest to ``POLYMARKET_ALERT_WEBHOOK`` when at
least one gap or reconnect burst is found (controlled by --alert).

Usage::

    # Default: scan ./data/heartbeats + ./logs/ws_reconnects.jsonl,
    # write manifest to ./data/.gap_manifest.json
    python scripts/audit_ws_gaps.py

    # Custom locations and alert on findings
    python scripts/audit_ws_gaps.py --data-dir /mnt/polymarket \\
        --log-dir /var/log/polymarket --alert

    # Tighter thresholds (e.g. for operational dashboards)
    python scripts/audit_ws_gaps.py --heartbeat-gap-ms 20000 \\
        --burst-threshold 3 --burst-window-s 30
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

# Make the polymarket_pipeline package importable when the script is
# launched from the repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load_heartbeats(heartbeats_dir: Path) -> list[dict]:
    """Load every parquet shard under ``heartbeats_dir`` into a flat list."""
    if not heartbeats_dir.exists():
        return []
    import pyarrow.parquet as pq
    rows: list[dict] = []
    for path in sorted(heartbeats_dir.glob("*.parquet")):
        try:
            table = pq.ParquetFile(path).read()
            rows.extend(table.to_pylist())
        except Exception as exc:
            print(f"WARN: could not read {path}: {exc}", file=sys.stderr)
    return rows


def _scan_heartbeat_gaps(
    rows: list[dict],
    max_gap_ms: int,
) -> list[dict]:
    """Find inter-heartbeat gaps exceeding ``max_gap_ms`` per series.

    A series is the tuple ``(source, shard_key, event_type)``.  Emits one
    gap record per run of ≥ threshold silence.
    """
    if not rows:
        return []
    # Group by series, keep ts_ms sorted
    series: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    for row in rows:
        key = (str(row.get("source", "")), str(row.get("shard_key", "")),
               str(row.get("event_type", "")))
        ts = int(row.get("ts_ms", 0) or 0)
        if ts > 0:
            series[key].append(ts)
    gaps: list[dict] = []
    for (source, shard_key, event_type), timestamps in series.items():
        timestamps.sort()
        for prev, curr in zip(timestamps, timestamps[1:]):
            delta = curr - prev
            if delta > max_gap_ms:
                gaps.append({
                    "source": source,
                    "shard_key": shard_key,
                    "event_type": event_type,
                    "start_ms": prev,
                    "end_ms": curr,
                    "gap_ms": delta,
                    "max_gap_ms": max_gap_ms,
                })
    gaps.sort(key=lambda g: g["gap_ms"], reverse=True)
    return gaps


def _scan_reconnect_bursts(
    log_dir: Path,
    *,
    threshold: int,
    window_s: float,
) -> list[dict]:
    """Find time windows where a shard saw ≥ ``threshold`` reconnects.

    Reads ``ws_reconnects.jsonl`` and all rotated backups (``.jsonl.1`` etc).
    A burst is emitted once per shard per ongoing run above threshold —
    the window is coalesced so a sustained storm produces one record, not N.
    """
    # Sort rotated logs by modification time so older files are read first.
    # Lexicographic order misorders ``RotatingFileHandler`` outputs once
    # the rotation index reaches double digits (``.10`` sorts before
    # ``.2``).  Per-line timestamps make this purely cosmetic for the
    # gap-scan output, but mtime sort is the correct contract.
    paths = sorted(
        log_dir.glob("ws_reconnects.jsonl*"),
        key=lambda p: p.stat().st_mtime,
    )
    if not paths:
        return []
    per_shard: dict[str, list[float]] = defaultdict(list)
    for path in paths:
        try:
            with path.open() as fh:
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        record = json.loads(raw)
                    except ValueError:
                        continue
                    ts = float(record.get("ts", 0) or 0)
                    shard = str(record.get("shard_key", ""))
                    if ts > 0 and shard:
                        per_shard[shard].append(ts)
        except OSError as exc:
            print(f"WARN: could not read {path}: {exc}", file=sys.stderr)
    bursts: list[dict] = []
    for shard, timestamps in per_shard.items():
        timestamps.sort()
        window: deque[float] = deque()
        in_burst = False
        burst_start = 0.0
        burst_count = 0
        for ts in timestamps:
            while window and ts - window[0] > window_s:
                window.popleft()
            window.append(ts)
            if len(window) >= threshold:
                if not in_burst:
                    in_burst = True
                    burst_start = window[0]
                    burst_count = len(window)
                else:
                    burst_count = max(burst_count, len(window))
            elif in_burst:
                bursts.append({
                    "shard_key": shard,
                    "count": burst_count,
                    "window_start_ts": burst_start,
                    "window_end_ts": ts,
                    "threshold": threshold,
                    "window_s": window_s,
                })
                in_burst = False
        if in_burst:
            bursts.append({
                "shard_key": shard,
                "count": burst_count,
                "window_start_ts": burst_start,
                "window_end_ts": timestamps[-1],
                "threshold": threshold,
                "window_s": window_s,
            })
    bursts.sort(key=lambda b: b["count"], reverse=True)
    return bursts


def _send_alert(manifest: dict) -> None:
    """POST the manifest to ``POLYMARKET_ALERT_WEBHOOK`` if set.

    Swallows errors — alerting must never prevent the audit from
    completing.
    """
    url = os.environ.get("POLYMARKET_ALERT_WEBHOOK", "").strip()
    if not url:
        return
    try:
        import requests
        requests.post(
            url,
            json={"kind": "gap_audit", "msg": "WS gap manifest non-empty", "extra": manifest},
            timeout=3,
        )
    except Exception as exc:
        print(f"WARN: alert webhook failed: {exc}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan WS heartbeats + reconnects for gaps")
    parser.add_argument("--data-dir", default="data",
                        help="Root data directory (contains heartbeats/ and .gap_manifest.json)")
    parser.add_argument("--log-dir", default=None,
                        help="Log directory containing ws_reconnects.jsonl (defaults to ./logs/)")
    parser.add_argument("--heartbeat-gap-ms", type=int, default=30_000,
                        help="Emit a gap when heartbeat spacing exceeds this (default: 30000 ms)")
    parser.add_argument("--burst-threshold", type=int, default=5,
                        help="Reconnects per window that trigger a burst record (default: 5)")
    parser.add_argument("--burst-window-s", type=float, default=60.0,
                        help="Burst window in seconds (default: 60)")
    parser.add_argument("--alert", action="store_true",
                        help="POST the manifest to POLYMARKET_ALERT_WEBHOOK when findings exist")
    # Exit-code policy.  By default the script exits 0 whenever the audit
    # itself runs to completion — finding gaps or bursts is the EXPECTED
    # output of a daily audit, not a script failure.  Two opt-in
    # thresholds let operators promote specific findings to a non-zero
    # exit so systemd's ``OnFailure=`` can fire on truly exceptional
    # conditions without firing on every benign restart-induced gap.
    parser.add_argument(
        "--fail-on-gap-ms",
        type=int,
        default=0,
        metavar="MS",
        help=(
            "Exit non-zero when ANY heartbeat gap exceeds this many ms. "
            "Default 0 = never fail on gaps.  Recommended: ~6 hours "
            "(21_600_000 ms) to flag major outages while ignoring "
            "routine reconnects."
        ),
    )
    parser.add_argument(
        "--fail-on-bursts",
        action="store_true",
        default=False,
        help=(
            "Exit non-zero when any reconnect-burst events are present "
            "in the manifest (sustained >threshold reconnects per "
            "burst-window).  Default off."
        ),
    )
    parser.add_argument(
        "--fail-window-hours",
        type=float,
        default=0.0,
        metavar="HOURS",
        help=(
            "Restrict the fail-on-* check to gaps/bursts that closed "
            "(or are currently ongoing) within the last N hours.  "
            "Default 0 = consider all findings.  Recommended: 48 for "
            "a daily audit — a once-real outage stops re-tripping "
            "OnFailure forever after the heartbeat data accumulates "
            "post-incident.  All findings still go in the manifest "
            "regardless of this flag."
        ),
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir).expanduser().resolve()
    log_dir = Path(args.log_dir).expanduser().resolve() if args.log_dir else Path("logs").resolve()
    heartbeats_dir = data_dir / "heartbeats"

    rows = _load_heartbeats(heartbeats_dir)
    gaps = _scan_heartbeat_gaps(rows, max_gap_ms=args.heartbeat_gap_ms)
    bursts = _scan_reconnect_bursts(
        log_dir,
        threshold=args.burst_threshold,
        window_s=args.burst_window_s,
    )

    ts_ms_min = min((int(row.get("ts_ms", 0) or 0) for row in rows), default=0)
    ts_ms_max = max((int(row.get("ts_ms", 0) or 0) for row in rows), default=0)

    manifest = {
        "scan_ts": time.time(),
        "data_dir": str(data_dir),
        "log_dir": str(log_dir),
        "window": {"ts_ms_min": ts_ms_min, "ts_ms_max": ts_ms_max, "rows": len(rows)},
        "thresholds": {
            "heartbeat_gap_ms": args.heartbeat_gap_ms,
            "burst_threshold": args.burst_threshold,
            "burst_window_s": args.burst_window_s,
        },
        "gaps": gaps,
        "reconnect_bursts": bursts,
    }

    manifest_path = data_dir / ".gap_manifest.json"
    data_dir.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w") as fh:
        json.dump(manifest, fh, indent=2)

    # Human-readable summary
    print(f"Scanned {len(rows)} heartbeat rows from {heartbeats_dir}")
    print(f"Heartbeat gaps (>{args.heartbeat_gap_ms} ms): {len(gaps)}")
    for g in gaps[:5]:
        print(f"  {g['source']}/{g['shard_key']}/{g['event_type']}: "
              f"{g['gap_ms']} ms between ts_ms={g['start_ms']} and {g['end_ms']}")
    print(f"Reconnect bursts (≥{args.burst_threshold}/{args.burst_window_s:.0f}s): {len(bursts)}")
    for b in bursts[:5]:
        print(f"  {b['shard_key']}: {b['count']} reconnects around ts={b['window_start_ts']:.0f}")
    print(f"Manifest written to {manifest_path}")

    if args.alert and (gaps or bursts):
        _send_alert(manifest)

    # Exit-code policy: a successful audit always exits 0 (the manifest
    # was written and any findings were logged + webhooked).  Promote
    # to a non-zero exit ONLY when the operator explicitly asked us to
    # via ``--fail-on-bursts`` / ``--fail-on-gap-ms``, so systemd's
    # ``OnFailure=`` only fires on truly exceptional conditions.
    #
    # ``--fail-window-hours`` further narrows the fail check to recent
    # findings, so a historical outage doesn't re-trip OnFailure on
    # every subsequent run forever.
    fail_gaps = gaps
    fail_bursts = bursts
    if args.fail_window_hours > 0:
        cutoff_ms = int((time.time() - args.fail_window_hours * 3600.0) * 1000.0)
        fail_gaps = [g for g in gaps if int(g.get("end_ms", 0)) >= cutoff_ms]
        # Burst records carry ``window_start_ts`` / ``window_end_ts``
        # in seconds (monotonic-flavoured ts).  Use ``window_end_ts``
        # if present; otherwise the burst is undated and we keep it.
        cutoff_s = time.time() - args.fail_window_hours * 3600.0
        fail_bursts = [
            b for b in bursts
            if "window_end_ts" not in b
            or float(b["window_end_ts"]) >= cutoff_s
        ]
        skipped_gaps = len(gaps) - len(fail_gaps)
        skipped_bursts = len(bursts) - len(fail_bursts)
        if skipped_gaps or skipped_bursts:
            print(
                f"Recency filter (--fail-window-hours {args.fail_window_hours}): "
                f"ignoring {skipped_gaps} historical gap(s) and "
                f"{skipped_bursts} historical burst(s) for the fail check"
            )

    fail = False
    if args.fail_on_bursts and fail_bursts:
        print(f"FAIL: {len(fail_bursts)} reconnect burst(s) present (--fail-on-bursts)")
        fail = True
    if args.fail_on_gap_ms > 0:
        large = [g for g in fail_gaps if int(g.get("gap_ms", 0)) > args.fail_on_gap_ms]
        if large:
            print(
                f"FAIL: {len(large)} gap(s) exceed {args.fail_on_gap_ms} ms "
                f"(--fail-on-gap-ms)"
            )
            fail = True
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
