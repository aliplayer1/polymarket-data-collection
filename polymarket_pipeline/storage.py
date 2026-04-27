"""Normalized Parquet storage layer and Hugging Face Hub upload.

Data is split into three tables:
  - **markets** (one row per market): market_id, question, crypto, timeframe, volume, resolution
  - **prices**  (one row per price-change event, ~60s median gap):
                  market_id, timestamp, up_price, down_price
  - **ticks**   (one row per on-chain trade fill, second-level precision):
                  market_id, timestamp_ms, token_id, outcome, side, price, size_usdc, tx_hash, block_number

Prices and ticks are Hive-partitioned on disk as  {table}/crypto=X/timeframe=Y/*.parquet
and stored with Zstandard compression + optimised dtypes.

Concurrency model
-----------------
Both `persist_normalized` and `persist_ticks` may be called from multiple threads
(e.g. `run_in_executor` flush coroutine) **and** from a separate OS process
(e.g. `polymarket-historical.service` running alongside `polymarket-websocket.service`).

Protection is two-layered:
  1. `threading.Lock` per canonical data-root — prevents races within one process.
  2. `fcntl.flock(LOCK_EX)` on a `.write.lock` sentinel file — prevents races across
     processes on the same host.  On non-Unix platforms (no `fcntl`) the file lock
     degrades to a no-op; the thread lock alone is still sufficient for in-process
     safety.

WebSocket tick staging
----------------------
The live WebSocket service uses `append_ws_ticks_staged()` instead of `persist_ticks()`
for its 5-second flush cycle.  This avoids loading the full consolidated ticks partition
(potentially millions of rows) just to append a small WS batch.  Instead, each
(crypto, timeframe) shard gets a lightweight ``ws_staging.parquet`` sidecar that
accumulates at most a few hours of WS fills (typically < 1 MB per partition).

The next `persist_ticks()` call (i.e. the --historical-only pass, every 6 h) reads
every *.parquet file in the partition via ``pq.ParquetDataset`` — including the
staging file — merges them all, deduplicates, and writes a single consolidated file
via ``_write_partitioned_atomic()``, which atomically replaces the stable
``part-{i}.parquet`` files and cleans up any stale shards (including
``ws_staging.parquet``) afterward.
"""

from __future__ import annotations

import errno as _errno
import logging
import os
import re
import shutil
import tempfile
import threading
import time
from contextlib import contextmanager
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import (
    HF_REPO_ID,
    PARQUET_MARKETS_PATH,
    PARQUET_PRICES_DIR,
    PARQUET_TICKS_DIR,
)

# ---------------------------------------------------------------------------
# Cross-process file locking (fcntl, Unix only)
# ---------------------------------------------------------------------------

try:
    import fcntl as _fcntl
    _HAVE_FCNTL = True
except ImportError:
    _HAVE_FCNTL = False  # Windows / non-Unix: thread lock still protects in-process


# Per canonical-path threading locks (in-process thread safety)
_WRITE_LOCKS: dict[str, threading.RLock] = {}
_WRITE_LOCKS_REGISTRY_LOCK = threading.Lock()

# fcntl deadlock prevention
_FLOCK_TIMEOUT_SECONDS: float = float(os.environ.get("PM_FLOCK_TIMEOUT", "300"))
_FLOCK_POLL_INTERVAL = 0.05   # seconds between LOCK_NB retry attempts

# Thread-local storage to track lock recursion depth.  This allows _write_lock
# to be reentrant within the same thread, preventing self-deadlocks when
# nested calls (e.g. upload_to_huggingface -> consolidate_ticks) occur.
_LOCK_STATE = threading.local()


def _get_thread_lock(canonical_root: str) -> threading.RLock:
    with _WRITE_LOCKS_REGISTRY_LOCK:
        if canonical_root not in _WRITE_LOCKS:
            _WRITE_LOCKS[canonical_root] = threading.RLock()
        return _WRITE_LOCKS[canonical_root]


def _acquire_flock(fd: Any, lock_path: str, *, timeout: float | None = None) -> None:
    """Acquire LOCK_EX with stale-PID detection and a hard timeout.

    Unlike plain ``flock(LOCK_EX)``, this function never blocks indefinitely:
    - Uses ``LOCK_EX | LOCK_NB`` and retries on EACCES/EAGAIN.
    - Reads the PID stamp written by the current holder and checks liveness
      via ``os.kill(pid, 0)``.  If the holder process is dead (stale lock),
      the stamp is cleared and acquisition retries immediately — the OS
      already released the flock when the process exited.
    - Raises ``TimeoutError`` after ``timeout`` seconds (default:
      ``_FLOCK_TIMEOUT_SECONDS``) so a live but stuck process never
      permanently blocks the pipeline.
    """
    effective_timeout = timeout if timeout is not None else _FLOCK_TIMEOUT_SECONDS
    deadline = time.monotonic() + effective_timeout
    last_pid_check = 0.0
    pid_check_interval_s = 1.0  # rate-limit PID liveness checks to once/second

    while True:
        try:
            _fcntl.flock(fd, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            # Acquired: stamp with our PID so future waiters can detect us as stale if we die.
            try:
                fd.seek(0)
                fd.truncate()
                fd.write(str(os.getpid()))
                fd.flush()
                # fsync the PID stamp so it survives a power loss between
                # write and crash — otherwise waiters can't determine
                # staleness and only the timeout fires.
                try:
                    os.fsync(fd.fileno())
                except OSError:
                    pass
            except OSError:
                pass  # best-effort PID stamp — the lock itself is already held
            return
        except OSError as exc:
            if exc.errno not in (_errno.EACCES, _errno.EAGAIN):
                raise  # unexpected OS error — propagate immediately

        # Lock is held by another descriptor.  Re-check holder
        # liveness periodically (not just once) — a holder that
        # was alive on the first iteration may die later, and we
        # want to detect that quickly rather than waiting for the
        # full timeout to expire.
        now = time.monotonic()
        if now - last_pid_check >= pid_check_interval_s:
            last_pid_check = now
            try:
                fd.seek(0)
                content = fd.read().strip()
                if content:
                    holder_pid = int(content)
                    try:
                        os.kill(holder_pid, 0)   # signal 0 = existence check only
                    except ProcessLookupError:
                        # Holder process is dead.  Its file descriptors were closed by
                        # the OS on termination, which released the flock automatically.
                        # Clear the stale PID stamp and retry immediately.
                        try:
                            fd.seek(0)
                            fd.truncate()
                            fd.flush()
                        except OSError:
                            pass
                        continue  # retry flock acquisition without sleeping
            except (OSError, ValueError):
                pass  # can't read PID — just wait with normal backoff

        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"_write_lock: could not acquire {lock_path!r} within "
                f"{effective_timeout:.0f}s — another process may be holding "
                "the lock for an unusually long time"
            )
        time.sleep(_FLOCK_POLL_INTERVAL)


@contextmanager
def _write_lock(data_root: str, *, timeout: float | None = None):
    """Exclusive write lock for a Parquet data root directory.

    Acquires both a threading.Lock (in-process) and an fcntl file lock
    (cross-process) before yielding, so the caller can safely execute the
    full read → merge → write cycle without racing another thread or process.

    Parameters
    ----------
    timeout:
        Maximum seconds to wait for the lock.  Defaults to
        ``_FLOCK_TIMEOUT_SECONDS`` (300 s).  Pass ``0`` for a non-blocking
        attempt that raises ``TimeoutError`` immediately if the lock is held.

    Improvements over naive ``flock(LOCK_EX)``:
    - Uses LOCK_NB with timeout — never hangs forever if the holder dies.
    - PID-stamps the lock file so waiters can detect and clear stale locks.
    - Raises ``TimeoutError`` (not an infinite hang) if the lock cannot be
      acquired within the timeout.
    - REENTRANT: Uses thread-local state to detect if this thread already holds
      the lock for the given path, allowing nested calls without deadlock.
    """
    canonical = os.path.abspath(data_root)
    thread_lock = _get_thread_lock(canonical)

    if not hasattr(_LOCK_STATE, "depths"):
        _LOCK_STATE.depths = {}

    depth = _LOCK_STATE.depths.get(canonical, 0)
    if depth > 0:
        # Already held by this thread — just increment and yield
        _LOCK_STATE.depths[canonical] = depth + 1
        try:
            yield
        finally:
            _LOCK_STATE.depths[canonical] = depth
        return

    # Not held — acquire both thread and file locks
    lock_path = os.path.join(canonical, ".write.lock")
    os.makedirs(canonical, exist_ok=True)

    with thread_lock:
        # Open for r+w without truncating so any existing PID stamp remains
        # readable by _acquire_flock during LOCK_NB retry attempts.
        raw_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fd = os.fdopen(raw_fd, "r+")
        try:
            if _HAVE_FCNTL:
                _acquire_flock(fd, lock_path, timeout=timeout)

            _LOCK_STATE.depths[canonical] = 1
            try:
                yield
            finally:
                _LOCK_STATE.depths[canonical] = 0
        finally:
            if _HAVE_FCNTL:
                try:
                    _fcntl.flock(fd, _fcntl.LOCK_UN)
                except OSError:
                    pass
            fd.close()


# ---------------------------------------------------------------------------
# Arrow schemas (enforced on every write)
# ---------------------------------------------------------------------------

MARKETS_SCHEMA = pa.schema([
    ("market_id", pa.string()),
    ("question", pa.string()),
    ("crypto", pa.dictionary(pa.int8(), pa.string())),
    ("timeframe", pa.dictionary(pa.int8(), pa.string())),
    ("volume", pa.float32()),
    ("resolution", pa.int8()),  # 0 / 1 / -1 (= unresolved)
    ("start_ts", pa.int64()),
    ("end_ts", pa.int64()),
    ("closed_ts", pa.int64()),  # closedTime from Gamma (0 = still open)
    ("condition_id", pa.string()),
    ("up_token_id", pa.string()),
    ("down_token_id", pa.string()),
    ("slug", pa.string()),                # market-level slug
    ("fee_rate_bps", pa.int16()),  # taker fee rate (basis points); -1 = unknown
])

PRICES_SCHEMA = pa.schema([
    ("market_id", pa.string()),
    ("timestamp", pa.int64()),
    ("up_price", pa.float32()),
    ("down_price", pa.float32()),
    # partition columns (crypto, timeframe) are implicit in the directory tree
])

CULTURE_MARKETS_SCHEMA = pa.schema([
    ("market_id", pa.string()),
    ("question", pa.string()),
    ("crypto", pa.dictionary(pa.int8(), pa.string())),
    ("timeframe", pa.dictionary(pa.int8(), pa.string())),
    ("volume", pa.float32()),
    ("resolution", pa.int8()),
    ("start_ts", pa.int64()),
    ("end_ts", pa.int64()),
    ("closed_ts", pa.int64()),      # closedTime from Gamma (0 = still open)
    ("condition_id", pa.string()),
    ("tokens", pa.string()),        # JSON serialized dict
    ("slug", pa.string()),          # market-level slug (includes bucket suffix)
    ("event_slug", pa.string()),    # parent event slug (derived)
    ("bucket_index", pa.int32()),   # Polymarket groupItemThreshold (-1 = unknown)
    ("bucket_label", pa.string()),  # groupItemTitle — e.g. "280-299", "240+"
])

CULTURE_PRICES_SCHEMA = pa.schema([
    ("market_id", pa.string()),
    ("timestamp", pa.int64()),
    ("token_id", pa.string()),
    ("outcome", pa.dictionary(pa.int8(), pa.string())),
    ("price", pa.float32()),
])

TICKS_SCHEMA = pa.schema([
    # One row per on-chain trade fill (Polygon OrderFilled event) or WebSocket trade event.
    # timestamp_ms is block-timestamp × 1000 for on-chain ticks (Polygon block ~2 s resolution)
    # or the WebSocket event timestamp for live ticks (millisecond precision).
    ("market_id",    pa.string()),
    ("timestamp_ms", pa.int64()),    # epoch milliseconds
    ("token_id",     pa.string()),
    ("outcome",      pa.dictionary(pa.int8(), pa.string())),   # "Up" / "Down" or any other name
    ("side",         pa.dictionary(pa.int8(), pa.string())),   # "BUY" / "SELL"
    ("price",        pa.float32()),
    ("size_usdc",    pa.float32()),
    ("tx_hash",      pa.string()),
    ("block_number", pa.int32()),
    ("log_index",    pa.int32()),
    ("source",       pa.dictionary(pa.int8(), pa.string())),   # "onchain" / "websocket"
    # Spot price of the underlying crypto asset at the instant this tick was received.
    # Sourced from Polymarket RTDS (Binance feed).  Nullable: None when RTDS hasn't
    # delivered a price yet or for historical on-chain ticks without live context.
    ("spot_price_usdt", pa.float32()),   # e.g. 67234.50 for BTC/USDT
    ("spot_price_ts_ms", pa.int64()),    # Binance timestamp of the spot price (epoch ms)
    # Local ``time.time_ns()`` at the moment the WS recv loop delivered the
    # message.  Combined with ``timestamp_ms`` (server-side) and
    # ``spot_price_ts_ms`` this gives three clocks, exposing asyncio
    # backpressure and clock skew directly.  Nullable — on-chain backfill
    # ticks and pre-v4 Parquet files have no local recv timestamp.
    ("local_recv_ts_ns", pa.int64()),
    # v5: unique order hash per fill within a transaction.  Populated by
    # the subgraph tick fetcher; NULL for WS ticks and pre-v5 on-chain
    # rows (which carry log_index as the dedup discriminator instead).
    # Consolidation uses ``COALESCE(order_hash, '')`` in the dedup key so
    # both paths cohabit correctly.
    ("order_hash", pa.string()),
    # partition columns (crypto, timeframe) are implicit
])

# Continuous spot price stream from Polymarket RTDS (Binance + Chainlink feeds).
# One row per price update — high frequency (~1-5 Hz for BTC Binance, ~0.1 Hz Chainlink).
# Flat directory (no Hive partitioning) — total volume is moderate.
SPOT_PRICES_SCHEMA = pa.schema([
    ("ts_ms",   pa.int64()),       # source timestamp (epoch ms): Binance or Chainlink ts
    ("symbol",  pa.string()),      # RTDS symbol: "btcusdt", "ethusdt", "btc/usd", "eth/usd"
    ("price",   pa.float64()),     # spot price in USD(T) — float64 for full precision
    ("source",  pa.dictionary(pa.int8(), pa.string())),   # "binance" / "chainlink"
])

# Per-token orderbook BBO snapshots from CLOB WebSocket price_change events.
# One row per BBO update per token — captures bid/ask/sizes for spread and OFI reconstruction.
# Hive-partitioned by crypto/timeframe (same as ticks).
ORDERBOOK_SCHEMA = pa.schema([
    ("ts_ms",          pa.int64()),     # local receipt timestamp (epoch ms)
    ("market_id",      pa.string()),
    ("token_id",       pa.string()),
    ("outcome",        pa.dictionary(pa.int8(), pa.string())),  # "Up" / "Down"
    ("best_bid",       pa.float32()),
    ("best_ask",       pa.float32()),
    ("best_bid_size",  pa.float32()),
    ("best_ask_size",  pa.float32()),
    # Nanosecond-precision local receipt timestamp.  Pre-v4 Parquet files
    # have no such column; union_by_name consolidation fills NULL.
    ("local_recv_ts_ns", pa.int64()),
    # partition columns (crypto, timeframe) implicit
])

# Heartbeat rows emitted on a fixed cadence by the WS flush loop even when
# no data is flowing, so downstream gap-scanning is O(scan-one-file).
# Flat directory (no partitioning): volume is tiny (~6 rows / 10 s).
HEARTBEATS_SCHEMA = pa.schema([
    ("ts_ms",              pa.int64()),   # when this heartbeat was emitted
    ("source",             pa.string()),  # "clob_ws" / "rtds_binance" / "rtds_chainlink"
    ("shard_key",          pa.string()),  # shard_idx for CLOB, symbol for RTDS
    ("event_type",         pa.string()),  # e.g. "price_change", "btcusdt"
    ("last_event_age_ms",  pa.int64()),   # ms since last real event (-1 = never seen)
])

# ---------------------------------------------------------------------------
# Schema versioning — embed in every Parquet file's metadata
# ---------------------------------------------------------------------------

# Increment this when any schema column is added, removed, or its type changes.
# The version is stored as ``b"schema_version"`` in each Parquet file's
# key-value metadata so consumers can detect and handle schema evolution.
#
# v3 → v4 (April 2026): added nullable ``local_recv_ts_ns`` to TICKS_SCHEMA
# and ORDERBOOK_SCHEMA for the three-clock observability scheme.  Pre-v4
# files are read back transparently via ``union_by_name=true`` in DuckDB
# consolidation; reads fill NULL for the missing column.
#
# v4 → v5 (April 2026): added nullable ``order_hash`` to TICKS_SCHEMA so
# subgraph-sourced fills have a unique per-fill discriminator within a
# transaction (replacing ``log_index`` which the subgraph doesn't
# expose).  Consolidation's dedup key uses ``COALESCE(order_hash, '')``
# so legacy on-chain rows and WS rows keep their previous dedup shape.
STORAGE_SCHEMA_VERSION = 5


def _stamp_schema_version(table: pa.Table) -> pa.Table:
    """Embed STORAGE_SCHEMA_VERSION in the Arrow table's Parquet metadata."""
    metadata = dict(table.schema.metadata or {})
    metadata[b"schema_version"] = str(STORAGE_SCHEMA_VERSION).encode()
    return table.replace_schema_metadata(metadata)


# ---------------------------------------------------------------------------
# Disk space guard
# ---------------------------------------------------------------------------

_MIN_FREE_DISK_GB: float = float(os.environ.get("PM_MIN_FREE_DISK_GB", "2.0"))


def _read_memory_limit_bytes(path: str) -> int | None:
    """Read a Linux memory-limit file and return bytes, or None if unavailable."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read().strip()
    except OSError:
        return None
    if not raw or raw.lower() == "max":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if 0 < value < (1 << 60) else None


def _detect_effective_memory_limit_bytes() -> int | None:
    """Best-effort detection of the process memory ceiling in bytes.

    Prefer the tightest Linux cgroup limit when present, otherwise fall back to
    physical RAM.  This keeps DuckDB tuning aligned with systemd/container caps.
    """
    candidates: list[int] = []

    for path in (
        "/sys/fs/cgroup/memory.max",  # cgroup v2
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",  # cgroup v1
    ):
        value = _read_memory_limit_bytes(path)
        if value is not None:
            candidates.append(value)

    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        page_count = int(os.sysconf("SC_PHYS_PAGES"))
        if page_size > 0 and page_count > 0:
            candidates.append(page_size * page_count)
    except (AttributeError, OSError, ValueError):
        pass

    return min(candidates) if candidates else None


def _duckdb_escape(value: str) -> str:
    """Escape a string for safe interpolation into DuckDB SQL literals."""
    return value.replace("'", "''")


def _get_consolidation_memory_limit() -> str | None:
    """Return the DuckDB memory limit string for consolidation queries."""
    override = os.environ.get("PM_DUCKDB_MEMORY_LIMIT")
    if override:
        if not re.fullmatch(r"[1-9]\d{0,5}(KB|MB|GB|TB)", override, re.IGNORECASE):
            raise ValueError(
                f"Invalid PM_DUCKDB_MEMORY_LIMIT={override!r}: "
                "expected format like '512MB' or '4GB' (no leading zeros, no spaces)"
            )
        return override

    effective_bytes = _detect_effective_memory_limit_bytes()
    if effective_bytes is None:
        return None

    # DuckDB recommends using ~50-60% of total available memory for workloads
    # that otherwise OOM despite disk spilling.
    target_mb = max(int(effective_bytes * 0.60 / (1024 * 1024)), 512)
    return f"{target_mb}MB"


def _get_consolidation_threads() -> int:
    """Return the thread count for DuckDB consolidation queries."""
    raw = os.environ.get("PM_DUCKDB_THREADS", "1")
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid PM_DUCKDB_THREADS={raw!r}: expected a positive integer") from exc
    if value < 1:
        raise ValueError(f"Invalid PM_DUCKDB_THREADS={raw!r}: expected a positive integer")
    return value


def _make_duckdb_temp_dir(label: str) -> str:
    """Return a fresh, writable scratch directory for DuckDB spills.

    Two failure modes have been observed in production:

    1. ``<partition>/.duckdb_tmp/`` (the historical default) lives
       inside the data tree, so DuckDB's spill files compete with
       the data files themselves for the same volume — a single
       consolidation pass over thousands of shards can exhaust the
       data partition and bring both writes down with one ENOSPC.

    2. ``/tmp`` on systemd-based Linux distros is typically tmpfs
       (RAM-backed) and capped at ~50% of physical RAM — on the
       8 GB CAX21 that's ~4 GB, which is *less* than the spill
       footprint of a tick consolidation merging a multi-million-row
       ``part-0.parquet`` accumulator.

    Resolution order:
      1. ``PM_DUCKDB_TEMP_DIR`` env var — explicit operator override.
      2. ``/var/tmp`` if it's a writable directory.  By POSIX/FHS
         convention this is on persistent disk (not tmpfs), and on
         a default Linux install it inherits the ample free space
         of the root filesystem rather than the tmpfs RAM cap.
      3. ``tempfile.mkdtemp()`` — final fallback, honours ``$TMPDIR``
         then ``/tmp`` (which may be tmpfs — last resort).

    The caller owns the returned directory and is responsible for
    ``shutil.rmtree(..., ignore_errors=True)`` cleanup.
    """
    parent = os.environ.get("PM_DUCKDB_TEMP_DIR")
    prefix = f"polymarket_duckdb_{label}_"
    if parent:
        os.makedirs(parent, exist_ok=True)
        return tempfile.mkdtemp(prefix=prefix, dir=parent)

    if os.name == "posix" and os.path.isdir("/var/tmp") and os.access("/var/tmp", os.W_OK):
        return tempfile.mkdtemp(prefix=prefix, dir="/var/tmp")

    return tempfile.mkdtemp(prefix=prefix)


def _scratch_free_bytes(path: str) -> int | None:
    """Best-effort free-bytes report for the volume that holds *path*.

    Returns ``None`` if the volume can't be statvfs'd (e.g. unusual
    filesystem).  Used purely for logging so operators can see whether
    the scratch volume has room before a long consolidation starts.
    """
    try:
        st = os.statvfs(path)
    except OSError:
        return None
    return st.f_bavail * st.f_frsize


def _configure_duckdb_for_consolidation(
    con: Any,
    *,
    temp_dir: str,
) -> tuple[str | None, int]:
    """Apply low-memory DuckDB settings used by tick consolidation."""
    con.execute(f"SET temp_directory='{_duckdb_escape(temp_dir)}'")

    memory_limit = _get_consolidation_memory_limit()
    if memory_limit:
        con.execute(f"SET memory_limit='{_duckdb_escape(memory_limit)}'")

    threads = _get_consolidation_threads()
    con.execute(f"SET threads={threads}")
    con.execute("SET preserve_insertion_order=false")
    return memory_limit, threads


def _check_disk_space(path: str) -> None:
    """Raise OSError if free disk space at *path*'s mount point is below threshold.

    Called before every atomic Parquet write to fail fast with a clear error
    rather than producing a corrupted partial file when the disk is almost full.
    The threshold defaults to 2 GB and can be adjusted via ``PM_MIN_FREE_DISK_GB``.
    """
    check_path = path if os.path.isdir(path) else (os.path.dirname(os.path.abspath(path)) or ".")
    try:
        usage = shutil.disk_usage(check_path)
        free_gb = usage.free / (1024 ** 3)
        if free_gb < _MIN_FREE_DISK_GB:
            raise OSError(
                f"Insufficient disk space for Parquet write: "
                f"{free_gb:.2f} GB free at {check_path!r}, "
                f"need ≥ {_MIN_FREE_DISK_GB:.1f} GB.  "
                "Free up space or lower PM_MIN_FREE_DISK_GB."
            )
    except OSError:
        raise
    except Exception as exc:
        # ``shutil.disk_usage`` may fail on a few exotic platforms /
        # filesystems (Linux container without /proc, etc.).  Skip the
        # check rather than crash the writer, but log the cause so an
        # operator can investigate why the safety net is disabled.
        logging.getLogger("polymarket_pipeline").debug(
            "_check_disk_space could not check %r (%s) — skipping",
            check_path, exc,
        )


def _resolution_to_int8(val: Any) -> int:
    """Map resolution: 0 -> 0, 1 -> 1, None -> -1."""
    if val is None or pd.isna(val):
        return -1
    return int(val)


# ---------------------------------------------------------------------------
# Split a flat DataFrame into the two normalised tables
# ---------------------------------------------------------------------------

_BINARY_MARKETS_EMPTY_COLS = [
    "market_id", "question", "crypto", "timeframe", "volume", "resolution",
    "start_ts", "end_ts", "closed_ts", "condition_id", "up_token_id",
    "down_token_id", "slug", "fee_rate_bps",
]


def split_markets_prices(flat_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a legacy flat DataFrame into (markets_df, prices_df).

    The flat schema has columns:
        market_id, crypto, timeframe, timestamp, up_price, down_price,
        volume, resolution, question
    """
    if flat_df.empty:
        markets_df = pd.DataFrame(columns=_BINARY_MARKETS_EMPTY_COLS)
        prices_df = pd.DataFrame(columns=["market_id", "timestamp", "up_price", "down_price", "crypto", "timeframe"])
        return markets_df, prices_df

    # Markets: one row per market_id, keep last-seen metadata
    agg_spec = {
        "question": "last",
        "crypto": "last",
        "timeframe": "last",
        "volume": "last",
        "resolution": "last",
        "start_ts": "last",
        "end_ts": "last",
        "condition_id": "last",
        "up_token_id": "last",
        "down_token_id": "last",
    }
    for col in ("closed_ts", "slug", "fee_rate_bps"):
        if col in flat_df.columns:
            agg_spec[col] = "last"
    markets_df = (
        flat_df.groupby("market_id", sort=False)
        .agg(agg_spec)
        .reset_index()
    )

    # Prices: tick-level, plus crypto/timeframe for partitioning
    prices_df = flat_df[["market_id", "timestamp", "up_price", "down_price", "crypto", "timeframe"]].copy()

    return markets_df, prices_df


# ---------------------------------------------------------------------------
# Dtype optimisation
# ---------------------------------------------------------------------------

def optimise_markets_df(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast dtypes on the markets DataFrame before writing.

    Inject defaults for any columns missing from the input rather than
    raising ``KeyError`` — this keeps the writer tolerant of legacy
    DataFrames or test fixtures that omit optional fields.
    ``MARKETS_SCHEMA`` is the authoritative list of expected columns.
    """
    df = df.copy()

    if "crypto" not in df.columns:
        df["crypto"] = ""
    if "timeframe" not in df.columns:
        df["timeframe"] = ""
    df["crypto"] = df["crypto"].astype("category")
    df["timeframe"] = df["timeframe"].astype("category")

    if "volume" not in df.columns:
        df["volume"] = 0.0
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0).astype("float32")

    if "resolution" not in df.columns:
        df["resolution"] = -1
    df["resolution"] = df["resolution"].apply(_resolution_to_int8).astype("int8")

    # Time fields — inject 0 if absent so the schema requirement is met.
    if "start_ts" not in df.columns:
        df["start_ts"] = 0
    df["start_ts"] = pd.to_numeric(df["start_ts"], errors="coerce").fillna(0).astype("int64")
    if "end_ts" not in df.columns:
        df["end_ts"] = 0
    df["end_ts"] = pd.to_numeric(df["end_ts"], errors="coerce").fillna(0).astype("int64")
    if "closed_ts" not in df.columns:
        df["closed_ts"] = 0
    df["closed_ts"] = pd.to_numeric(df["closed_ts"], errors="coerce").fillna(0).astype("int64")

    # String identifiers — fillna so NaN doesn't leak through to PyArrow.
    for col in ("condition_id", "up_token_id", "down_token_id"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype("string")

    if "slug" not in df.columns:
        df["slug"] = ""
    df["slug"] = df["slug"].fillna("").astype("string")
    if "fee_rate_bps" not in df.columns:
        df["fee_rate_bps"] = -1
    df["fee_rate_bps"] = pd.to_numeric(df["fee_rate_bps"], errors="coerce").fillna(-1).astype("int16")

    return df


def optimise_prices_df(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast dtypes on the prices DataFrame before writing."""
    df = df.copy()
    df["timestamp"] = df["timestamp"].astype("int64")
    df["up_price"] = df["up_price"].astype("float32")
    df["down_price"] = df["down_price"].astype("float32")
    df["crypto"] = df["crypto"].astype("category")
    df["timeframe"] = df["timeframe"].astype("category")
    return df


def optimise_ticks_df(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast dtypes on the ticks DataFrame before writing."""
    df = df.copy()
    df["timestamp_ms"] = df["timestamp_ms"].astype("int64")
    df["price"]        = df["price"].astype("float32")
    df["size_usdc"]    = df["size_usdc"].astype("float32")
    df["outcome"]      = df["outcome"].astype("category")
    df["side"]         = df["side"].astype("category")
    df["source"]       = df["source"].astype("category")
    df["crypto"]       = df["crypto"].astype("category")
    df["timeframe"]    = df["timeframe"].astype("category")
    if "block_number" in df.columns:
        df["block_number"] = df["block_number"].fillna(0).astype("int32")
    if "log_index" in df.columns:
        df["log_index"] = df["log_index"].fillna(0).astype("int32")
    if "tx_hash" not in df.columns:
        df["tx_hash"] = ""
    return df


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

def load_markets(markets_path: str | None = None) -> pd.DataFrame:
    """Load the markets table from Parquet (or return empty DF)."""
    path = markets_path or PARQUET_MARKETS_PATH
    if os.path.exists(path):
        return pq.read_table(path).to_pandas()
    return pd.DataFrame(columns=_BINARY_MARKETS_EMPTY_COLS)


def _read_hive_partitioned_robust(
    path: str,
    partition_cols: list[str],
    filters: list | None,
    empty_cols: list[str],
) -> pd.DataFrame:
    """File-by-file fallback for Hive-partitioned Parquet directories.

    Used when ``pq.ParquetDataset.read()`` raises ``ArrowInvalid`` due to
    dictionary index-type mismatches across shards — e.g. old files that have
    ``crypto`` / ``timeframe`` embedded as ``dict<int8>`` data columns next to
    newer files written by ``_write_partitioned_atomic`` where those columns
    are path-encoded (read back by PyArrow as ``dict<int32>``).

    Each file is read in isolation with ``pq.read_table``, any partition
    columns present in the file data are dropped, and the correct string
    values are re-injected from the directory path.  Partition-level filter
    pruning is applied before reading so unneeded directories are skipped.
    """
    # Build per-OR-group partition equality maps from the DNF filter list.
    # Filters format: [[("col","=","val"), ...], ...]  (OR of AND-groups).
    allowed: list[dict[str, str]] | None = None
    if filters:
        allowed = []
        for and_group in filters:
            group: dict[str, str] = {}
            for item in and_group:
                col, op, val = item[0], item[1], item[2]
                if op == "=" and col in partition_cols:
                    group[col] = str(val)
            allowed.append(group)

    frames: list[pd.DataFrame] = []
    for dirpath, _dirs, fnames in os.walk(path):
        parquet_files = [f for f in fnames if f.endswith(".parquet")]
        if not parquet_files:
            continue

        # Extract partition column values from the directory path segments.
        partition_values: dict[str, str] = {}
        rel = os.path.relpath(dirpath, path)
        for segment in rel.replace("\\", "/").split("/"):
            if "=" in segment:
                k, v = segment.split("=", 1)
                partition_values[k] = v

        # Skip directories that don't match any OR-group.
        if allowed is not None:
            if not any(
                all(partition_values.get(col) == val for col, val in grp.items())
                for grp in allowed
            ):
                continue

        for fname in parquet_files:
            try:
                # Use ParquetFile (not read_table) to bypass Hive path inference.
                # pq.read_table internally builds a ParquetDataset which detects
                # the crypto=X/timeframe=Y directory segments and tries to merge
                # the path-inferred schema (dict<int32>) with the embedded schema
                # in old-format files (dict<int8>), raising ArrowTypeError.
                # pq.ParquetFile reads the raw file bytes without any path-based
                # partitioning logic, so it returns the actual embedded schema.
                t = pq.ParquetFile(os.path.join(dirpath, fname)).read()
            except Exception:
                continue
            # Drop partition columns present in the file data (old-format files).
            for col in partition_cols:
                if col in t.schema.names:
                    t = t.remove_column(t.schema.get_field_index(col))
            df = t.to_pandas()
            # Re-add partition columns as plain strings from the directory path.
            for col, val in partition_values.items():
                df[col] = val
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=empty_cols)
    return pd.concat(frames, ignore_index=True)


_PRICES_EMPTY_COLS = ["market_id", "timestamp", "up_price", "down_price", "crypto", "timeframe"]
_HIVE_PARTITION_COLS = ["crypto", "timeframe"]


def load_prices(prices_dir: str | None = None, filters: list | None = None) -> pd.DataFrame:
    """Load the prices table from a Hive-partitioned Parquet directory.

    Parameters
    ----------
    prices_dir : path to the partitioned directory.
    filters : optional PyArrow filter expression list,
              e.g. [[("crypto", "=", "BTC"), ("timeframe", "=", "1-hour")]]
    """
    path = prices_dir or PARQUET_PRICES_DIR
    if not os.path.exists(path):
        return pd.DataFrame(columns=_PRICES_EMPTY_COLS)
    try:
        dataset = pq.ParquetDataset(path, filters=filters)
        return dataset.read().to_pandas()
    except Exception:
        # Schema mismatch between shards (e.g. old files with crypto/timeframe
        # as embedded dict<int8> vs newer path-encoded files read as dict<int32>).
        # We catch Exception to prevent dictionary unification mismatches
        # (which raise pa.lib.ArrowNotImplementedError in newer PyArrow)
        # from crashing the pipeline.
        # Fall back to reading each file individually and normalising.
        return _read_hive_partitioned_robust(path, _HIVE_PARTITION_COLS, filters, _PRICES_EMPTY_COLS)


def load_prices_for_timeframe(timeframe: str, prices_dir: str | None = None) -> pd.DataFrame:
    """Convenience: load prices filtered to a single timeframe."""
    return load_prices(prices_dir, filters=[[("timeframe", "=", timeframe)]])


_TICKS_EMPTY_COLS = [
    "market_id", "timestamp_ms", "token_id", "outcome", "side",
    "price", "size_usdc", "tx_hash", "block_number", "log_index", "source",
    "spot_price_usdt", "spot_price_ts_ms", "local_recv_ts_ns",
    "order_hash",
    "crypto", "timeframe",
]
# Dedup key.  ``order_hash`` is folded in via ``COALESCE(order_hash, '')``
# inside the consolidation SQL so legacy rows (NULL order_hash) dedup
# with their historical behaviour and subgraph rows (populated
# order_hash) don't collapse multi-fill transactions.
_TICKS_DEDUP_COLS = ["market_id", "timestamp_ms", "token_id", "tx_hash", "log_index"]

# Cap on shards processed in a single DuckDB COPY pass.  Each pass loads
# part-0.parquet (the running accumulator) plus this many fresh shards
# into one ``read_parquet`` call, so peak memory grows with both the
# accumulator size *and* the batch size.  Past this threshold the
# consolidator splits the work into multiple passes, each pass folding
# into ``part-0.parquet``.
#
# Default is conservative on purpose: production CAX21 has DuckDB capped
# at ~2 GB by the systemd cgroup, and a backlog of 16k shards on a hot
# partition (BTC/5-minute) plus a multi-million-row part-0 will OOM
# above ~300 shards/pass even with the legacy window function removed.
# Override via ``PM_TICK_CONSOLIDATION_BATCH_SIZE`` for hosts with more
# headroom.
_DEFAULT_TICK_CONSOLIDATION_BATCH_SIZE = 250


def _get_tick_consolidation_batch_size() -> int:
    raw = os.environ.get("PM_TICK_CONSOLIDATION_BATCH_SIZE")
    if not raw:
        return _DEFAULT_TICK_CONSOLIDATION_BATCH_SIZE
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"Invalid PM_TICK_CONSOLIDATION_BATCH_SIZE={raw!r}: expected a positive integer"
        ) from exc
    if value < 1:
        raise ValueError(
            f"Invalid PM_TICK_CONSOLIDATION_BATCH_SIZE={raw!r}: expected a positive integer"
        )
    return value


# Module-level constant retained so existing tests (which monkeypatch it)
# keep working.  ``consolidate_ticks`` reads the *effective* value via
# ``_get_tick_consolidation_batch_size()`` so the env override wins, and
# falls back to this attribute when no override is set — that's the
# hook the regression tests use.
_TICK_CONSOLIDATION_BATCH_SIZE = _DEFAULT_TICK_CONSOLIDATION_BATCH_SIZE


# Tier-2 compaction threshold: when a partition accumulates more than
# this many ``part-ws-*.parquet`` files, fold them into a single
# consolidated ``part-ws-*.parquet``.  The append-only consolidation
# path (``_consolidate_ws_ticks_appendonly`` for ticks,
# ``consolidate_orderbook`` for orderbook BBO) writes a NEW file every
# run; without this tier-2 fold the partition would grow without bound,
# slowing partition reads (DuckDB / pyarrow file-listing overhead) and
# inflating the HF Hub object count.  Threshold tuned for ~hourly
# upload-only runs producing 1 new part-ws-* per hot partition: a
# ~32-file ceiling means compaction runs at most once a day per
# partition, keeping amortised cost low.
_DEFAULT_PART_WS_COMPACTION_THRESHOLD = 32
_PART_WS_COMPACTION_THRESHOLD = _DEFAULT_PART_WS_COMPACTION_THRESHOLD


def _get_part_ws_compaction_threshold() -> int:
    raw = os.environ.get("PM_PART_WS_COMPACTION_THRESHOLD")
    if not raw:
        return _PART_WS_COMPACTION_THRESHOLD
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(
            f"Invalid PM_PART_WS_COMPACTION_THRESHOLD={raw!r}: expected a positive integer"
        ) from None
    if value < 2:
        raise ValueError(
            f"Invalid PM_PART_WS_COMPACTION_THRESHOLD={raw!r}: must be >= 2"
        )
    return value


def _tick_consolidation_select_sql(sort_key_sql: str) -> str:
    """Build the SELECT list for DuckDB shard consolidation."""
    select_exprs = []
    for col in TICKS_SCHEMA.names:
        if col in _TICKS_DEDUP_COLS:
            select_exprs.append(col)
        else:
            select_exprs.append(f"arg_max({col}, {sort_key_sql}) AS {col}")
    return ",\n                            ".join(select_exprs)


def load_ticks(ticks_dir: str | None = None, filters: list | None = None) -> pd.DataFrame:
    """Load the ticks table from a Hive-partitioned Parquet directory."""
    path = ticks_dir or PARQUET_TICKS_DIR
    if not os.path.exists(path):
        return pd.DataFrame(columns=_TICKS_EMPTY_COLS)
    try:
        dataset = pq.ParquetDataset(path, filters=filters)
        return dataset.read().to_pandas()
    except Exception:
        return _read_hive_partitioned_robust(path, _HIVE_PARTITION_COLS, filters, _TICKS_EMPTY_COLS)


def load_ticks_for_market(
    market_id: str,
    ticks_dir: str | None = None,
    *,
    crypto: str | None = None,
    timeframe: str | None = None,
) -> pd.DataFrame:
    """Load all ticks for a specific market_id.

    Pass *crypto* and *timeframe* to restrict the scan to the single Hive
    partition that contains this market, avoiding a full-table scan.
    """
    filters: list = []
    if crypto:
        filters.append(("crypto", "=", crypto))
    if timeframe:
        filters.append(("timeframe", "=", timeframe))
    df = load_ticks(ticks_dir, filters=filters if filters else None)
    if df.empty:
        return df
    return df[df["market_id"] == market_id].sort_values("timestamp_ms").reset_index(drop=True)


def persist_ticks(
    ticks_df: pd.DataFrame,
    *,
    ticks_dir: str | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Merge new tick rows with existing ticks on disk and write Parquet.

    Deduplicates on (market_id, timestamp_ms, token_id, tx_hash, log_index)
    plus ``order_hash`` and ``local_recv_ts_ns`` discriminators.
    Partitioned by crypto / timeframe.

    Implementation delegates to ``append_ticks_only`` + ``consolidate_ticks``
    so the dedup logic is shared with the WebSocket / backfill streaming
    path.  Two benefits:

    1. Memory: the previous implementation read the entire (crypto, timeframe)
       partition into pandas, then concat + drop_duplicates + sort — peak
       memory ~3× partition size, which OOMs the 8 GB CAX21 production
       host once partitions exceed a few GB.  Streaming via DuckDB caps
       memory regardless of partition size.

    2. Correctness: the previous pandas ``drop_duplicates`` used a
       weaker key that excluded ``order_hash`` and ``local_recv_ts_ns``,
       so it collapsed sub-millisecond WS trades AND multi-fill subgraph
       transactions that ``consolidate_ticks`` (with the stronger key)
       preserves.  Going through the same code path eliminates the
       divergence.
    """
    if ticks_df.empty:
        return

    log = logger or logging.getLogger("polymarket_pipeline")
    t_dir = ticks_dir or PARQUET_TICKS_DIR

    append_ticks_only(ticks_df, ticks_dir=t_dir, logger=log)
    consolidate_ticks(ticks_dir=t_dir, logger=log)


def append_ticks_only(
    ticks_df: pd.DataFrame,
    *,
    ticks_dir: str | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Append new tick rows to disk WITHOUT loading existing data.

    Writes each (crypto, timeframe) group as a separate shard file within
    the Hive-partitioned directory tree.  This avoids loading the full
    existing tick dataset into RAM (which can be 1+ GB), preventing OOM
    during the historical tick backfill.

    Deduplication is deferred to the next ``persist_ticks()`` call (which
    happens on the regular 6-hour timer run).
    """
    if ticks_df.empty:
        return

    log = logger or logging.getLogger("polymarket_pipeline")
    t_dir = ticks_dir or PARQUET_TICKS_DIR

    # Ensure required columns are present with defaults
    for col in _TICKS_EMPTY_COLS:
        if col not in ticks_df.columns:
            ticks_df = ticks_df.copy()
            if col in ("tx_hash",):
                ticks_df[col] = ""
            elif col in ("block_number", "log_index"):
                ticks_df[col] = 0
            else:
                ticks_df[col] = None

    # Flatten category columns
    for col in ("outcome", "side", "source", "crypto", "timeframe"):
        if col in ticks_df.columns and hasattr(ticks_df[col], "cat"):
            ticks_df = ticks_df.copy()
            ticks_df[col] = ticks_df[col].astype(str)

    # Write each (crypto, timeframe) group as a new shard file
    _check_disk_space(os.path.dirname(os.path.abspath(t_dir)) or ".")
    ticks_df = optimise_ticks_df(ticks_df)
    shard_id = f"{int(time.time())}_{os.getpid()}"
    for (crypto, timeframe), group in ticks_df.groupby(["crypto", "timeframe"], sort=False, observed=True):
        shard_dir = os.path.join(t_dir, f"crypto={crypto}", f"timeframe={timeframe}")
        os.makedirs(shard_dir, exist_ok=True)
        shard_path = os.path.join(shard_dir, f"backfill_{shard_id}.parquet")
        rows = group.drop(columns=["crypto", "timeframe"]).reset_index(drop=True)
        table = _stamp_schema_version(pa.Table.from_pandas(rows, preserve_index=False))
        pq.write_table(table, shard_path, compression="zstd")

    log.info("Ticks appended (no merge): %s/ (%s new rows)", t_dir, len(ticks_df))


def _run_tick_consolidation_pass(
    con: Any,
    *,
    dirpath: str,
    input_basenames: list[str],
    consolidated_path: str,
    rel: str,
    log: logging.Logger,
) -> int:
    """Run one DuckDB consolidation pass over *input_basenames* in *dirpath*.

    The order of *input_basenames* defines ``file_order`` (and therefore
    the dedup tiebreak): earlier entries lose ties to later entries.  When
    folding a previous ``part-0.parquet`` into a fresh batch, place it
    first so the new shards' rows win.

    Atomically replaces ``consolidated_path`` with the new output, then
    deletes every input file *except* ``part-0.parquet`` (which was just
    swapped in via ``os.replace``).  Returns the row count written.
    """
    file_paths = [os.path.join(dirpath, f) for f in input_basenames]
    tmp_path = f"{consolidated_path}.{os.getpid()}.tmp"

    files_sql = ", ".join(f"'{_duckdb_escape(p)}'" for p in file_paths)
    files_with_order_sql = ", ".join(
        f"('{_duckdb_escape(path)}', {idx})" for idx, path in enumerate(file_paths)
    )
    select_sql = _tick_consolidation_select_sql("sort_key")
    group_by_sql = ", ".join(_TICKS_DEDUP_COLS)

    desc_res = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet([{files_sql}], union_by_name=true)"
    ).fetchall()
    present_cols = {row[0] for row in desc_res}

    def _col_sql(name: str, default: str) -> str:
        if name in present_cols:
            return f"COALESCE(src.{name}, {default}) AS {name}"
        return f"{default} AS {name}"

    tx_hash_sql = (
        f"CAST(COALESCE(src.tx_hash, '') AS VARCHAR) AS tx_hash"
        if "tx_hash" in present_cols
        else "CAST('' AS VARCHAR) AS tx_hash"
    )
    block_number_sql = _col_sql("block_number", "0")
    log_index_sql = _col_sql("log_index", "0")
    spot_price_usdt_sql = (
        "CAST(src.spot_price_usdt AS FLOAT) AS spot_price_usdt"
        if "spot_price_usdt" in present_cols
        else "NULL::FLOAT AS spot_price_usdt"
    )
    spot_price_ts_ms_sql = (
        "CAST(src.spot_price_ts_ms AS BIGINT) AS spot_price_ts_ms"
        if "spot_price_ts_ms" in present_cols
        else "NULL::BIGINT AS spot_price_ts_ms"
    )
    local_recv_ts_ns_sql = (
        "CAST(src.local_recv_ts_ns AS BIGINT) AS local_recv_ts_ns"
        if "local_recv_ts_ns" in present_cols
        else "NULL::BIGINT AS local_recv_ts_ns"
    )
    # v5 order_hash — missing in pre-v5 shards, populated by
    # SubgraphTickFetcher.  Explicit VARCHAR cast: in production we
    # observed pandas writing all-None order_hash columns that
    # pyarrow stored with a non-string Arrow type, which then poisoned
    # the ``union_by_name`` schema and broke ``COALESCE(order_hash, '')``
    # downstream with a "Could not convert string '' to INT32" error.
    order_hash_sql = (
        "CAST(src.order_hash AS VARCHAR) AS order_hash"
        if "order_hash" in present_cols
        else "NULL::VARCHAR AS order_hash"
    )

    try:
        result = con.execute(f"""
            COPY (
                WITH source_files(file_path, file_order) AS (
                    VALUES {files_with_order_sql}
                ),
                ranked_rows AS (
                    SELECT
                        src.market_id,
                        src.timestamp_ms,
                        src.token_id,
                        src.outcome,
                        src.side,
                        src.price,
                        src.size_usdc,
                        {tx_hash_sql},
                        {block_number_sql},
                        {log_index_sql},
                        src.source,
                        {spot_price_usdt_sql},
                        {spot_price_ts_ms_sql},
                        {local_recv_ts_ns_sql},
                        {order_hash_sql},
                        ((f.file_order::BIGINT << 32) + src.file_row_number::BIGINT) AS sort_key
                    FROM read_parquet(
                        [{files_sql}],
                        union_by_name=true,
                        filename=true,
                        file_row_number=true
                    ) AS src
                    JOIN source_files AS f ON src.filename = f.file_path
                ),
                -- Hybrid legacy↔subgraph dedup.  Legacy (Etherscan/RPC)
                -- rows have populated tx_hash and log_index but NULL
                -- order_hash.  Subgraph rows have the same tx_hash,
                -- log_index=0, and a real order_hash.  When BOTH exist
                -- for the same (market_id, ts_ms, token_id, tx_hash),
                -- the subgraph rows are canonical — drop the legacy
                -- ones so re-collecting silently overwrites the old
                -- data with the new schema.
                --
                -- WS rows have tx_hash='' (unaffected) and ride the
                -- order_hash + local_recv_ts_ns discriminators below.
                --
                -- Implemented as a small DISTINCT CTE + LEFT JOIN
                -- rather than a ``MAX(...) OVER (PARTITION BY ...)``
                -- window function: the window function is a blocking
                -- operator that materialises the entire row set before
                -- emitting any output, which OOM'd the production
                -- 2 GB-capped DuckDB on hot partitions with thousands
                -- of shards.  The JOIN's build side is just the
                -- distinct (mid, ts, tid, tx_hash) tuples that have
                -- a subgraph sibling — typically a tiny fraction of
                -- the row set, and trivially spillable.
                subgraph_keys AS (
                    SELECT DISTINCT market_id, timestamp_ms, token_id, tx_hash
                    FROM ranked_rows
                    WHERE order_hash IS NOT NULL
                      AND tx_hash <> ''
                ),
                dedup_input AS (
                    SELECT r.*
                    FROM ranked_rows AS r
                    LEFT JOIN subgraph_keys AS sk
                        ON sk.market_id    = r.market_id
                       AND sk.timestamp_ms = r.timestamp_ms
                       AND sk.token_id     = r.token_id
                       AND sk.tx_hash      = r.tx_hash
                    WHERE NOT (
                        r.tx_hash <> ''           -- never filter WS rows
                        AND r.order_hash IS NULL  -- legacy on-chain
                        AND sk.market_id IS NOT NULL  -- subgraph sibling exists
                    )
                )
                -- Intentionally omit a final ORDER BY: the global sort is
                -- another blocking operator and was the main OOM trigger.
                --
                -- Dedup key includes:
                --   * COALESCE(order_hash, '') so subgraph-sourced rows
                --     (populated order_hash, unique per-fill within a
                --     tx) don't collapse with legacy on-chain / WS rows
                --     that have NULL order_hash.
                --   * COALESCE(local_recv_ts_ns, 0) so multiple WS
                --     trades on the same (market_id, ts_ms, token_id) —
                --     where tx_hash="", log_index=0, order_hash IS
                --     NULL — but with distinct nanosecond receive times
                --     survive consolidation.  Sub-millisecond fills can
                --     and do happen on hot tokens; without this
                --     discriminator they used to collapse to one row.
                --     Old shards without local_recv_ts_ns map to 0 and
                --     dedup as before.
                SELECT
                    {select_sql}
                FROM dedup_input AS ranked_rows
                GROUP BY {group_by_sql},
                         COALESCE(order_hash, ''),
                         COALESCE(local_recv_ts_ns, 0)
            ) TO '{_duckdb_escape(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """).fetchone()
        nrows = result[0] if result else 0

        # Replace consolidated file FIRST so a crash between the two
        # steps leaves orphan shards (idempotent — the next
        # consolidation run merges them again via dedup) rather than an
        # empty partition that the HF upload's delete_patterns would
        # propagate to the Hub.
        os.replace(tmp_path, consolidated_path)
        _fsync_dir(dirpath)

        remove_errors: list[str] = []
        for fname in input_basenames:
            if fname == "part-0.parquet":
                continue  # already replaced atomically above
            try:
                os.remove(os.path.join(dirpath, fname))
            except OSError as exc:
                remove_errors.append(f"{fname}: {exc}")
        if remove_errors:
            log.warning(
                "  -> could not remove %d old shard file(s) in %s; "
                "duplicates may remain until the next consolidation: %s",
                len(remove_errors),
                rel,
                "; ".join(remove_errors),
            )
        return nrows
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def consolidate_ticks(
    *,
    ticks_dir: str | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Consolidate tick shard files one partition at a time.

    Two-phase strategy.  The split is forced by production: a single
    hot partition (BTC/5-minute) accumulated 17 k+ shards during an
    outage, and the previous "fold part-0 into every batch pass"
    model OOM'd / ENOSPC'd because peak memory and spill scale with
    *cumulative* part-0 size — not batch size.

    Phase 1 — **WS shards** (``ws_ticks_*.parquet``): consume in
    batches, dedup *within batch*, write each batch out as a new
    ``part-ws-{epoch_ms}-{seq}.parquet`` file alongside any
    existing ``part-0.parquet``.  No part-0 fold.  WS rows have
    ``tx_hash=""``, ``log_index=0``, ``order_hash IS NULL`` and a
    system-monotonic ``local_recv_ts_ns`` per row — so they cannot
    dedup-collide with rows already in part-0 (or with rows in
    other batches in any realistic scenario).  There's nothing to
    fold, and nothing to gain by materialising the historical
    accumulator on every run.  Memory is bounded to one batch's
    worth of rows, regardless of partition history.

    Phase 2 — **backfill shards** (``backfill_*.parquet``): same
    shape as Phase 1.  Each batch dedup'd within itself, written
    to a new ``part-bf-{epoch_ms}-{pid}-{seq}.parquet`` file
    alongside ``part-0.parquet``; **part-0 is never folded.**
    Backfill rows can legitimately overlap rows already in part-0
    (re-fetched markets within the historical scan's 2-day
    rolling window), and the legacy "fold part-0 into every
    batch" path turned that into a hard ENOSPC failure mode once
    a single historical run produced 800+ shards on a multi-GB
    part-0.  Append-only keeps peak DuckDB memory + spill bounded
    by batch size regardless of how big part-0 grows; cross-file
    dedup of the resulting ``part-0`` ⊕ ``part-bf-*`` collisions
    is deferred to read time (every modern Parquet consumer
    already handles a multi-file Hive partition transparently
    via ``read_parquet(<dir>)``).

    Tier-2 compaction folds accumulated ``part-ws-*`` and
    ``part-bf-*`` files into single compact files per partition
    once their counts cross ``PM_PART_WS_COMPACTION_THRESHOLD`` —
    so the file count cannot drift unboundedly even though
    individual passes never touch part-0.
    """
    log = logger or logging.getLogger("polymarket_pipeline")
    t_dir = ticks_dir or PARQUET_TICKS_DIR

    if not os.path.exists(t_dir):
        return

    import duckdb

    data_root = os.path.dirname(os.path.abspath(t_dir))

    for dirpath, _dirs, _fnames in os.walk(t_dir):
        rel = os.path.relpath(dirpath, t_dir)

        with _write_lock(data_root):
            # Snapshot file list INSIDE the lock to prevent TOCTOU races
            # with append_ws_ticks_staged() running in the WS service.
            parquet_files = [f for f in os.listdir(dirpath) if f.endswith(".parquet")]
            ws_shards = [f for f in parquet_files if f.startswith("ws_ticks_")]
            non_ws_shards = [
                f for f in parquet_files
                if f != "part-0.parquet"
                and not f.startswith("ws_ticks_")
                and not f.startswith("part-ws-")  # already-consolidated WS outputs
                and not f.startswith("part-bf-")  # already-consolidated backfill outputs
            ]
            has_part0 = "part-0.parquet" in parquet_files
            part_ws_count = sum(
                1 for f in parquet_files if f.startswith("part-ws-")
            )
            part_bf_count = sum(
                1 for f in parquet_files if f.startswith("part-bf-")
            )
            # The tier-2 compaction needs to run even when no fresh
            # shards exist (a long-idle hot partition has only
            # accumulated part-ws-* / part-bf-* files), so include the
            # threshold check in the "is there work?" decision.
            compact_threshold = _get_part_ws_compaction_threshold()
            needs_compact = (
                part_ws_count >= compact_threshold
                or part_bf_count >= compact_threshold
            )

            if not ws_shards and not non_ws_shards and not needs_compact:
                continue  # nothing to consolidate (only part-0 and/or part-ws-* / part-bf-*)

            log.info(
                "Consolidating partition %s (ws=%d, backfill=%d, part_ws=%d, part_bf=%d, has_part0=%s)",
                rel, len(ws_shards), len(non_ws_shards),
                part_ws_count, part_bf_count,
                "yes" if has_part0 else "no",
            )
            _check_disk_space(dirpath)

            ws_shards.sort(
                key=lambda f: (os.path.getmtime(os.path.join(dirpath, f)), f)
            )
            non_ws_shards.sort(
                key=lambda f: (os.path.getmtime(os.path.join(dirpath, f)), f)
            )

            # Prefer the module attribute when tests monkeypatch it; only
            # consult the env var when the attribute is at its default.
            if _TICK_CONSOLIDATION_BATCH_SIZE != _DEFAULT_TICK_CONSOLIDATION_BATCH_SIZE:
                batch_size = _TICK_CONSOLIDATION_BATCH_SIZE
            else:
                batch_size = _get_tick_consolidation_batch_size()

            # Phase 1: WS append-only.
            if ws_shards:
                _consolidate_ws_ticks_appendonly(
                    duckdb_module=duckdb,
                    dirpath=dirpath,
                    shard_files=ws_shards,
                    batch_size=batch_size,
                    log=log,
                )

            # Tier-2: when accumulated part-ws-*.parquet count crosses
            # the compaction threshold, fold them into a single file.
            # This bounds long-term file accumulation under the
            # append-only design (each batch writes a new part-ws-* and
            # never re-folds).  Only runs when threshold is exceeded so
            # most consolidation passes pay zero extra cost.
            _compact_part_ws_ticks(
                duckdb_module=duckdb,
                dirpath=dirpath,
                log=log,
            )

            # Phase 2: backfill shards as append-only part-bf-* files.
            # Same shape as Phase 1: each batch dedup'd within itself,
            # written to a fresh part-bf-*.parquet, no part-0 fold.
            if non_ws_shards:
                _consolidate_backfill_appendonly(
                    duckdb_module=duckdb,
                    dirpath=dirpath,
                    shard_files=non_ws_shards,
                    batch_size=batch_size,
                    log=log,
                )

            # Tier-2: backfill compaction.  When accumulated
            # part-bf-*.parquet count crosses the compaction
            # threshold, fold them into a single file (same
            # invariants as the WS tier-2 fold above).
            _compact_part_bf_ticks(
                duckdb_module=duckdb,
                dirpath=dirpath,
                log=log,
            )


def _consolidate_ws_ticks_appendonly(
    *,
    duckdb_module: Any,
    dirpath: str,
    shard_files: list[str],
    batch_size: int,
    log: logging.Logger,
) -> None:
    """Consume WS shards into new ``part-ws-{epoch_ms}-{seq}.parquet`` files.

    No part-0 fold.  Each batch is deduplicated within itself (so
    pathological cases — e.g. two WS rows on the same hot token at
    the same nanosecond — still collapse) but cross-batch and
    cross-part-0 dedup are intentionally skipped: in practice the
    ``local_recv_ts_ns`` discriminator makes such collisions
    effectively impossible, and removing the part-0 fold is the
    only way to keep peak memory bounded against an arbitrarily
    large historical accumulator.
    """
    if not shard_files:
        return

    num_batches = max(1, (len(shard_files) + batch_size - 1) // batch_size)
    log.info(
        "  -> WS append-only: %d shards → %d batch(es) of <=%d",
        len(shard_files), num_batches, batch_size,
    )

    epoch_ms = int(time.time() * 1000)
    pid = os.getpid()
    settings_logged = False
    rows_total = 0

    for batch_idx in range(num_batches):
        batch = shard_files[batch_idx * batch_size : (batch_idx + 1) * batch_size]
        # Include PID so two consolidations on the same partition that
        # land in the same millisecond (rare but possible across systemd
        # timers) don't overwrite each other's batch_000 output.
        out_name = f"part-ws-{epoch_ms}-{pid}-{batch_idx:03d}.parquet"
        out_path = os.path.join(dirpath, out_name)
        tmp_path = f"{out_path}.{pid}.tmp"

        pass_temp_dir = _make_duckdb_temp_dir("ws_ticks")
        con = None
        try:
            con = duckdb_module.connect()
            memory_limit, threads = _configure_duckdb_for_consolidation(
                con, temp_dir=pass_temp_dir,
            )
            if not settings_logged:
                free = _scratch_free_bytes(pass_temp_dir)
                free_str = (
                    f"{free / (1024 ** 3):.1f}GB free"
                    if free is not None else "free=?"
                )
                log.info(
                    "  -> DuckDB settings: memory_limit=%s threads=%s temp_dir=%s (%s)",
                    memory_limit or "default", threads, pass_temp_dir, free_str,
                )
                settings_logged = True
            log.info(
                "  -> WS Batch %d/%d (%d shards)",
                batch_idx + 1, num_batches, len(batch),
            )

            nrows = _run_ws_appendonly_pass(
                con, dirpath=dirpath, input_basenames=batch, output_path=tmp_path,
            )

            os.replace(tmp_path, out_path)
            _fsync_dir(dirpath)

            for fname in batch:
                try:
                    os.remove(os.path.join(dirpath, fname))
                except OSError:
                    pass

            rows_total += nrows
        except Exception:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise
        finally:
            try:
                if con is not None:
                    con.close()
            finally:
                shutil.rmtree(pass_temp_dir, ignore_errors=True)

    log.info(
        "  -> WS append-only complete: %d rows in %d new file(s)",
        rows_total, num_batches,
    )


def _run_ws_appendonly_pass(
    con: Any,
    *,
    dirpath: str,
    input_basenames: list[str],
    output_path: str,
) -> int:
    """Single append-only pass: dedup ``input_basenames`` within the
    batch and write to ``output_path``.

    Includes the same legacy↔subgraph filter as
    ``_run_tick_consolidation_pass`` so the backfill path (which now
    flows through this function instead of folding part-0) preserves
    the schema-migration override: when the same (market_id, ts_ms,
    token_id, tx_hash) appears as both legacy (Etherscan/RPC: populated
    log_index, NULL order_hash) and subgraph (log_index=0, populated
    order_hash) rows in the batch, the legacy rows are dropped.

    The filter is a no-op for WS rows (``tx_hash=""``), so adding it
    here doesn't change WS-only callers' behaviour — it only matters
    when a backfill batch happens to contain both legacy and subgraph
    rows for the same on-chain transaction.
    """
    file_paths = [os.path.join(dirpath, f) for f in input_basenames]
    files_sql = ", ".join(f"'{_duckdb_escape(p)}'" for p in file_paths)
    files_with_order_sql = ", ".join(
        f"('{_duckdb_escape(path)}', {idx})" for idx, path in enumerate(file_paths)
    )

    desc_res = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet([{files_sql}], union_by_name=true)"
    ).fetchall()
    present_cols = {row[0] for row in desc_res}

    def _col_sql(name: str, default: str) -> str:
        if name in present_cols:
            return f"COALESCE(src.{name}, {default}) AS {name}"
        return f"{default} AS {name}"

    # Explicit CASTs on every pre-existing-but-recently-added column.
    # Pandas-written shards with all-NULL columns leave pyarrow free
    # to pick a non-canonical Arrow type (e.g. INT32 for an all-NULL
    # ``order_hash``), which then poisons DuckDB's ``union_by_name``
    # schema and breaks ``COALESCE(order_hash, '')`` with a
    # "Could not convert string '' to INT32" error.  The CASTs force
    # every column to its canonical TICKS_SCHEMA type before the
    # GROUP BY / COALESCE see it.
    tx_hash_sql = (
        f"CAST(COALESCE(src.tx_hash, '') AS VARCHAR) AS tx_hash"
        if "tx_hash" in present_cols
        else "CAST('' AS VARCHAR) AS tx_hash"
    )
    block_number_sql = _col_sql("block_number", "0")
    log_index_sql = _col_sql("log_index", "0")
    spot_price_usdt_sql = (
        "CAST(src.spot_price_usdt AS FLOAT) AS spot_price_usdt"
        if "spot_price_usdt" in present_cols
        else "NULL::FLOAT AS spot_price_usdt"
    )
    spot_price_ts_ms_sql = (
        "CAST(src.spot_price_ts_ms AS BIGINT) AS spot_price_ts_ms"
        if "spot_price_ts_ms" in present_cols
        else "NULL::BIGINT AS spot_price_ts_ms"
    )
    local_recv_ts_ns_sql = (
        "CAST(src.local_recv_ts_ns AS BIGINT) AS local_recv_ts_ns"
        if "local_recv_ts_ns" in present_cols
        else "NULL::BIGINT AS local_recv_ts_ns"
    )
    order_hash_sql = (
        "CAST(src.order_hash AS VARCHAR) AS order_hash"
        if "order_hash" in present_cols
        else "NULL::VARCHAR AS order_hash"
    )

    select_sql = _tick_consolidation_select_sql("sort_key")
    group_by_sql = ", ".join(_TICKS_DEDUP_COLS)

    result = con.execute(f"""
        COPY (
            WITH source_files(file_path, file_order) AS (
                VALUES {files_with_order_sql}
            ),
            ranked_rows AS (
                SELECT
                    src.market_id,
                    src.timestamp_ms,
                    src.token_id,
                    src.outcome,
                    src.side,
                    src.price,
                    src.size_usdc,
                    {tx_hash_sql},
                    {block_number_sql},
                    {log_index_sql},
                    src.source,
                    {spot_price_usdt_sql},
                    {spot_price_ts_ms_sql},
                    {local_recv_ts_ns_sql},
                    {order_hash_sql},
                    ((f.file_order::BIGINT << 32) + src.file_row_number::BIGINT) AS sort_key
                FROM read_parquet(
                    [{files_sql}],
                    union_by_name=true,
                    filename=true,
                    file_row_number=true
                ) AS src
                JOIN source_files AS f ON src.filename = f.file_path
            ),
            -- Legacy↔subgraph filter (see _run_tick_consolidation_pass
            -- for the rationale): drop legacy rows when a subgraph
            -- sibling exists for the same (market, ts, token, tx).
            -- WS rows have tx_hash='' so the filter is a no-op for them.
            subgraph_keys AS (
                SELECT DISTINCT market_id, timestamp_ms, token_id, tx_hash
                FROM ranked_rows
                WHERE order_hash IS NOT NULL
                  AND tx_hash <> ''
            ),
            dedup_input AS (
                SELECT r.*
                FROM ranked_rows AS r
                LEFT JOIN subgraph_keys AS sk
                    ON sk.market_id    = r.market_id
                   AND sk.timestamp_ms = r.timestamp_ms
                   AND sk.token_id     = r.token_id
                   AND sk.tx_hash      = r.tx_hash
                WHERE NOT (
                    r.tx_hash <> ''
                    AND r.order_hash IS NULL
                    AND sk.market_id IS NOT NULL
                )
            )
            SELECT
                {select_sql}
            FROM dedup_input AS ranked_rows
            GROUP BY {group_by_sql},
                     COALESCE(order_hash, ''),
                     COALESCE(local_recv_ts_ns, 0)
        ) TO '{_duckdb_escape(output_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """).fetchone()
    return result[0] if result else 0


def _compact_part_ws_ticks(
    *,
    duckdb_module: Any,
    dirpath: str,
    log: logging.Logger,
) -> None:
    """Tier-2 fold: collapse accumulated ``part-ws-*.parquet`` files into one.

    Each WS append-only batch creates a new ``part-ws-*.parquet`` file
    in the partition.  Without compaction these accumulate forever
    (production runs ~8 consolidations/day × N partitions).  When the
    count exceeds ``PM_PART_WS_COMPACTION_THRESHOLD`` we read all of
    them through DuckDB, run the same dedup pass used at append time
    (groupby on the full TICKS dedup key plus ``order_hash`` and
    ``local_recv_ts_ns``), and write a single new ``part-ws-*.parquet``
    file replacing the entire set.  ``part-0.parquet`` is **not**
    touched — same invariant as the regular WS append-only path.
    """
    threshold = _get_part_ws_compaction_threshold()
    part_ws_files = sorted(
        [
            f for f in os.listdir(dirpath)
            if f.startswith("part-ws-") and f.endswith(".parquet")
        ],
        key=lambda f: (os.path.getmtime(os.path.join(dirpath, f)), f),
    )
    if len(part_ws_files) < threshold:
        return

    log.info(
        "  -> Tier-2 compaction: folding %d part-ws-*.parquet files in %s",
        len(part_ws_files), dirpath,
    )

    epoch_ms = int(time.time() * 1000)
    pid = os.getpid()
    out_name = f"part-ws-{epoch_ms}-{pid}-compact.parquet"
    out_path = os.path.join(dirpath, out_name)
    tmp_path = f"{out_path}.{pid}.tmp"
    pass_temp_dir = _make_duckdb_temp_dir("ticks_compact")
    con = None
    try:
        con = duckdb_module.connect()
        _configure_duckdb_for_consolidation(con, temp_dir=pass_temp_dir)
        nrows = _run_ws_appendonly_pass(
            con,
            dirpath=dirpath,
            input_basenames=part_ws_files,
            output_path=tmp_path,
        )
        os.replace(tmp_path, out_path)
        _fsync_dir(dirpath)

        for fname in part_ws_files:
            try:
                os.remove(os.path.join(dirpath, fname))
            except OSError:
                pass

        log.info(
            "  -> Tier-2 compaction complete: %d rows in %s",
            nrows, out_name,
        )
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise
    finally:
        try:
            if con is not None:
                con.close()
        finally:
            shutil.rmtree(pass_temp_dir, ignore_errors=True)


def _consolidate_backfill_appendonly(
    *,
    duckdb_module: Any,
    dirpath: str,
    shard_files: list[str],
    batch_size: int,
    log: logging.Logger,
) -> None:
    """Consume backfill shards into new ``part-bf-{epoch_ms}-{pid}-{seq}.parquet`` files.

    Mirrors the WS append-only path: each batch is dedup'd within
    itself and emitted as a fresh ``part-bf-*`` file alongside
    ``part-0.parquet``; **part-0 is never folded.**  Cross-batch and
    cross-part-0 dedup are deferred to read time (every modern
    Parquet consumer already handles a multi-file partition via
    ``read_parquet(<dir>)``; downstream consumers GROUP BY the
    ``_TICKS_DEDUP_COLS`` key plus ``order_hash`` / ``local_recv_ts_ns``
    if they need to collapse re-fetched fills).

    Why not fold part-0 like the legacy path did?  Backfill is
    structurally allowed to repeat fills (re-fetched markets within
    the historical scan's 2-day rolling window), but folding part-0
    into every batch scales peak DuckDB memory + spill with
    **cumulative history** — the same failure mode that forced the
    WS append-only split.  In production (CAX21) one historical
    run wrote 874 BTC/5-minute backfill shards which, merged
    against a multi-GB part-0, ENOSPC'd a 22 GB scratch volume in
    < 2 minutes.  Append-only keeps peak spill bounded by batch
    size regardless of how big part-0 grows, and tier-2 compaction
    keeps the part-bf-* file count from drifting.
    """
    if not shard_files:
        return

    num_batches = max(1, (len(shard_files) + batch_size - 1) // batch_size)
    log.info(
        "  -> Backfill append-only: %d shards → %d batch(es) of <=%d",
        len(shard_files), num_batches, batch_size,
    )

    epoch_ms = int(time.time() * 1000)
    pid = os.getpid()
    settings_logged = False
    rows_total = 0

    for batch_idx in range(num_batches):
        batch = shard_files[batch_idx * batch_size : (batch_idx + 1) * batch_size]
        out_name = f"part-bf-{epoch_ms}-{pid}-{batch_idx:03d}.parquet"
        out_path = os.path.join(dirpath, out_name)
        tmp_path = f"{out_path}.{pid}.tmp"

        pass_temp_dir = _make_duckdb_temp_dir("backfill_ticks")
        con = None
        try:
            con = duckdb_module.connect()
            memory_limit, threads = _configure_duckdb_for_consolidation(
                con, temp_dir=pass_temp_dir,
            )
            if not settings_logged:
                free = _scratch_free_bytes(pass_temp_dir)
                free_str = (
                    f"{free / (1024 ** 3):.1f}GB free"
                    if free is not None else "free=?"
                )
                log.info(
                    "  -> DuckDB settings: memory_limit=%s threads=%s temp_dir=%s (%s)",
                    memory_limit or "default", threads, pass_temp_dir, free_str,
                )
                settings_logged = True
            log.info(
                "  -> Backfill Batch %d/%d (%d shards)",
                batch_idx + 1, num_batches, len(batch),
            )

            nrows = _run_ws_appendonly_pass(
                con, dirpath=dirpath, input_basenames=batch, output_path=tmp_path,
            )

            os.replace(tmp_path, out_path)
            _fsync_dir(dirpath)

            for fname in batch:
                try:
                    os.remove(os.path.join(dirpath, fname))
                except OSError:
                    pass

            rows_total += nrows
        except Exception:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise
        finally:
            try:
                if con is not None:
                    con.close()
            finally:
                shutil.rmtree(pass_temp_dir, ignore_errors=True)

    log.info(
        "  -> Backfill append-only complete: %d rows in %d new file(s)",
        rows_total, num_batches,
    )


def _compact_part_bf_ticks(
    *,
    duckdb_module: Any,
    dirpath: str,
    log: logging.Logger,
) -> None:
    """Tier-2 fold for ``part-bf-*.parquet`` files.

    Mirrors ``_compact_part_ws_ticks``: when the count of
    ``part-bf-*.parquet`` files in the partition crosses
    ``PM_PART_WS_COMPACTION_THRESHOLD``, fold them into a single
    ``part-bf-{epoch_ms}-{pid}-compact.parquet`` using the same
    dedup pass.  ``part-0.parquet`` is **not** touched — same
    invariant as the regular append-only path.

    The threshold env var is shared with WS compaction because
    the considerations are identical (read amplification, HF
    object count) and a single tunable is easier to reason about.
    """
    threshold = _get_part_ws_compaction_threshold()
    part_bf_files = sorted(
        [
            f for f in os.listdir(dirpath)
            if f.startswith("part-bf-") and f.endswith(".parquet")
        ],
        key=lambda f: (os.path.getmtime(os.path.join(dirpath, f)), f),
    )
    if len(part_bf_files) < threshold:
        return

    log.info(
        "  -> Tier-2 backfill compaction: folding %d part-bf-*.parquet files in %s",
        len(part_bf_files), dirpath,
    )

    epoch_ms = int(time.time() * 1000)
    pid = os.getpid()
    out_name = f"part-bf-{epoch_ms}-{pid}-compact.parquet"
    out_path = os.path.join(dirpath, out_name)
    tmp_path = f"{out_path}.{pid}.tmp"
    pass_temp_dir = _make_duckdb_temp_dir("backfill_compact")
    con = None
    try:
        con = duckdb_module.connect()
        _configure_duckdb_for_consolidation(con, temp_dir=pass_temp_dir)
        nrows = _run_ws_appendonly_pass(
            con,
            dirpath=dirpath,
            input_basenames=part_bf_files,
            output_path=tmp_path,
        )
        os.replace(tmp_path, out_path)
        _fsync_dir(dirpath)

        for fname in part_bf_files:
            try:
                os.remove(os.path.join(dirpath, fname))
            except OSError:
                pass

        log.info(
            "  -> Tier-2 backfill compaction complete: %d rows in %s",
            nrows, out_name,
        )
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise
    finally:
        try:
            if con is not None:
                con.close()
        finally:
            shutil.rmtree(pass_temp_dir, ignore_errors=True)


_WS_STAGING_FILENAME = "ws_staging.parquet"


def append_ws_ticks_staged(
    ticks_df: pd.DataFrame,
    *,
    ticks_dir: str | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Write WebSocket ticks as independent shard files per partition.

    Each flush writes a uniquely-named ``ws_ticks_{pid}_{ts}.parquet`` file.
    No read of existing data and no write-lock are needed (filenames are
    unique per PID+timestamp), so this function never blocks on the
    cross-process lock held by ``upload_to_huggingface()`` or
    ``consolidate_ticks()``.

    ``consolidate_ticks()`` merges all shard files on its next run and
    handles deduplication via DuckDB.  Shard files are excluded from
    Hugging Face uploads via the ``**/ws_ticks_*.parquet`` ignore pattern.
    """
    if ticks_df.empty:
        return

    log = logger or logging.getLogger("polymarket_pipeline")
    t_dir = ticks_dir or PARQUET_TICKS_DIR

    # Ensure required columns are present with defaults
    for col in _TICKS_EMPTY_COLS:
        if col not in ticks_df.columns:
            ticks_df = ticks_df.copy()
            if col in ("tx_hash",):
                ticks_df[col] = ""
            elif col in ("block_number", "log_index"):
                ticks_df[col] = 0
            else:
                ticks_df[col] = None

    # Flatten category columns so concat/dedup don't choke on dtype mismatches
    for col in ("outcome", "side", "source", "crypto", "timeframe"):
        if col in ticks_df.columns and hasattr(ticks_df[col], "cat"):
            ticks_df = ticks_df.copy()
            ticks_df[col] = ticks_df[col].astype(str)

    rows_staged = 0
    shard_suffix = f"ws_ticks_{os.getpid()}_{int(time.monotonic() * 1000)}.parquet"

    for (crypto, timeframe), group in ticks_df.groupby(
        ["crypto", "timeframe"], sort=False
    ):
        shard_dir = os.path.join(t_dir, f"crypto={crypto}", f"timeframe={timeframe}")
        os.makedirs(shard_dir, exist_ok=True)
        shard_path = os.path.join(shard_dir, shard_suffix)

        # Strip partition columns — they are encoded in the directory path
        new_rows = group.drop(columns=["crypto", "timeframe"]).reset_index(drop=True)
        for col in ("outcome", "side", "source"):
            if col in new_rows.columns and hasattr(new_rows[col], "cat"):
                new_rows[col] = new_rows[col].astype(str)

        table = pa.Table.from_pandas(new_rows, preserve_index=False)
        _write_parquet_atomic(table, shard_path)
        rows_staged += len(new_rows)

    log.info("WS ticks staged: %d new rows to %s/", rows_staged, t_dir)


def append_ws_spot_prices_staged(
    rows: list[dict],
    *,
    spot_prices_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Write RTDS spot price rows as an independent shard file.

    Each flush writes a uniquely-named ``ws_spot_{pid}_{ts}.parquet`` file.
    No read of existing data and no write-lock are needed (filenames are
    unique per PID+timestamp).  ``consolidate_spot_prices()`` merges all
    shard files on its next run.
    """
    if not rows:
        return

    log = logger or logging.getLogger("polymarket_pipeline")
    df = pd.DataFrame(rows)
    df["ts_ms"] = df["ts_ms"].astype("int64")
    df["price"] = df["price"].astype("float64")
    df["source"] = df["source"].astype("category")

    os.makedirs(spot_prices_dir, exist_ok=True)
    shard_name = f"ws_spot_{os.getpid()}_{int(time.monotonic() * 1000)}.parquet"
    shard_path = os.path.join(spot_prices_dir, shard_name)

    table = pa.Table.from_pandas(df, preserve_index=False)
    _write_parquet_atomic(table, shard_path)

    log.info("Spot prices staged: %d new rows to %s/", len(rows), spot_prices_dir)


def append_ws_heartbeats_staged(
    rows: list[dict],
    *,
    heartbeats_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Write WS heartbeat rows as an independent shard file.

    Each flush writes ``ws_hb_{pid}_{ts}.parquet``.  No read, no lock —
    the filename is unique per PID+monotonic-ms.  ``consolidate_heartbeats``
    merges them on its next run.

    Rows must carry the ``HEARTBEATS_SCHEMA`` columns
    (``ts_ms, source, shard_key, event_type, last_event_age_ms``).
    Missing columns get NULL in the output.
    """
    if not rows:
        return

    log = logger or logging.getLogger("polymarket_pipeline")
    df = pd.DataFrame(rows)
    # Coerce dtypes to match schema.
    if "ts_ms" in df.columns:
        df["ts_ms"] = pd.to_numeric(df["ts_ms"], errors="coerce").fillna(0).astype("int64")
    if "last_event_age_ms" in df.columns:
        df["last_event_age_ms"] = pd.to_numeric(df["last_event_age_ms"], errors="coerce").fillna(-1).astype("int64")
    for col in ("source", "shard_key", "event_type"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    os.makedirs(heartbeats_dir, exist_ok=True)
    shard_name = f"ws_hb_{os.getpid()}_{int(time.monotonic() * 1000)}.parquet"
    shard_path = os.path.join(heartbeats_dir, shard_name)
    table = pa.Table.from_pandas(df, preserve_index=False)
    _write_parquet_atomic(table, shard_path)
    log.debug("Heartbeats staged: %d new rows to %s/", len(rows), heartbeats_dir)


def append_ws_orderbook_staged(
    rows_df: pd.DataFrame,
    *,
    orderbook_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Write orderbook BBO rows as independent shard files per partition.

    Unlike the read-merge-write pattern used for ticks, orderbook data
    generates ~4 000 rows/s and reading back a growing staging file each
    flush would block the executor for seconds — causing the in-memory
    buffer to overflow and eventually OOM-kill the process.

    Instead, each flush writes a uniquely-named shard file per partition.
    No read of existing data and no write-lock are needed (filenames are
    unique per PID+timestamp).  ``consolidate_orderbook()`` merges all
    shard files on its next run.
    """
    if rows_df.empty:
        return

    log = logger or logging.getLogger("polymarket_pipeline")

    for col in ("outcome", "crypto", "timeframe"):
        if col in rows_df.columns and hasattr(rows_df[col], "cat"):
            rows_df = rows_df.copy()
            rows_df[col] = rows_df[col].astype(str)

    rows_staged = 0
    shard_suffix = f"ws_ob_{os.getpid()}_{int(time.monotonic() * 1000)}.parquet"

    for (crypto, timeframe), group in rows_df.groupby(
        ["crypto", "timeframe"], sort=False
    ):
        shard_dir = os.path.join(orderbook_dir, f"crypto={crypto}", f"timeframe={timeframe}")
        os.makedirs(shard_dir, exist_ok=True)
        shard_path = os.path.join(shard_dir, shard_suffix)

        new_rows = group.drop(columns=["crypto", "timeframe"]).reset_index(drop=True)
        for col in ("outcome",):
            if col in new_rows.columns and hasattr(new_rows[col], "cat"):
                new_rows[col] = new_rows[col].astype(str)

        table = pa.Table.from_pandas(new_rows, preserve_index=False)
        _write_parquet_atomic(table, shard_path)
        rows_staged += len(new_rows)

    log.info("Orderbook BBO staged: %d new rows to %s/", rows_staged, orderbook_dir)


def append_ws_prices_staged(
    prices_df: pd.DataFrame,
    *,
    prices_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Write WebSocket crypto price rows as independent shard files per partition.

    Each flush writes a uniquely-named ``ws_prices_{pid}_{ts}.parquet`` file.
    No read of existing data and no write-lock are needed.
    ``consolidate_prices()`` merges all shard files on its next run.
    """
    if prices_df.empty:
        return

    log = logger or logging.getLogger("polymarket_pipeline")

    for col in ("crypto", "timeframe"):
        if col in prices_df.columns and hasattr(prices_df[col], "cat"):
            prices_df = prices_df.copy()
            prices_df[col] = prices_df[col].astype(str)

    rows_staged = 0
    shard_suffix = f"ws_prices_{os.getpid()}_{int(time.monotonic() * 1000)}.parquet"

    for (crypto, timeframe), group in prices_df.groupby(
        ["crypto", "timeframe"], sort=False
    ):
        shard_dir = os.path.join(prices_dir, f"crypto={crypto}", f"timeframe={timeframe}")
        os.makedirs(shard_dir, exist_ok=True)
        shard_path = os.path.join(shard_dir, shard_suffix)

        # Keep only price columns — partition columns are in the directory path,
        # and market metadata columns (question, volume, etc.) belong in markets.parquet.
        price_cols = ["market_id", "timestamp", "up_price", "down_price"]
        new_rows = group[price_cols].reset_index(drop=True)
        new_rows["timestamp"] = new_rows["timestamp"].astype("int64")
        new_rows["up_price"] = new_rows["up_price"].astype("float32")
        new_rows["down_price"] = new_rows["down_price"].astype("float32")

        table = pa.Table.from_pandas(new_rows, schema=PRICES_SCHEMA, preserve_index=False)
        _write_parquet_atomic(table, shard_path)
        rows_staged += len(new_rows)

    log.info("WS prices staged: %d new rows to %s/", rows_staged, prices_dir)


def append_ws_culture_prices_staged(
    prices_df: pd.DataFrame,
    *,
    prices_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Write WebSocket culture price rows as independent shard files per partition.

    Each flush writes a uniquely-named ``ws_culture_prices_{pid}_{ts}.parquet``
    file.  No read of existing data and no write-lock are needed.
    ``consolidate_culture_prices()`` merges all shard files on its next run.
    """
    if prices_df.empty:
        return

    log = logger or logging.getLogger("polymarket_pipeline")

    for col in ("crypto", "timeframe", "outcome"):
        if col in prices_df.columns and hasattr(prices_df[col], "cat"):
            prices_df = prices_df.copy()
            prices_df[col] = prices_df[col].astype(str)

    rows_staged = 0
    shard_suffix = f"ws_culture_prices_{os.getpid()}_{int(time.monotonic() * 1000)}.parquet"

    for (crypto, timeframe), group in prices_df.groupby(
        ["crypto", "timeframe"], sort=False
    ):
        shard_dir = os.path.join(prices_dir, f"crypto={crypto}", f"timeframe={timeframe}")
        os.makedirs(shard_dir, exist_ok=True)
        shard_path = os.path.join(shard_dir, shard_suffix)

        price_cols = ["market_id", "timestamp", "token_id", "outcome", "price"]
        new_rows = group[price_cols].reset_index(drop=True)
        new_rows["timestamp"] = new_rows["timestamp"].astype("int64")
        new_rows["price"] = new_rows["price"].astype("float32")
        new_rows["outcome"] = new_rows["outcome"].astype(str)

        table = pa.Table.from_pandas(new_rows, schema=CULTURE_PRICES_SCHEMA, preserve_index=False)
        _write_parquet_atomic(table, shard_path)
        rows_staged += len(new_rows)

    log.info("WS culture prices staged: %d new rows to %s/", rows_staged, prices_dir)


def consolidate_prices(
    *,
    prices_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Consolidate WS price shard files into canonical part-0.parquet per partition.

    Walks the Hive-partitioned prices directory.  For each leaf partition that
    contains shard files (``ws_prices_*.parquet``), reads all ``.parquet`` files,
    deduplicates on ``(market_id, timestamp)``, and writes a single
    ``part-0.parquet``.  Shard files are removed after successful merge.
    """
    _consolidate_partitioned_prices(
        root_dir=prices_dir,
        dedup_cols=["market_id", "timestamp"],
        sort_cols=["market_id", "timestamp"],
        shard_prefix="ws_prices_",
        label="prices",
        logger=logger,
    )


def consolidate_culture_prices(
    *,
    prices_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Consolidate WS culture-price shard files per partition.

    Same as ``consolidate_prices`` but deduplicates on
    ``(market_id, timestamp, outcome)`` to handle multi-outcome markets.
    """
    _consolidate_partitioned_prices(
        root_dir=prices_dir,
        dedup_cols=["market_id", "timestamp", "outcome"],
        sort_cols=["market_id", "timestamp", "outcome"],
        shard_prefix="ws_culture_prices_",
        label="culture prices",
        logger=logger,
    )


def _consolidate_partitioned_prices(
    *,
    root_dir: str,
    dedup_cols: list[str],
    sort_cols: list[str],
    shard_prefix: str,
    label: str,
    logger: logging.Logger | None = None,
) -> None:
    """Shared implementation for partition-level price shard consolidation.

    Streams through DuckDB rather than concatenating all shards into pandas:
    a backed-up shard backlog (e.g. after a multi-day HF upload outage) used
    to materialise as multi-GB pandas DataFrames inside the 8 GB CAX21 and
    OOM the process.  DuckDB's ``arg_max`` aggregation, plus a configurable
    memory cap with disk spilling (``_configure_duckdb_for_consolidation``),
    keeps peak memory bounded regardless of input volume.
    """
    import duckdb

    log = logger or logging.getLogger("polymarket_pipeline")
    if not os.path.exists(root_dir):
        return

    data_root = os.path.dirname(os.path.abspath(root_dir))

    for dirpath, _dirs, _fnames in os.walk(root_dir):
        # Cheap pre-check OUTSIDE the lock to avoid acquiring it for every
        # leaf directory in the tree (each acquisition costs ~1 ms in fcntl).
        parquet_files_pre = [f for f in os.listdir(dirpath) if f.endswith(".parquet")]
        has_shards_pre = any(f.startswith(shard_prefix) for f in parquet_files_pre)
        if not has_shards_pre:
            continue

        with _write_lock(data_root):
            # Re-check inside lock (files may have been consolidated by another process)
            parquet_files = [f for f in os.listdir(dirpath) if f.endswith(".parquet")]
            has_shards = any(f.startswith(shard_prefix) for f in parquet_files)
            if not has_shards:
                continue

            rel = os.path.relpath(dirpath, root_dir)
            log.info("Consolidating %s partition %s (%d files)...", label, rel, len(parquet_files))

            # Deterministic read order: oldest file first, ties broken
            # lexicographically.  Each row is tagged with its file_order so
            # arg_max(keep latest by file_order) reproduces the
            # ``drop_duplicates(keep="last")`` semantics of the legacy path.
            parquet_files.sort(
                key=lambda f: (os.path.getmtime(os.path.join(dirpath, f)), f)
            )
            file_paths = [os.path.join(dirpath, f) for f in parquet_files]
            files_sql = ", ".join(f"'{_duckdb_escape(p)}'" for p in file_paths)
            files_with_order_sql = ", ".join(
                f"('{_duckdb_escape(path)}', {idx})" for idx, path in enumerate(file_paths)
            )
            consolidated_path = os.path.join(dirpath, "part-0.parquet")
            tmp_path = f"{consolidated_path}.{os.getpid()}.tmp"

            # Spill outside the data tree: see ``_make_duckdb_temp_dir``.
            temp_dir = _make_duckdb_temp_dir(f"prices_{label}")
            con = None
            try:
                _check_disk_space(dirpath)
                con = duckdb.connect()
                _configure_duckdb_for_consolidation(con, temp_dir=temp_dir)

                # Discover the union schema once so we can build the
                # SELECT list dynamically (legacy shards may be missing
                # newer columns; ``union_by_name=true`` fills them with NULL).
                desc = con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet([{files_sql}], union_by_name=true)"
                ).fetchall()
                present_cols = [row[0] for row in desc]

                # Project dedup_cols verbatim, all other columns through
                # ``arg_max`` keyed on (file_order, file_row_number) so the
                # newest shard's value wins on a tie — same semantics as
                # ``drop_duplicates(keep="last")``.
                #
                # Skip Hive partition columns (``crypto``, ``timeframe``):
                # they're encoded in the directory path and writing them
                # into the file body produces a dict-vs-string schema
                # mismatch when ``pq.ParquetDataset`` re-reads the
                # partitioned tree (the directory yields a dictionary
                # encoding while DuckDB's COPY emits plain strings).
                _PARTITION_COLS = {"crypto", "timeframe"}
                non_dedup = [
                    c for c in present_cols
                    if c not in dedup_cols and c not in _PARTITION_COLS
                ]
                non_dedup_sql = ",\n                            ".join(
                    f"arg_max(src.{c}, sort_key) AS {c}" for c in non_dedup
                )
                dedup_select_sql = ", ".join(f"src.{c}" for c in dedup_cols)
                group_by_sql = ", ".join(f"src.{c}" for c in dedup_cols)

                # Build the final projection.  ``non_dedup_sql`` may be
                # empty when every column is either a dedup key or a
                # partition column (e.g. extremely narrow schemas in
                # tests); guard the trailing comma.
                projection_sql = dedup_select_sql
                if non_dedup_sql:
                    projection_sql = f"{dedup_select_sql},\n                            {non_dedup_sql}"

                con.execute(f"""
                    COPY (
                        WITH source_files(file_path, file_order) AS (
                            VALUES {files_with_order_sql}
                        ),
                        ranked AS (
                            SELECT
                                src.*,
                                ((f.file_order::BIGINT << 32)
                                  + src.file_row_number::BIGINT) AS sort_key
                            FROM read_parquet(
                                [{files_sql}],
                                union_by_name=true,
                                filename=true,
                                file_row_number=true
                            ) AS src
                            JOIN source_files AS f ON src.filename = f.file_path
                        )
                        SELECT
                            {projection_sql}
                        FROM ranked AS src
                        GROUP BY {group_by_sql}
                    ) TO '{_duckdb_escape(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """)

                # Stamp schema version on the output (DuckDB doesn't
                # carry our Arrow metadata through COPY).  Use
                # ``ParquetFile.read()`` rather than ``pq.read_table()``
                # so ParquetDataset's Hive-partition auto-discovery
                # doesn't trip over sibling partitions with mismatched
                # categorical encodings.
                table = pq.ParquetFile(tmp_path).read()
                stamped = _stamp_schema_version(table)
                pq.write_table(stamped, tmp_path, compression="zstd")

                # Replace consolidated file FIRST so a crash between the
                # two steps leaves orphan shards (idempotent — the next
                # consolidation run merges them again) rather than an
                # empty partition that the HF upload's delete_patterns
                # would propagate to the Hub.
                os.replace(tmp_path, consolidated_path)
                _fsync_dir(dirpath)
                for fname in parquet_files:
                    if fname == "part-0.parquet":
                        continue  # already replaced atomically above
                    try:
                        os.remove(os.path.join(dirpath, fname))
                    except OSError:
                        pass

                nrows = pq.read_metadata(consolidated_path).num_rows
                log.info("  -> %s consolidated: %d rows", rel, nrows)
            except Exception:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                raise
            finally:
                try:
                    if con is not None:
                        con.close()
                finally:
                    shutil.rmtree(temp_dir, ignore_errors=True)


def consolidate_spot_prices(
    *,
    spot_prices_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Consolidate spot price staging files into a single sorted Parquet file."""
    log = logger or logging.getLogger("polymarket_pipeline")
    if not os.path.exists(spot_prices_dir):
        return

    parquet_files = [f for f in os.listdir(spot_prices_dir) if f.endswith(".parquet")]
    if not parquet_files or parquet_files == ["part-0.parquet"]:
        return

    data_root = os.path.dirname(os.path.abspath(spot_prices_dir))
    with _write_lock(data_root):
        # Re-check inside lock
        parquet_files = [f for f in os.listdir(spot_prices_dir) if f.endswith(".parquet")]
        if not parquet_files or parquet_files == ["part-0.parquet"]:
            return

        log.info("Consolidating spot prices (%d files)...", len(parquet_files))
        # Deterministic read order: oldest file first so drop_duplicates(
        # keep="last") selects the most recently written duplicate.
        parquet_files.sort(
            key=lambda f: (os.path.getmtime(os.path.join(spot_prices_dir, f)), f)
        )
        frames = []
        for fname in parquet_files:
            try:
                df = pq.ParquetFile(os.path.join(spot_prices_dir, fname)).read().to_pandas()
                frames.append(df)
            except Exception:
                continue

        if not frames:
            return

        merged = pd.concat(frames, ignore_index=True)
        # Dedup on (ts_ms, symbol, source) — same price at same time from same source
        merged = merged.drop_duplicates(subset=["ts_ms", "symbol", "source"], keep="last")
        merged = merged.sort_values("ts_ms").reset_index(drop=True)
        merged["source"] = merged["source"].astype("category")

        consolidated_path = os.path.join(spot_prices_dir, "part-0.parquet")
        table = pa.Table.from_pandas(merged, preserve_index=False)
        tmp_path = f"{consolidated_path}.{os.getpid()}.tmp"
        try:
            _check_disk_space(spot_prices_dir)
            table = _stamp_schema_version(table)
            pq.write_table(table, tmp_path, compression="zstd")
            # Replace consolidated file FIRST; orphaned shards on crash
            # are idempotent (next run merges them).  Reverse order risks
            # an empty partition that delete_patterns would propagate.
            os.replace(tmp_path, consolidated_path)
            _fsync_dir(spot_prices_dir)
            for fname in parquet_files:
                if fname == "part-0.parquet":
                    continue  # already replaced atomically above
                try:
                    os.remove(os.path.join(spot_prices_dir, fname))
                except OSError:
                    pass
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    log.info("Spot prices consolidated: %d rows", len(merged))


def consolidate_heartbeats(
    *,
    heartbeats_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Merge heartbeat shard files into a single sorted ``part-0.parquet``.

    Volume is tiny (~6 rows / 10 s / shard), so pandas is fine — no
    DuckDB needed.  Dedup key is ``(ts_ms, source, shard_key)``.
    """
    log = logger or logging.getLogger("polymarket_pipeline")
    if not os.path.exists(heartbeats_dir):
        return

    parquet_files = [f for f in os.listdir(heartbeats_dir) if f.endswith(".parquet")]
    if not parquet_files or parquet_files == ["part-0.parquet"]:
        return

    data_root = os.path.dirname(os.path.abspath(heartbeats_dir))
    with _write_lock(data_root):
        parquet_files = [f for f in os.listdir(heartbeats_dir) if f.endswith(".parquet")]
        if not parquet_files or parquet_files == ["part-0.parquet"]:
            return

        log.info("Consolidating heartbeats (%d files)...", len(parquet_files))
        parquet_files.sort(
            key=lambda f: (os.path.getmtime(os.path.join(heartbeats_dir, f)), f)
        )
        frames = []
        for fname in parquet_files:
            try:
                df = pq.ParquetFile(os.path.join(heartbeats_dir, fname)).read().to_pandas()
                frames.append(df)
            except Exception:
                continue

        if not frames:
            return

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["ts_ms", "source", "shard_key"], keep="last")
        merged = merged.sort_values("ts_ms").reset_index(drop=True)

        consolidated_path = os.path.join(heartbeats_dir, "part-0.parquet")
        tmp_path = f"{consolidated_path}.{os.getpid()}.tmp"
        try:
            table = pa.Table.from_pandas(merged, schema=HEARTBEATS_SCHEMA, preserve_index=False)
            _check_disk_space(heartbeats_dir)
            stamped = _stamp_schema_version(table)
            pq.write_table(stamped, tmp_path, compression="zstd")
            # Replace consolidated file FIRST; orphan shards on crash
            # are idempotent (next run merges them).
            os.replace(tmp_path, consolidated_path)
            _fsync_dir(heartbeats_dir)
            remove_errors: list[str] = []
            for fname in parquet_files:
                if fname == "part-0.parquet":
                    continue  # already replaced atomically above
                try:
                    os.remove(os.path.join(heartbeats_dir, fname))
                except OSError as exc:
                    remove_errors.append(f"{fname}: {exc}")
            if remove_errors:
                log.warning(
                    "  -> could not remove %d old heartbeat shard file(s): %s",
                    len(remove_errors), "; ".join(remove_errors),
                )
        except Exception:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise

        log.info("  -> heartbeats consolidated: %d rows", len(merged))


def _compact_part_ws_orderbook(
    *,
    duckdb_module: Any,
    dirpath: str,
    log: logging.Logger,
) -> None:
    """Tier-2 fold: collapse accumulated orderbook ``part-ws-*.parquet`` files.

    Mirrors ``_compact_part_ws_ticks`` but uses the orderbook's
    ``arg_max(... ORDER BY local_recv_ts_ns)`` recency-tiebreak dedup
    so multiple snapshots for the same
    ``(ts_ms, market_id, token_id, outcome)`` collapse to the latest.
    """
    threshold = _get_part_ws_compaction_threshold()
    part_ws_files = sorted(
        [
            f for f in os.listdir(dirpath)
            if f.startswith("part-ws-") and f.endswith(".parquet")
        ],
        key=lambda f: (os.path.getmtime(os.path.join(dirpath, f)), f),
    )
    if len(part_ws_files) < threshold:
        return

    log.info(
        "  -> Tier-2 orderbook compaction: folding %d part-ws-*.parquet files in %s",
        len(part_ws_files), dirpath,
    )

    epoch_ms = int(time.time() * 1000)
    pid = os.getpid()
    out_name = f"part-ws-{epoch_ms}-{pid}-compact.parquet"
    out_path = os.path.join(dirpath, out_name)
    tmp_path = f"{out_path}.{pid}.tmp"
    pass_temp_dir = _make_duckdb_temp_dir("orderbook_compact")
    con = None
    try:
        con = duckdb_module.connect()
        _configure_duckdb_for_consolidation(con, temp_dir=pass_temp_dir)

        file_paths = [os.path.join(dirpath, f) for f in part_ws_files]
        files_sql = ", ".join(f"'{_duckdb_escape(p)}'" for p in file_paths)

        desc_res = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet([{files_sql}], union_by_name=true)"
        ).fetchall()
        present_cols = {row[0] for row in desc_res}
        local_recv_present = "local_recv_ts_ns" in present_cols
        recency_sql = (
            "COALESCE(local_recv_ts_ns, ts_ms * 1000000)"
            if local_recv_present
            else "ts_ms * 1000000"
        )
        local_recv_select = (
            "max(local_recv_ts_ns) AS local_recv_ts_ns"
            if local_recv_present
            else "NULL::BIGINT AS local_recv_ts_ns"
        )
        result = con.execute(f"""
            COPY (
                SELECT
                    ts_ms, market_id, token_id, outcome,
                    arg_max(best_bid,       {recency_sql}) AS best_bid,
                    arg_max(best_ask,       {recency_sql}) AS best_ask,
                    arg_max(best_bid_size,  {recency_sql}) AS best_bid_size,
                    arg_max(best_ask_size,  {recency_sql}) AS best_ask_size,
                    {local_recv_select}
                FROM read_parquet(
                    [{files_sql}],
                    union_by_name=true
                )
                GROUP BY ts_ms, market_id, token_id, outcome
            ) TO '{_duckdb_escape(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """).fetchone()
        nrows = result[0] if result else 0

        os.replace(tmp_path, out_path)
        _fsync_dir(dirpath)

        for fname in part_ws_files:
            try:
                os.remove(os.path.join(dirpath, fname))
            except OSError:
                pass

        log.info(
            "  -> Tier-2 orderbook compaction complete: %d rows in %s",
            nrows, out_name,
        )
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise
    finally:
        try:
            if con is not None:
                con.close()
        finally:
            shutil.rmtree(pass_temp_dir, ignore_errors=True)


def consolidate_orderbook(
    *,
    orderbook_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Consolidate orderbook staging files per partition.

    Append-only by construction.  Orderbook has a single writer
    (``append_ws_orderbook_staged``) producing ``ws_ob_*.parquet``
    shards from CLOB WebSocket ``price_change`` events; there's no
    backfill path.  Shards never overlap with rows already in
    ``part-0.parquet`` (live BBO snapshots have current-time ts_ms
    values, not historical), so folding part-0 on every run was
    pure waste — peak memory and spill scaled with cumulative
    history rather than batch size, ENOSPC'ing the production
    CAX21 on backlogged hot partitions (e.g. BNB/5-minute with
    16 k+ shards estimated 14 hours / "0% remaining" before the
    GROUP BY would even start emitting rows).

    Each batch is written to a new ``part-ws-{epoch_ms}-{seq}.parquet``
    file alongside the existing ``part-0.parquet``.  Within-batch
    dedup keeps the latest BBO snapshot per
    ``(ts_ms, market_id, token_id, outcome)``; cross-batch dedup is
    skipped (production WS messages at the same ms are extremely
    rare and would represent legitimately distinct snapshots
    anyway).  Reading a multi-file partition is transparent to
    DuckDB / pyarrow / pandas via ``read_parquet(<dir>)``.
    """
    import duckdb

    log = logger or logging.getLogger("polymarket_pipeline")
    if not os.path.exists(orderbook_dir):
        return

    data_root = os.path.dirname(os.path.abspath(orderbook_dir))

    # Prefer the module attribute when tests monkeypatch it; only
    # consult the env var when the attribute is at its default.
    if _TICK_CONSOLIDATION_BATCH_SIZE != _DEFAULT_TICK_CONSOLIDATION_BATCH_SIZE:
        batch_size = _TICK_CONSOLIDATION_BATCH_SIZE
    else:
        batch_size = _get_tick_consolidation_batch_size()

    for dirpath, _dirs, _fnames in os.walk(orderbook_dir):
        with _write_lock(data_root):
            parquet_files = [f for f in os.listdir(dirpath) if f.endswith(".parquet")]
            # Anything that's not an already-consolidated part file
            # is a WS shard.  Production writes only ``ws_ob_*.parquet``
            # but we accept any non-``part-0`` / non-``part-ws-*`` name
            # so unit tests using arbitrary shard names continue to work.
            shard_files = [
                f for f in parquet_files
                if f != "part-0.parquet" and not f.startswith("part-ws-")
            ]
            part_ws_count = sum(
                1 for f in parquet_files if f.startswith("part-ws-")
            )
            needs_compact = part_ws_count >= _get_part_ws_compaction_threshold()
            if not shard_files and not needs_compact:
                continue

            rel = os.path.relpath(dirpath, orderbook_dir)
            log.info(
                "Consolidating orderbook partition %s (%d shard files, part_ws=%d)...",
                rel, len(shard_files), part_ws_count,
            )
            _check_disk_space(dirpath)

            shard_files.sort(
                key=lambda f: (os.path.getmtime(os.path.join(dirpath, f)), f)
            )

            # If only tier-2 compaction is needed (no fresh shards),
            # skip the batch loop entirely.  Otherwise run the normal
            # append path then tier-2 fold once at the end.
            num_batches = (
                max(1, (len(shard_files) + batch_size - 1) // batch_size)
                if shard_files else 0
            )
            if num_batches > 1:
                log.info(
                    "  -> Append-only: %d shards → %d batch(es) of <=%d",
                    len(shard_files), num_batches, batch_size,
                )

            epoch_ms = int(time.time() * 1000)
            pid = os.getpid()
            settings_logged = False
            rows_total = 0

            for batch_idx in range(num_batches):
                batch = shard_files[batch_idx * batch_size : (batch_idx + 1) * batch_size]
                # Include PID so two consolidations within the same
                # millisecond don't overwrite each other's batch_000.
                out_name = f"part-ws-{epoch_ms}-{pid}-{batch_idx:03d}.parquet"
                out_path = os.path.join(dirpath, out_name)
                tmp_path = f"{out_path}.{pid}.tmp"

                pass_temp_dir = _make_duckdb_temp_dir("orderbook")
                con = None
                try:
                    con = duckdb.connect()
                    memory_limit, threads = _configure_duckdb_for_consolidation(
                        con, temp_dir=pass_temp_dir,
                    )
                    if not settings_logged:
                        free = _scratch_free_bytes(pass_temp_dir)
                        free_str = (
                            f"{free / (1024 ** 3):.1f}GB free"
                            if free is not None else "free=?"
                        )
                        log.info(
                            "  -> DuckDB settings: memory_limit=%s threads=%s temp_dir=%s (%s)",
                            memory_limit or "default", threads, pass_temp_dir, free_str,
                        )
                        settings_logged = True
                    if num_batches > 1:
                        log.info(
                            "  -> Batch %d/%d (%d shards)",
                            batch_idx + 1, num_batches, len(batch),
                        )

                    file_paths = [os.path.join(dirpath, f) for f in batch]
                    files_sql = ", ".join(f"'{_duckdb_escape(p)}'" for p in file_paths)

                    desc_res = con.execute(
                        f"DESCRIBE SELECT * FROM read_parquet([{files_sql}], union_by_name=true)"
                    ).fetchall()
                    present_cols = {row[0] for row in desc_res}
                    local_recv_present = "local_recv_ts_ns" in present_cols
                    recency_sql = (
                        "COALESCE(local_recv_ts_ns, ts_ms * 1000000)"
                        if local_recv_present
                        else "ts_ms * 1000000"
                    )
                    local_recv_select = (
                        "max(local_recv_ts_ns) AS local_recv_ts_ns"
                        if local_recv_present
                        else "NULL::BIGINT AS local_recv_ts_ns"
                    )
                    result = con.execute(f"""
                        COPY (
                            SELECT
                                ts_ms, market_id, token_id, outcome,
                                arg_max(best_bid,       {recency_sql}) AS best_bid,
                                arg_max(best_ask,       {recency_sql}) AS best_ask,
                                arg_max(best_bid_size,  {recency_sql}) AS best_bid_size,
                                arg_max(best_ask_size,  {recency_sql}) AS best_ask_size,
                                {local_recv_select}
                            FROM read_parquet(
                                [{files_sql}],
                                union_by_name=true
                            )
                            GROUP BY ts_ms, market_id, token_id, outcome
                        ) TO '{_duckdb_escape(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
                    """).fetchone()
                    nrows = result[0] if result else 0

                    os.replace(tmp_path, out_path)
                    _fsync_dir(dirpath)

                    for fname in batch:
                        try:
                            os.remove(os.path.join(dirpath, fname))
                        except OSError:
                            pass

                    rows_total += nrows
                except Exception:
                    if os.path.exists(tmp_path):
                        try:
                            os.remove(tmp_path)
                        except OSError:
                            pass
                    raise
                finally:
                    try:
                        if con is not None:
                            con.close()
                    finally:
                        shutil.rmtree(pass_temp_dir, ignore_errors=True)

            if num_batches:
                log.info(
                    "  -> %s consolidated: %d rows in %d new file(s)",
                    rel, rows_total, num_batches,
                )

            # Tier-2 fold: bound long-term part-ws-* accumulation.
            _compact_part_ws_orderbook(
                duckdb_module=duckdb,
                dirpath=dirpath,
                log=log,
            )


# ---------------------------------------------------------------------------
# Write helpers  (atomic: write to per-PID .tmp dir then rename)
# ---------------------------------------------------------------------------

def _fsync_dir(path: str) -> None:
    """Best-effort fsync of a directory so a recent ``os.replace`` is durable.

    POSIX guarantees that ``os.replace`` is atomic, but NOT durable —
    on power loss between rename and dirent flush, the new file may be
    missing despite the call returning success.  fsync of the
    containing directory makes the rename survive a crash.

    Failures are swallowed: not all filesystems support directory
    fsync (notably Windows; some FUSE mounts), and we'd rather lose
    durability than crash the writer over an unsupported syscall.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


# All shard-file prefixes written by the lock-free WS / backfill paths.
# ``_write_partitioned_atomic`` uses this allowlist to avoid deleting
# in-flight shards during its post-replace cleanup.  Any new shard
# writer MUST add its prefix here (a regression test enforces parity).
_KNOWN_SHARD_PREFIXES: tuple[str, ...] = (
    "ws_ticks_", "ws_prices_", "ws_culture_prices_",
    "ws_spot_", "ws_ob_", "ws_hb_", "ws_staging",
    "backfill_", "binance_history_",
)


def _write_parquet_atomic(table: pa.Table, path: str) -> None:
    """Atomically write a single Parquet file using a per-PID temp path."""
    dest_dir = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(dest_dir, exist_ok=True)
    _check_disk_space(dest_dir)
    table = _stamp_schema_version(table)
    # Per-PID suffix prevents concurrent processes from colliding on the same tmp path.
    tmp_path = f"{path}.{os.getpid()}.tmp"
    try:
        pq.write_table(table, tmp_path, compression="zstd")
        os.replace(tmp_path, path)
        _fsync_dir(dest_dir)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def _write_partitioned_atomic(table: pa.Table, root_dir: str, partition_cols: list[str]) -> None:
    """Atomically write a Hive-partitioned Parquet dataset using a per-PID temp dir.

    Each process uses a unique ``<root>.tmp.<pid>`` directory so that a
    concurrent process running ``_write_partitioned_atomic`` on the same
    *root_dir* cannot accidentally delete or overwrite the in-flight data.
    The outer ``_write_lock`` still serialises the full read→merge→write
    cycle; the per-PID tmp is defence-in-depth against any unforeseen
    path where the lock is not held.

    Uses stable ``part-{i}.parquet`` filenames (instead of PyArrow's default
    UUID-based names) combined with ``os.replace`` (atomic on POSIX) so that
    files are never absent — only atomically swapped.  This prevents TOCTOU
    races where a concurrent reader (e.g. HF ``upload_folder``) lists a file
    and then fails to open it because a writer deleted it between listing and
    reading.
    """
    _check_disk_space(os.path.dirname(os.path.abspath(root_dir)) or ".")
    table = _stamp_schema_version(table)
    tmp_dir = f"{root_dir}.tmp.{os.getpid()}"
    try:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        pq.write_to_dataset(
            table,
            root_path=tmp_dir,
            partition_cols=partition_cols,
            compression="zstd",
            basename_template="part-{i}.parquet",
        )
        # Merge: move new partition files into the real directory.
        # Replace-then-clean: os.replace atomically swaps the file so that
        # concurrent readers always see a valid file (old or new content).
        # Stale files from previous writes (e.g. UUID-named files or extra
        # part-N files when the row-group count shrinks) are removed AFTER
        # the new files are in place — never before.
        os.makedirs(root_dir, exist_ok=True)
        for dirpath, _, filenames in os.walk(tmp_dir):
            rel = os.path.relpath(dirpath, tmp_dir)
            target_dir = os.path.join(root_dir, rel)
            os.makedirs(target_dir, exist_ok=True)
            new_files = set(filenames)
            for fname in filenames:
                src = os.path.join(dirpath, fname)
                dst = os.path.join(target_dir, fname)
                os.replace(src, dst)
            # Durability: fsync the target directory so the renames
            # survive a power loss between syscall return and dirent flush.
            _fsync_dir(target_dir)
            # Remove stale Parquet files that aren't part of the new write.
            # Skip lock-free shard files written by concurrent processes
            # (WS service, tick backfill) — those are merged by their own
            # consolidation functions and must not be deleted here.  The
            # prefix list is a module-level constant (``_KNOWN_SHARD_PREFIXES``)
            # so adding a new shard writer also requires updating the
            # allowlist; a regression test enforces parity.
            for old_f in os.listdir(target_dir):
                if old_f.endswith(".parquet") and old_f not in new_files:
                    if old_f.startswith(_KNOWN_SHARD_PREFIXES):
                        continue
                    try:
                        os.remove(os.path.join(target_dir, old_f))
                    except OSError:
                        pass
    finally:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)


def persist_normalized(
    markets_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    *,
    markets_path: str | None = None,
    prices_dir: str | None = None,
    logger: logging.Logger | None = None,
    skip_markets: bool = False,
    lock_timeout: float | None = None,
) -> None:
    """Merge new data with existing data on disk and write Parquet.

    - Markets: merge on market_id (keep latest).  Skipped when *skip_markets*
      is True (used by the WebSocket flush path where market metadata is
      already up-to-date from the startup historical scan, saving an
      unnecessary read+write on every 5-second flush cycle).
    - Prices: merge on (market_id, timestamp) (deduplicate).

    Both written with Zstd compression and optimised dtypes.

    The entire read → merge → write cycle is protected by an exclusive write
    lock (threading + fcntl) so concurrent callers — whether from
    ``run_in_executor`` threads or a separate OS process — cannot interleave.
    """
    log = logger or logging.getLogger("polymarket_pipeline")
    m_path = markets_path or PARQUET_MARKETS_PATH
    p_dir = prices_dir or PARQUET_PRICES_DIR

    # Derive the data root (parent directory of markets.parquet) to use as the
    # canonical lock key, so markets and prices always share the same lock.
    data_root = os.path.dirname(os.path.abspath(m_path))

    with _write_lock(data_root, timeout=lock_timeout):
        # --- Markets ---
        if not skip_markets:
            existing_markets = load_markets(m_path)
            # Flatten category columns to plain strings so pd.concat doesn't choke on
            # dtype mismatches between data read from Parquet and new incoming data.
            for col in ("crypto", "timeframe"):
                if col in existing_markets.columns and hasattr(existing_markets[col], "cat"):
                    existing_markets[col] = existing_markets[col].astype(str)
                if col in markets_df.columns and hasattr(markets_df[col], "cat"):
                    markets_df = markets_df.copy()
                    markets_df[col] = markets_df[col].astype(str)
            if not markets_df.empty:
                dfs_to_concat = [df for df in (existing_markets, markets_df) if not df.empty]
                merged_markets = (
                    pd.concat(dfs_to_concat, ignore_index=True) if dfs_to_concat else existing_markets
                )
                merged_markets = (
                    merged_markets.drop_duplicates(subset=["market_id"], keep="last")
                    .reset_index(drop=True)
                )
            else:
                merged_markets = existing_markets

            merged_markets = optimise_markets_df(merged_markets)
            table_m = pa.Table.from_pandas(merged_markets, schema=MARKETS_SCHEMA, preserve_index=False)
            _write_parquet_atomic(table_m, m_path)
            log.info("Markets table written: %s (%s rows)", m_path, len(merged_markets))

        # --- Prices (partitioned) ---
        # Load only the (crypto, timeframe) partitions present in the new data to
        # avoid reading the entire prices dataset when updating a small subset.
        if not prices_df.empty:
            # Flatten category columns to plain strings before operations.
            for col in ("crypto", "timeframe"):
                if col in prices_df.columns and hasattr(prices_df[col], "cat"):
                    prices_df = prices_df.copy()
                    prices_df[col] = prices_df[col].astype(str)

            partition_pairs = list(prices_df.groupby(["crypto", "timeframe"], sort=False).groups.keys())
            partition_filters = [[("crypto", "=", str(c)), ("timeframe", "=", str(t))] for c, t in partition_pairs]
            existing_prices = load_prices(p_dir, filters=partition_filters)
            for col in ("crypto", "timeframe"):
                if col in existing_prices.columns and hasattr(existing_prices[col], "cat"):
                    existing_prices[col] = existing_prices[col].astype(str)

            dfs_to_concat = [df for df in (existing_prices, prices_df) if not df.empty]
            merged_prices = (
                pd.concat(dfs_to_concat, ignore_index=True) if dfs_to_concat else existing_prices
            )
            merged_prices = (
                merged_prices.drop_duplicates(subset=["market_id", "timestamp"], keep="last")
                .sort_values(["market_id", "timestamp"])
                .reset_index(drop=True)
            )
            merged_prices = optimise_prices_df(merged_prices)
            table_p = pa.Table.from_pandas(merged_prices, preserve_index=False)
            _write_partitioned_atomic(table_p, p_dir, partition_cols=["crypto", "timeframe"])
            log.info("Prices table written: %s/ (%s rows)", p_dir, len(merged_prices))


def optimise_culture_markets_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["crypto"] = df["crypto"].astype("category")
    df["timeframe"] = df["timeframe"].astype("category")
    df["volume"] = df["volume"].astype("float32")
    df["resolution"] = df["resolution"].apply(_resolution_to_int8).astype("int8")

    if "start_ts" in df.columns:
        df["start_ts"] = pd.to_numeric(df["start_ts"], errors="coerce").fillna(0).astype("int64")
    if "end_ts" in df.columns:
        df["end_ts"] = pd.to_numeric(df["end_ts"], errors="coerce").fillna(0).astype("int64")
    if "closed_ts" not in df.columns:
        df["closed_ts"] = 0
    df["closed_ts"] = pd.to_numeric(df["closed_ts"], errors="coerce").fillna(0).astype("int64")
    if "condition_id" in df.columns:
        df["condition_id"] = df["condition_id"].astype("string")
    if "tokens" in df.columns:
        df["tokens"] = df["tokens"].astype("string")

    # Identity / grouping fields (default to empty / -1 for backwards
    # compatibility with pre-existing parquet files written by earlier
    # pipeline versions).
    if "slug" not in df.columns:
        df["slug"] = ""
    df["slug"] = df["slug"].fillna("").astype("string")
    if "event_slug" not in df.columns:
        df["event_slug"] = ""
    df["event_slug"] = df["event_slug"].fillna("").astype("string")
    if "bucket_index" not in df.columns:
        df["bucket_index"] = -1
    df["bucket_index"] = pd.to_numeric(df["bucket_index"], errors="coerce").fillna(-1).astype("int32")
    if "bucket_label" not in df.columns:
        df["bucket_label"] = ""
    df["bucket_label"] = df["bucket_label"].fillna("").astype("string")

    return df

def optimise_culture_prices_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = df["timestamp"].astype("int64")
    df["price"] = df["price"].astype("float32")
    df["crypto"] = df["crypto"].astype("category")
    df["timeframe"] = df["timeframe"].astype("category")
    df["outcome"] = df["outcome"].astype("category")
    return df

_CULTURE_MARKETS_EMPTY_COLS = [
    "market_id", "question", "crypto", "timeframe", "volume", "resolution",
    "start_ts", "end_ts", "closed_ts", "condition_id", "tokens",
    "slug", "event_slug", "bucket_index", "bucket_label",
]


def load_culture_markets(markets_path: str) -> pd.DataFrame:
    if os.path.exists(markets_path):
        return pq.read_table(markets_path).to_pandas()
    return pd.DataFrame(columns=_CULTURE_MARKETS_EMPTY_COLS)


def split_culture_markets_prices(flat_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if flat_df.empty:
        markets_df = pd.DataFrame(columns=_CULTURE_MARKETS_EMPTY_COLS)
        prices_df = pd.DataFrame(columns=["market_id", "timestamp", "token_id", "outcome", "price", "crypto", "timeframe"])
        return markets_df, prices_df

    # Aggregate market metadata — "last" so the most recent observation
    # (e.g. a closed/resolved market) overrides earlier open snapshots.
    agg_spec = {
        "question": "last",
        "crypto": "last",
        "timeframe": "last",
        "volume": "last",
        "resolution": "last",
        "start_ts": "last",
        "end_ts": "last",
        "condition_id": "last",
        "tokens": "last",
    }
    # Only include identity fields that exist (keeps backward compatibility
    # with code paths that don't supply them — e.g. older tests).
    for col in ("closed_ts", "slug", "event_slug", "bucket_index", "bucket_label"):
        if col in flat_df.columns:
            agg_spec[col] = "last"

    markets_df = (
        flat_df.groupby("market_id", sort=False)
        .agg(agg_spec)
        .reset_index()
    )

    prices_df = flat_df[["market_id", "timestamp", "token_id", "outcome", "price", "crypto", "timeframe"]].copy()

    return markets_df, prices_df

def persist_culture_normalized(
    markets_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    *,
    markets_path: str,
    prices_dir: str,
    logger: logging.Logger | None = None,
    skip_markets: bool = False,
    lock_timeout: float | None = None,
) -> None:
    log = logger or logging.getLogger("polymarket_pipeline")
    data_root = os.path.dirname(os.path.abspath(markets_path))

    with _write_lock(data_root, timeout=lock_timeout):
        if not skip_markets:
            existing_markets = load_culture_markets(markets_path)
            for col in ("crypto", "timeframe"):
                if col in existing_markets.columns and hasattr(existing_markets[col], "cat"):
                    existing_markets[col] = existing_markets[col].astype(str)
                if col in markets_df.columns and hasattr(markets_df[col], "cat"):
                    markets_df = markets_df.copy()
                    markets_df[col] = markets_df[col].astype(str)
            if not markets_df.empty:
                dfs_to_concat = [df for df in (existing_markets, markets_df) if not df.empty]
                merged_markets = (
                    pd.concat(dfs_to_concat, ignore_index=True) if dfs_to_concat else existing_markets
                )
                merged_markets = (
                    merged_markets.drop_duplicates(subset=["market_id"], keep="last")
                    .reset_index(drop=True)
                )
            else:
                merged_markets = existing_markets

            merged_markets = optimise_culture_markets_df(merged_markets)
            table_m = pa.Table.from_pandas(merged_markets, schema=CULTURE_MARKETS_SCHEMA, preserve_index=False)
            _write_parquet_atomic(table_m, markets_path)
            log.info("Culture markets table written: %s (%s rows)", markets_path, len(merged_markets))

        if not prices_df.empty:
            for col in ("crypto", "timeframe", "outcome"):
                if col in prices_df.columns and hasattr(prices_df[col], "cat"):
                    prices_df = prices_df.copy()
                    prices_df[col] = prices_df[col].astype(str)
            
            partition_pairs = list(prices_df.groupby(["crypto", "timeframe"], sort=False).groups.keys())
            partition_filters = [[("crypto", "=", str(c)), ("timeframe", "=", str(t))] for c, t in partition_pairs]
            existing_prices = load_prices(prices_dir, filters=partition_filters)
            
            for col in ("crypto", "timeframe", "outcome"):
                if col in existing_prices.columns and hasattr(existing_prices[col], "cat"):
                    existing_prices[col] = existing_prices[col].astype(str)

            dfs_to_concat = [df for df in (existing_prices, prices_df) if not df.empty]
            merged_prices = (
                pd.concat(dfs_to_concat, ignore_index=True) if dfs_to_concat else existing_prices
            )
            merged_prices = (
                merged_prices.drop_duplicates(subset=["market_id", "timestamp", "outcome"], keep="last")
                .sort_values(["market_id", "timestamp", "outcome"])
                .reset_index(drop=True)
            )
            merged_prices = optimise_culture_prices_df(merged_prices)
            table_p = pa.Table.from_pandas(merged_prices, preserve_index=False)
            _write_partitioned_atomic(table_p, prices_dir, partition_cols=["crypto", "timeframe"])
            log.info("Culture prices table written: %s/ (%s rows)", prices_dir, len(merged_prices))

# ---------------------------------------------------------------------------
# Hugging Face Hub upload
# ---------------------------------------------------------------------------

def upload_to_huggingface(
    repo_id: str | None = None,
    *,
    markets_path: str | None = None,
    prices_dir: str | None = None,
    ticks_dir: str | None = None,
    spot_prices_dir: str | None = None,
    orderbook_dir: str | None = None,
    heartbeats_dir: str | None = None,
    logger: logging.Logger | None = None,
    skip_consolidate: bool = False,
) -> None:
    """Upload the local Parquet dataset to the Hugging Face Hub.

    Requires a valid HF_TOKEN environment variable or ``huggingface-cli login``.
    Creates the repo as a *dataset* repo if it doesn't exist.

    Parameters
    ----------
    skip_consolidate:
        When True, skip the ``consolidate_ticks()`` call.  Pass this when the
        caller has already consolidated ticks in the same run to avoid a
        redundant full-partition scan.
    """
    from huggingface_hub import HfApi

    log = logger or logging.getLogger("polymarket_pipeline")
    repo = repo_id or HF_REPO_ID
    m_path = markets_path or PARQUET_MARKETS_PATH
    p_dir = prices_dir or PARQUET_PRICES_DIR
    t_dir = ticks_dir or PARQUET_TICKS_DIR

    api = HfApi()

    # Ensure the dataset repo exists (no-op if it already does)
    api.create_repo(repo_id=repo, repo_type="dataset", exist_ok=True)
    log.info("Hugging Face repo: https://huggingface.co/datasets/%s", repo)

    # Phase 1: Consolidate shard files.  Each consolidation function
    # acquires its own per-partition write lock internally, so the lock
    # is held only for one partition at a time (typically seconds, not
    # minutes).  No outer lock — other services can interleave between
    # partitions.  All writers (WS, historical) use lock-free shard files,
    # so only consolidation itself needs the lock.
    data_root = os.path.dirname(os.path.abspath(m_path))
    if os.path.exists(p_dir):
        consolidate_prices(prices_dir=p_dir, logger=log)
    if not skip_consolidate and os.path.exists(t_dir):
        consolidate_ticks(ticks_dir=t_dir, logger=log)
    if spot_prices_dir and os.path.exists(spot_prices_dir):
        consolidate_spot_prices(spot_prices_dir=spot_prices_dir, logger=log)
    if orderbook_dir and os.path.exists(orderbook_dir):
        consolidate_orderbook(orderbook_dir=orderbook_dir, logger=log)
    if heartbeats_dir and os.path.exists(heartbeats_dir):
        consolidate_heartbeats(heartbeats_dir=heartbeats_dir, logger=log)

    # Phase 2: Upload the entire data root in a single commit.
    # After consolidation the part-0.parquet files are stable — the WS
    # service only writes new shard files (which are excluded below), and
    # the historical service won't modify files until its next run.
    # A single upload_folder call creates one HF commit instead of five,
    # eliminating redundant remote-file listings and hash comparisons.
    _ignore = [
        # WS staging shards (lock-free, never uploaded)
        "**/ws_staging.parquet",
        "**/ws_ticks_*.parquet",
        "**/ws_prices_*.parquet",
        "**/ws_culture_prices_*.parquet",
        "**/ws_spot_*.parquet",
        "**/ws_ob_*.parquet",
        "**/ws_hb_*.parquet",
        # Backfill / history shards (consolidated into part-0.parquet)
        "**/backfill_*.parquet",
        "**/binance_history_*.parquet",
        # Atomic-write temp files
        "**/*.tmp",
        # Internal state files
        ".scan_checkpoint",
        ".write.lock",
        ".hf_sync_checkpoint",
        "**/.duckdb_tmp",
    ]
    # Restrict deletes to consolidated partition files (``part-*.parquet``).
    # Earlier this was ``*.parquet`` which would also wipe ``markets.parquet``
    # if it was momentarily missing locally — losing the only copy of all
    # market metadata.  Limiting the pattern keeps orphan-cleanup intent
    # while protecting the top-level metadata file.
    #
    # Additionally, abort the delete pass when local consolidation produced
    # no part-* files at all.  That signals consolidation failed (or hasn't
    # run yet) and applying delete_patterns would propagate the local hole
    # to the Hub.
    _delete: list[str] = ["**/part-*.parquet"]
    _local_partfile_count = 0
    for _root, _dirs, _files in os.walk(data_root):
        for _f in _files:
            if _f.startswith("part-") and _f.endswith(".parquet"):
                _local_partfile_count += 1
    if _local_partfile_count == 0:
        log.warning(
            "Local data root has zero part-*.parquet files — skipping "
            "delete_patterns to avoid wiping the remote dataset."
        )
        _delete = []

    if os.path.isdir(data_root):
        # Retry on transient file-not-found errors caused by concurrent
        # writers atomically replacing Parquet files between the SDK's
        # directory scan and file read.  The stable-filename fix in
        # _write_partitioned_atomic prevents most occurrences, but a
        # retry is belt-and-suspenders for edge cases.
        _max_upload_attempts = 3
        for _attempt in range(1, _max_upload_attempts + 1):
            try:
                api.upload_folder(
                    folder_path=data_root,
                    path_in_repo="data",
                    repo_id=repo,
                    repo_type="dataset",
                    ignore_patterns=_ignore,
                    delete_patterns=_delete,
                )
                break
            except ValueError as exc:
                if "is not a file" in str(exc) and _attempt < _max_upload_attempts:
                    log.warning(
                        "Upload attempt %d/%d: transient file race (%s) — retrying in 5s...",
                        _attempt, _max_upload_attempts, exc,
                    )
                    time.sleep(5)
                else:
                    raise

    log.info("Upload complete.")

    # Write a sync checkpoint so callers can track when the last successful
    # upload occurred and avoid redundant re-uploads.
    checkpoint_path = os.path.join(
        os.path.dirname(os.path.abspath(m_path)), ".hf_sync_checkpoint"
    )
    try:
        with open(checkpoint_path, "w") as _f:
            _f.write(str(int(time.time())))
    except OSError as e:
        log.warning("Could not write HF sync checkpoint to %s: %s", checkpoint_path, e)
