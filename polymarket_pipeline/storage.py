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
via ``_write_partitioned_atomic()``, which deletes all previous shards (including
``ws_staging.parquet``) as part of its atomic rename step.
"""

from __future__ import annotations

import errno as _errno
import logging
import os
import shutil
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


def _acquire_flock(fd: Any, lock_path: str) -> None:
    """Acquire LOCK_EX with stale-PID detection and a hard timeout.

    Unlike plain ``flock(LOCK_EX)``, this function never blocks indefinitely:
    - Uses ``LOCK_EX | LOCK_NB`` and retries on EACCES/EAGAIN.
    - Reads the PID stamp written by the current holder and checks liveness
      via ``os.kill(pid, 0)``.  If the holder process is dead (stale lock),
      the stamp is cleared and acquisition retries immediately — the OS
      already released the flock when the process exited.
    - Raises ``TimeoutError`` after ``_FLOCK_TIMEOUT_SECONDS`` so a live but
      stuck process never permanently blocks the pipeline.
    """
    deadline = time.monotonic() + _FLOCK_TIMEOUT_SECONDS
    stale_cleared = False

    while True:
        try:
            _fcntl.flock(fd, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            # Acquired: stamp with our PID so future waiters can detect us as stale if we die.
            try:
                fd.seek(0)
                fd.truncate()
                fd.write(str(os.getpid()))
                fd.flush()
            except OSError:
                pass  # best-effort PID stamp — the lock itself is already held
            return
        except OSError as exc:
            if exc.errno not in (_errno.EACCES, _errno.EAGAIN):
                raise  # unexpected OS error — propagate immediately

        # Lock is held by another descriptor.  Check if holder is still alive.
        if not stale_cleared:
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
                        stale_cleared = True
                        continue  # retry flock acquisition without sleeping
            except (OSError, ValueError):
                pass  # can't read PID — just wait with normal backoff

        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"_write_lock: could not acquire {lock_path!r} within "
                f"{_FLOCK_TIMEOUT_SECONDS:.0f}s — another process may be holding "
                "the lock for an unusually long time"
            )
        time.sleep(_FLOCK_POLL_INTERVAL)


@contextmanager
def _write_lock(data_root: str):
    """Exclusive write lock for a Parquet data root directory.

    Acquires both a threading.Lock (in-process) and an fcntl file lock
    (cross-process) before yielding, so the caller can safely execute the
    full read → merge → write cycle without racing another thread or process.

    Improvements over naive ``flock(LOCK_EX)``:
    - Uses LOCK_NB with timeout — never hangs forever if the holder dies.
    - PID-stamps the lock file so waiters can detect and clear stale locks.
    - Raises ``TimeoutError`` (not an infinite hang) if the lock cannot be
      acquired within ``_FLOCK_TIMEOUT_SECONDS``.
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
                _acquire_flock(fd, lock_path)

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
    ("condition_id", pa.string()),
    ("up_token_id", pa.string()),
    ("down_token_id", pa.string()),
])

PRICES_SCHEMA = pa.schema([
    ("market_id", pa.string()),
    ("timestamp", pa.int32()),
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
    ("condition_id", pa.string()),
    ("tokens", pa.string()), # JSON serialized dict
])

CULTURE_PRICES_SCHEMA = pa.schema([
    ("market_id", pa.string()),
    ("timestamp", pa.int32()),
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
    # partition columns (crypto, timeframe) are implicit
])

# ---------------------------------------------------------------------------
# Schema versioning — embed in every Parquet file's metadata
# ---------------------------------------------------------------------------

# Increment this when any schema column is added, removed, or its type changes.
# The version is stored as ``b"schema_version"`` in each Parquet file's
# key-value metadata so consumers can detect and handle schema evolution.
STORAGE_SCHEMA_VERSION = 2


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


def _get_consolidation_memory_limit() -> str | None:
    """Return the DuckDB memory limit string for consolidation queries."""
    override = os.environ.get("PM_DUCKDB_MEMORY_LIMIT")
    if override:
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


def _configure_duckdb_for_consolidation(
    con: Any,
    *,
    temp_dir: str,
) -> tuple[str | None, int]:
    """Apply low-memory DuckDB settings used by tick consolidation."""
    con.execute(f"SET temp_directory='{temp_dir}'")

    memory_limit = _get_consolidation_memory_limit()
    if memory_limit:
        con.execute(f"SET memory_limit='{memory_limit}'")

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
    except Exception:
        pass  # disk_usage not supported on this platform — skip check


def _resolution_to_int8(val: Any) -> int:
    """Map resolution: 0 -> 0, 1 -> 1, None -> -1."""
    if val is None or pd.isna(val):
        return -1
    return int(val)


# ---------------------------------------------------------------------------
# Split a flat DataFrame into the two normalised tables
# ---------------------------------------------------------------------------

def split_markets_prices(flat_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a legacy flat DataFrame into (markets_df, prices_df).

    The flat schema has columns:
        market_id, crypto, timeframe, timestamp, up_price, down_price,
        volume, resolution, question
    """
    if flat_df.empty:
        markets_df = pd.DataFrame(columns=["market_id", "question", "crypto", "timeframe", "volume", "resolution", "start_ts", "end_ts", "condition_id", "up_token_id", "down_token_id"])
        prices_df = pd.DataFrame(columns=["market_id", "timestamp", "up_price", "down_price", "crypto", "timeframe"])
        return markets_df, prices_df

    # Markets: one row per market_id, keep last-seen metadata
    markets_df = (
        flat_df.groupby("market_id", sort=False)
        .agg({
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
        })
        .reset_index()
    )

    # Prices: tick-level, plus crypto/timeframe for partitioning
    prices_df = flat_df[["market_id", "timestamp", "up_price", "down_price", "crypto", "timeframe"]].copy()

    return markets_df, prices_df


# ---------------------------------------------------------------------------
# Dtype optimisation
# ---------------------------------------------------------------------------

def optimise_markets_df(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast dtypes on the markets DataFrame before writing."""
    df = df.copy()
    df["crypto"] = df["crypto"].astype("category")
    df["timeframe"] = df["timeframe"].astype("category")
    df["volume"] = df["volume"].astype("float32")
    df["resolution"] = df["resolution"].apply(_resolution_to_int8).astype("int8")
    
    # New columns
    if "start_ts" in df.columns:
        df["start_ts"] = pd.to_numeric(df["start_ts"], errors="coerce").fillna(0).astype("int64")
    if "end_ts" in df.columns:
        df["end_ts"] = pd.to_numeric(df["end_ts"], errors="coerce").fillna(0).astype("int64")
    if "condition_id" in df.columns:
        df["condition_id"] = df["condition_id"].astype("string")
    if "up_token_id" in df.columns:
        df["up_token_id"] = df["up_token_id"].astype("string")
    if "down_token_id" in df.columns:
        df["down_token_id"] = df["down_token_id"].astype("string")
    
    return df


def optimise_prices_df(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast dtypes on the prices DataFrame before writing."""
    df = df.copy()
    df["timestamp"] = df["timestamp"].astype("int32")
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
    return pd.DataFrame(columns=["market_id", "question", "crypto", "timeframe", "volume", "resolution", "start_ts", "end_ts", "condition_id", "up_token_id", "down_token_id"])


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
    "spot_price_usdt", "spot_price_ts_ms",
    "crypto", "timeframe",
]
_TICKS_DEDUP_COLS = ["market_id", "timestamp_ms", "token_id", "tx_hash", "log_index"]


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

    Deduplicates on (market_id, timestamp_ms, token_id, tx_hash, log_index).
    Partitioned by crypto / timeframe.

    The entire read → merge → write cycle is protected by an exclusive write
    lock (threading + fcntl) so concurrent callers — whether from
    ``run_in_executor`` threads or a separate OS process — cannot interleave.
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

    # Load only the (crypto, timeframe) partitions present in the new data.
    for col in ("outcome", "side", "source", "crypto", "timeframe"):
        if col in ticks_df.columns and hasattr(ticks_df[col], "cat"):
            ticks_df = ticks_df.copy()
            ticks_df[col] = ticks_df[col].astype(str)

    data_root = os.path.dirname(os.path.abspath(t_dir))
    with _write_lock(data_root):
        if "crypto" in ticks_df.columns and "timeframe" in ticks_df.columns:
            partition_pairs = list(ticks_df.groupby(["crypto", "timeframe"], sort=False).groups.keys())
            partition_filters = [[("crypto", "=", str(c)), ("timeframe", "=", str(t))] for c, t in partition_pairs]
            existing = load_ticks(t_dir, filters=partition_filters)
        else:
            existing = load_ticks(t_dir)

        # Flatten category columns before concat
        for col in ("outcome", "side", "source", "crypto", "timeframe"):
            if col in existing.columns and hasattr(existing[col], "cat"):
                existing[col] = existing[col].astype(str)

        merged = (
            pd.concat([existing, ticks_df], ignore_index=True)
            .drop_duplicates(subset=_TICKS_DEDUP_COLS, keep="last")
            .sort_values(["market_id", "timestamp_ms"])
            .reset_index(drop=True)
        )
        merged = optimise_ticks_df(merged)
        table = pa.Table.from_pandas(merged, preserve_index=False)
        _write_partitioned_atomic(table, t_dir, partition_cols=["crypto", "timeframe"])
        log.info("Ticks table written: %s/ (%s rows)", t_dir, len(merged))


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


def consolidate_ticks(
    *,
    ticks_dir: str | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Consolidate backfill shard files one partition at a time.

    Walks the ticks directory and, for each (crypto, timeframe) leaf that
    contains more than one .parquet file, loads *only that partition*,
    deduplicates, and rewrites a single consolidated file.

    Uses DuckDB for out-of-core merge/dedup so peak memory stays well
    below the full partition size even with many shard files.
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
            if not parquet_files or parquet_files == ["part-0.parquet"]:
                continue  # empty, or already a single consolidated file

            log.info("Consolidating partition %s (%d shard files)...", rel, len(parquet_files))
            _check_disk_space(dirpath)

            file_paths = [os.path.join(dirpath, f) for f in parquet_files]
            file_paths.sort(key=lambda path: (os.path.getmtime(path), path))
            consolidated_path = os.path.join(dirpath, "part-0.parquet")
            tmp_path = f"{consolidated_path}.{os.getpid()}.tmp"

            # Paths are from os.listdir (not user input); safe to interpolate.
            files_sql = ", ".join(f"'{p}'" for p in file_paths)
            files_with_order_sql = ", ".join(
                f"('{path}', {idx})" for idx, path in enumerate(file_paths)
            )
            select_sql = _tick_consolidation_select_sql("sort_key")
            group_by_sql = ", ".join(_TICKS_DEDUP_COLS)
            temp_dir = os.path.join(dirpath, ".duckdb_tmp")
            con = None
            try:
                # Use a dedicated connection and configure for disk-spilling
                # plus conservative memory usage inside systemd/cgroup limits.
                con = duckdb.connect()
                os.makedirs(temp_dir, exist_ok=True)
                memory_limit, threads = _configure_duckdb_for_consolidation(
                    con,
                    temp_dir=temp_dir,
                )
                log.info(
                    "  -> DuckDB settings: memory_limit=%s threads=%s",
                    memory_limit or "default",
                    threads,
                )

                desc_res = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{files_sql}], union_by_name=true)").fetchall()
                present_cols = {row[0] for row in desc_res}

                def _col_sql(name: str, default: str) -> str:
                    if name in present_cols:
                        return f"COALESCE(src.{name}, {default}) AS {name}"
                    return f"{default} AS {name}"

                tx_hash_sql = _col_sql("tx_hash", "''")
                block_number_sql = _col_sql("block_number", "0")
                log_index_sql = _col_sql("log_index", "0")
                spot_price_usdt_sql = "src.spot_price_usdt" if "spot_price_usdt" in present_cols else "NULL::FLOAT AS spot_price_usdt"
                spot_price_ts_ms_sql = "src.spot_price_ts_ms" if "spot_price_ts_ms" in present_cols else "NULL::BIGINT AS spot_price_ts_ms"

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
                                ((f.file_order::BIGINT << 32) + src.file_row_number::BIGINT) AS sort_key
                            FROM read_parquet(
                                [{files_sql}],
                                union_by_name=true,
                                filename=true,
                                file_row_number=true
                            ) AS src
                            JOIN source_files AS f ON src.filename = f.file_path
                        )
                        -- Intentionally omit a final ORDER BY: the global sort is
                        -- another blocking operator and was the main OOM trigger.
                        SELECT
                            {select_sql}
                        FROM ranked_rows
                        GROUP BY {group_by_sql}
                    ) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """).fetchone()
                nrows = result[0] if result else 0

                remove_errors: list[str] = []
                for fname in parquet_files:
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
                os.replace(tmp_path, consolidated_path)
            except Exception:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                raise
            finally:
                try:
                    if con is not None:
                        con.close()
                finally:
                    shutil.rmtree(temp_dir, ignore_errors=True)

        log.info("  -> %s consolidated: %d rows", rel, nrows)


_WS_STAGING_FILENAME = "ws_staging.parquet"


def append_ws_ticks_staged(
    ticks_df: pd.DataFrame,
    *,
    ticks_dir: str | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Append WebSocket ticks to a lightweight per-partition staging file.

    Unlike ``persist_ticks()``, this function never reads the main consolidated
    ticks partition (which may contain millions of rows).  It only reads and
    rewrites the small ``ws_staging.parquet`` sidecar for each affected
    (crypto, timeframe) shard.  That staging file accumulates at most a few
    hours of WebSocket trade events — typically well under 1 MB per partition.

    Staging files are absorbed automatically the next time ``persist_ticks()``
    runs: ``pq.ParquetDataset`` reads every *.parquet file in the partition
    directory (including ``ws_staging.parquet``), and ``_write_partitioned_atomic``
    then removes it when it rewrites the consolidated file.
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

    data_root = os.path.dirname(os.path.abspath(t_dir))
    rows_staged = 0

    with _write_lock(data_root):
        for (crypto, timeframe), group in ticks_df.groupby(
            ["crypto", "timeframe"], sort=False
        ):
            shard_dir = os.path.join(t_dir, f"crypto={crypto}", f"timeframe={timeframe}")
            os.makedirs(shard_dir, exist_ok=True)
            staging_path = os.path.join(shard_dir, _WS_STAGING_FILENAME)

            # Strip partition columns — they are encoded in the directory path
            new_rows = group.drop(columns=["crypto", "timeframe"]).reset_index(drop=True)
            for col in ("outcome", "side", "source"):
                if col in new_rows.columns and hasattr(new_rows[col], "cat"):
                    new_rows[col] = new_rows[col].astype(str)

            if os.path.exists(staging_path):
                # Use ParquetFile (not read_table) to bypass Hive partition
                # discovery.  read_table internally builds a ParquetDataset
                # which detects the crypto=X/timeframe=Y parent directories
                # and tries to merge path-inferred dict<int32> indices with
                # the dict<int8> indices embedded in older staging files,
                # raising ArrowTypeError.
                existing = pq.ParquetFile(staging_path).read().to_pandas()
                merged = (
                    pd.concat([existing, new_rows], ignore_index=True)
                    .drop_duplicates(subset=_TICKS_DEDUP_COLS, keep="last")
                    .reset_index(drop=True)
                )
            else:
                merged = new_rows

            table = pa.Table.from_pandas(merged, preserve_index=False)
            _write_parquet_atomic(table, staging_path)
            rows_staged += len(new_rows)

    log.info("WS ticks staged: %d new rows to %s/", rows_staged, t_dir)


# ---------------------------------------------------------------------------
# Write helpers  (atomic: write to per-PID .tmp dir then rename)
# ---------------------------------------------------------------------------

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
        )
        # Merge: move new partition files into the real directory.
        # Remove any pre-existing Parquet files in each destination partition
        # first so that UUID-named files from previous writes don't accumulate
        # alongside the new ones (each partition is rewritten wholesale).
        os.makedirs(root_dir, exist_ok=True)
        for dirpath, _, filenames in os.walk(tmp_dir):
            rel = os.path.relpath(dirpath, tmp_dir)
            target_dir = os.path.join(root_dir, rel)
            os.makedirs(target_dir, exist_ok=True)
            # Purge stale Parquet files before placing the new file
            for old_f in os.listdir(target_dir):
                if old_f.endswith(".parquet"):
                    try:
                        os.remove(os.path.join(target_dir, old_f))
                    except OSError:
                        pass
            for fname in filenames:
                src = os.path.join(dirpath, fname)
                dst = os.path.join(target_dir, fname)
                os.replace(src, dst)
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

    with _write_lock(data_root):
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
    if "condition_id" in df.columns:
        df["condition_id"] = df["condition_id"].astype("string")
    if "tokens" in df.columns:
        df["tokens"] = df["tokens"].astype("string")
    
    return df

def optimise_culture_prices_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = df["timestamp"].astype("int32")
    df["price"] = df["price"].astype("float32")
    df["crypto"] = df["crypto"].astype("category")
    df["timeframe"] = df["timeframe"].astype("category")
    df["outcome"] = df["outcome"].astype("category")
    return df

def load_culture_markets(markets_path: str) -> pd.DataFrame:
    if os.path.exists(markets_path):
        return pq.read_table(markets_path).to_pandas()
    return pd.DataFrame(columns=["market_id", "question", "crypto", "timeframe", "volume", "resolution", "start_ts", "end_ts", "condition_id", "tokens"])

def split_culture_markets_prices(flat_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if flat_df.empty:
        markets_df = pd.DataFrame(columns=["market_id", "question", "crypto", "timeframe", "volume", "resolution", "start_ts", "end_ts", "condition_id", "tokens"])
        prices_df = pd.DataFrame(columns=["market_id", "timestamp", "token_id", "outcome", "price", "crypto", "timeframe"])
        return markets_df, prices_df

    markets_df = (
        flat_df.groupby("market_id", sort=False)
        .agg({
            "question": "last",
            "crypto": "last",
            "timeframe": "last",
            "volume": "last",
            "resolution": "last",
            "start_ts": "last",
            "end_ts": "last",
            "condition_id": "last",
            "tokens": "last",
        })
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
) -> None:
    log = logger or logging.getLogger("polymarket_pipeline")
    data_root = os.path.dirname(os.path.abspath(markets_path))

    with _write_lock(data_root):
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

    # Use the cross-process lock to ensure the dataset is stable during upload.
    # Holding the lock prevents the WebSocket service from flushing/deleting
    # files while the HF uploader is scanning the partition tree.
    data_root = os.path.dirname(os.path.abspath(m_path))
    with _write_lock(data_root):
        # Consolidate any shard/staging files before uploading so that
        # all data is in the main partition files.  Skip if the caller already
        # ran consolidation in this session (skip_consolidate=True).
        if not skip_consolidate and os.path.exists(t_dir):
            consolidate_ticks(ticks_dir=t_dir, logger=log)

        # Upload markets table
        if os.path.exists(m_path):
            api.upload_file(
                path_or_fileobj=m_path,
                path_in_repo="data/markets.parquet",
                repo_id=repo,
                repo_type="dataset",
            )
            log.info("Uploaded %s -> data/markets.parquet", m_path)

        # Patterns to exclude from folder uploads: staging files (actively
        # written by the WebSocket service), backfill shards (consolidated
        # into part-0.parquet), and atomic-write temp files.
        _ignore = ["**/ws_staging.parquet", "**/backfill_*.parquet", "**/*.tmp"]

        # Upload prices partition tree
        if os.path.exists(p_dir):
            api.upload_folder(
                folder_path=p_dir,
                path_in_repo="data/prices",
                repo_id=repo,
                repo_type="dataset",
                ignore_patterns=_ignore,
            )
            log.info("Uploaded %s/ -> data/prices/", p_dir)

        # Upload ticks partition tree
        if os.path.exists(t_dir):
            api.upload_folder(
                folder_path=t_dir,
                path_in_repo="data/ticks",
                repo_id=repo,
                repo_type="dataset",
                ignore_patterns=_ignore,
            )
            log.info("Uploaded %s/ -> data/ticks/", t_dir)

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
