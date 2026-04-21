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
    if "closed_ts" not in df.columns:
        df["closed_ts"] = 0
    df["closed_ts"] = pd.to_numeric(df["closed_ts"], errors="coerce").fillna(0).astype("int64")
    if "condition_id" in df.columns:
        df["condition_id"] = df["condition_id"].astype("string")
    if "up_token_id" in df.columns:
        df["up_token_id"] = df["up_token_id"].astype("string")
    if "down_token_id" in df.columns:
        df["down_token_id"] = df["down_token_id"].astype("string")
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

            files_sql = ", ".join(f"'{_duckdb_escape(p)}'" for p in file_paths)
            files_with_order_sql = ", ".join(
                f"('{_duckdb_escape(path)}', {idx})" for idx, path in enumerate(file_paths)
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
                local_recv_ts_ns_sql = "src.local_recv_ts_ns" if "local_recv_ts_ns" in present_cols else "NULL::BIGINT AS local_recv_ts_ns"
                # v5 order_hash — missing in pre-v5 shards, populated by SubgraphTickFetcher.
                order_hash_sql = "src.order_hash" if "order_hash" in present_cols else "NULL::VARCHAR AS order_hash"

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
                        )
                        -- Intentionally omit a final ORDER BY: the global sort is
                        -- another blocking operator and was the main OOM trigger.
                        --
                        -- Dedup key includes ``COALESCE(order_hash, '')`` so
                        -- subgraph-sourced rows (populated order_hash, unique
                        -- per-fill within a tx) don't collapse with legacy
                        -- on-chain / WS rows that have NULL order_hash.
                        SELECT
                            {select_sql}
                        FROM ranked_rows
                        GROUP BY {group_by_sql}, COALESCE(order_hash, '')
                    ) TO '{_duckdb_escape(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
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
    """Shared implementation for partition-level price shard consolidation."""
    log = logger or logging.getLogger("polymarket_pipeline")
    if not os.path.exists(root_dir):
        return

    data_root = os.path.dirname(os.path.abspath(root_dir))

    for dirpath, _dirs, _fnames in os.walk(root_dir):
        # Only process leaf partition directories that contain shard files
        parquet_files = [f for f in os.listdir(dirpath) if f.endswith(".parquet")]
        has_shards = any(f.startswith(shard_prefix) for f in parquet_files)
        if not has_shards:
            continue

        with _write_lock(data_root):
            # Re-check inside lock (files may have been consolidated by another process)
            parquet_files = [f for f in os.listdir(dirpath) if f.endswith(".parquet")]
            has_shards = any(f.startswith(shard_prefix) for f in parquet_files)
            if not has_shards:
                continue

            rel = os.path.relpath(dirpath, root_dir)
            log.info("Consolidating %s partition %s (%d files)...", label, rel, len(parquet_files))

            # Deterministic read order so that the newer data wins
            # drop_duplicates(keep="last"). Sort by (mtime, filename):
            # oldest file first, ties broken lexicographically. This
            # matches consolidate_orderbook's pattern and removes the
            # dependence on os.listdir() order, which is filesystem-
            # and OS-specific (passes on ext4/Python 3.10 locally but
            # fails on the ARM64 server with Python 3.12).
            parquet_files.sort(
                key=lambda f: (os.path.getmtime(os.path.join(dirpath, f)), f)
            )

            frames = []
            for fname in parquet_files:
                fpath = os.path.join(dirpath, fname)
                try:
                    df = pq.ParquetFile(fpath).read().to_pandas()
                    frames.append(df)
                except Exception:
                    continue

            if not frames:
                continue

            merged = pd.concat(frames, ignore_index=True)
            # Flatten category columns before dedup/sort
            for col in dedup_cols:
                if col in merged.columns and hasattr(merged[col], "cat"):
                    merged[col] = merged[col].astype(str)
            merged = (
                merged.drop_duplicates(subset=dedup_cols, keep="last")
                .sort_values(sort_cols)
                .reset_index(drop=True)
            )

            consolidated_path = os.path.join(dirpath, "part-0.parquet")
            tmp_path = f"{consolidated_path}.{os.getpid()}.tmp"
            try:
                _check_disk_space(dirpath)
                table = pa.Table.from_pandas(merged, preserve_index=False)
                table = _stamp_schema_version(table)
                pq.write_table(table, tmp_path, compression="zstd")
                # Place consolidated file first, then remove old shards.
                # If the process crashes between these steps, orphan shards
                # remain but no data is lost — the next consolidation run
                # will merge them again (dedup handles it).
                os.replace(tmp_path, consolidated_path)
                for fname in parquet_files:
                    if fname == "part-0.parquet":
                        continue  # already replaced above
                    try:
                        os.remove(os.path.join(dirpath, fname))
                    except OSError:
                        pass
            except Exception:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                raise

            log.info("  -> %s consolidated: %d rows", rel, len(merged))


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
            for fname in parquet_files:
                try:
                    os.remove(os.path.join(spot_prices_dir, fname))
                except OSError:
                    pass
            os.replace(tmp_path, consolidated_path)
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
            remove_errors: list[str] = []
            for fname in parquet_files:
                try:
                    os.remove(os.path.join(heartbeats_dir, fname))
                except OSError as exc:
                    remove_errors.append(f"{fname}: {exc}")
            if remove_errors:
                log.warning(
                    "  -> could not remove %d old heartbeat shard file(s): %s",
                    len(remove_errors), "; ".join(remove_errors),
                )
            os.replace(tmp_path, consolidated_path)
        except Exception:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise

        log.info("  -> heartbeats consolidated: %d rows", len(merged))


def consolidate_orderbook(
    *,
    orderbook_dir: str,
    logger: logging.Logger | None = None,
) -> None:
    """Consolidate orderbook staging files per partition using DuckDB.

    Uses the same memory-capped, disk-spilling DuckDB approach as
    ``consolidate_ticks`` to avoid OOM on large partitions (e.g. BTC/5-minute
    can exceed 8 GB when loaded entirely into pandas).
    """
    import duckdb

    log = logger or logging.getLogger("polymarket_pipeline")
    if not os.path.exists(orderbook_dir):
        return

    data_root = os.path.dirname(os.path.abspath(orderbook_dir))
    nrows = 0

    for dirpath, _dirs, _fnames in os.walk(orderbook_dir):
        with _write_lock(data_root):
            parquet_files = [f for f in os.listdir(dirpath) if f.endswith(".parquet")]
            if not parquet_files or parquet_files == ["part-0.parquet"]:
                continue

            rel = os.path.relpath(dirpath, orderbook_dir)
            log.info("Consolidating orderbook partition %s (%d files)...", rel, len(parquet_files))
            _check_disk_space(dirpath)

            file_paths = [os.path.join(dirpath, f) for f in parquet_files]
            file_paths.sort(key=lambda path: (os.path.getmtime(path), path))
            consolidated_path = os.path.join(dirpath, "part-0.parquet")
            tmp_path = f"{consolidated_path}.{os.getpid()}.tmp"

            files_sql = ", ".join(f"'{_duckdb_escape(p)}'" for p in file_paths)
            temp_dir = os.path.join(dirpath, ".duckdb_tmp")
            con = None
            try:
                con = duckdb.connect()
                os.makedirs(temp_dir, exist_ok=True)
                memory_limit, threads = _configure_duckdb_for_consolidation(
                    con, temp_dir=temp_dir,
                )

                # Intentionally omit ORDER BY: the global sort is a
                # blocking operator that requires materializing the full
                # result in memory/disk — the same OOM trigger that was
                # already removed from consolidate_ticks.  Downstream
                # queries can sort on read if needed.
                #
                # ``union_by_name=true`` tolerates old pre-v4 shard files
                # that lack ``local_recv_ts_ns``; DuckDB fills NULL.  We
                # still reference the column in SELECT so the output file
                # always carries the v4 schema shape.
                desc_res = con.execute(f"DESCRIBE SELECT * FROM read_parquet([{files_sql}], union_by_name=true)").fetchall()
                present_cols = {row[0] for row in desc_res}
                local_recv_sql = "local_recv_ts_ns" if "local_recv_ts_ns" in present_cols else "NULL::BIGINT AS local_recv_ts_ns"
                result = con.execute(f"""
                    COPY (
                        SELECT
                            ts_ms, market_id, token_id, outcome,
                            best_bid, best_ask, best_bid_size, best_ask_size,
                            {local_recv_sql}
                        FROM read_parquet(
                            [{files_sql}],
                            union_by_name=true
                        )
                    ) TO '{_duckdb_escape(tmp_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
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
                        "  -> could not remove %d old shard file(s) in %s: %s",
                        len(remove_errors), rel, "; ".join(remove_errors),
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
            # Remove stale Parquet files that aren't part of the new write.
            # Skip lock-free shard files written by concurrent processes
            # (WS service, tick backfill) — those are merged by their own
            # consolidation functions and must not be deleted here.
            for old_f in os.listdir(target_dir):
                if old_f.endswith(".parquet") and old_f not in new_files:
                    if old_f.startswith((
                        "ws_ticks_", "ws_prices_", "ws_culture_prices_",
                        "ws_spot_", "ws_ob_", "ws_hb_", "ws_staging",
                        "backfill_", "binance_history_",
                    )):
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
    _delete = ["*.parquet"]

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
