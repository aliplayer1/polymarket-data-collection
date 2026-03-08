"""Normalized Parquet storage layer and Hugging Face Hub upload.

Data is split into three tables:
  - **markets** (one row per market): market_id, question, crypto, timeframe, volume, resolution
  - **prices**  (one row per price-change event, ~60s median gap):
                  market_id, timestamp, up_price, down_price
  - **ticks**   (one row per on-chain trade fill, second-level precision):
                  market_id, timestamp_ms, token_id, outcome, side, price, size_usdc, tx_hash, block_number

Prices and ticks are Hive-partitioned on disk as  {table}/crypto=X/timeframe=Y/*.parquet
and stored with Zstandard compression + optimised dtypes.
"""

from __future__ import annotations

import logging
import os
import shutil
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import (
    HF_REPO_ID,
    PARQUET_DATA_DIR,
    PARQUET_MARKETS_PATH,
    PARQUET_PRICES_DIR,
    PARQUET_TICKS_DIR,
    PARQUET_TEST_DIR,
    PARQUET_TEST_MARKETS_PATH,
    PARQUET_TEST_PRICES_DIR,
    PARQUET_TEST_TICKS_DIR,
    TIME_FRAMES,
)

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
])

PRICES_SCHEMA = pa.schema([
    ("market_id", pa.string()),
    ("timestamp", pa.int32()),
    ("up_price", pa.float32()),
    ("down_price", pa.float32()),
    # partition columns (crypto, timeframe) are implicit in the directory tree
])

TICKS_SCHEMA = pa.schema([
    # One row per on-chain trade fill (Polygon OrderFilled event) or WebSocket trade event.
    # timestamp_ms is block-timestamp × 1000 for on-chain ticks (Polygon block ~2 s resolution)
    # or the WebSocket event timestamp for live ticks (millisecond precision).
    ("market_id",    pa.string()),
    ("timestamp_ms", pa.int64()),    # epoch milliseconds
    ("token_id",     pa.string()),
    ("outcome",      pa.dictionary(pa.int8(), pa.string())),   # "Up" / "Down"
    ("side",         pa.dictionary(pa.int8(), pa.string())),   # "BUY" / "SELL"
    ("price",        pa.float32()),
    ("size_usdc",    pa.float32()),
    ("tx_hash",      pa.string()),
    ("block_number", pa.int32()),
    ("log_index",    pa.int32()),
    ("source",       pa.dictionary(pa.int8(), pa.string())),   # "onchain" / "websocket"
    # partition columns (crypto, timeframe) are implicit
])


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
        markets_df = pd.DataFrame(columns=["market_id", "question", "crypto", "timeframe", "volume", "resolution"])
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
    return pd.DataFrame(columns=["market_id", "question", "crypto", "timeframe", "volume", "resolution"])


def load_prices(prices_dir: str | None = None, filters: list | None = None) -> pd.DataFrame:
    """Load the prices table from a Hive-partitioned Parquet directory.

    Parameters
    ----------
    prices_dir : path to the partitioned directory.
    filters : optional PyArrow filter expression list,
              e.g. [("crypto", "=", "BTC"), ("timeframe", "=", "1-hour")]
    """
    path = prices_dir or PARQUET_PRICES_DIR
    if not os.path.exists(path):
        return pd.DataFrame(columns=["market_id", "timestamp", "up_price", "down_price", "crypto", "timeframe"])
    dataset = pq.ParquetDataset(path, filters=filters)
    return dataset.read().to_pandas()


def load_prices_for_timeframe(timeframe: str, prices_dir: str | None = None) -> pd.DataFrame:
    """Convenience: load prices filtered to a single timeframe."""
    return load_prices(prices_dir, filters=[("timeframe", "=", timeframe)])


_TICKS_EMPTY_COLS = [
    "market_id", "timestamp_ms", "token_id", "outcome", "side",
    "price", "size_usdc", "tx_hash", "block_number", "log_index", "source", "crypto", "timeframe",
]


def load_ticks(ticks_dir: str | None = None, filters: list | None = None) -> pd.DataFrame:
    """Load the ticks table from a Hive-partitioned Parquet directory."""
    path = ticks_dir or PARQUET_TICKS_DIR
    if not os.path.exists(path):
        return pd.DataFrame(columns=_TICKS_EMPTY_COLS)
    dataset = pq.ParquetDataset(path, filters=filters)
    return dataset.read().to_pandas()


def load_ticks_for_market(
    market_id: str,
    ticks_dir: str | None = None,
) -> pd.DataFrame:
    """Load all ticks for a specific market_id."""
    df = load_ticks(ticks_dir)
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

    Deduplicates on (market_id, timestamp_ms, token_id, tx_hash).
    Partitioned by crypto / timeframe.
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
        .drop_duplicates(subset=["market_id", "timestamp_ms", "token_id", "tx_hash", "log_index"], keep="last")
        .sort_values(["market_id", "timestamp_ms"])
        .reset_index(drop=True)
    )
    merged = optimise_ticks_df(merged)
    table = pa.Table.from_pandas(merged, preserve_index=False)
    _write_partitioned_atomic(table, t_dir, partition_cols=["crypto", "timeframe"])
    log.info("Ticks table written: %s/ (%s rows)", t_dir, len(merged))


# ---------------------------------------------------------------------------
# Write helpers  (atomic: write to .tmp dir then rename)
# ---------------------------------------------------------------------------

def _write_parquet_atomic(table: pa.Table, path: str) -> None:
    """Atomically write a single Parquet file."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = path + ".tmp"
    try:
        pq.write_table(table, tmp_path, compression="zstd")
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def _write_partitioned_atomic(table: pa.Table, root_dir: str, partition_cols: list[str]) -> None:
    """Atomically write a Hive-partitioned Parquet dataset."""
    tmp_dir = root_dir + ".tmp"
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
) -> None:
    """Merge new data with existing data on disk and write Parquet.

    - Markets: merge on market_id (keep latest).
    - Prices: merge on (market_id, timestamp) (deduplicate).
    Both written with Zstd compression and optimised dtypes.
    """
    log = logger or logging.getLogger("polymarket_pipeline")
    m_path = markets_path or PARQUET_MARKETS_PATH
    p_dir = prices_dir or PARQUET_PRICES_DIR

    # --- Markets ---
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
        merged_markets = (
            pd.concat([existing_markets, markets_df], ignore_index=True)
            .drop_duplicates(subset=["market_id"], keep="last")
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

        merged_prices = (
            pd.concat([existing_prices, prices_df], ignore_index=True)
            .drop_duplicates(subset=["market_id", "timestamp"], keep="last")
            .sort_values(["market_id", "timestamp"])
            .reset_index(drop=True)
        )
        merged_prices = optimise_prices_df(merged_prices)
        table_p = pa.Table.from_pandas(merged_prices, preserve_index=False)
        _write_partitioned_atomic(table_p, p_dir, partition_cols=["crypto", "timeframe"])
        log.info("Prices table written: %s/ (%s rows)", p_dir, len(merged_prices))


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
) -> None:
    """Upload the local Parquet dataset to the Hugging Face Hub.

    Requires a valid HF_TOKEN environment variable or ``huggingface-cli login``.
    Creates the repo as a *dataset* repo if it doesn't exist.
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

    # Upload markets table
    if os.path.exists(m_path):
        api.upload_file(
            path_or_fileobj=m_path,
            path_in_repo="data/markets.parquet",
            repo_id=repo,
            repo_type="dataset",
        )
        log.info("Uploaded %s -> data/markets.parquet", m_path)

    # Upload prices partition tree
    if os.path.exists(p_dir):
        api.upload_folder(
            folder_path=p_dir,
            path_in_repo="data/prices",
            repo_id=repo,
            repo_type="dataset",
        )
        log.info("Uploaded %s/ -> data/prices/", p_dir)

    # Upload ticks partition tree
    if os.path.exists(t_dir):
        api.upload_folder(
            folder_path=t_dir,
            path_in_repo="data/ticks",
            repo_id=repo,
            repo_type="dataset",
        )
        log.info("Uploaded %s/ -> data/ticks/", t_dir)

    log.info("Upload complete.")
