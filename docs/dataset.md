# Dataset, Schema & Querying

This document covers the Parquet schema, Hugging Face upload integration, DuckDB querying, and how real-time blockchain tick ingestion flows.

## Tick-Level Trade Data

In addition to the OHLC-style price series, the pipeline collects **individual trade fills** (tick data) for use in ML model training — particularly scalping models that require sub-minute resolution.

### Sources

| Source | Resolution | Coverage | Requires |
| -------- | ----------- | ---------- | ---------- |
| **Polygon on-chain** (`OrderFilled` events) | ~2 s (block time) | Any historical market | Polygonscan API key (recommended) and/or RPC URL |
| **WebSocket** (`trade` events) | Millisecond | Live / active markets only | No extra credentials |

### On-Chain Backfill

Every Polymarket trade fill is settled on Polygon PoS as an `OrderFilled` event on the CTF Exchange contract (`0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`). The pipeline queries these events via the **Etherscan V2 unified API** (`api.etherscan.io/v2/api`, chain ID 137) with a fallback to the **native Polygonscan API** (`api.polygonscan.com/api`) or primarily via **direct JSON-RPC using Alchemy**, to pull exact trade data:

- Exact fill timestamp (block timestamp, ~2 s precision)
- Outcome token (`Up` / `Down`)
- Trade side (`BUY` / `SELL`)
- Fill price (USDC / outcome token)
- Notional size (USDC)

**Integrated Execution:** Historical tick backfill is handled by the `--historical-only` phase of the pipeline (run via `polymarket-historical.service` every 6 hours on the server). When running the combined pipeline locally (`python -m polymarket_pipeline`), the backfill runs automatically before the WebSocket phase begins. In the split-service deployment, `polymarket-websocket.service` runs `--websocket-only` concurrently with `polymarket-historical.service`, and a cross-process write lock ensures the two services never corrupt each other's Parquet files.

**Alchemy RPC Acceleration:** While the Etherscan V2 API offers historical events, it imposes strict 3 req/s limits. The codebase is heavily optimized for an **Alchemy JSON-RPC fallback**. Providing a free-tier Alchemy URL in `.env` (`POLYGON_RPC_URL`) removes API throttling delays and leverages 330 CU/s throughput to backfill tens of thousands of trades rapidly in bulk 10-block intervals prior to WebSocket streaming.

### WebSocket Ticks (Live)

For active markets, every incoming WebSocket `trade` event is stored as a tick with millisecond timestamp alongside the buffered price update.

### Tick Storage Schema

**`data/ticks/`** — one row per fill (Hive-partitioned: `crypto=X/timeframe=Y/*.parquet`):

| Column | Type | Description |
| -------- | ------ | ------------- |
| `market_id` | string | Foreign key to markets table |
| `timestamp_ms` | int64 | Fill timestamp in epoch milliseconds |
| `token_id` | string | ERC-1155 outcome token ID |
| `outcome` | dict(int8→string) | `"Up"` or `"Down"` |
| `side` | dict(int8→string) | `"BUY"` or `"SELL"` (taker perspective) |
| `price` | float32 | Fill price in [0, 1] |
| `size_usdc` | float32 | USDC notional of the fill |
| `tx_hash` | string | On-chain transaction hash (`""` for WebSocket ticks) |
| `block_number` | int32 | Polygon block number (`0` for WebSocket ticks) |
| `source` | dict(int8→string) | `"onchain"` or `"websocket"` |

Deduplication is applied on `(market_id, timestamp_ms, token_id, tx_hash)` so re-running the pipeline is safe.

## Storage Format

### Normalised Schema (Parquet)

The primary storage format is **Parquet with Zstandard compression**, split into two normalised tables:

**`data/markets.parquet`** — one row per market (metadata):

| Column | Type | Description |
| -------- | ------ | ------------- |
| `market_id` | string | Unique market identifier |
| `question` | string | Full market question text |
| `crypto` | dict(int8→string) | Asset symbol (`BTC`, `ETH`, `SOL`) — dictionary-encoded |
| `timeframe` | dict(int8→string) | Market prediction window — dictionary-encoded |
| `volume` | float32 | Total market volume at collection time |
| `resolution` | int8 | `1` = Up won, `0` = Down won, `-1` = unresolved |

**`data/prices/`** — one row per tick (Hive-partitioned: `crypto=X/timeframe=Y/*.parquet`):

| Column | Type | Description |
| -------- | ------ | ------------- |
| `market_id` | string | Foreign key to markets table |
| `timestamp` | int32 | Unix timestamp (seconds) of the price observation |
| `up_price` | float32 | Implied probability the price will go **up** (0–1) |
| `down_price` | float32 | Implied probability the price will go **down** (0–1) |

This normalised design eliminates metadata repetition: the `question` string (often 80+ bytes) is stored exactly once per market instead of once per tick. Dictionary-encoded categoricals (`crypto`, `timeframe`) reduce repeated strings to 1-byte indices. Float32 prices halve storage with negligible precision loss for ML.

### Hive Partitioning

Prices and ticks are partitioned on disk as:

```text
data/                                   ← overridden by --data-dir / POLYMARKET_DATA_DIR
  markets.parquet
  prices/
    crypto=BTC/timeframe=1-hour/part-0.parquet
    crypto=BTC/timeframe=5-minute/part-0.parquet
    crypto=ETH/timeframe=1-hour/part-0.parquet
    ...
  ticks/
    crypto=BTC/timeframe=1-hour/part-0.parquet
    crypto=BTC/timeframe=5-minute/part-0.parquet
    ...
```

Framework tools (PyArrow, DuckDB, HuggingFace `datasets`, Polars) automatically leverage this layout to read only the relevant files when filtering by `crypto` or `timeframe`.

### Test Mode Output (isolated, never affects production)

| Directory | Format |
| ----------- | -------- |
| `test_output_parquet/markets.parquet` | Test markets table |
| `test_output_parquet/prices/` | Test prices (Hive-partitioned) |
| `test_output_parquet/ticks/` | Test ticks (Hive-partitioned) |

## Querying Data Locally (DuckDB)

A built-in DuckDB query layer lets you run SQL directly over the Parquet files:

```bash
# CLI
.venv/bin/python -m polymarket_pipeline.query "SELECT * FROM prices WHERE crypto='BTC' LIMIT 10"

# With a join
.venv/bin/python -m polymarket_pipeline.query \
  "SELECT m.question, p.timestamp, p.up_price, p.down_price
   FROM prices p JOIN markets m ON p.market_id = m.market_id
   WHERE p.crypto='ETH' AND p.timeframe='1-hour'
   ORDER BY p.timestamp DESC LIMIT 20"

# Tick data
.venv/bin/python -m polymarket_pipeline.query \
  "SELECT crypto, timeframe, COUNT(*) AS fills, AVG(size_usdc) AS avg_usdc
   FROM ticks GROUP BY 1, 2 ORDER BY 1, 2"
```

```python
# Python
from polymarket_pipeline.query import query
df = query("SELECT * FROM prices WHERE crypto='BTC' AND timeframe='1-hour' LIMIT 100")
```

## Hugging Face Hub

The `--upload` flag pushes the local Parquet dataset to a Hugging Face Hub dataset repo.

### Authentication

Set `HF_TOKEN` and `HF_REPO_ID` in `.env`:

```bash
HF_TOKEN=hf_xxxxxxxxxxxxxxxxxxxx
HF_REPO_ID=myuser/polymarket-data
```

### Upload

```bash
# Upload using HF_REPO_ID from .env
.venv/bin/python -m polymarket_pipeline --historical-only --upload

# Upload to a specific repo
.venv/bin/python -m polymarket_pipeline --historical-only --upload --hf-repo myuser/my-dataset
```

The upload pushes:

- `markets.parquet` → `data/markets.parquet` in the repo
- `prices/` (full partition tree) → `data/prices/` in the repo
- `ticks/` (full partition tree) → `data/ticks/` in the repo

The repo is created automatically as a public dataset if it doesn't exist.

### Loading from Hugging Face

```python
from datasets import load_dataset

# Stream prices without downloading everything
ds = load_dataset(
    "myuser/polymarket-data",
    data_files={"train": "data/prices/**/*.parquet"},
    split="train",
    streaming=True,
)

# Download & cache (then filter in pandas)
ds = load_dataset("myuser/polymarket-data", split="train")
df = ds.to_pandas()
df_btc = df[df["crypto"] == "BTC"]
```

## Data Collection Details

### Price Data Source and Granularity

Price history is fetched from the Polymarket CLOB API (`clob.polymarket.com/prices-history`) with `fidelity=1`, which returns every recorded trade event within the requested time window (tick-level, irregularly spaced).

**Fetch window — prediction period only.** Each Polymarket crypto up/down market is *created* up to 24 hours before its prediction window opens, but no trading occurs during that dormant phase — prices sit flat at the default opening values (~0.50/0.50). The pipeline restricts the fetch to `[end_ts − timeframe_seconds, end_ts]`, i.e. exactly the active prediction window:

| Timeframe | Fetch window |
| ----------- | ------------- |
| 5-minute | last 300 s |
| 15-minute | last 900 s |
| 1-hour | last 3,600 s |
| 4-hour | last 14,400 s |

This produces 5–15 rows of real, volatile price data per market (reflecting live BTC/ETH/SOL price movement) instead of thousands of flat placeholder ticks.

For each market, both the Up and Down token histories are fetched **concurrently**. Because the two tokens trade independently, their timestamps rarely align exactly. A `merge_asof` forward-fill join on the union of all timestamps is used to produce a single aligned series per market.

### Real-Time Streaming

Active markets are monitored over a WebSocket connection (`ws-subscriptions-clob.polymarket.com`). Each incoming `trade` event updates the last-known price for the relevant token and is buffered in memory. The buffer is flushed to disk every 5 seconds or when 200 rows accumulate, whichever comes first. On disconnect, an exponential backoff reconnect strategy is used (10 s → 20 s → 40 s → … up to 120 s max).

### Reliability Features

- **HTTP connection pooling** — TCP/TLS connections are reused across requests (`pool_maxsize=20`).
- **Transport-level retries** — Automatic retries on 502/503/504 gateway errors before application-level retry logic.
- **Rate-limit handling** — On HTTP 429 responses, the `Retry-After` header is respected; otherwise exponential backoff with random jitter is applied.
- **Atomic writes** — Parquet files are written to a per-PID `.tmp` path (`{path}.{pid}.tmp`) and then atomically renamed, preventing partial/corrupt files on crash and avoiding collisions between concurrent processes.
- **Cross-process write lock** — `storage.py` acquires a `threading.Lock` combined with an `fcntl.flock(LOCK_EX)` advisory lock on `<data_root>/.write.lock` before every read→merge→write cycle. This allows `polymarket-websocket.service` and `polymarket-historical.service` to run side-by-side safely without corrupting shared Parquet files.
- **Non-blocking WebSocket flush** — The WS flush loop swaps the in-memory buffer atomically and offloads Parquet I/O to a thread pool (`run_in_executor`), so disk writes never block the asyncio event loop and cannot cause ping-timeout disconnects.
- **Partition-aware I/O** — On each write, only the affected `(crypto, timeframe)` Parquet partitions are loaded from disk for merging. Unrelated partitions are never read or overwritten.
- **Even shard distribution** — Token IDs are shuffled before WebSocket sharding so high-volume tokens (sorted first by `volume24hr`) spread evenly across all shards instead of concentrating in the first one or two connections.
- **Parallel initial price fetch** — On `--websocket-only` startup, last-known prices for ~3,000 active tokens are fetched via a `ThreadPoolExecutor(max_workers=20)` instead of sequentially, reducing startup latency significantly.
- **Price validation** — Out-of-range prices (outside [0, 1]) are filtered during fetch and logged as warnings.
- **Incremental scan checkpoint** — After each completed closed-market scan, the pipeline writes `data/.scan_checkpoint` with the highest `end_ts` seen. On subsequent runs this is read automatically and the market scan stops as soon as it reaches pages older than `checkpoint − 2 days`. The initial full scan takes ~60 s; every 6-hourly run after that takes 2–5 s. Pass `--from-date YYYY-MM-DD` to override the cutoff manually. The checkpoint is saved incrementally every 500 markets so an interrupted scan retains partial progress.
- **Non-fatal HF uploads** — Errors during Hugging Face Hub uploads are caught and logged as warnings rather than crashing the service, ensuring transient network or auth issues do not interrupt data collection.
- **Daily market re-discovery** — The WebSocket service subscribes to markets found at startup and does not re-scan while running. The `polymarket-restart.timer` restarts `polymarket-websocket.service` daily at 00:05 UTC so newly-created markets are picked up within 24 hours.
