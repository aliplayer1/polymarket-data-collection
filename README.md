# Polymarket Data Pipeline

Modular pipeline for collecting historical and real-time Polymarket crypto up/down market data, with normalised Parquet storage and Hugging Face Hub integration. Designed to run unattended on a cloud instance (OCI, AWS, etc.) and accumulate datasets suitable for training intramarket ML / scalping models.

## Table of Contents

- [Quick Reference](#quick-reference)
- [Structure](#structure)
- [Setup](#setup)
- [Usage](#usage)
  - [CLI Options](#cli-options)
  - [Environment Variables](#environment-variables)
  - [Examples](#examples)
- [Cloud Deployment (Oracle Cloud Free Tier)](#cloud-deployment-oracle-cloud-free-tier)
  - [Architecture](#architecture)
  - [Quick Start](#quick-start)
- [Tick-Level Trade Data](#tick-level-trade-data)
  - [Sources](#sources)
  - [On-Chain Backfill](#on-chain-backfill)
  - [WebSocket Ticks (Live)](#websocket-ticks-live)
  - [Tick Storage Schema](#tick-storage-schema)
- [Test Mode](#test-mode)
- [Storage Format](#storage-format)
  - [Normalised Schema (Parquet)](#normalised-schema-parquet)
  - [Hive Partitioning](#hive-partitioning)
  - [Test Mode Output](#test-mode-output-isolated-never-affects-production)
- [Querying Data Locally (DuckDB)](#querying-data-locally-duckdb)
- [Hugging Face Hub](#hugging-face-hub)
  - [Authentication](#authentication)
  - [Upload](#upload)
  - [Loading from Hugging Face](#loading-from-hugging-face)
- [Data Collection Details](#data-collection-details)
  - [Price Data Source and Granularity](#price-data-source-and-granularity)
  - [Real-Time Streaming](#real-time-streaming)
  - [Reliability Features](#reliability-features)

## Quick Reference

> Common commands for returning users. See [Usage](#usage) for the full option reference.

```bash
# First-time setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in your keys

# Full pipeline (historical backfill + live WebSocket)
.venv/bin/python -m polymarket_pipeline

# Historical only (safe to re-run; incremental)
.venv/bin/python -m polymarket_pipeline --historical-only

# Filter by asset / timeframe
.venv/bin/python -m polymarket_pipeline --historical-only --crypto BTC ETH --timeframe 15m 1h

# Limit initial backfill to the last 12 months (faster first run)
.venv/bin/python -m polymarket_pipeline --historical-only --from-date 2025-02-28

# Write to a specific directory (e.g. block volume) and log to file
.venv/bin/python -m polymarket_pipeline \
    --data-dir /mnt/data/polymarket \
    --log-file /var/log/polymarket/pipeline.log

# Collect, then upload to Hugging Face Hub
.venv/bin/python -m polymarket_pipeline --historical-only --upload --hf-repo myuser/polymarket-data

# Smoke-test — collect 10 markets into isolated output and validate
.venv/bin/python -m polymarket_pipeline --test 10

# Query local data with SQL (tables: markets, prices, ticks)
.venv/bin/python -m polymarket_pipeline.query "SELECT crypto, COUNT(*) FROM prices GROUP BY 1"
```

## Structure

| File | Purpose |
| ------ | --------- |
| `test.py` | Compatibility entrypoint |
| `requirements.txt` | Python dependencies |
| `.env.example` | Environment variable template |
| `polymarket_pipeline/config.py` | Constants and runtime configuration |
| `polymarket_pipeline/models.py` | Typed `MarketRecord` data model |
| `polymarket_pipeline/retry.py` | Retry/backoff utility with rate-limit handling |
| `polymarket_pipeline/parsing.py` | Market text and timestamp parsing helpers |
| `polymarket_pipeline/api.py` | Gamma/CLOB API access, connection pooling, price validation |
| `polymarket_pipeline/storage.py` | Normalised Parquet I/O and Hugging Face Hub upload |
| `polymarket_pipeline/query.py` | DuckDB SQL query layer over local Parquet files |
| `polymarket_pipeline/ticks.py` | On-chain tick data fetcher (Polygonscan + RPC fallback) |
| `polymarket_pipeline/pipeline.py` | Historical + WebSocket ingestion orchestration |
| `polymarket_pipeline/cli.py` | CLI argument parsing and logging setup |
| `polymarket_pipeline/__main__.py` | Module runner (`python -m polymarket_pipeline`) |
| `deploy/setup.sh` | One-shot OCI provisioner (Ubuntu 22.04) |
| `deploy/polymarket-live.service` | systemd unit — continuous WebSocket stream |
| `deploy/polymarket-historical.service` | systemd unit — historical fetch (one-shot) |
| `deploy/polymarket-historical.timer` | systemd timer — triggers historical fetch every 6 h |

## Setup

Create and activate a virtual environment, then install dependencies:

**Linux/macOS:**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Windows (PowerShell):**

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

## Usage

```bash
# Via module runner
.venv/bin/python -m polymarket_pipeline [OPTIONS]

# Via compatibility entrypoint
.venv/bin/python test.py [OPTIONS]
```

### CLI Options

| Flag | Type | Description |
| ------ | ------ | ------------- |
| `--historical-only` | flag | Collect historical closed markets only; skip active markets and WebSocket streaming |
| `--test N` | int | **Test mode**: collect N historical markets into isolated output, then run a validation report |
| `--markets ID ...` | list | Restrict collection to specific market IDs |
| `--crypto SYMBOL ...` | list | Filter by cryptocurrency symbol: `BTC`, `ETH`, `SOL` |
| `--timeframe TF ...` | list | Filter by timeframe: `5m`, `15m`, `1h`, `4h` |
| `--from-date YYYY-MM-DD` | str | Only scan markets that closed on or after this date. Useful for the initial backfill. On subsequent runs the cutoff is auto-detected from a saved checkpoint — you normally don't need this flag. |
| `--upload` | flag | Upload the Parquet dataset to Hugging Face Hub after collection |
| `--hf-repo USER/REPO` | str | Hugging Face dataset repo ID (default: `polymarket-crypto-updown`) |
| `--data-dir PATH` | str | Root directory for Parquet output. Defaults to `data/` relative to the working directory. Set to an absolute path (e.g. `/mnt/data/polymarket`) on a cloud instance. Overrides `POLYMARKET_DATA_DIR` env var. |
| `--log-file PATH` | str | Append log output to a file in addition to stdout. Parent directories are created automatically. Overrides `POLYMARKET_LOG_FILE` env var. |
| `--polygonscan-key KEY` | str | Polygonscan API key for on-chain tick backfill (overrides `POLYGONSCAN_API_KEY` env var) |
| `--rpc-url URL` | str | Polygon JSON-RPC endpoint for tick backfill fallback (overrides `POLYGON_RPC_URL` env var) |

### Environment Variables

All credentials and path overrides can be set in `.env` (loaded automatically via `python-dotenv`):

| Variable | Description |
| --------- | ------------- |
| `POLYGONSCAN_API_KEY` | Free API key from [polygonscan.com/myapikey](https://polygonscan.com/myapikey) |
| `POLYGON_RPC_URL` | Polygon JSON-RPC endpoint — fallback for tick backfill. Defaults to `https://polygon-bor-rpc.publicnode.com` in `.env.example`. Avoid `polygon-rpc.com` (returns 401). |
| `HF_TOKEN` | Hugging Face write token for `--upload` (from [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)) |
| `HF_REPO_ID` | Default Hugging Face repo ID (overridden per-run by `--hf-repo`) |
| `POLYMARKET_DATA_DIR` | Root directory for Parquet output (overridden per-run by `--data-dir`) |
| `POLYMARKET_LOG_FILE` | Log file path (overridden per-run by `--log-file`) |

### Examples

Run the full pipeline (historical backfill + real-time WebSocket):

```bash
.venv/bin/python -m polymarket_pipeline
```

Historical backfill only, filtered by asset and timeframe:

```bash
.venv/bin/python -m polymarket_pipeline --historical-only --crypto BTC ETH --timeframe 15m 1h
```

Collect and upload to Hugging Face Hub:

```bash
.venv/bin/python -m polymarket_pipeline --historical-only --upload --hf-repo myuser/polymarket-data
```

Write data to a specific directory (e.g. a mounted block volume):

```bash
.venv/bin/python -m polymarket_pipeline --data-dir /mnt/data/polymarket --log-file /var/log/polymarket/pipeline.log
```

Test mode — collect 10 markets and validate the output:

```bash
.venv/bin/python -m polymarket_pipeline --test 10
```

Test mode with filters applied:

```bash
.venv/bin/python -m polymarket_pipeline --test 5 --crypto BTC --timeframe 1h
```

Run with on-chain tick backfill (requires a Polygonscan API key in `.env` or via flag):

```bash
.venv/bin/python -m polymarket_pipeline --crypto BTC
```

## Cloud Deployment (Oracle Cloud Free Tier)

The `deploy/` directory contains everything needed to run the pipeline continuously on a free **Oracle Cloud Infrastructure (OCI)** instance.

### Architecture

```text
OCI VM.Standard.A1.Flex (4 OCPU / 24 GB RAM — Always Free)
  ├── polymarket-live.service      → 24/7 WebSocket stream, auto-restart
  └── polymarket-historical.timer  → historical re-fetch every 6 h + HF upload

  Block Volume (200 GB — Always Free)
  └── /mnt/data/polymarket/
        ├── markets.parquet
        ├── prices/   (Hive-partitioned)
        └── ticks/    (Hive-partitioned)

  Hugging Face Hub dataset repo
  └── updated every 6 h via --upload
```

### Quick Start

1. Create an **OCI VM.Standard.A1.Flex** instance (Ubuntu 22.04) with a **200 GB Block Volume** attached.
1. Push this repo to GitHub and update the clone URL in `deploy/setup.sh`.
1. SSH into the instance and run the provisioner as root:

```bash
sudo bash setup.sh
```

1. Edit `/opt/polymarket/.env` with your credentials, then restart the services:

```bash
sudo systemctl restart polymarket-live polymarket-historical.timer
```

1. Verify:

```bash
sudo journalctl -fu polymarket-live
sudo systemctl list-timers polymarket-historical.timer
```

See `deploy/setup.sh` for the full provisioning details.

## Tick-Level Trade Data

In addition to the OHLC-style price series, the pipeline collects **individual trade fills** (tick data) for use in ML model training — particularly scalping models that require sub-minute resolution.

### Sources

| Source | Resolution | Coverage | Requires |
| -------- | ----------- | ---------- | ---------- |
| **Polygon on-chain** (`OrderFilled` events) | ~2 s (block time) | Any historical market | Polygonscan API key (recommended) and/or RPC URL |
| **WebSocket** (`trade` events) | Millisecond | Live / active markets only | No extra credentials |

### On-Chain Backfill

Every Polymarket trade fill is settled on Polygon PoS as an `OrderFilled` event on the CTF Exchange contract (`0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`). The pipeline queries these events via the **Etherscan V2 unified API** (`api.etherscan.io/v2/api`, chain ID 137) with a fallback to the **native Polygonscan API** (`api.polygonscan.com/api`) for block-number lookups, and decodes the event data to reconstruct:

- Exact fill timestamp (block timestamp, ~2 s precision)
- Outcome token (`Up` / `Down`)
- Trade side (`BUY` / `SELL`)
- Fill price (USDC / outcome token)
- Notional size (USDC)

The backfill runs automatically after the historical price collection completes, provided `POLYGONSCAN_API_KEY` (or `--polygonscan-key`) is set. Rate limiting is respected: requests are throttled to **~2.5 req/s** (below the Etherscan V2 free plan limit of 3 req/s). If the Etherscan V2 endpoint fails (e.g. rate-limit spikes), the pipeline automatically retries via the native Polygonscan API before falling back to direct JSON-RPC.

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

## Test Mode

The `--test N` flag provides a safe, non-destructive way to verify the data collection
pipeline end-to-end before running a full backfill.

**What it does:**

1. Collects data for the first **N matching closed markets** found (applying any `--crypto`/`--timeframe`/`--markets` filters).
2. Writes output to an isolated `test_output_parquet/` directory — production data is never touched.
3. Skips active markets and WebSocket streaming entirely.
4. Prints an automated **validation report** when collection finishes.

**Validation checks run per timeframe:**

| Check | Description |
| ------- | ------------- |
| NaN prices | Fails if any `up_price` or `down_price` is missing |
| Price bounds | Fails if any price is outside [0, 1] |
| Price sum ≈ 1.0 | Warns if `up_price + down_price` deviates more than 0.15 from 1.0 |
| Monotonic timestamps | Fails if timestamps are not strictly increasing within a market |
| No duplicates | Fails if any `(market_id, timestamp)` pair appears more than once |

The report ends with a clear `ALL CHECKS PASSED` or `SOME CHECKS FAILED` summary, plus sample rows from each timeframe.

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

Set the `HF_TOKEN` environment variable in `.env`:

```bash
HF_TOKEN=hf_xxxxxxxxxxxxxxxxxxxx
```

Or log in interactively (one-time):

```bash
huggingface-cli login
```

### Upload

```bash
# Upload to the default repo
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
- **Atomic writes** — Parquet files are written to a `.tmp` path and then atomically renamed, preventing partial/corrupt files on crash.
- **Partition-aware I/O** — On each write, only the affected `(crypto, timeframe)` Parquet partitions are loaded from disk for merging. Unrelated partitions are never read or overwritten.
- **Price validation** — Out-of-range prices (outside [0, 1]) are filtered during fetch and logged as warnings.
- **Incremental scan checkpoint** — After each completed closed-market scan, the pipeline writes `data/.scan_checkpoint` with the highest `end_ts` seen. On subsequent runs this is read automatically and the market scan stops as soon as it reaches pages older than `checkpoint − 2 days`. The initial full scan (Polymarket has 26 k+ markets) takes ~60 s; every 6-hourly OCI run after that takes 2–5 s. Pass `--from-date YYYY-MM-DD` to override the cutoff manually for the initial backfill.
