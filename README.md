# Polymarket Data Pipeline

Modular pipeline for collecting historical and real-time Polymarket prediction market data, with normalised Parquet storage and Hugging Face Hub integration. The dataset targets Polymarket crypto up/down markets (BTC, ETH, SOL, BNB, XRP, DOGE, HYPE) and multi-outcome culture events (e.g. Elon Musk Tweets). The market-matching/parsing logic is completely centralized and driven by a generic dictionary model (`MarketRecord`), meaning any new binary or multi-outcome event can be seamlessly supported with localized definition updates.

---

## 📚 Documentation

To keep this repository easy to navigate, detailed guides have been split into the `docs/` directory:

- 📊 **[Dataset, Schema & Querying](docs/dataset.md)**: Details on the Parquet partitioning format, HuggingFace Hub loading, DuckDB query examples, and on-chain tick fetching architecture.
- ⚙️ **[CLI Options & Configuration](docs/cli.md)**: Full command-line flags, `.env` file templates, and `--test` mode validation.
- ☁️ **[Deployment & Server Operations](docs/deployment.md)**: Instructions for Hetzner cloud deployment, automated `systemd` background services, and real-time pipeline monitoring tools.

---

## Setup

Create and activate a virtual environment, then install dependencies:

**Linux/macOS:**

```bash
python3 -m venv .venv
source .venv/bin/activate
# Note: On Ubuntu 24.04 (PEP 668), always use the venv's pip path
./.venv/bin/pip install -r requirements.txt
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

## Quick Reference

> Common commands for local development. See the [CLI Options & Configuration](docs/cli.md) document for the full execution reference.

```bash
# First-time setup
python3 -m venv .venv && source .venv/bin/activate
./.venv/bin/pip install -r requirements.txt
cp .env.example .env   # then fill in your keys

# Full pipeline (1. Historical market scan + price history -> 2. Tick Backfill -> 3. Hugging Face Upload -> 4. Live WebSocket)
.venv/bin/python -m polymarket_pipeline --upload

# WebSocket-only (skip historical scan and tick backfill; useful alongside a separate --historical-only process)
.venv/bin/python -m polymarket_pipeline --websocket-only

# Historical only (safe to re-run; incremental)
.venv/bin/python -m polymarket_pipeline --historical-only

# Filter by asset / timeframe (works for Crypto or Culture datasets)
# Supports BTC, ETH, SOL, BNB, XRP, DOGE, HYPE, and ELON-TWEETS
.venv/bin/python -m polymarket_pipeline --historical-only --crypto BTC ETH ELON-TWEETS --timeframe 15m 1h 7-day 1-month

# Write to a specific directory and log to file
.venv/bin/python -m polymarket_pipeline \
    --data-dir /mnt/data/polymarket \
    --log-file /var/log/polymarket/pipeline.log

# Unit/regression tests
.venv/bin/python -m pytest -q

# Query local data with SQL (tables: markets, prices, ticks)
.venv/bin/python -m polymarket_pipeline.query "SELECT crypto, COUNT(*) FROM prices GROUP BY 1"
```

## Architecture at a Glance

The runtime is strictly partitioned to maintain isolation between heavily standardized Crypto data and dynamic Culture data:

1. **Dual-Root Storage Architecture**
   - **`data/`**: The strictly tabular, Hive-partitioned home of `crypto` Up/Down binary datasets. Features rigid `up_price` and `down_price` tables. The `markets.parquet` row for each binary market carries `slug`, `closed_ts`, `resolution` (`1`=Up, `0`=Down, `-1`=unresolved), `fee_rate_bps`, and the full set of identity/timestamp fields.
   - **`data-culture/`**: The home of multi-outcome events like Elon Musk tweets. Features completely dynamic JSON `tokens` dictionary columns in `markets.parquet` and long-format `(market_id, timestamp, token_id, outcome, price)` schemas in `prices/` capable of supporting 10+ distinct outcomes concurrently without schema mutations. The culture `markets.parquet` also includes `slug`, `event_slug` (parent event), `bucket_index` (Polymarket's canonical `groupItemThreshold`), `bucket_label` (`groupItemTitle`), `closed_ts`, and `resolution` — the latter is **per-bucket**: `1` means that bucket was the winning outcome of the parent event, `0` means it lost, `-1` means unresolved.

2. **Externalized market definitions** (`polymarket_pipeline/market_definitions.json`, `polymarket_pipeline/markets.py`)  
   Supported market families (Binary & Multi-Outcome), asset aliases, and vocabulary now live in bundled JSON-backed definitions instead of being hardcoded across multiple modules.

3. **Normalization + settings** (`polymarket_pipeline/market_normalization.py`, `polymarket_pipeline/settings.py`)  
   Raw Gamma payloads are converted into generalized `MarketRecord` objects, natively leveraging internal dictionary maps (`tokens: dict[str, str]`) instead of hardcoded UP/DOWN bindings. `slug`, `event_slug`, `bucket_index`, `bucket_label`, and `closed_ts` are extracted directly from the Gamma response (`groupItemThreshold`, `groupItemTitle`, `slug`, `closedTime`). Resolution is detected via `_binary_resolution_from_prices()` — which checks `closed=True` AND `outcomePrices[0] >= 0.99` — because the Gamma API does not expose a `resolved` boolean on its market objects.

4. **Pipeline phases** (`polymarket_pipeline/pipeline.py`, `polymarket_pipeline/phases/`)  
   - `PriceHistoryPhase`: Fetches OHLC data from Gamma/CLOB asynchronously across all mapped tokens.
   - `TickBackfillPhase`: Scans Polygon RPC for trade logs. Automatically **chunks large windows** (e.g. 4-day/7-day/1-month culture markets) into 6-hour segments to ensure memory stability and RPC reliability.
   - `RTDSStreamPhase`: Manages Binance spot prices (Gracefully skipped with `NULL` columns for non-crypto assets).
   - `WebSocketPhase`: Streams live Polymarket trade events with fully lock-free shard writes. Autonomously refreshes market subscriptions every hour. Features crash-resilient `category` handling for "Culture" markets.

### Advanced Data Reliability

- **Precise Market Matching**: Uses regex word boundaries (`\b`) for asset identification. This prevents substring false positives (e.g., "synthetic" incorrectly matching "ETH") and ensures "random" tokens are never erroneously collected.
- **Dynamic Timeframe Parsing**: Support for variable date ranges (e.g., "March 27 to April 3") and month-based ranges (e.g., "in April 2026") using advanced regex normalization in `markets.py`.
- **Streaming Tick Collection**: The `PolygonTickFetcher` uses a memory-efficient callback pattern to process on-chain logs. Events are decoded and filtered as they are streamed from RPC/Etherscan chunks, preventing OOM errors even during high-volume historical backfills.
- **Etherscan Daily-Limit Auto-Switchover**: Etherscan V2 is the primary source for on-chain tick fetching (3 req/s, 100K/day). The rate limiter is enforced on every HTTP attempt (including retries) across all concurrent threads. When the daily limit is hit, the pipeline seamlessly switches to RPC with automatic multi-provider rotation (e.g., Alchemy + QuickNode). Transient per-second rate limits fall back to RPC for the affected request only, keeping Etherscan available for subsequent calls.
- **Fully Lock-Free WebSocket Writes**: All five WS data types (prices, ticks, orderbook, spot prices, culture prices) use lock-free shard writes — the WS flush loop holds zero locks, eliminating contention with upload and historical services.
- **DuckDB Out-of-Core Consolidation**: Tick and orderbook shard consolidation uses DuckDB with configurable memory limits and disk spilling, preventing OOM on large partitions (e.g., BTC/5-minute orderbook with millions of rows).
- **DuckDB SQL Injection Hardening**: All file paths and environment variables interpolated into DuckDB SQL are escaped via `_duckdb_escape()`. The `PM_DUCKDB_MEMORY_LIMIT` env var is regex-validated.
- **Culture Data Hub**: Culture-specific data (e.g., Elon Musk tweets) is automatically isolated in `data-culture/` and uploaded to a dedicated Hugging Face repository (`HF_CULTURE_REPO_ID`).
- **Metadata-Refresh Backfill**: When a historical scan encounters a closed market whose price history is already fully cached, the `PriceHistoryPhase` no longer skips it silently — instead it batches the market into a metadata-only persist pass that rewrites only the relevant rows of `markets.parquet`. This is how the `resolution` field gets populated for events that closed *after* their prices were first captured.
- **Deterministic Consolidation**: Pandas-based consolidation (`_consolidate_partitioned_prices`, `consolidate_spot_prices`) now sorts input files by `(mtime, filename)` before concatenating, so `drop_duplicates(keep="last")` reliably picks the most recently written duplicate. Prior to this, the dedup winner depended on `os.listdir()` order, which is filesystem- and Python-version-specific (silently caused older prices to overwrite newer ones on ARM64 / Python 3.12).

### Extending Supported Markets

Adding a new market domain is completely configuration-driven:

1. **New Asset**: Add a new definition entry to `polymarket_pipeline/market_definitions.json` (Specify `binary` or `multi-outcome`).
2. **Phase Execution**: The `MarketRecord` mapping engine and the dynamic schema discovery engines will safely route, build, and store the dataset directly into the appropriate storage cluster. No `pipeline.py` or `.parquet` schema changes are required.

## Testing & Validation

Recommended local validation:

- **Regression tests:** `./.venv/bin/python -m pytest -q`  
  Covers parsing, storage behavior, definition loading, and offline pipeline run-mode matrix across all storage roots.
- **Lint:** `./.venv/bin/python -m ruff check polymarket_pipeline tests`  
  Catches import, formatting, and unused-code issues across the repository.
- **Live smoke test:** `./.venv/bin/python -m polymarket_pipeline --test 10`  
  Fetches a small real dataset into `test_output_parquet/` and runs data-quality checks against the generated Parquet output.

## Structure

| File | Purpose |
| ------ | --------- |
| `polymarket_pipeline/config.py` | Constants, definitions cache, and separate DB Directory configuration |
| `polymarket_pipeline/market_definitions.json` | Bundled multi-outcome / binary configs |
| `polymarket_pipeline/markets.py` | Definition validation schemas (`BinaryMarketDefinition`, `MultiOutcomeMarketDefinition`) |
| `polymarket_pipeline/market_normalization.py` | Populates category/tokens on unified generalized `MarketRecord` |
| `polymarket_pipeline/models.py` | Dynamic `tokens` map powering the pipeline. |
| `polymarket_pipeline/storage.py` | Atomic Parquet I/O, lock-free shard writes, DuckDB-based tick/orderbook consolidation, pandas-based price consolidation, HF Hub upload, cross-process locking |
| `polymarket_pipeline/ticks.py` | On-chain tick fetcher with Etherscan→RPC auto-switchover and multi-provider rotation |
| `polymarket_pipeline/query.py` | DuckDB SQL query layer over local Parquet files |
| `polymarket_pipeline/phases/` | Extracted pipeline phases utilizing generalized token processing. |
