# Polymarket Data Pipeline

Modular pipeline for collecting historical and real-time Polymarket prediction market data, with normalised Parquet storage and Hugging Face Hub integration. The current dataset targets Polymarket crypto up/down markets, and the market-matching/parsing logic is now centralized so additional binary market families can be added with localized changes instead of touching the full pipeline.

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

## Quick Reference

> Common commands for local development. See the [CLI Options & Configuration](docs/cli.md) document for the full execution reference.

```bash
# First-time setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in your keys

# Full pipeline (1. Historical market scan + price history -> 2. Tick Backfill -> 3. Hugging Face Upload -> 4. Live WebSocket)
.venv/bin/python -m polymarket_pipeline --upload

# WebSocket-only (skip historical scan and tick backfill; useful alongside a separate --historical-only process)
.venv/bin/python -m polymarket_pipeline --websocket-only

# Historical only (safe to re-run; incremental)
.venv/bin/python -m polymarket_pipeline --historical-only

# Upload-only (consolidate existing shards + push to Hugging Face; no new fetch)
.venv/bin/python -m polymarket_pipeline --upload-only

# Filter by asset / timeframe
.venv/bin/python -m polymarket_pipeline --historical-only --crypto BTC ETH --timeframe 15m 1h

# Limit initial backfill to the last 12 months (faster first run)
.venv/bin/python -m polymarket_pipeline --historical-only --from-date 2025-02-28

# Write to a specific directory and log to file
.venv/bin/python -m polymarket_pipeline \
    --data-dir /mnt/data/polymarket \
    --log-file /var/log/polymarket/pipeline.log

# Force exactly the Hugging Face upload directly (bypass pipeline)
.venv/bin/python -c 'from dotenv import load_dotenv; load_dotenv(); from polymarket_pipeline.storage import upload_to_huggingface; import logging; logging.basicConfig(level=logging.INFO); upload_to_huggingface(logger=logging.getLogger())'

# Smoke-test — collect 10 markets into isolated output and validate
.venv/bin/python -m polymarket_pipeline --test 10

# Unit/regression tests
.venv/bin/python -m pytest -q

# Lint the repo
.venv/bin/python -m ruff check polymarket_pipeline tests

# Run a single test
.venv/bin/python -m pytest tests/test_market_normalization.py::test_normalize_gamma_market_classifies_reversed_token_order -q

# Query local data with SQL (tables: markets, prices, ticks)
.venv/bin/python -m polymarket_pipeline.query "SELECT crypto, COUNT(*) FROM prices GROUP BY 1"
```

### Upload Tuning

Before `--upload` or `--upload-only`, the pipeline consolidates tick shard files with DuckDB. On smaller servers — or on hosts where `systemd`/cgroups cap the service below total RAM — you can pin safer settings in `.env`:

```bash
PM_DUCKDB_MEMORY_LIMIT=3GB
PM_DUCKDB_THREADS=1
```

If the upload service logs a DuckDB out-of-memory error, check the effective service memory cap with:

```bash
systemctl show polymarket-upload.service -p MemoryCurrent -p MemoryMax -p MemoryHigh
```

See [docs/deployment.md](docs/deployment.md) for the full troubleshooting notes.

## Architecture at a Glance

The runtime is organized into five layers:

1. **Externalized market definitions** (`polymarket_pipeline/market_definitions.json`, `polymarket_pipeline/markets.py`)  
   Supported market families, asset aliases, timeframe aliases, and binary outcome vocabulary now live in bundled JSON-backed definitions instead of being hardcoded across multiple modules.

2. **Normalization + settings** (`polymarket_pipeline/market_normalization.py`, `polymarket_pipeline/settings.py`)  
   Raw Gamma payloads are converted into canonical `MarketRecord` objects, while CLI/env configuration is merged into typed runtime settings and run options once at startup.

3. **Providers** (`polymarket_pipeline/providers.py`, `api.py`, `ticks.py`)  
   Fetching concerns are expressed through small protocols so Gamma/CLOB price history, last-trade lookups, and tick backfill sources can be swapped or stubbed cleanly in tests.

4. **Pipeline phases** (`polymarket_pipeline/pipeline.py`, `polymarket_pipeline/phases/`)  
   `pipeline.py` is now a thin orchestrator over dedicated modular phases:
   - `PriceHistoryPhase`: Fetches OHLC data from Gamma/CLOB.
   - `TickBackfillPhase`: Performs on-chain backfill of trade logs via Polygonscan/RPC.
   - `RTDSStreamPhase`: Manages the Real-Time Data Socket feed for Binance spot prices.
   - `WebSocketPhase`: Streams live Polymarket trade events and merges them with RTDS data.

5. **Persistence/query** (`storage.py`, `query.py`)  
   Stores markets, prices, and ticks in Hive-partitioned Parquet; provides DuckDB-backed querying and upload tooling. Tick data directly embeds crypto spot prices (`spot_price_usdt`) at the exact millisecond using dynamic schema discovery to remain resilient against legacy data shards.

### Data Flow

- **Historical flow:** Gamma market page -> definition-based normalization -> `PriceHistoryPhase` -> normalized Parquet write  
- **Live flow:** active markets -> `WebSocketPhase` (merging with `RTDSStreamPhase` spot price cache) -> staged tick writes with embedded spot prices / periodic price flushes  
- **On-chain flow:** shared market windows -> `TickBackfillPhase` -> canonical tick rows -> DuckDB-consolidated Parquet shards

### Extending Supported Markets

The current storage/query schema still assumes a binary market with `up_price` and `down_price`, but adding another **binary** market family is now much more localized:

1. Add a new definition entry to `polymarket_pipeline/market_definitions.json`
2. Reuse `market_normalization.py` to classify raw Gamma outcomes into canonical up/down sides
3. Add tests covering definition loading, parsing, and normalization for the new family

This keeps the rest of the ingestion pipeline largely unchanged as long as the market can still be represented as a binary pair.

## Testing & Validation

Recommended local validation:

- **Regression tests:** `./.venv/bin/python -m pytest -q`  
  Covers parsing, storage behavior, definition loading, settings, extracted phase helpers, and the offline pipeline run-mode matrix.
- **Lint:** `./.venv/bin/python -m ruff check polymarket_pipeline tests`  
  Catches import, formatting, and unused-code issues across the repository.
- **Live smoke test:** `./.venv/bin/python -m polymarket_pipeline --test 10`  
  Fetches a small real dataset into `test_output_parquet/` and runs data-quality checks against the generated Parquet output.

## Structure

| File | Purpose |
| ------ | --------- |
| `requirements.txt` | Python dependencies |
| `.env.example` | Environment variable template |
| `polymarket_pipeline/config.py` | Constants and runtime configuration |
| `polymarket_pipeline/market_definitions.json` | Bundled external market-definition data for supported market families |
| `polymarket_pipeline/markets.py` | Loader/validator for bundled market definitions plus matching helpers |
| `polymarket_pipeline/market_normalization.py` | Converts raw Gamma API payloads into canonical `MarketRecord` objects |
| `polymarket_pipeline/models.py` | Typed `MarketRecord` model with explicit canonical up/down token mappings |
| `polymarket_pipeline/retry.py` | Retry/backoff utility with rate-limit handling |
| `polymarket_pipeline/parsing.py` | Compatibility parsing helpers built on top of the market-definition layer |
| `polymarket_pipeline/settings.py` | Typed runtime settings and run-option normalization for CLI/env configuration |
| `polymarket_pipeline/providers.py` | Provider protocols and small adapters for pluggable data sources |
| `polymarket_pipeline/api.py` | Gamma/CLOB API access, connection pooling, and delegation to market normalization |
| `polymarket_pipeline/storage.py` | Normalised Parquet I/O and Hugging Face Hub upload |
| `polymarket_pipeline/query.py` | DuckDB SQL query layer over local Parquet files |
| `polymarket_pipeline/ticks.py` | On-chain tick data fetcher (Polygonscan preferred, RPC fallback) |
| `polymarket_pipeline/phases/` | Extracted pipeline phases and shared row/path helpers |
| `polymarket_pipeline/pipeline.py` | Thin orchestration layer that wires settings, providers, and pipeline phases together |
| `polymarket_pipeline/cli.py` | CLI argument parsing and logging setup |
| `polymarket_pipeline/__main__.py` | Module runner (`python -m polymarket_pipeline`) |
| `tests/test_market_definitions.py` | Regression tests for externalized market-definition loading and validation |
| `tests/test_market_normalization.py` | Regression tests for Gamma market normalization and canonical outcome mapping |
| `tests/test_phase_shared.py` | Regression tests for the shared binary storage-row builders |
| `tests/test_parsing.py` | Regression tests for timeframe/asset extraction and CLI timeframe normalization |
| `tests/test_pipeline.py` | Offline orchestration tests for `pipeline.run()` across test, historical-only, and websocket-only modes |
| `tests/test_price_history_phase.py` | Regression tests for extracted price-history phase behavior |
| `tests/test_settings.py` | Regression tests for typed settings and run-option normalization |
| `tests/test_storage.py` | Regression tests for Parquet storage, locking, and legacy tick-shard consolidation compatibility |
| `tests/test_tick_backfill_phase.py` | Regression tests for extracted tick-backfill phase batching |
| `scripts/tick_backfill.py` | Standalone historical tick backfill (Deprecated: handled natively by `pipeline.py`) |
| `deploy/setup.sh` | One-shot server provisioner (Ubuntu 22.04/24.04) |
| `deploy/polymarket-websocket.service` | systemd unit — continuous `--websocket-only` stream |
| `deploy/polymarket-historical.service` | systemd unit — `--historical-only --upload` one-shot fetch |
| `deploy/polymarket-historical.timer` | systemd timer — triggers historical fetch + HF upload every 6 h |
| `deploy/polymarket-upload.service` | systemd unit — `--upload-only` one-shot consolidation + Hugging Face upload |
| `deploy/polymarket-restart.service` | systemd oneshot unit — restarts `polymarket-websocket.service` |
| `deploy/polymarket-restart.timer` | systemd timer — restarts WebSocket service daily at 00:05 UTC to discover newly-created markets |
| `deploy/polymarket-live.service` | **Legacy** — combined historical + WebSocket service (superseded by the split service pair above) |
