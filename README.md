# Polymarket Data Pipeline

Modular pipeline for collecting historical and real-time Polymarket crypto up/down market data, with normalised Parquet storage and Hugging Face Hub integration. Designed to run unattended on a cloud server and accumulate datasets suitable for training intramarket ML / scalping models.

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

# Full pipeline (1. OHLC Scan -> 2. Tick Backfill -> 3. Hugging Face Upload -> 4. Live WebSocket)
.venv/bin/python -m polymarket_pipeline --upload

# Historical only (safe to re-run; incremental)
.venv/bin/python -m polymarket_pipeline --historical-only

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

# Query local data with SQL (tables: markets, prices, ticks)
.venv/bin/python -m polymarket_pipeline.query "SELECT crypto, COUNT(*) FROM prices GROUP BY 1"
```

## Structure

| File | Purpose |
| ------ | --------- |
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
| `scripts/tick_backfill.py` | Standalone historical tick backfill (Deprecated: handled natively by `pipeline.py`) |
| `deploy/setup.sh` | One-shot server provisioner (Ubuntu 22.04/24.04) |
| `deploy/polymarket-live.service` | systemd unit — continuous WebSocket stream |
| `deploy/polymarket-historical.service` | systemd unit — historical fetch (one-shot) |
| `deploy/polymarket-historical.timer` | systemd timer — triggers historical fetch every 6 h |
| `deploy/polymarket-restart.service` | systemd oneshot unit — restarts the live service |
| `deploy/polymarket-restart.timer` | systemd timer — restarts the live service daily at 00:05 UTC to discover newly-created markets |
