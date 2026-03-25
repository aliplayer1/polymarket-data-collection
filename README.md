# Polymarket Data Pipeline

Modular pipeline for collecting historical and real-time Polymarket prediction market data, with normalised Parquet storage and Hugging Face Hub integration. The dataset targets Polymarket crypto up/down markets and multi-outcome culture events (e.g. Elon Musk Tweets). The market-matching/parsing logic is completely centralized and driven by a generic dictionary model (`MarketRecord`), meaning any new binary or multi-outcome event can be seamlessly supported with localized definition updates.

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

# Filter by asset / timeframe (works for Crypto or Culture datasets)
.venv/bin/python -m polymarket_pipeline --historical-only --crypto BTC ETH ELON-TWEETS --timeframe 15m 1h 7-day

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
   - **`data/`**: The strictly tabular, Hive-partitioned home of `crypto` Up/Down binary datasets. Features rigid `up_price` and `down_price` tables.
   - **`data-culture/`**: The home of multi-outcome events like Elon Musk tweets. Features completely dynamic JSON `tokens` dictionary columns in `markets.parquet` and long-format `(market_id, timestamp, token_id, outcome, price)` schemas in `prices/` capable of supporting 10+ distinct outcomes concurrently without schema mutations.

2. **Externalized market definitions** (`polymarket_pipeline/market_definitions.json`, `polymarket_pipeline/markets.py`)  
   Supported market families (Binary & Multi-Outcome), asset aliases, and vocabulary now live in bundled JSON-backed definitions instead of being hardcoded across multiple modules.

3. **Normalization + settings** (`polymarket_pipeline/market_normalization.py`, `polymarket_pipeline/settings.py`)  
   Raw Gamma payloads are converted into generalized `MarketRecord` objects, natively leveraging internal dictionary maps (`tokens: dict[str, str]`) instead of hardcoded UP/DOWN bindings.

4. **Pipeline phases** (`polymarket_pipeline/pipeline.py`, `polymarket_pipeline/phases/`)  
   - `PriceHistoryPhase`: Fetches OHLC data from Gamma/CLOB asynchronously across all mapped tokens.
   - `TickBackfillPhase`: Scans Polygon RPC for trade logs involving any active token map.
   - `RTDSStreamPhase`: Manages Binance spot prices (Gracefully skipped with `NULL` columns for non-crypto assets).
   - `WebSocketPhase`: Streams live Polymarket trade events.

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
| `polymarket_pipeline/storage.py` | Separate atomic writers & DuckDB partitions for `data/` and `data-culture/` |
| `polymarket_pipeline/phases/` | Extracted pipeline phases utilizing generalized token processing. |
