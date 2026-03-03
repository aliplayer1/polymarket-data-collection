# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Activate venv (always required first)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the full pipeline (historical scan → tick backfill → HF upload → live WebSocket)
python -m polymarket_pipeline --upload

# Historical backfill only (safe to re-run; incremental)
python -m polymarket_pipeline --historical-only

# Filter by asset / timeframe
python -m polymarket_pipeline --historical-only --crypto BTC ETH --timeframe 15m 1h

# Smoke-test: collect 10 markets into isolated output and validate
python -m polymarket_pipeline --test 10

# SQL query over local Parquet (tables: markets, prices, ticks)
python -m polymarket_pipeline.query "SELECT crypto, COUNT(*) FROM prices GROUP BY 1"

# Force Hugging Face upload directly (bypasses pipeline)
python -c 'from dotenv import load_dotenv; load_dotenv(); from polymarket_pipeline.storage import upload_to_huggingface; import logging; logging.basicConfig(level=logging.INFO); upload_to_huggingface(logger=logging.getLogger())'
```

There are no automated tests (no `tests/` directory). Validation is done via `--test N` mode.

## Architecture

The pipeline collects Polymarket "crypto up/down" prediction market data and stores it as Parquet. There are two data flows:

**Historical:** `PolymarketApi.fetch_markets()` pages through the Gamma API, filtering for crypto up/down markets. For each closed market, `fetch_price_history()` fetches tick-level prices from the CLOB API for the active prediction window only (not the dormant pre-trade phase). Results are stored via `persist_normalized()` in `storage.py`.

**Live:** `PolymarketDataPipeline` subscribes to active markets over WebSocket (`ws-subscriptions-clob.polymarket.com`). Connections are sharded (≤500 token IDs per shard) to avoid server-side drops. Trade events are buffered in memory and flushed to Parquet every 5 s or 200 rows. WebSocket ticks are also stored to `data/ticks/` alongside on-chain backfilled ticks.

**On-chain ticks:** `PolygonTickFetcher` in `ticks.py` queries `OrderFilled` events from the Polygon CTF Exchange contract via Alchemy JSON-RPC (primary) or Polygonscan/Etherscan V2 (fallback).

### Module Responsibilities

| Module | Role |
|--------|------|
| `config.py` | All constants — API URLs, paths, timeframes, crypto aliases. Parquet paths are set here and imported everywhere else. |
| `models.py` | `MarketRecord` dataclass (single market's metadata + raw data). |
| `api.py` | `PolymarketApi` — HTTP session with pooling/retries, `fetch_markets()` generator, `fetch_price_history()`. |
| `pipeline.py` | `PolymarketDataPipeline` — orchestrates historical scan, tick backfill, and live WebSocket streaming. |
| `storage.py` | Parquet read/write. `persist_normalized()` and `persist_ticks()` load only the affected `(crypto, timeframe)` partitions before merging. `_write_partitioned_atomic()` writes to `.tmp` then renames partition-by-partition. |
| `ticks.py` | `PolygonTickFetcher` — on-chain fill data via Alchemy RPC or Polygonscan. |
| `parsing.py` | Extracts `crypto` and `timeframe` from market question strings; timestamp parsing. |
| `retry.py` | `api_call_with_retry()` — exponential backoff with jitter, respects `Retry-After` header. |
| `query.py` | DuckDB convenience wrapper; auto-registers `markets`, `prices`, `ticks` tables from local Parquet. |
| `cli.py` | Argument parsing and logging setup (stdout + optional file handler). |

### Storage Layout

```
data/                          ← overridden by --data-dir / POLYMARKET_DATA_DIR
  markets.parquet              ← one row per market (metadata only)
  prices/                      ← Hive-partitioned price series
    crypto=BTC/timeframe=1-hour/part-0.parquet
    ...
  ticks/                       ← Hive-partitioned on-chain + WS fills
    crypto=BTC/timeframe=1-hour/part-0.parquet
    ...
  .scan_checkpoint             ← highest end_ts seen; enables fast incremental scans
test_output_parquet/           ← isolated output for --test mode (never production)
```

Timeframe canonical names on disk: `5-minute`, `15-minute`, `1-hour`, `4-hour`.

### Key Design Patterns

- **Incremental scan checkpoint:** After a full scan `data/.scan_checkpoint` is written. Subsequent runs stop paging once market `end_date` < `checkpoint − 2 days`, reducing scan from ~50 pages to 1–2 pages.
- **Partition-aware I/O:** `persist_normalized` / `persist_ticks` load only the affected `(crypto, timeframe)` shard before merging, so unrelated partitions are never touched.
- **Atomic writes:** Parquet files go to `.tmp` first, then rename — prevents corrupt files on crash.
- **WebSocket sharding:** `WS_MAX_TOKENS_PER_SHARD = 500` tokens per connection to stay under the ~200 KB subscription message limit.

## Configuration

Copy `.env.example` to `.env`. Key variables:

| Variable | Purpose |
|----------|---------|
| `POLYGON_RPC_URL` | Alchemy (or similar) Polygon RPC URL — primary tick source, removes Polygonscan rate limits |
| `POLYGONSCAN_API_KEY` | Fallback tick source |
| `HF_TOKEN` / `HF_REPO_ID` | Hugging Face Hub upload credentials |
| `POLYMARKET_DATA_DIR` | Override default `data/` Parquet root |
| `POLYMARKET_LOG_FILE` | Append logs to file in addition to stdout |

All env vars can also be passed as CLI flags (see `docs/cli.md`).
