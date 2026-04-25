# Polymarket Data Pipeline

Modular pipeline for collecting historical and real-time Polymarket prediction market data, with normalised Parquet storage and Hugging Face Hub integration. The dataset targets Polymarket crypto up/down markets (BTC, ETH, SOL, BNB, XRP, DOGE, HYPE) and multi-outcome culture events (e.g. Elon Musk Tweets). The market-matching/parsing logic is completely centralized and driven by a generic dictionary model (`MarketRecord`), meaning any new binary or multi-outcome event can be seamlessly supported with localized definition updates.

---

## 📚 Documentation

To keep this repository easy to navigate, detailed guides have been split into the `docs/` directory:

- 📊 **[Dataset, Schema & Querying](docs/dataset.md)**: Parquet partitioning format, Hugging Face Hub loading, DuckDB query examples, and on-chain tick architecture.
- ⚙️ **[CLI Options & Configuration](docs/cli.md)**: Full command-line flags, `.env` file templates, and `--test` mode validation.
- ☁️ **[Deployment & Server Operations](docs/deployment.md)**: Hetzner cloud deployment, automated `systemd` background services, and real-time pipeline monitoring tools.

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

The pipeline runs without any on-chain RPC credentials by default — historical fills come from the Polymarket orderbook subgraph (Goldsky). The only optional knob is `SUBGRAPH_API_KEY`, which enables a Graph-Network fallback when Goldsky is unreachable.

## Quick Reference

> Common commands for local development. See the [CLI Options & Configuration](docs/cli.md) document for the full execution reference.

```bash
# First-time setup
python3 -m venv .venv && source .venv/bin/activate
./.venv/bin/pip install -r requirements.txt
cp .env.example .env   # then fill in your keys

# Full pipeline (1. Historical market scan + price history → 2. Tick backfill via subgraph → 3. Hugging Face upload → 4. Live WebSocket)
.venv/bin/python -m polymarket_pipeline --upload

# WebSocket-only (skip historical scan; useful alongside a separate --historical-only process)
.venv/bin/python -m polymarket_pipeline --websocket-only

# Historical only (safe to re-run; incremental via .scan_checkpoint)
.venv/bin/python -m polymarket_pipeline --historical-only

# Bounded historical window (server-side filtering bypasses Gamma's 250k-offset pagination cap)
.venv/bin/python -m polymarket_pipeline --historical-only \
    --from-date 2026-01-01 --to-date 2026-04-25

# Filter by asset / timeframe (works for crypto or culture datasets)
.venv/bin/python -m polymarket_pipeline --historical-only \
    --crypto BTC ETH ELON-TWEETS --timeframe 15m 1h 7-day 1-month

# Write to a specific directory and log to file
.venv/bin/python -m polymarket_pipeline \
    --data-dir /mnt/data/polymarket \
    --log-file /var/log/polymarket/pipeline.log

# Smoke test (10 real markets into isolated test_output_parquet/, with validation report)
.venv/bin/python -m polymarket_pipeline --test 10

# Unit/regression tests
.venv/bin/python -m pytest -q

# Query local data with SQL (six tables: markets, prices, ticks, orderbook, spot_prices, heartbeats)
.venv/bin/python -m polymarket_pipeline.query \
    "SELECT crypto, COUNT(*) FROM prices GROUP BY 1"

# One-time recollection of pre-subgraph windows (drops legacy Etherscan/RPC rows)
.venv/bin/python scripts/recollect_legacy_ticks.py \
    --data-dir data --max-workers 4 [--dry-run]
```

## Architecture at a Glance

The runtime is strictly partitioned to maintain isolation between heavily standardized crypto data and dynamic culture data:

1. **Dual-Root Storage Architecture**
   - **`data/`**: Strictly tabular Hive-partitioned home of `crypto` Up/Down binary datasets. Features rigid `up_price` / `down_price` price tables, `markets.parquet` with `slug`, `closed_ts`, `resolution` (`1`=Up, `0`=Down, `-1`=unresolved), `fee_rate_bps`, plus dedicated `orderbook/`, `spot_prices/`, and `heartbeats/` sub-roots.
   - **`data-culture/`**: Multi-outcome events like Elon Musk tweets. Features completely dynamic JSON `tokens` dictionary columns and long-format `(market_id, timestamp, token_id, outcome, price)` schemas capable of supporting 10+ outcomes concurrently without schema mutations. Carries `slug`, `event_slug` (parent event), `bucket_index` (Polymarket's `groupItemThreshold`), `bucket_label` (`groupItemTitle`), `closed_ts`, and **per-bucket** `resolution`.

2. **Externalised market definitions** (`polymarket_pipeline/market_definitions.json`, `markets.py`)
   Supported market families (binary & multi-outcome), asset aliases, and vocabulary live in bundled JSON-backed definitions instead of being hardcoded across multiple modules.

3. **Normalisation + settings** (`market_normalization.py`, `settings.py`)
   Raw Gamma payloads are converted into `MarketRecord` objects, natively leveraging internal `tokens: dict[str, str]` maps instead of hardcoded UP/DOWN bindings. `slug`, `event_slug`, `bucket_index`, `bucket_label`, and `closed_ts` are extracted directly from Gamma. Resolution is detected via `_binary_resolution_from_prices()` — `closed=True` AND any `outcomePrices >= 0.99` — because Gamma does not expose a `resolved` boolean.

4. **Pipeline phases** (`pipeline.py`, `phases/`)
   - **`PriceHistoryPhase`**: Fetches OHLC from Gamma/CLOB. Fee-rate fetches drain in their own pool BEFORE price-history work begins (happens-before barrier — eliminates a race that could persist `fee_rate_bps = -1`).
   - **`TickBackfillPhase`**: Fetches on-chain `OrderFilled` events via Polymarket's orderbook subgraph (Goldsky primary, Graph-Network fallback when `SUBGRAPH_API_KEY` is set). Auto-chunks large windows into 6-hour segments. The legacy Etherscan/RPC `eth_getLogs` path has been removed.
   - **`BinanceHistoryPhase`**: 1-minute spot-price klines for the `spot_prices/` table. Handles HTTP 418 (IP ban) and 429 explicitly with `Retry-After` honoured.
   - **`RTDSStreamPhase`**: Live Binance + Chainlink spot prices with manual PINGs, watchdog-driven reconnect, and a separate `prior_data` signal so misconfigured topics back off instead of tight-looping.
   - **`WebSocketPhase`**: Streams live Polymarket trade events with fully lock-free shard writes. Yields to the event loop every 100 messages so the watchdog co-task can't be starved on hot shards. Autonomously refreshes market subscriptions every hour. Embedded spot prices come with a 30-second freshness cap — stale RTDS feeds map to NULL rather than leaking old prices into fresh ticks.

### Data Reliability & Robustness

- **Subgraph-only tick backfill.** Polymarket's orderbook subgraph is the sole on-chain tick source. Subgraph rows zero out `block_number` / `log_index` (the subgraph doesn't expose them) and instead carry an `order_hash` (EIP-712, unique per fill within a transaction) that serves as the dedup discriminator. No rate limits, no daily caps, no provider rotation — a single GraphQL query per window returns canonical fill records.
- **Hybrid legacy↔subgraph dedup.** When `consolidate_ticks` sees a `(market_id, timestamp_ms, token_id, tx_hash)` group containing both legacy rows (`order_hash IS NULL`) and subgraph rows (`order_hash IS NOT NULL`), the legacy rows are dropped. Re-running historical via the subgraph for any pre-migration window silently overwrites the old data. WS rows (`tx_hash=""`) are explicitly excluded from the filter.
- **Sub-millisecond WS-tick preservation.** The dedup key includes `COALESCE(local_recv_ts_ns, 0)` so multiple WS fills on the same `(market_id, ts_ms, token_id)` survive consolidation as distinct rows. Sub-ms fills happen on hot tokens; without the discriminator they used to silently collapse to one row.
- **Two-layer watchdog.** TCP keepalive surfaces a half-open connection within ~25 s; on top of that, every WS connection (CLOB shards + RTDS feeds) carries a `DataHeartbeat` that flags per-key staleness. The watchdog reports `inf` as the age for keys that were *never seen*, distinguishing "session was healthy then went stale" from "session never delivered anything" so the reconnect loop only resets backoff in the former case.
- **Lock-free WebSocket writes.** All five WS data types (prices, ticks, orderbook, spot prices, heartbeats — and culture prices) write uniquely-named shard files. The flush loop holds zero locks. A module-level `_KNOWN_SHARD_PREFIXES` allowlist tells `_write_partitioned_atomic` which files NOT to delete during stale-file cleanup; a regression test pins parity between the allowlist and the actual writers.
- **Memory-bounded consolidation.** Every consolidation path (ticks, orderbook, prices, culture-prices, spot-prices, heartbeats) streams through DuckDB with a configurable memory cap (`PM_DUCKDB_MEMORY_LIMIT`) and disk spilling. Backed-up shard backlogs that previously OOM-ed the 8 GB CAX21 are now safe.
- **Crash-safe consolidation ordering.** Every consolidate function does `os.replace(tmp, part-0.parquet)` BEFORE removing the old shard files, so a SIGKILL between the two leaves orphan shards (idempotent — next run merges again) rather than an empty partition that the HF upload's `delete_patterns` would propagate to the Hub. After replace, the containing directory is `fsync`-ed for durability across power loss.
- **HF upload safeguards.** `delete_patterns=["**/part-*.parquet"]` (not `*.parquet`) protects `markets.parquet` from accidental remote wipes; if local consolidation produced zero `part-*.parquet`, the entire delete pass is skipped to avoid propagating a local hole. Transient retry covers `huggingface_hub.HfHubHTTPError` and `ConnectionError`, not just substring-matching the legacy ValueError text.
- **Atomic scan checkpoint.** `data/.scan_checkpoint` is written via `.tmp` + `os.replace`. A SIGKILL/power loss mid-write used to truncate it and silently force a multi-hour full re-scan; corrupt checkpoints now log a warning instead of being silent.
- **Reliable retry semantics.** `api_call_with_retry` skips retry for non-retryable 4xx (400/401/403/404/422) so callers' fallback paths (e.g. api.py's 422 → no-order rerun) fire immediately instead of waiting for 3 attempts × backoff. 408/429/503 still retry with `Retry-After` honoured.
- **Status-code based fallback.** The Gamma 422 fallback inspects `exc.response.status_code` instead of substring-matching `"422"` in the exception text (which fired on any HTML body that mentioned the code).
- **Pipeline failure surfaces non-zero.** `cli.main` raises `SystemExit(1)` on uncaught exceptions so systemd's `Restart=on-failure` actually fires and monitoring can distinguish a crash from a clean shutdown.
- **TZ-aware Gamma timestamps.** `parse_iso_timestamp` coerces tz-naive datetimes to UTC instead of letting `.timestamp()` interpret them in system local time — production runs UTC, but developer machines no longer drift by hours.
- **Precise market matching.** Regex word boundaries (`\b`) for asset identification prevent substring false positives (e.g. "synthetic" incorrectly matching "ETH").
- **Dynamic timeframe parsing.** Variable date ranges ("March 27 to April 3"), month-based ranges ("April 2026"), and locale-independent month-name handling.
- **Metadata-refresh backfill.** When a closed market's price history is already cached, `PriceHistoryPhase` batches it into a metadata-only persist pass that rewrites only `markets.parquet` — this is how `resolution` lands for events that closed *after* their prices were captured.
- **Culture data isolation.** Culture-specific data lives in `data-culture/` and uploads to its own HF repo (`HF_CULTURE_REPO_ID`), separate from the crypto dataset.
- **DuckDB SQL injection hardening.** All file paths and env-var values interpolated into DuckDB SQL go through `_duckdb_escape()`. `PM_DUCKDB_MEMORY_LIMIT` is regex-validated.

### Storage Layout

```
data/                          ← crypto binary markets
  markets.parquet              ← one row per market (metadata only)
  prices/crypto=*/timeframe=*/part-*.parquet     ← Hive-partitioned price series
  ticks/crypto=*/timeframe=*/part-*.parquet      ← Hive-partitioned trade fills
  orderbook/crypto=*/timeframe=*/part-*.parquet  ← per-token BBO snapshots
  spot_prices/part-*.parquet   ← continuous Binance + Chainlink spot prices
  heartbeats/part-*.parquet    ← WS connection-health heartbeats
  .scan_checkpoint             ← atomic incremental-scan cutoff
  .write.lock                  ← cross-process fcntl advisory lock

data-culture/                  ← multi-outcome culture markets
  markets.parquet
  prices/crypto=*/timeframe=*/part-*.parquet
  ticks/crypto=*/timeframe=*/part-*.parquet
```

### Tick Schema (`TICKS_SCHEMA`, v5)

| Column | Type | Notes |
|---|---|---|
| `market_id` | string | Polymarket market identifier |
| `timestamp_ms` | int64 | Trade timestamp, epoch ms (server-side) |
| `token_id` | string | CLOB token identifier |
| `outcome` | dict(int8→string) | "Up" / "Down" / culture label |
| `side` | dict(int8→string) | **Maker perspective**: SELL = maker held outcome shares; BUY = maker paid USDC. Same convention on both subgraph and (formerly) RPC paths. |
| `price` | float32 | 0.0–1.0, integer-comparison guarded so exact 1.0 par sweeps don't get rejected by FP rounding |
| `size_usdc` | float32 | Trade notional in USDC |
| `tx_hash` | string | Polygon tx hash (`""` for WS rows) |
| `block_number` | int32 | `0` for subgraph and WS rows; populated for legacy rows that haven't been re-collected yet |
| `log_index` | int32 | `0` for subgraph and WS rows; populated for legacy rows |
| `source` | dict(int8→string) | "onchain" or "websocket" |
| `spot_price_usdt` | float32 | Embedded RTDS Binance price; NULL when feed is stale (>30 s old) |
| `spot_price_ts_ms` | int64 | Source timestamp of the embedded spot price |
| `local_recv_ts_ns` | int64 | Wall-clock ns when WS received the row; NULL on subgraph fills. Used as a dedup discriminator for sub-ms WS trades |
| `order_hash` | string | EIP-712 order hash, unique per subgraph fill within a tx; NULL on legacy and WS rows |

Partition columns (`crypto`, `timeframe`) are encoded in the directory path and omitted from the consolidated file body.

### Extending Supported Markets

Adding a new market domain is fully configuration-driven:

1. **New asset**: Add a new entry to `polymarket_pipeline/market_definitions.json` (specify `binary` or `multi-outcome`).
2. **Phase execution**: The `MarketRecord` mapping engine and dynamic schema discovery route, build, and store the dataset directly into the appropriate storage cluster. No `pipeline.py` or Parquet schema changes required.

## Re-collecting Pre-Subgraph Historical Data

If you have historical tick data that was collected via the legacy Etherscan/RPC path, re-collect it via the subgraph using `scripts/recollect_legacy_ticks.py`. The hybrid filter in `consolidate_ticks` will silently swap the legacy rows for the new subgraph rows on the next consolidation. WS data is untouched.

```bash
# Preview what would be re-collected (no writes)
python scripts/recollect_legacy_ticks.py --data-dir data --dry-run

# Re-collect everything
python scripts/recollect_legacy_ticks.py --data-dir data --max-workers 4

# Re-collect a bounded window or a single asset/timeframe
python scripts/recollect_legacy_ticks.py --data-dir data \
    --from-date 2026-01-01 --to-date 2026-04-01 \
    --crypto BTC --timeframe 5-minute

# Push the merged result to HF
python -m polymarket_pipeline --upload-only
```

## Testing & Validation

Recommended local validation:

- **Regression tests**: `./.venv/bin/python -m pytest -q`
  Covers parsing, storage behaviour (244 tests), schema enforcement, definition loading, and offline pipeline run-mode matrix.
- **Lint**: `./.venv/bin/python -m ruff check polymarket_pipeline tests scripts`
- **Live smoke test**: `./.venv/bin/python -m polymarket_pipeline --test 10`
  Fetches 10 real markets into `test_output_parquet/` and runs data-quality checks. `--upload` is silently stripped in `--test` mode (warning emitted at startup, not after the run).

## Structure

| File | Purpose |
| ------ | --------- |
| `polymarket_pipeline/config.py` | Constants, paths, WS / RTDS / consolidation tuning knobs |
| `polymarket_pipeline/market_definitions.json` | Bundled multi-outcome / binary configs |
| `polymarket_pipeline/markets.py` | Definition validation schemas (`BinaryMarketDefinition`, `MultiOutcomeMarketDefinition`) and timeframe / asset extractors |
| `polymarket_pipeline/market_normalization.py` | Populates category/tokens on unified `MarketRecord` |
| `polymarket_pipeline/models.py` | `MarketRecord` dataclass with dynamic `tokens` map |
| `polymarket_pipeline/parsing.py` | ISO-8601 timestamp parsing (Gamma quirks + tz-naive UTC coercion) |
| `polymarket_pipeline/retry.py` | `api_call_with_retry` — exponential backoff with `Retry-After`, skips non-retryable 4xx |
| `polymarket_pipeline/api.py` | `PolymarketApi` — Gamma + CLOB client with status-code-based fallbacks |
| `polymarket_pipeline/providers.py` | `MarketProvider` / `TickBatchProvider` / `LastTradePriceProvider` Protocols |
| `polymarket_pipeline/subgraph_client.py` | Transport-only GraphQL client with primary/fallback rotation |
| `polymarket_pipeline/storage.py` | Atomic Parquet I/O, lock-free shard writes, DuckDB-streamed consolidation across all six datasets, hybrid legacy↔subgraph dedup, HF Hub upload, cross-process locking with stale-PID re-detection + fsync durability |
| `polymarket_pipeline/query.py` | DuckDB SQL query layer; auto-registers `markets`, `prices`, `ticks`, `orderbook`, `spot_prices`, `heartbeats` views |
| `polymarket_pipeline/alerts.py` | Webhook alerting + signal-handler installation for crash visibility |
| `polymarket_pipeline/cli.py` | Argparse + logging setup; raises `SystemExit(1)` on uncaught exceptions for systemd |
| `polymarket_pipeline/pipeline.py` | Phase orchestrator |
| `polymarket_pipeline/phases/subgraph_ticks.py` | `SubgraphTickFetcher` — sole on-chain tick source |
| `polymarket_pipeline/phases/binance_history.py` | 1-minute kline fetcher with HTTP 418/429 handling and per-PID-per-ms shard naming |
| `polymarket_pipeline/phases/price_history.py` | CLOB price-history fetch with fee-rate happens-before barrier |
| `polymarket_pipeline/phases/tick_backfill.py` | Auto-chunks large windows into 6-hour segments |
| `polymarket_pipeline/phases/websocket.py` | Live CLOB WS phase with yield-to-loop, drop-oldest buffers, BBO state, autonomous refresh |
| `polymarket_pipeline/phases/rtds_stream.py` | RTDS Binance + Chainlink stream with manual PINGs and prior-data backoff |
| `polymarket_pipeline/phases/ws_messages.py` | Pure parsers for CLOB/RTDS message types; rejects zero-size BBOs |
| `polymarket_pipeline/phases/ws_watchdog.py` | `DataHeartbeat`, `ReconnectRateMonitor`, `DropOldestBuffer` (with `requeue` for flush re-insert) |
| `polymarket_pipeline/phases/pyth_prices.py` | Optional Pyth Hermes historical spot prices (atomic per-symbol shard writes) |
| `polymarket_pipeline/phases/shared.py` | `PipelinePaths`, `build_binary_tick_row`, `build_binary_price_row` |
| `scripts/recollect_legacy_ticks.py` | One-time re-collection of pre-subgraph windows via the subgraph |
| `scripts/audit_ws_gaps.py` | Gap-scan tool for WS heartbeat output |
| `scripts/collect_targeted.py` | Standalone collector for ad-hoc month/asset windows (writes to `collections/`) |
| `scripts/profile_dataset.py` | DuckDB profiling queries against a collection |
