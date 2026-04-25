# CLI & Configuration

This document covers all pipeline execution arguments, `.env` configurations, and the isolated test environments.

## Usage

```bash
.venv/bin/python -m polymarket_pipeline [OPTIONS]
```

### CLI Options

| Flag | Type | Description |
| ------ | ------ | ------------- |
| `--historical-only` | flag | Collect historical closed markets only; skip active markets and WebSocket streaming |
| `--websocket-only` | flag | Skip the historical scan and tick backfill phases; go straight to WebSocket streaming for currently-active markets. Useful when a separate `--historical-only` process is already running. |
| `--test N` | int | **Test mode**: collect N historical markets into isolated output, then run a validation report |
| `--markets ID ...` | list | Restrict collection to specific market IDs |
| `--crypto SYMBOL ...` | list | Filter by cryptocurrency symbol: `BTC`, `ETH`, `SOL` |
| `--timeframe TF ...` | list | Filter by timeframe: `5m`, `15m`, `1h`, `4h` |
| `--from-date YYYY-MM-DD` | str | Only scan markets that closed on or after this date. On subsequent runs the cutoff is auto-detected from a saved checkpoint — you normally don't need this flag. |
| `--upload` | flag | Upload the Parquet dataset to Hugging Face Hub after collection |
| `--hf-repo USER/REPO` | str | Hugging Face dataset repo ID (overrides `HF_REPO_ID` env var) |
| `--data-dir PATH` | str | Root directory for Parquet output. Defaults to `data/`. Overrides `POLYMARKET_DATA_DIR` env var. |
| `--log-file PATH` | str | Append log output to a file in addition to stdout. Overrides `POLYMARKET_LOG_FILE` env var. |

### Environment Variables

All credentials and path overrides can be set in `.env` (loaded automatically via `python-dotenv`):

| Variable | Description |
| --------- | ------------- |
| `HF_TOKEN` | Hugging Face write token for `--upload` (from [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)) |
| `HF_HOME` | Path to Hugging Face cache directory. Required for system users without a home directory to store the `hf-xet` deduplication index. |
| `HF_REPO_ID` | Default Hugging Face repo ID (overridden per-run by `--hf-repo`) |
| `PM_FLOCK_TIMEOUT` | Cross-process lock timeout in seconds. Defaults to **300** (5 minutes) to accommodate large data uploads. |
| `PM_DUCKDB_MEMORY_LIMIT` | Override the DuckDB memory limit used during tick consolidation before uploads, e.g. `3GB`. |
| `PM_DUCKDB_THREADS` | Override the DuckDB thread count used during tick consolidation. Defaults to **1**. |
| `SUBGRAPH_API_KEY` | Optional Graph-Network API key. Set this to enable the fallback endpoint when the primary Goldsky subgraph is unreachable. Leave unset for normal operation. |
| `PM_DISABLE_TICK_BACKFILL` | Set to `1` to skip on-chain tick backfill entirely (e.g. for prices-only runs or when iterating offline). |
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

WebSocket-only stream (used alongside a concurrent `--historical-only` service):

```bash
.venv/bin/python -m polymarket_pipeline --websocket-only
```

Collect and upload to Hugging Face Hub:

```bash
.venv/bin/python -m polymarket_pipeline --historical-only --upload --hf-repo myuser/polymarket-data
```

Write data to a specific directory:

```bash
.venv/bin/python -m polymarket_pipeline --data-dir /mnt/data/polymarket --log-file /var/log/polymarket/pipeline.log
```

Test mode — collect 10 markets and validate the output:

```bash
.venv/bin/python -m polymarket_pipeline --test 10
```

## Test Mode

The `--test N` flag provides a safe, non-destructive way to verify the data collection pipeline end-to-end before running a full backfill.

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
