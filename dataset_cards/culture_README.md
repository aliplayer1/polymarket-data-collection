---
license: mit
task_categories:
  - time-series-forecasting
  - tabular-classification
tags:
  - polymarket
  - prediction-markets
  - culture
  - elon-musk
  - twitter
  - social-media
  - finance
language:
  - en
pretty_name: Polymarket Culture Markets
size_categories:
  - 1M<n<10M
configs:
  - config_name: markets
    data_files:
      - split: train
        path: data/markets.parquet
  - config_name: prices
    data_files:
      - split: train
        path: data/prices/**/*.parquet
  - config_name: ticks
    data_files:
      - split: train
        path: data/ticks/**/*.parquet
---

# Polymarket Culture Markets

Multi-outcome prediction market data from Polymarket for cultural events. Currently covers **Elon Musk tweet count** markets across 4-day, 7-day, and 1-month timeframes.

Updated automatically every 3 hours.

## Subsets

```python
from datasets import load_dataset

markets = load_dataset("aliplayer1/polymarket-culture-data", "markets")
prices = load_dataset("aliplayer1/polymarket-culture-data", "prices")
ticks = load_dataset("aliplayer1/polymarket-culture-data", "ticks")
```

Or query with DuckDB:

```python
import duckdb

duckdb.sql("""
    SELECT * FROM 'hf://datasets/aliplayer1/polymarket-culture-data/data/prices/**/*.parquet'
    WHERE crypto = 'ELON-TWEETS'
    LIMIT 100
""").show()
```

## Data Description

### `markets` — Market metadata

One row per market with question text, resolution, timeframe, a JSON
`tokens` map, and identity fields (`slug`, `event_slug`, `bucket_index`,
`bucket_label`) that let you group markets into their parent event and
reason about bucket ordering without parsing the question text.

| Column | Type | Description |
|--------|------|-------------|
| `market_id` | string | Polymarket market identifier |
| `question` | string | Market question text |
| `crypto` | string | Event category (e.g. "ELON-TWEETS") |
| `timeframe` | string | Market timeframe — see **Timeframe semantics** below |
| `volume` | float32 | Market volume in USDC |
| `resolution` | int8 | `1` = this bucket won, `0` = it lost, `-1` = unresolved / still open |
| `start_ts` | int64 | Market start timestamp (epoch seconds) |
| `end_ts` | int64 | Market end timestamp (epoch seconds) |
| `closed_ts` | int64 | `closedTime` from Gamma — when the market closed on-chain (`0` if still open) |
| `condition_id` | string | On-chain condition identifier |
| `tokens` | string | JSON map of outcome label → token ID |
| `slug` | string | Full market slug (includes the bucket suffix, e.g. `elon-musk-of-tweets-april-3-april-10-280-299`) |
| `event_slug` | string | Parent event slug (bucket suffix stripped, e.g. `elon-musk-of-tweets-april-3-april-10`) — groups all buckets of a single event |
| `bucket_index` | int32 | Polymarket's `groupItemThreshold` — canonical ordering within an event (`-1` if unknown) |
| `bucket_label` | string | Polymarket's `groupItemTitle` — e.g. `"280-299"` or `"240+"` |

### Timeframe semantics

Values in the `timeframe` column refer to each market's **total trading
lifetime on Polymarket**, not the resolution window of the underlying
question. A market labelled `4-day` is a 48-hour Elon tweet-count
market (the resolution window is 2 days) that was tradeable for
approximately 4 days before closing. The mapping is:

| `timeframe` | Resolution window | Polymarket tier |
|---|---|---|
| `4-day` | 48 hours (2-day question) | `48h` |
| `7-day` | 7 days | `weekly` |
| `1-month` | 1 calendar month | `monthly` |

### `prices` — Long-format price history

Price time series in long format (one row per token per timestamp), unlike the binary crypto dataset which uses wide format (up_price/down_price columns). Hive-partitioned by `crypto` and `timeframe`.

| Column | Type | Description |
|--------|------|-------------|
| `market_id` | string | Polymarket market identifier |
| `timestamp` | int64 | Price timestamp (epoch seconds) |
| `token_id` | string | CLOB token identifier |
| `outcome` | string | Outcome label (e.g. "10-19", "20-29", "Yes") |
| `price` | float32 | Outcome price (0.0-1.0) |
| `crypto` | string | Event category |
| `timeframe` | string | Market timeframe |

### `ticks` — Trade-level fills

Individual trades from on-chain events and WebSocket captures. Same schema as the crypto dataset's ticks table, Hive-partitioned by `crypto` and `timeframe`.

| Column | Type | Description |
|--------|------|-------------|
| `market_id` | string | Polymarket market identifier |
| `timestamp_ms` | int64 | Trade timestamp (epoch milliseconds) |
| `token_id` | string | CLOB token identifier |
| `outcome` | string | Outcome label |
| `side` | string | "BUY" or "SELL" (taker perspective) |
| `price` | float32 | Trade price (0.0-1.0) |
| `size_usdc` | float32 | Trade size in USDC |
| `tx_hash` | string | Transaction hash ("" for WS ticks) |
| `block_number` | int32 | Polygon block number (0 for WS ticks) |
| `log_index` | int32 | Log index within block |
| `source` | string | "onchain" or "websocket" |
| `spot_price_usdt` | float32 | Not applicable for culture markets |
| `spot_price_ts_ms` | int64 | Not applicable for culture markets |

## Known issues & historical caveats

- **Tick coverage is limited.** The `ticks` subset currently contains
  only websocket-captured trades from late March 2026 onward, covering
  a subset of markets. On-chain backfill is planned but not yet in place,
  so the `prices` subset (last-trade snapshots at ~60 s cadence) is the
  primary time series for historical analysis.
- **Sparse bucket coverage on pre-April events.** Events that closed
  before the pipeline's full rollout may have missing low-tweet-count
  buckets in `markets.parquet` (the lowest-price buckets were pruned
  from Polymarket's API response). Newer events have complete coverage.
  Use `bucket_index` (rather than positional ordering within the event)
  to identify buckets — it matches Polymarket's canonical grouping even
  when some sibling buckets are absent from the dataset.
- **`resolution` backfill.** Prior to April 2026 the pipeline wrote
  `resolution = -1` for every culture market because it checked for a
  `resolved` boolean that Polymarket's Gamma API doesn't actually emit.
  The fix (price-based detection + metadata-refresh on re-scan) populates
  this field for newly-closed events going forward, and backfills older
  events on each full scan.

## Pipeline

This dataset is produced by [polymarket-data-pipeline](https://github.com/aliplayer1/polymarket-data-pipeline). See the crypto dataset ([polymarket-crypto-updown](https://huggingface.co/datasets/aliplayer1/polymarket-crypto-updown)) for full pipeline documentation.

## License

MIT
