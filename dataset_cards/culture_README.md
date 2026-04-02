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

One row per market with question text, resolution, timeframe, and a JSON `tokens` map of token IDs to outcome labels.

| Column | Type | Description |
|--------|------|-------------|
| `market_id` | string | Polymarket market identifier |
| `question` | string | Market question text |
| `crypto` | string | Event category (e.g. "ELON-TWEETS") |
| `timeframe` | string | Market timeframe (4-day, 7-day, 1-month) |
| `volume` | float32 | Market volume in USDC |
| `resolution` | int8 | Market resolution |
| `start_ts` | int64 | Market start timestamp (epoch seconds) |
| `end_ts` | int64 | Market end timestamp (epoch seconds) |
| `condition_id` | string | On-chain condition identifier |
| `tokens` | string | JSON map of token ID to outcome label |

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

## Pipeline

This dataset is produced by [polymarket-data-pipeline](https://github.com/aliplayer1/polymarket-data-pipeline). See the crypto dataset ([polymarket-crypto-updown](https://huggingface.co/datasets/aliplayer1/polymarket-crypto-updown)) for full pipeline documentation.

## License

MIT
