---
license: mit
task_categories:
  - time-series-forecasting
  - tabular-classification
tags:
  - polymarket
  - prediction-markets
  - crypto
  - on-chain
  - orderbook
  - bitcoin
  - ethereum
  - defi
  - finance
language:
  - en
pretty_name: Polymarket Crypto Up/Down Markets
size_categories:
  - 10M<n<100M
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
  - config_name: spot_prices
    data_files:
      - split: train
        path: data/spot_prices/*.parquet
  - config_name: orderbook
    data_files:
      - split: train
        path: data/orderbook/**/*.parquet
---

# Polymarket Crypto Up/Down Markets

Comprehensive dataset of Polymarket binary prediction markets for cryptocurrency price movements. Covers **BTC, ETH, SOL, BNB, XRP, DOGE, and HYPE** across multiple timeframes (5-minute, 15-minute, 1-hour, 4-hour).

Updated automatically every 3 hours.

## Subsets

Load a specific subset:

```python
from datasets import load_dataset

markets = load_dataset("aliplayer1/polymarket-crypto-updown", "markets")
prices = load_dataset("aliplayer1/polymarket-crypto-updown", "prices")
ticks = load_dataset("aliplayer1/polymarket-crypto-updown", "ticks")
spot = load_dataset("aliplayer1/polymarket-crypto-updown", "spot_prices")
orderbook = load_dataset("aliplayer1/polymarket-crypto-updown", "orderbook")
```

Or query directly with DuckDB:

```python
import duckdb

duckdb.sql("""
    SELECT * FROM 'hf://datasets/aliplayer1/polymarket-crypto-updown/data/prices/**/*.parquet'
    WHERE crypto = 'BTC' AND timeframe = '1-hour'
    LIMIT 100
""").show()
```

## Data Description

### `markets` — Market metadata

One row per market. Contains market question, resolution, timeframe, token IDs, and fee rates.

| Column | Type | Description |
|--------|------|-------------|
| `market_id` | string | Polymarket market identifier |
| `question` | string | Market question text |
| `crypto` | string | Asset symbol (BTC, ETH, SOL, ...) |
| `timeframe` | string | Market timeframe (5-minute, 15-minute, 1-hour, 4-hour) |
| `volume` | float32 | Market volume in USDC |
| `resolution` | int8 | Market resolution (1=Up, 0=Down, -1=unknown) |
| `start_ts` | int64 | Market start timestamp (epoch seconds) |
| `end_ts` | int64 | Market end timestamp (epoch seconds) |
| `condition_id` | string | On-chain condition identifier |
| `up_token_id` | string | CLOB token ID for "Up" outcome |
| `down_token_id` | string | CLOB token ID for "Down" outcome |
| `fee_rate_bps` | int16 | Taker fee rate in basis points (-1 = unknown) |

### `prices` — OHLC price history

Historical price series from the CLOB API for the active prediction window of each market. Hive-partitioned by `crypto` and `timeframe`.

| Column | Type | Description |
|--------|------|-------------|
| `market_id` | string | Polymarket market identifier |
| `crypto` | string | Asset symbol |
| `timeframe` | string | Market timeframe |
| `timestamp` | int64 | Price timestamp (epoch seconds) |
| `up_price` | float32 | "Up" outcome price (0.0-1.0) |
| `down_price` | float32 | "Down" outcome price (0.0-1.0) |
| `volume` | float32 | Market volume |
| `question` | string | Market question text |
| `resolution` | string | Market resolution (nullable) |

### `ticks` — Trade-level fills

Individual trades from on-chain `OrderFilled` events (Etherscan/RPC) and live WebSocket captures. Hive-partitioned by `crypto` and `timeframe`.

| Column | Type | Description |
|--------|------|-------------|
| `market_id` | string | Polymarket market identifier |
| `timestamp_ms` | int64 | Trade timestamp (epoch milliseconds) |
| `token_id` | string | CLOB token identifier |
| `outcome` | string | "Up" or "Down" |
| `side` | string | "BUY" or "SELL" (taker perspective) |
| `price` | float32 | Trade price (0.0-1.0) |
| `size_usdc` | float32 | Trade size in USDC |
| `tx_hash` | string | Polygon transaction hash ("" for WS ticks) |
| `block_number` | int32 | Polygon block number (0 for WS ticks) |
| `log_index` | int32 | Log index within block |
| `source` | string | "onchain" or "websocket" |
| `spot_price_usdt` | float32 | Binance spot price at time of trade |
| `spot_price_ts_ms` | int64 | Binance spot price timestamp |

### `spot_prices` — Continuous spot price feed

Binance and Chainlink spot prices streamed in real-time alongside the prediction market data. Useful for correlating prediction market activity with underlying asset prices.

| Column | Type | Description |
|--------|------|-------------|
| `ts_ms` | int64 | Source timestamp (epoch ms) |
| `symbol` | string | e.g. "btcusdt", "eth/usd" |
| `price` | float64 | Spot price in USD(T) |
| `source` | string | "binance" or "chainlink" |

### `orderbook` — Best bid/ask snapshots

Per-token best bid and ask from CLOB WebSocket events. Hive-partitioned by `crypto` and `timeframe`.

| Column | Type | Description |
|--------|------|-------------|
| `ts_ms` | int64 | Receipt timestamp (epoch ms) |
| `market_id` | string | Polymarket market identifier |
| `token_id` | string | CLOB token identifier |
| `outcome` | string | "Up" or "Down" |
| `best_bid` | float32 | Best bid price (0.0-1.0) |
| `best_ask` | float32 | Best ask price (0.0-1.0) |
| `best_bid_size` | float32 | Best bid size (shares) |
| `best_ask_size` | float32 | Best ask size (shares) |

## Storage Layout

All files are Parquet with Zstd compression, Hive-partitioned where noted:

```
data/
  markets.parquet
  prices/crypto=BTC/timeframe=1-hour/part-0.parquet
  ticks/crypto=ETH/timeframe=5-minute/part-0.parquet
  spot_prices/part-0.parquet
  orderbook/crypto=SOL/timeframe=15-minute/part-0.parquet
```

## Pipeline

This dataset is produced by [polymarket-data-pipeline](https://github.com/aliplayer1/polymarket-data-pipeline), which runs three services:

- **Historical scan** (every 6h): pages through Polymarket's Gamma API for closed markets, fetches CLOB price history, and backfills on-chain tick data from Polygon.
- **Live WebSocket** (24/7): captures real-time trades, orderbook BBO, and spot prices.
- **Upload** (every 3h): consolidates shard files and pushes to this dataset.

## License

MIT
