import pandas as pd
import pytest

from polymarket_pipeline.models import MarketRecord
from polymarket_pipeline.phases.shared import (
    build_binary_price_frame,
    build_binary_price_row,
    build_binary_tick_row,
    build_spot_price_row,
    build_orderbook_row,
)


def _market() -> MarketRecord:
    return MarketRecord(
        market_id="m1",
        market_type="crypto-up-down",
        question="Bitcoin Up or Down - 5-Minute market",
        timeframe="5-minute",
        crypto="BTC",
        condition_id=None,
        start_ts=0,
        end_ts=300,
        up_token_id="up-token",
        down_token_id="down-token",
        up_outcome="Up",
        down_outcome="Down",
        volume=42.0,
        resolution=1,
        is_active=False,
    )


def test_shared_builders_preserve_binary_schema() -> None:
    market = _market()

    frame = build_binary_price_frame(
        market,
        timestamps=pd.Series([100, 200]),
        side_prices={"up": [0.6, 0.7], "down": [0.4, 0.3]},
        volume=market.volume,
        resolution=market.resolution,
        question=market.question,
    )
    row = build_binary_price_row(
        market,
        timestamp=250,
        side_prices={"up": 0.61, "down": 0.39},
        resolution=market.resolution,
    )
    tick = build_binary_tick_row(
        market,
        timestamp_ms=250000,
        token_id=market.up_token_id,
        outcome_side="up",
        trade_side="BUY",
        price=0.61,
        size_usdc=12.5,
        tx_hash="0xabc",
        block_number=123,
        log_index=4,
        source="websocket",
    )

    assert frame[["market_id", "timestamp", "up_price", "down_price"]].to_dict("records") == [
        {"market_id": "m1", "timestamp": 100, "up_price": 0.6, "down_price": 0.4},
        {"market_id": "m1", "timestamp": 200, "up_price": 0.7, "down_price": 0.3},
    ]
    assert row["question"] == market.question
    assert row["up_price"] == 0.61
    assert row["down_price"] == 0.39
    assert tick["outcome"] == market.up_outcome
    assert tick["source"] == "websocket"


def test_shared_builders_require_both_binary_sides() -> None:
    market = _market()

    with pytest.raises(ValueError, match="side_prices"):
        build_binary_price_row(
            market,
            timestamp=100,
            side_prices={"up": 0.5},
            resolution=None,
        )


def test_spot_price_row_builder() -> None:
    row = build_spot_price_row(
        ts_ms=1710000000000,
        symbol="btcusdt",
        price=67234.50,
        source="binance",
    )
    assert row["ts_ms"] == 1710000000000
    assert row["symbol"] == "btcusdt"
    assert row["price"] == 67234.50
    assert row["source"] == "binance"


def test_spot_price_row_builder_chainlink() -> None:
    row = build_spot_price_row(
        ts_ms=1710000000000,
        symbol="btc/usd",
        price=67200.12345,
        source="chainlink",
    )
    assert row["symbol"] == "btc/usd"
    assert row["source"] == "chainlink"
    assert row["price"] == 67200.12345


def test_orderbook_row_builder() -> None:
    market = _market()
    row = build_orderbook_row(
        market,
        ts_ms=1710000000000,
        token_id="up-token",
        outcome_side="up",
        best_bid=0.48,
        best_ask=0.52,
        best_bid_size=150.0,
        best_ask_size=200.0,
    )
    assert row["ts_ms"] == 1710000000000
    assert row["market_id"] == "m1"
    assert row["token_id"] == "up-token"
    assert row["outcome"] == "Up"  # outcome_name_for_side maps "up" -> "Up"
    assert row["best_bid"] == 0.48
    assert row["best_ask"] == 0.52
    assert row["best_bid_size"] == 150.0
    assert row["best_ask_size"] == 200.0
    assert row["crypto"] == "BTC"
    assert row["timeframe"] == "5-minute"
