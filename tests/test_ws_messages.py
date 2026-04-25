"""Unit tests for polymarket_pipeline.phases.ws_messages."""
from __future__ import annotations

import json
import math

from polymarket_pipeline.phases.ws_messages import (
    clob_subscribe_payload,
    is_valid_bbo,
    parse_book_snapshot,
    parse_last_trade_price,
    parse_price_change,
    parse_rtds_update,
    rtds_subscribe_payload,
)


# --- parse_price_change ----------------------------------------------------


def test_price_change_flattens_updates() -> None:
    event = {
        "event_type": "price_change",
        "price_changes": [
            {"asset_id": "0xA", "best_bid": "0.49", "best_ask": "0.51"},
            {"asset_id": "0xB", "best_bid_size": "100"},
        ],
    }
    out = parse_price_change(event)
    assert len(out) == 2
    assert out[0].asset_id == "0xA" and out[0].best_bid == 0.49
    # Partial update — fields the server didn't touch are None, not zero.
    assert out[1].asset_id == "0xB"
    assert out[1].best_bid is None and out[1].best_ask is None
    assert out[1].best_bid_size == 100.0


def test_price_change_skips_entries_without_asset_id() -> None:
    event = {"price_changes": [{"best_bid": "0.5"}, {"asset_id": "0xA"}]}
    out = parse_price_change(event)
    assert len(out) == 1 and out[0].asset_id == "0xA"


def test_price_change_empty_and_malformed() -> None:
    assert parse_price_change({}) == []
    assert parse_price_change({"price_changes": None}) == []
    assert parse_price_change({"price_changes": ["not-a-dict"]}) == []


# --- parse_last_trade_price -----------------------------------------------


def test_trade_parses_valid() -> None:
    event = {
        "asset_id": "0xA", "timestamp": 1_700_000_000_000,
        "price": "0.5", "size": "100", "side": "SELL", "hash": "0xTX",
    }
    trade = parse_last_trade_price(event)
    assert trade is not None
    assert trade.timestamp_ms == 1_700_000_000_000
    assert trade.price == 0.5
    assert trade.size_shares == 100.0
    assert trade.trade_side == "SELL"
    assert trade.tx_hash == "0xTX"


def test_trade_rejects_out_of_range_price() -> None:
    assert parse_last_trade_price({"asset_id": "0xA", "timestamp": 1, "price": 2.0}) is None
    assert parse_last_trade_price({"asset_id": "0xA", "timestamp": 1, "price": -0.1}) is None


def test_trade_rejects_missing_required() -> None:
    assert parse_last_trade_price({"timestamp": 1, "price": 0.5}) is None  # no asset_id
    assert parse_last_trade_price({"asset_id": "0xA", "price": 0.5}) is None  # no timestamp
    assert parse_last_trade_price({"asset_id": "0xA", "timestamp": 0, "price": 0.5}) is None


def test_trade_normalises_invalid_side_to_buy() -> None:
    event = {"asset_id": "0xA", "timestamp": 1, "price": 0.5, "side": "INVALID"}
    trade = parse_last_trade_price(event)
    assert trade and trade.trade_side == "BUY"


def test_trade_defaults_missing_size_to_zero() -> None:
    event = {"asset_id": "0xA", "timestamp": 1, "price": 0.5}
    trade = parse_last_trade_price(event)
    assert trade and trade.size_shares == 0.0


# --- parse_book_snapshot ---------------------------------------------------


def test_book_parses_valid() -> None:
    book = parse_book_snapshot({
        "asset_id": "0xA",
        "bids": [{"price": "0.49", "size": "100"}],
        "asks": [{"price": "0.51", "size": "200"}],
    })
    assert book and book.best_bid == 0.49 and book.best_ask == 0.51
    assert book.best_bid_size == 100 and book.best_ask_size == 200


def test_book_handles_empty_sides() -> None:
    book = parse_book_snapshot({"asset_id": "0xA", "bids": [], "asks": []})
    assert book and book.best_bid == 0.0 and book.best_ask == 0.0


def test_book_missing_asset_id_returns_none() -> None:
    assert parse_book_snapshot({"bids": []}) is None


# --- is_valid_bbo ----------------------------------------------------------


def test_is_valid_bbo_accepts_healthy_book() -> None:
    assert is_valid_bbo(0.49, 0.51, 100, 200)


def test_is_valid_bbo_rejects_empty_sides() -> None:
    assert not is_valid_bbo(0.0, 0.51, 100, 200)
    assert not is_valid_bbo(0.49, 0.0, 100, 200)


def test_is_valid_bbo_rejects_inverted() -> None:
    assert not is_valid_bbo(0.51, 0.51, 100, 200)
    assert not is_valid_bbo(0.55, 0.50, 100, 200)


def test_is_valid_bbo_rejects_non_finite() -> None:
    assert not is_valid_bbo(math.nan, 0.51, 100, 200)
    assert not is_valid_bbo(0.49, math.inf, 100, 200)


def test_is_valid_bbo_rejects_above_one() -> None:
    assert not is_valid_bbo(0.49, 1.01, 100, 200)


def test_is_valid_bbo_rejects_zero_sizes() -> None:
    """``parse_book_snapshot`` collapses missing-side entries to
    ``best_bid_size = 0.0``; without a guard those one-sided snapshots
    flowed into the orderbook table as if the BBO were valid.  Reject
    any zero or negative size.
    """
    assert not is_valid_bbo(0.49, 0.51, 0.0, 200.0)
    assert not is_valid_bbo(0.49, 0.51, 100.0, 0.0)
    assert not is_valid_bbo(0.49, 0.51, -1.0, 200.0)


# --- parse_rtds_update -----------------------------------------------------


def test_rtds_update_parses_valid() -> None:
    msg = json.dumps({
        "type": "update", "topic": "crypto_prices",
        "payload": {"symbol": "BTCUSDT", "value": 67234.5, "timestamp": 1_700_000_000_000},
    })
    u = parse_rtds_update(msg)
    assert u and u.symbol == "btcusdt"  # lower-cased
    assert u.price == 67234.5
    assert u.topic == "crypto_prices"


def test_rtds_update_ignores_pong_and_nonupdate() -> None:
    assert parse_rtds_update("PONG") is None
    assert parse_rtds_update(b"PONG") is None
    assert parse_rtds_update('{"type":"subscribed"}') is None
    assert parse_rtds_update("not json") is None


def test_rtds_update_rejects_out_of_range_price() -> None:
    bad_negative = json.dumps({
        "type": "update", "topic": "crypto_prices",
        "payload": {"symbol": "x", "value": -1, "timestamp": 1},
    })
    assert parse_rtds_update(bad_negative) is None
    bad_exponent = json.dumps({
        "type": "update", "topic": "crypto_prices",
        "payload": {"symbol": "x", "value": 1e15, "timestamp": 1},
    })
    assert parse_rtds_update(bad_exponent) is None


# --- Subscription payloads -------------------------------------------------


def test_clob_subscribe_is_safe_json() -> None:
    # Even adversarial IDs end up as plain strings inside the JSON array
    # (no interpolation), which is the injection-safety property we want.
    payload = clob_subscribe_payload(["0xAbC", '"evil"; DROP TABLE'])
    parsed = json.loads(payload)
    assert parsed["type"] == "market"
    assert parsed["custom_feature_enabled"] is True
    assert parsed["assets_ids"] == ["0xAbC", '"evil"; DROP TABLE']


def test_rtds_subscribe_builds_single_topic() -> None:
    parsed = json.loads(rtds_subscribe_payload("crypto_prices"))
    assert parsed["action"] == "subscribe"
    assert parsed["subscriptions"] == [{"topic": "crypto_prices", "type": "*"}]
