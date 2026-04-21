"""Offline tests for SubgraphTickFetcher.

All tests use canned GraphQL responses injected via ``monkeypatch`` on
``SubgraphClient.query`` — no live Goldsky / Graph Network calls.
"""
from __future__ import annotations

from unittest import mock

from polymarket_pipeline.models import MarketRecord
from polymarket_pipeline.phases.subgraph_ticks import SubgraphTickFetcher
from polymarket_pipeline.subgraph_client import SubgraphClient


def _market(
    market_id: str = "mkt",
    up_id: str = "1111111111111",
    down_id: str = "2222222222222",
) -> MarketRecord:
    return MarketRecord(
        market_id=market_id,
        market_type="binary",
        question="Will BTC be up?",
        timeframe="5-minute",
        crypto="BTC",
        condition_id="cond",
        start_ts=1_700_000_000,
        end_ts=1_700_000_300,
        volume=100.0,
        resolution=-1,
        is_active=True,
        closed_ts=None,
        up_token_id=up_id,
        down_token_id=down_id,
    )


def _stub_client(pages: list[list[dict]]) -> SubgraphClient:
    """Build a client whose ``query`` returns each page in sequence, then empty."""
    client = SubgraphClient(
        primary_url="https://primary.example",
        fallback_url=None,
        request_interval_s=0.0,
        max_retries=1,
    )
    call_log: list[dict] = []

    def fake_query(query: str, variables: dict | None = None) -> dict:
        call_log.append(dict(variables or {}))
        if pages:
            return {"orderFilledEvents": pages.pop(0)}
        return {"orderFilledEvents": []}

    client.query = fake_query  # type: ignore[assignment]
    client.call_log = call_log  # type: ignore[attr-defined]
    return client


# --- Decoder semantics ----------------------------------------------------


def test_decoder_maker_has_outcome_token_emits_sell():
    """maker holding the outcome token → side=SELL (matches legacy RPC path)."""
    m = _market(up_id="0xUP")
    fetcher = SubgraphTickFetcher(_stub_client([]))
    token_to_market = {"0xUP": m}
    event = {
        "id": "0xtx_0xorder",
        "transactionHash": "0xtx",
        "timestamp": "1700000100",
        "orderHash": "0xorder",
        "makerAssetId": "0xUP",           # maker holds shares
        "takerAssetId": "0",               # taker pays USDC
        "makerAmountFilled": "1000000",    # 1.0 share
        "takerAmountFilled": "500000",     # 0.5 USDC
    }
    result = fetcher._decode_fill(event, token_to_market)
    assert result is not None
    mid, row = result
    assert mid == m.market_id
    assert row["side"] == "SELL"
    assert row["token_id"] == "0xUP"
    assert row["outcome"] == "Up"
    assert row["price"] == 0.5
    assert row["size_usdc"] == 0.5
    assert row["tx_hash"] == "0xtx"
    assert row["order_hash"] == "0xorder"
    assert row["source"] == "onchain"
    assert row["block_number"] == 0
    assert row["log_index"] == 0
    assert row["timestamp_ms"] == 1_700_000_100_000


def test_decoder_taker_has_outcome_token_emits_buy():
    m = _market(down_id="0xDOWN")
    fetcher = SubgraphTickFetcher(_stub_client([]))
    token_to_market = {"0xDOWN": m}
    event = {
        "id": "0xtx_0xord2",
        "transactionHash": "0xtx2",
        "timestamp": "1700000200",
        "orderHash": "0xord2",
        "makerAssetId": "0",               # maker pays USDC
        "takerAssetId": "0xDOWN",          # taker holds shares
        "makerAmountFilled": "700000",     # 0.7 USDC
        "takerAmountFilled": "1000000",    # 1.0 share
    }
    result = fetcher._decode_fill(event, token_to_market)
    assert result is not None
    _, row = result
    assert row["side"] == "BUY"
    assert row["token_id"] == "0xDOWN"
    assert row["outcome"] == "Down"
    assert row["price"] == 0.7
    assert row["size_usdc"] == 0.7


def test_decoder_drops_event_for_unknown_token():
    m = _market(up_id="0xUP", down_id="0xDOWN")
    fetcher = SubgraphTickFetcher(_stub_client([]))
    token_to_market = {"0xUP": m, "0xDOWN": m}
    event = {
        "id": "x", "transactionHash": "0x", "timestamp": "1",
        "orderHash": "0xord",
        "makerAssetId": "0xOTHER", "takerAssetId": "0",
        "makerAmountFilled": "1000000", "takerAmountFilled": "500000",
    }
    assert fetcher._decode_fill(event, token_to_market) is None


def test_decoder_rejects_zero_sizes():
    m = _market(up_id="0xUP")
    fetcher = SubgraphTickFetcher(_stub_client([]))
    token_to_market = {"0xUP": m}
    event = {
        "id": "x", "transactionHash": "0x", "timestamp": "1",
        "orderHash": "0x",
        "makerAssetId": "0xUP", "takerAssetId": "0",
        "makerAmountFilled": "0", "takerAmountFilled": "500000",
    }
    assert fetcher._decode_fill(event, token_to_market) is None


def test_decoder_rejects_out_of_range_price():
    m = _market(up_id="0xUP")
    fetcher = SubgraphTickFetcher(_stub_client([]))
    token_to_market = {"0xUP": m}
    event = {
        "id": "x", "transactionHash": "0x", "timestamp": "1",
        "orderHash": "0x",
        "makerAssetId": "0xUP", "takerAssetId": "0",
        "makerAmountFilled": "1000000",      # 1 share
        "takerAmountFilled": "2000000",      # 2 USDC → price = 2.0, invalid
    }
    assert fetcher._decode_fill(event, token_to_market) is None


def test_decoder_rejects_non_numeric_amounts():
    m = _market(up_id="0xUP")
    fetcher = SubgraphTickFetcher(_stub_client([]))
    token_to_market = {"0xUP": m}
    event = {
        "id": "x", "transactionHash": "0x", "timestamp": "1",
        "orderHash": "0x",
        "makerAssetId": "0xUP", "takerAssetId": "0",
        "makerAmountFilled": "garbage", "takerAmountFilled": "500000",
    }
    assert fetcher._decode_fill(event, token_to_market) is None


# --- Pagination ----------------------------------------------------------


def test_paginate_stops_when_page_shorter_than_page_size():
    # Two pages: first full of page_size, second with fewer rows → stop.
    m = _market(up_id="0xUP")
    page1 = [
        {
            "id": f"id{i}", "transactionHash": f"0xtx{i}", "timestamp": "1700000100",
            "orderHash": f"0xord{i}",
            "makerAssetId": "0xUP", "takerAssetId": "0",
            "makerAmountFilled": "1000000", "takerAmountFilled": "500000",
        }
        for i in range(3)
    ]
    page2 = [
        {
            "id": "idZ", "transactionHash": "0xtxZ", "timestamp": "1700000101",
            "orderHash": "0xordZ",
            "makerAssetId": "0xUP", "takerAssetId": "0",
            "makerAmountFilled": "1000000", "takerAmountFilled": "500000",
        }
    ]
    client = _stub_client([page1, page2])
    fetcher = SubgraphTickFetcher(client, page_size=3)
    result = fetcher.get_ticks_for_markets_batch([m], start_ts=1, end_ts=9999999999)
    assert len(result[m.market_id]) == 4
    # Two queries per window (maker-filter + taker-filter).  Maker runs
    # twice (page1 full → continue, page2 shorter → stop); taker runs
    # once (empty → stop).  Total = 3.
    assert len(client.call_log) == 3


def test_paginate_cursor_advances_on_last_id():
    m = _market(up_id="0xUP")
    page1 = [
        {"id": "a1", "transactionHash": "x", "timestamp": "1700000100",
         "orderHash": "o1", "makerAssetId": "0xUP", "takerAssetId": "0",
         "makerAmountFilled": "1000000", "takerAmountFilled": "500000"},
        {"id": "a2", "transactionHash": "x", "timestamp": "1700000100",
         "orderHash": "o2", "makerAssetId": "0xUP", "takerAssetId": "0",
         "makerAmountFilled": "1000000", "takerAmountFilled": "500000"},
    ]
    client = _stub_client([page1])
    fetcher = SubgraphTickFetcher(client, page_size=2)
    fetcher.get_ticks_for_markets_batch([m], start_ts=1, end_ts=2)
    # First call: lastId="" (initial).  Second call: lastId="a2".
    assert client.call_log[0]["lastId"] == ""
    assert client.call_log[1]["lastId"] == "a2"


def test_empty_markets_returns_empty_dict_without_query():
    client = _stub_client([])
    fetcher = SubgraphTickFetcher(client)
    assert fetcher.get_ticks_for_markets_batch([], 0, 1) == {}
    assert len(client.call_log) == 0


# --- Filtering + grouping ------------------------------------------------


def test_ticks_grouped_by_market_id_with_filtering():
    m1 = _market(market_id="m1", up_id="0xUP_A", down_id="0xDOWN_A")
    m2 = _market(market_id="m2", up_id="0xUP_B", down_id="0xDOWN_B")
    page = [
        {"id": "1", "transactionHash": "x", "timestamp": "100",
         "orderHash": "o1", "makerAssetId": "0xUP_A", "takerAssetId": "0",
         "makerAmountFilled": "1000000", "takerAmountFilled": "500000"},
        {"id": "2", "transactionHash": "x", "timestamp": "101",
         "orderHash": "o2", "makerAssetId": "0xUP_B", "takerAssetId": "0",
         "makerAmountFilled": "1000000", "takerAmountFilled": "300000"},
        {"id": "3", "transactionHash": "x", "timestamp": "102",
         "orderHash": "o3", "makerAssetId": "0xIRRELEVANT", "takerAssetId": "0",
         "makerAmountFilled": "1000000", "takerAmountFilled": "500000"},
    ]
    client = _stub_client([page])
    fetcher = SubgraphTickFetcher(client, page_size=3)
    result = fetcher.get_ticks_for_markets_batch([m1, m2], 0, 999)
    assert len(result["m1"]) == 1
    assert len(result["m2"]) == 1
    assert result["m1"][0]["price"] == 0.5
    assert result["m2"][0]["price"] == 0.3


def test_spot_price_joined_when_lookup_provided():
    m = _market(up_id="0xUP")
    page = [{
        "id": "1", "transactionHash": "x", "timestamp": "1700000100",
        "orderHash": "o", "makerAssetId": "0xUP", "takerAssetId": "0",
        "makerAmountFilled": "1000000", "takerAmountFilled": "500000",
    }]
    client = _stub_client([page])
    fetcher = SubgraphTickFetcher(client, page_size=1)

    # Fake SpotPriceLookup-shaped object
    lookup = mock.MagicMock()
    lookup.get.return_value = (67_234.5, 1_700_000_099_000)
    fetcher.spot_price_lookup = lookup

    result = fetcher.get_ticks_for_markets_batch([m], 0, 9999999999)
    row = result[m.market_id][0]
    assert row["spot_price_usdt"] == 67_234.5
    assert row["spot_price_ts_ms"] == 1_700_000_099_000
    lookup.get.assert_called_once_with("BTC", 1_700_000_100_000)


def test_order_hash_is_populated_on_every_row():
    m = _market(up_id="0xUP")
    page = [{
        "id": "1", "transactionHash": "x", "timestamp": "100",
        "orderHash": "0xfeedfacecafebeef",
        "makerAssetId": "0xUP", "takerAssetId": "0",
        "makerAmountFilled": "1000000", "takerAmountFilled": "500000",
    }]
    fetcher = SubgraphTickFetcher(_stub_client([page]), page_size=1)
    result = fetcher.get_ticks_for_markets_batch([m], 0, 9999)
    assert result[m.market_id][0]["order_hash"] == "0xfeedfacecafebeef"
