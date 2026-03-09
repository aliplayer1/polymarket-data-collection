import logging

from polymarket_pipeline.market_normalization import normalize_gamma_market


def test_normalize_gamma_market_classifies_reversed_token_order() -> None:
    raw_market = {
        "id": "m1",
        "question": "Bitcoin Up or Down - 5-Minute market",
        "startDate": "2026-03-08T10:00:00Z",
        "endDate": "2026-03-08T10:05:00Z",
        "closed": True,
        "closedTime": "2026-03-08T10:05:00Z",
        "volume": "123.45",
        "tokens": [
            {"outcome": "Down", "tokenId": "tok-down", "winner": False},
            {"outcome": "Up", "tokenId": "tok-up", "winner": True},
        ],
        "resolved": True,
    }

    market = normalize_gamma_market(raw_market, is_active=False, logger=logging.getLogger("test"))

    assert market is not None
    assert market.market_type == "crypto-up-down"
    assert market.crypto == "BTC"
    assert market.timeframe == "5-minute"
    assert market.up_token_id == "tok-up"
    assert market.down_token_id == "tok-down"
    assert market.up_outcome == "Up"
    assert market.down_outcome == "Down"
    assert market.resolution == 1


def test_normalize_gamma_market_supports_outcomes_clob_ids_schema() -> None:
    raw_market = {
        "id": "m2",
        "question": "ETH Up or Down 9:00AM - 9:15AM ET",
        "start_date": "2026-03-08T09:00:00Z",
        "end_date": "2026-03-08T09:15:00Z",
        "closed": True,
        "closedTime": "2026-03-08T09:15:00Z",
        "volume": 0,
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": '["yes-token", "no-token"]',
        "resolved": False,
    }

    market = normalize_gamma_market(raw_market, is_active=False, logger=logging.getLogger("test"))

    assert market is not None
    assert market.crypto == "ETH"
    assert market.timeframe == "15-minute"
    assert market.up_token_id == "yes-token"
    assert market.down_token_id == "no-token"
    assert market.up_outcome == "Yes"
    assert market.down_outcome == "No"


def test_normalize_gamma_market_returns_none_for_unknown_market_family() -> None:
    raw_market = {
        "id": "m3",
        "question": "Will it rain tomorrow?",
        "startDate": "2026-03-08T10:00:00Z",
        "endDate": "2026-03-08T10:05:00Z",
        "closed": True,
        "tokens": [
            {"outcome": "Yes", "tokenId": "tok-yes"},
            {"outcome": "No", "tokenId": "tok-no"},
        ],
    }

    market = normalize_gamma_market(raw_market, is_active=False, logger=logging.getLogger("test"))

    assert market is None
