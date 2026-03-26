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


def test_normalize_gamma_market_avoids_substring_false_positives() -> None:
    # "synthetic" contains "eth", but it should NOT match the ETH asset alias "eth"
    # because of the word boundary regex in _contains_alias.
    raw_market = {
        "id": "m4",
        "question": "Will Synthetic Assets be up or down at 5PM ET?",
        "startDate": "2026-03-08T10:00:00Z",
        "endDate": "2026-03-08T10:05:00Z",
        "closed": True,
        "tokens": [
            {"outcome": "Up", "tokenId": "tok-up"},
            {"outcome": "Down", "tokenId": "tok-down"},
        ],
    }

    market = normalize_gamma_market(raw_market, is_active=False, logger=logging.getLogger("test"))

    # Should be None because although it has "up or down", it does NOT have a valid asset (BTC/ETH/SOL)
    assert market is None


def test_normalize_gamma_market_matches_exact_asset_word() -> None:
    # "ETH" as a standalone word SHOULD match.
    raw_market = {
        "id": "m5",
        "question": "Will ETH be up or down at 5PM ET?",
        "startDate": "2026-03-08T10:00:00Z",
        "endDate": "2026-03-08T10:05:00Z",
        "closed": True,
        "tokens": [
            {"outcome": "Up", "tokenId": "tok-up"},
            {"outcome": "Down", "tokenId": "tok-down"},
        ],
    }

    market = normalize_gamma_market(raw_market, is_active=False, logger=logging.getLogger("test"))

    assert market is not None
    assert market.crypto == "ETH"


def test_normalize_gamma_market_supports_new_assets() -> None:
    assets_to_test = [
        ("BNB Up or Down - March 27, 10AM ET", "BNB", "1-hour"),
        ("Hyperliquid Up or Down - March 26, 1:30PM-1:35PM ET", "HYPE", "5-minute"),
        ("XRP Up or Down - March 26, 2:05PM-2:10PM ET", "XRP", "5-minute"),
        ("Dogecoin Up or Down - March 26, 4:00AM-4:15AM ET", "DOGE", "15-minute"),
        ("HYPE Up or Down - March 26, 10PM ET", "HYPE", "1-hour"),
    ]

    for question, expected_crypto, expected_timeframe in assets_to_test:
        raw_market = {
            "id": "m_test",
            "question": question,
            "startDate": "2026-03-26T00:00:00Z",
            "endDate": "2026-03-26T01:00:00Z",
            "closed": True,
            "tokens": [
                {"outcome": "Up", "tokenId": "tok-up"},
                {"outcome": "Down", "tokenId": "tok-down"},
            ],
        }
        market = normalize_gamma_market(raw_market, is_active=False, logger=logging.getLogger("test"))
        assert market is not None, f"Failed to parse: {question}"
        assert market.crypto == expected_crypto
        assert market.timeframe == expected_timeframe


def test_normalize_gamma_market_supports_elon_musk_tweets() -> None:
    # 7-day range (March 27 to April 3 = 7 days = 10080 minutes)
    raw_market = {
        "id": "elon-1",
        "question": "Will Elon Musk post 20-39 tweets from March 27 to April 3, 2026?",
        "startDate": "2026-03-20T00:00:00Z",
        "endDate": "2026-04-03T00:00:00Z",
        "closed": False,
        "tokens": [
            {"outcome": "20-39", "tokenId": "tok-1"},
            {"outcome": "Other", "tokenId": "tok-2"},
        ],
    }
    market = normalize_gamma_market(raw_market, is_active=True, logger=logging.getLogger("test"))
    assert market is not None
    assert market.market_type == "elon-musk-tweets"
    assert market.crypto == "ELON-TWEETS"
    assert market.timeframe == "7-day"

    # Monthly range
    raw_market_monthly = {
        "id": "elon-2",
        "question": "Will Elon Musk post 1120-1159 tweets in April 2026?",
        "startDate": "2026-03-20T00:00:00Z",
        "endDate": "2026-04-30T00:00:00Z",
        "closed": False,
        "tokens": [
            {"outcome": "1120-1159", "tokenId": "tok-1"},
            {"outcome": "Other", "tokenId": "tok-2"},
        ],
    }
    market_monthly = normalize_gamma_market(raw_market_monthly, is_active=True, logger=logging.getLogger("test"))
    assert market_monthly is not None
    assert market_monthly.market_type == "elon-musk-tweets"
    assert market_monthly.crypto == "ELON-TWEETS"
    assert market_monthly.timeframe == "1-month"
