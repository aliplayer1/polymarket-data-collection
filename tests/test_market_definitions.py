import json

import pytest

from polymarket_pipeline.markets import MarketDefinitionError, load_market_definitions


def test_load_market_definitions_from_json(tmp_path) -> None:
    definitions_path = tmp_path / "market_definitions.json"
    definitions_path.write_text(
        json.dumps(
            {
                "version": 1,
                "definitions": [
                    {
                        "key": "doge-up-down",
                        "question_keywords": ["up or down"],
                        "asset_aliases": {"DOGE": ["dogecoin", "doge"]},
                        "up_outcome_aliases": ["yes"],
                        "down_outcome_aliases": ["no"],
                        "timeframes": [
                            {
                                "name": "1-hour",
                                "seconds": 3600,
                                "question_aliases": ["1 hour", "hourly"],
                                "cli_aliases": ["1h", "1hr", "1-hour"],
                                "range_minutes": 60,
                                "matches_single_hour_pattern": True,
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    definitions = load_market_definitions(definitions_path)
    definition = definitions[0]

    assert definition.key == "doge-up-down"
    assert definition.extract_asset("Will Dogecoin be Up or Down?") == "DOGE"
    assert definition.extract_timeframe("Dogecoin Up or Down 10:00AM - 11:00AM ET") == "1-hour"
    classified = definition.classify_outcomes([("Yes", "yes-token"), ("No", "no-token")])
    assert classified is not None
    assert classified.up_token_id == "yes-token"
    assert classified.down_token_id == "no-token"


def test_load_market_definitions_reject_invalid_payload(tmp_path) -> None:
    definitions_path = tmp_path / "invalid_market_definitions.json"
    definitions_path.write_text(json.dumps({"definitions": []}), encoding="utf-8")

    with pytest.raises(MarketDefinitionError):
        load_market_definitions(definitions_path)
