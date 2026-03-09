import logging

import pandas as pd

from polymarket_pipeline.models import MarketRecord
from polymarket_pipeline.phases.price_history import PriceHistoryPhase
from polymarket_pipeline.phases.shared import PipelinePaths


class DummyPriceHistoryProvider:
    def fetch_price_history(
        self,
        token_id: str,
        start_ts: int,
        end_ts: int,
        fidelity: int = 1,
    ) -> list[dict[str, float | int]]:
        if token_id == "up-token":
            return [
                {"t": start_ts, "p": 0.6},
                {"t": start_ts + 100, "p": 0.65},
            ]
        return [
            {"t": start_ts, "p": 0.4},
            {"t": start_ts + 50, "p": 0.35},
        ]


def test_price_history_phase_builds_dataframe_and_updates_cache(tmp_path) -> None:
    phase = PriceHistoryPhase(
        DummyPriceHistoryProvider(),
        logger=logging.getLogger("test"),
        paths=PipelinePaths.from_root(tmp_path / "data"),
    )
    market = MarketRecord(
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
        volume=10.0,
        resolution=None,
        is_active=False,
    )

    df = phase.build_market_dataframe(market)

    assert df is not None
    assert df["timestamp"].tolist() == [0, 50, 100]
    assert df["up_price"].tolist() == [0.6, 0.6, 0.65]
    assert df["down_price"].tolist() == [0.4, 0.35, 0.35]

    phase.persist_dataframe("5-minute", df)

    assert phase.last_cached_prices(market) == {"up": 0.65, "down": 0.35}


def test_market_start_ts_advances_when_cache_exists(tmp_path) -> None:
    phase = PriceHistoryPhase(
        DummyPriceHistoryProvider(),
        logger=logging.getLogger("test"),
        paths=PipelinePaths.from_root(tmp_path / "data"),
    )
    market = MarketRecord(
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
        volume=10.0,
        resolution=None,
        is_active=False,
    )
    phase.existing_dfs["5-minute"] = pd.DataFrame({"market_id": ["m1"], "timestamp": [125]})

    assert phase._market_start_ts(market) == 126
