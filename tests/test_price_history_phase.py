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


# ---------------------------------------------------------------------------
# Metadata-refresh-only path (for already-cached closed events)
# ---------------------------------------------------------------------------

def test_process_batch_refreshes_metadata_when_prices_are_cached(tmp_path) -> None:
    """Closed events whose prices are fully cached should still get their
    resolution field backfilled via the metadata-only persist path.

    This is the bug that caused every Elon-tweets market in the HF dataset
    to have resolution=-1: the price-history phase returned early when
    prices were already cached, so the newly-populated ``resolution`` on
    the MarketRecord never made it to parquet.
    """
    import pyarrow.parquet as pq

    data_dir = tmp_path / "data"
    data_culture_dir = tmp_path / "data-culture"
    data_dir.mkdir()

    phase = PriceHistoryPhase(
        DummyPriceHistoryProvider(),
        logger=logging.getLogger("test"),
        paths=PipelinePaths.from_root(data_dir),
    )

    # A culture market that's already been "seen" in a previous run — we
    # simulate this by pre-populating the existing_dfs cache with a row
    # whose timestamp == end_ts (so ``_market_start_ts`` returns end_ts
    # and ``build_market_dataframe`` returns None).
    market = MarketRecord(
        market_id="m-260-279",
        market_type="elon-musk-tweets",
        question="Will Elon Musk post 260-279 tweets from March 31 to April 7, 2026?",
        timeframe="7-day",
        crypto="ELON-TWEETS",
        condition_id="0xabc",
        start_ts=1_743_456_000,
        end_ts=1_744_041_600,
        closed_ts=1_744_041_605,
        volume=12_345.0,
        resolution=1,                  # newly populated — we want this to land in parquet
        is_active=False,
        tokens={"Yes": "tok-yes", "No": "tok-no"},
        category="culture",
        slug="elon-musk-of-tweets-march-31-april-7-260-279",
        event_slug="elon-musk-of-tweets-march-31-april-7",
        bucket_index=13,
        bucket_label="260-279",
    )
    # Pre-seed cache so the phase treats prices as fully fetched
    phase.existing_dfs["7-day"] = pd.DataFrame({
        "market_id": ["m-260-279"],
        "timestamp": [market.end_ts],
    })

    phase.process_market_batch([market])

    # metadata-only path should have written data-culture/markets.parquet
    markets_path = data_culture_dir / "markets.parquet"
    assert markets_path.exists(), "metadata refresh did not create markets.parquet"

    loaded = pq.read_table(markets_path).to_pandas()
    assert len(loaded) == 1
    row = loaded.iloc[0]
    assert row["market_id"] == "m-260-279"
    assert int(row["resolution"]) == 1          # ← the whole point
    assert int(row["bucket_index"]) == 13
    assert row["bucket_label"] == "260-279"
    assert row["event_slug"] == "elon-musk-of-tweets-march-31-april-7"
    assert int(row["closed_ts"]) == 1_744_041_605
