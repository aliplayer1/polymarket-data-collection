from __future__ import annotations

import logging
from pathlib import Path

import pytest

from polymarket_pipeline.models import MarketRecord
from polymarket_pipeline.pipeline import PolymarketDataPipeline
from polymarket_pipeline.settings import PipelineRunOptions, RuntimeSettings
from polymarket_pipeline.storage import load_markets, load_prices


@pytest.fixture(autouse=True)
def _disable_live_tick_provider(monkeypatch):
    """Force ``PM_BACKFILL_MODE=rpc`` for all pipeline tests.

    Without this, the pipeline constructor defaults to ``mode=subgraph``
    and builds a live ``SubgraphTickFetcher`` — which then attempts
    real network calls during the historical phase of these tests.
    Setting mode to ``rpc`` with no RPC config makes
    ``_build_tick_provider`` return ``None``, cleanly disabling tick
    backfill in isolation tests.  Tests that need a tick provider pass
    one explicitly via ``tick_provider=`` on the pipeline constructor.
    """
    monkeypatch.setenv("PM_BACKFILL_MODE", "rpc")
    yield


class DummyLastTradePriceProvider:
    def get_last_trade_price(self, token_id: str) -> float:
        return 0.5


class FakeApi:
    def __init__(
        self,
        *,
        closed_markets: list[MarketRecord] | None = None,
        active_markets: list[MarketRecord] | None = None,
    ) -> None:
        self.closed_markets = closed_markets or []
        self.active_markets = active_markets or []
        self.closed_calls: list[int | None] = []
        self.active_calls: list[int | None] = []

    def fetch_markets(
        self,
        *,
        active: bool = False,
        closed: bool = False,
        end_ts_min: int | None = None,
    ):
        if closed:
            self.closed_calls.append(end_ts_min)
            markets = self.closed_markets
        elif active:
            self.active_calls.append(end_ts_min)
            markets = self.active_markets
        else:
            markets = []
        for market in markets:
            yield market

    def fetch_price_history(
        self,
        token_id: str,
        start_ts: int,
        end_ts: int,
        fidelity: int = 1,
    ) -> list[dict[str, float | int]]:
        if token_id.endswith("up"):
            return [
                {"t": start_ts, "p": 0.6},
                {"t": start_ts + 100, "p": 0.65},
            ]
        return [
            {"t": start_ts, "p": 0.4},
            {"t": start_ts + 50, "p": 0.35},
        ]

    def fetch_fee_rate_bps(self, token_id: str) -> int | None:
        return 2500


class FakeTickProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], int, int]] = []

    def get_ticks_for_markets_batch(
        self,
        markets: list[MarketRecord],
        start_ts: int,
        end_ts: int,
    ) -> dict[str, list[dict[str, object]]]:
        self.calls.append((tuple(market.market_id for market in markets), start_ts, end_ts))
        return {
            market.market_id: [
                {
                    "market_id": market.market_id,
                    "timestamp_ms": end_ts * 1000,
                    "token_id": market.up_token_id,
                    "outcome": market.up_outcome,
                    "side": "BUY",
                    "price": 0.6,
                    "size_usdc": 10.0,
                    "tx_hash": f"0x{market.market_id}",
                    "block_number": 123,
                    "log_index": 0,
                    "source": "onchain",
                    "crypto": market.crypto,
                    "timeframe": market.timeframe,
                }
            ]
            for market in markets
        }


def _market(market_id: str, *, is_active: bool, start_ts: int = 0, end_ts: int = 2000000000) -> MarketRecord:
    return MarketRecord(
        market_id=market_id,
        market_type="crypto-up-down",
        question="Bitcoin Up or Down - 5-Minute market",
        timeframe="5-minute",
        crypto="BTC",
        condition_id=None,
        start_ts=start_ts,
        end_ts=end_ts,
        up_token_id=f"{market_id}-up",
        down_token_id=f"{market_id}-down",
        up_outcome="Up",
        down_outcome="Down",
        volume=100.0,
        resolution=None,
        is_active=is_active,
    )


def _pipeline(*, api: FakeApi, settings: RuntimeSettings, tick_provider=None) -> PolymarketDataPipeline:
    from polymarket_pipeline.phases.binance_history import SpotPriceLookup
    p = PolymarketDataPipeline(
        api=api,
        client=object(),
        last_trade_price_provider=DummyLastTradePriceProvider(),
        tick_provider=tick_provider,
        logger=logging.getLogger("test"),
        settings=settings,
    )
    # Stub out Binance phase to avoid real HTTP calls during tests
    p.binance_history_phase.run = lambda markets, *, spot_prices_dir: SpotPriceLookup()
    return p


def test_pipeline_test_mode_uses_test_output_and_skips_websocket(tmp_path, monkeypatch) -> None:
    test_output_dir = tmp_path / "test-output"
    monkeypatch.setattr("polymarket_pipeline.settings.PARQUET_TEST_DIR", str(test_output_dir))

    api = FakeApi(closed_markets=[_market("closed-1", is_active=False)])
    pipeline = _pipeline(api=api, settings=RuntimeSettings(data_dir=tmp_path / "prod-data"))
    report_calls: list[str] = []
    websocket_calls: list[list[str]] = []

    async def _record_websocket(markets: list[MarketRecord], run_options=None) -> None:
        websocket_calls.append([market.market_id for market in markets])

    pipeline._print_test_report = lambda: report_calls.append("printed")
    pipeline.run_websocket = _record_websocket

    pipeline.run(run_options=PipelineRunOptions.from_values(test_limit=1))

    assert pipeline.paths.data_dir == Path(test_output_dir)
    assert api.closed_calls == [None]
    assert api.active_calls == []
    assert websocket_calls == []
    assert report_calls == ["printed"]

    markets_df = load_markets(str(test_output_dir / "markets.parquet"))
    prices_df = load_prices(str(test_output_dir / "prices"))
    assert set(markets_df["market_id"]) == {"closed-1"}
    assert prices_df["market_id"].nunique() == 1
    assert sorted(prices_df["timestamp"].tolist()) == [0, 50, 100]


def test_pipeline_websocket_only_fetches_active_markets_and_runs_stream(tmp_path) -> None:
    api = FakeApi(active_markets=[_market("active-1", is_active=True)])
    pipeline = _pipeline(api=api, settings=RuntimeSettings(data_dir=tmp_path / "data"))
    load_calls: list[str] = []
    websocket_calls: list[list[str]] = []
    summary_calls: list[str] = []

    async def _record_websocket(markets: list[MarketRecord], run_options=None) -> None:
        websocket_calls.append([market.market_id for market in markets])

    pipeline.load_existing_data = lambda: load_calls.append("loaded")
    pipeline.run_websocket = _record_websocket
    pipeline.print_summary = lambda: summary_calls.append("printed")

    pipeline.run(run_options=PipelineRunOptions.from_values(websocket_only=True))

    assert api.closed_calls == []
    assert api.active_calls == [None]
    assert load_calls == []
    assert websocket_calls == [["active-1"]]
    assert summary_calls == ["printed"]


def test_pipeline_historical_only_runs_tick_backfill_without_websocket(tmp_path, monkeypatch) -> None:
    api = FakeApi(closed_markets=[_market("closed-1", is_active=False, start_ts=1000000, end_ts=1000300)])
    tick_provider = FakeTickProvider()
    pipeline = _pipeline(
        api=api,
        settings=RuntimeSettings(data_dir=tmp_path / "data"),
        tick_provider=tick_provider,
    )
    websocket_calls: list[list[str]] = []
    summary_calls: list[str] = []
    consolidate_calls: list[str] = []

    async def _record_websocket(markets: list[MarketRecord], run_options=None) -> None:
        websocket_calls.append([market.market_id for market in markets])

    monkeypatch.setattr(
        "polymarket_pipeline.pipeline.consolidate_ticks",
        lambda *, ticks_dir, logger: consolidate_calls.append(ticks_dir),
    )
    pipeline.run_websocket = _record_websocket
    pipeline.print_summary = lambda: summary_calls.append("printed")

    pipeline.run(run_options=PipelineRunOptions.from_values(historical_only=True))

    assert api.closed_calls == [None]
    assert api.active_calls == []
    assert tick_provider.calls == [(("closed-1",), 1000000, 1000300)]
    assert consolidate_calls == [str(tmp_path / "data" / "ticks")]
    assert websocket_calls == []
    assert summary_calls == ["printed"]
