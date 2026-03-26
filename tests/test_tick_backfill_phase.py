import logging

from polymarket_pipeline.models import MarketRecord
from polymarket_pipeline.phases.shared import PipelinePaths, build_binary_tick_row
from polymarket_pipeline.phases.tick_backfill import TickBackfillPhase


class DummyTickProvider:
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
                build_binary_tick_row(
                    market,
                    timestamp_ms=end_ts * 1000,
                    token_id=market.up_token_id,
                    outcome_side="up",
                    trade_side="BUY",
                    price=0.6,
                    size_usdc=5.0,
                    tx_hash=f"0x{market.market_id}",
                    block_number=123,
                    log_index=0,
                    source="onchain",
                )
            ]
            for market in markets
        }


def _market(market_id: str, *, start_ts: int, end_ts: int) -> MarketRecord:
    return MarketRecord(
        market_id=market_id,
        market_type="crypto-up-down",
        question=f"{market_id} question",
        timeframe="5-minute",
        crypto="BTC",
        condition_id=None,
        start_ts=start_ts,
        end_ts=end_ts,
        up_token_id=f"{market_id}-up",
        down_token_id=f"{market_id}-down",
        up_outcome="Up",
        down_outcome="Down",
        volume=1.0,
        resolution=None,
        is_active=False,
    )


def test_tick_backfill_phase_chunks_large_windows(tmp_path, monkeypatch) -> None:
    provider = DummyTickProvider()
    phase = TickBackfillPhase(
        provider,
        logger=logging.getLogger("test"),
        paths=PipelinePaths.from_root(tmp_path / "data"),
    )

    # Market with 4-day timeframe (345,600 seconds)
    m1 = MarketRecord(
        market_id="m-large",
        market_type="multi-outcome",
        question="Large market",
        timeframe="4-day",
        crypto="ELON-TWEETS",
        condition_id=None,
        start_ts=0,
        end_ts=345600,
        volume=1.0,
        resolution=None,
        is_active=False,
        category="culture",
        tokens={"Outcome 1": "tok1", "Outcome 2": "tok2"},
    )

    phase.run([m1])

    # 345600 / 21600 (6 hours) = 16 chunks
    assert len(provider.calls) == 16
    # Verify first and last chunk
    assert provider.calls[0] == (("m-large",), 0, 21600)
    assert provider.calls[-1] == (("m-large",), 324000, 345600)
