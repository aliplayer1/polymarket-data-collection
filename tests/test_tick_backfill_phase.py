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


def test_tick_backfill_phase_batches_shared_windows(tmp_path, monkeypatch) -> None:
    provider = DummyTickProvider()
    phase = TickBackfillPhase(
        provider,
        logger=logging.getLogger("test"),
        paths=PipelinePaths.from_root(tmp_path / "data"),
    )
    captured: list[tuple[int, str | None]] = []

    def _capture_append_ticks_only(ticks_df, *, ticks_dir=None, logger=None) -> None:
        captured.append((len(ticks_df), ticks_dir))

    monkeypatch.setattr(
        "polymarket_pipeline.phases.tick_backfill.append_ticks_only",
        _capture_append_ticks_only,
    )

    total_ticks = phase.run(
        [
            _market("m1", start_ts=0, end_ts=300),
            _market("m2", start_ts=0, end_ts=300),
            _market("m3", start_ts=300, end_ts=600),
        ]
    )

    assert total_ticks == 3
    assert provider.calls == [
        (("m1", "m2"), 0, 300),
        (("m3",), 300, 600),
    ]
    assert captured == [(3, str((tmp_path / "data" / "ticks")))]
