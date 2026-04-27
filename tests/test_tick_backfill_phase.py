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
        market_type="crypto-up-down",
        question="Large market",
        timeframe="4-day",
        crypto="BTC",
        condition_id=None,
        start_ts=0,
        end_ts=345600,
        volume=1.0,
        resolution=None,
        is_active=False,
        category="crypto",
        up_token_id="tok1",
        down_token_id="tok2",
        up_outcome="Up",
        down_outcome="Down",
    )

    phase.run([m1])

    # 345600 / 21600 (6 hours) = 16 chunks
    assert len(provider.calls) == 16
    # Chunks are half-open at the boundary: each subsequent chunk
    # advances ``current_start = current_end + 1`` so that subgraph
    # queries with inclusive ``timestamp_gte`` / ``timestamp_lte``
    # don't double-fetch fills landing exactly on a chunk boundary.
    assert provider.calls[0] == (("m-large",), 0, 21600)
    assert provider.calls[1] == (("m-large",), 21601, 43201)
    # 16th chunk: 15 prior chunks each advanced 21601s → start = 15*21601
    assert provider.calls[-1] == (("m-large",), 15 * 21601, 345600)
    # Boundaries strictly disjoint: each chunk's start is past the previous end
    for prev, nxt in zip(provider.calls, provider.calls[1:]):
        assert nxt[1] == prev[2] + 1


# ---------------------------------------------------------------------------
# Error-path coverage: provider failures, edge-case timestamps
# ---------------------------------------------------------------------------


class _RaisingProvider:
    """Provider that raises on the first call, then succeeds."""

    def __init__(self) -> None:
        self.call_count = 0

    def get_ticks_for_markets_batch(self, markets, start_ts, end_ts):
        self.call_count += 1
        if self.call_count == 1:
            raise RuntimeError("simulated subgraph timeout")
        return {m.market_id: [] for m in markets}


def test_tick_backfill_phase_continues_after_provider_exception(tmp_path, caplog) -> None:
    """A single window/provider failure must not crash the entire run.
    The phase should log the error and continue with the next window.
    """
    provider = _RaisingProvider()
    phase = TickBackfillPhase(
        provider,
        logger=logging.getLogger("test"),
        paths=PipelinePaths.from_root(tmp_path / "data"),
    )
    # Two markets so the run has multiple windows.
    m1 = _market("m1", start_ts=0, end_ts=300)
    m2 = _market("m2", start_ts=400, end_ts=700)

    with caplog.at_level(logging.ERROR, logger="test"):
        phase.run([m1, m2])

    # The provider was called more than once: the first failure didn't
    # abort the loop.
    assert provider.call_count >= 2


def test_tick_backfill_phase_handles_empty_market_list(tmp_path) -> None:
    """An empty market list must complete cleanly (no provider calls,
    no exceptions).
    """
    provider = DummyTickProvider()
    phase = TickBackfillPhase(
        provider,
        logger=logging.getLogger("test"),
        paths=PipelinePaths.from_root(tmp_path / "data"),
    )
    phase.run([])
    assert provider.calls == []


def test_tick_backfill_phase_handles_zero_duration_market(tmp_path) -> None:
    """A market with ``start_ts == end_ts`` must terminate (not loop)
    and either skip or emit a single trivially-bounded window.
    """
    provider = DummyTickProvider()
    phase = TickBackfillPhase(
        provider,
        logger=logging.getLogger("test"),
        paths=PipelinePaths.from_root(tmp_path / "data"),
    )
    m = _market("m-zero", start_ts=1000, end_ts=1000)
    phase.run([m])
    # The phase must either skip the market entirely (0 calls) or emit
    # exactly one window — not loop forever.  Either is acceptable
    # behaviour; what matters is termination + bounded call count.
    assert len(provider.calls) <= 1


def test_tick_backfill_phase_handles_inverted_market_window(tmp_path) -> None:
    """A market with ``start_ts > end_ts`` (corrupt input) must not
    crash; the phase should skip it gracefully.
    """
    provider = DummyTickProvider()
    phase = TickBackfillPhase(
        provider,
        logger=logging.getLogger("test"),
        paths=PipelinePaths.from_root(tmp_path / "data"),
    )
    m = _market("m-bad", start_ts=2000, end_ts=1000)  # inverted!
    # Phase must terminate (not loop forever) and not crash.
    phase.run([m])
