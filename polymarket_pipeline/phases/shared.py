from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from ..models import MarketRecord


@dataclass(frozen=True)
class PipelinePaths:
    data_dir: Path
    markets_path: Path
    prices_dir: Path
    ticks_dir: Path
    spot_prices_dir: Path
    orderbook_dir: Path
    heartbeats_dir: Path

    @classmethod
    def from_root(cls, root: str | Path) -> "PipelinePaths":
        data_dir = Path(root)
        return cls(
            data_dir=data_dir,
            markets_path=data_dir / "markets.parquet",
            prices_dir=data_dir / "prices",
            ticks_dir=data_dir / "ticks",
            spot_prices_dir=data_dir / "spot_prices",
            orderbook_dir=data_dir / "orderbook",
            heartbeats_dir=data_dir / "heartbeats",
        )

    def ensure_data_dir(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def scan_checkpoint_path(self) -> Path:
        return self.data_dir / ".scan_checkpoint"


def _validated_side_prices(side_prices: Mapping[str, Any]) -> tuple[Any, Any]:
    if "up" not in side_prices or "down" not in side_prices:
        raise ValueError("side_prices must contain 'up' and 'down' entries")
    return side_prices["up"], side_prices["down"]


def build_binary_price_frame(
    market: MarketRecord,
    *,
    timestamps: Any,
    side_prices: Mapping[str, Any],
    volume: float,
    resolution: int | None,
    question: str,
) -> pd.DataFrame:
    up_price, down_price = _validated_side_prices(side_prices)
    return pd.DataFrame(
        {
            "market_id": market.market_id,
            "crypto": market.crypto,
            "timeframe": market.timeframe,
            "timestamp": timestamps,
            "up_price": up_price,
            "down_price": down_price,
            "volume": volume,
            "resolution": resolution,
            "question": question,
            "start_ts": market.start_ts,
            "end_ts": market.end_ts,
            "closed_ts": market.closed_ts if market.closed_ts is not None else 0,
            "condition_id": market.condition_id,
            "up_token_id": market.up_token_id,
            "down_token_id": market.down_token_id,
            "slug": market.slug or "",
            "fee_rate_bps": market.fee_rate_bps if market.fee_rate_bps is not None else -1,
            "category": market.category,
        }
    )


def build_binary_price_row(
    market: MarketRecord,
    *,
    timestamp: int,
    side_prices: Mapping[str, float],
    resolution: int | None,
) -> dict[str, Any]:
    up_price, down_price = _validated_side_prices(side_prices)
    return {
        "market_id": market.market_id,
        "crypto": market.crypto,
        "timeframe": market.timeframe,
        "timestamp": timestamp,
        "up_price": float(up_price),
        "down_price": float(down_price),
        "volume": market.volume,
        "resolution": resolution,
        "question": market.question,
        "start_ts": market.start_ts,
        "end_ts": market.end_ts,
        "closed_ts": market.closed_ts if market.closed_ts is not None else 0,
        "condition_id": market.condition_id,
        "up_token_id": market.up_token_id,
        "down_token_id": market.down_token_id,
        "slug": market.slug or "",
        "fee_rate_bps": market.fee_rate_bps if market.fee_rate_bps is not None else -1,
        "category": market.category,
    }


def build_binary_tick_row(
    market: MarketRecord,
    *,
    timestamp_ms: int,
    token_id: str,
    outcome_side: str,
    trade_side: str,
    price: float,
    size_usdc: float,
    tx_hash: str,
    block_number: int,
    log_index: int,
    source: str,
    spot_price_usdt: float | None = None,
    spot_price_ts_ms: int | None = None,
    local_recv_ts_ns: int | None = None,
) -> dict[str, Any]:
    return {
        "timestamp_ms": timestamp_ms,
        "market_id": market.market_id,
        "crypto": market.crypto,
        "timeframe": market.timeframe,
        "token_id": token_id,
        "outcome": market.outcome_name_for_side(outcome_side),
        "side": trade_side,
        "price": price,
        "size_usdc": size_usdc,
        "tx_hash": tx_hash,
        "block_number": block_number,
        "log_index": log_index,
        "source": source,
        "spot_price_usdt": spot_price_usdt,
        "spot_price_ts_ms": spot_price_ts_ms,
        "local_recv_ts_ns": local_recv_ts_ns,
        "category": market.category,
    }


def build_spot_price_row(
    *,
    ts_ms: int,
    symbol: str,
    price: float,
    source: str,
) -> dict[str, Any]:
    """Build a single row for the spot_prices Parquet table."""
    return {
        "ts_ms": ts_ms,
        "symbol": symbol,
        "price": price,
        "source": source,
    }


def market_record_to_markets_df(market: MarketRecord) -> pd.DataFrame:
    """Serialize a single MarketRecord into a 1-row markets_df.

    Used by the metadata-refresh path in the historical phase: when a market's
    price history is already fully cached, we still want to persist any
    updated market-level metadata (resolution, volume, closed_ts) so that
    events that closed *after* their prices were first captured eventually
    get a populated resolution field.

    Returns a DataFrame whose schema matches what ``split_markets_prices``
    (for crypto) or ``split_culture_markets_prices`` (for culture) would
    produce, minus the grouping overhead. The caller selects the right
    persistence function based on ``market.category``.
    """
    import json as _json

    base: dict[str, Any] = {
        "market_id": market.market_id,
        "question": market.question,
        "crypto": market.crypto,
        "timeframe": market.timeframe,
        "volume": market.volume,
        "resolution": market.resolution,
        "start_ts": market.start_ts,
        "end_ts": market.end_ts,
        "closed_ts": market.closed_ts if market.closed_ts is not None else 0,
        "condition_id": market.condition_id,
        "slug": market.slug or "",
    }

    if market.category == "culture":
        base.update({
            "tokens": _json.dumps(market.tokens),
            "event_slug": market.event_slug or "",
            "bucket_index": market.bucket_index if market.bucket_index is not None else -1,
            "bucket_label": market.bucket_label or "",
        })
    else:
        base.update({
            "up_token_id": market.up_token_id,
            "down_token_id": market.down_token_id,
            "fee_rate_bps": market.fee_rate_bps if market.fee_rate_bps is not None else -1,
        })

    return pd.DataFrame([base])


def build_orderbook_row(
    market: MarketRecord,
    *,
    ts_ms: int,
    token_id: str,
    outcome_side: str,
    best_bid: float,
    best_ask: float,
    best_bid_size: float,
    best_ask_size: float,
    local_recv_ts_ns: int | None = None,
) -> dict[str, Any]:
    """Build a single row for the orderbook Parquet table."""
    return {
        "ts_ms": ts_ms,
        "market_id": market.market_id,
        "crypto": market.crypto,
        "timeframe": market.timeframe,
        "token_id": token_id,
        "outcome": market.outcome_name_for_side(outcome_side),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "best_bid_size": best_bid_size,
        "best_ask_size": best_ask_size,
        "local_recv_ts_ns": local_recv_ts_ns,
    }
