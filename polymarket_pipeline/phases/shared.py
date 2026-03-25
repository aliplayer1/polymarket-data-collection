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

    @classmethod
    def from_root(cls, root: str | Path) -> "PipelinePaths":
        data_dir = Path(root)
        return cls(
            data_dir=data_dir,
            markets_path=data_dir / "markets.parquet",
            prices_dir=data_dir / "prices",
            ticks_dir=data_dir / "ticks",
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
            "condition_id": market.condition_id,
            "up_token_id": market.up_token_id,
            "down_token_id": market.down_token_id,
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
        "condition_id": market.condition_id,
        "up_token_id": market.up_token_id,
        "down_token_id": market.down_token_id,
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
        "category": market.category,
    }
