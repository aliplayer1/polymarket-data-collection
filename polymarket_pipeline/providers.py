from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Protocol

from py_clob_client.client import ClobClient

from .models import MarketRecord


class MarketProvider(Protocol):
    def fetch_markets(
        self,
        *,
        active: bool = False,
        closed: bool = False,
        end_ts_min: int | None = None,
        end_ts_max: int | None = None,
    ) -> Iterator[MarketRecord]: ...


class PriceHistoryProvider(Protocol):
    def fetch_price_history(
        self,
        token_id: str,
        start_ts: int,
        end_ts: int,
        fidelity: int = 1,
    ) -> list[dict[str, Any]]: ...

    def fetch_fee_rate_bps(self, token_id: str) -> int | None: ...


class MarketDataProvider(MarketProvider, PriceHistoryProvider, Protocol):
    pass


class LastTradePriceProvider(Protocol):
    def get_last_trade_price(self, token_id: str) -> Any: ...


class TickBatchProvider(Protocol):
    def get_ticks_for_markets_batch(
        self,
        markets: list[MarketRecord],
        start_ts: int,
        end_ts: int,
    ) -> dict[str, list[dict[str, Any]]]: ...


@dataclass
class ClobLastTradePriceProvider:
    client: ClobClient

    def get_last_trade_price(self, token_id: str) -> Any:
        return self.client.get_last_trade_price(token_id)
