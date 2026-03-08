from dataclasses import dataclass


@dataclass
class MarketRecord:
    market_id: str
    question: str
    timeframe: str
    crypto: str
    condition_id: str | None
    start_ts: int
    end_ts: int
    token1_id: str
    token2_id: str
    outcome1: str
    outcome2: str
    volume: float
    resolution: int | None
    is_active: bool

    @property
    def up_token_id(self) -> str:
        return self.token1_id if ("up" in self.outcome1 or "yes" in self.outcome1) else self.token2_id

    @property
    def down_token_id(self) -> str:
        return self.token2_id if ("down" in self.outcome2 or "no" in self.outcome2) else self.token1_id
