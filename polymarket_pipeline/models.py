from dataclasses import dataclass


@dataclass
class MarketRecord:
    market_id: str
    market_type: str
    question: str
    timeframe: str
    crypto: str
    condition_id: str | None
    start_ts: int
    end_ts: int
    up_token_id: str
    down_token_id: str
    up_outcome: str
    down_outcome: str
    volume: float
    resolution: int | None
    is_active: bool
    closed_ts: int | None = None  # closedTime from Gamma API; used for incremental scan cutoff

    @property
    def asset(self) -> str:
        return self.crypto

    @property
    def token_ids(self) -> tuple[str, str]:
        return (self.up_token_id, self.down_token_id)

    def token_id_for_side(self, side: str) -> str:
        if side == "up":
            return self.up_token_id
        if side == "down":
            return self.down_token_id
        raise ValueError(f"Unknown market side: {side}")

    def outcome_name_for_side(self, side: str) -> str:
        if side == "up":
            return self.up_outcome
        if side == "down":
            return self.down_outcome
        raise ValueError(f"Unknown market side: {side}")

    def side_for_token_id(self, token_id: str) -> str | None:
        if token_id == self.up_token_id:
            return "up"
        if token_id == self.down_token_id:
            return "down"
        return None
