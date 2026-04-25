from dataclasses import dataclass, field


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
    volume: float
    resolution: int | None
    is_active: bool
    closed_ts: int | None = None  # closedTime from Gamma API; used for incremental scan cutoff

    # Binary-specific fields (for backwards compatibility)
    up_token_id: str = ""
    down_token_id: str = ""
    up_outcome: str = "Up"
    down_outcome: str = "Down"

    # Multi-outcome support
    tokens: dict[str, str] = field(default_factory=dict) # outcome_name -> token_id
    category: str = "crypto" # "crypto" or "culture"

    # Fee schedule (fetched from CLOB API; None if not yet fetched)
    fee_rate_bps: int | None = None

    # Identity / grouping fields (captured from Gamma API)
    slug: str | None = None            # Polymarket market slug
    event_slug: str | None = None      # Parent event slug (derived by stripping bucket suffix)
    bucket_index: int | None = None    # Polymarket's groupItemThreshold — canonical ordering
    bucket_label: str | None = None    # groupItemTitle — e.g. "280-299", "240+"

    def __post_init__(self):
        if not self.tokens and self.up_token_id and self.down_token_id:
            self.tokens = {
                self.up_outcome: self.up_token_id,
                self.down_outcome: self.down_token_id,
            }

    @property
    def asset(self) -> str:
        return self.crypto

    @property
    def token_ids(self) -> tuple[str, ...]:
        if self.category == "crypto" and self.up_token_id and self.down_token_id:
            return (self.up_token_id, self.down_token_id)
        return tuple(self.tokens.values())

    def token_id_for_side(self, side: str) -> str:
        if self.category == "crypto":
            if side == "up":
                return self.up_token_id
            if side == "down":
                return self.down_token_id
        
        if side in self.tokens:
            return self.tokens[side]
            
        raise ValueError(f"Unknown market side/outcome: {side}")

    def outcome_name_for_side(self, side: str) -> str:
        if self.category == "crypto":
            if side == "up":
                return self.up_outcome
            if side == "down":
                return self.down_outcome
                
        if side in self.tokens:
            return side
            
        raise ValueError(f"Unknown market side/outcome: {side}")

    def side_for_token_id(self, token_id: str) -> str | None:
        """Return the outcome label for ``token_id``, or ``None`` if unknown.

        Callers MUST handle the ``None`` return — a token id that
        doesn't match either side indicates either a stale subscription
        (the market has rolled over but the WS hasn't been refreshed),
        a tokens-mapping bug, or a server-side leak from another shard.
        Silently coercing ``None`` to a default (``"up"`` / first
        outcome / etc.) produces silently mis-labelled rows, which is
        far worse than dropping the message; downstream code should
        skip the message instead.
        """
        if self.category == "crypto":
            if token_id == self.up_token_id:
                return "up"
            if token_id == self.down_token_id:
                return "down"

        for outcome, t_id in self.tokens.items():
            if t_id == token_id:
                return outcome
        return None
