"""SubgraphTickFetcher — drop-in replacement for ``PolygonTickFetcher``.

Implements the ``TickBatchProvider`` protocol so ``TickBackfillPhase``
doesn't know or care that its upstream switched from raw on-chain
``eth_getLogs`` to GraphQL queries against Polymarket's orderbook
subgraph.

Why this exists
---------------
Per-market on-chain backfill via Etherscan / RPC hits daily quotas,
401 expired keys, per-second throttles, and intermittent provider
outages — in production a 13-day window has taken 9+ hours and
routinely stalls.  The subgraph exposes the same ``OrderFilled``
events pre-decoded, with ~200 ms latency per 1000-fill page, no rate
limits at the volume we need, and a single-endpoint failure mode
rather than a five-way RPC fan-out.

Data-parity promises
--------------------
Every field in ``TICKS_SCHEMA`` that the old path produced is produced
here too.  The only differences:

- ``block_number`` — subgraph does not expose this; written as ``0``.
- ``log_index``    — same; written as ``0``.
- ``order_hash``   — new v5 column, unique per fill within a
  transaction, carries the dedup discriminator that ``log_index``
  provided on the old path.
- ``source``       — still ``"onchain"`` (not ``"onchain-subgraph"``)
  so rows from this path are interchangeable with legacy data.  The
  presence of ``order_hash`` distinguishes subgraph-sourced rows when
  auditing.

The ``side`` label matches the existing ``_decode_log`` convention
exactly (maker-perspective labeling despite the docstring's
"taker perspective" claim) — fixing that is a separate migration.
"""
from __future__ import annotations

import logging
from typing import Any

from ..models import MarketRecord
from ..subgraph_client import SubgraphClient
from .binance_history import SpotPriceLookup
from .shared import build_binary_tick_row


# Two separate queries per window (maker-side and taker-side) unioned
# client-side.  We tried consolidating via the subgraph's `or:` operator
# but Goldsky's Postgres backend timed out on the compound query plan
# it produces (statement_timeout) — two simple filters run reliably.
_FILLS_QUERY_MAKER = """
query Fills($first: Int!, $startTs: BigInt!, $endTs: BigInt!, $lastId: ID!, $tokens: [String!]!) {
  orderFilledEvents(
    first: $first
    where: {
      timestamp_gte: $startTs
      timestamp_lte: $endTs
      id_gt: $lastId
      makerAssetId_in: $tokens
    }
    orderBy: id
    orderDirection: asc
  ) {
    id
    transactionHash
    timestamp
    orderHash
    maker
    taker
    makerAssetId
    takerAssetId
    makerAmountFilled
    takerAmountFilled
  }
}
""".strip()

_FILLS_QUERY_TAKER = _FILLS_QUERY_MAKER.replace("makerAssetId_in", "takerAssetId_in")


class SubgraphTickFetcher:
    """Fetches Polymarket trade fills via the orderbook subgraph.

    Constructor parameters match what ``pipeline.py`` injects; the
    ``spot_price_lookup`` attribute is assignable after construction
    (mirroring the ``PolygonTickFetcher`` convention so both providers
    receive the Binance lookup via the same injection point).
    """

    # 6 decimals on both USDC and CTF outcome shares — matches the raw
    # on-chain decoder's scaling.  If Polymarket ever deploys a new
    # exchange with different decimals this constant moves.
    _AMOUNT_DECIMALS: float = 1_000_000.0

    def __init__(
        self,
        client: SubgraphClient,
        *,
        logger: logging.Logger | None = None,
        page_size: int = 1000,
        spot_price_lookup: SpotPriceLookup | None = None,
    ) -> None:
        self.client = client
        self.logger = logger or logging.getLogger("polymarket_pipeline.subgraph_ticks")
        self.page_size = page_size
        # Assigned post-construction by pipeline.py after Binance history phase runs.
        self.spot_price_lookup = spot_price_lookup

    # ------------------------------------------------------------------
    # Public — matches TickBatchProvider protocol
    # ------------------------------------------------------------------

    def get_ticks_for_markets_batch(
        self,
        markets: list[MarketRecord],
        start_ts: int,
        end_ts: int,
    ) -> dict[str, list[dict[str, Any]]]:
        """Return ``{market_id: [tick_row, ...]}`` for fills in ``[start_ts, end_ts]``.

        The subgraph is queried TWICE per window — once filtering by
        ``makerAssetId_in`` and once by ``takerAssetId_in`` — because
        GraphQL's ``where`` clause has no cross-field OR.  Results are
        unioned and deduplicated by ``id``.  Server-side filtering drops
        per-query volume from ~100k fills (full window) to ~dozens.
        """
        result: dict[str, list[dict[str, Any]]] = {m.market_id: [] for m in markets}
        if not markets:
            return result

        token_to_market: dict[str, MarketRecord] = {}
        for market in markets:
            for token_id in market.token_ids:
                if token_id:
                    token_to_market[token_id] = market
        if not token_to_market:
            return result
        token_ids = list(token_to_market.keys())

        # Two queries per window (maker-side + taker-side), unioned by
        # event id client-side.  A fill only appears twice if both assets
        # happen to be in our set — nearly impossible on binary markets
        # where the non-outcome side is always USDC "0".
        seen_ids: set[str] = set()
        events: list[dict[str, Any]] = []
        for query in (_FILLS_QUERY_MAKER, _FILLS_QUERY_TAKER):
            for event in self._paginate_fills(query, start_ts, end_ts, token_ids):
                eid = event.get("id")
                if not eid or eid in seen_ids:
                    continue
                seen_ids.add(eid)
                events.append(event)

        for event in events:
            decoded = self._decode_fill(event, token_to_market)
            if decoded is None:
                continue
            market_id, row = decoded
            result[market_id].append(row)

        # Maintain existing log format so operators see the same "ticks for
        # market X" lines regardless of which provider is active.
        markets_by_id = {m.market_id: m for m in markets}
        for mid, ticks in result.items():
            if not ticks:
                continue
            ticks.sort(key=lambda t: t["timestamp_ms"])
            market = markets_by_id[mid]
            self.logger.info(
                "Subgraph ticks for market %s (%s/%s): %d fills",
                mid, market.crypto, market.timeframe, len(ticks),
            )

        return result

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    # Hard cap on pagination loops per query — guards against the
    # subgraph returning a non-advancing or repeating cursor (bug or
    # corruption) that would otherwise spin indefinitely.  At
    # page_size=1000 this allows up to 1M fills per window, which is
    # vastly more than realistic for a 6-hour CTF chunk.
    _MAX_PAGES_PER_QUERY: int = 1000

    def _paginate_fills(
        self,
        query: str,
        start_ts: int,
        end_ts: int,
        token_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Cursor through ``orderFilledEvents`` using ``id_gt`` until exhausted."""
        fills: list[dict[str, Any]] = []
        last_id = ""
        variables = {
            "first": self.page_size,
            "startTs": str(int(start_ts)),
            "endTs": str(int(end_ts)),
            "tokens": token_ids,
        }
        for page_num in range(self._MAX_PAGES_PER_QUERY):
            variables["lastId"] = last_id
            try:
                data = self.client.query(query, variables=dict(variables))
            except Exception as exc:
                self.logger.error(
                    "Subgraph fill pagination failed at id_gt=%s: %s", last_id, exc,
                )
                raise
            page = data.get("orderFilledEvents") or []
            if not page:
                break
            fills.extend(page)
            # Ordered by id asc, so the last element carries the next cursor.
            new_last_id = page[-1]["id"]
            if last_id and new_last_id <= last_id:
                self.logger.error(
                    "Subgraph cursor not advancing (last=%s, new=%s); aborting",
                    last_id, new_last_id,
                )
                break
            last_id = new_last_id
            if len(page) < self.page_size:
                break
        else:
            self.logger.error(
                "Subgraph pagination exceeded %d pages for window [%s, %s]; "
                "truncating result (some fills may be missing)",
                self._MAX_PAGES_PER_QUERY, start_ts, end_ts,
            )
        return fills

    # ------------------------------------------------------------------
    # Decoding
    # ------------------------------------------------------------------

    def _decode_fill(
        self,
        event: dict[str, Any],
        token_to_market: dict[str, MarketRecord],
    ) -> tuple[str, dict[str, Any]] | None:
        """Convert a GraphQL fill into ``(market_id, tick_row)`` or ``None``.

        ``None`` is returned for fills whose outcome token isn't in our
        target market set, or for degenerate amounts (zero, non-numeric,
        or producing a price outside [0, 1]).  The side-label logic
        matches ``PolygonTickFetcher._decode_log`` byte-for-byte so
        downstream data stays interchangeable.
        """
        maker_asset = str(event.get("makerAssetId", ""))
        taker_asset = str(event.get("takerAssetId", ""))

        market: MarketRecord | None = token_to_market.get(maker_asset)
        if market is not None:
            outcome_side = market.side_for_token_id(maker_asset)
            if outcome_side is None:
                return None
            side = "SELL"
            outcome_amt_raw = event.get("makerAmountFilled")
            usdc_amt_raw = event.get("takerAmountFilled")
            token_id = maker_asset
        else:
            market = token_to_market.get(taker_asset)
            if market is None:
                return None
            outcome_side = market.side_for_token_id(taker_asset)
            if outcome_side is None:
                return None
            side = "BUY"
            outcome_amt_raw = event.get("takerAmountFilled")
            usdc_amt_raw = event.get("makerAmountFilled")
            token_id = taker_asset

        try:
            outcome_amt_int = int(outcome_amt_raw)
            usdc_amt_int = int(usdc_amt_raw)
        except (TypeError, ValueError):
            return None

        outcome_size = outcome_amt_int / self._AMOUNT_DECIMALS
        usdc_size = usdc_amt_int / self._AMOUNT_DECIMALS
        if outcome_size <= 0 or usdc_size <= 0:
            return None
        # Reject impossible prices (USDC paid > outcome shares, which
        # implies > 1.0) using INTEGER comparison so we don't reject a
        # legitimate 1.0 fill due to FP rounding.  Then clamp the
        # resulting price to [0.0, 1.0] to absorb any sub-ULP rounding
        # that survives the integer guard (e.g. 1.0000000000000002).
        if usdc_amt_int > outcome_amt_int:
            return None
        price = usdc_size / outcome_size
        price = max(0.0, min(1.0, price))

        try:
            tick_ts_ms = int(event["timestamp"]) * 1000
        except (TypeError, ValueError, KeyError):
            return None

        spot_kwargs: dict[str, Any] = {}
        if self.spot_price_lookup is not None:
            spot = self.spot_price_lookup.get(market.crypto, tick_ts_ms)
            if spot is not None:
                spot_kwargs["spot_price_usdt"] = spot[0]
                spot_kwargs["spot_price_ts_ms"] = spot[1]

        row = build_binary_tick_row(
            market,
            timestamp_ms=tick_ts_ms,
            token_id=token_id,
            outcome_side=outcome_side,
            trade_side=side,
            price=round(price, 6),
            size_usdc=round(usdc_size, 6),
            tx_hash=str(event.get("transactionHash", "")),
            block_number=0,
            log_index=0,
            source="onchain",
            order_hash=str(event.get("orderHash", "")) or None,
            **spot_kwargs,
        )
        return market.market_id, row
