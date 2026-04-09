from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Sequence

from .markets import MarketDefinition, BinaryMarketDefinition, MultiOutcomeMarketDefinition, get_matching_market_definition, get_market_definitions
from .models import MarketRecord
from .parsing import parse_iso_timestamp


def _coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _extract_all_outcomes(market: dict[str, Any]) -> list[tuple[str, str]]:
    tokens = market.get("tokens", [])
    if tokens:
        extracted = [
            (
                str(token.get("outcome", "")).strip(),
                str(token.get("token_id") or token.get("tokenId") or "").strip(),
            )
            for token in tokens
        ]
        if all(outcome and token_id for outcome, token_id in extracted):
            return extracted

    outcomes = _coerce_list(market.get("outcomes"))
    clob_token_ids = _coerce_list(market.get("clobTokenIds"))
    if outcomes and clob_token_ids and len(outcomes) == len(clob_token_ids):
        extracted = [
            (str(out).strip(), str(tid).strip())
            for out, tid in zip(outcomes, clob_token_ids)
        ]
        if all(outcome and token_id for outcome, token_id in extracted):
            return extracted

    return []


def _extract_binary_outcomes(market: dict[str, Any]) -> list[tuple[str, str]] | None:
    extracted = _extract_all_outcomes(market)
    if len(extracted) == 2:
        return extracted
    return None


def _extract_winning_outcome(market: dict[str, Any]) -> str | None:
    for token in market.get("tokens", []):
        if token.get("winner", False):
            return str(token.get("outcome", "")).strip()
    return None


def _is_market_resolved(market: dict[str, Any]) -> bool:
    """Detect whether a market has a determined outcome.

    Gamma does not expose a ``resolved`` boolean field. A market is treated
    as resolved when both:

      * ``closed`` is True, and
      * one outcome in ``outcomePrices`` is effectively 1.0 (>= 0.99),
        meaning the market has a clear winner.

    This catches both on-chain finalised markets and UMA-proposed markets
    that have already converged in price. It deliberately does not wait
    for on-chain finalisation because, for our analytics use case, the
    winning outcome is already unambiguous.
    """
    if not market.get("closed", False):
        return False
    prices = _coerce_list(market.get("outcomePrices"))
    for p in prices:
        try:
            if float(p) >= 0.99:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _binary_resolution_from_prices(market: dict[str, Any]) -> int | None:
    """Return 0 / 1 / None for a resolved market's YES side.

    For multi-outcome (culture) markets, each bucket is its own binary
    YES/NO sub-market; a bucket "resolved with resolution=1" means that
    bucket was the winner of the parent event. For binary markets (crypto
    up/down), resolution=1 means the "Up" / "Yes" token won.
    """
    if not _is_market_resolved(market):
        return None
    prices = _coerce_list(market.get("outcomePrices"))
    if not prices:
        return None
    try:
        yes_price = float(prices[0])
    except (TypeError, ValueError):
        return None
    return 1 if yes_price >= 0.99 else 0


def _extract_group_info(market: dict[str, Any]) -> tuple[int | None, str | None]:
    """Extract (bucket_index, bucket_label) from Polymarket's group fields."""
    bucket_index: int | None = None
    raw_threshold = market.get("groupItemThreshold")
    if raw_threshold is not None:
        try:
            bucket_index = int(str(raw_threshold).strip())
        except (TypeError, ValueError):
            bucket_index = None
    bucket_label = market.get("groupItemTitle")
    bucket_label = str(bucket_label).strip() if bucket_label else None
    return bucket_index, bucket_label


def _derive_event_slug(market_slug: str | None, bucket_label: str | None) -> str | None:
    """Strip the trailing bucket label from a market slug to get the event slug.

    Example: ``elon-musk-of-tweets-april-3-april-10-280-299`` with
    bucket_label ``280-299`` → ``elon-musk-of-tweets-april-3-april-10``.
    The ``+`` in "240+" becomes "-240-plus" in slugs; handle that too.
    If the suffix doesn't match, return the slug as-is (still useful).
    """
    if not market_slug:
        return None
    if not bucket_label:
        return market_slug
    # Polymarket slugifies "240+" → "240-plus"
    slugified = bucket_label.lower().replace("+", "-plus")
    suffix = f"-{slugified}"
    if market_slug.endswith(suffix):
        return market_slug[: -len(suffix)]
    return market_slug


def normalize_gamma_market(
    market: dict[str, Any],
    *,
    is_active: bool,
    logger: logging.Logger,
    definitions: Sequence[MarketDefinition] | None = None,
) -> MarketRecord | None:
    question = str(market.get("question", ""))
    active_definitions = tuple(definitions) if definitions is not None else get_market_definitions()
    definition = get_matching_market_definition(question, active_definitions)
    if definition is None:
        return None

    timeframe = definition.extract_timeframe(question)
    crypto = definition.extract_asset(question)
    if not timeframe or not crypto:
        logger.warning(
            "UNPARSEABLE_MARKET market_id=%s market_type=%s timeframe=%r crypto=%r question=%r",
            market.get("id", "?"),
            definition.key,
            timeframe,
            crypto,
            question[:150],
        )
        return None

    start_iso = market.get("start_date") or market.get("startDate")
    end_iso = (
        market.get("end_date")
        or market.get("endDate")
        or datetime.now(timezone.utc).isoformat()
    )
    # Only override endDate for markets with no API-provided end date.
    # Crypto up/down markets have a fixed prediction window end time that
    # must be preserved — overriding it makes long-expired markets appear live.
    if not market.get("closed", False) and not (market.get("end_date") or market.get("endDate")):
        end_iso = datetime.now(timezone.utc).isoformat()

    start_ts = parse_iso_timestamp(start_iso)
    end_ts = parse_iso_timestamp(end_iso)
    if start_ts is None or end_ts is None or start_ts >= end_ts:
        return None

    closed_ts = parse_iso_timestamp(market.get("closedTime") or market.get("closed_time"))

    market_slug_raw = market.get("slug")
    market_slug = str(market_slug_raw).strip() if market_slug_raw else None
    bucket_index, bucket_label = _extract_group_info(market)
    event_slug = _derive_event_slug(market_slug, bucket_label)

    if isinstance(definition, BinaryMarketDefinition):
        extracted_outcomes = _extract_binary_outcomes(market)
        if extracted_outcomes is None:
            logger.debug(
                "Market %s: unexpected token structure — skipping binary market",
                market.get("id", "?"),
            )
            return None

        classified_outcomes = definition.classify_outcomes(extracted_outcomes)
        if classified_outcomes is None:
            logger.debug(
                "Market %s: outcome labels do not match %s — skipping",
                market.get("id", "?"),
                definition.key,
            )
            return None

        # Two resolution-detection paths: the per-token `winner` flag (set
        # on older Gamma responses with nested `tokens[]`) and the price-based
        # fallback that works on the current `/markets` response shape.
        resolution: int | None = None
        winning_outcome = _extract_winning_outcome(market)
        if winning_outcome is not None:
            resolution = definition.resolution_for_winner(winning_outcome)
        if resolution is None:
            resolution = _binary_resolution_from_prices(market)

        return MarketRecord(
            market_id=str(market.get("id", "")),
            market_type=definition.key,
            question=question,
            timeframe=timeframe,
            crypto=crypto,
            condition_id=market.get("conditionId") or market.get("condition_id"),
            start_ts=start_ts,
            end_ts=end_ts,
            up_token_id=classified_outcomes.up_token_id,
            down_token_id=classified_outcomes.down_token_id,
            up_outcome=classified_outcomes.up_outcome,
            down_outcome=classified_outcomes.down_outcome,
            tokens={
                classified_outcomes.up_outcome: classified_outcomes.up_token_id,
                classified_outcomes.down_outcome: classified_outcomes.down_token_id,
            },
            category="crypto",
            volume=float(market.get("volume", 0) or 0),
            resolution=resolution,
            is_active=is_active,
            closed_ts=closed_ts,
            slug=market_slug,
            event_slug=event_slug,
            bucket_index=bucket_index,
            bucket_label=bucket_label,
        )

    elif isinstance(definition, MultiOutcomeMarketDefinition):
        extracted_outcomes = _extract_all_outcomes(market)
        if not extracted_outcomes:
            logger.debug(
                "Market %s: unexpected token structure — skipping multi-outcome market",
                market.get("id", "?"),
            )
            return None

        # Per-bucket resolution semantics: each culture "market" is its own
        # binary YES/NO sub-market. resolution=1 means this bucket was the
        # winning outcome of the parent event; 0 means it lost; None/-1
        # means not yet resolved. Use the price-based detector which handles
        # current Gamma responses (no `resolved` field) and falls back to
        # the legacy per-token winner flag if present.
        resolution = _binary_resolution_from_prices(market)
        if resolution is None:
            winning_outcome = _extract_winning_outcome(market)
            if winning_outcome is not None:
                outcomes = [o for o, _ in extracted_outcomes]
                if outcomes and outcomes[0] == winning_outcome:
                    resolution = 1
                elif winning_outcome in outcomes:
                    resolution = 0

        return MarketRecord(
            market_id=str(market.get("id", "")),
            market_type=definition.key,
            question=question,
            timeframe=timeframe,
            crypto=crypto,
            condition_id=market.get("conditionId") or market.get("condition_id"),
            start_ts=start_ts,
            end_ts=end_ts,
            tokens=dict(extracted_outcomes),
            category="culture",
            volume=float(market.get("volume", 0) or 0),
            resolution=resolution,
            is_active=is_active,
            closed_ts=closed_ts,
            slug=market_slug,
            event_slug=event_slug,
            bucket_index=bucket_index,
            bucket_label=bucket_label,
        )

    return None
