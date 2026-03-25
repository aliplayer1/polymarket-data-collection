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
    if not market.get("closed", False):
        end_iso = datetime.now(timezone.utc).isoformat()

    start_ts = parse_iso_timestamp(start_iso)
    end_ts = parse_iso_timestamp(end_iso)
    if start_ts is None or end_ts is None or start_ts >= end_ts:
        return None

    closed_ts = parse_iso_timestamp(market.get("closedTime") or market.get("closed_time"))

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

        resolution = None
        if market.get("resolved", False):
            resolution = definition.resolution_for_winner(_extract_winning_outcome(market))

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
        )

    elif isinstance(definition, MultiOutcomeMarketDefinition):
        extracted_outcomes = _extract_all_outcomes(market)
        if not extracted_outcomes:
            logger.debug(
                "Market %s: unexpected token structure — skipping multi-outcome market",
                market.get("id", "?"),
            )
            return None
            
        winning_outcome = None
        if market.get("resolved", False):
            winning_outcome = _extract_winning_outcome(market)
        
        # We don't have a simple 0/1 resolution for multi-outcome unless we just map it.
        # But for culture markets we can leave resolution=None or map the index of the winner.
        # Let's map it to the index of the winning outcome string if possible.
        resolution = None
        if winning_outcome:
            try:
                outcomes = [o for o, t in extracted_outcomes]
                resolution = outcomes.index(winning_outcome)
            except ValueError:
                resolution = None
                
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
        )

    return None
