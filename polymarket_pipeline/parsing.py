from __future__ import annotations

from datetime import datetime
from typing import Sequence

from .markets import (
    BinaryMarketDefinition,
    DEFAULT_MARKET_DEFINITION,
    get_matching_market_definition,
    normalize_timeframe_value,
)


def extract_timeframe(
    question: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> str | None:
    definition = get_matching_market_definition(question, definitions) or DEFAULT_MARKET_DEFINITION
    return definition.extract_timeframe(question)


def extract_asset(
    question: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> str | None:
    definition = get_matching_market_definition(question, definitions) or DEFAULT_MARKET_DEFINITION
    return definition.extract_asset(question)


def extract_crypto(
    question: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> str | None:
    return extract_asset(question, definitions)


def normalize_timeframe_input(
    value: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> str:
    return normalize_timeframe_value(value, definitions)


def parse_iso_timestamp(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except (TypeError, ValueError):
        return None
