from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

TIME_RANGE_PATTERN = re.compile(
    r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*-\s*(\d{1,2}):(\d{2})\s*(AM|PM)",
    flags=re.IGNORECASE,
)

# Matches single-hour format: "Bitcoin Up or Down - March 1, 10AM ET"
# No end time means the market covers exactly one hour.
SINGLE_HOUR_PATTERN = re.compile(
    r"\b\d{1,2}(?:AM|PM)\s+ET\b",
    flags=re.IGNORECASE,
)

_FALLBACK_MARKET_DEFINITIONS_PAYLOAD: dict[str, Any] = {
    "version": 1,
    "definitions": [
        {
            "key": "crypto-up-down",
            "question_keywords": ["up or down"],
            "asset_aliases": {
                "BTC": ["bitcoin", "btc"],
                "ETH": ["ethereum", "eth"],
                "SOL": ["solana", "sol"],
            },
            "up_outcome_aliases": ["up", "yes"],
            "down_outcome_aliases": ["down", "no"],
            "timeframes": [
                {
                    "name": "15-minute",
                    "seconds": 15 * 60,
                    "question_aliases": ["15-minute", "15 minute", "15 min"],
                    "cli_aliases": ["15m", "15min", "15-minute", "15-min"],
                    "range_minutes": 15,
                    "matches_single_hour_pattern": False,
                },
                {
                    "name": "5-minute",
                    "seconds": 5 * 60,
                    "question_aliases": ["5-minute", "5 minute", "5 min"],
                    "cli_aliases": ["5m", "5min", "5-minute", "5-min"],
                    "range_minutes": 5,
                    "matches_single_hour_pattern": False,
                },
                {
                    "name": "4-hour",
                    "seconds": 4 * 60 * 60,
                    "question_aliases": ["4-hour", "4 hour"],
                    "cli_aliases": ["4h", "4hr", "4-hour"],
                    "range_minutes": 240,
                    "matches_single_hour_pattern": False,
                },
                {
                    "name": "1-hour",
                    "seconds": 60 * 60,
                    "question_aliases": ["1-hour", "1 hour", "hourly", "hour"],
                    "cli_aliases": ["1h", "1hr", "1-hour", "hourly"],
                    "range_minutes": 60,
                    "matches_single_hour_pattern": True,
                },
            ],
        }
    ],
}


class MarketDefinitionError(ValueError):
    """Raised when externalized market definitions are invalid."""


@dataclass(frozen=True)
class TimeframeDefinition:
    name: str
    seconds: int
    question_aliases: tuple[str, ...]
    cli_aliases: tuple[str, ...]
    range_minutes: int | None = None
    matches_single_hour_pattern: bool = False

    def matches_question(self, question_lower: str) -> bool:
        return _contains_alias(question_lower, self.question_aliases)

    def matches_cli_value(self, value_lower: str) -> bool:
        return value_lower == self.name or value_lower in self.cli_aliases


@dataclass(frozen=True)
class ClassifiedBinaryOutcomePair:
    up_token_id: str
    down_token_id: str
    up_outcome: str
    down_outcome: str


@dataclass(frozen=True)
class BinaryMarketDefinition:
    key: str
    question_keywords: tuple[str, ...]
    asset_aliases: dict[str, tuple[str, ...]]
    timeframes: tuple[TimeframeDefinition, ...]
    up_outcome_aliases: tuple[str, ...] = ("up", "yes")
    down_outcome_aliases: tuple[str, ...] = ("down", "no")

    def matches_question(self, question: str) -> bool:
        return _contains_alias(question.lower(), self.question_keywords)

    def extract_asset(self, question: str) -> str | None:
        q = question.lower()
        for symbol, aliases in self.asset_aliases.items():
            if _contains_alias(q, aliases):
                return symbol
        return None

    def extract_timeframe(self, question: str) -> str | None:
        q = question.lower()
        for timeframe in self.timeframes:
            if timeframe.matches_question(q):
                return timeframe.name

        range_minutes = _minutes_from_time_range(question)
        if range_minutes is not None:
            for timeframe in self.timeframes:
                if timeframe.range_minutes == range_minutes:
                    return timeframe.name

        if SINGLE_HOUR_PATTERN.search(question):
            for timeframe in self.timeframes:
                if timeframe.matches_single_hour_pattern:
                    return timeframe.name

        return None

    def normalize_timeframe_input(self, value: str) -> str:
        value_lower = value.lower().strip()
        for timeframe in self.timeframes:
            if timeframe.matches_cli_value(value_lower):
                return timeframe.name
        return value

    def classify_outcomes(
        self,
        outcome_tokens: Sequence[tuple[str, str]],
    ) -> ClassifiedBinaryOutcomePair | None:
        up: tuple[str, str] | None = None
        down: tuple[str, str] | None = None

        for outcome, token_id in outcome_tokens:
            normalized_outcome = outcome.lower()
            if _contains_alias(normalized_outcome, self.up_outcome_aliases):
                up = (outcome, token_id)
            elif _contains_alias(normalized_outcome, self.down_outcome_aliases):
                down = (outcome, token_id)

        if up is None or down is None:
            return None

        return ClassifiedBinaryOutcomePair(
            up_token_id=up[1],
            down_token_id=down[1],
            up_outcome=up[0],
            down_outcome=down[0],
        )

    def resolution_for_winner(self, winning_outcome: str | None) -> int | None:
        if not winning_outcome:
            return None

        normalized_outcome = winning_outcome.lower()
        if _contains_alias(normalized_outcome, self.up_outcome_aliases):
            return 1
        if _contains_alias(normalized_outcome, self.down_outcome_aliases):
            return 0
        return None

    @property
    def timeframe_names(self) -> tuple[str, ...]:
        return tuple(timeframe.name for timeframe in self.timeframes)

    @property
    def timeframe_seconds(self) -> dict[str, int]:
        return {timeframe.name: timeframe.seconds for timeframe in self.timeframes}


def _contains_alias(text: str, aliases: Sequence[str]) -> bool:
    return any(alias in text for alias in aliases)


def _minutes_from_time_range(question: str) -> int | None:
    match = TIME_RANGE_PATTERN.search(question)
    if not match:
        return None

    start_h, start_m, start_meridiem, end_h, end_m, end_meridiem = match.groups()
    start_dt = datetime.strptime(f"{start_h}:{start_m}{start_meridiem.upper()}", "%I:%M%p")
    end_dt = datetime.strptime(f"{end_h}:{end_m}{end_meridiem.upper()}", "%I:%M%p")
    delta_minutes = int((end_dt - start_dt).total_seconds() // 60)
    if delta_minutes <= 0:
        delta_minutes += 24 * 60
    return delta_minutes


def _validate_string_list(value: Any, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise MarketDefinitionError(f"{field_name} must be a non-empty list")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise MarketDefinitionError(f"{field_name} entries must be non-empty strings")
        items.append(item.strip())
    return tuple(items)


def _parse_asset_aliases(value: Any) -> dict[str, tuple[str, ...]]:
    if not isinstance(value, dict) or not value:
        raise MarketDefinitionError("asset_aliases must be a non-empty object")
    parsed: dict[str, tuple[str, ...]] = {}
    for symbol, aliases in value.items():
        if not isinstance(symbol, str) or not symbol.strip():
            raise MarketDefinitionError("asset_aliases keys must be non-empty strings")
        parsed[symbol.strip().upper()] = _validate_string_list(
            aliases,
            field_name=f"asset_aliases[{symbol}]",
        )
    return parsed


def _parse_timeframe_definition(payload: Any) -> TimeframeDefinition:
    if not isinstance(payload, dict):
        raise MarketDefinitionError("timeframe definitions must be objects")

    name = payload.get("name")
    seconds = payload.get("seconds")
    if not isinstance(name, str) or not name.strip():
        raise MarketDefinitionError("timeframe name must be a non-empty string")
    if not isinstance(seconds, int) or seconds <= 0:
        raise MarketDefinitionError(f"timeframe {name!r} must define a positive integer 'seconds'")

    range_minutes = payload.get("range_minutes")
    if range_minutes is not None and (not isinstance(range_minutes, int) or range_minutes <= 0):
        raise MarketDefinitionError(f"timeframe {name!r} has invalid range_minutes")

    matches_single_hour_pattern = bool(payload.get("matches_single_hour_pattern", False))

    return TimeframeDefinition(
        name=name.strip(),
        seconds=seconds,
        question_aliases=_validate_string_list(
            payload.get("question_aliases"),
            field_name=f"timeframe[{name}].question_aliases",
        ),
        cli_aliases=_validate_string_list(
            payload.get("cli_aliases"),
            field_name=f"timeframe[{name}].cli_aliases",
        ),
        range_minutes=range_minutes,
        matches_single_hour_pattern=matches_single_hour_pattern,
    )


def _parse_market_definition(payload: Any) -> BinaryMarketDefinition:
    if not isinstance(payload, dict):
        raise MarketDefinitionError("market definitions must be objects")

    key = payload.get("key")
    if not isinstance(key, str) or not key.strip():
        raise MarketDefinitionError("market definition key must be a non-empty string")

    timeframes_raw = payload.get("timeframes")
    if not isinstance(timeframes_raw, list) or not timeframes_raw:
        raise MarketDefinitionError(f"market definition {key!r} must define timeframes")

    return BinaryMarketDefinition(
        key=key.strip(),
        question_keywords=_validate_string_list(
            payload.get("question_keywords"),
            field_name=f"definition[{key}].question_keywords",
        ),
        asset_aliases=_parse_asset_aliases(payload.get("asset_aliases")),
        timeframes=tuple(_parse_timeframe_definition(item) for item in timeframes_raw),
        up_outcome_aliases=_validate_string_list(
            payload.get("up_outcome_aliases", ["up", "yes"]),
            field_name=f"definition[{key}].up_outcome_aliases",
        ),
        down_outcome_aliases=_validate_string_list(
            payload.get("down_outcome_aliases", ["down", "no"]),
            field_name=f"definition[{key}].down_outcome_aliases",
        ),
    )


def _parse_definitions_payload(payload: Any) -> tuple[BinaryMarketDefinition, ...]:
    if not isinstance(payload, dict):
        raise MarketDefinitionError("market definitions payload must be an object")

    raw_definitions = payload.get("definitions")
    if not isinstance(raw_definitions, list) or not raw_definitions:
        raise MarketDefinitionError("market definitions payload must contain a non-empty 'definitions' list")

    return tuple(_parse_market_definition(item) for item in raw_definitions)


def default_market_definitions_path() -> Path:
    return Path(__file__).with_name("market_definitions.json")


@lru_cache(maxsize=None)
def _load_market_definitions_cached(path_value: str | None) -> tuple[BinaryMarketDefinition, ...]:
    if path_value is None:
        try:
            with default_market_definitions_path().open("r", encoding="utf-8") as handle:
                return _parse_definitions_payload(json.load(handle))
        except (OSError, json.JSONDecodeError, MarketDefinitionError):
            return _parse_definitions_payload(_FALLBACK_MARKET_DEFINITIONS_PAYLOAD)

    path = Path(path_value)
    try:
        with path.open("r", encoding="utf-8") as handle:
            return _parse_definitions_payload(json.load(handle))
    except (OSError, json.JSONDecodeError, MarketDefinitionError) as exc:
        raise MarketDefinitionError(f"Failed to load market definitions from {path}: {exc}") from exc


def load_market_definitions(path: str | Path | None = None) -> tuple[BinaryMarketDefinition, ...]:
    resolved = str(Path(path).expanduser().resolve()) if path is not None else None
    return _load_market_definitions_cached(resolved)


def get_market_definitions(*, force_reload: bool = False) -> tuple[BinaryMarketDefinition, ...]:
    if force_reload:
        _load_market_definitions_cached.cache_clear()
    return load_market_definitions()


MARKET_DEFINITIONS: tuple[BinaryMarketDefinition, ...] = get_market_definitions()
DEFAULT_MARKET_DEFINITION = MARKET_DEFINITIONS[0]


def get_matching_market_definition(
    question: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> BinaryMarketDefinition | None:
    active_definitions = definitions if definitions is not None else MARKET_DEFINITIONS
    for definition in active_definitions:
        if definition.matches_question(question):
            return definition
    return None


def normalize_timeframe_value(
    value: str,
    definitions: Sequence[BinaryMarketDefinition] | None = None,
) -> str:
    active_definitions = definitions if definitions is not None else MARKET_DEFINITIONS
    for definition in active_definitions:
        normalized = definition.normalize_timeframe_input(value)
        if normalized != value or normalized in definition.timeframe_names:
            return normalized
    return value
