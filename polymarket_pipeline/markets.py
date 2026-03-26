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

SINGLE_HOUR_PATTERN = re.compile(
    r"\b\d{1,2}(?:AM|PM)\s+ET\b",
    flags=re.IGNORECASE,
)

DATE_RANGE_PATTERN = re.compile(
    r"([a-z]+)\s+(\d{1,2})(?:\s*-\s*|\s+to\s+)([a-z]+\s+)?(\d{1,2}),\s+(\d{4})",
    flags=re.IGNORECASE,
)

MONTH_YEAR_PATTERN = re.compile(
    r"\bin\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b",
    flags=re.IGNORECASE,
)

_FALLBACK_MARKET_DEFINITIONS_PAYLOAD: dict[str, Any] = {
    "version": 1,
    "definitions": [
        {
            "key": "crypto-up-down",
            "type": "binary",
            "question_keywords": ["up or down"],
            "asset_aliases": {
                "BTC": ["bitcoin", "btc"],
                "ETH": ["ethereum", "eth"],
                "SOL": ["solana", "sol"],
                "BNB": ["bnb", "binance coin"],
                "XRP": ["xrp", "ripple"],
                "DOGE": ["dogecoin", "doge"],
                "HYPE": ["hype", "hyperliquid"],
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
        },
        {
            "key": "elon-musk-tweets",
            "type": "multi-outcome",
            "question_keywords": ["# tweets", "elon musk"],
            "asset_aliases": {
                "ELON-TWEETS": ["elon musk", "tweets"],
            },
            "timeframes": [
                {
                    "name": "7-day",
                    "seconds": 7 * 24 * 60 * 60,
                    "question_aliases": ["march 17 - march 24", "march 17 to march 24"],
                    "cli_aliases": ["7d", "7-day"],
                    "range_minutes": 7 * 24 * 60,
                    "matches_single_hour_pattern": False,
                },
                {
                    "name": "4-day",
                    "seconds": 4 * 24 * 60 * 60,
                    "question_aliases": [
                        "march 23 - march 25",
                        "march 26 - march 28",
                        "march 23 to march 25",
                        "march 26 to march 28",
                    ],
                    "cli_aliases": ["4d", "4-day", "2d"],
                    "range_minutes": 4 * 24 * 60,
                    "matches_single_hour_pattern": False,
                },
                {
                    "name": "1-month",
                    "seconds": 30 * 24 * 60 * 60,
                    "question_aliases": ["monthly"],
                    "cli_aliases": ["1m", "1-month", "month"],
                    "range_minutes": None,
                    "matches_single_hour_pattern": False,
                    "matches_month_year_pattern": True,
                },
            ],
        },
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
    matches_month_year_pattern: bool = False

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
class MarketDefinition:
    key: str
    question_keywords: tuple[str, ...]
    asset_aliases: dict[str, tuple[str, ...]]
    timeframes: tuple[TimeframeDefinition, ...]
    
    @property
    def category(self) -> str:
        return "crypto"

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
        if range_minutes is None:
            range_minutes = _minutes_from_date_range(question)

        if range_minutes is not None:
            for timeframe in self.timeframes:
                if timeframe.range_minutes == range_minutes:
                    return timeframe.name

        if SINGLE_HOUR_PATTERN.search(question):
            for timeframe in self.timeframes:
                if timeframe.matches_single_hour_pattern:
                    return timeframe.name

        if MONTH_YEAR_PATTERN.search(question):
            for timeframe in self.timeframes:
                if timeframe.matches_month_year_pattern:
                    return timeframe.name

        return None

    def normalize_timeframe_input(self, value: str) -> str:
        value_lower = value.lower().strip()
        for timeframe in self.timeframes:
            if timeframe.matches_cli_value(value_lower):
                return timeframe.name
        return value

    @property
    def timeframe_names(self) -> tuple[str, ...]:
        return tuple(timeframe.name for timeframe in self.timeframes)

    @property
    def timeframe_seconds(self) -> dict[str, int]:
        return {timeframe.name: timeframe.seconds for timeframe in self.timeframes}


@dataclass(frozen=True)
class BinaryMarketDefinition(MarketDefinition):
    up_outcome_aliases: tuple[str, ...] = ("up", "yes")
    down_outcome_aliases: tuple[str, ...] = ("down", "no")

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


@dataclass(frozen=True)
class MultiOutcomeMarketDefinition(MarketDefinition):
    @property
    def category(self) -> str:
        return "culture"


def _contains_alias(text: str, aliases: Sequence[str]) -> bool:
    for alias in aliases:
        pattern = rf"\b{re.escape(alias)}\b"
        if re.search(pattern, text, flags=re.IGNORECASE):
            return True
    return False


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


def _minutes_from_date_range(question: str) -> int | None:
    match = DATE_RANGE_PATTERN.search(question)
    if not match:
        return None

    month_str, day1, month2_str, day2, year = match.groups()
    m1 = datetime.strptime(month_str, "%B").month
    m2 = m1 if not month2_str else datetime.strptime(month2_str.strip(), "%B").month

    d1 = datetime(int(year), m1, int(day1))
    d2 = datetime(int(year), m2, int(day2))

    delta_days = (d2 - d1).days
    if delta_days < 0:  # Crossed year boundary (e.g. Dec 30 to Jan 2)
        d2 = datetime(int(year) + 1, m2, int(day2))
        delta_days = (d2 - d1).days

    return delta_days * 24 * 60


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
    matches_month_year_pattern = bool(payload.get("matches_month_year_pattern", False))

    return TimeframeDefinition(
        name=name.strip(),
        seconds=seconds,
        question_aliases=_validate_string_list(
            payload.get("question_aliases", []),
            field_name=f"timeframe[{name}].question_aliases",
        ),
        cli_aliases=_validate_string_list(
            payload.get("cli_aliases", []),
            field_name=f"timeframe[{name}].cli_aliases",
        ),
        range_minutes=range_minutes,
        matches_single_hour_pattern=matches_single_hour_pattern,
        matches_month_year_pattern=matches_month_year_pattern,
    )


def _parse_market_definition(payload: Any) -> MarketDefinition:
    if not isinstance(payload, dict):
        raise MarketDefinitionError("market definitions must be objects")

    key = payload.get("key")
    if not isinstance(key, str) or not key.strip():
        raise MarketDefinitionError("market definition key must be a non-empty string")

    timeframes_raw = payload.get("timeframes")
    if not isinstance(timeframes_raw, list) or not timeframes_raw:
        raise MarketDefinitionError(f"market definition {key!r} must define timeframes")
        
    market_type = payload.get("type", "binary")

    if market_type == "binary":
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
    elif market_type == "multi-outcome":
        return MultiOutcomeMarketDefinition(
            key=key.strip(),
            question_keywords=_validate_string_list(
                payload.get("question_keywords"),
                field_name=f"definition[{key}].question_keywords",
            ),
            asset_aliases=_parse_asset_aliases(payload.get("asset_aliases")),
            timeframes=tuple(_parse_timeframe_definition(item) for item in timeframes_raw),
        )
    else:
        raise MarketDefinitionError(f"unknown market type {market_type!r} for definition {key!r}")


def _parse_definitions_payload(payload: Any) -> tuple[MarketDefinition, ...]:
    if not isinstance(payload, dict):
        raise MarketDefinitionError("market definitions payload must be an object")

    raw_definitions = payload.get("definitions")
    if not isinstance(raw_definitions, list) or not raw_definitions:
        raise MarketDefinitionError("market definitions payload must contain a non-empty 'definitions' list")

    return tuple(_parse_market_definition(item) for item in raw_definitions)


def default_market_definitions_path() -> Path:
    return Path(__file__).with_name("market_definitions.json")


@lru_cache(maxsize=None)
def _load_market_definitions_cached(path_value: str | None) -> tuple[MarketDefinition, ...]:
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


def load_market_definitions(path: str | Path | None = None) -> tuple[MarketDefinition, ...]:
    resolved = str(Path(path).expanduser().resolve()) if path is not None else None
    return _load_market_definitions_cached(resolved)


def get_market_definitions(*, force_reload: bool = False) -> tuple[MarketDefinition, ...]:
    if force_reload:
        _load_market_definitions_cached.cache_clear()
    return load_market_definitions()


MARKET_DEFINITIONS: tuple[MarketDefinition, ...] = get_market_definitions()
DEFAULT_MARKET_DEFINITION = MARKET_DEFINITIONS[0]


def get_matching_market_definition(
    question: str,
    definitions: Sequence[MarketDefinition] | None = None,
) -> MarketDefinition | None:
    active_definitions = definitions if definitions is not None else MARKET_DEFINITIONS
    for definition in active_definitions:
        if definition.matches_question(question):
            return definition
    return None


def normalize_timeframe_value(
    value: str,
    definitions: Sequence[MarketDefinition] | None = None,
) -> str:
    active_definitions = definitions if definitions is not None else MARKET_DEFINITIONS
    for definition in active_definitions:
        normalized = definition.normalize_timeframe_input(value)
        if normalized != value or normalized in definition.timeframe_names:
            return normalized
    return value
