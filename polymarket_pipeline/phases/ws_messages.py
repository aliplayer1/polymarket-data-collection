"""Typed parsed messages for CLOB + RTDS WebSocket feeds.

Using ``@dataclass(slots=True, frozen=True)`` makes allocation cheap and
catches field typos at class-definition time.  The ``parse_*`` helpers
convert raw dict payloads into typed records; invalid payloads return
``None`` (or an empty list) rather than raising, so the recv loop can
``continue`` on malformed data without a try/except per field.

The parsers perform all bounds checks that used to be sprinkled through
the recv loop:

- float/int coercion with NaN rejection (``math.isfinite``)
- trade-price range check (0.0 ≤ price ≤ 1.0)
- RTDS price range check (0 < price < 1e9)
- trade-side normalisation (BUY/SELL, default BUY)
- empty-string ``asset_id`` rejection

These are best-effort: a parser returning ``None`` only filters the
current message; it does not count as a validation failure for the
connection-health watchdog.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any


# --- CLOB message types ---------------------------------------------------


@dataclass(slots=True, frozen=True)
class PriceChangeEvent:
    """One per-token BBO update inside a ``price_change`` message.

    Fields are optional: the CLOB WS omits any BBO component that didn't
    change since the last update, so callers must merge this into a
    per-token running snapshot rather than using values directly.
    """
    asset_id: str
    best_bid: float | None = None
    best_ask: float | None = None
    best_bid_size: float | None = None
    best_ask_size: float | None = None


@dataclass(slots=True, frozen=True)
class LastTradePriceEvent:
    asset_id: str
    timestamp_ms: int
    price: float
    size_shares: float
    trade_side: str  # "BUY" or "SELL"
    tx_hash: str


@dataclass(slots=True, frozen=True)
class BookSnapshot:
    """Parsed ``event_type=book`` or an element of an initial snapshot array."""
    asset_id: str
    best_bid: float
    best_ask: float
    best_bid_size: float
    best_ask_size: float


# --- RTDS message types ---------------------------------------------------


@dataclass(slots=True, frozen=True)
class RTDSUpdate:
    topic: str   # "crypto_prices" or "crypto_prices_chainlink"
    symbol: str  # e.g. "btcusdt", "btc/usd"
    price: float
    ts_ms: int


# --- Helpers --------------------------------------------------------------


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (ValueError, TypeError):
        return None
    if not math.isfinite(f):
        return None
    return f


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


# --- CLOB parsers ---------------------------------------------------------


def parse_price_change(event: dict[str, Any]) -> list[PriceChangeEvent]:
    """Flatten a ``price_change`` event into a list of per-token updates.

    Returns ``[]`` for malformed or empty messages.
    """
    changes = event.get("price_changes") or []
    out: list[PriceChangeEvent] = []
    for change in changes:
        if not isinstance(change, dict):
            continue
        asset_id = change.get("asset_id")
        if not asset_id:
            continue
        out.append(PriceChangeEvent(
            asset_id=str(asset_id),
            best_bid=_safe_float(change.get("best_bid")),
            best_ask=_safe_float(change.get("best_ask")),
            best_bid_size=_safe_float(change.get("best_bid_size")),
            best_ask_size=_safe_float(change.get("best_ask_size")),
        ))
    return out


def parse_last_trade_price(event: dict[str, Any]) -> LastTradePriceEvent | None:
    asset_id = event.get("asset_id")
    if not asset_id:
        return None
    ts_ms = _safe_int(event.get("timestamp"))
    price = _safe_float(event.get("price"))
    if ts_ms is None or ts_ms <= 0:
        return None
    if price is None or not (0.0 <= price <= 1.0):
        return None
    size = _safe_float(event.get("size") if event.get("size") is not None else event.get("amount"))
    if size is None or size < 0:
        size = 0.0
    side_raw = event.get("side") or event.get("type_side") or ""
    side = str(side_raw).upper()
    if side not in ("BUY", "SELL"):
        side = "BUY"
    tx_hash = str(event.get("hash") or event.get("tx_hash") or "")
    return LastTradePriceEvent(
        asset_id=str(asset_id),
        timestamp_ms=ts_ms,
        price=price,
        size_shares=size,
        trade_side=side,
        tx_hash=tx_hash,
    )


def parse_book_snapshot(payload: dict[str, Any]) -> BookSnapshot | None:
    """Parse a top-level ``event_type=book`` event or one initial-snapshot entry.

    Missing/empty bid or ask books produce zeros on that side.  Callers
    should still validate (see ``is_valid_bbo``) before writing.
    """
    if not isinstance(payload, dict):
        return None
    asset_id = payload.get("asset_id")
    if not asset_id:
        return None
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    best_bid = _safe_float(bids[0].get("price")) if bids and isinstance(bids[0], dict) else None
    best_ask = _safe_float(asks[0].get("price")) if asks and isinstance(asks[0], dict) else None
    best_bid_sz = _safe_float(bids[0].get("size")) if bids and isinstance(bids[0], dict) else None
    best_ask_sz = _safe_float(asks[0].get("size")) if asks and isinstance(asks[0], dict) else None
    return BookSnapshot(
        asset_id=str(asset_id),
        best_bid=best_bid or 0.0,
        best_ask=best_ask or 0.0,
        best_bid_size=best_bid_sz or 0.0,
        best_ask_size=best_ask_sz or 0.0,
    )


def is_valid_bbo(best_bid: float, best_ask: float,
                 best_bid_size: float, best_ask_size: float) -> bool:
    """Reject obviously-broken BBOs before they pollute storage / signals.

    A valid BBO has both sides present, non-inverted, and finite sizes.
    Empty books during the first ~170 s of a market's life routinely
    produce ``best_bid = 0`` or ``best_bid >= best_ask`` — recording those
    silently corrupts spread-based features downstream.
    """
    if not (math.isfinite(best_bid) and math.isfinite(best_ask)):
        return False
    if not (math.isfinite(best_bid_size) and math.isfinite(best_ask_size)):
        return False
    if best_bid <= 0.0 or best_ask <= 0.0:
        return False
    if best_bid >= best_ask:
        return False
    if best_bid > 1.0 or best_ask > 1.0:
        return False
    return True


# --- RTDS parser ----------------------------------------------------------


_RTDS_PRICE_UPPER_BOUND = 1e9  # guards against exponent garbage (BTC < 1e7 realistically)


def parse_rtds_update(raw: str | bytes) -> RTDSUpdate | None:
    """Parse a single RTDS WebSocket frame.

    Returns ``None`` for:
    - ``PONG`` keepalive replies
    - non-``update`` message types (subscribed, snapshot-only, errors)
    - malformed JSON
    - missing/invalid symbol, price, or timestamp
    - prices <= 0 or > ``_RTDS_PRICE_UPPER_BOUND``
    """
    if raw == "PONG" or raw == b"PONG":
        return None
    try:
        msg = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(msg, dict):
        return None
    if msg.get("type") != "update":
        return None
    payload = msg.get("payload")
    if not isinstance(payload, dict):
        return None
    topic = str(msg.get("topic", ""))
    symbol = str(payload.get("symbol", "")).lower()
    price = _safe_float(payload.get("value"))
    ts_ms = _safe_int(payload.get("timestamp"))
    if not symbol or price is None or ts_ms is None:
        return None
    if price <= 0.0 or price >= _RTDS_PRICE_UPPER_BOUND:
        return None
    return RTDSUpdate(topic=topic, symbol=symbol, price=price, ts_ms=ts_ms)


# --- Subscription helpers (safe JSON) -------------------------------------


def clob_subscribe_payload(asset_ids: list[str]) -> str:
    """Build the CLOB market subscription payload as a safe JSON string."""
    return json.dumps({
        "assets_ids": list(asset_ids),
        "type": "market",
        "custom_feature_enabled": True,
    })


def rtds_subscribe_payload(topic: str) -> str:
    """Build an RTDS subscription payload for a single topic.

    ``topic`` must be one of ``"crypto_prices"`` or
    ``"crypto_prices_chainlink"``.
    """
    return json.dumps({
        "action": "subscribe",
        "subscriptions": [{"topic": topic, "type": "*"}],
    })
