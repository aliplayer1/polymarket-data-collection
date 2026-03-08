import logging
import json
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as Urllib3Retry

from .config import (
    CLOB_HOST,
    FILTER_KEYWORD,
    GAMMA_API,
    PAGE_SIZE,
    PRICE_HISTORY_CHUNK_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
)
from .models import MarketRecord
from .parsing import extract_crypto, extract_timeframe, parse_iso_timestamp
from .retry import api_call_with_retry


class PolymarketApi:
    def __init__(self, session: Session | None = None, logger: logging.Logger | None = None) -> None:
        self.session = session or self._create_session()
        self.logger = logger or logging.getLogger("polymarket_pipeline")

    @staticmethod
    def _create_session() -> Session:
        """Create a requests Session with connection pooling and transport-level retries."""
        session = requests.Session()
        retry_strategy = Urllib3Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=retry_strategy,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _request_json(self, url: str, params: dict[str, Any]) -> Any:
        def _fetch() -> Any:
            response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()

        try:
            return api_call_with_retry(_fetch, logger=self.logger)
        except Exception:
            self.logger.error("Request ultimately failed: %s params=%s", url, params)
            raise

    @staticmethod
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

    def _normalize_market(self, market: dict[str, Any], is_active: bool) -> MarketRecord | None:
        question = market.get("question", "")
        q_lower = question.lower()
        if FILTER_KEYWORD not in q_lower:
            return None

        timeframe = extract_timeframe(question)
        crypto = extract_crypto(question)
        if not timeframe or not crypto:
            return None

        tokens = market.get("tokens", [])
        outcomes = self._coerce_list(market.get("outcomes"))
        clob_token_ids = self._coerce_list(market.get("clobTokenIds"))

        outcome1 = ""
        outcome2 = ""
        token1_id = ""
        token2_id = ""

        if len(tokens) == 2:
            outcome1 = str(tokens[0].get("outcome", "")).lower()
            outcome2 = str(tokens[1].get("outcome", "")).lower()
            token1_id = str(tokens[0].get("token_id") or tokens[0].get("tokenId") or "")
            token2_id = str(tokens[1].get("token_id") or tokens[1].get("tokenId") or "")
        elif len(outcomes) == 2 and len(clob_token_ids) == 2:
            outcome1 = str(outcomes[0]).lower()
            outcome2 = str(outcomes[1]).lower()
            token1_id = str(clob_token_ids[0])
            token2_id = str(clob_token_ids[1])
        else:
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

        if not token1_id or not token2_id:
            return None

        resolution = None
        if market.get("resolved", False):
            for token in tokens:
                if token.get("winner", False):
                    winning_outcome = str(token.get("outcome", "")).lower()
                    resolution = 1 if ("up" in winning_outcome or "yes" in winning_outcome) else 0
                    break

        return MarketRecord(
            market_id=str(market.get("id", "")),
            question=question,
            timeframe=timeframe,
            crypto=crypto,
            condition_id=market.get("conditionId") or market.get("condition_id"),
            start_ts=start_ts,
            end_ts=end_ts,
            token1_id=token1_id,
            token2_id=token2_id,
            outcome1=outcome1,
            outcome2=outcome2,
            volume=float(market.get("volume", 0) or 0),
            resolution=resolution,
            is_active=is_active,
        )

    def fetch_markets(self, *, active: bool = False, closed: bool = False, end_ts_min: int | None = None) -> Iterator[MarketRecord]:
        offset = 0

        while True:
            order_field = "closed_time" if closed else "volume_24hr"
            params: dict[str, Any] = {
                "limit": PAGE_SIZE,
                "offset": offset,
                "order": order_field,
                "ascending": False,
            }
            if active:
                params["active"] = "true"
                params["closed"] = "false"
            elif closed:
                params["closed"] = "true"
                params["active"] = "false"

            self.logger.info("Fetching markets page (offset=%s, active=%s, closed=%s)...", offset, active, closed)
            try:
                page = self._request_json(f"{GAMMA_API}/markets", params=params)
            except Exception as exc:
                error_text = str(exc).lower()
                if "422" not in error_text or "order fields are not valid" not in error_text:
                    raise
                fallback_params = dict(params)
                fallback_params.pop("order", None)
                fallback_params.pop("ascending", None)
                self.logger.warning("Order field rejected by API, retrying without explicit order")
                page = self._request_json(f"{GAMMA_API}/markets", params=fallback_params)

            if not page:
                break

            # Track the maximum closedTime across all raw markets on this page.
            # Pages are sorted by closedTime DESC, so when the newest closedTime
            # on a page falls below our cutoff, every subsequent page will be
            # even older and we can stop the entire scan here.
            # We use closedTime (not endDate) because endDate can be a far-future
            # scheduled resolution date that would prevent the cutoff from triggering.
            page_max_closed_ts = 0
            for raw_market in page:
                raw_closed = parse_iso_timestamp(raw_market.get("closedTime"))
                if raw_closed is not None and raw_closed > page_max_closed_ts:
                    page_max_closed_ts = raw_closed
                parsed = self._normalize_market(raw_market, is_active=active)
                if parsed:
                    yield parsed

            if end_ts_min is not None and page_max_closed_ts > 0 and page_max_closed_ts < end_ts_min:
                self.logger.info(
                    "Scan cutoff reached (page max closedTime %s < cutoff %s); stopping early.",
                    page_max_closed_ts, end_ts_min,
                )
                return

            if len(page) < PAGE_SIZE:
                break

            offset += PAGE_SIZE
            time.sleep(0.5)


    def fetch_price_history(self, token_id: str, start_ts: int, end_ts: int, fidelity: int = 1) -> list[dict[str, Any]]:
        history: list[dict[str, Any]] = []
        current_start = start_ts

        while current_start < end_ts:
            current_end = min(current_start + PRICE_HISTORY_CHUNK_SECONDS, end_ts)
            params = {
                "market": token_id,
                "startTs": current_start,
                "endTs": current_end,
                "fidelity": fidelity,
            }

            try:
                chunk = self._request_json(f"{CLOB_HOST}/prices-history", params=params)
            except Exception as exc:
                error_text = str(exc).lower()
                if "400" not in error_text:
                    raise
                fallback_params = dict(params)
                fallback_params.pop("market", None)
                fallback_params["token_id"] = token_id
                self.logger.warning("Prices history rejected 'market' param, retrying with token_id")
                chunk = self._request_json(f"{CLOB_HOST}/prices-history", params=fallback_params)

            if chunk is None:
                break

            raw_points = chunk.get("history", [])
            valid_points = [
                pt for pt in raw_points
                if "t" in pt and "p" in pt and 0.0 <= float(pt["p"]) <= 1.0
            ]
            if len(valid_points) < len(raw_points):
                self.logger.warning(
                    "Filtered %s out-of-range price points for token %s",
                    len(raw_points) - len(valid_points),
                    token_id,
                )
            history.extend(valid_points)
            current_start = current_end
            time.sleep(0.2)

        return history
