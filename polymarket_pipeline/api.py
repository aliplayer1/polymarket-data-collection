import logging
import time
from typing import Any, Iterator, Sequence

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as Urllib3Retry

from .config import (
    CLOB_HOST,
    GAMMA_API,
    PAGE_SIZE,
    PRICE_HISTORY_CHUNK_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
)
from .market_normalization import normalize_gamma_market
from .markets import BinaryMarketDefinition, get_market_definitions
from .models import MarketRecord
from .parsing import parse_iso_timestamp
from .retry import api_call_with_retry


class PolymarketApi:
    def __init__(
        self,
        session: Session | None = None,
        logger: logging.Logger | None = None,
        market_definitions: Sequence[BinaryMarketDefinition] | None = None,
    ) -> None:
        self.session = session or self._create_session()
        self.logger = logger or logging.getLogger("polymarket_pipeline")
        self.market_definitions = tuple(market_definitions) if market_definitions is not None else get_market_definitions()

    @staticmethod
    def _create_session() -> Session:
        """Create a requests Session with connection pooling and transport-level retries."""
        session = requests.Session()
        retry_strategy = Urllib3Retry(
            total=0,
            # Application-level retry is handled by api_call_with_retry() —
            # keeping transport retries at 0 prevents double-stacked retries
            # that multiply rate-limit pressure (previously 2 × 3 = 6 attempts).
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
            # Detect Cloudflare WAF / CDN blocks before raise_for_status so we
            # can raise a descriptive error rather than a generic HTTP 403/503.
            # WAF responses return HTML with Content-Type: text/html even when the
            # client expects JSON, which would cause a silent JSON parse failure.
            ct = response.headers.get("content-type", "")
            if response.status_code in (403, 429, 503) and "text/html" in ct:
                snippet = response.text[:300].replace("\n", " ")
                raise RuntimeError(
                    f"WAF/CDN block at {url!r} (status={response.status_code}): {snippet!r}"
                )
            response.raise_for_status()
            return response.json()

        try:
            return api_call_with_retry(_fetch, logger=self.logger)
        except Exception:
            self.logger.error("Request ultimately failed: %s params=%s", url, params)
            raise

    def _normalize_market(self, market: dict[str, Any], is_active: bool) -> MarketRecord | None:
        return normalize_gamma_market(
            market,
            is_active=is_active,
            logger=self.logger,
            definitions=self.market_definitions,
        )

    def fetch_markets(self, *, active: bool = False, closed: bool = False, end_ts_min: int | None = None) -> Iterator[MarketRecord]:
        offset = 0

        # We must explicitly fetch high-frequency markets by their tag ID because 
        # Polymarket marks them as "restricted: true", which excludes them from
        # the default /markets endpoint results.
        # Tag 102127 = "Up or Down"
        UP_DOWN_TAG_ID = 102127

        while True:
            order_field = "closedTime" if closed else "volume24hr"
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
            
            # --- PHASE 1: Standard Markets ---
            try:
                page = self._request_json(f"{GAMMA_API}/markets", params=params)
            except Exception as exc:
                if "422" not in str(exc).lower():
                    raise
                fallback_params = dict(params)
                fallback_params.pop("order", None)
                fallback_params.pop("ascending", None)
                page = self._request_json(f"{GAMMA_API}/markets", params=fallback_params)

            # --- PHASE 2: High-Frequency (Restricted) Markets ---
            # On the first page, also fetch the "Up or Down" tag directly.
            if offset == 0:
                try:
                    tag_params = dict(params)
                    tag_params["tag_id"] = UP_DOWN_TAG_ID
                    tag_page = self._request_json(f"{GAMMA_API}/markets", params=tag_params)
                    if tag_page:
                        page.extend(tag_page)
                except Exception as exc:
                    self.logger.warning("Failed to fetch restricted up/down markets: %s", exc)

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


    def fetch_fee_rate_bps(self, token_id: str) -> int | None:
        """Fetch the taker fee rate (basis points) for a token from the CLOB API.

        Calls ``GET /fee-rate?token_id={token_id}`` which returns ``{"base_fee": N}``.
        Returns ``base_fee`` as an integer, or None on failure.
        """
        try:
            data = self._request_json(f"{CLOB_HOST}/fee-rate", {"token_id": token_id})
            if data and isinstance(data, dict):
                base_fee = data.get("base_fee")
                if base_fee is not None:
                    return int(base_fee)
        except Exception as exc:
            self.logger.debug("Fee rate fetch failed for token %s: %s", token_id[:20], exc)
        return None

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
