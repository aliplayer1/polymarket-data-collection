"""On-chain tick data fetcher for Polymarket prediction markets.

Every trade fill on Polymarket is settled on Polygon as an ERC-1155 transfer
emitted by the CTF Exchange contract (OrderFilled event).  This module queries
those events to reconstruct a per-market trade tick series with:

  timestamp_ms  -  block timestamp converted to milliseconds
  token_id      -  ERC-1155 outcome token (maps to Up/Down)
  outcome       -  "Up" or "Down"
  side          -  "BUY" or "SELL" (from taker perspective)
  price         -  fill price in [0, 1]
  size_usdc     -  USDC notional of the fill

Two data sources are supported, tried in order:
  1. Etherscan V2 API  (free API key from polygonscan.com, 3 req/s, 100k/day)
     Falls back to native Polygonscan API for block-number lookups.
  2. Direct JSON-RPC   (any Polygon RPC endpoint, public or paid)

Usage::

    fetcher = PolygonTickFetcher(
        rpc_url="https://polygon-mainnet.g.alchemy.com/v2/<key>",
        polygonscan_key="<your-free-polygonscan-key>",   # optional
    )
    ticks = fetcher.get_ticks_for_market(market, start_ts=1772137800, end_ts=1772138100)
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .config import (
    CTF_EXCHANGE_ADDRESS,
    ORDER_FILLED_TOPIC,
    BLOCK_TIME_SECONDS,
    ETHERSCAN_V2_API,
    POLYGONSCAN_NATIVE_API,
    POLYGON_CHAIN_ID,
)
from .models import MarketRecord


def _token_to_hex_topic(token_id: str) -> str:
    """Pad a 256-bit integer token ID to a 32-byte hex topic."""
    return "0x" + hex(int(token_id))[2:].zfill(64)


class PolygonTickFetcher:
    """Fetches individual trade fills for Polymarket outcome tokens from Polygon.

    Parameters
    ----------
    rpc_url:
        A Polygon JSON-RPC endpoint URL.  Can be a free public URL or your
        own Alchemy / QuickNode endpoint.  Examples:
          - "https://polygon-mainnet.g.alchemy.com/v2/<key>"
          - "https://polygon-bor-rpc.publicnode.com"
    polygonscan_key:
        Optional free API key from https://polygonscan.com/myapikey.
        When provided, Polygonscan's getLogs API is used first (more reliable
        than most public JSON-RPC nodes for historical log queries).
    timeout:
        HTTP timeout in seconds.
    """

    POLYGONSCAN_LOG_LIMIT = 1_000   # max results per Etherscan page
    POLYGONSCAN_BLOCK_CHUNK = 25    # blocks per getLogs query (CTF is very active)
    RPC_LOG_CHUNK_BLOCKS  = 10      # block range per eth_getLogs call (Alchemy free tier limit)

    # Minimum gap between any two Etherscan V2 API calls (3 req/s free plan)
    _ETHERSCAN_MIN_INTERVAL = 0.42   # slightly >1/3 s to stay safely below 3 req/s

    def __init__(
        self,
        rpc_url: str | None = None,
        polygonscan_key: str | None = None,
        timeout: int = 30,
        logger: logging.Logger | None = None,
    ) -> None:
        self.rpc_url = rpc_url
        self.polygonscan_key = polygonscan_key
        self.timeout = timeout
        self.logger = logger or logging.getLogger("polymarket_pipeline.ticks")
        self._session = requests.Session()
        self._block_cache: dict[int, int] = {}  # ts → block number
        self._last_etherscan_ts: float = 0.0    # global rate-limit tracker

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_ticks_for_markets_batch(
        self,
        markets: list[MarketRecord],
        start_ts: int,
        end_ts: int,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch logs once for a shared block range and distribute to multiple markets.

        More efficient than calling get_ticks_for_market() separately when several
        markets share the same time window (e.g. BTC/ETH/SOL at the same 5-min slot),
        because the expensive eth_getLogs query is issued only once.

        Returns a dict mapping market_id → list of tick dicts.
        """
        result: dict[str, list[dict[str, Any]]] = {m.market_id: [] for m in markets}

        # Build token_id → market lookup for O(1) dispatch
        token_to_market: dict[str, MarketRecord] = {}
        for m in markets:
            token_to_market[m.up_token_id] = m
            token_to_market[m.down_token_id] = m

        raw_logs = self._fetch_logs(start_ts, end_ts)
        if not raw_logs:
            return result

        for log in raw_logs:
            # Quick pre-filter: read the first two 32-byte words (maker/taker asset IDs)
            # before paying the cost of a full _decode_log call.
            data = log.get("data", "")[2:]
            if len(data) < 2 * 64:
                continue
            try:
                maker_asset = str(int(data[0:64], 16))
                taker_asset = str(int(data[64:128], 16))
            except (ValueError, IndexError):
                continue

            m = token_to_market.get(maker_asset) or token_to_market.get(taker_asset)
            if m is None:
                continue

            tick = self._decode_log(log, m, up_token=m.up_token_id, down_token=m.down_token_id)
            if tick is not None:
                result[m.market_id].append(tick)

        for mid, ticks in result.items():
            ticks.sort(key=lambda t: t["timestamp_ms"])
            self.logger.info(
                "On-chain ticks for market %s: %s fills (up=%s down=%s)",
                mid, len(ticks),
                sum(1 for t in ticks if t["outcome"] == "Up"),
                sum(1 for t in ticks if t["outcome"] == "Down"),
            )

        return result

    def get_ticks_for_market(
        self,
        market: MarketRecord,
        start_ts: int,
        end_ts: int,
    ) -> list[dict[str, Any]]:
        """Return a list of trade tick dicts for *both* outcome tokens.

        Each dict has keys:
            timestamp_ms, market_id, crypto, timeframe, token_id,
            outcome, side, price, size_usdc, tx_hash, block_number
        """
        up_token   = market.up_token_id
        down_token = market.down_token_id

        raw_logs = self._fetch_logs(start_ts, end_ts)
        if not raw_logs:
            self.logger.debug("No on-chain logs found for market %s", market.market_id)
            return []

        ticks: list[dict[str, Any]] = []
        for log in raw_logs:
            tick = self._decode_log(log, market, up_token=up_token, down_token=down_token)
            if tick is not None:
                ticks.append(tick)

        ticks.sort(key=lambda t: t["timestamp_ms"])
        self.logger.info(
            "On-chain ticks for market %s: %s fills (up=%s down=%s)",
            market.market_id,
            len(ticks),
            sum(1 for t in ticks if t["outcome"] == "Up"),
            sum(1 for t in ticks if t["outcome"] == "Down"),
        )
        return ticks

    # ------------------------------------------------------------------
    # Block timestamp → block number estimation
    # ------------------------------------------------------------------

    def _ts_to_block(self, target_ts: int) -> int | None:
        """Convert a Unix timestamp to the nearest Polygon block number.

        Results are cached so repeated lookups for the same timestamp
        (common when multiple markets share the same time window) are free.

        Tries Polygonscan ``getblocknobytime`` first (single HTTP call, exact),
        then falls back to a binary-search over JSON-RPC if an *rpc_url* is
        configured.
        """
        if target_ts in self._block_cache:
            return self._block_cache[target_ts]

        block = self._ts_to_block_uncached(target_ts)
        if block is not None:
            self._block_cache[target_ts] = block
        return block

    def _ts_to_block_uncached(self, target_ts: int) -> int | None:
        """Actual implementation of ts→block conversion (no cache)."""
        # 1. Polygonscan (preferred — one call, exact result)
        if self.polygonscan_key:
            block = self._ts_to_block_polygonscan(target_ts)
            if block is not None:
                return block

        # 2. RPC binary-search fallback
        current_block, current_ts = self._get_latest_block_info()
        if current_block is None:
            self.logger.warning("Could not fetch latest block; cannot estimate block number")
            return None

        secs_back = current_ts - target_ts
        if secs_back < 0:
            return current_block

        est = current_block - int(secs_back / BLOCK_TIME_SECONDS)

        # Narrow down with up to 20 binary-search steps
        low  = max(1, est - 500)
        high = min(current_block, est + 500)
        for _ in range(20):
            if low >= high:
                break
            mid = (low + high) // 2
            ts = self._get_block_timestamp(mid)
            if ts is None:
                break
            if ts < target_ts:
                low = mid + 1
            else:
                high = mid

        return low

    def _rate_limit_etherscan(self) -> None:
        """Block until at least _ETHERSCAN_MIN_INTERVAL seconds have passed since
        the last Etherscan V2 call, then update the timestamp.  All Etherscan V2
        requests must call this *before* issuing the HTTP request so that
        ``getblocknobytime`` and ``getLogs`` share a single global token bucket.
        """
        elapsed = time.time() - self._last_etherscan_ts
        if elapsed < self._ETHERSCAN_MIN_INTERVAL:
            time.sleep(self._ETHERSCAN_MIN_INTERVAL - elapsed)
        self._last_etherscan_ts = time.time()

    def _ts_to_block_polygonscan(self, target_ts: int) -> int | None:
        """Use the Etherscan V2 ``getblocknobytime`` endpoint to convert a Unix
        timestamp to the nearest Polygon block number.

        The native Polygonscan V1 API (api.polygonscan.com) is deprecated and
        returns an error on all endpoints; only Etherscan V2 is used here.
        """
        params = {
            "chainid":   POLYGON_CHAIN_ID,
            "module":    "block",
            "action":    "getblocknobytime",
            "timestamp": target_ts,
            "closest":   "before",
            "apikey":    self.polygonscan_key,
        }
        self._rate_limit_etherscan()
        data = self._etherscan_get(params, use_native=False)
        if data is not None:
            status = str(data.get("status"))
            result = str(data.get("result", ""))
            if status == "1" and result.isdigit():
                return int(result)
            self.logger.debug(
                "Etherscan V2 getblocknobytime: status=%s message=%s result=%s",
                status, data.get("message"), result[:200],
            )
        return None

    def _get_latest_block_info(self) -> tuple[int | None, int]:
        result = self._rpc("eth_getBlockByNumber", "latest", False)
        if result:
            return int(result["number"], 16), int(result["timestamp"], 16)
        return None, 0

    def _get_block_timestamp(self, block_num: int) -> int | None:
        result = self._rpc("eth_getBlockByNumber", hex(block_num), False)
        if result:
            return int(result["timestamp"], 16)
        return None

    # ------------------------------------------------------------------
    # Log fetching (Polygonscan preferred, direct RPC fallback)
    # ------------------------------------------------------------------

    def _fetch_logs(self, start_ts: int, end_ts: int) -> list[dict]:
        start_block = self._ts_to_block(start_ts)
        end_block   = self._ts_to_block(end_ts + 30)   # small buffer

        if start_block is None or end_block is None:
            self.logger.error("Could not estimate block range; no RPC available")
            return []

        # Add ±20 block buffer to absorb timestamp estimation errors (~44 s)
        start_block = max(1, start_block - 20)
        end_block   = end_block + 20

        self.logger.debug("Fetching logs for blocks %s–%s", start_block, end_block)

        if self.polygonscan_key:
            logs = self._fetch_logs_polygonscan(start_block, end_block)
            if logs is not None:
                return logs
            self.logger.warning("Polygonscan failed; falling back to JSON-RPC")

        return self._fetch_logs_rpc(start_block, end_block)

    _MAX_RETRIES = 3

    def _etherscan_get(self, params: dict, use_native: bool = False) -> dict | None:
        """Make an Etherscan/Polygonscan GET request with retry on transient errors.

        When *use_native* is True, queries the native Polygonscan API directly
        (``api.polygonscan.com/api``) instead of the unified Etherscan V2
        endpoint.  The ``chainid`` parameter is stripped for native calls.

        Returns the parsed JSON dict on success, or ``None`` after exhausting
        retries.
        """
        if use_native:
            url = POLYGONSCAN_NATIVE_API
            params = {k: v for k, v in params.items() if k != "chainid"}
        else:
            url = ETHERSCAN_V2_API

        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                if attempt < self._MAX_RETRIES:
                    wait = 2 ** attempt
                    self.logger.warning(
                        "Etherscan request to %s (attempt %d/%d), retrying in %ds: %s",
                        url, attempt, self._MAX_RETRIES, wait, exc,
                    )
                    time.sleep(wait)
                else:
                    self.logger.warning("Etherscan request to %s failed after %d retries: %s", url, self._MAX_RETRIES, exc)
            except Exception as exc:
                self.logger.warning("Etherscan request to %s failed: %s", url, exc)
                break
        return None

    def _fetch_logs_polygonscan(self, start_block: int, end_block: int) -> list[dict] | None:
        """Fetch OrderFilled logs via the Etherscan V2 API.

        The CTF Exchange contract emits events for ALL Polymarket trades, so
        even a small block range can contain thousands of logs.  The API
        rejects queries where the total result count exceeds 10 000, so
        we chunk the request into ``POLYGONSCAN_BLOCK_CHUNK``-block segments
        and paginate within each segment.

        The native Polygonscan V1 API (api.polygonscan.com) is deprecated and
        always returns an error, so only the Etherscan V2 endpoint is used.
        """
        return self._fetch_logs_polygonscan_endpoint(
            start_block, end_block, use_native=False, label="Etherscan V2",
        )

    def _fetch_logs_polygonscan_endpoint(
        self,
        start_block: int,
        end_block: int,
        *,
        use_native: bool = False,
        label: str = "Etherscan",
    ) -> list[dict] | None:
        """Fetch OrderFilled logs from a single Etherscan/Polygonscan endpoint."""
        all_logs: list[dict] = []
        cur = start_block

        while cur <= end_block:
            chunk_end = min(cur + self.POLYGONSCAN_BLOCK_CHUNK - 1, end_block)
            page = 1

            while True:
                params = {
                    "chainid":    POLYGON_CHAIN_ID,
                    "module":     "logs",
                    "action":     "getLogs",
                    "address":    CTF_EXCHANGE_ADDRESS,
                    "topic0":     ORDER_FILLED_TOPIC,
                    "fromBlock":  cur,
                    "toBlock":    chunk_end,
                    "page":       page,
                    "offset":     self.POLYGONSCAN_LOG_LIMIT,
                    "apikey":     self.polygonscan_key,
                }
                self._rate_limit_etherscan()
                data = self._etherscan_get(params, use_native=use_native)
                if data is None:
                    return None

                status = str(data.get("status"))
                if status != "1":
                    msg = data.get("message", "")
                    result_text = str(data.get("result", ""))[:200]
                    if msg == "No records found":
                        break
                    self.logger.warning(
                        "%s getLogs error (blocks %s–%s): %s (result: %s)",
                        label, cur, chunk_end, msg, result_text,
                    )
                    return None

                logs = data.get("result", [])
                all_logs.extend(logs)
                if len(logs) < self.POLYGONSCAN_LOG_LIMIT:
                    break
                page += 1
                # Etherscan hard cap: page * offset <= 10 000
                if page * self.POLYGONSCAN_LOG_LIMIT > 10_000:
                    self.logger.debug("%s pagination cap reached for blocks %s–%s", label, cur, chunk_end)
                    break

            cur = chunk_end + 1

        return all_logs

    def _fetch_logs_rpc(self, start_block: int, end_block: int) -> list[dict]:
        """Fetch OrderFilled logs via eth_getLogs in chunked block ranges."""
        if not self.rpc_url:
            self.logger.error("No RPC URL configured; cannot fetch on-chain ticks")
            return []

        all_logs: list[dict] = []
        chunk = self.RPC_LOG_CHUNK_BLOCKS
        cur = start_block

        while cur <= end_block:
            chunk_end = min(cur + chunk - 1, end_block)
            try:
                logs = self._rpc("eth_getLogs", {
                    "fromBlock": hex(cur),
                    "toBlock":   hex(chunk_end),
                    "address":   CTF_EXCHANGE_ADDRESS,
                    "topics":    [ORDER_FILLED_TOPIC],
                })
                if logs is None:
                    # All retries exhausted (e.g. Alchemy 503 rate limit).
                    # Back off before the next chunk so the provider can recover.
                    self.logger.warning(
                        "eth_getLogs returned no result for blocks %s–%s; "
                        "backing off 15 s before continuing.",
                        cur, chunk_end,
                    )
                    time.sleep(15)
                elif logs:
                    all_logs.extend(logs)
            except Exception as exc:
                self.logger.warning("eth_getLogs failed (blocks %s–%s): %s", cur, chunk_end, exc)
                time.sleep(15)
            cur = chunk_end + 1
            time.sleep(0.05)  # gentle pacing: ~20 req/s, well within Alchemy free tier

        return all_logs

    # ------------------------------------------------------------------
    # Log decoding
    # ------------------------------------------------------------------

    def _decode_log(
        self,
        log: dict,
        market: MarketRecord,
        up_token: str,
        down_token: str,
    ) -> dict[str, Any] | None:
        """Decode one OrderFilled log into a tick dict, or None if irrelevant."""
        try:
            # data = 5 × 32-byte words:
            #   [0] makerAssetId   [1] takerAssetId   [2] makerAmountFilled
            #   [3] takerAmountFilled   [4] fee
            data = log.get("data", "")[2:]
            if len(data) < 5 * 64:
                return None

            fields = [int(data[i * 64:(i + 1) * 64], 16) for i in range(5)]
            maker_asset, taker_asset, maker_amt_raw, taker_amt_raw, _ = fields

            maker_asset_s = str(maker_asset)
            taker_asset_s = str(taker_asset)

            if maker_asset_s == up_token:
                outcome, side = "Up", "SELL"   # maker sold Up → BUY from taker's view
                outcome_amt_raw = maker_amt_raw
                usdc_amt_raw    = taker_amt_raw
            elif taker_asset_s == up_token:
                outcome, side = "Up", "BUY"
                outcome_amt_raw = taker_amt_raw
                usdc_amt_raw    = maker_amt_raw
            elif maker_asset_s == down_token:
                outcome, side = "Down", "SELL"
                outcome_amt_raw = maker_amt_raw
                usdc_amt_raw    = taker_amt_raw
            elif taker_asset_s == down_token:
                outcome, side = "Down", "BUY"
                outcome_amt_raw = taker_amt_raw
                usdc_amt_raw    = maker_amt_raw
            else:
                return None   # trade is for a different market

            # Amounts are in units of 1e6 (USDC has 6 decimals)
            usdc_size    = usdc_amt_raw / 1_000_000
            outcome_size = outcome_amt_raw / 1_000_000

            if outcome_size <= 0 or usdc_size <= 0:
                return None

            price = usdc_size / outcome_size       # USDC per share = probability

            if not (0.001 <= price <= 0.999):
                return None

            # Block timestamp (Polygonscan returns decimal string; RPC returns hex)
            raw_ts = log.get("timeStamp") or log.get("timestamp", "0x0")
            if isinstance(raw_ts, str) and raw_ts.startswith("0x"):
                block_ts = int(raw_ts, 16)
            else:
                block_ts = int(raw_ts)

            block_num = log.get("blockNumber", "0x0")
            if isinstance(block_num, str) and block_num.startswith("0x"):
                block_num = int(block_num, 16)
            else:
                block_num = int(block_num)

            token_id = up_token if outcome == "Up" else down_token

            return {
                "timestamp_ms":  block_ts * 1000,   # block precision (Polygon ~2s)
                "market_id":     market.market_id,
                "crypto":        market.crypto,
                "timeframe":     market.timeframe,
                "token_id":      token_id,
                "outcome":       outcome,
                "side":          side,
                "price":         round(price, 6),
                "size_usdc":     round(usdc_size, 6),
                "tx_hash":       log.get("transactionHash", ""),
                "block_number":  block_num,
            }
        except Exception as exc:
            self.logger.debug("Failed to decode log: %s  log=%s", exc, str(log)[:120])
            return None

    # ------------------------------------------------------------------
    # JSON-RPC helper
    # ------------------------------------------------------------------

    def _rpc(self, method: str, *params: Any) -> Any:
        if not self.rpc_url:
            return None
        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                resp = self._session.post(
                    self.rpc_url,
                    json={"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)},
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                if "error" in data:
                    err = data["error"]
                    # Retry server-side JSON-RPC errors (code -32000..-32099 are server errors)
                    err_code = err.get("code", 0) if isinstance(err, dict) else 0
                    if -32099 <= err_code <= -32000 and attempt < self._MAX_RETRIES:
                        wait = 2 ** attempt
                        self.logger.warning(
                            "RPC server error %s (attempt %d/%d), retrying in %ds: %s",
                            method, attempt, self._MAX_RETRIES, wait, err,
                        )
                        time.sleep(wait)
                        continue
                    self.logger.warning("RPC error %s: %s", method, err)
                    return None
                return data.get("result")
            except requests.exceptions.RequestException as exc:
                if attempt < self._MAX_RETRIES:
                    wait = 2 ** attempt
                    self.logger.warning(
                        "RPC call %s (attempt %d/%d), retrying in %ds: %s",
                        method, attempt, self._MAX_RETRIES, wait, exc,
                    )
                    time.sleep(wait)
                else:
                    self.logger.warning("RPC call %s failed after %d retries: %s", method, self._MAX_RETRIES, exc)
            except Exception as exc:
                self.logger.warning("RPC call %s failed: %s", method, exc)
                break
        return None
