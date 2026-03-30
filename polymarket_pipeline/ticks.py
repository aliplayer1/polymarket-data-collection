"""On-chain tick data fetcher for Polymarket prediction markets.

Every trade fill on Polymarket is settled on Polygon as an ERC-1155 transfer
emitted by the CTF Exchange contract (OrderFilled event).  This module queries
those events to reconstruct a per-market trade tick series with:

  timestamp_ms  -  block timestamp converted to milliseconds
  token_id      -  ERC-1155 outcome token (maps to the market's binary sides)
  outcome       -  market outcome label (e.g. "Up", "Down", "Yes", "No")
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
from typing import Any, Callable

import requests

from .config import (
    CTF_EXCHANGE_ADDRESS,
    ORDER_FILLED_TOPIC,
    BLOCK_TIME_SECONDS,
    ETHERSCAN_V2_API,
    POLYGON_CHAIN_ID,
)
from .phases.shared import build_binary_tick_row
from .phases.binance_history import SpotPriceLookup
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
    # eth_getLogs chunk size for the RPC path.
    # Alchemy enforces a 10 000-result cap per call.  The CTF Exchange emits
    # OrderFilled events for *all* Polymarket markets, so the aggregate rate is
    # ~700–1 500 events/block.  25 blocks ≈ 17 000–37 500 events at peak, which
    # can exceed the cap.  We use 10 blocks as a safe static default for the
    # Alchemy Free tier (which enforces a strict 10-block range limit).
    RPC_LOG_CHUNK_BLOCKS  = 10

    # Minimum gap between any two Etherscan V2 API calls (3 req/s limit).
    # 0.4 s gives 2.5 req/s, providing safety headroom for the 3/sec cap
    _ETHERSCAN_MIN_INTERVAL = 0.4

    # Minimum gap between eth_getLogs RPC calls on Alchemy free tier.
    # 0.2 s gives 5 calls/s, which fits within Alchemy free tier throughput.
    _RPC_LOGS_MIN_INTERVAL = 0.2

    def __init__(
        self,
        rpc_url: str | None = None,
        polygonscan_key: str | None = None,
        timeout: int = 30,
        logger: logging.Logger | None = None,
        prefer_rpc: bool = False,
        spot_price_lookup: SpotPriceLookup | None = None,
    ) -> None:
        self.rpc_url = rpc_url
        self.polygonscan_key = polygonscan_key
        self.timeout = timeout
        self.logger = logger or logging.getLogger("polymarket_pipeline.ticks")
        self.prefer_rpc = prefer_rpc
        self.spot_price_lookup = spot_price_lookup
        self._session = requests.Session()
        self._block_cache: dict[int, int] = {}  # ts → block number
        self._last_etherscan_ts: float = 0.0    # global rate-limit tracker
        self._last_rpc_logs_ts: float = 0.0       # rate-limit tracker for eth_getLogs

        import threading
        self._etherscan_lock = threading.Lock()
        self._rpc_logs_lock = threading.Lock()
        self._block_cache_lock = threading.Lock()

    @property
    def _masked_rpc_url(self) -> str:
        """RPC URL with the API key replaced by <key> for safe logging.

        Alchemy URLs have the form ``.../v2/<key>``; we mask everything after
        the last ``/`` so the key never appears in log files.
        """
        if not self.rpc_url:
            return "<no rpc_url>"
        idx = self.rpc_url.rfind("/")
        return (self.rpc_url[: idx + 1] + "<key>") if idx >= 0 else "<rpc_url>"

    def _sanitize_exc(self, exc: Exception) -> str:
        """Return str(exc) with the raw RPC URL (containing API key) masked."""
        msg = str(exc)
        if self.rpc_url and self.rpc_url in msg:
            msg = msg.replace(self.rpc_url, self._masked_rpc_url)
        return msg

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
        markets_by_id = {m.market_id: m for m in markets}

        # Build token_id → market lookup for O(1) dispatch
        token_to_market: dict[str, MarketRecord] = {}
        for m in markets:
            for token_id in m.token_ids:
                if token_id:
                    token_to_market[token_id] = m

        def log_processor(log: dict) -> None:
            # Quick pre-filter: read the first two 32-byte words (maker/taker asset IDs)
            data = log.get("data", "")[2:]
            if len(data) < 2 * 64:
                return
            try:
                maker_asset = str(int(data[0:64], 16))
                taker_asset = str(int(data[64:128], 16))
            except (ValueError, IndexError):
                return

            m = token_to_market.get(maker_asset) or token_to_market.get(taker_asset)
            if m is None:
                return

            tick = self._decode_log(log, m)
            if tick is not None:
                result[m.market_id].append(tick)

        self._fetch_logs_streamed(start_ts, end_ts, log_processor)

        for mid, ticks in result.items():
            market = markets_by_id[mid]
            ticks.sort(key=lambda t: t["timestamp_ms"])
            if ticks:
                self.logger.info(
                    "On-chain ticks for market %s (%s/%s): %s fills",
                    mid,
                    market.crypto,
                    market.timeframe,
                    len(ticks),
                )

        return result

    def get_ticks_for_market(
        self,
        market: MarketRecord,
        start_ts: int,
        end_ts: int,
    ) -> list[dict[str, Any]]:
        """Return a list of trade tick dicts for all of the market's outcome tokens."""
        ticks: list[dict[str, Any]] = []

        def log_processor(log: dict) -> None:
            tick = self._decode_log(log, market)
            if tick is not None:
                ticks.append(tick)

        self._fetch_logs_streamed(start_ts, end_ts, log_processor)
        ticks.sort(key=lambda t: t["timestamp_ms"])
        
        if ticks:
            self.logger.info(
                "On-chain ticks for market %s (%s/%s): %s fills",
                market.market_id,
                market.crypto,
                market.timeframe,
                len(ticks),
            )
        return ticks

    # ------------------------------------------------------------------
    # Block timestamp → block number estimation
    # ------------------------------------------------------------------

    def _ts_to_block(self, target_ts: int) -> int | None:
        with self._block_cache_lock:
            if target_ts in self._block_cache:
                return self._block_cache[target_ts]

        block = self._ts_to_block_uncached(target_ts)
        if block is not None:
            with self._block_cache_lock:
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
        with self._etherscan_lock:
            elapsed = time.time() - self._last_etherscan_ts
            if elapsed < self._ETHERSCAN_MIN_INTERVAL:
                time.sleep(self._ETHERSCAN_MIN_INTERVAL - elapsed)
            self._last_etherscan_ts = time.time()

    def _rate_limit_rpc_logs(self) -> None:
        """Block until at least _RPC_LOGS_MIN_INTERVAL seconds have passed since
        the last eth_getLogs RPC call.  eth_getLogs is expensive on Alchemy
        (75 CU each) so must be paced separately from cheap calls like
        eth_getBlockByNumber (16 CU each).
        """
        with self._rpc_logs_lock:
            elapsed = time.time() - self._last_rpc_logs_ts
            if elapsed < self._RPC_LOGS_MIN_INTERVAL:
                time.sleep(self._RPC_LOGS_MIN_INTERVAL - elapsed)
            self._last_rpc_logs_ts = time.time()

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
        data = self._etherscan_get(params)
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

    def _fetch_logs_streamed(self, start_ts: int, end_ts: int, on_log: Callable[[dict], None]) -> bool:
        """Fetch OrderFilled logs for a time range and stream them to the callback.

        Handles block number estimation and dispatches to Etherscan V2 or RPC.
        Returns True on success, False if one or more chunks failed permanently.
        """
        start_block = self._ts_to_block(start_ts)
        end_block = self._ts_to_block(end_ts + 30)

        if start_block is None or end_block is None:
            self.logger.error("Could not estimate block range for on-chain ticks")
            return False

        # Add ±20 block buffer for timestamp jitter
        start_block = max(1, start_block - 20)
        end_block = end_block + 20

        self.logger.debug("Streaming logs for blocks %s–%s", start_block, end_block)

        # Order of sources depends on prefer_rpc
        sources = []
        if self.prefer_rpc:
            if self.rpc_url:
                sources.append(("RPC", self._fetch_logs_rpc_streamed))
            if self.polygonscan_key:
                sources.append(("Polygonscan", self._fetch_logs_etherscan_v2_streamed))
        else:
            if self.polygonscan_key:
                sources.append(("Polygonscan", self._fetch_logs_etherscan_v2_streamed))
            if self.rpc_url:
                sources.append(("RPC", self._fetch_logs_rpc_streamed))

        for name, fetch_func in sources:
            success = fetch_func(start_block, end_block, on_log)
            if success:
                return True
            if len(sources) > 1:
                self.logger.warning("%s stream failed; trying next source", name)

        self.logger.error("No valid log source (RPC/Polygonscan) available for streaming")
        return False

    def _fetch_logs_etherscan_v2_streamed(
        self,
        start_block: int,
        end_block: int,
        on_log: Callable[[dict], None],
    ) -> bool:
        cur = start_block
        while cur <= end_block:
            chunk_end = min(cur + self.POLYGONSCAN_BLOCK_CHUNK - 1, end_block)
            page = 1
            while True:
                params = {
                    "chainid": POLYGON_CHAIN_ID,
                    "module": "logs",
                    "action": "getLogs",
                    "address": CTF_EXCHANGE_ADDRESS,
                    "topic0": ORDER_FILLED_TOPIC,
                    "fromBlock": cur,
                    "toBlock": chunk_end,
                    "page": page,
                    "offset": self.POLYGONSCAN_LOG_LIMIT,
                    "apikey": self.polygonscan_key,
                }
                self._rate_limit_etherscan()
                data = self._etherscan_get(params)
                if data is None:
                    return False

                status = str(data.get("status"))
                if status != "1":
                    msg = data.get("message", "")
                    if msg == "No records found":
                        break
                    return False

                logs = data.get("result", [])
                for log in logs:
                    on_log(log)

                if len(logs) < self.POLYGONSCAN_LOG_LIMIT:
                    break
                page += 1
                if page * self.POLYGONSCAN_LOG_LIMIT > 10_000:
                    break
            cur = chunk_end + 1
        return True

    def _fetch_logs_rpc_streamed(
        self,
        start_block: int,
        end_block: int,
        on_log: Callable[[dict], None],
    ) -> bool:
        cur = start_block
        had_failure = False
        while cur <= end_block:
            chunk = self.RPC_LOG_CHUNK_BLOCKS
            for _ in range(4):
                chunk_end = min(cur + chunk - 1, end_block)
                self._rate_limit_rpc_logs()
                try:
                    logs = self._rpc("eth_getLogs", {
                        "fromBlock": hex(cur),
                        "toBlock": hex(chunk_end),
                        "address": CTF_EXCHANGE_ADDRESS,
                        "topics": [ORDER_FILLED_TOPIC],
                    })
                except Exception:
                    logs = None

                if logs is None:
                    if chunk > 1:
                        chunk = max(1, chunk // 2)
                        continue
                    had_failure = True
                    chunk_end = cur
                    break

                for log in logs:
                    on_log(log)
                break
            cur = chunk_end + 1
        return not had_failure

    _MAX_RETRIES = 3

    def _etherscan_get(self, params: dict) -> dict | None:
        """Make an Etherscan V2 GET request with retry on transient errors."""
        url = ETHERSCAN_V2_API
        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                data = resp.json()
                if data.get("status") == "0" and "rate limit" in str(data.get("result", "")).lower():
                    if attempt < self._MAX_RETRIES:
                        wait = 2**attempt + 2
                        self.logger.warning("Etherscan V2 rate limit hit (3/sec), retrying in %ds", wait)
                        time.sleep(wait)
                        continue
                return data
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status in (429, 503) and attempt < self._MAX_RETRIES:
                    wait = 2 ** attempt + 1
                    self.logger.warning("Etherscan %d (attempt %d/%d), retrying in %ds", status, attempt, self._MAX_RETRIES, wait)
                    time.sleep(wait)
                    continue
                break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                if attempt < self._MAX_RETRIES:
                    wait = 2 ** attempt
                    msg = str(exc)
                    if self.polygonscan_key and self.polygonscan_key in msg:
                        msg = msg.replace(self.polygonscan_key, "<key>")
                    self.logger.warning("Etherscan attempt %d/%d failed, retrying: %s", attempt, self._MAX_RETRIES, msg)
                    time.sleep(wait)
                else:
                    break
            except Exception:
                break
        return None

    def _decode_log(
        self,
        log: dict,
        market: MarketRecord,
    ) -> dict[str, Any] | None:
        """Decode one OrderFilled log into a tick dict, or None if irrelevant."""
        try:
            data = log.get("data", "")[2:]
            if len(data) < 5 * 64:
                return None

            fields = [int(data[i * 64:(i + 1) * 64], 16) for i in range(5)]
            maker_asset, taker_asset, maker_amt_raw, taker_amt_raw, _ = fields

            maker_asset_s = str(maker_asset)
            taker_asset_s = str(taker_asset)

            outcome_side = market.side_for_token_id(maker_asset_s)
            if outcome_side is not None:
                side = "SELL"
                outcome_amt_raw = maker_amt_raw
                usdc_amt_raw = taker_amt_raw
            else:
                outcome_side = market.side_for_token_id(taker_asset_s)
                if outcome_side is not None:
                    side = "BUY"
                    outcome_amt_raw = taker_amt_raw
                    usdc_amt_raw = maker_amt_raw
                else:
                    return None

            usdc_size = usdc_amt_raw / 1_000_000
            outcome_size = outcome_amt_raw / 1_000_000
            if outcome_size <= 0 or usdc_size <= 0:
                return None

            price = usdc_size / outcome_size
            if not (0.0 <= price <= 1.0):
                return None

            raw_ts = log.get("timeStamp") or log.get("timestamp", "0x0")
            block_ts = int(raw_ts, 16) if isinstance(raw_ts, str) and raw_ts.startswith("0x") else int(raw_ts)

            block_num = log.get("blockNumber", "0x0")
            block_num = int(block_num, 16) if isinstance(block_num, str) and block_num.startswith("0x") else int(block_num)

            log_idx = log.get("logIndex", "0x0")
            log_idx_int = int(log_idx, 16) if isinstance(log_idx, str) and log_idx.startswith("0x") else int(log_idx)

            token_id = market.token_id_for_side(outcome_side)
            tick_ts_ms = block_ts * 1000

            # Embed spot price from Binance historical lookup if available
            spot_kwargs: dict[str, Any] = {}
            if self.spot_price_lookup is not None:
                result = self.spot_price_lookup.get(market.crypto, tick_ts_ms)
                if result is not None:
                    spot_kwargs["spot_price_usdt"] = result[0]
                    spot_kwargs["spot_price_ts_ms"] = result[1]

            return build_binary_tick_row(
                market,
                timestamp_ms=tick_ts_ms,
                token_id=token_id,
                outcome_side=outcome_side,
                trade_side=side,
                price=round(price, 6),
                size_usdc=round(usdc_size, 6),
                tx_hash=str(log.get("transactionHash", "")),
                block_number=block_num,
                log_index=log_idx_int,
                source="onchain",
                **spot_kwargs,
            )
        except Exception:
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

                # --- Rate-limit handling (429 / 503) ----------------------------
                # Alchemy returns 429 for per-second CU overruns and 503 when the
                # node is temporarily overloaded.  Both responses may carry a
                # Retry-After header; if present we honour it exactly, otherwise
                # we use exponential back-off with a longer base than for generic
                # errors so that the rate limiter has time to refill.
                if resp.status_code in (429, 503):
                    retry_after_raw = resp.headers.get("Retry-After")
                    try:
                        wait = max(float(retry_after_raw), 1.0) if retry_after_raw else 2 ** attempt * 3
                    except (TypeError, ValueError):
                        wait = 2 ** attempt * 3
                    if attempt < self._MAX_RETRIES:
                        self.logger.warning(
                            "RPC call %s rate-limited (HTTP %s, attempt %d/%d), "
                            "retrying in %.1fs: %s",
                            method, resp.status_code, attempt, self._MAX_RETRIES, wait,
                            resp.text[:120],
                        )
                        # Reset the logs rate-limit clock so the next eth_getLogs
                        # call doesn't fire immediately after the back-off expires.
                        if method == "eth_getLogs":
                            self._last_rpc_logs_ts = time.time() + wait
                        time.sleep(wait)
                        continue
                    self.logger.warning(
                        "RPC call %s failed after %d retries: HTTP %s",
                        method, self._MAX_RETRIES, resp.status_code,
                    )
                    return None

                if resp.status_code == 400:
                    self.logger.error(
                        "RPC 400 Bad Request: %s | URL: %s",
                        resp.text[:500], self._masked_rpc_url,
                    )
                    # Return None immediately on 400 Client Error. These are typically
                    # permanent parameter errors (like block range too large) where
                    # retrying the exact same request is futile.
                    return None

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
                safe_exc = self._sanitize_exc(exc)
                if attempt < self._MAX_RETRIES:
                    wait = 2 ** attempt
                    self.logger.warning(
                        "RPC call %s (attempt %d/%d), retrying in %ds: %s",
                        method, attempt, self._MAX_RETRIES, wait, safe_exc,
                    )
                    time.sleep(wait)
                else:
                    self.logger.warning("RPC call %s failed after %d retries: %s", method, self._MAX_RETRIES, safe_exc)
            except Exception as exc:
                self.logger.warning("RPC call %s failed: %s", method, self._sanitize_exc(exc))
                break
        return None
