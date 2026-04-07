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
from collections.abc import Sequence
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
    # 1.2 s gives ~0.8 req/s, providing massive headroom for the
    # 3/sec cap to account for network jitter and multiple threads.
    _ETHERSCAN_MIN_INTERVAL = 1.2

    _MAX_RETRIES = 6

    # Minimum gap between eth_getLogs RPC calls on Alchemy free tier.
    # 0.3 s gives ~3.3 calls/s, which fits safely within Alchemy throughput.
    _RPC_LOGS_MIN_INTERVAL = 0.3

    def __init__(
        self,
        rpc_url: str | Sequence[str] | None = None,
        polygonscan_key: str | None = None,
        timeout: int = 30,
        logger: logging.Logger | None = None,
        prefer_rpc: bool = False,
        spot_price_lookup: SpotPriceLookup | None = None,
    ) -> None:
        # Final, absolute fix for multi-URL parsing:
        if isinstance(rpc_url, str):
            if "," in rpc_url:
                self._rpc_urls = [s.strip() for s in rpc_url.split(",") if s.strip()]
            else:
                self._rpc_urls = [rpc_url]
        elif rpc_url:
            self._rpc_urls = list(rpc_url)
        else:
            self._rpc_urls = []

        self._current_rpc_idx = 0
        self.polygonscan_key = polygonscan_key
        self.timeout = timeout
        self.logger = logger or logging.getLogger("polymarket_pipeline.ticks")
        self.prefer_rpc = prefer_rpc
        self.spot_price_lookup = spot_price_lookup
        self._session = requests.Session()
        self._block_cache: dict[int, int] = {}  # ts → block number
        self._last_etherscan_ts: float = time.monotonic()    # global rate-limit tracker
        self._last_rpc_logs_ts: float = time.monotonic()       # rate-limit tracker for eth_getLogs
        self._etherscan_exhausted: bool = False  # daily limit hit → skip until retry
        self._etherscan_exhausted_at: float = 0.0  # monotonic timestamp when limit was hit
        self._etherscan_call_count: int = 0      # total Etherscan API calls this session

        import threading
        self._etherscan_lock = threading.Lock()
        self._rpc_logs_lock = threading.Lock()
        self._block_cache_lock = threading.Lock()
        self._rpc_lock = threading.Lock()

        self.logger.info("PolygonTickFetcher initialized with %d RPC providers: %s", 
                         len(self._rpc_urls), ", ".join(self._masked_rpc_urls))

    @property
    def rpc_url(self) -> str | None:
        if not self._rpc_urls:
            return None
        return self._rpc_urls[self._current_rpc_idx]

    @property
    def _masked_rpc_urls(self) -> list[str]:
        masked = []
        for url in self._rpc_urls:
            idx = url.rfind("/")
            masked.append((url[: idx + 1] + "<key>") if idx >= 0 else "<rpc_url>")
        return masked

    def _rotate_rpc(self) -> str | None:
        """Switch to the next available RPC URL in the rotation."""
        with self._rpc_lock:
            if not self._rpc_urls or len(self._rpc_urls) <= 1:
                return self.rpc_url
            
            self._current_rpc_idx = (self._current_rpc_idx + 1) % len(self._rpc_urls)
            new_url = self.rpc_url
            self.logger.info("RPC limit reached; rotating to next provider: %s", self._masked_rpc_url)
            
            # Clear block cache on rotation just in case providers have different
            # view of the latest blocks or one is significantly lagging.
            with self._block_cache_lock:
                self._block_cache.clear()
            
            return new_url

    @property
    def _masked_rpc_url(self) -> str:
        """RPC URL with the API key replaced by <key> for safe logging."""
        current = self.rpc_url
        if not current:
            return "<no rpc_url>"
        idx = current.rfind("/")
        return (current[: idx + 1] + "<key>") if idx >= 0 else "<rpc_url>"

    def _sanitize_exc(self, exc: Exception) -> str:
        """Return str(exc) with any raw RPC URL masked."""
        msg = str(exc)
        for url in self._rpc_urls:
            if url in msg:
                idx = url.rfind("/")
                masked = (url[: idx + 1] + "<key>") if idx >= 0 else "<rpc_url>"
                msg = msg.replace(url, masked)
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
        # 1. Polygonscan (preferred — one call, exact result).
        #    Skipped when the daily limit is exhausted.
        if self.polygonscan_key and not self._etherscan_exhausted:
            for attempt in range(5):
                block = self._ts_to_block_polygonscan(target_ts)
                if block is not None:
                    return block
                if self._etherscan_exhausted:
                    break  # daily limit just detected — fall through to RPC
                if attempt < 4:
                    wait = 5 + (attempt * 5)
                    self.logger.warning("Etherscan block lookup failed; retrying in %ds before RPC fallback...", wait)
                    time.sleep(wait)

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
            elapsed = time.monotonic() - self._last_etherscan_ts
            if elapsed < self._ETHERSCAN_MIN_INTERVAL:
                time.sleep(self._ETHERSCAN_MIN_INTERVAL - elapsed)
            self._last_etherscan_ts = time.monotonic()

    def _rate_limit_rpc_logs(self) -> None:
        """Block until at least _RPC_LOGS_MIN_INTERVAL seconds have passed since
        the last eth_getLogs RPC call.  eth_getLogs is expensive on Alchemy
        (75 CU each) so must be paced separately from cheap calls like
        eth_getBlockByNumber (16 CU each).
        """
        with self._rpc_logs_lock:
            elapsed = time.monotonic() - self._last_rpc_logs_ts
            if elapsed < self._RPC_LOGS_MIN_INTERVAL:
                time.sleep(self._RPC_LOGS_MIN_INTERVAL - elapsed)
            self._last_rpc_logs_ts = time.monotonic()

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

        Source priority: Etherscan V2 first (reliable, exact results) with RPC
        as fallback.  When Etherscan's daily 100K-call limit is exhausted
        (``_etherscan_exhausted`` flag), all subsequent calls go straight to
        RPC with automatic Alchemy↔QuickNode rotation on rate limits.

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

        # Build source list: Etherscan first, RPC fallback.
        # When Etherscan is exhausted (daily limit) or prefer_rpc is set,
        # skip straight to RPC.
        sources: list[tuple[str, Callable]] = []
        etherscan_available = (
            self.polygonscan_key
            and not self._etherscan_exhausted
            and not self.prefer_rpc
        )
        if etherscan_available:
            sources.append(("Etherscan", self._fetch_logs_etherscan_v2_streamed))
        if self.rpc_url:
            sources.append(("RPC", self._fetch_logs_rpc_streamed))

        source_names = [s[0] for s in sources]
        self.logger.info(
            "Log source order: %s (etherscan_calls=%d, exhausted=%s)",
            " → ".join(source_names) if source_names else "NONE",
            self._etherscan_call_count,
            self._etherscan_exhausted,
        )
        # If prefer_rpc is False but Etherscan is exhausted, Etherscan
        # can still serve as a last-resort if RPC also fails and a new
        # day has started.  Not added here to keep things simple — the
        # exhausted flag resets on the next service invocation.

        for i, (name, fetch_func) in enumerate(sources):
            if i > 0 and not self._etherscan_exhausted:
                # Etherscan failed on a transient error (not daily limit).
                # Wait before falling back to give it a chance to recover.
                self.logger.warning("Primary source failed; waiting 60s before falling back to %s...", name)
                time.sleep(60)

            success = fetch_func(start_block, end_block, on_log)
            if success:
                return True
            if len(sources) > 1 and i < len(sources) - 1:
                self.logger.warning("%s stream failed; attempting fallback", name)

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

    # Re-probe Etherscan after this many seconds once the daily limit is hit.
    # Etherscan resets daily limits every 24h; 4h is a reasonable probe
    # interval for long-running backfills that span multiple days.
    _ETHERSCAN_REPROBE_INTERVAL_S = 4 * 3600

    def _etherscan_get(self, params: dict) -> dict | None:
        """Make an Etherscan V2 GET request with retry on transient errors.

        Sets ``_etherscan_exhausted`` when the daily call limit is detected,
        causing subsequent Etherscan calls to be skipped in favour of RPC.
        After ``_ETHERSCAN_REPROBE_INTERVAL_S`` seconds, the flag is cleared
        and Etherscan is re-probed — the daily limit may have reset.
        """
        if self._etherscan_exhausted:
            elapsed = time.monotonic() - self._etherscan_exhausted_at
            if elapsed < self._ETHERSCAN_REPROBE_INTERVAL_S:
                return None
            # Enough time passed — probe Etherscan to see if the daily limit reset.
            self._etherscan_exhausted = False
            self.logger.info(
                "Etherscan daily limit was hit %.1fh ago — re-probing to check if quota reset",
                elapsed / 3600,
            )

        url = ETHERSCAN_V2_API
        for attempt in range(1, self._MAX_RETRIES + 1):
            self._rate_limit_etherscan()
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                self._etherscan_call_count += 1
                data = resp.json()

                # Etherscan V2 returns 200 with status "0" for both per-second
                # rate limits and daily quota exhaustion.
                if data.get("status") == "0":
                    result_str = str(data.get("result", "")).lower()
                    message_str = str(data.get("message", "")).lower()
                    combined = result_str + " " + message_str

                    # Daily limit: only when the message explicitly mentions
                    # "daily" or "per day".  The generic "Max rate limit
                    # reached" message is a PER-SECOND throttle — not the
                    # 100K daily quota — and must fall through to the
                    # transient retry branch below.
                    is_daily = "daily" in combined or "per day" in combined
                    if is_daily:
                        self._etherscan_exhausted = True
                        self._etherscan_exhausted_at = time.monotonic()
                        self.logger.warning(
                            "Etherscan daily limit exhausted after ~%d calls this session — "
                            "switching to RPC for remaining tick fetches",
                            self._etherscan_call_count,
                        )
                        return None

                    # Per-second rate limit — transient, retry with backoff
                    if "rate limit" in combined or "max rate" in combined or "max calls" in combined:
                        if attempt < self._MAX_RETRIES:
                            wait = min(2**attempt + 5, 30)
                            self.logger.warning("Etherscan V2 rate limit (3/sec), retrying in %ds...", wait)
                            time.sleep(wait)
                            continue
                        # Exhausted retries on transient rate limit.  Do NOT
                        # set _etherscan_exhausted — with only a few hundred
                        # calls this is not the 100K daily limit.  Return None
                        # to fail this single request (stream falls back to
                        # RPC for this block range) but keep Etherscan
                        # available for subsequent calls.
                        self.logger.warning(
                            "Etherscan rate limit persists after %d retries (~%d calls this session) — "
                            "falling back to RPC for this request only",
                            self._MAX_RETRIES,
                            self._etherscan_call_count,
                        )
                        return None

                return data
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status in (429, 503) and attempt < self._MAX_RETRIES:
                    wait = min(2 ** attempt + 10, 60)
                    self.logger.warning("Etherscan %d (attempt %d/%d), retrying in %ds...", status, attempt, self._MAX_RETRIES, wait)
                    time.sleep(wait)
                    continue
                break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                if attempt < self._MAX_RETRIES:
                    wait = 2 ** attempt
                    msg = str(exc)
                    if self.polygonscan_key and self.polygonscan_key in msg:
                        msg = msg.replace(self.polygonscan_key, "<key>")
                    self.logger.warning("Etherscan attempt %d/%d failed, retrying in %ds: %s", attempt, self._MAX_RETRIES, wait, msg)
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
            self.logger.debug("_decode_log failed for tx=%s", log.get("transactionHash", "?"), exc_info=True)
            return None

    # ------------------------------------------------------------------
    # JSON-RPC helper
    # ------------------------------------------------------------------

    def _rpc(self, method: str, *params: Any) -> Any:
        if not self.rpc_url:
            return None
        # Outer loop caps provider rotations to prevent unbounded recursion
        # when all providers are rate-limited simultaneously.
        max_rotations = len(self._rpc_urls)
        for _rotation in range(max_rotations + 1):
            for attempt in range(1, self._MAX_RETRIES + 1):
                try:
                    resp = self._session.post(
                        self.rpc_url,
                        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)},
                        timeout=self.timeout,
                    )

                    # --- Rate-limit handling (429 / 503) ----------------------------
                    if resp.status_code in (429, 503):
                        if len(self._rpc_urls) > 1:
                            self.logger.warning(
                                "RPC %s hit rate limit (HTTP %s); rotating to next provider...",
                                method, resp.status_code
                            )
                            self._rotate_rpc()
                            break  # break inner loop, continue outer rotation loop

                        retry_after_raw = resp.headers.get("Retry-After")
                        try:
                            wait = max(float(retry_after_raw), 10.0) if retry_after_raw else (2 ** attempt * 5.0 + 5.0)
                        except (TypeError, ValueError):
                            wait = 2 ** attempt * 5.0 + 5.0
                        if attempt < self._MAX_RETRIES:
                            self.logger.warning(
                                "RPC %s rate-limited (HTTP %s), retrying in %.1fs: %s",
                                method, resp.status_code, wait, resp.text[:120],
                            )
                            if method == "eth_getLogs":
                                self._last_rpc_logs_ts = time.monotonic() + wait
                            time.sleep(wait)
                            continue
                        return None

                    if resp.status_code == 400:
                        self.logger.error("RPC 400: %s | URL: %s", resp.text[:500], self._masked_rpc_url)
                        return None

                    # 413 = response too large for this provider (e.g. block
                    # range exceeds QuickNode's limit).  Return None so the
                    # caller (_fetch_logs_rpc_streamed) can halve the block
                    # range and retry on the SAME provider with a smaller
                    # request, rather than rotating to a provider that may
                    # be rate-limited.
                    if resp.status_code == 413:
                        self.logger.warning(
                            "RPC %s returned 413 Request Entity Too Large; URL: %s",
                            method, self._masked_rpc_url,
                        )
                        return None

                    resp.raise_for_status()
                    data = resp.json()
                    if "error" in data:
                        err = data["error"]
                        err_code = err.get("code", 0) if isinstance(err, dict) else 0
                        err_msg = str(err).lower()

                        # --- JSON-RPC Level Rate Limiting (e.g. Alchemy Monthly Limit) ---
                        if err_code == 429 or "capacity limit" in err_msg or "rate limit" in err_msg:
                            if len(self._rpc_urls) > 1:
                                self.logger.warning(
                                    "RPC %s returned capacity/rate error (code %s); rotating to next provider...",
                                    method, err_code
                                )
                                self._rotate_rpc()
                                break  # break inner loop, continue outer rotation loop

                        # Retry server-side JSON-RPC errors
                        if -32099 <= err_code <= -32000 and attempt < self._MAX_RETRIES:
                            wait = 2 ** attempt
                            self.logger.warning("RPC server error %s, retrying in %ds: %s", method, wait, err)
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
                    return None
            else:
                # Inner loop exhausted without a break (no rotation needed)
                return None
        self.logger.warning("RPC call %s failed: all %d providers rate-limited", method, len(self._rpc_urls))
        return None
