"""BinanceHistoryPhase — back-fills spot prices from Binance klines.

During the historical pipeline run, markets are processed for closed time
windows where no live RTDS data is available.  This phase fetches 1-minute
Binance klines (candlestick data) for the time ranges of those markets,
producing two outputs:

1. **spot_prices Parquet rows** — persisted to ``data/spot_prices/`` so the
   continuous BTC/ETH/SOL price time series is available in the dataset.
2. **A spot price lookup** — an in-memory dict keyed by ``(crypto, minute)``
   for O(1) embedding into historical on-chain tick rows.

Binance REST API (no key required for klines):
    GET https://api.binance.com/api/v3/klines
    ?symbol=BTCUSDT&interval=1m&startTime={ms}&endTime={ms}&limit=1000

Rate limit: 6000 request weight/min (each /klines with limit>500 costs 2).
1-minute klines for a 5-minute market need only 1 request (5 candles).
With range merging, thousands of markets produce only hundreds of requests.
"""
from __future__ import annotations

import bisect
import logging
import time
from typing import Any

import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from ..models import MarketRecord

# Binance symbol mapping from pipeline crypto codes
CRYPTO_TO_BINANCE: dict[str, str] = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "BNB": "BNBUSDT",
    "XRP": "XRPUSDT",
    "DOGE": "DOGEUSDT",
    "HYPE": "HYPEUSDT",
}

# Mapping from Binance symbol to Chainlink RTDS symbol format.
# Used to store Binance prices as a Chainlink proxy for historical data
# where live RTDS Chainlink data is unavailable.
_BINANCE_TO_CHAINLINK_SYMBOL: dict[str, str] = {
    "btcusdt": "btc/usd",
    "ethusdt": "eth/usd",
    "solusdt": "sol/usd",
    "bnbusdt": "bnb/usd",
    "xrpusdt": "xrp/usd",
    "dogeusdt": "doge/usd",
    "hypeusdt": "hype/usd",
}

_BINANCE_KLINE_URL = "https://api.binance.com/api/v3/klines"
_KLINE_LIMIT = 1000  # max candles per request
_MIN_REQUEST_INTERVAL = 0.035  # 35ms → ~28 req/s ≈ 3360 weight/min (56% of 6000 cap)


class SpotPriceLookup:
    """O(1)-amortised lookup of the nearest Binance spot price for a timestamp.

    Internally stores a sorted list of ``(ts_ms, price)`` tuples per crypto
    and uses binary search (bisect) for nearest-timestamp lookup.
    """

    def __init__(self) -> None:
        # {crypto: [(ts_ms, price), ...]} — sorted by ts_ms
        self._data: dict[str, list[tuple[int, float]]] = {}

    def add(self, crypto: str, ts_ms: int, price: float) -> None:
        if crypto not in self._data:
            self._data[crypto] = []
        self._data[crypto].append((ts_ms, price))

    def finalize(self) -> None:
        """Sort all arrays after bulk insertion.  Must be called before lookup."""
        for crypto in self._data:
            self._data[crypto].sort()

    def get(self, crypto: str, ts_ms: int) -> tuple[float, int] | None:
        """Return ``(price, price_ts_ms)`` for the latest entry whose
        ``price_ts_ms <= ts_ms``, or ``None`` if no such entry exists.

        Point-in-time correct: never returns a price observed in the
        future.  Earlier this helper used nearest-neighbour bisect, which
        — even after the ``open_time → close_time`` keying fix
        (commit a2eb3bc8) — could still return a future kline's
        close_price for a query landing inside an in-progress minute
        (e.g. at ``open_time + 30s`` the *current* minute hadn't
        closed yet but its close_time was the nearest stored entry).
        That residual ~30s leak corrupted ``spot_price_usdt`` embedded
        into tick rows by ``subgraph_ticks.py``.

        The strict ``<=`` semantics mean queries before the first
        observed close return ``None``, which downstream code maps to
        a NULL ``spot_price_usdt`` — strictly better than a wrong
        forward-looking value.
        """
        arr = self._data.get(crypto)
        if not arr:
            return None
        # ``bisect_right(arr, (ts_ms, +inf))`` puts us *just past* every
        # entry whose ts_ms == ts_ms (and after every entry strictly
        # less).  ``idx - 1`` is therefore the largest entry with
        # ``price_ts_ms <= ts_ms``.  Using +inf in the second tuple
        # field guarantees the comparison is purely on ts_ms regardless
        # of stored prices.
        idx = bisect.bisect_right(arr, (ts_ms, float("inf"))) - 1
        if idx < 0:
            return None
        price_ts_ms, price = arr[idx]
        return price, price_ts_ms

    def __len__(self) -> int:
        return sum(len(v) for v in self._data.values())


def _merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping or adjacent time ranges."""
    if not ranges:
        return []
    sorted_ranges = sorted(ranges)
    merged = [sorted_ranges[0]]
    for start, end in sorted_ranges[1:]:
        if start <= merged[-1][1] + 60_000:  # adjacent within 1 minute
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


class BinanceHistoryPhase:
    """Fetches historical Binance klines and populates the spot_prices table.

    Usage::

        phase = BinanceHistoryPhase(logger=log)
        lookup = phase.run(markets, spot_prices_dir="data/spot_prices")
        # lookup.get("BTC", timestamp_ms) → (price, price_ts_ms) or None
    """

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        timeout: int = 20,
    ) -> None:
        self.logger = logger or logging.getLogger("polymarket_pipeline.binance_history")
        self.timeout = timeout
        self._session = requests.Session()
        self._last_request_ts: float = time.monotonic()

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_ts = time.monotonic()

    def _fetch_klines(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
    ) -> list[list]:
        """Fetch 1-minute klines from Binance, handling pagination."""
        all_klines: list[list] = []
        current_start = start_ms

        while current_start < end_ms:
            self._rate_limit()
            try:
                resp = self._session.get(
                    _BINANCE_KLINE_URL,
                    params={
                        "symbol": symbol,
                        "interval": "1m",
                        "startTime": current_start,
                        "endTime": end_ms,
                        "limit": _KLINE_LIMIT,
                    },
                    timeout=self.timeout,
                )
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", "10"))
                    self.logger.warning(
                        "Binance rate limit hit, waiting %.1fs", retry_after
                    )
                    time.sleep(retry_after)
                    continue
                if resp.status_code == 418:
                    # HTTP 418 = IP-banned for repeatedly violating
                    # request-weight rate limits.  Treat like 429 but
                    # with a longer cool-off (Binance bans escalate
                    # from minutes up to days) and obey Retry-After
                    # if the response provided one.
                    retry_after = float(resp.headers.get("Retry-After", "300"))
                    self.logger.error(
                        "Binance HTTP 418 IP ban, sleeping %.0fs", retry_after,
                    )
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                klines = resp.json()
            except Exception as exc:
                self.logger.warning(
                    "Binance kline fetch failed for %s [%s-%s]: %s",
                    symbol, current_start, end_ms, exc,
                )
                break

            if not klines:
                break

            all_klines.extend(klines)

            # Each kline: [open_time, open, high, low, close, volume, close_time, ...]
            last_close_time = int(klines[-1][6])
            if last_close_time >= end_ms or len(klines) < _KLINE_LIMIT:
                break
            current_start = last_close_time + 1

        return all_klines

    def run(
        self,
        markets: list[MarketRecord],
        *,
        spot_prices_dir: str,
    ) -> SpotPriceLookup:
        """Fetch Binance klines for all market time ranges.

        Returns a ``SpotPriceLookup`` for embedding prices into ticks.
        Also persists the data to the spot_prices Parquet table.
        """
        lookup = SpotPriceLookup()

        if not markets:
            return lookup

        # Group markets by crypto and collect time ranges
        ranges_by_crypto: dict[str, list[tuple[int, int]]] = {}
        for market in markets:
            crypto = market.crypto
            if crypto not in CRYPTO_TO_BINANCE:
                continue
            if crypto not in ranges_by_crypto:
                ranges_by_crypto[crypto] = []
            # Convert to milliseconds
            ranges_by_crypto[crypto].append(
                (market.start_ts * 1000, market.end_ts * 1000)
            )

        total_rows = 0

        for crypto, ranges in ranges_by_crypto.items():
            binance_symbol = CRYPTO_TO_BINANCE[crypto]
            merged = _merge_ranges(ranges)
            self.logger.info(
                "Binance history: fetching %s klines for %d merged range(s) "
                "(from %d markets)",
                binance_symbol,
                len(merged),
                len(ranges),
            )

            spot_rows: list[dict[str, Any]] = []

            for range_start, range_end in merged:
                klines = self._fetch_klines(binance_symbol, range_start, range_end)
                rtds_symbol = binance_symbol.lower()
                chainlink_symbol = _BINANCE_TO_CHAINLINK_SYMBOL.get(rtds_symbol)
                for kline in klines:
                    # kline format: [open_time, open, high, low, close, volume, close_time, ...]
                    # close_price is the last trade in [open_time, close_time]; it is
                    # only observable AT close_time. Keying it on open_time would
                    # leak up-to-60s of future BTC into any consumer that joins on
                    # ts_ms (e.g. subgraph_to_csv interpolation, hf_to_csv spot
                    # lookup), corrupting Optuna training fold by point-in-time
                    # violation.
                    close_time = int(kline[6])
                    close_price = float(kline[4])

                    lookup.add(crypto, close_time, close_price)
                    spot_rows.append({
                        "ts_ms": close_time,
                        "symbol": rtds_symbol,
                        "price": close_price,
                        "source": "binance",
                    })
                    # Emit a Chainlink proxy row so the converter can derive
                    # chainlink_price / chainlink_stale_ms for historical data
                    # where live RTDS Chainlink is unavailable.
                    if chainlink_symbol:
                        spot_rows.append({
                            "ts_ms": close_time,
                            "symbol": chainlink_symbol,
                            "price": close_price,
                            "source": "chainlink_proxy",
                        })

            if spot_rows:
                # Write directly as an independent shard file instead of merging
                # into ws_staging.parquet.  This avoids the read-merge-write cycle
                # under the write lock, preventing lock contention with the live
                # WebSocket service that also stages spot prices.
                df = pd.DataFrame(spot_rows)
                df["ts_ms"] = df["ts_ms"].astype("int64")
                df["price"] = df["price"].astype("float64")
                df["source"] = df["source"].astype("string")
                os.makedirs(spot_prices_dir, exist_ok=True)
                # Include a monotonic millisecond timestamp in the
                # shard name so a same-process re-invocation (e.g. two
                # historical phases in one CLI run) does not overwrite
                # the prior shard via ``os.replace``.  PID alone was
                # insufficient.
                shard_name = (
                    f"binance_history_{crypto}_{os.getpid()}_{int(time.time() * 1000)}.parquet"
                )
                shard_path = os.path.join(spot_prices_dir, shard_name)
                tmp_path = f"{shard_path}.tmp"
                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(table, tmp_path, compression="zstd")
                os.replace(tmp_path, shard_path)
                total_rows += len(spot_rows)
                self.logger.info(
                    "  -> %s: %d kline rows stored", binance_symbol, len(spot_rows)
                )

        lookup.finalize()
        self.logger.info(
            "Binance history complete: %d spot price rows across %d cryptos",
            total_rows,
            len(ranges_by_crypto),
        )
        return lookup
