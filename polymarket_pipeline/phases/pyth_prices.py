import logging
import os
import time
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from ..models import MarketRecord
from ..retry import api_call_with_retry
from ..config import TIMEFRAME_SECONDS
from .shared import PipelinePaths

class PythPricePhase:
    HERMES_URL = "https://hermes.pyth.network/v2/updates/price/{ts}"
    FEEDS = {
        "BTC": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
        "ETH": "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
        "SOL": "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
    }
    
    # Hermes rate limit: 30 req/10s per IP.  429 triggers a 60-second ban.
    _HERMES_MIN_INTERVAL = 0.34  # ~3 req/s, safely within 30/10s

    def __init__(self, paths: PipelinePaths, logger: logging.Logger | None = None):
        self.paths = paths
        self.logger = logger or logging.getLogger("polymarket_pipeline.pyth")
        self.spot_prices_dir = self.paths.data_dir / "spot_prices"

        self.is_enabled = os.environ.get("PYTH_PRICES_ENABLED") == "1"
        self.session = requests.Session()
        self._last_hermes_ts: float = 0.0
        
    def _rate_limit_hermes(self) -> None:
        """Enforce per-IP Hermes rate limit before each request."""
        elapsed = time.time() - self._last_hermes_ts
        if elapsed < self._HERMES_MIN_INTERVAL:
            time.sleep(self._HERMES_MIN_INTERVAL - elapsed)
        self._last_hermes_ts = time.time()

    def _fetch_one(self, ts: int) -> dict[str, Any]:
        url = self.HERMES_URL.format(ts=ts)
        params = [("ids[]", feed_id) for feed_id in self.FEEDS.values()]

        def _do_fetch():
            self._rate_limit_hermes()
            resp = self.session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()

        return api_call_with_retry(
            _do_fetch,
            max_attempts=5,
            base_delay_seconds=2.0,
            min_retry_after=61,  # Hermes 429 triggers a 60-second ban
            logger=self.logger,
        )

    def _extract(self, data: dict[str, Any], feed_id: str) -> tuple[float, float]:
        for parsed in data.get("parsed", []):
            if parsed.get("id") == feed_id:
                price_info = parsed.get("price", {})
                price = float(price_info.get("price", 0))
                expo = int(price_info.get("expo", 0))
                conf = float(price_info.get("conf", 0))
                
                price_usd = price * (10 ** expo)
                conf_usd = conf * (10 ** expo)
                return price_usd, conf_usd
        return float("nan"), float("nan")

    def fetch_for_market(self, market: MarketRecord) -> pd.DataFrame:
        """Fetch 1-second prices for [start_ts, end_ts] range, capped by timeframe."""
        rows = []
        
        # Limit the search window to just the timeframe before resolution
        duration = TIMEFRAME_SECONDS.get(market.timeframe, 86400)
        start_ts = max(market.start_ts, market.end_ts - duration)
        
        for ts in range(start_ts, market.end_ts + 1):
            data = self._fetch_one(ts)
            for symbol, feed_id in self.FEEDS.items():
                price, conf = self._extract(data, feed_id)
                rows.append({
                    "ts_second": ts,
                    "symbol": symbol,
                    "price_usd": price,
                    "conf": conf
                })
            time.sleep(0.05)  # minimal sleep — rate limiting is handled by _rate_limit_hermes
        df = pd.DataFrame(rows)
        # Enforce schema explicitly to avoid problems
        if not df.empty:
            df["ts_second"] = df["ts_second"].astype("int64")
            df["symbol"] = df["symbol"].astype("string")
            df["price_usd"] = df["price_usd"].astype("float64")
            df["conf"] = df["conf"].astype("float64")
        return df

    def run(self, markets: list[MarketRecord]) -> None:
        """Write spot_prices/symbol=X/*.parquet (Hive-partitioned)."""
        if not self.is_enabled or not markets:
            return

        self.logger.info("Starting PythPricePhase. Checking %s markets.", len(markets))
        
        covered_ranges = []
        if self.spot_prices_dir.exists() and any(self.spot_prices_dir.iterdir()):
            try:
                con = duckdb.connect()
                # Find min and max ts_second to do a rough caching 
                # (Ideally we'd track exactly which ranges are covered)
                result = con.execute(f"SELECT MIN(ts_second), MAX(ts_second) FROM read_parquet('{self.spot_prices_dir}/**/*.parquet')").fetchone()
                if result and result[0] is not None:
                    covered_ranges.append((result[0], result[1]))
                con.close()
            except Exception as e:
                self.logger.warning("Could not query existing spot_prices: %s", e)

        markets_to_fetch = []
        for m in markets:
            is_covered = False
            for (min_ts, max_ts) in covered_ranges:
                if m.start_ts >= min_ts and m.end_ts <= max_ts:
                    is_covered = True
                    break
            if not is_covered:
                markets_to_fetch.append(m)

        if not markets_to_fetch:
            self.logger.info("All markets are already covered by existing spot_prices.")
            return

        self.logger.info("Fetching Pyth spot prices for %s new markets...", len(markets_to_fetch))
        
        for i, m in enumerate(markets_to_fetch):
            duration = TIMEFRAME_SECONDS.get(m.timeframe, 86400)
            actual_duration = m.end_ts - max(m.start_ts, m.end_ts - duration)
            self.logger.info("[%s/%s] Fetching Pyth prices for market %s (duration %s seconds)", i+1, len(markets_to_fetch), m.market_id, actual_duration)
            try:
                df = self.fetch_for_market(m)
                if not df.empty:
                    table = pa.Table.from_pandas(df)
                    pq.write_to_dataset(
                        table,
                        root_path=str(self.spot_prices_dir),
                        partition_cols=["symbol"],
                        compression="zstd",
                    )
            except Exception as e:
                self.logger.error("Error fetching Pyth prices for market %s: %s", m.market_id, e)
