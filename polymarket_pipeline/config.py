import os

from .markets import get_market_definitions

_MARKET_DEFINITIONS = get_market_definitions()
_DEFAULT_MARKET_DEFINITION = _MARKET_DEFINITIONS[0]

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID = 137

# --- Polygon on-chain tick data ---
# CTF Exchange: emits OrderFilled for every trade fill on Polymarket
CTF_EXCHANGE_ADDRESS = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
BLOCK_TIME_SECONDS = 2.19          # average Polygon PoS block time
ETHERSCAN_V2_API = "https://api.etherscan.io/v2/api"  # Etherscan V2 with chainid=137 for Polygon PoS
POLYGONSCAN_NATIVE_API = "https://api.polygonscan.com/api"  # Native Polygonscan endpoint (no chainid needed)
POLYGON_CHAIN_ID = 137
# Default RPC URL – override with --rpc-url or POLYGON_RPC_URL env var.
# Free options that support eth_getLogs:
#   Alchemy:    https://polygon-mainnet.g.alchemy.com/v2/<key>  (300M CU/mo free)
#   QuickNode:  https://xxx.matic.quiknode.pro/<key>              (10M req/mo free)
#   Infura:     https://polygon-mainnet.infura.io/v3/<key>        (100k req/day free)
POLYGON_RPC_URL: str | None = None  # set at runtime via CLI or env

REQUEST_TIMEOUT_SECONDS = 20
PAGE_SIZE = 500
PRICE_HISTORY_CHUNK_SECONDS = 7 * 24 * 60 * 60
WS_FLUSH_INTERVAL_SECONDS = 5
WS_FLUSH_BATCH_SIZE = 200
# Max token IDs per WebSocket connection.  Polymarket silently drops connections
# whose subscription message exceeds ~200 KB.  1300+ active markets produce
# 2600+ token IDs (~200 KB) in a single message → server drops every 30-90 s.
# 500 tokens ≈ 38 KB per shard; with ~6 shards all connections stay stable.
WS_MAX_TOKENS_PER_SHARD = 500
# Legacy total-cap reference (still exported for external monitoring scripts).
# Enforcement moved to per-type drop-oldest buffers below — the old
# cliff-style emergency eviction was replaced by continuous bounded drop.
WS_BUFFER_MAX_ROWS = 600_000

# Per-type bounded buffer capacities (drop-oldest on overflow).  Sum is
# the effective memory cap for WS-phase buffers.  Sizing rationale:
#   - Prices/ticks: rare events (few per second across all markets)
#   - Orderbook: ~4 000 rows/s, must absorb 5–30 s of flush delay
#   - Spot: ~30 RTDS updates/s, small
WS_PRICE_BUFFER_MAX: int = 100_000
WS_TICK_BUFFER_MAX: int = 100_000
WS_OB_BUFFER_MAX: int = 400_000
WS_SPOT_BUFFER_MAX: int = 20_000
# Alert threshold: fire a webhook when a buffer has dropped this many
# rows since the last flush cycle.  Continuous drop = flush loop or
# disk is backing up.
WS_DROP_ALERT_THRESHOLD: int = 500
# Orderbook BBO events flush on the same cadence as ticks/prices.
# Bounding crash-loss to ~5 s is worth the shard-count trade-off: at
# ~4 000 rows/s across 6 000+ tokens this produces ~20 K rows per flush
# (~12 shards/min per partition ≈ 2 200 between 3-hour consolidation runs),
# which DuckDB handles comfortably with ``union_by_name`` and disk spill.
# Raise this back to 30.0 only if consolidation OOMs or shard-listing
# latency on HF upload becomes measurable.
WS_OB_FLUSH_INTERVAL_S: float = 5.0
# How often (seconds) to re-fetch active markets from the Gamma API and
# update WebSocket subscriptions.  This replaces the need for the daily
# polymarket-restart.timer — the WS phase handles market rotation
# autonomously.  1 hour catches new markets within their first prediction
# window for all timeframes except 5-minute.
WS_MARKET_REFRESH_INTERVAL_S: float = 3600.0

TIME_FRAMES = tuple(
    tf for d in _MARKET_DEFINITIONS for tf in d.timeframe_names
)

# Duration of each prediction window in seconds.  The price fetch for a closed
# market is limited to [end_ts - window_seconds, end_ts] so that we only
# capture price action during the actual prediction period, not the long
# dormant pre-trading phase where prices sit flat at the default 0.5/0.5.
TIMEFRAME_SECONDS: dict[str, int] = {}
for _d in _MARKET_DEFINITIONS:
    TIMEFRAME_SECONDS.update(_d.timeframe_seconds)

# --- Parquet storage (normalized schema) ---
PARQUET_DATA_DIR = "data"
PARQUET_MARKETS_PATH = f"{PARQUET_DATA_DIR}/markets.parquet"
PARQUET_PRICES_DIR = f"{PARQUET_DATA_DIR}/prices"  # Hive-partitioned: crypto=X/timeframe=Y/
PARQUET_TICKS_DIR  = f"{PARQUET_DATA_DIR}/ticks"   # Hive-partitioned: crypto=X/timeframe=Y/
PARQUET_TEST_DIR = "test_output_parquet"
PARQUET_TEST_MARKETS_PATH = f"{PARQUET_TEST_DIR}/markets.parquet"
PARQUET_TEST_PRICES_DIR = f"{PARQUET_TEST_DIR}/prices"
PARQUET_TEST_TICKS_DIR  = f"{PARQUET_TEST_DIR}/ticks"

# Culture specific storage
CULTURE_DATA_DIR = "data-culture"

# --- Hugging Face Hub ---
HF_REPO_ID = os.environ.get("HF_REPO_ID", "aliplayer1/polymarket-crypto-updown")
HF_CULTURE_REPO_ID = os.environ.get("HF_CULTURE_REPO_ID", "aliplayer1/polymarket-culture-data")

PRICE_SUM_TOLERANCE = 0.15
MAX_WS_RECONNECT_DELAY_SECONDS = 120

# --- WebSocket reconnect + watchdog tuning ---
# Backoff: delay = random.uniform(0.5, 1.5) * min(BASE * 2^n, CAP).  The
# 30 s cap (vs. the legacy 120 s) targets fast MTTR on server-initiated
# clean drops (Polymarket RTDS routinely closes cleanly every ~2 h).
# Jitter in `_jittered_backoff` prevents thundering-herd when several
# shards disconnect simultaneously.
WS_RECONNECT_BASE_S: float = 1.0
WS_RECONNECT_CAP_S: float = 30.0
# ``jitter_range`` keys into ``random.uniform(lo, hi) * raw_delay`` so we
# can widen/narrow jitter without editing call sites.
WS_RECONNECT_JITTER_LO: float = 0.5
WS_RECONNECT_JITTER_HI: float = 1.5

# Data-level watchdog staleness thresholds.  Exceeding one of these forces
# a reconnect of the affected connection (TCP is alive but data stalled).
WS_STALENESS_CLOB_PRICE_CHANGE_S: float = 60.0
WS_STALENESS_RTDS_BINANCE_S: float = 30.0
WS_STALENESS_RTDS_CHAINLINK_S: float = 120.0  # ~0.1 Hz feed, longer threshold
WS_WATCHDOG_CHECK_INTERVAL_S: float = 5.0
WS_WATCHDOG_GRACE_PERIOD_S: float = 30.0

# Heartbeat rows emitted by the WS flush loop even when no data is flowing.
# Makes gap detection O(scan-one-file) instead of O(join-ticks-by-ts).
WS_HEARTBEAT_INTERVAL_S: float = 10.0

# Reconnect burst monitor — fire a WARNING webhook when any shard sees
# more reconnects than the threshold within the window (indicates a
# server-side incident, e.g. the 12-reconnect storm observed during BTC
# whipsaw events).
WS_RECONNECT_BURST_THRESHOLD: int = 5
WS_RECONNECT_BURST_WINDOW_S: float = 60.0

FILTER_KEYWORD = _DEFAULT_MARKET_DEFINITION.question_keywords[0]
CRYPTO_ALIASES = {}
for _d in _MARKET_DEFINITIONS:
    CRYPTO_ALIASES.update(_d.asset_aliases)
