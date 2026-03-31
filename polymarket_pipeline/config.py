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
# Hard cap on total in-memory WS+tick buffer rows across all timeframes.
# If the flush loop behind (slow disk / lock contention), the buffer
# would grow unboundedly and eventually OOM.  When this cap is hit the oldest
# half of each buffer is evicted and an ERROR is logged.
WS_BUFFER_MAX_ROWS = 600_000
# Orderbook BBO events flush on a longer cadence than ticks/prices because
# price_change events generate ~4 000 rows/s across 6 000+ tokens.  30 s
# keeps per-flush batch size to ~120 K rows and limits shard-file count to
# ~2/min per partition (≈360 between 3-hour consolidation runs).
WS_OB_FLUSH_INTERVAL_S: float = 30.0

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

FILTER_KEYWORD = _DEFAULT_MARKET_DEFINITION.question_keywords[0]
CRYPTO_ALIASES = {}
for _d in _MARKET_DEFINITIONS:
    CRYPTO_ALIASES.update(_d.asset_aliases)
