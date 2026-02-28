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

TIME_FRAMES = ("5-minute", "15-minute", "1-hour", "4-hour")

# Duration of each prediction window in seconds.  The price fetch for a closed
# market is limited to [end_ts - window_seconds, end_ts] so that we only
# capture price action during the actual prediction period, not the long
# dormant pre-trading phase where prices sit flat at the default 0.5/0.5.
TIMEFRAME_SECONDS: dict[str, int] = {
    "5-minute":  5  * 60,   # 300 s
    "15-minute": 15 * 60,   # 900 s
    "1-hour":    60 * 60,   # 3600 s
    "4-hour":    4 * 60 * 60,  # 14400 s
}

# --- Parquet storage (normalized schema) ---
PARQUET_DATA_DIR = "data"
PARQUET_MARKETS_PATH = f"{PARQUET_DATA_DIR}/markets.parquet"
PARQUET_PRICES_DIR = f"{PARQUET_DATA_DIR}/prices"  # Hive-partitioned: crypto=X/timeframe=Y/
PARQUET_TICKS_DIR  = f"{PARQUET_DATA_DIR}/ticks"   # Hive-partitioned: crypto=X/timeframe=Y/
PARQUET_TEST_DIR = "test_output_parquet"
PARQUET_TEST_MARKETS_PATH = f"{PARQUET_TEST_DIR}/markets.parquet"
PARQUET_TEST_PRICES_DIR = f"{PARQUET_TEST_DIR}/prices"
PARQUET_TEST_TICKS_DIR  = f"{PARQUET_TEST_DIR}/ticks"

# --- Hugging Face Hub ---
HF_REPO_ID = "polymarket-crypto-updown"  # default, overridable via --hf-repo

PRICE_SUM_TOLERANCE = 0.15
MAX_WS_RECONNECT_DELAY_SECONDS = 120

FILTER_KEYWORD = "up or down"
CRYPTO_ALIASES = {
    "BTC": ("bitcoin", "btc"),
    "ETH": ("ethereum", "eth"),
    "SOL": ("solana", "sol"),
}
