import argparse
import logging
import os

from dotenv import load_dotenv

from .pipeline import PolymarketDataPipeline

load_dotenv()  # reads .env into os.environ (won't overwrite existing vars)


def configure_logging(log_file: str | None = None) -> logging.Logger:
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)
    return logging.getLogger("polymarket_pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket Data Pipeline")
    parser.add_argument(
        "--historical-only",
        action="store_true",
        help="Collect historical data only, up to but not including the current active market.",
    )
    parser.add_argument(
        "--websocket-only",
        action="store_true",
        help=(
            "Skip the historical scan and tick backfill phases entirely; "
            "go straight to WebSocket streaming for currently-active markets. "
            "Useful when a separate --historical-only process is already running."
        ),
    )
    parser.add_argument(
        "--markets",
        nargs="+",
        help="List of specific market IDs to collect data for. If not provided, collects for all matching markets.",
    )
    parser.add_argument(
        "--crypto",
        nargs="+",
        help="List of cryptocurrencies to filter by (e.g., BTC ETH).",
    )
    parser.add_argument(
        "--timeframe",
        nargs="+",
        help="List of timeframes to filter by (e.g., 15m 1h).",
    )
    parser.add_argument(
        "--test",
        type=int,
        metavar="N",
        default=None,
        help="Test mode: collect data for N historical markets, write to test_output/, and print a validation report.",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload the dataset to Hugging Face Hub after collection finishes.",
    )
    parser.add_argument(
        "--hf-repo",
        type=str,
        default=None,
        metavar="USER/REPO",
        help="Hugging Face dataset repo ID (e.g., 'myuser/polymarket-data'). Defaults to config value.",
    )
    # ── Storage ──────────────────────────────────────────────────────────────
    parser.add_argument(
        "--from-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Only scan markets that closed on or after this date. "
            "Useful for the initial backfill when you only want recent history "
            "(e.g. --from-date 2024-01-01). "
            "On subsequent runs the cutoff is auto-detected from a saved checkpoint "
            "so you normally don't need this flag."
        ),
    )
    parser.add_argument(
        "--upload-only",
        action="store_true",
        help=(
            "Consolidate tick shard files and upload to Hugging Face Hub, "
            "without running any data collection. Intended for a dedicated "
            "upload timer that runs independently of the historical backfill."
        ),
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        metavar="PATH",
        help=(
            "Root directory for Parquet output (markets.parquet, prices/, ticks/). "
            "Defaults to 'data/' relative to the working directory. "
            "Set to an absolute path (e.g. /mnt/data/polymarket) when running on a "
            "cloud instance with a dedicated block volume. "
            "Also reads from env var POLYMARKET_DATA_DIR."
        ),
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        metavar="PATH",
        help=(
            "Append log output to this file in addition to stdout. "
            "Parent directories are created automatically. "
            "Also reads from env var POLYMARKET_LOG_FILE."
        ),
    )
    # ── Tick-level (on-chain) data settings ──────────────────────────────────
    parser.add_argument(
        "--rpc-url",
        type=str,
        default=None,
        metavar="URL",
        help=(
            "Polygon JSON-RPC endpoint for fetching on-chain trade ticks. "
            "Free options: Alchemy (https://polygon-mainnet.g.alchemy.com/v2/<key>), "
            "QuickNode, or Infura. "
            "Also reads from env var POLYGON_RPC_URL."
        ),
    )
    parser.add_argument(
        "--polygonscan-key",
        type=str,
        default=None,
        metavar="KEY",
        help=(
            "Free Polygonscan API key for getLogs queries (https://polygonscan.com/myapikey). "
            "Preferred over direct RPC for reliability. "
            "Also reads from env var POLYGONSCAN_API_KEY."
        ),
    )
    args = parser.parse_args()

    # Fall back to environment variables for all credentials / paths
    rpc_url         = args.rpc_url         or os.environ.get("POLYGON_RPC_URL")
    polygonscan_key = args.polygonscan_key  or os.environ.get("POLYGONSCAN_API_KEY")
    data_dir        = args.data_dir         or os.environ.get("POLYMARKET_DATA_DIR")
    log_file        = args.log_file         or os.environ.get("POLYMARKET_LOG_FILE")
    hf_repo         = args.hf_repo         or os.environ.get("HF_REPO_ID")

    logger = configure_logging(log_file)
    try:
        # Upload-only mode: consolidate + upload without any data collection
        if args.upload_only:
            from .storage import upload_to_huggingface
            from .config import PARQUET_DATA_DIR, PARQUET_MARKETS_PATH, PARQUET_PRICES_DIR, PARQUET_TICKS_DIR
            markets_path = os.path.join(data_dir, "markets.parquet") if data_dir else PARQUET_MARKETS_PATH
            prices_dir   = os.path.join(data_dir, "prices")          if data_dir else PARQUET_PRICES_DIR
            ticks_dir    = os.path.join(data_dir, "ticks")           if data_dir else PARQUET_TICKS_DIR
            logger.info("Upload-only mode: consolidating and uploading...")
            upload_to_huggingface(
                repo_id=hf_repo,
                markets_path=markets_path,
                prices_dir=prices_dir,
                ticks_dir=ticks_dir,
                logger=logger,
            )
            return

        pipeline = PolymarketDataPipeline(
            logger=logger,
            rpc_url=rpc_url,
            polygonscan_key=polygonscan_key,
        )
        pipeline.run(
            historical_only=args.historical_only,
            websocket_only=args.websocket_only,
            market_ids=args.markets,
            cryptos=args.crypto,
            timeframes=args.timeframe,
            test_limit=args.test,
            upload=args.upload,
            hf_repo=hf_repo,
            data_dir=data_dir,
            from_date=args.from_date,
        )
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
