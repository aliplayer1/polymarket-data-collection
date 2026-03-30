import argparse
import logging
import os
from os import PathLike

from dotenv import load_dotenv

load_dotenv()  # reads .env into os.environ before settings/config are imported


def configure_logging(log_file: str | PathLike[str] | None = None) -> logging.Logger:
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        log_path = os.fspath(log_file)
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        handlers.append(logging.FileHandler(log_path))
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)
    return logging.getLogger("polymarket_pipeline")


def main() -> None:
    from .pipeline import PolymarketDataPipeline
    from .settings import PipelineRunOptions, RuntimeSettings

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
    parser.add_argument(
        "--prefer-rpc",
        action="store_true",
        default=False,
        help="Use RPC provider preferentially over Polygonscan for historical logs (faster if using Alchemy).",
    )
    args = parser.parse_args()

    runtime_settings = RuntimeSettings.from_args(args)
    run_options = PipelineRunOptions.from_args(args)

    logger = configure_logging(runtime_settings.log_file_str)
    try:
        if run_options.upload_only:
            from .storage import upload_to_huggingface

            paths = runtime_settings.resolve_paths(run_options)
            logger.info("Upload-only mode: consolidating and uploading...")
            upload_to_huggingface(
                repo_id=runtime_settings.hf_repo,
                markets_path=str(paths.markets_path),
                prices_dir=str(paths.prices_dir),
                ticks_dir=str(paths.ticks_dir),
                spot_prices_dir=str(paths.spot_prices_dir),
                orderbook_dir=str(paths.orderbook_dir),
                logger=logger,
            )

            import os
            from .config import HF_CULTURE_REPO_ID
            culture_root = paths.data_dir.parent / "data-culture"
            if culture_root.exists():
                logger.info("Uploading culture dataset to Hugging Face...")
                upload_to_huggingface(
                    repo_id=os.environ.get("HF_CULTURE_REPO_ID", HF_CULTURE_REPO_ID),
                    markets_path=str(culture_root / "markets.parquet"),
                    prices_dir=str(culture_root / "prices"),
                    ticks_dir=str(culture_root / "ticks"),
                    logger=logger,
                    skip_consolidate=False,
                )
            
            return

        pipeline = PolymarketDataPipeline(
            logger=logger,
            settings=runtime_settings,
        )
        pipeline.run(run_options=run_options)
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
