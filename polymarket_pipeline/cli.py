import argparse
import atexit
import logging
import logging.handlers
import os
import queue
from os import PathLike

from dotenv import load_dotenv

load_dotenv()  # reads .env into os.environ before settings/config are imported


# Bounded queue capacity for the async log handler.  Log records that
# would overflow this queue are dropped at the emitter — we prefer
# dropping log lines over stalling the WS recv loop.
_LOG_QUEUE_MAXSIZE = 10_000

# Reconnect-log retention: 10 MB × 5 files ≈ 50 MB max on disk.  Long
# enough to cover a week of healthy operation plus a burst incident.
_RECONNECT_LOG_MAX_BYTES = 10 * 1024 * 1024
_RECONNECT_LOG_BACKUPS = 5


def _install_async_wrapper(real_handlers: list[logging.Handler]) -> logging.handlers.QueueListener:
    """Move slow I/O off the emitter thread via QueueHandler + QueueListener.

    The recv loop runs on the main asyncio thread; synchronous stderr /
    disk writes from ``logger.info(...)`` during heavy logging can stall
    the loop for milliseconds.  QueueHandler does only a ``queue.put``
    (sub-µs); the listener thread drains and invokes the real handlers
    off-loop.

    Returns the listener so the caller can register an atexit stop hook.
    """
    log_queue: queue.Queue = queue.Queue(maxsize=_LOG_QUEUE_MAXSIZE)
    queue_handler = logging.handlers.QueueHandler(log_queue)
    listener = logging.handlers.QueueListener(
        log_queue, *real_handlers, respect_handler_level=True,
    )
    listener.start()
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(queue_handler)
    root.setLevel(logging.INFO)
    return listener


def _configure_reconnect_log(log_dir: str) -> None:
    """Attach a rotating jsonl handler to ``polymarket_pipeline.ws_reconnects``.

    Keeps reconnect events out of the main pipeline log (they would be
    noisy during incidents) and makes them trivially greppable:
    ``jq '.' < ws_reconnects.jsonl``.
    """
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, "ws_reconnects.jsonl")
    handler = logging.handlers.RotatingFileHandler(
        path,
        maxBytes=_RECONNECT_LOG_MAX_BYTES,
        backupCount=_RECONNECT_LOG_BACKUPS,
    )
    # Raw JSON — the message itself is already json.dumps(payload) from
    # the emitter, so we don't want level/timestamp prefixes.
    handler.setFormatter(logging.Formatter("%(message)s"))
    reconnect_logger = logging.getLogger("polymarket_pipeline.ws_reconnects")
    reconnect_logger.propagate = False
    for h in list(reconnect_logger.handlers):
        reconnect_logger.removeHandler(h)
    reconnect_logger.addHandler(handler)
    reconnect_logger.setLevel(logging.INFO)


def configure_logging(log_file: str | PathLike[str] | None = None) -> logging.Logger:
    """Set up async-wrapped logging + the dedicated reconnect log.

    ``log_file`` controls where the main pipeline log goes.  The
    reconnect jsonl lands in the same directory (or ``./logs/`` when no
    log file is configured).
    """
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    real_handlers: list[logging.Handler] = [logging.StreamHandler()]
    log_dir: str
    if log_file:
        log_path = os.fspath(log_file)
        log_dir = os.path.dirname(os.path.abspath(log_path)) or "."
        os.makedirs(log_dir, exist_ok=True)
        real_handlers.append(logging.FileHandler(log_path))
    else:
        log_dir = os.path.abspath("logs")
    for handler in real_handlers:
        handler.setFormatter(logging.Formatter(fmt))

    listener = _install_async_wrapper(real_handlers)
    atexit.register(listener.stop)

    _configure_reconnect_log(log_dir)

    return logging.getLogger("polymarket_pipeline")


def main() -> None:
    from .alerts import install_death_alerts, send_alert_async
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
        "--to-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Only scan markets that closed on or before this date "
            "(inclusive — the named day is included).  Pairs with "
            "--from-date to scan a bounded historical window; server-side "
            "filtering bypasses Gamma's ~250K-offset pagination cap.  Useful "
            "when restoring multi-month gaps in chunks."
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
        default=None,
        help="Use RPC provider preferentially over Polygonscan for historical logs (faster if using Alchemy).",
    )
    args = parser.parse_args()

    runtime_settings = RuntimeSettings.from_args(args)
    run_options = PipelineRunOptions.from_args(args)

    logger = configure_logging(runtime_settings.log_file_str)

    # Install death alerts immediately after logging is set up, on the main
    # thread (``signal.signal`` rejects secondary threads on POSIX).  This
    # ensures SIGTERM from ``systemctl stop`` surfaces as a webhook even
    # if the pipeline dies before the WS phase starts.
    install_death_alerts()
    send_alert_async("boot", "process booted")

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
                heartbeats_dir=str(paths.heartbeats_dir),
                logger=logger,
            )

            import os
            from .config import HF_CULTURE_REPO_ID
            from .storage import consolidate_culture_prices
            culture_root = paths.data_dir.parent / "data-culture"
            if culture_root.exists():
                logger.info("Uploading culture dataset to Hugging Face...")
                culture_prices_dir = str(culture_root / "prices")
                if os.path.exists(culture_prices_dir):
                    consolidate_culture_prices(prices_dir=culture_prices_dir, logger=logger)
                upload_to_huggingface(
                    repo_id=os.environ.get("HF_CULTURE_REPO_ID", HF_CULTURE_REPO_ID),
                    markets_path=str(culture_root / "markets.parquet"),
                    prices_dir=culture_prices_dir,
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
