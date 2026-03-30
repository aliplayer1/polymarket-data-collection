from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .config import HF_REPO_ID, PARQUET_DATA_DIR, PARQUET_TEST_DIR
from .parsing import normalize_timeframe_input


def _coalesce(*values: object) -> object | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _coerce_path(value: object | None) -> Path | None:
    if value is None:
        return None
    return Path(str(value)).expanduser()


def _coerce_tuple(values: object | None, *, transform=lambda item: item) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        items: Iterable[object] = (values,)
    else:
        items = values if isinstance(values, Iterable) else (values,)
    result: list[str] = []
    for item in items:
        text = str(item).strip()
        if text:
            result.append(transform(text))
    return tuple(result)


@dataclass(frozen=True)
class PipelineRunOptions:
    historical_only: bool = False
    websocket_only: bool = False
    market_ids: tuple[str, ...] = ()
    cryptos: tuple[str, ...] = ()
    timeframes: tuple[str, ...] = ()
    test_limit: int | None = None
    upload: bool = False
    upload_only: bool = False
    from_date: str | None = None

    @classmethod
    def from_args(cls, args: Any) -> "PipelineRunOptions":
        return cls.from_values(
            historical_only=bool(getattr(args, "historical_only", False)),
            websocket_only=bool(getattr(args, "websocket_only", False)),
            market_ids=getattr(args, "markets", None),
            cryptos=getattr(args, "crypto", None),
            timeframes=getattr(args, "timeframe", None),
            test_limit=getattr(args, "test", None),
            upload=bool(getattr(args, "upload", False)),
            upload_only=bool(getattr(args, "upload_only", False)),
            from_date=getattr(args, "from_date", None),
        )

    @classmethod
    def from_values(
        cls,
        *,
        historical_only: bool = False,
        websocket_only: bool = False,
        market_ids: list[str] | tuple[str, ...] | None = None,
        cryptos: list[str] | tuple[str, ...] | None = None,
        timeframes: list[str] | tuple[str, ...] | None = None,
        test_limit: int | None = None,
        upload: bool = False,
        upload_only: bool = False,
        from_date: str | None = None,
    ) -> "PipelineRunOptions":
        normalized_test_limit = test_limit if test_limit is not None and test_limit > 0 else None
        normalized_from_date = from_date.strip() if isinstance(from_date, str) and from_date.strip() else None
        return cls(
            historical_only=historical_only,
            websocket_only=websocket_only,
            market_ids=_coerce_tuple(market_ids),
            cryptos=_coerce_tuple(cryptos, transform=lambda item: item.upper()),
            timeframes=_coerce_tuple(timeframes, transform=normalize_timeframe_input),
            test_limit=normalized_test_limit,
            upload=upload,
            upload_only=upload_only,
            from_date=normalized_from_date,
        )

    @property
    def is_test(self) -> bool:
        return self.test_limit is not None and self.test_limit > 0


@dataclass(frozen=True)
class RuntimeSettings:
    rpc_urls: tuple[str, ...] = ()
    polygonscan_key: str | None = None
    data_dir: Path | None = None
    log_file: Path | None = None
    hf_repo: str = HF_REPO_ID
    prefer_rpc: bool = False

    @classmethod
    def from_args(cls, args: Any) -> "RuntimeSettings":
        raw_rpc = _coalesce(
            getattr(args, "rpc_url", None),
            os.environ.get("POLYGON_RPC_URL"),
        )
        rpc_urls = _coerce_tuple(raw_rpc, transform=lambda item: item.strip())

        return cls(
            rpc_urls=rpc_urls,
            polygonscan_key=_coalesce(
                getattr(args, "polygonscan_key", None),
                os.environ.get("POLYGONSCAN_API_KEY"),
            ),
            data_dir=_coerce_path(
                _coalesce(
                    getattr(args, "data_dir", None),
                    os.environ.get("POLYMARKET_DATA_DIR"),
                )
            ),
            log_file=_coerce_path(
                _coalesce(
                    getattr(args, "log_file", None),
                    os.environ.get("POLYMARKET_LOG_FILE"),
                )
            ),
            hf_repo=str(
                _coalesce(
                    getattr(args, "hf_repo", None),
                    os.environ.get("HF_REPO_ID"),
                    HF_REPO_ID,
                )
            ),
            prefer_rpc=bool(
                _coalesce(
                    getattr(args, "prefer_rpc", False),
                    os.environ.get("PREFER_RPC", "").lower() == "true",
                )
            ),
        )

    def with_overrides(
        self,
        *,
        data_dir: str | Path | None = None,
        hf_repo: str | None = None,
    ) -> "RuntimeSettings":
        return RuntimeSettings(
            rpc_urls=self.rpc_urls,
            polygonscan_key=self.polygonscan_key,
            data_dir=_coerce_path(data_dir) or self.data_dir,
            log_file=self.log_file,
            hf_repo=str(_coalesce(hf_repo, self.hf_repo)),
        )

    def resolve_paths(self, run_options: PipelineRunOptions):
        from .phases.shared import PipelinePaths

        if run_options.is_test:
            return PipelinePaths.from_root(PARQUET_TEST_DIR)
        if self.data_dir is not None:
            return PipelinePaths.from_root(self.data_dir)
        return PipelinePaths.from_root(PARQUET_DATA_DIR)

    @property
    def rpc_url(self) -> str | None:
        return self.rpc_urls[0] if self.rpc_urls else None

    @property
    def log_file_str(self) -> str | None:
        return str(self.log_file) if self.log_file is not None else None
