from argparse import Namespace

from polymarket_pipeline.settings import PipelineRunOptions, RuntimeSettings


def test_runtime_settings_prefer_cli_values(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("POLYGON_RPC_URL", "https://env-rpc")
    monkeypatch.setenv("POLYGONSCAN_API_KEY", "env-key")
    monkeypatch.setenv("POLYMARKET_DATA_DIR", str(tmp_path / "env-data"))
    monkeypatch.setenv("POLYMARKET_LOG_FILE", str(tmp_path / "env.log"))
    monkeypatch.setenv("HF_REPO_ID", "env/repo")

    args = Namespace(
        rpc_url="https://cli-rpc",
        polygonscan_key="cli-key",
        data_dir=str(tmp_path / "cli-data"),
        log_file=str(tmp_path / "logs" / "pipeline.log"),
        hf_repo="cli/repo",
    )

    settings = RuntimeSettings.from_args(args)

    assert settings.rpc_url == "https://cli-rpc"
    assert settings.polygonscan_key == "cli-key"
    assert settings.data_dir == tmp_path / "cli-data"
    assert settings.log_file == tmp_path / "logs" / "pipeline.log"
    assert settings.hf_repo == "cli/repo"


def test_run_options_normalize_filters() -> None:
    options = PipelineRunOptions.from_values(
        cryptos=["btc", "eth"],
        timeframes=["5m", "1hr", "custom-window"],
        test_limit=0,
        market_ids=["m1", "m2"],
    )

    assert options.cryptos == ("BTC", "ETH")
    assert options.timeframes == ("5-minute", "1-hour", "custom-window")
    assert options.market_ids == ("m1", "m2")
    assert options.test_limit is None
    assert not options.is_test


def test_runtime_settings_resolve_test_paths() -> None:
    settings = RuntimeSettings()
    options = PipelineRunOptions.from_values(test_limit=5)

    paths = settings.resolve_paths(options)

    assert paths.data_dir.name == "test_output_parquet"
    assert paths.markets_path.name == "markets.parquet"
