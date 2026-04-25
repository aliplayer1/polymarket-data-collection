import warnings
from argparse import Namespace

from polymarket_pipeline.settings import PipelineRunOptions, RuntimeSettings


def test_runtime_settings_prefer_cli_values(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("POLYMARKET_DATA_DIR", str(tmp_path / "env-data"))
    monkeypatch.setenv("POLYMARKET_LOG_FILE", str(tmp_path / "env.log"))
    monkeypatch.setenv("HF_REPO_ID", "env/repo")

    args = Namespace(
        data_dir=str(tmp_path / "cli-data"),
        log_file=str(tmp_path / "logs" / "pipeline.log"),
        hf_repo="cli/repo",
    )

    settings = RuntimeSettings.from_args(args)

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


def test_run_options_warn_when_test_combined_with_upload() -> None:
    """``--upload`` is silently ignored in ``--test`` mode (test data is
    never pushed to HF).  The previous behaviour deferred the warning
    to AFTER the test run; users waited through the entire run before
    learning the flag was a no-op.  Now it's normalised at construction
    and surfaced as a ``warnings.warn`` so it's visible immediately.
    """
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        options = PipelineRunOptions.from_values(test_limit=5, upload=True)
    assert any("--upload" in str(w.message) for w in captured), captured
    # Upload flag was normalised away.
    assert options.upload is False
    assert options.is_test


def test_run_options_no_warning_when_test_alone_or_upload_alone() -> None:
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        PipelineRunOptions.from_values(test_limit=5)  # test only
        PipelineRunOptions.from_values(upload=True)   # upload only
    assert not any("--upload" in str(w.message) for w in captured), captured


def test_run_options_from_date_to_date_normalised() -> None:
    """``from_date`` / ``to_date`` are stripped to None when blank, kept
    as trimmed strings otherwise (the historical phase parses them).
    """
    o1 = PipelineRunOptions.from_values(from_date="  2026-01-01  ", to_date="2026-04-25")
    assert o1.from_date == "2026-01-01"
    assert o1.to_date == "2026-04-25"

    o2 = PipelineRunOptions.from_values(from_date="", to_date="   ")
    assert o2.from_date is None
    assert o2.to_date is None

    o3 = PipelineRunOptions.from_values()
    assert o3.from_date is None
    assert o3.to_date is None
