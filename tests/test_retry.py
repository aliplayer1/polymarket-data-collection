"""Unit tests for ``api_call_with_retry``."""
from __future__ import annotations

import logging

import pytest

from polymarket_pipeline.retry import (
    _is_non_retryable_client_error,
    _parse_retry_after,
    api_call_with_retry,
)


class _FakeResponse:
    def __init__(self, status_code: int, headers: dict | None = None):
        self.status_code = status_code
        self.headers = headers or {}


class _FakeHTTPError(Exception):
    def __init__(self, status_code: int, headers: dict | None = None):
        super().__init__(f"HTTP {status_code}")
        self.response = _FakeResponse(status_code, headers)


def test_non_retryable_client_error_skips_retry(monkeypatch):
    """A 422 (or 400/401/403/404) must NOT retry — it will never
    succeed and just delays the caller's own fallback path.
    """
    sleeps: list[float] = []
    monkeypatch.setattr(
        "polymarket_pipeline.retry.time.sleep",
        lambda s: sleeps.append(s),
    )

    call_count = {"n": 0}

    def fail_with_422():
        call_count["n"] += 1
        raise _FakeHTTPError(422)

    with pytest.raises(_FakeHTTPError):
        api_call_with_retry(fail_with_422, max_attempts=3, logger=logging.getLogger("t"))

    assert call_count["n"] == 1, "422 must raise after 1 attempt, not retry"
    assert sleeps == [], "no sleep should be issued for non-retryable errors"


def test_retry_attempts_429(monkeypatch):
    """429 (Too Many Requests) IS retried, honouring Retry-After."""
    monkeypatch.setattr(
        "polymarket_pipeline.retry.time.sleep",
        lambda s: None,
    )

    call_count = {"n": 0}
    target_value = "ok"

    def succeed_after_one_429():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise _FakeHTTPError(429, {"Retry-After": "2"})
        return target_value

    out = api_call_with_retry(succeed_after_one_429, max_attempts=3, logger=logging.getLogger("t"))
    assert out == target_value
    assert call_count["n"] == 2


def test_retry_attempts_503(monkeypatch):
    """503 (Service Unavailable) is retried."""
    monkeypatch.setattr(
        "polymarket_pipeline.retry.time.sleep",
        lambda s: None,
    )

    call_count = {"n": 0}

    def succeed_after_one_503():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise _FakeHTTPError(503)
        return "ok"

    assert api_call_with_retry(succeed_after_one_503, max_attempts=3) == "ok"
    assert call_count["n"] == 2


def test_retry_attempts_408(monkeypatch):
    """408 (Request Timeout) is retried — it's a transient condition."""
    monkeypatch.setattr(
        "polymarket_pipeline.retry.time.sleep",
        lambda s: None,
    )

    call_count = {"n": 0}

    def succeed_after_one_408():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise _FakeHTTPError(408)
        return "ok"

    assert api_call_with_retry(succeed_after_one_408, max_attempts=3) == "ok"
    assert call_count["n"] == 2


def test_is_non_retryable_client_error_classification():
    assert _is_non_retryable_client_error(_FakeHTTPError(400)) is True
    assert _is_non_retryable_client_error(_FakeHTTPError(401)) is True
    assert _is_non_retryable_client_error(_FakeHTTPError(403)) is True
    assert _is_non_retryable_client_error(_FakeHTTPError(404)) is True
    assert _is_non_retryable_client_error(_FakeHTTPError(422)) is True
    # Retryable
    assert _is_non_retryable_client_error(_FakeHTTPError(408)) is False
    assert _is_non_retryable_client_error(_FakeHTTPError(429)) is False
    assert _is_non_retryable_client_error(_FakeHTTPError(500)) is False
    assert _is_non_retryable_client_error(_FakeHTTPError(503)) is False
    # No response attached
    assert _is_non_retryable_client_error(RuntimeError("boom")) is False


def test_parse_retry_after_default_when_header_absent():
    assert _parse_retry_after(_FakeHTTPError(429)) == 5.0
    assert _parse_retry_after(_FakeHTTPError(503)) == 3.0
    assert _parse_retry_after(_FakeHTTPError(404)) is None  # not a rate-limit


def test_parse_retry_after_honours_header():
    assert _parse_retry_after(_FakeHTTPError(429, {"Retry-After": "30"})) == 30.0
    assert _parse_retry_after(_FakeHTTPError(503, {"Retry-After": "0.5"})) == 1.0  # floor at 1s


def test_retry_eventually_raises_after_max_attempts(monkeypatch):
    """A persistent retryable failure raises after ``max_attempts`` calls."""
    monkeypatch.setattr(
        "polymarket_pipeline.retry.time.sleep",
        lambda s: None,
    )

    call_count = {"n": 0}

    def always_fail():
        call_count["n"] += 1
        raise _FakeHTTPError(503)

    with pytest.raises(_FakeHTTPError):
        api_call_with_retry(always_fail, max_attempts=3)
    assert call_count["n"] == 3
