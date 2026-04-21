"""Offline unit tests for polymarket_pipeline.subgraph_client.

No live network: every test patches ``requests.Session.post`` with
canned responses so we can exercise retry, timeout, and fallback
activation without hitting Goldsky or The Graph.
"""
from __future__ import annotations

from unittest import mock

import pytest
import requests

from polymarket_pipeline.subgraph_client import SubgraphClient, SubgraphError


def _fake_response(status_code: int = 200, body: dict | str | None = None) -> mock.MagicMock:
    r = mock.MagicMock()
    r.status_code = status_code
    r.text = str(body) if body is not None else ""
    if isinstance(body, dict):
        r.json.return_value = body
    else:
        r.json.side_effect = ValueError("not JSON")
    return r


def _client(**kw):
    kw.setdefault("primary_url", "https://primary.example/graphql")
    kw.setdefault("fallback_url", None)
    kw.setdefault("request_interval_s", 0.0)   # disable throttle in tests
    kw.setdefault("max_retries", 2)
    return SubgraphClient(**kw)


# --- Happy path ------------------------------------------------------------


def test_query_returns_data_on_first_try(monkeypatch):
    client = _client()
    payload = {"data": {"orderFilledEvents": [{"id": "a"}]}}
    post = mock.MagicMock(return_value=_fake_response(200, payload))
    monkeypatch.setattr(client._session, "post", post)

    data = client.query("{ orderFilledEvents { id } }")
    assert data == payload["data"]
    assert post.call_count == 1


def test_query_sends_variables_when_provided(monkeypatch):
    client = _client()
    post = mock.MagicMock(return_value=_fake_response(200, {"data": {"x": 1}}))
    monkeypatch.setattr(client._session, "post", post)

    client.query("query ($x: Int!) { x }", variables={"x": 42})
    _, kwargs = post.call_args
    assert kwargs["json"]["variables"] == {"x": 42}


# --- Retryable failures ---------------------------------------------------


def test_retries_on_503_then_succeeds(monkeypatch):
    client = _client(max_retries=3)
    responses = [
        _fake_response(503, "overloaded"),
        _fake_response(200, {"data": {"ok": True}}),
    ]
    post = mock.MagicMock(side_effect=responses)
    monkeypatch.setattr(client._session, "post", post)

    data = client.query("{ ok }")
    assert data == {"ok": True}
    assert post.call_count == 2


def test_retries_on_timeout_then_succeeds(monkeypatch):
    client = _client(max_retries=3)
    post = mock.MagicMock(side_effect=[
        requests.exceptions.Timeout("slow"),
        _fake_response(200, {"data": {"ok": True}}),
    ])
    monkeypatch.setattr(client._session, "post", post)

    data = client.query("{ ok }")
    assert data == {"ok": True}
    assert post.call_count == 2


def test_retries_on_200_with_transient_graphql_errors(monkeypatch):
    client = _client(max_retries=3)
    responses = [
        _fake_response(200, {"errors": [{"message": "indexer lagging"}]}),
        _fake_response(200, {"data": {"ok": True}}),
    ]
    post = mock.MagicMock(side_effect=responses)
    monkeypatch.setattr(client._session, "post", post)

    data = client.query("{ ok }")
    assert data == {"ok": True}
    assert post.call_count == 2


# --- Fatal failures (no retry) --------------------------------------------


def test_fatal_on_400_does_not_retry(monkeypatch):
    client = _client(max_retries=3)
    post = mock.MagicMock(return_value=_fake_response(400, "bad query"))
    monkeypatch.setattr(client._session, "post", post)

    with pytest.raises(SubgraphError):
        client.query("{ broken }")
    assert post.call_count == 1  # no retries on 4xx


def test_fatal_on_non_transient_graphql_error_does_not_retry(monkeypatch):
    client = _client(max_retries=3)
    post = mock.MagicMock(return_value=_fake_response(
        200, {"errors": [{"message": "Field 'xyz' not found"}]}
    ))
    monkeypatch.setattr(client._session, "post", post)

    with pytest.raises(SubgraphError):
        client.query("{ xyz }")
    assert post.call_count == 1


# --- Fallback activation --------------------------------------------------


def test_fallback_activated_when_primary_exhausted(monkeypatch):
    client = _client(
        fallback_url="https://fallback.example/graphql",
        max_retries=2,
    )
    # Primary: 2 retries of 503. Fallback: 200 OK.
    post = mock.MagicMock(side_effect=[
        _fake_response(503),
        _fake_response(503),
        _fake_response(200, {"data": {"ok": True}}),
    ])
    monkeypatch.setattr(client._session, "post", post)

    data = client.query("{ ok }")
    assert data == {"ok": True}
    # 2 primary attempts + 1 fallback attempt
    assert post.call_count == 3
    # Third call hit the fallback URL
    last_call_args = post.call_args_list[-1][0]
    assert last_call_args[0] == "https://fallback.example/graphql"


def test_both_endpoints_fail_raises_subgraph_error(monkeypatch):
    client = _client(
        fallback_url="https://fallback.example/graphql",
        max_retries=1,
    )
    post = mock.MagicMock(return_value=_fake_response(503))
    monkeypatch.setattr(client._session, "post", post)

    with pytest.raises(SubgraphError):
        client.query("{ ok }")
    assert post.call_count == 2  # 1 primary + 1 fallback


# --- API key templating ---------------------------------------------------


def test_api_key_substituted_into_fallback_url(monkeypatch):
    client = SubgraphClient(
        primary_url="https://primary.example",
        fallback_url="https://gateway/api/{api_key}/subgraphs/id/abc",
        api_key="MYKEY",
        request_interval_s=0.0,
        max_retries=1,
    )
    assert "MYKEY" in client._fallback
    assert "{api_key}" not in client._fallback


def test_no_api_key_disables_fallback_with_template(monkeypatch):
    client = SubgraphClient(
        primary_url="https://primary.example",
        fallback_url="https://gateway/api/{api_key}/subgraphs/id/abc",
        api_key=None,
        request_interval_s=0.0,
        max_retries=1,
    )
    assert client._fallback is None


def test_no_api_key_keeps_plain_fallback(monkeypatch):
    client = SubgraphClient(
        primary_url="https://primary.example",
        fallback_url="https://plain-fallback.example",
        api_key=None,
        request_interval_s=0.0,
    )
    assert client._fallback == "https://plain-fallback.example"


# --- Throttling -----------------------------------------------------------


def test_throttle_spaces_consecutive_requests(monkeypatch):
    client = _client(request_interval_s=0.05)
    post = mock.MagicMock(return_value=_fake_response(200, {"data": {"ok": True}}))
    monkeypatch.setattr(client._session, "post", post)

    import time
    t0 = time.monotonic()
    client.query("{ ok }")
    client.query("{ ok }")
    elapsed = time.monotonic() - t0
    # Two calls with 0.05s throttle → at least ~0.05s between them
    assert elapsed >= 0.04


# --- Masking --------------------------------------------------------------


def test_mask_redacts_long_hex_blobs():
    # API-key-looking hex runs should be scrubbed from log output.
    masked = SubgraphClient._mask("https://x/api/abc123def456abc123def456abc123def456/sub/id")
    assert "<redacted>" in masked
