"""Minimal GraphQL client for Polymarket's orderbook subgraph.

Transport-only: retry on 5xx / timeout / transient GraphQL errors with
exponential backoff and jitter, fail over from a primary endpoint to an
optional fallback endpoint, and enforce a global politeness throttle.
No subgraph-semantic logic lives here — ``SubgraphTickFetcher`` owns
that.

Default endpoints (from ``config.py``):

- **Primary**: Goldsky's public hosting of Polymarket's official
  orderbook-subgraph.  Anonymous, no API key required.
- **Fallback**: The Graph decentralized network.  Requires the
  ``SUBGRAPH_API_KEY`` env var; when absent, fallback is disabled and
  primary failures propagate after retry exhaustion.
"""
from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any

import requests

from .config import (
    SUBGRAPH_MAX_RETRIES,
    SUBGRAPH_REQUEST_INTERVAL_S,
    SUBGRAPH_TIMEOUT_S,
    SUBGRAPH_URL_FALLBACK,
    SUBGRAPH_URL_PRIMARY,
)


class SubgraphError(RuntimeError):
    """Raised when all configured subgraph endpoints have failed."""


class SubgraphClient:
    """Thread-safe GraphQL client with retry + fallback.

    Parameters
    ----------
    primary_url
        Main endpoint to query.  Defaults to Goldsky public.
    fallback_url
        Optional secondary endpoint, tried only when primary exhausts
        its retry budget.  May contain a ``{api_key}`` placeholder that
        is substituted from ``api_key``; if ``api_key`` is ``None`` and
        the URL contains the placeholder, fallback is disabled.
    api_key
        API key for a fallback endpoint that needs one (e.g. The Graph
        gateway).  Passed via ``{api_key}`` substitution, never logged.
    timeout_s, max_retries, request_interval_s
        Per-endpoint transport knobs.  Defaults come from ``config.py``.
    """

    def __init__(
        self,
        primary_url: str = SUBGRAPH_URL_PRIMARY,
        fallback_url: str | None = SUBGRAPH_URL_FALLBACK,
        api_key: str | None = None,
        *,
        timeout_s: int = SUBGRAPH_TIMEOUT_S,
        max_retries: int = SUBGRAPH_MAX_RETRIES,
        request_interval_s: float = SUBGRAPH_REQUEST_INTERVAL_S,
        logger: logging.Logger | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger("polymarket_pipeline.subgraph")
        self._primary = primary_url
        self._fallback = self._resolve_fallback(fallback_url, api_key)
        self._timeout_s = timeout_s
        self._max_retries = max_retries
        self._request_interval_s = request_interval_s
        self._session = requests.Session()
        self._last_request_ts = 0.0
        self._lock = threading.Lock()

        self.logger.info(
            "SubgraphClient initialized — primary=%s, fallback=%s",
            self._mask(self._primary),
            self._mask(self._fallback) if self._fallback else "disabled",
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query and return its ``data`` payload.

        Retries transient failures on the primary endpoint; on full
        exhaustion tries the fallback.  Raises :class:`SubgraphError`
        when all configured endpoints have failed.
        """
        endpoints: list[tuple[str, str]] = [("primary", self._primary)]
        if self._fallback:
            endpoints.append(("fallback", self._fallback))

        last_exc: Exception | None = None
        for label, url in endpoints:
            for attempt in range(1, self._max_retries + 1):
                try:
                    return self._post_once(url, query, variables)
                except _RetryableError as exc:
                    last_exc = exc
                    delay = self._backoff(attempt)
                    self.logger.warning(
                        "Subgraph %s attempt %d/%d: %s — retrying in %.1fs",
                        label, attempt, self._max_retries, exc, delay,
                    )
                    time.sleep(delay)
                except _FatalError as exc:
                    last_exc = exc
                    break  # don't retry fatal errors on this endpoint
            self.logger.warning(
                "Subgraph %s exhausted retries; %s",
                label,
                "switching to fallback" if label == "primary" and self._fallback else "giving up",
            )

        raise SubgraphError(f"All subgraph endpoints failed; last error: {last_exc}")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _post_once(
        self,
        url: str,
        query: str,
        variables: dict[str, Any] | None,
    ) -> dict[str, Any]:
        self._throttle()
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        try:
            resp = self._session.post(url, json=payload, timeout=self._timeout_s)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            raise _RetryableError(f"transport: {exc}") from exc

        if resp.status_code in (429, 500, 502, 503, 504):
            raise _RetryableError(f"HTTP {resp.status_code}")
        if resp.status_code >= 400:
            raise _FatalError(f"HTTP {resp.status_code}: {self._mask(resp.text)[:200]}")

        try:
            body = resp.json()
        except ValueError as exc:
            raise _RetryableError(f"invalid JSON: {exc}") from exc

        errors = body.get("errors")
        if errors:
            # GraphQL-level errors: treat indexer-lag and similar as
            # retryable, everything else as fatal (query shape bug).
            msg = str(errors)[:200]
            if any(hint in msg.lower() for hint in ("indexer", "timeout", "rate")):
                raise _RetryableError(f"GraphQL transient: {msg}")
            raise _FatalError(f"GraphQL: {msg}")

        data = body.get("data")
        if not isinstance(data, dict):
            raise _FatalError(f"response missing 'data': {self._mask(resp.text)[:200]}")
        return data

    def _throttle(self) -> None:
        """Sleep so consecutive requests are spaced at least ``request_interval_s`` apart."""
        with self._lock:
            now = time.monotonic()
            delta = now - self._last_request_ts
            if delta < self._request_interval_s:
                time.sleep(self._request_interval_s - delta)
            self._last_request_ts = time.monotonic()

    def _backoff(self, attempt: int) -> float:
        raw = min(1.0 * (2 ** (attempt - 1)), 30.0)
        return raw * random.uniform(0.8, 1.2)

    @staticmethod
    def _resolve_fallback(url: str | None, api_key: str | None) -> str | None:
        if not url:
            return None
        if "{api_key}" in url:
            if not api_key:
                return None
            return url.replace("{api_key}", api_key)
        return url

    @staticmethod
    def _mask(url_or_text: str | None) -> str:
        """Redact likely-secret segments from URLs / response bodies for logs."""
        if not url_or_text:
            return ""
        # Redact 32-64 char hex blobs that look like API keys.
        import re
        return re.sub(r"[0-9a-fA-F]{24,}", "<redacted>", url_or_text)


class _RetryableError(Exception):
    """Transient failure — will retry with backoff."""


class _FatalError(Exception):
    """Non-transient failure — skip remaining retries on this endpoint."""
