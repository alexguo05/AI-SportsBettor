"""Signed HTTP client shared by the Kalshi collectors.

Kalshi authenticates every request with an RSA-PSS signature over
``timestamp_ms + METHOD + path`` (path without query parameters). Rate limits
are token-bucket based (Basic tier: 200 read tokens/second at 10 tokens per
request), so the client paces requests client-side and backs off on 429s,
which carry no Retry-After header.
"""

from __future__ import annotations

import base64
import os
import random
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import dotenv_values

DEFAULT_BASE_URL = "https://api.elections.kalshi.com"
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class KalshiCredentials:
    key_id: str
    private_key_pem: bytes


def load_kalshi_credentials(src_dir: Path) -> KalshiCredentials:
    """Read the API key ID and private-key path from src/.env or the process env."""
    dotenv_path = src_dir / ".env"
    file_values = dotenv_values(dotenv_path) if dotenv_path.exists() else {}
    values = {key: str(value) for key, value in file_values.items() if value is not None}
    values.update(os.environ)
    key_id = values.get("KALSHI_API_KEY_ID")
    key_path = values.get("KALSHI_PRIVATE_KEY_PATH")
    if not key_id or not key_path:
        raise ValueError(
            "KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH must be configured"
        )
    pem = Path(key_path).expanduser().read_bytes()
    return KalshiCredentials(key_id=key_id, private_key_pem=pem)


class KalshiClient:
    """Signed GET client with retries and client-side request pacing."""

    def __init__(
        self,
        *,
        credentials: KalshiCredentials,
        base_url: str = DEFAULT_BASE_URL,
        session: requests.Session | None = None,
        timeout_seconds: float = 30,
        max_attempts: int = 5,
        min_request_interval_seconds: float = 0.07,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[float, float], float] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if min_request_interval_seconds < 0:
            raise ValueError("min_request_interval_seconds cannot be negative")
        self.key_id = credentials.key_id
        self.private_key = serialization.load_pem_private_key(
            credentials.private_key_pem, password=None
        )
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.min_request_interval_seconds = min_request_interval_seconds
        self.sleep = sleep
        self.jitter = jitter or random.uniform
        self.monotonic = monotonic
        self.headers = {"User-Agent": "ai-sports-bettor-kalshi-ingest/1.0"}
        self._last_request_at: float | None = None

    def _signed_headers(self, method: str, path: str) -> dict[str, str]:
        timestamp = str(int(datetime.now(UTC).timestamp() * 1000))
        message = (timestamp + method + path.split("?")[0]).encode("utf-8")
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            **self.headers,
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("ascii"),
        }

    def _pace(self) -> None:
        if self._last_request_at is not None:
            elapsed = self.monotonic() - self._last_request_at
            remaining = self.min_request_interval_seconds - elapsed
            if remaining > 0:
                self.sleep(remaining)
        self._last_request_at = self.monotonic()

    def get_json(
        self,
        path: str,
        params: dict[str, str] | list[tuple[str, str]] | None = None,
        *,
        description: str = "request",
    ) -> tuple[dict[str, Any], int]:
        """GET ``path`` and return (parsed JSON object, attempts used)."""
        for attempt in range(1, self.max_attempts + 1):
            self._pace()
            response: requests.Response | None = None
            try:
                response = self.session.get(
                    self.base_url + path,
                    params=params,
                    headers=self._signed_headers("GET", path),
                    timeout=self.timeout_seconds,
                )
                if response.status_code not in RETRYABLE_STATUSES:
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise ValueError(f"Kalshi {description} response must be an object")
                    return payload, attempt
                if attempt == self.max_attempts:
                    response.raise_for_status()
            except (requests.ConnectionError, requests.Timeout):
                if attempt == self.max_attempts:
                    raise
            delay = min(30.0, 2.0**attempt) + self.jitter(0.0, 1.0)
            status = response.status_code if response is not None else "network error"
            print(
                f"WARNING: Kalshi {description} returned {status}; retrying "
                f"attempt {attempt + 1}/{self.max_attempts} in {delay:.1f}s",
                file=sys.stderr,
            )
            self.sleep(delay)
        raise RuntimeError("unreachable Kalshi retry state")
