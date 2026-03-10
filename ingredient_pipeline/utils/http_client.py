from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class TransientHttpError(Exception):
    """Retryable HTTP failures."""


@dataclass(slots=True)
class RateLimitedSession:
    session: requests.Session
    requests_per_second: float
    timeout_seconds: int
    max_retries: int
    _last_request_ts: float = 0.0

    def _sleep_if_needed(self) -> None:
        min_interval = 1.0 / self.requests_per_second if self.requests_per_second > 0 else 0
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_ts = time.monotonic()

    @retry(
        retry=retry_if_exception_type(TransientHttpError),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self._sleep_if_needed()
        timeout = kwargs.pop("timeout", self.timeout_seconds)
        response = self.session.request(method, url, timeout=timeout, **kwargs)
        if response.status_code in {429, 500, 502, 503, 504}:
            raise TransientHttpError(f"{method} {url} failed with {response.status_code}")
        return response


def build_session(user_agent: str, requests_per_second: float, timeout_seconds: int, max_retries: int) -> RateLimitedSession:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    client = RateLimitedSession(
        session=session,
        requests_per_second=requests_per_second,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    return client


def save_binary_payload(target_path: Path, payload: bytes) -> str:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(payload)
    return sha256_bytes(payload)


def save_json_payload(target_path: Path, payload: Any) -> str:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    target_path.write_text(content, encoding="utf-8")
    return sha256_bytes(content.encode("utf-8"))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
