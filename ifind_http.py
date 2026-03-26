#!/usr/bin/env python3
"""Minimal iFind HTTP client based on refresh/access token auth."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

import requests

ACCESS_TOKEN_URL = "https://quantapi.51ifind.com/api/v1/get_access_token"
DEFAULT_BASE_URL = "https://quantapi.51ifind.com/api/v1"
DEFAULT_TIMEOUT = 30


class IFindHTTPError(RuntimeError):
    """Raised when the iFind HTTP API returns an error."""


@dataclass
class TokenState:
    access_token: str
    expires_at: float


class IFindHTTPClient:
    """Small helper for iFind HTTP auth and JSON POST requests."""

    def __init__(
        self,
        refresh_token: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.refresh_token = (refresh_token or os.getenv("IFIND_REFRESH_TOKEN", "")).strip()
        if not self.refresh_token:
            raise RuntimeError("缺少 IFIND_REFRESH_TOKEN 环境变量")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._token_state: TokenState | None = None

    def _request_access_token(self) -> TokenState:
        resp = requests.post(
            ACCESS_TOKEN_URL,
            headers={
                "Content-Type": "application/json",
                "refresh_token": self.refresh_token,
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if str(data.get("errorcode", 0)) not in {"0", "00", ""}:
            raise IFindHTTPError(f"get_access_token failed: {json.dumps(data, ensure_ascii=False)}")
        token = str(data.get("data", {}).get("access_token", "")).strip()
        if not token:
            raise IFindHTTPError(f"get_access_token returned empty token: {json.dumps(data, ensure_ascii=False)}")
        # Official docs say access_token is valid for 7 days; refresh slightly early.
        return TokenState(access_token=token, expires_at=time.time() + 6.5 * 24 * 3600)

    def _ensure_access_token(self) -> str:
        if self._token_state is None or time.time() >= self._token_state.expires_at:
            self._token_state = self._request_access_token()
        return self._token_state.access_token

    def post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        token = self._ensure_access_token()
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}/{endpoint.lstrip('/')}"
        resp = requests.post(
            url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "access_token": token,
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if str(data.get("errorcode", 0)) not in {"0", "00", ""}:
            raise IFindHTTPError(f"{endpoint} failed: {json.dumps(data, ensure_ascii=False)}")
        return data


def build_client() -> IFindHTTPClient:
    return IFindHTTPClient()
