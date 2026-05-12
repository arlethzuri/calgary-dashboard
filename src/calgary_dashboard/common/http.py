"""Lightweight HTTP helpers for consistent request behavior."""

from __future__ import annotations

from typing import Any

import requests

def get_json(
    url: str,
    headers: dict[str, str] | None = None,
) -> Any:
    """GET a URL and return parsed JSON, raising for non-2xx responses."""
    response = requests.get(url, headers=headers or {})
    response.raise_for_status()
    return response.json()
