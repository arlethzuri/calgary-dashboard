"""Date and snapshot helpers."""

from __future__ import annotations

from datetime import datetime


def snapshot_date() -> str:
    """Return current date in YYYYMMDD format for snapshot folders."""
    return datetime.now().strftime("%Y%m%d")

