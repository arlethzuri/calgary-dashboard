"""Backward-compatible config shim.

Prefer importing from:
- calgary_dashboard.config.settings
- calgary_dashboard.config.paths
- calgary_dashboard.config.logging
"""

from .paths import DATA_ROOT, LOG_ROOT, PROCESSED_DATA_ROOT, PROJECT_ROOT, RAW_DATA_ROOT
from .settings import Settings, get_settings

__all__ = [
    "DATA_ROOT",
    "LOG_ROOT",
    "PROCESSED_DATA_ROOT",
    "PROJECT_ROOT",
    "RAW_DATA_ROOT",
    "Settings",
    "get_settings",
]
