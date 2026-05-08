"""Project path helpers."""

from __future__ import annotations

from pathlib import Path

from .settings import get_settings

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src" / "calgary_dashboard"

_settings = get_settings()
DATA_ROOT = (
    Path(_settings.data_root).expanduser()
    if _settings.data_root
    else PROJECT_ROOT / "data" / "calgary"
)
RAW_DATA_ROOT = DATA_ROOT / "data"
PROCESSED_DATA_ROOT = DATA_ROOT / "processed_data"
LOG_ROOT = PROJECT_ROOT / "logs"
