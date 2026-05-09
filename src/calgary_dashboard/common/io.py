"""File and directory helpers shared across pipelines."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def ensure_dir(path: Path) -> Path:
    """Create a directory path if needed and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path

def read_json(path: Path) -> Any:
    """Read JSON file content."""
    with path.open("r") as file_obj:
        return json.load(file_obj)


def write_json(path: Path, payload: Any, *, indent: int = 2) -> None:
    """Write JSON file content."""
    ensure_dir(path.parent)
    with path.open("w") as file_obj:
        json.dump(payload, file_obj, indent=indent)
        file_obj.write("\n")

