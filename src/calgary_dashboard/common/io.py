"""File, directory, and snapshot-layout helpers shared across pipelines."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from calgary_dashboard.config.paths import SRC_ROOT


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


def is_snapshot_name(name: str) -> bool:
    """Return True when folder name matches YYYYMMDD snapshot format."""
    return name.isdigit() and len(name) == 8


def list_subdirectories(root: Path) -> list[Path]:
    """Return sorted child directories under root."""
    return sorted(path for path in root.iterdir() if path.is_dir())


def latest_snapshot(snapshot_root: Path) -> str:
    """Return latest snapshot directory name (YYYYMMDD-like)."""
    # Restrict to numeric folder names so helper dirs (e.g. manual_download)
    # are never selected as a "latest snapshot".
    snapshots = sorted(
        path.name
        for path in snapshot_root.iterdir()
        if path.is_dir() and is_snapshot_name(path.name)
    )
    if not snapshots:
        raise FileNotFoundError(f"No snapshot folders found in {snapshot_root}")
    return snapshots[-1]


def resolve_snapshot(snapshot_root: Path, snapshot_date: str | None = None) -> str:
    """Return explicit snapshot_date or latest snapshot folder name."""
    if snapshot_date:
        return snapshot_date
    return latest_snapshot(snapshot_root)


def prepare_output_dirs(output_root: Path, snapshot_date: str) -> tuple[Path, Path, Path]:
    """Create and return (snapshot_dir, features_dir, metadata_dir)."""
    snapshot_dir = ensure_dir(output_root / snapshot_date)
    features_dir = ensure_dir(snapshot_dir / "features")
    metadata_dir = ensure_dir(snapshot_dir / "metadata")
    return snapshot_dir, features_dir, metadata_dir

def resolve_latest_catalog() -> Path:
    """Resolve latest generated catalog JSON from src/calgary_dashboard/catalog."""
    catalog_dir = SRC_ROOT / "catalog"
    candidates = sorted(catalog_dir.glob("processed_data_catalog_*.json"))
    if not candidates:
        raise FileNotFoundError(
            f"No processed_data_catalog_*.json files under {catalog_dir}"
        )
    return candidates[-1]