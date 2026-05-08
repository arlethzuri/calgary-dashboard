"""Shared helpers for source cleaning pipelines."""

from __future__ import annotations

from pathlib import Path

from calgary_dashboard.common.io import ensure_dir


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