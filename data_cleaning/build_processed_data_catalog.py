#!/usr/bin/env python3
"""
Build a JSON catalog of files under data/calgary/processed_data for all YYYYMMDD
snapshots found under vendor dirs.

Output:
    Pretty-printed JSON: summary counts, one files[] record per file, and
    metadata_feature_pairings linking Socrata-style *_metadata.json to feature
    Parquet/JSON (Open Calgary uses *_feature.*; Enmax uses *_features.parquet).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

# Default scan root
DEFAULT_ROOT = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "calgary"
    / "processed_data"
)
DEFAULT_OUT = Path(__file__).resolve().parent / f"processed_data_catalog_{datetime.now().strftime('%Y%m%d')}.json"

def _is_snapshot_name(name: str) -> bool:
    """
    Snapshot folders in this project are named like YYYYMMDD (download date), ignore other folders.
    """
    return name.isdigit() and len(name) == 8


def _discover_snapshots(processed_root: Path) -> list[str]:
    """
    Find every distinct snapshot folder name that appears under any vendor directory.

    Layout assumed: processed_root / <vendor> / <YYYYMMDD> / ...
    We do not recurse deep; only immediate children of each vendor are candidates.
    """
    found: set[str] = set()
    for vendor in processed_root.iterdir():
        if not vendor.is_dir():
            continue
        # e.g. enmax/20260327, open_calgary/20260327
        for child in vendor.iterdir():
            if child.is_dir() and _is_snapshot_name(child.name):
                found.add(child.name)
    return sorted(found)


# -----------------------------------------------------------------------------
# FileEntry - one JSON object inside the top-level "files" array
# -----------------------------------------------------------------------------
@dataclass
class FileEntry:
    relative_path: str
    # e.g. enmax, open calgary.
    source: str
    snapshot: str
    # e.g. feature, metadata, log, other.
    role: str
    # geojson type if any
    geometry_bucket: str | None
    # e.g. .parquet, .json, .log.
    extension: str
    # e.g. foo_feature, foo_features.
    filename: str
    # size in bytes
    size_bytes: int
    # Optional: {"columns": [...], "dtypes": [...]} for Parquet when libraries succeed.
    schema: dict[str, Any] | None = None


def _classify_under_snapshot(rel: Path, snapshot: str) -> tuple[str, str, str | None]:
    """
    Turn a path relative to processed_data into (source, role, geometry_bucket).

    Path semantics:
        - parts[0] - vendor / source
        - parts[1] - must match snapshot
        - parts[2:] - remainder: metadata/...`, features/point/...`, logs, etc.
    """
    parts = rel.parts
    if len(parts) < 3:
        # need at least vendor / snapshot / something
        return "unknown", "other", None

    source = parts[0]
    if parts[1] != snapshot:
        # Defensive: walker should only emit paths under the chosen snapshot directory.
        return source, "other", None

    # Everything after vendor / snapshot /
    rest = list(parts[2:])
    role = "other"
    geometry_bucket: str | None = None

    if "metadata" in rest:
        role = "metadata"
    elif "features" in rest:
        role = "feature"
        fi = rest.index("features")
        # Pattern features/<geometry>/file - the segment after features if it is a
        # known geometry bucket name (Enmax layout; Open Calgary geo splits).
        if fi + 1 < len(rest) and rest[fi + 1] in (
            "linestring",
            "multipolygon",
            "point",
            "polygon",
        ):
            geometry_bucket = rest[fi + 1]
    elif any("_data_prep_" in p for p in parts):
        # log files from pipeline naming convention.
        role = "log"
    elif parts[-1].endswith(".log"):
        role = "log"

    return source, role, geometry_bucket

def _parquet_schema(path: Path) -> dict[str, Any] | None:
    """
    Column names and dtypes via pyarrow (reads footer metadata only).
    """
    try:
        schema = pq.read_schema(path)
        return {
            "columns": schema.names,
            "dtypes": [str(schema.field(i).type) for i in range(len(schema.names))],
        }
    except Exception:
        return None


def _walk_snapshot(
    processed_root: Path, snapshot: str
) -> list[FileEntry]:
    """
    Enumerate every file under each vendor / snapshot / directory that exists.

    Vendors are direct subdirectories of processed_root (enmax, open_calgary).

    Each discovered file becomes one FileEntry

    Args:
        processed_root: The processed_data directory (contains vendor folders).
        snapshot: Resolved YYYYMMDD string.

    Returns:
        Sorted list of FileEntry (sort key: relative_path for stable diffs).
    """
    entries: list[FileEntry] = []

    for vendor_dir in sorted(processed_root.iterdir()):
        if not vendor_dir.is_dir():
            continue
        snap_dir = vendor_dir / snapshot
        if not snap_dir.is_dir():
            # This vendor simply has no export for the chosen snapshot.
            continue
        source_name = vendor_dir.name
        # Only descend inside this snapshot folder.
        for p in snap_dir.rglob("*"):
            if not p.is_file():
                continue
            rel = p.relative_to(processed_root)
            rel_s = rel.as_posix()
            source, role, geometry_bucket = _classify_under_snapshot(rel, snapshot)
            if source == "unknown":
                # Rare short path; fall back to directory name we walked from.
                source = source_name
            ext = p.suffix.lower()
            schema = _parquet_schema(p)
            st = p.stat()

            entries.append(
                FileEntry(
                    relative_path=rel_s,
                    source=source,
                    snapshot=snapshot,
                    role=role,
                    geometry_bucket=geometry_bucket,
                    extension=ext,
                    filename=p.stem,
                    size_bytes=st.st_size,
                    schema=schema,
                )
            )
    entries.sort(key=lambda e: e.relative_path)
    return entries


def _pair_metadata_features(entries: list[FileEntry]) -> list[dict[str, Any]]:
    """
    Build rows that connect catalog metadata files to their feature layers.

    Because the whole entries list is already scoped to one snapshot, pairing is a
    single pass: collect all role == "feature" rows, then for each metadata JSON strip
    the _metadata suffix from the basename and look for feature files whose stem is <base>_feature.
    """
    features = [e for e in entries if e.role == "feature"]
    pairings: list[dict[str, Any]] = []
    for e in entries:
        if e.role != "metadata" or not e.relative_path.endswith("_metadata.json"):
            continue
        # e.filename is stem: foo_metadata from foo_metadata.json
        base = e.filename[: -len("_metadata")] if e.filename.endswith("_metadata") else e.filename
        candidates = [
            f.relative_path
            for f in features
            if f.filename in (f"{base}_feature", f"{base}_features")
        ]
        pairings.append(
            {
                "snapshot": e.snapshot,
                "source": e.source,
                "asset_key": base,
                "metadata_relative_path": e.relative_path,
                "feature_relative_paths": sorted(candidates),
            }
        )
    pairings.sort(key=lambda x: (x["snapshot"], x["source"], x["asset_key"]))
    return pairings


def _summarize(entries: list[FileEntry], snapshots: list[str]) -> dict[str, Any]:
    by_source: dict[str, int] = {}
    by_role: dict[str, int] = {}
    by_ext: dict[str, int] = {}
    by_snapshot: dict[str, int] = {}
    for e in entries:
        by_source[e.source] = by_source.get(e.source, 0) + 1
        by_role[e.role] = by_role.get(e.role, 0) + 1
        by_ext[e.extension] = by_ext.get(e.extension, 0) + 1
        by_snapshot[e.snapshot] = by_snapshot.get(e.snapshot, 0) + 1
    return {
        "snapshots": snapshots,
        "file_count": len(entries),
        "by_source": dict(sorted(by_source.items())),
        "by_role": dict(sorted(by_role.items())),
        "by_extension": dict(sorted(by_ext.items())),
        "by_snapshot": dict(sorted(by_snapshot.items())),
    }


def main() -> None:
    processed_root = DEFAULT_ROOT.resolve()

    if not processed_root.is_dir():
        raise SystemExit(f"Root is not a directory: {processed_root}")

    discovered = _discover_snapshots(processed_root)
    if not discovered:
        raise SystemExit(f"No YYYYMMDD snapshot folders found under {processed_root}")
    snapshots = discovered

    entries: list[FileEntry] = []
    for snap in snapshots:
        entries.extend(_walk_snapshot(processed_root, snap))

    out_path = DEFAULT_OUT

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "root": str(processed_root),
        "snapshots": snapshots,
        "summary": _summarize(entries, snapshots),
        "files": [asdict(e) for e in entries],
        "metadata_feature_pairings": _pair_metadata_features(entries),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    snap_msg = ", ".join(snapshots)
    print(f"Wrote {len(entries)} file records for snapshot(s) {snap_msg} to {out_path}")


# Run ``main()`` only when executed as a script; importing this module defines helpers only.
if __name__ == "__main__":
    main()
