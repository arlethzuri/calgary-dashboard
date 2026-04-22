#!/usr/bin/env python3
"""
Build a JSON catalog of files under ``data/calgary/processed_data`` for a *single* snapshot.

Output
    Pretty-printed JSON: summary counts, one ``files[]`` record per file, and
    ``metadata_feature_pairings`` linking Socrata-style ``*_metadata.json`` to feature
    Parquet/JSON (Open Calgary uses ``*_feature.*``; Enmax uses ``*_features.parquet``).

Optional Parquet schemas
    If polars or pyarrow is installed, each ``.parquet`` row can include column names and
    dtypes (footer read only). Use ``--no-schema`` to skip that for speed or when those
    libraries are not installed.
"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from __future__ import annotations

# ``argparse`` builds the CLI (--root, --snapshot, --out, --no-schema).
import argparse
# ``json.dumps`` serializes the catalog; UTF-8 text written to disk.
import json
# ``sys.stderr`` / ``sys.stdin.isatty()`` for prompts vs CI, and stderr status lines.
import sys
# ``dataclass`` defines ``FileEntry``; ``asdict`` turns instances into JSON-serializable dicts.
from dataclasses import asdict, dataclass
# ``datetime`` stamps when the catalog was generated.
from datetime import datetime, timezone
# ``Path`` handles paths on all OSes, ``relative_to``, ``rglob``, ``iterdir``.
from pathlib import Path
# Loose typing for the optional ``schema`` dict (column names + dtype strings).
from typing import Any

# -----------------------------------------------------------------------------
# Default scan root
# -----------------------------------------------------------------------------
# This file lives in ``ontology/``; repo root is two parents up, then into ``data/...``.
# Override with ``--root`` if your checkout or data lives elsewhere.
DEFAULT_ROOT = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "calgary"
    / "processed_data"
)


def _is_snapshot_name(name: str) -> bool:
    """
    Snapshot folders in this project are named like YYYYMMDD (export / freeze date).

    We only treat *exactly* eight digits as a snapshot id so random folder names are ignored.
    """
    return name.isdigit() and len(name) == 8


def _discover_snapshots(processed_root: Path) -> list[str]:
    """
    Find every distinct snapshot folder name that appears under any vendor directory.

    Layout assumed: ``processed_root / <vendor> / <YYYYMMDD> / ...``
    We do *not* recurse deep; only immediate children of each vendor are candidates.
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


def _snapshot_exists_anywhere(processed_root: Path, snapshot: str) -> bool:
    """
    Return True if *at least one* vendor has a directory ``<vendor>/<snapshot>/``.

    Used after the user passes ``--snapshot`` to fail fast with a helpful error instead of
    writing an empty catalog.
    """
    if not _is_snapshot_name(snapshot):
        return False
    for vendor in processed_root.iterdir():
        if not vendor.is_dir():
            continue
        if (vendor / snapshot).is_dir():
            return True
    return False


def _resolve_snapshot_interactively(processed_root: Path, snapshot: str | None) -> str:
    """
    Decide which snapshot string the rest of the script will use.

    Branch A — ``snapshot`` is not None (user passed ``--snapshot``)
        Validate eight-digit form and that some ``<vendor>/<snapshot>/`` exists; return it.

    Branch B — ``snapshot`` is None (user did not pass ``--snapshot``)
        1. Discover candidates via ``_discover_snapshots``.
        2. If none: exit (nothing to catalog).
        3. If exactly one: print to stderr and return it (convenient for single-date repos).
        4. If several and stdin is not a TTY (e.g. CI pipe): exit with message to pass
           ``--snapshot`` explicitly—no blocking ``input()``.
        5. If several and TTY: print menu to stderr, loop ``input()`` until valid number
           or raw YYYYMMDD string.

    Returns:
        The chosen snapshot id, e.g. ``"20260327"``.
    """
    if snapshot is not None:
        if not _is_snapshot_name(snapshot):
            raise SystemExit(f"Snapshot must be an 8-digit YYYYMMDD folder name, got: {snapshot!r}")
        if not _snapshot_exists_anywhere(processed_root, snapshot):
            raise SystemExit(
                f"No data under {processed_root} for snapshot {snapshot!r}. "
                f"Known snapshots: {_discover_snapshots(processed_root) or '(none)'}"
            )
        return snapshot

    candidates = _discover_snapshots(processed_root)
    if not candidates:
        raise SystemExit(f"No YYYYMMDD snapshot folders found under {processed_root}")
    if len(candidates) == 1:
        only = candidates[0]
        print(f"Using snapshot {only} (only one found under {processed_root}).", file=sys.stderr)
        return only

    if not sys.stdin.isatty():
        raise SystemExit(
            "Multiple snapshots found; pass --snapshot YYYYMMDD non-interactively. "
            f"Options: {', '.join(candidates)}"
        )

    print("Select snapshot:", file=sys.stderr)
    for i, s in enumerate(candidates, 1):
        print(f"  {i}. {s}", file=sys.stderr)
    while True:
        raw = input("Enter number or YYYYMMDD: ").strip()
        if raw in candidates:
            return raw
        if raw.isdigit() and 1 <= int(raw) <= len(candidates):
            return candidates[int(raw) - 1]
        print("Invalid choice; try again.", file=sys.stderr)


# -----------------------------------------------------------------------------
# ``FileEntry`` — one JSON object inside the top-level ``"files"`` array
# -----------------------------------------------------------------------------
@dataclass
class FileEntry:
    # Path from ``processed_data`` root using forward slashes (good for URLs and Neo4j props).
    relative_path: str
    # First path segment: ``enmax``, ``open_calgary``, or any future vendor folder name.
    source: str
    # The single snapshot this catalog run was built for (duplicated on each row on purpose
    # so each file record is self-contained if you split the JSON later).
    snapshot: str
    # High-level kind: ``feature`` (under ``features/``), ``metadata``, ``log``, or ``other``.
    role: str
    # When features are grouped by geometry type: ``point``, ``polygon``, etc.; else None.
    geometry_bucket: str | None
    # Normalized suffix, e.g. ``.parquet``, ``.json``, ``.log``.
    extension: str
    # Basename without extension; used to strip ``_metadata`` / match ``_feature`` / ``_features``.
    filename: str
    # ``os.stat`` size in bytes (handy for estimating load time / storage in Neo4j).
    size_bytes: int
    # Optional: ``{"columns": [...], "dtypes": [...]}`` for Parquet when libraries succeed.
    schema: dict[str, Any] | None = None


def _classify_under_snapshot(rel: Path, snapshot: str) -> tuple[str, str, str | None]:
    """
    Turn a path *relative to processed_data* into ``(source, role, geometry_bucket)``.

    Preconditions (enforced by ``_walk_snapshot``):
        ``rel`` should look like ``enmax/20260327/features/...`` so ``parts[1]`` equals
        ``snapshot``. If not, we return ``role="other"`` so bad paths do not corrupt counts
        silently.

    Path semantics:
        - ``parts[0]`` → vendor / ``source``
        - ``parts[1]`` → must match ``snapshot``
        - ``parts[2:]`` → remainder: ``metadata/...``, ``features/point/...``, logs, etc.

    Returns:
        ``(source, role, geometry_bucket)``.
    """
    parts = rel.parts
    if len(parts) < 3:
        # Need at least vendor / snapshot / something
        return "unknown", "other", None

    source = parts[0]
    if parts[1] != snapshot:
        # Defensive: walker should only emit paths under the chosen snapshot directory.
        return source, "other", None

    # Everything after ``<vendor>/<snapshot>/``
    rest = list(parts[2:])
    role = "other"
    geometry_bucket: str | None = None

    if "metadata" in rest:
        role = "metadata"
    elif "features" in rest:
        role = "feature"
        fi = rest.index("features")
        # Pattern ``features/<geometry>/file`` — the segment after ``features`` if it is a
        # known geometry bucket name (Enmax layout; Open Calgary geo splits).
        if fi + 1 < len(rest) and rest[fi + 1] in (
            "linestring",
            "multipolygon",
            "point",
            "polygon",
        ):
            geometry_bucket = rest[fi + 1]
    elif any("_data_prep_" in p for p in parts):
        # ETL log files from your pipeline naming convention.
        role = "log"
    elif parts[-1].endswith(".log"):
        role = "log"

    return source, role, geometry_bucket


def _parquet_schema(path: Path) -> dict[str, Any] | None:
    """
    Try to read column names and Arrow types from a Parquet file without loading rows.

    Order of attempts:
        1. Polars lazy scan + ``collect_schema()`` — reads footer metadata.
        2. ``pyarrow.parquet.read_schema`` — same idea if polars missing or errors.

    Returns:
        A small dict suitable for JSON, or ``None`` if both fail (missing deps, corrupt file).
    """
    try:
        import polars as pl  # type: ignore[import-not-found]

        schema = pl.scan_parquet(path).collect_schema()
        return {
            "columns": list(schema.names()),
            "dtypes": [str(t) for t in schema.dtypes()],
        }
    except Exception:
        pass
    try:
        import pyarrow.parquet as pq  # type: ignore[import-not-found]

        schema = pq.read_schema(path)
        return {
            "columns": schema.names,
            "dtypes": [str(schema.field(i).type) for i in range(len(schema.names))],
        }
    except Exception:
        return None


def _walk_snapshot(
    processed_root: Path, snapshot: str, include_schema: bool = True
) -> list[FileEntry]:
    """
    Enumerate every file under each ``<vendor>/<snapshot>/`` directory that exists.

    Vendors are *direct subdirectories* of ``processed_root`` (``enmax``, ``open_calgary``).
    If a vendor has no ``<snapshot>`` folder, it is skipped—no error (you might only have
    one provider for a given date).

    Each discovered file becomes one ``FileEntry`` with ``snapshot`` set to the same
    string passed in, so downstream code never has to infer the date from the path again.

    Args:
        processed_root: The ``processed_data`` directory (contains vendor folders).
        snapshot: Resolved YYYYMMDD string.
        include_schema: When False, skip Parquet footer reads entirely (faster / no deps).

    Returns:
        Sorted list of ``FileEntry`` (sort key: ``relative_path`` for stable diffs).
    """
    entries: list[FileEntry] = []
    # Sorted vendor iteration → deterministic catalog order across machines.
    for vendor_dir in sorted(processed_root.iterdir()):
        if not vendor_dir.is_dir():
            continue
        snap_dir = vendor_dir / snapshot
        if not snap_dir.is_dir():
            # This vendor simply has no export for the chosen date.
            continue
        source_name = vendor_dir.name
        # Only descend inside this snapshot folder—never sibling dates.
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
            schema = None
            if include_schema and ext == ".parquet":
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

    Because the whole ``entries`` list is already scoped to one snapshot, pairing is a
    single pass: collect all ``role == "feature"`` rows, then for each metadata JSON strip
    the ``_metadata`` suffix from the basename and look for feature files whose stem is
    ``<base>_feature`` (Open Calgary) or ``<base>_features`` (Enmax plural).
    """
    features = [e for e in entries if e.role == "feature"]
    pairings: list[dict[str, Any]] = []
    for e in entries:
        if e.role != "metadata" or not e.relative_path.endswith("_metadata.json"):
            continue
        # ``e.filename`` is stem: ``foo_metadata`` from ``foo_metadata.json``
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


def _summarize(entries: list[FileEntry], snapshot: str) -> dict[str, Any]:
    """
    Aggregate counts for the ``summary`` block in the output JSON.

    ``snapshot`` is passed explicitly so the summary always documents the run even if
    ``entries`` were empty (edge case: valid snapshot path but no files).
    """
    by_source: dict[str, int] = {}
    by_role: dict[str, int] = {}
    by_ext: dict[str, int] = {}
    for e in entries:
        by_source[e.source] = by_source.get(e.source, 0) + 1
        by_role[e.role] = by_role.get(e.role, 0) + 1
        by_ext[e.extension] = by_ext.get(e.extension, 0) + 1
    return {
        "snapshot": snapshot,
        "file_count": len(entries),
        "by_source": dict(sorted(by_source.items())),
        "by_role": dict(sorted(by_role.items())),
        "by_extension": dict(sorted(by_ext.items())),
    }


def main() -> None:
    """
    CLI entry: parse args, resolve snapshot, walk disk, write JSON, print one status line.

    Default output file name includes the snapshot so two runs for different dates do not
    overwrite each other unless you pass an explicit ``--out``.
    """
    parser = argparse.ArgumentParser(
        description="Build JSON catalog for one processed_data snapshot (Neo4j planning)."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="processed_data root (default: repo data/calgary/processed_data)",
    )
    parser.add_argument(
        "--snapshot",
        type=str,
        default=None,
        metavar="YYYYMMDD",
        help="Snapshot folder to scan. If omitted: use the only snapshot if unique, else prompt.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output JSON path (default: ontology/processed_data_catalog_<snapshot>.json)",
    )
    parser.add_argument(
        "--no-schema",
        action="store_true",
        help="Do not load Parquet schemas (skip polars/pyarrow)",
    )
    args = parser.parse_args()
    # ``~`` expansion for home-relative ``--root`` paths; ``resolve()`` → absolute for JSON.
    processed_root: Path = args.root.expanduser().resolve()

    if not processed_root.is_dir():
        raise SystemExit(f"Root is not a directory: {processed_root}")

    # Single string used for walking, stamping rows, and default output filename.
    snapshot = _resolve_snapshot_interactively(processed_root, args.snapshot)
    entries = _walk_snapshot(processed_root, snapshot, include_schema=not args.no_schema)

    out_path = args.out
    if out_path is None:
        out_path = (
            Path(__file__).resolve().parent / f"processed_data_catalog_{snapshot}.json"
        )

    # One JSON document: metadata at top, big arrays below.
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "root": str(processed_root),
        "snapshot": snapshot,
        "summary": _summarize(entries, snapshot),
        "files": [asdict(e) for e in entries],
        "metadata_feature_pairings": _pair_metadata_features(entries),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(entries)} file records for snapshot {snapshot} to {out_path}")


# Run ``main()`` only when executed as a script; importing this module defines helpers only.
if __name__ == "__main__":
    main()
