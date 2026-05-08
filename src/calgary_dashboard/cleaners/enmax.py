"""ENMAX cleaner.

Converts raw ENMAX ArcGIS JSON payloads into:
- cleaned metadata JSON files
- GeoParquet feature files grouped by geometry type
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd

from calgary_dashboard.common.cleaning import (
    list_subdirectories,
    prepare_output_dirs,
    resolve_snapshot,
)
from calgary_dashboard.common.io import ensure_dir, read_json, write_json
from calgary_dashboard.common.naming import standardized_file_name
from calgary_dashboard.config.logging import configure_logger
from calgary_dashboard.config.paths import PROCESSED_DATA_ROOT, RAW_DATA_ROOT

RAW_ENMAX_ROOT = RAW_DATA_ROOT / "enmax"
PROCESSED_ENMAX_ROOT = PROCESSED_DATA_ROOT / "enmax"


def _base_name_from_layer_file(layer_path: Path) -> str:
    """Extract the base name from a layer file path."""
    stem = layer_path.stem
    if stem.endswith("_features"):
        return stem[: -len("_features")]
    if stem.endswith("_metadata"):
        return stem[: -len("_metadata")]
    return stem


def clean_snapshot(snapshot_date: str | None = None) -> Path:
    """Clean one ENMAX snapshot and return processed output directory."""
    resolved_snapshot = resolve_snapshot(RAW_ENMAX_ROOT, snapshot_date)
    raw_snapshot_dir = RAW_ENMAX_ROOT / resolved_snapshot
    if not raw_snapshot_dir.exists():
        raise FileNotFoundError(f"Raw ENMAX snapshot not found: {raw_snapshot_dir}")

    processed_snapshot_dir, features_root, metadata_root = prepare_output_dirs(
        PROCESSED_ENMAX_ROOT, resolved_snapshot
    )
    logger = configure_logger(
        __name__,
        processed_snapshot_dir / f"en_data_prep_{resolved_snapshot}.log",
    )

    feature_server_dirs = list_subdirectories(raw_snapshot_dir)
    logger.info("Found %d feature server folders in %s", len(feature_server_dirs), raw_snapshot_dir)

    for server_dir in feature_server_dirs:
        feature_files = sorted(server_dir.glob("*_features.json"))
        metadata_files = sorted(server_dir.glob("*_metadata.json"))

        for feature_path in feature_files:
            feature_obj = read_json(feature_path)
            base_name = _base_name_from_layer_file(feature_path)
            file_name = standardized_file_name(None, base_name, "features")

            try:
                gdf = gpd.GeoDataFrame.from_features(feature_obj)
            except Exception:
                logger.exception("Failed to build GeoDataFrame for %s", feature_path)
                # Preserve payload for debugging and downstream recovery.
                write_json(features_root / f"{file_name}.json", feature_obj)
                logger.info("Saved fallback JSON %s.json", file_name)
                continue

            # if gdf is empty or has no valid geometry, save as json
            if gdf.empty or not gdf.geometry.notna().any():
                write_json(features_root / f"{file_name}.json", feature_obj)
                logger.info("Saved fallback JSON %s.json", file_name)
                continue

            # save as parquet under directory corresponding to the file's
            # geometry type
            geom_type = gdf.geom_type.dropna().iloc[0].lower()
            geom_dir = ensure_dir(features_root / geom_type)
            parquet_path = geom_dir / f"{file_name}.parquet"
            gdf.to_parquet(parquet_path)
            logger.info("Saved features %s (%d rows, geom=%s)", parquet_path.name, len(gdf), geom_type)

        for metadata_path in metadata_files:
            metadata_obj = read_json(metadata_path)
            # add feature-server provenance to each layer metadata record
            metadata_obj["layer_name"] = server_dir.name
            base_name = _base_name_from_layer_file(metadata_path)
            file_name = standardized_file_name(None, base_name, "metadata")
            write_json(metadata_root / f"{file_name}.json", metadata_obj)
            logger.info("Saved metadata %s.json", file_name)

    return processed_snapshot_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean ENMAX raw snapshot data.")
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default=None,
        help="Snapshot folder name in YYYYMMDD. Defaults to latest snapshot.",
    )
    args = parser.parse_args()

    output_dir = clean_snapshot(snapshot_date=args.snapshot_date)
    print(f"ENMAX cleaning complete: {output_dir}")


if __name__ == "__main__":
    main()

