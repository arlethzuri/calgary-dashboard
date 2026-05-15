"""Open Calgary cleaner.

Converts downloaded Open Calgary JSON payloads into:
- cleaned metadata JSON files
- GeoParquet feature files when valid geometries exist
- fallback JSON feature files when geometries cannot be parsed
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
from shapely.geometry import shape

from calgary_dashboard.common.crs_from_metadata import infer_crs_from_open_calgary_metadata
from calgary_dashboard.common.definitions import GEOMETRY_FIELD_NAMES
from calgary_dashboard.common.io import (
    ensure_dir,
    list_subdirectories,
    prepare_output_dirs,
    read_json,
    resolve_snapshot,
    write_json,
)
from calgary_dashboard.common.naming import standardized_file_name
from calgary_dashboard.config.logging import configure_logger
from calgary_dashboard.config.paths import PROCESSED_DATA_ROOT, RAW_DATA_ROOT

RAW_OPEN_CALGARY_ROOT = RAW_DATA_ROOT / "open_calgary"
PROCESSED_OPEN_CALGARY_ROOT = PROCESSED_DATA_ROOT / "open_calgary"


def _extract_geometry_field(records: list[dict]) -> str | None:
    """Extract the geometry field from the records."""
    for row in records:
        for key in row:
            if key.lower() in GEOMETRY_FIELD_NAMES:
                return key
    return None


def _records_to_geodataframe(records: list[dict]) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame from the records after ensuring they have a geometry field."""
    if not records:
        return gpd.GeoDataFrame(geometry=[])

    # Extract the geometry field from the records
    geometry_key = _extract_geometry_field(records)
    if geometry_key is None:
        raise ValueError("No geometry column found in records")

    rows = []
    for row in records:
        props = dict(row)
        # Open Calgary list payloads keep geometry as a nested dict under a
        # geometry-like column; normalize to a dedicated GeoPandas geometry column.
        raw_geometry = props.pop(geometry_key, None)
        geometry = shape(raw_geometry) if isinstance(raw_geometry, dict) else None
        props["geometry"] = geometry
        rows.append(props)

    return gpd.GeoDataFrame(rows, geometry="geometry")


def _build_geodataframe(feature_payload: object) -> gpd.GeoDataFrame | None:
    """Build a GeoDataFrame from the feature payload."""
    try:
        # Some datasets are standard GeoJSON FeatureCollections.
        if isinstance(feature_payload, dict) and "features" in feature_payload:
            return gpd.GeoDataFrame.from_features(feature_payload)
        # Others are flat rows with a geometry column.
        if isinstance(feature_payload, list) and feature_payload:
            return _records_to_geodataframe(feature_payload)
    except Exception:
        return None
    return None


def clean_snapshot(snapshot_date: str | None = None) -> Path:
    """Clean one Open Calgary snapshot and return processed output directory."""
    resolved_snapshot = resolve_snapshot(RAW_OPEN_CALGARY_ROOT, snapshot_date)
    raw_snapshot_dir = RAW_OPEN_CALGARY_ROOT / resolved_snapshot
    if not raw_snapshot_dir.exists():
        raise FileNotFoundError(f"Raw Open Calgary snapshot not found: {raw_snapshot_dir}")

    # Prepare output directories
    processed_snapshot_dir, features_root, metadata_root = prepare_output_dirs(
        PROCESSED_OPEN_CALGARY_ROOT, resolved_snapshot
    )

    # Configure logger
    logger = configure_logger(
        __name__,
        processed_snapshot_dir / f"oc_data_prep_{resolved_snapshot}.log",
    )

    # List dataset directories
    dataset_dirs = list_subdirectories(raw_snapshot_dir)
    logger.info("Found %d dataset folders in %s", len(dataset_dirs), raw_snapshot_dir)

    # Loop through each dataset directory
    for dataset_dir in dataset_dirs:
        # Find metadata and feature files
        metadata_matches = sorted(dataset_dir.glob("*_metadata.json"))
        feature_matches = sorted(dataset_dir.glob("*_data.json"))
        if not metadata_matches or not feature_matches:
            logger.warning("Skipping %s: missing *_metadata.json or *_data.json", dataset_dir)
            continue

        # Read metadata and feature files
        metadata_obj = read_json(metadata_matches[0])
        feature_obj = read_json(feature_matches[0])
        # Get dataset name and id
        dataset_name = metadata_obj.get("name", dataset_dir.name)
        dataset_id = str(metadata_obj.get("id", dataset_dir.name))

        # Create metadata and feature file names
        metadata_file_name = standardized_file_name(dataset_id, dataset_name, "metadata")
        feature_file_name = standardized_file_name(dataset_id, dataset_name, "features")

        # Write metadata file
        write_json(metadata_root / f"{metadata_file_name}.json", metadata_obj)
        logger.info("Saved metadata %s.json", metadata_file_name)

        # Build a GeoDataFrame from the feature object
        gdf = _build_geodataframe(feature_obj)
        has_valid_geometry = (
            gdf is not None and not gdf.empty and gdf.geometry.notna().any()
        )

        # If the GeoDataFrame has valid geometry, save as parquet
        if has_valid_geometry:
            # CRS from CKAN metadata (Map Projection); avoids anonymous geometries in Parquet.
            crs_hint = infer_crs_from_open_calgary_metadata(metadata_obj)
            if crs_hint:
                gdf = gdf.set_crs(crs_hint, allow_override=True)
            else:
                logger.warning(
                    "No CRS from metadata for dataset id=%s name=%s; Parquet may omit crs",
                    dataset_id,
                    dataset_name,
                )

            # Use actual geometry values from the parsed GeoDataFrame rather than
            # raw schema keys like "the_geom" or "geometry".
            geometry_type = gdf.geom_type.dropna().iloc[-1].lower()
            geom_save_dir = ensure_dir(features_root / geometry_type)
            feature_path = geom_save_dir / f"{feature_file_name}.parquet"
            gdf.to_parquet(feature_path)
            logger.info(
                "Saved features %s (%d rows, geom=%s)",
                feature_path.name,
                len(gdf),
                geometry_type,
            )
        else:
            # If the GeoDataFrame does not have valid geometry, save as json
            write_json(features_root / f"{feature_file_name}.json", feature_obj)
            logger.info("Saved fallback JSON %s.json", feature_file_name)

    return processed_snapshot_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean Open Calgary raw snapshot data.")
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default=None,
        help="Snapshot folder name in YYYYMMDD. Defaults to latest snapshot.",
    )
    args = parser.parse_args()

    output_dir = clean_snapshot(snapshot_date=args.snapshot_date)
    print(f"Open Calgary cleaning complete: {output_dir}")


if __name__ == "__main__":
    main()

