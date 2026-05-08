"""StatCan cleaner.

Converts raw StatCan CSV snapshots into:
- Calgary-only dissemination area GeoParquet features
- metadata CSV files
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd

from calgary_dashboard.common.cleaning import prepare_output_dirs, resolve_snapshot
from calgary_dashboard.common.naming import standardized_file_name
from calgary_dashboard.config.logging import configure_logger
from calgary_dashboard.config.paths import PROCESSED_DATA_ROOT, RAW_DATA_ROOT

RAW_STATCAN_ROOT = RAW_DATA_ROOT / "statcan"
PROCESSED_STATCAN_ROOT = PROCESSED_DATA_ROOT / "statcan"
# TODO: make less fragile/automate
# This file was manually downloaded from:
# https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/index2021-eng.cfm?Year=21
# selections were type: digital boundary files
# statistical boundaries: dissemination areas
# format: shapefile
DISSEMINATION_AREA_SHP = (
    RAW_STATCAN_ROOT / "manual_download" / "lda_000a21a_e" / "lda_000a21a_e.shp"
)


def _file_prefix(file_name: str) -> str:
    return Path(file_name).stem.split("_")[0]


def _group_statcan_files(raw_snapshot_dir: Path) -> tuple[dict[str, Path], dict[str, Path]]:
    # StatCan exports pair data and metadata files by a shared product-id prefix.
    data_files = sorted(
        path for path in raw_snapshot_dir.glob("*.csv") if "_MetaData" not in path.name
    )
    metadata_files = sorted(raw_snapshot_dir.glob("*_MetaData.csv"))

    data_by_prefix = {_file_prefix(path.name): path for path in data_files}
    metadata_by_prefix = {_file_prefix(path.name): path for path in metadata_files}
    return data_by_prefix, metadata_by_prefix


def _calgary_slice(data_df: pd.DataFrame) -> pd.DataFrame:
    calgary_rows = data_df[data_df["GEO"] == "Calgary"]
    if calgary_rows.empty:
        raise ValueError("No Calgary row found in GEO column.")
    # Use Calgary's DGUID prefix to include matching local dissemination areas.
    cal_code = str(calgary_rows["DGUID"].values[0])[-7:-3]
    # last 8 digits of DGUID is DAUID, first two of which are province id and census division code
    # https://www150.statcan.gc.ca/n1/pub/92f0138m/92f0138m2019001-eng.htm
    mask = data_df["DGUID"].astype(str).str[-8:-4] == cal_code
    return data_df[mask]


def clean_snapshot(snapshot_date: str | None = None) -> Path:
    """Clean one StatCan snapshot and return processed output directory."""
    resolved_snapshot = resolve_snapshot(RAW_STATCAN_ROOT, snapshot_date)
    raw_snapshot_dir = RAW_STATCAN_ROOT / resolved_snapshot
    if not raw_snapshot_dir.exists():
        raise FileNotFoundError(f"Raw StatCan snapshot not found: {raw_snapshot_dir}")
    if not DISSEMINATION_AREA_SHP.exists():
        raise FileNotFoundError(
            "Dissemination area shapefile not found. Expected: "
            f"{DISSEMINATION_AREA_SHP}"
        )

    processed_snapshot_dir, features_root, metadata_root = prepare_output_dirs(
        PROCESSED_STATCAN_ROOT, resolved_snapshot
    )
    logger = configure_logger(
        __name__,
        processed_snapshot_dir / f"sc_data_prep_{resolved_snapshot}.log",
    )

    # load manually downloaded dissemination areas from StatCan
    dissemination_areas_gdf = gpd.read_file(DISSEMINATION_AREA_SHP)
    data_by_prefix, metadata_by_prefix = _group_statcan_files(raw_snapshot_dir)
    logger.info("Found %d StatCan datasets in %s", len(data_by_prefix), raw_snapshot_dir)

    for prefix, data_path in data_by_prefix.items():
        # load data and metadata files, metadata file only use first 2 lines read from file
        metadata_path = metadata_by_prefix.get(prefix)
        data_df = pd.read_csv(data_path)
        metadata_df = pd.read_csv(metadata_path, nrows=1)

        # drop Symbols columns as they're all nan
        symbol_columns = [col for col in data_df.columns if col.startswith("Symbols")]
        if symbol_columns:
            data_df = data_df.drop(columns=symbol_columns)

        try:
            cal_data_df = _calgary_slice(data_df)
        except Exception:
            logger.exception("Failed to isolate Calgary rows for %s", data_path.name)
            continue

        # join polygons from dissemination areas table to cal_data_df
        cal_data_gdf = dissemination_areas_gdf.merge(cal_data_df, on="DGUID", how="right")

        # create file names using pid and 'cube title'
        product_id = str(metadata_df["Product Id"].values[0])
        title = str(metadata_df["Cube Title"].values[0])
        # keep filenames readable while preventing very long paths.
        short_title = " ".join(title.split(" ")[:5])

        features_name = standardized_file_name(product_id, short_title, "features")
        metadata_name = standardized_file_name(product_id, short_title, "metadata")

        # save cleaned features to parquet and metadata to csv
        cal_data_gdf.to_parquet(features_root / f"{features_name}.parquet", index=False)
        metadata_df.to_csv(metadata_root / f"{metadata_name}.csv", index=False)
        logger.info("Saved features %s.parquet and metadata %s.csv", features_name, metadata_name)

    return processed_snapshot_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean StatCan raw snapshot data.")
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default=None,
        help="Snapshot folder name in YYYYMMDD. Defaults to latest snapshot.",
    )
    args = parser.parse_args()

    output_dir = clean_snapshot(snapshot_date=args.snapshot_date)
    print(f"StatCan cleaning complete: {output_dir}")


if __name__ == "__main__":
    main()

