#!/usr/bin/env python3
"""Copy allowlisted processed parquets into demos/demo4/data for offline use."""

from __future__ import annotations

import shutil
from pathlib import Path

from calgary_dashboard.common.definitions import GEOMETRY_BUCKET_NAMES
from calgary_dashboard.common.io import list_subdirectories
from calgary_dashboard.config.paths import PROCESSED_DATA_ROOT

HERE = Path(__file__).resolve().parent
DEST = HERE / "data"

OPEN_CALGARY_DEMO_STEMS = frozenset(
    {
        "uqkc-h9wi_solarproductionsitesmap",
        "a2cn-dxht_hydrology",
        "ab7m-fwn6_communityboundaries",
        "cf5t-fjzu_parkscemeteries",
        "icxc-6yk3_naturalareas",
        "mw9j-jik5_landusedistricts",
        "n4vp-3exq_suburbanresidentialgrowthsrgforecastmap",
        "xmep-aasr_schoolsincommunities",
    }
)


def parquet_feature_stem(path: Path) -> str:
    return path.stem.replace("_features", "").lower()


def is_demo_dataset(path: Path, root: Path) -> bool:
    rel = path.relative_to(root)
    source = rel.parts[0]
    name = parquet_feature_stem(path)
    if source == "enmax":
        if "loadcapacity" in name and "hosting" not in name:
            return True
        return "hostingcapacity" in name
    if source == "statcan":
        return "98100015" in name or "populationanddwelling" in name
    if source == "open_calgary":
        if "floodmap" in name:
            return True
        return name in OPEN_CALGARY_DEMO_STEMS
    return False


def collect_parquets(root: Path) -> list[Path]:
    files: list[Path] = []
    for source_dir in list_subdirectories(root):
        for snapshot_dir in list_subdirectories(source_dir):
            features_dir = snapshot_dir / "features"
            if not features_dir.exists():
                continue
            for child in features_dir.iterdir():
                if child.is_dir() and child.name in GEOMETRY_BUCKET_NAMES:
                    for path in sorted(child.glob("*.parquet")):
                        if is_demo_dataset(path, root):
                            files.append(path)
                elif child.suffix == ".parquet" and is_demo_dataset(child, root):
                    files.append(child)
    return files


def main() -> None:
    if not PROCESSED_DATA_ROOT.is_dir():
        raise SystemExit(f"Processed data not found: {PROCESSED_DATA_ROOT}")

    files = collect_parquets(PROCESSED_DATA_ROOT)
    if not files:
        raise SystemExit("No demo datasets found under processed_data.")

    if DEST.exists():
        shutil.rmtree(DEST)

    for path in files:
        rel = path.relative_to(PROCESSED_DATA_ROOT)
        out = DEST / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, out)

    total_mb = sum(p.stat().st_size for p in DEST.rglob("*.parquet")) / 1e6
    print(f"Copied {len(files)} files to {DEST} ({total_mb:.1f} MB)")


if __name__ == "__main__":
    main()
