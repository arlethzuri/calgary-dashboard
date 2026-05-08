"""Download data from sources."""

import argparse
import os
from pathlib import Path
from calgary_dashboard.collectors import open_calgary as oc_collector
from calgary_dashboard.collectors import enmax as enmax_collector
from calgary_dashboard.collectors import statcan as statcan_collector

SOURCES_DIR = Path(__file__).resolve().parent / "sources"
CALGARY_SOURCES_DIR = Path(SOURCES_DIR / "open_calgary")
STATCAN_SOURCES_DIR = Path(SOURCES_DIR / "statcan")

if __name__ == "__main__":
    # allow user to select which vendors to download from
    parser = argparse.ArgumentParser(
        description="Download data from sources."
    )
    parser.add_argument(
        "vendors",
        type=str,
        nargs="+",
        help=(
            "List the vendors you want to download data from:\n"
            "open_calgary, enmax, statcan.\n"
            "Or say 'all' to download all vendors.\n"
        ),
        default=None,
    )
    args = parser.parse_args()
    vendors = args.vendors
    print(f"vendors[0] == 'all': {vendors[0] == 'all'}")
    if vendors[0] == "all":
        vendors = ["open_calgary", "enmax", "statcan"]
    for vendor in vendors:
        if vendor == "open_calgary":
            files = os.listdir(CALGARY_SOURCES_DIR)
            for file in files:
                curr_file = Path(CALGARY_SOURCES_DIR / file)
                oc_collector.collect_from_sources_file(curr_file)
        elif vendor == "enmax":
            enmax_collector.collect()
        elif vendor == "statcan":
            files = os.listdir(STATCAN_SOURCES_DIR)
            for file in files:
                curr_file = Path(STATCAN_SOURCES_DIR / file)
                statcan_collector.collect(curr_file)
        else:
            raise ValueError(f"Vendor(s) {vendors} not found.")