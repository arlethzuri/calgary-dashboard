#!/usr/bin/env python3
"""Run ENMAX, Open Calgary, and StatCan cleaners (latest snapshot unless --snapshot-date is set).

  PYTHONPATH=src python3 scripts/run_cleaners.py
  PYTHONPATH=src python3 scripts/run_cleaners.py --snapshot-date 20260327
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from calgary_dashboard.cleaners.enmax import clean_snapshot as clean_enmax
from calgary_dashboard.cleaners.open_calgary import clean_snapshot as clean_open_calgary
from calgary_dashboard.cleaners.statcan import clean_snapshot as clean_statcan


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all calgary_dashboard cleaners.")
    parser.add_argument(
        "--snapshot-date",
        default=None,
        help="YYYYMMDD folder under each vendor (default: latest per vendor).",
    )
    args = parser.parse_args()
    snap = args.snapshot_date

    print("ENMAX …")
    print(clean_enmax(snapshot_date=snap))
    print("Open Calgary …")
    print(clean_open_calgary(snapshot_date=snap))
    print("StatCan …")
    print(clean_statcan(snapshot_date=snap))


if __name__ == "__main__":
    main()
