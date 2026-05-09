#!/usr/bin/env bash
set -euo pipefail

# Make sure you activated `conda activate calgary-dev`
# Usage:
#   ./scripts/build_class_file_map.sh
#   ./scripts/build_class_file_map.sh --min-score 0.2 --top-k 1

PYTHONPATH=src python3 -m calgary_dashboard.ontology.build_class_file_map "$@"
