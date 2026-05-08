"""Ontology IO layer.

Responsibilities:
- resolve standard ontology paths from config

Non-responsibilities:
- no fuzzy matching or mapping logic
- no orchestration decisions
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from calgary_dashboard.config.paths import PROJECT_ROOT


@dataclass(frozen=True)
class OntologyPaths:
    """Standardized paths for ontology artifacts."""

    root: Path
    ontology_json: Path
    class_file_map_json: Path
    catalog_json: Path


def default_paths() -> OntologyPaths:
    """Return default ontology paths under the new ontology package."""
    root = PROJECT_ROOT / "src" / "calgary_dashboard" / "ontology"
    return OntologyPaths(
        root=root,
        ontology_json=root / "calgary-ontology.json",
        class_file_map_json=root / "class_file_map.json",
        catalog_json=root / "catalog.json",
    )

