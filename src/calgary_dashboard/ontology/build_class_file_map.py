"""Build class_file_map.json suggestions from ontology + catalog.

Score feature files against each ontology class using string similarity 
and required-field coverage to determine the best class for each feature file.

Usage:
  PYTHONPATH=src python3 -m calgary_dashboard.ontology.build_class_file_map \
    --ontology src/calgary_dashboard/ontology/calgary-ontology.json \
    --catalog src/calgary_dashboard/ontology/catalog.json \
    --output src/calgary_dashboard/ontology/class_file_map.generated.json
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from calgary_dashboard.common.definitions import (
    MEASUREMENT_LIKE_TOKENS,
    NODE_LIKE_TOKENS,
)
from calgary_dashboard.common.io import read_json, write_json
from calgary_dashboard.config.paths import SRC_ROOT
from calgary_dashboard.ontology.similarity import combined_similarity

# Hand-made ontology
DEFAULT_ONTOLOGY = SRC_ROOT / "ontology" / "calgary-ontology.json"
# Output path for generated class_file_map.json
DEFAULT_OUTPUT = SRC_ROOT / "ontology" / "class_file_map.generated.json"

def _resolve_latest_catalog() -> Path:
    """Resolve latest generated catalog JSON from src/calgary_dashboard/catalog."""
    catalog_dir = SRC_ROOT / "catalog"
    candidates = sorted(catalog_dir.glob("processed_data_catalog_*.json"))
    if candidates:
        return candidates[-1]

# Latest generated catalog JSON from src/calgary_dashboard/catalog
DEFAULT_CATALOG = _resolve_latest_catalog()

@dataclass(frozen=True)
class FeatureFile:
    """Minimal feature-file view used during scoring.

    We isolate only the fields needed for ranking so scoring logic stays
    independent from raw catalog JSON shape.
    """

    source: str
    file_name: str
    geometry_bucket: str | None
    columns: list[str]


def _resolve_required_fields(classes: dict[str, Any], class_name: str) -> list[str]:
    """For a given ontology class, resolve full required field list including 
    parent classes.
    """
    # Accumulates required properties as we walk from child -> parent for the 
    # given class.
    # We intentionally preserve encounter order because downstream scoring and
    # debugging are easier when fields appear in ontology-defined sequence.
    required: list[str] = []
    visited: set[str] = set()
    current: str | None = class_name

    # Start at requested class, then repeatedly follow `parent_class`.
    while current:
        # If we encounter a class we already visited, raise error of circular
        # parent_class.
        if current in visited:
            raise ValueError(f"Circular parent_class detected at {current}")
        visited.add(current)

        # Get the class definition from the ontology.
        cls = classes.get(current)
        if not isinstance(cls, dict):
            raise ValueError(f"Class '{current}' is missing or invalid in ontology.")
        # Missing/empty `properties` is treated as no requirements.
        props = cls.get("properties", {})
        if cls.get("type") == "abstract_class":
            # Abstract classes contribute only shared inherited requirements.
            required.extend(props.get("required", []))
        else:
            # Concrete classes contribute local requirements plus any explicit
            # class-level required fields.
            required.extend(props.get("required_local", []))
            required.extend(props.get("required", []))
        # Move upward through inheritance chain.
        current = cls.get("parent_class")

    # Deduplicate while preserving original order.
    # Example: if both parent and child include "geometry", keep first occurrence.
    seen: set[str] = set()
    required_fields: list[str] = []
    for field in required:
        if field not in seen:
            seen.add(field)
            required_fields.append(field)
    # Final list is the canonical required-field set for this class in scoring.
    return required_fields


def _extract_features(catalog: dict[str, Any]) -> list[FeatureFile]:
    """Extract scorable feature files from processed-data catalog."""
    features: list[FeatureFile] = []

    # Check all file entries in catalog.
    for file_entry in catalog.get("files", []):
        # Only consider feature files.
        if file_entry.get("role") != "feature":
            continue

        # Get the filename and source of the current file_entry.
        file_name = file_entry.get("file_name")
        source = file_entry.get("source")
        if not isinstance(file_name, str) or not isinstance(source, str):
            continue

        # Get the schema and columns of the current file_entry, if available.
        schema = file_entry.get("schema") or {}
        raw_cols = schema.get("columns", [])
        # Ignore Socrata/internal columns that start with ":" because they
        # are usually metadata/system fields, not domain attributes.
        columns = [col for col in raw_cols if isinstance(col, str) and not col.startswith(":")]
        features.append(
            FeatureFile(
                source=source,
                file_name=file_name,
                geometry_bucket=file_entry.get("geometry_bucket"),
                columns=columns,
            )
        )
    return features


def _class_mode(class_name: str) -> str:
    """Infer default map mode from ontology class name."""
    class_lower = class_name.lower()
    if any(token in class_lower for token in MEASUREMENT_LIKE_TOKENS):
        return "measurement"
    if any(token in class_lower for token in NODE_LIKE_TOKENS):
        return "node"
    return "node"


def _field_coverage_score(required_fields: list[str], columns: list[str]) -> float:
    """Score how many required ontology fields appear in dataset columns."""
    if not required_fields:
        # If class defines no required fields, treat coverage as neutral low
        # signal and let name similarity dominate ranking.
        return 0.0
    col_set = {col.lower() for col in columns}
    matched = 0
    for field in required_fields:
        if field.lower() in col_set:
            matched += 1
    return matched / len(required_fields)


def _score(class_name: str, required_fields: list[str], feature: FeatureFile) -> float:
    """Blend class/file-name similarity with required-field coverage."""
    name_score = combined_similarity(class_name, feature.file_name)
    field_score = _field_coverage_score(required_fields, feature.columns)
    # Name is slightly stronger than schema coverage because some high-quality
    # JSON feature files may have sparse/absent schema metadata in catalog.
    return (0.55 * name_score) + (0.45 * field_score)


def _build_pairs(
    ontology: dict[str, Any],
    catalog: dict[str, Any],
    *,
    min_score: float,
    top_k: int,
) -> list[dict[str, str]]:
    """Generate class-file pairs by finding best classes per feature file."""
    # Load all classes from ontology file
    classes = ontology.get("classes")
    if not isinstance(classes, dict):
        raise ValueError("Ontology 'classes' is missing or invalid.")

    # Filter out abstract classes, since features should not directly map to an
    # abstract class, only concrete classes.
    concrete_classes = [
        class_name
        for class_name, class_def in classes.items()
        if isinstance(class_def, dict) and class_def.get("type") != "abstract_class"
    ]
    # Extract all feature files from the catalog.
    features = _extract_features(catalog)

    # Precompute required fields once per class to avoid repeating inheritance
    # traversal for every feature file.
    required_by_class = {
        class_name: _resolve_required_fields(classes, class_name)
        for class_name in concrete_classes
    }

    pairs: list[dict[str, str]] = []

    # For each feature file, rank classes and keep the best.
    for feature in features:
        scored_classes: list[tuple[float, str]] = []
        for class_name in concrete_classes:
            score = _score(class_name, required_by_class[class_name], feature)
            if score >= min_score:
                scored_classes.append((score, class_name))

        # Sort classes by score in descending order and keep the top_k classes.
        scored_classes.sort(key=lambda item: item[0], reverse=True)
        for score, class_name in scored_classes[:top_k]:
            _ = score  # reserved for optional debug/report output later
            pairs.append(
                {
                    "class_name": class_name,
                    "file_name": feature.file_name,
                    "source": feature.source,
                    "geometry_bucket": feature.geometry_bucket,
                    "mode": _class_mode(class_name),
                }
            )

    # Deduplicate exact rows while preserving order.
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for pair in pairs:
        key = (pair["class_name"], pair["file_name"], pair["mode"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(pair)
    return deduped


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate class_file_map suggestions.")
    parser.add_argument("--ontology", type=Path, default=DEFAULT_ONTOLOGY, help="Path to ontology JSON.")
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG, help="Path to processed-data catalog JSON.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output path for generated map JSON.")
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.2,
        help="Minimum score [0,1] to keep class-file suggestions (higher = stricter).",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=2,
        help="Maximum class suggestions to keep per feature file.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    ontology = read_json(args.ontology)
    catalog = read_json(args.catalog)
    # Generate suggestions purely from current ontology + catalog state.
    generated_pairs = _build_pairs(
        ontology,
        catalog,
        min_score=args.min_score,
        top_k=args.top_k,
    )

    payload: dict[str, Any] = {"pairs": generated_pairs}

    # Save pairs using shared JSON writer conventions.
    write_json(args.output, payload)
    print(f"Wrote {len(payload['pairs'])} class-file pairs to {args.output}\
        using min_score={args.min_score} and top_k={args.top_k}.")


if __name__ == "__main__":
    main()

