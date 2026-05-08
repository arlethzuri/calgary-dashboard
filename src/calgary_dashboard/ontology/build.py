"""Ontology build orchestration layer.

Responsibilities:
- validate runtime config
- coordinate calls between ontology.io and ontology.mapping
- expose a stable callable API and CLI entrypoint
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from calgary_dashboard.common.io import read_json, write_json
from calgary_dashboard.config.paths import PROCESSED_DATA_ROOT
from calgary_dashboard.ontology.io import default_paths
from calgary_dashboard.ontology.mapping import (
    MappingRequest,
    build_feature_column_lookup,
    find_catalog_entry,
    generate_mapping,
    iter_feature_file_columns_with_json,
    resolve_lookup_path,
)


@dataclass(frozen=True)
class OntologyBuildConfig:
    """Runtime config for ontology build orchestration."""

    ontology_path: Path
    catalog_path: Path
    output_path: Path
    data_root: Path
    class_name: str | None
    relative_path: str | None
    min_mapped: int
    class_file_map_path: Path | None


def run_build(config: OntologyBuildConfig) -> Path:
    """Generate mapping drafts from ontology + processed-data catalog."""
    ontology = read_json(config.ontology_path)
    catalog = read_json(config.catalog_path)
    classes = ontology.get("classes", {})
    if not isinstance(classes, dict):
        raise ValueError(f"Ontology classes are missing/invalid in {config.ontology_path}")

    current: dict[str, dict[str, object]] = {"mappings": {}}
    if config.class_file_map_path is not None:
        pair_map = read_json(config.class_file_map_path)
        pairs = pair_map.get("pairs", [])
        if not isinstance(pairs, list):
            raise ValueError("class-file-map JSON must contain a list at key 'pairs'.")

        feature_lookup = build_feature_column_lookup(catalog, config.data_root)
        print(f"feature_lookup: {feature_lookup}")
        for pair in pairs:
            class_name = pair.get("class_name")
            relative_path = pair.get("relative_path")
            mode = pair.get("mode")
            if not class_name or not relative_path:
                continue
            if class_name not in classes:
                continue

            resolved_path, columns = resolve_lookup_path(feature_lookup, relative_path)
            if not columns:
                continue

            result = generate_mapping(
                ontology_classes=classes,
                request=MappingRequest(
                    class_name=class_name,
                    relative_path=resolved_path or relative_path,
                    columns=columns,
                ),
            )
            if len(result.field_mapping) < config.min_mapped:
                continue

            key = f"{(resolved_path or relative_path)}::{class_name}"
            entry: dict[str, object] = {
                "class_name": result.class_name,
                "field_mapping": result.field_mapping,
                "missing_required": result.missing_required,
                "unmapped_source_columns": result.unmapped_source_columns,
            }
            if mode:
                entry["mode"] = mode
            current["mappings"][key] = entry
    else:
        if not config.relative_path or not config.class_name:
            raise ValueError("Single-file mode requires both relative_path and class_name.")
        entry = find_catalog_entry(catalog, config.relative_path)
        raw_columns = entry.get("schema", {}).get("columns", [])
        columns = [c for c in raw_columns if isinstance(c, str) and not c.startswith(":")]
        if not columns:
            raise ValueError(f"No schema.columns found for: {config.relative_path}")

        result = generate_mapping(
            ontology_classes=classes,
            request=MappingRequest(
                class_name=config.class_name,
                relative_path=config.relative_path,
                columns=columns,
            ),
        )
        current["mappings"][config.relative_path] = {
            "class_name": result.class_name,
            "field_mapping": result.field_mapping,
            "missing_required": result.missing_required,
            "unmapped_source_columns": result.unmapped_source_columns,
        }

    write_json(config.output_path, current)
    return config.output_path


def _parse_args() -> OntologyBuildConfig:
    paths = default_paths()
    parser = argparse.ArgumentParser(description="Run ontology build pipeline.")
    parser.add_argument(
        "--ontology-path",
        type=Path,
        default=paths.ontology_json,
        help="Path to ontology JSON input.",
    )
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=paths.catalog_json,
        help="Path to processed data catalog JSON.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=paths.root / "ontology_mappings.json",
        help="Output mapping draft JSON path.",
    )
    parser.add_argument(
        "--relative-path",
        type=str,
        default=None,
        help="Catalog relative_path for one dataset file.",
    )
    parser.add_argument(
        "--class-name",
        type=str,
        default=None,
        help="Ontology class to map.",
    )
    parser.add_argument(
        "--min-mapped",
        type=int,
        default=1,
        help="Keep mappings with at least this many mapped fields.",
    )
    parser.add_argument(
        "--class-file-map",
        type=Path,
        default=paths.class_file_map_json,
        help='Path to class/file allowlist JSON (expects {"pairs":[...]}).',
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=PROCESSED_DATA_ROOT,
        help="Base directory for feature files referenced by catalog relative_path.",
    )
    args = parser.parse_args()
    return OntologyBuildConfig(
        ontology_path=args.ontology_path.resolve(),
        catalog_path=args.catalog_path.resolve(),
        output_path=args.output_path.resolve(),
        data_root=args.data_root.resolve(),
        class_name=args.class_name,
        relative_path=args.relative_path,
        min_mapped=args.min_mapped,
        class_file_map_path=(args.class_file_map.resolve() if args.class_file_map else None),
    )


def main() -> None:
    config = _parse_args()
    output = run_build(config)
    print(f"Ontology build complete: {output}")


if __name__ == "__main__":
    main()

