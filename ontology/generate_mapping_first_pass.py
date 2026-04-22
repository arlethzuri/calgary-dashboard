"""Generate a first-pass mapping for one dataset file and one ontology class.

Usage example:
python ontology/generate_mapping_first_pass.py \
  --ontology ontology/calgary-ontology.json \
  --catalog data_cleaning/processed_data_catalog_20260327.json \
  --relative-path open_calgary/20260327/features/point/g9s5-qhu5_AirQualityDataNear_feature.parquet \
  --class-name AirQualityReading
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


def _norm(text: str) -> str:
    """Normalize names for fuzzy matching."""
    return re.sub(r"[^a-z0-9]+", "", text.lower())


# Canonical property aliases to reduce manual mapping.
ALIASES: Dict[str, List[str]] = {
    "name": ["name", "station_name", "facility_name", "building_name", "title"],
    "description": ["description", "details", "project_details", "units_description"],
    "geometry": ["geometry", "geom"],
    "spatial_reference": ["spatial_reference", "spatialreference", "crs", "wkid"],
    "maintained_by": ["maintained_by", "owner", "steward", "source"],
    "id": ["id", "record_id"],
    "abbreviation": ["abbreviation", "abbr"],
    "parameter": ["parameter"],
    "value": ["value"],
    "units": ["units", "unit"],
    "observed_at": ["readingdate", "date_last_updated", "updated_at"],
    "site_id": ["site_id", "id"],
    "feeder_id": ["feeder_id", "feederid", "FEEDERID"],
    "phase_designation": ["phase_designation", "Phase_Designation", "phase"],
    "capacity_available": ["capacity_available", "Capacity_Available", "available_capacity"],
    "kva_number": ["kva_number", "KVA_Number", "kva_no", "kva", "range", "Range"],
    "date_last_updated": ["date_last_updated", "Date_Last_Updated", "updated_at", "readingdate"],
    "type": ["type", "Type"],
    "link": ["link", "Link"],
    "address": ["address", "Address", "facilityaddress"],
    "energy_type": ["energy_type", "Energy_Type", "energy_description"],
    "year": ["year", "Year"],
    "month": ["month", "Month"],
    "total_consumption": ["total_consumption", "Total_Consumption", "total_consumption"],
    "unit": ["unit", "Unit"],
}

DEFAULT_DATA_ROOT = Path("/Users/arleth/Desktop/calgary-dashboard/data/calgary/processed_data")


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def find_catalog_entry(catalog: dict, relative_path: str) -> dict:
    for entry in catalog.get("files", []):
        if entry.get("relative_path") == relative_path:
            return entry
    raise ValueError(f"Could not find relative_path in catalog: {relative_path}")


def resolve_required_fields(classes: dict, class_name: str) -> List[str]:
    if class_name not in classes:
        raise ValueError(f"Class not found in ontology: {class_name}")

    required: List[str] = []
    visited: Set[str] = set()
    current: Optional[str] = class_name

    while current:
        if current in visited:
            raise ValueError(f"Circular parent_class detected at: {current}")
        visited.add(current)

        cls = classes[current]
        props = cls.get("properties", {})
        if cls.get("type") == "abstract_class":
            required.extend(props.get("required", []))
        else:
            required.extend(props.get("required_local", []))
            required.extend(props.get("required", []))

        current = cls.get("parent_class")

    # Keep order while deduplicating.
    unique: List[str] = []
    seen: Set[str] = set()
    for item in required:
        if item not in seen:
            unique.append(item)
            seen.add(item)
    return unique


def build_column_index(columns: List[str]) -> Dict[str, str]:
    index: Dict[str, str] = {}
    for col in columns:
        if col.startswith(":"):
            continue
        index[_norm(col)] = col
    return index


def pick_source_column(canonical: str, columns: List[str], col_index: Dict[str, str]) -> Optional[str]:
    # 1) exact
    if canonical in columns:
        return canonical

    # 2) normalized canonical
    norm_canonical = _norm(canonical)
    if norm_canonical in col_index:
        return col_index[norm_canonical]

    # 3) aliases
    for alias in ALIASES.get(canonical, []):
        if alias in columns:
            return alias
        norm_alias = _norm(alias)
        if norm_alias in col_index:
            return col_index[norm_alias]

    return None


def create_mapping_entry(
    classes: dict,
    class_name: str,
    relative_path: str,
    columns: List[str],
) -> Dict[str, object]:
    required_fields = resolve_required_fields(classes, class_name)
    column_index = build_column_index(columns)

    field_mapping: Dict[str, str] = {}
    missing_required: List[str] = []
    for field in required_fields:
        source_col = pick_source_column(field, columns, column_index)
        if source_col is None:
            missing_required.append(field)
        else:
            field_mapping[field] = source_col

    return {
        "class_name": class_name,
        "field_mapping": field_mapping,
        "missing_required": missing_required,
        "unmapped_source_columns": [c for c in columns if c not in field_mapping.values()],
    }


def iter_feature_file_columns(catalog: dict) -> List[Tuple[str, List[str]]]:
    out: List[Tuple[str, List[str]]] = []
    for entry in catalog.get("files", []):
        if entry.get("role") != "feature":
            continue
        relative_path = entry.get("relative_path")
        if not relative_path:
            continue
        schema = entry.get("schema") or {}
        raw_columns = schema.get("columns", [])
        columns = [c for c in raw_columns if isinstance(c, str) and not c.startswith(":")]
        if not columns:
            continue
        out.append((relative_path, columns))
    return out


def infer_json_array_columns(json_path: Path) -> List[str]:
    """Infer columns from the first object of a JSON array file."""
    decoder = json.JSONDecoder()
    with json_path.open("r", encoding="utf-8", errors="ignore") as f:
        sample = f.read(2_000_000)

    if not sample:
        return []

    idx = 0
    while idx < len(sample) and sample[idx].isspace():
        idx += 1
    if idx >= len(sample) or sample[idx] != "[":
        return []
    idx += 1
    while idx < len(sample) and (sample[idx].isspace() or sample[idx] == ","):
        idx += 1
    if idx >= len(sample):
        return []

    try:
        obj, _ = decoder.raw_decode(sample, idx)
    except json.JSONDecodeError:
        return []

    if not isinstance(obj, dict):
        return []
    return [k for k in obj.keys() if isinstance(k, str) and not k.startswith(":")]


def iter_feature_file_columns_with_json(
    catalog: dict,
    data_root: Path,
) -> List[Tuple[str, List[str]]]:
    out: List[Tuple[str, List[str]]] = []
    for entry in catalog.get("files", []):
        if entry.get("role") != "feature":
            continue
        relative_path = entry.get("relative_path")
        if not relative_path:
            continue
        schema = entry.get("schema") or {}
        raw_columns = schema.get("columns", [])
        columns = [c for c in raw_columns if isinstance(c, str) and not c.startswith(":")]
        if not columns and str(relative_path).endswith(".json"):
            json_path = data_root / relative_path
            if json_path.exists():
                columns = infer_json_array_columns(json_path)
        if not columns:
            continue
        out.append((relative_path, columns))
    return out


def build_feature_column_lookup(catalog: dict, data_root: Path) -> Dict[str, List[str]]:
    lookup: Dict[str, List[str]] = {}
    for relative_path, columns in iter_feature_file_columns_with_json(catalog, data_root):
        lookup[relative_path] = columns
    return lookup


def resolve_lookup_path(
    feature_lookup: Dict[str, List[str]], relative_path: str
) -> Tuple[Optional[str], Optional[List[str]]]:
    """Resolve exact or common alternate relative paths."""
    columns = feature_lookup.get(relative_path)
    if columns:
        return relative_path, columns

    # Some JSON features are cataloged under features/<file>.json without geometry bucket.
    if relative_path.endswith(".json"):
        alt = re.sub(r"/features/(point|linestring|polygon|multipolygon)/", "/features/", relative_path)
        if alt != relative_path:
            alt_columns = feature_lookup.get(alt)
            if alt_columns:
                return alt, alt_columns

    return None, None


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate first-pass class mapping for one dataset file.")
    parser.add_argument("--ontology", required=True, help="Path to ontology JSON.")
    parser.add_argument("--catalog", required=True, help="Path to processed data catalog JSON.")
    parser.add_argument("--relative-path", help="Catalog relative_path for one dataset file.")
    parser.add_argument("--class-name", help="Ontology class to map.")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate drafts for all feature files x all non-abstract classes.",
    )
    parser.add_argument(
        "--min-mapped",
        type=int,
        default=1,
        help="For --all mode, keep mappings with at least this many mapped fields.",
    )
    parser.add_argument(
        "--class-file-map",
        help="Path to class/file allowlist JSON (expects {\"pairs\":[...]}).",
    )
    parser.add_argument(
        "--output",
        default="ontology/mappings_first_pass.json",
        help="Where to write/update mapping draft JSON.",
    )
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Base directory for feature files referenced by catalog relative_path.",
    )
    args = parser.parse_args()

    ontology = load_json(Path(args.ontology))
    catalog = load_json(Path(args.catalog))
    classes = ontology.get("classes", {})
    data_root = Path(args.data_root)

    output_path = Path(args.output)
    current = {"mappings": {}}

    if args.class_file_map:
        pair_map = load_json(Path(args.class_file_map))
        pairs = pair_map.get("pairs", [])
        if not isinstance(pairs, list):
            raise ValueError("class-file-map JSON must contain a list at key 'pairs'.")

        feature_lookup = build_feature_column_lookup(catalog, data_root)
        total_written = 0
        skipped = 0

        for pair in pairs:
            class_name = pair.get("class_name")
            relative_path = pair.get("relative_path")
            mode = pair.get("mode")

            if not class_name or not relative_path:
                print(f"Skipping invalid pair (missing class_name/relative_path): {pair}")
                skipped += 1
                continue

            if class_name not in classes:
                print(f"Skipping pair with unknown class '{class_name}': {relative_path}")
                skipped += 1
                continue

            resolved_path, columns = resolve_lookup_path(feature_lookup, relative_path)
            if not columns:
                print(f"Skipping pair with missing/unsupported feature schema: {relative_path}")
                skipped += 1
                continue

            entry = create_mapping_entry(classes, class_name, resolved_path or relative_path, columns)
            if len(entry["field_mapping"]) < args.min_mapped:
                skipped += 1
                continue

            if mode:
                entry["mode"] = mode

            key = f"{(resolved_path or relative_path)}::{class_name}"
            current["mappings"][key] = entry
            total_written += 1

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, ensure_ascii=True)

        print(f"Wrote allowlist-based mapping draft to: {output_path}")
        print(f"- pairs scanned: {len(pairs)}")
        print(f"- entries written (min_mapped={args.min_mapped}): {total_written}")
        print(f"- pairs skipped: {skipped}")
    elif args.all:
        feature_files = iter_feature_file_columns_with_json(catalog, data_root)
        concrete_classes = [name for name, cls in classes.items() if cls.get("type") != "abstract_class"]
        total_written = 0

        for relative_path, columns in feature_files:
            for class_name in concrete_classes:
                entry = create_mapping_entry(classes, class_name, relative_path, columns)
                mapped_count = len(entry["field_mapping"])
                if mapped_count < args.min_mapped:
                    continue
                key = f"{relative_path}::{class_name}"
                current["mappings"][key] = entry
                total_written += 1

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, ensure_ascii=True)

        print(f"Wrote batch mapping draft to: {output_path}")
        print(f"- feature files scanned: {len(feature_files)}")
        print(f"- concrete classes scanned: {len(concrete_classes)}")
        print(f"- entries written (min_mapped={args.min_mapped}): {total_written}")
    else:
        if not args.relative_path or not args.class_name:
            raise ValueError("Single-file mode requires both --relative-path and --class-name.")
        entry = find_catalog_entry(catalog, args.relative_path)
        raw_columns = entry.get("schema", {}).get("columns", [])
        columns = [c for c in raw_columns if not c.startswith(":")]
        if not columns:
            raise ValueError(f"No schema.columns found for: {args.relative_path}")

        mapping_entry = create_mapping_entry(classes, args.class_name, args.relative_path, columns)
        current["mappings"][args.relative_path] = mapping_entry

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, ensure_ascii=True)

        print(f"Wrote mapping draft to: {output_path}")
        print(f"- class: {args.class_name}")
        print(f"- relative_path: {args.relative_path}")
        print(f"- mapped required fields: {len(mapping_entry['field_mapping'])}")
        print(f"- missing required fields: {len(mapping_entry['missing_required'])}")
        if mapping_entry["missing_required"]:
            print(f"  missing: {mapping_entry['missing_required']}")


if __name__ == "__main__":
    main()
