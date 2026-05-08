"""Ontology mapping logic layer.

Responsibilities:
- pure mapping/normalization/inference logic
- deterministic transforms from input metadata to mapping outputs

Non-responsibilities:
- file reads/writes
- CLI parsing
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class MappingRequest:
    """Inputs for generating one mapping artifact."""

    class_name: str
    relative_path: str
    columns: list[str]


@dataclass(frozen=True)
class MappingResult:
    """Result container for one generated mapping."""

    class_name: str
    field_mapping: dict[str, str]
    missing_required: list[str]
    unmapped_source_columns: list[str]


ALIASES: dict[str, list[str]] = {
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


def normalize_name(text: str) -> str:
    """Normalize names for fuzzy matching."""
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def resolve_required_fields(classes: dict[str, Any], class_name: str) -> list[str]:
    """Resolve required fields including inherited requirements."""
    if class_name not in classes:
        raise ValueError(f"Class not found in ontology: {class_name}")

    required: list[str] = []
    visited: set[str] = set()
    current: str | None = class_name

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
    unique: list[str] = []
    seen: set[str] = set()
    for item in required:
        if item not in seen:
            unique.append(item)
            seen.add(item)
    return unique


def build_column_index(columns: list[str]) -> dict[str, str]:
    """Build normalized lookup for source column names."""
    index: dict[str, str] = {}
    for col in columns:
        if col.startswith(":"):
            continue
        index[normalize_name(col)] = col
    return index


def pick_source_column(
    canonical: str, columns: list[str], column_index: dict[str, str]
) -> str | None:
    """Pick best matching source column for a canonical field."""
    if canonical in columns:
        return canonical

    norm_canonical = normalize_name(canonical)
    if norm_canonical in column_index:
        return column_index[norm_canonical]

    for alias in ALIASES.get(canonical, []):
        if alias in columns:
            return alias
        norm_alias = normalize_name(alias)
        if norm_alias in column_index:
            return column_index[norm_alias]
    return None


def generate_mapping(
    *,
    ontology_classes: dict[str, Any],
    request: MappingRequest,
) -> MappingResult:
    """Generate one mapping result for one class/path/column set."""
    required_fields = resolve_required_fields(ontology_classes, request.class_name)
    column_index = build_column_index(request.columns)

    field_mapping: dict[str, str] = {}
    missing_required: list[str] = []
    for field in required_fields:
        source_col = pick_source_column(field, request.columns, column_index)
        if source_col is None:
            missing_required.append(field)
        else:
            field_mapping[field] = source_col

    return MappingResult(
        class_name=request.class_name,
        field_mapping=field_mapping,
        missing_required=missing_required,
        unmapped_source_columns=[
            c for c in request.columns if c not in field_mapping.values()
        ],
    )


def find_catalog_entry(catalog: dict[str, Any], relative_path: str) -> dict[str, Any]:
    """Find one file entry by relative_path in processed-data catalog."""
    for entry in catalog.get("files", []):
        if entry.get("relative_path") == relative_path:
            return entry
    raise ValueError(f"Could not find relative_path in catalog: {relative_path}")


def infer_json_array_columns(json_path: Path) -> list[str]:
    """Infer columns from the first object of a JSON array file."""
    decoder = json.JSONDecoder()
    with json_path.open("r", encoding="utf-8", errors="ignore") as file_obj:
        sample = file_obj.read(2_000_000)
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
    catalog: dict[str, Any], data_root: Path
) -> list[tuple[str, list[str]]]:
    """Yield feature relative_path + columns, inferring JSON columns when needed."""
    out: list[tuple[str, list[str]]] = []
    for entry in catalog.get("files", []):
        if entry.get("role") != "feature":
            continue
        relative_path = entry.get("relative_path")
        if not relative_path:
            continue
        schema = entry.get("schema") or {}
        raw_columns = schema.get("columns", [])
        columns = [
            c
            for c in raw_columns
            if isinstance(c, str) and not c.startswith(":")
        ]
        if not columns and str(relative_path).endswith(".json"):
            json_path = data_root / relative_path
            if json_path.exists():
                columns = infer_json_array_columns(json_path)
        if columns:
            out.append((relative_path, columns))
    return out


def build_feature_column_lookup(
    catalog: dict[str, Any], data_root: Path
) -> dict[str, list[str]]:
    """Build lookup of relative_path -> inferred columns."""
    lookup: dict[str, list[str]] = {}
    for relative_path, columns in iter_feature_file_columns_with_json(
        catalog, data_root
    ):
        lookup[relative_path] = columns
    return lookup


def resolve_lookup_path(
    feature_lookup: dict[str, list[str]], relative_path: str
) -> tuple[str | None, list[str] | None]:
    """Resolve exact or common alternate relative paths."""
    columns = feature_lookup.get(relative_path)
    if columns:
        return relative_path, columns

    if relative_path.endswith(".json"):
        alt = re.sub(
            r"/features/(point|linestring|polygon|multipolygon)/",
            "/features/",
            relative_path,
        )
        if alt != relative_path:
            alt_columns = feature_lookup.get(alt)
            if alt_columns:
                return alt, alt_columns
    return None, None

