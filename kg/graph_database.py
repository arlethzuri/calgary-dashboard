"""Load ontology-mapped Calgary data into Neo4j.

This script reads:
- ontology/calgary-ontology.json
- ontology/mappings_from_pairs.json
- data_cleaning/processed_data_catalog_20260327.json

It loads node instances for each mapped class/file pair, enriches with metadata
fields (including spatial_reference), and creates relationships defined by
ontology join keys.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import geopandas as gpd
from neo4j import GraphDatabase
from shapely import wkt as shapely_wkt

DEFAULT_BASE = Path("/Users/arleth/Desktop/calgary-dashboard")
DEFAULT_ONTOLOGY = DEFAULT_BASE / "ontology/calgary-ontology.json"
DEFAULT_MAPPINGS = DEFAULT_BASE / "ontology/mappings_from_pairs.json"
DEFAULT_CATALOG = DEFAULT_BASE / "data_cleaning/processed_data_catalog_20260327.json"
DEFAULT_DATA_ROOT = DEFAULT_BASE / "data/calgary/processed_data"


@dataclass
class LoadedNode:
    class_name: str
    kg_id: str
    properties: Dict[str, Any]


def chunked(items: List[Dict[str, str]], size: int = 5000) -> Iterable[List[Dict[str, str]]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def sanitize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=True)
    if isinstance(value, list):
        return [sanitize_value(v) for v in value]
    return str(value)


def feature_to_metadata_map(catalog: dict) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for pair in catalog.get("metadata_feature_pairings", []):
        metadata_path = pair.get("metadata_relative_path")
        for feature_path in pair.get("feature_relative_paths", []):
            if metadata_path and feature_path:
                out[feature_path] = metadata_path
    return out


def read_metadata_fields(metadata_path: Optional[Path]) -> Dict[str, Any]:
    if metadata_path is None or not metadata_path.exists():
        return {}
    meta = load_json(metadata_path)
    spatial_ref = meta.get("spatialReference", {}) or {}
    custom_fields = (meta.get("metadata", {}) or {}).get("custom_fields", {}) or {}
    geospatial = custom_fields.get("Geospatial Information", {}) or {}
    return {
        "name": meta.get("name"),
        "description": meta.get("description"),
        "maintained_by": meta.get("copyrightText"),
        "spatial_reference": (
            spatial_ref.get("latestWkid")
            or spatial_ref.get("wkid")
            or spatial_ref.get("wkt2")
            or spatial_ref.get("wkt")
            or geospatial.get("Map Projection")
        ),
    }


def iter_source_rows(feature_path: Path) -> Iterable[Tuple[int, Dict[str, Any]]]:
    if feature_path.suffix.lower() == ".parquet":
        gdf = gpd.read_parquet(feature_path)
        geom_name = gdf.geometry.name if gdf.geometry is not None else "geometry"
        for i, row in gdf.iterrows():
            row_dict = {k: row[k] for k in gdf.columns if k != geom_name}
            geom = row[geom_name] if geom_name in gdf.columns else None
            row_dict["geometry"] = geom.wkt if geom is not None and not geom.is_empty else None
            yield int(i), row_dict
        return

    if feature_path.suffix.lower() == ".json":
        with feature_path.open("r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return
        for i, row in enumerate(data):
            if isinstance(row, dict):
                clean = {k: v for k, v in row.items() if not str(k).startswith(":")}
                yield i, clean
        return

    raise ValueError(f"Unsupported feature file extension: {feature_path.suffix}")


def build_node_properties(
    row: Dict[str, Any],
    field_mapping: Dict[str, str],
    metadata_fields: Dict[str, Any],
    source_relpath: str,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for canonical, source_col in field_mapping.items():
        out[canonical] = sanitize_value(row.get(source_col))

    # Backfill required fields from metadata when not row-mapped.
    for key in ("name", "description", "maintained_by", "spatial_reference"):
        if out.get(key) in (None, "") and metadata_fields.get(key) not in (None, ""):
            out[key] = sanitize_value(metadata_fields[key])

    out["source_relative_path"] = source_relpath
    return out


def make_kg_id(
    class_name: str,
    mode: str,
    props: Dict[str, Any],
    source_relpath: str,
    row_index: int,
) -> str:
    if "id" in props and props.get("id") not in (None, ""):
        return f"{class_name}:{props['id']}"
    if "feeder_id" in props and "date_last_updated" in props and mode == "measurement":
        return f"{class_name}:{props.get('feeder_id')}:{props.get('date_last_updated')}:{row_index}"
    if "feeder_id" in props and mode == "node":
        return f"{class_name}:{props.get('feeder_id')}"
    if "abbreviation" in props and mode == "node":
        return f"{class_name}:{props.get('abbreviation')}"
    if "site_id" in props and "year" in props and "month" in props and mode == "measurement":
        return f"{class_name}:{props.get('site_id')}:{props.get('year')}:{props.get('month')}"
    if "site_id" in props and mode == "node":
        return f"{class_name}:{props.get('site_id')}"
    return f"{class_name}:{source_relpath}:{row_index}"


def ensure_constraints(driver: GraphDatabase.driver, database: str) -> None:
    driver.execute_query(
        "CREATE CONSTRAINT kg_entity_id_unique IF NOT EXISTS FOR (n:Entity) REQUIRE n.kg_id IS UNIQUE",
        database_=database,
    )


def upsert_node(driver: GraphDatabase.driver, database: str, class_name: str, kg_id: str, props: Dict[str, Any]) -> None:
    query = f"""
    MERGE (n:Entity:`{class_name}` {{kg_id: $kg_id}})
    SET n += $props, n.class_name = $class_name
    """
    driver.execute_query(query, kg_id=kg_id, props=props, class_name=class_name, database_=database)


def create_relationships(
    driver: GraphDatabase.driver,
    database: str,
    ontology: dict,
    loaded_nodes: List[LoadedNode],
) -> None:
    by_class: Dict[str, List[LoadedNode]] = {}
    for n in loaded_nodes:
        by_class.setdefault(n.class_name, []).append(n)

    for rel in ontology.get("relationships", []):
        rel_name = rel.get("name")
        from_class = rel.get("from")
        to_class = rel.get("to")
        join = rel.get("join_on", {})
        from_prop = join.get("from_property")
        to_prop = join.get("to_property")

        if not rel_name or not from_class or not to_class or not from_prop or not to_prop:
            continue
        if not isinstance(from_class, str) or not isinstance(to_class, str):
            continue

        from_nodes = by_class.get(from_class, [])
        to_nodes = by_class.get(to_class, [])
        if not from_nodes or not to_nodes:
            continue

        target_by_value: Dict[str, List[LoadedNode]] = {}
        for n in to_nodes:
            value = n.properties.get(to_prop)
            if value in (None, ""):
                continue
            key = str(value).strip().lower()
            target_by_value.setdefault(key, []).append(n)

        rel_type = str(rel_name).upper().replace(" ", "_")
        query = f"""
        UNWIND $rows AS row
        MATCH (a:Entity {{kg_id: row.from_id}})
        MATCH (b:Entity {{kg_id: row.to_id}})
        MERGE (a)-[:`{rel_type}`]->(b)
        """
        rel_rows: List[Dict[str, str]] = []
        for src in from_nodes:
            value = src.properties.get(from_prop)
            if value in (None, ""):
                continue
            key = str(value).strip().lower()
            for target in target_by_value.get(key, []):
                rel_rows.append({"from_id": src.kg_id, "to_id": target.kg_id})

        if not rel_rows:
            print(f"No rows for relationship {rel_type} ({from_class} -> {to_class}).")
            continue

        for batch in chunked(rel_rows, size=5000):
            driver.execute_query(query, rows=batch, database_=database)
        print(f"Created/merged {len(rel_rows)} rows for relationship {rel_type} ({from_class} -> {to_class}).")


def build_children_index(classes: Dict[str, Dict[str, Any]]) -> Dict[str, List[str]]:
    children: Dict[str, List[str]] = {}
    for class_name, class_spec in classes.items():
        parent = class_spec.get("parent_class")
        if parent:
            children.setdefault(parent, []).append(class_name)
    return children


def descendants_including_self(class_name: str, children_index: Dict[str, List[str]]) -> List[str]:
    out: List[str] = [class_name]
    stack: List[str] = [class_name]
    seen = {class_name}
    while stack:
        current = stack.pop()
        for child in children_index.get(current, []):
            if child in seen:
                continue
            seen.add(child)
            out.append(child)
            stack.append(child)
    return out


def create_spatial_contains_relationships(
    driver: GraphDatabase.driver,
    database: str,
    ontology: dict,
    loaded_nodes: List[LoadedNode],
) -> None:
    _ = ontology  # reserved for future policy filtering

    rows: List[Dict[str, Any]] = []
    for n in loaded_nodes:
        geom_wkt = n.properties.get("geometry")
        if not geom_wkt:
            continue
        try:
            geom = shapely_wkt.loads(str(geom_wkt))
        except Exception:
            continue
        if geom is None or geom.is_empty:
            continue
        rows.append({"kg_id": n.kg_id, "class_name": n.class_name, "geometry": geom})

    if not rows:
        print("No geometries available for spatial containment pass.")
        return

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    sindex = gdf.sindex

    rel_rows: List[Dict[str, str]] = []
    for src_idx, src_row in gdf.iterrows():
        src_geom = src_row.geometry
        if src_geom is None or src_geom.is_empty:
            continue

        candidate_idxs = list(sindex.intersection(src_geom.bounds))
        for tgt_idx in candidate_idxs:
            if src_idx == tgt_idx:
                continue
            tgt_row = gdf.iloc[tgt_idx]
            tgt_geom = tgt_row.geometry
            if tgt_geom is None or tgt_geom.is_empty:
                continue
            # Avoid mirrored duplicates on equal geometries.
            if src_geom.equals(tgt_geom):
                continue
            # covers() allows boundary-touching children; better for polygon boundaries.
            if src_geom.covers(tgt_geom):
                rel_rows.append({"from_id": str(src_row.kg_id), "to_id": str(tgt_row.kg_id)})

    if not rel_rows:
        print("No SPATIALLY_CONTAINS relationships found from geometry.")
        return

    query = """
    UNWIND $rows AS row
    MATCH (a:Entity {kg_id: row.from_id})
    MATCH (b:Entity {kg_id: row.to_id})
    MERGE (a)-[:SPATIALLY_CONTAINS]->(b)
    """
    driver.execute_query(query, rows=rel_rows, database_=database)
    print(f"Created/merged {len(rel_rows)} SPATIALLY_CONTAINS relationships from geometry.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load mapped Calgary ontology data into Neo4j.")
    parser.add_argument("--uri", default="neo4j://localhost")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="esmicontrasena")
    parser.add_argument("--database", default="neo4j")
    parser.add_argument("--ontology", default=str(DEFAULT_ONTOLOGY))
    parser.add_argument("--mappings", default=str(DEFAULT_MAPPINGS))
    parser.add_argument("--catalog", default=str(DEFAULT_CATALOG))
    parser.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT))
    parser.add_argument("--row-limit", type=int, default=0, help="Optional per-file row cap for testing.")
    parser.add_argument(
        "--skip-join-pass",
        action="store_true",
        help="Skip ontology join_on relationship creation pass.",
    )
    parser.add_argument(
        "--skip-spatial-pass",
        action="store_true",
        help="Skip geometry-based SPATIALLY_CONTAINS pass.",
    )
    args = parser.parse_args()

    ontology = load_json(Path(args.ontology))
    mappings_doc = load_json(Path(args.mappings))
    catalog = load_json(Path(args.catalog))

    feature_meta = feature_to_metadata_map(catalog)
    data_root = Path(args.data_root)
    loaded_nodes: List[LoadedNode] = []

    with GraphDatabase.driver(args.uri, auth=(args.user, args.password)) as driver:
        driver.verify_connectivity()
        ensure_constraints(driver, args.database)
        print("Connected to Neo4j and ensured constraints.")

        mappings = mappings_doc.get("mappings", {})
        for mapping_key, mapping_entry in mappings.items():
            class_name = mapping_entry.get("class_name")
            mode = mapping_entry.get("mode", "node")
            field_mapping = mapping_entry.get("field_mapping", {})
            if not class_name or not isinstance(field_mapping, dict):
                continue

            source_relpath = mapping_key.split("::", 1)[0]
            source_path = data_root / source_relpath
            if not source_path.exists():
                print(f"SKIP missing source file: {source_relpath}")
                continue

            metadata_relpath = feature_meta.get(source_relpath)
            metadata_path = (data_root / metadata_relpath) if metadata_relpath else None
            metadata_fields = read_metadata_fields(metadata_path)

            inserted = 0
            for row_index, row in iter_source_rows(source_path):
                if args.row_limit > 0 and inserted >= args.row_limit:
                    break
                props = build_node_properties(row, field_mapping, metadata_fields, source_relpath)
                kg_id = make_kg_id(class_name, mode, props, source_relpath, row_index)
                upsert_node(driver, args.database, class_name, kg_id, props)
                loaded_nodes.append(LoadedNode(class_name=class_name, kg_id=kg_id, properties=props))
                inserted += 1
            print(f"Loaded {inserted} rows into class {class_name} from {source_relpath}")

        print("Starting relationship creation passes...")
        if not args.skip_join_pass:
            print("Running join_on relationship pass...")
            create_relationships(driver, args.database, ontology, loaded_nodes)
        else:
            print("Skipping join_on relationship pass (--skip-join-pass).")

        if not args.skip_spatial_pass:
            print("Running spatial containment pass...")
            create_spatial_contains_relationships(driver, args.database, ontology, loaded_nodes)
        else:
            print("Skipping spatial containment pass (--skip-spatial-pass).")
        print("Finished relationship creation passes.")


if __name__ == "__main__":
    main()