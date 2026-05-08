"""Palantir-style geospatial command dashboard for Neo4j ontology data.

Run from repo root:
    panel serve kg/neo4j_palantir_map_dashboard.py --show
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import cartopy.crs as ccrs
import geopandas as gpd
import geoviews as gv
import holoviews as hv
import pandas as pd
import panel as pn
from neo4j import GraphDatabase
from shapely import wkt
from shapely.geometry import box

from calgary_dashboard.config.settings import get_settings

gv.extension("bokeh")

_settings = get_settings()
URI = _settings.neo4j_uri
AUTH = (_settings.neo4j_user, _settings.neo4j_password)
DATABASE = _settings.neo4j_database
TARGET_CRS = "EPSG:4326"
CALGARY_BOUNDS = (-114.35, 50.88, -113.85, 51.22)
DEFAULT_LIMIT = 7000
MAX_LIMIT = 40000

LAYER_COLORS = {
    "AdministrativeZone": "#4E79A7",
    "Environment": "#59A14F",
    "Measurements": "#E15759",
    "Other": "#76B7B2",
}
INFRA_PALETTE = ["#F28E2B", "#76B7B2", "#EDC948", "#B07AA1", "#FF9DA7", "#9C755F"]

CLASS_GROUPS = {
    "AdministrativeZone": {"CityLimits", "CityQuadrant", "ENMAXServiceArea", "AirportVicinity"},
    "Infrastructure": {"FeederSegment", "MonitoringStation", "SolarProductionSite", "CityBuilding"},
    "Measurements": {"FeederMeasurement", "AirQualityReading", "EnergyConsumptionRecord"},
}
INFRA_TO_MEASUREMENTS = {
    "FeederSegment": ["FeederMeasurement"],
    "MonitoringStation": ["AirQualityReading"],
    "CityBuilding": ["EnergyConsumptionRecord"],
    "SolarProductionSite": [],
}

CUSTOM_CSS = """
.cmd-card {
  background: #ffffff;
  border: 1px solid #d7dee8;
  border-radius: 12px;
  padding: 10px 12px;
  margin-bottom: 8px;
}
.cmd-title { color: #5f6b7a; font-size: 11px; text-transform: uppercase; letter-spacing: .08em; }
.cmd-value { color: #1f2937; font-size: 24px; font-weight: 700; line-height: 1.2; }
.cmd-sub { color: #6b7280; font-size: 12px; }
"""

pn.extension("tabulator", raw_css=[CUSTOM_CSS])


def class_group(class_name: str) -> str:
    for group, members in CLASS_GROUPS.items():
        if class_name in members:
            return group
    if "Zone" in class_name or "City" in class_name:
        return "AdministrativeZone"
    if "Measurement" in class_name or "Reading" in class_name or "Record" in class_name:
        return "Measurements"
    return "Infrastructure" if "Segment" in class_name or "Station" in class_name else "Other"


def fetch_class_counts(driver: GraphDatabase.driver) -> pd.DataFrame:
    query = """
    MATCH (n:Entity)
    RETURN n.class_name AS class_name, count(*) AS node_count
    ORDER BY node_count DESC, class_name
    """
    rows = driver.execute_query(query, database_=DATABASE).records
    df = pd.DataFrame([r.data() for r in rows])
    if df.empty:
        return pd.DataFrame(columns=["class_name", "node_count", "group"])
    df["group"] = df["class_name"].map(class_group)
    return df


def fetch_relationship_counts(driver: GraphDatabase.driver) -> pd.DataFrame:
    query = """
    MATCH ()-[r]->()
    RETURN type(r) AS rel_type, count(*) AS rel_count
    ORDER BY rel_count DESC
    """
    rows = driver.execute_query(query, database_=DATABASE).records
    return pd.DataFrame([r.data() for r in rows])


def fetch_nodes(driver: GraphDatabase.driver, class_names: List[str], limit: int) -> pd.DataFrame:
    query = """
    MATCH (n:Entity)
    WHERE n.class_name IN $class_names
      AND n.geometry IS NOT NULL
      AND n.geometry <> ''
    RETURN n.kg_id AS kg_id,
           n.class_name AS class_name,
           n.name AS name,
           n.description AS description,
           n.source_relative_path AS source_relative_path,
           n.geometry AS geometry_wkt
    LIMIT $limit
    """
    rows = driver.execute_query(query, class_names=class_names, limit=limit, database_=DATABASE).records
    df = pd.DataFrame([r.data() for r in rows])
    if df.empty:
        return df
    df["group"] = df["class_name"].map(class_group)
    return df


def load_geometries(df: pd.DataFrame) -> gpd.GeoDataFrame:
    if df.empty:
        return gpd.GeoDataFrame(df.copy(), geometry=[], crs=TARGET_CRS)
    local = df.copy()
    local["geometry"] = local["geometry_wkt"].map(wkt.loads)
    return gpd.GeoDataFrame(local, geometry="geometry", crs=TARGET_CRS)


def build_plot(gdf: gpd.GeoDataFrame) -> hv.Overlay:
    minx, miny, maxx, maxy = CALGARY_BOUNDS
    bounds = gpd.GeoDataFrame(geometry=[box(minx, miny, maxx, maxy)], crs=TARGET_CRS).to_crs(epsg=3857).total_bounds
    base = gv.tile_sources.CartoDark.opts(
        width=1220,
        height=760,
        xlim=(float(bounds[0]), float(bounds[2])),
        ylim=(float(bounds[1]), float(bounds[3])),
        tools=["pan", "wheel_zoom", "box_zoom", "reset", "save"],
        active_tools=["wheel_zoom"],
    )
    if gdf.empty:
        return base

    gdf = gdf.copy()
    infra_classes = sorted(gdf.loc[gdf["group"] == "Infrastructure", "class_name"].dropna().astype(str).unique().tolist())
    infra_color_map = {
        class_name: INFRA_PALETTE[i % len(INFRA_PALETTE)] for i, class_name in enumerate(infra_classes)
    }
    gdf["layer_color"] = gdf.apply(
        lambda row: infra_color_map.get(str(row["class_name"]))
        if str(row.get("group")) == "Infrastructure"
        else LAYER_COLORS.get(str(row.get("group")), "#76B7B2"),
        axis=1,
    )
    points = gdf[gdf.geometry.geom_type.isin(["Point", "MultiPoint"])].copy()
    lines = gdf[gdf.geometry.geom_type.isin(["LineString", "MultiLineString"])].copy()
    polys = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    overlay = base
    if not polys.empty:
        poly = gv.Polygons(polys, vdims=["class_name", "name", "group", "layer_color"], crs=ccrs.PlateCarree()).opts(
            color="layer_color",
            fill_alpha=0.08,
            line_alpha=0.75,
            line_width=1.4,
            tools=[],
        )
        overlay = overlay * gv.project(poly, projection=ccrs.GOOGLE_MERCATOR)

    if not lines.empty:
        line_paths = gv.Path(lines, vdims=["class_name", "group", "layer_color"], crs=ccrs.PlateCarree()).opts(
            color="layer_color",
            line_width=2.2,
            alpha=0.9,
            tools=[],
        )
        overlay = overlay * gv.project(line_paths, projection=ccrs.GOOGLE_MERCATOR)

    if not points.empty:
        pts = points.copy()
        pts["lon"] = pts.geometry.x
        pts["lat"] = pts.geometry.y
        point_plot = gv.Points(
            pts, kdims=["lon", "lat"], vdims=["class_name", "name", "kg_id", "group", "layer_color"], crs=ccrs.PlateCarree()
        ).opts(
            color="layer_color",
            size=8,
            alpha=0.95,
            tools=["hover"],
            hover_tooltips=[("group", "@group"), ("class", "@class_name"), ("name", "@name"), ("id", "@kg_id")],
        )
        overlay = overlay * gv.project(point_plot, projection=ccrs.GOOGLE_MERCATOR)

    return overlay


def build_legend_html(df: pd.DataFrame) -> str:
    if df.empty or "group" not in df.columns:
        return "<div class='cmd-sub'>No legend available yet.</div>"

    infra_classes = sorted(df.loc[df["group"] == "Infrastructure", "class_name"].dropna().astype(str).unique().tolist())
    infra_color_map = {
        class_name: INFRA_PALETTE[i % len(INFRA_PALETTE)] for i, class_name in enumerate(infra_classes)
    }
    items: List[Tuple[str, str]] = []

    # Non-infrastructure groups are fixed colors.
    for group in ["AdministrativeZone", "Measurements", "Environment", "Other"]:
        if (df["group"] == group).any():
            items.append((group, LAYER_COLORS.get(group, "#76B7B2")))

    # Infrastructure classes each get their own color.
    for class_name in infra_classes:
        items.append((f"Infrastructure / {class_name}", infra_color_map[class_name]))

    rows = []
    for label, color in items:
        rows.append(
            f"<div style='display:flex;align-items:center;margin:4px 0;'>"
            f"<span style='display:inline-block;width:12px;height:12px;border-radius:2px;background:{color};margin-right:8px;border:1px solid #c9d2df;'></span>"
            f"<span style='font-size:12px;color:#334155;'>{label}</span>"
            f"</div>"
        )
    return "".join(rows) if rows else "<div class='cmd-sub'>No legend items for current selection.</div>"


def metric_card(title: str, value: str, subtitle: str = "") -> pn.pane.HTML:
    return pn.pane.HTML(
        f"""
        <div class="cmd-card">
          <div class="cmd-title">{title}</div>
          <div class="cmd-value">{value}</div>
          <div class="cmd-sub">{subtitle}</div>
        </div>
        """,
        sizing_mode="stretch_width",
    )


with GraphDatabase.driver(URI, auth=AUTH) as _driver:
    CLASS_COUNTS = fetch_class_counts(_driver)
    REL_COUNTS = fetch_relationship_counts(_driver)

CLASS_OPTIONS = sorted(CLASS_COUNTS["class_name"].tolist()) if not CLASS_COUNTS.empty else []
ADMIN_OPTIONS = sorted([c for c in CLASS_OPTIONS if class_group(c) == "AdministrativeZone"])
INFRA_OPTIONS = sorted([c for c in CLASS_OPTIONS if class_group(c) == "Infrastructure"])
MEAS_OPTIONS_ALL = sorted([c for c in CLASS_OPTIONS if class_group(c) == "Measurements"])

admin_selector = pn.widgets.MultiChoice(
    name="Administrative Zone",
    options=ADMIN_OPTIONS,
    value=ADMIN_OPTIONS[: min(2, len(ADMIN_OPTIONS))],
)
infra_selector = pn.widgets.MultiChoice(
    name="Infrastructure",
    options=INFRA_OPTIONS,
    value=INFRA_OPTIONS[: min(2, len(INFRA_OPTIONS))],
)
measurement_selector = pn.widgets.MultiChoice(
    name="Measurements (based on infrastructure)",
    options=[],
    value=[],
)


def sync_measurement_options(_event=None) -> None:
    picked = set(infra_selector.value or [])
    suggested: List[str] = []
    for infra in sorted(picked):
        suggested.extend(INFRA_TO_MEASUREMENTS.get(infra, []))
    allowed = sorted([m for m in set(suggested) if m in MEAS_OPTIONS_ALL])
    measurement_selector.options = allowed
    measurement_selector.value = [m for m in measurement_selector.value if m in allowed]
    if allowed and not measurement_selector.value:
        measurement_selector.value = allowed
    measurement_selector.disabled = len(allowed) == 0


infra_selector.param.watch(sync_measurement_options, "value")
sync_measurement_options()

limit_slider = pn.widgets.IntSlider(name="Max geospatial entities", start=200, end=MAX_LIMIT, step=200, value=DEFAULT_LIMIT)
refresh_button = pn.widgets.Button(name="Refresh Intelligence View", button_type="primary")
refresh_button.name = "Refresh Map View"

status = pn.pane.Markdown("Ready. Choose layers and refresh.", sizing_mode="stretch_width")
plot_pane = pn.pane.HoloViews(sizing_mode="stretch_both")
rows_table = pn.widgets.Tabulator(pd.DataFrame(), disabled=True, height=260, sizing_mode="stretch_width")
rel_table = pn.widgets.Tabulator(REL_COUNTS, disabled=True, height=220, sizing_mode="stretch_width")
legend_pane = pn.pane.HTML("<div class='cmd-sub'>Refresh to populate legend.</div>", sizing_mode="stretch_width")

total_nodes = int(CLASS_COUNTS["node_count"].sum()) if not CLASS_COUNTS.empty else 0
total_classes = int(CLASS_COUNTS["class_name"].nunique()) if not CLASS_COUNTS.empty else 0
total_rels = int(REL_COUNTS["rel_count"].sum()) if not REL_COUNTS.empty else 0
spatial_contains = int(REL_COUNTS.loc[REL_COUNTS["rel_type"] == "SPATIALLY_CONTAINS", "rel_count"].sum()) if not REL_COUNTS.empty else 0

cards = pn.Row(
    metric_card("Entities", f"{total_nodes:,}", "loaded in Neo4j"),
    metric_card("Classes", f"{total_classes:,}", "ontology classes present"),
    metric_card("Relationships", f"{total_rels:,}", "all edge types"),
    metric_card("Spatial Contains", f"{spatial_contains:,}", "geometric containment edges"),
    sizing_mode="stretch_width",
)


def refresh() -> None:
    selected = sorted(
        set(list(admin_selector.value or []) + list(infra_selector.value or []) + list(measurement_selector.value or []))
    )
    if not selected:
        status.object = "Select at least one administrative/infrastructure layer."
        plot_pane.object = build_plot(gpd.GeoDataFrame(geometry=[], crs=TARGET_CRS))
        rows_table.value = pd.DataFrame()
        return

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        df = fetch_nodes(driver, selected, int(limit_slider.value))

    gdf = load_geometries(df)
    plot_pane.object = build_plot(gdf)
    legend_pane.object = build_legend_html(df)
    preview_cols = [c for c in ["kg_id", "group", "class_name", "name", "source_relative_path"] if c in df.columns]
    rows_table.value = df[preview_cols].head(1200) if not df.empty else pd.DataFrame(columns=preview_cols)
    group_counts = df["group"].value_counts().to_dict() if not df.empty else {}
    status.object = (
        f"Displaying **{len(gdf):,}** geospatial entities. "
        f"Group mix: {', '.join([f'{k}:{v}' for k, v in group_counts.items()]) if group_counts else 'none'}."
    )


refresh_button.on_click(lambda _event: refresh())
plot_pane.object = build_plot(gpd.GeoDataFrame(geometry=[], crs=TARGET_CRS))

template = pn.template.FastListTemplate(
    title="Calgary Dashboard",
    sidebar=[
        pn.pane.Markdown("### Data Selection"),
        admin_selector,
        infra_selector,
        measurement_selector,
        limit_slider,
        refresh_button,
        pn.pane.Markdown("### Legend"),
        legend_pane,
        pn.pane.Markdown("### Relationship Intelligence"),
        rel_table,
    ],
    main=[
        cards,
        status,
        plot_pane,
        pn.pane.Markdown("### Entity Feed"),
        rows_table,
    ],
    accent_base_color="#2f80ed",
)

template.servable()
