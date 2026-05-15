"""Calgary geospatial demo (demo4) — one map, checkbox-driven layers.

Reads a curated subset of processed GeoParquet layers. Bundled data lives in
``demos/demo4/data/``; if missing, falls back to ``data/calgary/processed_data``
(or ``DATA_ROOT`` from ``.env``).

Included layers: all flood maps, solar production sites, all ENMAX load/hosting
capacity, StatCan population/dwellings, and selected Open Calgary themes
(hydrology, communities, parks/cemeteries, natural areas, land use, SRG growth,
schools).

Run::

    PYTHONPATH=src bokeh serve demos/demo4/app.py --show --dev

Requires: bokeh, geopandas, pandas, shapely, numpy.
"""
from __future__ import annotations

import html
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import (
    ColorBar,
    Div,
    GeoJSONDataSource,
    HoverTool,
    LinearColorMapper,
    CheckboxGroup,
    Select,
    WheelZoomTool,
)
from bokeh.palettes import Category20, Plasma256
from bokeh.plotting import figure
from shapely.geometry import box

from calgary_dashboard.common.definitions import GEOMETRY_BUCKET_NAMES
from calgary_dashboard.common.io import list_subdirectories
from calgary_dashboard.config.paths import DATA_ROOT, PROCESSED_DATA_ROOT

DOWNTOWN_LON = -114.065
DOWNTOWN_LAT = 51.045
VIEW_HALF_KM = 6.0
CLIP_WORK = "EPSG:32611"
PLOT_CRS = "EPSG:4326"
WEB_MERCATOR = "EPSG:3857"

CHOROPLETH_FILL_ALPHA = 0.55
OTHER_POLY_FILL_ALPHA = 0.38
OVERLAY_POLY_FILL_ALPHA = 0.22

HERE = Path(__file__).resolve().parent

# StatCan columns are stored with long metadata labels; we expose short names in the UI.
STATCAN_COLUMNS: list[tuple[str, str]] = [
    ("Population and dwelling counts (5): Population, 2021 [1]", "Population (2021)"),
    ("Population and dwelling counts (5): Total private dwellings, 2021 [2]", "Total dwellings"),
    (
        "Population and dwelling counts (5): Private dwellings occupied by usual residents, 2021 [3]",
        "Occupied dwellings",
    ),
    ("Population and dwelling counts (5): Land area in square kilometres, 2021 [4]", "Land area (km²)"),
    ("Population and dwelling counts (5): Population density per square kilometre, 2021 [5]", "Population density"),
]

# Discrete ENMAX legend bins → hex colours.
# We keep both load (KVA) and hosting (kW) bins in one map so `capacity_line_color`
# can normalize and return a colour for either dataset.
CAPACITY_COLORS: dict[str, str] = {
    "11,000 - 12,999 KVA": "#6b87b8",  # lowest likelihood for system upgrades
    "9,000 - 10,999 KVA": "#4aa8c8",
    "7,000 - 8,999 KVA": "#34bfa3",
    "5,000 - 6,999 KVA": "#7ad151",
    "3,000 - 4,999 KVA": "#dce319",
    "1,000 - 2,999 KVA": "#fdae61",
    "< 999 KVA": "#d73027",  # highest likelihood for system upgrades
    # Hosting capacity (kW) legend bins — colors chosen to match ENMAX style.
    ">10,000 kW": "#4caf50",  # lowest likelihood for system upgrades
    "5,000 kW - 9,999 kW": "#42a5f5",
    "1,000 kW - 4,999 kW": "#ab47bc",
    "500 kW - 999 kW": "#ffeb3b",
    "200 kW - 499 kW": "#ffb74d",
    "< 199 kW": "#e57373",  # highest likelihood for system upgrades
}

PHASE_COLORS: dict[str, str] = {"A": "#2563eb", "B": "#ea580c", "C": "#16a34a", "1": "#9333ea", "3": "#dc2626"}

PROJECT_COLORS: dict[str, str] = {
    "OrangeSubstation": "#ea580c",
    "BlueTransmission": "#2563eb",
    "GreenDistribution": "#16a34a",
    "YellowOther": "#ca8a04",
}

# Curated Open Calgary feature stems (parquet filename without ``_features``).
OPEN_CALGARY_DEMO_STEMS: frozenset[str] = frozenset(
    {
        "uqkc-h9wi_solarproductionsitesmap",
        "a2cn-dxht_hydrology",
        "ab7m-fwn6_communityboundaries",
        "cf5t-fjzu_parkscemeteries",
        "icxc-6yk3_naturalareas",
        "mw9j-jik5_landusedistricts",
        "n4vp-3exq_suburbanresidentialgrowthsrgforecastmap",
        "xmep-aasr_schoolsincommunities",
    }
)


def parquet_feature_stem(path: Path) -> str:
    return path.stem.replace("_features", "").lower()


def _dataset_source(path: Path) -> str | None:
    for part in path.parts:
        if part in ("open_calgary", "enmax", "statcan"):
            return part
    return None


def is_demo_dataset(path: Path) -> bool:
    """Return True if this parquet is in the demo4 allowlist."""
    source = _dataset_source(path)
    if source is None:
        return False
    name = parquet_feature_stem(path)

    if source == "enmax":
        if "loadcapacity" in name and "hosting" not in name:
            return True
        return "hostingcapacity" in name

    if source == "statcan":
        return "98100015" in name or "populationanddwelling" in name

    if source == "open_calgary":
        if "floodmap" in name:
            return True
        return name in OPEN_CALGARY_DEMO_STEMS

    return False


def is_flood_data(label: str, gdf: gpd.GeoDataFrame) -> bool:
    if "floodmap" in stem(label):
        return True
    if "open_calgary" in label.lower() and "flood" in stem(label):
        return True
    cset = {str(c).lower() for c in gdf.columns}
    if {"scenario", "reach", "flow_rate"}.issubset(cset):
        return True
    return False


def flood_odds_from_string(text: str) -> int | None:
    if not text or text == "nan":
        return None
    m = re.search(r"1\s*in\s*(\d+)", text, re.I)
    if m:
        return int(m.group(1))
    return None


_FLOOD_RAW_TO_ODDS: dict[int, int] = {
    12: 2,
    150: 50,
    1100: 100,
}


def flood_odds_from_label(label: str) -> int | None:
    """Decode annual flood probability (1-in-N) from Open Calgary layer ids.

    Filenames use a leading ``1`` prefix on multi-digit codes (e.g. ``110`` → 1-in-10,
    ``1200`` → 1-in-200). A few ids are special-cased (``12`` → 1-in-2).
    """
    s = stem(label)
    if "regulatoryfloodmap" in s:
        return None

    m = re.search(r"(\d+)floodmap", s, re.I)
    if m:
        raw = int(m.group(1))
        if raw in _FLOOD_RAW_TO_ODDS:
            return _FLOOD_RAW_TO_ODDS[raw]
        sraw = str(raw)
        if len(sraw) >= 3 and sraw.startswith("1"):
            return int(sraw[1:])
        return raw

    m = re.search(r"floodmap(\d+)chance", s, re.I)
    if m:
        return int(m.group(1))
    return None


def flood_display_name(label: str) -> str:
    """Sidebar / legend label for a flood layer."""
    s = stem(label)
    if "regulatoryfloodmap" in s or ("bylaw" in s and "flood" in s):
        return "Regulatory flood hazard (bylaw)"
    odds = flood_odds_from_label(label)
    if odds is not None:
        return f"1 in {odds} annual flood probability"
    # Annual exceedance % in filename (e.g. FloodMap10Chance).
    m = re.search(r"floodmap(\d+(?:\.\d+)?)chance", s, re.I)
    if m:
        pct = float(m.group(1))
        if pct >= 50:
            return "1 in 2 annual flood probability"
        est = max(2, round(100 / pct)) if pct > 0 else None
        if est:
            return f"1 in {est} annual flood probability ({pct:g}% per year)"
    tail = label.split("/")[-1].replace("_", " ")
    return tail[:70] + ("…" if len(tail) > 70 else "")


def flood_layer_sort_key(label: str) -> tuple[int, str]:
    odds = flood_odds_from_label(label)
    if odds is None:
        return (10_000_000, label)
    return (odds, label)


def layer_legend_name(label: str) -> str:
    s = stem(label)
    if "floodmap" in s or "regulatoryfloodmap" in s:
        name = flood_display_name(label)
        return name if len(name) <= 42 else name[:39] + "…"
    short = label.split("/")[-1]
    return short[:42] + ("…" if len(short) > 42 else "")


def wants_open_calgary_hover(label: str, census_key: str | None) -> bool:
    if census_key and label == census_key:
        return False
    return label.lower().startswith("open_calgary/")


def format_hover_tip(parts: list[str]) -> str:
    return "<br>".join(html.escape(p) for p in parts)


def hover_meta_columns(columns: list[str], label: str) -> list[str]:
    """Open Calgary tooltips omit GeoJSON-style fields whose names start with ``:``."""
    if not label.lower().startswith("open_calgary/"):
        return columns
    return [c for c in columns if not str(c).startswith(":")]


def flood_risk_fill_line(odds_n: int | None) -> tuple[str, str]:
    """Lower odds denominator (1 in N) -> more frequent -> warmer fill."""
    if odds_n is None:
        return "#cbd5e1", "#64748b"
    if odds_n <= 50:
        return "#fecaca", "#b91c1c"
    if odds_n <= 100:
        return "#fed7aa", "#c2410c"
    if odds_n <= 150:
        return "#fde68a", "#b45309"
    if odds_n <= 400:
        return "#d9f99d", "#4d7c0f"
    return "#bae6fd", "#0369a1"


def style_flood_layer(label: str, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Per-row colours from scenario text + layer; protected areas highlighted."""
    g = gdf.copy()
    g["_layer"] = label
    fallback_p = flood_odds_from_label(label)
    geom0 = g.geometry.iloc[0]
    gtype = geom0.geom_type if geom0 is not None else ""

    fills: list[str] = []
    lines: list[str] = []
    for _, row in g.iterrows():
        p: int | None = None
        if "scenario" in g.columns and pd.notna(row.get("scenario")):
            p = flood_odds_from_string(str(row["scenario"]))
        if p is None:
            p = fallback_p
        fill, line = flood_risk_fill_line(p)
        typ = str(row.get("type", "")).lower()
        if "protected" in typ:
            fill = "#fef9c3"
            line = "#ca8a04"
        fills.append(fill)
        lines.append(line)
    g["fill_c"] = fills
    g["line_c"] = lines

    skip = {"geometry", "fill_c", "line_c", "_layer", "_family", "stat_val"}
    meta_cols = hover_meta_columns([c for c in g.columns if c not in skip], label)

    def tip_row(row: pd.Series) -> str:
        parts: list[str] = []
        if "scenario" in row.index and pd.notna(row.get("scenario")):
            parts.append(str(row["scenario"]))
        for c in meta_cols:
            if c == "scenario":
                continue
            v = row.get(c)
            if pd.isna(v) or v is None:
                continue
            s = str(v)
            if len(s) > 220:
                s = s[:217] + "…"
            parts.append(f"{c}: {s}")
        return format_hover_tip(parts[:14])

    g["_tip"] = g.apply(tip_row, axis=1)
    if gtype in ("LineString", "MultiLineString", "LinearRing"):
        g["fill_c"] = g["line_c"]
    return g


def mute_flood_layer_colors(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    o = gdf.copy()
    o["fill_c"] = "#d4d4d8"
    o["line_c"] = "#a1a1aa"
    return o


def flood_odds_label(odds_n: int) -> str:
    """Human label as '1 in N' odds."""
    if odds_n == 2:
        return "1 in 2 probability"
    if odds_n == 100:
        return "1 in 100 probability"
    return f"1 in {odds_n}"


def resolve_data_dir() -> Path:
    """Prefer bundled ``demos/demo4/data``, else project ``processed_data``."""
    bundled = HERE / "data"
    if bundled.is_dir():
        try:
            next(bundled.iterdir())
            return bundled
        except StopIteration:
            pass
    root = PROCESSED_DATA_ROOT
    if root.is_dir():
        try:
            next(root.iterdir())
            return root
        except StopIteration:
            pass
    return bundled if bundled.is_dir() else root


DATA_DIR = resolve_data_dir()
DATA_ROOT_LABEL = str(DATA_DIR)


def stem(label: str) -> str:
    return label.split("/")[-1].lower()


def find_parquet_files(root: Path) -> list[Path]:
    """Discover allowlisted feature parquet files under processed_data."""
    files: list[Path] = []
    if not root.is_dir():
        return files
    for source_dir in list_subdirectories(root):
        for snapshot_dir in list_subdirectories(source_dir):
            features_dir = snapshot_dir / "features"
            if not features_dir.exists():
                continue
            for child in features_dir.iterdir():
                if child.is_dir() and child.name in GEOMETRY_BUCKET_NAMES:
                    candidates = sorted(child.glob("*.parquet"))
                elif child.suffix == ".parquet":
                    candidates = [child]
                else:
                    continue
                for path in candidates:
                    if is_demo_dataset(path):
                        files.append(path)
    return files


def fix_lonlat_metadata(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalize CRS to `PLOT_CRS` while handling common metadata issues."""
    if gdf.empty:
        return gdf
    try:
        b = gdf.total_bounds
    except Exception:
        return gdf
    span_x = b[2] - b[0]
    span_y = b[3] - b[1]
    looks_geo = (
        -180 <= b[0] <= 180
        and -90 <= b[1] <= 90
        and span_x < 10
        and span_y < 10
        and abs(b[0]) < 400
        and abs(b[2]) < 400
    )
    if looks_geo and gdf.crs is not None and not gdf.crs.is_geographic:
        return gdf.set_crs(PLOT_CRS, allow_override=True)
    if gdf.crs is None:
        return gdf.set_crs(PLOT_CRS)
    if gdf.crs.is_geographic:
        return gdf.to_crs(PLOT_CRS)
    return gdf.to_crs(PLOT_CRS)


def downtown_clip_mask() -> gpd.GeoDataFrame:
    """A square clip box centered on downtown Calgary.

    The demo keeps plotting fast by clipping all layers to this window.
    """
    half_m = VIEW_HALF_KM * 1000.0
    center = gpd.GeoDataFrame(geometry=gpd.points_from_xy([DOWNTOWN_LON], [DOWNTOWN_LAT]), crs=PLOT_CRS).to_crs(CLIP_WORK)
    cx, cy = float(center.geometry.iloc[0].x), float(center.geometry.iloc[0].y)
    b = box(cx - half_m, cy - half_m, cx + half_m, cy + half_m)
    return gpd.GeoDataFrame(geometry=[b], crs=CLIP_WORK).to_crs(PLOT_CRS)


def clip_layers(layers: dict[str, gpd.GeoDataFrame], mask: gpd.GeoDataFrame) -> dict[str, gpd.GeoDataFrame]:
    """Clip all layers to the mask, dropping empty/failed clips."""
    out: dict[str, gpd.GeoDataFrame] = {}
    for label, gdf in layers.items():
        if gdf is None or gdf.empty:
            continue
        g = fix_lonlat_metadata(gdf)
        try:
            c = g.clip(mask)
        except Exception as exc:
            print(f"clip skip {label}: {exc}")
            continue
        if c.empty:
            continue
        out[label] = c
    return out


def layer_label(path: Path) -> str:
    """Stable key: ``source/snapshot/layer`` relative to processed_data."""
    rel = path.relative_to(DATA_DIR)
    parts = rel.parts
    source = parts[0] if parts else "unknown"
    snapshot = parts[1] if len(parts) > 1 and parts[1].isdigit() else ""
    stem_name = path.stem.replace("_features", "")
    if snapshot:
        return f"{source}/{snapshot}/{stem_name}"
    return f"{source}/{stem_name}"


def load_all_parquet(paths: list[Path]) -> dict[str, gpd.GeoDataFrame]:
    """Read all parquet layers into memory keyed by a short label."""
    layers: dict[str, gpd.GeoDataFrame] = {}
    for path in paths:
        label = layer_label(path)
        try:
            gdf = gpd.read_parquet(path)
            layers[label] = fix_lonlat_metadata(gdf)
        except Exception as exc:
            print(f"skip {label}: {exc}")
    return layers


def is_census_label(label: str, gdf: gpd.GeoDataFrame) -> bool:
    """Heuristic for identifying the StatCan population/dwelling dataset."""
    if "statcan" in label.lower():
        return True
    if "98100015" in stem(label) or "populationanddwelling" in stem(label):
        return True
    return "DAUID" in gdf.columns


def partition_layer_keys(clipped: dict[str, gpd.GeoDataFrame]) -> dict[str, list[str] | str | None]:
    """Split clipped layers into sidebar groups."""
    census_layers = [k for k, g in clipped.items() if is_census_label(k, g)]
    census_key = census_layers[0] if census_layers else None

    flood_keys = [k for k, g in clipped.items() if is_flood_data(k, g)]
    solar_keys = [k for k in clipped if "solarproductionsites" in stem(k)]
    load_keys: list[str] = []
    hosting_keys: list[str] = []
    open_calgary_keys: list[str] = []

    for k in clipped:
        if k == census_key or k in flood_keys or k in solar_keys:
            continue
        s = stem(k)
        low = k.lower()
        if "enmax" in low:
            if "loadcapacity" in s and "hosting" not in s:
                load_keys.append(k)
            elif "hosting" in s:
                hosting_keys.append(k)
        elif low.startswith("open_calgary/"):
            open_calgary_keys.append(k)

    return {
        "census_key": census_key,
        "flood": flood_keys,
        "solar": solar_keys,
        "load": load_keys,
        "hosting": hosting_keys,
        "open_calgary": open_calgary_keys,
    }


def _hash_color(key: str, palette: list[str]) -> str:
    return palette[hash(key) % len(palette)]


def capacity_line_color(raw: object) -> str:
    """Map ENMAX discrete capacity-bin strings to hex.

    This supports both:
    - load capacity (KVA) bins
    - hosting capacity (kW) bins

    The source data often varies formatting (commas, whitespace, units), so we
    normalize to robustly hit the same legend colours.
    """
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return "#64748b"
    s = str(raw).strip().replace("\xa0", " ").replace("  ", " ")
    if s in CAPACITY_COLORS:
        return CAPACITY_COLORS[s]

    slow = s.lower()
    for key, col in CAPACITY_COLORS.items():
        if key.lower() == slow:
            return col

    # Normalize common formatting variants: commas, units, dashes, etc.
    norm = re.sub(r"[^0-9<>=-]+", "", slow.replace(",", ""))

    # KVA load-capacity bins
    if "<999" in norm or "<=999" in norm:
        return CAPACITY_COLORS["< 999 KVA"]
    if "1000-2999" in norm:
        return CAPACITY_COLORS["1,000 - 2,999 KVA"]
    if "3000-4999" in norm:
        return CAPACITY_COLORS["3,000 - 4,999 KVA"]
    if "5000-6999" in norm:
        return CAPACITY_COLORS["5,000 - 6,999 KVA"]
    if "7000-8999" in norm:
        return CAPACITY_COLORS["7,000 - 8,999 KVA"]
    if "9000-10999" in norm:
        return CAPACITY_COLORS["9,000 - 10,999 KVA"]
    if "11000-12999" in norm:
        return CAPACITY_COLORS["11,000 - 12,999 KVA"]

    # If the dataset uses the old wider bins, map them into the closest legend bins.
    if "999-4999" in norm:
        return CAPACITY_COLORS["3,000 - 4,999 KVA"]
    if "5000-9999" in norm:
        return CAPACITY_COLORS["7,000 - 8,999 KVA"]
    if "10000-49999" in norm:
        return CAPACITY_COLORS["11,000 - 12,999 KVA"]

    # kW hosting-capacity bins
    if "<199" in norm or "<=199" in norm:
        return CAPACITY_COLORS["< 199 kW"]
    if "200-499" in norm:
        return CAPACITY_COLORS["200 kW - 499 kW"]
    if "500-999" in norm:
        return CAPACITY_COLORS["500 kW - 999 kW"]
    if "1000-4999" in norm:
        return CAPACITY_COLORS["1,000 kW - 4,999 kW"]
    if "5000-9999" in norm:
        return CAPACITY_COLORS["5,000 kW - 9,999 kW"]
    if "10000" in norm and "-" not in norm:
        return CAPACITY_COLORS[">10,000 kW"]

    return "#64748b"


def style_vectors(label: str, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Assign `fill_c`, `line_c`, and tooltip fields for non-choropleth layers."""
    if is_flood_data(label, gdf):
        return style_flood_layer(label, gdf)
    g = gdf.copy()
    family = label.split("/")[0].lower()
    g["_layer"] = label
    geom0 = g.geometry.iloc[0]
    gtype = geom0.geom_type if geom0 is not None else ""
    base = _hash_color(label, Category20[20])

    if "Capacity_Available" in g.columns:
        g["line_c"] = g["Capacity_Available"].map(capacity_line_color)
        g["fill_c"] = g["line_c"]
    elif "Phase_Designation" in g.columns:
        g["line_c"] = g["Phase_Designation"].astype(str).map(lambda x: PHASE_COLORS.get(str(x).strip(), base))
        g["fill_c"] = g["line_c"]
    elif "scenario" in g.columns and not is_flood_data(label, g):
        scenarios = sorted(g["scenario"].astype(str).unique().tolist())
        scen_palette = {s: Category20[20][i % 20] for i, s in enumerate(scenarios)}
        g["fill_c"] = g["scenario"].astype(str).map(lambda x: scen_palette.get(x, base))
        g["line_c"] = "#475569"
    elif "type" in g.columns and family == "open_calgary":
        types = sorted(g["type"].astype(str).unique().tolist())
        tpal = {t: Category20[20][i % 20] for i, t in enumerate(types)}
        g["fill_c"] = g["type"].astype(str).map(lambda x: tpal.get(x, base))
        g["line_c"] = "#475569"
    elif "Project_Colour_Type" in g.columns:
        g["fill_c"] = g["Project_Colour_Type"].astype(str).map(lambda x: PROJECT_COLORS.get(x, base))
        g["line_c"] = "#334155"
    else:
        g["line_c"] = base
        g["fill_c"] = base

    skip = {"geometry", "fill_c", "line_c", "_layer", "_family", "stat_val"}
    meta_cols = hover_meta_columns([c for c in g.columns if c not in skip], label)

    def tip_row(row: pd.Series) -> str:
        parts: list[str] = []
        for c in meta_cols:
            v = row.get(c)
            if pd.isna(v) or v is None:
                continue
            s = str(v)
            if len(s) > 240:
                s = s[:237] + "…"
            parts.append(f"{c}: {s}")
        return format_hover_tip(parts[:16])

    g["_tip"] = g.apply(tip_row, axis=1)
    if gtype in ("LineString", "MultiLineString", "LinearRing"):
        g["fill_c"] = g["line_c"]
    return g


def style_statcan_numeric(gdf: gpd.GeoDataFrame, label: str, value_col: str, mapper: LinearColorMapper) -> gpd.GeoDataFrame:
    g = gdf.copy()
    g["_layer"] = label
    vals = pd.to_numeric(g[value_col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)
    g["stat_val"] = vals.astype(float)
    vmin, vmax = float(vals.min()), float(vals.max())
    if vmin == vmax:
        vmax = vmin + 1e-6 if vmin == 0 else vmin * 1.0001
    mapper.low = vmin
    mapper.high = vmax
    g["line_c"] = "#475569"
    skip = {"geometry", "_layer", "stat_val", "line_c"}
    meta_cols = [c for c in g.columns if c not in skip]

    def tip_row(row: pd.Series) -> str:
        parts = [f"{html.escape(value_col)}: {row.get('stat_val', '')}"]
        for c in meta_cols:
            if c == value_col:
                continue
            v = row.get(c)
            if pd.isna(v):
                continue
            parts.append(f"{c}: {str(v)[:200]}")
        return format_hover_tip(parts[:18])

    g["_tip"] = g.apply(tip_row, axis=1)
    return g


def simplify_mercator(gdf: gpd.GeoDataFrame, tol_m: float) -> gpd.GeoDataFrame:
    if gdf.empty:
        return gdf
    kind = gdf.geometry.iloc[0].geom_type
    if kind in ("Point", "MultiPoint"):
        return gdf.to_crs(WEB_MERCATOR)
    gm = gdf.to_crs(WEB_MERCATOR)
    gm = gm.set_geometry(gm.geometry.simplify(tol_m, preserve_topology=True))
    gm = gm[gm.geometry.notna() & ~gm.geometry.is_empty]
    return gm


def geojson_plain(gdf: gpd.GeoDataFrame) -> str:
    cols = [c for c in ("geometry", "fill_c", "line_c", "_layer", "_tip") if c in gdf.columns]
    slim = gdf[cols].copy()
    for c in ("fill_c", "line_c", "_layer", "_tip"):
        if c in slim.columns:
            slim[c] = slim[c].astype(str)
    return slim.to_json()


def geojson_choropleth(gdf: gpd.GeoDataFrame) -> str:
    slim = gdf[["geometry", "stat_val", "line_c", "_layer", "_tip"]].copy()
    slim["stat_val"] = slim["stat_val"].astype(float)
    for c in ("line_c", "_layer", "_tip"):
        slim[c] = slim[c].astype(str)
    return slim.to_json()


def square_bounds(gdfs: list[gpd.GeoDataFrame], pad: float = 0.1) -> tuple[float, float, float, float]:
    if not gdfs:
        b = gpd.GeoDataFrame(
            geometry=[box(DOWNTOWN_LON - 0.05, DOWNTOWN_LAT - 0.05, DOWNTOWN_LON + 0.05, DOWNTOWN_LAT + 0.05)],
            crs=PLOT_CRS,
        ).to_crs(WEB_MERCATOR).total_bounds
    else:
        minx = miny = float("inf")
        maxx = maxy = float("-inf")
        for gdf in gdfs:
            bb = gdf.total_bounds
            minx, miny = min(minx, bb[0]), min(miny, bb[1])
            maxx, maxy = max(maxx, bb[2]), max(maxy, bb[3])
        b = (minx, miny, maxx, maxy)
    minx, miny, maxx, maxy = float(b[0]), float(b[1]), float(b[2]), float(b[3])
    cx, cy = (minx + maxx) / 2, (miny + maxy) / 2
    half = max(maxx - minx, maxy - miny) / 2 * (1 + pad)
    return cx - half, cy - half, cx + half, cy + half


def order_stack(
    items: list[tuple[str, gpd.GeoDataFrame]],
    census_key: str | None,
    choropleth_bottom: bool,
) -> list[tuple[str, gpd.GeoDataFrame]]:
    """Bottom → top: choropleth (census) poly, other polys, lines, points."""
    choro_poly: list[tuple[str, gpd.GeoDataFrame]] = []
    poly: list[tuple[str, gpd.GeoDataFrame]] = []
    lines: list[tuple[str, gpd.GeoDataFrame]] = []
    pts: list[tuple[str, gpd.GeoDataFrame]] = []
    for label, gdf in items:
        gt = gdf.geometry.iloc[0].geom_type
        if gt in ("Polygon", "MultiPolygon"):
            if choropleth_bottom and census_key and label == census_key:
                choro_poly.append((label, gdf))
            else:
                poly.append((label, gdf))
        elif gt in ("LineString", "MultiLineString", "LinearRing"):
            lines.append((label, gdf))
        elif gt in ("Point", "MultiPoint"):
            pts.append((label, gdf))
    choro_poly.sort(key=lambda x: x[0])
    poly.sort(key=lambda x: x[0])
    lines.sort(key=lambda x: x[0])
    pts.sort(key=lambda x: x[0])
    return choro_poly + poly + lines + pts


def prepare_layer_styled(
    label: str,
    key_census: str | None,
    value_col: str | None,
    mapper: LinearColorMapper | None,
    clipped: dict[str, gpd.GeoDataFrame],
    *,
    flood_highlight_label: str | None = None,
) -> gpd.GeoDataFrame | None:
    """Style a layer, optionally applying choropleth and flood muting, then simplify."""
    raw = clipped.get(label)
    if raw is None or raw.empty:
        return None
    if key_census and label == key_census and value_col and mapper:
        g = style_statcan_numeric(raw, label, value_col, mapper)
    else:
        g = style_vectors(label, raw)
    if flood_highlight_label is not None and is_flood_data(label, raw):
        if label != flood_highlight_label:
            g = mute_flood_layer_colors(g)
    tol = 14.0 if len(g) > 800 else 9.0
    return simplify_mercator(g, tol_m=tol)


def draw_figure_for_stack(
    stacked: list[tuple[str, gpd.GeoDataFrame]],
    census_key: str | None,
    mapper: LinearColorMapper | None,
    title: str,
    *,
    flood_highlight_label: str | None = None,
    muted_flood_poly_alpha: float = 0.3,
    enable_hover: bool = False,
    hover_value_label: str = "Value",
) -> tuple[figure, GeoJSONDataSource | None]:
    """Render the stacked layers into a single Bokeh figure.

    Notes:
    - Census choropleth and Open Calgary layers get hover tools (attribute details).
    - ENMAX lines are slightly thinner + more transparent for legibility.
    - "river" layers use a dashed line pattern to stand out without a colour ramp.
    """
    census_src: GeoJSONDataSource | None = None
    census_renderer = None
    open_calgary_hover_renderers: list = []

    bounds_list = [g for _, g in stacked]
    xmin, ymin, xmax, ymax = square_bounds(bounds_list)
    p = figure(
        width=940,
        height=760,
        title=title,
        x_range=(xmin, xmax),
        y_range=(ymin, ymax),
        x_axis_type="mercator",
        y_axis_type="mercator",
        match_aspect=True,
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
        background_fill_color="#f6f8fa",
        border_fill_color="#ffffff",
        outline_line_color="#d0d7de",
    )
    p.title.text_color = "#24292f"
    p.xaxis.visible = False
    p.yaxis.visible = False
    p.add_tile("CartoDB Positron", retina=True)
    for t in p.toolbar.tools:
        if isinstance(t, WheelZoomTool):
            t.zoom_on_axis = False

    for label, gdf in stacked:
        gt = gdf.geometry.iloc[0].geom_type
        short = layer_legend_name(label)
        is_choro = bool(census_key and label == census_key and mapper is not None and "stat_val" in gdf.columns)
        is_overlay_poly = bool(census_key and label != census_key and gt in ("Polygon", "MultiPolygon"))
        is_muted_flood = (
            flood_highlight_label is not None
            and gt in ("Polygon", "MultiPolygon")
            and not is_choro
            and is_flood_data(label, gdf)
            and label != flood_highlight_label
        )

        if gt in ("Polygon", "MultiPolygon"):
            if is_choro:
                src = GeoJSONDataSource(geojson=geojson_choropleth(gdf))
                census_src = src
                p.patches(
                    xs="xs",
                    ys="ys",
                    source=src,
                    fill_color=dict(field="stat_val", transform=mapper),
                    line_color="line_c",
                    fill_alpha=CHOROPLETH_FILL_ALPHA,
                    line_width=0.6,
                    legend_label=short,
                )
                census_renderer = p.renderers[-1]
            else:
                src = GeoJSONDataSource(geojson=geojson_plain(gdf))
                if is_muted_flood:
                    fa = muted_flood_poly_alpha
                    lw = 0.55
                elif is_overlay_poly:
                    fa = OVERLAY_POLY_FILL_ALPHA
                    lw = 1.0
                else:
                    fa = OTHER_POLY_FILL_ALPHA
                    lw = 1.0
                p.patches(
                    xs="xs",
                    ys="ys",
                    source=src,
                    fill_color="fill_c",
                    line_color="line_c",
                    fill_alpha=fa,
                    line_width=lw,
                    legend_label=short,
                )
        elif gt in ("LineString", "MultiLineString", "LinearRing"):
            src = GeoJSONDataSource(geojson=geojson_plain(gdf))
            label_l = label.lower()
            is_enmax = "enmax" in label_l
            is_river = "river" in label_l
            # ENMAX lines: slightly more transparent so underlying context shows through.
            line_alpha = 0.98 - 0.005 if is_enmax else 0.98
            line_width = 3.2 - 0.8 if is_enmax else 3.2
            p.multi_line(
                xs="xs",
                ys="ys",
                source=src,
                line_color="line_c",
                line_width=line_width,
                line_alpha=line_alpha,
                line_dash="4 2" if is_river else "solid",
                legend_label=short,
            )
        else:
            src = GeoJSONDataSource(geojson=geojson_plain(gdf))
            p.scatter(
                x="x",
                y="y",
                source=src,
                size=12,
                fill_color="fill_c",
                line_color="#1e293b",
                line_width=1.1,
                alpha=0.96,
                legend_label=short,
            )

        if (
            "_tip" in gdf.columns
            and wants_open_calgary_hover(label, census_key)
            and p.renderers
        ):
            open_calgary_hover_renderers.append(p.renderers[-1])

    if enable_hover and census_renderer is not None:
        p.add_tools(
            HoverTool(
                tooltips=[(hover_value_label, "@stat_val{0,0.##}")],
                renderers=[census_renderer],
            )
        )

    if open_calgary_hover_renderers:
        p.add_tools(
            HoverTool(
                tooltips=[("Details", "@_tip{safe}")],
                renderers=open_calgary_hover_renderers,
                attachment="vertical",
            )
        )

    p.legend.click_policy = "hide"
    p.legend.label_text_font_size = "9pt"
    p.legend.background_fill_color = "#ffffff"
    p.legend.border_line_color = "#d0d7de"
    p.legend.label_text_color = "#24292f"

    return p, census_src


def build_root():
    """Build the Bokeh document root for demo4.

    This function:
    - loads & clips all layers
    - creates the sidebar checkbox controls
    - renders a single map figure from the selected layers
    - keeps legends and zoom/pan state in sync across re-renders
    """
    mask = downtown_clip_mask()
    paths = find_parquet_files(DATA_DIR)
    raw = load_all_parquet(paths)
    clipped = clip_layers(raw, mask)
    header = Div(
        text=(
            "<div style='font-family:system-ui,sans-serif;padding:8px 4px 12px 4px;color:#24292f;'>"
            "<b style='color:#0969da;'>demo4</b> · ~" + f"{VIEW_HALF_KM * 2:.0f}" + " km window · "
            "<code style='background:#f6f8fa;padding:2px 6px;border-radius:4px;'>"
            + html.escape(str(DATA_DIR))
            + "</code>"
            " <span style='color:#57606a;font-size:12px;'>(DATA_ROOT: "
            + html.escape(DATA_ROOT_LABEL)
            + ")</span></div>"
        ),
        width=980,
    )

    if not clipped:
        return column(header, Div(text="<span style='color:#cf222e'>No features clipped.</span>"))

    def short_label(key: str) -> str:
        # Make labels readable in a narrow sidebar: show the file stem, wrap.
        tail = key.split("/")[-1]
        tail = tail.replace("_", " ")
        if len(tail) > 70:
            tail = tail[:67] + "…"
        return tail

    parts = partition_layer_keys(clipped)
    census_key = parts["census_key"]
    flood_keys = list(parts["flood"])
    solar_keys = sorted(parts["solar"])
    load_keys = list(parts["load"])
    hosting_keys = list(parts["hosting"])
    open_calgary_keys = sorted(parts["open_calgary"])

    census_labels = ["Census · Population & dwellings"] if census_key else []

    # Default: July 2024 ENMAX load layers selected (both single + two/three phase if present).
    default_load_keys: set[str] = set()
    for k in load_keys:
        s_low = stem(k).lower()
        if "july2024" in s_low or ("july" in s_low and "2024" in s_low):
            default_load_keys.add(k)

    load_default_active: list[int] = [i for i, k in enumerate(load_keys) if k in default_load_keys]

    def checkbox_height(n: int, *, tall: bool = False) -> int:
        """Compact list height that still allows scrolling when the list grows."""
        cap = 320 if tall else 140
        return max(70, min(cap, 22 * n + 14))

    flood_keys_sorted = sorted(flood_keys, key=flood_layer_sort_key)
    flood_checkbox = CheckboxGroup(
        labels=[flood_display_name(k) for k in flood_keys_sorted],
        active=[],
        width=280,
        height=checkbox_height(len(flood_keys)),
        styles={"overflow-y": "auto", "overflow-x": "auto", "white-space": "normal"},
    )

    load_checkbox = CheckboxGroup(
        labels=[short_label(k) for k in load_keys],
        active=load_default_active,
        width=280,
        height=checkbox_height(len(load_keys)),
        styles={"overflow-y": "auto", "overflow-x": "auto", "white-space": "normal"},
    )

    hosting_checkbox = CheckboxGroup(
        labels=[short_label(k) for k in hosting_keys],
        active=[],
        width=280,
        height=checkbox_height(len(hosting_keys)),
        styles={"overflow-y": "auto", "overflow-x": "auto", "white-space": "normal"},
    )

    solar_checkbox = CheckboxGroup(
        labels=[short_label(k) for k in solar_keys],
        active=[],
        width=280,
        height=checkbox_height(len(solar_keys), tall=False),
        styles={"overflow-y": "auto", "overflow-x": "auto", "white-space": "normal"},
    )

    open_calgary_checkbox = CheckboxGroup(
        labels=[short_label(k) for k in open_calgary_keys],
        active=[],
        width=280,
        height=checkbox_height(len(open_calgary_keys), tall=True),
        styles={"overflow-y": "auto", "overflow-x": "auto", "white-space": "normal"},
    )

    census_checkbox = CheckboxGroup(
        labels=census_labels,
        active=[],
        width=280,
        height=90,
        styles={"overflow-y": "auto", "overflow-x": "auto", "white-space": "normal"},
    )

    stat_select: Select | None = None
    col_by_friendly: dict[str, str] = {}
    stat_pairs: list[tuple[str, str]] = []
    if census_key:
        stat_pairs = [(c, s) for c, s in STATCAN_COLUMNS if c in clipped[census_key].columns]
        if stat_pairs:
            col_by_friendly = {s: c for c, s in stat_pairs}
            friendly = [s for _, s in stat_pairs]
            stat_select = Select(
                title="Choropleth column",
                value=friendly[0],
                options=friendly,
                width=280,
            )

    mapper = LinearColorMapper(palette=Plasma256, low=0, high=1)

    plot_slot = column(width=960)

    # Legends live under the map, not in the left sidebar. They update dynamically
    # based on selected ENMAX layers.
    load_legend_div = Div(text="", width=460)
    hosting_legend_div = Div(text="", width=460)
    legends_row = row(load_legend_div, hosting_legend_div, width=940)

    def rerender() -> None:
        """Rebuild the map figure based on current selections.

        Important behavior:
        - preserves the current zoom/pan by reusing the previous plot ranges
        - updates the ENMAX discrete legends under the map
        - uses the census choropleth mapper only when census is enabled
        """
        # Preserve current zoom/pan by reusing the previous plot ranges.
        prev_x = prev_y = None
        if plot_slot.children and hasattr(plot_slot.children[0], "x_range") and hasattr(plot_slot.children[0], "y_range"):
            prev_fig = plot_slot.children[0]
            try:
                prev_x = (float(prev_fig.x_range.start), float(prev_fig.x_range.end))
                prev_y = (float(prev_fig.y_range.start), float(prev_fig.y_range.end))
            except Exception:
                prev_x = prev_y = None

        selected_flood = [flood_keys_sorted[i] for i in (flood_checkbox.active or [])]
        selected_load = [load_keys[i] for i in (load_checkbox.active or [])]
        selected_hosting = [hosting_keys[i] for i in (hosting_checkbox.active or [])]
        selected_solar = [solar_keys[i] for i in (solar_checkbox.active or [])]
        selected_open_calgary = [open_calgary_keys[i] for i in (open_calgary_checkbox.active or [])]
        census_on = len(census_checkbox.active or []) > 0 and census_key is not None

        def header_from_keys(keys: list[str], kind: str) -> str:
            """Pick a readable legend title with a best-effort month/date suffix."""
            # Best-effort label based on filename.
            s_all = " ".join([stem(k).lower() for k in keys])
            if "july2024" in s_all or ("july" in s_all and "2024" in s_all):
                return f"{kind} (July 2024)"
            if "december2025" in s_all or ("december" in s_all and "2025" in s_all):
                return f"{kind} (December 2025)"
            if "february2025" in s_all or ("february" in s_all and "2025" in s_all):
                return f"{kind} (February 2025)"
            return kind

        def load_legend_html(keys: list[str]) -> str:
            h = header_from_keys(keys, "Estimated Remaining Load Capacity")
            return (
                f"<div style='font-size:12px;color:#24292f;margin:6px 0 8px 0;'><b>{h}</b></div>"
                "<div style='font-size:11px;color:#57606a;line-height:1.4;'>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#6b87b8;margin-right:6px;'></span>"
                "11,000 - 12,999 KVA (Lowest likelihood for system upgrades)</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#4aa8c8;margin-right:6px;'></span>"
                "9,000 - 10,999 KVA</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#34bfa3;margin-right:6px;'></span>"
                "7,000 - 8,999 KVA</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#7ad151;margin-right:6px;'></span>"
                "5,000 - 6,999 KVA</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#dce319;margin-right:6px;'></span>"
                "3,000 - 4,999 KVA</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#fdae61;margin-right:6px;'></span>"
                "1,000 - 2,999 KVA</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#d73027;margin-right:6px;'></span>"
                "&lt; 999 KVA (Highest likelihood for system upgrades)</div>"
                "</div>"
            )

        def hosting_legend_html(keys: list[str]) -> str:
            h = header_from_keys(keys, "Estimated Remaining Hosting Capacity")
            return (
                f"<div style='font-size:12px;color:#24292f;margin:6px 0 8px 0;'><b>{h}</b></div>"
                "<div style='font-size:11px;color:#57606a;line-height:1.4;'>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#4caf50;margin-right:6px;'></span>"
                "&gt;10,000 kW (Lowest likelihood for system upgrades)</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#42a5f5;margin-right:6px;'></span>"
                "5,000 kW - 9,999 kW</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#ab47bc;margin-right:6px;'></span>"
                "1,000 kW - 4,999 kW</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#ffeb3b;margin-right:6px;'></span>"
                "500 kW - 999 kW</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#ffb74d;margin-right:6px;'></span>"
                "200 kW - 499 kW</div>"
                "<div><span style='display:inline-block;width:14px;height:8px;background:#e57373;margin-right:6px;'></span>"
                "&lt; 199 kW (Highest likelihood for system upgrades)</div>"
                "</div>"
            )

        load_legend_div.text = load_legend_html(selected_load) if selected_load else ""
        hosting_legend_div.text = hosting_legend_html(selected_hosting) if selected_hosting else ""

        selected_non_census = (
            selected_flood
            + selected_solar
            + selected_load
            + selected_hosting
            + selected_open_calgary
        )

        vc: str | None = None
        key_census: str | None = census_key if census_on else None
        if census_on and stat_select is not None:
            lab = stat_select.value
            vc = col_by_friendly.get(lab) or next((c for c, s in STATCAN_COLUMNS if s == lab), None)

        prepared: list[tuple[str, gpd.GeoDataFrame]] = []
        for lb in selected_non_census:
            g = prepare_layer_styled(
                lb,
                key_census,
                vc,
                mapper if vc else None,
                clipped,
                flood_highlight_label=None,
            )
            if g is not None and not g.empty:
                prepared.append((lb, g))

        # Add census choropleth layer if selected (rendered as a polygon choropleth).
        if key_census is not None:
            g_c = prepare_layer_styled(key_census, key_census, vc, mapper if vc else None, clipped, flood_highlight_label=None)
            if g_c is not None and not g_c.empty:
                prepared.append((key_census, g_c))

        if not prepared:
            fig_empty = figure(
                width=940,
                height=760,
                title="No layers selected",
                x_axis_type="mercator",
                y_axis_type="mercator",
                match_aspect=True,
                background_fill_color="#f6f8fa",
            )
            fig_empty.add_tile("CartoDB Positron", retina=True)
            if prev_x is not None and prev_y is not None:
                fig_empty.x_range.start, fig_empty.x_range.end = prev_x
                fig_empty.y_range.start, fig_empty.y_range.end = prev_y
            plot_slot.children = [fig_empty]
            return

        stacked = order_stack(prepared, key_census, choropleth_bottom=bool(key_census))
        fig, _csrc = draw_figure_for_stack(
            stacked,
            key_census,
            mapper if (key_census is not None and vc) else None,
            title=f"demo4 · {len(prepared)} layer(s)",
            flood_highlight_label=None,
            enable_hover=(key_census is not None),
            hover_value_label=(stat_select.value if stat_select is not None else "Value"),
        )

        if key_census is not None and vc is not None:
            fig.add_layout(
                ColorBar(
                    color_mapper=mapper,
                    width=14,
                    margin=10,
                    title="Value",
                    title_text_color="#57606a",
                    major_label_text_color="#57606a",
                    background_fill_color="#ffffff",
                    border_line_color="#d0d7de",
                ),
                "right",
            )

        if prev_x is not None and prev_y is not None:
            fig.x_range.start, fig.x_range.end = prev_x
            fig.y_range.start, fig.y_range.end = prev_y
        plot_slot.children = [fig]

    # Wire UI events.
    flood_checkbox.on_change("active", lambda _a, _o, _n: rerender())
    load_checkbox.on_change("active", lambda _a, _o, _n: rerender())
    hosting_checkbox.on_change("active", lambda _a, _o, _n: rerender())
    solar_checkbox.on_change("active", lambda _a, _o, _n: rerender())
    open_calgary_checkbox.on_change("active", lambda _a, _o, _n: rerender())
    census_checkbox.on_change("active", lambda _a, _o, _n: rerender())
    if stat_select is not None:
        stat_select.on_change("value", lambda _a, _o, _n: rerender())

    # Initial render.
    rerender()

    sidebar_sections: list = [
        Div(text="<div style='font-weight:600;color:#0969da;margin-bottom:8px;'>Layers to show</div>", width=280),
        Div(text="<div style='font-size:12px;color:#24292f;margin:10px 0 6px 0;'><b>Flood</b></div>", width=280),
        flood_checkbox,
        Div(text="<div style='font-size:12px;color:#24292f;margin:10px 0 6px 0;'><b>Load capacity (ENMAX)</b></div>", width=280),
        load_checkbox,
        Div(text="<div style='font-size:12px;color:#24292f;margin:10px 0 6px 0;'><b>Hosting capacity (ENMAX)</b></div>", width=280),
        hosting_checkbox,
    ]
    if solar_keys:
        sidebar_sections.extend(
            [
                Div(text="<div style='font-size:12px;color:#24292f;margin:10px 0 6px 0;'><b>Solar</b></div>", width=280),
                solar_checkbox,
            ]
        )
    if open_calgary_keys:
        sidebar_sections.extend(
            [
                Div(
                    text=(
                        "<div style='font-size:12px;color:#24292f;margin:10px 0 6px 0;'>"
                        f"<b>Open Calgary</b> <span style='color:#57606a;'>({len(open_calgary_keys)})</span></div>"
                    ),
                    width=280,
                ),
                open_calgary_checkbox,
            ]
        )
    sidebar_sections.extend(
        [
            Div(text="<div style='font-size:12px;color:#24292f;margin:10px 0 6px 0;'><b>Census choropleth</b></div>", width=280),
            census_checkbox,
            stat_select if stat_select is not None else Div(text="", width=280),
        ]
    )

    sidebar = column(*sidebar_sections, width=300)

    right_side = column(plot_slot, legends_row, width=960)
    body = row(sidebar, right_side, sizing_mode="fixed")
    return column(header, body)


doc = curdoc()
doc.add_root(build_root())
doc.title = "demo4 · layer checkboxes"
