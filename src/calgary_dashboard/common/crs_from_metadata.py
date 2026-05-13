"""Infer CRS strings from vendor metadata for GeoParquet export.

Requires **pyproj** (installed with geopandas in normal environments).

ArcGIS / ENMAX layer metadata typically exposes ``spatialReference`` (wkid or WKT).
Open Calgary CKAN metadata often nests a human-readable projection under
``metadata.custom_fields["Geospatial Information"]["Map Projection"]``.

Returned values are strings suitable for ``GeoDataFrame.set_crs(...)`` from
``CRS.to_string()``. ``None`` means the caller should log and skip rather than
guess wrong coordinates.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

from pyproj import CRS


def infer_crs_from_arcgis_spatial_reference(sr: Optional[Dict[str, Any]]) -> Optional[str]:
    """Map ArcGIS ``spatialReference`` dict to a GeoPandas-compatible CRS string."""
    if not sr:
        return None

    # WKT2 if available
    wkt2_raw = sr.get("wkt2")
    if isinstance(wkt2_raw, str) and wkt2_raw.strip():
        try:
            return CRS.from_wkt(wkt2_raw.strip()).to_string()
        except Exception:
            pass

    # WKT otherwise
    wkt_raw = sr.get("wkt")
    if isinstance(wkt_raw, str) and wkt_raw.strip():
        try:
            return CRS.from_wkt(wkt_raw.strip()).to_string()
        except Exception:
            pass

    # use latestWkid, then wkid if unavailable
    wkid_for_epsg: Any = None
    if sr.get("latestWkid") is not None:
        wkid_for_epsg = sr.get("latestWkid")
    elif sr.get("wkid") is not None:
        wkid_for_epsg = sr.get("wkid")

    if wkid_for_epsg is None:
        return None
    try:
        code = int(wkid_for_epsg)
        return CRS.from_epsg(code).to_string()
    except (TypeError, ValueError):
        return None


def infer_crs_from_enmax_metadata(meta: Dict[str, Any]) -> Optional[str]:
    """CRS from ENMAX ``*_metadata.json`` (top-level ArcGIS service metadata)."""
    sr = meta.get("spatialReference")
    if isinstance(sr, dict):
        return infer_crs_from_arcgis_spatial_reference(sr)
    return None


def _open_calgary_map_projection_try_pieces(label: str) -> list[str]:
    """Try tokens split on whitespace and ``/``, then the full string.

    CKAN strings like ``EPSG:3857 / WGS 84`` or ``WGS 84 / UTM zone 11N`` should
    yield parseable fragments (e.g. ``EPSG:3857``) before falling back to the whole label.
    """
    s = label.strip()
    if not s:
        return []
    # Split on runs of spaces, tabs, newlines, or slashes — same idea as split() plus "/".
    raw_toks = [t for t in re.split(r"[\s/]+", s) if t]
    seen: set[str] = set()
    pieces: list[str] = []
    for tok in raw_toks:
        if tok not in seen:
            seen.add(tok)
            pieces.append(tok)
    if s not in seen:
        pieces.append(s)
    return pieces


def infer_crs_from_open_calgary_metadata(meta: Dict[str, Any]) -> Optional[str]:
    """CRS from Open Calgary ``*_metadata.json`` custom_fields geospatial block."""
    custom = (meta.get("metadata") or {}).get("custom_fields") or {}
    geo = custom.get("Geospatial Information") or custom.get("geospatial information") or {}
    if not isinstance(geo, dict):
        return None

    # Get 'Map Projection' field which contains string defining CRS
    proj = geo.get("Map Projection")
    if not isinstance(proj, str) or not proj.strip():
        return None

    # Break 'Map Projection' into substrings and try getting CRS
    for piece in _open_calgary_map_projection_try_pieces(proj):
        try:
            return CRS.from_user_input(piece).to_string()
        except Exception:
            continue
    return None
