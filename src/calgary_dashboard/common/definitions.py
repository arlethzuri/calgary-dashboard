"""Shared constant definitions across collectors, cleaners, and KG modules."""

from __future__ import annotations

# Geometry field names observed in source payloads, including legacy schema keys.
GEOMETRY_FIELD_NAMES = frozenset(
    {
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "the_geom",
        "geometry",
        "geom",
    }
)

# Canonical geometry names used for feature directory bucketing.
GEOMETRY_BUCKET_NAMES = frozenset(
    {
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
    }
)

