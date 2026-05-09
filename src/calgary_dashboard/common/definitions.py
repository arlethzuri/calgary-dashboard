"""Shared constant definitions across collectors, cleaners, and KG modules."""

from __future__ import annotations

# Geometry field names observed in original datasources, including legacy schema keys.
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

# Tokens that we consider either node-like or measurement-like for use in
# ontology and knowledge graph modules.
NODE_LIKE_TOKENS = frozenset(
    {
        "station",
        "site",
        "segment",
        "building",
        "zone",
        "area",
        "limits",
        "quadrant",
    }
)
MEASUREMENT_LIKE_TOKENS = frozenset(
    {
        "measurement",
        "reading",
        "record",
        "usage",
        "consumption",
    }
)

