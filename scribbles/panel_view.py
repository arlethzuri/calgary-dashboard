import panel as pn
import hvplot.pandas
import pandas as pd
from shapely.geometry import Polygon

import geopandas as gpd
import json

# Load the json data files
DATA_DIR = "/Users/arleth/Desktop/calgary-dashboard/data/0_raw/enmax"
with open(f"{DATA_DIR}/Feeder_Load_Capacity_Rev9_20251211/ENMAX Service Area_features.json") as f:
    service_area_data = json.load(f)

# Convert the json data to a geopandas dataframe
gdf = gpd.GeoDataFrame.from_features(service_area_data)
gdf.crs = "EPSG:4326"

# Load facility point data
with open(f"{DATA_DIR}/uqkc-h9wi.json") as f:
    facilities_data = json.load(f)

facilities_df = pd.DataFrame(
    [
        {
            "facility_name": feat.get("facility_name"),
            "facility_address": feat.get("facility_address"),
            "rated_capacity": (
                float(feat["rated_capacity"])
                if feat.get("rated_capacity") not in (None, "")
                else None
            ),
            "lon": feat["geom"]["coordinates"][0],
            "lat": feat["geom"]["coordinates"][1],
        }
        for feat in facilities_data
    ]
)

facilities_gdf = gpd.GeoDataFrame(
    facilities_df,
    geometry=gpd.points_from_xy(facilities_df.lon, facilities_df.lat),
    crs="EPSG:4326",
)

# Base polygons map (ENMAX service area)
polygons_plot = gdf.hvplot.polygons(
    color="gray",  # Use a constant color, so no color bar
    alpha=0.5,
    geo=False,
    tiles="CartoDark",
    title="ENMAX Service Area with Facilities",
    height=600,
    responsive=False,
).opts(
    xlabel="Longitude",
    ylabel="Latitude"
)

# Facility locations as points
facilities_plot = facilities_gdf.hvplot.points(
    x="lon",
    y="lat",
    geo=True,
    color="yellow",
    size=8,
    alpha=0.9,
    hover_cols=["facility_name", "facility_address", "rated_capacity"],
    legend=False,
    responsive=True
).opts(
    xlabel="Longitude",
    ylabel="Latitude"
)

# Overlay polygons and points
map_plot = polygons_plot * facilities_plot

# # Build the Panel Dashboard
# pn.extension(design="material", theme="dark")

dashboard = pn.template.FastListTemplate(
    title="Calgary Dashboard",
    sidebar=[
        pn.pane.Markdown("### 📊 Metrics"),
        # pn.widgets.StaticText(name="Total Features", value=len(gdf)),
        pn.widgets.StaticText(name="Total Facilities", value=len(facilities_gdf)),
    ],
    main=[map_plot],
    accent_base_color="#f0ad4e",  # 'Calgary Solar' Orange
)

dashboard.servable()