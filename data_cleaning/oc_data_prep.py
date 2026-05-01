### For Open Calgary data downloaded with Esri API:
### Create geoparquet file per feature and json file with corresponding metadata
import os
import json
import geopandas as gpd
from shapely.geometry import shape
import logging
import re

# directory to raw open calgary data, use latest download
date = "20260426"
DATA_DIR = f"/Users/arleth/Desktop/calgary-dashboard/data/calgary/data/open_calgary/{date}"
# DATA_DIR = f"/sci-it/hosts/olympus/calgary/data/open_calgary/{date}"
# directory to save the cleaned data
SAVE_DIR = f"/Users/arleth/Desktop/calgary-dashboard/data/calgary/processed_data/open_calgary/{date}"
# SAVE_DIR = f"/sci-it/hosts/olympus/calgary/processed_data/open_calgary/{date}"
os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(f"{SAVE_DIR}/features", exist_ok=True)
os.makedirs(f"{SAVE_DIR}/metadata", exist_ok=True)

# Set up logging
# ref: https://realpython.com/python-logging/
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f"{SAVE_DIR}/oc_data_prep_{date}.log"),
        logging.StreamHandler()
    ]
)

def create_standardized_file_name(dataset_id, dataset_name, append_str):
    """
    Creates a standardized file name for a given file name.

    Args:
        file_name (str): The file name to create a standardized file name for.
        append_str (str): The string to append to the end of the file name. (e.g. 'features', 'metadata')

    Returns:
        str: The standardized file name.
    """
    dataset_name = dataset_name.replace(' ', '_') # replace spaces with underscores
    dataset_name = re.sub(r'[^A-Za-z0-9_]', '', dataset_name)
    dataset_name_parts = dataset_name.split('_') # split by underscores

    # create camel case file name
    camel_case_dataset_name = ''.join([p.title() for p in dataset_name_parts[:-1]])
    camel_case_file_name = f"{dataset_id}_{camel_case_dataset_name}_{append_str}"
    return camel_case_file_name

# identified these by reviewing files directly, the_geom is an outdated schema
GEOMETRY_FIELD_NAMES = frozenset(
    ("point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "the_geom", "geometry", "geom")
)

def get_geometry_type(data_json_obj: dict) -> str:
    """
    Returns the first lowercase geometry kind (e.g. 'multipolygon') found in the data,
    or None if not found. Searches for any column key in each row matching a known geometry field name.
    """
    for row in data_json_obj:
        for k in row:
            if k.lower() in GEOMETRY_FIELD_NAMES:
                return k.lower()
    return None

def open_calgary_list_to_gdf(records: list) -> gpd.GeoDataFrame:
    """
    Build a GeoDataFrame from Open Calgary *_data.json: a JSON array of flat dicts
    with one geometry field (multipolygon, point, etc.) containing a GeoJSON geometry dict.

    This is not the same as GeoJSON Feature objects; GeoDataFrame.from_features() does not apply.
    """
    if not records:
        return gpd.GeoDataFrame(geometry=[])

    # find the geometry key
    geom_key = get_geometry_type(records)

    if geom_key is None:
        raise ValueError("No geometry column in any record")

    rows = []
    for row in records:
        props = dict(row)
        raw = props.pop(geom_key, None)
        if raw is None:
            geom = None
        elif isinstance(raw, dict):
            geom = shape(raw)
        else:
            geom = None
        props["geometry"] = geom
        rows.append(props)

    gdf = gpd.GeoDataFrame(rows, geometry="geometry")
    return gdf

if __name__ == "__main__":
    # list all subdirectories in DATA_DIR, i.e. data sets
    subdirs = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]

    # for each dataset find feature and metadata files and create geoparquet and json metadata files
    for subdir in subdirs:
        # get list of files in subdir
        files = sorted(os.listdir(os.path.join(DATA_DIR, subdir)))
        # find data and metadata files
        feature_file = [f for f in files if f.endswith('_data.json')][0]
        metadata_file = [f for f in files if f.endswith('_metadata.json')][0]
        # define data and metadata file paths
        metadata_file_path = f"{DATA_DIR}/{subdir}/{metadata_file}"
        feature_file_path = f"{DATA_DIR}/{subdir}/{feature_file}"

        # load metadata file
        with open(metadata_file_path, 'r') as f:
            metadata_obj = json.load(f)

        # load data file
        with open(feature_file_path, 'r') as f:
            feature_obj = json.load(f)

        # flag to create parquet file
        created_parquet_file = False

        # retrieve information from metadata
        name = metadata_obj['name']
        id = metadata_obj['id']

        # create metadata file name
        metadata_file_name = create_standardized_file_name(id, name, 'metadata')

        # save to json file 
        with open(f"{SAVE_DIR}/metadata/{metadata_file_name}.json", 'w') as f:
            json.dump(metadata_obj, f)
        logger.info(f"Saved {metadata_file_name} as json file")

        # create file name
        feature_file_name = create_standardized_file_name(id, name, 'features')

        # Build GeoDataFrame: Open Calgary uses a JSON array of flat records with a geometry
        # field (e.g. multipolygon), not GeoJSON Feature objects — from_features() is wrong here.
        gdf = None

        # get geometry type
        geom_type = get_geometry_type(feature_obj)

        # build GeoDataFrame
        try:
            if isinstance(feature_obj, dict) and "features" in feature_obj:
                gdf = gpd.GeoDataFrame.from_features(feature_obj)
            elif isinstance(feature_obj, list) and feature_obj and geom_type is not None:
                gdf = open_calgary_list_to_gdf(feature_obj)
        except Exception:
            logger.exception(
                "Failed to build GeoDataFrame for %s/%s", subdir, feature_file
            )
            gdf = None

        if gdf is not None and not gdf.empty and gdf.geometry.notna().any():
            # find what the geom type, NOT using nan or geom_type which can potentailly be 'the_geom' or 'geometry'
            true_geom_type = gdf.geom_type.unique()[-1].lower()

            save_dir = f"{SAVE_DIR}/features/{true_geom_type}"
            os.makedirs(save_dir, exist_ok=True)
            gdf.to_parquet(f"{save_dir}/{feature_file_name}.parquet")
            created_parquet_file = True
            logger.info(
                "Saved %s as parquet (%d rows, %s)", feature_file_name, len(gdf), geom_type
            )
        else:
            if gdf is not None and gdf.empty: 
                logger.info("Empty geometry for %s; skipping parquet.", feature_file)
            else:
                logger.info("Could not build valid geometries for %s.", feature_file)
            created_parquet_file = False

        # save as json if failed to create parquet file
        if not created_parquet_file:
            # save to json file
            with open(f"{SAVE_DIR}/features/{feature_file_name}.json", 'w') as f:
                json.dump(feature_obj, f)
            logger.info(f"Saved {feature_file_name} as json file")
