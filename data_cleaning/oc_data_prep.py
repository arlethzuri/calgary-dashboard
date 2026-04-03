### For Open Calgary data downloaded with Esri API:
### Create geoparquet file per feature and json file with corresponding metadata
import os
import json
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
import logging
import re

# directory to raw ENMAX data, use latest download
date = "20260327"
DATA_DIR = f"/sci-it/hosts/olympus/calgary/data/open_calgary/{date}"
# directory to save the cleaned data
SAVE_DIR = f"/sci-it/hosts/olympus/calgary/processed_data/open_calgary/{date}"
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

def get_geometry_type(data_json_obj):
    """
    Get the geometry type from the Open Calgary dataset JSON.
    """
    geometry_types = ['point', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'multipolygon']
    data_json_obj_keys = [k.lower() for k in data_json_obj.keys()]
    for geometry_type in geometry_types:
        if geometry_type in data_json_obj_keys:
            return geometry_type
    return None

# def get_projection(metadata_json_obj):
#     """
#     Get the first listed projection from the Open Calgary dataset JSON.
#     """
#     projection_str = metadata_json_obj

#     projection = projection_str.split(' ')[0]
#     return projection

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

        # try:
        # load metadata file
        metadata_file_path = f"{DATA_DIR}/{subdir}/{metadata_file}"
        with open(metadata_file_path, 'r') as f:
            metadata_obj = json.load(f)

        # retrieve information from metadata
        name = metadata_obj['name']
        id = metadata_obj['id']

        # create file name
        metadata_file_name = create_standardized_file_name(id, name, 'metadata')

        # save to json file 
        with open(f"{SAVE_DIR}/metadata/{metadata_file_name}.json", 'w') as f:
            json.dump(metadata_obj, f)

        # save feature file as geoparquet, using ID as name
        # create geopandas dataframe
        feature_file_path = f"{DATA_DIR}/{subdir}/{feature_file}"

        # load json file, an iterable object of GeoJSON feature(s)
        with open(feature_file_path, 'r') as f:
            feature_obj = json.load(f)

        # unwrap feature_obj 
        feature_obj = feature_obj[0]

        # get GeoJSON geometry type from data file
        geometry_type = get_geometry_type(feature_obj)

        if geometry_type is not None:
            # convert to shapely object for loading into geodataframe
            geoms = [shape(feature_obj[geometry_type])]

            # create geodataframe
            gdf = gpd.GeoDataFrame(geometry=geoms)

            # create file name
            feature_file_name = create_standardized_file_name(id, name, 'feature')

            # save to geoparquet file in directory corresponding to geojson geometric type
            # create dir with geom_type
            geom_type = gdf.geom_type[0]
            save_dir = f"{SAVE_DIR}/features/{geom_type}"
            os.makedirs(save_dir, exist_ok=True)
            gdf.to_parquet(f"{save_dir}/{feature_file_name}.parquet")
        else:
            logger.error(f"Failed to create geoparquet file for {feature_file}: No geometry type found\nName: {name}\nKeys:{feature_obj.keys()}\nDescription: {metadata_obj['description']}")
            continue
        # except Exception as e:
        #     logger.error(f"Failed to process id {id} ({name}): {e}")
        #     continue