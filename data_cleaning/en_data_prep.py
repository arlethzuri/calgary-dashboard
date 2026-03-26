### For ENMAX data downloaded with Esri API:
### Create geoparquet file per feature and json file with corresponding metadata
from utilities import *
import os
import json

def create_standardized_file_name(file_name, append_str):
    """
    Creates a standardized file name for a given file name.

    Args:
        file_name (str): The file name to create a standardized file name for.
        append_str (str): The string to append to the end of the file name.

    Returns:
        str: The standardized file name.
    """
    file_name = file_name.split('.')[0] # remove .json
    file_name = file_name.replace(' ', '_') # replace spaces with underscores
    file_parts = file_name.split('_') # split by underscores

    # create camel case file name, remove dashes
    camel_case_file_name = ''.join([p.title().replace('-', '') for p in file_parts[:-1]])
    camel_case_file_name = f"{camel_case_file_name}_{append_str}"
    return camel_case_file_name

# directory to raw ENMAX data
DATA_DIR = "../data/0_raw/enmax"
# directory to save the cleaned data
SAVE_DIR = "../data/1_processed/enmax"

if __name__ == "__main__":
    # list all subdirectories in DATA_DIR, i.e. feature servers
    subdirs = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]

    # for each feature server find feature and metadata files and create geoparquet and json metadata files
    for subdir in subdirs:
        # get list of files in subdir
        files = os.listdir(os.path.join(DATA_DIR, subdir))
        # find feature and metadata files
        feature_files = [f for f in files if f.endswith('_features.json')]
        metadata_files = [f for f in files if f.endswith('_metadata.json')]

        # save feature file as geoparquet
        for feature_file in feature_files:
            # standardize name
            file_name = create_standardized_file_name(feature_file, 'features')

            # create geopandas dataframe
            file_path = f"{DATA_DIR}/{subdir}/{feature_file}"

            # load json file, an iterable object of GeoJSON feature(s)
            with open(file_path, 'r') as f:
                obj = json.load(f)

            # create geopandas dataframe
            gdf = gpd.GeoDataFrame.from_features(obj)

            # save to geoparquet file
            gdf.to_parquet(f"{SAVE_DIR}/{file_name}.parquet")

        # load describe.json for use in creating updated metadata files
        describe_file = f"{DATA_DIR}/{subdir}/describe.json"
        with open(describe_file, 'r') as f:
            describe_obj = json.load(f)

        # update metadata files:
        # 1. add original file name of metadata file and features file
        # 2. append all fields from describe.json, even if some are unneeded for visualization or analysis, they may be useful later for data versioning, data governance, etc
        for metadata_file in metadata_files:
            # standardize name
            file_name = create_standardized_file_name(metadata_file, 'metadata')

            # load metadata file
            with open(file_path, 'r') as f:
                obj = json.load(f)

            # append describe.json fields
            