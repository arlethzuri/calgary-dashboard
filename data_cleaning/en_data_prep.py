### For ENMAX data downloaded with Esri API:
### Create geoparquet file per feature and json file with corresponding metadata
import os
import json
import geopandas as gpd

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

# directory to raw ENMAX data, use latest download
date = "20260327"
DATA_DIR = f"/sci-it/hosts/olympus/calgary/data/enmax/{date}"
# directory to save the cleaned data
SAVE_DIR = f"/sci-it/hosts/olympus/calgary/processed_data/enmax/{date}"
os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(f"{SAVE_DIR}/features", exist_ok=True)
os.makedirs(f"{SAVE_DIR}/metadata", exist_ok=True)

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

            # save to geoparquet file in directory corresponding to geojson geometric type
            # create dir with geom_type
            geom_type = gdf.geom_type[0]
            save_dir = f"{SAVE_DIR}/features/{geom_type}"
            os.makedirs(save_dir, exist_ok=True)
            gdf.to_parquet(f"{save_dir}/{file_name}.parquet")

        # update metadata:
        # 1. add layer name
        for metadata_file in metadata_files:
            # standardize name
            file_name = create_standardized_file_name(metadata_file, 'metadata')

            # load metadata file
            file_path = f"{DATA_DIR}/{subdir}/{metadata_file}"
            with open(file_path, 'r') as f:
                obj = json.load(f)

            # add layer name
            obj['layer_name'] = subdir

            # save to json file
            with open(f"{SAVE_DIR}/metadata/{file_name}.json", 'w') as f:
                json.dump(obj, f)