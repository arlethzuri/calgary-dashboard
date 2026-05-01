### For StatCan CSV data:
### Clean downloaded source and save cleaned csv + metadata
import os
import json
import logging
import re
import csv
import pandas as pd
import geopandas as gpd

# directory to raw statcan data, use latest download
date = "20260430"
DATA_DIR = f"/Users/arleth/Desktop/calgary-dashboard/data/calgary/data/statcan/{date}"
MANUAL_DATA_DIR = f"/Users/arleth/Desktop/calgary-dashboard/data/calgary/data/statcan/manual_download"
# DATA_DIR = f"/sci-it/hosts/olympus/calgary/data/statcan/{date}"
# directory to save the cleaned data
SAVE_DIR = f"/Users/arleth/Desktop/calgary-dashboard/data/calgary/processed_data/statcan/{date}"
# SAVE_DIR = f"/sci-it/hosts/olympus/calgary/processed_data/statcan/{date}"
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
        logging.FileHandler(f"{SAVE_DIR}/sc_data_prep_{date}.log"),
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

# Group files using the product id prefix
def get_file_prefix(file_name: str) -> str:
    stem = os.path.splitext(file_name)[0]
    return stem.split("_")[0]

# read *_MetaData.csv and return key dataset metadata.
def get_dataset_metadata(metadata_path: str) -> dict:
    with open(metadata_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        first_row = next(reader)

    return {
        "id": first_row.get("Product Id", "").strip(),
        "title": first_row.get("Cube Title", "").strip(),
    }

# load manually downloaded dissemination areas from StatCan
# from 2021 census
# https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/index2021-eng.cfm?year=21
def load_dissemination_areas_gdf() -> gpd.GeoDataFrame:
    path = f"{MANUAL_DATA_DIR}/lda_000a21a_e/lda_000a21a_e.shp"
    return gpd.read_file(path)

if __name__ == "__main__":
    # list all downloaded statcan files
    files = sorted(os.listdir(DATA_DIR))

    # group by metadata and data files
    data_files = [f for f in files if f.endswith(".csv") and "_MetaData" not in f]
    metadata_files = [f for f in files if f.endswith("_MetaData.csv")]

    # group meta and data files by prefix
    data_by_prefix = {}
    for file_name in data_files:
        data_by_prefix[get_file_prefix(file_name)] = file_name

    metadata_by_prefix = {}
    for file_name in metadata_files:
        metadata_by_prefix[get_file_prefix(file_name)] = file_name

    # load meta and data files with same prefix
    for prefix in data_by_prefix.keys():
        data_file = data_by_prefix[prefix]
        metadata_file = metadata_by_prefix[prefix]

        # load data and metadata files, metadata file only use first 2 lines read from file
        data_df = pd.read_csv(f"{DATA_DIR}/{data_file}")
        metadata_df = pd.read_csv(f"{DATA_DIR}/{metadata_file}", nrows=1)
   
        # drop Symbols columns as they're all nan
        data_df = data_df.drop(columns=[col for col in data_df.columns if col.startswith("Symbols")])

        # get 2-digit province id and 2-digit census division code of Calgary
        cal_code = data_df[data_df['GEO'] == "Calgary"]['DGUID'].values[0][-7:-3]     
        # last 8 digits of DGUID is DAUID, first two of which are province id and census division code
        # https://www150.statcan.gc.ca/n1/pub/92f0138m/92f0138m2019001-eng.htm
        mask = data_df['DGUID'].astype(str).str[-8:-4] == cal_code
        cal_data_df = data_df[mask]

        # join polygons from dissemination areas table to cal_data_df
        dissemination_areas_gdf = load_dissemination_areas_gdf()
        cal_data_gdf = dissemination_areas_gdf.merge(cal_data_df, on="DGUID", how="right")

        # save cleaned features to parquet and metadata to csv
        pid = metadata_df["Product Id"].values[0]
        title = ' '.join(metadata_df["Cube Title"].values[0].split(" ")[0:5])
        data_file_name = create_standardized_file_name(pid, title, "features")
        metadata_file_name = create_standardized_file_name(pid, title, "metadata")  
        cal_data_gdf.to_parquet(f"{SAVE_DIR}/features/{data_file_name}.parquet", index=False)
        metadata_df.to_csv(f"{SAVE_DIR}/metadata/{metadata_file_name}.csv", index=False)