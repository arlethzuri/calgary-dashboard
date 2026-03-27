import requests
import json
import os
import logging
import datetime as dt

# get current date to label download dir
curr_date = dt.datetime.now().strftime("%Y%m%d")

# token can be created with account at data.calgary.ca
APP_TOKEN = "g8EtMlEOBGi7qHws7qqJ5GCVM"
OPEN_CALGARY_DATA_DIR = f"/sci-it/hosts/olympus/calgary/data/open_calgary/"
DOWNLOAD_DIR = f"{OPEN_CALGARY_DATA_DIR}/{curr_date}"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Set up logging
# ref: https://realpython.com/python-logging/
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f"{OPEN_CALGARY_DATA_DIR}/{curr_date}/manual_get_data_{curr_date}.log"),
        logging.StreamHandler()
    ]
)

# Load URLs of datasets we manually identified on Open Calgary portal (data.calgary.ca)
with open('./manually_selected_datasets.txt', 'r') as f:
    DATASET_URLS = [line.strip() for line in f if line.strip()]

# Extract dataset IDs from each URL. If 'about_data' is in the URL, get the string before 'about_data' and split by '/', 
# otherwise get the string after the last '/'.
def extract_dataset_id(url):
    # Example: https://data.calgary.ca/Environment/Tree-Canopy-2015/ainq-wn9v/about_data
    parts = url.strip("/").split('/')
    if 'about_data' in parts or 'about-data' in parts:
        # Take the second last part as the ID
        return parts[-2]
    else:
        return parts[-1]

def get_record_count(id):
    # get end point and retrieve record count at this URL
    api_url = f"https://data.calgary.ca/resource/{id}.json?$select=count(*)"

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        count = int(data[0]['count'])
        return count
    except Exception as e:
        return f"Error: {e}"

DATASET_IDS = [extract_dataset_id(url) for url in DATASET_URLS]

# # Download data and metadata for each dataset and log failures
download_log = f"download_log_{curr_date}.log"
for ds_id in DATASET_IDS:
    record_count = get_record_count(ds_id)
    # Get API URL for each dataset, set limit based on number of available records
    # TODO: this may break if record count is too big, get in chunks
    # ref: https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset
    # e.g. ref: https://dev.socrata.com/foundry/data.calgary.ca/tbsv-89ps
    api_data_url = f"https://data.calgary.ca/api/v3/views/{ds_id}/query.json?limit={record_count}&app_token={APP_TOKEN}"
    api_metadata_url = f"https://data.calgary.ca/api/views/{ds_id}"

    # try downloading data
    try:
        # download data from API
        response = requests.get(api_data_url)
        response.raise_for_status()
        data = response.json()
        dataset_dir = os.path.join(DOWNLOAD_DIR, ds_id)
        os.makedirs(dataset_dir, exist_ok=True)
        with open(os.path.join(dataset_dir, f"{ds_id}_data.json"), "w") as f:
            json.dump(data, f)
        logger.info(f"Downloaded data from {api_data_url}")
    except Exception as e:
        logger.error(f"Failed to download data from {api_data_url}: {e}, record count is: {record_count}")
        continue


    # try downloading metadata
    try:
        # download metadata from API
        response = requests.get(api_metadata_url)
        response.raise_for_status()
        metadata = response.json()
        with open(os.path.join(dataset_dir, f"{ds_id}_metadata.json"), "w") as f:
            json.dump(metadata, f)
        logger.info(f"Downloaded metadata from {api_metadata_url}")
    except Exception as e:
        logger.error(f"Failed to download metadata from {api_metadata_url}: {e}")
        continue