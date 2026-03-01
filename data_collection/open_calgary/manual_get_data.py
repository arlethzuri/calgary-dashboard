import requests
import json
import os
import logging

# Set up logging
# ref: https://realpython.com/python-logging/
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("/sci-it/hosts/olympus/calgary/data/open_calgary/manual_get_data.log"),
        logging.StreamHandler()
    ]
)

# token can be created with account at data.calgary.ca
APP_TOKEN = "g8EtMlEOBGi7qHws7qqJ5GCVM"
DOWNLOAD_DIR = "/sci-it/hosts/olympus/calgary/data/open_calgary"

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

# # Download data for each dataset and log failures
download_log = "download_log.log"
for ds_id in DATASET_IDS:
    record_count = get_record_count(ds_id)
    # Get API URL for each dataset, set limit based on number of available records
    # TODO: this may break if record count is too big, get in chunks
    # ref: https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset
    # e.g. ref: https://dev.socrata.com/foundry/data.calgary.ca/tbsv-89ps
    api_url = f"https://data.calgary.ca/api/v3/views/{ds_id}/query.json?limit={record_count}&app_token={APP_TOKEN}"

    try:
        response = requests.get(api_url) # download data from API
        response.raise_for_status()
        data = response.json()
        dataset_dir = os.path.join(DOWNLOAD_DIR, ds_id)
        os.makedirs(dataset_dir, exist_ok=True)
        with open(os.path.join(dataset_dir, f"{ds_id}.json"), "w") as f:
            json.dump(data, f)
        logger.info(f"Downloaded {api_url}")
        
    except Exception as e:
        logger.error(f"Failed to download {api_url}: {e}, record count is: {record_count}")
        continue