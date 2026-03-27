import requests
import os
from concurrent.futures import ThreadPoolExecutor
import datetime as dt

# get current date to label download dir
curr_date = dt.datetime.now().strftime("%Y%m%d")

# app token can be created at data.calgary.ca
DOMAIN = "data.calgary.ca"
DATA_DIR = f"/sci-it/hosts/olympus/calgary/data/open_calgary/{curr_date}"
os.makedirs(DATA_DIR, exist_ok=True)
APP_TOKEN = "g8EtMlEOBGi7qHws7qqJ5GCVM"

def get_all_datasets():
    # Discovery API returns the full catalog for a domain
    # https://dev.socrata.com/docs/other/discovery#?route=get-/catalog/v1-search_context-domain-domains-domain-
    url = f"https://api.us.socrata.com/api/catalog/v1?domains={DOMAIN}&search_context={DOMAIN}&only=datasets"
    response = requests.get(url)
    return response.json().get('results', [])

def download_dataset(dataset):
    ds_id = dataset['resource']['id']
    name = dataset['resource']['name'].replace("/", "-")
    print(f"Starting download: {name} ({ds_id})")
    
    # Try to download the dataset in .csv, .json, and .geojson formats, skipping any that fail.
    # Log successful and failed downloads for review.
    formats = ["csv", "json", "geojson"]
    dataset_dir = os.path.join(DATA_DIR, name)
    os.makedirs(dataset_dir, exist_ok=True)

    success_log = os.path.join(DATA_DIR, "success.log")
    failed_log = os.path.join(DATA_DIR, "failed.log")

    for ext in formats:
        download_url = f"https://data.calgary.ca/api/v3/views/{ds_id}/query.{ext}"
        dest = os.path.join(dataset_dir, f"{name}.{ext}")
        try:
            r = requests.get(download_url, stream=True)
            r.raise_for_status()
            with open(dest, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded {name}.{ext}")
            with open(success_log, "a") as slog:
                slog.write(f"{name}.{ext} : SUCCESS ({download_url})\n")
        except Exception as e:
            print(f"Skipping {name}.{ext}: {e}")
            with open(failed_log, "a") as flog:
                flog.write(f"{name}.{ext} : FAILED ({download_url}) - {e}\n")

# Get metadata and start downloads
datasets = get_all_datasets()
print(f"Found {len(datasets)} datasets. Starting bulk download...")

# Use threads to download 5 at a time (much faster than one by one)
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(download_dataset, datasets)