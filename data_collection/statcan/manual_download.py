# using statcanada web data service:
# https://www.statcan.gc.ca/en/developers?HPA=1
# https://www.statcan.gc.ca/en/developers/wds/user-guide#a12-6
import os
import requests
import datetime as dt
import logging
import zipfile
import io

# --- Config ---
LANG = "en" # 'en' or 'fr'
CALGARY_GEO = "Calgary" # string to filter in the GEO column

# get current date to label download dir
curr_date = dt.datetime.now().strftime("%Y%m%d")

# Paths
STATCAN_DATA_DIR = f"/Users/arleth/Desktop/calgary-dashboard/data/calgary/data/statcan"
# STATCAN_DATA_DIR = f"/sci-it/hosts/olympus/calgary/data/statcan/"
DOWNLOAD_DIR = f"{STATCAN_DATA_DIR}/{curr_date}"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Set up logging
# ref: https://realpython.com/python-logging/
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f"{STATCAN_DATA_DIR}/{curr_date}/manual_download_{curr_date}.log"),
        logging.StreamHandler()
    ]
)

dataset_urls_file = './download_sources.txt'
# Load URLs of datasets we manually identified on statcanada
with open(dataset_urls_file, 'r') as f:
    DATASET_URLS = [line.strip() for line in f if line.strip()]

for url in DATASET_URLS:
    try:
        # use WDS API for the CSV download URL
        response = requests.get(url, allow_redirects=True, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # download ZIP file which has data and metadata
        zip_url = data['object']
        zr = requests.get(zip_url)
        zip_file = zipfile.ZipFile(io.BytesIO(zr.content))
        zip_file.extractall(DOWNLOAD_DIR)
        logger.info(f"Downloaded and extracted {zip_url}")

    except Exception as e:
        logger.error(f"Failed to download from {url}: {e}")