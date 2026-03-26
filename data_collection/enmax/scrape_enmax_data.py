import os
import json
import requests
import logging
# to get geodata from ESRI endpoint: https://github.com/openaddresses/pyesridump
from esridump.dumper import EsriDumper
import datetime as dt

# specify the directories to ArcGIS services for ENMAX
DATA_DIR = "/sci-it/hosts/olympus/calgary/data/0_raw/enmax/march_download"
SERVICES_DIRECTORY = 'https://services1.arcgis.com/NKgP4VcXUzEyOnmg/ArcGIS/rest/services'
response = requests.get(f'{SERVICES_DIRECTORY}?f=pjson')
feature_servers = response.json()['services']
FEATURE_SERVERS = [fs['name'] for fs in feature_servers]

# Set up logging
# ref: https://realpython.com/python-logging/
curr_date = dt.datetime.now().strftime("%Y%m%d")
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, f"scrape_enmax_data_{curr_date}.log")),
        logging.StreamHandler()
    ]
)


# download FeatureServer metadata, FeatureServer features at each layer, and features' metadata
for feature_server in FEATURE_SERVERS:
    # create subdirectory for this feature server
    server_dir = os.path.join(DATA_DIR, feature_server)
    os.makedirs(server_dir, exist_ok=True)

    # specify the URL of the FeatureServer
    server_url = f'{SERVICES_DIRECTORY}/{feature_server}/FeatureServer'

    logger.info(f"Downloading metadata and features from {server_url}")
    try:
        # save metadata of FeatureServer
        describe_resp = requests.get(f'{server_url}?f=pjson')
        describe_resp.raise_for_status()
        describe_json = describe_resp.json()
        
        with open(os.path.join(server_dir, f'describe.json'), 'w') as f:
            json.dump(describe_json, f)

        # get list of layers and download features
        layers_resp = requests.get(f'{server_url}?f=pjson')
        layers_resp.raise_for_status()
        layers_json = layers_resp.json()
        layer_infos = layers_json.get('layers', [])

        # download features and metadata at each layer
        for layer in layer_infos:
            layer_id = layer['id']
            layer_name = layer['name']
            
            features_path = os.path.join(server_dir, f'{layer_name}_features.json')
            metadata_path = os.path.join(server_dir, f'{layer_name}_metadata.json')
            try:
                # download metadata at current featureserver layer
                metadata_resp = requests.get(f'{server_url}/{layer_id}?f=pjson')
                metadata_resp.raise_for_status()
                metadata_json = metadata_resp.json()
                
                with open(metadata_path, 'w') as f:
                    json.dump(metadata_json, f)

                # download current feature layer
                d = EsriDumper(f'{server_url}/{layer_id}')
                features = list(d)
                if features:
                    with open(features_path, 'w') as f:
                        json.dump(features, f)
                logger.info(f"SUCCESS: {feature_server}/{layer_name}")
            except Exception as e:
                logger.error(f"FAILED: {feature_server}/{layer_name} ({e})")
    except Exception as e:
        logger.error(f"FAILED to process feature server {feature_server} ({e})")