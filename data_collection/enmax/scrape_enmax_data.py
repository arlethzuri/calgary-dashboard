import os
import json
import requests
from esridump.dumper import EsriDumper

# specify the directories to ArcGIS services for ENMAX
SERVICES_DIRECTORY = 'https://services1.arcgis.com/NKgP4VcXUzEyOnmg/ArcGIS/rest/services'
response = requests.get(f'{SERVICES_DIRECTORY}?f=pjson')
feature_servers = response.json()['services']
FEATURE_SERVERS = [fs['name'] for fs in feature_servers]

# log which downloads fail
success_log = os.path.join('../../data/enmax', 'successful_downloads.log')
failed_log = os.path.join('../../data/enmax', 'failed_downloads.log')

# download FeatureServer metadata, FeatureServer features at each layer, and features' metadata
for feature_server in FEATURE_SERVERS:
    # create subdirectory for this feature server
    server_dir = os.path.join('../../data/enmax', feature_server)
    os.makedirs(server_dir, exist_ok=True)

    # specify the URL of the FeatureServer
    server_url = f'{SERVICES_DIRECTORY}/{feature_server}/FeatureServer'

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

        # download features at each layer
        for layer in layer_infos:
            layer_id = layer['id']
            layer_name = layer['name']
            layer_success = False
            features_path = os.path.join(server_dir, f'{layer_name}_features.json')
            metadata_path = os.path.join(server_dir, f'{layer_name}_metadata.json')
            try:
                d = EsriDumper(f'{server_url}/{layer_id}')
                features = list(d)
                if features:
                    with open(features_path, 'w') as f:
                        json.dump(features, f)
                with open(metadata_path, 'w') as f:
                    json.dump(layer, f)
                with open(success_log, "a") as slog:
                    slog.write(f"{feature_server} : {layer_name} : SUCCESS\n")
                print(f"SUCCESS: {feature_server}/{layer_name}")
                layer_success = True
            except Exception as e:
                with open(failed_log, "a") as flog:
                    flog.write(f"{feature_server} : {layer_name} : FAILED - {e}\n")
                print(f"FAILED: {feature_server}/{layer_name} ({e})")
    except Exception as e:
        with open(failed_log, "a") as flog:
            flog.write(f"{feature_server} : FAILED to get server metadata or layers - {e}\n")
        print(f"FAILED to process feature server {feature_server} ({e})")