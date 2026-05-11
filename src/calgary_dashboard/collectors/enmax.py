"""ENMAX data collector.

Imported from a script and driven with `collect()`. Pulls feature server
metadata and features from ENMAX's public ArcGIS services directory.
"""

from __future__ import annotations

from pathlib import Path

from esridump.dumper import EsriDumper

from calgary_dashboard.common.dates import snapshot_date
from calgary_dashboard.common.http import get_json
from calgary_dashboard.common.io import ensure_dir, write_json
from calgary_dashboard.config.logging import configure_logger
from calgary_dashboard.config.paths import RAW_DATA_ROOT
from calgary_dashboard.config.settings import get_settings

settings = get_settings()
ENMAX_DATA_DIR = RAW_DATA_ROOT / "enmax"
LOG_SNAPSHOT = snapshot_date()
# If ENMAX ArcGIS services URL changes, update .env file
SERVICES_DIRECTORY = settings.enmax_services_directory

# Configure logger
logger = configure_logger(
    __name__,
    log_file=ENMAX_DATA_DIR / LOG_SNAPSHOT / f"enmax_collector_{LOG_SNAPSHOT}.log",
)


def list_feature_servers(services_url: str) -> list[str]:
    """Fetch feature server names hosted at an ArcGIS services directory URL."""
    payload = get_json(f"{services_url}?f=pjson")
    return [service["name"] for service in payload.get("services", [])]


def download_layer(
    server_url: str,
    layer_id: int,
    layer_name: str,
    server_dir: Path,
) -> None:
    """Download metadata and features for one FeatureServer layer."""
    metadata_url = f"{server_url}/{layer_id}?f=pjson"
    try:
        # Get metadata from the layer
        metadata = get_json(metadata_url)
        # Write metadata to the server directory
        write_json(server_dir / f"{layer_name}_metadata.json", metadata)
        logger.info("Downloaded layer metadata from %s", metadata_url)
    except Exception as error:
        logger.error(
            "Failed to download layer metadata from %s: %s", metadata_url, error
        )
        return

    try:
        # Use pyesridump to dump the features from the layer
        dumper = EsriDumper(f"{server_url}/{layer_id}")
        # Get the features from the layer
        features = list(dumper)
        if features:
            # Write the features to the server directory
            write_json(server_dir / f"{layer_name}_features.json", features)
        logger.info("Downloaded layer features from %s/%s", server_url, layer_id)
    except Exception as error:
        logger.error(
            "Failed to download layer features from %s/%s: %s",
            server_url,
            layer_id,
            error,
        )


def download_feature_server(
    services_url: str,
    feature_server: str,
    download_dir: Path,
) -> None:
    """Download describe metadata and all layer payloads for one FeatureServer."""
    server_url = f"{services_url}/{feature_server}/FeatureServer"
    server_dir = ensure_dir(download_dir / feature_server)

    try:
        # Get FeatureServer metadata
        describe = get_json(f"{server_url}?f=pjson")
        # Write describe metadata under server_dir
        write_json(server_dir / "describe.json", describe)
        logger.info("Downloaded describe.json from %s", server_url)
    except Exception as error:
        logger.error("Failed to download describe.json from %s: %s", server_url, error)
        return

    # Download metadata and features for each layer
    for layer in describe.get("layers", []):
        download_layer(server_url, layer["id"], layer["name"], server_dir)


def collect(services_url: str = SERVICES_DIRECTORY) -> Path:
    """Download all ENMAX feature servers reachable from the services directory,
    return the directory where data was saved."""
    # Get snapshot date and create directory to save data to
    run_snapshot = snapshot_date()
    download_dir = ensure_dir(ENMAX_DATA_DIR / run_snapshot)

    logger.info("Writing snapshot to %s", download_dir)

    try:
        # List feature servers at the services directory
        feature_servers = list_feature_servers(services_url)
    except Exception as error:
        logger.error(
            "Failed to list feature servers at %s: %s", services_url, error
        )
        return download_dir

    logger.info(
        "Found %d feature servers at %s", len(feature_servers), services_url
    )
    # Loop through each feature server and download the metadata and features
    for feature_server in feature_servers:
        download_feature_server(services_url, feature_server, download_dir)

    return download_dir
