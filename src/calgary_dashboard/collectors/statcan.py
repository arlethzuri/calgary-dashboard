"""Statistics Canada data collector.

Downloads ZIP payloads from StatCan Web Data Service table download URLs and
extracts them into a dated raw-data snapshot directory.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import requests

from calgary_dashboard.common.dates import snapshot_date
from calgary_dashboard.common.http import get_json
from calgary_dashboard.common.io import ensure_dir
from calgary_dashboard.config.logging import configure_logger
from calgary_dashboard.config.paths import RAW_DATA_ROOT

STATCAN_DATA_DIR = RAW_DATA_ROOT / "statcan"
LOG_SNAPSHOT = snapshot_date()

logger = configure_logger(
    __name__,
    log_file=STATCAN_DATA_DIR / LOG_SNAPSHOT / f"statcan_collector_{LOG_SNAPSHOT}.log",
)


def load_download_urls(sources_file: Path) -> list[str]:
    """Load non-empty StatCan WDS download URLs from a plain text file."""
    with sources_file.open("r") as file_obj:
        return [line.strip() for line in file_obj if line.strip()]


def get_zip_url(download_url: str) -> str:
    """Fetch the ZIP download URL from a StatCan WDS table download endpoint."""
    payload = get_json(download_url)
    zip_url = payload.get("object")
    if not zip_url:
        raise ValueError(f"StatCan WDS response did not include an object URL: {download_url}")
    return zip_url


def extract_zip_bytes(zip_bytes: bytes, download_dir: Path) -> None:
    """Extract a ZIP payload into the snapshot directory."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
        zip_file.extractall(download_dir)


def download_dataset(download_url: str, download_dir: Path) -> None:
    """Download and extract one StatCan table ZIP from a WDS endpoint URL."""
    try:
        # use WDS API for the CSV download ZIP URL
        zip_url = get_zip_url(download_url)
    except Exception as error:
        logger.error("Failed to get ZIP URL from %s: %s", download_url, error)
        return

    try:
        response = requests.get(zip_url, allow_redirects=True, timeout=30)
        response.raise_for_status()
        extract_zip_bytes(response.content, download_dir)
        logger.info("Downloaded and extracted %s", zip_url)
    except Exception as error:
        logger.error("Failed to download and extract %s: %s", zip_url, error)


def collect(sources_file: Path) -> Path:
    """Download all StatCan tables listed in the source file,
    return the directory where data was saved."""
    # if sources_file is not Path object raise error
    if not isinstance(sources_file, Path):
        raise ValueError("sources_file must be a Path object")

    # get source file path and check if it exists
    source_path = sources_file.expanduser().resolve()
    if not source_path.is_file():
        raise FileNotFoundError(f"Sources file not found: {source_path}")

    # get snapshot date and create directory to save data to and load dataset URLs
    run_snapshot = snapshot_date()
    download_dir = ensure_dir(STATCAN_DATA_DIR / run_snapshot)
    download_urls = load_download_urls(source_path)

    logger.info("Loaded %d source URLs from %s", len(download_urls), source_path)
    logger.info("Writing snapshot to %s", download_dir)

    for download_url in download_urls:
        download_dataset(download_url, download_dir)

    return download_dir
