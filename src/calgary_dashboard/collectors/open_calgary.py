"""Open Calgary data collector.

Can be used interactively when run directly, or imported from a script and
driven with `collect_from_sources_file(...)`.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from calgary_dashboard.common.dates import snapshot_date
from calgary_dashboard.common.http import get_json
from calgary_dashboard.common.io import ensure_dir, write_json
from calgary_dashboard.config.logging import configure_logger
from calgary_dashboard.config.paths import RAW_DATA_ROOT
from calgary_dashboard.config.settings import get_settings

settings = get_settings()
OPEN_CALGARY_DATA_DIR = RAW_DATA_ROOT / "open_calgary"
LOG_SNAPSHOT = snapshot_date()

logger = configure_logger(
    __name__,
    log_file=OPEN_CALGARY_DATA_DIR / LOG_SNAPSHOT / f"open_calgary_collector_{LOG_SNAPSHOT}.log",
)


def extract_dataset_id(url: str) -> str:
    """Extract dataset ID from an Open Calgary dataset URL."""
    parts = url.strip("/").split("/")
    if "about_data" in parts or "about-data" in parts:
        return parts[-2]
    return parts[-1]


def load_dataset_urls(sources_file: Path) -> list[str]:
    """Load non-empty URLs from a plain text file."""
    with sources_file.open("r") as file_obj:
        return [line.strip() for line in file_obj if line.strip()]


def get_record_count(dataset_id: str) -> int | str:
    """Fetch total row count with Socrata API for a dataset ID."""
    api_url = f"https://data.calgary.ca/resource/{dataset_id}.json?$select=count(*)"
    try:
        data = get_json(api_url)
        return int(data[0]["count"])
    except Exception as error:
        return f"Error: {error}"


def download_dataset(ds_id: str, download_dir: Path, app_token: str) -> None:
    """Download data and metadata payloads for one dataset ID from Socrata API."""
    record_count = get_record_count(ds_id)
    api_data_url = (
        f"https://data.calgary.ca/api/v3/views/{ds_id}/query.json"
        f"?limit={record_count}"
    )
    api_metadata_url = f"https://data.calgary.ca/api/views/{ds_id}"
    headers = {"X-App-Token": app_token} if app_token else {}

    try:
        # Get data from Socrata API
        data = get_json(api_data_url, headers=headers)
        # Create directory named with ds_id to save data under
        dataset_dir = ensure_dir(download_dir / ds_id)
        write_json(dataset_dir / f"{ds_id}_data.json", data)
        logger.info("Downloaded data from %s", api_data_url)
    except Exception as error:
        logger.error(
            "Failed to download data from %s: %s, record count is: %s",
            api_data_url,
            error,
            record_count,
        )
        return

    try:
        # Get metadata from Socrata API
        metadata = get_json(api_metadata_url)
        # Save metadata to ds_id dir and write to JSON
        write_json(dataset_dir / f"{ds_id}_metadata.json", metadata)
        logger.info("Downloaded metadata from %s", api_metadata_url)
    except Exception as error:
        logger.error("Failed to download metadata from %s: %s", api_metadata_url, error)


def collect_from_sources_file(
    sources_file: Path,
) -> Path:
    """Download all datasets listed in a source file,
    return the directory where data was saved."""
    # Get app token from settings, set accordingly in .env
    token = settings.open_calgary_app_token
    if not token:
        raise ValueError("OPEN_CALGARY_APP_TOKEN is not set. Add it to your .env.")

    # Resolve for ~ and symlinks before existence check.
    source_path = sources_file.expanduser().resolve()
    if not source_path.is_file():
        raise FileNotFoundError(f"Sources file not found: {source_path}")

    # Get snapshot date and create directory to save data to and load dataset URLs
    run_snapshot = snapshot_date()
    download_dir = ensure_dir(OPEN_CALGARY_DATA_DIR / run_snapshot)
    # Load dataset URLs from source file
    dataset_urls = load_dataset_urls(source_path)
    # Extract dataset IDs from dataset URLs
    dataset_ids = [extract_dataset_id(url) for url in dataset_urls]

    logger.info("Loaded %d source URLs from %s", len(dataset_urls), source_path)
    logger.info("Writing snapshot to %s", download_dir)

    # Loop through each dataset ID and download the data and metadata
    for ds_id in dataset_ids:
        download_dataset(ds_id, download_dir, token)

    return download_dir


def _prompt_sources_file() -> Path:
    """Prompt user for sources file path."""
    response = input(
        "Enter path to list of (e.g. ~/path/to/sources.txt): "
    ).strip()
    return Path(response)


def main() -> None:
    """CLI entrypoint for interactive or scripted use."""
    parser = argparse.ArgumentParser(
        description="Download Open Calgary data from a text file of source URLs."
    )
    parser.add_argument(
        "--sources-file",
        type=Path,
        default=None,
        help=(
            "Path to text file with one URL per line."
        ),
    )
    args = parser.parse_args()

    sources_file = args.sources_file if args.sources_file else _prompt_sources_file()
    output_dir = collect_from_sources_file(
        sources_file=sources_file,
    )
    print(f"Download complete: {output_dir}")


if __name__ == "__main__":
    main()
