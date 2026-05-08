"""Environment-backed runtime settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
LEGACY_ENV_PATH = PROJECT_ROOT / "src" / "calgary_dashboard" / "config" / ".env"

# Prefer the repo-root .env.
if DEFAULT_ENV_PATH.exists():
    load_dotenv(dotenv_path=DEFAULT_ENV_PATH)


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    data_root: str = os.getenv("DATA_ROOT", "")
    open_calgary_app_token: str = os.getenv("OPEN_CALGARY_APP_TOKEN", "")
    enmax_services_directory: str = os.getenv("ENMAX_SERVICES_DIRECTORY", "")
    neo4j_uri: str = os.getenv("NEO4J_URI", "neo4j://localhost")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "")
    neo4j_database: str = os.getenv("NEO4J_DATABASE", "neo4j")

    def validate(self) -> None:
        """Validate obvious invalid placeholder values."""
        if self.open_calgary_app_token == "YOUR_OPEN_CALGARY_APP_TOKEN":
            raise ValueError("Set OPEN_CALGARY_APP_TOKEN in your .env file.")
        # if self.neo4j_password == "YOUR_NEO4J_PASSWORD":
        #     raise ValueError("Set NEO4J_PASSWORD in your .env file.")


def get_settings() -> Settings:
    """Return a settings object using current environment values."""
    return Settings()

