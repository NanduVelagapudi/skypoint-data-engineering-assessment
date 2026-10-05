"""Runtime settings, read from environment variables.

Defaults suit a local run from a repository checkout. Docker sets every value
explicitly in docker-compose.yml.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from pipeline.errors import ConfigError

REPO_ROOT = Path(__file__).resolve().parents[2]

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    output_dir: Path
    raw_db_path: Path
    schema_contract_path: Path
    log_level: str

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"


def _path(env: Mapping[str, str], name: str, default: Path) -> Path:
    value = env.get(name, "").strip()
    return Path(value).resolve() if value else default.resolve()


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build Settings from `env` (defaults to os.environ) and validate them."""
    env = os.environ if env is None else env

    settings = Settings(
        data_dir=_path(env, "DATA_DIR", REPO_ROOT / "data"),
        output_dir=_path(env, "OUTPUT_DIR", REPO_ROOT / "output"),
        raw_db_path=_path(env, "RAW_DB_PATH", REPO_ROOT / "work" / "raw.duckdb"),
        schema_contract_path=_path(
            env, "SCHEMA_CONTRACT_PATH", REPO_ROOT / "config" / "schema_contracts.json"
        ),
        log_level=env.get("LOG_LEVEL", "INFO").strip().upper() or "INFO",
    )

    if settings.log_level not in LOG_LEVELS:
        raise ConfigError(f"LOG_LEVEL must be one of {', '.join(LOG_LEVELS)}")

    # The raw database holds PHI; output/ is shared with analysts and committed.
    if settings.raw_db_path.is_relative_to(settings.output_dir):
        raise ConfigError("RAW_DB_PATH must be outside OUTPUT_DIR (raw layer contains PHI)")

    return settings
