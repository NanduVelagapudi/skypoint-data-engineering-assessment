"""Runtime settings, read from environment variables.

Defaults suit a local run from a repository checkout. Under Docker, every value
still arrives as an environment variable: docker-compose.yml sets the container
paths, and Compose loads the other values from .env.example and an optional .env.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pipeline.errors import ConfigError

REPO_ROOT = Path(__file__).resolve().parents[2]

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")

# Task 6 publish gate: a batch is not published when MORE than this share of its
# received rows fail error-level DQ checks. 0 blocks any error; 1 never blocks.
DEFAULT_DQ_GATE_MAX_ERROR_SHARE = Decimal("0.05")


class MissingSecretError(ConfigError):
    """PATIENT_KEY_HMAC_SECRET is not set. The class name is what the logs show."""


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    output_dir: Path
    raw_db_path: Path
    schema_contract_path: Path
    log_level: str
    reference_dir: Path
    facility_aliases_path: Path
    # Never shown in repr, so printing or logging Settings cannot leak it.
    patient_key_secret: str | None = field(default=None, repr=False)
    dq_gate_max_error_share: Decimal = DEFAULT_DQ_GATE_MAX_ERROR_SHARE

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"


def require_patient_key_secret(settings: Settings) -> bytes:
    """The HMAC key for patient_key. Missing or blank is a configuration error."""
    secret = settings.patient_key_secret
    if secret is None or not secret.strip():
        raise MissingSecretError("PATIENT_KEY_HMAC_SECRET must be set to build patient_key")
    return secret.encode("utf-8")


def _path(env: Mapping[str, str], name: str, default: Path) -> Path:
    value = env.get(name, "").strip()
    return Path(value).resolve() if value else default.resolve()


def _max_error_share(env: Mapping[str, str]) -> Decimal:
    """DQ_GATE_MAX_ERROR_SHARE as an exact Decimal between 0 and 1, e.g. 0.05 for 5%."""
    text = env.get("DQ_GATE_MAX_ERROR_SHARE", "").strip()
    if not text:
        return DEFAULT_DQ_GATE_MAX_ERROR_SHARE
    try:
        value = Decimal(text)
    except InvalidOperation:
        value = None
    if value is None or not value.is_finite() or not Decimal(0) <= value <= Decimal(1):
        raise ConfigError("DQ_GATE_MAX_ERROR_SHARE must be a decimal between 0 and 1")
    return value


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build Settings from `env` (defaults to os.environ) and validate them."""
    env = os.environ if env is None else env
    data_dir = _path(env, "DATA_DIR", REPO_ROOT / "data")

    settings = Settings(
        data_dir=data_dir,
        output_dir=_path(env, "OUTPUT_DIR", REPO_ROOT / "output"),
        raw_db_path=_path(env, "RAW_DB_PATH", REPO_ROOT / "work" / "raw.duckdb"),
        schema_contract_path=_path(
            env, "SCHEMA_CONTRACT_PATH", REPO_ROOT / "config" / "schema_contracts.json"
        ),
        log_level=env.get("LOG_LEVEL", "INFO").strip().upper() or "INFO",
        reference_dir=_path(env, "REFERENCE_DIR", data_dir / "reference"),
        facility_aliases_path=_path(env, "FACILITY_ALIASES_PATH", REPO_ROOT / "config" / "facility_aliases.json"),
        patient_key_secret=env.get("PATIENT_KEY_HMAC_SECRET"),
        dq_gate_max_error_share=_max_error_share(env),
    )

    if settings.log_level not in LOG_LEVELS:
        raise ConfigError(f"LOG_LEVEL must be one of {', '.join(LOG_LEVELS)}")

    # The raw database holds PHI; output/ is shared with analysts and committed.
    if settings.raw_db_path.is_relative_to(settings.output_dir):
        raise ConfigError("RAW_DB_PATH must be outside OUTPUT_DIR (raw layer contains PHI)")

    return settings
