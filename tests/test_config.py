from pathlib import Path

import pytest

from pipeline.config import REPO_ROOT, MissingSecretError, load_settings, require_patient_key_secret
from pipeline.errors import ConfigError


def test_defaults_point_into_the_repository():
    settings = load_settings({})

    assert settings.data_dir == REPO_ROOT / "data"
    assert settings.landing_dir == REPO_ROOT / "data" / "landing"
    assert settings.output_dir == REPO_ROOT / "output"
    assert settings.raw_db_path == REPO_ROOT / "work" / "raw.duckdb"
    assert settings.schema_contract_path.is_file()
    assert settings.log_level == "INFO"


def test_environment_overrides_defaults(tmp_path: Path):
    settings = load_settings(
        {
            "DATA_DIR": str(tmp_path / "d"),
            "OUTPUT_DIR": str(tmp_path / "o"),
            "RAW_DB_PATH": str(tmp_path / "w" / "raw.duckdb"),
            "LOG_LEVEL": "debug",
        }
    )

    assert settings.data_dir == (tmp_path / "d").resolve()
    assert settings.output_dir == (tmp_path / "o").resolve()
    assert settings.raw_db_path == (tmp_path / "w" / "raw.duckdb").resolve()
    assert settings.log_level == "DEBUG"


@pytest.mark.parametrize("raw_db", ["raw.duckdb", "nested/deeper/raw.duckdb"])
def test_raw_db_inside_output_dir_is_refused(tmp_path: Path, raw_db: str):
    env = {"OUTPUT_DIR": str(tmp_path / "output"), "RAW_DB_PATH": str(tmp_path / "output" / raw_db)}

    with pytest.raises(ConfigError, match="outside OUTPUT_DIR"):
        load_settings(env)


def test_raw_db_in_sibling_with_similar_name_is_allowed(tmp_path: Path):
    env = {"OUTPUT_DIR": str(tmp_path / "output"), "RAW_DB_PATH": str(tmp_path / "output_raw" / "raw.duckdb")}

    assert load_settings(env).raw_db_path.parent.name == "output_raw"


def test_unknown_log_level_is_refused():
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        load_settings({"LOG_LEVEL": "VERBOSE"})


# --- Task 3: reference folder and patient_key secret ---


def test_reference_dir_defaults_under_data_dir_and_can_be_overridden(tmp_path: Path):
    assert load_settings({}).reference_dir == REPO_ROOT / "data" / "reference"
    assert load_settings({"DATA_DIR": str(tmp_path)}).reference_dir == (tmp_path / "reference").resolve()
    assert load_settings({"REFERENCE_DIR": str(tmp_path / "ref")}).reference_dir == (tmp_path / "ref").resolve()


def test_patient_key_secret_is_read_and_returned_as_bytes():
    settings = load_settings({"PATIENT_KEY_HMAC_SECRET": "unit-test-secret-ZZSECRET"})

    assert require_patient_key_secret(settings) == b"unit-test-secret-ZZSECRET"


def test_secret_never_appears_in_settings_repr():
    settings = load_settings({"PATIENT_KEY_HMAC_SECRET": "unit-test-secret-ZZSECRET"})

    assert "ZZSECRET" not in repr(settings) and "ZZSECRET" not in str(settings)


@pytest.mark.parametrize("env", [{}, {"PATIENT_KEY_HMAC_SECRET": ""}, {"PATIENT_KEY_HMAC_SECRET": "   "}])
def test_missing_or_blank_secret_is_a_config_error(env):
    with pytest.raises(MissingSecretError) as exc_info:
        require_patient_key_secret(load_settings(env))

    assert isinstance(exc_info.value, ConfigError)
    assert "PATIENT_KEY_HMAC_SECRET" in str(exc_info.value)
