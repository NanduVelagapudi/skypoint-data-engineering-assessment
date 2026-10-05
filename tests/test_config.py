from pathlib import Path

import pytest

from pipeline.config import REPO_ROOT, load_settings
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
