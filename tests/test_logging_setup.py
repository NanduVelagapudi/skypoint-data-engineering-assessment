import io
import json
import logging

import pytest

from pipeline.logging_setup import ALLOWED_EXTRA_KEYS, configure_logging

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

BASE_KEYS = {"ts", "level", "event"}


def _lines(stream: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_log_lines_are_json_with_context_fields():
    stream = io.StringIO()
    configure_logging("INFO", stream)

    logging.getLogger("pipeline.batch_processor").info(
        "file_validated",
        extra={"step": "validate", "batch_id": "batch_001", "file_name": "f.csv", "received_count": 3},
    )

    [line] = _lines(stream)
    assert line["level"] == "INFO"
    assert line["event"] == "file_validated"
    assert line["step"] == "validate"
    assert line["batch_id"] == "batch_001"
    assert line["file_name"] == "f.csv"
    assert line["received_count"] == 3
    assert line["ts"].endswith("+00:00")


def test_unexpected_extra_keys_are_dropped():
    stream = io.StringIO()
    configure_logging("INFO", stream)

    logging.getLogger("pipeline.batch_processor").info(
        "file_validated",
        extra={
            "step": "validate",
            "patient_last_name": "Doe",
            "row_values": ["Doe", "Jane", "1980-01-01", "555-0100"],
        },
    )

    output = stream.getvalue()
    [line] = _lines(stream)
    assert set(line) == BASE_KEYS | {"step"}
    for fake_phi in ("patient_last_name", "row_values", "Doe", "Jane", "1980-01-01", "555-0100"):
        assert fake_phi not in output


def test_every_allowed_extra_key_is_emitted():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    extra = {key: f"value-of-{key}" for key in ALLOWED_EXTRA_KEYS}

    logging.getLogger("pipeline.batch_processor").info("all_fields", extra=extra)

    [line] = _lines(stream)
    assert set(line) == BASE_KEYS | set(ALLOWED_EXTRA_KEYS)
    for key in ALLOWED_EXTRA_KEYS:
        assert line[key] == f"value-of-{key}"


def test_exceptions_log_type_only_never_message_or_traceback():
    stream = io.StringIO()
    configure_logging("INFO", stream)

    try:
        raise ValueError("Doe, Jane 1980-01-01")
    except ValueError:
        logging.getLogger("pipeline.main").exception("pipeline_failed", extra={"step": "load"})

    output = stream.getvalue()
    [line] = _lines(stream)
    assert line["error_type"] == "ValueError"
    assert "Doe" not in output and "1980" not in output and "Traceback" not in output


def test_level_filters_lower_records():
    stream = io.StringIO()
    configure_logging("WARNING", stream)

    logging.getLogger("pipeline.x").info("hidden")
    logging.getLogger("pipeline.x").warning("shown")

    assert [line["event"] for line in _lines(stream)] == ["shown"]
