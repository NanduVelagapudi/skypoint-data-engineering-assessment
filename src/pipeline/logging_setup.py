"""Structured JSON logging on stdout.

Modules use logging.getLogger(__name__) (a child of the "pipeline" logger) and
attach context through `extra`, e.g.
    log.info("file_validated", extra={"step": "validate", "batch_id": ..., "file_name": ...})

PHI rule: never pass source field values in the message or in `extra`. Rows are
referred to by batch_id, file_name and source_row_number only. Only the `extra`
keys in ALLOWED_EXTRA_KEYS are emitted; any other key is dropped. For
exceptions only the exception type is logged, never its message or traceback,
because a third-party message could quote source data.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import TextIO

# The only `extra` keys that may appear in a log line, in output order.
ALLOWED_EXTRA_KEYS = (
    "step",
    "batch_id",
    "file_name",
    "source_system",
    "schema_version",
    "source_row_number",
    "expected_count",
    "received_count",
    "accepted_count",
    "duplicate_count",
    "stale_count",
    "quarantined_count",
    "error_count",
    "warning_count",
    "reason_code",
    "error_type",
    "status",
    "duration_ms",
)

LOGGER_NAME = "pipeline"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "event": record.getMessage(),
        }
        attrs = vars(record)
        for key in ALLOWED_EXTRA_KEYS:
            if key in attrs:
                payload[key] = attrs[key]
        if record.exc_info and record.exc_info[0] is not None:
            payload["error_type"] = record.exc_info[0].__name__
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(level: str = "INFO", stream: TextIO | None = None) -> logging.Logger:
    """Send the pipeline logger's records to `stream` (stdout by default) as JSON lines."""
    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(JsonFormatter())

    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers[:] = [handler]
    logger.setLevel(level)
    return logger
