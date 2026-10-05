"""Error types and batch-rejection reason codes.

Two kinds of failure, handled differently:

* Data/input validation failures (ValidationFailure, BatchRejected) reject a
  batch. They are expected, recorded in batch_audit, and the run exits 0.
* Pipeline/system failures (PipelineError and its subclasses, or any
  unexpected exception) stop the run with a non-zero exit code.

Nothing in this module may carry source field values: details refer to rows by
record number and to columns by names taken from configuration only.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ReasonCode(StrEnum):
    """Why a batch or one of its files was rejected."""

    MANIFEST_MISSING = "MANIFEST_MISSING"
    MANIFEST_INVALID = "MANIFEST_INVALID"
    BATCH_ID_MISMATCH = "BATCH_ID_MISMATCH"
    UNKNOWN_SOURCE_SYSTEM = "UNKNOWN_SOURCE_SYSTEM"
    FILE_MISSING = "FILE_MISSING"
    UNEXPECTED_FILE = "UNEXPECTED_FILE"
    SHA256_MISMATCH = "SHA256_MISMATCH"
    ENCODING_ERROR = "ENCODING_ERROR"
    CSV_PARSE_ERROR = "CSV_PARSE_ERROR"
    HEADER_MISSING = "HEADER_MISSING"
    UNKNOWN_SCHEMA_CHANGE = "UNKNOWN_SCHEMA_CHANGE"
    MALFORMED_RECORD = "MALFORMED_RECORD"
    ROW_COUNT_MISMATCH = "ROW_COUNT_MISMATCH"
    SIBLING_FILE_REJECTED = "SIBLING_FILE_REJECTED"


@dataclass(frozen=True)
class ValidationFailure:
    """One reason a file or batch failed validation. `detail` must be PHI-free."""

    code: ReasonCode
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.code}({self.detail})" if self.detail else str(self.code)


def format_reasons(failures: list[ValidationFailure]) -> str:
    """Render failures as the `reason` text stored in batch_audit."""
    return "; ".join(str(f) for f in failures)


class BatchRejected(Exception):
    """Raised when a batch-level check fails before any file can be validated."""

    def __init__(self, failures: list[ValidationFailure]):
        self.failures = failures
        super().__init__(format_reasons(failures))


class PipelineError(Exception):
    """A genuine pipeline/system failure: the run must exit non-zero."""


class ConfigError(PipelineError):
    """Invalid runtime settings or schema contract configuration."""
