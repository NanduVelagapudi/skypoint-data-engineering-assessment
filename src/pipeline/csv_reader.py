"""Decode and parse one delivered CSV file, preserving every value as delivered.

Parsing uses only the standard csv module, so quoted commas, escaped quotes,
quoted newlines, CRLF and LF are handled by the parser, never by line
splitting. Every field stays text, exactly as delivered.

Failures refer to records by number only. Field values and csv module error
messages are never included.
"""

from __future__ import annotations

import codecs
import csv
import io
from dataclasses import dataclass

from pipeline.errors import ReasonCode, ValidationFailure

# Malformed records reported individually; the rest are summarised as a count.
MAX_REPORTED_MALFORMED = 5


@dataclass(frozen=True)
class CsvParseResult:
    header: list[str] | None
    records: list[list[str]]  # data records after the header, as delivered
    failures: list[ValidationFailure]
    complete: bool  # False when decoding or parsing stopped before the end

    @property
    def record_count(self) -> int | None:
        """Data records excluding the header; None if the file could not be read to the end."""
        return len(self.records) if self.complete else None


def _decode(content: bytes, encoding: str) -> str:
    codec = codecs.lookup(encoding).name
    if codec == "utf-8":
        codec = "utf-8-sig"  # same decoding, but strips a leading byte-order mark
    return content.decode(codec, errors="strict")


def parse_csv(content: bytes, encoding: str = "utf-8") -> CsvParseResult:
    """Parse raw file bytes into a header and data records."""
    try:
        text = _decode(content, encoding)
    except UnicodeDecodeError as exc:
        failure = ValidationFailure(ReasonCode.ENCODING_ERROR, f"byte_offset={exc.start}")
        return CsvParseResult(None, [], [failure], complete=False)

    header: list[str] | None = None
    records: list[list[str]] = []
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    try:
        for row in reader:
            if header is None:
                header = row
            else:
                records.append(row)
    except csv.Error:
        position = "header" if header is None else f"record={len(records) + 1}"
        failure = ValidationFailure(ReasonCode.CSV_PARSE_ERROR, position)
        return CsvParseResult(header, [], [failure], complete=False)

    if not header:  # empty file, or a blank first line
        return CsvParseResult(None, [], [ValidationFailure(ReasonCode.HEADER_MISSING)], complete=False)

    malformed = [
        (number, len(record))
        for number, record in enumerate(records, start=1)
        if len(record) != len(header)
    ]
    failures = [
        ValidationFailure(
            ReasonCode.MALFORMED_RECORD, f"record={number},fields={width},expected={len(header)}"
        )
        for number, width in malformed[:MAX_REPORTED_MALFORMED]
    ]
    if len(malformed) > MAX_REPORTED_MALFORMED:
        extra = len(malformed) - MAX_REPORTED_MALFORMED
        failures.append(ValidationFailure(ReasonCode.MALFORMED_RECORD, f"additional_records={extra}"))

    return CsvParseResult(header, records, failures, complete=True)
