"""CSV export of every PHI-free clean and mart table, and the Task 6 DQ tables, to OUTPUT_DIR.

One file per table, named after the table. Each file has:
    the table's columns in their fixed (DDL) order as the header;
    rows ordered by the table's primary key, or for the DQ report and the
    quarantine (whose keys can be NULL) by their fixed ORDER_BY, NULLs first;
    ISO 8601 dates; TIMESTAMP values (stored as naive UTC) as
    YYYY-MM-DDTHH:MM:SSZ, with fractional seconds only when present;
    DECIMAL amounts with exactly two decimals; booleans as true/false;
    NULL as an empty string;
    UTF-8, LF line endings, written through a temp file and a rename.

Only tables in the clean, mart and ops schemas can be exported, and a table
with a PHI column (clean_patients.PHI_COLUMNS, also as a *_raw column) is
refused, so nothing from the raw layer, no chief_complaint and no patient name
reaches OUTPUT_DIR. The ops tables exported are only the DQ report and the
quarantine, which hold codes, keys and lineage. batch_audit.csv is written
separately by batch_audit.export_csv, and the Task 7 export
chronic_acute_encounters.csv by chronic_acute_export with write_csv.
"""

from __future__ import annotations

import csv
import logging
import os
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from pipeline.clean_patients import PHI_COLUMNS
from pipeline.errors import PipelineError

log = logging.getLogger(__name__)

NULLS_FIRST = " NULLS FIRST"

# table -> the columns that order its rows: its primary key, or the fixed order of a DQ table
# (a column suffixed NULLS_FIRST puts its NULLs first)
EXPORTED_TABLES = {
    "clean.encounter_versions": ("version_key",),
    "clean.encounter_row_outcomes": ("batch_id", "file_name", "source_row_number"),
    "clean.encounter_patients": ("batch_id", "file_name", "source_row_number"),
    "clean.encounter_version_fields": ("version_key",),
    "mart.dim_date": ("date_key",),
    "mart.dim_facility": ("facility_id",),
    "mart.dim_diagnosis": ("icd10_code",),
    "mart.dim_payer": ("payer_category",),
    "mart.dim_provider": ("provider_sk",),
    "mart.dim_patient": ("patient_key",),
    "mart.fact_encounter_version": ("version_key",),
    "mart.fact_encounter_current": ("encounter_key",),
    "clean.version_dq_issues": ("version_key", "check_code"),
    "ops.dq_report": ("batch_id", f"file_name{NULLS_FIRST}", "check_code"),
    "ops.quarantine": ("batch_id", "file_name", f"source_row_number{NULLS_FIRST}", "quarantine_level"),
}
EXPORT_SCHEMAS = ("clean", "mart", "ops")


def file_name(table: str) -> str:
    return f"{table.split('.', 1)[1]}.csv"


def format_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise ValueError("TIMESTAMP values are stored as naive UTC")
        return value.isoformat(timespec="seconds" if value.microsecond == 0 else "microseconds") + "Z"
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    if isinstance(value, float):
        raise ValueError("float values are not exported; amounts must be DECIMAL")
    return str(value)


def check_no_phi_columns(name: str, columns: Sequence[str]) -> None:
    phi = {c for c in columns if c in PHI_COLUMNS or c.removesuffix("_raw") in PHI_COLUMNS}
    if phi:
        raise PipelineError(f"{name} has PHI columns and cannot be exported")


def _check_exportable(table: str, columns: list[str]) -> None:
    if table.split(".", 1)[0] not in EXPORT_SCHEMAS:
        raise PipelineError(f"{table} is not in an exportable schema")
    check_no_phi_columns(table, columns)


def write_csv(path: Path, columns: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    """Write a header and formatted rows to `path` through a temp file and a rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(columns)
            writer.writerows([format_value(v) for v in row] for row in rows)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def export_table(con: duckdb.DuckDBPyConnection, table: str, order_by: tuple[str, ...], path: Path) -> int:
    """Write one table to `path`; returns the number of rows."""
    columns = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
    _check_exportable(table, columns)
    if not {c.removesuffix(NULLS_FIRST) for c in order_by} <= set(columns):
        raise PipelineError(f"{table} lacks its export ordering columns")
    rows = con.execute(f"SELECT {', '.join(columns)} FROM {table} ORDER BY {', '.join(order_by)}").fetchall()
    write_csv(path, columns, rows)
    return len(rows)


def export_tables(con: duckdb.DuckDBPyConnection, output_dir: Path) -> dict[str, int]:
    """Export every table in EXPORTED_TABLES; returns rows per file name."""
    counts = {}
    for table, order_by in EXPORTED_TABLES.items():
        counts[file_name(table)] = export_table(con, table, order_by, output_dir / file_name(table))
        log.info("table_exported", extra={"step": "export", "file_name": file_name(table)})
    return counts
