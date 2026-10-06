"""Task 2 fields of every encounter version: clean.encounter_version_fields.

One row per version (version_key), parsed from the version's first-seen raw
row with the Task 2 parsers. Every field keeps its raw value, the cleaned
value and why it is NULL, side by side:
    <field>_raw      the value as delivered
    <cleaned>        the cleaned value, NULL when it could not be cleaned
    <cleaned>_reason why it is NULL (set exactly when the cleaned value is NULL)
    <cleaned>_warning something notable about a valid value, where a parser
                     reports one (blank category -> UNKNOWN, ICD-10 code not in
                     the reference, NPI in no roster, discharge before admit)

Only non-PHI fields are read: no MRN, names, DOB, phone, ZIP or chief_complaint.
Dates are checked against the delivery time of the version's first-seen batch.

The table is insert-only: each accepted batch adds rows for its new versions,
inside the batch transaction, and a version's row never changes. A change to
the reference data or the facility aliases therefore needs
python -m pipeline.main --rebuild-derived.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime

import duckdb

from pipeline.encounter_history import VERSIONS_TABLE
from pipeline.parsers.amount import parse_amount
from pipeline.parsers.categorical import parse_claim_status, parse_encounter_type, parse_payer_category
from pipeline.parsers.dates import admit_period, discharge_before_admit, parse_date
from pipeline.parsers.facility import resolve_facility
from pipeline.parsers.icd10 import parse_diagnosis
from pipeline.parsers.npi import npi_not_in_roster, parse_npi
from pipeline.parsers.result import ParseResult
from pipeline.reference_data import CleaningReference
from pipeline.source_conventions import SourceConventions

log = logging.getLogger(__name__)

TABLE = "clean.encounter_version_fields"

KEY_COLUMNS = (
    "version_key",
    "encounter_key",
    "source_system",
    "source_record_id",
    "source_batch_id",
    "source_file_name",
    "source_row_number",
)
FIELD_COLUMNS = (
    "facility_name_raw", "facility_id", "facility_id_reason",
    "admit_date_raw", "admit_date", "admit_date_reason", "admit_year", "admit_quarter", "admit_month",
    "discharge_date_raw", "discharge_date", "discharge_date_reason", "discharge_date_warning",
    "encounter_type_raw", "encounter_type", "encounter_type_reason", "encounter_type_warning",
    "claim_status_raw", "claim_status", "claim_status_reason",
    "payer_name_raw", "payer_category", "payer_category_reason", "payer_category_warning",
    "primary_dx_code_raw", "primary_dx_code", "primary_dx_code_reason", "primary_dx_code_warning",
    "attending_npi_raw", "attending_npi", "attending_npi_reason", "attending_npi_warning",
    "billed_amount_raw", "billed_amount_usd", "billed_amount_usd_reason",
)  # fmt: skip
COLUMNS = KEY_COLUMNS + FIELD_COLUMNS

# The raw columns read, all non-PHI.
RAW_COLUMNS = (
    "facility_name",
    "admit_date",
    "discharge_date",
    "encounter_type",
    "claim_status",
    "payer_name",
    "primary_dx_code",
    "attending_npi",
    "billed_amount",
)

_TYPES = {
    "source_row_number": "INTEGER",
    "admit_date": "DATE",
    "discharge_date": "DATE",
    "admit_year": "INTEGER",
    "admit_quarter": "INTEGER",
    "admit_month": "INTEGER",
    "billed_amount_usd": "DECIMAL(18,2)",
}

_CREATE_TABLE = (
    f"CREATE TABLE IF NOT EXISTS {TABLE} (\n    "
    + ",\n    ".join(f"{c} {_TYPES.get(c, 'VARCHAR')}" + (" NOT NULL" if c in KEY_COLUMNS else "") for c in COLUMNS)
    + ",\n    PRIMARY KEY (version_key)\n)"
)


def _field(prefix: str, result: ParseResult, *, warning: bool = False) -> dict[str, object]:
    """The cleaned value, its reason and (optionally) its warning, under the cleaned column's name."""
    cleaned = result.cleaned_value
    values = {
        prefix: str(cleaned) if cleaned is not None else None,
        f"{prefix}_reason": result.reason_code and str(result.reason_code),
    }
    if warning:
        values[f"{prefix}_warning"] = result.warning_flag and str(result.warning_flag)
    return values


def clean_fields(
    raw: Mapping[str, str | None],
    *,
    source_system: str,
    delivered_at: datetime,
    convention: SourceConventions,
    reference: CleaningReference,
) -> dict[str, object]:
    """The FIELD_COLUMNS of one version from its raw values. Pure: no I/O."""

    def parse(text: str | None) -> ParseResult:
        return parse_date(
            text,
            date_order=convention.date_order,
            delivered_at=delivered_at,
            two_digit_year_century=convention.two_digit_year_century,
        )

    admit, discharge = parse(raw["admit_date"]), parse(raw["discharge_date"])
    period = admit_period(admit.cleaned_value) if admit.cleaned_value else None
    npi = parse_npi(raw["attending_npi"])
    flag = discharge_before_admit(admit.cleaned_value, discharge.cleaned_value)

    fields: dict[str, object] = {"facility_name_raw": raw["facility_name"]}
    fields |= _field("facility_id", resolve_facility(raw["facility_name"], source_system, reference.facility_index))
    fields |= {"admit_date_raw": raw["admit_date"]} | _field("admit_date", admit)
    fields |= {
        "admit_year": period and period.year,
        "admit_quarter": period and period.quarter,
        "admit_month": period and period.month,
    }
    fields |= {"discharge_date_raw": raw["discharge_date"]} | _field("discharge_date", discharge)
    fields["discharge_date_warning"] = flag and str(flag)
    fields |= {"encounter_type_raw": raw["encounter_type"]}
    fields |= _field("encounter_type", parse_encounter_type(raw["encounter_type"]), warning=True)
    fields |= {"claim_status_raw": raw["claim_status"]} | _field("claim_status", parse_claim_status(raw["claim_status"]))
    fields |= {"payer_name_raw": raw["payer_name"]}
    fields |= _field("payer_category", parse_payer_category(raw["payer_name"]), warning=True)
    fields |= {"primary_dx_code_raw": raw["primary_dx_code"]}
    fields |= _field("primary_dx_code", parse_diagnosis(raw["primary_dx_code"], reference.icd10_codes), warning=True)
    fields |= {"attending_npi_raw": raw["attending_npi"]} | _field("attending_npi", npi)
    not_in_roster = npi_not_in_roster(npi.cleaned_value, reference.roster_npis)
    fields["attending_npi_warning"] = not_in_roster and str(not_in_roster)
    fields |= {"billed_amount_raw": raw["billed_amount"]}
    fields |= _field("billed_amount_usd", parse_amount(raw["billed_amount"], convention.amount_unit))
    if tuple(fields) != FIELD_COLUMNS:
        raise AssertionError("clean_fields columns are out of step with FIELD_COLUMNS")
    return fields


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS clean")
    con.execute(_CREATE_TABLE)


def clear(con: duckdb.DuckDBPyConnection) -> None:
    """Empty the table, inside the caller's transaction (rebuild only)."""
    con.execute(f"DELETE FROM {TABLE}")


def missing_count(con: duckdb.DuckDBPyConnection, batch_id: str | None = None) -> int:
    """Versions (of one batch, or of all) that have no fields row."""
    where, params = ("AND v.first_seen_batch_id = ?", [batch_id]) if batch_id else ("", [])
    return con.execute(
        f"SELECT count(*) FROM {VERSIONS_TABLE} v "
        f"WHERE NOT EXISTS (SELECT 1 FROM {TABLE} x WHERE x.version_key = v.version_key) {where}",
        params,
    ).fetchone()[0]


def add_batch_fields(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
    conventions: Mapping[str, SourceConventions],
    reference: CleaningReference,
) -> int:
    """Insert fields for the versions first seen in `batch_id` that have none; returns rows written.

    Runs inside the caller's (batch) transaction. Reads only the batch's new
    versions, joined to their first-seen raw rows.
    """
    records = con.execute(
        f"""
        SELECT v.version_key, v.encounter_key, v.source_system, v.source_record_id,
               v.first_seen_batch_id, v.first_seen_file_name, v.first_seen_source_row_number,
               f.delivered_at, {", ".join(f"e.{c}" for c in RAW_COLUMNS)}
        FROM {VERSIONS_TABLE} AS v
        JOIN raw.encounters AS e
          ON e.batch_id = v.first_seen_batch_id
         AND e.file_name = v.first_seen_file_name
         AND e.source_row_number = v.first_seen_source_row_number
        JOIN raw.ingested_files AS f ON f.batch_id = e.batch_id AND f.file_name = e.file_name
        WHERE v.first_seen_batch_id = ?
          AND NOT EXISTS (SELECT 1 FROM {TABLE} x WHERE x.version_key = v.version_key)
        ORDER BY v.version_key
        """,
        [batch_id],
    ).fetchall()

    rows = []
    delivered: dict[str, datetime] = {}
    for record in records:
        keys = dict(zip(KEY_COLUMNS, record[:7], strict=True))
        delivered_text, raw_values = record[7], dict(zip(RAW_COLUMNS, record[8:], strict=True))
        if delivered_text not in delivered:
            delivered[delivered_text] = datetime.fromisoformat(delivered_text)
        source_system = keys["source_system"]
        rows.append(
            keys
            | clean_fields(
                raw_values,
                source_system=source_system,
                delivered_at=delivered[delivered_text],
                convention=conventions[source_system],
                reference=reference,
            )
        )
    _insert(con, rows)
    return len(rows)


def _insert(con: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]) -> None:
    """One JSON document (see raw_store for why), cast to the column types in SQL."""
    if not rows:
        return
    schema = json.dumps([{c: ("INTEGER" if _TYPES.get(c) == "INTEGER" else "VARCHAR") for c in COLUMNS}])
    selected = ", ".join(
        f"CAST(r.{c} AS {_TYPES[c]})" if _TYPES.get(c) in ("DATE", "DECIMAL(18,2)") else f"r.{c}" for c in COLUMNS
    )
    con.execute(
        f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) SELECT {selected} FROM (SELECT unnest(from_json(?, ?)) AS r)",
        [json.dumps(rows, ensure_ascii=False), schema],
    )
