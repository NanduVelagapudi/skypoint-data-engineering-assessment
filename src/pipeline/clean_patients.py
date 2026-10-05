"""Task 3 cleaning stage: clean.encounter_patients.

One row per accepted raw row, holding only PHI-free patient attributes:
patient_key, link status, sex, age band at admission and ZIP3, plus lineage
(batch_id, file_name, source_row_number) and the non-PHI source identifiers
(source_system, source_record_id) to trace the row back to raw.

Names, MRN, DOB, phone, full ZIP and chief_complaint are read from the raw
layer and used in memory only (linkage, age band, ZIP3); none of them is
written here, logged, or put in an error message.

The table is rebuilt from all accepted raw rows on every run, because linkage
looks across every batch and source system. Same secret and same raw rows give
identical output.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime

import duckdb

from pipeline.errors import PipelineError
from pipeline.parsers.dates import parse_date
from pipeline.parsers.patient import age_band, normalise_sex, zip3
from pipeline.patient_identity import IdentityRow, build_linkage_key, resolve_identities
from pipeline.source_conventions import SourceConventions

log = logging.getLogger(__name__)

TABLE = "clean.encounter_patients"

CLEAN_COLUMNS = (
    "batch_id",
    "file_name",
    "source_row_number",
    "source_system",
    "source_record_id",
    "patient_key",
    "patient_link_status",
    "patient_link_reason",
    "sex",
    "sex_reason",
    "age_band",
    "age_band_reason",
    "zip3",
    "zip3_reason",
)

# Raw columns that are PHI or may hold it. None may ever be a clean column.
PHI_COLUMNS = frozenset(
    {
        "patient_mrn",
        "patient_first_name",
        "patient_last_name",
        "patient_dob",
        "patient_phone",
        "patient_zip",
        "chief_complaint",
    }
)
if PHI_COLUMNS & set(CLEAN_COLUMNS):
    raise AssertionError("a PHI column is listed in CLEAN_COLUMNS")

_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    batch_id            VARCHAR NOT NULL,
    file_name           VARCHAR NOT NULL,
    source_row_number   INTEGER NOT NULL,
    source_system       VARCHAR NOT NULL,
    source_record_id    VARCHAR,
    patient_key         VARCHAR,
    patient_link_status VARCHAR NOT NULL,
    patient_link_reason VARCHAR,
    sex                 VARCHAR NOT NULL,
    sex_reason          VARCHAR,
    age_band            VARCHAR NOT NULL,
    age_band_reason     VARCHAR,
    zip3                VARCHAR,
    zip3_reason         VARCHAR,
    PRIMARY KEY (batch_id, file_name, source_row_number)
)
"""

# source_system comes from raw.ingested_files (validated against the manifest and
# schema contract), not from the delivered source_system column of the row.
_RAW_ROWS = """
SELECT e.batch_id, e.file_name, e.source_row_number, f.source_system, e.source_record_id,
       e.patient_mrn, e.patient_first_name, e.patient_last_name, e.patient_dob,
       e.patient_sex, e.patient_zip, e.admit_date, f.delivered_at
FROM raw.encounters AS e
JOIN raw.ingested_files AS f USING (batch_id, file_name)
ORDER BY e.batch_id, e.file_name, e.source_row_number
"""


def _delivered_at(text: str, batch_id: str) -> datetime:
    try:
        value = datetime.fromisoformat(text)
    except ValueError:
        value = None
    if value is None or value.tzinfo is None:
        raise PipelineError(f"delivered_at of {batch_id} is not an ISO-8601 timestamp with an offset")
    return value


def build_encounter_patients(
    con: duckdb.DuckDBPyConnection, conventions: dict[str, SourceConventions], secret: bytes
) -> int:
    """Rebuild clean.encounter_patients from every accepted raw row; returns the row count."""
    con.execute("CREATE SCHEMA IF NOT EXISTS clean")
    con.execute(_CREATE_TABLE)
    raw_rows = con.execute(_RAW_ROWS).fetchall()

    delivered: dict[str, datetime] = {}
    staged = []
    for (batch_id, file_name, row_number, source_system, record_id,
         mrn, first, last, dob_text, sex_text, zip_text, admit_text, delivered_text) in raw_rows:
        if batch_id not in delivered:
            delivered[batch_id] = _delivered_at(delivered_text, batch_id)
        source = conventions[source_system]

        def parse(text: str | None) -> date | None:
            return parse_date(
                text,
                date_order=source.date_order,
                delivered_at=delivered[batch_id],
                two_digit_year_century=source.two_digit_year_century,
            ).cleaned_value

        dob, admit = parse(dob_text), parse(admit_text)
        sex = normalise_sex(sex_text)
        identity = IdentityRow(
            source_system=source_system,
            mrn=mrn or "",
            linkage_key=build_linkage_key(last, first, dob, sex.cleaned_value),
            has_dob=dob is not None,
        )
        staged.append((batch_id, file_name, row_number, source_system, record_id, identity, sex,
                       age_band(dob, admit), zip3(zip_text)))

    identities = resolve_identities((s[5] for s in staged), secret)

    clean_rows = []
    for batch_id, file_name, row_number, source_system, record_id, identity_row, sex, band, zip_result in staged:
        identity = identities[(identity_row.source_system, identity_row.mrn.strip())]
        clean_rows.append(
            {
                "batch_id": batch_id,
                "file_name": file_name,
                "source_row_number": row_number,
                "source_system": source_system,
                "source_record_id": record_id,
                "patient_key": identity.patient_key,
                "patient_link_status": str(identity.link_status),
                "patient_link_reason": identity.reason and str(identity.reason),
                "sex": str(sex.cleaned_value),
                "sex_reason": sex.warning_flag and str(sex.warning_flag),
                "age_band": str(band.cleaned_value),
                "age_band_reason": band.warning_flag and str(band.warning_flag),
                "zip3": zip_result.cleaned_value,
                "zip3_reason": zip_result.reason_code and str(zip_result.reason_code),
            }
        )

    _replace_table(con, clean_rows)
    log.info("encounter_patients_built", extra={"step": "clean", "accepted_count": len(clean_rows)})
    return len(clean_rows)


def _replace_table(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Swap in the rebuilt rows in one transaction, loading them as one JSON document."""
    schema = json.dumps([{c: ("INTEGER" if c == "source_row_number" else "VARCHAR") for c in CLEAN_COLUMNS}])
    con.begin()
    try:
        con.execute(f"DELETE FROM {TABLE}")
        if rows:
            con.execute(
                f"INSERT INTO {TABLE} ({', '.join(CLEAN_COLUMNS)}) "
                f"SELECT {', '.join(f'r.{c}' for c in CLEAN_COLUMNS)} FROM (SELECT unnest(from_json(?, ?)) AS r)",
                [json.dumps(rows, ensure_ascii=False), schema],
            )
        con.commit()
    except BaseException as exc:
        con.rollback()
        log.error("encounter_patients_rolled_back", extra={"step": "clean", "error_type": type(exc).__name__})
        raise
