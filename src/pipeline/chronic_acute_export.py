"""Task 7: the required export chronic_acute_encounters.csv.

One row per encounter, from its current version (mart.fact_encounter_current),
when all of these hold:
    1. the facility resolved to the facility master
    2. claim_status IS DISTINCT FROM 'VOID' (a NULL claim status is kept)
    3. encounter_type is INPATIENT, OBSERVATION or EMERGENCY
    4. the primary diagnosis is in the ICD-10 reference with is_chronic = Y
       (a valid code missing from the reference has is_chronic unknown: left out)
    5. admit_date is in calendar year 2024
    6. billed_amount_usd is valid (not NULL) and at least 5,000.00
and, as in README queries 1 and 5, the current version has no Task 6 ERROR in
clean.version_dq_issues. There is no fallback to an older version. The ERRORs
(facility unresolved, admit date unusable) also fail conditions 1 and 5.

readmit_30d_flag, for INPATIENT rows: 1 when the same patient_key has another
INPATIENT encounter admitted 1 to 30 days after this row's discharge_date, at
any facility; otherwise 0. Blank for other encounter types. The other
encounter is searched among all current encounters, not only this export's
rows, and must be non-VOID (IS DISTINCT FROM), at a resolved facility, and
have a valid admit_date and a valid discharge_date (both parsed, not NULL).
A row with a NULL discharge_date, or a NULL patient_key, gets 0: there is no
window, or no patient to match.

Columns are in the brief's order. attending_specialty_at_encounter and
attending_employment_status_at_encounter come from the dim_provider row the
point-in-time lookup chose for the version (provider_sk), and are NULL when
there is none. The lineage columns are the fact's: the row that supplied the
current version. Rows are sorted by admit_date, source_system,
source_record_id. Formats are the table exports' (exports.write_csv): ISO
dates, two-decimal amounts, NULL as an empty string, LF line endings, written
through a temp file and a rename. Only mart tables and the DQ issues are read.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from pipeline.dimensions import DIAGNOSIS_TABLE, FACILITY_TABLE, PROVIDER_TABLE
from pipeline.dq_rules import Severity
from pipeline.encounter_facts import CURRENT_FACT_TABLE
from pipeline.errors import PipelineError
from pipeline.exports import check_no_phi_columns, write_csv
from pipeline.parsers.categorical import ClaimStatus, EncounterType
from pipeline.version_dq import TABLE as ISSUES_TABLE

log = logging.getLogger(__name__)

FILE_NAME = "chronic_acute_encounters.csv"

COLUMNS = (
    "encounter_key",
    "source_system",
    "source_record_id",
    "facility_id",
    "facility_name",
    "facility_type",
    "patient_key",
    "age_band",
    "sex",
    "patient_zip3",
    "admit_date",
    "discharge_date",
    "length_of_stay_days",
    "encounter_type",
    "primary_dx_code",
    "dx_description",
    "chronic_category",
    "attending_npi",
    "attending_specialty_at_encounter",
    "attending_employment_status_at_encounter",
    "payer_category",
    "billed_amount_usd",
    "claim_status",
    "readmit_30d_flag",
    "version_count",
    "source_batch_id",
    "source_file_name",
    "source_row_number",
)

ENCOUNTER_TYPES = (EncounterType.INPATIENT, EncounterType.OBSERVATION, EncounterType.EMERGENCY)
ADMIT_YEAR = 2024
MIN_BILLED_AMOUNT = "5000.00"
READMIT_WINDOW_DAYS = (1, 30)

INPATIENT, VOID = EncounterType.INPATIENT, ClaimStatus.VOID

QUERY = f"""
WITH readmissions AS (
    -- Every current INPATIENT encounter that can count as a readmission, in this export or not.
    SELECT encounter_key, patient_key, admit_date
    FROM {CURRENT_FACT_TABLE}
    WHERE encounter_type = '{INPATIENT}'
      AND claim_status IS DISTINCT FROM '{VOID}'
      AND facility_id IS NOT NULL
      AND admit_date IS NOT NULL
      AND discharge_date IS NOT NULL
      AND patient_key IS NOT NULL
)
SELECT f.encounter_key, f.source_system, f.source_record_id,
       f.facility_id, fac.facility_name, fac.facility_type,
       f.patient_key, f.age_band, f.sex, f.zip3 AS patient_zip3,
       f.admit_date, f.discharge_date, f.length_of_stay_days, f.encounter_type,
       f.primary_dx_code, dx.description AS dx_description, dx.category AS chronic_category,
       f.attending_npi,
       p.specialty AS attending_specialty_at_encounter,
       p.employment_status AS attending_employment_status_at_encounter,
       f.payer_category, f.billed_amount_usd, f.claim_status,
       CASE
           WHEN f.encounter_type <> '{INPATIENT}' THEN NULL
           WHEN EXISTS (
               SELECT 1 FROM readmissions AS r
               WHERE r.patient_key = f.patient_key
                 AND r.encounter_key <> f.encounter_key
                 AND date_diff('day', f.discharge_date, r.admit_date)
                     BETWEEN {READMIT_WINDOW_DAYS[0]} AND {READMIT_WINDOW_DAYS[1]}
           ) THEN 1
           ELSE 0
       END AS readmit_30d_flag,
       f.version_count, f.source_batch_id, f.source_file_name, f.source_row_number
FROM {CURRENT_FACT_TABLE} AS f
JOIN {FACILITY_TABLE} AS fac ON fac.facility_id = f.facility_id
JOIN {DIAGNOSIS_TABLE} AS dx ON dx.icd10_code = f.primary_dx_code
LEFT JOIN {PROVIDER_TABLE} AS p ON p.provider_sk = f.provider_sk
WHERE f.claim_status IS DISTINCT FROM '{VOID}'
  AND f.encounter_type IN ({", ".join(f"'{t}'" for t in ENCOUNTER_TYPES)})
  AND dx.in_reference AND dx.is_chronic
  AND year(f.admit_date) = {ADMIT_YEAR}
  AND f.billed_amount_usd >= {MIN_BILLED_AMOUNT}
  AND NOT EXISTS (
      SELECT 1 FROM {ISSUES_TABLE} AS i
      WHERE i.version_key = f.version_key AND i.severity = '{Severity.ERROR}'
  )
ORDER BY f.admit_date, f.source_system, f.source_record_id
"""


def export_rows(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """The export's rows, in COLUMNS order and sorted."""
    cursor = con.execute(QUERY)
    if tuple(d[0] for d in cursor.description) != COLUMNS:
        raise PipelineError("the Task 7 query does not return the export's columns")
    return cursor.fetchall()


def export_chronic_acute(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    """Write the export to `path`; returns the number of rows."""
    check_no_phi_columns(FILE_NAME, COLUMNS)
    rows = export_rows(con)
    write_csv(path, COLUMNS, rows)
    log.info("table_exported", extra={"step": "export", "file_name": FILE_NAME})
    return len(rows)
