"""Task 5 facts and the patient dimension, rebuilt deterministically on every run.

    mart.fact_encounter_version  one row per encounter version (version_key)
    mart.fact_encounter_current  one row per encounter (encounter_key): its
                                 current version, plus version_count
    mart.dim_patient             one row per patient_key

A version's row combines what earlier stages already hold, all PHI-free:
    clean.encounter_versions        keys, last_updated_ts_utc, first_seen_batch_id,
                                    arrival_outcome, first-seen lineage
    clean.encounter_version_fields  the cleaned Task 2 fields and their reasons
    clean.encounter_patients        patient_key, age band, sex and ZIP3 of the
                                    first-seen row (joined on its lineage)
    mart.dim_provider               provider_sk, point-in-time on admit_date
No raw table is read.

Every nullable value has a <column>_reason that is set whenever it is NULL.
age_band and sex are never NULL (UNKNOWN carries the reason). The lineage
columns source_batch_id, source_file_name and source_row_number point to the
row that supplied the version (its first arrival); first_seen_batch_id is the
same batch, kept under the name the as-of queries use.

length_of_stay_days = discharge_date - admit_date in days (a same-day stay is
0). It is NULL with LENGTH_OF_STAY_ADMIT_UNAVAILABLE or
LENGTH_OF_STAY_DISCHARGE_UNAVAILABLE when a date is missing (admit checked
first), or DISCHARGE_BEFORE_ADMIT.

dim_patient holds patient_key, the link status and reason, and sex and ZIP3
from the patient's most recent version (latest last_updated_ts_utc, then the
latest lineage), with attributes_vary when the patient's versions disagree on
sex or ZIP3. The age band depends on the admission, so it stays on the facts.

Task 6 will flag versions in a separate table keyed by version_key; these
tables do not change for it.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Sequence
from datetime import date

import duckdb

from pipeline.dimensions import PROVIDER_TABLE, SCHEMA, ProviderLookup, ProviderRow, insert_rows
from pipeline.encounter_history import CURRENT_VIEW, VERSIONS_TABLE
from pipeline.parsers.result import FieldReason
from pipeline.version_fields import TABLE as FIELDS_TABLE

log = logging.getLogger(__name__)

VERSION_FACT_TABLE = "mart.fact_encounter_version"
CURRENT_FACT_TABLE = "mart.fact_encounter_current"
PATIENT_TABLE = "mart.dim_patient"
PATIENTS_TABLE = "clean.encounter_patients"  # clean_patients.TABLE, named here to avoid importing the HMAC stage

_FACT_COLUMNS = """
    version_key VARCHAR NOT NULL, encounter_key VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL, source_record_id VARCHAR NOT NULL,
    last_updated_ts_utc TIMESTAMP NOT NULL, first_seen_batch_id VARCHAR NOT NULL, arrival_outcome VARCHAR NOT NULL,
    source_batch_id VARCHAR NOT NULL, source_file_name VARCHAR NOT NULL, source_row_number INTEGER NOT NULL,
    facility_id VARCHAR, facility_id_reason VARCHAR,
    patient_key VARCHAR, patient_key_reason VARCHAR,
    age_band VARCHAR NOT NULL, age_band_reason VARCHAR,
    sex VARCHAR NOT NULL, sex_reason VARCHAR,
    zip3 VARCHAR, zip3_reason VARCHAR,
    attending_npi VARCHAR, attending_npi_reason VARCHAR,
    provider_sk VARCHAR, provider_sk_reason VARCHAR,
    admit_date DATE, admit_date_reason VARCHAR,
    discharge_date DATE, discharge_date_reason VARCHAR,
    length_of_stay_days INTEGER, length_of_stay_days_reason VARCHAR,
    encounter_type VARCHAR, encounter_type_reason VARCHAR,
    claim_status VARCHAR, claim_status_reason VARCHAR,
    primary_dx_code VARCHAR, primary_dx_code_reason VARCHAR,
    payer_category VARCHAR, payer_category_reason VARCHAR,
    billed_amount_usd DECIMAL(18,2), billed_amount_usd_reason VARCHAR"""

_DDL = {
    VERSION_FACT_TABLE: _FACT_COLUMNS + ",\n    PRIMARY KEY (version_key)",
    CURRENT_FACT_TABLE: _FACT_COLUMNS + """,
    version_count INTEGER NOT NULL, encounter_first_seen_batch_id VARCHAR NOT NULL,
    PRIMARY KEY (encounter_key)""",
    PATIENT_TABLE: """
    patient_key VARCHAR NOT NULL PRIMARY KEY,
    patient_link_status VARCHAR NOT NULL, patient_link_reason VARCHAR,
    sex VARCHAR NOT NULL, sex_reason VARCHAR,
    zip3 VARCHAR, zip3_reason VARCHAR,
    attributes_vary BOOLEAN NOT NULL,
    source_batch_id VARCHAR NOT NULL, source_file_name VARCHAR NOT NULL, source_row_number INTEGER NOT NULL""",
}

# Cleaned fields copied onto the facts unchanged, each with its reason.
_COPIED_FIELDS = (
    "facility_id", "attending_npi", "admit_date", "discharge_date", "encounter_type", "claim_status",
    "primary_dx_code", "payer_category", "billed_amount_usd",
)  # fmt: skip

_SOURCE_ROWS = f"""
SELECT v.version_key, v.encounter_key, v.source_system, v.source_record_id, v.last_updated_ts_utc,
       v.first_seen_batch_id, v.arrival_outcome,
       v.first_seen_batch_id AS source_batch_id, v.first_seen_file_name AS source_file_name,
       v.first_seen_source_row_number AS source_row_number,
       {", ".join(f"f.{c}, f.{c}_reason" for c in _COPIED_FIELDS)},
       p.patient_key, p.patient_link_status, p.patient_link_reason,
       p.age_band, p.age_band_reason, p.sex, p.sex_reason, p.zip3, p.zip3_reason
FROM {VERSIONS_TABLE} AS v
JOIN {FIELDS_TABLE} AS f USING (version_key)
JOIN {PATIENTS_TABLE} AS p
  ON p.batch_id = v.first_seen_batch_id
 AND p.file_name = v.first_seen_file_name
 AND p.source_row_number = v.first_seen_source_row_number
ORDER BY v.version_key
"""


def length_of_stay(admit: date | None, discharge: date | None) -> tuple[int | None, str | None]:
    """(days, None), or (None, reason). A same-day stay is 0."""
    if admit is None:
        return None, str(FieldReason.LENGTH_OF_STAY_ADMIT_UNAVAILABLE)
    if discharge is None:
        return None, str(FieldReason.LENGTH_OF_STAY_DISCHARGE_UNAVAILABLE)
    if discharge < admit:
        return None, str(FieldReason.DISCHARGE_BEFORE_ADMIT)
    return (discharge - admit).days, None


def _provider_lookup(con: duckdb.DuckDBPyConnection) -> ProviderLookup:
    cursor = con.execute(f"SELECT * FROM {PROVIDER_TABLE}")
    names = [d[0] for d in cursor.description]
    return ProviderLookup(ProviderRow(**dict(zip(names, row, strict=True))) for row in cursor.fetchall())


def version_fact_rows(con: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """(fact_encounter_version rows, dim_patient rows), in key order."""
    lookup = _provider_lookup(con)
    cursor = con.execute(_SOURCE_ROWS)
    names = [d[0] for d in cursor.description]
    facts, by_patient = [], defaultdict(list)
    for record in cursor.fetchall():
        source = dict(zip(names, record, strict=True))
        provider_sk, provider_reason = lookup.at(source["attending_npi"], source["admit_date"], source["attending_npi_reason"])
        stay, stay_reason = length_of_stay(source["admit_date"], source["discharge_date"])
        fact = {
            **{k: source[k] for k in ("version_key", "encounter_key", "source_system", "source_record_id",
                                      "last_updated_ts_utc", "first_seen_batch_id", "arrival_outcome",
                                      "source_batch_id", "source_file_name", "source_row_number",
                                      "facility_id", "facility_id_reason")},
            "patient_key": source["patient_key"],
            # A patient_key is missing only for a blank MRN; its link reason says so.
            "patient_key_reason": source["patient_link_reason"] if source["patient_key"] is None else None,
            **{k: source[k] for k in ("age_band", "age_band_reason", "sex", "sex_reason", "zip3", "zip3_reason",
                                      "attending_npi", "attending_npi_reason")},
            "provider_sk": provider_sk,
            "provider_sk_reason": provider_reason,
            **{k: source[k] for k in ("admit_date", "admit_date_reason", "discharge_date", "discharge_date_reason")},
            "length_of_stay_days": stay,
            "length_of_stay_days_reason": stay_reason,
            **{k: source[k] for c in ("encounter_type", "claim_status", "primary_dx_code", "payer_category",
                                      "billed_amount_usd") for k in (c, f"{c}_reason")},
        }  # fmt: skip
        facts.append(fact)
        if source["patient_key"] is not None:
            by_patient[source["patient_key"]].append(source)
    return facts, [_patient_row(key, rows) for key, rows in sorted(by_patient.items())]


def _patient_row(patient_key: str, rows: Sequence[dict[str, object]]) -> dict[str, object]:
    latest = max(rows, key=lambda r: (r["last_updated_ts_utc"], r["source_batch_id"], r["source_file_name"], r["source_row_number"]))
    return {
        "patient_key": patient_key,
        "patient_link_status": latest["patient_link_status"],
        "patient_link_reason": latest["patient_link_reason"],
        "sex": latest["sex"],
        "sex_reason": latest["sex_reason"],
        "zip3": latest["zip3"],
        "zip3_reason": latest["zip3_reason"],
        "attributes_vary": len({r["sex"] for r in rows}) > 1 or len({r["zip3"] for r in rows}) > 1,
        "source_batch_id": latest["source_batch_id"],
        "source_file_name": latest["source_file_name"],
        "source_row_number": latest["source_row_number"],
    }


def write_facts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Drop and recreate the facts and dim_patient inside the caller's transaction; returns rows per table.

    Needs mart.dim_provider, so it runs after the dimensions.
    """
    facts, patients = version_fact_rows(con)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    for table in _DDL:
        con.execute(f"CREATE OR REPLACE TABLE {table} ({_DDL[table]})")
    insert_rows(con, VERSION_FACT_TABLE, facts)
    insert_rows(con, PATIENT_TABLE, patients)
    con.execute(
        f"INSERT INTO {CURRENT_FACT_TABLE} "
        f"SELECT f.*, c.version_count, c.encounter_first_seen_batch_id "
        f"FROM {VERSION_FACT_TABLE} AS f JOIN {CURRENT_VIEW} AS c USING (version_key) ORDER BY f.encounter_key"
    )
    current = con.execute(f"SELECT count(*) FROM {CURRENT_FACT_TABLE}").fetchone()[0]
    return {VERSION_FACT_TABLE: len(facts), CURRENT_FACT_TABLE: current, PATIENT_TABLE: len(patients)}
