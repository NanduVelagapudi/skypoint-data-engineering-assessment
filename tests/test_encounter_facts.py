"""Task 5 facts and dim_patient: fact_encounter_version, fact_encounter_current, dim_patient.

Synthetic batches run through main() into tmp_path, with the real reference
files (read only). The real-data pins live in test_incremental_parity.py.
"""

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import REAL_DATA_DIR, FileSpec, attach, contract_header, csv_bytes, state_digest, write_batch

from pipeline import dimensions, encounter_facts
from pipeline.encounter_facts import CURRENT_FACT_TABLE, PATIENT_TABLE, VERSION_FACT_TABLE, length_of_stay
from pipeline.main import main
from pipeline.reference_data import load_roster_snapshots

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

T1, T2 = "2024-03-01T10:00:00Z", "2024-03-02T10:00:00Z"
ROSTER_NPI = load_roster_snapshots(REAL_DATA_DIR / "reference" / "provider_roster")[0].entries[0].npi


def epic_file(rows):
    header = contract_header("EPIC_NORTH")
    records = []
    for record_id, ts, changes in rows:
        values = {column: f"{column}-zz" for column in header}
        values.update(
            source_system="EPIC_NORTH", source_record_id=record_id, last_updated_ts=ts,
            facility_name="Lakeshore General Hospital", patient_mrn="EP-ZZ-1", patient_zip="53201",
            admit_date="03/05/2024", discharge_date="03/07/2024", encounter_type="IP", claim_status="Paid",
            payer_name="Medicare", primary_dx_code="E11.9", attending_npi=ROSTER_NPI, billed_amount="$5,000.00",
        )  # fmt: skip
        values.update(changes)
        records.append([values[column] for column in header])
    return FileSpec("encounters_epic_north.csv", "EPIC_NORTH", csv_bytes(header, records), len(records))


BATCH_001 = [epic_file([
    ("E1", T1, {}),
    ("E2", T1, {"claim_status": ""}),  # NULL claim_status
    ("E3", T1, {"discharge_date": "03/05/2024"}),  # same-day stay
    ("E4", T1, {"discharge_date": "03/04/2024"}),  # discharge before admit
    ("E5", T1, {"discharge_date": "", "attending_npi": "N/A", "patient_mrn": ""}),
    ("E6", T1, {"claim_status": "Void", "facility_name": "Zz Unknown Clinic"}),
])]  # fmt: skip
BATCH_002 = [epic_file([("E1", T2, {"billed_amount": "$7,250.10", "patient_zip": "60601"})])]


def query(env, sql):
    con = attach(Path(env["RAW_DB_PATH"]))
    try:
        cursor = con.execute(sql)
        names = [d[0] for d in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    finally:
        con.close()


def by_id(rows):
    return {r["source_record_id"]: r for r in rows}


@pytest.fixture
def loaded(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH_001)
    write_batch(landing_dir, "batch_002", BATCH_002)
    assert main(pipeline_env) == 0
    return pipeline_env


# --- length of stay ---


@pytest.mark.parametrize(
    "admit, discharge, expected",
    [
        (date(2024, 3, 5), date(2024, 3, 5), (0, None)),
        (date(2024, 3, 5), date(2024, 3, 7), (2, None)),
        (date(2024, 12, 30), date(2025, 1, 2), (3, None)),
        (None, date(2024, 3, 7), (None, "LENGTH_OF_STAY_ADMIT_UNAVAILABLE")),
        (None, None, (None, "LENGTH_OF_STAY_ADMIT_UNAVAILABLE")),  # admit is checked first
        (date(2024, 3, 5), None, (None, "LENGTH_OF_STAY_DISCHARGE_UNAVAILABLE")),
        (date(2024, 3, 5), date(2024, 3, 4), (None, "DISCHARGE_BEFORE_ADMIT")),
    ],
)
def test_length_of_stay(admit, discharge, expected):
    assert length_of_stay(admit, discharge) == expected


# --- fact_encounter_version and fact_encounter_current ---


def test_one_version_row_per_version_and_one_current_row_per_encounter(loaded):
    versions = query(loaded, f"SELECT * FROM {VERSION_FACT_TABLE} ORDER BY source_record_id, last_updated_ts_utc")
    current = by_id(query(loaded, f"SELECT * FROM {CURRENT_FACT_TABLE}"))

    assert [(v["source_record_id"], v["first_seen_batch_id"], v["arrival_outcome"]) for v in versions] == [
        ("E1", "batch_001", "NEW_ENCOUNTER"), ("E1", "batch_002", "NEW_VERSION"),
        ("E2", "batch_001", "NEW_ENCOUNTER"), ("E3", "batch_001", "NEW_ENCOUNTER"),
        ("E4", "batch_001", "NEW_ENCOUNTER"), ("E5", "batch_001", "NEW_ENCOUNTER"),
        ("E6", "batch_001", "NEW_ENCOUNTER"),
    ]  # fmt: skip
    e1 = current["E1"]
    assert (e1["version_key"], e1["version_count"], e1["encounter_first_seen_batch_id"]) == (
        versions[1]["version_key"], 2, "batch_001")
    assert (e1["source_batch_id"], e1["source_file_name"], e1["source_row_number"]) == (
        "batch_002", "encounters_epic_north.csv", 1)
    assert e1["billed_amount_usd"] == Decimal("7250.10") and versions[0]["billed_amount_usd"] == Decimal("5000.00")
    view = by_id(query(loaded, "SELECT * FROM clean.encounter_current"))
    assert {k: v["version_key"] for k, v in current.items()} == {k: v["version_key"] for k, v in view.items()}


def test_length_of_stay_and_reasons_on_the_facts(loaded):
    current = by_id(query(loaded, f"SELECT * FROM {CURRENT_FACT_TABLE}"))

    assert [(k, current[k]["length_of_stay_days"], current[k]["length_of_stay_days_reason"]) for k in sorted(current)] == [
        ("E1", 2, None),
        ("E2", 2, None),
        ("E3", 0, None),
        ("E4", None, "DISCHARGE_BEFORE_ADMIT"),
        ("E5", None, "LENGTH_OF_STAY_DISCHARGE_UNAVAILABLE"),
        ("E6", 2, None),
    ]
    assert (current["E5"]["provider_sk"], current["E5"]["provider_sk_reason"]) == (None, "NPI_PLACEHOLDER")
    assert (current["E5"]["patient_key"], current["E5"]["patient_key_reason"]) == (None, "PATIENT_MRN_MISSING")
    assert (current["E6"]["facility_id"], current["E6"]["facility_id_reason"]) == (None, "FACILITY_UNRESOLVED")


def test_provider_is_looked_up_point_in_time(loaded):
    [row] = query(
        loaded,
        f"SELECT f.provider_sk, p.npi, p.valid_from, p.valid_to FROM {CURRENT_FACT_TABLE} f "
        f"JOIN {dimensions.PROVIDER_TABLE} p USING (provider_sk) WHERE f.source_record_id = 'E1'",
    )

    assert row["npi"] == ROSTER_NPI and row["valid_from"] <= date(2024, 3, 5) < row["valid_to"]


def test_null_claim_status_is_kept_and_survives_the_void_filter(loaded):
    """IS DISTINCT FROM 'VOID' keeps a NULL claim_status; <> 'VOID' would silently drop it."""
    e2 = by_id(query(loaded, f"SELECT * FROM {CURRENT_FACT_TABLE}"))["E2"]
    assert (e2["claim_status"], e2["claim_status_reason"]) == (None, "CLAIM_STATUS_MISSING")

    kept = {r["source_record_id"] for r in query(loaded, f"SELECT source_record_id FROM {CURRENT_FACT_TABLE} "
                                                         "WHERE claim_status IS DISTINCT FROM 'VOID'")}
    dropped_by_not_equal = {r["source_record_id"] for r in query(loaded, f"SELECT source_record_id FROM {CURRENT_FACT_TABLE} "
                                                                         "WHERE claim_status <> 'VOID'")}
    assert kept == {"E1", "E2", "E3", "E4", "E5"}  # only the VOID encounter is excluded
    assert kept - dropped_by_not_equal == {"E2"}


def test_every_null_has_a_reason_and_every_foreign_key_exists(loaded):
    for table in (VERSION_FACT_TABLE, CURRENT_FACT_TABLE):
        columns = [r["column_name"] for r in query(loaded, f"SELECT column_name FROM (DESCRIBE {table})")]
        nullable = [c for c in columns if f"{c}_reason" in columns]
        assert nullable == [
            "facility_id", "patient_key", "age_band", "sex", "zip3", "attending_npi", "provider_sk", "admit_date",
            "discharge_date", "length_of_stay_days", "encounter_type", "claim_status", "primary_dx_code",
            "payer_category", "billed_amount_usd",
        ]  # fmt: skip
        for column in nullable:
            [row] = query(loaded, f"SELECT count(*) AS n FROM {table} WHERE {column} IS NULL AND {column}_reason IS NULL")
            assert row["n"] == 0, (table, column)
    [orphans] = query(loaded, f"""
        SELECT count(*) FILTER (WHERE facility_id NOT IN (SELECT facility_id FROM mart.dim_facility)) AS facility,
               count(*) FILTER (WHERE provider_sk NOT IN (SELECT provider_sk FROM mart.dim_provider)) AS provider,
               count(*) FILTER (WHERE patient_key NOT IN (SELECT patient_key FROM mart.dim_patient)) AS patient,
               count(*) FILTER (WHERE primary_dx_code NOT IN (SELECT icd10_code FROM mart.dim_diagnosis)) AS dx,
               count(*) FILTER (WHERE payer_category NOT IN (SELECT payer_category FROM mart.dim_payer)) AS payer,
               count(*) FILTER (WHERE admit_date NOT IN (SELECT date_key FROM mart.dim_date)
                                   OR discharge_date NOT IN (SELECT date_key FROM mart.dim_date)) AS dates
        FROM {VERSION_FACT_TABLE}""")
    assert orphans == {"facility": 0, "provider": 0, "patient": 0, "dx": 0, "payer": 0, "dates": 0}


# --- dim_patient ---


def test_dim_patient_takes_attributes_from_the_most_recent_version(loaded):
    [patient] = query(loaded, f"SELECT * FROM {PATIENT_TABLE}")  # one MRN; the blank-MRN row has no patient_key

    assert patient["zip3"] == "606"  # E1's batch_002 version is the latest
    assert patient["attributes_vary"] is True  # earlier versions had 532
    assert (patient["source_batch_id"], patient["source_row_number"]) == ("batch_002", 1)
    assert patient["patient_link_status"] == "UNLINKED"  # the synthetic DOB does not parse
    assert "age_band" not in patient  # it depends on the admission, so it lives on the facts


# --- rebuilt with the dimensions ---


def test_facts_are_rebuilt_identically(loaded):
    before = state_digest(Path(loaded["RAW_DB_PATH"]), exclude_run_times=False)

    assert main(loaded) == 0

    assert state_digest(Path(loaded["RAW_DB_PATH"]), exclude_run_times=False) == before


def test_a_failure_in_the_facts_keeps_the_whole_previous_mart(loaded, monkeypatch):
    before = state_digest(Path(loaded["RAW_DB_PATH"]), exclude_run_times=False)
    original = encounter_facts.insert_rows

    def fail_on_patients(con, table, rows):
        if table == PATIENT_TABLE:
            raise RuntimeError("simulated failure after the dimensions and version facts were written")
        original(con, table, rows)

    monkeypatch.setattr(encounter_facts, "insert_rows", fail_on_patients)

    assert main(loaded) == 1
    assert state_digest(Path(loaded["RAW_DB_PATH"]), exclude_run_times=False) == before  # dimensions too


def test_rows_with_missing_or_extra_columns_are_refused(loaded):
    con = attach(Path(loaded["RAW_DB_PATH"]))
    try:
        with pytest.raises(ValueError, match="exactly its columns"):
            dimensions.insert_rows(con, dimensions.PAYER_TABLE, [{"payer_categry": "MEDICARE"}])
    finally:
        con.close()
