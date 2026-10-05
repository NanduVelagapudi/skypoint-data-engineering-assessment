"""End-to-end Task 3 tests: synthetic batches through python -m pipeline.main's main(), into tmp_path.

The synthetic patient values are PHI-shaped on purpose (names, MRNs, DOBs,
phones, ZIPs, a complaint with a phone number), so the tests can prove none of
them reaches the cleaned table, the logs or an error.
"""

from pathlib import Path

import pytest
from conftest import CONTRACT_PATH, TEST_PATIENT_KEY_SECRET, FileSpec, contract_header, csv_bytes, write_batch

from pipeline import clean_patients
from pipeline.clean_patients import CLEAN_COLUMNS, PHI_COLUMNS
from pipeline.main import main
from pipeline.raw_store import open_store
from pipeline.schema_contract import load_contracts

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

CONTRACTS = load_contracts(CONTRACT_PATH)
FORBIDDEN_COLUMNS = {
    "patient_mrn",
    "patient_first_name",
    "patient_last_name",
    "patient_dob",
    "patient_phone",
    "patient_zip",
    "chief_complaint",
}

PATIENTS = {
    # Same person in Epic (MDY) and Meditech (DMY): must share one LINKED key.
    "EPIC_NORTH": [
        dict(patient_mrn="EP0000001", patient_last_name="O'Zztesta", patient_first_name="Zzanna B",
             patient_dob="07/15/1980", patient_sex="F", patient_zip="53201", patient_phone="555-010-0001",
             chief_complaint="Zzcaller asked to ring 555-010-0001"),
        dict(patient_mrn="EP0000002", patient_last_name="Zzdoe", patient_first_name="Zzjohn",
             patient_dob="", patient_sex="M", patient_zip="53202", patient_phone="555-010-0002",
             chief_complaint="Zzchest pain"),
    ],
    "LEGACY_MEDITECH": [
        dict(patient_mrn="MT0000001", patient_last_name="OZZTESTA", patient_first_name="ZZANNA",
             patient_dob="15/07/1980", patient_sex="Female", patient_zip="53201-1234", patient_phone="555-010-0003",
             chief_complaint="Zzfollow up"),
        dict(patient_mrn="MT0000002", patient_last_name="Zzdoe", patient_first_name="Zzjohn",
             patient_dob="", patient_sex="Male", patient_zip="53202", patient_phone="555-010-0004",
             chief_complaint="Zzcough"),
    ],
    "ATHENA_CLINICS": [
        dict(patient_mrn="AT0000001", patient_last_name="Zzroe", patient_first_name="Zzsam",
             patient_dob="01/02/1990", patient_sex="U", patient_zip="ZZ532", patient_phone="555-010-0005",
             chief_complaint="Zzrash"),
    ],
}
# Every PHI value above (sex is not PHI; only its normalised value is kept). Short
# values are skipped so a 3-character ZIP3 or a hex patient_key cannot match by accident.
PHI_VALUES = {v for rows in PATIENTS.values() for r in rows for c, v in r.items() if c in FORBIDDEN_COLUMNS and len(v) >= 4}


def patient_file(source_system: str) -> FileSpec:
    header = contract_header(source_system)
    rows = []
    for number, patient in enumerate(PATIENTS[source_system], start=1):
        values = {column: f"{column}-{number}" for column in header}
        values.update(source_system=source_system, admit_date="2024-03-05", **patient)
        rows.append([values[column] for column in header])
    return FileSpec(CONTRACTS.source_systems[source_system].file_name, source_system, csv_bytes(header, rows), len(rows))


def write_patients(landing_dir):
    write_batch(landing_dir, "batch_001", [patient_file(s) for s in PATIENTS])


def clean_rows(env) -> dict[str, dict]:
    """Clean rows keyed by source_record_id, as column -> value dicts."""
    con = open_store(Path(env["RAW_DB_PATH"]), CONTRACTS.canonical_columns)
    try:
        cursor = con.execute(f"SELECT * FROM {clean_patients.TABLE}")
        names = [d[0] for d in cursor.description]
        rows = [dict(zip(names, values)) for values in cursor.fetchall()]
    finally:
        con.close()
    return {f"{r['source_system']}:{r['source_record_id']}": r for r in rows}


def test_clean_columns_never_include_phi():
    assert FORBIDDEN_COLUMNS <= PHI_COLUMNS
    assert not FORBIDDEN_COLUMNS & set(CLEAN_COLUMNS)


def test_cleaned_table_has_no_phi_columns_or_values(landing_dir, pipeline_env):
    write_patients(landing_dir)

    assert main(pipeline_env) == 0

    rows = clean_rows(pipeline_env)
    assert len(rows) == 5
    for row in rows.values():
        assert list(row) == list(CLEAN_COLUMNS)
        assert not FORBIDDEN_COLUMNS & set(row)
        for value in row.values():
            for phi in PHI_VALUES:
                assert phi not in str(value), "a PHI value reached clean.encounter_patients"


def test_linkage_age_band_sex_and_zip3_end_to_end(landing_dir, pipeline_env):
    write_patients(landing_dir)
    main(pipeline_env)

    rows = clean_rows(pipeline_env)
    epic_1, epic_2 = rows["EPIC_NORTH:source_record_id-1"], rows["EPIC_NORTH:source_record_id-2"]
    meditech_1, meditech_2 = rows["LEGACY_MEDITECH:source_record_id-1"], rows["LEGACY_MEDITECH:source_record_id-2"]
    athena_1 = rows["ATHENA_CLINICS:source_record_id-1"]

    # Same person across Epic and Meditech, despite date order, case, punctuation and the middle initial.
    assert epic_1["patient_key"] == meditech_1["patient_key"]
    assert (epic_1["patient_link_status"], meditech_1["patient_link_status"]) == ("LINKED", "LINKED")
    # Same name and sex but no DOB: never linked, and scoped to each system.
    assert epic_2["patient_key"] != meditech_2["patient_key"]
    assert epic_2["patient_link_reason"] == meditech_2["patient_link_reason"] == "PATIENT_UNLINKED_NO_DOB"
    # DOB present but sex unknown.
    assert (athena_1["patient_link_status"], athena_1["patient_link_reason"]) == ("UNLINKED", "PATIENT_UNLINKED_INCOMPLETE")
    assert (athena_1["sex"], athena_1["sex_reason"]) == ("UNKNOWN", "SEX_UNMAPPED")  # raw "U"
    assert (meditech_1["sex"], meditech_1["sex_reason"]) == ("F", None)  # raw "Female"
    assert (meditech_2["sex"], meditech_2["sex_reason"]) == ("M", None)  # raw "Male"
    assert (epic_1["sex"], epic_2["sex"]) == ("F", "M")  # raw "F", "M"
    # Age band at admission (2024-03-05) and its reasons.
    assert (epic_1["age_band"], epic_1["age_band_reason"]) == ("40-64", None)
    assert (epic_2["age_band"], epic_2["age_band_reason"]) == ("UNKNOWN", "AGE_BAND_DOB_UNAVAILABLE")
    assert athena_1["age_band"] == "18-39"
    # ZIP3 only.
    assert (epic_1["zip3"], meditech_1["zip3"]) == ("532", "532")
    assert (athena_1["zip3"], athena_1["zip3_reason"]) == (None, "ZIP_INVALID")
    # Lineage back to the raw row.
    assert (epic_1["batch_id"], epic_1["file_name"], epic_1["source_row_number"]) == ("batch_001", "encounters_epic_north.csv", 1)


def test_clean_table_keeps_normalised_sex_but_not_the_raw_label(landing_dir, pipeline_env):
    write_patients(landing_dir)
    main(pipeline_env)

    rows = clean_rows(pipeline_env)
    assert "sex" in CLEAN_COLUMNS and "sex_raw" not in CLEAN_COLUMNS
    for row in rows.values():
        assert "sex_raw" not in row
        assert row["sex"] in {"F", "M", "UNKNOWN"}
        # The source spellings stay in the raw layer only.
        assert not {"Female", "Male"} & {str(value) for value in row.values()}


def test_patient_key_is_deterministic_across_runs_and_rebuilds(landing_dir, pipeline_env, tmp_path):
    write_patients(landing_dir)
    main(pipeline_env)
    first = {k: r["patient_key"] for k, r in clean_rows(pipeline_env).items()}

    main(pipeline_env)  # re-run on the same database
    second = {k: r["patient_key"] for k, r in clean_rows(pipeline_env).items()}
    fresh_env = {**pipeline_env, "RAW_DB_PATH": str(tmp_path / "fresh" / "raw.duckdb")}
    main(fresh_env)  # full rebuild in a new database
    fresh = {k: r["patient_key"] for k, r in clean_rows(fresh_env).items()}

    assert first == second == fresh
    assert len(clean_rows(pipeline_env)) == 5  # rebuilt, not appended


def test_a_different_secret_gives_different_keys(landing_dir, pipeline_env, tmp_path):
    write_patients(landing_dir)
    main(pipeline_env)
    other_env = {**pipeline_env, "RAW_DB_PATH": str(tmp_path / "other" / "raw.duckdb"),
                 "PATIENT_KEY_HMAC_SECRET": "another-test-only-secret"}
    main(other_env)

    ours, theirs = clean_rows(pipeline_env), clean_rows(other_env)
    assert all(ours[k]["patient_key"] != theirs[k]["patient_key"] for k in ours)


@pytest.mark.parametrize("secret", [None, "", "   "])
def test_missing_secret_fails_before_any_work(landing_dir, pipeline_env, secret, capsys):
    write_patients(landing_dir)
    env = dict(pipeline_env)
    if secret is None:
        del env["PATIENT_KEY_HMAC_SECRET"]
    else:
        env["PATIENT_KEY_HMAC_SECRET"] = secret

    assert main(env) == 1

    output = capsys.readouterr().out
    assert '"error_type": "MissingSecretError"' in output and '"step": "config"' in output
    assert not Path(env["RAW_DB_PATH"]).exists()  # nothing was ingested or cleaned


def test_secret_and_phi_never_reach_logs_or_errors(landing_dir, pipeline_env, capsys, caplog, monkeypatch):
    write_patients(landing_dir)
    assert main({**pipeline_env, "LOG_LEVEL": "DEBUG"}) == 0

    def fail_with_sensitive_message(*args, **kwargs):
        raise RuntimeError(f"failed with key {TEST_PATIENT_KEY_SECRET} near {sorted(PHI_VALUES)[0]}")

    monkeypatch.setattr("pipeline.main.build_encounter_patients", fail_with_sensitive_message)
    assert main(pipeline_env) == 1

    captured = capsys.readouterr()
    for text in (captured.out, captured.err):
        assert TEST_PATIENT_KEY_SECRET not in text
        assert "ZZSECRET" not in text
        assert not any(phi in text for phi in PHI_VALUES)
    for record in caplog.records:
        for value in vars(record).values():
            assert TEST_PATIENT_KEY_SECRET not in str(value)
            assert not any(phi in str(value) for phi in PHI_VALUES)
