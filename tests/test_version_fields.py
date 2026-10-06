"""clean.encounter_version_fields: Task 2 fields per version, raw, cleaned and reason side by side.

Unit tests call clean_fields with synthetic values and the real reference data
(read only). Pipeline tests run synthetic batches through main() into tmp_path.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import (
    CONTRACT_PATH,
    REAL_DATA_DIR,
    REPO_ROOT,
    FileSpec,
    attach,
    contract_header,
    csv_bytes,
    state_digest,
    table_columns,
    write_batch,
)

from pipeline import encounter_history, version_fields
from pipeline.clean_patients import PHI_COLUMNS
from pipeline.main import main
from pipeline.parsers.npi import npi_check_digit
from pipeline.reference_data import load_cleaning_reference, load_roster_snapshots
from pipeline.schema_contract import load_contracts
from pipeline.source_conventions import load_source_conventions
from pipeline.version_fields import FIELD_COLUMNS, clean_fields

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

REFERENCE = load_cleaning_reference(REAL_DATA_DIR / "reference", REPO_ROOT / "config" / "facility_aliases.json")
CONVENTIONS = load_source_conventions(REAL_DATA_DIR / "reference" / "source_systems_and_facilities.json")
CONTRACTS = load_contracts(CONTRACT_PATH)
DELIVERED = datetime(2025, 1, 6, 6, 0, tzinfo=UTC)
ROSTER_NPI = load_roster_snapshots(REAL_DATA_DIR / "reference" / "provider_roster")[0].entries[0].npi
UNKNOWN_NPI = "123456789" + str(npi_check_digit("123456789"))  # valid check digit, in no roster
CLEANED_WITH_REASON = (
    "facility_id", "admit_date", "discharge_date", "encounter_type", "claim_status",
    "payer_category", "primary_dx_code", "attending_npi", "billed_amount_usd",
)  # fmt: skip


def raw(**changes):
    values = {
        "facility_name": "Lakeshore General Hospital",
        "admit_date": "03/05/2024",
        "discharge_date": "03/07/2024",
        "encounter_type": "ER",
        "claim_status": "Paid",
        "payer_name": "Medicare",
        "primary_dx_code": "e119",
        "attending_npi": ROSTER_NPI,
        "billed_amount": "$1,200.50",
    }
    values.update(changes)
    return values


def clean(system="EPIC_NORTH", **changes):
    return clean_fields(
        raw(**changes), source_system=system, delivered_at=DELIVERED, convention=CONVENTIONS[system], reference=REFERENCE
    )


# --- clean_fields ---


def test_valid_epic_row_keeps_raw_and_cleaned_side_by_side():
    fields = clean()

    assert tuple(fields) == FIELD_COLUMNS
    assert (fields["facility_name_raw"], fields["facility_id"], fields["facility_id_reason"]) == (
        "Lakeshore General Hospital", "FAC001", None)
    assert (fields["admit_date_raw"], fields["admit_date"]) == ("03/05/2024", "2024-03-05")
    assert (fields["admit_year"], fields["admit_quarter"], fields["admit_month"]) == (2024, 1, 3)
    assert (fields["encounter_type_raw"], fields["encounter_type"]) == ("ER", "EMERGENCY")
    assert (fields["claim_status"], fields["payer_category"], fields["primary_dx_code"]) == ("PAID", "MEDICARE", "E11.9")
    assert (fields["attending_npi"], fields["attending_npi_warning"]) == (ROSTER_NPI, None)
    assert (fields["billed_amount_raw"], fields["billed_amount_usd"]) == ("$1,200.50", "1200.50")
    assert all(fields[f"{c}_reason"] is None for c in CLEANED_WITH_REASON)


def test_meditech_uses_its_own_conventions():
    """The brief's example: 1845000 cents admitted 05/03/2024 day-first is $18,450.00 on 5 March 2024."""
    fields = clean("LEGACY_MEDITECH", facility_name="Riverbend Community Hospital",
                   billed_amount="1845000", admit_date="05/03/2024", discharge_date="07/03/2024")

    assert (fields["facility_id"], fields["billed_amount_usd"], fields["admit_date"]) == ("FAC004", "18450.00", "2024-03-05")


def test_every_null_has_a_reason():
    fields = clean(
        facility_name="Zz Unknown Clinic",
        admit_date="2026-01-01",  # after the batch was delivered
        discharge_date="",
        claim_status="",
        primary_dx_code="250.00",
        attending_npi="N/A",
        billed_amount="N/A",
    )

    assert [(c, fields[c], fields[f"{c}_reason"]) for c in CLEANED_WITH_REASON if fields[c] is None] == [
        ("facility_id", None, "FACILITY_UNRESOLVED"),
        ("admit_date", None, "DATE_AFTER_DELIVERY"),
        ("discharge_date", None, "DATE_MISSING"),
        ("claim_status", None, "CLAIM_STATUS_MISSING"),
        ("primary_dx_code", None, "DX_ICD9"),
        ("attending_npi", None, "NPI_PLACEHOLDER"),
        ("billed_amount_usd", None, "AMOUNT_PLACEHOLDER"),
    ]
    assert (fields["admit_year"], fields["admit_quarter"], fields["admit_month"]) == (None, None, None)
    assert fields["claim_status_raw"] == ""  # the raw value stays next to the NULL


def test_warnings_flag_valid_values():
    fields = clean(discharge_date="03/04/2024", encounter_type="", payer_name="", primary_dx_code="Z99.89",
                   attending_npi=UNKNOWN_NPI)

    assert UNKNOWN_NPI not in REFERENCE.roster_npis
    assert fields["discharge_date_warning"] == "DISCHARGE_BEFORE_ADMIT"
    assert (fields["encounter_type"], fields["encounter_type_warning"]) == ("UNKNOWN", "ENCOUNTER_TYPE_MISSING")
    assert (fields["payer_category"], fields["payer_category_warning"]) == ("UNKNOWN", "PAYER_MISSING")
    assert (fields["primary_dx_code"], fields["primary_dx_code_warning"]) == ("Z99.89", "DX_NOT_IN_REFERENCE")
    assert (fields["attending_npi"], fields["attending_npi_warning"]) == (UNKNOWN_NPI, "NPI_NOT_IN_ROSTER")


def test_fields_hold_no_phi_columns():
    assert not set(version_fields.COLUMNS) & PHI_COLUMNS
    assert not {c.removesuffix("_raw") for c in version_fields.COLUMNS} & PHI_COLUMNS
    assert set(version_fields.RAW_COLUMNS).isdisjoint(PHI_COLUMNS)


# --- pipeline ---

T1, T2 = "2024-03-01T10:00:00Z", "2024-03-02T10:00:00Z"


def epic_file(rows):
    header = contract_header("EPIC_NORTH")
    records = []
    for record_id, ts, *overrides in rows:
        values = {column: f"{column}-zz" for column in header}
        values.update(source_system="EPIC_NORTH", source_record_id=record_id, last_updated_ts=ts, **raw())
        for override in overrides:
            values.update(override)
        records.append([values[column] for column in header])
    return FileSpec("encounters_epic_north.csv", "EPIC_NORTH", csv_bytes(header, records), len(records))


BATCH_001 = [epic_file([("E1", T1), ("E1", T1), ("E2", T1, {"claim_status": ""})])]
BATCH_002 = [epic_file([("E1", T2, {"billed_amount": "99.00"}), ("E1", T1), ("E3", T1)])]


def fields(env):
    con = attach(Path(env["RAW_DB_PATH"]))
    try:
        cursor = con.execute(f"SELECT * FROM {version_fields.TABLE} ORDER BY source_record_id, source_batch_id")
        names = [d[0] for d in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    finally:
        con.close()


def test_one_row_per_new_version_from_its_first_seen_row(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH_001)
    write_batch(landing_dir, "batch_002", BATCH_002)

    assert main(pipeline_env) == 0

    rows = fields(pipeline_env)
    assert [(r["source_record_id"], r["source_batch_id"], r["source_row_number"]) for r in rows] == [
        ("E1", "batch_001", 1),  # its duplicate (row 2) and re-send in batch_002 add nothing
        ("E1", "batch_002", 1),
        ("E2", "batch_001", 3),
        ("E3", "batch_002", 3),
    ]
    e1_new = rows[1]
    assert (e1_new["billed_amount_raw"], e1_new["billed_amount_usd"]) == ("99.00", Decimal("99.00"))
    assert (e1_new["admit_date"], e1_new["facility_id"]) == (date(2024, 3, 5), "FAC001")
    con = attach(Path(pipeline_env["RAW_DB_PATH"]))
    try:
        assert con.execute(f"SELECT count(*) FROM {encounter_history.VERSIONS_TABLE}").fetchone() == (len(rows),)
    finally:
        con.close()


def test_null_claim_status_row_is_kept_with_a_reason(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH_001)

    main(pipeline_env)

    [e2] = [r for r in fields(pipeline_env) if r["source_record_id"] == "E2"]
    assert (e2["claim_status_raw"], e2["claim_status"], e2["claim_status_reason"]) == ("", None, "CLAIM_STATUS_MISSING")


def test_fields_are_insert_only(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH_001)
    main(pipeline_env)
    before = fields(pipeline_env)
    write_batch(landing_dir, "batch_002", BATCH_002)

    main(pipeline_env)

    after = fields(pipeline_env)
    assert [r for r in after if r["source_batch_id"] == "batch_001"] == before


def test_a_fields_failure_rolls_back_the_whole_batch(landing_dir, pipeline_env, monkeypatch):
    write_batch(landing_dir, "batch_001", BATCH_001)
    main(pipeline_env)
    before = state_digest(Path(pipeline_env["RAW_DB_PATH"]), exclude_run_times=False)
    write_batch(landing_dir, "batch_002", BATCH_002)
    original = version_fields._insert

    def insert_then_fail(con, rows):
        original(con, rows)
        raise RuntimeError("simulated failure after the fields insert")

    monkeypatch.setattr(version_fields, "_insert", insert_then_fail)

    assert main(pipeline_env) == 1
    assert state_digest(Path(pipeline_env["RAW_DB_PATH"]), exclude_run_times=False) == before


def test_a_database_without_fields_is_refused_until_rebuilt(landing_dir, pipeline_env, tmp_path, capsys):
    write_batch(landing_dir, "batch_001", BATCH_001)
    main(pipeline_env)
    con = attach(Path(pipeline_env["RAW_DB_PATH"]))
    con.execute(f"DROP TABLE {version_fields.TABLE}")  # what a database from Task 4 looks like
    con.close()
    write_batch(landing_dir, "batch_002", BATCH_002)
    capsys.readouterr()

    assert main(pipeline_env) == 1
    [failure] = [json.loads(line) for line in capsys.readouterr().out.splitlines() if '"pipeline_failed"' in line]
    assert failure["error_type"] == "PipelineError"

    assert main(pipeline_env, ["--rebuild-derived"]) == 0
    fresh = {**pipeline_env, "RAW_DB_PATH": str(tmp_path / "fresh" / "raw.duckdb")}
    assert main(fresh) == 0
    assert state_digest(Path(pipeline_env["RAW_DB_PATH"])) == state_digest(Path(fresh["RAW_DB_PATH"]))


def test_table_columns_and_types(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH_001)
    main(pipeline_env)

    con = attach(Path(pipeline_env["RAW_DB_PATH"]))
    try:
        assert table_columns(con, version_fields.TABLE) == list(version_fields.COLUMNS)
        types = dict(con.execute(f"SELECT column_name, column_type FROM (DESCRIBE {version_fields.TABLE})").fetchall())
    finally:
        con.close()
    assert (types["admit_date"], types["discharge_date"], types["billed_amount_usd"], types["admit_year"]) == (
        "DATE", "DATE", "DECIMAL(18,2)", "INTEGER")
