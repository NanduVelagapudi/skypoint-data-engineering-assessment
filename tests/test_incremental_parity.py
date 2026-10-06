"""Task 4 group (c): incremental, one-shot, rerun and rebuild runs on the real data pack agree.

The data pack is only read: batch folders are copied into tmp_path for the
incremental runs. Every database and output folder lives in tmp_path, and the
repository's data/, output/ and work/ are checked to be unchanged afterwards.

Runs compared:
    incremental   landing holds 001, then 001-002, 001-003, 001-004 (one run each)
    rerun         the incremental database run again
    one_shot      a fresh database, landing 001-004 in one run
    rebuild       a copy of the one-shot database taken back to the pre-Task-4
                  layout, then python -m pipeline.main --rebuild-derived
    rebuild_live  a copy of the incremental database rebuilt in place

Tables are compared by digest (conftest.state_digest): the compared columns,
the excluded columns, the row count and a hash of the sorted rows, so a failure
names the table without printing PHI. Only ingested_at, start_time and end_time
are excluded, and a test checks that.
"""

import csv
import hashlib
import logging
import shutil
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import pytest
from conftest import (
    CONTRACT_PATH,
    REAL_DATA_DIR,
    REPO_ROOT,
    RUN_TIME_COLUMNS,
    STATE_TABLES,
    TEST_PATIENT_KEY_SECRET,
    attach,
    independent_monthly_totals as monthly_totals,
    make_pre_task4,
    state_digest,
)

from pipeline import version_fields
from pipeline.batch_audit import AUDIT_COLUMNS, TASK4_COLUMNS
from pipeline.dimensions import VALID_FROM_EARLIEST, ProviderLookup, ProviderRow
from pipeline.exports import EXPORTED_TABLES, file_name
from pipeline.main import main
from pipeline.parsers.dates import parse_date
from pipeline.source_conventions import load_source_conventions

LANDING = REAL_DATA_DIR / "landing"
REFERENCE = REAL_DATA_DIR / "reference"
CONVENTIONS = load_source_conventions(REFERENCE / "source_systems_and_facilities.json")
BATCH_IDS = ("batch_001", "batch_002", "batch_003", "batch_004")
FINAL_RUNS = ("batch_004", "rerun", "one_shot", "rebuild", "rebuild_live")
DERIVED_TABLES = tuple(t for t in STATE_TABLES if t != "ops.batch_audit")
CSV_RUN_TIME_COLUMNS = ("start_time", "end_time")
ATHENA, EPIC, MEDITECH = "encounters_athena_clinics.csv", "encounters_epic_north.csv", "encounters_legacy_meditech.csv"

# (received, accepted, new_encounter, new_version, duplicate, stale, stale_new_version,
#  quarantined, history_rows_written, current_changed) - the approved Task 4 table.
APPROVED_PER_FILE = {
    ("batch_001", ATHENA): (1090, 1082, 1082, 0, 8, 0, 0, 0, 1082, 1082),
    ("batch_001", EPIC): (1432, 1415, 1415, 0, 17, 0, 0, 0, 1415, 1415),
    ("batch_001", MEDITECH): (341, 336, 336, 0, 5, 0, 0, 0, 336, 336),
    ("batch_002", ATHENA): (147, 133, 61, 72, 14, 0, 0, 0, 133, 133),
    ("batch_002", EPIC): (174, 154, 69, 85, 20, 0, 0, 0, 154, 154),
    ("batch_002", MEDITECH): (264, 258, 232, 26, 6, 0, 0, 0, 258, 258),
    ("batch_003", ATHENA): (74, 65, 25, 40, 0, 9, 0, 0, 65, 65),
    ("batch_003", EPIC): (83, 71, 36, 35, 0, 12, 0, 0, 71, 71),
    ("batch_003", MEDITECH): (31, 27, 18, 9, 0, 4, 0, 0, 27, 27),
}


def fingerprint(folder: Path) -> dict[str, str]:
    if not folder.exists():
        return {}
    return {str(p.relative_to(folder)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(folder.rglob("*")) if p.is_file()}


def make_env(base: Path, data_dir: Path) -> dict[str, str]:
    return {
        "DATA_DIR": str(data_dir),
        "REFERENCE_DIR": str(REFERENCE),
        "OUTPUT_DIR": str(base / "output"),
        "RAW_DB_PATH": str(base / "work" / "raw.duckdb"),
        "SCHEMA_CONTRACT_PATH": str(CONTRACT_PATH),
        "LOG_LEVEL": "WARNING",
        "PATIENT_KEY_HMAC_SECRET": TEST_PATIENT_KEY_SECRET,
    }


def late_update_months(con) -> set[tuple[int, int]]:
    """Admit months of encounters that received a newer version in batch_003."""
    rows = con.execute(
        "SELECT f.source_system, f.delivered_at, e.admit_date "
        "FROM clean.encounter_row_outcomes o "
        "JOIN raw.encounters e USING (batch_id, file_name, source_row_number) "
        "JOIN raw.ingested_files f USING (batch_id, file_name) "
        "WHERE o.batch_id = 'batch_003' AND o.outcome = 'NEW_VERSION'"
    ).fetchall()
    months = set()
    for system, delivered_at, admit_text in rows:
        c = CONVENTIONS[system]
        admit = parse_date(admit_text, date_order=c.date_order, delivered_at=datetime.fromisoformat(delivered_at),
                           two_digit_year_century=c.two_digit_year_century).cleaned_value
        if admit is not None:
            months.add((admit.year, admit.month))
    return months


def read_audit_csv(env, *, drop_run_times: bool) -> list[dict]:
    with (Path(env["OUTPUT_DIR"]) / "batch_audit.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if drop_run_times:
        rows = [{k: v for k, v in row.items() if k not in CSV_RUN_TIME_COLUMNS} for row in rows]
    return rows


FLAG_COLUMNS = tuple(c for c in version_fields.FIELD_COLUMNS if c.endswith(("_reason", "_warning")))
CLEANED_COLUMNS = tuple(c.removesuffix("_reason") for c in version_fields.FIELD_COLUMNS if c.endswith("_reason"))


def field_flags(con) -> dict[str, dict[tuple[str, str], int]]:
    """Counts of every reason and warning in the cleaned fields, for current and superseded versions."""
    flags = {"current": {}, "superseded": {}}
    for column in FLAG_COLUMNS:
        for is_current, value, count in con.execute(
            f"SELECT c.version_key IS NOT NULL, f.{column}, count(*) FROM {version_fields.TABLE} f "
            f"LEFT JOIN clean.encounter_current c USING (version_key) WHERE f.{column} IS NOT NULL GROUP BY ALL"
        ).fetchall():
            flags["current" if is_current else "superseded"][(column, value)] = count
    return flags


def null_without_reason(con) -> int:
    """Rows where a cleaned value is NULL without a reason, or has a reason but is not NULL."""
    checks = " + ".join(f"count(*) FILTER (WHERE ({c} IS NULL) <> ({c}_reason IS NOT NULL))" for c in CLEANED_COLUMNS)
    return con.execute(f"SELECT {checks} FROM {version_fields.TABLE}").fetchone()[0]


def provider_lookup(con) -> dict:
    """Point-in-time provider lookup for every current version, in Python and in SQL."""
    cursor = con.execute("SELECT * FROM mart.dim_provider")
    names = [d[0] for d in cursor.description]
    rows = [ProviderRow(**dict(zip(names, r, strict=True))) for r in cursor.fetchall()]
    lookup, by_sk = ProviderLookup(rows), {r.provider_sk: r for r in rows}
    latest = {r.npi: r for r in rows if r.is_current}
    outcomes, differs_from_latest = Counter(), 0
    for npi, admit, npi_reason in con.execute(
        f"SELECT f.attending_npi, f.admit_date, f.attending_npi_reason FROM {version_fields.TABLE} f "
        "JOIN clean.encounter_current c USING (version_key)"
    ).fetchall():
        sk, reason = lookup.at(npi, admit, npi_reason)
        if sk is None:
            outcomes[reason] += 1
            continue
        outcomes["found, admit before 2024-07-01" if admit < date(2024, 7, 1) else "found, admit from 2024-07-01"] += 1
        found, newest = by_sk[sk], latest.get(npi)
        if newest and (found.specialty, found.employment_status) != (newest.specialty, newest.employment_status):
            differs_from_latest += 1
    sql_matches = con.execute(
        f"SELECT count(*) FROM {version_fields.TABLE} f JOIN clean.encounter_current c USING (version_key) "
        "JOIN mart.dim_provider p ON p.npi = f.attending_npi AND f.admit_date >= p.valid_from AND f.admit_date < p.valid_to"
    ).fetchone()[0]
    return {"outcomes": dict(outcomes), "differs_from_latest": differs_from_latest, "sql_matches": sql_matches}


FACT_TABLES = ("mart.fact_encounter_version", "mart.fact_encounter_current")
FOREIGN_KEYS = {
    "facility_id": ("mart.dim_facility", "facility_id"),
    "provider_sk": ("mart.dim_provider", "provider_sk"),
    "patient_key": ("mart.dim_patient", "patient_key"),
    "primary_dx_code": ("mart.dim_diagnosis", "icd10_code"),
    "payer_category": ("mart.dim_payer", "payer_category"),
    "admit_date": ("mart.dim_date", "date_key"),
    "discharge_date": ("mart.dim_date", "date_key"),
}


def fact_checks(con) -> dict:
    """Integrity counts for the fact tables (all should be 0), and their distributions."""
    checks = {}
    for table in FACT_TABLES:
        columns = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
        checks[(table, "null_without_reason")] = sum(
            con.execute(f"SELECT count(*) FROM {table} WHERE {c} IS NULL AND {c}_reason IS NULL").fetchone()[0]
            for c in columns if f"{c}_reason" in columns
        )
        for column, (dim, key) in FOREIGN_KEYS.items():
            checks[(table, f"orphan_{column}")] = con.execute(
                f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL AND {column} NOT IN (SELECT {key} FROM {dim})"
            ).fetchone()[0]
        # Lineage resolves to a raw row (the test joins raw; the mart itself never reads it).
        checks[(table, "lineage_unresolved")] = con.execute(
            f"SELECT count(*) FROM {table} f ANTI JOIN raw.encounters e ON e.batch_id = f.source_batch_id "
            "AND e.file_name = f.source_file_name AND e.source_row_number = f.source_row_number"
        ).fetchone()[0]
    return {
        "integrity": checks,
        "length_of_stay_reasons": dict(con.execute(
            "SELECT length_of_stay_days_reason, count(*) FROM mart.fact_encounter_current GROUP BY 1").fetchall()),
        "provider_reasons": dict(con.execute(
            "SELECT provider_sk_reason, count(*) FROM mart.fact_encounter_current GROUP BY 1").fetchall()),
        "version_counts": dict(con.execute(
            "SELECT version_count, count(*) FROM mart.fact_encounter_current GROUP BY 1").fetchall()),
        "patients": dict(con.execute(
            "SELECT patient_link_status || CASE WHEN attributes_vary THEN ', varies' ELSE '' END, count(*) "
            "FROM mart.dim_patient GROUP BY 1").fetchall()),
        "current_matches_view": con.execute(
            "SELECT count(*) FROM mart.fact_encounter_current f JOIN clean.encounter_current c "
            "USING (encounter_key, version_key)").fetchone()[0],
    }


def scd2_rows_from_roster_files() -> int:
    """dim_provider's row count recomputed straight from the roster CSVs: one row per unchanged stretch."""
    snapshots = []
    for path in sorted((REFERENCE / "provider_roster").glob("roster_*.csv")):
        with path.open(encoding="utf-8-sig", newline="") as handle:
            snapshots.append({row["npi"]: tuple(v for k, v in row.items() if k not in ("as_of_date", "npi"))
                              for row in csv.DictReader(handle)})
    rows = 0
    for npi in {npi for snapshot in snapshots for npi in snapshot}:
        previous = None
        for snapshot in snapshots:
            current = snapshot.get(npi)
            if current is not None and current != previous:
                rows += 1
            previous = current
    return rows


def capture(env) -> dict:
    db = Path(env["RAW_DB_PATH"])
    con = attach(db)
    try:
        state = {
            "audit": con.execute(f"SELECT {', '.join(AUDIT_COLUMNS)} FROM ops.batch_audit ORDER BY batch_id, file_name").fetchall(),
            "counts": {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in STATE_TABLES},
            "current_totals": monthly_totals(con),
            "asof_002_totals": monthly_totals(con, "batch_002"),
            "late_update_months": late_update_months(con),
            "field_flags": field_flags(con),
            "null_without_reason": null_without_reason(con),
            "provider_lookup": provider_lookup(con),
            "dim_provider_starts": dict(con.execute(
                "SELECT valid_from, count(*) FROM mart.dim_provider GROUP BY 1").fetchall()),
            "dim_date_range": con.execute("SELECT min(date_key), max(date_key), count(*) FROM mart.dim_date").fetchone(),
            "dates_outside_dim_date": con.execute(
                f"SELECT count(*) FROM {version_fields.TABLE} f WHERE "
                "(f.admit_date IS NOT NULL AND f.admit_date NOT IN (SELECT date_key FROM mart.dim_date)) OR "
                "(f.discharge_date IS NOT NULL AND f.discharge_date NOT IN (SELECT date_key FROM mart.dim_date))"
            ).fetchone()[0],
            "dim_diagnosis_in_reference": dict(con.execute(
                "SELECT in_reference, count(*) FROM mart.dim_diagnosis GROUP BY 1").fetchall()),
            "facts": fact_checks(con),
        }
    finally:
        con.close()
    state["digest"] = state_digest(db)
    state["digest_all"] = state_digest(db, exclude_run_times=False)
    state["csv"] = read_audit_csv(env, drop_run_times=True)
    state["csv_bytes"] = (Path(env["OUTPUT_DIR"]) / "batch_audit.csv").read_bytes()
    state["exports"] = {
        p.name: p.read_bytes() for p in sorted(Path(env["OUTPUT_DIR"]).glob("*.csv")) if p.name != "batch_audit.csv"
    }
    return state


@pytest.fixture(scope="module")
def runs(tmp_path_factory):
    base = tmp_path_factory.mktemp("parity")
    watched = {"data": REAL_DATA_DIR, "output": REPO_ROOT / "output", "work": REPO_ROOT / "work"}
    before = {name: fingerprint(path) for name, path in watched.items()}
    logger = logging.getLogger("pipeline")
    handlers, level = logger.handlers[:], logger.level
    states = {}
    try:
        incremental_data = base / "incremental" / "data"
        (incremental_data / "landing").mkdir(parents=True)
        incremental = make_env(base / "incremental", incremental_data)
        for batch_id in BATCH_IDS:
            shutil.copytree(LANDING / batch_id, incremental_data / "landing" / batch_id)
            assert main(incremental) == 0, batch_id
            states[batch_id] = capture(incremental)

        assert main(incremental) == 0
        states["rerun"] = capture(incremental)

        one_shot = make_env(base / "one_shot", REAL_DATA_DIR)
        assert main(one_shot) == 0
        states["one_shot"] = capture(one_shot)

        rebuild = make_env(base / "rebuild", REAL_DATA_DIR)
        Path(rebuild["RAW_DB_PATH"]).parent.mkdir(parents=True)
        shutil.copyfile(one_shot["RAW_DB_PATH"], rebuild["RAW_DB_PATH"])
        make_pre_task4(Path(rebuild["RAW_DB_PATH"]))
        states["pre_task4_refused"] = main(rebuild)
        assert main(rebuild, ["--rebuild-derived"]) == 0
        states["rebuild"] = capture(rebuild)

        rebuild_live = make_env(base / "rebuild_live", incremental_data)
        Path(rebuild_live["RAW_DB_PATH"]).parent.mkdir(parents=True)
        shutil.copyfile(incremental["RAW_DB_PATH"], rebuild_live["RAW_DB_PATH"])
        assert main(rebuild_live, ["--rebuild-derived"]) == 0
        states["rebuild_live"] = capture(rebuild_live)
    finally:
        logger.handlers[:] = handlers
        logger.setLevel(level)
    states["watched_unchanged"] = {name: fingerprint(path) == before[name] for name, path in watched.items()}
    return states


# --- safety ---


def test_real_data_output_and_work_are_untouched(runs):
    assert runs["watched_unchanged"] == {"data": True, "output": True, "work": True}


def test_only_operational_timestamps_are_excluded_from_comparison(runs):
    for run in FINAL_RUNS:
        for table, (compared, excluded, _, _) in runs[run]["digest"].items():
            assert excluded == RUN_TIME_COLUMNS.get(table, ()), (run, table)
            all_columns = runs[run]["digest_all"][table][0]
            assert compared == tuple(c for c in all_columns if c not in excluded), (run, table)
    assert set(runs["one_shot"]["csv"][0]) == set(AUDIT_COLUMNS) - set(CSV_RUN_TIME_COLUMNS)


# --- parity ---


@pytest.mark.parametrize("run", FINAL_RUNS[1:])
def test_final_state_is_identical_to_the_incremental_run(runs, run):
    reference = runs["batch_004"]
    for table in STATE_TABLES:
        assert runs[run]["digest"][table] == reference["digest"][table], (run, table)
    assert runs[run]["csv"] == reference["csv"]
    assert runs[run]["current_totals"] == reference["current_totals"]


def test_a_pre_task4_database_is_refused_then_migrated_by_rebuild(runs):
    assert runs["pre_task4_refused"] == 1
    assert runs["rebuild"]["digest"] == runs["one_shot"]["digest"]


# --- approved counts ---


def test_approved_per_file_counts(runs):
    columns = AUDIT_COLUMNS
    counted = {}
    for row in runs["batch_004"]["audit"]:
        values = dict(zip(columns, row, strict=True))
        if values["status"] == "ACCEPTED":
            counted[(values["batch_id"], values["file_name"])] = (
                values["received_count"], values["accepted_count"],
                *(values[c] for c in TASK4_COLUMNS if c != "reconciliation_status"),
            )
            assert values["reconciliation_status"] == "RECONCILED"
    assert counted == APPROVED_PER_FILE


def test_approved_totals(runs):
    state = runs["batch_004"]
    audit = [dict(zip(AUDIT_COLUMNS, row, strict=True)) for row in state["audit"] if row[AUDIT_COLUMNS.index("status")] == "ACCEPTED"]

    def total(column):
        return sum(row[column] for row in audit)

    assert total("new_encounter_count") == 3274
    assert total("new_version_count") == 267
    assert total("duplicate_count") == 70
    assert total("stale_count") == 25
    assert total("quarantined_count") == 0
    assert total("history_rows_written") == state["counts"]["clean.encounter_versions"] == 3541
    assert state["counts"]["clean.encounter_current"] == 3274
    assert state["counts"]["clean.encounter_row_outcomes"] == state["counts"]["raw.encounters"] == 3636


# --- batch_004 and idempotency ---


def test_rejected_batch_004_changes_no_derived_state(runs):
    after_003, after_004 = runs["batch_003"], runs["batch_004"]

    for table in DERIVED_TABLES:
        assert after_004["digest_all"][table] == after_003["digest_all"][table], table
    assert after_004["current_totals"] == after_003["current_totals"]
    new_rows = [row for row in after_004["audit"] if row not in after_003["audit"]]
    assert after_004["audit"][: len(after_003["audit"])] == after_003["audit"]  # earlier rows untouched, timings too
    assert len(new_rows) == 3
    for row in new_rows:
        values = dict(zip(AUDIT_COLUMNS, row, strict=True))
        assert (values["batch_id"], values["status"]) == ("batch_004", "REJECTED")
        assert all(values[c] is None for c in TASK4_COLUMNS), values["file_name"]


def test_rerun_adds_and_changes_nothing(runs):
    before, after = runs["batch_004"], runs["rerun"]

    assert after["digest_all"] == before["digest_all"]  # every column, timings included
    assert after["counts"] == before["counts"]
    assert after["audit"] == before["audit"]
    assert after["csv_bytes"] == before["csv_bytes"]


# --- late arrivals ---


def test_late_updates_change_historical_months_but_not_the_as_of_state(runs):
    after_002, final = runs["batch_002"], runs["batch_004"]

    # As of batch_002, read from the final history, is exactly what batch_002 left as current:
    # later versions did not overwrite it.
    assert final["asof_002_totals"] == after_002["current_totals"]
    # The current state does differ in months that existed at batch_002 ...
    changed = {
        month for month, totals in after_002["current_totals"].items() if final["current_totals"].get(month) != totals
    }
    assert changed
    # ... including months whose encounters got a newer version in batch_003 (late updates),
    # all of them months that had already been reported.
    late = final["late_update_months"]
    assert late and late <= set(after_002["current_totals"])
    assert changed & late
    assert max(late) < (2025, 1)  # batch_003 was delivered 2025-01-20


# --- cleaned version fields (Task 5 group a) ---

# Every reason and warning on current versions, as counted in the Task 5 design profile.
APPROVED_CURRENT_FLAGS = {
    ("facility_id_reason", "FACILITY_UNRESOLVED"): 31,
    ("admit_date_reason", "DATE_INVALID"): 12,
    ("admit_date_reason", "DATE_AFTER_DELIVERY"): 4,
    ("admit_date_reason", "DATE_PLACEHOLDER"): 2,
    ("discharge_date_reason", "DATE_MISSING"): 868,
    ("discharge_date_warning", "DISCHARGE_BEFORE_ADMIT"): 7,
    ("encounter_type_warning", "ENCOUNTER_TYPE_MISSING"): 18,
    ("payer_category_warning", "PAYER_MISSING"): 12,
    ("primary_dx_code_reason", "DX_ICD9"): 14,
    ("primary_dx_code_reason", "DX_UNPARSEABLE"): 10,
    ("primary_dx_code_reason", "DX_PLACEHOLDER"): 2,
    ("primary_dx_code_warning", "DX_NOT_IN_REFERENCE"): 18,
    ("attending_npi_reason", "NPI_PLACEHOLDER"): 2,
    ("attending_npi_reason", "NPI_INVALID_FORMAT"): 9,
    ("attending_npi_reason", "NPI_CHECKSUM_FAILED"): 4,
    ("attending_npi_reason", "NPI_MISSING"): 5,
    ("attending_npi_warning", "NPI_NOT_IN_ROSTER"): 18,
    ("billed_amount_usd_reason", "AMOUNT_PLACEHOLDER"): 21,
    ("billed_amount_usd_reason", "AMOUNT_MISSING"): 14,
    ("billed_amount_usd_reason", "AMOUNT_SPREADSHEET_ERROR"): 8,
    ("billed_amount_usd_reason", "AMOUNT_UNPARSEABLE"): 12,
}
# Superseded versions: the profile found 59 missing discharge dates and 1 unresolved facility and
# no other reason. Their warnings (NPI roster, discharge before admit) were first counted here: none.
APPROVED_SUPERSEDED_FLAGS = {
    ("discharge_date_reason", "DATE_MISSING"): 59,
    ("facility_id_reason", "FACILITY_UNRESOLVED"): 1,
}


def test_one_fields_row_per_version(runs):
    counts = runs["batch_004"]["counts"]

    assert counts["clean.encounter_version_fields"] == counts["clean.encounter_versions"] == 3541


def test_approved_field_reasons_and_warnings(runs):
    flags = runs["batch_004"]["field_flags"]

    assert flags["current"] == APPROVED_CURRENT_FLAGS
    assert flags["superseded"] == APPROVED_SUPERSEDED_FLAGS


def test_every_null_cleaned_value_has_a_reason(runs):
    for run in FINAL_RUNS:
        assert runs[run]["null_without_reason"] == 0, run


# --- mart dimensions (Task 5 group b) ---


def test_dim_provider_rows_match_an_independent_recount_of_the_roster():
    assert scd2_rows_from_roster_files() == 65


def test_dim_provider_scd2_rows(runs):
    state = runs["batch_004"]

    assert state["counts"]["mart.dim_provider"] == scd2_rows_from_roster_files() == 65
    # 50 rows from the earliest snapshot apply backwards; 12 start in July 2024 and 3 in January 2025.
    assert state["dim_provider_starts"] == {VALID_FROM_EARLIEST: 50, date(2024, 7, 1): 12, date(2025, 1, 1): 3}


def test_point_in_time_provider_lookup_on_current_versions(runs):
    lookup = runs["batch_004"]["provider_lookup"]

    # The Task 5 design profile: 2,374 + 844 found in the snapshot applicable at admission,
    # 18 valid NPIs in no roster, 18 admit dates unknown, and 20 invalid NPIs (2 + 9 + 4 + 5).
    assert lookup["outcomes"] == {
        "found, admit before 2024-07-01": 2374,
        "found, admit from 2024-07-01": 844,
        "NPI_NOT_IN_ROSTER": 18,
        "PROVIDER_ADMIT_DATE_UNKNOWN": 18,
        "NPI_PLACEHOLDER": 2,
        "NPI_INVALID_FORMAT": 9,
        "NPI_CHECKSUM_FAILED": 4,
        "NPI_MISSING": 5,
    }
    assert lookup["differs_from_latest"] == 336  # why SCD2 matters: point-in-time differs from the latest snapshot
    assert lookup["sql_matches"] == 2374 + 844  # the SQL join on valid_from/valid_to agrees with the lookup


def test_other_dimensions(runs):
    state = runs["batch_004"]

    assert state["counts"]["mart.dim_facility"] == 8
    assert state["counts"]["mart.dim_payer"] == 6
    assert state["dim_diagnosis_in_reference"] == {True: 43, False: 3}
    assert state["dim_date_range"] == (date(2023, 1, 1), date(2025, 12, 31), 1096)
    assert state["dates_outside_dim_date"] == 0


# --- mart facts and dim_patient (Task 5 group c) ---


def test_fact_and_patient_row_counts(runs):
    counts = runs["batch_004"]["counts"]

    assert counts["mart.fact_encounter_version"] == 3541
    assert counts["mart.fact_encounter_current"] == 3274
    assert counts["mart.dim_patient"] == 907  # every patient_key in clean.encounter_patients
    assert runs["batch_004"]["facts"]["current_matches_view"] == 3274


@pytest.mark.parametrize("run", FINAL_RUNS)
def test_fact_integrity(runs, run):
    """Every NULL has a reason, every non-NULL foreign key exists, every row's lineage resolves to a raw row."""
    integrity = runs[run]["facts"]["integrity"]

    assert integrity and all(count == 0 for count in integrity.values()), {k: v for k, v in integrity.items() if v}


def test_fact_distributions_match_the_profile(runs):
    facts = runs["batch_004"]["facts"]

    # Profile: 2,385 stays computed, 7 discharges before admit, 18 admit dates and 864 more
    # discharge dates unavailable (868 missing discharges, 4 of them already lacking an admit date).
    assert facts["length_of_stay_reasons"] == {
        None: 2385,
        "DISCHARGE_BEFORE_ADMIT": 7,
        "LENGTH_OF_STAY_ADMIT_UNAVAILABLE": 18,
        "LENGTH_OF_STAY_DISCHARGE_UNAVAILABLE": 864,
    }
    assert facts["provider_reasons"] == {
        None: 3218, "NPI_NOT_IN_ROSTER": 18, "PROVIDER_ADMIT_DATE_UNKNOWN": 18,
        "NPI_PLACEHOLDER": 2, "NPI_INVALID_FORMAT": 9, "NPI_CHECKSUM_FAILED": 4, "NPI_MISSING": 5,
    }  # fmt: skip
    assert facts["version_counts"] == {1: 3029, 2: 223, 3: 22}
    # Profile: 895 LINKED and 12 UNLINKED patient keys; 1 (a linked one) varies in sex or ZIP3.
    assert facts["patients"] == {"LINKED": 894, "LINKED, varies": 1, "UNLINKED": 12}


# --- CSV exports (Task 5 group d) ---


@pytest.mark.parametrize("run", FINAL_RUNS[1:])
def test_exported_csvs_are_byte_identical(runs, run):
    """Every table CSV is byte for byte the same; batch_audit.csv is compared without its timings above."""
    reference = runs["batch_004"]["exports"]

    assert sorted(reference) == sorted(file_name(t) for t in EXPORTED_TABLES)
    assert runs[run]["exports"].keys() == reference.keys()
    for name, content in reference.items():
        assert runs[run]["exports"][name] == content, (run, name)


def test_rejected_batch_004_changes_no_exported_csv(runs):
    assert runs["batch_004"]["exports"] == runs["batch_003"]["exports"]
