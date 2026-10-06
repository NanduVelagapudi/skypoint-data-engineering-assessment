"""CSV exports of the clean and mart tables, the README query files, and a PHI sentinel scan.

Synthetic batches run through main() into tmp_path, with the real reference
files (read only). Real-data checks live in test_real_data_exports.py and the
byte-level parity in test_incremental_parity.py.
"""

import csv
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import REPO_ROOT, FileSpec, attach, contract_header, csv_bytes, write_batch

from pipeline import dq_report, exports, quarantine, version_dq
from pipeline.chronic_acute_export import FILE_NAME as CHRONIC_ACUTE_FILE
from pipeline.clean_patients import PHI_COLUMNS
from pipeline.errors import PipelineError
from pipeline.exports import EXPORTED_TABLES, NULLS_FIRST, export_table, file_name, format_value
from pipeline.main import main

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

SQL_DIR = REPO_ROOT / "sql"
QUERY_FILES = sorted(SQL_DIR.glob("q*.sql"))
EXPORT_FILES = sorted([*(file_name(t) for t in EXPORTED_TABLES), "batch_audit.csv", CHRONIC_ACUTE_FILE])


def run_query(con, name, **params):
    return con.execute((SQL_DIR / name).read_text(encoding="utf-8"), params).fetchall()


def read_csv(path):
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))


def encounter_file(system, rows):
    header = contract_header(system)
    records = []
    for record_id, ts, changes in rows:
        values = {column: f"{column}-zz" for column in header}
        values.update(
            source_system=system, source_record_id=record_id, last_updated_ts=ts,
            facility_name="Lakeshore General Hospital", admit_date="03/05/2024", discharge_date="03/07/2024",
            encounter_type="IP", claim_status="Paid", payer_name="Medicare", primary_dx_code="E11.9",
            billed_amount="$5,000.00",
        )  # fmt: skip
        values.update(changes)
        records.append([values[column] for column in header])
    file = {"EPIC_NORTH": "encounters_epic_north.csv", "LEGACY_MEDITECH": "encounters_legacy_meditech.csv"}[system]
    return FileSpec(file, system, csv_bytes(header, records), len(records))


BATCH = [
    encounter_file("EPIC_NORTH", [
        ("E1", "2024-03-01T10:00:00Z", {}),
        ("E2", "2024-03-01T10:00:00Z", {"claim_status": ""}),  # NULL claim_status
        ("E3", "2024-03-01T10:00:00Z", {"claim_status": "Void", "billed_amount": "$1,000.50"}),
        ("", "2024-03-01T10:00:00Z", {}),  # quarantined: no source_record_id
    ])
]  # fmt: skip


@pytest.fixture
def loaded(landing_dir, pipeline_env):
    write_batch(landing_dir, "batch_001", BATCH)
    assert main(pipeline_env) == 0
    return pipeline_env


# --- value formats ---


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, ""),
        (True, "true"),
        (False, "false"),
        (date(2024, 3, 5), "2024-03-05"),
        (datetime(2024, 3, 1, 10, 0, 0), "2024-03-01T10:00:00Z"),
        (datetime(2024, 3, 1, 10, 0, 0, 250000), "2024-03-01T10:00:00.250000Z"),
        (Decimal("5000"), "5000.00"),
        (Decimal("18450.5"), "18450.50"),
        (Decimal("0.10"), "0.10"),
        (3, "3"),
        ("FAC001", "FAC001"),
    ],
)
def test_format_value(value, expected):
    assert format_value(value) == expected


@pytest.mark.parametrize("value", [1.5, datetime(2024, 3, 1, tzinfo=timezone.utc)])
def test_floats_and_aware_timestamps_are_refused(value):
    with pytest.raises(ValueError):
        format_value(value)


# --- files ---


def test_every_table_is_exported_with_its_columns_ordered_by_primary_key(loaded):
    output = Path(loaded["OUTPUT_DIR"])

    assert sorted(p.name for p in output.iterdir()) == EXPORT_FILES  # no temp files left behind
    con = attach(Path(loaded["RAW_DB_PATH"]))
    try:
        for table, key in EXPORTED_TABLES.items():
            header, *rows = read_csv(output / file_name(table))
            assert header == [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()], table
            assert len(rows) == con.execute(f"SELECT count(*) FROM {table}").fetchone()[0], table
            columns = [c.removesuffix(NULLS_FIRST) for c in key]
            positions = [header.index(c) for c in columns]

            def sort_value(value, column):
                if column == "source_row_number":
                    return int(value) if value else -1  # NULL (empty) sorts first
                return value  # an empty string (NULL) already sorts first

            keys = [tuple(sort_value(r[i], c) for i, c in zip(positions, columns)) for r in rows]
            assert keys == sorted(keys) and len(set(keys)) == len(keys), table
    finally:
        con.close()


def test_amounts_dates_timestamps_and_nulls_are_formatted(loaded):
    header, *rows = read_csv(Path(loaded["OUTPUT_DIR"]) / "fact_encounter_current.csv")
    by_id = {r[header.index("source_record_id")]: dict(zip(header, r, strict=True)) for r in rows}

    assert by_id["E3"]["billed_amount_usd"] == "1000.50"
    assert by_id["E1"]["billed_amount_usd"] == "5000.00"
    assert by_id["E1"]["admit_date"] == "2024-03-05"
    assert by_id["E1"]["last_updated_ts_utc"] == "2024-03-01T10:00:00Z"
    assert (by_id["E2"]["claim_status"], by_id["E2"]["claim_status_reason"]) == ("", "CLAIM_STATUS_MISSING")
    all_amounts = [r[header.index("billed_amount_usd")] for r in rows]
    assert all(re.fullmatch(r"-?\d+\.\d{2}", a) for a in all_amounts if a)


def test_exports_are_rewritten_identically(loaded):
    output = Path(loaded["OUTPUT_DIR"])
    before = {p.name: p.read_bytes() for p in output.iterdir() if p.name != "batch_audit.csv"}

    assert main(loaded) == 0

    assert {p.name: p.read_bytes() for p in output.iterdir() if p.name != "batch_audit.csv"} == before


def test_only_clean_mart_and_ops_tables_can_be_exported(loaded, tmp_path):
    con = attach(Path(loaded["RAW_DB_PATH"]))
    try:
        with pytest.raises(PipelineError, match="not in an exportable schema"):
            export_table(con, "raw.encounters", ("batch_id",), tmp_path / "x.csv")
        con.execute("CREATE TABLE clean.zz_phi_probe (version_key VARCHAR, patient_first_name_raw VARCHAR)")
        with pytest.raises(PipelineError, match="PHI columns"):
            export_table(con, "clean.zz_phi_probe", ("version_key",), tmp_path / "y.csv")
    finally:
        con.close()
    assert not (tmp_path / "x.csv").exists() and not (tmp_path / "y.csv").exists()


def test_no_exported_table_has_a_phi_column(loaded):
    for name in EXPORT_FILES:
        header = read_csv(Path(loaded["OUTPUT_DIR"]) / name)[0]
        assert not {c for c in header if c in PHI_COLUMNS or c.removesuffix("_raw") in PHI_COLUMNS}, name


def test_a_failed_export_leaves_no_temp_file(loaded, monkeypatch):
    def fail(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(exports.os, "replace", fail)

    assert main(loaded) == 1
    assert not [p for p in Path(loaded["OUTPUT_DIR"]).iterdir() if p.suffix == ".tmp"]


# --- PHI sentinel ---

SENTINELS = {
    "patient_mrn": "ZZMRNSENTINEL01",
    "patient_first_name": "Zzfirstsentinel",
    "patient_last_name": "Zzlastsentinel",
    "patient_phone": "555-019-8765",
    "chief_complaint": "Zzcomplaint sentinel call 555-019-8765",
}
SENTINEL_DOB = {"EPIC_NORTH": ("02/29/1904", "1904-02-29"), "LEGACY_MEDITECH": ("29/02/1904", "1904-02-29")}
SENTINEL_ZIP = "99954"


def test_phi_sentinels_never_reach_an_exported_csv(landing_dir, pipeline_env):
    batch = [
        encounter_file(system, [(f"S{i}", "2024-03-01T10:00:00Z" if system == "EPIC_NORTH" else "01/03/2024 10:00:00",
                                 {**SENTINELS, "patient_dob": SENTINEL_DOB[system][0], "patient_zip": SENTINEL_ZIP,
                                  "admit_date": "03/05/2024" if system == "EPIC_NORTH" else "05/03/2024",
                                  "discharge_date": ""})
                                for i in (1, 2)])
        for system in ("EPIC_NORTH", "LEGACY_MEDITECH")
    ]  # fmt: skip
    write_batch(landing_dir, "batch_001", batch)

    assert main(pipeline_env) == 0

    con = attach(Path(pipeline_env["RAW_DB_PATH"]))
    try:  # the sentinels did reach the restricted raw layer, so the scan below is meaningful
        assert con.execute("SELECT count(*) FROM raw.encounters WHERE patient_mrn = ?", [SENTINELS["patient_mrn"]]).fetchone() == (4,)
    finally:
        con.close()
    forbidden = [*SENTINELS.values(), "ZZMRN", "Zzfirst", "Zzlast", "Zzcomplaint", "5550198765",
                 *SENTINEL_DOB["EPIC_NORTH"], SENTINEL_DOB["LEGACY_MEDITECH"][0], SENTINEL_ZIP]
    output = Path(pipeline_env["OUTPUT_DIR"])
    assert sorted(p.name for p in output.iterdir()) == EXPORT_FILES
    for path in output.iterdir():
        text = path.read_text(encoding="utf-8")
        for value in forbidden:
            assert value.lower() not in text.lower(), (path.name, "a sentinel value")
    header, *rows = read_csv(output / "encounter_patients.csv")
    assert {r[header.index("zip3")] for r in rows} == {"999"}  # ZIP3 is allowed; the full ZIP is not


# --- README query files ---


def test_there_are_six_query_files_reading_no_raw_table():
    assert [p.name for p in QUERY_FILES] == [
        "q1_monthly_volume_current.sql",
        "q2_encounter_version_history.sql",
        "q3_export_row_lineage.sql",
        "q4_provider_at_encounter.sql",
        "q5_monthly_volume_as_of.sql",
        "q6_quarantined_and_rejected.sql",
    ]
    for path in QUERY_FILES:
        sql = "\n".join(line.split("--")[0] for line in path.read_text(encoding="utf-8").splitlines())
        assert "raw." not in sql, path.name


@pytest.mark.parametrize("name", ["q1_monthly_volume_current.sql", "q5_monthly_volume_as_of.sql"])
def test_monthly_queries_use_is_distinct_from_void(name):
    sql = (SQL_DIR / name).read_text(encoding="utf-8")

    assert "IS DISTINCT FROM 'VOID'" in sql
    assert "<> 'VOID'" not in "\n".join(line.split("--")[0] for line in sql.splitlines())


def test_monthly_queries_keep_a_null_claim_status_and_drop_void(loaded):
    con = attach(Path(loaded["RAW_DB_PATH"]))
    try:
        current = run_query(con, "q1_monthly_volume_current.sql")
        as_of = run_query(con, "q5_monthly_volume_as_of.sql", as_of_batch="batch_001")
    finally:
        con.close()

    # E1 (PAID) and E2 (NULL claim_status) are counted; E3 (VOID) is not.
    assert current == as_of == [(2024, 3, "FAC001", "Lakeshore General Hospital", "INPATIENT", 2, Decimal("10000.00"))]


def test_quarantined_rows_and_rejected_files_are_listed(loaded, landing_dir):
    write_batch(landing_dir, "batch_002", BATCH, manifest_overrides={"encounters_epic_north.csv": {"sha256": "0" * 64}})
    assert main(loaded) == 0
    con = attach(Path(loaded["RAW_DB_PATH"]))
    try:
        rows = run_query(con, "q6_quarantined_and_rejected.sql")
    finally:
        con.close()

    assert rows == [
        ("ROW", "HISTORY_ORDERING", "batch_001", "encounters_epic_north.csv", 4, "", None, None,
         "SOURCE_RECORD_ID_MISSING", None),
        ("FILE", "BATCH_VALIDATION", "batch_002", "encounters_epic_north.csv", None, None, None, None,
         "SHA256_MISMATCH", "SHA256_MISMATCH"),
    ]  # fmt: skip


# --- Task 6: analytics exclude ERROR versions; the DQ tables are exported ---

GOOD = {"facility_name": "Lakeshore General Hospital"}
UNRESOLVED = {"facility_name": "Nowhere Clinic"}
T2 = "2024-03-02T10:00:00Z"


@pytest.fixture
def with_errors(landing_dir, pipeline_env):
    """batch_001: E1 passes, E2 has an unresolved facility, E3 is VOID, E4 passes.
    batch_002: E4's newer version has an unresolved facility (it becomes current; no fallback)."""
    write_batch(landing_dir, "batch_001", [encounter_file("EPIC_NORTH", [
        ("E1", "2024-03-01T10:00:00Z", GOOD),
        ("E2", "2024-03-01T10:00:00Z", UNRESOLVED),
        ("E3", "2024-03-01T10:00:00Z", {"claim_status": "Void"}),
        ("E4", "2024-03-01T10:00:00Z", GOOD),
    ])])  # fmt: skip
    write_batch(landing_dir, "batch_002", [encounter_file("EPIC_NORTH", [("E4", T2, UNRESOLVED)])])
    assert main(pipeline_env) == 0  # the fixture's gate threshold is 1: both batches are published
    return pipeline_env


def test_monthly_queries_exclude_error_versions_without_fallback(with_errors):
    con = attach(Path(with_errors["RAW_DB_PATH"]))
    try:
        current = run_query(con, "q1_monthly_volume_current.sql")
        as_of_001 = run_query(con, "q5_monthly_volume_as_of.sql", as_of_batch="batch_001")
        as_of_002 = run_query(con, "q5_monthly_volume_as_of.sql", as_of_batch="batch_002")
        still_current = con.execute(
            "SELECT source_record_id, source_batch_id, facility_id_reason FROM mart.fact_encounter_current "
            "WHERE facility_id IS NULL ORDER BY 1").fetchall()  # fmt: skip
    finally:
        con.close()

    month = (2024, 3, "FAC001", "Lakeshore General Hospital", "INPATIENT")
    assert current == as_of_002 == [(*month, 1, Decimal("5000.00"))]  # E1 only: E2, E4 are ERRORs, E3 is VOID
    assert as_of_001 == [(*month, 2, Decimal("10000.00"))]  # at batch_001, E4's passing version was current
    # The ERROR versions stay current in the mart; the queries leave them out.
    assert still_current == [("E2", "batch_001", "FACILITY_UNRESOLVED"), ("E4", "batch_002", "FACILITY_UNRESOLVED")]


def test_q6_lists_version_errors_with_their_current_flag(with_errors):
    con = attach(Path(with_errors["RAW_DB_PATH"]))
    try:
        rows = run_query(con, "q6_quarantined_and_rejected.sql")
    finally:
        con.close()

    assert [(r[0], r[1], r[2], r[4], r[5], r[7], r[8]) for r in rows] == [
        ("VERSION", "VERSION_DQ", "batch_001", 2, "E2", True, "FACILITY_UNRESOLVED"),
        ("VERSION", "VERSION_DQ", "batch_002", 1, "E4", True, "FACILITY_UNRESOLVED"),
    ]


@pytest.mark.parametrize(
    "csv_name, module",
    [("version_dq_issues.csv", version_dq), ("quarantine.csv", quarantine), ("dq_report.csv", dq_report)],
)
def test_dq_csvs_have_the_fixed_columns_and_order(with_errors, csv_name, module):
    header, *rows = read_csv(Path(with_errors["OUTPUT_DIR"]) / csv_name)
    con = attach(Path(with_errors["RAW_DB_PATH"]))
    try:
        order = getattr(module, "ORDER_BY", "version_key, check_code")
        stored = con.execute(f"SELECT * FROM {module.TABLE} ORDER BY {order}").fetchall()
    finally:
        con.close()

    assert header == list(module.COLUMNS)
    assert rows == [[format_value(v) for v in row] for row in stored]  # same rows, same order, same formats


def test_dq_report_csv_formats(with_errors):
    header, *rows = read_csv(Path(with_errors["OUTPUT_DIR"]) / "dq_report.csv")
    by_key = {(r[0], r[3], r[5]): dict(zip(header, r, strict=True)) for r in rows}

    assert [r[3] for r in rows[:2]] == ["", ""]  # batch rows (file_name NULL) come first
    gate = by_key[("batch_001", "", "PUBLISH_GATE")]
    assert (gate["evaluated_count"], gate["observed_count"], gate["observed_pct"], gate["threshold_pct"]) == (
        "4", "1", "25.00", "100.00")
    facility = by_key[("batch_001", "encounters_epic_north.csv", "FACILITY_RESOLVED")]
    assert (facility["observed_pct"], facility["expected_count"], facility["status"]) == ("25.00", "", "FAIL")
    assert {r[header.index("threshold_pct")] for r in rows if r[5] != "PUBLISH_GATE"} == {""}


def test_export_order_matches_the_dq_tables_order():
    assert ", ".join(EXPORTED_TABLES[dq_report.TABLE]) == dq_report.ORDER_BY
    assert ", ".join(EXPORTED_TABLES[quarantine.TABLE]) == quarantine.ORDER_BY
