"""Task 6 group (b): the publish gate, ops.gate_rejected_issues and ops.quarantine.

Synthetic Epic batches (conftest.epic_file: valid rows apart from the given
changes; patient values are fake) run through main() into tmp_path. The
pipeline_env fixture disables the gate (threshold 1), so each test sets the
threshold it needs. Real-data pins live in test_incremental_parity.py.
"""

import json
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import attach, epic_file, state_digest, write_batch

from pipeline import batch_processor, dq_report, dq_rules, publish_gate, quarantine
from pipeline.batch_audit import TASK4_COLUMNS
from pipeline.main import main

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

EPIC = "encounters_epic_north.csv"
BAD_FACILITY = {"facility_name": "Nowhere Clinic"}
SENTINEL = "ZZPHI"
# Tables a gate-rejected batch may add rows to; every other table must stay exactly as it was.
REPORTING_TABLES = ("ops.batch_audit", "ops.dq_report", "ops.quarantine", "ops.gate_rejected_issues")


def good(prefix, n):
    return [(f"{prefix}{i}", {}) for i in range(1, n + 1)]


def with_threshold(env, share):
    return {**env, "DQ_GATE_MAX_ERROR_SHARE": share}


def query(env, sql, params=()):
    con = attach(Path(env["RAW_DB_PATH"]))
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def audit(env, batch_id):
    return query(env, "SELECT file_name, status, received_count, accepted_count, reason FROM ops.batch_audit "
                      "WHERE batch_id = ? ORDER BY file_name", [batch_id])  # fmt: skip


def report(env, batch_id, check_code):
    rows = query(env, f"SELECT {', '.join(dq_report.COLUMNS)} FROM {dq_report.TABLE} "
                      "WHERE batch_id = ? AND check_code = ? ORDER BY file_name NULLS FIRST", [batch_id, check_code])  # fmt: skip
    return [dict(zip(dq_report.COLUMNS, r, strict=True)) for r in rows]


def quarantine_rows(env):
    rows = query(env, f"SELECT {', '.join(quarantine.COLUMNS)} FROM {quarantine.TABLE} ORDER BY {quarantine.ORDER_BY}")
    return [dict(zip(quarantine.COLUMNS, r, strict=True)) for r in rows]


def events(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]


def digest_without_reporting(env):
    return {t: d for t, d in state_digest(Path(env["RAW_DB_PATH"])).items() if t not in REPORTING_TABLES}


REPORTING_CSVS = ("batch_audit.csv", "dq_report.csv", "quarantine.csv")  # they report rejected batches too


def exported(env):
    return {p.name: p.read_bytes() for p in Path(env["OUTPUT_DIR"]).glob("*.csv") if p.name not in REPORTING_CSVS}


def csv_lines(env, name):
    return (Path(env["OUTPUT_DIR"]) / name).read_text(encoding="utf-8").splitlines()


# --- unit ------------------------------------------------------------------


@pytest.mark.parametrize(
    "error_rows, received, share, blocked",
    [
        (1, 20, "0.05", False),  # exactly 5%: not MORE than the threshold
        (2, 20, "0.05", True),
        (0, 0, "0", False),  # an empty batch passes
        (0, 10, "0", False),
        (1, 10, "0", True),  # 0 blocks any error
        (10, 10, "1", False),  # 1 never blocks
        (44, 2863, "0.05", False),  # batch_001
    ],
)
def test_the_gate_blocks_only_more_than_the_threshold_share(error_rows, received, share, blocked):
    assert publish_gate.exceeds(error_rows, received, Decimal(share)) is blocked


def test_threshold_pct():
    assert publish_gate.threshold_pct(Decimal("0.05")) == Decimal("5.00")
    assert publish_gate.threshold_pct(Decimal("0.125")) == Decimal("12.50")


@pytest.mark.parametrize(
    "reason, check_code",
    [
        ("SOURCE_RECORD_ID_MISSING", "SOURCE_RECORD_ID_PRESENT"),
        ("TIMESTAMP_UNPARSEABLE", "LAST_UPDATED_TS_VALID"),
        ("VERSION_CONFLICT_SAME_TS", "VERSION_CONFLICT_SAME_TS"),
    ],
)
def test_a_task4_quarantined_row_fails_its_row_check(reason, check_code):
    assert publish_gate.row_issues("QUARANTINED", reason, {}) == [(check_code, reason)]


def test_a_row_is_judged_by_the_errors_of_its_version_only():
    errors = {dq_rules.SRC_FACILITY: "FACILITY_UNRESOLVED", dq_rules.SRC_ADMIT: "DATE_INVALID"}
    warnings = {dq_rules.SRC_DISCHARGE: "DATE_MISSING", dq_rules.SRC_AMOUNT: "AMOUNT_PLACEHOLDER"}

    assert publish_gate.row_issues("DUPLICATE", "DUPLICATE_IN_BATCH", {**errors, **warnings}) == [
        ("ADMIT_DATE_VALID", "DATE_INVALID"), ("FACILITY_RESOLVED", "FACILITY_UNRESOLVED")]
    assert publish_gate.row_issues("NEW_ENCOUNTER", None, warnings) == []


def test_every_version_error_can_be_evaluated_inside_the_batch_transaction():
    assert dq_rules.GATE_VERSION_SOURCES == (dq_rules.SRC_FACILITY, dq_rules.SRC_ADMIT)
    assert set(dq_rules.GATE_CHECKS) == {
        "SOURCE_RECORD_ID_PRESENT", "LAST_UPDATED_TS_VALID", "VERSION_CONFLICT_SAME_TS",
        "FACILITY_RESOLVED", "ADMIT_DATE_VALID"}  # fmt: skip


def test_the_duplicate_file_code_matches_the_batch_processor():
    assert dq_rules.DUPLICATE_FILE == batch_processor.DUPLICATE_FILE


# --- a batch the gate rejects ----------------------------------------------


@pytest.fixture
def gate_rejected(landing_dir, pipeline_env, roster_npi, capsys):
    """batch_001 clean and published; batch_002 with 2 of 10 rows unresolved (20% > 5%); batch_003 clean."""
    env = with_threshold(pipeline_env, "0.05")
    write_batch(landing_dir, "batch_001", [epic_file(good("A", 10), roster_npi=roster_npi)])
    assert main(env) == 0
    before = {"digest": digest_without_reporting(env), "exports": exported(env),
              "reporting": {name: csv_lines(env, name) for name in ("dq_report.csv", "quarantine.csv")}}  # fmt: skip
    bad = [*good("B", 8), ("B9", BAD_FACILITY), ("B10", BAD_FACILITY)]
    write_batch(landing_dir, "batch_002", [epic_file(bad, roster_npi=roster_npi)])
    capsys.readouterr()

    assert main(env) == 0

    return {"env": env, "before": before, "events": events(capsys), "landing": landing_dir}


def test_a_rejected_batch_is_audited_with_its_gate_numbers(gate_rejected):
    env = gate_rejected["env"]

    assert audit(env, "batch_002") == [
        (EPIC, "REJECTED", 10, 0, "DQ_GATE_FAILED(error_rows=2,received=10,threshold_pct=5.00)")]
    task4 = query(env, f"SELECT {', '.join(TASK4_COLUMNS)} FROM ops.batch_audit WHERE batch_id = 'batch_002'")
    assert task4 == [(None,) * len(TASK4_COLUMNS)]  # nothing was kept, as for any rejected batch


def test_a_rejected_batch_publishes_nothing(gate_rejected):
    env = gate_rejected["env"]

    assert digest_without_reporting(env) == gate_rejected["before"]["digest"]
    assert exported(env) == gate_rejected["before"]["exports"]
    for name, lines_before in gate_rejected["before"]["reporting"].items():  # only batch_002's lines are added
        lines = csv_lines(env, name)
        assert [line for line in lines if not line.startswith("batch_002,")] == lines_before, name
        assert any(line.startswith("batch_002,") for line in lines), name
    for table, column in (("raw.encounters", "batch_id"), ("raw.ingested_files", "batch_id"),
                          ("clean.encounter_row_outcomes", "batch_id"), ("clean.encounter_versions", "first_seen_batch_id"),
                          ("clean.encounter_version_fields", "source_batch_id"), ("clean.encounter_patients", "batch_id"),
                          ("mart.fact_encounter_version", "source_batch_id")):  # fmt: skip
        assert query(env, f"SELECT count(*) FROM {table} WHERE {column} = 'batch_002'") == [(0,)], table


def test_the_rejection_is_a_logged_data_outcome(gate_rejected):
    by_event = {}
    for event in gate_rejected["events"]:
        by_event.setdefault(event["event"], []).append(event)

    [gate] = [e for e in by_event["publish_gate_evaluated"] if e["batch_id"] == "batch_002"]
    assert (gate["level"], gate["status"], gate["received_count"], gate["error_count"]) == ("WARNING", "FAILED", 10, 2)
    [rollback] = by_event["batch_write_rolled_back"]
    assert (rollback["level"], rollback["error_type"]) == ("WARNING", "PublishGateFailed")
    [rejected] = by_event["batch_rejected"]
    assert rejected["reason_code"] == "DQ_GATE_FAILED"
    assert "pipeline_failed" not in by_event


def test_the_failing_rows_are_kept_with_lineage_and_codes(gate_rejected):
    env = gate_rejected["env"]

    assert query(env, f"SELECT {', '.join(publish_gate.COLUMNS)} FROM {publish_gate.TABLE} ORDER BY ALL") == [
        ("batch_002", EPIC, 9, "EPIC_NORTH", "B9", "FACILITY_RESOLVED", "FACILITY_UNRESOLVED"),
        ("batch_002", EPIC, 10, "EPIC_NORTH", "B10", "FACILITY_RESOLVED", "FACILITY_UNRESOLVED"),
    ]


def test_a_rejected_batch_in_the_quarantine(gate_rejected):
    rows = [r for r in quarantine_rows(gate_rejected["env"]) if r["batch_id"] == "batch_002"]

    assert [(r["file_name"], r["source_row_number"], r["quarantine_level"], r["quarantine_source"], r["batch_status"],
             r["source_record_id"], r["reason_codes"], r["row_count"]) for r in rows] == [
        (EPIC, None, "FILE", "PUBLISH_GATE", "REJECTED", None, "DQ_GATE_FAILED", 10),
        (EPIC, 9, "ROW", "PUBLISH_GATE", "REJECTED", "B9", "FACILITY_UNRESOLVED", 1),
        (EPIC, 10, "ROW", "PUBLISH_GATE", "REJECTED", "B10", "FACILITY_UNRESOLVED", 1),
    ]  # fmt: skip
    assert rows[0]["reason_detail"] == "DQ_GATE_FAILED(error_rows=2,received=10,threshold_pct=5.00)"
    assert all(r["version_key"] is None and r["is_current_version"] is None for r in rows)


def test_a_rejected_batch_in_the_dq_report(gate_rejected):
    env = gate_rejected["env"]

    [gate] = report(env, "batch_002", "PUBLISH_GATE")
    assert (gate["evaluated_count"], gate["observed_count"], gate["observed_pct"], gate["threshold_pct"],
            gate["status"], gate["reason_codes"]) == (10, 2, Decimal("20.00"), Decimal("5.00"), "FAIL", "DQ_GATE_FAILED")  # fmt: skip
    [facility] = report(env, "batch_002", "FACILITY_RESOLVED")
    assert (facility["evaluated_count"], facility["observed_count"], facility["status"], facility["reason_codes"]) == (
        10, 2, "FAIL", "FACILITY_UNRESOLVED")  # counted on rows: the versions were rolled back
    [admit] = report(env, "batch_002", "ADMIT_DATE_VALID")
    assert (admit["observed_count"], admit["status"]) == (0, "PASS")
    assert [r["status"] for r in report(env, "batch_002", "FILE_SHA256_MATCH")] == ["PASS"]
    assert [r["status"] for r in report(env, "batch_002", "REJECTED_BATCH_NOT_LOADED")] == ["PASS"]
    for warning_check in ("DISCHARGE_DATE_VALID", "RECON_RAW_ROWS", "RECON_QUARANTINE_ROWS"):
        [row] = report(env, "batch_002", warning_check)
        assert (row["status"], row["reason_codes"]) == ("NOT_EVALUATED", "BATCH_REJECTED"), warning_check


def test_processing_continues_after_a_gate_rejection(gate_rejected, roster_npi):
    env = gate_rejected["env"]
    write_batch(gate_rejected["landing"], "batch_003", [epic_file(good("C", 10), roster_npi=roster_npi)])

    assert main(env) == 0
    assert [r[1] for r in audit(env, "batch_003")] == ["ACCEPTED"]
    [gate] = report(env, "batch_003", "PUBLISH_GATE")
    assert (gate["observed_count"], gate["status"]) == (0, "PASS")


# --- where the line is drawn -----------------------------------------------


def test_the_threshold_itself_passes_and_one_row_more_fails(landing_dir, pipeline_env, roster_npi, capsys):
    env = with_threshold(pipeline_env, "0.10")
    write_batch(landing_dir, "batch_001", [epic_file([*good("A", 9), ("A10", BAD_FACILITY)], roster_npi=roster_npi)])
    write_batch(landing_dir, "batch_002", [epic_file([*good("B", 8), ("B9", BAD_FACILITY), ("B10", BAD_FACILITY)],
                                                     roster_npi=roster_npi)])  # fmt: skip
    capsys.readouterr()

    assert main(env) == 0

    assert [r[1] for r in audit(env, "batch_001")] == ["ACCEPTED"]  # 10% is not more than 10%
    assert [r[1] for r in audit(env, "batch_002")] == ["REJECTED"]  # 20% is
    # The report recomputes the accepted batch's gate with the same rules the gate used.
    [logged] = [e for e in events(capsys) if e["event"] == "publish_gate_evaluated" and e["batch_id"] == "batch_001"]
    [gate] = report(env, "batch_001", "PUBLISH_GATE")
    assert (gate["evaluated_count"], gate["observed_count"]) == (logged["received_count"], logged["error_count"]) == (10, 1)
    assert (gate["observed_pct"], gate["threshold_pct"], gate["status"]) == (Decimal("10.00"), Decimal("10.00"), "PASS")


def test_warnings_never_count_towards_the_gate(landing_dir, pipeline_env, roster_npi):
    """Every row has five warnings; with threshold 0 a single error would block."""
    warnings = {"discharge_date": "", "billed_amount": "N/A", "payer_name": "", "primary_dx_code": "250.00",
                "attending_npi": "12345"}  # fmt: skip
    env = with_threshold(pipeline_env, "0")
    write_batch(landing_dir, "batch_001", [epic_file([(f"W{i}", warnings) for i in range(1, 6)], roster_npi=roster_npi)])

    assert main(env) == 0

    assert [r[1] for r in audit(env, "batch_001")] == ["ACCEPTED"]
    assert query(env, "SELECT DISTINCT severity FROM clean.version_dq_issues") == [("WARNING",)]
    assert query(env, "SELECT count(*) FROM clean.version_dq_issues") == [(25,)]
    [gate] = report(env, "batch_001", "PUBLISH_GATE")
    assert (gate["observed_count"], gate["status"]) == (0, "PASS")


def test_task4_quarantined_rows_count_towards_the_gate(landing_dir, pipeline_env, roster_npi):
    env = with_threshold(pipeline_env, "0.05")
    write_batch(landing_dir, "batch_001", [epic_file([*good("A", 9), ("", {})], roster_npi=roster_npi)])

    assert main(env) == 0

    assert audit(env, "batch_001")[0][4] == "DQ_GATE_FAILED(error_rows=1,received=10,threshold_pct=5.00)"
    assert query(env, f"SELECT source_row_number, check_code, reason_code FROM {publish_gate.TABLE}") == [
        (10, "SOURCE_RECORD_ID_PRESENT", "SOURCE_RECORD_ID_MISSING")]


def test_every_copy_of_an_error_version_counts(landing_dir, pipeline_env, roster_npi):
    """One unresolved version sent twice: 2 of 10 rows fail (20% > 15%), though only 1 of 9 versions does."""
    env = with_threshold(pipeline_env, "0.15")
    rows = [*good("A", 8), ("A9", BAD_FACILITY), ("A9", BAD_FACILITY)]
    write_batch(landing_dir, "batch_001", [epic_file(rows, roster_npi=roster_npi)])

    assert main(env) == 0

    assert audit(env, "batch_001")[0][4] == "DQ_GATE_FAILED(error_rows=2,received=10,threshold_pct=15.00)"
    assert query(env, f"SELECT source_row_number FROM {publish_gate.TABLE} ORDER BY 1") == [(9,), (10,)]


# --- rules that change after publication -----------------------------------


@pytest.fixture
def published_with_errors(landing_dir, pipeline_env, roster_npi):
    """batch_001 with 1 of 10 rows unresolved, published while the gate was off (threshold 1)."""
    write_batch(landing_dir, "batch_001", [epic_file([*good("A", 9), ("A10", BAD_FACILITY)], roster_npi=roster_npi)])
    assert main(pipeline_env) == 0
    return pipeline_env


def test_a_rebuild_fails_when_a_published_batch_now_fails_the_gate(published_with_errors, capsys):
    env = published_with_errors
    before = state_digest(Path(env["RAW_DB_PATH"]), exclude_run_times=False)
    capsys.readouterr()

    assert main(with_threshold(env, "0.05"), ["--rebuild-derived"]) == 1

    failed = [e for e in events(capsys) if e["event"] == "published_batch_fails_gate"]
    assert [(e["batch_id"], e["received_count"], e["error_count"], e["reason_code"]) for e in failed] == [
        ("batch_001", 10, 1, "DQ_GATE_FAILED")]
    assert state_digest(Path(env["RAW_DB_PATH"]), exclude_run_times=False) == before  # everything rolled back
    assert main(env, ["--rebuild-derived"]) == 0  # with the threshold it was published under, it rebuilds


def test_a_normal_run_reports_a_published_batch_that_now_exceeds_the_threshold(published_with_errors):
    env = with_threshold(published_with_errors, "0.05")

    assert main(env) == 0

    [gate] = report(env, "batch_001", "PUBLISH_GATE")
    assert (gate["batch_status"], gate["observed_count"], gate["threshold_pct"], gate["status"]) == (
        "ACCEPTED", 1, Decimal("5.00"), "FAIL")  # still published; the report shows today's rules fail it


# --- the quarantine of accepted batches ------------------------------------


def test_quarantine_of_accepted_batches(landing_dir, pipeline_env, roster_npi):
    """E1 fails then is fixed by a newer version; E2 passes then its newer version fails; one row has no id."""
    t1, t2 = {"last_updated_ts": "2024-03-01T10:00:00Z"}, {"last_updated_ts": "2024-03-02T10:00:00Z"}
    write_batch(landing_dir, "batch_001", [epic_file([("E1", {**t1, **BAD_FACILITY}), ("E2", t1), ("", t1)],
                                                     roster_npi=roster_npi)])  # fmt: skip
    write_batch(landing_dir, "batch_002", [epic_file([("E1", t2), ("E2", {**t2, **BAD_FACILITY})], roster_npi=roster_npi)])

    assert main(pipeline_env) == 0

    assert [(r["batch_id"], r["source_row_number"], r["quarantine_level"], r["quarantine_source"], r["source_record_id"],
             r["is_current_version"], r["reason_codes"], r["version_key"] is not None) for r in quarantine_rows(pipeline_env)] == [
        ("batch_001", 1, "VERSION", "VERSION_DQ", "E1", False, "FACILITY_UNRESOLVED", True),
        ("batch_001", 3, "ROW", "HISTORY_ORDERING", "", None, "SOURCE_RECORD_ID_MISSING", False),
        ("batch_002", 2, "VERSION", "VERSION_DQ", "E2", True, "FACILITY_UNRESOLVED", True),
    ]  # fmt: skip
    # The failing newest version of E2 stays current (no fallback to the passing older one).
    assert query(pipeline_env, "SELECT source_batch_id, facility_id_reason FROM mart.fact_encounter_current "
                               "WHERE source_record_id = 'E2'") == [("batch_002", "FACILITY_UNRESOLVED")]  # fmt: skip
    # Task 4 and Task 6 stay apart: only the row without an id is in quarantined_count.
    assert query(pipeline_env, "SELECT batch_id, quarantined_count FROM ops.batch_audit ORDER BY 1") == [
        ("batch_001", 1), ("batch_002", 0)]
    assert {r["status"] for b in ("batch_001", "batch_002") for r in report(pipeline_env, b, "RECON_QUARANTINE_ROWS")} == {"PASS"}


def test_a_validation_rejected_batch_is_one_entry_per_file(landing_dir, pipeline_env, roster_npi):
    write_batch(landing_dir, "batch_001", [epic_file(good("A", 3), roster_npi=roster_npi)],
                manifest_overrides={EPIC: {"sha256": "0" * 64}})

    assert main(pipeline_env) == 0

    [entry] = quarantine_rows(pipeline_env)
    assert (entry["quarantine_level"], entry["quarantine_source"], entry["reason_codes"], entry["reason_detail"],
            entry["row_count"]) == ("FILE", "BATCH_VALIDATION", "SHA256_MISMATCH", "SHA256_MISMATCH", 3)  # fmt: skip


# --- no PHI ----------------------------------------------------------------


def test_phi_never_reaches_the_gate_tables_audit_or_logs(landing_dir, pipeline_env, roster_npi, capsys, caplog):
    phi = {c: f"{SENTINEL}-{c}" for c in ("patient_mrn", "patient_first_name", "patient_last_name", "patient_dob",
                                          "patient_phone", "patient_zip", "chief_complaint")}  # fmt: skip
    rows = [(f"A{i}", phi) for i in range(1, 9)] + [(f"A{i}", {**phi, "facility_name": f"{SENTINEL} clinic"}) for i in (9, 10)]
    write_batch(landing_dir, "batch_001", [epic_file(rows, roster_npi=roster_npi)])

    assert main({**with_threshold(pipeline_env, "0.05"), "LOG_LEVEL": "DEBUG"}) == 0

    assert [r[1] for r in audit(pipeline_env, "batch_001")] == ["REJECTED"]
    for table in ("ops.batch_audit", publish_gate.TABLE, quarantine.TABLE, dq_report.TABLE):
        content = query(pipeline_env, f"SELECT * FROM {table}")
        assert content and SENTINEL not in repr(content), table
    assert SENTINEL not in capsys.readouterr().out
    assert all(SENTINEL not in str(v) for record in caplog.records for v in vars(record).values())
