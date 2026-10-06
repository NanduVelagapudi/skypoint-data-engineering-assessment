"""A sentinel PHI-like value must never reach logs, exceptions, audit rows, the audit CSV or the DQ report.

Every failure path that sees delivered content is exercised with the sentinel
inside that content. The sentinel is also checked to have reached
raw.encounters, so the test cannot pass by never seeing it.
"""

import json
import logging
from pathlib import Path

import pytest
from conftest import CONTRACT_PATH, FileSpec, contract_header, csv_bytes, valid_specs, write_batch

from pipeline import batch_processor
from pipeline.main import main
from pipeline.raw_store import open_store
from pipeline.schema_contract import load_contracts

pytestmark = pytest.mark.usefixtures("restore_pipeline_logger")

CONTRACTS = load_contracts(CONTRACT_PATH)
SENTINEL = "ZZPHI"
EPIC = "encounters_epic_north.csv"
MEDITECH = "encounters_legacy_meditech.csv"


def _with_epic(epic: FileSpec, tag: str) -> list[FileSpec]:
    return [epic, *valid_specs(tag)[1:]]


def build_landing(landing_dir: Path) -> None:
    header = contract_header("EPIC_NORTH")

    # 001: valid batch; every value carries the sentinel, so raw holds PHI-like data.
    write_batch(landing_dir, "batch_001", valid_specs(SENTINEL))

    # 002: malformed record whose values carry the sentinel and a quoted comma.
    rows = [[f"{SENTINEL}, {c}" for c in header], [f"{SENTINEL} short row"]]
    write_batch(landing_dir, "batch_002", _with_epic(FileSpec(EPIC, "EPIC_NORTH", csv_bytes(header, rows), 2), "b"))

    # 003: file sent without a header: its first data row is read as the header.
    first_row = [f"{SENTINEL}-{c}" for c in header]
    content = csv_bytes(first_row, [[f"{SENTINEL}{i}" for i in range(len(header))]])
    write_batch(landing_dir, "batch_003", _with_epic(FileSpec(EPIC, "EPIC_NORTH", content, 1), "c"))

    # 004: unterminated quote around a sentinel value.
    content = csv_bytes(header, []) + f'"{SENTINEL} unterminated'.encode("utf-8")
    write_batch(landing_dir, "batch_004", _with_epic(FileSpec(EPIC, "EPIC_NORTH", content, 1), "d"))

    # 005: unexpected column named with the sentinel, plus an unlisted CSV named with it.
    extra_header = [*header, SENTINEL]
    content = csv_bytes(extra_header, [[f"v{i}" for i in range(len(extra_header))]])
    batch_dir = write_batch(landing_dir, "batch_005", _with_epic(FileSpec(EPIC, "EPIC_NORTH", content, 1), "e"))
    (batch_dir / f"{SENTINEL}_export.csv").write_bytes(b"x\n")

    # 006: sentinel in the manifest's batch_id, delivered_at, source_system and file_name.
    batch_dir = write_batch(
        landing_dir,
        "batch_006",
        valid_specs("f"),
        manifest_overrides={EPIC: {"source_system": SENTINEL}, MEDITECH: {"file_name": f"{SENTINEL}.csv"}},
    )
    manifest = json.loads((batch_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest.update(batch_id=SENTINEL, delivered_at=SENTINEL)
    (batch_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    # 007: invalid UTF-8 right after a sentinel value.
    content = csv_bytes(header, []) + SENTINEL.encode("utf-8") + b"\xff\xfe\n"
    write_batch(landing_dir, "batch_007", _with_epic(FileSpec(EPIC, "EPIC_NORTH", content, 1), "g"))


def assert_no_sentinel_in_logs(capsys, caplog):
    captured = capsys.readouterr()
    assert SENTINEL not in captured.out
    assert SENTINEL not in captured.err
    assert caplog.records, "expected log records to inspect"
    for record in caplog.records:
        # Every attribute, including `extra` keys the formatter would drop.
        for value in vars(record).values():
            assert SENTINEL not in str(value), record.getMessage()


def test_sentinel_never_leaves_the_raw_layer(landing_dir, pipeline_env, capsys, caplog):
    build_landing(landing_dir)
    env = {**pipeline_env, "LOG_LEVEL": "DEBUG"}

    assert main(env) == 0

    assert_no_sentinel_in_logs(capsys, caplog)
    con = open_store(Path(env["RAW_DB_PATH"]), CONTRACTS.canonical_columns)
    try:
        statuses = dict(con.execute("SELECT DISTINCT batch_id, status FROM ops.batch_audit").fetchall())
        audit_text = repr(con.execute("SELECT * FROM ops.batch_audit").fetchall())
        dq_text = repr(con.execute("SELECT * FROM ops.dq_report").fetchall())
        dq_report_rows = con.execute("SELECT count(*) FROM ops.dq_report").fetchone()[0]
        raw_hits = con.execute(
            "SELECT count(*) FROM raw.encounters WHERE patient_mrn LIKE ?", [f"%{SENTINEL}%"]
        ).fetchone()[0]
    finally:
        con.close()

    assert statuses == {"batch_001": "ACCEPTED", **{f"batch_00{i}": "REJECTED" for i in range(2, 8)}}
    assert raw_hits == 9  # the sentinel did reach the restricted raw layer
    assert SENTINEL not in audit_text
    assert dq_report_rows > 0 and SENTINEL not in dq_text  # every batch, accepted or rejected, is reported
    assert SENTINEL not in (Path(env["OUTPUT_DIR"]) / "batch_audit.csv").read_text(encoding="utf-8")


def test_exception_message_with_sentinel_is_not_logged(landing_dir, pipeline_env, capsys, caplog, monkeypatch):
    write_batch(landing_dir, "batch_001", valid_specs(SENTINEL))

    def failing_write(*args, **kwargs):
        raise RuntimeError(f"driver error near value '{SENTINEL}-patient_last_name'")

    monkeypatch.setattr(batch_processor.raw_store, "write_accepted_batch", failing_write)

    assert main(pipeline_env) == 1

    assert_no_sentinel_in_logs(capsys, caplog)
    [failure] = [r for r in caplog.records if r.getMessage() == "pipeline_failed"]
    assert failure.error_type == "RuntimeError"
