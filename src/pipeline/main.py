"""Single entry point: python -m pipeline.main [--rebuild-derived]

Processes every pending landing batch in order (each accepted batch also adds
its rows to the encounter history, Task 4, and the cleaned Task 2 fields of its
new versions to clean.encounter_version_fields), rebuilds the PHI-free
clean.encounter_patients table (Task 3) and the mart tables (Task 5), then
exports output/batch_audit.csv and one CSV per PHI-free clean and mart table.

--rebuild-derived first rebuilds the Task 4 history and audit counts from the
raw layer, replaying every accepted batch through the same step as an
incremental load, then carries on as a normal run. It is also how a database
from before Task 4, which a normal run refuses, is migrated.

Exit codes:
  0  the run completed, including when batches were rejected (a data outcome)
  1  a pipeline/system failure: bad configuration (including a missing
     PATIENT_KEY_HMAC_SECRET), missing landing folder, unwritable database or
     output, a database from before Task 4 without --rebuild-derived, or any
     unexpected exception
  2  unknown command-line arguments
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Mapping, Sequence

from pipeline.batch_audit import BatchStatus, export_csv
from pipeline.batch_processor import BatchResult, rebuild_derived, run_pending_batches
from pipeline.clean_patients import build_encounter_patients
from pipeline.config import Settings, load_settings, require_patient_key_secret
from pipeline.errors import PipelineError
from pipeline.exports import export_tables
from pipeline.logging_setup import configure_logging
from pipeline.raw_store import open_store
from pipeline.reference_data import load_cleaning_reference, load_warehouse_reference
from pipeline.schema_contract import load_contracts
from pipeline.source_conventions import load_source_conventions
from pipeline.warehouse import rebuild_mart

# Not __name__: under `python -m pipeline.main` that is "__main__", which sits
# outside the configured "pipeline" logger and would lose these log lines.
log = logging.getLogger("pipeline.main")

AUDIT_CSV_NAME = "batch_audit.csv"


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m pipeline.main")
    parser.add_argument(
        "--rebuild-derived",
        action="store_true",
        help="rebuild the Task 4 history and audit counts from the raw layer before processing pending batches",
    )
    return parser.parse_args(list(argv))


def run(settings: Settings, secret: bytes, rebuild_derived_state: bool = False) -> list[BatchResult]:
    contracts = load_contracts(settings.schema_contract_path)
    conventions = load_source_conventions(settings.reference_dir / "source_systems_and_facilities.json")
    reference = load_cleaning_reference(settings.reference_dir, settings.facility_aliases_path)
    warehouse_reference = load_warehouse_reference(settings.reference_dir)
    if not settings.landing_dir.is_dir():
        raise PipelineError("landing folder not found")

    con = open_store(settings.raw_db_path, contracts.canonical_columns, allow_outdated_audit=rebuild_derived_state)
    try:
        if rebuild_derived_state:
            rebuild_derived(con, conventions, reference)
        results = run_pending_batches(con, settings.landing_dir, contracts, conventions, reference)
        build_encounter_patients(con, conventions, secret)
        rebuild_mart(con, warehouse_reference)
        export_csv(con, settings.output_dir / AUDIT_CSV_NAME)
        export_tables(con, settings.output_dir)
    finally:
        con.close()
    return results


def main(env: Mapping[str, str] | None = None, argv: Sequence[str] = ()) -> int:
    args = parse_args(argv)  # exits 2 on unknown arguments, before any work
    configure_logging("INFO")  # until settings are loaded
    step = "config"
    try:
        settings = load_settings(env)
        configure_logging(settings.log_level)
        # Checked before any work, so a missing secret never leaves a partial run behind.
        secret = require_patient_key_secret(settings)
        step = "run"
        log.info("pipeline_started", extra={"step": "start"})
        results = run(settings, secret, rebuild_derived_state=args.rebuild_derived)
    except Exception as exc:
        # Type only: an exception message could quote source data.
        log.error("pipeline_failed", extra={"step": step, "error_type": type(exc).__name__})
        return 1

    rejected = [r for r in results if r.status == BatchStatus.REJECTED]
    log.info(
        "pipeline_finished",
        extra={
            "step": "finish",
            "status": "COMPLETED_WITH_REJECTIONS" if rejected else "COMPLETED",
            "received_count": sum(r.received_rows for r in results),
            "accepted_count": sum(r.accepted_rows for r in results),
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(argv=sys.argv[1:]))
