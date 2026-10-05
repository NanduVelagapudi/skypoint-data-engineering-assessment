"""Single entry point: python -m pipeline.main

Processes every pending landing batch in order, then exports
output/batch_audit.csv.

Exit codes:
  0  the run completed, including when batches were rejected (a data outcome)
  1  a pipeline/system failure: bad configuration, missing landing folder,
     unwritable database or output, or any unexpected exception
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Mapping

from pipeline.batch_audit import BatchStatus, export_csv
from pipeline.batch_processor import BatchResult, run_pending_batches
from pipeline.config import Settings, load_settings
from pipeline.errors import PipelineError
from pipeline.logging_setup import configure_logging
from pipeline.raw_store import open_store
from pipeline.schema_contract import load_contracts

# Not __name__: under `python -m pipeline.main` that is "__main__", which sits
# outside the configured "pipeline" logger and would lose these log lines.
log = logging.getLogger("pipeline.main")

AUDIT_CSV_NAME = "batch_audit.csv"


def run(settings: Settings) -> list[BatchResult]:
    contracts = load_contracts(settings.schema_contract_path)
    if not settings.landing_dir.is_dir():
        raise PipelineError("landing folder not found")

    con = open_store(settings.raw_db_path, contracts.canonical_columns)
    try:
        results = run_pending_batches(con, settings.landing_dir, contracts)
        export_csv(con, settings.output_dir / AUDIT_CSV_NAME)
    finally:
        con.close()
    return results


def main(env: Mapping[str, str] | None = None) -> int:
    configure_logging("INFO")  # until settings are loaded
    step = "config"
    try:
        settings = load_settings(env)
        configure_logging(settings.log_level)
        step = "run"
        log.info("pipeline_started", extra={"step": "start"})
        results = run(settings)
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
            "accepted_count": sum(r.accepted_rows for r in results),
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
