"""Rebuild the mart schema (Task 5) and the DQ tables (Task 6) in one transaction on every run.

The dimensions come first, because the facts look providers up in
dim_provider. The DQ tables come last: clean.version_dq_issues reads the facts,
ops.quarantine reads the issues, and ops.dq_report reads both and checks the
pipeline invariants. All tables are dropped and recreated together, so a
failure (including a failed invariant) leaves the previous mart and DQ tables
untouched rather than new dimensions beside old facts.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Mapping
from decimal import Decimal

import duckdb

from pipeline.dimensions import write_dimensions
from pipeline.dq_report import write_dq_report
from pipeline.encounter_facts import write_facts
from pipeline.quarantine import write_quarantine
from pipeline.reference_data import WarehouseReference
from pipeline.source_conventions import SourceConventions
from pipeline.version_dq import write_version_issues

log = logging.getLogger(__name__)


def rebuild_mart(
    con: duckdb.DuckDBPyConnection,
    reference: WarehouseReference,
    conventions: Mapping[str, SourceConventions],
    max_error_share: Decimal,
) -> dict[str, int]:
    """Rows per mart and DQ table after the rebuild; max_error_share is the publish gate's threshold."""
    con.begin()
    try:
        counts = write_dimensions(con, reference)
        counts |= write_facts(con)
        counts |= write_version_issues(con, conventions)
        counts |= write_quarantine(con)
        counts |= write_dq_report(con, max_error_share)
        con.commit()
    except BaseException as exc:
        with contextlib.suppress(duckdb.Error):  # a failed commit may already have rolled back
            con.rollback()
        log.error("mart_rolled_back", extra={"step": "mart", "error_type": type(exc).__name__})
        raise
    log.info("mart_built", extra={"step": "mart"})
    return counts
