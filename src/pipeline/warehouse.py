"""Rebuild the mart schema (Task 5) in one transaction on every run.

The dimensions come first, because the facts look providers up in
dim_provider. All tables are dropped and recreated together, so a failure
leaves the previous mart untouched rather than new dimensions beside old facts.
"""

from __future__ import annotations

import contextlib
import logging

import duckdb

from pipeline.dimensions import write_dimensions
from pipeline.encounter_facts import write_facts
from pipeline.reference_data import WarehouseReference

log = logging.getLogger(__name__)


def rebuild_mart(con: duckdb.DuckDBPyConnection, reference: WarehouseReference) -> dict[str, int]:
    """Rows per mart table after the rebuild."""
    con.begin()
    try:
        counts = write_dimensions(con, reference)
        counts |= write_facts(con)
        con.commit()
    except BaseException as exc:
        with contextlib.suppress(duckdb.Error):  # a failed commit may already have rolled back
            con.rollback()
        log.error("mart_rolled_back", extra={"step": "mart", "error_type": type(exc).__name__})
        raise
    log.info("mart_built", extra={"step": "mart"})
    return counts
