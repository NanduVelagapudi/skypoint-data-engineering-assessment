"""Task 5 dimensions in the mart schema, rebuilt deterministically on every run.

    mart.dim_provider   SCD2 over the roster snapshots, one row per NPI per
                        stretch of unchanged attributes
    mart.dim_facility   the facility master
    mart.dim_diagnosis  the ICD-10 reference, plus valid ICD-10 codes seen in
                        the data that the reference lacks (in_reference = false)
    mart.dim_payer      the six payer categories
    mart.dim_date       every day of the calendar years spanned by the cleaned
                        admit and discharge dates

dim_provider (SCD2). A snapshot's values hold from its as_of_date until the
next snapshot. Every roster attribute is tracked, names included; a new row
starts when any of them changes, or when the NPI reappears after missing from
a snapshot. Rows that start at the earliest snapshot are valid from 0001-01-01
(the earliest snapshot also applies to all earlier dates); rows that start
later are valid from their snapshot date only. valid_to is exclusive:
9999-12-31 for a row still open in the latest snapshot, otherwise the date of
the snapshot that changed or dropped the NPI.

Point-in-time lookup (ProviderLookup.at) for an encounter's NPI and admit date.
No row means no provider, never the latest snapshot instead:
    NPI invalid                       the NPI's own reason (from the cleaned fields)
    valid NPI in no snapshot          NPI_NOT_IN_ROSTER
    admit date unknown                PROVIDER_ADMIT_DATE_UNKNOWN
    no row covers the admit date      PROVIDER_NOT_ON_ROSTER_AT_ADMIT

All five tables are dropped and recreated inside the mart rebuild transaction
(warehouse.rebuild_mart). Their inputs are the reference files and, for
dim_diagnosis and dim_date, the cleaned version fields, so the same inputs
always give the same rows. No table holds patient
data; dim_provider holds provider names from the roster.
"""

from __future__ import annotations

import hashlib
import json
import logging
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal

import duckdb

from pipeline.errors import ConfigError
from pipeline.parsers.categorical import PayerCategory
from pipeline.parsers.result import FieldReason
from pipeline.reference_data import (
    FACILITY_MASTER_FILE,
    ICD10_REFERENCE_FILE,
    FacilityRecord,
    Icd10Entry,
    RosterSnapshot,
    WarehouseReference,
)
from pipeline.version_fields import TABLE as FIELDS_TABLE

log = logging.getLogger(__name__)

SCHEMA = "mart"
PROVIDER_TABLE = "mart.dim_provider"
FACILITY_TABLE = "mart.dim_facility"
DIAGNOSIS_TABLE = "mart.dim_diagnosis"
PAYER_TABLE = "mart.dim_payer"
DATE_TABLE = "mart.dim_date"

VALID_FROM_EARLIEST = date(1, 1, 1)
VALID_TO_OPEN = date(9999, 12, 31)
PROVIDER_KEY_VERSION = "provider/v1"
TRACKED_ATTRIBUTES = (
    "provider_last_name",
    "provider_first_name",
    "credential",
    "specialty",
    "primary_facility_id",
    "employment_status",
)


@dataclass(frozen=True)
class ProviderRow:
    provider_sk: str
    npi: str
    provider_last_name: str
    provider_first_name: str
    credential: str
    specialty: str
    primary_facility_id: str
    employment_status: str
    valid_from: date
    valid_to: date  # exclusive
    is_current: bool  # still open in the latest snapshot
    snapshot_as_of_date: date  # the snapshot that started the row
    source_file_name: str
    source_row_number: int


def provider_sk(npi: str, valid_from: date) -> str:
    payload = json.dumps([PROVIDER_KEY_VERSION, npi, valid_from.isoformat()], separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_provider_rows(snapshots: Sequence[RosterSnapshot], facility_ids: Iterable[str]) -> tuple[ProviderRow, ...]:
    """The SCD2 rows, ordered by NPI then valid_from. The input order of snapshots does not matter."""
    ordered = sorted(snapshots, key=lambda s: s.as_of_date)
    if len({s.as_of_date for s in ordered}) != len(ordered):
        raise ConfigError("two roster snapshots have the same as_of_date")
    known_facilities = set(facility_ids)
    by_npi = [{entry.npi: entry for entry in snapshot.entries} for snapshot in ordered]

    rows: list[ProviderRow] = []
    for npi in sorted({npi for entries in by_npi for npi in entries}):
        open_row = None  # (entry that started the row, valid_from)
        for index, (snapshot, entries) in enumerate(zip(ordered, by_npi, strict=True)):
            entry = entries.get(npi)
            if entry is not None and entry.primary_facility_id not in known_facilities:
                raise ConfigError(
                    f"roster file {entry.file_name} row {entry.source_row_number}: primary_facility_id is not in the facility master"
                )
            unchanged = (
                entry is not None
                and open_row is not None
                and all(getattr(entry, a) == getattr(open_row[0], a) for a in TRACKED_ATTRIBUTES)
            )
            if unchanged:
                continue
            if open_row is not None:
                rows.append(_provider_row(*open_row, valid_to=snapshot.as_of_date))
                open_row = None
            if entry is not None:
                open_row = (entry, VALID_FROM_EARLIEST if index == 0 else snapshot.as_of_date)
        if open_row is not None:
            rows.append(_provider_row(*open_row, valid_to=VALID_TO_OPEN))
    return tuple(rows)


def _provider_row(entry, valid_from: date, *, valid_to: date) -> ProviderRow:
    return ProviderRow(
        provider_sk=provider_sk(entry.npi, valid_from),
        npi=entry.npi,
        **{a: getattr(entry, a) for a in TRACKED_ATTRIBUTES},
        valid_from=valid_from,
        valid_to=valid_to,
        is_current=valid_to == VALID_TO_OPEN,
        snapshot_as_of_date=entry.as_of_date,
        source_file_name=entry.file_name,
        source_row_number=entry.source_row_number,
    )


class ProviderLookup:
    """Point-in-time lookup of dim_provider rows by NPI and admit date."""

    def __init__(self, rows: Iterable[ProviderRow]):
        self._rows: dict[str, list[ProviderRow]] = defaultdict(list)
        for row in sorted(rows, key=lambda r: (r.npi, r.valid_from)):
            self._rows[row.npi].append(row)
        self._starts = {npi: [r.valid_from for r in rows] for npi, rows in self._rows.items()}

    def at(
        self, npi: str | None, admit_date: date | None, npi_reason: str | None = None
    ) -> tuple[str | None, str | None]:
        """(provider_sk, None) for the row covering admit_date, or (None, reason)."""
        if npi is None:
            return None, npi_reason
        rows = self._rows.get(npi)
        if not rows:
            return None, str(FieldReason.NPI_NOT_IN_ROSTER)
        if admit_date is None:
            return None, str(FieldReason.PROVIDER_ADMIT_DATE_UNKNOWN)
        index = bisect_right(self._starts[npi], admit_date) - 1
        if index >= 0 and admit_date < rows[index].valid_to:
            return rows[index].provider_sk, None
        return None, str(FieldReason.PROVIDER_NOT_ON_ROSTER_AT_ADMIT)


def facility_rows(records: Iterable[FacilityRecord]) -> list[dict[str, object]]:
    return [
        {**{k: v for k, v in asdict(r).items() if k != "source_position"},
         "source_file_name": FACILITY_MASTER_FILE, "source_position": r.source_position}
        for r in sorted(records, key=lambda r: r.facility_id)
    ]


def diagnosis_rows(entries: Iterable[Icd10Entry], observed_codes: Iterable[str]) -> list[dict[str, object]]:
    """The reference codes, plus observed valid codes not in it (description, category, is_chronic unknown)."""
    rows = {
        e.icd10_code: {
            "icd10_code": e.icd10_code,
            "description": e.description,
            "category": e.category,
            "is_chronic": e.is_chronic,
            "in_reference": True,
            "source_file_name": ICD10_REFERENCE_FILE,
            "source_row_number": e.source_row_number,
        }
        for e in entries
    }
    for code in observed_codes:
        rows.setdefault(code, {
            "icd10_code": code, "description": None, "category": None, "is_chronic": None,
            "in_reference": False, "source_file_name": None, "source_row_number": None,
        })  # fmt: skip
    return [rows[code] for code in sorted(rows)]


def payer_rows() -> list[dict[str, object]]:
    return [{"payer_category": str(category)} for category in PayerCategory]


# --- storage ---------------------------------------------------------------

_DDL = {
    PROVIDER_TABLE: """
        provider_sk VARCHAR NOT NULL PRIMARY KEY, npi VARCHAR NOT NULL,
        provider_last_name VARCHAR NOT NULL, provider_first_name VARCHAR NOT NULL, credential VARCHAR NOT NULL,
        specialty VARCHAR NOT NULL, primary_facility_id VARCHAR NOT NULL, employment_status VARCHAR NOT NULL,
        valid_from DATE NOT NULL, valid_to DATE NOT NULL, is_current BOOLEAN NOT NULL,
        snapshot_as_of_date DATE NOT NULL, source_file_name VARCHAR NOT NULL, source_row_number INTEGER NOT NULL,
        UNIQUE (npi, valid_from), CHECK (valid_from < valid_to)""",
    FACILITY_TABLE: """
        facility_id VARCHAR NOT NULL PRIMARY KEY, facility_name VARCHAR NOT NULL, source_system VARCHAR NOT NULL,
        facility_type VARCHAR NOT NULL, city VARCHAR NOT NULL, state VARCHAR NOT NULL, bed_count INTEGER NOT NULL,
        ownership VARCHAR NOT NULL, go_live_date DATE NOT NULL,
        source_file_name VARCHAR NOT NULL, source_position INTEGER NOT NULL""",
    DIAGNOSIS_TABLE: """
        icd10_code VARCHAR NOT NULL PRIMARY KEY, description VARCHAR, category VARCHAR, is_chronic BOOLEAN,
        in_reference BOOLEAN NOT NULL, source_file_name VARCHAR, source_row_number INTEGER""",
    PAYER_TABLE: "payer_category VARCHAR NOT NULL PRIMARY KEY",
    DATE_TABLE: """
        date_key DATE NOT NULL PRIMARY KEY, year INTEGER NOT NULL, quarter INTEGER NOT NULL, month INTEGER NOT NULL,
        month_name VARCHAR NOT NULL, year_month VARCHAR NOT NULL, day_of_month INTEGER NOT NULL,
        iso_day_of_week INTEGER NOT NULL, day_name VARCHAR NOT NULL, is_weekend BOOLEAN NOT NULL""",
}

_MONTH_NAMES = ("January", "February", "March", "April", "May", "June", "July", "August", "September",
                "October", "November", "December")  # fmt: skip
_DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


def _columns(con: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute(f"DESCRIBE {table}").fetchall()]


def _json_value(value: object) -> object:
    if isinstance(value, datetime):  # TIMESTAMP columns hold naive UTC
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):  # exact text, never a float
        return str(value)
    return value


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: Sequence[Mapping[str, object]]) -> None:
    """Insert rows into a mart table as one JSON document (see raw_store), cast to its column types in SQL.

    Every value travels as text, so DECIMAL amounts are never floats.
    """
    if not rows:
        return
    columns = _columns(con, table)
    expected = {name for name, _ in columns}
    if any(set(row) != expected for row in rows):
        raise ValueError(f"rows for {table} do not have exactly its columns")  # a typo would otherwise load as NULL
    document = json.dumps(
        [{name: _json_value(value) for name, value in row.items()} for row in rows],
        ensure_ascii=False,
    )
    schema = json.dumps([{name: "VARCHAR" for name, _ in columns}])
    selected = ", ".join(f"CAST(r.{name} AS {kind})" for name, kind in columns)
    con.execute(
        f"INSERT INTO {table} SELECT {selected} FROM (SELECT unnest(from_json(?, ?)) AS r)", [document, schema]
    )


def date_rows(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    """Every day of the full calendar years spanned by the cleaned admit and discharge dates."""
    first, last = con.execute(
        f"SELECT min(d), max(d) FROM (SELECT admit_date AS d FROM {FIELDS_TABLE} "
        f"UNION ALL SELECT discharge_date FROM {FIELDS_TABLE})"
    ).fetchone()
    if first is None:
        return []
    rows = []
    day = date(first.year, 1, 1)
    while day <= date(last.year, 12, 31):
        rows.append({
            "date_key": day, "year": day.year, "quarter": (day.month - 1) // 3 + 1, "month": day.month,
            "month_name": _MONTH_NAMES[day.month - 1], "year_month": f"{day.year:04d}-{day.month:02d}",
            "day_of_month": day.day, "iso_day_of_week": day.isoweekday(),
            "day_name": _DAY_NAMES[day.isoweekday() - 1], "is_weekend": day.isoweekday() >= 6,
        })  # fmt: skip
        day = date.fromordinal(day.toordinal() + 1)
    return rows


def observed_unreferenced_codes(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT primary_dx_code FROM {FIELDS_TABLE} "
            f"WHERE primary_dx_code_warning = '{FieldReason.DX_NOT_IN_REFERENCE}' ORDER BY 1"
        ).fetchall()
    ]


def write_dimensions(con: duckdb.DuckDBPyConnection, reference: WarehouseReference) -> dict[str, int]:
    """Drop and recreate the five dimensions inside the caller's transaction; returns rows per table."""
    providers = build_provider_rows(reference.roster, (f.facility_id for f in reference.facilities))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    contents = {
        PROVIDER_TABLE: [asdict(p) for p in providers],
        FACILITY_TABLE: facility_rows(reference.facilities),
        DIAGNOSIS_TABLE: diagnosis_rows(reference.icd10, observed_unreferenced_codes(con)),
        PAYER_TABLE: payer_rows(),
        DATE_TABLE: date_rows(con),
    }
    for table, rows in contents.items():
        con.execute(f"CREATE OR REPLACE TABLE {table} ({_DDL[table]})")
        insert_rows(con, table, rows)
    return {table: len(rows) for table, rows in contents.items()}
