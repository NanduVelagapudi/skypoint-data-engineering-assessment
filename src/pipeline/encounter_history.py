"""Task 4: encounter versions, one outcome per raw row, and the current state.

Identity
    encounter  (source_system, source_record_id)
    version    (source_system, source_record_id, last_updated_ts as a UTC instant)
source_system is the file's validated system (raw.ingested_files), not the
row's own column. source_record_id is used exactly as delivered; blank or
whitespace-only means missing.

Keys are SHA-256 over canonical JSON, hex-encoded. Their inputs are not PHI,
so no secret is involved, and the same inputs always give the same key:
    encounter_key  ["encounter/v1", source_system, source_record_id]
    version_key    ["encounter_version/v1", source_system, source_record_id,
                    "YYYY-MM-DDTHH:MM:SS.ffffffZ"]

Classification. Each row of a batch is compared with the versions held at the
end of the previous batch, and with the rows before it in the same batch in
lineage order (file_name, source_row_number), so the result never depends on
the order rows are passed in. First match wins:
    1  no source_record_id, or last_updated_ts not parseable   QUARANTINED
    2  older than the held current version                     STALE
         STALE_REPLAY       the version is already known
         STALE_NEW_VERSION  a never-held older version: written to history,
                            never current
    3  the version is already known                            DUPLICATE
         DUPLICATE_OF_HELD_VERSION / DUPLICATE_IN_BATCH, or QUARANTINED with
         VERSION_CONFLICT_SAME_TS when its values differ (the first arrival
         is kept)
    4  otherwise a new version. For an encounter with no held version, its
       earliest version in the batch is NEW_ENCOUNTER and any later ones
       NEW_VERSION; for a held encounter all are NEW_VERSION.
A version is "known" if it was held before the batch or appeared earlier in
the batch.

Two rows of the same version are compared (the fingerprint guard):
    EXACT       every canonical column identical as delivered
    EQUIVALENT  identical apart from the last_updated_ts text (it is the same
                instant) and columns NULL in either row, i.e. absent from that
                file's schema version (an empty string was delivered, so it is
                compared)
    CONFLICT    anything else

clean.encounter_versions is insert-only. Which version is current, and the
order of versions, are derived in the clean.encounter_current view, so a late,
older version never forces an update to rows already written.

Raw values are read to compare rows and are PHI. They are held in memory only:
never written to these tables, logged, or shown in a repr.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

import duckdb

from pipeline.batch_audit import db_timestamp
from pipeline.parsers.result import FieldReason
from pipeline.parsers.timestamps import parse_timestamp
from pipeline.raw_store import LINEAGE_COLUMNS
from pipeline.source_conventions import SourceConventions

log = logging.getLogger(__name__)

VERSIONS_TABLE = "clean.encounter_versions"
OUTCOMES_TABLE = "clean.encounter_row_outcomes"
CURRENT_VIEW = "clean.encounter_current"

ENCOUNTER_KEY_VERSION = "encounter/v1"
VERSION_KEY_VERSION = "encounter_version/v1"
TIMESTAMP_COLUMN = "last_updated_ts"


class Outcome(StrEnum):
    NEW_ENCOUNTER = "NEW_ENCOUNTER"
    NEW_VERSION = "NEW_VERSION"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    QUARANTINED = "QUARANTINED"


class OutcomeReason(StrEnum):
    """Detail for DUPLICATE, STALE and QUARANTINED outcomes.

    A row quarantined for its timestamp carries the parser's FieldReason
    (TIMESTAMP_MISSING, TIMESTAMP_UNPARSEABLE, ...) instead.
    """

    DUPLICATE_OF_HELD_VERSION = "DUPLICATE_OF_HELD_VERSION"
    DUPLICATE_IN_BATCH = "DUPLICATE_IN_BATCH"
    STALE_REPLAY = "STALE_REPLAY"
    STALE_NEW_VERSION = "STALE_NEW_VERSION"
    SOURCE_RECORD_ID_MISSING = "SOURCE_RECORD_ID_MISSING"
    VERSION_CONFLICT_SAME_TS = "VERSION_CONFLICT_SAME_TS"


class MatchType(StrEnum):
    EXACT = "EXACT"
    EQUIVALENT = "EQUIVALENT"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class BatchRow:
    """One raw row, ready to classify. `values` holds PHI and is left out of repr."""

    batch_id: str
    file_name: str
    source_row_number: int
    source_system: str  # validated, from raw.ingested_files
    source_record_id: str | None
    last_updated_ts_utc: datetime | None  # None when the timestamp did not parse
    timestamp_reason: FieldReason | None
    values: Mapping[str, str | None] = field(repr=False)  # canonical column -> value as delivered


@dataclass(frozen=True)
class HeldVersion:
    """A version already in history, with the raw values of its first-seen row."""

    version_key: str
    last_updated_ts_utc: datetime
    values: Mapping[str, str | None] = field(repr=False)


@dataclass(frozen=True)
class RowOutcome:
    batch_id: str
    file_name: str
    source_row_number: int
    source_system: str
    source_record_id: str | None
    encounter_key: str | None  # None only when source_record_id is missing
    version_key: str | None  # None when the row cannot be placed in version order
    last_updated_ts_utc: datetime | None
    outcome: Outcome
    outcome_reason: OutcomeReason | FieldReason | None
    match_type: MatchType | None  # set when an earlier row of the same version exists
    held_current_version_key: str | None  # the current version before this batch


@dataclass(frozen=True)
class NewVersion:
    version_key: str
    encounter_key: str
    source_system: str
    source_record_id: str
    last_updated_ts_utc: datetime
    first_seen_batch_id: str
    first_seen_file_name: str
    first_seen_source_row_number: int
    arrival_outcome: Outcome  # NEW_ENCOUNTER, NEW_VERSION or STALE


@dataclass(frozen=True)
class ClassifiedBatch:
    batch_id: str
    outcomes: tuple[RowOutcome, ...]  # one per row, in lineage order
    versions: tuple[NewVersion, ...]  # versions this batch adds to history, in lineage order


def _sha256(payload: list[str]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


def instant_text(value: datetime) -> str:
    """Fixed-width UTC text of an instant; equal instants always give equal text."""
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def encounter_key(source_system: str, source_record_id: str) -> str:
    return _sha256([ENCOUNTER_KEY_VERSION, source_system, source_record_id])


def version_key(source_system: str, source_record_id: str, last_updated_ts_utc: datetime) -> str:
    return _sha256([VERSION_KEY_VERSION, source_system, source_record_id, instant_text(last_updated_ts_utc)])


def compare_values(first: Mapping[str, str | None], other: Mapping[str, str | None]) -> MatchType:
    """How a later row of a version compares with the version's first row."""
    if first == other:
        return MatchType.EXACT
    for column in first.keys() | other.keys():
        if column == TIMESTAMP_COLUMN:
            continue  # same version, so the same instant; only the text can differ
        a, b = first.get(column), other.get(column)
        if a is not None and b is not None and a != b:
            return MatchType.CONFLICT
    return MatchType.EQUIVALENT


def has_record_id(source_record_id: str | None) -> bool:
    return bool((source_record_id or "").strip())


def observe_row(
    batch_id: str,
    file_name: str,
    source_row_number: int,
    source_system: str,
    values: Mapping[str, str | None],
    conventions: Mapping[str, SourceConventions],
) -> BatchRow:
    """A raw row with its last_updated_ts parsed to UTC using the source's conventions."""
    convention = conventions[source_system]
    ts = parse_timestamp(
        values.get(TIMESTAMP_COLUMN),
        timezone_name=convention.timestamp_timezone,
        date_order=convention.date_order,
    )
    return BatchRow(
        batch_id=batch_id,
        file_name=file_name,
        source_row_number=source_row_number,
        source_system=source_system,
        source_record_id=values.get("source_record_id"),
        last_updated_ts_utc=ts.cleaned_value,
        timestamp_reason=ts.reason_code,
        values=dict(values),
    )


def _lineage(row: BatchRow) -> tuple[str, int]:
    return row.file_name, row.source_row_number


def _outcome(
    row: BatchRow,
    enc_key: str | None,
    ver_key: str | None,
    outcome: Outcome,
    reason: OutcomeReason | FieldReason | None = None,
    match: MatchType | None = None,
    held_current_key: str | None = None,
) -> RowOutcome:
    return RowOutcome(
        batch_id=row.batch_id,
        file_name=row.file_name,
        source_row_number=row.source_row_number,
        source_system=row.source_system,
        source_record_id=row.source_record_id,
        encounter_key=enc_key,
        version_key=ver_key,
        last_updated_ts_utc=row.last_updated_ts_utc,
        outcome=outcome,
        outcome_reason=reason,
        match_type=match,
        held_current_version_key=held_current_key,
    )


def classify_batch(rows: Iterable[BatchRow], held: Mapping[str, Sequence[HeldVersion]]) -> ClassifiedBatch:
    """Classify one batch against `held`: encounter_key -> its versions before this batch.

    `held` needs entries only for the encounters in the batch. The result does
    not depend on the order of `rows`.
    """
    ordered = sorted(rows, key=_lineage)
    batch_ids = {row.batch_id for row in ordered}
    if len(batch_ids) != 1:
        raise ValueError("classify_batch needs the rows of exactly one batch")

    outcomes: list[RowOutcome] = []
    by_encounter: dict[str, list[BatchRow]] = defaultdict(list)
    for row in ordered:
        if not has_record_id(row.source_record_id):
            outcomes.append(_outcome(row, None, None, Outcome.QUARANTINED, OutcomeReason.SOURCE_RECORD_ID_MISSING))
        elif row.last_updated_ts_utc is None:
            enc_key = encounter_key(row.source_system, row.source_record_id)
            outcomes.append(_outcome(row, enc_key, None, Outcome.QUARANTINED, row.timestamp_reason))
        else:
            by_encounter[encounter_key(row.source_system, row.source_record_id)].append(row)

    versions: list[NewVersion] = []
    for enc_key, encounter_rows in by_encounter.items():
        encounter_outcomes, encounter_versions = _classify_encounter(enc_key, encounter_rows, held.get(enc_key, ()))
        outcomes.extend(encounter_outcomes)
        versions.extend(encounter_versions)

    outcomes.sort(key=lambda o: (o.file_name, o.source_row_number))
    versions.sort(key=lambda v: (v.first_seen_file_name, v.first_seen_source_row_number))
    return ClassifiedBatch(batch_ids.pop(), tuple(outcomes), tuple(versions))


def _classify_encounter(
    enc_key: str, rows: list[BatchRow], held_versions: Sequence[HeldVersion]
) -> tuple[list[RowOutcome], list[NewVersion]]:
    """One encounter's rows of the batch, already in lineage order."""
    held_by_ts = {v.last_updated_ts_utc: v for v in held_versions}
    held_current = max(held_versions, key=lambda v: v.last_updated_ts_utc, default=None)
    current_ts = held_current.last_updated_ts_utc if held_current else None
    current_key = held_current.version_key if held_current else None

    first_in_batch: dict[datetime, BatchRow] = {}
    labelled: list[tuple[BatchRow, str, Outcome, OutcomeReason | None, MatchType | None]] = []
    for row in rows:
        ts = row.last_updated_ts_utc
        ver_key = version_key(row.source_system, row.source_record_id, ts)
        held_version = held_by_ts.get(ts)
        earlier = first_in_batch.get(ts)
        reference = held_version.values if held_version else (earlier.values if earlier else None)
        match = compare_values(reference, row.values) if reference is not None else None

        if current_ts is not None and ts < current_ts:
            outcome = Outcome.STALE
            reason = OutcomeReason.STALE_REPLAY if reference is not None else OutcomeReason.STALE_NEW_VERSION
        elif reference is not None:
            if match == MatchType.CONFLICT:
                outcome, reason = Outcome.QUARANTINED, OutcomeReason.VERSION_CONFLICT_SAME_TS
            else:
                outcome = Outcome.DUPLICATE
                reason = OutcomeReason.DUPLICATE_OF_HELD_VERSION if held_version else OutcomeReason.DUPLICATE_IN_BATCH
        else:
            outcome, reason = Outcome.NEW_VERSION, None
        if reference is None:
            first_in_batch[ts] = row
        labelled.append((row, ver_key, outcome, reason, match))

    # An encounter new in this batch: its earliest version is the NEW_ENCOUNTER.
    first_version_ts = min(first_in_batch) if held_current is None else None

    outcomes, versions = [], []
    for row, ver_key, outcome, reason, match in labelled:
        is_first_seen = first_in_batch.get(row.last_updated_ts_utc) is row
        if is_first_seen and row.last_updated_ts_utc == first_version_ts:
            outcome = Outcome.NEW_ENCOUNTER
        outcomes.append(_outcome(row, enc_key, ver_key, outcome, reason, match, current_key))
        if is_first_seen:
            versions.append(
                NewVersion(
                    version_key=ver_key,
                    encounter_key=enc_key,
                    source_system=row.source_system,
                    source_record_id=row.source_record_id,
                    last_updated_ts_utc=row.last_updated_ts_utc,
                    first_seen_batch_id=row.batch_id,
                    first_seen_file_name=row.file_name,
                    first_seen_source_row_number=row.source_row_number,
                    arrival_outcome=outcome,
                )
            )
    return outcomes, versions


# --- storage ---------------------------------------------------------------


def _in_list(values: Iterable[str]) -> str:
    return ", ".join(f"'{v}'" for v in values)  # enum members only, never data


_CREATE_VERSIONS = f"""
CREATE TABLE IF NOT EXISTS {VERSIONS_TABLE} (
    version_key                  VARCHAR   NOT NULL PRIMARY KEY,
    encounter_key                VARCHAR   NOT NULL,
    source_system                VARCHAR   NOT NULL,
    source_record_id             VARCHAR   NOT NULL,
    last_updated_ts_utc          TIMESTAMP NOT NULL,
    first_seen_batch_id          VARCHAR   NOT NULL,
    first_seen_file_name         VARCHAR   NOT NULL,
    first_seen_source_row_number INTEGER   NOT NULL,
    arrival_outcome              VARCHAR   NOT NULL CHECK (arrival_outcome IN
        ({_in_list([Outcome.NEW_ENCOUNTER, Outcome.NEW_VERSION, Outcome.STALE])})),
    UNIQUE (encounter_key, last_updated_ts_utc)
)
"""

_CREATE_OUTCOMES = f"""
CREATE TABLE IF NOT EXISTS {OUTCOMES_TABLE} (
    batch_id                 VARCHAR   NOT NULL,
    file_name                VARCHAR   NOT NULL,
    source_row_number        INTEGER   NOT NULL,
    source_system            VARCHAR   NOT NULL,
    source_record_id         VARCHAR,
    encounter_key            VARCHAR,
    version_key              VARCHAR,
    last_updated_ts_utc      TIMESTAMP,
    outcome                  VARCHAR   NOT NULL CHECK (outcome IN ({_in_list(Outcome)})),
    outcome_reason           VARCHAR,
    match_type               VARCHAR   CHECK (match_type IN ({_in_list(MatchType)})),
    held_current_version_key VARCHAR,
    PRIMARY KEY (batch_id, file_name, source_row_number)
)
"""

# The latest last_updated_ts is current. (encounter_key, last_updated_ts_utc) is
# unique, so the lineage columns in ORDER BY only make the choice explicit.
_CREATE_CURRENT_VIEW = f"""
CREATE OR REPLACE VIEW {CURRENT_VIEW} AS
SELECT
    encounter_key,
    source_system,
    source_record_id,
    version_key,
    last_updated_ts_utc,
    count(*) OVER (PARTITION BY encounter_key)                  AS version_count,
    min(first_seen_batch_id) OVER (PARTITION BY encounter_key)  AS encounter_first_seen_batch_id,
    first_seen_batch_id                                         AS source_batch_id,
    first_seen_file_name                                        AS source_file_name,
    first_seen_source_row_number                                AS source_row_number
FROM {VERSIONS_TABLE}
QUALIFY row_number() OVER (
    PARTITION BY encounter_key
    ORDER BY last_updated_ts_utc DESC, first_seen_batch_id, first_seen_file_name, first_seen_source_row_number
) = 1
"""

_OUTCOME_COLUMNS = (
    "batch_id",
    "file_name",
    "source_row_number",
    "source_system",
    "source_record_id",
    "encounter_key",
    "version_key",
    "last_updated_ts_utc",
    "outcome",
    "outcome_reason",
    "match_type",
    "held_current_version_key",
)
_VERSION_COLUMNS = (
    "version_key",
    "encounter_key",
    "source_system",
    "source_record_id",
    "last_updated_ts_utc",
    "first_seen_batch_id",
    "first_seen_file_name",
    "first_seen_source_row_number",
    "arrival_outcome",
)
_COLUMN_TYPES = {
    "source_row_number": "INTEGER",
    "first_seen_source_row_number": "INTEGER",
    "last_updated_ts_utc": "TIMESTAMP",
}


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS clean")
    con.execute(_CREATE_VERSIONS)
    con.execute(_CREATE_OUTCOMES)
    con.execute(_CREATE_CURRENT_VIEW)


def read_batch_rows(
    con: duckdb.DuckDBPyConnection, batch_id: str, conventions: Mapping[str, SourceConventions]
) -> list[BatchRow]:
    """Every raw row of one accepted batch, with last_updated_ts parsed."""
    cursor = con.execute(
        "SELECT f.source_system AS validated_source_system, e.* "
        "FROM raw.encounters AS e JOIN raw.ingested_files AS f USING (batch_id, file_name) "
        "WHERE e.batch_id = ? ORDER BY e.file_name, e.source_row_number",
        [batch_id],
    )
    names = [d[0] for d in cursor.description]
    rows = []
    for record in cursor.fetchall():
        named = dict(zip(names, record, strict=True))
        values = {k: v for k, v in named.items() if k not in LINEAGE_COLUMNS and k != "validated_source_system"}
        rows.append(
            observe_row(
                named["batch_id"],
                named["file_name"],
                named["source_row_number"],
                named["validated_source_system"],
                values,
                conventions,
            )
        )
    return rows


def load_held_versions(con: duckdb.DuckDBPyConnection, encounter_keys: Iterable[str]) -> dict[str, list[HeldVersion]]:
    """History of the given encounters only, each version with its first-seen raw values."""
    keys = sorted(set(encounter_keys))
    if not keys:
        return {}
    cursor = con.execute(
        f"""
        WITH touched AS (SELECT unnest(from_json(?, '["VARCHAR"]')) AS encounter_key)
        SELECT v.encounter_key AS held_encounter_key, v.version_key AS held_version_key,
               v.last_updated_ts_utc AS held_ts, e.* EXCLUDE ({", ".join(LINEAGE_COLUMNS)})
        FROM {VERSIONS_TABLE} AS v
        JOIN touched USING (encounter_key)
        JOIN raw.encounters AS e
          ON e.batch_id = v.first_seen_batch_id
         AND e.file_name = v.first_seen_file_name
         AND e.source_row_number = v.first_seen_source_row_number
        """,
        [json.dumps(keys)],
    )
    names = [d[0] for d in cursor.description]
    held: dict[str, list[HeldVersion]] = defaultdict(list)
    for record in cursor.fetchall():
        enc_key, ver_key, ts, *values = record
        held[enc_key].append(HeldVersion(ver_key, ts.replace(tzinfo=UTC), dict(zip(names[3:], values, strict=True))))
    return dict(held)


def _json_value(value: object) -> object:
    if isinstance(value, datetime):
        return db_timestamp(value).isoformat(sep=" ")
    return value  # StrEnum members serialise as their value


def _insert_json(con: duckdb.DuckDBPyConnection, table: str, columns: Sequence[str], rows: Sequence[object]) -> None:
    """Insert dataclass rows as one JSON document (see raw_store for why not executemany)."""
    if not rows:
        return
    schema = json.dumps([{c: _COLUMN_TYPES.get(c, "VARCHAR") for c in columns}])
    document = json.dumps([{c: _json_value(getattr(row, c)) for c in columns} for row in rows], ensure_ascii=False)
    con.execute(
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"SELECT {', '.join(f'r.{c}' for c in columns)} FROM (SELECT unnest(from_json(?, ?)) AS r)",
        [document, schema],
    )


def write_classified_batch(con: duckdb.DuckDBPyConnection, classified: ClassifiedBatch) -> None:
    """Insert the batch's row outcomes and new versions, inside the caller's transaction."""
    _insert_json(con, OUTCOMES_TABLE, _OUTCOME_COLUMNS, classified.outcomes)
    _insert_json(con, VERSIONS_TABLE, _VERSION_COLUMNS, classified.versions)


def apply_batch(
    con: duckdb.DuckDBPyConnection, batch_id: str, conventions: Mapping[str, SourceConventions]
) -> ClassifiedBatch:
    """Classify an accepted batch already in raw against history, and write the result.

    Reads history only for the encounters the batch touches. Runs inside the
    caller's transaction; applying the same batch twice fails on the primary keys.
    """
    rows = read_batch_rows(con, batch_id, conventions)
    touched = {encounter_key(r.source_system, r.source_record_id) for r in rows if has_record_id(r.source_record_id)}
    classified = classify_batch(rows, load_held_versions(con, touched)) if rows else ClassifiedBatch(batch_id, (), ())
    write_classified_batch(con, classified)

    counts = Counter(o.outcome for o in classified.outcomes)
    log.info(
        "batch_classified",
        extra={
            "step": "history",
            "batch_id": batch_id,
            "received_count": len(classified.outcomes),
            "accepted_count": counts[Outcome.NEW_ENCOUNTER] + counts[Outcome.NEW_VERSION],
            "duplicate_count": counts[Outcome.DUPLICATE],
            "stale_count": counts[Outcome.STALE],
            "quarantined_count": counts[Outcome.QUARANTINED],
        },
    )
    return classified
