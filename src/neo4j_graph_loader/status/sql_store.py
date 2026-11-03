"""The SQL status store (E15.F1.S4): the schema, its migrations, the transactional writer and the readers.

Needs the optional ``[sql]`` extra (SQLAlchemy Core) and, for PostgreSQL, ``[postgres]`` (psycopg 3). Nothing else in
this package imports this module at import time, so a base install never needs either dependency.

Tables (all names are in the configured schema on PostgreSQL, default ``graph_loader``; SQLite has no schemas):

``schema_version`` (one row: the schema's version), ``executions`` (PK ``execution_id``; run id, state, outcome, host,
mode, runtime, versions, timestamps, ``heartbeat_at``, ``revision``, ``history_incomplete``, ``telemetry``, ``totals``),
``execution_activities``, ``execution_phases`` (E15.F1.S1 phases; not in the first sketch of the design),
``execution_labels``, ``execution_types`` (per node label / relationship type: its progress fields as JSON),
``execution_roles`` (PK ``execution_id, incarnation``: an incarnation is unique, so it identifies the row;
``role`` and ``replica`` are columns) and ``execution_events`` (PK ``execution_id, seq``; the event as JSON and its
SHA-256). Relationships are by ``execution_id``; there are no foreign keys, so a failed write of one table can never
block another and ``prune`` deletes explicitly, children first.

A snapshot is written in ONE transaction and only if its ``revision`` is higher than the stored one. An event whose
key exists with the same payload hash is skipped (delivery is at least once); with a different hash it is an error.
Only the CLI connects; roles never receive the URL. The password never appears in a log line, an exception or a file
of this package: the connection URL is scrubbed from every message.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

import sqlalchemy as sa
from sqlalchemy.engine import Engine, make_url

from neo4j_graph_loader.status import locator as locator_module, model, schemas
from neo4j_graph_loader.status.validate import validate as _validate

logger = logging.getLogger(__name__)

DB_SCHEMA_VERSION = 1
URL_ENV = "GRAPH_LOADER_STATUS_DB_URL"
SCHEMA_ENV = "GRAPH_LOADER_STATUS_SCHEMA"
AUTO_MIGRATE_ENV = "GRAPH_LOADER_STATUS_AUTO_MIGRATE"
SHUTDOWN_DEADLINE_ENV = "GRAPH_LOADER_STATUS_SHUTDOWN_DEADLINE_S"
DEFAULT_SCHEMA = "graph_loader"
TERMINAL = model.TERMINAL_STATES


class StatusDatabaseError(RuntimeError):
    """The status database cannot be used (message never contains credentials)."""


class SchemaNotReady(StatusDatabaseError):
    """The database has no status schema, or an older or newer one than this version writes."""

    def __init__(self, found: int | None):
        self.found = found
        if found is None:
            message = ("the status database has no graph-loader status schema yet; run `graph-loader status-store migrate` "
                       "(or start with --status-store-auto-migrate)")
        elif found < DB_SCHEMA_VERSION:
            message = (f"the status database schema is version {found}, this version needs {DB_SCHEMA_VERSION}; run "
                       "`graph-loader status-store migrate` (or start with --status-store-auto-migrate)")
        else:
            message = (f"the status database schema is version {found}, newer than this version's {DB_SCHEMA_VERSION}; "
                       "upgrade graph-loader or use another schema")
        super().__init__(message)


class EventConflict(StatusDatabaseError):
    """An event key already exists with a different payload: history would be rewritten, so nothing is written."""

    def __init__(self, execution_id: str, seq: int):
        self.execution_id, self.seq = execution_id, seq
        super().__init__(f"event {seq} of execution {execution_id} already exists with a different payload")


def _scrub(text: str, url: str | None) -> str:
    """``text`` without the connection URL, its password or its user."""
    if url:
        try:
            parsed = make_url(url)
        except Exception:                                   # noqa: BLE001
            parsed = None
        for secret in (url, getattr(parsed, "password", None)):
            if secret:
                text = text.replace(str(secret), "***")
    return re.sub(r"(?i)(password|passwd|pwd)\s*[=:]\s*\S+", r"\1=***", text)


def _ts(text: str | None) -> datetime | None:
    if text is None:
        return None
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def _text(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)                # SQLite hands back naive UTC
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def payload_hash(event: dict) -> str:
    return hashlib.sha256(json.dumps(event, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def parse_duration(text: str) -> timedelta:
    """``90s``, ``45m``, ``12h``, ``30d`` or ``2w`` as a timedelta."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhdw])\s*", text or "")
    if not match:
        raise ValueError(f"not a duration: {text!r} (use for example 90s, 45m, 12h, 30d, 2w)")
    seconds = float(match.group(1)) * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[match.group(2)]
    return timedelta(seconds=seconds)


def build_tables(schema: str | None) -> tuple[sa.MetaData, dict[str, sa.Table]]:
    meta = sa.MetaData(schema=schema)
    stamp = sa.DateTime(timezone=True)

    def table(name: str, *columns, **kwargs) -> sa.Table:
        return sa.Table(name, meta, *columns, **kwargs)

    tables = {
        "schema_version": table("schema_version", sa.Column("version", sa.Integer, primary_key=True, autoincrement=False),
                                sa.Column("applied_at", stamp, nullable=False)),
        "executions": table(
            "executions",
            sa.Column("execution_id", sa.String(32), primary_key=True),
            sa.Column("run_id", sa.String(128)), sa.Column("state", sa.String(16), nullable=False),
            sa.Column("mode", sa.String(16)), sa.Column("runtime", sa.String(16), nullable=False),
            sa.Column("host", sa.String(255), nullable=False), sa.Column("pid", sa.Integer, nullable=False),
            sa.Column("versions", sa.JSON, nullable=False),
            sa.Column("started_at", stamp, nullable=False), sa.Column("updated_at", stamp, nullable=False),
            sa.Column("heartbeat_at", stamp, nullable=False), sa.Column("finished_at", stamp),
            sa.Column("outcome", sa.JSON(none_as_null=True)), sa.Column("exit_code", sa.Integer),
            sa.Column("revision", sa.Integer, nullable=False), sa.Column("history_incomplete", sa.Boolean, nullable=False),
            sa.Column("telemetry", sa.JSON, nullable=False), sa.Column("totals", sa.JSON, nullable=False),
            sa.Index("ix_executions_run_id", "run_id"), sa.Index("ix_executions_started_at", "started_at"),
        ),
        "execution_activities": table(
            "execution_activities", sa.Column("execution_id", sa.String(32), primary_key=True),
            sa.Column("name", sa.String(32), primary_key=True), sa.Column("state", sa.String(16), nullable=False),
            sa.Column("started_at", stamp), sa.Column("finished_at", stamp), sa.Column("updated_at", stamp, nullable=False)),
        "execution_phases": table(
            "execution_phases", sa.Column("execution_id", sa.String(32), primary_key=True),
            sa.Column("phase_id", sa.String(128), primary_key=True), sa.Column("ordinal", sa.Integer, nullable=False),
            sa.Column("kind", sa.String(64), nullable=False), sa.Column("idx", sa.Integer), sa.Column("edge_type", sa.String(255)),
            sa.Column("state", sa.String(16), nullable=False), sa.Column("started_at", stamp, nullable=False),
            sa.Column("finished_at", stamp)),
        "execution_labels": table(
            "execution_labels", sa.Column("execution_id", sa.String(32), primary_key=True),
            sa.Column("name", sa.String(128), primary_key=True), sa.Column("value", sa.Text, nullable=False)),
        "execution_types": table(
            "execution_types", sa.Column("execution_id", sa.String(32), primary_key=True),
            sa.Column("kind", sa.String(8), primary_key=True), sa.Column("name", sa.String(255), primary_key=True),
            sa.Column("ordinal", sa.Integer, nullable=False), sa.Column("fields", sa.JSON, nullable=False)),
        "execution_roles": table(
            "execution_roles", sa.Column("execution_id", sa.String(32), primary_key=True),
            sa.Column("incarnation", sa.String(32), primary_key=True), sa.Column("ordinal", sa.Integer, nullable=False),
            sa.Column("role", sa.String(64), nullable=False), sa.Column("name", sa.String(255), nullable=False),
            sa.Column("replica", sa.Integer), sa.Column("restart_count", sa.Integer, nullable=False),
            sa.Column("state", sa.String(16), nullable=False), sa.Column("container_id", sa.String(128)),
            sa.Column("pid", sa.Integer), sa.Column("started_at", stamp, nullable=False), sa.Column("finished_at", stamp),
            sa.Column("exit_code", sa.Integer), sa.Column("exit_reason", sa.String(255)),
            sa.Column("last_error", sa.JSON(none_as_null=True))),
        "execution_events": table(
            "execution_events", sa.Column("execution_id", sa.String(32), primary_key=True),
            sa.Column("seq", sa.Integer, primary_key=True), sa.Column("event_type", sa.String(64), nullable=False),
            sa.Column("ts", stamp, nullable=False), sa.Column("payload", sa.JSON, nullable=False),
            sa.Column("payload_hash", sa.String(64), nullable=False)),
    }
    return meta, tables


@dataclass
class ExecutionRow:
    execution_id: str
    run_id: str | None
    state: str
    started_at: str
    updated_at: str
    heartbeat_at: str
    finished_at: str | None
    revision: int
    mode: str | None
    runtime: str
    host: str
    pid: int | None = None


class SqlStatusStore:
    """Connection, schema and transactional operations of the SQL status store. Thread-safe (one engine, one
    transaction per call)."""

    def __init__(self, url: str | None = None, *, schema: str | None = None, engine: Engine | None = None,
                 metrics_listener: Callable[[str], None] | None = None):
        url = url or os.environ.get(URL_ENV)
        if not url and engine is None:
            raise StatusDatabaseError(f"no status database URL: set {URL_ENV}")
        self._url = url
        self._metrics = metrics_listener
        try:
            self.engine = engine or sa.create_engine(url, pool_pre_ping=True)
        except Exception as exc:                                              # noqa: BLE001
            raise StatusDatabaseError(_scrub(f"cannot create the status database engine: {exc}", url)) from None
        self.dialect = self.engine.dialect.name
        self.schema = None if self.dialect == "sqlite" else (schema or os.environ.get(SCHEMA_ENV) or DEFAULT_SCHEMA)
        self.meta, self.t = build_tables(self.schema)

    # -- plumbing ---------------------------------------------------------------------------------------------
    def _fail(self, what: str, exc: BaseException) -> StatusDatabaseError:
        return StatusDatabaseError(_scrub(f"{what}: {type(exc).__name__}: {exc}", self._url))

    def describe(self) -> str:
        """The database in words, without credentials."""
        return locator_module.sanitize_db_url(self._url) if self._url else str(self.engine.url.render_as_string(hide_password=True))

    def close(self) -> None:
        self.engine.dispose()

    # -- schema -----------------------------------------------------------------------------------------------
    def schema_version(self) -> int | None:
        """The stored schema version; ``None`` when the schema (or its version row) is absent."""
        try:
            with self.engine.connect() as connection:
                if not sa.inspect(connection).has_table("schema_version", schema=self.schema):
                    return None
                version = connection.execute(sa.select(sa.func.max(self.t["schema_version"].c.version))).scalar()
                return int(version) if version is not None else None
        except Exception as exc:                                              # noqa: BLE001
            raise self._fail("cannot read the status schema version", exc) from None

    def check_schema(self) -> None:
        """Raise :class:`SchemaNotReady` unless the schema is exactly this version's."""
        found = self.schema_version()
        if found != DB_SCHEMA_VERSION:
            raise SchemaNotReady(found)

    def migrate(self) -> int:
        """Create or upgrade the schema to this version. Idempotent; returns the version in place."""
        try:
            with self.engine.begin() as connection:
                if self.schema:
                    connection.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"'))
                self.meta.create_all(connection, checkfirst=True)
                current = connection.execute(sa.select(sa.func.max(self.t["schema_version"].c.version))).scalar()
                if current is not None and current > DB_SCHEMA_VERSION:
                    raise SchemaNotReady(int(current))
                if current is None or current < DB_SCHEMA_VERSION:
                    connection.execute(sa.insert(self.t["schema_version"]).values(
                        version=DB_SCHEMA_VERSION, applied_at=datetime.now(timezone.utc)))
        except SchemaNotReady:
            raise
        except Exception as exc:                                              # noqa: BLE001
            raise self._fail("cannot migrate the status schema", exc) from None
        return DB_SCHEMA_VERSION

    # -- snapshots --------------------------------------------------------------------------------------------
    def write_snapshot(self, snapshot: dict) -> bool:
        """Store ``snapshot`` in one transaction. Returns ``False`` (nothing changes) when the database already holds
        this revision or a newer one."""
        t, execution_id, revision = self.t, snapshot["execution_id"], snapshot["revision"]
        outcome = snapshot["outcome"]
        values = dict(
            run_id=snapshot["run_id"], state=snapshot["state"], mode=snapshot["mode"], runtime=snapshot["runtime"],
            host=snapshot["host"], pid=snapshot["pid"], versions=snapshot["versions"], started_at=_ts(snapshot["started_at"]),
            updated_at=_ts(snapshot["updated_at"]), heartbeat_at=_ts(snapshot["heartbeat_at"]),
            finished_at=_ts(snapshot["finished_at"]), outcome=outcome, exit_code=None if outcome is None else outcome["exit_code"],
            revision=revision, history_incomplete=snapshot["history_incomplete"], telemetry=snapshot["telemetry"],
            totals=snapshot["totals"])
        try:
            with self.engine.begin() as connection:
                stored = connection.execute(sa.select(t["executions"].c.revision).where(
                    t["executions"].c.execution_id == execution_id)).scalar()
                if stored is None:
                    connection.execute(sa.insert(t["executions"]).values(execution_id=execution_id, **values))
                else:
                    updated = connection.execute(sa.update(t["executions"]).where(
                        t["executions"].c.execution_id == execution_id, t["executions"].c.revision < revision).values(**values))
                    if updated.rowcount == 0:
                        return False                                          # an older or equal revision: rejected
                for name in ("execution_activities", "execution_phases", "execution_labels", "execution_types", "execution_roles"):
                    connection.execute(sa.delete(t[name]).where(t[name].c.execution_id == execution_id))
                self._insert_children(connection, snapshot)
        except sa.exc.IntegrityError:
            return False                                                      # a concurrent writer inserted the row first
        except Exception as exc:                                              # noqa: BLE001
            self._count("write_failed")
            raise self._fail("cannot write the status snapshot", exc) from None
        return True

    def _insert_children(self, connection, snapshot: dict) -> None:
        t, execution_id = self.t, snapshot["execution_id"]
        rows = [dict(execution_id=execution_id, name=name, state=item["state"], started_at=_ts(item["started_at"]),
                     finished_at=_ts(item["finished_at"]), updated_at=_ts(item["updated_at"]))
                for name, item in snapshot["activities"].items()]
        if rows:
            connection.execute(sa.insert(t["execution_activities"]), rows)
        rows = [dict(execution_id=execution_id, phase_id=item["phase_id"], ordinal=index, kind=item["kind"], idx=item["index"],
                     edge_type=item["edge_type"], state=item["state"], started_at=_ts(item["started_at"]),
                     finished_at=_ts(item["finished_at"])) for index, item in enumerate(snapshot["phases"])]
        if rows:
            connection.execute(sa.insert(t["execution_phases"]), rows)
        rows = [dict(execution_id=execution_id, name=name, value=value) for name, value in snapshot["labels"].items()]
        if rows:
            connection.execute(sa.insert(t["execution_labels"]), rows)
        rows = [dict(execution_id=execution_id, kind=item["kind"], name=item["name"], ordinal=index,
                     fields={key: value for key, value in item.items() if key not in ("kind", "name")})
                for index, item in enumerate(snapshot["types"])]
        if rows:
            connection.execute(sa.insert(t["execution_types"]), rows)
        rows = [dict(execution_id=execution_id, incarnation=item["incarnation"], ordinal=index, role=item["role"],
                     name=item["name"], replica=item["replica"], restart_count=item["restart_count"], state=item["state"],
                     container_id=item["container_id"], pid=item["pid"], started_at=_ts(item["started_at"]),
                     finished_at=_ts(item["finished_at"]), exit_code=item["exit_code"], exit_reason=item["exit_reason"],
                     last_error=item["last_error"]) for index, item in enumerate(snapshot["roles"])]
        if rows:
            connection.execute(sa.insert(t["execution_roles"]), rows)

    # -- events -----------------------------------------------------------------------------------------------
    def write_events(self, events: list[dict]) -> tuple[int, int]:
        """Insert ``events`` (any executions) idempotently. Returns ``(inserted, already present with the same
        payload)``. An existing key with a different payload raises :class:`EventConflict` and writes nothing."""
        if not events:
            return 0, 0
        table = self.t["execution_events"]
        try:
            with self.engine.begin() as connection:
                inserted, duplicates, fresh, seen = 0, 0, [], {}
                for event in events:
                    key = (event["execution_id"], event["seq"])
                    digest = payload_hash(event)
                    if key in seen:
                        if seen[key] != digest:
                            raise EventConflict(*key)
                        duplicates += 1
                        continue
                    seen[key] = digest
                    fresh.append((key, digest, event))
                for execution_id in {key[0] for key in seen}:
                    sequences = [key[1] for key in seen if key[0] == execution_id]
                    stored = dict(connection.execute(sa.select(table.c.seq, table.c.payload_hash).where(
                        table.c.execution_id == execution_id, table.c.seq.in_(sequences))).all())
                    for key, digest, _event in list(fresh):
                        if key[0] == execution_id and key[1] in stored:
                            if stored[key[1]] != digest:
                                raise EventConflict(*key)
                            duplicates += 1
                            fresh.remove((key, digest, _event))
                rows = [dict(execution_id=event["execution_id"], seq=event["seq"], event_type=event["type"],
                             ts=_ts(event["ts"]), payload=event, payload_hash=digest) for _key, digest, event in fresh]
                if rows:
                    connection.execute(sa.insert(table), rows)
                    inserted = len(rows)
                return inserted, duplicates
        except EventConflict:
            self._count("event_conflict")
            raise
        except Exception as exc:                                              # noqa: BLE001
            self._count("write_failed")
            raise self._fail("cannot write status events", exc) from None

    def _count(self, reason: str) -> None:
        if self._metrics is not None:
            try:
                self._metrics(reason)
            except Exception:                                                 # noqa: BLE001
                pass

    # -- reading ----------------------------------------------------------------------------------------------
    def read_status(self, execution_id: str) -> dict:
        t = self.t
        try:
            with self.engine.connect() as connection:
                row = connection.execute(sa.select(t["executions"]).where(t["executions"].c.execution_id == execution_id)).mappings().first()
                if row is None:
                    raise LookupError(f"no execution {execution_id} in the status database")

                def rows(name: str, order=None):
                    query = sa.select(t[name]).where(t[name].c.execution_id == execution_id)
                    if order is not None:
                        query = query.order_by(order)
                    return connection.execute(query).mappings().all()

                activities = {r["name"]: {"state": r["state"], "started_at": _text(r["started_at"]),
                                          "finished_at": _text(r["finished_at"]), "updated_at": _text(r["updated_at"])}
                              for r in rows("execution_activities")}
                phases = [{"phase_id": r["phase_id"], "kind": r["kind"], "index": r["idx"], "edge_type": r["edge_type"],
                           "state": r["state"], "started_at": _text(r["started_at"]), "finished_at": _text(r["finished_at"])}
                          for r in rows("execution_phases", t["execution_phases"].c.ordinal)]
                labels = {r["name"]: r["value"] for r in rows("execution_labels")}
                types = [{"kind": r["kind"], "name": r["name"], **r["fields"]}
                         for r in rows("execution_types", t["execution_types"].c.ordinal)]
                roles = [{"role": r["role"], "name": r["name"], "replica": r["replica"], "incarnation": r["incarnation"],
                          "restart_count": r["restart_count"], "state": r["state"], "container_id": r["container_id"],
                          "pid": r["pid"], "started_at": _text(r["started_at"]), "finished_at": _text(r["finished_at"]),
                          "exit_code": r["exit_code"], "exit_reason": r["exit_reason"], "last_error": r["last_error"]}
                         for r in rows("execution_roles", t["execution_roles"].c.ordinal)]
        except LookupError:
            raise
        except Exception as exc:                                              # noqa: BLE001
            raise self._fail("cannot read the status", exc) from None
        return {
            "schema_version": model.SCHEMA_VERSION, "kind": "status", "execution_id": row["execution_id"], "run_id": row["run_id"],
            "revision": row["revision"], "state": row["state"], "outcome": row["outcome"], "mode": row["mode"],
            "runtime": row["runtime"], "host": row["host"], "pid": row["pid"], "versions": row["versions"],
            "started_at": _text(row["started_at"]), "updated_at": _text(row["updated_at"]),
            "heartbeat_at": _text(row["heartbeat_at"]), "finished_at": _text(row["finished_at"]), "activities": activities,
            "phases": phases, "roles": roles, "labels": labels, "types": types, "totals": row["totals"],
            "telemetry": row["telemetry"], "history_incomplete": bool(row["history_incomplete"]),
        }

    def read_events(self, execution_id: str, *, start_seq: int = 1, limit: int | None = None) -> list[dict]:
        table = self.t["execution_events"]
        query = sa.select(table.c.payload).where(table.c.execution_id == execution_id, table.c.seq >= start_seq).order_by(table.c.seq)
        if limit:
            query = query.limit(limit)
        try:
            with self.engine.connect() as connection:
                return [row[0] for row in connection.execute(query)]
        except Exception as exc:                                              # noqa: BLE001
            raise self._fail("cannot read the status events", exc) from None

    def list_executions(self, *, run_id: str | None = None, active: bool = False, limit: int = 100) -> list[ExecutionRow]:
        table = self.t["executions"]
        query = sa.select(table).order_by(table.c.started_at.desc()).limit(limit)
        if run_id is not None:
            query = query.where(table.c.run_id == run_id)
        if active:
            query = query.where(table.c.state.notin_(TERMINAL))
        try:
            with self.engine.connect() as connection:
                return [ExecutionRow(r["execution_id"], r["run_id"], r["state"], _text(r["started_at"]), _text(r["updated_at"]),
                                     _text(r["heartbeat_at"]), _text(r["finished_at"]), r["revision"], r["mode"], r["runtime"],
                                     r["host"], r["pid"]) for r in connection.execute(query).mappings()]
        except Exception as exc:                                              # noqa: BLE001
            raise self._fail("cannot list the executions", exc) from None

    # -- retention --------------------------------------------------------------------------------------------
    def prune(self, older_than: timedelta, *, now: datetime | None = None) -> dict:
        """Delete FINISHED executions (a terminal state) that finished more than ``older_than`` ago, with all their rows.
        Running, starting and stopping executions are never deleted, however old. Returns the counts."""
        cutoff = (now or datetime.now(timezone.utc)) - older_than
        t = self.t
        try:
            with self.engine.begin() as connection:
                ids = [row[0] for row in connection.execute(sa.select(t["executions"].c.execution_id).where(
                    t["executions"].c.state.in_(TERMINAL), t["executions"].c.finished_at.is_not(None),
                    t["executions"].c.finished_at < cutoff))]
                counts = {"executions": len(ids), "events": 0}
                for start in range(0, len(ids), 200):
                    chunk = ids[start:start + 200]
                    for name in ("execution_activities", "execution_phases", "execution_labels", "execution_types", "execution_roles"):
                        connection.execute(sa.delete(t[name]).where(t[name].c.execution_id.in_(chunk)))
                    counts["events"] += connection.execute(sa.delete(t["execution_events"]).where(
                        t["execution_events"].c.execution_id.in_(chunk))).rowcount or 0
                    connection.execute(sa.delete(t["executions"]).where(t["executions"].c.execution_id.in_(chunk)))
                return counts
        except Exception as exc:                                              # noqa: BLE001
            raise self._fail("cannot prune the status database", exc) from None


# ----------------------------------------------------------------------------------------------------------------------
# The asynchronous journal the CLI uses (E15.F1.S4 interim; E15.F1.S5 puts a durable outbox in front of the database)
# ----------------------------------------------------------------------------------------------------------------------
class SqlJournal:
    """Tracker journal and snapshot sink backed by a background writer; the load never waits for the database.

    ``seq`` is assigned in order by this journal; events and snapshots are written from a thread, retrying with
    backoff when the database fails. In this story the queue is in memory and bounded; the durable outbox of E15.F1.S5
    replaces it."""

    MAX_PENDING_EVENTS = 50_000
    BATCH = 200

    def __init__(self, store: SqlStatusStore, *, snapshot_interval: float = 2.0, retry_seconds: float = 1.0,
                 max_retry_seconds: float = 15.0):
        self._store, self._interval = store, snapshot_interval
        self._retry, self._max_retry = retry_seconds, max_retry_seconds
        self._lock = threading.Lock()
        self._events: deque[dict] = deque()
        self._next_seq = 1
        self._snapshot: dict | None = None
        self._written_revision = 0
        self._wake, self._closed = threading.Event(), threading.Event()
        self._last_write = float("-inf")
        self._deadline = float("inf")
        self.dropped = 0
        self.failures = 0
        self._warned = False
        self._thread = threading.Thread(target=self._run, name="status-sql-writer", daemon=True)
        self._thread.start()

    # journal protocol
    def append(self, make_event: Callable[[int], dict]) -> dict:
        with self._lock:
            event = make_event(self._next_seq)
            self._next_seq += 1
            if len(self._events) >= self.MAX_PENDING_EVENTS:
                self._events.popleft()
                self.dropped += 1
            self._events.append(event)
        self._wake.set()
        return event

    def offer_snapshot(self, snapshot: dict) -> None:
        with self._lock:
            if self._snapshot is None or snapshot["revision"] > self._snapshot["revision"]:
                self._snapshot = snapshot
        self._wake.set()

    def _run(self) -> None:
        delay = self._retry
        while True:
            self._wake.wait(timeout=self._interval)
            self._wake.clear()
            try:
                self.flush_once()
                delay = self._retry
            except Exception as exc:                                          # noqa: BLE001
                self.failures += 1
                if not self._warned:
                    self._warned = True
                    logger.warning("stage=status reason=the status database is not accepting writes, retrying in the "
                                   "background (the load continues): %s", exc)
                if self._closed.is_set():
                    if time.monotonic() >= self._deadline:
                        return                                # shutdown deadline reached: what is left stays undelivered
                    time.sleep(min(delay, 0.1))
                else:
                    self._closed.wait(delay)
                delay = min(delay * 2, self._max_retry)
                self._wake.set()
                continue
            if self._closed.is_set():
                with self._lock:
                    if not self._events and (self._snapshot is None or self._snapshot["revision"] <= self._written_revision):
                        return

    def flush_once(self) -> None:
        """Write the newest snapshot (rate limited) and every pending event; raises when the database refuses."""
        with self._lock:
            snapshot = self._snapshot
            due = snapshot is not None and snapshot["revision"] > self._written_revision and (
                self._closed.is_set() or time.monotonic() - self._last_write >= self._interval)
        if due:
            self._store.write_snapshot(snapshot)
            self._last_write = time.monotonic()
            self._written_revision = snapshot["revision"]
        while True:
            with self._lock:
                batch = [self._events[i] for i in range(min(self.BATCH, len(self._events)))]
            if not batch:
                return
            self._store.write_events(batch)
            with self._lock:
                for _ in batch:
                    self._events.popleft()

    def close(self, deadline_seconds: float | None = None) -> bool:
        """Wait up to ``deadline_seconds`` (default ``GRAPH_LOADER_STATUS_SHUTDOWN_DEADLINE_S``, else 10) for
        everything to be written. Returns whether it all was."""
        if deadline_seconds is None:
            try:
                deadline_seconds = float(os.environ.get(SHUTDOWN_DEADLINE_ENV, "10"))
            except ValueError:
                deadline_seconds = 10.0
        self._deadline = time.monotonic() + deadline_seconds
        self._closed.set()
        self._wake.set()
        self._thread.join(timeout=deadline_seconds + 1)
        with self._lock:
            done = not self._events and (self._snapshot is None or self._snapshot["revision"] <= self._written_revision)
        finished = done and not self._thread.is_alive()
        if finished:
            self._store.close()
        return finished


# ----------------------------------------------------------------------------------------------------------------------
# Reader registration (the accessor in ``status.reader`` calls these for a locator whose store is ``sql``)
# ----------------------------------------------------------------------------------------------------------------------
def _store_for(locator: dict) -> SqlStatusStore:
    database = locator.get("database") or {}
    recorded = database.get("url")
    url = os.environ.get(URL_ENV)
    if url and recorded and locator_module.sanitize_db_url(url) != recorded:
        url = None                                                           # never read another database than the run's
    url = url or recorded
    if not url:
        raise StatusDatabaseError(f"the locator names no database; set {URL_ENV}")
    return SqlStatusStore(url, schema=database.get("schema"))


def _read_status(directory, locator: dict, *, validate: bool = True) -> dict:
    store = _store_for(locator)
    try:
        document = store.read_status(locator["execution_id"])
    except LookupError as exc:
        from neo4j_graph_loader.status.reader import StatusNotFound
        raise StatusNotFound(str(exc)) from None
    finally:
        store.close()
    if validate:
        _validate(document, schemas.STATUS_SCHEMA)
    return document


def _iter_events(directory, locator: dict, *, follow: bool, start_seq: int, poll_interval: float, stop, idle_timeout,
                 validate: bool) -> Iterator[dict]:
    store = _store_for(locator)
    next_seq, last_progress = max(1, start_seq), time.monotonic()
    try:
        while True:
            events = store.read_events(locator["execution_id"], start_seq=next_seq, limit=500)
            for event in events:
                if event["seq"] > next_seq:
                    yield {"schema_version": model.SCHEMA_VERSION, "execution_id": locator["execution_id"], "seq": next_seq,
                           "ts": model.timestamp(time.time()), "type": "gap", "from_seq": next_seq, "to_seq": event["seq"] - 1,
                           "count": event["seq"] - next_seq, "reason": "missing"}
                if validate:
                    _validate(event, schemas.EVENT_SCHEMA)
                yield event
                next_seq = event["seq"] + 1
                last_progress = time.monotonic()
                if follow and event.get("type") == "execution.finished":
                    return
            if not follow:
                if len(events) < 500:
                    return
                continue
            if not events:
                if idle_timeout is not None and time.monotonic() - last_progress >= idle_timeout:
                    return
                if stop is not None and stop():
                    return
                time.sleep(poll_interval)
    finally:
        store.close()


def register() -> None:
    from neo4j_graph_loader.status import reader
    reader.register_store_reader("sql", read_status=_read_status, iter_events=_iter_events)


register()
