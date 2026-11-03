"""E15.F1.S4: the SQL status store on a temporary SQLite file (the PostgreSQL cells run on the host, see the evidence)."""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

pytest.importorskip("sqlalchemy")

import sqlalchemy as sa  # noqa: E402

from neo4j_graph_loader import cli  # noqa: E402
from neo4j_graph_loader.status import (  # noqa: E402
    StatusNotFound, StatusStoreError, StatusTracker, iter_events, model, read_status, schemas, validate, json_store, reader,
)
from neo4j_graph_loader.status import locator as locator_module, sql_store  # noqa: E402
from neo4j_graph_loader.status.sql_store import EventConflict, SchemaNotReady, SqlJournal, SqlStatusStore  # noqa: E402

STRICT_STATUS, STRICT_EVENT = schemas.STRICT_STATUS_SCHEMA, schemas.STRICT_EVENT_SCHEMA


class Clock:
    def __init__(self, start=1_790_000_000.0):
        self.now = start

    def __call__(self):
        self.now += 0.5
        return self.now


def tracker(**kwargs) -> StatusTracker:
    kwargs.setdefault("clock", Clock())
    return StatusTracker(host="test-host", pid=4242, package_version="0.1.0a2", **kwargs)


PG_URL_ENV = "E15_STATUS_TEST_PG_URL"        # an OWNED throwaway PostgreSQL (the host cells); SQLite always runs


@pytest.fixture(params=["sqlite", "postgres"])
def url(request, tmp_path, monkeypatch):
    """The database under test. ``postgres`` runs only when ``E15_STATUS_TEST_PG_URL`` names an owned throwaway
    server; each test gets its own schema there (``GRAPH_LOADER_STATUS_SCHEMA``), dropped afterwards."""
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'status.db'}"
        return
    pg = os.environ.get(PG_URL_ENV)
    if not pg:
        pytest.skip(f"{PG_URL_ENV} is not set")
    schema = "t_" + model.new_id()[:12]
    monkeypatch.setenv(sql_store.SCHEMA_ENV, schema)
    yield pg
    engine = sa.create_engine(pg)
    with engine.begin() as connection:
        connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    engine.dispose()


@pytest.fixture
def store(url):
    sql = SqlStatusStore(url)
    sql.migrate()
    yield sql
    sql.close()


def stream_run(execution: StatusTracker, *, finish: bool = True) -> StatusTracker:
    execution.role_started("node-loader", "Person", 0, container_id="c1", incarnation="a" * 32)
    execution.role_started("edge-loader", "KNOWS", 0, container_id="c2", incarnation="b" * 32)
    execution.update_type("node", "Person", records_written=model.observed(12, "2026-11-03T00:00:00.000Z", "role-snapshots"),
                          lag=model.observed(3, "2026-11-03T00:00:00.000Z", "node-loader"))
    execution.update_type("edge", "KNOWS", records_written=None, lag=None, retry_written=model.observed(1, "2026-11-03T00:00:00.000Z", "retry-consumer"))
    execution.update_totals(records_written=model.observed(12, "2026-11-03T00:00:00.000Z", "role-snapshots", lower_bound=True))
    execution.error("edge-write", "one transient failure")
    execution.role_exited("b" * 32, exit_code=1, error={"type": "RoleExit", "message": "exited with code 1"})
    execution.role_started("edge-loader", "KNOWS", 0, container_id="c3", incarnation="c" * 32)
    execution.heartbeat()
    if finish:
        execution.finish(0)
    return execution


def planned_run(execution: StatusTracker) -> StatusTracker:
    for name in ("planning", "nodes", "edges", "draining"):
        execution.declare_activity(name)
    execution.activity("planning", "running")
    execution.activity("nodes", "running")
    execution.activity("planning", "succeeded")
    execution.phase_started("r-bulk-0-aa", "bulk", 0, "KNOWS")
    execution.phase_started("r-topic-1-bb", "topic", 1, "KNOWS")
    execution.role_started("planned-dispatcher", "-", None, container_id="d1", incarnation="d" * 32)
    execution.role_started("planned-edge-writer", "-", 0, container_id="w0", incarnation="e" * 32)
    execution.update_type("edge", "KNOWS", plan_chunks_done=model.observed(2, "2026-11-03T00:00:00.000Z", "dispatcher-status"),
                          plan_chunks_total=model.observed(8, "2026-11-03T00:00:00.000Z", "dispatcher-status"))
    execution.update_totals(plan_chunks_done=model.observed(2, "2026-11-03T00:00:00.000Z", "dispatcher-status"),
                            rejected=model.observed(4, "2026-11-03T00:00:00.000Z", "planner"))
    execution.set_telemetry(False, "no role has written a readable status snapshot")
    execution.activity("nodes", "succeeded")
    execution.phase_finished("r-bulk-0-aa", "succeeded")
    execution.finish(1, reason="writer failed")
    return execution


def flush_to(store: SqlStatusStore, execution: StatusTracker) -> SqlJournal:
    journal = SqlJournal(store, snapshot_interval=0.0)
    assert execution.attach_journal(journal)
    execution.subscribe(on_snapshot=journal.offer_snapshot)
    return journal


# -- schema and migrations ------------------------------------------------------------------------------------------------------
def test_migration_is_idempotent_and_records_the_version(store):
    assert store.schema_version() == sql_store.DB_SCHEMA_VERSION
    assert store.migrate() == store.migrate() == sql_store.DB_SCHEMA_VERSION
    with store.engine.connect() as connection:
        assert connection.execute(sa.select(sa.func.count()).select_from(store.t["schema_version"])).scalar() == 1
        names = set(sa.inspect(connection).get_table_names(schema=store.schema))
    assert names == {"schema_version", "executions", "execution_activities", "execution_phases", "execution_labels",
                     "execution_types", "execution_roles", "execution_events"}
    store.check_schema()


def test_an_empty_database_an_older_and_a_newer_schema_are_refused_with_a_clear_message(url):
    fresh = SqlStatusStore(url)
    with pytest.raises(SchemaNotReady, match="status-store migrate"):
        fresh.check_schema()
    assert fresh.schema_version() is None
    fresh.migrate()
    for found, wording in ((0, "older|version 0"), (sql_store.DB_SCHEMA_VERSION + 1, "newer")):
        with fresh.engine.begin() as connection:
            connection.execute(sa.delete(fresh.t["schema_version"]))
            connection.execute(sa.insert(fresh.t["schema_version"]).values(version=found, applied_at=datetime.now(timezone.utc)))
        with pytest.raises(SchemaNotReady) as caught:
            fresh.check_schema()
        assert caught.value.found == found and __import__("re").search(wording, str(caught.value))
    with pytest.raises(SchemaNotReady):
        fresh.migrate()                                                       # never downgrades or rewrites a newer schema
    fresh.close()


def test_an_older_schema_is_upgraded_in_place_by_migrate(url):
    sql = SqlStatusStore(url)
    sql.migrate()
    with sql.engine.begin() as connection:
        connection.execute(sa.update(sql.t["schema_version"]).values(version=0))
    assert sql.schema_version() == 0
    assert sql.migrate() == sql_store.DB_SCHEMA_VERSION
    sql.check_schema()
    sql.close()


# -- snapshots: one transaction, monotonic revisions ----------------------------------------------------------------------------------
def test_snapshot_revisions_are_monotonic_and_an_older_one_is_rejected(store):
    execution = stream_run(tracker(mode="stream", run_id="r"))
    first = execution.snapshot()
    assert store.write_snapshot(first) is True
    assert store.write_snapshot(first) is False                              # same revision
    older = {**first, "revision": first["revision"] - 1, "state": "running", "roles": []}
    assert store.write_snapshot(older) is False
    assert store.read_status(first["execution_id"]) == first                 # untouched
    newer = {**first, "revision": first["revision"] + 1, "roles": first["roles"][:1], "labels": {"team": "graph"}}
    assert store.write_snapshot(newer) is True
    again = store.read_status(first["execution_id"])
    assert again["revision"] == newer["revision"] and len(again["roles"]) == 1 and again["labels"] == {"team": "graph"}


def test_a_snapshot_is_one_transaction_a_failure_midway_changes_nothing(store):
    execution = stream_run(tracker(mode="stream", run_id="r"))
    first = execution.snapshot()
    store.write_snapshot(first)
    newer = {**first, "revision": first["revision"] + 1, "state": "failed", "roles": [], "phases": [], "types": []}
    with patch.object(SqlStatusStore, "_insert_children", side_effect=RuntimeError("disk full")), pytest.raises(sql_store.StatusDatabaseError):
        store.write_snapshot(newer)
    assert store.read_status(first["execution_id"]) == first                  # the parent row was rolled back too
    assert store.write_snapshot(newer) is True


def test_a_status_document_survives_the_round_trip_exactly(store):
    for run in (stream_run(tracker(mode="stream", run_id="r")), planned_run(tracker(mode="bulk", run_id="p"))):
        snapshot = run.snapshot()
        store.write_snapshot(snapshot)
        stored = store.read_status(snapshot["execution_id"])
        assert stored == snapshot
        validate(stored, STRICT_STATUS)
    with store.engine.connect() as connection:
        table = store.t["executions"]
        assert connection.execute(sa.select(sa.func.count()).select_from(table).where(table.c.outcome.is_(None))).scalar() == 0
        roles = store.t["execution_roles"]
        assert connection.execute(sa.select(sa.func.count()).select_from(roles).where(roles.c.last_error.is_(None))).scalar() > 0


# -- events: at least once, deduplicated, conflicts are errors -----------------------------------------------------------------------------------
def test_events_are_idempotent_and_a_conflicting_payload_is_an_error_not_a_silent_skip(store):
    reported: list[str] = []
    store._metrics = reported.append
    execution = stream_run(tracker(mode="stream", run_id="r"))
    events = execution.events()
    assert store.write_events(events) == (len(events), 0)
    assert store.write_events(events) == (0, len(events))                    # delivered again: nothing changes
    assert store.write_events(events[:3] + [events[3], {**events[4]}]) == (0, 5)
    assert store.read_events(execution.execution_id) == events
    tampered = {**events[2], "message": "different"} if "message" in events[2] else {**events[2], "ts": "2026-01-01T00:00:00.000Z"}
    fresh = {**events[-1], "seq": events[-1]["seq"] + 1}
    with pytest.raises(EventConflict) as caught:
        store.write_events([fresh, tampered])
    assert (caught.value.execution_id, caught.value.seq) == (execution.execution_id, tampered["seq"])
    assert reported == ["event_conflict"]
    assert len(store.read_events(execution.execution_id)) == len(events)       # the whole batch was refused, `fresh` included
    twice = {**events[0], "ts": "2027-01-01T00:00:00.000Z"}
    with pytest.raises(EventConflict):
        store.write_events([events[0], twice])                                 # a conflict inside one batch


def test_the_conflict_counter_is_a_metric_family_with_a_closed_reason_set():
    from prometheus_client import CollectorRegistry
    from neo4j_graph_loader.utils import metrics

    registry = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    for reason in metrics.STATUS_STORE_ERROR_REASONS:
        registry.status_store_errors.labels(kind="cli", reason=reason).inc()
    assert registry.registry.get_sample_value("graph_loader_status_store_errors_total", {"kind": "cli", "reason": "event_conflict"}) == 1.0
    assert "graph_loader_status_store_errors" in metrics.EXPECTED_METRIC_FAMILIES


def test_repeated_and_concurrent_executions_with_the_same_run_id_stay_separate(store):
    runs = [tracker(mode="stream", run_id="shared") for _ in range(4)]
    journals = [flush_to(store, run) for run in runs]
    threads = [threading.Thread(target=stream_run, args=(run,)) for run in runs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for journal in journals:
        assert journal.close(20)
    rows = store.list_executions(run_id="shared")
    assert len(rows) == 4 and len({row.execution_id for row in rows}) == 4
    for run in runs:
        stored = store.read_status(run.execution_id)
        assert stored["run_id"] == "shared" and stored == run.snapshot()
        assert store.read_events(run.execution_id) == run.events()


# -- the same content as the JSON store ----------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("scenario, mode", [(stream_run, "stream"), (planned_run, "bulk")])
def test_the_sql_and_json_stores_hold_the_same_status_and_events(tmp_path, store, scenario, mode):
    ident = "0123456789abcdef" * 2
    json_dir = tmp_path / "json"
    json_dir.mkdir()
    json_run = tracker(mode=mode, run_id="same", execution_id=ident)
    json_journal = json_store.JsonStatusStore(json_dir, snapshot_interval=0.0, fsync=False)
    json_run.attach_journal(json_journal)
    json_run.subscribe(on_snapshot=json_journal.offer_snapshot)
    locator_module.write_locator(json_dir, locator_module.build_locator(
        store="json", execution_id=ident, run_id="same", created_at=json_run.snapshot()["started_at"]))
    sql_run = tracker(mode=mode, run_id="same", execution_id=ident)
    sql_journal = flush_to(store, sql_run)
    for run in (json_run, sql_run):
        scenario(run)
    json_journal.close()
    assert sql_journal.close(20)
    from_json, from_sql = read_status(json_dir), store.read_status(ident)
    assert from_sql == from_json == json_run.snapshot() == sql_run.snapshot()
    assert store.read_events(ident) == list(iter_events(json_dir)) == json_run.events()
    assert [event["seq"] for event in store.read_events(ident)] == list(range(1, len(json_run.events()) + 1))


# -- retention -----------------------------------------------------------------------------------------------------------------------------------
def test_prune_deletes_only_finished_executions_older_than_the_cutoff_with_all_their_rows(store):
    now = datetime.now(timezone.utc)

    def clocked(days_ago: float) -> Clock:
        return Clock((now - timedelta(days=days_ago)).timestamp())

    old_done = stream_run(tracker(mode="stream", run_id="a", clock=clocked(40)))
    old_failed = planned_run(tracker(mode="bulk", run_id="b", clock=clocked(35)))
    recent_done = stream_run(tracker(mode="stream", run_id="c", clock=clocked(2)))
    old_running = stream_run(tracker(mode="stream", run_id="d", clock=clocked(90)), finish=False)
    old_stopping = stream_run(tracker(mode="stream", run_id="e", clock=clocked(80)), finish=False)
    old_stopping.transition("stopping")
    old_starting = tracker(mode="stream", run_id="f", clock=clocked(70))
    everything = [old_done, old_failed, recent_done, old_running, old_stopping, old_starting]
    for run in everything:
        store.write_snapshot(run.snapshot())
        store.write_events(run.events())
    counts = store.prune(timedelta(days=30), now=now)
    assert counts["executions"] == 2 and counts["events"] == len(old_done.events()) + len(old_failed.events())
    remaining = {row.execution_id for row in store.list_executions()}
    assert remaining == {run.execution_id for run in (recent_done, old_running, old_stopping, old_starting)}
    with store.engine.connect() as connection:
        for name in ("execution_activities", "execution_phases", "execution_labels", "execution_types", "execution_roles", "execution_events"):
            table = store.t[name]
            ids = {row[0] for row in connection.execute(sa.select(table.c.execution_id).distinct())}
            assert ids <= remaining, name                                      # no orphan rows of a pruned execution
    assert store.prune(timedelta(days=30), now=now) == {"executions": 0, "events": 0}
    assert store.prune(timedelta(days=0.5), now=now)["executions"] == 1         # `recent_done` is finished, so it goes; the live ones stay
    assert {row.state for row in store.list_executions()} == {"running", "stopping", "starting"}


def test_durations_are_parsed_strictly():
    assert sql_store.parse_duration("30d") == timedelta(days=30) and sql_store.parse_duration("90s") == timedelta(seconds=90)
    assert sql_store.parse_duration("2w") == timedelta(weeks=2) and sql_store.parse_duration(" 12h ") == timedelta(hours=12)
    for bad in ("", "30", "d", "-5d", "5 days", "1y"):
        with pytest.raises(ValueError):
            sql_store.parse_duration(bad)


# -- listing --------------------------------------------------------------------------------------------------------------------------------------------
def test_listing_filters_active_executions_and_orders_newest_first(store):
    done = stream_run(tracker(mode="stream", run_id="x", clock=Clock(1_790_000_000.0)))
    live = stream_run(tracker(mode="stream", run_id="x", clock=Clock(1_790_100_000.0)), finish=False)
    for run in (done, live):
        store.write_snapshot(run.snapshot())
    assert [row.execution_id for row in store.list_executions()] == [live.execution_id, done.execution_id]
    assert [row.execution_id for row in store.list_executions(active=True)] == [live.execution_id]
    assert store.list_executions(run_id="nobody") == []


# -- the journal never blocks the load, retries, and delivers in order -------------------------------------------------------------------------------
def test_the_journal_survives_a_database_that_refuses_writes_and_delivers_later(store, caplog):
    execution = tracker(mode="stream", run_id="r")
    journal = SqlJournal(store, snapshot_interval=0.0, retry_seconds=0.05, max_retry_seconds=0.1)
    real = store.write_events
    failures = {"left": 4}

    def flaky(events):
        if failures["left"]:
            failures["left"] -= 1
            raise sql_store.StatusDatabaseError("database is down")
        return real(events)

    store.write_events = flaky
    assert execution.attach_journal(journal)
    execution.subscribe(on_snapshot=journal.offer_snapshot)
    started = time.monotonic()
    stream_run(execution)
    assert time.monotonic() - started < 2                                     # the tracker never waited for the database
    assert journal.close(20)
    assert journal.failures >= 4 and caplog.text.count("not accepting writes") == 1
    assert store.read_events(execution.execution_id) == execution.events()
    assert store.read_status(execution.execution_id) == execution.snapshot()


def test_the_journal_assigns_a_gap_free_seq(store):
    execution = tracker(mode="stream")
    journal = flush_to(store, execution)
    for index in range(60):
        execution.error("s", f"e{index}")
    assert journal.close(20)
    assert [e["seq"] for e in store.read_events(execution.execution_id)] == list(range(1, 62))


# -- credentials never appear ---------------------------------------------------------------------------------------------------------------------------
def test_credentials_are_absent_from_every_output(tmp_path, monkeypatch, capsys, caplog):
    pytest.importorskip("psycopg", reason="a PostgreSQL URL needs the [postgres] extra (psycopg 3)")
    password = "Zx9-very-secret-pw"
    bad_url = f"postgresql+psycopg://svc_user:{password}@127.0.0.1:1/runs?sslmode=disable"
    monkeypatch.setenv(sql_store.URL_ENV, bad_url)
    monkeypatch.setenv(sql_store.SHUTDOWN_DEADLINE_ENV, "0.3")
    caplog.set_level("DEBUG")

    sql = SqlStatusStore(bad_url)
    for call in (sql.migrate, sql.schema_version, lambda: sql.write_snapshot(tracker(mode="stream").snapshot()),
                 lambda: sql.write_events(tracker(mode="stream").events()), lambda: sql.read_status("0" * 32)):
        with pytest.raises(sql_store.StatusDatabaseError) as caught:
            call()
        assert password not in str(caught.value) and "svc_user:" not in str(caught.value)
    assert password not in sql.describe() and "svc_user" not in sql.describe()

    base = tmp_path / "runs"
    assert cli.main(["status-store", "migrate"]) == 1
    assert cli.main(["status-store", "prune", "--older-than", "1d"]) == 1
    with patch("neo4j_graph_loader.cli.handle_start", return_value=0):
        assert cli.main(["start", "--mode", "stream", "--status-store", "sql", "--run-dir", str(base)]) == 0
    out = capsys.readouterr()
    (attempt,) = list(base.glob("graph-loader-*"))
    everything = out.out + out.err + caplog.text + "".join(
        path.read_text(errors="replace") for path in attempt.rglob("*") if path.is_file())
    assert password not in everything and "svc_user" not in everything
    assert "127.0.0.1:1" in (attempt / "status-locator.json").read_text()
    loaded = locator_module.read_locator(attempt)
    assert loaded["store"] == "sql" and loaded["database"]["url"] == "postgresql+psycopg://127.0.0.1:1/runs?sslmode=disable"
    sql.close()


# -- the CLI --------------------------------------------------------------------------------------------------------------------------------------------------
def test_status_store_commands_migrate_and_prune(url, monkeypatch, capsys, caplog):
    monkeypatch.setenv(sql_store.URL_ENV, url)
    assert cli.main(["status-store", "migrate"]) == 0
    assert "status schema is at version 1" in capsys.readouterr().out
    assert cli.main(["status-store", "migrate"]) == 0
    assert cli.main(["status-store", "prune", "--older-than", "30d"]) == 0
    assert "pruned 0 finished execution(s) older than 30d" in capsys.readouterr().out
    assert cli.main(["status-store", "prune", "--older-than", "soon"]) == 1 and "not a duration" in caplog.text
    monkeypatch.delenv(sql_store.URL_ENV)
    caplog.clear()
    assert cli.main(["status-store", "migrate"]) == 1 and "no status database URL" in caplog.text


def test_prune_refuses_a_database_without_the_schema(url, monkeypatch, caplog):
    monkeypatch.setenv(sql_store.URL_ENV, url)
    assert cli.main(["status-store", "prune", "--older-than", "1d"]) == 1
    assert "status-store migrate" in caplog.text


def test_start_refuses_a_database_without_the_current_schema_unless_auto_migrate_is_given(tmp_path, url, monkeypatch, caplog):
    monkeypatch.setenv(sql_store.URL_ENV, url)
    base = tmp_path / "runs"
    handler = patch("neo4j_graph_loader.cli.handle_start", return_value=0)
    with handler as mocked:
        assert cli.main(["start", "--mode", "stream", "--status-store", "sql", "--run-dir", str(base)]) == 1
        mocked.assert_not_called()                                            # nothing was launched
    assert "status-store migrate" in caplog.text and "--status-store-auto-migrate" in caplog.text
    sql = SqlStatusStore(url)
    assert sql.schema_version() is None                                       # and the refusal changed nothing
    with handler as mocked:
        assert cli.main(["start", "--mode", "stream", "--status-store", "sql", "--status-store-auto-migrate", "--run-dir", str(base)]) == 0
        mocked.assert_called_once()
    assert sql.schema_version() == sql_store.DB_SCHEMA_VERSION
    sql.migrate()
    with handler:
        assert cli.main(["start", "--mode", "stream", "--status-store", "sql", "--run-dir", str(base)]) == 0   # now current
    sql.close()


def test_a_run_recorded_in_sql_is_read_back_through_the_locator_and_the_accessor(tmp_path, url, monkeypatch):
    monkeypatch.setenv(sql_store.URL_ENV, url)
    SqlStatusStore(url).migrate()
    base = tmp_path / "runs"

    def handle(args):
        args.execution.role_started("node-loader", "Person", 0, container_id="aa")
        args.execution.set_run_id("generated")
        return 0

    with patch("neo4j_graph_loader.cli.handle_start", side_effect=handle):
        assert cli.main(["start", "--mode", "bulk", "--status-store", "sql", "--run-dir", str(base)]) == 0
    (attempt,) = list(base.glob("graph-loader-*"))
    assert not (attempt / "status.json").exists() and not list(attempt.glob("events.*"))
    for source in (attempt, base):
        status = read_status(source)
        assert status["state"] == "succeeded" and status["run_id"] == "generated" and status["roles"][0]["name"] == "Person"
        events = list(iter_events(source))
        assert [e["seq"] for e in events] == list(range(1, len(events) + 1)) and events[-1]["type"] == "execution.finished"
    assert [e["seq"] for e in iter_events(attempt, follow=True, poll_interval=0.01, idle_timeout=1)] == [e["seq"] for e in events]
    other = tmp_path / "other.db"
    monkeypatch.setenv(sql_store.URL_ENV, f"sqlite:///{other}")
    SqlStatusStore(f"sqlite:///{other}").migrate()
    if url.startswith("sqlite"):
        assert read_status(attempt)["state"] == "succeeded"                   # an environment URL for another database is not used
        monkeypatch.delenv(sql_store.URL_ENV)                                 # the credential-free locator URL is enough for SQLite
        assert read_status(attempt)["state"] == "succeeded"
    else:                                                                     # PostgreSQL needs its password from the environment
        with pytest.raises(sql_store.StatusDatabaseError):
            read_status(attempt)
    with pytest.raises(StatusNotFound):
        read_status(attempt, execution_id=model.new_id())


def test_a_schema_load_failure_is_recorded_in_the_database(tmp_path, url, monkeypatch):
    monkeypatch.setenv(sql_store.URL_ENV, url)
    SqlStatusStore(url).migrate()
    monkeypatch.setattr(cli, "load_schema", lambda path: (_ for _ in ()).throw(cli.SchemaLoadError("not a schema")))
    schema = tmp_path / "broken.yaml"
    schema.write_text("nodes: [")
    base = tmp_path / "runs"
    assert cli.main(["start", "--mode", "stream", "--status-store", "sql", "--config", str(schema), "--run-dir", str(base)]) == 1
    (attempt,) = list(base.glob("graph-loader-*"))
    status = read_status(attempt)
    assert status["state"] == "failed" and status["outcome"]["exit_code"] == 1
    assert [e["type"] for e in iter_events(attempt)][0] == "execution.started"


def test_rejected_input_creates_nothing_with_the_sql_store_either(tmp_path, url, monkeypatch):
    monkeypatch.setenv(sql_store.URL_ENV, url)
    SqlStatusStore(url).migrate()
    base = tmp_path / "runs"
    assert cli.main(["start", "--mode", "bulk", "--status-store", "sql", "--bulk-timeout-seconds", "0", "--run-dir", str(base)]) == 1
    assert cli.main(["start", "--mode", "bulk", "--status-store", "sql", "--config", str(tmp_path / "missing"), "--run-dir", str(base)]) == 1
    assert not base.exists()
    sql = SqlStatusStore(url)
    assert sql.list_executions() == []
    sql.close()


def test_an_unreachable_database_never_fails_the_load(tmp_path, monkeypatch, caplog):
    pytest.importorskip("psycopg", reason="a PostgreSQL URL needs the [postgres] extra (psycopg 3)")
    monkeypatch.setenv(sql_store.SHUTDOWN_DEADLINE_ENV, "0.3")
    monkeypatch.setenv(sql_store.URL_ENV, "postgresql+psycopg://u:p4ss@127.0.0.1:1/runs")
    with patch("neo4j_graph_loader.cli.handle_start", return_value=0):
        assert cli.main(["start", "--mode", "stream", "--status-store", "sql", "--run-dir", str(tmp_path / "runs")]) == 0
    assert "not reachable yet" in caplog.text and "p4ss" not in caplog.text


# -- optional dependencies --------------------------------------------------------------------------------------------------------------------------------
def test_importing_the_cli_and_the_status_package_does_not_import_sqlalchemy():
    code = ("import sys, neo4j_graph_loader.cli, neo4j_graph_loader.status\n"
            "print('sqlalchemy' in sys.modules, 'psycopg' in sys.modules, 'neo4j_graph_loader.status.sql_store' in sys.modules)")
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.split() == ["False", "False", "False"]


def test_without_the_sql_extra_the_choice_is_one_log_line_and_the_accessor_says_what_to_install(tmp_path, url, monkeypatch, caplog):
    monkeypatch.setenv(sql_store.URL_ENV, url)
    directory = tmp_path / "graph-loader-x"
    directory.mkdir()
    locator_module.write_locator(directory, locator_module.build_locator(
        store="sql", execution_id=model.new_id(), run_id=None, created_at="2026-11-03T00:00:00.000Z", db_url=url, schema=None))
    blocked = {name: None for name in list(sys.modules) if name == "sqlalchemy" or name.startswith(("sqlalchemy.", "neo4j_graph_loader.status.sql_store"))}
    blocked.update({"sqlalchemy": None, "neo4j_graph_loader.status.sql_store": None})
    reader._STORE_READERS.pop("sql", None)
    import neo4j_graph_loader.status as status_package
    monkeypatch.delattr(status_package, "sql_store", raising=False)               # `from package import module` finds attributes first
    with patch.dict(sys.modules, blocked):
        with pytest.raises(StatusStoreError, match=r"neo4j-graph-loader\[sql\]"):
            read_status(directory)
        with patch("neo4j_graph_loader.cli.handle_start", return_value=0):
            assert cli.main(["start", "--mode", "stream", "--status-store", "sql", "--run-dir", str(tmp_path / "runs")]) == 0
    assert caplog.text.count("needs the optional SQL support") == 1


def test_a_driver_message_that_contains_the_url_or_the_password_is_scrubbed():
    pytest.importorskip("psycopg", reason="a PostgreSQL URL needs the [postgres] extra (psycopg 3)")
    password = "Qw3rty-pw"
    secret_url = f"postgresql+psycopg://svc:{password}@db.internal:5432/runs"
    sql = SqlStatusStore(secret_url)
    leaks = [f"could not connect using {secret_url}", f"FATAL: password authentication failed (password={password})",
             f"dsn=host=db.internal password={password} dbname=runs", f"bad credentials {password} for svc"]
    for message in leaks:
        with patch.object(sql.engine, "connect", side_effect=RuntimeError(message)), patch.object(sql.engine, "begin", side_effect=RuntimeError(message)):
            for call in (sql.schema_version, sql.migrate, lambda: sql.write_snapshot(tracker(mode="stream").snapshot()),
                         lambda: sql.write_events(tracker(mode="stream").events()), lambda: sql.read_events("0" * 32)):
                with pytest.raises(sql_store.StatusDatabaseError) as caught:
                    call()
                assert password not in str(caught.value) and secret_url not in str(caught.value)
    assert sql_store._scrub(f"x {secret_url} y", secret_url) == "x *** y"


# -- TLS against an owned PostgreSQL with an owned CA (host cells; skipped unless E15_STATUS_TEST_PG_TLS_* is set) --------------------------------------
def _tls_setting(name: str) -> str:
    value = os.environ.get(f"E15_STATUS_TEST_PG_TLS_{name}")
    if not value:
        pytest.skip(f"E15_STATUS_TEST_PG_TLS_{name} is not set")
    return value


def _tls_url(host: str, mode: str, root: str | None) -> str:
    from urllib.parse import quote
    base = (f"postgresql+psycopg://{_tls_setting('USER')}:{quote(_tls_setting('PASSWORD'), safe='')}@{host}:{_tls_setting('PORT')}/"
            f"{_tls_setting('DATABASE')}?sslmode={mode}")
    return base + (f"&sslrootcert={root}" if root else "")


def test_tls_verify_full_against_the_owned_ca_succeeds_and_every_other_combination_is_refused(monkeypatch, capsys, caplog):
    password, ca, wrong_ca = _tls_setting("PASSWORD"), _tls_setting("CA"), _tls_setting("WRONG_CA")
    schema = "tls_" + model.new_id()[:10]
    monkeypatch.setenv(sql_store.SCHEMA_ENV, schema)
    good = SqlStatusStore(_tls_url("localhost", "verify-full", ca))
    try:
        assert good.migrate() == sql_store.DB_SCHEMA_VERSION
        execution = stream_run(tracker(mode="stream", run_id="tls"))
        assert good.write_snapshot(execution.snapshot()) and good.write_events(execution.events())[0] == len(execution.events())
        assert good.read_status(execution.execution_id) == execution.snapshot()
        with good.engine.connect() as connection:                              # the session really is encrypted
            assert connection.execute(sa.text("select ssl from pg_stat_ssl where pid = pg_backend_pid()")).scalar() is True
        refused = {
            "wrong CA": _tls_url("localhost", "verify-full", wrong_ca),
            "host name not in the certificate": _tls_url("127.0.0.1", "verify-full", ca),
            "verify-full without a root certificate": _tls_url("localhost", "verify-full", None),
            "TLS disabled against a hostssl-only server": _tls_url("localhost", "disable", None),
        }
        for what, url in refused.items():
            bad = SqlStatusStore(url)
            with pytest.raises(sql_store.StatusDatabaseError) as caught:
                bad.schema_version()
            assert password not in str(caught.value), what
            bad.close()
        encrypted_only = SqlStatusStore(_tls_url("127.0.0.1", "verify-ca", ca))   # the same certificate, host name not checked
        assert encrypted_only.schema_version() == sql_store.DB_SCHEMA_VERSION
        encrypted_only.close()
    finally:
        with good.engine.begin() as connection:
            connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        good.close()
    out = capsys.readouterr()
    assert password not in out.out + out.err + caplog.text


def test_the_sql_store_choice_writes_its_locator_without_credentials_and_an_unreachable_database_is_a_warning(tmp_path, monkeypatch, caplog):
    pytest.importorskip("psycopg", reason="a PostgreSQL URL needs the [postgres] extra (psycopg 3)")
    monkeypatch.setenv(cli.STATUS_DB_URL_ENV, "postgresql://svc:hunter2@127.0.0.1:1/runs")
    monkeypatch.setenv("GRAPH_LOADER_STATUS_SHUTDOWN_DEADLINE_S", "0.2")
    base = tmp_path / "runs"
    with patch("neo4j_graph_loader.cli.handle_start", return_value=0):
        assert cli.main(["start", "--mode", "stream", "--status-store", "sql", "--run-dir", str(base)]) == 0
    (attempt,) = list(base.glob("graph-loader-*"))
    text = (attempt / "status-locator.json").read_text()
    assert "hunter2" not in text and "svc" not in text and "hunter2" not in caplog.text
    loaded = locator_module.read_locator(attempt)
    assert loaded["store"] == "sql" and loaded["database"] == {"url": "postgresql://127.0.0.1:1/runs", "schema": "graph_loader"}
    assert "not reachable yet" in caplog.text and not (attempt / "status.json").exists()
