"""E15.F1.S5: the durable outbox in front of the SQL status store, and its recovery (SQLite stands in for the database)."""
from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

pytest.importorskip("sqlalchemy")

import sqlalchemy as sa  # noqa: E402

from neo4j_graph_loader import cli  # noqa: E402
from neo4j_graph_loader.status import StatusTracker, json_store, locator as locator_module, model  # noqa: E402
from neo4j_graph_loader.status import outbox as outbox_module, sql_store  # noqa: E402
from neo4j_graph_loader.status.outbox import AttemptLock, Outbox, OutboxJournal, OutboxLocked  # noqa: E402
from neo4j_graph_loader.status.sql_store import SqlStatusStore  # noqa: E402

CHILD = Path(__file__).resolve().parent / "fixtures" / "e15_outbox_child.py"


PG_URL_ENV = "E15_STATUS_TEST_PG_URL"        # an OWNED throwaway PostgreSQL (the host cells); SQLite always runs


@pytest.fixture(params=["sqlite", "postgres"])
def db(request, tmp_path, monkeypatch):
    """A reachable database (migrated). ``postgres`` runs only when ``E15_STATUS_TEST_PG_URL`` names an owned throwaway
    server; each test gets its own schema there (``GRAPH_LOADER_STATUS_SCHEMA``), dropped afterwards."""
    schema = None
    if request.param == "sqlite":
        url = f"sqlite:///{tmp_path / 'status.db'}"
    else:
        url = os.environ.get(PG_URL_ENV)
        if not url:
            pytest.skip(f"{PG_URL_ENV} is not set")
        schema = "t_" + model.new_id()[:12]
        monkeypatch.setenv(sql_store.SCHEMA_ENV, schema)
    monkeypatch.setenv(sql_store.URL_ENV, url)                  # recovery takes its credentials from the environment, as it does for real
    store = SqlStatusStore(url)
    store.migrate()
    yield url, store
    store.close()
    if schema:
        engine = sa.create_engine(url)
        with engine.begin() as connection:
            connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        engine.dispose()


def down_url(tmp_path) -> str:
    return f"sqlite:///{tmp_path / 'no-such-directory' / 'status.db'}"          # opening it fails: the database is down


def new_attempt(tmp_path, name="graph-loader-a1") -> Path:
    attempt = tmp_path / name
    attempt.mkdir()
    return attempt


def run_child(tmp_path, attempt, url, mode, events) -> tuple[int, list[int]]:
    result = subprocess.run([sys.executable, str(CHILD), str(attempt), url, mode, str(events)], capture_output=True, text=True, timeout=60)
    acked = [int(line.split()[1]) for line in result.stdout.splitlines() if line.startswith("acked ")]
    return result.returncode, acked


def attempt_execution_id(attempt) -> str:
    return locator_module.read_locator(attempt)["execution_id"]


def flush(attempt, url, monkeypatch, capsys=None, wait="30") -> int:
    monkeypatch.setenv(sql_store.URL_ENV, url)
    return cli.main(["status-store", "flush", "--run-dir", str(attempt), "--wait", wait])


# -- the durable acknowledgement ----------------------------------------------------------------------------------------------------
def test_an_event_is_fsynced_before_append_returns_and_gets_its_seq_there(tmp_path):
    outbox = Outbox(new_attempt(tmp_path))
    order: list[str] = []
    real_write, real_fsync = os.write, os.fsync

    def write(fd, data):
        order.append("write")
        return real_write(fd, data)

    def fsync(fd):
        order.append("fsync")
        return real_fsync(fd)

    with patch("os.write", write), patch("os.fsync", fsync):
        event = outbox.append(lambda seq: {"seq": seq, "type": "error"})
        order.append("returned")
    last_fsync = len(order) - 1 - order[::-1].index("fsync")           # (the first fsync is the new segment's directory entry)
    assert event["seq"] == 1 and order.index("write") < last_fsync < order.index("returned")
    assert [json.loads(line)["seq"] for line in next(path for _f, path in json_store.list_segments(outbox.directory)).read_text().splitlines()] == [1]
    with pytest.raises(ValueError):
        outbox.append(lambda seq: {"seq": seq + 5})                           # a make_event that disagrees about seq is refused


def test_seq_continues_gap_free_across_segments_and_across_a_restart(tmp_path):
    attempt = new_attempt(tmp_path)
    outbox = Outbox(attempt, segment_bytes=300, max_bytes=10**9)
    for _ in range(20):
        outbox.append(lambda seq: {"seq": seq, "type": "error", "message": "x" * 40})
    assert len(json_store.list_segments(outbox.directory)) > 3
    again = Outbox(attempt)
    again.recover()
    assert again.next_seq == 21
    assert again.append(lambda seq: {"seq": seq, "type": "error"})["seq"] == 21
    assert [event["seq"] for event in again.pending("e" * 32, 100)] == list(range(1, 22))


# -- the database is down for the whole run, then the CLI is killed -----------------------------------------------------------------------
@pytest.mark.parametrize("mode", ["kill-after-ack", "kill-before-write"])
def test_every_acknowledged_event_is_delivered_by_flush_and_an_unacknowledged_one_never_appears(tmp_path, db, monkeypatch, mode, capsys):
    url, store = db
    attempt = new_attempt(tmp_path)
    code, acked = run_child(tmp_path, attempt, down_url(tmp_path), mode, 12)
    assert code == -signal.SIGKILL and acked == list(range(2, 14))             # execution.started is seq 1; the child acks seq 2..13
    # the child's locator names the database that was down; the same database is up now (same schema, other path)
    execution_id = attempt_execution_id(attempt)
    locator = locator_module.read_locator(attempt)
    locator["database"]["url"] = locator_module.sanitize_db_url(url)
    locator_module.write_locator(attempt, locator)
    assert store.read_events(execution_id) == []                              # nothing reached the database during the run
    assert flush(attempt, url, monkeypatch) == 0
    delivered = [event["seq"] for event in store.read_events(execution_id)]
    assert delivered == list(range(1, 14)) and len(set(delivered)) == len(delivered)    # no missing, no duplicate seq
    assert all(event["type"] in ("execution.started", "error") for event in store.read_events(execution_id))
    assert "never acknowledged" not in json.dumps(store.read_events(execution_id))
    status = store.read_status(execution_id)
    assert status["state"] in ("starting", "running") and status["history_incomplete"] is False
    capsys.readouterr()
    assert flush(attempt, url, monkeypatch) == 0                              # a second flush finds nothing and changes nothing
    assert "delivered 0 event(s)" in capsys.readouterr().out
    assert [event["seq"] for event in store.read_events(execution_id)] == delivered


def test_a_clean_finish_with_the_database_down_leaves_the_outbox_for_flush_within_the_deadline(tmp_path, db, monkeypatch):
    url, store = db
    attempt = new_attempt(tmp_path)
    started = time.monotonic()
    result = subprocess.run([sys.executable, str(CHILD), str(attempt), down_url(tmp_path), "clean", "5"], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0 and "closed False" in result.stdout
    assert time.monotonic() - started < 30                                    # the shutdown deadline (0.3 s) was honoured
    locator = locator_module.read_locator(attempt)
    locator["database"]["url"] = locator_module.sanitize_db_url(url)
    locator_module.write_locator(attempt, locator)
    assert flush(attempt, url, monkeypatch) == 0
    status = store.read_status(attempt_execution_id(attempt))
    assert status["state"] == "succeeded"                                     # the terminal snapshot was in the outbox too
    events = store.read_events(attempt_execution_id(attempt))
    assert events[-1]["type"] == "execution.finished" and [e["seq"] for e in events] == list(range(1, len(events) + 1))


# -- an outbox that cannot be written ----------------------------------------------------------------------------------------------------
def test_an_unwritable_outbox_then_sigkill_claims_no_exact_gap_and_flags_the_history_incomplete(tmp_path, db, monkeypatch):
    url, store = db
    attempt = new_attempt(tmp_path)
    code, acked = run_child(tmp_path, attempt, down_url(tmp_path), "unwritable", 4)
    assert code == -signal.SIGKILL and len(acked) == 7                        # 4 acknowledged, then 3 only queued in memory
    assert (attempt / outbox_module.DEGRADED_NAME).exists()
    outbox_dir = attempt / outbox_module.OUTBOX_DIR
    outbox_dir.chmod(stat.S_IRWXU)                                            # the operator fixed the disk
    locator = locator_module.read_locator(attempt)
    locator["database"]["url"] = locator_module.sanitize_db_url(url)
    locator_module.write_locator(attempt, locator)
    assert flush(attempt, url, monkeypatch) == 0
    execution_id = attempt_execution_id(attempt)
    events = store.read_events(execution_id)
    assert [e["seq"] for e in events] == list(range(1, 6))                    # started + the 4 durable ones; the in-memory 3 are gone
    assert not [e for e in events if e["type"] == "gap"]                      # and no gap is claimed for them
    assert store.read_status(execution_id)["history_incomplete"] is True      # but the history is flagged


def test_a_live_run_with_an_unwritable_outbox_degrades_once_and_still_delivers_from_memory(tmp_path, db, caplog):
    url, store = db
    attempt = new_attempt(tmp_path)
    execution = StatusTracker(mode="stream", run_id="live")
    errors: list[str] = []
    journal = OutboxJournal(attempt, store, execution.execution_id, snapshot_interval=0.0, on_error=errors.append,
                            on_degraded=execution.mark_history_incomplete)
    assert execution.attach_journal(journal)
    execution.subscribe(on_snapshot=journal.offer_snapshot)
    execution.error("s", "durable")
    journal._outbox.close_segment()                                           # the next append must create a file
    (attempt / outbox_module.OUTBOX_DIR).chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        if os.access(attempt / outbox_module.OUTBOX_DIR, os.W_OK):
            pytest.skip("running as a user that ignores directory permissions")
        for index in range(5):
            execution.error("s", f"queued {index}")
        execution.finish(0)
        assert journal.close(20)
    finally:
        (attempt / outbox_module.OUTBOX_DIR).chmod(stat.S_IRWXU)
    assert errors.count("outbox_unwritable") == 1 and caplog.text.count("outbox cannot be written") == 1
    assert [e["seq"] for e in store.read_events(execution.execution_id)] == [e["seq"] for e in execution.events()]
    stored = store.read_status(execution.execution_id)
    assert stored["history_incomplete"] is True and stored["state"] == "succeeded"


# -- overflow ------------------------------------------------------------------------------------------------------------------------------
def test_overflow_replaces_the_oldest_events_by_a_durable_gap_record_written_before_they_are_deleted(tmp_path, db):
    url, store = db
    attempt = new_attempt(tmp_path)
    outbox = Outbox(attempt, segment_bytes=400, max_bytes=1200)
    seen: list[dict] = []
    outbox.on_gap = lambda gap: (gap.update(execution_id="f" * 32), seen.append(dict(gap)))
    gaps_at_unlink: list[list[dict]] = []
    real_unlink = Path.unlink

    def checking_unlink(self, *args, **kwargs):
        if self.name.startswith("events."):
            gaps_at_unlink.append(outbox._read_gaps())                       # what is durable on disk at the moment of deletion
        return real_unlink(self, *args, **kwargs)

    with patch.object(Path, "unlink", checking_unlink):
        for _ in range(60):
            outbox.append(lambda seq: {"schema_version": 1, "execution_id": "f" * 32, "seq": seq, "ts": "2026-11-03T00:00:00.000Z",
                                       "type": "error", "stage": "s", "message": "m" * 30})
    assert seen and all(g["reason"] == "outbox-overflow" and g["count"] == g["to_seq"] - g["from_seq"] + 1 for g in seen)
    for gap, durable in zip(seen, gaps_at_unlink):
        assert any(d["from_seq"] == gap["from_seq"] and d["to_seq"] == gap["to_seq"] for d in durable)      # written first
    records = outbox.pending("f" * 32, 1000)
    covered = set()
    for record in records:
        covered |= set(range(record["from_seq"], record["to_seq"] + 1)) if record["type"] == "gap" else {record["seq"]}
    assert covered == set(range(1, 61))                                       # every seq is an event or inside a reported gap
    assert outbox.bytes_used() <= 1200 + 400


def test_the_database_receives_the_gap_as_an_event_and_a_reader_continues_past_it(tmp_path, db):
    url, store = db
    attempt = new_attempt(tmp_path)
    execution = StatusTracker(mode="stream", run_id="overflow")
    locator_module.write_locator(attempt, locator_module.build_locator(
        store="sql", execution_id=execution.execution_id, run_id="overflow", created_at=execution.snapshot()["started_at"], db_url=url))
    journal = OutboxJournal(attempt, store, execution.execution_id, snapshot_interval=0.0, retry_seconds=0.05, max_retry_seconds=0.1,
                            settings={"max_bytes": 1500, "segment_bytes": 500})
    original = store.write_events
    store.write_events = lambda events: (_ for _ in ()).throw(sql_store.StatusDatabaseError("down"))     # down while overflowing
    assert execution.attach_journal(journal)
    execution.subscribe(on_snapshot=journal.offer_snapshot)
    for index in range(50):
        execution.error("s", f"message {index}" + "x" * 20)
    store.write_events = original                                              # the database comes back
    execution.finish(0)
    assert journal.close(20)
    stored = store.read_events(execution.execution_id)
    gaps = [e for e in stored if e["type"] == "gap"]
    assert gaps and gaps[0]["seq"] == gaps[0]["from_seq"] and gaps[0]["reason"] == "outbox-overflow"
    covered = set()
    for event in stored:
        covered |= set(range(event["from_seq"], event["to_seq"] + 1)) if event["type"] == "gap" else {event["seq"]}
    assert covered == set(range(1, len(execution.events()) + 1))
    for event in stored:
        from neo4j_graph_loader.status import schemas, validate
        validate(event, schemas.STRICT_EVENT_SCHEMA)


# -- locking and recovery ------------------------------------------------------------------------------------------------------------------
def prepared_attempt(tmp_path, db, events=10, name="graph-loader-p1"):
    url, store = db
    attempt = new_attempt(tmp_path, name)
    execution = StatusTracker(mode="stream", run_id="p")
    locator_module.write_locator(attempt, locator_module.build_locator(
        store="sql", execution_id=execution.execution_id, run_id="p", created_at=execution.snapshot()["started_at"], db_url=url))
    outbox = Outbox(attempt)
    for event in execution.events():
        outbox.append(lambda seq, event=event: {**event, "seq": seq})
    for index in range(events):
        outbox.append(lambda seq, index=index: {"schema_version": 1, "execution_id": execution.execution_id, "seq": seq,
                                                "ts": "2026-11-03T00:00:00.000Z", "type": "error", "stage": "s", "message": f"m{index}"})
    outbox.write_snapshot({**execution.snapshot(), "revision": 50})
    return attempt, execution.execution_id, store


def test_concurrent_flushes_are_serialized_by_the_lock_and_deliver_each_event_once(tmp_path, db):
    attempt, execution_id, store = prepared_attempt(tmp_path, db, events=40)
    results, errors = [], []

    def run():
        try:
            results.append(outbox_module.recover_attempt(attempt, lock_timeout=20))
        except Exception as exc:                                              # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors and sorted(r["events"] for r in results) == [0, 0, 0, 41]
    assert [e["seq"] for e in store.read_events(execution_id)] == list(range(1, 42))


def test_a_live_owner_blocks_flush_and_start_recovery_until_it_lets_go(tmp_path, db, monkeypatch, caplog):
    attempt, execution_id, store = prepared_attempt(tmp_path, db)
    lock = AttemptLock(attempt)
    lock.acquire()
    try:
        with pytest.raises(OutboxLocked):
            outbox_module.recover_attempt(attempt, lock_timeout=0)
        assert flush(attempt, db[0], monkeypatch, wait="0.2") == 1 and "in use by another process" in caplog.text
        releaser = threading.Timer(0.4, lock.release)
        releaser.start()
        assert outbox_module.recover_attempt(attempt, lock_timeout=10)["events"] == 11
        releaser.join()
    finally:
        lock.release()
    assert [e["seq"] for e in store.read_events(execution_id)] == list(range(1, 12))


def test_a_torn_tail_is_truncated_and_only_complete_records_are_delivered(tmp_path, db):
    attempt, execution_id, store = prepared_attempt(tmp_path, db, events=6)
    (_first, path), = json_store.list_segments(attempt / outbox_module.OUTBOX_DIR)
    with open(path, "ab") as handle:
        handle.write(b'{"schema_version":1,"execution_id":"' + execution_id.encode() + b'","seq":99,"ty')
    result = outbox_module.recover_attempt(attempt)
    assert result["cut_bytes"] > 0 and result["events"] == 7
    assert [e["seq"] for e in store.read_events(execution_id)] == list(range(1, 8))
    assert path.read_bytes().endswith(b"\n")
    again = Outbox(attempt, create=False)
    again.recover()
    assert again.next_seq == 8


def test_an_interrupted_replay_resumes_from_the_watermark_and_a_redelivered_batch_is_deduplicated(tmp_path, db):
    attempt, execution_id, store = prepared_attempt(tmp_path, db, events=30)
    real = store.write_events
    calls = {"n": 0}

    def flaky(events):
        calls["n"] += 1
        if calls["n"] == 2:
            raise sql_store.StatusDatabaseError("connection lost")
        return real(events)

    store.write_events = flaky
    outbox = Outbox(attempt, create=False)
    outbox.recover()
    with pytest.raises(sql_store.StatusDatabaseError):
        outbox_module.deliver(outbox, store, execution_id, batch=10)
    assert outbox.ack()["through_seq"] == 10 and [e["seq"] for e in real.__self__.read_events(execution_id)] == list(range(1, 11))
    store.write_events = real
    # a crash between the database commit and the ack: rows 11-20 are in the database but the watermark still says 10
    store.write_events(outbox.pending(execution_id, 10))
    delivered = outbox_module.deliver(outbox, store, execution_id, batch=10)
    assert delivered["events"] == 21 and outbox.ack()["through_seq"] == 31
    assert [e["seq"] for e in store.read_events(execution_id)] == list(range(1, 32))
    assert not outbox.pending(execution_id, 5)


def test_recovery_flags_a_degraded_run_incomplete_even_though_it_can_prove_no_gap(tmp_path, db):
    attempt, execution_id, store = prepared_attempt(tmp_path, db, events=2)
    assert outbox_module.write_degraded_marker(attempt, "OSError: disk full")
    result = outbox_module.recover_attempt(attempt)
    assert result["history_incomplete"] is True and store.read_status(execution_id)["history_incomplete"] is True
    assert not [e for e in store.read_events(execution_id) if e["type"] == "gap"]


def test_the_next_start_delivers_what_an_earlier_run_left_behind(tmp_path, db, monkeypatch):
    url, store = db
    monkeypatch.setenv(sql_store.URL_ENV, url)
    base = tmp_path / "runs"
    base.mkdir()
    attempt, execution_id, _ = prepared_attempt(base, db, events=8, name="graph-loader-old1")
    with patch("neo4j_graph_loader.cli.handle_start", return_value=0):
        assert cli.main(["start", "--mode", "stream", "--status-store", "sql", "--status-store-auto-migrate", "--run-dir", str(base)]) == 0
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and len(store.read_events(execution_id)) < 9:
        time.sleep(0.05)
    assert [e["seq"] for e in store.read_events(execution_id)] == list(range(1, 10))
    assert len(store.list_executions()) == 2                                  # the old run and the new one


def test_flush_reports_nothing_to_deliver_and_refuses_a_non_sql_run(tmp_path, db, monkeypatch, capsys, caplog):
    monkeypatch.setenv(sql_store.URL_ENV, db[0])
    empty = tmp_path / "runs"
    empty.mkdir()
    assert cli.main(["status-store", "flush", "--run-dir", str(empty)]) == 0 and "nothing to deliver" in capsys.readouterr().out
    json_attempt = new_attempt(tmp_path, "graph-loader-json")
    locator_module.write_locator(json_attempt, locator_module.build_locator(
        store="json", execution_id=model.new_id(), run_id=None, created_at="2026-11-03T00:00:00.000Z"))
    assert cli.main(["status-store", "flush", "--run-dir", str(json_attempt)]) == 1 and "no outbox to deliver" in caplog.text


# -- the database's latency does not reach the code that emits events ---------------------------------------------------------------------------
def test_two_seconds_of_database_latency_leave_the_emit_path_unchanged(tmp_path):
    def timed(latency: float) -> float:
        attempt = new_attempt(tmp_path, f"graph-loader-latency-{latency}")
        store = SqlStatusStore(f"sqlite:///{tmp_path / f'db-{latency}.db'}")
        store.migrate()
        real = store.write_events

        def slow(events):
            time.sleep(latency)
            return real(events)

        store.write_events = slow
        execution = StatusTracker(mode="stream", run_id="lat")
        journal = OutboxJournal(attempt, store, execution.execution_id, snapshot_interval=0.0)
        execution.attach_journal(journal)
        execution.subscribe(on_snapshot=journal.offer_snapshot)
        started = time.perf_counter()
        for index in range(150):
            execution.error("s", f"event {index}")
            execution.update_totals(records_written=model.observed(index, "2026-11-03T00:00:00.000Z", "t"))
        elapsed = time.perf_counter() - started
        journal.close(0.3)
        return elapsed

    fast, slow = timed(0.0), timed(2.0)
    print(f"150 events + 150 totals updates: no latency {fast:.3f}s, 2 s database latency {slow:.3f}s")
    assert slow < fast + 0.5 and slow < 3.0


def test_close_honours_its_deadline_when_the_database_stays_down(tmp_path):
    attempt = new_attempt(tmp_path)
    store = SqlStatusStore(down_url(tmp_path))
    execution = StatusTracker(mode="stream", run_id="d")
    lock = AttemptLock(attempt)
    lock.acquire()
    journal = OutboxJournal(attempt, store, execution.execution_id, snapshot_interval=0.0, retry_seconds=0.05, max_retry_seconds=0.1, lock=lock)
    execution.attach_journal(journal)
    execution.subscribe(on_snapshot=journal.offer_snapshot)
    for index in range(10):
        execution.error("s", f"e{index}")
    started = time.monotonic()
    assert journal.close(0.5) is False
    assert time.monotonic() - started < 3
    outbox = Outbox(attempt, create=False)
    assert len(outbox.pending(execution.execution_id, 100)) == 11              # everything is still on disk for recovery
    other = AttemptLock(attempt)
    other.acquire()                                                           # the lock was released at close
    other.release()


def test_the_database_going_down_and_up_again_is_delivered_in_order_without_loss(tmp_path, db):
    url, store = db
    attempt = new_attempt(tmp_path)
    execution = StatusTracker(mode="stream", run_id="flap")
    real = store.write_events
    state = {"up": False}
    store.write_events = lambda events: real(events) if state["up"] else (_ for _ in ()).throw(sql_store.StatusDatabaseError("down"))
    journal = OutboxJournal(attempt, store, execution.execution_id, snapshot_interval=0.0, retry_seconds=0.05, max_retry_seconds=0.1)
    execution.attach_journal(journal)
    execution.subscribe(on_snapshot=journal.offer_snapshot)
    for index in range(30):
        execution.error("s", f"e{index}")
    time.sleep(0.3)
    assert store.read_events(execution.execution_id) == []
    state["up"] = True
    for index in range(30, 60):
        execution.error("s", f"e{index}")
    execution.finish(0)
    assert journal.close(20)
    assert store.read_events(execution.execution_id) == execution.events()
    assert not Outbox(attempt, create=False).pending(execution.execution_id)


def test_the_overflow_and_unwritable_reasons_are_part_of_the_closed_store_error_set():
    from neo4j_graph_loader.utils import metrics

    assert {"event_conflict", "write_failed", "outbox_unwritable", "outbox_overflow"} <= set(metrics.STATUS_STORE_ERROR_REASONS)
    assert sa is not None
