"""E15.F1.S3: the JSON status store, the locator and the Python accessor (``read_status`` / ``iter_events``)."""
from __future__ import annotations

import json
import os
import stat
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from neo4j_graph_loader import cli
from neo4j_graph_loader.status import (
    StatusDataError, StatusNotFound, StatusStoreError, StatusTracker, iter_events, model, read_status,
    schemas, validate,
)
from neo4j_graph_loader.status import json_store, locator as locator_module, reader

STRICT_EVENT = schemas.STRICT_EVENT_SCHEMA


def make_store(directory: Path, **kwargs) -> json_store.JsonStatusStore:
    kwargs.setdefault("snapshot_interval", 0.0)
    kwargs.setdefault("fsync", False)
    return json_store.JsonStatusStore(directory, **kwargs)


def attached(directory: Path, **kwargs):
    """A tracker whose journal and snapshots go to a JSON store in ``directory`` (what ``start`` sets up)."""
    execution = StatusTracker(mode="stream", run_id="r1")
    store = make_store(directory, **kwargs)
    assert execution.attach_journal(store)
    execution.subscribe(on_snapshot=store.offer_snapshot)
    locator_module.write_locator(directory, locator_module.build_locator(
        store="json", execution_id=execution.execution_id, run_id="r1", created_at=execution.snapshot()["started_at"]))
    return execution, store


def seqs(directory: Path) -> list[int]:
    result = []
    for _first, path in json_store.list_segments(directory):
        result += [json.loads(line)["seq"] for line in path.read_text().splitlines()]
    return result


# -- the event journal: seq is assigned at the durable append, gap-free ----------------------------------------------------------
def test_seq_is_assigned_by_the_store_when_the_line_is_written_and_is_gap_free(tmp_path):
    execution, store = attached(tmp_path)
    for index in range(40):
        execution.error("stage", f"event {index}")
    store.close()
    assert seqs(tmp_path) == list(range(1, 42)) == [event["seq"] for event in execution.events()]
    for _first, path in json_store.list_segments(tmp_path):
        for line in path.read_text().splitlines():
            validate(json.loads(line), STRICT_EVENT)


def test_attaching_replays_the_events_already_recorded_with_the_same_seq(tmp_path):
    execution = StatusTracker(mode="stream", run_id="r")
    execution.set_run_id("r") if execution.run_id is None else None
    execution.activity("stream", "running")
    before = [event["seq"] for event in execution.events()]
    store = make_store(tmp_path)
    assert execution.attach_journal(store) and seqs(tmp_path) == before == [1, 2]
    execution.error("s", "m")
    assert seqs(tmp_path) == [1, 2, 3]
    store.close()


def test_a_journal_that_disagrees_about_seq_is_refused_and_the_run_goes_on(tmp_path, caplog):
    class Wrong:
        def append(self, make_event):
            return make_event(99)

    execution = StatusTracker(mode="stream")
    assert execution.attach_journal(Wrong()) is False
    execution.error("s", "still works")
    assert execution.snapshot()["history_incomplete"] is True
    assert [event["seq"] for event in execution.events()] == [1, 2]
    assert caplog.text.count("event journal failed") == 1


# -- atomic snapshot -----------------------------------------------------------------------------------------------------------------
def test_a_one_millisecond_polling_reader_never_sees_a_partial_status_file(tmp_path):
    execution, store = attached(tmp_path, snapshot_interval=0.0)
    for index in range(30):                                         # make the document large enough to be written in pieces
        incarnation = execution.role_started("node-loader", f"Label{index}", 0, container_id=f"c{index}")
    stop, failures, reads, revisions = threading.Event(), [], [0], []

    def poll():
        path = tmp_path / "status.json"
        while not stop.is_set():
            try:
                text = path.read_text()
            except FileNotFoundError:
                continue
            reads[0] += 1
            try:
                revisions.append(json.loads(text)["revision"])
            except ValueError as exc:
                failures.append(exc)
            time.sleep(0.001)

    thread = threading.Thread(target=poll)
    thread.start()
    try:
        for index in range(300):
            execution.update_type("node", "Label0", records_written=model.observed(index, "2026-11-03T00:00:00.000Z", "t"))
            store.write_snapshot()
    finally:
        time.sleep(0.05)
        stop.set()
        thread.join()
    store.close()
    assert not failures and reads[0] > 20 and revisions == sorted(revisions)
    assert incarnation and read_status(tmp_path)["revision"] == execution.snapshot()["revision"]


def test_snapshots_are_coalesced_to_the_interval_and_the_close_writes_the_last_one(tmp_path):
    execution, store = attached(tmp_path, snapshot_interval=0.3)
    store.write_snapshot()
    written = [(tmp_path / "status.json").stat().st_mtime_ns]
    for _ in range(50):
        execution.heartbeat()
    time.sleep(0.05)
    assert (tmp_path / "status.json").stat().st_mtime_ns == written[0]       # inside the interval: nothing new yet
    time.sleep(0.5)
    assert (tmp_path / "status.json").stat().st_mtime_ns != written[0]
    execution.finish(0)
    store.close()
    assert read_status(tmp_path)["state"] == "succeeded"                    # the terminal snapshot is always written


# -- rotation and gaps -----------------------------------------------------------------------------------------------------------------
def test_rotation_keeps_the_newest_segments_and_the_reader_reports_the_deleted_history_as_a_gap(tmp_path):
    execution, store = attached(tmp_path, max_file_bytes=900, max_files=3)
    for index in range(60):
        execution.error("s", f"message number {index}")
    store.close()
    segments = json_store.list_segments(tmp_path)
    assert len(segments) == 3 and all(path.stat().st_size <= 900 for _f, path in segments)
    kept = seqs(tmp_path)
    assert kept == list(range(kept[0], 62)) and kept[0] > 1                      # contiguous tail, oldest history gone
    events = list(iter_events(tmp_path))
    assert events[0]["type"] == "gap" and (events[0]["from_seq"], events[0]["to_seq"]) == (1, kept[0] - 1)
    assert events[0]["count"] == kept[0] - 1 and events[0]["seq"] == 1 and events[0]["reason"] == "history-rotated"
    assert [event["seq"] for event in events[1:]] == kept
    validate(events[0], STRICT_EVENT)
    assert list(iter_events(tmp_path, start_seq=kept[0]))[0]["seq"] == kept[0]      # asking from the kept range: no gap


def test_without_rotation_the_reader_yields_every_event_once_in_order(tmp_path):
    execution, store = attached(tmp_path)
    execution.finish(0)
    store.close()
    events = list(iter_events(tmp_path))
    assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
    assert events[0]["type"] == "execution.started" and events[-1]["type"] == "execution.finished"


def test_follow_continues_across_rotation_and_ends_at_the_end_of_the_execution(tmp_path):
    execution, store = attached(tmp_path, max_file_bytes=700, max_files=200)
    collected, errors = [], []

    def consume():
        try:
            collected.extend(iter_events(tmp_path, follow=True, poll_interval=0.005, idle_timeout=20))
        except Exception as exc:                                              # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=consume)
    thread.start()
    for index in range(150):
        execution.error("s", f"event {index}")
        if index % 25 == 0:
            time.sleep(0.02)
    execution.finish(0)
    thread.join(timeout=25)
    store.close()
    assert not thread.is_alive() and not errors
    assert len(json_store.list_segments(tmp_path)) > 5                         # it really rotated
    assert [event["seq"] for event in collected] == list(range(1, len(collected) + 1))
    assert collected[-1]["type"] == "execution.finished" and not [e for e in collected if e["type"] == "gap"]


def test_follow_that_falls_behind_rotation_sees_a_gap_not_silence(tmp_path):
    execution, store = attached(tmp_path, max_file_bytes=500, max_files=2)
    iterator = iter_events(tmp_path, follow=True, poll_interval=0.005, idle_timeout=1)
    first = next(iterator)
    assert first["seq"] == 1
    for index in range(80):
        execution.error("s", f"event {index}")
    execution.finish(0)
    store.close()
    rest = list(iterator)
    gaps = [event for event in rest if event["type"] == "gap"]
    assert len(gaps) == 1 and gaps[0]["from_seq"] == 2
    following = [event["seq"] for event in rest if event["type"] != "gap"]
    assert following == list(range(gaps[0]["to_seq"] + 1, following[-1] + 1))


def test_follow_ends_on_stop_and_on_idle_timeout(tmp_path):
    execution, store = attached(tmp_path)
    started = time.monotonic()
    assert [e["seq"] for e in iter_events(tmp_path, follow=True, poll_interval=0.01, idle_timeout=0.2)] == [1]
    assert 0.15 < time.monotonic() - started < 3
    calls = []
    assert [e["seq"] for e in iter_events(tmp_path, follow=True, poll_interval=0.01, stop=lambda: calls.append(1) or len(calls) > 3)] == [1]
    store.close()


# -- torn and damaged files ------------------------------------------------------------------------------------------------------------
def test_a_torn_final_line_is_not_an_event_until_it_is_complete(tmp_path):
    execution, store = attached(tmp_path)
    execution.error("s", "complete")
    store.close()
    (_first, path), = json_store.list_segments(tmp_path)
    line = json.dumps({**execution.events()[-1], "seq": 3, "type": "error", "stage": "s", "message": "late"}, separators=(",", ":"))
    with open(path, "ab") as handle:
        handle.write(line.encode()[:25])
    assert [e["seq"] for e in iter_events(tmp_path)] == [1, 2]
    with open(path, "ab") as handle:
        handle.write(line.encode()[25:] + b"\n")
    assert [e["seq"] for e in iter_events(tmp_path)] == [1, 2, 3]


def test_a_damaged_complete_line_is_an_error_and_a_seq_hole_is_reported_as_a_gap(tmp_path):
    execution, store = attached(tmp_path)
    for _ in range(3):
        execution.error("s", "m")
    store.close()
    (_first, path), = json_store.list_segments(tmp_path)
    lines = path.read_text().splitlines()
    path.write_text("\n".join([lines[0], lines[1], lines[3], ""]))              # seq 3 is missing
    events = list(iter_events(tmp_path))
    assert [e["type"] for e in events] == ["execution.started", "execution.state", "gap", "error"] or events[2]["type"] == "gap"
    assert (events[2]["from_seq"], events[2]["to_seq"], events[2]["reason"]) == (3, 3, "missing")
    path.write_text(lines[0] + "\n{not json}\n")
    with pytest.raises(StatusDataError):
        list(iter_events(tmp_path))


def test_documents_are_validated_against_the_published_schema_and_future_additions_pass(tmp_path):
    execution, store = attached(tmp_path)
    store.close()
    (_f, path), = json_store.list_segments(tmp_path)
    future = {"schema_version": 1, "execution_id": execution.execution_id, "seq": 2, "ts": "2026-11-03T00:00:00.000Z",
              "type": "lag.sampled", "lag": 3, "extra": True}
    with open(path, "ab") as handle:
        handle.write((json.dumps(future) + "\n").encode())
    assert [e["type"] for e in iter_events(tmp_path)][-1] == "lag.sampled"
    bad = {**future, "seq": 3, "execution_id": "nope"}
    with open(path, "ab") as handle:
        handle.write((json.dumps(bad) + "\n").encode())
    with pytest.raises(StatusDataError):
        list(iter_events(tmp_path))
    assert len(list(iter_events(tmp_path, validate=False))) == 3
    document = json.loads((tmp_path / "status.json").read_text())
    document["from_the_future"] = {"x": 1}
    (tmp_path / "status.json").write_text(json.dumps(document))
    assert read_status(tmp_path)["from_the_future"] == {"x": 1}
    document["state"] = "paused"
    (tmp_path / "status.json").write_text(json.dumps(document))
    with pytest.raises(StatusDataError):
        read_status(tmp_path)
    assert read_status(tmp_path, validate=False)["state"] == "paused"


# -- failure of the directory after start --------------------------------------------------------------------------------------------------
def test_a_directory_that_becomes_read_only_disables_status_with_one_log_line_and_never_raises(tmp_path, caplog):
    execution, store = attached(tmp_path)
    store.write_snapshot()
    tmp_path.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        if os.access(tmp_path, os.W_OK):
            pytest.skip("running as a user that ignores directory permissions")
        # a new segment cannot be created once rotation asks for one; force it with a tiny limit
        store._max_bytes = 10
        for index in range(5):
            execution.error("s", f"event {index}")               # must not raise
        execution.heartbeat()
        store.write_snapshot()
        execution.finish(1)
        store.close()
    finally:
        tmp_path.chmod(stat.S_IRWXU)
    snapshot = execution.snapshot()
    assert snapshot["history_incomplete"] is True and snapshot["state"] == "failed"
    assert [event["seq"] for event in execution.events()] == list(range(1, len(execution.events()) + 1))
    assert caplog.text.count("stage=status") == 1 and caplog.text.count("JSON status store stopped writing") == 1
    assert store.disabled is not None


def test_a_store_that_cannot_start_in_the_directory_is_a_log_line_not_a_failure(tmp_path, caplog):
    blocked = tmp_path / "file"
    blocked.write_text("not a directory")
    store = make_store(blocked)                                     # constructing never touches the disk
    execution = StatusTracker(mode="stream")
    assert execution.attach_journal(store) is False
    execution.error("s", "after")
    store.close()
    assert execution.snapshot()["history_incomplete"] is True
    assert caplog.text.count("stage=status") == 1 and caplog.text.count("stopped writing") == 1


# -- locator -----------------------------------------------------------------------------------------------------------------------------------
def test_the_locator_names_the_store_and_never_holds_credentials(tmp_path):
    password = "s3cr3t-pw"
    url = f"postgresql+psycopg://svc:{password}@db.example.com:5432/runs?sslmode=verify-full&password={password}&sslpassword=x"
    clean = locator_module.sanitize_db_url(url)
    assert clean == "postgresql+psycopg://db.example.com:5432/runs?sslmode=verify-full"
    assert locator_module.sanitize_db_url("sqlite:////tmp/status.db") == "sqlite:////tmp/status.db"
    assert locator_module.sanitize_db_url("postgresql://u@h/db") == "postgresql://h/db"
    execution_id = model.new_id()
    for store, extra in (("json", {}), ("sql", {"db_url": url, "schema": "graph_loader"})):
        directory = tmp_path / store
        directory.mkdir()
        locator_module.write_locator(directory, locator_module.build_locator(
            store=store, execution_id=execution_id, run_id=None, created_at="2026-11-03T00:00:00.000Z", **extra))
        text = (directory / "status-locator.json").read_text()
        assert password not in text and "svc" not in text
        loaded = locator_module.read_locator(directory)
        assert loaded["store"] == store and loaded["execution_id"] == execution_id and loaded["run_id"] is None
    assert json.loads((tmp_path / "sql" / "status-locator.json").read_text())["database"] == {
        "url": "postgresql+psycopg://db.example.com:5432/runs?sslmode=verify-full", "schema": "graph_loader"}
    with pytest.raises(ValueError):
        locator_module.build_locator(store="redis", execution_id=execution_id, run_id=None, created_at="x")
    (tmp_path / "bad").mkdir()
    (tmp_path / "bad" / "status-locator.json").write_text('{"kind": "other"}')
    for broken in (tmp_path / "bad", tmp_path / "missing"):
        with pytest.raises(locator_module.LocatorError):
            locator_module.read_locator(broken)


def test_location_resolution_by_attempt_directory_locator_file_base_and_execution_id(tmp_path):
    older, newer = tmp_path / "graph-loader-aaa", tmp_path / "graph-loader-bbb"
    ids = {}
    for directory, created in ((older, "2026-11-03T10:00:00.000Z"), (newer, "2026-11-03T11:00:00.000Z")):
        directory.mkdir()
        ids[directory] = model.new_id()
        locator_module.write_locator(directory, locator_module.build_locator(
            store="json", execution_id=ids[directory], run_id="same", created_at=created))
        (directory / "status.json").write_text(json.dumps({"execution_id": ids[directory]}))
    (tmp_path / "unrelated").mkdir()
    assert reader.resolve(tmp_path)[0] == newer                                   # newest by default
    assert reader.resolve(tmp_path, execution_id=ids[older])[0] == older
    assert reader.resolve(older)[0] == older and reader.resolve(older / "status-locator.json")[0] == older
    with pytest.raises(StatusNotFound):
        reader.resolve(older, execution_id=ids[newer])
    with pytest.raises(StatusNotFound):
        reader.resolve(tmp_path / "unrelated")
    with pytest.raises(StatusNotFound):
        reader.resolve(tmp_path / "does-not-exist")
    assert read_status(tmp_path, validate=False)["execution_id"] == ids[newer]


def test_a_store_without_a_reader_is_a_clear_error_and_a_registered_reader_is_used(tmp_path):
    directory = tmp_path / "graph-loader-sql"
    directory.mkdir()
    execution_id = model.new_id()
    locator_module.write_locator(directory, locator_module.build_locator(
        store="sql", execution_id=execution_id, run_id=None, created_at="2026-11-03T00:00:00.000Z",
        db_url="sqlite:///x.db", schema=None))
    reader._STORE_READERS.pop("sql", None)
    import sys
    import neo4j_graph_loader.status as status_package
    with patch.dict(sys.modules, {"neo4j_graph_loader.status.sql_store": None}), \
            patch.object(status_package, "sql_store", None, create=True):          # the optional SQL support is "not installed"
        with pytest.raises(StatusStoreError, match=r"\[sql\]"):
            read_status(directory)
        with pytest.raises(StatusStoreError):
            list(iter_events(directory))
    reader.register_store_reader("sql", read_status=lambda d, loc, validate: {"execution_id": loc["execution_id"]},
                                 iter_events=lambda d, loc, **kw: iter([{"seq": 1}]))
    try:
        assert read_status(directory) == {"execution_id": execution_id}
        assert list(iter_events(directory)) == [{"seq": 1}]
    finally:
        reader._STORE_READERS.pop("sql", None)


def test_settings_come_from_the_environment_and_bad_values_fall_back(monkeypatch):
    monkeypatch.setenv(json_store.FILE_MB_ENV, "2")
    monkeypatch.setenv(json_store.FILES_ENV, "5")
    monkeypatch.setenv(json_store.SNAPSHOT_INTERVAL_ENV, "0.25")
    assert json_store.settings_from_environment() == {"snapshot_interval": 0.25, "max_file_bytes": 2 * 1024 * 1024, "max_files": 5}
    for name, value in ((json_store.FILE_MB_ENV, "abc"), (json_store.FILES_ENV, "0"), (json_store.SNAPSHOT_INTERVAL_ENV, "-1")):
        monkeypatch.setenv(name, value)
    defaults = json_store.settings_from_environment()
    assert defaults == {"snapshot_interval": 1.0, "max_file_bytes": 8 * 1024 * 1024, "max_files": 8}


# -- the CLI: the attempt directory, the locator and the store exist before the schema is loaded ----------------------------------------------
def test_a_schema_load_failure_is_recorded_in_the_attempt_directory_before_anything_else(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "load_schema", lambda path: (_ for _ in ()).throw(cli.SchemaLoadError("not a schema")))
    schema = tmp_path / "broken.yaml"
    schema.write_text("nodes: [")
    base = tmp_path / "runs"
    assert cli.main(["start", "--mode", "stream", "--config", str(schema), "--run-dir", str(base)]) == 1
    (attempt,) = list(base.glob("graph-loader-*"))
    locator = locator_module.read_locator(attempt)
    status = read_status(attempt)
    assert locator["store"] == "json" and locator["execution_id"] == status["execution_id"]
    assert status["state"] == "failed" and status["outcome"]["exit_code"] == 1 and status["finished_at"]
    events = list(iter_events(attempt))
    assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
    assert events[0]["type"] == "execution.started" and events[-1]["type"] == "execution.finished"
    assert events[-1]["state"] == "failed"
    assert read_status(base)["execution_id"] == status["execution_id"]            # found from the base directory too
    assert stat.S_IMODE(attempt.stat().st_mode) == 0o700


def test_a_normal_start_keeps_the_same_attempt_directory_for_the_run_and_the_status(tmp_path):
    base = tmp_path / "runs"
    seen = {}

    def handle(args):
        seen["root"] = args.runtime.root
        args.execution.role_started("node-loader", "Person", 0, container_id="aa")
        return 0

    with patch("neo4j_graph_loader.cli.handle_start", side_effect=handle):
        assert cli.main(["start", "--mode", "stream", "--run-id", "my-run", "--run-dir", str(base)]) == 0
    (attempt,) = list(base.glob("graph-loader-*"))
    assert seen["root"] == attempt.resolve() and (attempt / "runtime_schema.yaml").exists()
    status = read_status(attempt)
    assert (status["state"], status["run_id"]) == ("succeeded", "my-run")
    assert locator_module.read_locator(attempt)["run_id"] == "my-run"             # the locator follows the run id
    types = [event["type"] for event in iter_events(attempt)]
    assert types[:3] == ["execution.started", "role.started", "execution.state"] or "role.started" in types
    assert types[-1] == "execution.finished"


def test_an_unusable_status_directory_is_one_log_line_and_the_load_goes_on(tmp_path, caplog):
    with patch("neo4j_graph_loader._runtime.create_attempt_root", side_effect=OSError("read-only file system")), \
            patch("neo4j_graph_loader.cli.handle_start", return_value=0):
        assert cli.main(["start", "--mode", "stream", "--run-dir", str(tmp_path / "runs")]) == 0
    assert caplog.text.count("status is disabled for this run") == 1


def test_a_locator_that_cannot_be_written_disables_status_without_failing_the_load(tmp_path, caplog):
    with patch("neo4j_graph_loader.status.locator.atomic_write", side_effect=OSError("disk full")), \
            patch("neo4j_graph_loader.cli.handle_start", return_value=0):
        assert cli.main(["start", "--mode", "stream", "--run-dir", str(tmp_path / "runs")]) == 0
    assert caplog.text.count("status is disabled for this run") == 1




def test_an_unknown_status_store_is_refused_like_any_bad_option_and_creates_no_state(tmp_path, monkeypatch, caplog):
    assert cli.main(["start", "--mode", "stream", "--status-store", "redis", "--run-dir", str(tmp_path / "runs")]) == 1
    assert "status store must be one of" in caplog.text
    assert not (tmp_path / "runs").exists()                                       # like every rejected flag
    monkeypatch.setenv(cli.STATUS_STORE_ENV, "nonsense")
    assert cli.main(["start", "--mode", "stream", "--run-dir", str(tmp_path / "runs2")]) == 1
    assert not (tmp_path / "runs2").exists()


def test_rejected_flags_and_a_missing_schema_file_still_create_no_state(tmp_path):
    runs = tmp_path / "runs"
    assert cli.main(["start", "--mode", "bulk", "--config", str(tmp_path / "missing"), "--run-dir", str(runs)]) == 1
    assert cli.main(["start", "--mode", "bulk", "--bulk-timeout-seconds", "0", "--run-dir", str(runs)]) == 1
    assert not runs.exists()


def test_other_commands_create_no_attempt_directory(tmp_path):
    with patch("neo4j_graph_loader.cli.handle_stop", return_value=0), patch("tempfile.mkdtemp") as mkdtemp:
        assert cli.main(["stop"]) == 0
    mkdtemp.assert_not_called()




def test_a_run_id_assigned_after_start_up_reaches_the_locator_and_the_status(tmp_path):
    base = tmp_path / "runs"

    def handle(args):
        assert locator_module.read_locator(args.runtime.root)["run_id"] is None       # not known yet
        args.execution.set_run_id("generated-run")
        assert locator_module.read_locator(args.runtime.root)["run_id"] == "generated-run"
        return 0

    with patch("neo4j_graph_loader.cli.handle_start", side_effect=handle):
        assert cli.main(["start", "--mode", "bulk", "--run-dir", str(base)]) == 0
    (attempt,) = list(base.glob("graph-loader-*"))
    assert read_status(attempt)["run_id"] == "generated-run"
    assert "execution.run_id" in [event["type"] for event in iter_events(attempt)]


@pytest.mark.parametrize("url, clean", [
    ("sqlite:////tmp/status.db", "sqlite:////tmp/status.db"), ("sqlite:///relative.db", "sqlite:///relative.db"),
    ("sqlite://", "sqlite://"), ("postgresql://u:p@h:5432/db", "postgresql://h:5432/db"),
    ("postgresql+psycopg://u@h/db?sslmode=require&password=x", "postgresql+psycopg://h/db?sslmode=require"),
])
def test_sanitized_database_urls_keep_their_double_slash_on_every_python(url, clean, monkeypatch):
    """CPython before 3.12.4 drops the '//' of an empty netloc in urlunsplit (sqlite:////abs -> sqlite://abs); emulate it."""
    import urllib.parse

    def old_urlunsplit(parts):
        scheme, netloc, path, query, fragment = parts
        text = (netloc and "//" + netloc) + path if netloc or not path.startswith("//") else path
        return f"{scheme}:{text}" + (f"?{query}" if query else "")
    monkeypatch.setattr(urllib.parse, "urlunsplit", old_urlunsplit)
    monkeypatch.setattr(locator_module, "urlunsplit", old_urlunsplit, raising=False)
    assert locator_module.sanitize_db_url(url) == clean
