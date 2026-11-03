"""E15.F1.S1: identity and status model, the status and event JSON Schemas, and the CLI wiring.

Everything here is offline and in memory. ``jsonschema`` (a development tool, never a dependency) cross-checks the
in-package validator when it is installed.
"""
from __future__ import annotations

import copy
import json
import re
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from neo4j_graph_loader import cli
from neo4j_graph_loader.orchestrator.role_launch import RoleLauncher
from neo4j_graph_loader.status import InvalidTransition, StatusTracker, ValidationError, model, redact, schemas, validate

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "docs" / "status-schemas"
STATUS, EVENT = schemas.STRICT_STATUS_SCHEMA, schemas.STRICT_EVENT_SCHEMA        # closed: tests of our own writer
PUBLISHED_STATUS, PUBLISHED_EVENT = schemas.STATUS_SCHEMA, schemas.EVENT_SCHEMA


class Clock:
    def __init__(self):
        self.now = 1_790_000_000.0

    def __call__(self) -> float:
        self.now += 0.5
        return self.now


def tracker(**kwargs) -> StatusTracker:
    kwargs.setdefault("clock", Clock())
    return StatusTracker(host="test-host", pid=4242, package_version="0.1.0a2", **kwargs)


def check(execution: StatusTracker) -> None:
    validate(execution.snapshot(), STATUS)
    for event in execution.events():
        validate(event, EVENT)


def planned_bulk() -> StatusTracker:
    """A planned bulk run: planning, nodes and edges overlap; two phases; restarted writer; progress fields."""
    execution = tracker(run_id="load-1", mode="bulk", labels={"team": "graph"})
    for name in ("planning", "nodes", "edges", "draining"):
        execution.declare_activity(name)
    execution.activity("planning", "running")
    execution.activity("nodes", "running")
    execution.activity("edges", "running")
    execution.activity("planning", "succeeded")
    execution.phase_started("load-1-bulk-0-ab12cd34", "bulk", 0, "KNOWS")
    execution.phase_started("load-1-isolated-1-ef56ab78", "isolated", 1, "WORKS_AT")
    first = execution.role_started("planned-edge-writer", "-", 0, container_id="c0ffee")
    execution.role_started("node-loader", "Person", 0, container_id="deadbeef")
    execution.role_exited(first, exit_code=137, exit_reason="signal", error={"type": "Killed", "message": "SIGKILL"})
    execution.role_started("planned-edge-writer", "-", 0, container_id="c0ffe1")
    at = "2026-11-03T12:00:00.000Z"
    execution.update_type("node", "Person", records_written=model.observed(1200, at, "node-snapshot"),
                          ready=model.observed(True, at, "node-monitor"), lag=None)
    execution.update_type("edge", "KNOWS", records_written=model.observed(9000, at, "edge-snapshot", lower_bound=True),
                          plan_chunks_done=model.observed(3, at, "ledger"), plan_chunks_total=model.observed(8, at, "ledger"),
                          rejected=model.observed(2, at, "planner"))
    execution.update_totals(records_written=model.observed(10200, at, "snapshots"))
    execution.activity("nodes", "succeeded")
    execution.activity("draining", "running")
    execution.phase_finished("load-1-bulk-0-ab12cd34", "succeeded")
    return execution


# -- schemas ---------------------------------------------------------------------------------------------------------
def test_the_status_schema_is_versioned_and_the_strict_variant_is_closed():
    assert STATUS["properties"]["schema_version"] == {"const": 1}
    assert STATUS["additionalProperties"] is False
    assert "additionalProperties" not in json.dumps(PUBLISHED_STATUS).replace('"additionalProperties": {"type": "string"}', "")
    assert EVENT["$defs"]["gap"]["properties"]["schema_version"] == {"const": 1}


def test_generated_schema_files_equal_the_python_source():
    for name, schema in schemas.schemas().items():
        assert json.loads((SCHEMA_DIR / name).read_text()) == schema, name
        assert (SCHEMA_DIR / name).read_text() == schemas.render(schema)


def test_a_fresh_execution_validates():
    execution = tracker(mode="stream")
    check(execution)
    snapshot = execution.snapshot()
    assert snapshot["state"] == "starting" and snapshot["run_id"] is None and snapshot["outcome"] is None


def test_a_planned_bulk_run_with_overlapping_activities_validates():
    execution = planned_bulk()
    check(execution)
    activities = execution.snapshot()["activities"]
    running = {name for name, entry in activities.items() if entry["state"] == "running"}
    assert running == {"edges", "draining"}
    assert activities["planning"]["state"] == "succeeded" and activities["nodes"]["state"] == "succeeded"
    # planning, nodes and edges were running at the same time at some point
    events = [event for event in execution.events() if event["type"] == "activity.state"]
    state = {}
    overlap = False
    for event in events:
        state[event["activity"]] = event["to"]
        overlap |= {name for name, value in state.items() if value == "running"} >= {"planning", "nodes", "edges"}
    assert overlap


def test_stream_activity_follows_the_first_role():
    execution = tracker(mode="stream")
    execution.role_started("node-loader", "Person", 0, container_id="aa")
    assert execution.snapshot()["activities"]["stream"]["state"] == "running"
    assert execution.snapshot()["state"] == "running"
    execution.finish(0)
    snapshot = execution.snapshot()
    assert snapshot["activities"]["stream"]["state"] == "succeeded" and snapshot["activities"]["stream"]["finished_at"]
    check(execution)


def _everything() -> list[dict]:
    execution = planned_bulk()
    execution.error("edge-write", "boom")
    execution.role_oom(execution.snapshot()["roles"][1]["incarnation"])
    execution.group_oom(3, 4)
    execution.finish(1, reason="edge loader failed")
    events = execution.events()
    events.append({**events[0], "seq": 99, "type": "gap", "from_seq": 5, "to_seq": 9, "count": 5, "reason": "outbox-overflow"})
    for key in ("run_id", "mode", "runtime", "host", "pid", "versions"):
        events[-1].pop(key)
    return events


def test_every_event_type_validates_and_none_is_unused():
    events = _everything()
    for event in events:
        validate(event, EVENT)
    assert {event["type"] for event in events} == set(schemas.EVENT_TYPES) - {"execution.run_id"}


def test_the_run_id_event_validates():
    execution = tracker(mode="stream")
    execution.set_run_id("r-1")
    check(execution)
    assert execution.events()[-1]["type"] == "execution.run_id"


@pytest.mark.parametrize("mutate, reason", [
    (lambda d: d.update(extra=1), "member is not allowed"),
    (lambda d: d.update(schema_version=2), "must equal"),
    (lambda d: d.update(execution_id="not-an-id"), "must match"),
    (lambda d: d.update(state="paused"), "one of"),
    (lambda d: d.update(started_at="yesterday"), "must match"),
    (lambda d: d.update(pid=0), "at least"),
    (lambda d: d.update(revision=0), "at least"),
    (lambda d: d.pop("heartbeat_at"), "missing required"),
    (lambda d: d["types"][0].update(records_written={"value": 1, "source": "x"}), "missing required"),
    (lambda d: d["types"][0].update(records_written={"value": -1, "observed_at": "2026-11-03T12:00:00.000Z", "source": "x"}), "at least"),
    (lambda d: d["types"][0].update(records_written={"value": True, "observed_at": "2026-11-03T12:00:00.000Z", "source": "x"}), "type"),
    (lambda d: d["types"][0].update(records_written=0), "type object"),
    (lambda d: d["types"][0].update(ready={"value": 1, "observed_at": "2026-11-03T12:00:00.000Z", "source": "x"}), "type"),
    (lambda d: d["types"][0].update(bogus_field=None), "not allowed"),
    (lambda d: d["roles"][0].update(incarnation="short"), "must match"),
    (lambda d: d["roles"][0].update(state="zombie"), "one of"),
    (lambda d: d["phases"][0].update(state="pending"), "one of"),
    (lambda d: d["activities"].update(compiling={"state": "running"}), "not allowed"),
    (lambda d: d["activities"]["nodes"].update(state="done"), "one of"),
    (lambda d: d["telemetry"].update(available="yes"), "type"),
    (lambda d: d["labels"].update(team=3), "type"),
    (lambda d: d.update(outcome={"exit_code": "1", "reason": None, "error": None}), "type integer"),
])
def test_invalid_status_documents_are_rejected(mutate, reason):
    document = planned_bulk().snapshot()
    mutate(document)
    with pytest.raises(ValidationError) as caught:
        validate(document, STATUS)
    assert reason in str(caught.value)


@pytest.mark.parametrize("mutate", [
    lambda e: e.update(seq=0),
    lambda e: e.update(seq="1"),
    lambda e: e.update(type="execution.exploded"),
    lambda e: e.pop("ts"),
    lambda e: e.update(extra=True),
    lambda e: e.update(execution_id="X" * 32),
])
def test_invalid_events_are_rejected(mutate):
    event = tracker(mode="stream").events()[0]
    mutate(event)
    with pytest.raises(ValidationError):
        validate(event, EVENT)


def test_an_event_of_a_known_type_with_the_fields_of_another_is_rejected():
    event = tracker(mode="stream").events()[0]
    event["type"] = "execution.finished"
    with pytest.raises(ValidationError):
        validate(event, EVENT)


def test_the_validator_agrees_with_jsonschema():
    jsonschema = pytest.importorskip("jsonschema")
    for schema in (STATUS, EVENT):
        jsonschema.Draft202012Validator.check_schema(schema)
    documents = [(STATUS, planned_bulk().snapshot()), (STATUS, tracker().snapshot())]
    documents += [(EVENT, event) for event in _everything()]
    broken = []
    for schema, document in list(documents):
        for key in list(document)[:12]:
            copy_ = copy.deepcopy(document)
            copy_.pop(key)
            broken.append((schema, copy_))
            copy_ = copy.deepcopy(document)
            copy_[key] = [] if not isinstance(document[key], list) else None
            broken.append((schema, copy_))
            copy_ = copy.deepcopy(document)
            copy_["surprise"] = 1
            broken.append((schema, copy_))
    for schema, document in documents + broken:
        reference = jsonschema.Draft202012Validator(schema)
        expected = reference.is_valid(document)
        try:
            validate(document, schema)
            actual = True
        except ValidationError:
            actual = False
        assert actual == expected, (document, [error.message for error in reference.iter_errors(document)][:3])
    assert any(not jsonschema.Draft202012Validator(s).is_valid(d) for s, d in broken)


# -- identity ----------------------------------------------------------------------------------------------------------
def test_identifiers_are_unique_32_hex_digit_uuids():
    ids = {tracker().execution_id for _ in range(50)}
    assert len(ids) == 50 and all(re.fullmatch(r"[0-9a-f]{32}", value) for value in ids)
    assert model.is_id(model.new_id()) and not model.is_id("ABC") and not model.is_id(None)


def test_the_run_id_is_assigned_once():
    execution = tracker(mode="bulk")
    execution.set_run_id("abc")
    execution.set_run_id("abc")
    with pytest.raises(ValueError):
        execution.set_run_id("other")
    with pytest.raises(ValueError):
        execution.set_run_id("bad id")
    assert [event["type"] for event in execution.events()].count("execution.run_id") == 1


def test_an_explicit_run_id_is_shared_by_two_executions_that_stay_separate():
    one, two = tracker(run_id="same", mode="bulk"), tracker(run_id="same", mode="bulk")
    assert one.run_id == two.run_id == "same" and one.execution_id != two.execution_id
    assert one.events()[0]["run_id"] == "same"


def test_construction_rejects_unknown_vocabulary():
    for kwargs in ({"mode": "batch"}, {"runtime": "k8s"}, {"run_id": "-bad"}):
        with pytest.raises(ValueError):
            tracker(**kwargs)


def test_role_incarnations_distinguish_restarts():
    execution = tracker(mode="stream")
    first = execution.role_started("node-loader", "Person", 0, container_id="a")
    execution.role_exited(first, exit_code=1)
    second = execution.role_started("node-loader", "Person", 0, container_id="b")
    other = execution.role_started("node-loader", "Person", 1, container_id="c")
    roles = {role["incarnation"]: role for role in execution.snapshot()["roles"]}
    assert len({first, second, other}) == 3
    assert [roles[i]["restart_count"] for i in (first, second, other)] == [0, 1, 0]
    assert roles[first]["state"] == "failed" and roles[second]["state"] == "running"
    assert execution.role_started("node-loader", "Person", 1, incarnation=other) == other      # idempotent by id
    with pytest.raises(ValueError):
        execution.role_started("node-loader", "Person", 2, incarnation="nope")
    check(execution)


def test_phases_are_recorded_as_children_of_the_execution():
    execution = tracker(run_id="r", mode="bulk")
    assert execution.phase_started("r-bulk-1", "bulk", 0, "KNOWS") is True
    assert execution.phase_started("r-bulk-1", "bulk", 0, "KNOWS") is False
    execution.phase_started("r-topic-2", "topic", 1, "WORKS_AT")
    execution.phase_finished("r-bulk-1", "failed")
    execution.phase_finished("r-bulk-1", "succeeded")          # already finished: ignored
    phases = {phase["phase_id"]: phase for phase in execution.snapshot()["phases"]}
    assert phases["r-bulk-1"]["state"] == "failed" and phases["r-topic-2"]["state"] == "running"
    assert phases["r-bulk-1"]["finished_at"] and phases["r-topic-2"]["finished_at"] is None
    with pytest.raises(ValueError):
        execution.phase_finished("r-topic-2", "running")
    with pytest.raises(ValueError):
        execution.phase_started("bad id", "bulk")
    check(execution)


# -- state machine, events, revisions -------------------------------------------------------------------------------
def test_transitions_follow_the_table_and_terminal_states_are_final():
    execution = tracker(mode="stream")
    with pytest.raises(InvalidTransition):
        execution.transition("starting-again")
    execution.transition("running")
    execution.transition("stopping", "operator")
    execution.transition("stopped")
    for state in model.EXECUTION_STATES:
        if state != "stopped":
            with pytest.raises(InvalidTransition):
                execution.transition(state)
    execution.transition("stopped")            # same state: no-op
    assert execution.snapshot()["finished_at"]
    check(execution)


def test_sequence_numbers_are_gap_free_and_revisions_strictly_increase():
    revisions: list[int] = []
    execution = tracker(mode="bulk")
    execution.subscribe(on_snapshot=lambda snapshot: revisions.append(snapshot["revision"]))
    planned = planned_bulk()
    for run in (execution, planned):
        run.heartbeat()
        run.finish(0)
    assert [event["seq"] for event in planned.events()] == list(range(1, len(planned.events()) + 1))
    assert revisions == sorted(set(revisions)) and revisions[0] >= 1
    assert planned.snapshot()["revision"] > len(planned.events()) - 1


def test_concurrent_mutations_keep_seq_gap_free():
    execution = tracker(mode="stream")

    def worker(index: int) -> None:
        for step in range(25):
            execution.role_started("node-loader", f"L{index}", step, container_id=f"{index}-{step}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    sequence = [event["seq"] for event in execution.events()]
    assert sequence == list(range(1, len(sequence) + 1))
    check(execution)


def test_subscribers_get_the_history_then_live_changes_and_cannot_break_the_load(caplog):
    execution = tracker(mode="bulk")
    execution.set_run_id("r")
    seen: list[str] = []
    execution.subscribe(on_event=lambda event: seen.append(event["type"]))
    execution.activity("nodes", "running")

    def broken(_payload):
        raise RuntimeError("disk full password=hunter2")

    execution.subscribe(on_snapshot=broken)
    execution.activity("nodes", "succeeded")
    execution.heartbeat()
    assert seen == ["execution.started", "execution.run_id", "activity.state", "activity.state"]
    assert caplog.text.count("subscriber failed") == 1 and "hunter2" not in caplog.text


# -- outcomes -----------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("code, stop, error, expected", [
    (0, False, None, "succeeded"), (1, False, None, "failed"), (0, True, None, "stopped"), (1, True, None, "stopped"),
    (0, False, RuntimeError("x"), "failed"), (0, True, RuntimeError("x"), "failed"),
])
def test_finish_decides_the_terminal_state(code, stop, error, expected):
    execution = tracker(mode="stream")
    execution.role_started("node-loader", "Person", 0, container_id="a")
    execution.phase_started("p-1", "bulk")
    if stop:
        execution.request_stop()
    assert execution.finish(code, error=error) == expected
    snapshot = execution.snapshot()
    assert snapshot["state"] == expected and snapshot["outcome"]["exit_code"] == code
    assert snapshot["finished_at"] and snapshot["phases"][0]["state"] == (expected if expected != "succeeded" else "succeeded")
    assert snapshot["roles"][0]["state"] == "exited" and snapshot["roles"][0]["exit_reason"] == "execution-finished"
    assert all(entry["state"] != "running" for entry in snapshot["activities"].values())
    if stop:
        assert ("stopping" in [event.get("to") for event in execution.events()]) or snapshot["outcome"]["reason"] == "stop requested"
    check(execution)
    assert execution.finish(0) == expected                       # idempotent; later calls change nothing
    revision = execution.snapshot()["revision"]
    execution.heartbeat()
    execution.role_started("node-loader", "Late", 0, container_id="z")
    assert execution.snapshot()["revision"] == revision


def test_a_start_up_failure_before_any_role_is_a_terminal_failed_status():
    execution = tracker(mode="stream")
    execution.finish(1, error=ValueError("bad schema"))
    snapshot = execution.snapshot()
    assert snapshot["state"] == "failed" and snapshot["roles"] == [] and snapshot["outcome"]["error"]["type"] == "ValueError"
    assert [event["type"] for event in execution.events()][-2:] == ["execution.state", "execution.finished"]
    check(execution)


def test_credentials_never_enter_a_status_document():
    secrets = ["hunter2", "s3cr3t-token", "pa55w0rd"]
    execution = tracker(mode="stream")
    execution.error("connect", "failed bolt://neo4j:hunter2@db:7687 with token=s3cr3t-token")
    role = execution.role_started("node-loader", "Person", 0, container_id="a")
    execution.role_exited(role, exit_code=1, error=RuntimeError("PASSWORD: pa55w0rd rejected"))
    execution.finish(1, error=RuntimeError("postgresql://svc:hunter2@pg/db unreachable"))
    dump = json.dumps([execution.snapshot(), execution.events()])
    assert not any(secret in dump for secret in secrets)
    assert "bolt://neo4j:***@db:7687" in dump
    assert redact("nothing to hide") == "nothing to hide"


def test_telemetry_fields_are_checked_and_a_missing_value_is_null_not_zero():
    execution = tracker(mode="stream")
    with pytest.raises(ValueError):
        execution.update_type("node", "Person", records_writen=None)
    with pytest.raises(ValueError):
        execution.update_type("relationship", "KNOWS")
    execution.update_type("node", "Person", lag=None)
    execution.set_telemetry(False, "timing directory is read-only")
    snapshot = execution.snapshot()
    assert snapshot["types"] == [{"kind": "node", "name": "Person", "lag": None}]
    assert snapshot["telemetry"] == {"available": False, "reason": "timing directory is read-only"}
    check(execution)


# -- the CLI --------------------------------------------------------------------------------------------------------------
@pytest.fixture
def captured(monkeypatch):
    """Every StatusTracker the CLI creates, and what the tracker looked like when the schema was first loaded."""
    created: list[StatusTracker] = []

    class Recording(StatusTracker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    monkeypatch.setattr(cli, "StatusTracker", Recording)
    return created


def test_the_execution_exists_before_the_schema_is_loaded_and_a_load_failure_is_a_terminal_failed_status(
        captured, tmp_path, monkeypatch):
    seen = []

    def failing_load(_path):
        seen.append((len(captured), captured[0].snapshot()["state"]))
        raise cli.SchemaLoadError("not a schema")

    monkeypatch.setattr(cli, "load_schema", failing_load)
    schema = tmp_path / "broken.yaml"
    schema.write_text("nodes: [", encoding="utf-8")
    assert cli.main(["start", "--mode", "stream", "--config", str(schema), "--run-dir", str(tmp_path)]) == 1
    assert seen == [(1, "starting")]
    snapshot = captured[0].snapshot()
    assert snapshot["state"] == "failed" and snapshot["outcome"]["exit_code"] == 1
    assert snapshot["finished_at"] and snapshot["mode"] == "stream" and snapshot["runtime"] == "docker"
    check(captured[0])


def test_a_missing_config_file_is_a_terminal_failed_status(captured, tmp_path):
    assert cli.main(["start", "--mode", "bulk", "--config", str(tmp_path / "absent.yaml")]) == 1
    assert captured[0].snapshot()["state"] == "failed"


def test_an_invalid_run_id_still_gets_a_failed_execution_with_no_run_id(captured):
    assert cli.main(["start", "--mode", "stream", "--run-id", "bad id!"]) == 1
    snapshot = captured[0].snapshot()
    assert snapshot["state"] == "failed" and snapshot["run_id"] is None


def test_an_explicit_run_id_is_on_the_execution_from_the_start(captured, tmp_path):
    with patch("neo4j_graph_loader.cli.handle_start", return_value=0):
        assert cli.main(["start", "--mode", "stream", "--run-id", "my-run", "--run-dir", str(tmp_path)]) == 0
    snapshot = captured[0].snapshot()
    assert snapshot["run_id"] == "my-run" and snapshot["state"] == "succeeded" and snapshot["outcome"]["exit_code"] == 0
    check(captured[0])


@pytest.mark.parametrize("effect, state, code", [
    (RuntimeError("launch exploded"), "failed", 1),
    (KeyboardInterrupt(), "stopped", 130),
    (SystemExit(3), "failed", 3),
])
def test_an_exception_out_of_start_still_ends_the_execution(captured, tmp_path, effect, state, code):
    with patch("neo4j_graph_loader.cli.handle_start", side_effect=effect), pytest.raises(type(effect)):
        cli.main(["start", "--mode", "stream", "--run-dir", str(tmp_path)])
    snapshot = captured[0].snapshot()
    assert snapshot["state"] == state and snapshot["outcome"]["exit_code"] == code
    check(captured[0])


def test_a_nonzero_exit_of_start_is_failed(captured, tmp_path):
    with patch("neo4j_graph_loader.cli.handle_start", return_value=1):
        assert cli.main(["start", "--mode", "stream", "--run-dir", str(tmp_path)]) == 1
    assert captured[0].snapshot()["state"] == "failed"


def test_other_commands_create_no_execution(captured):
    with patch("neo4j_graph_loader.cli.handle_stop", return_value=0):
        assert cli.main(["stop"]) == 0
    assert captured == []


def test_a_signal_during_start_marks_the_execution_stop_requested(captured, tmp_path):
    import signal

    def run(args):
        cleanups: list = []
        shutdown = cli._install_shutdown_handlers(cleanups, cli._execution_of(args))
        try:
            signal.raise_signal(signal.SIGTERM)
            assert shutdown.is_set()
            assert cli._execution_of(args).stop_requested
        finally:
            for cleanup in cleanups:
                cleanup()                    # restores the handlers and clears the module-level shutdown event
        return 0

    previous = signal.getsignal(signal.SIGTERM)
    try:
        with patch("neo4j_graph_loader.cli.handle_start", side_effect=run):
            assert cli.main(["start", "--mode", "stream", "--run-dir", str(tmp_path)]) == 0
    finally:
        signal.signal(signal.SIGTERM, previous)
    snapshot = captured[0].snapshot()
    assert snapshot["state"] == "stopped" and snapshot["outcome"]["reason"] == "stop requested"
    assert "stopping" in [event.get("to") for event in captured[0].events()]


def test_phases_run_through_record_phase():
    class Args:
        pass

    args = Args()
    args.execution = tracker(run_id="r", mode="bulk")
    assert cli._record_phase(args, "r-isolated-0", "isolated", 0, "KNOWS", lambda: 0) == 0
    assert cli._record_phase(args, "r-shared-0", "shared", 0, None, lambda: 1) == 1
    with pytest.raises(RuntimeError):
        cli._record_phase(args, "r-topic-0", "topic", 0, "X", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    phases = {phase["phase_id"]: phase["state"] for phase in args.execution.snapshot()["phases"]}
    assert phases == {"r-isolated-0": "succeeded", "r-shared-0": "failed", "r-topic-0": "failed"}
    assert cli._record_phase(object(), "x", "isolated", 0, None, lambda: 7) == 7              # no execution: just runs


# -- role launch ---------------------------------------------------------------------------------------------------------------
class DockerLike:
    id = "9f8e7d6c5b4a" * 5 + "abcd"


class ProcessLike:
    def __init__(self, incarnation: str, pid: int):
        self.id, self.pid = incarnation, pid


def test_launched_roles_are_recorded_for_both_runtimes():
    launcher = RoleLauncher()
    launcher.status = execution = tracker(mode="bulk")
    incarnation = model.new_id()
    assert launcher._note_role_started(DockerLike(), {"component": "node-loader", "node_label": "Person", "replica_id": "2"})
    assert launcher._note_role_started(ProcessLike(incarnation, 777), {"component": "planned-edge-writer", "writer_id": "w3"})
    assert launcher._note_role_started(DockerLike(), {"component": "retry-consumer", "replica_id": "0"})
    roles = execution.snapshot()["roles"]
    assert [(r["role"], r["name"], r["replica"]) for r in roles] == [
        ("node-loader", "Person", 2), ("planned-edge-writer", "-", 3), ("retry-consumer", "-", 0)]
    assert roles[0]["container_id"] == DockerLike.id and roles[0]["pid"] is None and model.is_id(roles[0]["incarnation"])
    assert roles[1]["incarnation"] == incarnation and roles[1]["pid"] == 777 and roles[1]["container_id"] is None
    assert execution.snapshot()["state"] == "running"
    check(execution)


def test_role_recording_never_fails_a_launch(caplog):
    launcher = RoleLauncher()
    assert launcher._note_role_started("handle", {"component": "x"}) == "handle"        # no tracker
    launcher.status = tracker(mode="bulk")
    launcher.status.finish(1)
    assert launcher._note_role_started("handle", None) == "handle"
    launcher.status = object()                                                           # a broken tracker
    assert launcher._note_role_started("handle", {"component": "x"}) == "handle"
    assert "could not record a role start" in caplog.text


def test_a_docker_launch_records_each_replica_with_its_container_id(tmp_path):
    from unittest.mock import MagicMock
    from docker_support import configure_images
    from neo4j_graph_loader.orchestrator.docker_service import DockerService

    client = configure_images(MagicMock())
    client.containers.run.side_effect = lambda **kwargs: MagicMock(id="c" * 64, labels=kwargs["labels"])
    service = DockerService(client=client)
    service.status = execution = tracker(mode="stream")
    service.run_node_loader(node_label="Person", topic="t", mode="stream", config_path="c.yaml", replicas=2,
                            network="n", rejection_dir=str(tmp_path))
    roles = execution.snapshot()["roles"]
    assert sorted((r["role"], r["name"], r["replica"]) for r in roles) == [("node-loader", "Person", 0), ("node-loader", "Person", 1)]
    assert all(r["container_id"] == "c" * 64 and r["pid"] is None and model.is_id(r["incarnation"]) for r in roles)
    assert len({r["incarnation"] for r in roles}) == 2
    check(execution)


def test_a_process_launch_reuses_the_registry_incarnation(tmp_path):
    from neo4j_graph_loader import _runtime
    from neo4j_graph_loader.orchestrator.process_service import ProcessService

    root = tmp_path / "attempt"
    for name in ("rejections", "plans", "spill", "timings"):
        (root / name).mkdir(parents=True)
    schema = root / "runtime_schema.yaml"
    schema.write_text("x: 1\n")
    paths = _runtime.RunPaths(root, schema, root / "rejections", root / "plans", root / "spill", root / "timings")
    service = ProcessService(paths, ready_timeout=60)
    service.status = execution = tracker(mode="bulk")
    try:
        handle = service.run_bucket_lease_server(run_id="run-s1", bucket_count=16, network="n", keyed_labels=[])
        (role,) = execution.snapshot()["roles"]
        assert role["incarnation"] == handle.id and role["pid"] == handle.pid and role["container_id"] is None
        assert role["role"] == "bucket-lease-server"
        registry = [json.loads(line.split(" ", 1)[1]) for line in (root / "registry.jsonl").read_text().splitlines()]
        assert any(record.get("id") == role["incarnation"] and record["kind"] == "started" for record in registry)
    finally:
        service.close()
    check(execution)


def test_the_bulk_scheduler_records_its_phases_and_their_outcomes():
    """A precedence type (three topic sub-stages) next to a plain type under the completion-driven scheduler."""
    from unittest.mock import MagicMock
    from test_precedence_substages import MARKS, TOPICS, _args, _schema

    works = next(e for e in _schema().edges if e.type == "WORKS_AT")
    other = works.model_copy(update={"type": "EMPLOYED_AT", "topics": ["other-topic"], "topic_conflict": None})
    docker = MagicMock()
    docker.run_edge_loader.side_effect = lambda **kwargs: [MagicMock() for _ in range(kwargs["replicas"])]

    def monitor_factory(edges, run_id, expected, tracked, **kwargs):
        monitor = MagicMock()
        monitor.capture_boundary.return_value = {(edges[0].type, t, p): 10 for t in edges[0].topics for p in (0, 1)}
        return monitor

    args = _args(bulk_edge_overlap="shared", max_concurrent_bulk_edges=0)
    args.execution = execution = tracker(run_id="parent", mode="bulk")
    with patch.object(cli, "RelationshipBulkMonitor", MagicMock(side_effect=monitor_factory)), \
            patch.object(cli, "_topic_watermarks", side_effect=lambda t: {k: v for k, v in MARKS.items() if k[0] in t}), \
            patch.object(cli, "_group_commits", return_value={}), \
            patch.object(cli, "_stop_exact_containers", return_value=[]), \
            patch.object(cli, "_remove_exact_containers"):
        rc = cli._run_completion_driven_bulk_edges(
            (works, other), docker, args, parent_run_id="parent", consumer_group_prefix="g",
            ready_labels={works.nodes.source, works.nodes.target},
        )
    assert rc == 0
    phases = execution.snapshot()["phases"]
    kinds = sorted((phase["kind"], phase["edge_type"]) for phase in phases)
    assert kinds == sorted([("bulk", "WORKS_AT"), ("bulk", "EMPLOYED_AT")] + [("topic", "WORKS_AT")] * len(TOPICS))
    assert all(phase["state"] == "succeeded" and phase["finished_at"] for phase in phases)
    assert len({phase["phase_id"] for phase in phases}) == len(phases)
    assert all(phase["phase_id"] == "parent" or phase["phase_id"].startswith("parent") for phase in phases)
    check(execution)


def _orchestration_edges():
    from test_cli import _rotating_edge
    return (_rotating_edge("WORKS_AT", "works", "Person", "Company"), _rotating_edge("KNOWS", "knows", "Person", "Person"))


def test_bulk_orchestration_records_the_isolated_phase_with_its_outcome():
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    works, knows = _orchestration_edges()
    schema = SimpleNamespace(edges=[knows, works], loading=SimpleNamespace(consumer_group_id="loader"))
    args = SimpleNamespace(mode="bulk", config="c.yaml", network="n", bulk_timeout_seconds=1, edge_classic=True,
                           execution=tracker(run_id="parent-run", mode="bulk"))
    for result, expected in ((0, "succeeded"), (1, "failed")):
        args.execution = tracker(run_id="parent-run", mode="bulk")
        with patch.object(cli, "_run_completion_driven_bulk_edges", return_value=0), \
                patch.object(cli, "_run_isolated_relationship_bulk_phase", return_value=result):
            assert cli._run_relationship_orchestration(schema, MagicMock(), args, parent_run_id="parent-run") == result
        (phase,) = args.execution.snapshot()["phases"]
        assert (phase["kind"], phase["edge_type"], phase["state"]) == ("isolated", "KNOWS", expected)
        assert phase["phase_id"] == cli._derive_relationship_phase_run_id(
            "parent-run", phase_kind="isolated", phase_index=phase["index"], edge_type="KNOWS")
        check(args.execution)


def test_stream_orchestration_records_the_isolated_phase():
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    _works, knows = _orchestration_edges()
    schema = SimpleNamespace(edges=[knows], loading=SimpleNamespace(consumer_group_id="loader"))
    args = SimpleNamespace(mode="stream", config="c.yaml", network="n", bulk_timeout_seconds=1,
                           execution=tracker(run_id="stream-parent", mode="stream"))
    with patch.object(cli, "_run_isolated_relationship_stream_phase", return_value=0):
        assert cli._run_relationship_orchestration(schema, MagicMock(), args, parent_run_id="stream-parent") == 0
    (phase,) = args.execution.snapshot()["phases"]
    assert (phase["kind"], phase["edge_type"], phase["state"]) == ("isolated", "KNOWS", "succeeded")


def test_stream_orchestration_records_the_shared_fleet_phase():
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    works, _knows = _orchestration_edges()
    schema = SimpleNamespace(edges=[works], loading=SimpleNamespace(consumer_group_id="loader"))
    args = SimpleNamespace(mode="stream", config="c.yaml", network="n", bulk_timeout_seconds=1,
                           execution=tracker(run_id="stream-parent", mode="stream"))
    with patch.object(cli, "_run_rotating_relationship_fleet", return_value=1):
        assert cli._run_relationship_orchestration(schema, MagicMock(), args, parent_run_id="stream-parent") == 1
    (phase,) = args.execution.snapshot()["phases"]
    assert (phase["kind"], phase["edge_type"], phase["state"]) == ("shared", None, "failed")


def test_published_schemas_accept_additive_documents_that_the_strict_ones_reject():
    document = planned_bulk().snapshot()
    document["new_top_level"] = {"anything": 1}
    document["roles"][0]["new_role_member"] = True
    document["types"][0]["records_written"]["new_observation_member"] = "x"
    document["totals"]["new_progress_field"] = None
    validate(document, PUBLISHED_STATUS)
    with pytest.raises(ValidationError):
        validate(document, STATUS)
    event = tracker(mode="stream").events()[0]
    event["new_member"] = [1]
    validate(event, PUBLISHED_EVENT)
    with pytest.raises(ValidationError):
        validate(event, EVENT)


def test_published_event_schema_accepts_an_unknown_type_but_not_a_malformed_known_one():
    base = tracker(mode="stream").events()[0]
    future = {key: base[key] for key in ("schema_version", "execution_id", "seq", "ts")} | {"type": "lag.sampled", "lag": 4}
    validate(future, PUBLISHED_EVENT)
    with pytest.raises(ValidationError):
        validate(future, EVENT)
    for broken in (dict(future, seq=0), dict(future, schema_version=2), dict(future, type="Lag"), {**future, "execution_id": "x"}):
        with pytest.raises(ValidationError):
            validate(broken, PUBLISHED_EVENT)
    known_but_wrong = dict(base, pid="not-an-int")
    with pytest.raises(ValidationError):
        validate(known_but_wrong, PUBLISHED_EVENT)


def test_published_schemas_still_reject_wrong_types_and_missing_members():
    document = planned_bulk().snapshot()
    for mutate in (lambda d: d.pop("state"), lambda d: d.update(state="paused"), lambda d: d.update(pid="1"),
                   lambda d: d["types"][0].update(records_written={"value": -1, "observed_at": d["started_at"], "source": "x"})):
        broken = copy.deepcopy(document)
        mutate(broken)
        with pytest.raises(ValidationError):
            validate(broken, PUBLISHED_STATUS)


def test_the_validator_agrees_with_jsonschema_on_the_published_schemas():
    jsonschema = pytest.importorskip("jsonschema")
    for schema in (PUBLISHED_STATUS, PUBLISHED_EVENT):
        jsonschema.Draft202012Validator.check_schema(schema)
    base = tracker(mode="stream").events()[0]
    documents = [(PUBLISHED_STATUS, planned_bulk().snapshot()), (PUBLISHED_EVENT, base),
                 (PUBLISHED_EVENT, {**base, "type": "lag.sampled"}), (PUBLISHED_EVENT, {**base, "type": "Lag"}),
                 (PUBLISHED_EVENT, {**base, "pid": "x"}), (PUBLISHED_EVENT, {**base, "extra": 1})]
    for schema, document in documents:
        expected = jsonschema.Draft202012Validator(schema).is_valid(document)
        try:
            validate(document, schema)
            actual = True
        except ValidationError:
            actual = False
        assert actual == expected, document


# -- a role failure before a stop makes the execution failed -------------------------------------------------------------------
def _started():
    execution = tracker(mode="stream")
    return execution, execution.role_started("node-loader", "Person", 0, container_id="a")


def test_a_role_failure_before_the_stop_request_ends_failed_with_the_real_exit_code():
    execution, role = _started()
    execution.role_exited(role, exit_code=1, exit_reason="crash")
    execution.request_stop()
    assert execution.finish(143) == "failed"
    outcome = execution.snapshot()["outcome"]
    assert outcome == {"exit_code": 143, "reason": "role failure", "error": None}
    check(execution)


def test_an_oom_before_the_stop_request_ends_failed():
    execution, role = _started()
    execution.role_oom(role)
    execution.request_stop()
    assert execution.finish(0) == "failed"


def test_a_role_exit_caused_by_our_own_stop_does_not_make_the_run_failed():
    execution, role = _started()
    execution.request_stop()
    execution.role_exited(role, exit_code=143, exit_reason="signal")           # the loaders' reaction to our SIGTERM
    other = execution.role_started("node-loader", "Company", 0, container_id="b")
    execution.role_oom(other)                                                   # even an OOM during shutdown
    assert execution.finish(130) == "stopped"
    assert execution.snapshot()["outcome"]["reason"] == "stop requested"


def test_a_clean_role_exit_before_the_stop_request_keeps_the_run_stopped():
    execution, role = _started()
    execution.role_exited(role, exit_code=0)
    execution.request_stop()
    assert execution.finish(130) == "stopped"


def test_a_role_failure_without_any_stop_still_follows_the_exit_code():
    execution, role = _started()
    execution.role_exited(role, exit_code=2)
    assert execution.finish(0) == "succeeded"          # nothing asked it to fail; the caller's exit code decides
