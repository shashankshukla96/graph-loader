"""Unit tests for the CLI's planned-edge phase (``--edge-plan``) and its launches."""
from __future__ import annotations

import argparse
import json
from threading import Condition, Event, Thread
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.cli import AUTO_EDGE_PLAN, _validate_edge_plan_settings, build_parser, handle_start
from src.loader.edge_plan import ChunkSpec, ResourceTable, assemble_plan, resource_pair, write_plan_file
from src.orchestrator.docker_service import DockerService
from src.orchestrator.planned_edges import (
    PlannedEdgeError, run_planned_edge_phase, validate_plan_against_schema, verify_ledger, verify_plan_topic,
    wait_for_plan,
)


def _edge(edge_type, source, target, mode="python_apoc"):
    return SimpleNamespace(
        type=edge_type, topic=edge_type.lower(), topics=[edge_type.lower()], replicas=1,
        nodes=SimpleNamespace(source=source, target=target, is_self_referencing=source == target),
        execution=SimpleNamespace(mode=mode),
    )


SCHEMA = SimpleNamespace(
    nodes=[SimpleNamespace(label="Person", topic="person", replicas=1),
           SimpleNamespace(label="Title", topic="title", replicas=1)],
    edges=[_edge("ACTED_IN", "Person", "Title"), _edge("EPISODE_OF", "Title", "Title")],
    loading=SimpleNamespace(consumer_group_id="loader"),
)


def make_plan(rows=(3, 2, 4)):
    table = ResourceTable(["Person", "Title"], 2)
    specs = [
        ChunkSpec("A:0", "ACTED_IN", ("ACTED_IN", 0), 0, resource_pair(0, 2), rows[0], 0, 0),
        ChunkSpec("A:1", "ACTED_IN", ("ACTED_IN", 0), 1, resource_pair(0, 2), rows[1], 0, 1),
        ChunkSpec("E:0", "EPISODE_OF", ("EPISODE_OF", 1), 0, resource_pair(3, 3), rows[2], 1, 0),
    ]
    return assemble_plan(
        plan_id="plan-9", topic="chunks", partitions=2, resources=table,
        types={"ACTED_IN": ("Person", "Title", rows[0] + rows[1]), "EPISODE_OF": ("Title", "Title", rows[2])},
        specs=specs,
    )


def summary_for(plan, *, state="complete", drop=None, duplicate=None, rows=None):
    ledger = [[chunk.id, "w0", chunk.rows, 100.0 + chunk.id, 101.0 + chunk.id, 5.0] for chunk in plan.chunks]
    if drop is not None:
        ledger = [entry for entry in ledger if entry[0] != drop]
    if duplicate is not None:
        ledger.append(list(ledger[duplicate]))
    if rows is not None:
        ledger[0][2] = rows
    return {"state": state, "plan_id": plan.plan_id, "failure": None, "ledger": ledger,
            "types": {"ACTED_IN": {"rows": 5, "chunks": 2, "write_ms": 10.0, "first_grant": 100.0, "last_done": 102.0}},
            "stats": {"grants": 3}}


def test_parser_accepts_edge_plan_file_or_auto_and_derives_writers_by_default() -> None:
    args = build_parser().parse_args(["start", "--mode", "bulk", "--edge-plan", "plan.json"])
    assert args.edge_plan == "plan.json" and args.plan_writers is None
    auto = build_parser().parse_args(["start", "--mode", "bulk", "--edge-plan", "--plan-writers", "6"])
    assert auto.edge_plan == AUTO_EDGE_PLAN and auto.plan_writers == 6
    assert build_parser().parse_args(["start", "--mode", "bulk"]).edge_plan is None
    trailing = build_parser().parse_args(["start", "--mode", "bulk", "--config", "s.yaml", "--edge-plan"])
    assert trailing.edge_plan == AUTO_EDGE_PLAN and trailing.config == "s.yaml"
    assert trailing.plan_memory_mb is None and trailing.plan_spill_dir is None
    budget = build_parser().parse_args(["start", "--mode", "bulk", "--edge-plan", "--plan-memory-mb", "512",
                                        "--plan-spill-dir", "/data/spill"])
    assert budget.plan_memory_mb == 512 and budget.plan_spill_dir == "/data/spill"


@pytest.mark.parametrize("overrides, message", [
    ({"mode": "stream"}, "requires --mode bulk"),
    ({"edge_leases": True}, "mutually exclusive"),
    ({"plan_writers": 0}, "--plan-writers"),
    ({"plan_chunk_rows": 0}, "--plan-chunk-rows"),
    ({"plan_buckets": 5000}, "--plan-buckets"),
    ({"plan_memory_mb": 8}, "--plan-memory-mb"),
])
def test_edge_plan_settings_are_validated(overrides, message) -> None:
    args = argparse.Namespace(**{"mode": "bulk", "edge_plan": "p.json", "edge_leases": False, "plan_writers": 8,
                                 **overrides})
    with pytest.raises(ValueError, match=message):
        _validate_edge_plan_settings(SCHEMA, args)
    native = SimpleNamespace(edges=[_edge("X", "Person", "Title", mode="native_disjoint")])
    with pytest.raises(ValueError, match="python_apoc"):
        _validate_edge_plan_settings(native, argparse.Namespace(mode="bulk", edge_plan="p", edge_leases=False,
                                                               plan_writers=8))


def test_wait_for_plan_returns_on_the_file_and_fails_on_marker_timeout_or_cancel(tmp_path) -> None:
    path = tmp_path / "plan.json"
    path.write_text("{}", encoding="utf-8")
    wait_for_plan(path, 1)
    missing = tmp_path / "later.json"
    (tmp_path / "later.json.failed").write_text("ValueError: bad row\n", encoding="utf-8")
    with pytest.raises(PlannedEdgeError, match="edge planner failed: ValueError: bad row"):
        wait_for_plan(missing, 1)
    now = [0.0]
    with pytest.raises(PlannedEdgeError, match="did not appear"):
        wait_for_plan(tmp_path / "never.json", 1, clock=lambda: now[0], sleep=lambda s: now.__setitem__(0, now[0] + 1))
    cancelled = Event()
    cancelled.set()
    with pytest.raises(PlannedEdgeError, match="cancelled"):
        wait_for_plan(tmp_path / "never.json", 10, cancelled=cancelled)


def test_waiting_for_the_plan_times_out_only_when_the_planner_stops_making_progress(tmp_path) -> None:
    path = tmp_path / "plan.json"
    progress = tmp_path / "plan.json.progress"
    now = [0.0]

    def sleep(seconds):
        now[0] += 1.0
        # A planner that makes progress for 50 s (5x the timeout), then writes the plan.
        if now[0] < 50:
            progress.write_text(json.dumps({"rows": int(now[0]) * 1000}), encoding="utf-8")
        elif now[0] >= 55:
            path.write_text("{}", encoding="utf-8")

    wait_for_plan(path, 10, clock=lambda: now[0], sleep=sleep)
    assert now[0] >= 55
    # The heartbeat stops changing: the wait fails one timeout after the last progress.
    path.unlink()
    now[0] = 0.0
    progress.write_text('{"rows": 7}', encoding="utf-8")
    with pytest.raises(PlannedEdgeError, match=r"no progress within 10s; last progress \{\"rows\": 7\}"):
        wait_for_plan(path, 10, clock=lambda: now[0], sleep=lambda s: now.__setitem__(0, now[0] + 1))
    assert 10 <= now[0] <= 12


def _stall_phase(tmp_path, statuses, *, ready, timeout=5, steps=200):
    """Run the phase on a fake clock (1 s per status poll) with ``statuses`` repeated as given."""
    plan = make_plan()
    path = tmp_path / "edge-plan.json"
    write_plan_file(path, plan)
    now = [0.0]
    polls = iter(statuses)
    control = FakeControl(plan, [{"state": "running", "done": 0}])

    def status():
        now[0] += 1.0
        item = next(polls, None)
        if item is None:
            control.completed = True
            return {"state": "complete", "done": 3}
        return item

    control.status = status
    docker = MagicMock()
    docker.planned_dispatcher_addresses.return_value = ("a:1", "b:2")
    docker.run_planned_edge_writers.return_value = [_WriterUntilEnd({"control": control})]
    args = argparse.Namespace(edge_plan=str(path), plan_writers=1, network="n", config="c",
                              bulk_timeout_seconds=timeout)
    ready_labels, condition = set(ready), Condition()
    result = run_planned_edge_phase(
        SCHEMA, docker, args, parent_run_id="run1", ready_labels=ready_labels, ready_condition=condition,
        cancelled=Event(), control_factory=lambda address, digest: control,
        stop_exact=lambda containers: [], remove_exact=lambda containers: None,
        verify_topic=lambda plan, bootstrap: None, clock=lambda: now[0], sleep=lambda seconds: None,
    )
    return result, control


def test_dispatch_timeout_is_progress_based_and_ignores_waiting_for_node_labels(tmp_path, monkeypatch) -> None:
    import src.orchestrator.planned_edges as planned_edges
    monkeypatch.setattr(planned_edges, "_POLL_SECONDS", 0.001)
    # 100 s of steady chunk completions (20x the timeout) succeed.
    steady = [{"state": "running", "done": step, "granted": [step % 3]} for step in range(100)]
    result, control = _stall_phase(tmp_path, steady, ready=("Person", "Title"))
    assert result == 0 and not control.aborted
    # 60 s with nothing granted while an endpoint label still loads: waiting on nodes is not a stall.
    waiting = [{"state": "running", "done": 0, "granted": []}] * 60
    result, control = _stall_phase(tmp_path, waiting, ready=("Person",))
    assert result == 0 and not control.aborted
    # A granted chunk that never completes for longer than the timeout fails the phase closed.
    stuck = [{"state": "running", "done": 1, "granted": [2]}] * 60
    result, control = _stall_phase(tmp_path, stuck, ready=("Person", "Title"))
    assert result == 1 and "no planned chunk completed within 5s" in control.aborted[0]


def test_plan_must_match_the_schema() -> None:
    plan = make_plan()
    validate_plan_against_schema(plan, SCHEMA)
    other = SimpleNamespace(nodes=SCHEMA.nodes, edges=[SCHEMA.edges[0]])
    with pytest.raises(PlannedEdgeError, match="unknown=\\['EPISODE_OF'\\]"):
        validate_plan_against_schema(plan, other)
    swapped = SimpleNamespace(nodes=SCHEMA.nodes, edges=[_edge("ACTED_IN", "Title", "Person"), SCHEMA.edges[1]])
    with pytest.raises(PlannedEdgeError, match="endpoint labels differ"):
        validate_plan_against_schema(plan, swapped)
    no_loader = SimpleNamespace(nodes=SCHEMA.nodes[:1], edges=SCHEMA.edges)
    with pytest.raises(PlannedEdgeError, match="lack node loaders"):
        validate_plan_against_schema(plan, no_loader)


def _topic_consumer(watermarks, partitions=(0, 1)):
    consumer = MagicMock()
    consumer.list_topics.return_value = SimpleNamespace(topics={
        "chunks": SimpleNamespace(error=None, partitions={partition: object() for partition in partitions}),
    })
    consumer.get_watermark_offsets.side_effect = lambda tp, timeout: watermarks[tp.partition]
    return consumer


def test_plan_topic_must_hold_exactly_the_planned_chunks() -> None:
    plan = make_plan()
    verify_plan_topic(plan, "kafka:1", consumer_factory=lambda config: _topic_consumer({0: (0, 2), 1: (0, 1)}))
    with pytest.raises(PlannedEdgeError, match="retains 4 message"):
        verify_plan_topic(plan, "kafka:1", consumer_factory=lambda config: _topic_consumer({0: (0, 3), 1: (0, 1)}))
    with pytest.raises(PlannedEdgeError, match="not retained"):
        verify_plan_topic(plan, "kafka:1", consumer_factory=lambda config: _topic_consumer({0: (1, 3), 1: (0, 1)}))
    with pytest.raises(PlannedEdgeError, match="partitions"):
        verify_plan_topic(plan, "kafka:1", consumer_factory=lambda config: _topic_consumer({0: (0, 3)}, (0,)))


def test_ledger_must_hold_every_chunk_exactly_once_with_planned_rows() -> None:
    plan = make_plan()
    per_type = verify_ledger(plan, summary_for(plan))
    assert per_type["ACTED_IN"]["rows"] == 5 and per_type["EPISODE_OF"]["rows"] == 4
    assert per_type["ACTED_IN"]["write_ms"] == 10.0
    with pytest.raises(PlannedEdgeError, match="holds 2 of 3"):
        verify_ledger(plan, summary_for(plan, drop=1))
    with pytest.raises(PlannedEdgeError, match="twice"):
        verify_ledger(plan, summary_for(plan, duplicate=0))
    with pytest.raises(PlannedEdgeError, match="records 9 rows"):
        verify_ledger(plan, summary_for(plan, rows=9))
    with pytest.raises(PlannedEdgeError, match="dispatcher is failed"):
        verify_ledger(plan, summary_for(plan, state="failed"))


class FakeControl:
    def __init__(self, plan, statuses):
        self.plan, self.statuses = plan, list(statuses)
        self.ready_calls, self.aborted, self.closed = [], [], False

    def connect(self):
        return self.plan.plan_id

    def ready(self, labels):
        self.ready_calls.append(sorted(labels))

    completed = False

    def status(self):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        self.completed = status.get("state") == "complete"
        return status

    def summary(self):
        return summary_for(self.plan)

    def abort(self, reason):
        self.aborted.append(reason)

    def close(self):
        self.closed = True


def _exited(code):
    container = MagicMock()
    container.attrs = {"State": {"Status": "exited", "ExitCode": code}}
    return container


def _running():
    container = MagicMock()
    container.attrs = {"State": {"Status": "running"}}
    return container


class _WriterUntilEnd:
    """Runs until the fake dispatcher reported complete, then exits with ``code``."""

    def __init__(self, control_holder, code=0):
        self.control_holder, self.code, self.name = control_holder, code, "writer"

    def reload(self):
        pass

    @property
    def attrs(self):
        control = self.control_holder.get("control")
        if control is not None and control.completed:
            return {"State": {"Status": "exited", "ExitCode": self.code}}
        return {"State": {"Status": "running"}}

    def logs(self, tail=None):
        return b""


def _phase(tmp_path, statuses, *, writers=None, ready=("Person",), plan=None, monkeypatch=None, cancelled=None,
           holder=None):
    plan = plan or make_plan()
    path = tmp_path / "edge-plan.json"
    write_plan_file(path, plan)
    control = FakeControl(plan, statuses)
    if holder is not None:
        holder["control"] = control
    docker = MagicMock()
    dispatcher = MagicMock()
    dispatcher.logs.return_value = b"INFO: stage=planned-dispatcher-stats state=complete\n"
    docker.run_planned_dispatcher.return_value = dispatcher
    docker.planned_dispatcher_addresses.return_value = ("172.18.0.5:7701", "127.0.0.1:40000")
    writer_containers = writers if writers is not None else [_exited(0), _exited(0)]
    docker.run_planned_edge_writers.return_value = writer_containers
    stopped, removed = [], []
    args = argparse.Namespace(edge_plan=str(path), plan_writers=len(writer_containers), network="net",
                              config="schema.yaml", bulk_timeout_seconds=5)
    ready_labels, condition = set(ready), Condition()
    result = run_planned_edge_phase(
        SCHEMA, docker, args, parent_run_id="run1", ready_labels=ready_labels, ready_condition=condition,
        cancelled=cancelled or Event(), control_factory=lambda address, digest: control,
        stop_exact=lambda containers: stopped.extend(containers) or [],
        remove_exact=lambda containers: removed.extend(containers),
        verify_topic=lambda plan, bootstrap: None, sleep=lambda seconds: None,
    )
    return result, SimpleNamespace(control=control, docker=docker, dispatcher=dispatcher, stopped=stopped,
                                   removed=removed, writers=writer_containers, ready=ready_labels, plan=plan)


def test_phase_succeeds_only_after_complete_ledger_and_clean_writer_exits(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(tmp_path))
    running = {"state": "running", "done": 1}
    holder = {}
    result, phase = _phase(tmp_path, [running, {"state": "complete", "done": 3}],
                           writers=[_WriterUntilEnd(holder), _WriterUntilEnd(holder)], ready=("Person", "Title"),
                           holder=holder)
    assert result == 0
    launch = phase.docker.run_planned_dispatcher.call_args.kwargs
    assert launch["ready_labels"] == ["Person", "Title"] and launch["run_id"] == "run1"
    writers = phase.docker.run_planned_edge_writers.call_args.kwargs
    assert writers["dispatcher_address"] == "172.18.0.5:7701" and writers["writers"] == 2
    assert phase.control.aborted == [] and phase.control.closed
    assert phase.dispatcher in phase.stopped and phase.dispatcher in phase.removed
    summary = json.loads((tmp_path / "planned-summary.json").read_text(encoding="utf-8"))
    assert summary["rows"] == 9 and summary["writers"] == 2 and summary["dispatch_seconds"] == pytest.approx(3.0)
    stage = json.loads((tmp_path / "stage-ACTED_IN.json").read_text(encoding="utf-8"))
    assert stage["succeeded"] is True and stage["elapsed_seconds"] == pytest.approx(2.0)
    assert (tmp_path / "planned-dispatcher.log").read_text(encoding="utf-8").startswith("INFO")
    assert len(json.loads((tmp_path / "planned-ledger.json").read_text(encoding="utf-8"))) == 3
    durable = json.loads((tmp_path / "edge-plan.json.ledger.json").read_text(encoding="utf-8"))
    assert durable["state"] == "complete" and durable["rows"] == 9 and len(durable["ledger"]) == 3
    assert len(durable["plan_digest"]) == 64


def test_phase_forwards_labels_as_they_become_ready(tmp_path) -> None:
    plan = make_plan()
    path = tmp_path / "edge-plan.json"
    write_plan_file(path, plan)
    ready_labels, condition = {"Person"}, Condition()
    statuses = iter([{"state": "running", "done": 0}] * 5)
    control = FakeControl(plan, [{"state": "running", "done": 0}])
    released = Event()

    def status():
        if control.ready_calls and ["Title"] in control.ready_calls:
            control.completed = True
            return {"state": "complete", "done": 3}
        if not released.is_set():
            released.set()
        return next(statuses, {"state": "running", "done": 0})

    control.status = status
    docker = MagicMock()
    docker.planned_dispatcher_addresses.return_value = ("a:1", "b:2")
    docker.run_planned_edge_writers.return_value = [_WriterUntilEnd({"control": control})]
    args = argparse.Namespace(edge_plan=str(path), plan_writers=1, network="n", config="c", bulk_timeout_seconds=5)

    def later():
        assert released.wait(3)
        with condition:
            ready_labels.add("Title")
            condition.notify_all()

    thread = Thread(target=later)
    thread.start()
    result = run_planned_edge_phase(
        SCHEMA, docker, args, parent_run_id="run1", ready_labels=ready_labels, ready_condition=condition,
        cancelled=Event(), control_factory=lambda address, digest: control,
        stop_exact=lambda containers: [], remove_exact=lambda containers: None,
        verify_topic=lambda plan, bootstrap: None,
    )
    thread.join(3)
    assert result == 0
    assert docker.run_planned_dispatcher.call_args.kwargs["ready_labels"] == ["Person"]
    assert ["Title"] in control.ready_calls


def test_phase_fails_closed_when_the_dispatcher_fails(tmp_path) -> None:
    result, phase = _phase(tmp_path, [{"state": "failed", "failure": "writer w1 disconnected", "done": 1}],
                           writers=[_running()])
    assert result == 1
    assert not (tmp_path / "edge-plan.json.ledger.json").exists()
    assert phase.control.aborted and "writer w1 disconnected" in phase.control.aborted[0]
    assert phase.writers[0] in phase.stopped and phase.dispatcher in phase.stopped


def test_phase_fails_when_a_writer_exits_early_or_nonzero(tmp_path) -> None:
    result, phase = _phase(tmp_path, [{"state": "running", "done": 0}], writers=[_exited(1)])
    assert result == 1 and phase.control.aborted
    holder = {}
    result, phase = _phase(tmp_path, [{"state": "complete", "done": 3}], writers=[_WriterUntilEnd(holder, 3)],
                           holder=holder)
    assert result == 1


def test_phase_stops_when_cancelled_by_a_node_failure(tmp_path) -> None:
    cancelled = Event()
    cancelled.set()
    result, phase = _phase(tmp_path, [{"state": "running", "done": 0}], writers=[_running()], cancelled=cancelled)
    assert result == 1 and phase.control.aborted


def test_operator_stop_is_logged_as_a_warning_and_aborts_with_the_stop_reason(tmp_path, caplog) -> None:
    from src.loader.planned_writer import STOP_REASON
    plan = make_plan()
    path = tmp_path / "edge-plan.json"
    write_plan_file(path, plan)
    control = FakeControl(plan, [{"state": "running", "done": 0}])
    docker = MagicMock()
    docker.planned_dispatcher_addresses.return_value = ("172.18.0.5:7701", "127.0.0.1:40000")
    docker.run_planned_edge_writers.return_value = [_running()]
    args = argparse.Namespace(edge_plan=str(path), plan_writers=1, network="net", config="s.yaml",
                              bulk_timeout_seconds=5)
    cancelled = Event()
    cancelled.set()
    with caplog.at_level("INFO"):
        result = run_planned_edge_phase(
            SCHEMA, docker, args, parent_run_id="run1", ready_labels={"Person"}, ready_condition=Condition(),
            cancelled=cancelled, control_factory=lambda address, digest: control,
            stop_exact=lambda containers: [], remove_exact=lambda containers: None,
            verify_topic=lambda plan, bootstrap: None, sleep=lambda seconds: None, stop_requested=lambda: True,
        )
    assert result == 1
    assert control.aborted == [STOP_REASON]
    assert not [r for r in caplog.records if r.levelname == "ERROR"]
    assert any("stopped: operator/signal" in r.getMessage() and r.levelname == "WARNING" for r in caplog.records)


def test_phase_without_a_plan_launches_nothing(tmp_path) -> None:
    (tmp_path / "edge-plan.json.failed").write_text("boom", encoding="utf-8")
    docker = MagicMock()
    args = argparse.Namespace(edge_plan=str(tmp_path / "edge-plan.json"), plan_writers=2, network="n", config="c",
                              bulk_timeout_seconds=1)
    assert run_planned_edge_phase(
        SCHEMA, docker, args, parent_run_id="run1", ready_labels=set(), ready_condition=Condition(),
        cancelled=Event(), stop_exact=lambda containers: [], remove_exact=lambda containers: None,
    ) == 1
    docker.run_planned_dispatcher.assert_not_called()
    docker.run_planned_edge_writers.assert_not_called()


@patch("src.cli._run_relationship_orchestration")
@patch("src.cli._run_completion_driven_bulk_edges")
@patch("src.cli.run_planned_edge_phase")
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_runs_the_planned_phase_instead_of_per_type_loaders(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, planned, per_type, orchestration, tmp_path,
):
    mock_load.return_value = SCHEMA
    mock_driver.return_value = MagicMock()
    docker = MagicMock()
    docker.run_node_loader.side_effect = lambda node_label, **_kwargs: [MagicMock(name=node_label)]
    mock_docker.return_value = docker
    monitor = MagicMock()

    def drive(labels, timeout, *, stop_label, on_ready, should_abort):
        for label in sorted(labels):
            on_ready(label)

    monitor.drive_label_readiness.side_effect = drive
    monitor_cls.return_value = monitor
    seen = {}

    def run_phase(schema, docker_service, args, *, parent_run_id, ready_labels, ready_condition, cancelled,
                  stop_requested=None):
        with ready_condition:
            assert ready_condition.wait_for(lambda: {"Person", "Title"} <= ready_labels, timeout=3)
        seen["plan"] = args.edge_plan
        return 0

    planned.side_effect = run_phase
    args = argparse.Namespace(mode="bulk", config="c.yaml", network="net", skip_image_build=True,
                              bulk_timeout_seconds=3, edge_plan=str(tmp_path / "p.json"), plan_writers=8)
    assert handle_start(args) == 0
    assert seen["plan"] == str((tmp_path / "p.json").resolve())
    per_type.assert_not_called()
    orchestration.assert_not_called()
    docker.run_edge_loader.assert_not_called()


@patch("src.cli.run_planned_edge_phase", return_value=1)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_fails_when_the_planned_phase_fails(mock_load, mock_driver, _apply, mock_docker, monitor_cls, _planned):
    mock_load.return_value = SCHEMA
    docker = MagicMock()
    docker.run_node_loader.side_effect = lambda node_label, **_kwargs: [MagicMock(name=node_label)]
    mock_docker.return_value = docker
    monitor = MagicMock()
    monitor.drive_label_readiness.side_effect = lambda labels, timeout, **kwargs: [kwargs["on_ready"](l) for l in labels]
    monitor_cls.return_value = monitor
    args = argparse.Namespace(mode="bulk", config="c.yaml", network="net", skip_image_build=True,
                              bulk_timeout_seconds=3, edge_plan="p.json", plan_writers=8)
    assert handle_start(args) == 1


class TestPlannedDockerLaunches:
    def setup_method(self):
        self.client = MagicMock()
        self.service = DockerService(client=self.client)

    def test_dispatcher_mounts_the_plan_and_publishes_its_port_on_loopback(self, tmp_path):
        plan = tmp_path / "edge-plan.json"
        plan.write_text("{}", encoding="utf-8")
        self.service.run_planned_dispatcher(run_id="r1", plan_path=str(plan), network="net", ready_labels=["B", "A"])
        kwargs = self.client.containers.run.call_args.kwargs
        assert kwargs["entrypoint"] == ["python", "-m", "src.orchestrator.planned_dispatcher"]
        assert kwargs["command"][-2:] == ["--ready-labels", "A,B"]
        assert kwargs["volumes"] == {str(plan): {"bind": "/app/edge-plan.json", "mode": "ro"}}
        assert kwargs["ports"] == {"7701/tcp": ("127.0.0.1", None), "9464/tcp": ("127.0.0.1", None)}
        assert kwargs["labels"]["component"] == "planned-dispatcher" and kwargs["image"] == "graph-loader-edge:latest"
        assert kwargs["labels"]["graph_loader.metrics_port"] == "9464"
        assert kwargs["labels"]["graph_loader.kind"] == "planned-dispatcher"
        with pytest.raises(ValueError, match="does not exist"):
            self.service.run_planned_dispatcher(run_id="r1", plan_path=str(tmp_path / "missing.json"))

    def test_dispatcher_addresses_prefer_network_ip_and_host_port(self):
        container = MagicMock()
        container.attrs = {"NetworkSettings": {
            "Networks": {"net": {"IPAddress": "172.18.0.9"}},
            "Ports": {"7701/tcp": [{"HostIp": "127.0.0.1", "HostPort": "40123"}]},
        }}
        assert DockerService.planned_dispatcher_addresses(container, network="net") == (
            "172.18.0.9:7701", "127.0.0.1:40123",
        )

    def test_writers_share_one_plan_and_log_timings_per_writer(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(tmp_path / "timings"))
        monkeypatch.setenv("NEO4J_DATABASE", "bench")
        monkeypatch.setenv("LOADER_CPUSET", "12-31")
        plan = tmp_path / "edge-plan.json"
        plan.write_text("{}", encoding="utf-8")
        containers = self.service.run_planned_edge_writers(
            run_id="r1", plan_path=str(plan), config_path="schema.yaml", dispatcher_address="172.18.0.9:7701",
            writers=3, network="net",
        )
        assert len(containers) == 3
        calls = sorted((call.kwargs for call in self.client.containers.run.call_args_list), key=lambda k: k["name"])
        assert [kwargs["command"][kwargs["command"].index("--writer-id") + 1] for kwargs in calls] == ["w0", "w1", "w2"]
        first = calls[0]
        assert first["entrypoint"] == ["python", "-m", "src.loader.planned_writer"]
        assert "--dispatcher" in first["command"] and "172.18.0.9:7701" in first["command"]
        assert first["environment"]["NEO4J_DATABASE"] == "bench"
        assert first["environment"]["GRAPH_LOADER_TIMING_LOG_PATH"] == "/app/write-timings/edge-planned-w0.log"
        assert first["environment"]["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"] == "/app/write-timings/metrics-planned-writer-w0.prom"
        assert first["volumes"][str(plan)] == {"bind": "/app/edge-plan.json", "mode": "ro"}
        assert first["cpuset_cpus"] == "12-31" and first["labels"]["component"] == "planned-edge-writer"
        assert first["ports"] == {"9464/tcp": ("127.0.0.1", None)}
        assert first["labels"]["graph_loader.kind"] == "planned-writer"
        with pytest.raises(ValueError, match="1-64"):
            self.service.run_planned_edge_writers(run_id="r1", plan_path=str(plan), config_path="s",
                                                  dispatcher_address="a:1", writers=0)


class _FakeAdmin:
    topics = {"graph-loader-edge-chunks-r1"}
    removed: list = []
    lag_lists = 0  # list_topics calls after a delete that still show the topic (async deletion)
    _pending: set = set()

    def __init__(self, config):
        pass

    def list_topics(self, timeout=None):
        shown = set(self.topics)
        if self._pending and type(self).lag_lists > 0:
            type(self).lag_lists -= 1
            shown |= self._pending
        return SimpleNamespace(topics={t: None for t in shown})

    def delete_topics(self, topics, operation_timeout=None):
        self.removed.extend(topics)
        type(self)._pending = set(topics)
        type(self).topics = set(self.topics) - set(topics)
        future = MagicMock()
        future.result.return_value = None
        return {t: future for t in topics}


def _service_with(*statuses):
    service = MagicMock()
    service.list_run_containers.return_value = [
        SimpleNamespace(name=f"c{i}", status=status) for i, status in enumerate(statuses)
    ]
    return service


def test_stale_chunk_topic_is_replaced_only_when_no_container_of_the_run_is_alive() -> None:
    from src.orchestrator.planned_edges import replace_stale_chunk_topic
    _FakeAdmin.removed, _FakeAdmin.topics, _FakeAdmin.lag_lists = [], {"graph-loader-edge-chunks-r1"}, 2
    assert replace_stale_chunk_topic(
        _service_with("exited"), "r1", "k:1", admin_factory=_FakeAdmin, sleep=lambda seconds: None,
    ) is True
    assert _FakeAdmin.removed == ["graph-loader-edge-chunks-r1"]
    assert _FakeAdmin.lag_lists == 0  # it waited out the asynchronous deletion


def test_stale_chunk_topic_is_kept_while_a_container_of_the_run_is_alive() -> None:
    from src.orchestrator.planned_edges import replace_stale_chunk_topic
    _FakeAdmin.removed, _FakeAdmin.topics, _FakeAdmin._pending = [], {"graph-loader-edge-chunks-r1"}, set()
    with pytest.raises(PlannedEdgeError, match="still alive"):
        replace_stale_chunk_topic(_service_with("exited", "running"), "r1", "k:1", admin_factory=_FakeAdmin)
    assert _FakeAdmin.removed == []


def test_no_stale_chunk_topic_is_a_noop() -> None:
    from src.orchestrator.planned_edges import replace_stale_chunk_topic
    _FakeAdmin.topics, _FakeAdmin._pending = {"graph-loader-edge-chunks-r1"}, set()
    assert replace_stale_chunk_topic(_service_with(), "other", "k:1", admin_factory=_FakeAdmin) is False


@patch("src.cli._ensure_dead_letter_topics")
@patch("src.cli.start_kafka_edge_planner")
@patch("src.cli._validate_edge_plan_settings", wraps=_validate_edge_plan_settings)
@patch("src.cli.auto_settings.probe_neo4j_cores", return_value=8)
@patch("src.cli.run_planned_edge_phase", return_value=0)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_flagless_bulk_start_reaches_the_planned_path(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, phase, _cores, validate, start_planner, ensure_dlq, caplog,
):
    mock_load.return_value = SCHEMA
    docker = MagicMock()
    docker.run_node_loader.side_effect = lambda node_label, **_kwargs: [MagicMock(name=node_label)]
    mock_docker.return_value = docker
    monitor = MagicMock()
    monitor.drive_label_readiness.side_effect = lambda labels, timeout, **kwargs: [kwargs["on_ready"](l) for l in labels]
    monitor_cls.return_value = monitor
    args = build_parser().parse_args(["start", "--mode", "bulk", "--config", "c.yaml", "--skip-image-build",
                                      "--bulk-timeout-seconds", "3"])
    start_planner.return_value.rejected = 3
    assert handle_start(args) == 0
    assert "stage=outcome rejected=3" in caplog.text
    validate.assert_called_once()
    start_planner.assert_called_once()
    assert start_planner.call_args.kwargs["writers"] == 10  # derived exactly as for a bare --edge-plan
    assert ensure_dlq.call_args.kwargs["include_edges"] is False
    phase.assert_called_once()
    docker.run_edge_loader.assert_not_called()
