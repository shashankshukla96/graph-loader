"""E14.F2.S4 (Mac-runnable part): ProcessService turns the Docker launch builders' output into child processes.

The Linux role matrix with real Kafka and Neo4j (exact graph comparison against Docker mode) is the live part of S4
and runs on the host. Here: the translation, port reservation and replacement, discovery, second-shell commands,
the platform gate, and two real roles that need no external service (lease server, planned dispatcher).
"""
from __future__ import annotations

import os
import platform
import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from neo4j_graph_loader import _runtime
from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime
from tests.test_process_observability import _listening
from neo4j_graph_loader.orchestrator.process_service import (
    ProcessPreflightError, ProcessService, check_platform, describe_registered, recover_registered, resolve_registry, stop_registered,
)
from neo4j_graph_loader.orchestrator.run_registry import RunRegistry, StaleRegistry

FIXTURES = Path(__file__).resolve().parent / "fixtures"
linux_only = pytest.mark.skipif(platform.system() != "Linux", reason="pending-host: needs Linux")


class RecordingLifecycle:
    """Stands in for ProcessRuntime: records every spawn and returns a handle double."""

    def __init__(self, root):
        self.root = Path(root)
        self.calls = []
        self._python = sys.executable

    def spawn(self, role, module, args, env, **kwargs):
        self.calls.append(SimpleNamespace(role=role, module=module, args=list(args), env=dict(env), **kwargs))
        handle = SimpleNamespace(name=kwargs.get("name"), labels=kwargs.get("labels") or {}, auto_remove=kwargs.get("auto_remove"),
                                 ports={"control": 40123}, _exited=SimpleNamespace(is_set=lambda: False), status="running", id="abc" * 8)
        handle.hold_for_stop = lambda: 0.0
        handle.stop = lambda *a, **k: setattr(handle, "stopped", True)
        return handle

    def handles(self):
        return [SimpleNamespace(**{})]


@pytest.fixture
def paths(tmp_path):
    root = tmp_path / "attempt"
    for name in ("rejections", "plans", "spill", "timings"):
        (root / name).mkdir(parents=True)
    schema = root / "runtime_schema.yaml"
    schema.write_text("x: 1\n")
    return _runtime.RunPaths(root, schema, root / "rejections", root / "plans", root / "spill", root / "timings")


@pytest.fixture
def service(paths, monkeypatch):
    for name in ("LOADER_NEO4J_URI", "LOADER_KAFKA_BOOTSTRAP_SERVERS", "LOADER_CPUSET", "GRAPH_LOADER_METRICS_ENABLED", "NEO4J_URI",
                 "KAFKA_BOOTSTRAP_SERVERS"):
        monkeypatch.delenv(name, raising=False)
    lifecycle = RecordingLifecycle(paths.root)
    return ProcessService(paths, lifecycle=lifecycle)


def test_node_loader_is_translated_to_a_host_process(service, paths, monkeypatch) -> None:
    monkeypatch.setenv("NEO4J_URI", "bolt://db.example:7687")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.example:9092")
    monkeypatch.setenv("LOADER_CPUSET", "2-3")
    handles = service.run_node_loader(node_label="Person", topic="people", mode="bulk", config_path=str(paths.schema),
                                      replicas=2, run_id="run-1", network="ignored")
    calls = service.lifecycle.calls
    assert len(handles) == 2 and [c.name for c in calls] == [h.name for h in handles]
    first = calls[0]
    assert first.module == "neo4j_graph_loader.loader.node_loader" and first.role == "node-loader"
    assert first.args[:2] == ["--config", str(paths.schema)]                       # /app/runtime_schema.yaml -> the host file
    assert "--node-label" in first.args and first.args[first.args.index("--topic") + 1] == "people"
    assert first.env["NEO4J_URI"] == "bolt://db.example:7687"                       # Docker-network default -> the CLI's own endpoint
    assert first.env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka.example:9092"
    assert first.env["REJECTION_LOG_PATH"] == f"{paths.rejections}/Person-0.jsonl"
    assert first.cpuset == "2-3" and first.auto_remove is False
    assert first.labels["component"] == "node-loader" and first.labels["node_label"] == "Person" and first.labels["run_id"] == "run-1"
    assert first.env["GRAPH_LOADER_METRICS_LISTEN"] == "127.0.0.1:0"                # own loopback metrics port, never a shared 9464
    assert not any("/app/" in str(value) for value in [*first.args, *first.env.values()]), "no container path may leak"


def test_explicit_loader_endpoints_win_over_the_cli_endpoints(service, paths, monkeypatch) -> None:
    monkeypatch.setenv("LOADER_NEO4J_URI", "bolt://explicit:7687")
    monkeypatch.setenv("NEO4J_URI", "bolt://db.example:7687")
    service.run_node_loader(node_label="Person", topic="p", mode="stream", config_path=str(paths.schema), run_id="r")
    assert service.lifecycle.calls[0].env["NEO4J_URI"] == "bolt://explicit:7687"


def test_timing_directory_is_mapped_to_the_run_timings_directory(service, paths) -> None:
    service.run_node_loader(node_label="Person", topic="p", mode="bulk", config_path=str(paths.schema), run_id="r")
    env = service.lifecycle.calls[0].env
    assert any(str(paths.timings) in value for value in env.values()), env


def test_lease_server_binds_a_reserved_loopback_port_and_a_replacement_reuses_it(service) -> None:
    first = service.run_bucket_lease_server(run_id="run-1", bucket_count=16, network="ignored", keyed_labels=["Person"])
    other = service.run_bucket_lease_server(run_id="run-2", bucket_count=16, network="ignored", keyed_labels=[])
    replacement = service.run_bucket_lease_server(run_id="run-1", bucket_count=16, network="ignored", keyed_labels=["Person"],
                                                  quiet_period_seconds=35.0, restart_attempt=1)
    calls = service.lifecycle.calls

    def option(call, flag):
        return call.args[call.args.index(flag) + 1]
    assert all(option(c, "--host") == "127.0.0.1" for c in calls)
    assert option(calls[0], "--port") == option(calls[2], "--port") != option(calls[1], "--port")     # same port across the replacement
    assert all(c.module == "neo4j_graph_loader.orchestrator.lease_server" for c in calls)
    assert service.bucket_lease_address(first, network="n", prefer_name=True) == "127.0.0.1:40123"
    assert replacement is not first and other is not first


def test_dispatcher_and_writers(service, paths, tmp_path) -> None:
    plan = tmp_path / "edge-plan.json"
    plan.write_text("{}")
    dispatcher = service.run_planned_dispatcher(run_id="run-1", plan_path=str(plan), network="n", ready_labels=["B", "A"])
    internal, host = service.planned_dispatcher_addresses(dispatcher, network="n")
    assert internal == host == "127.0.0.1:40123"
    service.run_planned_edge_writers(run_id="run-1", plan_path=str(plan), config_path=str(paths.schema),
                                     dispatcher_address=internal, writers=2, network="n")
    disp, w0, w1 = service.lifecycle.calls
    assert disp.module == "neo4j_graph_loader.orchestrator.planned_dispatcher" and disp.args[disp.args.index("--plan") + 1] == str(plan)
    assert w0.module == "neo4j_graph_loader.loader.planned_writer" and str(plan) in w0.args and str(paths.schema) in w0.args
    assert w0.labels["component"] == "planned-edge-writer" and w1.name != w0.name


def test_discovery_by_labels_and_node_loader_stop(service, paths) -> None:
    service.lifecycle.handles = lambda: list(created)
    created = []
    created += service.run_node_loader(node_label="Person", topic="p", mode="stream", config_path=str(paths.schema), run_id="r")
    created += service.run_node_loader(node_label="Company", topic="c", mode="stream", config_path=str(paths.schema), run_id="r")
    assert [h.labels["node_label"] for h in service.list_run_containers(label_filters=("component=node-loader",))] == ["Person", "Company"]
    assert service.list_run_containers(label_filters=("component=node-loader", "node_label=Company"))[0] is created[1]
    assert service.list_run_containers(label_filters=("component=lease",)) == []
    service.stop_node_loaders(node_label="Person")
    assert getattr(created[0], "stopped", False) and not getattr(created[1], "stopped", False)
    assert [row["node_label"] for row in service.list_node_loaders()] == ["Person", "Company"]


def test_unknown_image_role_and_foreign_entrypoint_are_refused(service) -> None:
    with pytest.raises(ValueError, match="cannot run entrypoint"):
        service._run_container(image="role:edge", entrypoint=["sh", "-c", "x"], command=[], labels={})


# -- platform gate and preflight ------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("system,kernel,ok", [("Darwin", "24.0.0", False), ("Windows", "10", False), ("Linux", "4.19.0", False),
                                              ("Linux", "5.2.9", False), ("Linux", "5.3.0-generic", True), ("Linux", "7.0.0-38-generic", True)])
def test_platform_gate(system, kernel, ok) -> None:
    if ok:
        check_platform(system, kernel)
    else:
        with pytest.raises(ProcessPreflightError):
            check_platform(system, kernel)


@linux_only
def test_preflight_passes_without_a_docker_socket(service, paths, monkeypatch) -> None:
    monkeypatch.setenv("DOCKER_HOST", "unix:///nonexistent/docker.sock")
    ProcessService(paths).preflight_topology("ignored")


def test_preflight_refuses_off_linux(paths, monkeypatch) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Darwin")                      # the refusal is exercised on every host
    monkeypatch.setattr(platform, "release", lambda: "24.0.0")
    with pytest.raises(ProcessPreflightError, match="Linux only"):
        ProcessService(paths).preflight_topology("ignored")


# -- second shell: stop / status through the registry -----------------------------------------------------------------------------------
ROLE = "script:" + str(FIXTURES / "e14_role.py")
ALLOW = {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"}


def test_status_and_stop_by_label_from_a_second_shell(tmp_path) -> None:
    runtime = ProcessRuntime(tmp_path / "attempt", ready_timeout=30)
    runtime.register_cli()
    person = runtime.spawn("node-loader", ROLE, ["ready", str(tmp_path / "m1")], ALLOW, name="node-Person-0",
                           labels={"component": "node-loader", "node_label": "Person"})
    company = runtime.spawn("node-loader", ROLE, ["ready", str(tmp_path / "m2")], ALLOW, name="node-Company-0",
                            labels={"component": "node-loader", "node_label": "Company"})
    try:
        rows = describe_registered(str(tmp_path / "attempt"))
        assert sorted((r["name"], r["node_label"], r["status"]) for r in rows) == [
            ("node-Company-0", "Company", "running"), ("node-Person-0", "Person", "running")]
        assert stop_registered(str(tmp_path), "Person") == ["node-Person-0"]            # base directory resolves to the one live run
        assert person.wait(20) == 0 and (tmp_path / "m1").read_text() == "term\n"        # drained gracefully by SIGTERM
        assert company.status == "running"
        assert [r["name"] for r in describe_registered(str(tmp_path / "attempt"))] == ["node-Company-0"]
        assert stop_registered(str(tmp_path / "attempt"), "Missing") == []
    finally:
        runtime.close()


def test_resolve_registry_cases(tmp_path) -> None:
    with pytest.raises(StaleRegistry, match="no run registry"):
        resolve_registry(tmp_path)
    for name in ("a", "b"):
        (tmp_path / name).mkdir()
        registry = RunRegistry(tmp_path / name / "registry.jsonl")
        registry.append("cli", pid=os.getpid(), **__import__("neo4j_graph_loader.orchestrator.run_registry", fromlist=["x"]).process_identity(os.getpid()))
        registry.close()
    with pytest.raises(StaleRegistry, match="2 runs"):
        resolve_registry(tmp_path)
    assert resolve_registry(tmp_path / "a") == tmp_path / "a" / "registry.jsonl"


# -- recovery after the CLI died abruptly ------------------------------------------------------------------------------------------------------
def _orphaned_attempt(tmp_path, runtime):
    """A second attempt directory holding the runtime's child records under a CLI record whose process is already gone."""
    from neo4j_graph_loader.orchestrator.run_registry import RunRegistry, process_identity
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    ident = process_identity(dead.pid) or {}
    dead.wait()
    attempt = tmp_path / "orphaned"
    attempt.mkdir()
    registry = RunRegistry(attempt / "registry.jsonl")
    registry.append("cli", pid=dead.pid, pgid=dead.pid, **ident)
    for record in RunRegistry.read(runtime.registry.path):
        if record.get("kind") in {"intent", "started", "ready"}:
            registry.append(**record)
    registry.close()
    return attempt


def test_recover_refuses_while_the_cli_is_alive(tmp_path) -> None:
    runtime = ProcessRuntime(tmp_path / "attempt", ready_timeout=30)
    runtime.register_cli()
    try:
        with pytest.raises(StaleRegistry, match="still alive"):
            recover_registered(str(tmp_path / "attempt"))
    finally:
        runtime.close()


@pytest.mark.parametrize("mode, killed", [("ready", []), ("ignore-term", ["orphan-0"])])
def test_recover_signals_what_the_dead_cli_left_behind_and_escalates_when_sigterm_is_ignored(tmp_path, mode, killed) -> None:
    runtime = ProcessRuntime(tmp_path / "attempt", ready_timeout=30)
    runtime.register_cli()
    handle = runtime.spawn("node-loader", ROLE, [mode, str(tmp_path / "marker")], ALLOW, name="orphan-0",
                           labels={"component": "node-loader"})
    try:
        attempt = _orphaned_attempt(tmp_path, runtime)
        kept = ["plans/edge-plan.json.progress", "plans/edge-plan.json.lock", "plans/edge-plan.json.failed", "spill/.edge-plan-spill-0"]
        for name in kept:
            (attempt / name).parent.mkdir(exist_ok=True)
            (attempt / name).write_text("x")
        report = recover_registered(str(attempt), grace=1.0)
        assert report["signalled"] == ["orphan-0"] and report["killed"] == killed and report["remaining"] == []
        assert report["retained"] == sorted(kept) and all((attempt / name).is_file() for name in kept)     # reported by path, deleted never
        assert handle.wait(20) is not None
        assert any(r["kind"] == "recovered" for r in RunRegistry.read(attempt / "registry.jsonl"))
        again = recover_registered(str(attempt), grace=1.0)                 # nothing left: a second recovery is a no-op report
        assert again["signalled"] == [] and again["remaining"] == []
    finally:
        runtime.close()


@linux_only
def test_recover_empties_the_discovery_file_and_touches_no_run_data(tmp_path) -> None:
    """E15.F2.S12: a crashed run's metrics/targets.json is rewritten as [] by `stop --recover` (the stage=recover line says so); only the discovery
    file changes, so a later run reusing the default CLI port is no longer scraped under the old file's target labels."""
    import json
    runtime = ProcessRuntime(tmp_path / "attempt", ready_timeout=30)
    runtime.register_cli()
    runtime.cli_metrics_port = 9464
    runtime._targets_changed()
    try:
        attempt = _orphaned_attempt(tmp_path, runtime)
        (attempt / "metrics").mkdir(exist_ok=True)
        stale = [{"labels": {"component": "cli", "exporter": "cli-aaaaaaaa"}, "targets": ["127.0.0.1:9464"]}]
        (attempt / "metrics" / "targets.json").write_text(json.dumps(stale))
        (attempt / "spill").mkdir(exist_ok=True)
        (attempt / "spill" / "keep").write_text("x")
        report = recover_registered(str(attempt), grace=1.0)
        assert report["targets_cleared"] is True and json.loads((attempt / "metrics" / "targets.json").read_text()) == []
        assert (attempt / "spill" / "keep").read_text() == "x" and (attempt / "registry.jsonl").is_file()
        assert recover_registered(str(attempt), grace=1.0)["targets_cleared"] is True      # idempotent
    finally:
        runtime.close()


# -- real roles that need no external service ---------------------------------------------------------------------------------------------------
def _ask(address: str, line: bytes = b"HELLO\n") -> bytes:
    host, _, port = address.rpartition(":")
    with socket.create_connection((host, int(port)), timeout=5) as connection:
        connection.sendall(line)
        connection.settimeout(5)
        return connection.recv(256)


def test_real_lease_server_is_ready_on_loopback_and_its_replacement_keeps_the_port(tmp_path, paths) -> None:
    service = ProcessService(paths, ready_timeout=60)
    try:
        server = service.run_bucket_lease_server(run_id="run-real", bucket_count=16, network="n", keyed_labels=[])
        address = service.bucket_lease_address(server, network="n", prefer_name=True)
        assert address.startswith("127.0.0.1:") and server.ports["control"] == int(address.rpartition(":")[2])
        with socket.create_connection(("127.0.0.1", server.ports["control"]), timeout=5):
            pass
        listening = _listening(server.pid)
        assert listening and all(address == "127.0.0.1" for address, _port in listening), listening       # loopback only
        server.stop(timeout=10)
        assert server.wait(10) == 0
        server.remove(force=True)
        replacement = service.run_bucket_lease_server(run_id="run-real", bucket_count=16, network="n", keyed_labels=[],
                                                      quiet_period_seconds=0.0, restart_attempt=1)
        assert replacement.ports == server.ports                                         # same port: lease clients stay valid
        replacement.stop(timeout=10)
    finally:
        service.close()


def test_real_planned_dispatcher_reports_its_port(tmp_path, paths) -> None:
    from neo4j_graph_loader.loader.edge_plan import ChunkSpec, ResourceTable, assemble_plan, resource_pair, write_plan_file
    plan = assemble_plan(plan_id="p", topic="chunks", partitions=1, resources=ResourceTable(["Person", "Title"], 2),
                         types={"ACTED_IN": ("Person", "Title", 3)},
                         specs=[ChunkSpec("A:0", "ACTED_IN", ("ACTED_IN", 0), 0, resource_pair(0, 2), 3, 0, 0)])
    path = tmp_path / "edge-plan.json"
    write_plan_file(path, plan)
    service = ProcessService(paths, ready_timeout=60)
    try:
        dispatcher = service.run_planned_dispatcher(run_id="run-real", plan_path=str(path), network="n", ready_labels=[])
        internal, host = service.planned_dispatcher_addresses(dispatcher, network="n")
        assert internal == host and internal.startswith("127.0.0.1:")
        with socket.create_connection(("127.0.0.1", dispatcher.ports["control"]), timeout=5):
            pass
        dispatcher.stop(timeout=10)
        assert dispatcher.wait(10) == 0
    finally:
        service.close()


# -- process-only options are refused with the Docker runtime, not silently ignored ---------------------------------------------
@pytest.mark.parametrize("argv, option", [(["stop", "--recover"], "--recover"), (["stop", "--run-dir", "/x"], "--run-dir"),
                                          (["status", "--config", "x.yaml", "--run-dir", "/x"], "--run-dir")])
def test_process_only_second_shell_options_are_refused_with_the_docker_runtime(argv, option, caplog, monkeypatch) -> None:
    from neo4j_graph_loader import cli
    monkeypatch.setattr(cli, "_docker_service_or_none", lambda args: pytest.fail("the Docker service must not be reached"))
    with caplog.at_level("ERROR"):
        assert cli.main([argv[0], "--runtime", "docker", *argv[1:]]) == 1
    assert f"{option} applies to --runtime process only" in caplog.text


# -- --replace-stale-plan-topic must see another command's live roles, not only this command's ---------------------------------
class _TopicAdmin:
    topics = {"graph-loader-edge-chunks-r1": object()}

    def __init__(self, conf):
        pass

    def list_topics(self, timeout=None):
        return SimpleNamespace(topics=dict(_TopicAdmin.topics))


def test_stale_chunk_topic_replacement_sees_a_live_role_of_another_command_of_the_same_run_id(tmp_path, monkeypatch) -> None:
    from neo4j_graph_loader.orchestrator import planned_edges
    monkeypatch.setattr("neo4j_graph_loader.loader.edge_planner.delete_chunk_topic", lambda *a, **k: _TopicAdmin.topics.clear())
    first = ProcessRuntime(tmp_path / "attempt-a", ready_timeout=30)
    first.register_cli()
    handle = first.spawn("edge-loader", ROLE, ["ready", str(tmp_path / "m")], ALLOW, name="edge-PURCHASED-0",
                         labels={"component": "edge-loader", "run_id": "r1"})
    _TopicAdmin.topics = {"graph-loader-edge-chunks-r1": object()}
    second_paths = _runtime.RunPaths(tmp_path / "attempt-b", tmp_path / "attempt-b" / "s.yaml", tmp_path / "attempt-b" / "rej",
                                     tmp_path / "attempt-b" / "plans", tmp_path / "attempt-b" / "spill", tmp_path / "attempt-b" / "timings")
    second = ProcessService(second_paths, root=tmp_path / "attempt-b")
    try:
        assert [c.name for c in second.other_attempts_of_run("r1")] == ["edge-PURCHASED-0"]
        assert second.other_attempts_of_run("another-run") == []
        with pytest.raises(planned_edges.PlannedEdgeError, match=r"edge-PURCHASED-0.*graph-loader stop --runtime process"):
            planned_edges.replace_stale_chunk_topic(second, "r1", "k:1", admin_factory=_TopicAdmin)
        assert _TopicAdmin.topics, "a live role of that run: the topic must not be deleted"
        handle.stop(timeout=10)
        assert handle.wait(20) == 0
        assert second.other_attempts_of_run("r1") == []                               # gone: the same call now proceeds
    finally:
        second.close()
        first.close()


@pytest.mark.parametrize("failure", [ProcessLookupError(3, "No such process"), StaleRegistry("pid is no longer that process")])
def test_recover_treats_a_leftover_that_exits_during_the_recovery_as_already_gone(tmp_path, monkeypatch, failure) -> None:
    """Review fix 4: a pid reaped between the identity check and the signal must not abort the recovery before the `recovered` record."""
    from neo4j_graph_loader.orchestrator import process_service as module
    runtime = ProcessRuntime(tmp_path / "attempt", ready_timeout=30)
    runtime.register_cli()
    first = runtime.spawn("node-loader", ROLE, ["ready", str(tmp_path / "m1")], ALLOW, name="orphan-0", labels={"component": "node-loader"})
    second = runtime.spawn("node-loader", ROLE, ["ready", str(tmp_path / "m2")], ALLOW, name="orphan-1", labels={"component": "node-loader"})
    real = module._signal_child
    calls = []

    def flaky(state, signum):
        calls.append((state["name"], signum))
        if state["name"] == "orphan-0":
            raise failure                                   # it "exited" while we were about to signal it
        return real(state, signum)
    try:
        attempt = _orphaned_attempt(tmp_path, runtime)
        monkeypatch.setattr(module, "_signal_child", flaky)
        report = recover_registered(str(attempt), grace=2.0)
        assert "orphan-1" in report["signalled"] and report["remaining"] in ([], ["orphan-0"])
        assert any(r["kind"] == "recovered" for r in RunRegistry.read(attempt / "registry.jsonl")), "the recovered record must still be written"
        assert second.wait(20) is not None
    finally:
        first.kill()
        runtime.close()
