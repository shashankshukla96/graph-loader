"""E15.F2.S8: run identity on every metric sample, in both runtimes, and the aggregated endpoint of the process runtime.

Unit cells (labels, reserved names, cardinality, the merge) run everywhere; the real-child cells use the lease server and the
CLI's own registry (no Kafka, no Neo4j).
"""
from __future__ import annotations

import json
import re
import time
import urllib.request

import pytest
from prometheus_client.parser import text_string_to_metric_families

from neo4j_graph_loader import _runtime
from neo4j_graph_loader.orchestrator.aggregated_metrics import AggregatedMetrics, merge_expositions
from neo4j_graph_loader.orchestrator.process_service import ProcessService
from neo4j_graph_loader.status.tracker import StatusTracker
from neo4j_graph_loader.utils import identity, logging_config, metrics as metrics_module
from neo4j_graph_loader.utils.metrics import GraphLoaderMetrics, initialize_zero_series

IDENTITY = ("run_id", "execution_id", "exporter", "runtime")
EXECUTION = "e" * 32
INCARNATION = "abcdef0123456789abcdef0123456789"


@pytest.fixture(autouse=True)
def clean_identity(monkeypatch):
    for key in ("GRAPH_LOADER_EXECUTION_ID", "GRAPH_LOADER_INCARNATION", "GRAPH_LOADER_RUN_ID", "GRAPH_LOADER_METRICS_LISTEN", "GRAPH_LOADER_RUNTIME"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.delenv("GRAPH_LOADER_METRICS_PORT", raising=False)
    logging_config.reset_for_tests()
    yield
    logging_config.reset_for_tests()


def tracked_env(monkeypatch, *, execution=EXECUTION, incarnation=INCARNATION, run_id=None, process=False):
    monkeypatch.setenv("GRAPH_LOADER_EXECUTION_ID", execution)
    monkeypatch.setenv("GRAPH_LOADER_INCARNATION", incarnation)
    if run_id is not None:
        monkeypatch.setenv("GRAPH_LOADER_RUN_ID", run_id)
    if process:
        monkeypatch.setenv("GRAPH_LOADER_METRICS_LISTEN", "127.0.0.1:0")
        monkeypatch.setenv("GRAPH_LOADER_RUNTIME", "process")


def exposition(metrics: GraphLoaderMetrics) -> str:
    from prometheus_client import generate_latest
    return generate_latest(metrics.exposition_registry()).decode()


def samples(text: str) -> list:
    return [sample for family in text_string_to_metric_families(text) for sample in family.samples]


def fresh(kind="node", name="Person", replica=None) -> GraphLoaderMetrics:
    metrics = GraphLoaderMetrics()
    metrics._identity_source = (kind, name)
    initialize_zero_series(metrics, kind=kind, name=name)
    metrics.process_up.labels(kind=kind, name=name).set(1)
    if replica is not None:
        logging_config.set_log_context(replica=replica)
    return metrics


# -- reserved names ---------------------------------------------------------------------------------------------------------------
def test_reserved_label_names_are_rejected_and_others_pass() -> None:
    for name in IDENTITY:
        with pytest.raises(identity.ReservedLabelError, match=name):
            identity.validate_extra_labels({name: "x"})
    identity.validate_extra_labels({"team": "a", "environment": "prod"})
    with pytest.raises(identity.ReservedLabelError, match="execution_id.*run_id|run_id.*execution_id"):
        identity.validate_extra_labels(["run_id", "execution_id", "ok"])


def test_exporter_names_are_unique_per_incarnation_and_stable_in_shape() -> None:
    first = identity.exporter_name("node", "Person", 0, "aaaaaaaa11111111")
    second = identity.exporter_name("node", "Person", 0, "bbbbbbbb22222222")
    assert first == "node-Person-r0-aaaaaaaa" and first != second
    assert identity.exporter_name("lease-server", "-", None, "cccccccc33333333") == "lease-server-cccccccc"
    assert identity.exporter_name("cli", None, None, None, execution_id=EXECUTION) == "cli-eeeeeeee"
    assert identity.component_exporter({"component": "planned-edge-writer", "writer_id": "w3", "edge_type": "KNOWS"},
                                       INCARNATION) == "planned-writer-KNOWS-r3-abcdef01"


# -- labels on every sample, both runtimes ------------------------------------------------------------------------------------------
@pytest.mark.parametrize("process", [False, True], ids=["docker", "process"])
def test_every_sample_carries_the_four_identity_labels(monkeypatch, process) -> None:
    tracked_env(monkeypatch, run_id="run-1", process=process)
    metrics = fresh()
    found = samples(exposition(metrics))
    assert found
    for sample in found:
        if sample.name == "graph_loader_exporter_up":
            continue
        assert all(key in sample.labels for key in IDENTITY), sample
    sample = next(s for s in found if s.name == "graph_loader_process_up")
    assert sample.labels["run_id"] == "run-1" and sample.labels["execution_id"] == EXECUTION
    assert sample.labels["exporter"] == "node-Person-abcdef01"
    assert sample.labels["runtime"] == ("process" if process else "docker")


def test_a_run_without_run_id_has_an_empty_label_and_an_untracked_process_has_none(monkeypatch) -> None:
    tracked_env(monkeypatch)
    assert {s.labels["run_id"] for s in samples(exposition(fresh()))} == {""}
    for key in ("GRAPH_LOADER_EXECUTION_ID", "GRAPH_LOADER_INCARNATION"):
        monkeypatch.delenv(key)
    untracked = samples(exposition(fresh()))
    assert untracked and not any(key in s.labels for s in untracked for key in IDENTITY)


def test_the_http_endpoint_and_the_snapshot_expose_the_same_labels(monkeypatch, tmp_path) -> None:
    tracked_env(monkeypatch, run_id="run-1", process=True)
    metrics = fresh()
    server = metrics_module.start_metrics_server(metrics, kind="node", name="Person", snapshot_path=str(tmp_path / "metrics-node.prom"))
    try:
        assert server is not None
        with urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/metrics", timeout=5) as response:
            served = response.read().decode()
        metrics_module.write_metrics_snapshot(metrics, str(tmp_path / "snap.prom"))
        written = (tmp_path / "snap.prom").read_text()
        for text in (served, written):
            found = [s for s in samples(text) if s.name != "graph_loader_exporter_up"]
            assert found and all(s.labels.get("exporter") == "node-Person-abcdef01" and s.labels["execution_id"] == EXECUTION for s in found)
    finally:
        metrics_module.stop_metrics_server(server)


def test_a_sample_that_has_its_own_exporter_label_keeps_it(monkeypatch) -> None:
    tracked_env(monkeypatch)
    metrics = fresh(kind="cli", name="-")
    metrics.exporter_up.labels(exporter="node-Person-11111111").set(1)
    own = [s for s in samples(exposition(metrics)) if s.name == "graph_loader_exporter_up"]
    assert [s.labels["exporter"] for s in own] == ["node-Person-11111111"]


# -- cardinality budget -----------------------------------------------------------------------------------------------------------
def series_keys(text: str) -> set:
    return {(s.name, tuple(sorted(s.labels.items()))) for s in samples(text)}


def test_identity_adds_no_series_within_an_exporter(monkeypatch) -> None:
    """Active series of one live exporter are exactly the pre-identity series: the labels are constant per process."""
    plain = series_keys(exposition(fresh()))
    plain_names = {(name, tuple(label for label in dict(labels) if label not in IDENTITY)) for name, labels in plain}
    tracked_env(monkeypatch, run_id="run-1")
    tracked = series_keys(exposition(fresh()))
    assert len(tracked) == len(plain)
    assert {(name, tuple(label for label, _ in labels if label not in IDENTITY)) for name, labels in tracked} == plain_names


def test_series_churn_is_linear_in_exporter_starts_and_per_exporter_constant(monkeypatch) -> None:
    """The budget: new series per execution = series of one exporter x exporters started; nothing multiplies."""
    tracked_env(monkeypatch, run_id="reused-run", execution=EXECUTION)
    one = series_keys(exposition(fresh()))
    union: set = set(one)
    executions = 6
    for index in range(1, executions):
        tracked_env(monkeypatch, run_id="reused-run", execution=f"{index:032x}", incarnation=f"{index:032x}")
        keys = series_keys(exposition(fresh()))
        assert len(keys) == len(one), "series per live exporter must not grow between executions"
        assert not (keys & union), "an execution never reuses another execution's series"
        union |= keys
    assert len(union) == executions * len(one)                  # run_id is reused across all of them: it adds no series by itself
    assert {dict(labels)["run_id"] for _, labels in union} == {"reused-run"}


# -- the merge --------------------------------------------------------------------------------------------------------------------
A = '# HELP graph_loader_x_total help\n# TYPE graph_loader_x_total counter\ngraph_loader_x_total{exporter="a"} 1.0\n'
B = '# HELP graph_loader_x_total help\n# TYPE graph_loader_x_total counter\ngraph_loader_x_total{exporter="b"} 2.0\n# HELP graph_loader_y gauge\n# TYPE graph_loader_y gauge\ngraph_loader_y{exporter="b"} 5.0\n'


def type_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith("# TYPE") or line.startswith("# HELP")]


def test_merge_has_one_help_and_type_per_family_and_every_sample_once() -> None:
    body, stats = merge_expositions([A, B])
    text = body.decode()
    assert text.count("# HELP graph_loader_x_total ") == 1 and text.count("# TYPE graph_loader_x_total counter") == 1
    assert series_keys(text) == series_keys(A) | series_keys(B)
    assert stats == {"type_conflicts": 0, "duplicates": 0}


def test_merge_drops_a_conflicting_type_and_a_duplicate_series_and_counts_them() -> None:
    wrong = '# TYPE graph_loader_x gauge\ngraph_loader_x{exporter="c"} 9.0\n'
    body, stats = merge_expositions([A, wrong, A])
    assert 'exporter="c"' not in body.decode() and body.decode().count('exporter="a"') == 1
    assert stats["type_conflicts"] == 1 and stats["duplicates"] == 1


def test_exporter_up_rows_are_added_and_replace_a_child_exposed_family() -> None:
    body, _ = merge_expositions([A], up=[({"exporter": "node-Person-11111111", "runtime": "process"}, 1), ({"exporter": "edge-KNOWS-r0-22222222"}, 0)])
    rows = {s.labels["exporter"]: s.value for s in samples(body.decode()) if s.name == "graph_loader_exporter_up"}
    assert rows == {"node-Person-11111111": 1.0, "edge-KNOWS-r0-22222222": 0.0}
    assert body.decode().count("# TYPE graph_loader_exporter_up gauge") == 1


def group(exporter: str, port: int | None, **labels) -> dict:
    return {"targets": [f"127.0.0.1:{port}"] if port else [], "labels": {"exporter": exporter, "execution_id": EXECUTION, "run_id": "r",
                                                                         "runtime": "process", **labels}}


def aggregate(groups, bodies: dict, own=b"", **options) -> AggregatedMetrics:
    def fetch(target, timeout):
        body = bodies[target]
        if isinstance(body, Exception):
            raise body
        return body
    return AggregatedMetrics("127.0.0.1", 0, own=lambda: own, targets=lambda: groups(), fetch=fetch, **options)


def test_aggregate_equals_the_union_of_the_scrapes_plus_exporter_up() -> None:
    groups = [group("a", 1001), group("b", 1002), group("cli-eeeeeeee", 1003, component="cli")]
    aggregator = aggregate(lambda: groups, {"127.0.0.1:1001": A, "127.0.0.1:1002": B}, own=b"# TYPE graph_loader_z gauge\ngraph_loader_z 3.0\n")
    text = aggregator.render().decode()
    expected = series_keys(A) | series_keys(B) | {("graph_loader_z", ())}
    got = {key for key in series_keys(text) if key[0] != "graph_loader_exporter_up"}
    assert got == expected
    up = {s.labels["exporter"]: s.value for s in samples(text) if s.name == "graph_loader_exporter_up"}
    assert up == {"a": 1.0, "b": 1.0}                              # the CLI is its own registry, never scraped through itself
    families = [line.split()[2] for line in text.splitlines() if line.startswith("# TYPE")]
    assert len(families) == len(set(families))


def test_a_failed_scrape_is_omitted_and_reported_as_exporter_up_zero_then_recovers() -> None:
    bodies = {"127.0.0.1:1001": A, "127.0.0.1:1002": OSError("connection refused")}
    aggregator = aggregate(lambda: [group("a", 1001), group("b", 1002)], bodies)
    text = aggregator.render().decode()
    assert 'exporter="b"' in text and series_keys(text) >= series_keys(A)
    up = {s.labels["exporter"]: s.value for s in samples(text) if s.name == "graph_loader_exporter_up"}
    assert up == {"a": 1.0, "b": 0.0}
    assert not [s for s in samples(text) if s.labels.get("exporter") == "b" and s.name != "graph_loader_exporter_up"]
    assert aggregator.failures == {"b": 1}
    bodies["127.0.0.1:1002"] = B
    again = {s.labels["exporter"]: s.value for s in samples(aggregator.render().decode()) if s.name == "graph_loader_exporter_up"}
    assert again == {"a": 1.0, "b": 1.0}


def test_an_exited_child_disappears_a_restart_is_a_new_exporter_and_a_failed_bind_is_zero() -> None:
    live = [group("a", 1001), group("b-old", 1002)]
    bodies = {"127.0.0.1:1001": A, "127.0.0.1:1002": B, "127.0.0.1:1003": B.replace('"b"', '"b-new"')}
    aggregator = aggregate(lambda: live, bodies)
    names = lambda: {s.labels["exporter"] for s in samples(aggregator.render().decode()) if s.name == "graph_loader_exporter_up"}  # noqa: E731
    assert names() == {"a", "b-old"}
    live.pop()                                                       # b-old exited: no row, no series
    assert names() == {"a"}
    live.append(group("b-new", 1003))                                # the restart is a different exporter
    assert names() == {"a", "b-new"}
    live.append({"targets": [], "labels": {"exporter": "c", "metrics_state": "failed"}})
    live.append({"targets": [], "labels": {"exporter": "d", "metrics_state": "disabled"}})
    rows = {s.labels["exporter"]: s.value for s in samples(aggregator.render().decode()) if s.name == "graph_loader_exporter_up"}
    assert rows["c"] == 0.0 and "d" not in rows                      # a bind failure is down; disabling by choice is silent


def test_a_slow_child_does_not_hold_the_others_beyond_the_timeout() -> None:
    import threading
    release = threading.Event()

    def fetch(target, timeout):
        if target.endswith(":1002"):
            release.wait(timeout)
            raise TimeoutError("slow")
        return A
    aggregator = AggregatedMetrics("127.0.0.1", 0, own=lambda: b"", targets=lambda: [group("a", 1001), group("b", 1002)], fetch=fetch, timeout=0.3)
    started = time.monotonic()
    text = aggregator.render().decode()
    release.set()
    assert time.monotonic() - started < 3
    up = {s.labels["exporter"]: s.value for s in samples(text) if s.name == "graph_loader_exporter_up"}
    assert up == {"a": 1.0, "b": 0.0}


def test_the_served_endpoint_answers_metrics_and_404s_elsewhere() -> None:
    aggregator = aggregate(lambda: [group("a", 1001)], {"127.0.0.1:1001": A})
    port = aggregator.start()
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
            assert response.status == 200 and "graph_loader_x_total" in response.read().decode()
            assert response.headers["Content-Type"].startswith("text/plain")
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/other", timeout=5)
        assert error.value.code == 404
    finally:
        aggregator.close()


# -- real children ------------------------------------------------------------------------------------------------------------------
@pytest.fixture
def paths(tmp_path):
    root = tmp_path / "attempt"
    for name in ("rejections", "plans", "spill", "timings"):
        (root / name).mkdir(parents=True)
    schema = root / "runtime_schema.yaml"
    schema.write_text("x: 1\n")
    return _runtime.RunPaths(root, schema, root / "rejections", root / "plans", root / "spill", root / "timings")


def scrape(port: int) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        return response.read().decode()


def tracked_service(paths, run_id="run-agg"):
    service = ProcessService(paths, ready_timeout=60)
    tracker = StatusTracker(run_id=run_id, mode="stream", runtime="process")
    service.status = tracker
    service.lifecycle.status = tracker
    service.lifecycle.identity_provider = lambda: {"execution_id": tracker.execution_id, "run_id": tracker.run_id or ""}
    return service, tracker


def lease(service, run_id):
    return service.run_bucket_lease_server(run_id=run_id, bucket_count=16, network="n", keyed_labels=[])


def test_real_children_carry_the_labels_and_the_aggregate_is_their_union(paths) -> None:
    service, tracker = tracked_service(paths)
    cli = GraphLoaderMetrics()
    cli._identity_source = ("cli", "-")
    logging_config.set_log_context(execution_id=tracker.execution_id, run_id="run-agg", runtime="process")
    cli.process_up.labels(kind="cli", name="-").set(1)
    from prometheus_client import generate_latest
    try:
        first, second = lease(service, "run-agg-a"), lease(service, "run-agg-b")
        bodies = [scrape(first.metrics["port"]), scrape(second.metrics["port"])]
        exporters = set()
        for body in bodies:
            found = [s for s in samples(body) if s.name != "graph_loader_exporter_up"]
            assert found
            for sample in found:
                assert sample.labels["execution_id"] == tracker.execution_id and sample.labels["run_id"] in ("run-agg-a", "run-agg-b")
                assert sample.labels["runtime"] == "process"
                assert re.fullmatch(r"lease-server-[0-9a-f]{8}", sample.labels["exporter"]), sample.labels["exporter"]
                exporters.add(sample.labels["exporter"])
        assert len(exporters) == 2                                     # two incarnations, two exporters
        listed = {g["labels"]["exporter"] for g in service.lifecycle.build_targets() if g["labels"].get("component") != "cli"}
        assert listed == exporters                                     # file_sd and the samples name the same exporter
        aggregator = AggregatedMetrics("127.0.0.1", 0, own=lambda: generate_latest(cli.exposition_registry()),
                                       targets=service.lifecycle.build_targets)
        text = aggregator.render().decode()
        merged = {key for key in series_keys(text) if key[0] != "graph_loader_exporter_up"}
        # a per-child scrape moves between calls (counters, time), so compare the series, not the values
        expected = set().union(*[series_keys(body) for body in bodies]) | series_keys(generate_latest(cli.exposition_registry()).decode())
        assert merged == {key for key in expected if key[0] != "graph_loader_exporter_up"}      # the union, nothing more, nothing less
        up = {s.labels["exporter"]: s.value for s in samples(text) if s.name == "graph_loader_exporter_up"}
        assert up == {exporter: 1.0 for exporter in exporters}
        families = [line.split()[2] for line in text.splitlines() if line.startswith("# TYPE")]
        assert len(families) == len(set(families)) and len(type_lines(text)) == 2 * len(families)
        # exit: the stopped child leaves the aggregate
        first.stop(timeout=10)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and len({s.labels["exporter"] for s in samples(aggregator.render().decode())
                                                   if s.name == "graph_loader_exporter_up"}) != 1:
            time.sleep(0.1)
        after = {s.labels["exporter"] for s in samples(aggregator.render().decode()) if s.name == "graph_loader_exporter_up"}
        assert len(after) == 1 and after < exporters
    finally:
        service.close()


def test_the_cli_target_group_names_the_cli_exporter(paths) -> None:
    service, tracker = tracked_service(paths)
    try:
        service.lifecycle.cli_metrics_port = 4242
        cli_group = next(g for g in service.lifecycle.build_targets() if g["labels"].get("component") == "cli")
        assert cli_group["labels"]["exporter"] == f"cli-{tracker.execution_id[:8]}" and cli_group["labels"]["execution_id"] == tracker.execution_id
    finally:
        service.close()


def test_the_docker_role_environment_has_exactly_the_three_s2_keys_and_the_labels_derive_from_them(monkeypatch) -> None:
    """The Docker runtime needs no change for S8: the labels come from the three keys S2 already injects (golden unchanged)."""
    tracked_env(monkeypatch, run_id="run-d")
    labels = identity.identity_labels("edge", "KNOWS", 0, context=logging_config.log_context())
    assert labels == {"run_id": "run-d", "execution_id": EXECUTION, "exporter": "edge-KNOWS-r0-abcdef01", "runtime": "docker"}
    from neo4j_graph_loader.orchestrator.role_launch import RoleLauncher  # noqa: F401  (the injected keys are asserted in the parity golden)
    golden = json.loads(open("tests/golden/docker_parity_0.1.0a2.json").read())
    assert "GRAPH_LOADER_EXECUTION_ID" in json.dumps(golden) and "GRAPH_LOADER_METRICS_LISTEN" not in json.dumps(golden)


# -- the CLI flag ---------------------------------------------------------------------------------------------------------------------
def test_metrics_port_is_a_process_runtime_option(caplog) -> None:
    from neo4j_graph_loader import cli
    with caplog.at_level("ERROR"):
        assert cli.main(["start", "--mode", "bulk", "--metrics-port", "9500"]) == 1
    assert "--metrics-port applies to --runtime process only" in caplog.text


def test_the_cli_starts_serves_and_stops_the_aggregate(monkeypatch, paths) -> None:
    from types import SimpleNamespace
    from neo4j_graph_loader import cli
    service, tracker = tracked_service(paths)
    tracked_env(monkeypatch, process=True)
    logging_config.set_log_context(execution_id=tracker.execution_id, run_id="run-agg", runtime="process")
    cli_metrics = GraphLoaderMetrics()
    cli_metrics._identity_source = ("cli", "-")
    cli_metrics.process_up.labels(kind="cli", name="-").set(1)
    cleanups: list = []
    try:
        child = service.run_bucket_lease_server(run_id="run-agg", bucket_count=16, network="n", keyed_labels=[])
        assert cli._start_aggregated_metrics(SimpleNamespace(metrics_port=0), service, cli_metrics, cleanups)
        aggregator = cleanups[0].__self__
        bound = aggregator._server.server_port
        text = scrape(bound)
        assert 'kind="cli"' in text and 'kind="lease-server"' in text
        assert f'exporter="cli-{tracker.execution_id[:8]}"' in text and "graph_loader_exporter_up" in text
        assert child.metrics["port"] != bound
        cleanups[0]()
        with pytest.raises(OSError):
            scrape(bound)
    finally:
        service.close()


@pytest.mark.parametrize("port", [-1, 65536])
def test_an_invalid_metrics_port_is_refused(port, caplog) -> None:
    from types import SimpleNamespace
    from neo4j_graph_loader import cli
    with caplog.at_level("ERROR"):
        assert not cli._start_aggregated_metrics(SimpleNamespace(metrics_port=port), SimpleNamespace(metrics_host="127.0.0.1"), None, [])
    assert "--metrics-port must be between 0 and 65535" in caplog.text


def test_an_occupied_metrics_port_fails_the_start_with_one_error(caplog) -> None:
    import socket
    from types import SimpleNamespace
    from neo4j_graph_loader import cli
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        with caplog.at_level("ERROR"):
            assert not cli._start_aggregated_metrics(SimpleNamespace(metrics_port=taken.getsockname()[1]),
                                                     SimpleNamespace(metrics_host="127.0.0.1", lifecycle=SimpleNamespace(build_targets=list)),
                                                     GraphLoaderMetrics(), [])
    assert "cannot bind the aggregated metrics endpoint" in caplog.text


def test_a_command_leaves_no_identity_behind_in_the_process(tmp_path) -> None:
    """An in-process caller (a test, a library user of ``cli.main``) must not see the previous command's execution on its samples."""
    from neo4j_graph_loader import cli
    seen = {}
    original = cli._dispatch

    def spy(args):
        seen["during"] = logging_config.log_context()["execution_id"]
        return original(args)
    cli._dispatch = spy
    try:
        cli.main(["start", "--mode", "bulk", "--config", str(tmp_path / "missing.yaml")])
    finally:
        cli._dispatch = original
    assert seen["during"] and logging_config.log_context()["execution_id"] is None


def test_runtime_is_explicit_never_inferred_from_the_metrics_listen_key(monkeypatch) -> None:
    """A Docker-style environment that happens to set GRAPH_LOADER_METRICS_LISTEN (and no RUNTIME key) is still docker."""
    tracked_env(monkeypatch)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_LISTEN", "127.0.0.1:0")
    assert identity.identity_labels("node", "Person", 0)["runtime"] == "docker"
    assert {s.labels["runtime"] for s in samples(exposition(fresh()))} == {"docker"}
    monkeypatch.setenv("GRAPH_LOADER_RUNTIME", "process")
    assert identity.identity_labels("node", "Person", 0)["runtime"] == "process"
    monkeypatch.setenv("GRAPH_LOADER_RUNTIME", "docker")
    assert identity.identity_labels("node", "Person", 0)["runtime"] == "docker"


@pytest.mark.parametrize("value", ["docker", "process"])
def test_docker_launches_never_forward_graph_loader_runtime_into_containers(monkeypatch, tmp_path, value) -> None:
    """GRAPH_LOADER_RUNTIME selects the CLI's backend (and labels process-runtime children); a Docker launch must not pass it on,
    whatever the CLI's own environment holds (the 0.1.0a2 golden covers the recorded scenarios, this covers the setting itself)."""
    import docker_parity_support as support
    monkeypatch.setenv("GRAPH_LOADER_RUNTIME", value)
    context = support.Context(tmp_path)
    service = context.service()
    service.run_bucket_lease_server(run_id="run-x", bucket_count=16, network="n", keyed_labels=[])
    launches = [event["args"] for event in context.recorder.events if event["op"] == "containers.run"]
    assert launches
    for launch in launches:
        assert "GRAPH_LOADER_RUNTIME" not in launch["environment"]
        assert "GRAPH_LOADER_METRICS_LISTEN" not in launch["environment"]
