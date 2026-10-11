"""Stream-mode node writes share the relationship writers' bucket leases."""
from __future__ import annotations

import threading
from pathlib import Path
from threading import Event
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import TopicPartition
from prometheus_client import CollectorRegistry

from src.loader.edge_execution import BucketLeaseExecutionCoordinator
from src.loader.lease_client import BucketCellRouter, LeaseCoordinatorError
from src.loader.node_loader import (
    NODE_LEASE_WORKERS, LeasedNodeWriter, NodeLoader, NodeRecord, NodeWriter, build_leased_node_writer,
    main as node_loader_main,
)
from src.models.schema import LoadingConfig
from src.utils.metrics import GraphLoaderMetrics
from src.utils.schema_loader import load_schema
from test_bucket_lease_execution import _FakeClient, _Hub
import test_cli_shutdown as _cli_tests

harness = _cli_tests.harness  # reuse the CLI launch harness fixture

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def schema():
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


@pytest.fixture(scope="module")
def person_config(schema):
    return next(node for node in schema.nodes if node.label == "Person")


def _record(key: str) -> NodeRecord:
    return NodeRecord(key=key, properties={"personId": key, "name": "n"})


class _RecordingNodeWriter:
    """Stands in for NodeWriter: asserts every write holds its cell's lease."""

    def __init__(self, hub: _Hub, router: BucketCellRouter, *, fail_on_key=None) -> None:
        self.hub, self.router, self.fail_on_key = hub, router, fail_on_key
        self.lock = threading.Lock()
        self.batches: list[list[NodeRecord]] = []
        self.violations: list[str] = []
        self.deadlines: list = []
        self.on_write = None

    def write_batch(self, records, *, deadline=None, lease_liveness_check=None) -> None:
        resources = {token for r in records for token in self.router.cell(r.key, r.key)}
        if len(resources) != 1:
            self.violations.append("cell spans several resources")
        if not self.hub.held(resources):
            self.violations.append("write without lease")
        if self.fail_on_key is not None and any(r.key == self.fail_on_key for r in records):
            raise RuntimeError("neo4j write failed")
        with self.lock:
            self.batches.append(list(records))
            self.deadlines.append(deadline)
        if self.on_write is not None:
            self.on_write()


def _leased(router, writer, hub, *, workers=1, client_kwargs=None, **kwargs):
    clients = []

    def factory():
        client = _FakeClient(hub, **(client_kwargs or {}))
        clients.append(client)
        return client

    coordinator = BucketLeaseExecutionCoordinator(
        writer, router=router, worker_count=workers, batch_size=1000, client_factory=factory,
        merge_target_rows=0, kind="node", edge_type="Person", **kwargs,
    )
    return LeasedNodeWriter(coordinator), clients


def _loader_with_messages(person_config, leased, count, *, shutdown=None, **loading):
    consumer, producer = MagicMock(), MagicMock()
    producer.flush.return_value = 0
    shutdown = shutdown if shutdown is not None else Event()
    loader = NodeLoader(
        consumer, leased, person_config, producer=producer, shutdown_requested=shutdown,
        loading_config=LoadingConfig(unwind_batch_size=20, **loading),
    )
    loader._on_assign(consumer, [TopicPartition("person-events", 0)])
    loader._pending_assignment_events.clear()
    for offset in range(count):
        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = f'{{"personId":"p-{offset}","name":"A"}}'.encode()
        msg.topic.return_value, msg.partition.return_value, msg.offset.return_value = "person-events", 0, offset
        loader._deferred_messages.append(msg)
    shutdown.set()
    return loader, consumer


def test_node_cells_map_to_the_same_resources_as_edges_touching_them() -> None:
    for keyed in (frozenset(), frozenset({"Person"}), frozenset({"Day"})):
        node_router = BucketCellRouter("Person", "Person", 16, keyed)
        edge_router = BucketCellRouter("Person", "Day", 16, keyed)
        reverse_router = BucketCellRouter("Day", "Person", 16, keyed)
        for key in ("p-1", "p-2", "nm0001", 42):
            (node_resource,) = node_router.resources(node_router.cell(key, key))
            assert node_resource in edge_router.resources(edge_router.cell(key, "2026-01-01"))
            assert node_resource in reverse_router.resources(reverse_router.cell("2026-01-01", key))


def test_one_lease_per_cell_and_every_row_written_under_its_lease() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Person", 4)
    writer = _RecordingNodeWriter(hub, router)
    leased, _clients = _leased(router, writer, hub)
    records = [_record(f"p-{i}") for i in range(40)]

    leased.write_batch(records)

    expected_cells = {router.resources(router.cell(r.key, r.key)) for r in records}
    assert not writer.violations
    assert len(writer.batches) == len(expected_cells) == hub.grants
    assert sorted(r.key for batch in writer.batches for r in batch) == sorted(r.key for r in records)
    assert not hub.holder  # all released


def test_offsets_commit_only_after_every_cell_is_durable(person_config) -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Person", 4)
    events: list[str] = []
    writer = _RecordingNodeWriter(hub, router)
    writer.on_write = lambda: events.append("cell")
    leased, _ = _leased(router, writer, hub)
    loader, consumer = _loader_with_messages(person_config, leased, 20)
    consumer.commit.side_effect = lambda **_k: events.append("commit")

    assert loader.run() == 0

    cells = events.count("cell")
    assert cells > 1
    assert events == ["cell"] * cells + ["commit"]


def test_failed_cell_fails_the_flush_and_commits_no_offset(person_config) -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Person", 4)
    writer = _RecordingNodeWriter(hub, router, fail_on_key="p-7")
    leased, _ = _leased(router, writer, hub)
    loader, consumer = _loader_with_messages(person_config, leased, 10, retry_max_attempts=1)

    assert loader.run() == 1
    consumer.commit.assert_not_called()
    assert not hub.holder


def test_lease_loss_before_commit_rolls_back_and_fails_the_flush(person_config) -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value
    hub, router = _Hub(), BucketCellRouter("Person", "Person", 4)
    leased, _clients = _leased(router, NodeWriter(driver, person_config), hub, tx_timeout_seconds=30.0)
    with patch.object(_FakeClient, "ping", lambda self: False):
        with pytest.raises(LeaseCoordinatorError, match="broke before commit"):
            leased.write_batch([_record("p-1")])
    transaction.commit.assert_not_called()


def test_node_writer_applies_the_deadline_as_a_neo4j_timeout(person_config) -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    writer = NodeWriter(driver, person_config, clock=lambda: 100.0)

    writer.write_batch([_record("p-1")], deadline=130.0, lease_liveness_check=lambda: True)
    # The fenced budget: 30 s left minus the monitor slack and BEGIN allowance (src/loader/fenced_tx.py).
    assert session.begin_transaction.call_args.kwargs == {"timeout": 26.0}

    session.begin_transaction.reset_mock()
    with pytest.raises(LeaseCoordinatorError, match="deadline exceeded"):
        writer.write_batch([_record("p-1")], deadline=100.5)
    session.begin_transaction.assert_not_called()

    # Bulk / unfenced write: no kwargs at all, exactly as before.
    writer.write_batch([_record("p-1")])
    assert session.begin_transaction.call_args.kwargs == {}


def test_lease_wait_metric_uses_node_kind() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Person", 4)
    registry = CollectorRegistry()
    metrics = GraphLoaderMetrics(registry)
    leased, _ = _leased(router, _RecordingNodeWriter(hub, router), hub, metrics=metrics)
    leased.write_batch([_record("p-1")])
    assert registry.get_sample_value(
        "graph_loader_lease_wait_seconds_count", {"kind": "node", "name": "Person"}
    ) == 1


def test_build_leased_node_writer_is_off_without_a_lease_address(person_config, schema) -> None:
    assert build_leased_node_writer(person_config, MagicMock(), schema.loading, environ={}) is None


def test_build_leased_node_writer_primes_and_uses_the_shared_settings(person_config, schema) -> None:
    hub = _Hub()
    clients = []

    def factory():
        clients.append(_FakeClient(hub))
        return clients[-1]

    environ = {
        "GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7000", "GRAPH_LOADER_LEASE_BUCKETS": "8",
        "GRAPH_LOADER_LEASE_KEYED_LABELS": "Day",
    }
    writer = build_leased_node_writer(
        person_config, MagicMock(), schema.loading, environ=environ, lease_client_factory=factory,
    )
    assert isinstance(writer, LeasedNodeWriter)
    assert len(clients) == NODE_LEASE_WORKERS  # whole pool primed: a stop-time drain cannot open new ones
    coordinator = writer._coordinator
    assert coordinator._router.bucket_count == 8
    assert coordinator._router.keyed_labels == frozenset({"Day"})
    assert coordinator._kind == "node"


def test_build_leased_node_writer_passes_the_shutdown_event_as_the_drain_stop(person_config, schema) -> None:
    hub, stop = _Hub(), Event()
    writer = build_leased_node_writer(
        person_config, MagicMock(), schema.loading, shutdown_requested=stop,
        environ={"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7000", "GRAPH_LOADER_LEASE_BUCKETS": "8"},
        lease_client_factory=lambda: _FakeClient(hub),
    )
    assert writer._coordinator._stop_event is stop


def test_stopping_node_loader_abandons_unobtainable_leases_exits_zero_commits_nothing(person_config, caplog) -> None:
    """F1 for nodes: a dead holder's fenced resources must not hold a stopping node loader
    past the drain budget; nothing is written or committed, exit 0, WARNING."""
    import logging

    hub, router = _Hub(), BucketCellRouter("Person", "Person", 4)
    writer = _RecordingNodeWriter(hub, router)
    stop = Event()
    leased, _ = _leased(
        router, writer, hub, client_kwargs={"deny_all": True}, stop_event=stop, drain_budget_seconds=0.2,
    )
    loader, consumer = _loader_with_messages(person_config, leased, 5, shutdown=stop)
    with caplog.at_level(logging.WARNING):
        assert loader.run() == 0
    consumer.commit.assert_not_called()
    assert writer.batches == []
    assert any("stage=node-drain-abandoned" in r.getMessage() for r in caplog.records)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def _run_main(schema, mode, **patches):
    consumer, driver, loader, producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.node_loader.Consumer", return_value=consumer), \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.build_leased_node_writer", **patches) as build, \
         patch("src.loader.node_loader.NodeLoader", return_value=loader) as node_loader_cls:
        code = node_loader_main(["--node-label", "Person", "--mode", mode, "--max-messages", "1"])
    return code, build, node_loader_cls


def test_bulk_mode_never_builds_a_leased_writer(schema) -> None:
    code, build, node_loader_cls = _run_main(schema, "bulk")
    assert code == 0
    build.assert_not_called()
    assert isinstance(node_loader_cls.call_args.kwargs["writer"], NodeWriter)


def test_stream_mode_passes_the_leased_writer_to_the_loader_and_closes_it(schema) -> None:
    leased = MagicMock()
    code, _build, node_loader_cls = _run_main(schema, "stream", return_value=leased)
    assert code == 0
    assert node_loader_cls.call_args.kwargs["writer"] is leased
    leased.close.assert_called_once()


# -- orchestration: lease server before node loaders, env passthrough ---------

def test_run_node_loader_passes_the_lease_settings_to_node_containers(tmp_path) -> None:
    from src.orchestrator.docker_service import DockerService

    client = MagicMock()
    service = DockerService(client=client)
    service.run_node_loader(
        node_label="Person", topic="person", mode="stream", config_path="config.yaml",
        rejection_dir=str(tmp_path), lease_address="graph-loader-lease-x:7700", lease_buckets=8,
        lease_keyed_labels=("Day", "Genre"),
    )
    env = client.containers.run.call_args.kwargs["environment"]
    assert env["GRAPH_LOADER_BUCKET_LEASE_ADDR"] == env["GRAPH_LOADER_LEASE_ADDR"] == "graph-loader-lease-x:7700"
    assert env["GRAPH_LOADER_LEASE_BUCKETS"] == "8"
    assert env["GRAPH_LOADER_LEASE_KEYED_LABELS"] == "Day,Genre"

    client.reset_mock()
    service.run_node_loader(
        node_label="Person", topic="person", mode="bulk", config_path="config.yaml", rejection_dir=str(tmp_path),
    )
    env = client.containers.run.call_args.kwargs["environment"]
    assert not any("LEASE" in name for name in env)
    with pytest.raises(ValueError, match="require lease_address"):
        service.run_node_loader(
            node_label="Person", topic="person", mode="stream", config_path="config.yaml",
            rejection_dir=str(tmp_path), lease_buckets=8,
        )


def test_stream_start_launches_the_lease_server_before_the_node_loaders(harness) -> None:
    from test_cli_shutdown import _edge, _schema, _stream_args

    captured = {}

    def fake_fleet(schema, docker, args, **kwargs):
        captured.update(kwargs)
        return 0

    with patch("src.cli._run_lease_relationship_fleet", side_effect=fake_fleet):
        result = harness.run(_schema(edges=[_edge("WORKS_AT", "Person", "Company")]), _stream_args())

    assert result == 0
    calls = [call[0] for call in harness.state.docker.method_calls]
    assert calls.index("run_bucket_lease_server") < calls.index("run_node_loader")
    assert harness.state.docker.run_bucket_lease_server.call_count == 1
    for call in harness.state.docker.run_node_loader.call_args_list:
        assert call.kwargs["lease_address"] == "lease:7700"
        assert call.kwargs["lease_buckets"] == 16
        assert call.kwargs["lease_keyed_labels"] == ()
    lease = harness.fleet.started[0]
    assert lease.name == "lease-server"
    assert captured["prestarted_lease_container"] is lease
    assert [c.name for c in captured["node_containers"]] == ["node-Person-0", "node-Company-0"]


def test_bulk_start_does_not_start_a_lease_server_for_nodes(harness) -> None:
    from test_cli_shutdown import FakeBulkMonitor, _bulk_args, _schema

    with patch("src.cli.BulkMonitor", FakeBulkMonitor):
        harness.run(_schema(), _bulk_args())
    harness.state.docker.run_bucket_lease_server.assert_not_called()
    for call in harness.state.docker.run_node_loader.call_args_list:
        assert "lease_address" not in call.kwargs


def test_node_launch_failure_stops_the_prestarted_lease_server(harness) -> None:
    from test_cli_shutdown import _edge, _fake_docker, _schema, _stream_args

    docker = _fake_docker(harness.fleet)
    docker.run_node_loader.side_effect = RuntimeError("launch failed")
    result = harness.run(
        _schema(edges=[_edge("WORKS_AT", "Person", "Company")]), _stream_args(), docker=docker,
    )
    assert result == 1
    assert harness.fleet.started and not any(c.running for c in harness.fleet.started)


def test_stream_shutdown_stops_edges_then_nodes_then_the_lease_server(harness) -> None:
    from test_cli_shutdown import FakeContainer, _edge, _schema, _stream_args

    order: list[str] = []
    real_stop = FakeContainer.stop

    def recording_stop(self, timeout=None):
        if self.running:
            order.append(self.name.split("-")[0])
        real_stop(self, timeout)

    class Monitor:
        def __init__(self, *args, **kwargs):
            self.tracked_lease_server = kwargs["tracked_lease_server"]

        def wait_for_lease_server_ready(self, timeout, shutdown_requested=None, **_k):
            pass

        def supervise_stream(self, shutdown_requested, **_k):
            pass

        def close(self):
            pass

    schema = _schema(edges=[_edge("WORKS_AT", "Person", "Company")])
    with patch("src.cli.RelationshipBulkMonitor", Monitor), patch.object(FakeContainer, "stop", recording_stop):
        assert harness.run(schema, _stream_args()) == 0
    assert harness.state.docker.run_bucket_lease_server.call_count == 1
    assert order.index("edge") < order.index("node") < order.index("lease")
