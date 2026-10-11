"""Rebalance safety for relationship loaders.

Bulk edge replicas statically ``assign()`` their partitions so a staggered
start never rebalances a writing sibling; any ``subscribe()`` loader settles
its buffered flush (write + commit) inside the revoke callback instead of
failing the run.
"""
from __future__ import annotations

import json
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import OFFSET_BEGINNING, KafkaError, TopicPartition

from src.loader.edge_execution import LaneExecutionResult
from src.loader.edge_loader import (
    EdgeLoader,
    StaticAssignmentError,
    main as edge_loader_main,
    static_partition_assignment,
)
from src.models.schema import LoadingConfig
from src.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor
from src.utils.schema_loader import load_schema

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOPIC = "works-at-events"


@pytest.fixture(scope="module")
def schema():
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


@pytest.fixture(scope="module")
def works_at(schema):
    return next(edge for edge in schema.edges if edge.type == "WORKS_AT")


def _payload(person: str = "p-001") -> bytes:
    return (
        b'{"source":{"personId":"' + person.encode() + b'"},"target":{"companyId":"c-001"},'
        b'"properties":{"since":"2020-01-02"}}'
    )


def _message(payload: bytes, *, partition: int = 0, offset: int = 3):
    message = MagicMock()
    message.value.return_value = payload
    message.error.return_value = None
    message.topic.return_value = TOPIC
    message.partition.return_value = partition
    message.offset.return_value = offset
    return message


def _producer(events: list[str] | None = None) -> MagicMock:
    producer = MagicMock()
    producer.flush.return_value = 0
    if events is not None:
        producer.produce.side_effect = lambda *args, **kwargs: events.append(
            "control:" + json.loads(kwargs["value"])["type"]
        )
    return producer


def _payloads(producer: MagicMock) -> list[dict]:
    return [json.loads(call.kwargs["value"]) for call in producer.produce.call_args_list]


def _metadata(partitions=(0, 1, 2, 3)):
    return SimpleNamespace(topics={
        TOPIC: SimpleNamespace(error=None, partitions={p: SimpleNamespace() for p in partitions}),
    })


def _static_consumer(partitions=(0, 1, 2, 3), committed: dict[int, int] | None = None) -> MagicMock:
    consumer = MagicMock()
    consumer.list_topics.return_value = _metadata(partitions)
    committed = committed or {}
    consumer.committed.side_effect = lambda requested, timeout: [
        TopicPartition(item.topic, item.partition, committed.get(item.partition, -1001))
        for item in requested
    ]
    consumer.poll.return_value = None
    consumer.commit.side_effect = lambda **kwargs: kwargs["offsets"]
    return consumer


# --- static partition assignment -------------------------------------------------


def test_static_assignment_math_is_disjoint_and_complete() -> None:
    partitions = [3, 0, 2, 1]
    assert [static_partition_assignment(partitions, i, 4) for i in range(4)] == [(0,), (1,), (2,), (3,)]
    assert [static_partition_assignment(partitions, i, 2) for i in range(2)] == [(0, 2), (1, 3)]
    assert static_partition_assignment(partitions, 0, 1) == (0, 1, 2, 3)
    for count in range(1, 10):
        owned = [p for i in range(count) for p in static_partition_assignment(range(8), i, count)]
        assert sorted(owned) == list(range(8))  # every partition exactly once


def test_static_assignment_with_more_replicas_than_partitions_leaves_surplus_empty() -> None:
    assert [static_partition_assignment(range(4), i, 6) for i in range(6)] == [
        (0,), (1,), (2,), (3,), (), (),
    ]


@pytest.mark.parametrize("index,count", [(-1, 4), (4, 4), (0, 0), (True, 2), (0, True)])
def test_static_assignment_rejects_invalid_replica_identity(index, count) -> None:
    with pytest.raises(ValueError):
        static_partition_assignment(range(4), index, count)


def test_static_assign_resumes_committed_offsets_or_earliest_without_subscribe(works_at) -> None:
    consumer = _static_consumer(committed={1: 42})
    producer = _producer()
    loader = EdgeLoader(
        consumer, MagicMock(), works_at, producer=producer, run_id="run-1", replica_id="1",
        static_assignment=(1, 2),
    )

    consumer.subscribe.assert_not_called()
    consumer.list_topics.assert_called_once()
    assert consumer.list_topics.call_args.kwargs["topic"] == TOPIC
    requested = consumer.committed.call_args.args[0]
    assert [(item.topic, item.partition) for item in requested] == [(TOPIC, 1), (TOPIC, 3)]
    assigned = consumer.assign.call_args.args[0]
    assert [(item.topic, item.partition, item.offset) for item in assigned] == [
        (TOPIC, 1, 42), (TOPIC, 3, OFFSET_BEGINNING),
    ]
    loader._publish_pending_assignments()
    assert _payloads(producer) == [{
        "run_id": "run-1", "edge_type": "WORKS_AT", "replica_id": "1", "type": "ASSIGNMENT",
        "assignment_epoch": 1,
        "assigned_partitions": [{"topic": TOPIC, "partition": 1}, {"topic": TOPIC, "partition": 3}],
    }]


def test_static_bulk_loader_writes_commits_to_group_and_drains(works_at) -> None:
    consumer = _static_consumer()
    messages = iter([_message(_payload(), partition=2, offset=7)])
    shutdown = Event()

    def poll(_timeout):
        message = next(messages, None)
        if message is None:
            shutdown.set()
        return message

    consumer.poll.side_effect = poll
    events: list[str] = []
    writer = MagicMock()
    writer.write_batch.side_effect = lambda records: events.append("write")
    consumer.commit.side_effect = lambda **kwargs: (events.append("commit"), kwargs["offsets"])[1]
    producer = _producer(events)
    loader = EdgeLoader(
        consumer, writer, works_at, producer=producer, run_id="run-1", replica_id="2",
        static_assignment=(2, 4), shutdown_requested=shutdown,
        loading_config=LoadingConfig(unwind_batch_size=100, flush_interval_ms=60_000),
    )

    assert loader.run() == 0
    assert events == ["control:ASSIGNMENT", "write", "commit", "control:DRAIN_COMPLETE"]
    committed = consumer.commit.call_args.kwargs["offsets"]
    assert [(item.topic, item.partition, item.offset) for item in committed] == [(TOPIC, 2, 8)]
    assert [payload["assignment_epoch"] for payload in _payloads(producer)] == [1, 1]
    consumer.subscribe.assert_not_called()


def test_surplus_static_replica_publishes_idle_surplus_and_matching_drain(works_at) -> None:
    consumer = _static_consumer(partitions=(0, 1, 2, 3))
    producer = _producer()
    shutdown = Event()
    shutdown.set()
    loader = EdgeLoader(
        consumer, MagicMock(), works_at, producer=producer, run_id="run-1", replica_id="5",
        static_assignment=(5, 6), shutdown_requested=shutdown,
    )

    assert loader.run() == 0
    consumer.committed.assert_not_called()
    consumer.assign.assert_called_once_with([])
    consumer.subscribe.assert_not_called()
    assert [(p["type"], p["assignment_epoch"]) for p in _payloads(producer)] == [
        ("IDLE_SURPLUS", 1), ("DRAIN_COMPLETE", 1),
    ]
    assert "assigned_partitions" not in _payloads(producer)[0]


def test_static_assign_fails_closed_on_missing_metadata_or_offset_error(works_at) -> None:
    consumer = _static_consumer()
    consumer.list_topics.return_value = SimpleNamespace(topics={})
    with pytest.raises(StaticAssignmentError, match="no partition metadata"):
        EdgeLoader(consumer, MagicMock(), works_at, static_assignment=(0, 4))
    consumer.assign.assert_not_called()

    consumer = _static_consumer()
    consumer.committed.side_effect = lambda requested, timeout: [
        SimpleNamespace(topic=TOPIC, partition=0, offset=-1001, error=KafkaError(KafkaError._TIMED_OUT)),
    ]
    with pytest.raises(StaticAssignmentError, match="committed offset lookup failed"):
        EdgeLoader(consumer, MagicMock(), works_at, static_assignment=(0, 4))
    consumer.assign.assert_not_called()

    consumer = _static_consumer()
    consumer.committed.side_effect = RuntimeError("coordinator unavailable")
    with pytest.raises(StaticAssignmentError, match="committed offsets unavailable"):
        EdgeLoader(consumer, MagicMock(), works_at, static_assignment=(0, 4))


def _monitor_for(works_at, replicas: int):
    admin, watermark, control = MagicMock(), MagicMock(), MagicMock()
    admin.list_topics.return_value = _metadata((0, 1, 2, 3))
    control.poll.return_value = None
    expected = {("WORKS_AT", str(i)) for i in range(replicas)}
    containers = {}
    for replica in expected:
        container = MagicMock()
        container.attrs = {"State": {"Status": "exited", "ExitCode": 0}}
        containers[replica] = container
    monitor = RelationshipBulkMonitor(
        [works_at], "run-1", expected, containers,
        bootstrap_servers="kafka:9092", consumer_group_prefix="graph-loader",
        admin_client=admin, watermark_consumer=watermark, control_consumer=control,
        poll_interval_seconds=0.01,
    )
    return monitor


@pytest.mark.parametrize("replicas", [1, 3, 4, 6])
def test_static_replicas_satisfy_monitor_coverage_and_drain(works_at, replicas) -> None:
    monitor = _monitor_for(works_at, replicas)
    producers = []
    for index in range(replicas):
        producer = _producer()
        shutdown = Event()
        shutdown.set()
        loader = EdgeLoader(
            _static_consumer(), MagicMock(), works_at, producer=producer, run_id="run-1",
            replica_id=str(index), static_assignment=(index, replicas), shutdown_requested=shutdown,
        )
        assert loader.run() == 0
        producers.append(producer)
    assignments = [p for producer in producers for p in _payloads(producer) if p["type"] != "DRAIN_COMPLETE"]
    for payload in assignments:
        monitor._decode_control(_encoded(payload))
    assert monitor._coverage() is True
    assert monitor._drain_complete() is False
    for producer in producers:
        for payload in _payloads(producer):
            if payload["type"] == "DRAIN_COMPLETE":
                monitor._decode_control(_encoded(payload))
    assert monitor._drain_complete() is True
    monitor._check_health(allow_drain=True)  # exited replicas are acknowledged drains


def test_monitor_coverage_requires_every_static_replica(works_at) -> None:
    monitor = _monitor_for(works_at, 2)
    producer = _producer()
    loader = EdgeLoader(
        _static_consumer(), MagicMock(), works_at, producer=producer, run_id="run-1",
        replica_id="0", static_assignment=(0, 2),
    )
    loader._publish_pending_assignments()
    monitor._decode_control(_encoded(_payloads(producer)[0]))
    assert monitor._coverage() is False  # partitions 1 and 3 are unclaimed


def _encoded(payload: dict):
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = json.dumps(payload).encode()
    return message


def _main_patches(schema, consumer, loader, producer=None):
    return (
        patch("src.loader.edge_loader.Consumer", return_value=consumer),
        patch("src.loader.edge_loader.Producer", return_value=producer or _producer()),
        patch("src.loader.edge_loader.get_neo4j_driver", return_value=MagicMock()),
        patch("src.loader.edge_loader.load_schema", return_value=schema),
        patch("src.loader.edge_loader.EdgeLoader", return_value=loader),
    )


def test_main_bulk_replica_count_requests_static_assignment(schema) -> None:
    consumer, loader = MagicMock(), MagicMock()
    loader.run.return_value = 0
    patches = _main_patches(schema, consumer, loader)
    with patches[0], patches[1], patches[2], patches[3], patches[4] as loader_cls:
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--mode", "bulk", "--run-id", "run-9",
            "--replica-id", "2", "--replica-count", "4",
        ]) == 0
    assert loader_cls.call_args.kwargs["static_assignment"] == (2, 4)
    consumer.subscribe.assert_not_called()


def test_main_without_replica_count_keeps_subscription(schema) -> None:
    consumer, loader = MagicMock(), MagicMock()
    loader.run.return_value = 0
    patches = _main_patches(schema, consumer, loader)
    with patches[0], patches[1], patches[2], patches[3], patches[4] as loader_cls:
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--mode", "stream", "--replica-id", "1"]) == 0
    assert loader_cls.call_args.kwargs["static_assignment"] is None


@pytest.mark.parametrize("argv", [
    ["--edge-type", "WORKS_AT", "--mode", "stream", "--replica-count", "2"],
    ["--edge-type", "WORKS_AT", "--mode", "bulk", "--replica-id", "4", "--replica-count", "4"],
    ["--edge-type", "WORKS_AT", "--mode", "bulk", "--replica-id", "x", "--replica-count", "4"],
    ["--edge-type", "WORKS_AT", "--mode", "bulk", "--replica-count", "0"],
    [
        "--edge-type", "WORKS_AT", "--mode", "bulk", "--replica-count", "2", "--slot-gating",
        "--coordination-topic", "graph.loader.coordination", "--fleet-edge-types", "WORKS_AT",
        "--run-id", "run-1",
    ],
])
def test_main_rejects_invalid_static_assignment_before_resources(argv) -> None:
    with patch("src.loader.edge_loader.Consumer") as consumer_cls, pytest.raises(SystemExit):
        edge_loader_main(argv)
    consumer_cls.assert_not_called()


def test_static_assignment_cannot_combine_with_slot_gating(works_at) -> None:
    from src.models.schema import CoordinationConfig
    from src.loader.slot_admission import SlotAdmission, SlotAwareAdmissionBuffer

    consumer = _static_consumer()
    with pytest.raises(ValueError, match="slot gating"):
        EdgeLoader(
            consumer, MagicMock(), works_at, run_id="run-slot",
            loading_config=LoadingConfig(
                unwind_batch_size=1,
                coordination=CoordinationConfig(bucket_count=64, slot_duration_ms=10, lease_timeout_ms=20),
            ),
            slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
            slot_buffer=SlotAwareAdmissionBuffer(max_records=2),
            coordination_poll=lambda _timeout: None, wall_clock_ms=lambda: 11,
            static_assignment=(0, 1),
        )
    consumer.assign.assert_not_called()


# --- commit-on-revoke for subscribe() loaders -------------------------------------


class _LeaseCoordinator:
    """Bucket-lease coordinator stand-in: routes and writes a whole flush."""

    routes_records = True

    def __init__(self, events: list[str], fail: bool = False) -> None:
        self.events, self.fail, self.calls = events, fail, 0

    def execute_pending(self, records):
        self.calls += 1
        self.events.append("lease-write:" + ",".join(f"{r.partition}@{r.offset}" for r in records))
        return (LaneExecutionResult(0, tuple(records), not self.fail, RuntimeError("cell") if self.fail else None),)

    def describe_last_flush(self) -> str:
        return "cells=1 transactions=1"


def _running_loader(works_at, **kwargs):
    consumer, writer = MagicMock(), MagicMock()
    events: list[str] = []
    writer.write_batch.side_effect = lambda records: events.append(f"write:{len(records)}")

    def commit(**commit_kwargs):
        events.append("commit:" + ",".join(f"{o.partition}@{o.offset}" for o in commit_kwargs["offsets"]))
        return commit_kwargs["offsets"]

    consumer.commit.side_effect = commit
    consumer.unassign.side_effect = lambda: events.append("unassign")
    loader = EdgeLoader(consumer, writer, works_at, **kwargs)
    loader._running = True  # as inside run()
    return loader, consumer, writer, events


def test_revoke_with_partial_batch_writes_and_commits_before_unassign(works_at) -> None:
    loader, consumer, _, events = _running_loader(
        works_at, loading_config=LoadingConfig(unwind_batch_size=100),
    )
    p0, p1 = TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)
    loader._on_assign(consumer, [p0, p1])
    assert loader._buffer_message(_message(_payload("p-001"), partition=0, offset=7))
    assert loader._buffer_message(_message(_payload("p-002"), partition=1, offset=2))

    loader._on_revoke(consumer, [p0, p1])

    assert events == ["write:2", "commit:0@8,1@3", "unassign"]
    assert loader._rebalance_failure is False
    assert not loader._batch and not loader._partition_ledgers


def test_revoke_during_post_write_poll_commits_once_without_rewrite(works_at) -> None:
    loader, consumer, writer, events = _running_loader(works_at)
    p0 = TopicPartition(TOPIC, 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_payload(), offset=3))

    def revoke_on_poll(_timeout):
        loader._on_revoke(consumer, [p0])
        return None

    consumer.poll.side_effect = revoke_on_poll

    assert loader._flush_batch() is True
    assert events == ["write:1", "commit:0@4", "unassign"]
    assert loader._rebalance_failure is False
    writer.write_batch.assert_called_once()


def test_lease_mode_flush_is_settled_once_on_revoke(works_at) -> None:
    events: list[str] = []
    coordinator = _LeaseCoordinator(events)
    loader, consumer, writer, events = _running_loader(
        works_at, coordinator=coordinator, loading_config=LoadingConfig(unwind_batch_size=100),
    )
    coordinator.events = events
    p0, p1 = TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)
    loader._on_assign(consumer, [p0, p1])
    assert loader._buffer_message(_message(_payload("p-001"), partition=0, offset=4))
    assert loader._buffer_message(_message(_payload("p-002"), partition=1, offset=9))

    def revoke_on_poll(_timeout):
        loader._on_revoke(consumer, [p0, p1])
        return None

    consumer.poll.side_effect = revoke_on_poll
    # The revoke fires in the post-write poll of a lease-mode flush.
    assert loader._flush_batch() is True
    assert events == ["lease-write:0@4,1@9", "commit:0@5,1@10", "unassign"]
    assert coordinator.calls == 1
    writer.write_batch.assert_not_called()


def test_lane_mode_partial_batch_is_settled_on_revoke(works_at) -> None:
    partitioner, batcher, coordinator = MagicMock(), MagicMock(), MagicMock()
    coordinator.routes_records = False
    routed = []
    partitioner.route.side_effect = lambda record, **kw: routed.append(SimpleNamespace(record=record, **kw)) or routed[-1]
    batcher.drain_all.side_effect = lambda: [tuple(routed)]
    coordinator.execute.side_effect = lambda lanes: tuple(
        LaneExecutionResult(i, lane, True) for i, lane in enumerate(lanes)
    )
    loader, consumer, writer, events = _running_loader(
        works_at, partitioner=partitioner, lane_batcher=batcher, coordinator=coordinator,
        loading_config=LoadingConfig(unwind_batch_size=100),
    )
    p0 = TopicPartition(TOPIC, 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_payload(), partition=0, offset=11))

    loader._on_revoke(consumer, [p0])

    coordinator.execute.assert_called_once()
    assert events == ["commit:0@12", "unassign"]
    assert loader._rebalance_failure is False


def test_revoke_drops_prefetched_messages_of_released_partitions(works_at) -> None:
    loader, consumer, _, events = _running_loader(works_at)
    p0 = TopicPartition(TOPIC, 0)
    loader._on_assign(consumer, [p0])
    loader._deferred_messages.extend([_message(_payload(), offset=5), _message(_payload(), offset=6)])

    loader._on_revoke(consumer, [p0])

    assert not loader._deferred_messages
    assert loader._rebalance_failure is False
    assert events == ["unassign"]  # nothing buffered: no write, no commit


@pytest.mark.parametrize("failure", ["write", "lease", "commit"])
def test_revoke_settlement_failure_still_fails_closed(works_at, failure) -> None:
    kwargs = {}
    if failure == "lease":
        kwargs["coordinator"] = _LeaseCoordinator([], fail=True)
    loader, consumer, writer, events = _running_loader(works_at, **kwargs)
    if failure == "write":
        writer.write_batch.side_effect = RuntimeError("neo4j down")
    if failure == "commit":
        consumer.commit.side_effect = RuntimeError("coordinator moved")
    p0 = TopicPartition(TOPIC, 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_payload(), offset=3))

    loader._on_revoke(consumer, [p0])

    assert loader._rebalance_failure is True
    if failure != "commit":
        consumer.commit.assert_not_called()
    assert events[-1] == "unassign"


def test_revoke_after_run_returned_never_writes_or_commits(works_at) -> None:
    loader, consumer, writer, _ = _running_loader(works_at)
    p0 = TopicPartition(TOPIC, 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_payload(), offset=3))
    loader._running = False  # e.g. consumer.close() after run() returned

    loader._on_revoke(consumer, [p0])

    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()
    assert loader._rebalance_failure is True


def test_run_flag_is_cleared_when_run_returns(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    loader = EdgeLoader(consumer, writer, works_at)
    assert loader.run(max_messages=1) == 0
    assert loader._running is False
    writes = writer.write_batch.call_count
    loader._on_assign(consumer, [TopicPartition(TOPIC, 0)])
    assert loader._buffer_message(_message(_payload(), offset=4))
    loader._on_revoke(consumer, [TopicPartition(TOPIC, 0)])
    assert writer.write_batch.call_count == writes


def test_partition_reassigned_later_commits_from_new_position(works_at) -> None:
    loader, consumer, _, events = _running_loader(works_at, loading_config=LoadingConfig(unwind_batch_size=100))
    p0 = TopicPartition(TOPIC, 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_payload(), offset=3))
    loader._on_revoke(consumer, [p0])
    # Another replica consumed 4..9 meanwhile; this replica resumes at 10.
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_payload(), offset=10))

    assert loader._flush_batch() is True
    assert events[-1] == "commit:0@11"


def test_rebalance_mid_run_does_not_fail_and_drain_follows_commits(works_at) -> None:
    events: list[str] = []
    producer = _producer(events)
    shutdown = Event()
    consumer, writer = MagicMock(), MagicMock()
    writer.write_batch.side_effect = lambda records: events.append("write")
    consumer.commit.side_effect = lambda **kwargs: (events.append("commit"), kwargs["offsets"])[1]
    loader = EdgeLoader(
        consumer, writer, works_at, producer=producer, shutdown_requested=shutdown,
        loading_config=LoadingConfig(unwind_batch_size=100, flush_interval_ms=60_000),
    )
    p0 = TopicPartition(TOPIC, 0)
    loader._on_assign(consumer, [p0])
    steps = iter(["msg", "revoke", "assign", "stop"])

    def poll(_timeout):
        step = next(steps, "stop")
        if step == "msg":
            return _message(_payload(), offset=3)
        if step == "revoke":
            loader._on_revoke(consumer, [p0])
        elif step == "assign":
            loader._on_assign(consumer, [p0])
        else:
            shutdown.set()
        return None

    consumer.poll.side_effect = poll

    assert loader.run() == 0
    assert events == [
        "control:ASSIGNMENT", "write", "commit", "control:IDLE_SURPLUS",
        "control:ASSIGNMENT", "control:DRAIN_COMPLETE",
    ]
    epochs = [p["assignment_epoch"] for p in _payloads(producer)]
    assert epochs == [1, 2, 3, 3]


def test_revoke_during_final_drain_publishes_epoch_before_drain_ack(works_at) -> None:
    events: list[str] = []
    producer = _producer(events)
    shutdown = Event()
    consumer, writer = MagicMock(), MagicMock()
    writer.write_batch.side_effect = lambda records: events.append("write")
    consumer.commit.side_effect = lambda **kwargs: (events.append("commit"), kwargs["offsets"])[1]
    loader = EdgeLoader(
        consumer, writer, works_at, producer=producer, shutdown_requested=shutdown,
        loading_config=LoadingConfig(unwind_batch_size=100, flush_interval_ms=60_000),
    )
    p0 = TopicPartition(TOPIC, 0)
    loader._on_assign(consumer, [p0])
    polls = iter(["msg", "stop"])

    def poll(_timeout):
        step = next(polls, "revoke")
        if step == "msg":
            return _message(_payload(), offset=3)
        if step == "stop":
            shutdown.set()
        else:  # a sibling left during the final drain: revoke in the post-write poll
            loader._on_revoke(consumer, [p0])
        return None

    consumer.poll.side_effect = poll

    assert loader.run() == 0
    assert events == [
        "control:ASSIGNMENT", "write", "commit", "control:IDLE_SURPLUS", "control:DRAIN_COMPLETE",
    ]
    payloads = _payloads(producer)
    assert payloads[-1]["assignment_epoch"] == payloads[-2]["assignment_epoch"] == 2


def test_slot_gated_revoke_still_fails_closed_while_running(works_at) -> None:
    from src.models.schema import CoordinationConfig
    from src.loader.slot_admission import SlotAdmission, SlotAwareAdmissionBuffer

    consumer, writer = MagicMock(), MagicMock()
    loader = EdgeLoader(
        consumer, writer, works_at, run_id="run-slot",
        loading_config=LoadingConfig(
            unwind_batch_size=1,
            coordination=CoordinationConfig(bucket_count=64, slot_duration_ms=10, lease_timeout_ms=20),
        ),
        slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
        slot_buffer=SlotAwareAdmissionBuffer(max_records=2),
        coordination_poll=lambda _timeout: None, wall_clock_ms=lambda: 11,
    )
    loader._running = True
    assigned = TopicPartition(TOPIC, 0)
    loader._on_assign(consumer, [assigned])
    assert loader._buffer_message(_message(_payload(), offset=3))  # unleased: buffered

    loader._on_revoke(consumer, [assigned])

    assert loader._rebalance_failure is True
    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()
