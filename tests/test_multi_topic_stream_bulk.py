"""v7-S2: stream and bulk classic/lease over every topic of a relationship type."""
from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml
from confluent_kafka import OFFSET_BEGINNING, TopicPartition
from prometheus_client import CollectorRegistry

from src import cli
from src.loader.edge_execution import ApocLockedEdgeWriter
from src.loader.edge_loader import (
    EdgeLoader,
    MissingRelationshipEndpointError,
    StaticAssignmentError,
    static_topic_partition_assignment,
    _resolve_cli_topics,
    build_parser,
    main as edge_loader_main,
)
from src.loader.edge_records import EdgeRecord
from src.loader.mix_and_batch import MixAndBatchPartitioner
from src.models.schema import EdgeConfig, GraphSchema, LoadingConfig
from src.orchestrator.bulk_monitor import OffsetResolutionError, QuiescentViolationError
from src.orchestrator.docker_service import DockerService
from src.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor
from src.utils.metrics import GraphLoaderMetrics
from src.utils.schema_loader import load_schema

_REAL_PREFLIGHT = cli._preflight_stream_topics  # conftest stubs the module attribute
ROOT = Path(__file__).resolve().parents[1]
T1, T2 = "works-at-a", "works-at-b"


@pytest.fixture(scope="module")
def schema():
    return load_schema(ROOT / "config" / "graph_schema.yaml")


def _multi_schema(**overrides) -> GraphSchema:
    """The repo's default schema with WORKS_AT fed by two topics (``None`` drops a key)."""
    data = yaml.safe_load((ROOT / "config" / "graph_schema.yaml").read_text())
    if overrides.get("topic_conflict") == "precedence":
        data.get("loading", {}).pop("mode", None)  # precedence is load-legal only when no stream mode is declared
    for edge in data["edges"]:
        if edge["type"] == "WORKS_AT":
            edge.pop("topic")
            edge.pop("dead_letter", None)
            edge["topics"] = [T1, T2]
            edge["order_by"] = "since"
            for key, value in overrides.items():
                if value is None:
                    edge.pop(key, None)
                else:
                    edge[key] = value
    return GraphSchema.model_validate(data)


def _multi_edge(schema, **overrides) -> EdgeConfig:
    return next(e for e in _multi_schema(**overrides).edges if e.type == "WORKS_AT")


@pytest.fixture
def edge(schema):
    return _multi_edge(schema)


def _payload(person: str = "p-001", company: str = "c-001", since: str = "2020-01-02") -> bytes:
    return json.dumps({
        "source": {"personId": person}, "target": {"companyId": company}, "properties": {"since": since},
    }).encode()


def _message(payload: bytes, *, topic: str, partition: int = 0, offset: int = 0):
    message = MagicMock()
    message.value.return_value = payload
    message.error.return_value = None
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    return message


def _metadata(mapping: dict[str, tuple[int, ...]]):
    return SimpleNamespace(topics={
        topic: SimpleNamespace(error=None, partitions={p: SimpleNamespace() for p in partitions})
        for topic, partitions in mapping.items()
    })


# --- Story 2.1: subscription, static assignment, per-(topic, partition) ledgers -------


def test_loader_subscribes_to_every_topic_of_the_type(edge) -> None:
    consumer = MagicMock()
    EdgeLoader(consumer, MagicMock(), edge)
    assert consumer.subscribe.call_args.args[0] == [T1, T2]


def test_legacy_single_topic_argument_and_single_topic_types_are_unchanged(schema) -> None:
    works_at = next(e for e in schema.edges if e.type == "WORKS_AT")
    consumer = MagicMock()
    EdgeLoader(consumer, MagicMock(), works_at)
    assert consumer.subscribe.call_args.args[0] == ["works-at-events"]
    consumer = MagicMock()
    EdgeLoader(consumer, MagicMock(), works_at, topic="override")
    assert consumer.subscribe.call_args.args[0] == ["override"]


@pytest.mark.parametrize("topics", [[], ["a", "a"], ["a", " "]])
def test_loader_rejects_bad_topic_lists(edge, topics) -> None:
    with pytest.raises(ValueError):
        EdgeLoader(MagicMock(), MagicMock(), edge, topics=topics)


def _static_consumer(mapping, committed=None):
    consumer = MagicMock()
    consumer.list_topics.side_effect = lambda topic, timeout: _metadata({topic: mapping[topic]}) if topic in mapping else _metadata({})
    committed = committed or {}
    consumer.committed.side_effect = lambda requested, timeout: [
        TopicPartition(item.topic, item.partition, committed.get((item.topic, item.partition), -1001))
        for item in requested
    ]
    consumer.poll.return_value = None
    return consumer


def test_static_assignment_covers_every_topic_and_resumes_per_topic_offsets(edge) -> None:
    consumer = _static_consumer({T1: (0, 1, 2, 3), T2: (0, 1)}, committed={(T1, 1): 42, (T2, 1): 7})
    producer = MagicMock()
    producer.flush.return_value = 0
    loader = EdgeLoader(
        consumer, MagicMock(), edge, producer=producer, run_id="run-1", replica_id="1", static_assignment=(1, 2),
    )
    consumer.subscribe.assert_not_called()
    assigned = consumer.assign.call_args.args[0]
    assert [(i.topic, i.partition, i.offset) for i in assigned] == [
        (T1, 1, 42), (T1, 3, OFFSET_BEGINNING), (T2, 1, 7),
    ]
    loader._publish_pending_assignments()
    payload = json.loads(producer.produce.call_args.kwargs["value"])
    assert payload["assigned_partitions"] == [
        {"topic": T1, "partition": 1}, {"topic": T1, "partition": 3}, {"topic": T2, "partition": 1},
    ]


def test_static_assignment_replicas_partition_every_topic_disjointly(edge) -> None:
    owned = []
    for index in range(3):
        consumer = _static_consumer({T1: (0, 1, 2, 3, 4), T2: (0, 1, 2)})
        EdgeLoader(consumer, MagicMock(), edge, run_id="r", replica_id=str(index), static_assignment=(index, 3))
        owned += [(i.topic, i.partition) for i in consumer.assign.call_args.args[0]]
    assert sorted(owned) == sorted([(T1, p) for p in range(5)] + [(T2, p) for p in range(3)])


def test_static_assignment_fails_closed_naming_the_topic_with_no_metadata(edge) -> None:
    consumer = _static_consumer({T1: (0, 1)})  # T2 missing
    with pytest.raises(StaticAssignmentError, match=T2):
        EdgeLoader(consumer, MagicMock(), edge, static_assignment=(0, 1))
    consumer.assign.assert_not_called()


def _running_loader(edge, **kwargs):
    consumer, writer = MagicMock(), MagicMock()
    events: list[str] = []
    writer.write_batch.side_effect = lambda records: events.append(f"write:{len(records)}")

    def commit(**commit_kwargs):
        events.append("commit:" + ",".join(f"{o.topic}/{o.partition}@{o.offset}" for o in commit_kwargs["offsets"]))
        return commit_kwargs["offsets"]

    consumer.commit.side_effect = commit
    consumer.unassign.side_effect = lambda: events.append("unassign")
    loader = EdgeLoader(consumer, writer, edge, **kwargs)
    loader._running = True
    return loader, consumer, writer, events


def test_mixed_topic_batch_commits_contiguous_prefix_per_topic_partition(edge) -> None:
    loader, consumer, _, events = _running_loader(edge, loading_config=LoadingConfig(unwind_batch_size=100))
    a0, b0 = TopicPartition(T1, 0), TopicPartition(T2, 0)
    loader._on_assign(consumer, [a0, b0])
    # Same partition number and overlapping offsets in two topics must never share a ledger.
    assert loader._buffer_message(_message(_payload("p-1"), topic=T1, offset=5))
    assert loader._buffer_message(_message(_payload("p-1"), topic=T2, offset=5))
    assert loader._buffer_message(_message(_payload("p-2"), topic=T2, offset=6))
    assert loader._flush_batch() is True
    assert events == ["write:3", f"commit:{T1}/0@6,{T2}/0@7"]


def test_revoke_of_one_topic_partition_settles_the_whole_buffer_first(edge) -> None:
    loader, consumer, _, events = _running_loader(edge, loading_config=LoadingConfig(unwind_batch_size=100))
    a0, b0 = TopicPartition(T1, 0), TopicPartition(T2, 0)
    loader._on_assign(consumer, [a0, b0])
    assert loader._buffer_message(_message(_payload("p-1"), topic=T1, offset=1))
    assert loader._buffer_message(_message(_payload("p-2"), topic=T2, offset=1))
    loader._on_revoke(consumer, [a0])
    # Nothing is lost: the whole buffered batch is written and committed before the unassign
    # (settle-before-revoke); the un-revoked topic's partition is committed too, not dropped.
    assert events[0] == "write:2"
    assert f"{T1}/0@2" in events[1] and f"{T2}/0@2" in events[1]
    assert events[-1] == "unassign"
    assert loader._rebalance_failure is False


def test_ownership_is_keyed_by_topic_and_partition(edge) -> None:
    loader, consumer, _, _ = _running_loader(edge, loading_config=LoadingConfig(unwind_batch_size=100))
    a0, b0 = TopicPartition(T1, 0), TopicPartition(T2, 0)
    loader._on_assign(consumer, [a0, b0])
    assert loader._owns((T1, 0)) and loader._owns((T2, 0))
    assert not loader._owns((T2, 9))


# --- CLI / container plumbing -------------------------------------------------------


def test_cli_topics_resolution(edge) -> None:
    parser = build_parser()
    assert _resolve_cli_topics(edge, parser.parse_args(["--edge-type", "X"])) == [T1, T2]
    assert _resolve_cli_topics(edge, parser.parse_args(["--edge-type", "X", "--topic", "solo"])) == ["solo"]
    assert _resolve_cli_topics(edge, parser.parse_args(["--edge-type", "X", "--topics", f"{T2}, {T1}"])) == [T2, T1]
    with pytest.raises(ValueError):
        _resolve_cli_topics(edge, parser.parse_args(["--edge-type", "X", "--topics", f"{T1},"]))
    with pytest.raises(ValueError):
        _resolve_cli_topics(edge, parser.parse_args(["--edge-type", "X", "--topic", "a", "--topics", "b,c"]))


def test_edge_loader_main_passes_all_topics_to_the_loader(edge) -> None:
    schema = _multi_schema()
    consumer, loader = MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
            patch("src.loader.edge_loader.Producer", return_value=MagicMock()), \
            patch("src.loader.edge_loader.get_neo4j_driver", return_value=MagicMock()), \
            patch("src.loader.edge_loader.load_schema", return_value=schema), \
            patch("src.loader.edge_loader.EdgeLoader", return_value=loader) as loader_cls:
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--mode", "stream"]) == 0
    assert loader_cls.call_args.kwargs["topics"] == [T1, T2]
    # one consumer group per type, not per topic
    assert consumer is not None


def test_edge_loader_main_refuses_precedence_in_stream_before_any_resource() -> None:
    schema = SimpleNamespace(edges=[SimpleNamespace(
        type="WORKS_AT", is_multi_topic=True, conflict_rule="precedence", topics=[T1, T2],
    )])
    with patch("src.loader.edge_loader.Consumer") as consumer_cls, \
            patch("src.loader.edge_loader.load_schema", return_value=schema), \
            patch("src.loader.edge_loader._select_edge_config", return_value=schema.edges[0]):
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--mode", "stream"]) == 1
    consumer_cls.assert_not_called()


def _service():
    service = DockerService.__new__(DockerService)
    service.client = MagicMock()
    return service


def test_run_edge_loader_passes_repeated_topics_only_for_multi_topic(tmp_path) -> None:
    config = tmp_path / "schema.yaml"
    config.write_text("x: 1")
    service = _service()
    service.client.containers.run.return_value = MagicMock()
    with patch("src.orchestrator.docker_service._mount_write_timing_directory", return_value=False):
        service.run_edge_loader(edge_type="WORKS_AT", topics=[T1, T2], mode="bulk", config_path=str(config),
                                rejection_dir=str(tmp_path / "rej"), environment={})
        multi = service.client.containers.run.call_args.kwargs["command"]
        service.run_edge_loader(edge_type="WORKS_AT", topics=["solo"], mode="bulk", config_path=str(config),
                                rejection_dir=str(tmp_path / "rej"), environment={})
        single = service.client.containers.run.call_args.kwargs["command"]
        service.run_edge_loader(edge_type="WORKS_AT", topic="solo", mode="bulk", config_path=str(config),
                                rejection_dir=str(tmp_path / "rej"), environment={})
        legacy = service.client.containers.run.call_args.kwargs["command"]
    assert "--topics" in multi and multi[multi.index("--topics") + 1] == f"{T1},{T2}" and "--topic" not in multi
    assert single[single.index("--topic") + 1] == "solo" and "--topics" not in single
    assert legacy == single
    with pytest.raises(ValueError):
        service.run_edge_loader(edge_type="W", topics=[T1, T1], mode="bulk", config_path=str(config))
    with pytest.raises(ValueError):
        service.run_edge_loader(edge_type="W", mode="bulk", config_path=str(config))


# --- Story 2.2: lanes and order_by are topic-agnostic ---------------------------------


def test_same_pair_from_two_topics_routes_to_one_lane(edge) -> None:
    partitioner = MixAndBatchPartitioner(edge)
    record = EdgeRecord("p-1", "c-1", {"since": "2020-01-01"})
    first = partitioner.route(record, topic=T1, partition=0, offset=3)
    second = partitioner.route(record, topic=T2, partition=2, offset=90)
    assert (first.lane_id, first.direction) == (second.lane_id, second.direction)
    assert first.topic != second.topic


def _writer(edge):
    return ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=MissingRelationshipEndpointError)


def test_order_by_collapse_keeps_newest_across_topics_and_spares_missing_values(edge) -> None:
    writer = _writer(edge)
    # Rows in consumption order; provenance (topic) is irrelevant: T1, T2, T2, T1, T2
    rows = [
        {"source_key": "a", "target_key": "b", "properties": {"since": 5, "role": "t1-old"}},   # T1
        {"source_key": "a", "target_key": "b", "properties": {"since": 9, "role": "t2-new"}},   # T2
        {"source_key": "a", "target_key": "b", "properties": {"role": "t2-no-order"}},           # T2, no order value
        {"source_key": "a", "target_key": "b", "properties": {"since": 7, "role": "t1-mid"}},   # T1
        {"source_key": "x", "target_key": "y", "properties": {"since": 1, "role": "solo"}},      # T2
    ]
    collapsed = writer._collapse_same_pair_rows_for_order_by(rows)
    assert collapsed[1]["properties"] == {"since": 9, "role": "t2-new"}
    assert collapsed[0]["properties"] == {} and collapsed[3]["properties"] == {}
    assert collapsed[2]["properties"] == {"role": "t2-no-order"}  # never emptied, never compared
    assert collapsed[4]["properties"] == {"since": 1, "role": "solo"}


# --- Story 2.3: boundary / zero-lag over all topics ---------------------------------


def _monitor(edge, *, mapping, watermarks, committed, metrics=None, replicas=1):
    admin, watermark, control = MagicMock(), MagicMock(), MagicMock()
    admin.list_topics.return_value = _metadata(mapping)
    watermark.get_watermark_offsets.side_effect = lambda tp, timeout=None: watermarks[(tp.topic, tp.partition)]
    state = {"committed": dict(committed)}

    def offsets(requests, require_stable=True):
        group = requests[0].group_id
        future = MagicMock()
        future.result.return_value = SimpleNamespace(topic_partitions=[
            TopicPartition(t, p, o) for (t, p), o in state["committed"].items()
        ])
        return {group: future}

    admin.list_consumer_group_offsets.side_effect = offsets
    control.poll.return_value = None
    container = MagicMock()
    container.attrs = {"State": {"Status": "running"}}
    kwargs = {"metrics": metrics} if metrics is not None else {}
    monitor = RelationshipBulkMonitor(
        [edge], "run-1", {("WORKS_AT", "0")}, {("WORKS_AT", "0"): container},
        bootstrap_servers="kafka:9092", consumer_group_prefix="graph-loader",
        admin_client=admin, watermark_consumer=watermark, control_consumer=control,
        poll_interval_seconds=0.01, **kwargs,
    )
    return monitor, state, admin


def _ack(monitor, pairs):
    payload = {
        "run_id": "run-1", "edge_type": "WORKS_AT", "replica_id": "0", "type": "ASSIGNMENT", "assignment_epoch": 1,
        "assigned_partitions": [{"topic": t, "partition": p} for t, p in pairs],
    }
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = json.dumps(payload).encode()
    monitor._decode_control(message)


MAPPING = {T1: (0, 1, 2), T2: (0, 1, 2)}


def test_topology_spans_every_topic_and_partition(edge) -> None:
    marks = {(t, p): (0, 10) for t in (T1, T2) for p in range(3)}
    monitor, _, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed={})
    assert monitor._expected == {("WORKS_AT", t, p) for t in (T1, T2) for p in range(3)}
    assert monitor._raw_expected["WORKS_AT"] == {(t, p) for t in (T1, T2) for p in range(3)}


def test_missing_topic_metadata_fails_the_stage_naming_the_topic(edge) -> None:
    with pytest.raises(OffsetResolutionError, match=T2):
        _monitor(edge, mapping={T1: (0, 1)}, watermarks={}, committed={})


def test_coverage_requires_every_topic_partition_to_be_claimed(edge) -> None:
    marks = {(t, p): (0, 10) for t in (T1, T2) for p in range(3)}
    monitor, _, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed={})
    _ack(monitor, [(T1, 0), (T1, 1), (T1, 2)])
    assert monitor._coverage() is False
    _ack_all = [(t, p) for t in (T1, T2) for p in range(3)]
    monitor2, _, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed={})
    _ack(monitor2, _ack_all)
    assert monitor2._coverage() is True


def test_one_lagging_topic_keeps_the_stage_open_and_full_drain_closes_it(edge) -> None:
    marks = {(t, p): (0, 10) for t in (T1, T2) for p in range(3)}
    done_a = {(T1, p): 10 for p in range(3)}
    monitor, state, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed={**done_a, (T2, 0): 10, (T2, 1): 10, (T2, 2): 4})
    _ack(monitor, [(t, p) for t in (T1, T2) for p in range(3)])
    monitor.capture_boundary()
    assert monitor._at_boundary() is False
    state["committed"][(T2, 2)] = 10
    assert monitor._at_boundary() is True


def test_empty_topic_does_not_block_the_boundary(edge) -> None:
    marks = {(T1, p): (0, 10) for p in range(3)}
    marks.update({(T2, p): (0, 0) for p in range(3)})   # never written
    committed = {(T1, p): 10 for p in range(3)}          # Kafka records no offset for T2
    monitor, _, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed=committed)
    _ack(monitor, [(t, p) for t in (T1, T2) for p in range(3)])
    monitor.capture_boundary()
    assert monitor._at_boundary() is True


def test_retention_emptied_topic_does_not_block_the_boundary(edge) -> None:
    marks = {(T1, p): (0, 10) for p in range(3)}
    marks.update({(T2, p): (50, 50) for p in range(3)})  # trimmed empty: low == high > 0
    committed = {(T1, p): 10 for p in range(3)}
    monitor, _, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed=committed)
    _ack(monitor, [(t, p) for t in (T1, T2) for p in range(3)])
    monitor.capture_boundary()
    assert monitor._at_boundary() is True
    monitor.verify_zero_lag()  # the final zero-lag proof accepts the same empty partitions


def test_late_record_in_an_empty_partition_after_the_snapshot_fails_the_stage(edge) -> None:
    marks = {(T1, p): (0, 10) for p in range(3)}
    marks.update({(T2, p): (50, 50) for p in range(3)})
    committed = {(T1, p): 10 for p in range(3)}
    monitor, _, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed=committed)
    _ack(monitor, [(t, p) for t in (T1, T2) for p in range(3)])
    monitor.capture_boundary()
    marks[(T2, 1)] = (50, 51)  # record lands after the snapshot
    with pytest.raises(QuiescentViolationError):
        monitor.verify_zero_lag()
    with pytest.raises(QuiescentViolationError):
        monitor._at_boundary()


def test_reused_run_id_commit_at_or_below_low_of_an_empty_partition_is_done(edge) -> None:
    marks = {(T1, p): (0, 10) for p in range(3)}
    marks.update({(T2, p): (50, 50) for p in range(3)})
    committed = {(T1, p): 10 for p in range(3)}
    committed.update({(T2, p): 20 for p in range(3)})  # stale commit from before retention trimmed it
    monitor, _, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed=committed)
    _ack(monitor, [(t, p) for t in (T1, T2) for p in range(3)])
    monitor.capture_boundary()
    assert monitor._at_boundary() is True
    monitor.verify_zero_lag()


def test_uncommitted_nonempty_partition_still_blocks(edge) -> None:
    marks = {(t, p): (0, 10) for t in (T1, T2) for p in range(3)}
    committed = {(T1, p): 10 for p in range(3)}          # T2 has data but no commit
    monitor, _, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed=committed)
    _ack(monitor, [(t, p) for t in (T1, T2) for p in range(3)])
    monitor.capture_boundary()
    assert monitor._at_boundary() is False


# --- Story 2.5: one lag series per (type, topic), pre-touched -----------------------


def test_lag_series_exist_per_topic_at_startup_and_track_each_topic(edge) -> None:
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    marks = {(t, p): (0, 10) for t in (T1, T2) for p in range(3)}
    committed = {(T1, p): 10 for p in range(3)}
    monitor, _, _ = _monitor(edge, mapping=MAPPING, watermarks=marks, committed=committed, metrics=metrics)

    def sample(topic):
        return metrics.registry.get_sample_value(
            "graph_loader_consumer_lag_records", {"kind": "cli", "name": "WORKS_AT", "topic": topic},
        )

    assert sample(T1) is None and sample(T2) is None  # a bulk stage never exports the gauge
    monitor._pretouch_lag_series()  # stream supervision does, at its start
    assert sample(T1) == 0.0 and sample(T2) == 0.0  # present (not absent) before any refresh
    series = [s for family in metrics.registry.collect() if family.name == "graph_loader_consumer_lag_records"
              for s in family.samples]
    assert len(series) == 2

    monitor._refresh_consumer_lag()
    assert sample(T1) == 0.0
    assert sample(T2) == 30.0
    series = [s for family in metrics.registry.collect() if family.name == "graph_loader_consumer_lag_records"
              for s in family.samples]
    assert len(series) == 2


# --- CLI start: preflight, precedence, call sites ------------------------------------


def test_preflight_lists_every_topic_of_a_multi_topic_type(edge) -> None:
    schema = SimpleNamespace(
        nodes=[SimpleNamespace(topic="person-events")],
        edges=[SimpleNamespace(type="WORKS_AT", topics=[T1, T2])],
    )
    admin = MagicMock()
    admin.list_topics.return_value.topics = {"person-events": SimpleNamespace(error=None), T1: SimpleNamespace(error=None)}
    with patch("confluent_kafka.admin.AdminClient", return_value=admin):
        assert cli._missing_schema_topics(schema, "kafka:9092") == [T2]
        assert _REAL_PREFLIGHT(schema, "kafka:9092") is False


def test_preflight_does_not_blame_the_broker_for_schema_errors() -> None:
    class Bad:
        type = "X"

        @property
        def topics(self):
            raise ValueError("boom: not a broker problem")

    schema = SimpleNamespace(nodes=[], edges=[Bad()])
    with patch("confluent_kafka.admin.AdminClient") as admin_cls, pytest.raises(ValueError, match="boom"):
        _REAL_PREFLIGHT(schema, "kafka:9092")
    admin_cls.assert_not_called()


def _precedence_schema(edge=None):
    return _multi_schema(order_by=None, topic_conflict="precedence")


@pytest.mark.parametrize("mode,edge_plan,ok", [
    ("stream", None, False), ("bulk", None, True), ("bulk", "plan", True),
])
def test_start_time_precedence_check(edge, mode, edge_plan, ok) -> None:
    schema = _precedence_schema(edge)
    args = Namespace(mode=mode, edge_plan=edge_plan)
    if ok:
        cli._require_supported_topic_conflicts(schema, args)
    else:
        with pytest.raises(ValueError, match="precedence"):
            cli._require_supported_topic_conflicts(schema, args)


def test_order_by_and_arrival_types_pass_the_start_time_check(edge) -> None:
    arrival = _multi_schema(order_by=None, topic_conflict="arrival")
    for mode in ("stream", "bulk"):
        cli._require_supported_topic_conflicts(_multi_schema(), Namespace(mode=mode, edge_plan=None))
        cli._require_supported_topic_conflicts(arrival, Namespace(mode=mode, edge_plan=None))


def test_handle_start_refuses_precedence_in_stream_before_any_launch(edge) -> None:
    schema = _precedence_schema(edge)
    args = Namespace(mode="stream", config="c.yaml", network="n", skip_image_build=True)
    with patch.object(cli, "load_schema", return_value=schema), \
            patch.object(cli, "_preflight_stream_topics") as preflight, \
            patch.object(cli, "get_neo4j_credentials") as creds:
        assert cli.handle_start(args) == 1
    preflight.assert_not_called()
    creds.assert_not_called()


def test_dead_letter_provisioning_does_not_raise_for_a_multi_topic_type_without_dlq(edge) -> None:
    schema = _multi_schema()
    with patch.object(cli, "ensure_dead_letter_topic") as ensure:
        cli._ensure_dead_letter_topics(schema, "kafka:9092", include_edges=True)
        cli._ensure_retry_topics(schema, "kafka:9092")
    ensure.assert_not_called()


def test_loader_rejects_topic_and_topics_together(edge) -> None:
    with pytest.raises(ValueError, match="not both"):
        EdgeLoader(MagicMock(), MagicMock(), edge, topic=T1, topics=[T1, T2])


@pytest.mark.parametrize("sizes,count", [
    ({T1: 1, T2: 1}, 2), ({T1: 2, T2: 1}, 3), ({T1: 4, T2: 3}, 5), ({T1: 4, T2: 3}, 8), ({T1: 1, T2: 1, "c": 1}, 1),
])
def test_static_round_robin_is_exactly_once_and_spreads_small_topics(sizes, count) -> None:
    pairs = [(t, p) for t, n in sizes.items() for p in range(n)]
    owned = [static_topic_partition_assignment(pairs, i, count) for i in range(count)]
    flat = [item for part in owned for item in part]
    assert sorted(flat) == sorted(pairs) and len(flat) == len(set(flat))
    assert all(len(part) >= 1 for part in owned[: min(count, len(pairs))])  # no idle replica while work remains
    assert owned == [static_topic_partition_assignment(list(reversed(pairs)), i, count) for i in range(count)]  # deterministic


def test_static_round_robin_single_topic_equals_modulo() -> None:
    from src.loader.edge_loader import static_partition_assignment
    for count in (1, 2, 3, 4, 7):
        for i in range(count):
            assert [p for _, p in static_topic_partition_assignment([("t", p) for p in range(6)], i, count)] == \
                list(static_partition_assignment(range(6), i, count))


# --- start-time checks (multi-topic + dead_letter accepted since v7-S4; precedence) -----


@pytest.mark.parametrize("mode", ["bulk", "stream"])
def test_multi_topic_type_with_dead_letter_is_accepted(mode) -> None:
    """v7-S4 lifted the S2 refusal: one DLQ per type."""
    schema = _multi_schema(dead_letter={"enabled": True})
    cli._require_supported_topic_conflicts(schema, Namespace(mode=mode, edge_plan=None))


def test_stream_accepts_another_dead_letter_type_next_to_a_multi_topic_type() -> None:
    raw = _multi_schema().model_dump(exclude_unset=True)
    other = next(e for e in raw['edges'] if e['type'] != 'WORKS_AT')['type']
    for edge in raw["edges"]:
        if edge["type"] == other:
            edge["dead_letter"] = {"enabled": True}
    cli._require_supported_topic_conflicts(GraphSchema.model_validate(raw), Namespace(mode="stream", edge_plan=None))


def test_bulk_precedence_with_dead_letter_is_accepted() -> None:
    schema = _multi_schema(order_by=None, topic_conflict="precedence", dead_letter={"enabled": True})
    cli._require_supported_topic_conflicts(schema, Namespace(mode="bulk", edge_plan=None, edge_classic=True))


def test_multi_topic_dlq_is_provisioned_once_sized_over_every_topic() -> None:
    schema = _multi_schema(dead_letter={"enabled": True, "max_retry_attempts": 3}, order_by=None,
                           topic_conflict="arrival")
    with patch.object(cli, "ensure_dead_letter_topic") as ensure, \
            patch.object(cli, "ensure_parked_pair_index_topic") as ensure_index:
        cli._ensure_dead_letter_topics(schema, "kafka:9092", include_edges=True)
        cli._ensure_retry_topics(schema, "kafka:9092")
    dlq = [c for c in ensure.call_args_list if c.args[1] == "WORKS_AT.dlq"]
    assert len(dlq) == 1 and dlq[0].kwargs == {"source_topics": [T1, T2]}
    parked = [c for c in ensure.call_args_list if c.args[1] == "WORKS_AT.dlq.parked"]
    assert len(parked) == 1 and parked[0].kwargs == {"source_topic": "WORKS_AT.dlq"}
    ensure_index.assert_called_once_with("kafka:9092", "WORKS_AT.dlq.parked-pairs", source_topic="WORKS_AT.dlq")


def test_stream_precedence_message_points_at_bulk_modes() -> None:
    with pytest.raises(ValueError, match="bulk mode only"):
        cli._require_supported_topic_conflicts(_precedence_schema(), Namespace(mode="stream", edge_plan=None))


def test_planned_runs_ignore_edge_dead_letter_for_the_start_check() -> None:
    schema = _multi_schema(dead_letter={"enabled": True})
    cli._require_supported_topic_conflicts(schema, Namespace(mode="bulk", edge_plan="plan"))
