"""Unit tests for the pure schema-driven relationship record contract."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from confluent_kafka import TopicPartition
from threading import Event

from src.loader.edge_loader import (
    EdgeRecord,
    EdgeRecordValidationError,
    EdgeLoader,
    EdgeWriter,
    MissingRelationshipEndpointError,
    build_edge_upsert_query,
    main as edge_loader_main,
    normalize_edge_record,
)
from src.loader.edge_execution import LaneExecutionResult
from src.loader.node_loader import ControlDeliveryError, RejectionSink
from src.models.schema import EdgeConfig, LoadingConfig
from src.utils.schema_loader import load_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def schema():
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


@pytest.fixture(scope="module")
def works_at(schema):
    return next(edge for edge in schema.edges if edge.type == "WORKS_AT")


@pytest.fixture(scope="module")
def knows(schema):
    return next(edge for edge in schema.edges if edge.type == "KNOWS")


def test_normalizes_works_at_event(works_at) -> None:
    record = normalize_edge_record(
        {
            "source": {"personId": "p-001"},
            "target": {"companyId": "c-001"},
            "properties": {"since": "2020-01-02", "role": "Engineer"},
        },
        works_at,
    )
    assert record.source_key == "p-001"
    assert record.target_key == "c-001"
    assert record.properties == {"since": date(2020, 1, 2), "role": "Engineer"}


def test_normalizes_self_referencing_event(knows) -> None:
    record = normalize_edge_record(
        {
            "source": {"personId": "p-001"},
            "target": {"personId": "p-002"},
            "properties": {"since": "2020-01-02", "strength": 1},
        },
        knows,
    )
    assert (record.source_key, record.target_key) == ("p-001", "p-002")
    assert record.properties["strength"] == 1.0


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        {"source": {}, "target": {}, "properties": {}, "extra": {}},
        {"source": {}, "target": {}, "properties": None},
        {"source": {"personId": "p-001", "extra": "no"}, "target": {"companyId": "c-001"}, "properties": {"since": "2020-01-02"}},
        {"source": {"personId": None}, "target": {"companyId": "c-001"}, "properties": {"since": "2020-01-02"}},
        {"source": {"personId": "p-001"}, "target": {"companyId": "c-001"}, "properties": {"unknown": "no", "since": "2020-01-02"}},
        {"source": {"personId": "p-001"}, "target": {"companyId": "c-001"}, "properties": {"since": None}},
        {"source": {"personId": 1}, "target": {"companyId": "c-001"}, "properties": {"since": "2020-01-02"}},
        {"source": {"personId": "p-001"}, "target": {"companyId": 1}, "properties": {"since": "2020-01-02"}},
    ],
)
def test_invalid_envelopes_are_rejected(works_at, payload: object) -> None:
    with pytest.raises(EdgeRecordValidationError):
        normalize_edge_record(payload, works_at)


def test_optional_null_property_is_omitted(works_at) -> None:
    record = normalize_edge_record(
        {
            "source": {"personId": "p-001"},
            "target": {"companyId": "c-001"},
            "properties": {"since": "2020-01-02", "role": None},
        },
        works_at,
    )
    assert record.properties == {"since": date(2020, 1, 2)}


def test_validation_error_never_echoes_raw_value(works_at) -> None:
    with pytest.raises(EdgeRecordValidationError) as exc_info:
        normalize_edge_record(
            {
                "source": {"personId": "secret-payload"},
                "target": {"companyId": "c-001"},
                "properties": {"since": "not-a-date"},
            },
            works_at,
        )
    assert "secret-payload" not in str(exc_info.value)


def test_direct_edge_config_fails_without_graph_schema_binding() -> None:
    edge = EdgeConfig(
        type="WORKS_AT",
        topic="works-at-events",
        nodes={"source": "Person", "target": "Company"},
        source_key_property="personId",
        target_key_property="companyId",
        properties={"since": {"type": "date", "required": True}},
    )
    with pytest.raises(EdgeRecordValidationError, match="unresolved endpoint schema bindings"):
        normalize_edge_record(
            {
                "source": {"personId": "p-001"},
                "target": {"companyId": "c-001"},
                "properties": {"since": "2020-01-02"},
            },
            edge,
        )


def _record() -> EdgeRecord:
    return EdgeRecord(
        source_key="p-001",
        target_key="c-001",
        properties={"since": date(2020, 1, 2)},
    )


def _writer_with_preflight(works_at, preflight_rows: list[dict[str, int]]):
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value
    preflight = MagicMock()
    preflight.__iter__.return_value = iter(preflight_rows)
    upsert = MagicMock()
    transaction.run.side_effect = [preflight, upsert]
    return EdgeWriter(driver, works_at), driver, transaction, preflight, upsert


def test_build_edge_upsert_query_uses_declared_identifiers(works_at, knows) -> None:
    works_query = build_edge_upsert_query(works_at)
    knows_query = build_edge_upsert_query(knows)
    assert "MATCH (s:`Person` {`personId`: row.source_key})" in works_query
    assert "MATCH (t:`Company` {`companyId`: row.target_key})" in works_query
    assert "MERGE (s)-[r:`WORKS_AT`]->(t)" in works_query
    assert "MERGE (s)-[r:`KNOWS`]->(t)" in knows_query
    assert "CREATE" not in works_query


def test_writer_preflights_then_merges_and_commits(works_at) -> None:
    writer, driver, transaction, preflight, upsert = _writer_with_preflight(
        works_at, [{"source_matches": 1, "target_matches": 1}]
    )
    writer.write(_record())
    rows = [{"source_key": "p-001", "target_key": "c-001", "properties": {"since": date(2020, 1, 2)}}]
    assert transaction.run.call_count == 2
    assert transaction.run.call_args_list[0].kwargs["rows"] == rows
    assert transaction.run.call_args_list[1].kwargs["rows"] == rows
    preflight.consume.assert_called_once_with()
    upsert.consume.assert_called_once_with()
    transaction.commit.assert_called_once_with()
    driver.close.assert_not_called()


def test_writer_accepts_repeated_input_events_as_distinct_rows(works_at) -> None:
    writer, _driver, transaction, preflight, upsert = _writer_with_preflight(
        works_at,
        [
            {"row_index": 0, "source_matches": 1, "target_matches": 1},
            {"row_index": 1, "source_matches": 1, "target_matches": 1},
        ],
    )
    writer.write_batch([_record(), _record()])
    assert "row_index" in transaction.run.call_args_list[0].args[0]
    assert len(transaction.run.call_args_list[1].kwargs["rows"]) == 2
    preflight.consume.assert_called_once_with()
    upsert.consume.assert_called_once_with()
    transaction.commit.assert_called_once_with()


@pytest.mark.parametrize(
    "counts",
    [
        {"source_matches": 0, "target_matches": 1},
        {"source_matches": 1, "target_matches": 0},
        {"source_matches": 2, "target_matches": 1},
    ],
)
def test_missing_or_duplicate_endpoints_never_merge_or_commit(works_at, counts) -> None:
    writer, _driver, transaction, preflight, _upsert = _writer_with_preflight(works_at, [counts])
    with pytest.raises(MissingRelationshipEndpointError, match="WORKS_AT"):
        writer.write(_record())
    assert transaction.run.call_count == 1
    preflight.consume.assert_called_once_with()
    transaction.commit.assert_not_called()


def test_writer_propagates_query_failure_without_commit(works_at) -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value
    transaction.run.side_effect = RuntimeError("driver failed")
    with pytest.raises(RuntimeError, match="driver failed"):
        EdgeWriter(driver, works_at).write(_record())
    transaction.commit.assert_not_called()


def test_empty_batch_does_not_open_session(works_at) -> None:
    driver = MagicMock()
    EdgeWriter(driver, works_at).write_batch([])
    driver.session.assert_not_called()


def _message(
    payload: bytes,
    error=None,
    *,
    topic: str = "works-at-events",
    partition: int = 0,
    offset: int = 3,
):
    message = MagicMock()
    message.value.return_value = payload
    message.error.return_value = error
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    return message


def _payload() -> bytes:
    return (
        b'{"source":{"personId":"p-001"},"target":{"companyId":"c-001"},'
        b'"properties":{"since":"2020-01-02"}}'
    )


def test_edge_loader_writes_before_committing_next_offset(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=9)
    call_order: list[str] = []
    writer.write_batch.side_effect = lambda _records: call_order.append("write")
    consumer.commit.side_effect = lambda **_kwargs: call_order.append("commit")
    loader = EdgeLoader(consumer, writer, works_at)

    assert loader.run(max_messages=1) == 0
    consumer.subscribe.assert_called_once()
    writer.write_batch.assert_called_once()
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.topic, committed.partition, committed.offset) == ("works-at-events", 0, 10)
    assert consumer.commit.call_args.kwargs["asynchronous"] is False
    assert call_order == ["write", "commit"]


def test_edge_loader_rejects_malformed_before_resolving_offset(works_at) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    consumer.poll.return_value = _message(b"{bad", partition=2, offset=17)
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)

    assert loader.run(max_messages=1) == 0
    sink.append.assert_called_once()
    assert sink.append.call_args.kwargs["edge_type"] == "WORKS_AT"
    assert sink.append.call_args.kwargs["offset"] == 17
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (2, 18)
    writer.write_batch.assert_not_called()


def test_edge_rejection_sink_uses_edge_discriminator_and_fsync(tmp_path) -> None:
    path = tmp_path / "edge-rejections.jsonl"
    sink = RejectionSink(path)
    with patch("src.loader.node_loader.os.fsync") as fsync:
        sink.append(
            topic="works-at-events", partition=1, offset=3,
            edge_type="WORKS_AT", reason="invalid envelope",
        )
    entry = __import__("json").loads(path.read_text())
    assert entry["edge_type"] == "WORKS_AT"
    assert "node_label" not in entry
    fsync.assert_called_once()


def test_rejection_sink_preserves_node_json_shape_after_edge_extension(tmp_path) -> None:
    path = tmp_path / "node-rejections.jsonl"
    RejectionSink(path).append(
        topic="person-events", partition=1, offset=3,
        node_label="Person", reason="invalid record",
    )
    entry = __import__("json").loads(path.read_text())
    assert entry == {
        "topic": "person-events", "partition": 1, "offset": 3,
        "node_label": "Person", "reason": "invalid record",
    }


@pytest.mark.parametrize("failure", [MissingRelationshipEndpointError("missing"), RuntimeError("neo4j failed")])
def test_missing_endpoint_or_write_failure_leaves_offset_uncommitted(works_at, failure) -> None:
    consumer, writer = MagicMock(), MagicMock()
    writer.write_batch.side_effect = failure
    consumer.poll.side_effect = [_message(_payload(), offset=3), _message(_payload(), offset=4)]
    loader = EdgeLoader(consumer, writer, works_at)

    assert loader.run(max_messages=2) == 1
    assert consumer.poll.call_count == 1
    consumer.commit.assert_not_called()


def test_edge_loader_commits_independent_partitions(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.side_effect = [
        _message(_payload(), partition=1, offset=7),
        _message(_payload(), partition=2, offset=11),
    ]
    loader = EdgeLoader(
        consumer, writer, works_at, loading_config=LoadingConfig(unwind_batch_size=2)
    )

    assert loader.run(max_messages=2) == 0
    committed = [call.kwargs["offsets"][0] for call in consumer.commit.call_args_list]
    assert [(item.partition, item.offset) for item in committed] == [(1, 8), (2, 12)]


def test_later_malformed_offset_waits_behind_earlier_valid_gap(works_at) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)
    assert loader._buffer_message(_message(_payload(), offset=3))
    assert loader._buffer_message(_message(b"{bad", offset=4))
    consumer.commit.assert_not_called()

    assert loader._flush_batch() is True
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (0, 5)


def test_post_write_revoke_prevents_edge_offset_commit(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at)
    assigned = TopicPartition("works-at-events", 0)
    loader._on_assign(consumer, [assigned])
    assert loader._buffer_message(_message(_payload(), offset=3))

    def revoke_on_poll(_timeout):
        loader._on_revoke(consumer, [assigned])
        return None

    consumer.poll.side_effect = revoke_on_poll
    assert loader._flush_batch() is False
    consumer.commit.assert_not_called()
    consumer.unassign.assert_called_once()


def test_malformed_resolution_revoke_prevents_edge_offset_commit(works_at) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)
    assigned = TopicPartition("works-at-events", 0)
    loader._on_assign(consumer, [assigned])

    def revoke_on_poll(_timeout):
        loader._on_revoke(consumer, [assigned])
        return None

    consumer.poll.side_effect = revoke_on_poll
    assert loader._buffer_message(_message(b"{bad", offset=3)) is False
    sink.append.assert_called_once()
    consumer.commit.assert_not_called()


def test_edge_loader_idle_flushes_partial_batch(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    shutdown_requested = Event()
    message = _message(_payload(), offset=7)
    calls: list[str] = []

    def poll(_timeout):
        if consumer.poll.call_count == 1:
            return message
        shutdown_requested.set()
        return None

    consumer.poll.side_effect = poll
    writer.write_batch.side_effect = lambda _records: calls.append("write")
    clock_values = iter([0.0, 1.1])
    loader = EdgeLoader(
        consumer, writer, works_at, shutdown_requested=shutdown_requested,
        loading_config=LoadingConfig(unwind_batch_size=5, flush_interval_ms=1000),
        clock=lambda: next(clock_values),
    )
    assert loader.run() == 0
    writer.write_batch.assert_called_once()
    assert calls == ["write"]
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 8


def test_self_referencing_edge_uses_same_loader_path(knows) -> None:
    consumer, writer = MagicMock(), MagicMock()
    payload = (
        b'{"source":{"personId":"p-001"},"target":{"personId":"p-002"},'
        b'"properties":{"since":"2020-01-02"}}'
    )
    consumer.poll.return_value = _message(payload, topic="knows-events", offset=6)
    assert EdgeLoader(consumer, writer, knows).run(max_messages=1) == 0
    record = writer.write_batch.call_args.args[0][0]
    assert (record.source_key, record.target_key) == ("p-001", "p-002")


def test_edge_loader_fails_when_rejection_sink_is_not_durable(works_at) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    sink.append.side_effect = OSError("disk full")
    consumer.poll.return_value = _message(b"{bad", offset=4)
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)

    assert loader.run(max_messages=1) == 1
    consumer.commit.assert_not_called()


def test_edge_loader_main_uses_manual_commit_and_closes_resources(schema) -> None:
    consumer, driver, loader = MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer) as consumer_cls, \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader):
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--max-messages", "1", "--replica-id", "2"]) == 0
    config = consumer_cls.call_args.args[0]
    assert config["enable.auto.commit"] is False
    assert config["group.id"] == f"{schema.loading.consumer_group_id}-WORKS_AT"
    assert loader.run.call_args.kwargs == {"max_messages": 1}
    consumer.close.assert_called_once()
    driver.close.assert_called_once()


def test_edge_loader_main_rejects_unknown_type_and_invalid_cap() -> None:
    assert edge_loader_main(["--edge-type", "UNKNOWN", "--max-messages", "1"]) == 1
    with pytest.raises(SystemExit, match="positive"):
        edge_loader_main(["--edge-type", "WORKS_AT", "--max-messages", "0"])


def _control_producer() -> MagicMock:
    producer = MagicMock()
    producer.flush.return_value = 0
    return producer


def _control_payload(producer: MagicMock, call_index: int = 0) -> dict:
    return __import__("json").loads(producer.produce.call_args_list[call_index].kwargs["value"])


def test_edge_bulk_assignment_ack_is_run_scoped_and_topic_qualified(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    loader = EdgeLoader(
        consumer, writer, works_at, producer=producer, run_id="run-7", replica_id="3"
    )

    loader._on_assign(consumer, [TopicPartition("works-at-events", 2)])
    loader._publish_pending_assignments()

    payload = _control_payload(producer)
    assert payload == {
        "run_id": "run-7",
        "edge_type": "WORKS_AT",
        "replica_id": "3",
        "type": "ASSIGNMENT",
        "assignment_epoch": 1,
        "assigned_partitions": [{"topic": "works-at-events", "partition": 2}],
    }
    assert producer.produce.call_args.args[0] == "__graph_loader_control"
    assert producer.produce.call_args.kwargs["key"] == "run-7"


def test_edge_bulk_idle_surplus_and_revoke_advance_control_epoch(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    assignment = TopicPartition("works-at-events", 0)

    loader._on_assign(consumer, [assignment])
    loader._publish_pending_assignments()
    loader._on_revoke(consumer, [assignment])
    loader._publish_pending_assignments()

    assert _control_payload(producer, 0)["type"] == "ASSIGNMENT"
    revoke_payload = _control_payload(producer, 1)
    assert revoke_payload["type"] == "IDLE_SURPLUS"
    assert revoke_payload["assignment_epoch"] == 2
    consumer.unassign.assert_called_once()


def test_edge_partial_revoke_with_unassign_never_claims_remaining_partitions(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    first = TopicPartition("works-at-events", 0)
    second = TopicPartition("works-at-events", 1)

    loader._on_assign(consumer, [first, second])
    loader._publish_pending_assignments()
    loader._on_revoke(consumer, [first])
    loader._publish_pending_assignments()

    payload = _control_payload(producer, 1)
    assert payload["type"] == "IDLE_SURPLUS"
    assert "assigned_partitions" not in payload


def test_edge_assignment_control_delivery_failure_returns_nonzero(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    producer.flush.return_value = 1
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    loader._on_assign(consumer, [TopicPartition("works-at-events", 0)])

    assert loader.run(max_messages=1) == 1
    assert _control_payload(producer)["type"] == "ASSIGNMENT"
    assert producer.produce.call_count == 1


@pytest.mark.parametrize("failure_method", ["produce", "flush"])
def test_edge_control_producer_exception_returns_nonzero_without_drain(works_at, failure_method) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    setattr(producer, failure_method, MagicMock(side_effect=BufferError("broker unavailable")))
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    loader._on_assign(consumer, [TopicPartition("works-at-events", 0)])

    assert loader.run(max_messages=1) == 1
    if failure_method == "produce":
        producer.flush.assert_not_called()
    assert producer.produce.call_count == 1


def test_edge_sigterm_flushes_then_acknowledges_drain(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    shutdown_requested = Event()
    poll_count = 0

    def poll(_timeout):
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return _message(_payload(), offset=7)
        shutdown_requested.set()
        return None

    consumer.poll.side_effect = poll
    loader = EdgeLoader(
        consumer,
        writer,
        works_at,
        producer=producer,
        shutdown_requested=shutdown_requested,
        loading_config=LoadingConfig(unwind_batch_size=5),
    )
    assert loader.run() == 0
    writer.write_batch.assert_called_once()
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 8
    assert _control_payload(producer)["type"] == "DRAIN_COMPLETE"


@pytest.mark.parametrize("failure", [RuntimeError("write failed"), RuntimeError("commit failed")])
def test_edge_bulk_failure_never_acknowledges_drain(works_at, failure) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    consumer.poll.return_value = _message(_payload(), offset=3)
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    if str(failure) == "write failed":
        writer.write_batch.side_effect = failure
    else:
        consumer.commit.side_effect = failure

    assert loader.run(max_messages=1) == 1
    assert all(_control_payload(producer, index)["type"] != "DRAIN_COMPLETE"
               for index in range(producer.produce.call_count))


def test_edge_final_drain_delivery_failure_returns_nonzero(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    producer.flush.return_value = 1
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)

    assert loader.run(max_messages=0) == 1
    assert _control_payload(producer)["type"] == "DRAIN_COMPLETE"


def test_edge_stream_mode_has_no_control_producer_side_effects(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload())

    assert EdgeLoader(consumer, writer, works_at).run(max_messages=1) == 0


def test_edge_loader_main_bulk_passes_and_flushes_control_producer(schema) -> None:
    consumer, producer, driver, loader = MagicMock(), _control_producer(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.Producer", return_value=producer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader) as loader_cls:
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--mode", "bulk", "--run-id", "run-9", "--replica-id", "2"
        ]) == 0
    assert loader_cls.call_args.kwargs["producer"] is producer
    assert loader_cls.call_args.kwargs["run_id"] == "run-9"
    assert loader_cls.call_args.kwargs["replica_id"] == "2"
    producer.flush.assert_called_once()


def test_edge_loader_coordinator_success_commits_after_execution(works_at) -> None:
    consumer, writer, coordinator, partitioner, batcher = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    routed = MagicMock(); routed.lane_id = 1; routed.record = _record(); routed.topic = "works-at-events"; routed.partition = 0; routed.offset = 3
    partitioner.route.return_value = routed; batcher.drain_all.return_value = [(routed,)]
    coordinator.execute.return_value = (LaneExecutionResult(1, (routed,), True),)
    assert EdgeLoader(consumer, writer, works_at, partitioner=partitioner, lane_batcher=batcher, coordinator=coordinator).run(max_messages=1) == 0
    coordinator.execute.assert_called_once(); consumer.commit.assert_called_once()


def test_edge_loader_failed_coordinator_keeps_offsets_uncommitted(works_at) -> None:
    consumer, writer, coordinator, partitioner, batcher = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    routed = MagicMock(); routed.lane_id = 1; routed.record = _record(); routed.topic = "works-at-events"; routed.partition = 0; routed.offset = 3
    partitioner.route.return_value = routed; batcher.drain_all.return_value = [(routed,)]
    coordinator.execute.return_value = (LaneExecutionResult(1, (routed,), False, RuntimeError("boom")),)
    assert EdgeLoader(consumer, writer, works_at, partitioner=partitioner, lane_batcher=batcher, coordinator=coordinator).run(max_messages=1) == 1
    consumer.commit.assert_not_called()
