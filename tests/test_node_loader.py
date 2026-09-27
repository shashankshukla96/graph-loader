"""Unit tests for the pure schema-driven node record contract."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from threading import Event
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from confluent_kafka import TopicPartition
from neo4j.exceptions import ClientError, TransientError

from src.loader.node_loader import (
    ControlDeliveryError,
    NodeRecordValidationError,
    NodeWriter,
    NodeLoader,
    RejectionSink,
    build_node_upsert_query,
    normalize_node_record,
    main as node_loader_main,
)
from src.models.schema import LoadingConfig
from src.utils.schema_loader import load_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def schema():
    """Load the canonical schema through the public schema-loader API."""
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


@pytest.fixture(scope="module")
def person_config(schema):
    return next(node for node in schema.nodes if node.label == "Person")


@pytest.fixture(scope="module")
def company_config(schema):
    return next(node for node in schema.nodes if node.label == "Company")


def test_normalizes_complete_person_record(person_config) -> None:
    record = normalize_node_record(
        {
            "personId": "p-001",
            "name": "Ada",
            "age": 37,
            "salary": 12.5,
            "birthDate": "1990-01-02",
            "lastLogin": "2026-09-13T12:34:56Z",
        },
        person_config,
    )

    assert record.key == "p-001"
    assert record.properties["birthDate"] == date(1990, 1, 2)
    assert record.properties["lastLogin"] == datetime(2026, 9, 13, 12, 34, 56, tzinfo=record.properties["lastLogin"].tzinfo)
    assert record.properties["lastLogin"].utcoffset() is not None


def test_normalizes_company_with_its_configured_key(company_config) -> None:
    record = normalize_node_record(
        {"companyId": "c-001", "name": "Acme", "foundedYear": 2001}, company_config
    )

    assert record.key == "c-001"
    assert record.properties["companyId"] == "c-001"


def test_optional_null_is_omitted(person_config) -> None:
    record = normalize_node_record(
        {"personId": "p-001", "name": "Ada", "age": None}, person_config
    )

    assert "age" not in record.properties
    assert record.properties["personId"] == "p-001"


def test_integer_input_normalizes_to_float(person_config) -> None:
    record = normalize_node_record(
        {"personId": "p-001", "name": "Ada", "salary": 7}, person_config
    )

    assert record.properties["salary"] == 7.0
    assert isinstance(record.properties["salary"], float)


@pytest.mark.parametrize("record", [{"name": "Ada"}, {"personId": None, "name": "Ada"}, {"personId": "p-001"}])
def test_missing_or_null_required_values_fail(person_config, record) -> None:
    with pytest.raises(NodeRecordValidationError, match="required"):
        normalize_node_record(record, person_config)


def test_unknown_field_does_not_echo_its_value(person_config) -> None:
    with pytest.raises(NodeRecordValidationError) as exc_info:
        normalize_node_record(
            {"personId": "p-001", "name": "Ada", "unknown": "secret-payload"}, person_config
        )

    assert "unknown" in str(exc_info.value)
    assert "secret-payload" not in str(exc_info.value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(True, id="bool"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="infinity"),
        pytest.param(-(10**10000), id="negative-overflowing-integer"),
        pytest.param(10**10000, id="overflowing-integer"),
    ],
)
def test_invalid_float_values_fail(person_config, value) -> None:
    with pytest.raises(NodeRecordValidationError, match="salary"):
        normalize_node_record({"personId": "p-001", "name": "Ada", "salary": value}, person_config)


def test_boolean_is_not_an_integer(person_config) -> None:
    with pytest.raises(NodeRecordValidationError, match="age"):
        normalize_node_record({"personId": "p-001", "name": "Ada", "age": True}, person_config)


@pytest.mark.parametrize(
    ("property_name", "value"),
    [("name", 123), ("age", "37"), ("salary", "12.5")],
)
def test_invalid_scalar_types_fail(person_config, property_name: str, value: object) -> None:
    record = {"personId": "p-001", "name": "Ada", property_name: value}
    with pytest.raises(NodeRecordValidationError, match=property_name):
        normalize_node_record(record, person_config)


@pytest.mark.parametrize("value", ["2026-02-30", "not-a-date"])
def test_invalid_dates_fail(person_config, value: str) -> None:
    with pytest.raises(NodeRecordValidationError, match="birthDate"):
        normalize_node_record({"personId": "p-001", "name": "Ada", "birthDate": value}, person_config)


@pytest.mark.parametrize(
    "value",
    ["2026-09-13 12:34:56+00:00", "2026-09-13X12:34:56+00:00", "2026-09-13T12:34:56", "not-a-datetime"],
)
def test_invalid_or_naive_datetimes_fail(person_config, value: str) -> None:
    with pytest.raises(NodeRecordValidationError, match="lastLogin"):
        normalize_node_record({"personId": "p-001", "name": "Ada", "lastLogin": value}, person_config)


def test_build_node_upsert_query_uses_person_schema_identifiers(person_config) -> None:
    query = build_node_upsert_query(person_config)

    assert "UNWIND $batch AS record" in query
    assert "MERGE (n:`Person` {`personId`: record.key})" in query
    assert "SET n += record.properties" in query


def test_build_node_upsert_query_uses_company_schema_identifiers(company_config) -> None:
    query = build_node_upsert_query(company_config)

    assert "MERGE (n:`Company` {`companyId`: record.key})" in query
    assert "Person" not in query


def test_node_writer_uses_singleton_batch_and_closes_session(person_config) -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value
    record = normalize_node_record({"personId": "p-001", "name": "Ada"}, person_config)

    NodeWriter(driver, person_config).write(record)

    transaction.run.assert_called_once_with(
        build_node_upsert_query(person_config),
        batch=[{"key": "p-001", "properties": {"personId": "p-001", "name": "Ada"}}],
    )
    session.begin_transaction.assert_called_once()
    transaction.run.return_value.consume.assert_called_once_with()
    transaction.commit.assert_called_once_with()
    driver.close.assert_not_called()
    driver.session.return_value.__exit__.assert_called_once()


def test_node_writer_propagates_transaction_error_and_closes_session(person_config) -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value
    transaction.run.side_effect = RuntimeError("write failed")
    record = normalize_node_record({"personId": "p-001", "name": "Ada"}, person_config)

    with pytest.raises(RuntimeError, match="write failed"):
        NodeWriter(driver, person_config).write(record)

    driver.close.assert_not_called()
    driver.session.return_value.__exit__.assert_called_once()


def _message(
    payload: bytes,
    error=None,
    *,
    topic: str = "person-events",
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


def test_node_loader_subscribes_and_commits_after_write(person_config):
    consumer, writer = MagicMock(), MagicMock()
    calls = []
    writer.write.side_effect = lambda record: calls.append("write")
    consumer.commit.side_effect = lambda **kwargs: calls.append("commit")
    loader = NodeLoader(consumer, writer, person_config)
    message = _message(b'{"personId":"p-1","name":"Ada"}')

    assert loader.process_message(message) is True
    consumer.subscribe.assert_called_once()
    assert consumer.subscribe.call_args[0][0] == ["person-events"]
    assert "on_assign" in consumer.subscribe.call_args[1]
    assert calls == ["write", "commit"]
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)


@pytest.mark.parametrize("payload", [b"[1]", b"1", b"null", b"{bad", b'{"personId":"p-1","name":NaN}'])
def test_node_loader_fails_closed_for_bad_payload(person_config, payload):
    consumer, writer = MagicMock(), MagicMock()
    loader = NodeLoader(consumer, writer, person_config)
    assert loader.process_message(_message(payload)) is False
    writer.write.assert_not_called()
    consumer.commit.assert_not_called()


def test_node_loader_durably_rejects_malformed_then_processes_later_message(person_config):
    consumer, writer, producer, rejection_sink = (
        MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    producer.flush.return_value = 0
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        rejection_sink=rejection_sink,
    )
    consumer.poll.side_effect = [
        _message(b"{bad", offset=3),
        _message(b'{"personId":"p-2","name":"Ada"}', offset=4),
    ]

    assert loader.run(max_messages=2) == 0

    assert consumer.poll.call_count == 2
    rejection_sink.append.assert_called_once()
    writer.write_batch.assert_called_once()
    assert [call.kwargs["offsets"][0].offset for call in consumer.commit.call_args_list] == [4, 5]


@pytest.mark.parametrize("failure", ["message", "write", "commit"])
def test_node_loader_failure_paths_stop_before_later_commit(person_config, failure):
    consumer, writer = MagicMock(), MagicMock()
    loader = NodeLoader(consumer, writer, person_config)
    first = _message(b'{"personId":"p-1","name":"Ada"}', error="broker error" if failure == "message" else None)
    later = _message(b'{"personId":"p-2","name":"Later"}')
    if failure == "write":
        writer.write_batch.side_effect = RuntimeError("write failed")
    if failure == "commit":
        consumer.commit.side_effect = RuntimeError("commit failed")
    consumer.poll.side_effect = [first, later]
    assert loader.run(max_messages=2) == 1
    assert consumer.poll.call_count == 1
    if failure != "commit":
        consumer.commit.assert_not_called()
    else:
        consumer.commit.assert_called_once()


def test_node_loader_main_rejects_unknown_label_and_invalid_cap():
    assert node_loader_main(["--node-label", "Unknown", "--max-messages", "1"]) == 1
    with pytest.raises(SystemExit, match="positive"):
        node_loader_main(["--node-label", "Person", "--max-messages", "0"])


def test_node_loader_main_closes_resources_when_subscription_fails(schema):
    consumer, driver, producer = MagicMock(), MagicMock(), MagicMock()
    with patch("src.loader.node_loader.Consumer", return_value=consumer), \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.NodeLoader", side_effect=RuntimeError("subscribe failed")):
        assert node_loader_main(["--node-label", "Person"]) == 1
    consumer.close.assert_called_once()
    producer.flush.assert_called_once()
    driver.close.assert_called_once()


def test_node_loader_main_uses_config_and_closes_resources(schema):
    consumer, driver, loader, producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.node_loader.Consumer", return_value=consumer) as consumer_cls, \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.NodeLoader", return_value=loader) as loader_cls:
        assert node_loader_main(["--node-label", "Person", "--max-messages", "1", "--run-id", "test-run", "--replica-id", "2"]) == 0
    config = consumer_cls.call_args.args[0]
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["group.id"] == f"{schema.loading.consumer_group_id}-Person"
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "earliest"
    assert config["max.poll.interval.ms"] == schema.loading.max_poll_interval_ms
    assert config["session.timeout.ms"] == schema.loading.session_timeout_ms

    loader_kwargs = loader_cls.call_args.kwargs
    assert loader_kwargs["run_id"] == "test-run"
    assert loader_kwargs["replica_id"] == "2"
    assert loader_kwargs["loading_config"] is schema.loading

    loader.run.assert_called_once_with(max_messages=1)
    consumer.close.assert_called_once()
    producer.flush.assert_called_once()
    driver.close.assert_called_once()


def test_node_loader_main_subscribes_only_to_selected_topic(schema, monkeypatch):
    consumer, driver, producer = MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-test:9092")
    with patch("src.loader.node_loader.Consumer", return_value=consumer) as consumer_cls, \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch.object(NodeLoader, "run", return_value=0):
        assert node_loader_main(["--node-label", "Person", "--max-messages", "1"]) == 0
    assert consumer_cls.call_args.args[0]["bootstrap.servers"] == "kafka-test:9092"
    consumer.subscribe.assert_called_once()
    assert consumer.subscribe.call_args[0][0] == ["person-events"]

import json

def test_publish_control(person_config):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    loader = NodeLoader(consumer, writer, person_config, producer=producer, run_id="run-1", replica_id="0")

    loader._publish_control("TEST_MSG", {"foo": "bar"}, flush=True)

    producer.produce.assert_called_once()
    call_args = producer.produce.call_args.kwargs
    assert call_args["key"] == "run-1"

    payload = json.loads(call_args["value"])
    assert payload["run_id"] == "run-1"
    assert payload["node_label"] == "Person"
    assert payload["replica_id"] == "0"
    assert payload["type"] == "TEST_MSG"
    assert payload["assignment_epoch"] == 0
    assert payload["foo"] == "bar"

    producer.flush.assert_called_once()


def test_on_assign_flags_publish(person_config):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    loader = NodeLoader(consumer, writer, person_config, producer=producer)

    p1 = MagicMock()
    p1.topic = "person-events"
    p1.partition = 1

    loader._on_assign(consumer, [p1])

    assert loader._assignment_epoch == 1
    assert list(loader._pending_assignment_events) == [
        (1, [{"topic": "person-events", "partition": 1}])
    ]
    consumer.assign.assert_called_once_with([p1])

    # Should publish in run loop and then exit
    consumer.poll.side_effect = KeyboardInterrupt()
    loader.run(max_messages=1)

    assert producer.produce.call_count == 1
    first_call_value = producer.produce.call_args_list[0].kwargs["value"]
    payload = json.loads(first_call_value)
    assert payload["type"] == "ASSIGNMENT"
    assert payload["assigned_partitions"] == [{"topic": "person-events", "partition": 1}]


def test_on_assign_idle_surplus(person_config):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    loader = NodeLoader(consumer, writer, person_config, producer=producer)

    loader._on_assign(consumer, [])

    assert loader._assignment_epoch == 1
    assert list(loader._pending_assignment_events) == [(1, [])]
    consumer.assign.assert_called_once_with([])

    consumer.poll.side_effect = KeyboardInterrupt()
    loader.run(max_messages=1)

    assert producer.produce.call_count == 1
    first_call_value = producer.produce.call_args_list[0].kwargs["value"]
    payload = json.loads(first_call_value)
    assert payload["type"] == "IDLE_SURPLUS"


def test_shutdown_event_publishes_drain_complete(person_config):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    shutdown_requested = Event()
    shutdown_requested.set()
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        shutdown_requested=shutdown_requested,
    )

    assert loader.run(max_messages=5) == 0

    consumer.poll.assert_not_called()
    producer.produce.assert_called_once()
    payload = json.loads(producer.produce.call_args.kwargs["value"])
    assert payload["type"] == "DRAIN_COMPLETE"
    producer.flush.assert_called_once()


def test_control_publish_fails_when_flush_leaves_messages_undelivered(person_config):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 1
    loader = NodeLoader(consumer, writer, person_config, producer=producer)

    with pytest.raises(ControlDeliveryError, match="undelivered"):
        loader._publish_control("ASSIGNMENT", {})


def test_control_publish_propagates_producer_error(person_config):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.produce.side_effect = RuntimeError("broker unavailable")
    loader = NodeLoader(consumer, writer, person_config, producer=producer)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        loader._publish_control("ASSIGNMENT", {})


def test_control_publish_fails_on_delivery_callback_error(person_config):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0

    def produce(*_args, **kwargs):
        kwargs["on_delivery"](RuntimeError("authorization failed"), MagicMock())

    producer.produce.side_effect = produce
    loader = NodeLoader(consumer, writer, person_config, producer=producer)

    with pytest.raises(ControlDeliveryError, match="authorization failed"):
        loader._publish_control("ASSIGNMENT", {})


def test_control_publish_requires_producer(person_config):
    loader = NodeLoader(MagicMock(), MagicMock(), person_config)

    with pytest.raises(ControlDeliveryError, match="without a Kafka producer"):
        loader._publish_control("ASSIGNMENT", {})


def test_rapid_assignments_publish_each_epoch_in_fifo_order(person_config):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    loader = NodeLoader(consumer, writer, person_config, producer=producer)
    first, second = MagicMock(), MagicMock()
    first.topic, first.partition = "person-events", 0
    second.topic, second.partition = "person-events", 1

    loader._on_assign(consumer, [first])
    loader._on_assign(consumer, [second])
    loader._publish_pending_assignments()

    payloads = [json.loads(call.kwargs["value"]) for call in producer.produce.call_args_list]
    assert [payload["assignment_epoch"] for payload in payloads] == [1, 2]
    assert [payload["assigned_partitions"] for payload in payloads] == [
        [{"topic": "person-events", "partition": 0}],
        [{"topic": "person-events", "partition": 1}],
    ]


@pytest.mark.parametrize("shutdown_during", ["write", "commit"])
def test_shutdown_waits_for_inflight_record_before_drain(person_config, shutdown_during):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    shutdown_requested = Event()
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        shutdown_requested=shutdown_requested,
    )
    message = _message(b'{"personId":"p-1","name":"Ada"}')
    consumer.poll.return_value = message
    if shutdown_during == "write":
        writer.write_batch.side_effect = lambda _records: shutdown_requested.set()
    else:
        consumer.commit.side_effect = lambda **_kwargs: shutdown_requested.set()

    assert loader.run(max_messages=2) == 0
    writer.write_batch.assert_called_once()
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.topic, committed.partition, committed.offset) == ("person-events", 0, 4)
    assert consumer.commit.call_args.kwargs["asynchronous"] is False
    payload = json.loads(producer.produce.call_args.kwargs["value"])
    assert payload["type"] == "DRAIN_COMPLETE"


def test_node_writer_materializes_one_batch_in_one_transaction(person_config) -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value
    result = transaction.run.return_value
    records = [
        normalize_node_record({"personId": "p-1", "name": "Ada"}, person_config),
        normalize_node_record({"personId": "p-2", "name": "Grace"}, person_config),
    ]

    NodeWriter(driver, person_config).write_batch(records)

    session.begin_transaction.assert_called_once()
    transaction.run.assert_called_once_with(
        build_node_upsert_query(person_config),
        batch=[
            {"key": "p-1", "properties": {"personId": "p-1", "name": "Ada"}},
            {"key": "p-2", "properties": {"personId": "p-2", "name": "Grace"}},
        ],
    )
    result.consume.assert_called_once_with()
    transaction.commit.assert_called_once_with()


def test_node_loader_flushes_at_configured_batch_size(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    consumer.poll.side_effect = [
        _message(b'{"personId":"p-1","name":"Ada"}', offset=3),
        _message(b'{"personId":"p-2","name":"Grace"}', offset=4),
    ]
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        loading_config=LoadingConfig(unwind_batch_size=2),
    )

    assert loader.run(max_messages=2) == 0

    records = writer.write_batch.call_args.args[0]
    assert [record.key for record in records] == ["p-1", "p-2"]
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.topic, committed.partition, committed.offset) == ("person-events", 0, 5)


def test_node_loader_final_flushes_partial_batch_at_message_cap(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    consumer.poll.return_value = _message(b'{"personId":"p-1","name":"Ada"}', offset=11)
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        loading_config=LoadingConfig(unwind_batch_size=5),
    )

    assert loader.run(max_messages=1) == 0

    writer.write_batch.assert_called_once()
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert committed.offset == 12


def test_node_loader_idle_flushes_partial_batch(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    shutdown_requested = Event()
    message = _message(b'{"personId":"p-1","name":"Ada"}', offset=7)

    calls = []

    def poll(_timeout):
        if consumer.poll.call_count == 1:
            return message
        calls.append("second-poll")
        shutdown_requested.set()
        return None

    consumer.poll.side_effect = poll
    writer.write_batch.side_effect = lambda _records: calls.append("write")
    clock_values = iter([0.0, 1.1])
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        shutdown_requested=shutdown_requested,
        loading_config=LoadingConfig(unwind_batch_size=5, flush_interval_ms=1000),
        clock=lambda: next(clock_values),
    )

    assert loader.run() == 0

    writer.write_batch.assert_called_once()
    assert calls == ["write", "second-poll"]
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert committed.offset == 8


def test_node_loader_commits_each_partition_from_nonzero_offsets(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    consumer.poll.side_effect = [
        _message(b'{"personId":"p-1","name":"Ada"}', partition=0, offset=8),
        _message(b'{"personId":"p-2","name":"Grace"}', partition=1, offset=21),
    ]
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        loading_config=LoadingConfig(unwind_batch_size=2),
    )

    assert loader.run(max_messages=2) == 0

    committed = [call.kwargs["offsets"][0] for call in consumer.commit.call_args_list]
    assert [(item.topic, item.partition, item.offset) for item in committed] == [
        ("person-events", 0, 9),
        ("person-events", 1, 22),
    ]


def test_partial_partition_commit_failure_preserves_confirmed_progress(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    consumer.commit.side_effect = [None, RuntimeError("commit failed")]
    consumer.poll.side_effect = [
        _message(b'{"personId":"p-1","name":"Ada"}', partition=0, offset=8),
        _message(b'{"personId":"p-2","name":"Grace"}', partition=1, offset=21),
    ]
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        loading_config=LoadingConfig(unwind_batch_size=2),
    )

    assert loader.run(max_messages=2) == 1

    assert loader._committed_next_offsets == {("person-events", 0): 9}
    assert len(loader._batch) == 2
    producer.produce.assert_not_called()


def test_partition_ledger_does_not_commit_past_offset_gap(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    consumer.poll.side_effect = [
        _message(b'{"personId":"p-8","name":"Eight"}', offset=8),
        _message(b'{"personId":"p-10","name":"Ten"}', offset=10),
    ]
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        loading_config=LoadingConfig(unwind_batch_size=2),
    )

    assert loader.run(max_messages=2) == 1

    first_commit = consumer.commit.call_args_list[0].kwargs["offsets"][0]
    assert first_commit.offset == 9
    ledger = loader._partition_ledgers[("person-events", 0)]
    assert ledger.next_offset == 9
    assert ledger.resolved_offsets == {10}
    producer.produce.assert_not_called()


def test_later_gap_resolution_advances_through_buffered_contiguous_offset(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    consumer.poll.side_effect = [
        _message(b'{"personId":"p-8","name":"Eight"}', offset=8),
        _message(b'{"personId":"p-10","name":"Ten"}', offset=10),
        _message(b'{"personId":"p-9","name":"Nine"}', offset=9),
    ]
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        loading_config=LoadingConfig(unwind_batch_size=2),
    )

    assert loader.run(max_messages=3) == 0

    committed = [call.kwargs["offsets"][0].offset for call in consumer.commit.call_args_list]
    assert committed == [9, 11]


def test_final_batch_commit_failure_emits_no_drain_complete(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    consumer.commit.side_effect = RuntimeError("commit failed")
    consumer.poll.return_value = _message(b'{"personId":"p-1","name":"Ada"}', offset=4)
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        loading_config=LoadingConfig(unwind_batch_size=5),
    )

    assert loader.run(max_messages=1) == 1

    writer.write_batch.assert_called_once()
    producer.produce.assert_not_called()


def test_final_batch_write_failure_emits_no_drain_complete(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    writer.write_batch.side_effect = RuntimeError("write failed")
    consumer.poll.return_value = _message(b'{"personId":"p-1","name":"Ada"}', offset=4)
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        loading_config=LoadingConfig(unwind_batch_size=5),
    )

    assert loader.run(max_messages=1) == 1

    consumer.commit.assert_not_called()
    producer.produce.assert_not_called()


class _FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_rejection_sink_writes_metadata_jsonl_and_fsyncs(tmp_path) -> None:
    path = tmp_path / "nested" / "rejections.jsonl"
    sink = RejectionSink(path)

    with patch("src.loader.node_loader.os.fsync") as fsync:
        sink.append(
            topic="person-events",
            partition=2,
            offset=17,
            node_label="Person",
            reason="property is required",
        )

    assert json.loads(path.read_text()) == {
        "topic": "person-events",
        "partition": 2,
        "offset": 17,
        "node_label": "Person",
        "reason": "property is required",
    }
    fsync.assert_called_once()


def test_malformed_only_record_commits_immediately_after_durable_rejection(person_config) -> None:
    consumer, writer, producer, rejection_sink = (
        MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    producer.flush.return_value = 0
    consumer.poll.return_value = _message(b"{bad", partition=2, offset=17)
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        rejection_sink=rejection_sink,
    )

    assert loader.run(max_messages=1) == 0

    rejection_sink.append.assert_called_once_with(
        topic="person-events",
        partition=2,
        offset=17,
        node_label="Person",
        reason="Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
    )
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (2, 18)
    writer.write_batch.assert_not_called()


def test_malformed_resolution_waits_behind_pending_valid_record(person_config) -> None:
    consumer, writer, producer, rejection_sink = (
        MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    producer.flush.return_value = 0
    consumer.poll.side_effect = [
        _message(b'{"personId":"p-3","name":"Ada"}', offset=3),
        _message(b"{bad", offset=4),
    ]
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        rejection_sink=rejection_sink,
        loading_config=LoadingConfig(unwind_batch_size=2),
    )

    assert loader.run(max_messages=2) == 0

    rejection_sink.append.assert_called_once()
    consumer.commit.assert_called_once()
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 5


def test_rejection_sink_failure_leaves_offset_uncommitted(person_config) -> None:
    consumer, writer, rejection_sink = MagicMock(), MagicMock(), MagicMock()
    rejection_sink.append.side_effect = OSError("disk full")
    consumer.poll.return_value = _message(b"{bad", offset=4)
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        rejection_sink=rejection_sink,
    )

    assert loader.run(max_messages=1) == 1

    consumer.commit.assert_not_called()
    assert loader._partition_ledgers[("person-events", 0)].resolved_offsets == set()


def test_retryable_write_uses_bounded_capped_backoff_and_pauses(person_config) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = None
    clock = _FakeTime()
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        loading_config=LoadingConfig(
            unwind_batch_size=1,
            retry_max_attempts=4,
            retry_base_delay_ms=100,
            retry_max_delay_ms=150,
        ),
        clock=clock.clock,
        sleeper=clock.sleep,
    )
    assigned = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [assigned])
    writer.write_batch.side_effect = [
        TransientError("retry one"),
        TransientError("retry two"),
        TransientError("retry three"),
        None,
    ]
    assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}'))

    assert loader._flush_batch() is True

    assert writer.write_batch.call_count == 4
    assert clock.sleeps == pytest.approx([0.1, 0.15, 0.15])
    consumer.pause.assert_called_once()
    consumer.resume.assert_called_once()
    consumer.commit.assert_called_once()
    assert consumer.poll.call_count >= 3


def test_node_batch_log_reports_write_duration(person_config) -> None:
    consumer, writer, event_logger = MagicMock(), MagicMock(), MagicMock()
    loader = NodeLoader(consumer, writer, person_config, event_logger=event_logger)
    assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}'))

    with patch("src.loader.node_loader.time.perf_counter", side_effect=[1.0, 1.0125]):
        assert loader._write_batch_with_retry()

    message, *values = event_logger.info.call_args.args
    rendered = message % tuple(values)
    assert "stage=node-write label=Person" in rendered
    assert "records=1 attempts=1 write_ms=12.500" in rendered


def test_retry_exhaustion_and_nonretryable_errors_fail_closed(person_config) -> None:
    for errors, expected_calls in [
        ([TransientError("one"), TransientError("two")], 2),
        ([ClientError("invalid")], 1),
    ]:
        consumer, writer = MagicMock(), MagicMock()
        clock = _FakeTime()
        writer.write_batch.side_effect = errors
        loader = NodeLoader(
            consumer,
            writer,
            person_config,
            loading_config=LoadingConfig(
                unwind_batch_size=1,
                retry_max_attempts=2,
                retry_base_delay_ms=10,
            ),
            clock=clock.clock,
            sleeper=clock.sleep,
        )
        assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}'))

        assert loader._flush_batch() is False

        assert writer.write_batch.call_count == expected_calls
        consumer.commit.assert_not_called()


def test_shutdown_during_retry_backoff_fails_without_commit(person_config) -> None:
    consumer, writer = MagicMock(), MagicMock()
    shutdown_requested = Event()
    clock = _FakeTime()

    def stop_during_sleep(seconds: float) -> None:
        clock.sleep(seconds)
        shutdown_requested.set()

    writer.write_batch.side_effect = TransientError("retry")
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        shutdown_requested=shutdown_requested,
        loading_config=LoadingConfig(unwind_batch_size=1, retry_base_delay_ms=100),
        clock=clock.clock,
        sleeper=stop_during_sleep,
    )
    assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}'))

    assert loader._flush_batch() is False

    consumer.commit.assert_not_called()


def test_post_write_poll_revoke_prevents_commit(person_config) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = NodeLoader(consumer, writer, person_config)
    assigned = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [assigned])
    assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}'))

    def revoke_on_poll(_timeout):
        loader._on_revoke(consumer, [assigned])
        return None

    consumer.poll.side_effect = revoke_on_poll

    assert loader._flush_batch() is False

    consumer.unassign.assert_called_once()
    consumer.commit.assert_not_called()


def test_malformed_commit_poll_revoke_prevents_commit(person_config) -> None:
    consumer, writer, rejection_sink = MagicMock(), MagicMock(), MagicMock()
    loader = NodeLoader(consumer, writer, person_config, rejection_sink=rejection_sink)
    assigned = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [assigned])

    def revoke_on_poll(_timeout):
        loader._on_revoke(consumer, [assigned])
        return None

    consumer.poll.side_effect = revoke_on_poll

    assert loader._buffer_message(_message(b"{bad", offset=3)) is False

    rejection_sink.append.assert_called_once()
    consumer.commit.assert_not_called()


def test_prefetched_retry_message_is_deferred_then_processed_once(person_config) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    clock = _FakeTime()
    first = _message(b'{"personId":"p-1","name":"Ada"}', offset=3)
    prefetched = _message(b'{"personId":"p-2","name":"Grace"}', offset=4)
    poll_results = iter([first, prefetched, None, None, None, None, None])
    consumer.poll.side_effect = lambda _timeout: next(poll_results, None)
    writer.write_batch.side_effect = [TransientError("retry"), None, None]
    loader = NodeLoader(
        consumer,
        writer,
        person_config,
        producer=producer,
        loading_config=LoadingConfig(unwind_batch_size=1, retry_base_delay_ms=10),
        clock=clock.clock,
        sleeper=clock.sleep,
    )
    assigned = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [assigned])

    assert loader.run(max_messages=2) == 0

    batches = writer.write_batch.call_args_list
    assert [[record.key for record in call.args[0]] for call in batches] == [
        ["p-1"],
        ["p-1"],
        ["p-2"],
    ]
    assert not loader._deferred_messages
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 5


def test_parser_rejection_does_not_replace_sigterm_handler():
    import signal

    previous_handler = signal.getsignal(signal.SIGTERM)
    with pytest.raises(SystemExit):
        node_loader_main([])
    assert signal.getsignal(signal.SIGTERM) is previous_handler


def test_consumer_close_failure_still_restores_sigterm_handler(schema):
    import signal

    consumer, driver, loader, producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    consumer.close.side_effect = RuntimeError("close failed")
    loader.run.return_value = 0
    previous_handler = signal.getsignal(signal.SIGTERM)

    with patch("src.loader.node_loader.Consumer", return_value=consumer), \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.NodeLoader", return_value=loader):
        assert node_loader_main(["--node-label", "Person", "--max-messages", "1"]) == 0

    producer.flush.assert_called_once()
    driver.close.assert_called_once()
    assert signal.getsignal(signal.SIGTERM) is previous_handler
