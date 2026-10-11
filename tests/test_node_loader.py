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
from prometheus_client import CollectorRegistry

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
from src.utils.metrics import GraphLoaderMetrics
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
    rejection_sink.route.assert_called_once()
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
    assert config["heartbeat.interval.ms"] == schema.loading.heartbeat_interval_ms == 500

    loader_kwargs = loader_cls.call_args.kwargs
    assert loader_kwargs["run_id"] == "test-run"
    assert loader_kwargs["replica_id"] == "2"
    assert loader_kwargs["loading_config"] is schema.loading

    loader.run.assert_called_once_with(max_messages=1)
    consumer.close.assert_called_once()
    producer.flush.assert_called_once()
    driver.close.assert_called_once()


def test_node_loader_main_runs_end_to_end_with_metrics_disabled(schema, monkeypatch):
    """P6-S2 Story 4 AC: GRAPH_LOADER_METRICS_ENABLED=0 still runs main()
    correctly end to end -- the metric *objects* passed into NodeLoader
    still accept .inc()/.observe() even with no HTTP server exposing them;
    only the exposition, not the accounting, is disabled. (The whole suite
    already runs with this env var set via tests/conftest.py's autouse
    fixture -- this test makes the AC an explicit, named assertion.)"""
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")
    consumer, driver, loader, producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.node_loader.Consumer", return_value=consumer), \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.NodeLoader", return_value=loader):
        assert node_loader_main(["--node-label", "Person", "--max-messages", "1"]) == 0
    loader.run.assert_called_once_with(max_messages=1)


def test_node_loader_main_wires_dead_letter_sink_and_flushes_its_producer(schema):
    """main() routes to build_malformed_record_sink and flushes its producer.

    Uses a schema copy with Person's dead_letter enabled, so the factory
    picks the DeadLetterRecordSink branch instead of the RejectionSink
    fallback -- proving node_config.dead_letter actually reaches main().
    """
    from src.models.schema import DeadLetterConfig

    dlq_schema = schema.model_copy(deep=True)
    person_index = next(i for i, node in enumerate(dlq_schema.nodes) if node.label == "Person")
    dlq_schema.nodes[person_index] = dlq_schema.nodes[person_index].model_copy(
        update={"dead_letter": DeadLetterConfig(enabled=True, topic="person-events.dlq")}
    )

    consumer, driver, loader, producer, dlq_producer = (
        MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    loader.run.return_value = 0
    with patch("src.loader.node_loader.Consumer", return_value=consumer), \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=dlq_schema), \
         patch("src.loader.node_loader.NodeLoader", return_value=loader) as loader_cls, \
         patch(
             "src.loader.node_loader.build_malformed_record_sink",
             return_value=(MagicMock(), dlq_producer),
         ) as build_sink:
        assert node_loader_main(["--node-label", "Person", "--max-messages", "1"]) == 0

    build_sink_kwargs = build_sink.call_args.kwargs
    assert build_sink_kwargs["dead_letter"] is dlq_schema.nodes[person_index].dead_letter
    assert build_sink_kwargs["entity_topic"] == "person-events"
    assert build_sink_kwargs["entity_kind"] == "node"
    assert loader_cls.call_args.kwargs["rejection_sink"] is build_sink.return_value[0]
    dlq_producer.flush.assert_called_once()


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


def test_successful_assigned_node_flush_keeps_broker_prefetch_active(person_config) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = NodeLoader(consumer, writer, person_config)
    loader._on_assign(consumer, [TopicPartition("person-events", 0)])
    assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}', offset=3))

    assert loader._flush_batch() is True

    consumer.pause.assert_not_called()
    consumer.resume.assert_not_called()
    consumer.commit.assert_called_once()


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

    message = consumer.poll.return_value
    rejection_sink.route.assert_called_once_with(
        topic="person-events",
        partition=2,
        offset=17,
        key=message.key(),
        value=b"{bad",
        node_label="Person",
        reason="Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
        reason_code="invalid_json",
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

    rejection_sink.route.assert_called_once()
    consumer.commit.assert_called_once()
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 5


def test_rejection_sink_failure_leaves_offset_uncommitted(person_config) -> None:
    consumer, writer, rejection_sink = MagicMock(), MagicMock(), MagicMock()
    rejection_sink.route.side_effect = OSError("disk full")
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


# ── DLQ routing (Story 4) ─────────────────────────────────────────────────────


def test_dead_letter_sink_receives_exact_raw_bytes_and_commits_only_after_publish(person_config) -> None:
    from src.loader.dead_letter import DeadLetterRecordSink

    dlq_producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="person-events.dlq", run_id="run-1",
        replica_id="0", kind="node", clock=lambda: 1_700_000_000_000,
    )
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    message = _message(b"{bad", partition=2, offset=17)
    message.key.return_value = b"p-17"
    consumer.poll.return_value = message
    loader = NodeLoader(
        consumer, writer, person_config, producer=producer, rejection_sink=sink,
    )

    assert loader.run(max_messages=1) == 0

    dlq_producer.publish.assert_called_once()
    (published_topic, envelope), _ = dlq_producer.publish.call_args
    assert published_topic == "person-events.dlq"
    assert envelope.key_bytes == b"p-17"
    assert envelope.value_bytes == b"{bad"
    assert envelope.entity_kind == "node"
    assert envelope.entity_name == "Person"
    assert envelope.failure_class == "malformed"
    assert envelope.attempt_count == 0
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (2, 18)


def test_dlq_routed_record_increments_dlq_counter_not_records_consumed(person_config) -> None:
    """P6-S2 Story 4: a malformed record routed to the DLQ increments
    dlq_routed_total{failure_class="malformed"} exactly once and does NOT
    increment records_consumed_total (a DLQ'd record was not "consumed" in
    the successful-write sense - Story 2's resolved deviation).

    Uses a real DeadLetterProducer (wrapping a fake confluent-style Kafka
    producer, matching tests/test_dead_letter.py's own
    `_delivering_producer` pattern) so `on_dlq_routed` fires exactly the
    way it does in production - after a real ack - proving the listener
    installed by `install_dlq_routed_listener` reaches this loader's own
    injected metrics instance end to end, not merely that the seam can be
    called directly.
    """
    from src.loader.dead_letter import DeadLetterProducer, DeadLetterRecordSink
    from src.utils import metrics as metrics_module

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    try:
        metrics_module.install_dlq_routed_listener(metrics)

        kafka_producer = MagicMock()
        delivery_state = {"on_delivery": None}

        def produce(topic, key, value, on_delivery):
            delivery_state["on_delivery"] = on_delivery

        def flush():
            delivery_state["on_delivery"](None, MagicMock())
            return 0

        kafka_producer.produce.side_effect = produce
        kafka_producer.flush.side_effect = flush
        dlq_producer = DeadLetterProducer(kafka_producer)
        sink = DeadLetterRecordSink(
            producer=dlq_producer, topic="person-events.dlq", run_id="run-1",
            replica_id="0", kind="node", clock=lambda: 1_700_000_000_000,
        )
        consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
        producer.flush.return_value = 0
        message = _message(b"{bad", partition=2, offset=17)
        message.key.return_value = b"p-17"
        consumer.poll.return_value = message
        loader = NodeLoader(
            consumer, writer, person_config, producer=producer, rejection_sink=sink,
            metrics=metrics,
        )

        assert loader.run(max_messages=1) == 0

        assert metrics.registry.get_sample_value(
            "graph_loader_dlq_routed_total",
            {"kind": "node", "name": "Person", "failure_class": "malformed"},
        ) == 1.0
        assert metrics.registry.get_sample_value(
            "graph_loader_records_consumed_total",
            {"kind": "node", "name": "Person", "label_or_type": "Person"},
        ) is None
    finally:
        metrics_module._reset_dlq_routed_listener_for_tests()


def test_dead_letter_publish_failure_leaves_offset_uncommitted(person_config) -> None:
    from src.loader.dead_letter import DeadLetterPublishError, DeadLetterRecordSink

    dlq_producer = MagicMock()
    dlq_producer.publish.side_effect = DeadLetterPublishError("broker unreachable")
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="person-events.dlq", run_id="run-1",
        replica_id="0", kind="node", clock=lambda: 1_700_000_000_000,
    )
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(b"{bad", offset=4)
    loader = NodeLoader(consumer, writer, person_config, rejection_sink=sink)

    assert loader.run(max_messages=1) == 1

    consumer.commit.assert_not_called()
    assert loader._partition_ledgers[("person-events", 0)].resolved_offsets == set()


def test_mixed_batch_dlq_offset_resolves_independent_of_batch_flush_timing(person_config) -> None:
    """A malformed record DLQ'd immediately; two valid records flush later.

    The committed offset must end up covering both once the batch flushes,
    regardless of which resolved first (regression of
    _commit_contiguous_resolutions' gap-filling).
    """
    dlq_producer = MagicMock()
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    from src.loader.dead_letter import DeadLetterRecordSink

    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="person-events.dlq", run_id="run-1",
        replica_id="0", kind="node", clock=lambda: 1_700_000_000_000,
    )
    consumer.poll.side_effect = [
        _message(b'{"personId":"p-1","name":"Ada"}', offset=3),
        _message(b"{bad", offset=4),
        _message(b'{"personId":"p-2","name":"Bea"}', offset=5),
    ]
    loader = NodeLoader(
        consumer, writer, person_config, producer=producer, rejection_sink=sink,
        loading_config=LoadingConfig(unwind_batch_size=2),
    )

    assert loader.run(max_messages=3) == 0

    dlq_producer.publish.assert_called_once()
    writer.write_batch.assert_called_once()
    final_offset = consumer.commit.call_args_list[-1].kwargs["offsets"][0].offset
    assert final_offset == 6


def test_dead_letter_disabled_is_byte_identical_to_file_fallback(person_config, tmp_path) -> None:
    """dead_letter absent/disabled keeps writing the exact pre-slice JSONL entry."""
    from src.loader.dead_letter import RejectionSink

    path = tmp_path / "rejections.jsonl"
    sink = RejectionSink(path)
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    consumer.poll.return_value = _message(b"{bad", partition=2, offset=17)
    loader = NodeLoader(
        consumer, writer, person_config, producer=producer, rejection_sink=sink,
    )

    assert loader.run(max_messages=1) == 0

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    # Byte-identical to the pre-slice format: canonical, sorted, no whitespace.
    assert lines[0] == (
        '{"node_label":"Person","offset":17,"partition":2,'
        '"reason":"Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",'
        '"topic":"person-events"}'
    )


def test_dead_letter_redelivery_produces_a_second_envelope_at_least_once(person_config) -> None:
    """A restart redelivering an uncommitted malformed message DLQs it again.

    Documented as at-least-once (matches P5-Q5's counter precedent): not
    deduplicated in this slice.
    """
    from src.loader.dead_letter import DeadLetterRecordSink

    dlq_producer = MagicMock()
    clock_values = iter([1_700_000_000_000, 1_700_000_000_999])
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="person-events.dlq", run_id="run-1",
        replica_id="0", kind="node", clock=lambda: next(clock_values),
    )
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    loader = NodeLoader(
        consumer, writer, person_config, producer=producer, rejection_sink=sink,
    )

    for _ in range(2):
        consumer.poll.return_value = _message(b"{bad", offset=4)
        assert loader._buffer_message(consumer.poll.return_value) is True

    assert dlq_producer.publish.call_count == 2
    first_envelope = dlq_producer.publish.call_args_list[0][0][1]
    second_envelope = dlq_producer.publish.call_args_list[1][0][1]
    assert first_envelope.dlq_routed_at_ms != second_envelope.dlq_routed_at_ms


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


def test_metrics_failure_never_blocks_a_successful_batch_write(person_config, monkeypatch) -> None:
    """P6-S2 review finding S4-1: a metrics-library exception must never
    turn an already-successful batch write into a reported failure."""
    consumer, writer = MagicMock(), MagicMock()
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())

    def boom(**_kwargs):
        raise ValueError("simulated metrics failure")

    monkeypatch.setattr(metrics.records_consumed, "labels", boom)
    loader = NodeLoader(consumer, writer, person_config, metrics=metrics)
    assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}'))

    assert loader._write_batch_with_retry() is True


def test_successful_batch_increments_records_consumed_and_write_duration_once(person_config) -> None:
    """P6-S2 Story 4: one observe/inc per batch, not per record, and the
    histogram value matches the same timer the log line already uses."""
    consumer, writer = MagicMock(), MagicMock()
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    loader = NodeLoader(consumer, writer, person_config, metrics=metrics)
    assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}'))
    assert loader._buffer_message(_message(b'{"personId":"p-2","name":"Bob"}'))

    with patch("src.loader.node_loader.time.perf_counter", side_effect=[1.0, 1.0125]):
        assert loader._write_batch_with_retry()

    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "node", "name": "Person", "label_or_type": "Person"},
    ) == 2.0
    histogram_count = metrics.registry.get_sample_value(
        "graph_loader_neo4j_write_duration_seconds_count",
        {"kind": "node", "name": "Person", "stage": "node-write"},
    )
    assert histogram_count == 1.0
    histogram_sum = metrics.registry.get_sample_value(
        "graph_loader_neo4j_write_duration_seconds_sum",
        {"kind": "node", "name": "Person", "stage": "node-write"},
    )
    assert histogram_sum == pytest.approx(0.0125)


def test_retried_then_successful_batch_counts_retries_and_consumed_once(person_config) -> None:
    consumer, writer = MagicMock(), MagicMock()
    clock = _FakeTime()
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    loader = NodeLoader(
        consumer, writer, person_config,
        loading_config=LoadingConfig(
            unwind_batch_size=1, retry_max_attempts=4,
            retry_base_delay_ms=10, retry_max_delay_ms=20,
        ),
        clock=clock.clock, sleeper=clock.sleep, metrics=metrics,
    )
    assigned = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [assigned])
    writer.write_batch.side_effect = [
        TransientError("retry one"), TransientError("retry two"), None,
    ]
    assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}'))

    assert loader._flush_batch() is True

    # Two retryable attempts before success -> retry_total == 2.
    assert metrics.registry.get_sample_value(
        "graph_loader_retry_total",
        {"kind": "node", "name": "Person", "reason": "neo4j-transient"},
    ) == 2.0
    # records_consumed_total incremented once, by N, not once per attempt.
    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "node", "name": "Person", "label_or_type": "Person"},
    ) == 1.0


def test_exhausted_retry_does_not_increment_consumed_or_leave_partial_retry_count(person_config) -> None:
    consumer, writer = MagicMock(), MagicMock()
    clock = _FakeTime()
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    loader = NodeLoader(
        consumer, writer, person_config,
        loading_config=LoadingConfig(
            unwind_batch_size=1, retry_max_attempts=2, retry_base_delay_ms=10,
        ),
        clock=clock.clock, sleeper=clock.sleep, metrics=metrics,
    )
    writer.write_batch.side_effect = [TransientError("one"), TransientError("two")]
    assert loader._buffer_message(_message(b'{"personId":"p-1","name":"Ada"}'))

    assert loader._flush_batch() is False

    # One retryable attempt retried (attempt 1 -> attempt 2); the final,
    # exhausted attempt is not itself counted as a "retry".
    assert metrics.registry.get_sample_value(
        "graph_loader_retry_total",
        {"kind": "node", "name": "Person", "reason": "neo4j-transient"},
    ) == 1.0
    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "node", "name": "Person", "label_or_type": "Person"},
    ) is None


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

    rejection_sink.route.assert_called_once()
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


def test_consumer_close_failure_still_keeps_sigterm_handler(schema):
    import signal

    consumer, driver, loader, producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    consumer.close.side_effect = RuntimeError("close failed")
    loader.run.return_value = 0

    with patch("src.loader.node_loader.Consumer", return_value=consumer), \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.NodeLoader", return_value=loader):
        # P6-S5 Story 4: a close/flush failure during an otherwise-clean
        # shutdown must be reflected in the exit code (previously silently
        # swallowed here, unlike edge_loader.py's existing
        # `finalization_error` behavior for the identical scenario).
        assert node_loader_main(["--node-label", "Person", "--max-messages", "1"]) == 1

    producer.flush.assert_called_once()
    driver.close.assert_called_once()
    # F2: SIGTERM is ignored as the last shutdown step (CPython resets Python handlers
    # to SIG_DFL at interpreter exit; only SIG_IGN survives a repeated stop signal).
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN


def test_node_loader_sigterm_mid_run_leaves_snapshot_with_accumulated_counters(schema, tmp_path, monkeypatch):
    """P6-S2 Story 9/10 (review finding S9-1): a SIGTERM arriving mid-run,
    after at least one counted write, still leaves a metrics-*.prom
    snapshot with that write's counters - not an empty or missing file.
    Drives the real main()/NodeLoader (not mocked out), captures the
    installed SIGTERM handler, and invokes it between two poll() calls to
    simulate the signal arriving while the loader is still running."""
    import signal

    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    # P6-S2 review finding S10-2: an unset port would bind the real
    # 0.0.0.0:9464 during the unit suite (silently skipped on a
    # collision, but still an unnecessary all-interfaces listener and a
    # cross-worktree collision risk) - pin to a free ephemeral port.
    import socket as _socket_module
    with _socket_module.socket(_socket_module.AF_INET, _socket_module.SOCK_STREAM) as _s:
        _s.bind(("127.0.0.1", 0))
        monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(_s.getsockname()[1]))
    snapshot_path = tmp_path / "metrics-node-Person-0.prom"
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(snapshot_path))

    consumer, driver, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    handlers: dict[str, object] = {}
    real_signal = signal.signal

    def capturing_signal(sig, handler):
        previous = real_signal(sig, handler)
        if sig == signal.SIGTERM:
            handlers["current"] = handler
        return previous

    poll_state = {"n": 0}

    def poll_side_effect(timeout):
        poll_state["n"] += 1
        if poll_state["n"] == 1:
            return _message(b'{"personId":"p-1","name":"Ada"}')
        # Simulate the SIGTERM signal arriving between poll calls.
        handlers["current"](signal.SIGTERM, None)
        return None

    consumer.poll.side_effect = poll_side_effect

    with patch("src.loader.node_loader.Consumer", return_value=consumer), \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.signal.signal", side_effect=capturing_signal):
        assert node_loader_main(["--node-label", "Person"]) == 0

    assert snapshot_path.exists()
    from prometheus_client.parser import text_string_to_metric_families

    families = {family.name: family for family in text_string_to_metric_families(snapshot_path.read_text())}
    samples = families["graph_loader_records_consumed"].samples
    assert any(sample.value > 0 for sample in samples)


# --- Commit-on-revoke settlement ---------------------------------------------------
#
# A late sibling joining the group makes Kafka (eager protocol) revoke every
# member's partitions.  Previously any member holding a partial batch failed
# closed and exited; now the revoke callback, which still owns the partitions,
# writes and commits the batch before releasing them.


def _person(key: str) -> bytes:
    return f'{{"personId":"{key}","name":"Ada"}}'.encode()


def _running_loader(person_config, **kwargs):
    consumer, writer = MagicMock(), MagicMock()
    events: list[str] = []
    writer.write_batch.side_effect = lambda records: events.append(
        "write:" + ",".join(str(record.key) for record in records)
    )
    consumer.commit.side_effect = lambda **commit_kwargs: events.append(
        "commit:" + ",".join(f"{o.partition}@{o.offset}" for o in commit_kwargs["offsets"])
    )
    consumer.unassign.side_effect = lambda: events.append("unassign")
    loader = NodeLoader(consumer, writer, person_config, **kwargs)
    loader._running = True  # as inside run()
    return loader, consumer, writer, events


def test_revoke_with_partial_batch_writes_and_commits_before_unassign(person_config) -> None:
    loader, consumer, _, events = _running_loader(person_config)
    p0, p1 = TopicPartition("person-events", 0), TopicPartition("person-events", 1)
    loader._on_assign(consumer, [p0, p1])
    assert loader._buffer_message(_message(_person("p-1"), partition=0, offset=7))
    assert loader._buffer_message(_message(_person("p-2"), partition=1, offset=2))

    loader._on_revoke(consumer, [p0, p1])

    assert events == ["write:p-1,p-2", "commit:0@8", "commit:1@3", "unassign"]
    assert loader._rebalance_failure is False
    assert not loader._batch and not loader._partition_ledgers


def test_revoke_settles_buffered_valid_record_and_already_dlqd_malformed_record(person_config) -> None:
    """Story 4 AC4: a revoke while one record is DLQ'd and one is still buffered.

    The DLQ publish already completed (synchronously, before this revoke)
    -- Story 4's design makes a genuine mid-publish revoke impossible (the
    node loader is single-threaded and _buffer_message never runs from
    inside a rebalance callback). What must still hold is that the revoke's
    settlement writes the buffered valid record and commits a prefix that
    covers both the DLQ'd and the written offset, never less.
    """
    from src.loader.dead_letter import DeadLetterRecordSink

    dlq_producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="person-events.dlq", run_id="run-1",
        replica_id="0", kind="node", clock=lambda: 1,
    )
    loader, consumer, writer, events = _running_loader(
        person_config, rejection_sink=sink, loading_config=LoadingConfig(unwind_batch_size=2),
    )
    p0 = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_person("p-1"), offset=3)) is True
    assert loader._buffer_message(_message(b"{bad", offset=4)) is True
    dlq_producer.publish.assert_called_once()

    loader._on_revoke(consumer, [p0])

    assert events == ["write:p-1", "commit:0@5", "unassign"]
    assert loader._rebalance_failure is False
    assert not loader._batch and not loader._partition_ledgers


def test_revoke_after_dlq_publish_failure_fails_closed_without_committing(person_config) -> None:
    """A DLQ publish failure never lets a later revoke commit past the gap."""
    from src.loader.dead_letter import DeadLetterPublishError, DeadLetterRecordSink

    dlq_producer = MagicMock()
    dlq_producer.publish.side_effect = DeadLetterPublishError("broker unreachable")
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="person-events.dlq", run_id="run-1",
        replica_id="0", kind="node", clock=lambda: 1,
    )
    loader, consumer, writer, events = _running_loader(person_config, rejection_sink=sink)
    p0 = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [p0])

    assert loader._buffer_message(_message(b"{bad", offset=3)) is False

    loader._on_revoke(consumer, [p0])

    # The failed publish never marked anything resolved, so there is no
    # commit-eligible prefix at all: offset 3 stays uncommitted and replays
    # on restart. (In production, _run() already returned 1 the moment
    # _buffer_message returned False, before any later poll/revoke would
    # even be dispatched; this pins down that a revoke reaching this state
    # regardless still cannot commit past the gap.)
    consumer.commit.assert_not_called()
    assert events == ["unassign"]


def test_revoke_during_post_write_poll_commits_once_without_rewrite(person_config) -> None:
    loader, consumer, _, events = _running_loader(person_config)
    p0 = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_person("p-1"), offset=3))

    def revoke_on_poll(_timeout):
        loader._on_revoke(consumer, [p0])
        return None

    consumer.poll.side_effect = revoke_on_poll

    assert loader._flush_batch() is True
    assert events == ["write:p-1", "commit:0@4", "unassign"]
    assert loader._rebalance_failure is False


def test_revoke_during_retry_backoff_settles_and_retry_does_not_rewrite(person_config) -> None:
    clock = _FakeTime()
    loader, consumer, writer, events = _running_loader(
        person_config,
        loading_config=LoadingConfig(unwind_batch_size=10, retry_base_delay_ms=10),
        clock=clock.clock, sleeper=clock.sleep,
    )
    p0 = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_person("p-1"), offset=3))
    attempts = iter([TransientError("retry")])

    def write(records):
        error = next(attempts, None)
        if error is not None:
            raise error
        events.append("write:" + ",".join(str(record.key) for record in records))

    writer.write_batch.side_effect = write
    revoked = []

    def revoke_once_on_poll(_timeout):
        if not revoked:
            revoked.append(True)
            loader._on_revoke(consumer, [p0])
        return None

    consumer.poll.side_effect = revoke_once_on_poll

    assert loader._flush_batch() is True
    assert events == ["write:p-1", "commit:0@4", "unassign"]


def test_revoke_drops_only_revoked_prefetched_messages(person_config) -> None:
    loader, consumer, _, events = _running_loader(person_config)
    p0, p1 = TopicPartition("person-events", 0), TopicPartition("person-events", 1)
    loader._on_assign(consumer, [p0, p1])
    kept = _message(_person("p-3"), partition=1, offset=9)
    loader._deferred_messages.extend([_message(_person("p-2"), partition=0, offset=5), kept])

    loader._on_revoke(consumer, [p0])

    assert list(loader._deferred_messages) == [kept]
    assert loader._rebalance_failure is False
    assert events == ["unassign"]  # nothing buffered: no write, no commit


def test_revoke_settlement_failure_still_fails_closed(person_config) -> None:
    loader, consumer, writer, events = _running_loader(person_config)
    writer.write_batch.side_effect = ClientError("constraint")
    p0 = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_person("p-1"), offset=3))

    loader._on_revoke(consumer, [p0])

    assert loader._rebalance_failure is True
    consumer.commit.assert_not_called()
    assert events == ["unassign"]


def test_revoke_after_run_returned_never_writes_or_commits(person_config) -> None:
    loader, consumer, writer, _ = _running_loader(person_config)
    p0 = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_person("p-1"), offset=3))
    loader._running = False  # e.g. consumer.close() after a failed run()

    loader._on_revoke(consumer, [p0])

    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()
    assert loader._rebalance_failure is True


def test_partition_reassigned_later_commits_from_new_position(person_config) -> None:
    loader, consumer, _, events = _running_loader(person_config)
    p0 = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_person("p-1"), offset=3))
    loader._on_revoke(consumer, [p0])
    # Another replica consumed 4..9 meanwhile; this replica resumes at 10.
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_person("p-2"), offset=10))

    assert loader._flush_batch() is True
    assert events[-1] == "commit:0@11"


def test_rebalance_mid_run_does_not_fail_and_drain_follows_commits(person_config) -> None:
    producer = MagicMock()
    producer.flush.return_value = 0
    shutdown = Event()
    consumer, writer = MagicMock(), MagicMock()
    events: list[str] = []
    writer.write_batch.side_effect = lambda records: events.append("write")
    consumer.commit.side_effect = lambda **kwargs: events.append("commit")
    producer.produce.side_effect = lambda *args, **kwargs: events.append(
        "control:" + __import__("json").loads(kwargs["value"])["type"]
    )
    loader = NodeLoader(
        consumer, writer, person_config, producer=producer, shutdown_requested=shutdown,
        loading_config=LoadingConfig(unwind_batch_size=100, flush_interval_ms=60_000),
    )
    p0 = TopicPartition("person-events", 0)
    loader._on_assign(consumer, [p0])
    steps = iter(["msg", "revoke", "assign", "stop"])

    def poll(_timeout):
        step = next(steps, "stop")
        if step == "msg":
            return _message(_person("p-1"), offset=3)
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
        "control:ASSIGNMENT", "write", "commit", "control:ASSIGNMENT", "control:DRAIN_COMPLETE",
    ]


def _drain_loader(person_config, *, unwind: int, prefetched: int):
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    shutdown = Event()
    loader = NodeLoader(
        consumer, writer, person_config, producer=producer, shutdown_requested=shutdown,
        loading_config=LoadingConfig(unwind_batch_size=unwind),
    )
    loader._on_assign(consumer, [TopicPartition("person-events", 0)])
    loader._pending_assignment_events.clear()
    for offset in range(prefetched):
        loader._deferred_messages.append(
            _message(f'{{"personId":"p-{offset}","name":"A"}}'.encode(), offset=offset)
        )
    return consumer, writer, producer, loader, shutdown


def test_graceful_stop_drains_only_buffered_work_without_new_polls(person_config):
    """D2: with a large prefetch, stop drains the deferred queue in
    unwind-sized transactions and never polls new data."""
    consumer, writer, producer, loader, shutdown = _drain_loader(person_config, unwind=10, prefetched=50)
    # An endless stream of new data would keep a polling drain running forever.
    endless = iter(lambda: _message(b'{"personId":"x","name":"A"}', offset=10_000), None)
    consumer.poll.side_effect = lambda *_a, **_k: next(endless)
    shutdown.set()

    assert loader.run() == 0

    assert [len(call.args[0]) for call in writer.write_batch.call_args_list] == [10] * 5
    # Only the rebalance-callback polls (poll(0)) may happen; their data is dropped.
    assert not loader._deferred_messages
    consumer.pause.assert_called_once()
    consumer.resume.assert_not_called()
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 50
    payload = json.loads(producer.produce.call_args.kwargs["value"])
    assert payload["type"] == "DRAIN_COMPLETE"


def test_graceful_stop_drops_data_polled_during_drain(person_config):
    consumer, writer, _producer, loader, shutdown = _drain_loader(person_config, unwind=100, prefetched=3)
    consumer.poll.side_effect = lambda *_a, **_k: _message(b'{"personId":"z","name":"A"}', offset=99)
    shutdown.set()

    assert loader.run() == 0

    assert [len(call.args[0]) for call in writer.write_batch.call_args_list] == [3]


def test_sigterm_handler_installed_before_any_startup_work(schema):
    import signal

    order = []
    real_signal = signal.signal

    def tracking_signal(sig, handler):
        if sig == signal.SIGTERM:
            order.append("sigterm")
        return real_signal(sig, handler)

    previous = signal.getsignal(signal.SIGTERM)
    with patch("src.loader.node_loader.signal.signal", side_effect=tracking_signal), \
         patch("src.loader.node_loader.build_parser",
               side_effect=lambda: order.append("parse") or (_ for _ in ()).throw(SystemExit(2))):
        with pytest.raises(SystemExit):
            node_loader_main([])
    assert order[:2] == ["sigterm", "parse"]
    assert signal.getsignal(signal.SIGTERM) is previous
