"""Unit tests for the pure schema-driven node record contract."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from src.loader.node_loader import (
    NodeRecordValidationError,
    NodeWriter,
    NodeLoader,
    build_node_upsert_query,
    normalize_node_record,
    main as node_loader_main,
)
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
    transaction = MagicMock()
    session.execute_write.side_effect = lambda callback: callback(transaction)
    record = normalize_node_record({"personId": "p-001", "name": "Ada"}, person_config)

    NodeWriter(driver, person_config).write(record)

    transaction.run.assert_called_once_with(
        build_node_upsert_query(person_config),
        batch=[{"key": "p-001", "properties": {"personId": "p-001", "name": "Ada"}}],
    )
    session.execute_write.assert_called_once()
    driver.close.assert_not_called()
    driver.session.return_value.__exit__.assert_called_once()


def test_node_writer_propagates_transaction_error_and_closes_session(person_config) -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    session.execute_write.side_effect = RuntimeError("write failed")
    record = normalize_node_record({"personId": "p-001", "name": "Ada"}, person_config)

    with pytest.raises(RuntimeError, match="write failed"):
        NodeWriter(driver, person_config).write(record)

    driver.close.assert_not_called()
    driver.session.return_value.__exit__.assert_called_once()


def _message(payload: bytes, error=None):
    message = MagicMock()
    message.value.return_value = payload
    message.error.return_value = error
    message.topic.return_value = "person-events"
    message.partition.return_value = 0
    message.offset.return_value = 3
    return message


def test_node_loader_subscribes_and_commits_after_write(person_config):
    consumer, writer = MagicMock(), MagicMock()
    calls = []
    writer.write.side_effect = lambda record: calls.append("write")
    consumer.commit.side_effect = lambda **kwargs: calls.append("commit")
    loader = NodeLoader(consumer, writer, person_config)
    message = _message(b'{"personId":"p-1","name":"Ada"}')

    assert loader.process_message(message) is True
    consumer.subscribe.assert_called_once_with(["person-events"])
    assert calls == ["write", "commit"]
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)


@pytest.mark.parametrize("payload", [b"[1]", b"1", b"null", b"{bad", b'{"personId":"p-1","name":NaN}'])
def test_node_loader_fails_closed_for_bad_payload(person_config, payload):
    consumer, writer = MagicMock(), MagicMock()
    loader = NodeLoader(consumer, writer, person_config)
    assert loader.process_message(_message(payload)) is False
    writer.write.assert_not_called()
    consumer.commit.assert_not_called()


def test_node_loader_stops_before_later_message_after_failure(person_config):
    consumer, writer = MagicMock(), MagicMock()
    loader = NodeLoader(consumer, writer, person_config)
    consumer.poll.side_effect = [_message(b"{bad"), _message(b'{"personId":"p-2","name":"Ada"}')]
    assert loader.run(max_messages=2) == 1
    assert consumer.poll.call_count == 1
    consumer.commit.assert_not_called()


@pytest.mark.parametrize("failure", ["message", "normalize", "write", "commit"])
def test_node_loader_failure_paths_stop_before_later_commit(person_config, failure):
    consumer, writer = MagicMock(), MagicMock()
    loader = NodeLoader(consumer, writer, person_config)
    first = _message(b'{"personId":"p-1","name":"Ada"}', error="broker error" if failure == "message" else None)
    later = _message(b'{"personId":"p-2","name":"Later"}')
    if failure == "normalize":
        first = _message(b'{"personId":"p-1"}')
    if failure == "write":
        writer.write.side_effect = RuntimeError("write failed")
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
    consumer, driver = MagicMock(), MagicMock()
    with patch("src.loader.node_loader.Consumer", return_value=consumer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.NodeLoader", side_effect=RuntimeError("subscribe failed")):
        assert node_loader_main(["--node-label", "Person"]) == 1
    consumer.close.assert_called_once()
    driver.close.assert_called_once()


def test_node_loader_main_uses_config_and_closes_resources(schema):
    consumer, driver, loader = MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.node_loader.Consumer", return_value=consumer) as consumer_cls, \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.NodeLoader", return_value=loader):
        assert node_loader_main(["--node-label", "Person", "--max-messages", "1"]) == 0
    config = consumer_cls.call_args.args[0]
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["group.id"] == schema.loading.consumer_group_id
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "earliest"
    assert config["max.poll.interval.ms"] == schema.loading.max_poll_interval_ms
    assert config["session.timeout.ms"] == schema.loading.session_timeout_ms
    loader.run.assert_called_once_with(max_messages=1)
    consumer.close.assert_called_once()
    driver.close.assert_called_once()


def test_node_loader_main_subscribes_only_to_selected_topic(schema, monkeypatch):
    consumer, driver = MagicMock(), MagicMock()
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-test:9092")
    with patch("src.loader.node_loader.Consumer", return_value=consumer) as consumer_cls, \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch.object(NodeLoader, "run", return_value=0):
        assert node_loader_main(["--node-label", "Person", "--max-messages", "1"]) == 0
    assert consumer_cls.call_args.args[0]["bootstrap.servers"] == "kafka-test:9092"
    consumer.subscribe.assert_called_once_with(["person-events"])
