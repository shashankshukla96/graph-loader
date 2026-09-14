"""Schema-driven record validation, Neo4j writing, and direct Kafka loading."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
import json
import logging
import math
import os
from typing import Mapping

from confluent_kafka import Consumer, Message
from neo4j import Driver

from src.cli import get_neo4j_credentials
from src.models.schema import NodeConfig, PropertyConfig
from src.orchestrator.schema_initializer import get_neo4j_driver
from src.utils.schema_loader import load_schema


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NodeRecord:
    """A validated Neo4j-ready record for one schema-declared node."""

    key: object
    properties: dict[str, object]


class NodeRecordValidationError(ValueError):
    """Raised when a raw node record violates its ``NodeConfig`` contract."""


def _raise_record_error(node_config: NodeConfig, property_name: str, reason: str) -> None:
    """Raise a concise validation error without echoing raw record contents."""
    raise NodeRecordValidationError(
        f"Node '{node_config.label}' property '{property_name}' {reason}"
    )


def _normalize_property_value(
    value: object, property_config: PropertyConfig, node_config: NodeConfig, property_name: str
) -> object:
    """Convert one configured scalar value to a Neo4j-driver-compatible type."""
    property_type = property_config.type

    if property_type == "string":
        if not isinstance(value, str):
            _raise_record_error(node_config, property_name, "must be a string")
        return value

    if property_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            _raise_record_error(node_config, property_name, "must be an integer")
        return value

    if property_type == "float":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            _raise_record_error(node_config, property_name, "must be a finite number")
        try:
            numeric_value = float(value)
        except (OverflowError, ValueError):
            _raise_record_error(node_config, property_name, "must be a finite number")
        if not math.isfinite(numeric_value):
            _raise_record_error(node_config, property_name, "must be a finite number")
        return numeric_value

    if property_type == "date":
        if not isinstance(value, str):
            _raise_record_error(node_config, property_name, "must be an ISO-8601 date")
        try:
            return date.fromisoformat(value)
        except ValueError:
            _raise_record_error(node_config, property_name, "must be an ISO-8601 date")

    if property_type == "datetime":
        if not isinstance(value, str):
            _raise_record_error(node_config, property_name, "must be an ISO-8601 datetime")
        if len(value) < 11 or value[10] != "T":
            _raise_record_error(node_config, property_name, "must use a 'T' datetime separator")
        normalized_value = f"{value[:-1]}+00:00" if value.endswith("Z") else value
        try:
            parsed_value = datetime.fromisoformat(normalized_value)
        except ValueError:
            _raise_record_error(node_config, property_name, "must be an ISO-8601 datetime")
        if parsed_value.tzinfo is None or parsed_value.utcoffset() is None:
            _raise_record_error(node_config, property_name, "must include a timezone offset")
        return parsed_value

    _raise_record_error(node_config, property_name, f"has unsupported type '{property_type}'")


def normalize_node_record(
    raw_record: Mapping[str, object], node_config: NodeConfig
) -> NodeRecord:
    """Validate a top-level record and normalize it to Neo4j-supported values.

    Declared properties form an allowlist.  Required fields and the key must
    be present and non-null.  Optional nulls are intentionally omitted so a
    later ``SET n += record.properties`` write has PATCH semantics.
    """
    declared_properties = node_config.properties
    for property_name in raw_record:
        if property_name not in declared_properties:
            _raise_record_error(node_config, property_name, "is not declared in the schema")

    normalized_properties: dict[str, object] = {}
    for property_name, property_config in declared_properties.items():
        value = raw_record.get(property_name)
        is_missing_or_null = property_name not in raw_record or value is None
        if is_missing_or_null:
            if property_name == node_config.key_property or property_config.required:
                _raise_record_error(node_config, property_name, "is required")
            continue
        normalized_properties[property_name] = _normalize_property_value(
            value, property_config, node_config, property_name
        )

    return NodeRecord(
        key=normalized_properties[node_config.key_property],
        properties=normalized_properties,
    )


def build_node_upsert_query(node_config: NodeConfig) -> str:
    """Return schema-derived, parameterized Cypher for one node label.

    Story 1 validates every interpolated identifier. Record values are passed
    only through the ``$batch`` parameter.
    """
    return (
        "UNWIND $batch AS record\n"
        f"MERGE (n:`{node_config.label}` {{`{node_config.key_property}`: record.key}})\n"
        "SET n += record.properties"
    )


class NodeWriter:
    """Write normalized records for one ``NodeConfig`` through a Neo4j driver.

    The caller owns the injected driver. Each call to :meth:`write` owns and
    closes its session while preserving any driver or transaction exception.
    """

    def __init__(self, driver: Driver, node_config: NodeConfig) -> None:
        self._driver = driver
        self._query = build_node_upsert_query(node_config)

    def write(self, record: NodeRecord) -> None:
        """Execute one idempotent singleton-batch node upsert."""
        batch = [{"key": record.key, "properties": record.properties}]
        with self._driver.session() as session:
            session.execute_write(lambda tx: tx.run(self._query, batch=batch))


class NodeLoader:
    """Consume one configured node topic and synchronously write records."""

    def __init__(self, consumer: Consumer, writer: NodeWriter, node_config: NodeConfig,
                 event_logger: logging.Logger = logger) -> None:
        self._consumer = consumer
        self._writer = writer
        self._node_config = node_config
        self._logger = event_logger
        self._consumer.subscribe([node_config.topic])

    def _log_failure(self, message: Message, reason: str) -> None:
        self._logger.error("Node load failed label=%s topic=%s partition=%s offset=%s reason=%s",
                           self._node_config.label, message.topic(), message.partition(),
                           message.offset(), reason)

    def process_message(self, message: Message) -> bool:
        """Validate, write, and synchronously commit one message on success."""
        if message.error():
            self._log_failure(message, str(message.error()))
            return False
        try:
            payload = message.value()
            if payload is None:
                raise ValueError("message payload is empty")
            decoded = json.loads(payload.decode("utf-8"), parse_constant=_reject_json_constant)
            if not isinstance(decoded, dict):
                raise ValueError("JSON payload must be a top-level object")
            record = normalize_node_record(decoded, self._node_config)
            self._writer.write(record)
            self._consumer.commit(message=message, asynchronous=False)
            return True
        except (UnicodeDecodeError, json.JSONDecodeError, NodeRecordValidationError, ValueError) as exc:
            self._log_failure(message, str(exc))
        except Exception as exc:
            self._log_failure(message, f"{type(exc).__name__}: {exc}")
        return False

    def run(self, *, max_messages: int | None = None) -> int:
        """Poll until interrupted, capped, or a message fails closed."""
        processed = 0
        try:
            while max_messages is None or processed < max_messages:
                message = self._consumer.poll(1.0)
                if message is None:
                    continue
                if not self.process_message(message):
                    return 1
                processed += 1
        except KeyboardInterrupt:
            self._logger.warning("Node loader interrupted label=%s", self._node_config.label)
            return 1
        return 0

    def close(self) -> None:
        """Close the owned Kafka consumer."""
        self._consumer.close()


def _reject_json_constant(value: str) -> None:
    """Reject non-standard JSON numeric constants such as NaN and Infinity."""
    raise ValueError(f"unsupported JSON numeric constant {value}")


def _select_node_config(schema, label: str) -> NodeConfig:
    """Return the one configured node matching ``label`` or raise ValueError."""
    matches = [node for node in schema.nodes if node.label == label]
    if len(matches) != 1:
        raise ValueError(f"No configured node label named '{label}'")
    return matches[0]


def build_parser() -> argparse.ArgumentParser:
    """Build the direct node-loader command parser."""
    parser = argparse.ArgumentParser(description="Run one schema-declared Kafka node loader")
    parser.add_argument("--config", default="config/graph_schema.yaml")
    parser.add_argument("--node-label", required=True)
    parser.add_argument("--max-messages", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one selected node loader and close all resources on every path."""
    args = build_parser().parse_args(argv)
    if args.max_messages is not None and args.max_messages <= 0:
        raise SystemExit("--max-messages must be positive")
    consumer = None
    driver = None
    try:
        schema = load_schema(args.config)
        node_config = _select_node_config(schema, args.node_label)
        consumer = Consumer({
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "group.id": schema.loading.consumer_group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
            "session.timeout.ms": schema.loading.session_timeout_ms,
        })
        driver = get_neo4j_driver(*get_neo4j_credentials())
        return NodeLoader(consumer, NodeWriter(driver, node_config), node_config).run(
            max_messages=args.max_messages
        )
    except Exception as exc:
        logger.error("Unable to start node loader: %s", exc)
        return 1
    finally:
        if consumer is not None:
            consumer.close()
        if driver is not None:
            driver.close()


if __name__ == "__main__":
    raise SystemExit(main())
