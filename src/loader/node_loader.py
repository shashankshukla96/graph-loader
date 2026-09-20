"""Schema-driven record validation, Neo4j writing, and direct Kafka loading."""
from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import signal
from threading import Event
import time
from typing import Callable, Deque, Mapping, Sequence

from confluent_kafka import Consumer, Message, Producer, TopicPartition
from neo4j import Driver
from neo4j.exceptions import Neo4jError

from src.cli import get_neo4j_credentials
from src.loader.control import CONTROL_TOPIC
from src.models.schema import LoadingConfig, NodeConfig, PropertyConfig
from src.loader.record_validation import normalize_property_value
from src.orchestrator.schema_initializer import get_neo4j_driver
from src.utils.schema_loader import load_schema


logger = logging.getLogger(__name__)

class ControlDeliveryError(RuntimeError):
    """Raised when a control acknowledgement was not durably delivered."""


@dataclass(frozen=True)
class NodeRecord:
    """A validated Neo4j-ready record for one schema-declared node."""

    key: object
    properties: dict[str, object]


@dataclass(frozen=True)
class PendingRecord:
    """A normalized node record coupled to its source Kafka position."""

    record: NodeRecord
    topic: str
    partition: int
    offset: int


@dataclass
class PartitionLedger:
    """Track durably resolved offsets until a contiguous commit is possible."""

    next_offset: int
    resolved_offsets: set[int]


class RejectionSink:
    """Durably log malformed records until Phase 5 replaces this with a DLQ."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    @property
    def path(self) -> Path:
        """Return the configured JSONL path."""
        return self._path

    def append(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        reason: str,
        node_label: str | None = None,
        edge_type: str | None = None,
    ) -> None:
        """Append and fsync one metadata-only node or relationship rejection.

        Exactly one discriminator is retained so existing node JSONL records
        remain compatible while relationship records identify their edge type.
        """
        if (node_label is None) == (edge_type is None):
            raise ValueError("exactly one of node_label or edge_type is required")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "reason": reason,
        }
        if node_label is not None:
            entry["node_label"] = node_label
        else:
            entry["edge_type"] = edge_type
        with self._path.open("a", encoding="utf-8") as rejection_file:
            rejection_file.write(json.dumps(entry, separators=(",", ":"), sort_keys=True))
            rejection_file.write("\n")
            rejection_file.flush()
            os.fsync(rejection_file.fileno())


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
    """Convert a configured scalar while retaining node-specific errors."""
    return normalize_property_value(
        value,
        property_config,
        entity_description=f"Node '{node_config.label}'",
        property_name=property_name,
        error_factory=NodeRecordValidationError,
    )


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
        self.write_batch([record])

    def write_batch(self, records: Sequence[NodeRecord]) -> None:
        """Write one materialized UNWIND batch in a single transaction."""
        batch = [{"key": record.key, "properties": record.properties} for record in records]
        with self._driver.session() as session:
            with session.begin_transaction() as transaction:
                transaction.run(self._query, batch=batch).consume()
                transaction.commit()


class NodeLoader:
    """Consume one configured node topic and synchronously write records."""

    def __init__(self, consumer: Consumer, writer: NodeWriter, node_config: NodeConfig,
                 topic: str | None = None, event_logger: logging.Logger = logger,
                 producer: Producer | None = None, run_id: str = "default_run",
                 replica_id: str = "0", shutdown_requested: Event | None = None,
                 loading_config: LoadingConfig | None = None,
                 clock: Callable[[], float] = time.monotonic,
                 sleeper: Callable[[float], None] = time.sleep,
                 rejection_sink: RejectionSink | None = None) -> None:
        self._consumer = consumer
        self._writer = writer
        self._node_config = node_config
        self._logger = event_logger
        self._topic = topic or node_config.topic

        self._producer = producer
        self._run_id = run_id
        self._replica_id = replica_id
        self._assignment_epoch = 0
        self._pending_assignment_events: Deque[tuple[int, list[dict[str, object]]]] = deque()
        self._shutdown_requested = shutdown_requested or Event()
        self._loading_config = loading_config or LoadingConfig(unwind_batch_size=1)
        self._clock = clock
        self._sleeper = sleeper
        self._rejection_sink = rejection_sink or RejectionSink(
            self._loading_config.rejection_log_path
        )
        self._batch: list[PendingRecord] = []
        self._batch_started_at: float | None = None
        self._committed_next_offsets: dict[tuple[str, int], int] = {}
        self._partition_ledgers: dict[tuple[str, int], PartitionLedger] = {}
        self._assigned_partitions: set[tuple[str, int]] = set()
        self._assignment_observed = False
        self._rebalance_failure = False
        self._deferred_messages: Deque[Message] = deque()
        self._paused = False

        self._consumer.subscribe(
            [self._topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

    def _publish_control(
        self,
        message_type: str,
        data: Mapping[str, object],
        *,
        flush: bool = True,
        assignment_epoch: int | None = None,
    ) -> None:
        """Synchronously publish one run-scoped control acknowledgement.

        A control event is considered acknowledged only after Kafka invokes its
        delivery callback without an error and ``flush`` reports no undelivered
        messages.  This makes completion signals fail closed.
        """
        if self._producer is None:
            raise ControlDeliveryError(
                f"Control event '{message_type}' cannot be published without a Kafka producer"
            )
        delivery_error: Exception | None = None

        def on_delivery(error: Exception | None, _message: Message) -> None:
            nonlocal delivery_error
            if error is not None:
                delivery_error = error

        payload = {
            "run_id": self._run_id,
            "node_label": self._node_config.label,
            "replica_id": self._replica_id,
            "type": message_type,
            "assignment_epoch": assignment_epoch if assignment_epoch is not None else self._assignment_epoch,
        }
        payload.update(data)
        self._producer.produce(
            CONTROL_TOPIC,
            key=self._run_id,
            value=json.dumps(payload),
            on_delivery=on_delivery,
        )
        if flush:
            undelivered_count = self._producer.flush()
            if undelivered_count:
                raise ControlDeliveryError(
                    f"Control event '{message_type}' has {undelivered_count} undelivered message(s)"
                )
            if delivery_error is not None:
                raise ControlDeliveryError(
                    f"Control event '{message_type}' delivery failed: {delivery_error}"
                )

    def _on_assign(self, consumer: Consumer, partitions: list) -> None:
        """Accept Kafka's assignment before asynchronously reporting it."""
        consumer.assign(partitions)
        self._assignment_observed = True
        self._assigned_partitions = {
            (partition.topic or self._topic, partition.partition)
            for partition in partitions
        }
        self._paused = False
        self._assignment_epoch += 1
        assignments = [
            {"topic": partition.topic or self._topic, "partition": partition.partition}
            for partition in partitions
        ]
        self._pending_assignment_events.append((self._assignment_epoch, assignments))

    def _on_revoke(self, consumer: Consumer, partitions: list) -> None:
        """Remove revoked ownership before any subsequent offset decision."""
        revoked = {
            (partition.topic or self._topic, partition.partition)
            for partition in partitions
        }
        pending = {(item.topic, item.partition) for item in self._batch}
        pending.update(
            key for key, ledger in self._partition_ledgers.items()
            if ledger.resolved_offsets
        )
        pending.update(
            (message.topic(), message.partition()) for message in self._deferred_messages
        )
        if revoked & pending:
            self._rebalance_failure = True
            self._logger.error(
                "Kafka partitions revoked with unresolved work label=%s partitions=%s",
                self._node_config.label,
                sorted(revoked & pending),
            )
        self._assigned_partitions.difference_update(revoked)
        self._paused = False
        consumer.unassign()

    def _publish_pending_assignments(self) -> None:
        """Publish every queued assignment epoch in order after Kafka polling."""
        while self._pending_assignment_events:
            epoch, assignments = self._pending_assignment_events[0]
            if assignments:
                self._publish_control(
                    "ASSIGNMENT",
                    {"assigned_partitions": assignments},
                    assignment_epoch=epoch,
                )
            else:
                self._publish_control("IDLE_SURPLUS", {}, assignment_epoch=epoch)
            self._pending_assignment_events.popleft()

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

    def _buffer_message(self, message: Message) -> bool:
        """Buffer a valid record or durably resolve one malformed record."""
        if message.error():
            self._log_failure(message, str(message.error()))
            return False
        topic, partition, offset = message.topic(), message.partition(), message.offset()
        ledger_key = (topic, partition)
        ledger = self._partition_ledgers.setdefault(
            ledger_key,
            PartitionLedger(next_offset=offset, resolved_offsets=set()),
        )
        try:
            payload = message.value()
            if payload is None:
                raise ValueError("message payload is empty")
            decoded = json.loads(payload.decode("utf-8"), parse_constant=_reject_json_constant)
            if not isinstance(decoded, dict):
                raise ValueError("JSON payload must be a top-level object")
            pending = PendingRecord(
                normalize_node_record(decoded, self._node_config),
                topic,
                partition,
                offset,
            )
            if not self._batch:
                self._batch_started_at = self._clock()
            self._batch.append(pending)
            return True
        except (UnicodeDecodeError, json.JSONDecodeError, NodeRecordValidationError, ValueError) as exc:
            reason = str(exc)
            try:
                self._rejection_sink.append(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    node_label=self._node_config.label,
                    reason=reason,
                )
            except Exception as sink_error:
                self._log_failure(
                    message,
                    f"rejection sink {type(sink_error).__name__}: {sink_error}",
                )
                return False
            self._logger.warning(
                "Malformed node record durably rejected label=%s topic=%s partition=%s "
                "offset=%s reason=%s phase5_action=route_to_dlq",
                self._node_config.label,
                topic,
                partition,
                offset,
                reason,
            )
            ledger.resolved_offsets.add(offset)
            return self._commit_contiguous_resolutions()

    def _poll_for_callbacks(self) -> bool:
        """Dispatch Kafka callbacks and preserve any prefetched data message."""
        if not self._assignment_observed:
            return True
        message = self._consumer.poll(0)
        if message is not None:
            self._deferred_messages.append(message)
        return not self._rebalance_failure

    def _owns(self, key: tuple[str, int]) -> bool:
        """Return whether a partition is safe to commit under known ownership."""
        return not self._assignment_observed or key in self._assigned_partitions

    def _pause_assignments(self) -> None:
        """Pause known assignments while a batch is unresolved."""
        if not self._assignment_observed or self._paused or not self._assigned_partitions:
            return
        partitions = [
            TopicPartition(topic, partition)
            for topic, partition in sorted(self._assigned_partitions)
        ]
        self._consumer.pause(partitions)
        self._paused = True

    def _resume_assignments(self) -> None:
        """Resume assignments only after all current work is durable."""
        if not self._assignment_observed or not self._paused or not self._assigned_partitions:
            return
        partitions = [
            TopicPartition(topic, partition)
            for topic, partition in sorted(self._assigned_partitions)
        ]
        self._consumer.resume(partitions)
        self._paused = False

    @staticmethod
    def _is_retryable_neo4j_error(error: Exception) -> bool:
        """Classify only driver-declared Neo4j retryable failures for retry."""
        return isinstance(error, Neo4jError) and error.is_retryable()

    def _heartbeat_backoff(self, delay_seconds: float) -> bool:
        """Wait for retry while polling often enough to retain group membership."""
        deadline = self._clock() + delay_seconds
        heartbeat_slice = max(
            0.001,
            self._loading_config.max_poll_interval_ms / 3000,
        )
        while True:
            if self._shutdown_requested.is_set():
                return False
            if not self._poll_for_callbacks():
                return False
            remaining = deadline - self._clock()
            if remaining <= 0:
                return True
            self._sleeper(min(heartbeat_slice, remaining))

    def _write_batch_with_retry(self) -> bool:
        """Write the current atomic batch with bounded retryable-only backoff."""
        records = [pending.record for pending in self._batch]
        attempts = self._loading_config.retry_max_attempts
        for attempt in range(attempts):
            try:
                self._writer.write_batch(records)
                return True
            except Exception as exc:
                retryable = self._is_retryable_neo4j_error(exc)
                if not retryable or attempt + 1 >= attempts:
                    first = self._batch[0]
                    self._logger.error(
                        "Node batch write failed label=%s topic=%s partition=%s offset=%s "
                        "attempt=%s retryable=%s reason=%s",
                        self._node_config.label,
                        first.topic,
                        first.partition,
                        first.offset,
                        attempt + 1,
                        retryable,
                        exc,
                    )
                    return False
                delay_ms = min(
                    self._loading_config.retry_base_delay_ms * (2 ** attempt),
                    self._loading_config.retry_max_delay_ms,
                )
                if not self._heartbeat_backoff(delay_ms / 1000):
                    return False
        return False

    def _commit_contiguous_resolutions(self) -> bool:
        """Commit each owned partition's longest durably resolved prefix."""
        for (topic, partition), ledger in sorted(self._partition_ledgers.items()):
            next_offset = ledger.next_offset
            while next_offset in ledger.resolved_offsets:
                next_offset += 1
            if next_offset == ledger.next_offset:
                continue
            if not self._poll_for_callbacks():
                return False
            key = (topic, partition)
            if not self._owns(key):
                self._logger.error(
                    "Refusing offset commit without ownership label=%s topic=%s partition=%s",
                    self._node_config.label,
                    topic,
                    partition,
                )
                self._rebalance_failure = True
                return False
            try:
                self._consumer.commit(
                    offsets=[TopicPartition(topic, partition, next_offset)],
                    asynchronous=False,
                )
            except Exception as exc:
                self._logger.error(
                    "Offset commit failed label=%s topic=%s partition=%s offset=%s reason=%s",
                    self._node_config.label,
                    topic,
                    partition,
                    next_offset,
                    exc,
                )
                return False
            self._committed_next_offsets[key] = next_offset
            for resolved_offset in range(ledger.next_offset, next_offset):
                ledger.resolved_offsets.discard(resolved_offset)
            ledger.next_offset = next_offset
        return True

    def _flush_batch(self) -> bool:
        """Atomically write the batch, then commit each partition's next offset."""
        if not self._batch:
            return True
        self._pause_assignments()
        if not self._write_batch_with_retry():
            return False
        if not self._poll_for_callbacks():
            return False
        batch_keys = {(pending.topic, pending.partition) for pending in self._batch}
        if not all(self._owns(key) for key in batch_keys):
            self._rebalance_failure = True
            return False
        for pending in self._batch:
            key = (pending.topic, pending.partition)
            self._partition_ledgers[key].resolved_offsets.add(pending.offset)
        if not self._commit_contiguous_resolutions():
            return False
        self._batch.clear()
        self._batch_started_at = None
        self._resume_assignments()
        return True

    def _idle_flush_due(self) -> bool:
        if self._batch_started_at is None:
            return False
        return (self._clock() - self._batch_started_at) * 1000 >= self._loading_config.flush_interval_ms

    def _has_uncommitted_resolutions(self) -> bool:
        """Return whether a resolved offset is still blocked behind a gap."""
        return any(ledger.resolved_offsets for ledger in self._partition_ledgers.values())

    def run(self, *, max_messages: int | None = None) -> int:
        """Poll until interrupted, capped, or a message fails closed."""
        processed = 0
        try:
            while max_messages is None or processed < max_messages:
                self._publish_pending_assignments()
                if self._rebalance_failure:
                    return 1
                if self._shutdown_requested.is_set():
                    self._logger.info("Node loader gracefully stopping label=%s", self._node_config.label)
                    if not self._flush_batch():
                        return 1
                    if self._deferred_messages:
                        message = self._deferred_messages.popleft()
                        if not self._buffer_message(message):
                            return 1
                        processed += 1
                        continue
                    if self._has_uncommitted_resolutions():
                        self._logger.error(
                            "Node loader cannot complete with offset gaps label=%s",
                            self._node_config.label,
                        )
                        return 1
                    self._publish_control("DRAIN_COMPLETE", {}, flush=True)
                    return 0

                if self._idle_flush_due() and not self._flush_batch():
                    return 1

                if self._deferred_messages:
                    message = self._deferred_messages.popleft()
                else:
                    message = self._consumer.poll(
                        min(1.0, self._loading_config.flush_interval_ms / 1000)
                    )
                if message is None:
                    continue
                if not self._buffer_message(message):
                    return 1
                processed += 1
                if len(self._batch) >= self._loading_config.unwind_batch_size and not self._flush_batch():
                    return 1
        except KeyboardInterrupt:
            self._logger.warning("Node loader interrupted label=%s", self._node_config.label)
            return 1

        if not self._flush_batch():
            return 1
        if self._deferred_messages or self._has_uncommitted_resolutions():
            self._logger.error(
                "Node loader cannot complete with deferred or offset-gap work label=%s",
                self._node_config.label,
            )
            return 1
        self._publish_control("DRAIN_COMPLETE", {}, flush=True)
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
    parser.add_argument("--mode", choices=["bulk", "stream"], default="stream")
    parser.add_argument("--topic")
    parser.add_argument("--run-id", default="default_run")
    parser.add_argument("--replica-id", default="0")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one selected node loader and close all resources on every path."""
    args = build_parser().parse_args(argv)
    if args.max_messages is not None and args.max_messages <= 0:
        raise SystemExit("--max-messages must be positive")
    shutdown_requested = Event()
    previous_sigterm_handler = signal.signal(
        signal.SIGTERM,
        lambda _signum, _frame: shutdown_requested.set(),
    )
    consumer = None
    producer = None
    driver = None
    try:
        schema = load_schema(args.config)
        node_config = _select_node_config(schema, args.node_label)

        kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        consumer = Consumer({
            "bootstrap.servers": kafka_bootstrap_servers,
            "group.id": os.environ.get("KAFKA_GROUP_ID", f"{schema.loading.consumer_group_id}-{node_config.label}"),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
            "session.timeout.ms": schema.loading.session_timeout_ms,
        })
        producer = Producer({
            "bootstrap.servers": kafka_bootstrap_servers,
        })
        driver = get_neo4j_driver(*get_neo4j_credentials())

        loader = NodeLoader(
            consumer=consumer,
            writer=NodeWriter(driver, node_config),
            node_config=node_config,
            topic=args.topic,
            producer=producer,
            run_id=args.run_id,
            replica_id=args.replica_id,
            shutdown_requested=shutdown_requested,
            loading_config=schema.loading,
            rejection_sink=RejectionSink(
                os.environ.get("REJECTION_LOG_PATH", schema.loading.rejection_log_path)
            ),
        )
        return loader.run(max_messages=args.max_messages)
    except Exception as exc:
        logger.error("Unable to start node loader: %s", exc)
        return 1
    finally:
        try:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    logger.exception("Unable to close Kafka consumer during loader shutdown")
            if producer is not None:
                try:
                    producer.flush()
                except Exception:
                    logger.exception("Unable to flush control producer during loader shutdown")
            if driver is not None:
                try:
                    driver.close()
                except Exception:
                    logger.exception("Unable to close Neo4j driver during loader shutdown")
        finally:
            signal.signal(signal.SIGTERM, previous_sigterm_handler)


if __name__ == "__main__":
    raise SystemExit(main())
