"""Schema-driven relationship validation, Neo4j writing, and Kafka loading."""
from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
import json
import logging
import os
import signal
from threading import Event
import time
from typing import Callable, Deque, Mapping, Sequence

from confluent_kafka import Consumer, Message, Producer, TopicPartition
from neo4j import Driver

from src.cli import get_neo4j_credentials
from src.loader.control import CONTROL_TOPIC
from src.loader.node_loader import (
    ControlDeliveryError,
    PartitionLedger,
    RejectionSink,
    _reject_json_constant,
)
from src.loader.record_validation import normalize_property_value
from src.models.schema import EdgeConfig, LoadingConfig, PropertyConfig
from src.orchestrator.schema_initializer import get_neo4j_driver
from src.loader.edge_execution import build_edge_execution
from src.utils.schema_loader import load_schema


logger = logging.getLogger(__name__)


class EdgeRecordValidationError(ValueError):
    """Raised when a relationship event violates its declared schema."""


class MissingRelationshipEndpointError(RuntimeError):
    """Raised when a relationship record cannot resolve both endpoint nodes."""


@dataclass(frozen=True)
class EdgeRecord:
    """Normalized keys and allowlisted properties for one relationship event."""

    source_key: object
    target_key: object
    properties: dict[str, object]


@dataclass(frozen=True)
class PendingEdgeRecord:
    """A normalized edge record coupled to its Kafka source position."""

    record: EdgeRecord
    topic: str
    partition: int
    offset: int


def _raise_error(edge_config: EdgeConfig, field: str, reason: str) -> None:
    raise EdgeRecordValidationError(f"Edge '{edge_config.type}' field '{field}' {reason}")


def _normalize_endpoint(
    section: Mapping[str, object],
    *,
    section_name: str,
    key_property: str,
    property_config: PropertyConfig,
    edge_config: EdgeConfig,
) -> object:
    unknown = set(section) - {key_property}
    if unknown:
        _raise_error(edge_config, f"{section_name}.{sorted(unknown)[0]}", "is not declared")
    if key_property not in section or section[key_property] is None:
        _raise_error(edge_config, f"{section_name}.{key_property}", "is required")
    return normalize_property_value(
        section[key_property],
        property_config,
        entity_description=f"Edge '{edge_config.type}' {section_name}",
        property_name=key_property,
        error_factory=EdgeRecordValidationError,
    )


def normalize_edge_record(raw_record: object, edge_config: EdgeConfig) -> EdgeRecord:
    """Validate the canonical envelope and normalize its declared values.

    Edge configs must come from a fully validated :class:`GraphSchema`, which
    binds endpoint scalar definitions.  Direct ``EdgeConfig`` construction is
    rejected deterministically rather than accepting unconstrained values.
    """
    if not isinstance(raw_record, Mapping):
        _raise_error(edge_config, "event", "must be a mapping")

    source_property = edge_config.source_key_config
    target_property = edge_config.target_key_config
    if source_property is None or target_property is None:
        raise EdgeRecordValidationError(
            f"Edge '{edge_config.type}' has unresolved endpoint schema bindings"
        )

    expected_sections = {"source", "target", "properties"}
    unknown_sections = set(raw_record) - expected_sections
    if unknown_sections:
        _raise_error(edge_config, sorted(unknown_sections)[0], "is not declared")
    for section_name in expected_sections:
        section = raw_record.get(section_name)
        if not isinstance(section, Mapping):
            _raise_error(edge_config, section_name, "must be a mapping")

    source = raw_record["source"]
    target = raw_record["target"]
    properties = raw_record["properties"]
    assert isinstance(source, Mapping) and isinstance(target, Mapping) and isinstance(properties, Mapping)
    source_key = _normalize_endpoint(
        source,
        section_name="source",
        key_property=edge_config.source_key_property,
        property_config=source_property,
        edge_config=edge_config,
    )
    target_key = _normalize_endpoint(
        target,
        section_name="target",
        key_property=edge_config.target_key_property,
        property_config=target_property,
        edge_config=edge_config,
    )

    declared_properties = edge_config.properties
    for property_name in properties:
        if property_name not in declared_properties:
            _raise_error(edge_config, f"properties.{property_name}", "is not declared")
    normalized_properties: dict[str, object] = {}
    for property_name, property_config in declared_properties.items():
        value = properties.get(property_name)
        if property_name not in properties or value is None:
            if property_config.required:
                _raise_error(edge_config, f"properties.{property_name}", "is required")
            continue
        normalized_properties[property_name] = normalize_property_value(
            value,
            property_config,
            entity_description=f"Edge '{edge_config.type}' properties",
            property_name=property_name,
            error_factory=EdgeRecordValidationError,
        )
    return EdgeRecord(source_key=source_key, target_key=target_key, properties=normalized_properties)


def build_edge_upsert_query(edge_config: EdgeConfig) -> str:
    """Return schema-derived Cypher for idempotently upserting relationships."""
    return (
        "UNWIND $rows AS row\n"
        f"MATCH (s:`{edge_config.nodes.source}` "
        f"{{`{edge_config.source_key_property}`: row.source_key}})\n"
        f"MATCH (t:`{edge_config.nodes.target}` "
        f"{{`{edge_config.target_key_property}`: row.target_key}})\n"
        f"MERGE (s)-[r:`{edge_config.type}`]->(t)\n"
        "SET r += row.properties"
    )


def _build_endpoint_preflight_query(edge_config: EdgeConfig) -> str:
    """Return Cypher that verifies every row resolves exactly two endpoints."""
    return (
        "UNWIND range(0, size($rows) - 1) AS row_index\n"
        "WITH row_index, $rows[row_index] AS row\n"
        f"OPTIONAL MATCH (s:`{edge_config.nodes.source}` "
        f"{{`{edge_config.source_key_property}`: row.source_key}})\n"
        "WITH row_index, row, count(s) AS source_matches\n"
        f"OPTIONAL MATCH (t:`{edge_config.nodes.target}` "
        f"{{`{edge_config.target_key_property}`: row.target_key}})\n"
        "RETURN row_index, source_matches, count(t) AS target_matches"
    )


class EdgeWriter:
    """Write normalized relationship records through a caller-owned driver.

    Every batch is preflighted and merged within one explicit transaction. This
    sequential foundation deliberately contains no concurrency or lock policy.
    """

    def __init__(self, driver: Driver, edge_config: EdgeConfig) -> None:
        self._driver = driver
        self._edge_config = edge_config
        self._preflight_query = _build_endpoint_preflight_query(edge_config)
        self._upsert_query = build_edge_upsert_query(edge_config)

    def write(self, record: EdgeRecord) -> None:
        """Write one edge via the same preflighted batch path."""
        self.write_batch([record])

    def write_batch(self, records: Sequence[EdgeRecord]) -> None:
        """Preflight and merge a non-empty batch atomically.

        Missing or duplicate endpoint matches raise before the merge runs, so a
        caller never receives a false durable-write acknowledgement.
        """
        if not records:
            return
        rows = [
            {
                "source_key": record.source_key,
                "target_key": record.target_key,
                "properties": record.properties,
            }
            for record in records
        ]
        with self._driver.session() as session:
            with session.begin_transaction() as transaction:
                preflight_result = transaction.run(self._preflight_query, rows=rows)
                preflight_rows = list(preflight_result)
                preflight_result.consume()
                for endpoint_counts in preflight_rows:
                    if (
                        endpoint_counts["source_matches"] != 1
                        or endpoint_counts["target_matches"] != 1
                    ):
                        raise MissingRelationshipEndpointError(
                            f"Edge '{self._edge_config.type}' did not resolve exactly "
                            "one source and target endpoint"
                        )
                transaction.run(self._upsert_query, rows=rows).consume()
                transaction.commit()


class EdgeLoader:
    """Consume one relationship topic without committing ahead of Neo4j.

    Valid records use a per-partition contiguous-resolution ledger. Malformed
    records resolve only after their metadata is fsync'd; endpoint and Neo4j
    failures resolve nothing, causing restart-safe replay.
    """

    def __init__(
        self,
        consumer: Consumer,
        writer: EdgeWriter,
        edge_config: EdgeConfig,
        topic: str | None = None,
        event_logger: logging.Logger = logger,
        loading_config: LoadingConfig | None = None,
        rejection_sink: RejectionSink | None = None,
        shutdown_requested: Event | None = None,
        clock: Callable[[], float] = time.monotonic,
        partitioner=None,
        lane_batcher=None,
        coordinator=None,
        producer: Producer | None = None,
        run_id: str = "default_run",
        replica_id: str = "0",
    ) -> None:
        self._consumer = consumer
        self._writer = writer
        self._edge_config = edge_config
        self._topic = topic or edge_config.topic
        self._logger = event_logger
        self._loading_config = loading_config or LoadingConfig(unwind_batch_size=1)
        self._rejection_sink = rejection_sink or RejectionSink(
            self._loading_config.rejection_log_path
        )
        self._shutdown_requested = shutdown_requested or Event()
        self._clock = clock
        self._partitioner = partitioner
        self._lane_batcher = lane_batcher
        self._coordinator = coordinator
        self._producer = producer
        self._run_id = run_id
        self._replica_id = replica_id
        self._assignment_epoch = 0
        self._pending_assignment_events: Deque[tuple[int, list[dict[str, object]]]] = deque()
        self._batch: list[PendingEdgeRecord] = []
        self._batch_started_at: float | None = None
        self._partition_ledgers: dict[tuple[str, int], PartitionLedger] = {}
        self._assigned_partitions: set[tuple[str, int]] = set()
        self._assignment_observed = False
        self._rebalance_failure = False
        self._deferred_messages: Deque[Message] = deque()
        self._paused = False
        self._consumer.subscribe([self._topic], on_assign=self._on_assign, on_revoke=self._on_revoke)

    def _on_assign(self, consumer: Consumer, partitions: list) -> None:
        """Accept Kafka ownership before any offset can be resolved."""
        consumer.assign(partitions)
        self._assignment_observed = True
        self._assigned_partitions = {
            (partition.topic or self._topic, partition.partition) for partition in partitions
        }
        self._paused = False
        self._assignment_epoch += 1
        if self._producer is not None:
            self._pending_assignment_events.append((
                self._assignment_epoch,
                [
                    {"topic": partition.topic or self._topic, "partition": partition.partition}
                    for partition in partitions
                ],
            ))

    def _on_revoke(self, consumer: Consumer, partitions: list) -> None:
        """Fail closed if Kafka revokes partitions with unresolved work."""
        revoked = {(partition.topic or self._topic, partition.partition) for partition in partitions}
        pending = {(item.topic, item.partition) for item in self._batch}
        pending.update(key for key, ledger in self._partition_ledgers.items() if ledger.resolved_offsets)
        pending.update((message.topic(), message.partition()) for message in self._deferred_messages)
        if revoked & pending:
            self._rebalance_failure = True
            self._logger.error(
                "Kafka partitions revoked with unresolved relationship work edge=%s partitions=%s",
                self._edge_config.type,
                sorted(revoked & pending),
            )
        # ``unassign()`` releases the entire current assignment, even when a
        # cooperative callback reports only a subset.  Do not advertise any
        # remaining partition as owned after that call.
        self._assigned_partitions.clear()
        self._paused = False
        consumer.unassign()
        self._assignment_epoch += 1
        if self._producer is not None:
            self._pending_assignment_events.append((
                self._assignment_epoch,
                [
                    {"topic": topic, "partition": partition}
                    for topic, partition in sorted(self._assigned_partitions)
                ],
            ))

    def _publish_control(
        self,
        message_type: str,
        data: Mapping[str, object],
        *,
        assignment_epoch: int | None = None,
    ) -> None:
        """Synchronously acknowledge one run-scoped edge lifecycle event."""
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
            "edge_type": self._edge_config.type,
            "replica_id": self._replica_id,
            "type": message_type,
            "assignment_epoch": (
                self._assignment_epoch if assignment_epoch is None else assignment_epoch
            ),
        }
        payload.update(data)
        try:
            self._producer.produce(
                CONTROL_TOPIC,
                key=self._run_id,
                value=json.dumps(payload),
                on_delivery=on_delivery,
            )
        except Exception as exc:
            raise ControlDeliveryError(
                f"Control event '{message_type}' could not be queued: {exc}"
            ) from exc
        try:
            undelivered_count = self._producer.flush()
        except Exception as exc:
            raise ControlDeliveryError(
                f"Control event '{message_type}' could not be flushed: {exc}"
            ) from exc
        if undelivered_count:
            raise ControlDeliveryError(
                f"Control event '{message_type}' has {undelivered_count} undelivered message(s)"
            )
        if delivery_error is not None:
            raise ControlDeliveryError(
                f"Control event '{message_type}' delivery failed: {delivery_error}"
            )

    def _publish_pending_assignments(self) -> None:
        """Durably publish every queued assignment epoch in order."""
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
        self._logger.error(
            "Relationship load failed edge=%s topic=%s partition=%s offset=%s reason=%s",
            self._edge_config.type,
            message.topic(),
            message.partition(),
            message.offset(),
            reason,
        )

    def _poll_for_callbacks(self) -> bool:
        """Dispatch rebalance callbacks without dropping a prefetched message."""
        if not self._assignment_observed:
            return True
        message = self._consumer.poll(0)
        if message is not None:
            self._deferred_messages.append(message)
        return not self._rebalance_failure

    def _owns(self, key: tuple[str, int]) -> bool:
        return not self._assignment_observed or key in self._assigned_partitions

    def _pause_assignments(self) -> None:
        if not self._assignment_observed or self._paused or not self._assigned_partitions:
            return
        self._consumer.pause(
            [TopicPartition(topic, partition) for topic, partition in sorted(self._assigned_partitions)]
        )
        self._paused = True

    def _resume_assignments(self) -> None:
        if not self._assignment_observed or not self._paused or not self._assigned_partitions:
            return
        self._consumer.resume(
            [TopicPartition(topic, partition) for topic, partition in sorted(self._assigned_partitions)]
        )
        self._paused = False

    def _commit_contiguous_resolutions(self) -> bool:
        """Synchronously commit only each owned partition's durable prefix."""
        for (topic, partition), ledger in sorted(self._partition_ledgers.items()):
            next_offset = ledger.next_offset
            while next_offset in ledger.resolved_offsets:
                next_offset += 1
            if next_offset == ledger.next_offset:
                continue
            if not self._poll_for_callbacks() or not self._owns((topic, partition)):
                self._rebalance_failure = True
                return False
            try:
                self._consumer.commit(
                    offsets=[TopicPartition(topic, partition, next_offset)], asynchronous=False
                )
            except Exception as exc:
                self._logger.error(
                    "Relationship offset commit failed edge=%s topic=%s partition=%s offset=%s reason=%s",
                    self._edge_config.type, topic, partition, next_offset, exc,
                )
                return False
            ledger.resolved_offsets.difference_update(range(ledger.next_offset, next_offset))
            ledger.next_offset = next_offset
        return True

    def _buffer_message(self, message: Message) -> bool:
        """Buffer a valid record or fsync-and-resolve a malformed one."""
        if message.error():
            self._log_failure(message, str(message.error()))
            return False
        topic, partition, offset = message.topic(), message.partition(), message.offset()
        ledger = self._partition_ledgers.setdefault(
            (topic, partition), PartitionLedger(next_offset=offset, resolved_offsets=set())
        )
        try:
            payload = message.value()
            if payload is None:
                raise ValueError("message payload is empty")
            decoded = json.loads(payload.decode("utf-8"), parse_constant=_reject_json_constant)
            record = normalize_edge_record(decoded, self._edge_config)
            if not self._batch:
                self._batch_started_at = self._clock()
            self._batch.append(PendingEdgeRecord(record, topic, partition, offset))
            return True
        except (UnicodeDecodeError, json.JSONDecodeError, EdgeRecordValidationError, ValueError) as exc:
            reason = str(exc)
            try:
                self._rejection_sink.append(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    edge_type=self._edge_config.type,
                    reason=reason,
                )
            except Exception as sink_error:
                self._log_failure(message, f"rejection sink {type(sink_error).__name__}: {sink_error}")
                return False
            self._logger.warning(
                "Malformed relationship record durably rejected edge=%s topic=%s partition=%s "
                "offset=%s reason=%s phase5_action=route_to_dlq",
                self._edge_config.type, topic, partition, offset, reason,
            )
            ledger.resolved_offsets.add(offset)
            return self._commit_contiguous_resolutions()

    def _flush_batch(self) -> bool:
        """Durably write the current batch, then resolve/commit its offsets."""
        if not self._batch:
            return True
        self._pause_assignments()
        try:
            if self._coordinator is not None:
                for pending in self._batch:
                    self._lane_batcher.add(self._partitioner.route(
                        pending.record, topic=pending.topic, partition=pending.partition, offset=pending.offset
                    ))
                results = self._coordinator.execute(self._lane_batcher.drain_all())
                if not all(result.success for result in results):
                    return False
            else:
                self._writer.write_batch([pending.record for pending in self._batch])
        except Exception as exc:
            first = self._batch[0]
            self._logger.error(
                "Relationship write failed edge=%s topic=%s partition=%s offset=%s reason=%s",
                self._edge_config.type, first.topic, first.partition, first.offset, exc,
            )
            return False
        if not self._poll_for_callbacks():
            return False
        batch_keys = {(pending.topic, pending.partition) for pending in self._batch}
        if not all(self._owns(key) for key in batch_keys):
            self._rebalance_failure = True
            return False
        for pending in self._batch:
            self._partition_ledgers[(pending.topic, pending.partition)].resolved_offsets.add(pending.offset)
        if not self._commit_contiguous_resolutions():
            return False
        self._batch.clear()
        self._batch_started_at = None
        self._resume_assignments()
        return True

    def _idle_flush_due(self) -> bool:
        return self._batch_started_at is not None and (
            (self._clock() - self._batch_started_at) * 1000 >= self._loading_config.flush_interval_ms
        )

    def _has_uncommitted_resolutions(self) -> bool:
        return any(ledger.resolved_offsets for ledger in self._partition_ledgers.values())

    def _finish_run(self) -> bool:
        """Flush all prefetched work, then durably declare a bulk drain."""
        if not self._flush_batch():
            return False
        while self._deferred_messages:
            if not self._buffer_message(self._deferred_messages.popleft()):
                return False
            if not self._flush_batch():
                return False
        if self._has_uncommitted_resolutions():
            self._logger.error(
                "Relationship loader cannot complete with offset gaps edge=%s replica=%s",
                self._edge_config.type,
                self._replica_id,
            )
            return False
        if self._producer is not None:
            self._publish_control("DRAIN_COMPLETE", {})
        return True

    def run(self, *, max_messages: int | None = None) -> int:
        """Run until cap, shutdown, or an unsafe Kafka/Neo4j condition."""
        processed = 0
        try:
            while max_messages is None or processed < max_messages:
                self._publish_pending_assignments()
                if self._rebalance_failure:
                    return 1
                if self._shutdown_requested.is_set():
                    self._logger.info(
                        "Relationship loader gracefully stopping edge=%s replica=%s",
                        self._edge_config.type,
                        self._replica_id,
                    )
                    return 0 if self._finish_run() else 1
                if self._idle_flush_due() and not self._flush_batch():
                    return 1
                message = (
                    self._deferred_messages.popleft()
                    if self._deferred_messages
                    else self._consumer.poll(min(1.0, self._loading_config.flush_interval_ms / 1000))
                )
                if message is None:
                    continue
                if not self._buffer_message(message):
                    return 1
                processed += 1
                if len(self._batch) >= self._loading_config.unwind_batch_size and not self._flush_batch():
                    return 1
        except KeyboardInterrupt:
            self._logger.warning("Relationship loader interrupted edge=%s", self._edge_config.type)
            return 1
        except ControlDeliveryError as exc:
            self._logger.error(
                "Relationship lifecycle acknowledgement failed edge=%s replica=%s reason=%s",
                self._edge_config.type,
                self._replica_id,
                exc,
            )
            return 1

        try:
            return 0 if self._finish_run() else 1
        except ControlDeliveryError as exc:
            self._logger.error(
                "Relationship lifecycle acknowledgement failed edge=%s replica=%s reason=%s",
                self._edge_config.type,
                self._replica_id,
                exc,
            )
            return 1

    def close(self) -> None:
        """Close the owned Kafka consumer."""
        self._consumer.close()


def _select_edge_config(schema, edge_type: str) -> EdgeConfig:
    """Return the single configured edge type or raise a concise error."""
    matches = [edge for edge in schema.edges if edge.type == edge_type]
    if len(matches) != 1:
        raise ValueError(f"No configured edge type named '{edge_type}'")
    return matches[0]


def build_parser() -> argparse.ArgumentParser:
    """Build the direct relationship-loader command parser."""
    parser = argparse.ArgumentParser(description="Run one schema-declared Kafka relationship loader")
    parser.add_argument("--config", default="config/graph_schema.yaml")
    parser.add_argument("--edge-type", required=True)
    parser.add_argument("--topic")
    parser.add_argument("--mode", choices=["bulk", "stream"], default="stream")
    parser.add_argument("--replica-id", default="0")
    parser.add_argument("--run-id", default="default_run")
    parser.add_argument("--max-messages", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run a selected edge loader and close Kafka/Neo4j resources on all paths."""
    args = build_parser().parse_args(argv)
    if args.max_messages is not None and args.max_messages <= 0:
        raise SystemExit("--max-messages must be positive")
    shutdown_requested = Event()
    previous_sigterm_handler = signal.signal(
        signal.SIGTERM, lambda _signum, _frame: shutdown_requested.set()
    )
    consumer = None
    producer = None
    driver = None
    try:
        schema = load_schema(args.config)
        edge_config = _select_edge_config(schema, args.edge_type)
        consumer = Consumer({
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "group.id": os.environ.get(
                "KAFKA_GROUP_ID", f"{schema.loading.consumer_group_id}-{edge_config.type}"
            ),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
            "session.timeout.ms": schema.loading.session_timeout_ms,
        })
        if args.mode == "bulk":
            producer = Producer({"bootstrap.servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            )})
        driver = get_neo4j_driver(*get_neo4j_credentials())
        writer, partitioner, lane_batcher, coordinator = build_edge_execution(edge_config, driver)
        loader = EdgeLoader(
            consumer=consumer,
            writer=writer,
            edge_config=edge_config,
            topic=args.topic,
            loading_config=schema.loading,
            shutdown_requested=shutdown_requested,
            rejection_sink=RejectionSink(
                os.environ.get("REJECTION_LOG_PATH", schema.loading.rejection_log_path)
            ),
            partitioner=partitioner,
            lane_batcher=lane_batcher,
            coordinator=coordinator,
            producer=producer,
            run_id=args.run_id,
            replica_id=args.replica_id,
        )
        return loader.run(max_messages=args.max_messages)
    except Exception as exc:
        logger.error("Unable to start relationship loader: %s", exc)
        return 1
    finally:
        try:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    logger.exception("Unable to close Kafka consumer during relationship-loader shutdown")
            if producer is not None:
                try:
                    producer.flush()
                except Exception:
                    logger.exception("Unable to flush relationship control producer during shutdown")
            if driver is not None:
                try:
                    driver.close()
                except Exception:
                    logger.exception("Unable to close Neo4j driver during relationship-loader shutdown")
        finally:
            signal.signal(signal.SIGTERM, previous_sigterm_handler)


if __name__ == "__main__":
    raise SystemExit(main())
