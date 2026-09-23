"""Schema-driven relationship validation, Neo4j writing, and Kafka loading."""
from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
import json
import logging
import os
import signal
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
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
from src.orchestrator.dependency_manager import build_conflict_families
from src.orchestrator.fleet_contract import parse_fleet_edge_types, select_shared_fleet_edges
from src.orchestrator.rotation import build_rotation_plan
from src.loader.edge_execution import build_edge_execution
from src.loader.mix_and_batch import endpoint_buckets
from src.loader.slot_admission import (
    GatedPendingRecord,
    LeaseStateError,
    SlotAdmission,
    SlotAwareAdmissionBuffer,
)
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


@dataclass(frozen=True)
class WorkerBatch:
    """One immutable poll-owner admitted batch handed to the write worker."""

    batch_id: int
    records: tuple[PendingEdgeRecord, ...]
    gated_records: tuple[GatedPendingRecord, ...]
    lease_epoch: int | None
    lease_slot_id: int | None
    lease_expires_at_ms: int | None

    def __post_init__(self) -> None:
        if isinstance(self.batch_id, bool) or not isinstance(self.batch_id, int) or self.batch_id <= 0:
            raise ValueError("worker batch id must be positive")
        provenance = [(item.topic, item.partition, item.offset) for item in self.records]
        if not provenance or len(set(provenance)) != len(provenance):
            raise ValueError("worker batch requires unique nonempty Kafka provenance")
        offsets: dict[tuple[str, int], list[int]] = {}
        for topic, partition, offset in provenance:
            offsets.setdefault((topic, partition), []).append(offset)
        if any(values != sorted(values) for values in offsets.values()):
            raise ValueError("worker batch offsets must be ordered per partition")
        gated_provenance = [item.provenance for item in self.gated_records]
        if bool(gated_provenance) != bool(self.lease_epoch is not None):
            raise ValueError("worker gated records and lease context must agree")
        if gated_provenance and set(gated_provenance) != set(provenance):
            raise ValueError("worker gated provenance must match batch records")
        context = (self.lease_epoch, self.lease_slot_id, self.lease_expires_at_ms)
        if any(value is None for value in context) and any(value is not None for value in context):
            raise ValueError("worker batch lease context must be complete or absent")


@dataclass(frozen=True)
class WorkerResult:
    """A worker outcome validated by the poll owner before offset resolution."""

    batch: WorkerBatch
    outcome: str
    error: Exception | None = None

    def __post_init__(self) -> None:
        if self.outcome not in {"SUCCESS", "WRITE_FAILURE", "CANCELLED_PRESTART"}:
            raise ValueError("worker result has invalid outcome")
        if self.outcome == "WRITE_FAILURE" and self.error is None:
            raise ValueError("worker write failure requires an exception")
        if self.outcome != "WRITE_FAILURE" and self.error is not None:
            raise ValueError("successful or cancelled worker result cannot carry an exception")


@dataclass(frozen=True)
class InFlightResourceHold:
    """A lock-linearized local hold exposed for Slice 3 fleet handoff."""

    batch_id: int
    endpoint_buckets: frozenset[int]
    lease_epoch: int | None
    slot_id: int | None


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
        slot_admission: SlotAdmission | None = None,
        slot_buffer: SlotAwareAdmissionBuffer | None = None,
        coordination_poll: Callable[[float], object | None] | None = None,
        wall_clock_ms: Callable[[], int] | None = None,
        queue_factory=Queue,
        thread_factory=Thread,
        worker_join_timeout_seconds: float = 5.0,
        worker_wait_seconds: float = 0.01,
        worker_deadline_clock: Callable[[], float] = time.monotonic,
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
        slot_dependencies = (slot_admission, slot_buffer, coordination_poll, wall_clock_ms)
        if any(item is not None for item in slot_dependencies) and not all(item is not None for item in slot_dependencies):
            raise LeaseStateError(
                f"stage=lease edge={edge_config.type} run_id={run_id} reason=slot gating dependencies must be all present or all absent"
            )
        self._slot_admission = slot_admission
        self._slot_buffer = slot_buffer
        self._coordination_poll = coordination_poll
        self._wall_clock_ms = wall_clock_ms
        self._slot_gating = slot_admission is not None
        if (
            self._slot_gating
            and slot_admission.bucket_count != self._loading_config.coordination.bucket_count
        ):
            raise LeaseStateError(
                f"stage=lease edge={edge_config.type} replica={replica_id} run_id={run_id} "
                "reason=slot admission bucket count does not match loading coordination"
            )
        self._assignment_epoch = 0
        self._pending_assignment_events: Deque[tuple[int, list[dict[str, object]]]] = deque()
        self._batch: list[PendingEdgeRecord] = []
        self._gated_batch: list[GatedPendingRecord] = []
        self._batch_started_at: float | None = None
        self._partition_ledgers: dict[tuple[str, int], PartitionLedger] = {}
        self._assigned_partitions: set[tuple[str, int]] = set()
        self._assignment_observed = False
        self._rebalance_failure = False
        self._deferred_messages: Deque[Message] = deque()
        self._paused = False
        if (
            isinstance(worker_join_timeout_seconds, bool) or not isinstance(worker_join_timeout_seconds, (int, float))
            or isinstance(worker_wait_seconds, bool) or not isinstance(worker_wait_seconds, (int, float))
            or worker_join_timeout_seconds <= 0 or worker_wait_seconds <= 0
        ):
            raise ValueError("worker timeouts must be positive")
        self._worker_join_timeout_seconds = worker_join_timeout_seconds
        self._worker_wait_seconds = worker_wait_seconds
        self._thread_factory = thread_factory
        self._worker_deadline_clock = worker_deadline_clock
        self._worker_queue: Queue[WorkerBatch | object] = queue_factory(
            maxsize=self._loading_config.slot_worker_queue_max_batches
        )
        self._worker_results: Queue[WorkerResult] = queue_factory(maxsize=0)
        self._worker_sentinel = object()
        self._worker_shutdown = Event()
        self._worker_lock = Lock()
        self._worker: Thread | None = None
        self._next_worker_batch_id = 1
        self._outstanding_batches: dict[int, WorkerBatch] = {}
        self._permit_states: dict[int, str] = {}
        self._delivered_results: dict[int, WorkerResult] = {}
        self._consumer.subscribe([self._topic], on_assign=self._on_assign, on_revoke=self._on_revoke)

    @property
    def inflight_resource_holds(self) -> tuple[InFlightResourceHold, ...]:
        """Snapshot unresolved queued/started resources for a future handoff barrier."""
        with self._worker_lock:
            return tuple(
                InFlightResourceHold(
                    batch_id=batch_id,
                    endpoint_buckets=frozenset(
                        bucket for item in batch.gated_records
                        for bucket in (item.buckets.source, item.buckets.target)
                    ),
                    lease_epoch=batch.lease_epoch,
                    slot_id=batch.lease_slot_id,
                )
                for batch_id, batch in sorted(self._outstanding_batches.items())
                if self._permit_states.get(batch_id) in {"QUEUED", "STARTED"}
            )

    def _start_worker(self) -> None:
        """Start the non-daemon writer worker exactly once."""
        if self._worker is None:
            try:
                self._worker_shutdown.clear()
                worker = self._thread_factory(
                    target=self._worker_loop,
                    name=f"edge-writer-{self._edge_config.type}-{self._replica_id}",
                    daemon=False,
                )
                worker.start()
            except Exception as exc:
                raise self._worker_failure("launch failed", exc=exc) from exc
            self._worker = worker

    def _worker_loop(self) -> None:
        """Write only poll-owner admitted batches; never access Kafka clients."""
        while True:
            batch = self._worker_queue.get()
            if batch is self._worker_sentinel:
                return
            assert isinstance(batch, WorkerBatch)
            with self._worker_lock:
                if self._worker_shutdown.is_set() or self._permit_states.get(batch.batch_id) != "QUEUED":
                    self._worker_results.put(WorkerResult(batch, "CANCELLED_PRESTART"))
                    continue
                self._permit_states[batch.batch_id] = "STARTED"
            try:
                if self._coordinator is not None:
                    for pending in batch.records:
                        self._lane_batcher.add(self._partitioner.route(pending.record, topic=pending.topic, partition=pending.partition, offset=pending.offset))
                    results = self._coordinator.execute(self._lane_batcher.drain_all())
                    failed = next((result for result in results if not result.success), None)
                    if failed is not None:
                        if failed.exception is None:
                            raise RuntimeError("lane execution failed")
                        raise RuntimeError("lane execution failed") from failed.exception
                else:
                    self._writer.write_batch([pending.record for pending in batch.records])
                self._worker_results.put(WorkerResult(batch, "SUCCESS"))
            except Exception as exc:
                self._worker_results.put(WorkerResult(batch, "WRITE_FAILURE", exc))
                # A later batch cannot be written until the poll owner has
                # observed and failed this run.  Leave it replayable instead.
                return

    def _enqueue_worker_batch(self) -> bool:
        """Hand one lease-validated snapshot to the writer without committing it."""
        self._poll_coordination()
        self._revalidate_gated_batch()
        if not self._batch:
            return True
        lease = self._slot_admission.current.lease if self._slot_gating and self._slot_admission.current else None
        batch = WorkerBatch(
            self._next_worker_batch_id, tuple(self._batch), tuple(self._gated_batch),
            None if lease is None else lease.epoch,
            None if lease is None else lease.slot_id,
            None if lease is None else lease.expires_at_ms,
        )
        self._next_worker_batch_id += 1
        with self._worker_lock:
            if batch.batch_id in self._outstanding_batches:
                raise self._slot_failure("duplicate worker batch id")
            self._outstanding_batches[batch.batch_id] = batch
            self._permit_states[batch.batch_id] = "QUEUED"
        try:
            self._worker_queue.put_nowait(batch)
        except Full as exc:
            with self._worker_lock:
                self._outstanding_batches.pop(batch.batch_id, None)
                self._permit_states.pop(batch.batch_id, None)
            raise self._worker_failure("queue is full", batch, exc) from exc
        self._batch.clear()
        self._gated_batch.clear()
        self._batch_started_at = None
        return True

    def _resolve_worker_result(self, result: WorkerResult) -> bool:
        """Resolve only one exact successful worker snapshot in the poll owner."""
        batch = self._outstanding_batches.get(result.batch.batch_id)
        if batch != result.batch or result.batch.batch_id in self._delivered_results:
            raise self._worker_failure("unexpected or duplicate result", result.batch)
        self._delivered_results[result.batch.batch_id] = result
        if result.outcome == "CANCELLED_PRESTART":
            with self._worker_lock:
                self._outstanding_batches.pop(result.batch.batch_id, None)
                self._permit_states.pop(result.batch.batch_id, None)
                self._delivered_results.pop(result.batch.batch_id, None)
            return True
        if result.outcome != "SUCCESS":
            raise self._worker_failure(f"outcome={result.outcome}", result.batch, result.error)
        self._poll_coordination(release_owned=False)
        self._verify_written_gated_batch(batch.gated_records)
        if not self._poll_for_callbacks():
            return False
        keys = {(item.topic, item.partition) for item in batch.records}
        if not all(self._owns(key) for key in keys):
            self._rebalance_failure = True
            return False
        for pending in batch.records:
            self._partition_ledgers[(pending.topic, pending.partition)].resolved_offsets.add(pending.offset)
        if not self._commit_contiguous_resolutions():
            return False
        with self._worker_lock:
            self._permit_states[result.batch.batch_id] = "RESULT"
            self._outstanding_batches.pop(result.batch.batch_id, None)
            self._permit_states.pop(result.batch.batch_id, None)
            self._delivered_results.pop(result.batch.batch_id, None)
        self._release_slot_buffer()
        return True

    def _drain_worker_results(self) -> bool:
        """Process all delivered worker outcomes in the poll-owner thread."""
        while True:
            try:
                result = self._worker_results.get_nowait()
            except Empty:
                return True
            if not self._resolve_worker_result(result):
                return False

    def _stop_worker(self) -> bool:
        """Request and join the writer worker after all results are resolved."""
        if self._worker is None:
            return True
        self._worker_shutdown.set()
        try:
            now = self._worker_deadline_clock()
            deadline = now + self._worker_join_timeout_seconds
        except Exception:
            return False
        sentinel_sent = False
        stalled_clock_reads = 0
        while True:
            try:
                current = self._worker_deadline_clock()
            except Exception:
                return False
            if current >= deadline:
                return False
            if current <= now:
                stalled_clock_reads += 1
                # A frozen injected deadline clock must not turn shutdown into
                # an unbounded busy loop.  Real monotonic clocks are allowed a
                # few equal-resolution reads around a nonblocking queue poll.
                if stalled_clock_reads >= 3:
                    return False
            else:
                stalled_clock_reads = 0
            now = current
            if not self._worker.is_alive():
                self._worker = None
                return True
            if not sentinel_sent:
                try:
                    self._worker_queue.put_nowait(self._worker_sentinel)
                    sentinel_sent = True
                except Full:
                    self._consumer.poll(0)
                    try:
                        self._poll_coordination()
                        if not self._drain_worker_results():
                            return False
                    except Exception:
                        return False
                    continue
            self._worker.join(timeout=min(self._worker_wait_seconds, max(0.0, deadline - current)))
            if not self._worker.is_alive():
                self._worker = None
                return True
            try:
                self._consumer.poll(0)
                self._poll_coordination()
                if not self._drain_worker_results():
                    return False
            except Exception:
                return False
        return False

    def _slot_now_ms(self) -> int:
        """Return an injected wall-clock value only while slot gating is active."""
        assert self._wall_clock_ms is not None
        try:
            value = self._wall_clock_ms()
        except Exception as exc:
            raise self._slot_failure("wall clock failed", exc) from exc
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise self._slot_failure("wall clock returned invalid value")
        return value

    def _slot_failure(self, reason: str, exc: Exception | None = None) -> LeaseStateError:
        """Return one attributed lease failure without exposing control payloads."""
        lease = self._slot_admission.current if self._slot_admission is not None else None
        suffix = "" if lease is None else f" epoch={lease.epoch} slot={lease.slot_id}"
        error = LeaseStateError(
            f"stage=lease edge={self._edge_config.type} replica={self._replica_id} "
            f"run_id={self._run_id}{suffix} reason={reason}"
        )
        if exc is not None:
            error.__cause__ = exc
        return error

    def _worker_failure(self, reason: str, batch: WorkerBatch | None = None, exc: Exception | None = None) -> RuntimeError:
        """Return an attributed worker failure without exposing record payloads."""
        suffix = ""
        if batch is not None and batch.lease_epoch is not None:
            suffix = f" epoch={batch.lease_epoch} slot={batch.lease_slot_id}"
        error = RuntimeError(
            f"stage=worker edge={self._edge_config.type} replica={self._replica_id} run_id={self._run_id}{suffix} reason={reason}"
        )
        if exc is not None:
            error.__cause__ = exc
        return error

    def _append_gated_batch(self, items: Sequence[GatedPendingRecord]) -> None:
        """Append lease-owned records to the ordinary batch while retaining metadata."""
        if not items:
            return
        if not self._batch:
            self._batch_started_at = self._clock()
        self._gated_batch.extend(items)
        self._batch.extend(item.pending for item in items)

    def _release_slot_buffer(self) -> None:
        """Move currently owned lease-gated work into the write batch."""
        if not self._slot_gating:
            return
        assert self._slot_admission is not None and self._slot_buffer is not None
        try:
            blocked = frozenset(
                bucket for hold in self.inflight_resource_holds for bucket in hold.endpoint_buckets
            )
            released = self._slot_buffer.release_owned(
                self._slot_admission, now_ms=self._slot_now_ms(), blocked_buckets=blocked
            )
        except LeaseStateError as exc:
            raise self._slot_failure("slot buffer release failed", exc) from exc
        self._append_gated_batch(released)

    def _poll_coordination(self, *, release_owned: bool = True) -> None:
        """Drain coordination values before any admission or write boundary."""
        if not self._slot_gating:
            return
        assert self._coordination_poll is not None and self._slot_admission is not None
        while True:
            lease = self._slot_admission.current
            suffix = "" if lease is None else f" epoch={lease.epoch} slot={lease.slot_id}"
            try:
                message = self._coordination_poll(0)
            except Exception as exc:
                raise self._slot_failure("coordination poll failed", exc) from exc
            if message is None:
                return
            if hasattr(message, "error") and message.error():
                raise self._slot_failure("coordination consumer error")
            value = message.value() if hasattr(message, "value") else message
            if not isinstance(value, bytes):
                raise self._slot_failure("coordination value is not bytes")
            try:
                accepted = self._slot_admission.accept_lease(value, now_ms=self._slot_now_ms())
            except LeaseStateError as exc:
                raise self._slot_failure("clock lease rejected", exc) from exc
            if accepted:
                self._cancel_queued_for_new_lease()
            if accepted and release_owned:
                self._release_slot_buffer()

    def _cancel_queued_for_new_lease(self) -> None:
        """Cancel only not-started old-generation batches and re-buffer them."""
        if not self._slot_gating or self._slot_buffer is None:
            return
        returning: list[GatedPendingRecord] = []
        with self._worker_lock:
            for batch_id, batch in self._outstanding_batches.items():
                if self._permit_states.get(batch_id) == "QUEUED":
                    self._permit_states[batch_id] = "CANCELLED_PRESTART"
                    returning.extend(batch.gated_records)
        if returning:
            try:
                self._slot_buffer.requeue(returning)
            except LeaseStateError as exc:
                raise self._slot_failure("queued worker batch requeue failed", exc) from exc

    def _revalidate_gated_batch(self) -> None:
        """Requeue work that lost lease ownership before a durable write starts."""
        if not self._slot_gating or not self._gated_batch:
            return
        assert self._slot_admission is not None and self._slot_buffer is not None
        owned: list[GatedPendingRecord] = []
        returned: list[GatedPendingRecord] = []
        now_ms = self._slot_now_ms()
        try:
            for item in self._gated_batch:
                if self._slot_admission.owns(item.buckets, now_ms=now_ms):
                    owned.append(item)
                else:
                    returned.append(item)
        except LeaseStateError as exc:
            raise self._slot_failure("batch lease revalidation failed", exc) from exc
        if returned:
            try:
                self._slot_buffer.requeue(returned)
            except LeaseStateError as exc:
                raise self._slot_failure("slot buffer requeue failed", exc) from exc
        self._gated_batch = owned
        self._batch = [item.pending for item in owned]
        if not self._batch:
            self._batch_started_at = None

    def _verify_written_gated_batch(self, written: Sequence[GatedPendingRecord]) -> None:
        """Fail closed when a lease changed while a synchronous write blocked."""
        if not self._slot_gating or not written:
            return
        assert self._slot_admission is not None
        now_ms = self._slot_now_ms()
        try:
            for item in written:
                if not self._slot_admission.owns(item.buckets, now_ms=now_ms):
                    raise LeaseStateError("lease ownership changed during durable write")
        except LeaseStateError as exc:
            raise self._slot_failure("post-write lease revalidation failed", exc) from exc

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
        with self._worker_lock:
            for batch in self._outstanding_batches.values():
                pending.update((item.topic, item.partition) for item in batch.records)
            for result in self._delivered_results.values():
                pending.update((item.topic, item.partition) for item in result.batch.records)
        if self._slot_buffer is not None:
            pending.update(self._slot_buffer.unresolved_partitions)
        pending.update(key for key, ledger in self._partition_ledgers.items() if ledger.resolved_offsets)
        pending.update((message.topic(), message.partition()) for message in self._deferred_messages)
        if revoked & pending:
            self._rebalance_failure = True
            if self._slot_gating:
                lease = self._slot_admission.current if self._slot_admission is not None else None
                suffix = "" if lease is None else f" epoch={lease.epoch} slot={lease.slot_id}"
                self._logger.error(
                    "stage=rebalance edge=%s replica=%s run_id=%s%s revoked unresolved partitions=%s",
                    self._edge_config.type, self._replica_id, self._run_id, suffix, sorted(revoked & pending),
                )
            else:
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
            pending_record = PendingEdgeRecord(record, topic, partition, offset)
            if self._slot_gating:
                assert self._slot_buffer is not None
                buckets = endpoint_buckets(record, self._loading_config.coordination.bucket_count)
                try:
                    self._slot_buffer.add(pending_record, buckets)
                except LeaseStateError as exc:
                    raise self._slot_failure("slot buffer add failed", exc) from exc
                self._release_slot_buffer()
                return True
            if not self._batch:
                self._batch_started_at = self._clock()
            self._batch.append(pending_record)
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
        if self._slot_gating:
            if self._worker is not None:
                return self._enqueue_worker_batch()
            self._poll_coordination()
            self._revalidate_gated_batch()
        if not self._batch:
            return True
        written_batch = tuple(self._batch)
        written_gated_batch = tuple(self._gated_batch)
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
        if self._slot_gating:
            self._poll_coordination(release_owned=False)
            self._verify_written_gated_batch(written_gated_batch)
        if not self._poll_for_callbacks():
            return False
        batch_keys = {(pending.topic, pending.partition) for pending in written_batch}
        if not all(self._owns(key) for key in batch_keys):
            self._rebalance_failure = True
            return False
        for pending in written_batch:
            self._partition_ledgers[(pending.topic, pending.partition)].resolved_offsets.add(pending.offset)
        if not self._commit_contiguous_resolutions():
            return False
        self._batch.clear()
        self._gated_batch.clear()
        self._batch_started_at = None
        if self._slot_gating:
            self._release_slot_buffer()
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
        if self._slot_gating:
            self._poll_coordination()
            self._release_slot_buffer()
        if not self._flush_batch():
            return False
        while self._deferred_messages:
            if not self._buffer_message(self._deferred_messages.popleft()):
                return False
            if not self._flush_batch():
                return False
        while self._outstanding_batches or self._batch:
            if self._batch and not self._flush_batch():
                return False
            self._poll_coordination()
            if self._slot_gating and self._slot_admission is not None:
                self._slot_admission.require_current(now_ms=self._slot_now_ms())
            if not self._drain_worker_results():
                return False
            if self._outstanding_batches:
                self._consumer.poll(0)
                try:
                    result = self._worker_results.get(timeout=self._worker_wait_seconds)
                except Empty:
                    continue
                if not self._resolve_worker_result(result):
                    return False
        if self._slot_gating and not self._stop_worker():
            raise self._worker_failure("shutdown failed")
        if self._slot_buffer is not None and len(self._slot_buffer):
            self._logger.error(
                "Relationship loader cannot complete with unleased work edge=%s replica=%s",
                self._edge_config.type,
                self._replica_id,
            )
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
            if self._slot_gating:
                self._start_worker()
            while max_messages is None or processed < max_messages:
                self._poll_coordination()
                if self._slot_gating and self._slot_admission is not None and self._slot_admission.current is not None:
                    self._slot_admission.require_current(now_ms=self._slot_now_ms())
                if not self._drain_worker_results():
                    return 1
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
                self._poll_coordination()
                if not self._drain_worker_results():
                    return 1
                if message is None:
                    continue
                if not self._buffer_message(message):
                    return 1
                processed += 1
                if len(self._batch) >= self._loading_config.unwind_batch_size and not self._flush_batch():
                    return 1
            return 0 if self._finish_run() else 1
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
        except LeaseStateError as exc:
            self._logger.error("Relationship slot lease failure edge=%s replica=%s reason=%s", self._edge_config.type, self._replica_id, exc)
            return 1
        except RuntimeError as exc:
            self._logger.error(
                "Relationship worker failure edge=%s replica=%s reason=%s",
                self._edge_config.type, self._replica_id, exc, exc_info=True,
            )
            return 1
        finally:
            if self._worker is not None and not self._stop_worker():
                self._logger.error(
                    "stage=worker edge=%s replica=%s run_id=%s reason=shutdown join failed",
                    self._edge_config.type, self._replica_id, self._run_id,
                )

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
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--slot-gating", action="store_true")
    parser.add_argument("--coordination-topic")
    parser.add_argument("--fleet-edge-types")
    parser.add_argument("--max-messages", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run a selected edge loader and close Kafka/Neo4j resources on all paths."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.max_messages is not None and args.max_messages <= 0:
        raise SystemExit("--max-messages must be positive")
    if args.coordination_topic is not None and not args.slot_gating:
        parser.error("--coordination-topic requires --slot-gating")
    if args.fleet_edge_types is not None and not args.slot_gating:
        parser.error("--fleet-edge-types requires --slot-gating")
    if args.slot_gating and (not isinstance(args.coordination_topic, str) or not args.coordination_topic.strip()):
        parser.error("--slot-gating requires a nonblank --coordination-topic")
    if args.slot_gating and (not isinstance(args.run_id, str) or not args.run_id.strip()):
        parser.error("--slot-gating requires an explicitly supplied nonblank --run-id")
    if args.slot_gating and (not isinstance(args.fleet_edge_types, str) or not args.fleet_edge_types.strip()):
        parser.error("--slot-gating requires --fleet-edge-types")
    run_id = args.run_id if args.run_id is not None else "default_run"
    shutdown_requested = Event()
    previous_sigterm_handler = signal.signal(
        signal.SIGTERM, lambda _signum, _frame: shutdown_requested.set()
    )
    consumer = None
    coordination_consumer = None
    producer = None
    driver = None
    edge_config = None
    exit_code = 1
    finalization_error = False
    try:
        schema = load_schema(args.config)
        edge_config = _select_edge_config(schema, args.edge_type)
        rotation_plan = None
        if args.slot_gating:
            if args.coordination_topic != schema.loading.coordination.topic:
                raise ValueError("stage=lease reason=coordination topic does not match schema")
            fleet_types = parse_fleet_edge_types(args.fleet_edge_types)
            fleet_edges = select_shared_fleet_edges(schema.edges, fleet_types)
            if edge_config.type not in fleet_types:
                raise ValueError("stage=lease reason=edge type is absent from fleet contract")
            rotation_plan = build_rotation_plan(
                build_conflict_families(fleet_edges),
                bucket_count=schema.loading.coordination.bucket_count,
            )
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        work_group = os.environ.get(
            "KAFKA_GROUP_ID", f"{schema.loading.consumer_group_id}-{edge_config.type}"
        )
        coordination_group = None
        if args.slot_gating:
            coordination_group = os.environ.get(
                "KAFKA_COORDINATION_GROUP_ID",
                f"{work_group}-clock-{edge_config.type}-{run_id}-{args.replica_id}",
            )
            if not isinstance(work_group, str) or not work_group.strip():
                raise ValueError("stage=coordination reason=work consumer group is blank")
            if not isinstance(coordination_group, str) or not coordination_group.strip():
                raise ValueError("stage=coordination reason=coordination consumer group is blank")
            if work_group == coordination_group:
                raise ValueError("stage=coordination reason=work and coordination consumer groups must differ")
        consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": work_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
            "session.timeout.ms": schema.loading.session_timeout_ms,
        })
        slot_kwargs = {}
        if args.slot_gating:
            coordination_consumer = Consumer({
                "bootstrap.servers": bootstrap_servers,
                "group.id": coordination_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
                "session.timeout.ms": schema.loading.session_timeout_ms,
            })
            coordination_consumer.subscribe([args.coordination_topic])
            slot_kwargs = {
                "slot_admission": SlotAdmission(
                    edge_type=edge_config.type, run_id=run_id,
                    bucket_count=schema.loading.coordination.bucket_count,
                    rotation_plan=rotation_plan,
                    endpoint_labels=(edge_config.nodes.source, edge_config.nodes.target),
                ),
                "slot_buffer": SlotAwareAdmissionBuffer(
                    max_records=schema.loading.slot_buffer_max_records,
                ),
                "coordination_poll": coordination_consumer.poll,
                "wall_clock_ms": lambda: int(time.time() * 1000),
            }
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
            run_id=run_id,
            replica_id=args.replica_id,
            **slot_kwargs,
        )
        exit_code = loader.run(max_messages=args.max_messages)
    except Exception as exc:
        logger.error("Unable to start relationship loader: %s", exc)
        exit_code = 1
    finally:
        try:
            if coordination_consumer is not None:
                try:
                    coordination_consumer.close()
                except Exception as exc:
                    finalization_error = True
                    logger.error(
                        "stage=coordination edge=%s replica=%s run_id=%s reason=close failed: %s",
                        args.edge_type, args.replica_id, run_id, exc,
                    )
            if consumer is not None:
                try:
                    consumer.close()
                except Exception as exc:
                    finalization_error = True
                    logger.error(
                        "stage=shutdown edge=%s replica=%s run_id=%s component=work-consumer reason=close failed: %s",
                        args.edge_type, args.replica_id, run_id, exc,
                    )
            if producer is not None:
                try:
                    producer.flush()
                except Exception as exc:
                    finalization_error = True
                    logger.error(
                        "stage=shutdown edge=%s replica=%s run_id=%s component=producer reason=flush failed: %s",
                        args.edge_type, args.replica_id, run_id, exc,
                    )
            if driver is not None:
                try:
                    driver.close()
                except Exception as exc:
                    finalization_error = True
                    logger.error(
                        "stage=shutdown edge=%s replica=%s run_id=%s component=driver reason=close failed: %s",
                        args.edge_type, args.replica_id, run_id, exc,
                    )
        finally:
            signal.signal(signal.SIGTERM, previous_sigterm_handler)
    return 1 if finalization_error else exit_code


if __name__ == "__main__":
    raise SystemExit(main())
