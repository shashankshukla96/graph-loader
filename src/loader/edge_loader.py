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

from confluent_kafka import OFFSET_BEGINNING, Consumer, Message, Producer, TopicPartition
from neo4j import Driver

from src.cli import get_neo4j_credentials
from src.loader.control import CONTROL_TOPIC
from src.loader.dead_letter import (
    DeadLetterRecordSink, MalformedRecordSink, RejectionSink, _reason_code, build_malformed_record_sink,
)
from src.loader.node_loader import (
    ControlDeliveryError,
    PartitionLedger,
    _reject_json_constant,
)
from src.loader.edge_records import (  # noqa: F401 - re-exported
    EdgeRecord, EdgeRecordValidationError, _normalize_endpoint, _raise_error, normalize_edge_record,
)
from src.models.schema import EdgeConfig, LoadingConfig, edge_parked_pair_index_topic_name, require_no_precedence_in_stream
from src.orchestrator.schema_initializer import get_neo4j_driver
from src.orchestrator.dependency_manager import build_conflict_families
from src.orchestrator.fleet_contract import parse_fleet_edge_types, select_shared_fleet_edges
from src.orchestrator.rotation import build_rotation_plan
from src.loader.edge_execution import (
    LeaseDrainAbandoned, build_edge_execution, lease_mode_settings, streaming_lease_client_factory,
)
from src.loader.mix_and_batch import endpoint_buckets
from src.loader.lease_client import EDGE_OUTER_BATCH_ENV, MAX_EDGE_OUTER_BATCH_SIZE
from src.loader.parked_pair_index import ParkedPairIndex, warn_index_leftovers
from src.loader.write_timing_capture import configure_write_timing_capture
from src.loader.write_outage import (
    FAIL as OUTAGE_FAIL, FATAL as OUTAGE_FATAL, WAIT_DROPPED, WAIT_POLL_FAILED, WAIT_STOPPED,
    StreamWriteOutage, WriteOutageDrainAbandoned,
)
from src.loader.slot_admission import (
    GatedPendingRecord,
    LeaseStateError,
    SlotAdmission,
    SlotAwareAdmissionBuffer,
)
from src.utils.schema_loader import load_schema
from src.utils.neo4j_session import open_session
from src.utils import metrics as metrics_module
from src.utils.consumer_groups import stream_edge_group


logger = logging.getLogger(__name__)


class MissingRelationshipEndpointError(RuntimeError):
    """Raised when a relationship record cannot resolve both endpoint nodes."""


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
        if self.outcome not in {"SUCCESS", "WRITE_FAILURE", "CANCELLED_PRESTART", "DRAIN_ABANDONED"}:
            raise ValueError("worker result has invalid outcome")
        if self.outcome in {"WRITE_FAILURE", "DRAIN_ABANDONED"} and self.error is None:
            raise ValueError("worker write failure requires an exception")
        if self.outcome not in {"WRITE_FAILURE", "DRAIN_ABANDONED"} and self.error is not None:
            raise ValueError("successful or cancelled worker result cannot carry an exception")


@dataclass(frozen=True)
class InFlightResourceHold:
    """A lock-linearized local hold exposed for Slice 3 fleet handoff."""

    batch_id: int
    endpoint_buckets: frozenset[int]
    lease_epoch: int | None
    slot_id: int | None


class StaticAssignmentError(RuntimeError):
    """Raised when a bulk replica cannot establish its static partition set."""


def static_partition_assignment(
    partitions: Sequence[int], replica_index: int, replica_count: int,
) -> tuple[int, ...]:
    """Return the partitions ``{p : p % replica_count == replica_index}``.

    Every partition id belongs to exactly one replica of ``range(replica_count)``,
    so the replicas of one bulk edge cover the topic disjointly without a
    consumer-group rebalance.  A replica whose index is at least the number of
    partitions (more replicas than partitions) owns nothing.
    """
    if (
        isinstance(replica_count, bool) or not isinstance(replica_count, int) or replica_count < 1
        or isinstance(replica_index, bool) or not isinstance(replica_index, int)
        or not 0 <= replica_index < replica_count
    ):
        raise ValueError("static assignment requires 0 <= replica_index < replica_count")
    if any(isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in partitions):
        raise ValueError("static assignment partitions must be nonnegative integers")
    return tuple(sorted(item for item in set(partitions) if item % replica_count == replica_index))


def static_topic_partition_assignment(
    topic_partitions: Sequence[tuple[str, int]], replica_index: int, replica_count: int,
) -> tuple[tuple[str, int], ...]:
    """Round-robin the sorted ``(topic, partition)`` list: replica ``i`` owns entries ``i, i+N, ...``.

    Deterministic, exactly-once across replicas, and (unlike ``p % N`` per topic) it does not leave
    replicas idle when each of several topics has fewer partitions than replicas. For one topic
    with partitions ``0..n-1`` it equals :func:`static_partition_assignment`.
    """
    ordered = sorted(set(topic_partitions))
    static_partition_assignment([partition for _, partition in ordered], replica_index, replica_count)  # validates
    return tuple(ordered[replica_index::replica_count])


def build_edge_upsert_query(edge_config: EdgeConfig) -> str:
    """Return schema-derived Cypher for idempotently upserting relationships."""
    edge_config.single_pair  # v7-S7: a multi-pair parent raises MultiPairEdgeError; pass a pair view
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
    edge_config.single_pair  # v7-S7: views only
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
        with open_session(self._driver) as session:
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
        topics: Sequence[str] | None = None,
        event_logger: logging.Logger = logger,
        loading_config: LoadingConfig | None = None,
        rejection_sink: MalformedRecordSink | None = None,
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
        static_assignment: tuple[int, int] | None = None,
        metadata_timeout_seconds: float = 30.0,
        metrics: "metrics_module.GraphLoaderMetrics | None" = None,
        admission_index=None,
        admission_warn_after_seconds: float = 10.0,
        write_outage: StreamWriteOutage | None = None,
    ) -> None:
        self._consumer = consumer
        self._writer = writer
        # Stream outage ride-out (src/loader/write_outage.py). Only
        # edge_loader.main installs it, and only for --mode stream without
        # --slot-gating. With None (bulk, planned and every other caller),
        # _flush_batch is exactly the fail-fast _flush_batch_once.
        if write_outage is not None and (
            static_assignment is not None or any(
                item is not None for item in (slot_admission, slot_buffer, coordination_poll, wall_clock_ms)
            )
        ):
            raise ValueError(
                f"edge={edge_config.type} the write-outage ride-out is for unsliced stream loaders only "
                "(no static assignment, no slot gating)"
            )
        self._write_outage = write_outage
        self._last_write_failure: Exception | None = None
        self._outage_paused = False
        # v7-S4 admission barrier: with a parked-pair index, every assignment
        # holds the assigned partitions (paused, prefetched records deferred)
        # until this process's copy of the index has caught up with the index
        # as of the assignment. The previous owner indexed its unresolved rows
        # before committing past them, so after the barrier `is_blocked` and the
        # sequenced `add` never act on a view that lacks them. Never times out
        # into a stale view: it waits, warns and exports
        # graph_loader_parked_pair_index_wait_seconds; a stop still exits.
        self._admission_index = admission_index
        self._admission_warn_after_seconds = admission_warn_after_seconds
        self._admission_barrier: dict | None = None
        self._barrier_paused = False
        self._edge_config = edge_config
        # v7-S7: a multi-pair type normalises each record with the pair view of
        # the topic it was read from (decision 3: the topic decides the pair,
        # never the record). Empty for single-pair types, whose path is unchanged.
        self._config_for_topic: dict[str, EdgeConfig] = (
            {topic: edge_config.topic_view(topic) for topic in edge_config.topics}
            if edge_config.is_multi_pair else {}
        )
        # v7-S2: one loader group reads every topic of the relationship type.
        # ``topics`` wins over the legacy single ``topic``; with neither, the
        # schema's ``edge_config.topics`` (all of them) are consumed.
        if topics is not None and topic:
            raise ValueError(f"edge={edge_config.type} pass topic or topics, not both")
        if topics is not None:
            resolved_topics = list(topics)
            if not resolved_topics or any(not isinstance(t, str) or not t.strip() for t in resolved_topics):
                raise ValueError(f"edge={edge_config.type} topics must be a nonempty list of nonblank names")
        elif topic:
            resolved_topics = [topic]
        else:
            resolved_topics = list(edge_config.topics)
        if len(set(resolved_topics)) != len(resolved_topics):
            raise ValueError(f"edge={edge_config.type} topics must be unique")
        self._topics: tuple[str, ...] = tuple(resolved_topics)
        # Log label / fallback only; ledgers, commits and revokes key on
        # (message topic, partition), never on this.
        self._topic = ",".join(self._topics)
        self._logger = event_logger
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        self._loading_config = loading_config or LoadingConfig(unwind_batch_size=1)
        self._outer_batch_size = (
            self._loading_config.edge_unwind_batch_size
            or self._loading_config.unwind_batch_size
        )
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
        self._write_paused = False
        self._buffer_paused = False
        # True once the current synchronous batch is durably written but not
        # yet committed, so a revoke-time settlement never rewrites it.
        self._batch_written = False
        # Set while Kafka runs a rebalance callback: nested polling is unsafe.
        self._in_rebalance_callback = False
        # Revoke-time settlement is only attempted while run() owns the loop;
        # a close() after run() returned must not write or commit anything.
        self._running = False
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
        self._static_assignment = static_assignment
        if static_assignment is not None:
            if self._slot_gating:
                raise ValueError("static partition assignment cannot combine with slot gating")
            if (
                isinstance(metadata_timeout_seconds, bool)
                or not isinstance(metadata_timeout_seconds, (int, float))
                or metadata_timeout_seconds <= 0
            ):
                raise ValueError("metadata timeout must be positive")
            self._assign_static(*static_assignment, timeout=metadata_timeout_seconds)
        else:
            self._consumer.subscribe(list(self._topics), on_assign=self._on_assign, on_revoke=self._on_revoke)

    def _assign_static(self, replica_index: int, replica_count: int, *, timeout: float) -> None:
        """Own the round-robin share of the sorted ``(topic, partition)`` list (entries ``i, i+N, ...``) without joining the group.

        Bulk replicas all start from one launch, so group membership only adds
        rebalances (a late sibling revokes every earlier member's partitions
        mid-flush).  ``assign()`` never rebalances.  Positions resume from the
        work group's committed offsets, or the earliest offset when none exist;
        commits still go to the same group so the bulk monitor can prove the
        committed offsets reach its boundary.  The resulting set -- possibly
        empty when there are more replicas than partitions -- is queued as the
        one epoch-1 ASSIGNMENT/IDLE_SURPLUS acknowledgement.
        """
        owned = list(static_topic_partition_assignment(
            [
                (topic_name, partition)
                for topic_name in self._topics
                for partition in self._static_topic_partitions(timeout, topic_name)
            ],
            replica_index, replica_count,
        ))
        requested = [TopicPartition(name, partition) for name, partition in owned]
        assignment: list[TopicPartition] = []
        if requested:
            try:
                committed = self._consumer.committed(requested, timeout=timeout)
            except Exception as exc:
                raise StaticAssignmentError(
                    f"stage=assign edge={self._edge_config.type} replica={self._replica_id} "
                    f"run_id={self._run_id} reason=committed offsets unavailable: {exc}"
                ) from exc
            by_partition = {
                (item.topic, item.partition): item for item in (committed or ())
            }
            for name, partition in owned:
                item = by_partition.get((name, partition))
                if item is None or item.error is not None:
                    raise StaticAssignmentError(
                        f"stage=assign edge={self._edge_config.type} replica={self._replica_id} "
                        f"run_id={self._run_id} topic={name} partition={partition} "
                        f"reason=committed offset lookup failed: {None if item is None else item.error}"
                    )
                offset = item.offset if isinstance(item.offset, int) and item.offset >= 0 else OFFSET_BEGINNING
                assignment.append(TopicPartition(name, partition, offset))
        self._consumer.assign(assignment)
        self._assignment_observed = True
        self._assigned_partitions = set(owned)
        self._assignment_epoch = 1
        if self._producer is not None:
            self._pending_assignment_events.append((
                self._assignment_epoch,
                [{"topic": name, "partition": partition} for name, partition in owned],
            ))
        self._logger.info(
            "stage=assign edge=%s topic=%s replica=%s/%s run_id=%s mode=static partitions=%s offsets=%s",
            self._edge_config.type, self._topic, replica_index, replica_count, self._run_id,
            [f"{name}:{partition}" if len(self._topics) > 1 else partition for name, partition in owned],
            [item.offset for item in assignment],
        )

    def _static_topic_partitions(self, timeout: float, topic_name: str | None = None) -> list[int]:
        """Return one work topic's partition ids from broker metadata."""
        topic_name = topic_name if topic_name is not None else self._topics[0]
        try:
            metadata = self._consumer.list_topics(topic=topic_name, timeout=timeout)
        except Exception as exc:
            raise StaticAssignmentError(
                f"stage=assign edge={self._edge_config.type} replica={self._replica_id} "
                f"run_id={self._run_id} topic={topic_name} reason=topic metadata unavailable: {exc}"
            ) from exc
        topic = metadata.topics.get(topic_name) if metadata is not None else None
        if topic is None or getattr(topic, "error", None) is not None or not getattr(topic, "partitions", None):
            raise StaticAssignmentError(
                f"stage=assign edge={self._edge_config.type} replica={self._replica_id} "
                f"run_id={self._run_id} topic={topic_name} reason=topic has no partition metadata"
            )
        return [int(partition) for partition in topic.partitions]

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
            started_at = time.perf_counter()
            write_failure: Exception | None = None
            try:
                if self._coordinator is not None:
                    results = self._execute_coordinated(batch.records)
                    failed = next((result for result in results if not result.success), None)
                    if failed is not None:
                        if isinstance(failed.exception, LeaseDrainAbandoned):
                            self._worker_results.put(WorkerResult(batch, "DRAIN_ABANDONED", failed.exception))
                            return
                        if failed.exception is None:
                            raise RuntimeError("lane execution failed")
                        raise RuntimeError("lane execution failed") from failed.exception
                else:
                    self._writer.write_batch([pending.record for pending in batch.records])
            except Exception as exc:
                write_failure = exc

            if write_failure is None:
                # Log + metrics emission happen outside the `try:` that
                # guards the write itself (P6-S2 review finding S4-1's
                # pattern, applied here too): a metrics-library exception
                # here must never be mistaken for a write failure this far
                # after the write already succeeded. The whole block is
                # additionally wrapped so that even an unexpected failure
                # in the log line itself (e.g. `_flush_detail()`) or in
                # `_worker_results.put` can never leave this worker batch
                # unresolved (P6-S2 review finding N-a): the write is
                # already durable by this point, so SUCCESS is still
                # reported regardless.
                try:
                    elapsed_seconds = time.perf_counter() - started_at
                    self._logger.info(
                        "stage=edge-write edge=%s topic=%s replica=%s run_id=%s "
                        "records=%s write_ms=%.3f%s",
                        self._edge_config.type, self._topic, self._replica_id,
                        self._run_id, len(batch.records),
                        elapsed_seconds * 1000, self._flush_detail(),
                    )
                    # One observe/inc per worker batch (never per
                    # row/lane) - P6-S2 Story 5. Routed through safe_emit.
                    metrics_module.safe_emit(lambda: self._metrics.write_duration.labels(
                        kind="edge", name=self._edge_config.type, stage="edge-write"
                    ).observe(elapsed_seconds))
                    metrics_module.safe_emit(lambda: self._metrics.records_consumed.labels(
                        kind="edge", name=self._edge_config.type,
                        label_or_type=self._edge_config.type,
                    ).inc(len(batch.records)))
                except Exception as post_write_exc:  # noqa: BLE001 - see comment above
                    self._logger.warning(
                        "stage=edge-write-post-write-error edge=%s reason=%s",
                        self._edge_config.type, post_write_exc,
                    )
                self._worker_results.put(WorkerResult(batch, "SUCCESS"))
            else:
                self._logger.error(
                    "Relationship worker write failed edge=%s topic=%s replica=%s "
                    "run_id=%s records=%s write_ms=%.3f reason=%s",
                    self._edge_config.type, self._topic, self._replica_id,
                    self._run_id, len(batch.records),
                    (time.perf_counter() - started_at) * 1000, write_failure,
                )
                self._worker_results.put(WorkerResult(batch, "WRITE_FAILURE", write_failure))
                # A later batch cannot be written until the poll owner has
                # observed and failed this run.  Leave it replayable instead.
                return

    def _abandon_drain(self, records: int, reason: Exception | None) -> None:
        """Log that a stopping loader gave up on an unflushed batch.

        No offset is committed for it, so it is redelivered and replayed (MERGE is
        idempotent); cells of the flush that were already written are replayed too.
        The caller then raises ``LeaseDrainAbandoned``, which ``_run`` turns into exit 0.
        """
        self._logger.warning(
            "stage=edge-drain-abandoned edge=%s replica=%s run_id=%s records=%s "
            "reason=no bucket lease within the stop drain budget (%s); batch left uncommitted for redelivery",
            self._edge_config.type, self._replica_id, self._run_id, records, reason,
        )

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
        self._refresh_slot_backpressure()
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
        if result.outcome == "DRAIN_ABANDONED":
            self._abandon_drain(len(result.batch.records), result.error)
            raise result.error
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
        self._refresh_slot_backpressure()

    def _slot_capacity_used(self) -> int:
        """Reserve room for records that may return from an unstarted write."""
        if self._slot_buffer is None:
            return 0
        with self._worker_lock:
            queued = sum(
                len(batch.gated_records)
                for batch_id, batch in self._outstanding_batches.items()
                if self._permit_states.get(batch_id) == "QUEUED"
            )
        return len(self._slot_buffer) + len(self._gated_batch) + queued

    def _refresh_slot_backpressure(self) -> None:
        if self._slot_buffer is None:
            return
        self._buffer_paused = self._slot_capacity_used() >= self._slot_buffer.max_records
        self._sync_assignment_pause()

    def _poll_coordination(self, *, release_owned: bool = True) -> None:
        """Drain coordination values before any admission or write boundary."""
        if not self._slot_gating:
            return
        assert self._coordination_poll is not None and self._slot_admission is not None
        while True:
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
            self._refresh_slot_backpressure()

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
        self._refresh_slot_backpressure()

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
            (partition.topic or self._topics[0], partition.partition) for partition in partitions
        }
        self._paused = False
        self._write_paused = False
        if self._admission_index is not None and self._assigned_partitions:
            self._admission_barrier = {"snapshot": None, "since": self._clock(), "warned_at": None}
            self._barrier_paused = True
        self._refresh_slot_backpressure()
        self._sync_assignment_pause()
        self._assignment_epoch += 1
        if self._producer is not None:
            self._pending_assignment_events.append((
                self._assignment_epoch,
                [
                    {"topic": partition.topic or self._topics[0], "partition": partition.partition}
                    for partition in partitions
                ],
            ))

    def _settle_before_revoke(self, released: set[tuple[str, int]]) -> None:
        """Durably finish buffered work while Kafka still grants ownership.

        Under the eager protocol the revoke callback runs before this member
        rejoins, so its partitions are still owned and a synchronous commit is
        valid (Kafka's standard commit-on-revoke).  The buffered flush -- in
        classic, lane, or bucket-lease mode -- is written (unless already
        written) and committed; prefetched messages of released partitions are
        dropped uncommitted for their next owner to re-read.  Any failure
        leaves work pending, so ``_on_revoke`` still fails closed.  Slot-gated
        loaders keep their fail-closed revoke: their worker and lease buffer
        cannot be settled synchronously inside a Kafka callback.
        """
        if not self._running or self._rebalance_failure or self._slot_gating:
            return
        if self._write_outage is not None and self._write_outage.active and self._batch and not self._batch_written:
            # Stream outage ride-out: never write inside a Kafka callback while
            # Neo4j is down. Hand the held batch to its next owner instead.
            self._drop_unwritten_for_revoke(released)
            return
        records = len(self._batch)
        self._in_rebalance_callback = True
        try:
            if self._batch and not self._flush_batch():
                if (
                    self._write_outage is not None and self._batch and not self._batch_written
                    and self._last_write_failure is not None
                    and self._write_outage.rides_out(self._last_write_failure)
                ):
                    # The settlement write itself hit a Neo4j outage.
                    self._drop_unwritten_for_revoke(released)
                return
            if not self._commit_contiguous_resolutions():
                return
        finally:
            self._in_rebalance_callback = False
        retained = deque(
            message for message in self._deferred_messages
            if (message.topic(), message.partition()) not in released
        )
        dropped = len(self._deferred_messages) - len(retained)
        self._deferred_messages = retained
        for key in released:
            ledger = self._partition_ledgers.get(key)
            if ledger is not None and not ledger.resolved_offsets:
                # The next owner (possibly this replica again) resumes from
                # the committed offset; a stale ledger would open a false gap.
                del self._partition_ledgers[key]
        self._logger.info(
            "stage=rebalance edge=%s replica=%s run_id=%s settled before revoke "
            "records=%s dropped_prefetched=%s partitions=%s",
            self._edge_config.type, self._replica_id, self._run_id, records, dropped,
            sorted(released),
        )

    def _drop_unwritten_for_revoke(self, released: set[tuple[str, int]]) -> None:
        """Hand the unwritten batch to the partitions' next owner (stream outage only).

        Nothing of it was committed: its offsets never resolved, so the next
        owner, maybe this replica again, re-reads it from the committed offset
        in the same order. That is the redelivery a crash would cause. Cells of
        it that did commit before the outage are rewritten, and ``MERGE`` is
        idempotent. Ledgers of released partitions are forgotten too, including
        resolved but uncommitted malformed records. The next owner routes those
        again (a duplicate DLQ copy, as after a crash).
        """
        dropped = [item for item in self._batch if (item.topic, item.partition) in released]
        self._batch = [item for item in self._batch if (item.topic, item.partition) not in released]
        if not self._batch:
            self._batch_started_at = None
        retained = deque(
            message for message in self._deferred_messages
            if (message.topic(), message.partition()) not in released
        )
        prefetched = len(self._deferred_messages) - len(retained)
        self._deferred_messages = retained
        for key in released:
            self._partition_ledgers.pop(key, None)
        self._logger.warning(
            "stage=rebalance edge=%s replica=%s run_id=%s reason=neo4j_outage revoked during a Neo4j outage: "
            "handed %s unwritten record(s) and %s prefetched message(s) to the next owner uncommitted partitions=%s",
            self._edge_config.type, self._replica_id, self._run_id, len(dropped), prefetched, sorted(released),
        )
        if not self._batch:
            self._write_outage.abandon("partitions revoked")

    def _on_revoke(self, consumer: Consumer, partitions: list) -> None:
        """Settle buffered work, then fail closed if any revoked work remains."""
        revoked = {(partition.topic or self._topics[0], partition.partition) for partition in partitions}
        # ``unassign()`` below releases the whole assignment, so settle all of it.
        self._settle_before_revoke(revoked | self._assigned_partitions)
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
            elif not self._running and self._shutdown_requested.is_set():
                # consumer.close() after a requested stop: run() has returned,
                # so the settlement above committed nothing and the held work
                # stays uncommitted for redelivery. Expected on a stop path.
                self._logger.warning(
                    "Kafka partitions revoked with unresolved relationship work on a requested stop edge=%s "
                    "partitions=%s; nothing committed, the work is redelivered",
                    self._edge_config.type,
                    sorted(revoked & pending),
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
        self._write_paused = False
        self._admission_barrier = None
        self._barrier_paused = False
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
        if self._in_rebalance_callback:
            # Ownership cannot change until the running callback returns.
            return not self._rebalance_failure
        if not self._assignment_observed:
            return True
        message = self._consumer.poll(0)
        if message is not None:
            self._deferred_messages.append(message)
            self._refresh_slot_backpressure()
        return not self._rebalance_failure

    def _owns(self, key: tuple[str, int]) -> bool:
        return not self._assignment_observed or key in self._assigned_partitions

    def _pause_assignments(self) -> None:
        self._write_paused = True
        self._sync_assignment_pause()

    def _resume_assignments(self) -> None:
        self._write_paused = False
        self._sync_assignment_pause()

    def _sync_assignment_pause(self) -> None:
        if not self._assignment_observed or not self._assigned_partitions:
            return
        should_pause = self._write_paused or self._buffer_paused or self._barrier_paused
        if should_pause == self._paused:
            return
        partitions = [TopicPartition(topic, partition) for topic, partition in sorted(self._assigned_partitions)]
        if should_pause:
            self._consumer.pause(partitions)
        else:
            self._consumer.resume(partitions)
        self._paused = should_pause

    def _admission_ready(self) -> bool:
        """Whether the current assignment's records may be processed (v7-S4).

        Non-blocking: one snapshot per assignment (retried if the broker does
        not answer), then a local ``caught_up`` check per call."""
        barrier = self._admission_barrier
        if barrier is None:
            return True
        index = self._admission_index
        try:
            if barrier["snapshot"] is None:
                barrier["snapshot"] = index.watermark_snapshot()
            ready = index.caught_up(barrier["snapshot"])
        except Exception as exc:
            ready, error = False, exc
        else:
            error = None
        waited = self._clock() - barrier["since"]
        if ready:
            self._admission_barrier = None
            self._barrier_paused = False
            self._sync_assignment_pause()
            self._emit_admission_wait(0.0)
            self._logger.info(
                "stage=assign edge=%s replica=%s run_id=%s parked-pair index caught up after %.1fs; "
                "admitting partitions", self._edge_config.type, self._replica_id, self._run_id, waited,
            )
            return True
        self._emit_admission_wait(waited)
        if waited >= self._admission_warn_after_seconds and (
            barrier["warned_at"] is None or self._clock() - barrier["warned_at"] >= 30.0
        ):
            barrier["warned_at"] = self._clock()
            self._logger.warning(
                "stage=assign edge=%s replica=%s run_id=%s reason=parked_pair_index_lagging waited_s=%.0f "
                "error=%s -- holding the assigned partitions until the index has caught up",
                self._edge_config.type, self._replica_id, self._run_id, waited, error,
            )
        return False

    def _emit_admission_wait(self, seconds: float) -> None:
        metrics_module.safe_emit(lambda: self._metrics.parked_pair_index_wait.labels(
            kind="edge", name=self._edge_config.type,
        ).set(seconds))

    def _commit_contiguous_resolutions(self) -> bool:
        """Synchronously commit all owned durable prefixes in one request."""
        offsets: list[TopicPartition] = []
        advances: list[tuple[PartitionLedger, int]] = []
        if not self._poll_for_callbacks():
            self._rebalance_failure = True
            return False
        for (topic, partition), ledger in sorted(self._partition_ledgers.items()):
            next_offset = ledger.next_offset
            while next_offset in ledger.resolved_offsets:
                next_offset += 1
            if next_offset == ledger.next_offset:
                continue
            if not self._owns((topic, partition)):
                self._rebalance_failure = True
                return False
            offsets.append(TopicPartition(topic, partition, next_offset))
            advances.append((ledger, next_offset))
        if not offsets:
            return True
        try:
            committed = self._consumer.commit(offsets=offsets, asynchronous=False)
        except Exception as exc:
            self._logger.error(
                "Relationship offset commit failed edge=%s offsets=%s reason=%s",
                self._edge_config.type,
                [(item.topic, item.partition, item.offset) for item in offsets], exc,
            )
            return False
        # The real synchronous Consumer API returns one TopicPartition per
        # requested offset.  Mocks used by older unit tests return an opaque
        # sentinel, so only inspect per-partition results when a list is given.
        if committed is None:
            self._logger.error(
                "Relationship offset commit returned no result edge=%s",
                self._edge_config.type,
            )
            return False
        if isinstance(committed, list):
            expected = {(item.topic, item.partition): item.offset for item in offsets}
            returned = {(item.topic, item.partition): item for item in committed}
            if (
                len(committed) != len(offsets)
                or set(returned) != set(expected)
                or any(item.error is not None or item.offset != expected[key]
                       for key, item in returned.items())
            ):
                self._logger.error(
                    "Relationship offset commit returned incomplete results edge=%s offsets=%s",
                    self._edge_config.type,
                    [(item.topic, item.partition, item.offset) for item in offsets],
                )
                return False
        for ledger, next_offset in advances:
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
        # v7-S7: KeyError (a loader bug, never a malformed record) if a multi-pair
        # type is handed a topic it does not read.
        record_config = self._config_for_topic[topic] if self._config_for_topic else self._edge_config
        # Only decode/JSON-parse/normalize errors are "malformed" (DLQ-able,
        # terminal). Deliberately narrow: slot-admission/lease machinery
        # below (endpoint_buckets, _slot_buffer.add, ...) can also raise
        # ValueError-family exceptions for a *valid* record hitting a loader
        # bug or a lease-protocol violation -- those must propagate and fail
        # the loader, never be silently DLQ'd and committed as if the record
        # itself were bad data.
        try:
            payload = message.value()
            if payload is None:
                raise ValueError("message payload is empty")
            decoded = json.loads(payload.decode("utf-8"), parse_constant=_reject_json_constant)
            record = normalize_edge_record(decoded, record_config)
        except (UnicodeDecodeError, json.JSONDecodeError, EdgeRecordValidationError, ValueError) as exc:
            reason = str(exc)
            try:
                self._rejection_sink.route(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    key=message.key(),
                    value=payload,
                    reason=reason,
                    reason_code=_reason_code(exc),
                    edge_type=self._edge_config.type,
                )
            except Exception as sink_error:
                self._log_failure(message, f"rejection sink {type(sink_error).__name__}: {sink_error}")
                return False
            self._logger.warning(
                "Malformed relationship record durably rejected edge=%s topic=%s partition=%s "
                "offset=%s reason=%s phase5_action=%s",
                self._edge_config.type, topic, partition, offset, reason,
                # Only a configured DLQ sink routes to a DLQ; the legacy file
                # sink just records the rejection locally.
                "route_to_dlq" if isinstance(self._rejection_sink, DeadLetterRecordSink) else "reject_to_file",
            )
            ledger.resolved_offsets.add(offset)
            return self._commit_contiguous_resolutions()

        pending_record = PendingEdgeRecord(record, topic, partition, offset)
        if self._slot_gating:
            assert self._slot_buffer is not None
            buckets = endpoint_buckets(record, self._loading_config.coordination.bucket_count)
            try:
                self._slot_buffer.add(pending_record, buckets)
            except LeaseStateError as exc:
                raise self._slot_failure("slot buffer add failed", exc) from exc
            self._release_slot_buffer()
            self._refresh_slot_backpressure()
            return True
        if not self._batch:
            self._batch_started_at = self._clock()
        self._batch.append(pending_record)
        return True

    def _flush_detail(self) -> str:
        """Return extra ``key=value`` timing fields from a lease-mode flush."""
        describe = getattr(self._coordinator, "describe_last_flush", None)
        return f" {describe()}" if callable(describe) and getattr(self._coordinator, "routes_records", False) is True else ""

    def _execute_coordinated(self, records: Sequence[PendingEdgeRecord]):
        """Write one flush through the lane or bucket-lease coordinator."""
        if getattr(self._coordinator, "routes_records", False) is True:
            # Bucket-lease mode routes records into cells itself.
            return self._coordinator.execute_pending(tuple(records))
        for pending in records:
            self._lane_batcher.add(self._partitioner.route(
                pending.record, topic=pending.topic, partition=pending.partition, offset=pending.offset
            ))
        return self._coordinator.execute(self._lane_batcher.drain_all())

    def _flush_batch(self) -> bool:
        """Durably write the current batch, then resolve/commit its offsets.

        Bulk, planned and every loader built without a ``write_outage`` run
        :meth:`_flush_batch_once` and nothing else (fail fast). A stream loader
        rides out connectivity-class Neo4j failures (:meth:`_flush_batch_through_outage`),
        except inside a Kafka rebalance callback, where it never waits.
        """
        if self._write_outage is not None and not self._in_rebalance_callback:
            return self._flush_batch_through_outage()
        return self._flush_batch_once()

    def _flush_batch_through_outage(self) -> bool:
        """Stream mode: retry the same batch, in order, while Neo4j is unavailable.

        Each failed attempt has already rolled back and given up its leases
        (see :mod:`src.loader.write_outage`), and nothing was committed. Row,
        unknown, capacity and server-class failures, commit failures and
        rebalance failures return False exactly as :meth:`_flush_batch_once` does.
        """
        outage = self._write_outage
        assert outage is not None
        while True:
            self._last_write_failure = None
            if self._flush_batch_once():
                outage.recovered()
                if self._outage_paused:
                    self._outage_paused = False
                    if not self._shutdown_requested.is_set():
                        self._resume_assignments()
                return True
            failure = self._last_write_failure
            if failure is None or self._rebalance_failure:
                return False
            verdict = outage.on_failure(failure)
            if verdict.action == OUTAGE_FAIL:
                return False
            if verdict.action == OUTAGE_FATAL:
                outage.log_fatal(failure)
                return False
            if self._shutdown_requested.is_set():
                raise self._outage_drain_abandoned(failure)
            outage.log_retry(verdict, failure)
            if not self._outage_paused and not self._write_paused:
                # Pause only if nothing else (a finite drain) already did, so
                # recovery resumes exactly the pause this ride-out added.
                self._outage_paused = True
                self._pause_assignments()
            waited = outage.wait(
                verdict.delay_seconds, poll=self._poll_for_callbacks, stop=self._shutdown_requested,
                pending=lambda: bool(self._batch),
            )
            if waited == WAIT_STOPPED:
                raise self._outage_drain_abandoned(failure)
            if waited == WAIT_POLL_FAILED:
                return False
            if waited == WAIT_DROPPED:
                # A revoke handed the batch to its next owner, uncommitted.
                self._outage_paused = False
                return True

    def _outage_drain_abandoned(self, failure: Exception) -> WriteOutageDrainAbandoned:
        """A stop during a Neo4j outage: leave the batch uncommitted and exit 0."""
        self._logger.warning(
            "stage=edge-drain-abandoned edge=%s replica=%s run_id=%s records=%s reason=neo4j_outage "
            "stop requested while Neo4j writes were failing (%s: %s); batch left uncommitted for redelivery",
            self._edge_config.type, self._replica_id, self._run_id, len(self._batch),
            type(failure).__name__, failure,
        )
        return WriteOutageDrainAbandoned(f"stop requested during a Neo4j outage: {failure}")

    def _report_write_failure(self, exc: Exception, message: str, *args) -> None:
        """Log a failed flush write and keep it for the outage ride-out.

        Without a ride-out (bulk and every other caller) this is the ERROR it
        always was. A stream loader logs a failure it will ride out as a
        WARNING; :mod:`src.loader.write_outage` logs the sustained ERROR."""
        self._last_write_failure = exc
        if (
            self._write_outage is not None and not self._in_rebalance_callback
            and self._write_outage.rides_out(exc)
        ):
            self._logger.warning(message, *args)
        else:
            self._logger.error(message, *args)

    def _flush_batch_once(self) -> bool:
        """One attempt: durably write the current batch, then resolve/commit its offsets."""
        if self._slot_gating:
            if self._worker is not None:
                return self._enqueue_worker_batch()
            self._poll_coordination()
            self._revalidate_gated_batch()
        if not self._batch:
            return True
        written_batch = tuple(self._batch)
        written_gated_batch = tuple(self._gated_batch)
        # Bulk writes are synchronous and the poll owner cannot take another
        # work message until this batch is durable.  Keep broker prefetching
        # active; pausing every flush adds nearly a second before the next
        # batch in this workload.  Lease-gated writes retain their pause.
        if self._slot_gating:
            self._pause_assignments()
        if self._batch_written:
            # A revoke-time settlement resumes an already durable batch.
            return self._resolve_written_batch(written_batch, written_gated_batch)
        started_at = time.perf_counter()
        try:
            if self._coordinator is not None:
                results = self._execute_coordinated(self._batch)
                failed = next((result for result in results if not result.success), None)
                if failed is not None:
                    if isinstance(failed.exception, LeaseDrainAbandoned):
                        self._abandon_drain(len(written_batch), failed.exception)
                        raise failed.exception
                    self._report_write_failure(
                        failed.exception if failed.exception is not None else RuntimeError("lane execution failed"),
                        "Relationship write failed edge=%s topic=%s replica=%s "
                        "run_id=%s records=%s write_ms=%.3f reason=%s",
                        self._edge_config.type, self._topic, self._replica_id,
                        self._run_id, len(written_batch),
                        (time.perf_counter() - started_at) * 1000,
                        failed.exception or "lane execution failed",
                    )
                    return False
            else:
                self._writer.write_batch([pending.record for pending in self._batch])
        except LeaseDrainAbandoned:
            raise
        except Exception as exc:
            first = self._batch[0]
            self._report_write_failure(
                exc,
                "Relationship write failed edge=%s topic=%s partition=%s offset=%s "
                "records=%s write_ms=%.3f reason=%s",
                self._edge_config.type, first.topic, first.partition, first.offset,
                len(written_batch), (time.perf_counter() - started_at) * 1000, exc,
            )
            return False
        elapsed_seconds = time.perf_counter() - started_at
        self._logger.info(
            "stage=edge-write edge=%s topic=%s replica=%s run_id=%s "
            "records=%s write_ms=%.3f%s",
            self._edge_config.type, self._topic, self._replica_id,
            self._run_id, len(written_batch),
            elapsed_seconds * 1000, self._flush_detail(),
        )
        # This is the classic/bulk (non-slot-gated) synchronous write path
        # - _worker_loop above covers the slot-gated streaming path; both
        # converge on the same one-observe/inc-per-batch shape (P6-S2
        # Story 5). Routed through safe_emit: this runs after the write
        # already durably succeeded (outside the try/except above).
        metrics_module.safe_emit(lambda: self._metrics.write_duration.labels(
            kind="edge", name=self._edge_config.type, stage="edge-write"
        ).observe(elapsed_seconds))
        metrics_module.safe_emit(lambda: self._metrics.records_consumed.labels(
            kind="edge", name=self._edge_config.type, label_or_type=self._edge_config.type,
        ).inc(len(written_batch)))
        self._batch_written = True
        return self._resolve_written_batch(written_batch, written_gated_batch)

    def _resolve_written_batch(
        self,
        written_batch: tuple[PendingEdgeRecord, ...],
        written_gated_batch: tuple[GatedPendingRecord, ...],
    ) -> bool:
        """Resolve and commit one durably written synchronous batch."""
        if self._slot_gating:
            self._poll_coordination(release_owned=False)
            self._verify_written_gated_batch(written_gated_batch)
        if not self._poll_for_callbacks():
            return False
        if not self._batch_written:
            # A revoke during that poll already committed this batch while
            # its partitions were still owned.
            return True
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
        self._batch_written = False
        self._batch_started_at = None
        if self._slot_gating:
            self._release_slot_buffer()
        if self._slot_gating:
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
        else:
            # Stop fetching new work once the caller requests a finite drain.
            # One final pause is cheap; pausing every write batch is not.
            self._pause_assignments()
        if self._admission_barrier is not None and self._deferred_messages:
            # v7-S4: records deferred behind the admission barrier were never admitted: the
            # index view may lack the previous owner's entries. Drop them uncommitted; the
            # next owner re-reads them from the committed offset.
            self._logger.info(
                "stage=shutdown edge=%s replica=%s run_id=%s dropped %d record(s) held behind the parked-pair "
                "index admission barrier (uncommitted, re-read by the next owner)",
                self._edge_config.type, self._replica_id, self._run_id, len(self._deferred_messages),
            )
            self._deferred_messages.clear()
        while self._deferred_messages or self._batch:
            while self._deferred_messages and len(self._batch) < self._outer_batch_size:
                if not self._buffer_message(self._deferred_messages.popleft()):
                    return False
            if self._batch and not self._flush_batch():
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
            # A rebalance settled during the drain advances the epoch; the
            # monitor accepts a drain only for an epoch it has seen assigned.
            self._publish_pending_assignments()
            self._publish_control("DRAIN_COMPLETE", {})
        return True

    def run(self, *, max_messages: int | None = None) -> int:
        """Run until cap, shutdown, or an unsafe Kafka/Neo4j condition."""
        self._running = True
        try:
            return self._run(max_messages=max_messages)
        finally:
            self._running = False

    def _run(self, *, max_messages: int | None) -> int:
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
                if not self._admission_ready():
                    # v7-S4: assigned partitions are paused; keep polling for
                    # rebalances and defer anything already fetched.
                    message = self._consumer.poll(min(0.1, self._loading_config.flush_interval_ms / 1000))
                    if message is not None:
                        self._deferred_messages.append(message)
                    continue
                if self._slot_gating and self._buffer_paused:
                    # Continue polling for rebalances while Kafka work fetches
                    # are paused. A prefetched work message stays unresolved.
                    message = self._consumer.poll(min(1.0, self._loading_config.flush_interval_ms / 1000))
                    if message is not None:
                        self._deferred_messages.append(message)
                        self._refresh_slot_backpressure()
                    continue
                message = (
                    self._deferred_messages.popleft()
                    if self._deferred_messages
                    else self._consumer.poll(min(1.0, self._loading_config.flush_interval_ms / 1000))
                )
                self._refresh_slot_backpressure()
                self._poll_coordination()
                if not self._drain_worker_results():
                    return 1
                if message is None:
                    continue
                if not self._buffer_message(message):
                    return 1
                processed += 1
                if len(self._batch) >= self._outer_batch_size and not self._flush_batch():
                    return 1
            return 0 if self._finish_run() else 1
        except LeaseDrainAbandoned:
            # Stop drain budget spent waiting for a lease: nothing was written
            # without a lease and nothing was committed -- a clean exit.
            return 0
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


def apply_edge_outer_batch_override(
    loading_config: LoadingConfig, environ: Mapping[str, str] = os.environ,
) -> LoadingConfig:
    """Return ``loading_config`` with the orchestrator's edge outer-flush override.

    Bucket-lease mode splits each flush into up to ``B*B`` cells, so it needs
    a much larger outer flush than lane mode to keep cell transactions large.
    """
    raw = environ.get(EDGE_OUTER_BATCH_ENV, "").strip()
    if not raw:
        return loading_config
    if not raw.isdigit() or not 1 <= int(raw) <= MAX_EDGE_OUTER_BATCH_SIZE:
        raise ValueError(f"{EDGE_OUTER_BATCH_ENV} must be an integer from 1 through {MAX_EDGE_OUTER_BATCH_SIZE}")
    return loading_config.model_copy(update={"edge_unwind_batch_size": int(raw)})


def _select_edge_config(schema, edge_type: str) -> EdgeConfig:
    """Return the single configured edge type or raise a concise error."""
    matches = [edge for edge in schema.edges if edge.type == edge_type]
    if len(matches) != 1:
        raise ValueError(f"No configured edge type named '{edge_type}'")
    return matches[0]


def _resolve_cli_topics(edge_config: EdgeConfig, args) -> list[str]:
    """Topics this loader process reads: ``--topics``, else ``--topic``, else the schema's."""
    if args.topics is not None and args.topic is not None:
        raise ValueError("--topic and --topics are mutually exclusive")
    if args.topics is not None:
        names = [item.strip() for item in args.topics.split(",")]
        if not all(names):
            raise ValueError("--topics must be a comma-separated list of nonblank topic names")
        return names
    if args.topic is not None:
        return [args.topic]
    return list(edge_config.topics)


def build_parser() -> argparse.ArgumentParser:
    """Build the direct relationship-loader command parser."""
    parser = argparse.ArgumentParser(description="Run one schema-declared Kafka relationship loader")
    parser.add_argument("--config", default="config/graph_schema.yaml")
    parser.add_argument("--edge-type", required=True)
    parser.add_argument("--topic")
    parser.add_argument(
        "--topics",
        help="comma-separated topics (a relationship type may be fed by several); overrides the schema's list",
    )
    parser.add_argument("--mode", choices=["bulk", "stream"], default="stream")
    parser.add_argument("--replica-id", default="0")
    parser.add_argument(
        "--replica-count", type=int, default=None,
        help="bulk only: statically assign partitions p %% N == replica id instead of joining the group",
    )
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
    configure_write_timing_capture(logger, "edge-write")
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
    static_assignment = None
    if args.replica_count is not None:
        if args.mode != "bulk" or args.slot_gating:
            parser.error("--replica-count requires --mode bulk without --slot-gating")
        if args.replica_count < 1:
            parser.error("--replica-count must be positive")
        if not args.replica_id.isdigit() or not 0 <= int(args.replica_id) < args.replica_count:
            parser.error("--replica-id must be an integer in [0, --replica-count) for static assignment")
        static_assignment = (int(args.replica_id), args.replica_count)
    run_id = args.run_id if args.run_id is not None else "default_run"
    shutdown_requested = Event()
    signal.signal(
        signal.SIGTERM, lambda _signum, _frame: shutdown_requested.set()
    )
    consumer = None
    coordination_consumer = None
    producer = None
    dlq_producer = None
    driver = None
    coordinator = None
    edge_config = None
    parked_pair_index = None
    exit_code = 1
    finalization_error = False
    process_metrics = metrics_module.process_metrics()
    metrics_module.install_dlq_routed_listener()
    metrics_server = metrics_module.start_metrics_server(process_metrics, kind="edge", name=args.edge_type)
    try:
        schema = load_schema(args.config)
        edge_config = _select_edge_config(schema, args.edge_type)
        # v7-S2: `topic_conflict: precedence` needs bulk sub-stages; refuse it in stream.
        require_no_precedence_in_stream(schema, args.mode)
        metrics_module.initialize_zero_series(process_metrics, kind="edge", name=edge_config.type)
        rotation_plan = None
        if args.slot_gating and edge_config.is_multi_pair:
            raise ValueError(
                f"edge type {edge_config.type} binds several label pairs; --slot-gating (the deprecated "
                "--stream-clock-rotation path) supports one pair per type; use the default lease fleet"
            )
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
            "KAFKA_GROUP_ID", stream_edge_group(schema.loading.consumer_group_id, edge_config.type)
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
            "heartbeat.interval.ms": schema.loading.heartbeat_interval_ms,
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
        loading_config = apply_edge_outer_batch_override(schema.loading)
        driver = get_neo4j_driver(*get_neo4j_credentials())
        # P5-S1 Story 7: a streaming lease client must ride out a
        # supervised lease-server restart (unbounded reconnect, capped
        # backoff, interruptible by this container's own shutdown event) --
        # bulk keeps build_edge_execution's bounded default. Harmless to
        # compute when lease mode isn't actually active at all:
        # streaming_lease_client_factory returns None in that case, and
        # build_edge_execution's classic branch never looks at it.
        lease_client_factory = (
            streaming_lease_client_factory(os.environ, schema.loading.bucket_lease, shutdown_requested)
            if args.mode == "stream" else None
        )
        # P5-S3 Story 5: the malformed-record sink is built *before*
        # build_edge_execution so its DeadLetterRecordSink (when dead_letter
        # resolves a real DLQ topic, not the RejectionSink file fallback)
        # can be reused as the row-level missing-endpoint sink too -- both
        # failure classes publish to the identical per-edge-type DLQ topic,
        # already provisioned once by the CLI's _ensure_dead_letter_topics,
        # with failure_class as the discriminating field. No new topic, no
        # new producer, no new flush/close call: the existing `finally`
        # block below still flushes/closes exactly one dlq_producer either way.
        malformed_sink, dlq_producer = build_malformed_record_sink(
            dead_letter=edge_config.dead_letter,
            entity_topic=edge_config.dead_letter_entity,
            entity_kind="edge",
            rejection_log_path=os.environ.get(
                "REJECTION_LOG_PATH", schema.loading.rejection_log_path
            ),
            bootstrap_servers=bootstrap_servers,
            run_id=run_id,
            replica_id=args.replica_id,
            max_poll_interval_ms=schema.loading.max_poll_interval_ms,
        )
        missing_endpoint_sink = malformed_sink if isinstance(malformed_sink, DeadLetterRecordSink) else None
        # P5-S4 Story 3 / review finding M16: every live replica of a
        # streaming, DLQ-enabled, non-order_by edge type must run its own
        # writable ParkedPairIndex instance -- not just the retry consumer.
        # Without one here, IndexedMissingEndpointSink is never active for
        # the live write path, so no DLQ'd row is ever recorded, and the
        # retry consumer's own contains()/is_blocked_by_earlier() checks
        # see nothing to retry or block on. Every instance (this replica,
        # every sibling replica, and the retry consumer's own instance)
        # publishes add/remove events to the same compacted topic and
        # rebuilds its local view from it -- the "cross-process" index the
        # module docstring describes, not a single shared owner.
        if (
            args.mode == "stream"
            and isinstance(missing_endpoint_sink, DeadLetterRecordSink)
            and edge_config.order_by is None
        ):
            index_topic = edge_parked_pair_index_topic_name(edge_config)
            if index_topic is not None:
                # v7-S4: one index per type; a multi-topic type stamps a causal
                # per-pair sequence so retries keep arrival order across topics.
                parked_pair_index = ParkedPairIndex(
                    topic=index_topic, bootstrap_servers=bootstrap_servers, writable=True,
                    sequenced=edge_config.is_multi_topic,
                    # v7-S7: label-pair-qualified in memory (wire keys unchanged; values stamp the pair, S2).
                    pair_of_topic=edge_config.pair_labels_for_topic if edge_config.is_multi_pair else None,
                )
                parked_pair_index.start()
                warn_index_leftovers(parked_pair_index, edge_config, stage="edge-loader")
        # Stream outage resilience: only a --mode stream loader of the default
        # lease fleet (a bucket-lease address set, no --slot-gating) rides out a
        # Neo4j outage. Its writes are fenced (transaction timeout T, leases
        # quarantined on an ambiguous failure), so a retry can never wait on an
        # orphaned transaction's locks. Every other writer gets None and fails
        # fast: bulk (classic, --edge-leases, static replicas), the deprecated
        # slot-gated clock path and its unleased isolated self-reference phase.
        write_outage = (
            StreamWriteOutage(
                kind="edge", name=edge_config.type, metrics=process_metrics, event_logger=logger,
                context=f"replica={args.replica_id} run_id={run_id}",
            )
            if args.mode == "stream" and not args.slot_gating and lease_mode_settings(os.environ) is not None
            else None
        )
        # A retried batch re-routes rows an earlier attempt already dead-lettered. For
        # those, both the index re-add and the publish are skipped (HeldBatchDlqMemory):
        # a re-add without a copy orphans the entry, and a re-add with a copy resurrects
        # a row the retry consumer may already have resolved (an older value applied
        # after a newer one; seen in a Docker proof). The writer's index (and
        # every scoped per-pair view) is wrapped; the admission barrier keeps the plain index.
        write_index = parked_pair_index
        if write_outage is not None and missing_endpoint_sink is not None:
            dlq_memory = write_outage.remember_dlq_acks()
            missing_endpoint_sink = dlq_memory.wrap_sink(missing_endpoint_sink)
            if parked_pair_index is not None:
                write_index = dlq_memory.wrap_index(parked_pair_index)
        writer, partitioner, lane_batcher, coordinator = build_edge_execution(
            edge_config, driver, metrics=process_metrics,
            # P5-S1 Story 4 / P5-Q1 fencing amendment: only consumed by
            # build_edge_execution's lease branch; classic mode ignores it.
            bucket_lease_tx_timeout_seconds=schema.loading.bucket_lease.tx_timeout_ms / 1000,
            lease_client_factory=lease_client_factory,
            # P5-S3 Stories 4-5: mode is the only signal build_edge_execution
            # needs, together with dead_letter/route_missing_endpoints_in_bulk,
            # to decide whether row-level DLQ activates for this writer.
            mode=args.mode, missing_endpoint_sink=missing_endpoint_sink,
            # P5-S4 Story 3: None on every path except the lease/stream/
            # non-order_by combination above -- build_edge_execution itself
            # also enforces order_by/bulk exclusion (belt and suspenders).
            parked_pair_index=write_index, stop_event=shutdown_requested,
        )
        loader = EdgeLoader(
            consumer=consumer,
            writer=writer,
            edge_config=edge_config,
            topics=_resolve_cli_topics(edge_config, args),
            admission_index=parked_pair_index,
            loading_config=loading_config,
            shutdown_requested=shutdown_requested,
            rejection_sink=malformed_sink,
            partitioner=partitioner,
            lane_batcher=lane_batcher,
            coordinator=coordinator,
            producer=producer,
            run_id=run_id,
            replica_id=args.replica_id,
            static_assignment=static_assignment,
            metrics=process_metrics,
            write_outage=write_outage,
            **slot_kwargs,
        )
        exit_code = loader.run(max_messages=args.max_messages)
    except Exception as exc:
        # P5-S1 Story 7, review finding S7-2: a SIGTERM arriving while
        # still connecting to the lease server (e.g. inside
        # build_edge_execution's coordinator.prime()) surfaces as
        # LeaseCoordinatorError("... shutdown requested") from the
        # streaming client's stop_event -- a clean, requested shutdown,
        # not a crash, so it must not be reported as one.
        if shutdown_requested.is_set() and "shutdown requested" in str(exc):
            logger.info("stage=edge-loader-shutdown reason=SIGTERM during startup: %s", exc)
            exit_code = 0
        else:
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
            if dlq_producer is not None:
                try:
                    dlq_producer.flush()
                except Exception as exc:
                    finalization_error = True
                    logger.error(
                        "stage=shutdown edge=%s replica=%s run_id=%s component=dlq-producer reason=flush failed: %s",
                        args.edge_type, args.replica_id, run_id, exc,
                    )
            if coordinator is not None and hasattr(coordinator, "close"):
                try:
                    coordinator.close()
                except Exception as exc:
                    logger.warning(
                        "stage=shutdown edge=%s replica=%s run_id=%s component=lease-clients reason=close failed: %s",
                        args.edge_type, args.replica_id, run_id, exc,
                    )
            if parked_pair_index is not None:
                try:
                    parked_pair_index.stop()
                except Exception as exc:
                    logger.warning(
                        "stage=shutdown edge=%s replica=%s run_id=%s component=parked-pair-index reason=close failed: %s",
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
            metrics_module.write_metrics_snapshot(
                process_metrics, os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH")
            )
            metrics_module.stop_metrics_server(metrics_server)
            # F2: last statement of the shutdown path: CPython resets every Python-level handler to
            # SIG_DFL at interpreter exit (only SIG_IGN survives), so a repeated SIGTERM from a second
            # container stop would otherwise exit 143 during teardown.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
    return 1 if finalization_error else exit_code


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
