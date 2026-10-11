"""Schema-driven record validation, Neo4j writing, and direct Kafka loading."""
from __future__ import annotations

import os
import signal
from threading import Event


def _exit_stopped_while_importing(_signum, _frame) -> None:
    """Early SIGTERM handler: a stop before main() runs has nothing to drain.

    Until main() replaces it, the process has opened no connection, owned no
    partition and read, written or committed nothing, so the stop's outcome
    is an immediate exit 0. Finishing the imports first would take seconds
    on a busy loader CPU and could outlast Docker's stop grace.
    """
    os.write(2, b"WARNING: stage=node-startup-stopped reason=stop requested while the loader was still "
                b"importing; nothing was read, written or committed\n")
    os._exit(0)


# F1: as the container entrypoint (``python -m src.loader.node_loader``) the
# SIGTERM handler is installed here, before the heavy imports below (Kafka,
# Neo4j, the CLI module: about 0.5 s of CPU, several seconds when a dozen
# loaders start on one CPU). Without it a stop in that window killed the
# process with the default action (exit 143). Importing this module
# elsewhere (tests, the CLI) installs nothing.
if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _exit_stopped_while_importing)

# The rest of the imports follow the early handler on purpose (E402 is not enforced).
import argparse
from collections import deque
from dataclasses import dataclass
import json
import logging
import time
from typing import Callable, Deque, Mapping, Sequence

from confluent_kafka import Consumer, Message, Producer, TopicPartition
from neo4j import Driver
from neo4j.exceptions import Neo4jError

from src.cli import get_neo4j_credentials
from src.loader.control import CONTROL_TOPIC
from src.loader.fenced_tx import begin_fenced_transaction, check_deadline
from src.loader.lease_client import LeaseCoordinatorError
from src.loader.dead_letter import (  # noqa: F401 - RejectionSink re-exported for compatibility
    DeadLetterRecordSink,
    MalformedRecordSink,
    RejectionSink,
    _reason_code,
    build_malformed_record_sink,
)
from src.models.schema import LoadingConfig, NodeConfig, PropertyConfig
from src.loader.record_validation import normalize_property_value
from src.loader.write_timing_capture import configure_write_timing_capture
from src.orchestrator.schema_initializer import get_neo4j_driver
from src.utils.schema_loader import load_schema
from src.utils.neo4j_session import open_session
from src.utils import metrics as metrics_module


logger = logging.getLogger(__name__)

class ControlDeliveryError(RuntimeError):
    """Raised when a control acknowledgement was not durably delivered."""


@dataclass(frozen=True)
class NodeRecord:
    """A validated Neo4j-ready record for one schema-declared node."""

    key: object
    properties: dict[str, object]

    # A node is its own lease "cell": exposing the key as both endpoints lets
    # the relationship-side ``BucketCellRouter`` map it to the very same
    # ``Label:bucket`` / ``Label#digest`` resource the edges touching it use.
    @property
    def source_key(self) -> object:
        return self.key

    @property
    def target_key(self) -> object:
        return self.key


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

    Lease-fenced writes (stream mode, P5-Q1): when a lease coordinator calls
    :meth:`write_batch` it passes the lease ``deadline`` (grant time + T) and
    a ``lease_liveness_check``.  The transaction is begun fenced
    (:func:`src.loader.fenced_tx.begin_fenced_transaction`): Neo4j itself
    ends it by the deadline, whatever this process notices, and it is rolled back rather
    than committed if the lease connection is found broken just before
    commit.  Without them (bulk) the write is unchanged.
    """

    def __init__(self, driver: Driver, node_config: NodeConfig,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._driver = driver
        self._label = node_config.label
        self._query = build_node_upsert_query(node_config)
        self._clock = clock

    def write(self, record: NodeRecord) -> None:
        """Execute one idempotent singleton-batch node upsert."""
        self.write_batch([record])

    def write_batch(
        self, records: Sequence[NodeRecord], *, deadline: float | None = None,
        lease_liveness_check: Callable[[], bool] | None = None,
    ) -> None:
        """Write one materialized UNWIND batch in a single transaction."""
        batch = [{"key": record.key, "properties": record.properties} for record in records]
        what = f"node={self._label}"
        if deadline is not None:
            # Refuse before opening a session; the fenced BEGIN checks again.
            check_deadline(deadline, self._clock(), what)
        with open_session(self._driver) as session:
            with begin_fenced_transaction(
                session, deadline=deadline, tx_timeout_seconds=None, clock=self._clock, what=what,
            ) as transaction:
                transaction.run(self._query, batch=batch).consume()
                if lease_liveness_check is not None and not lease_liveness_check():
                    # Leaving the block without commit rolls the transaction back.
                    raise LeaseCoordinatorError(
                        f"node={self._label} lease connection broke before commit; "
                        "aborting instead of committing"
                    )
                transaction.commit()


@dataclass(frozen=True)
class _CellItem:
    """Adapter giving a node record the ``.record`` shape the lease coordinator routes."""

    record: NodeRecord


class LeasedNodeWriter:
    """Stream-mode node writer: node-disjoint from relationship writers.

    ``write_batch`` splits a flush into per-resource cells (the same
    ``Label:bucket`` / keyed-label resources that relationship cells use),
    and the shared :class:`~src.loader.edge_execution.BucketLeaseExecutionCoordinator`
    leases each cell (``ANY`` over pending cells, largest first), writes it
    in one fenced transaction through the wrapped :class:`NodeWriter`, and
    releases.  It returns only when every cell is durable; any cell failure
    (including lease loss) raises, so the loader commits no offset.  Cells
    already written are idempotent upserts, so a retry of the whole flush is
    safe.
    """

    def __init__(self, coordinator) -> None:
        self._coordinator = coordinator

    def write(self, record: NodeRecord) -> None:
        self.write_batch([record])

    def write_batch(self, records: Sequence[NodeRecord]) -> None:
        if not records:
            return
        results = self._coordinator.execute_pending([_CellItem(record) for record in records])
        for result in results:
            if not result.success:
                raise result.exception if result.exception is not None else RuntimeError(
                    "node lease cell was not completely written"
                )

    def close(self) -> None:
        self._coordinator.close()


# Concurrent cell transactions one node loader may run per flush.
NODE_LEASE_WORKERS = 4


def build_leased_node_writer(
    node_config: NodeConfig, driver: Driver, loading_config: LoadingConfig, *,
    environ=os.environ, shutdown_requested: Event | None = None, executor_factory=None,
    lease_client_factory=None, metrics: "metrics_module.GraphLoaderMetrics | None" = None,
) -> LeasedNodeWriter | None:
    """Return a lease-fenced node writer, or ``None`` when lease mode is off.

    Uses the same lease settings (address, bucket count, keyed labels) and
    the same streaming client factory (bounded reconnect backoff, stop
    event) as the edge loaders, so node and relationship resources coincide.
    """
    from src.loader.edge_execution import (
        BucketLeaseExecutionCoordinator, lease_mode_settings, streaming_lease_client_factory,
    )
    from src.loader.lease_client import BucketCellRouter

    leases = lease_mode_settings(environ)
    if leases is None:
        return None
    _address, bucket_count, keyed_labels = leases
    factory = lease_client_factory or streaming_lease_client_factory(
        environ, loading_config.bucket_lease, shutdown_requested or Event(),
    )
    extra = {} if executor_factory is None else {"executor_factory": executor_factory}
    coordinator = BucketLeaseExecutionCoordinator(
        NodeWriter(driver, node_config),
        router=BucketCellRouter(node_config.label, node_config.label, bucket_count, keyed_labels),
        worker_count=NODE_LEASE_WORKERS,
        batch_size=max(1, loading_config.unwind_batch_size),
        client_factory=factory, edge_type=node_config.label, kind="node",
        merge_target_rows=0,  # a node cell shares no resource with another cell
        metrics=metrics, tx_timeout_seconds=loading_config.bucket_lease.tx_timeout_ms / 1000,
        stop_event=shutdown_requested,
        **extra,
    )
    # Pre-fill the whole pool: a graceful stop must be able to drain through
    # every worker without opening (stop-event-blocked) new connections.
    coordinator.prime(NODE_LEASE_WORKERS)
    logger.info(
        "stage=node-lease-mode label=%s address=%s buckets=%s keyed_labels=%s workers=%s",
        node_config.label, _address, bucket_count, ",".join(sorted(keyed_labels)) or "-",
        NODE_LEASE_WORKERS,
    )
    return LeasedNodeWriter(coordinator)


def _is_drain_abandoned(exc: BaseException) -> bool:
    from src.loader.edge_execution import LeaseDrainAbandoned

    return isinstance(exc, LeaseDrainAbandoned)


class NodeLoader:
    """Consume one configured node topic and synchronously write records."""

    def __init__(self, consumer: Consumer, writer: NodeWriter, node_config: NodeConfig,
                 topic: str | None = None, event_logger: logging.Logger = logger,
                 producer: Producer | None = None, run_id: str = "default_run",
                 replica_id: str = "0", shutdown_requested: Event | None = None,
                 loading_config: LoadingConfig | None = None,
                 clock: Callable[[], float] = time.monotonic,
                 sleeper: Callable[[float], None] = time.sleep,
                 rejection_sink: MalformedRecordSink | None = None,
                 metrics: "metrics_module.GraphLoaderMetrics | None" = None,
                 write_outage=None) -> None:
        self._consumer = consumer
        self._writer = writer
        # Stream outage ride-out (src/loader/write_outage.StreamWriteOutage).
        # Only node_loader.main installs it, and only for --mode stream. With
        # None (bulk and every other caller), _write_batch_with_retry is
        # exactly the bounded, fail-fast _write_batch_with_bounded_retry.
        self._write_outage = write_outage
        self._last_write_failure: Exception | None = None
        self._node_config = node_config
        self._logger = event_logger
        self._topic = topic or node_config.topic
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()

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
        # True once the current batch is durably written but not yet
        # committed, so a revoke-time settlement never rewrites it.
        self._batch_written = False
        # Set while Kafka runs a rebalance callback: nested polling is unsafe.
        self._in_rebalance_callback = False
        # Revoke-time settlement is only attempted while run() owns the loop;
        # a close() after run() returned must not write or commit anything.
        self._running = False
        # D2: once graceful stop begins, the loader only drains what is
        # already buffered/deferred; it never fetches new data again.
        self._draining = False

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

    def _settle_before_revoke(self, revoked: set[tuple[str, int]]) -> None:
        """Durably finish buffered work while Kafka still grants ownership.

        Under the eager protocol the revoke callback runs before this member
        rejoins, so the partitions are still owned and a synchronous commit is
        valid (Kafka's standard commit-on-revoke).  The buffered batch is
        written (unless already written) and committed; prefetched messages of
        revoked partitions are dropped uncommitted for their next owner to
        re-read.  Without this, any rebalance caused by a late sibling
        joining while this replica held a partial batch failed the loader.
        """
        if not self._running or self._rebalance_failure:
            return
        if self._write_outage is not None and self._write_outage.active and self._batch and not self._batch_written:
            # Stream outage ride-out: never write inside a Kafka callback while
            # Neo4j is down. Hand the revoked part of the batch to its next owner.
            self._drop_unwritten_for_revoke(revoked)
            return
        self._in_rebalance_callback = True
        try:
            if self._batch and not self._flush_batch():
                if (
                    self._write_outage is not None and self._batch and not self._batch_written
                    and self._last_write_failure is not None
                    and self._write_outage.rides_out(self._last_write_failure)
                ):
                    # The settlement write itself hit a Neo4j outage.
                    self._drop_unwritten_for_revoke(revoked)
                return
            if not self._commit_contiguous_resolutions():
                return
        finally:
            self._in_rebalance_callback = False
        retained = deque(
            message for message in self._deferred_messages
            if (message.topic(), message.partition()) not in revoked
        )
        dropped = len(self._deferred_messages) - len(retained)
        self._deferred_messages = retained
        for key in revoked:
            ledger = self._partition_ledgers.get(key)
            if ledger is not None and not ledger.resolved_offsets:
                # The next owner (possibly this replica again) resumes from
                # the committed offset; a stale ledger would open a false gap.
                del self._partition_ledgers[key]
        if dropped:
            self._logger.info(
                "Dropped %s uncommitted prefetched message(s) of revoked partitions label=%s",
                dropped, self._node_config.label,
            )

    def _drop_unwritten_for_revoke(self, revoked: set[tuple[str, int]]) -> None:
        """Hand the revoked part of the unwritten batch to its next owner (stream outage only).

        Nothing of it was committed, so the next owner re-reads it from the
        committed offset, in order: the redelivery a crash would cause. Node
        upserts are idempotent. Ledgers of revoked partitions are forgotten,
        including resolved but uncommitted malformed records, which the next
        owner routes again.
        """
        dropped = [item for item in self._batch if (item.topic, item.partition) in revoked]
        self._batch = [item for item in self._batch if (item.topic, item.partition) not in revoked]
        if not self._batch:
            self._batch_started_at = None
        retained = deque(
            message for message in self._deferred_messages
            if (message.topic(), message.partition()) not in revoked
        )
        prefetched = len(self._deferred_messages) - len(retained)
        self._deferred_messages = retained
        for key in revoked:
            self._partition_ledgers.pop(key, None)
        self._logger.warning(
            "stage=rebalance label=%s replica=%s run_id=%s reason=neo4j_outage revoked during a Neo4j outage: "
            "handed %s unwritten record(s) and %s prefetched message(s) to the next owner uncommitted partitions=%s",
            self._node_config.label, self._replica_id, self._run_id, len(dropped), prefetched, sorted(revoked),
        )
        if not self._batch:
            self._write_outage.abandon("partitions revoked")

    def _on_revoke(self, consumer: Consumer, partitions: list) -> None:
        """Settle buffered work, then remove revoked ownership."""
        revoked = {
            (partition.topic or self._topic, partition.partition)
            for partition in partitions
        }
        self._settle_before_revoke(revoked)
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
                self._rejection_sink.route(
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    key=message.key(),
                    value=payload,
                    reason=reason,
                    reason_code=_reason_code(exc),
                    node_label=self._node_config.label,
                )
            except Exception as sink_error:
                self._log_failure(
                    message,
                    f"rejection sink {type(sink_error).__name__}: {sink_error}",
                )
                return False
            self._logger.warning(
                "Malformed node record durably rejected label=%s topic=%s partition=%s "
                "offset=%s reason=%s phase5_action=%s",
                self._node_config.label,
                topic,
                partition,
                offset,
                reason,
                # N1: name the sink that actually took the record (as edge_loader does).
                "route_to_dlq" if isinstance(self._rejection_sink, DeadLetterRecordSink) else "reject_to_file",
            )
            ledger.resolved_offsets.add(offset)
            return self._commit_contiguous_resolutions()

    def _poll_for_callbacks(self) -> bool:
        """Dispatch Kafka callbacks and preserve any prefetched data message."""
        if self._in_rebalance_callback:
            # Ownership cannot change until the running callback returns.
            return not self._rebalance_failure
        if not self._assignment_observed:
            return True
        message = self._consumer.poll(0)
        if message is not None:
            if self._draining:
                # Defensive (e.g. a partition assigned after the drain pause):
                # never extend the drain. The record is uncommitted, so its
                # next owner re-reads it.
                self._logger.debug(
                    "Dropped uncommitted record polled while draining label=%s",
                    self._node_config.label,
                )
            else:
                self._deferred_messages.append(message)
        return not self._rebalance_failure

    def _begin_drain(self) -> None:
        """Stop fetching data: pause assignments but keep servicing callbacks."""
        if self._draining:
            return
        self._draining = True
        self._pause_assignments()

    def _owns(self, key: tuple[str, int]) -> bool:
        """Return whether a partition is safe to commit under known ownership."""
        return not self._assignment_observed or key in self._assigned_partitions

    def _pause_assignments(self) -> None:
        """Pause known assignments during retry backoff while polling callbacks."""
        if not self._assignment_observed or self._paused or not self._assigned_partitions:
            return
        partitions = [
            TopicPartition(topic, partition)
            for topic, partition in sorted(self._assigned_partitions)
        ]
        self._consumer.pause(partitions)
        self._paused = True

    def _resume_assignments(self) -> None:
        """Resume retry-paused assignments after all current work is durable."""
        if self._draining:
            return
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
        """Write the current atomic batch.

        Bulk and every loader built without a ``write_outage`` run the bounded,
        retryable-only :meth:`_write_batch_with_bounded_retry` and nothing else.
        A stream loader also rides out connectivity-class Neo4j failures
        (:meth:`_write_batch_through_outage`), except inside a Kafka rebalance
        callback, where it never waits.
        """
        if self._write_outage is not None and not self._in_rebalance_callback:
            return self._write_batch_through_outage()
        return self._write_batch_with_bounded_retry()

    def _write_batch_through_outage(self) -> bool:
        """Stream mode: rewrite the same batch, in order, while Neo4j is unavailable.

        Each attempt is the bounded retry below. When it fails with a
        connectivity-class error, the failed transaction has rolled back and
        the lease coordinator has given up its leases, and nothing was
        committed. The loader then waits a capped backoff with its partitions
        paused while it keeps polling, and tries again.
        """
        from src.loader.write_outage import FAIL, FATAL, WAIT_DROPPED, WAIT_POLL_FAILED, WAIT_STOPPED

        outage = self._write_outage
        while True:
            self._last_write_failure = None
            if self._write_batch_with_bounded_retry():
                outage.recovered()
                return True
            failure = self._last_write_failure
            if failure is None or self._rebalance_failure:
                return False
            if not self._batch or self._batch_written:
                # A revoke during the bounded backoff already wrote and committed it.
                return True
            verdict = outage.on_failure(failure)
            if verdict.action == FAIL:
                return False
            if verdict.action == FATAL:
                outage.log_fatal(failure)
                return False
            if self._shutdown_requested.is_set():
                raise self._outage_drain_abandoned(failure)
            outage.log_retry(verdict, failure)
            self._pause_assignments()
            waited = outage.wait(
                verdict.delay_seconds, poll=self._poll_for_callbacks, stop=self._shutdown_requested,
                pending=lambda: bool(self._batch) and not self._batch_written,
            )
            if waited == WAIT_STOPPED:
                raise self._outage_drain_abandoned(failure)
            if waited == WAIT_POLL_FAILED:
                return False
            if waited == WAIT_DROPPED:
                return True

    def _outage_drain_abandoned(self, failure: Exception):
        """A stop during a Neo4j outage: leave the batch uncommitted and exit 0."""
        from src.loader.write_outage import WriteOutageDrainAbandoned

        self._logger.warning(
            "stage=node-drain-abandoned label=%s topic=%s replica=%s run_id=%s records=%s reason=neo4j_outage "
            "stop requested while Neo4j writes were failing (%s: %s); batch left uncommitted for redelivery",
            self._node_config.label, self._topic, self._replica_id, self._run_id, len(self._batch),
            type(failure).__name__, failure,
        )
        return WriteOutageDrainAbandoned(f"stop requested during a Neo4j outage: {failure}")

    def _write_batch_with_bounded_retry(self) -> bool:
        """Write the current atomic batch with bounded retryable-only backoff."""
        records = [pending.record for pending in self._batch]
        attempts = self._loading_config.retry_max_attempts
        started_at = time.perf_counter()
        for attempt in range(attempts):
            write_failure: Exception | None = None
            try:
                self._writer.write_batch(records)
            except Exception as exc:
                if _is_drain_abandoned(exc):
                    self._logger.warning(
                        "stage=node-drain-abandoned label=%s topic=%s replica=%s run_id=%s records=%s "
                        "reason=no bucket lease within the stop drain budget (%s); batch left uncommitted "
                        "for redelivery (cells already written are replayed)",
                        self._node_config.label, self._topic, self._replica_id, self._run_id,
                        len(records), exc,
                    )
                    raise
                write_failure = exc

            if write_failure is None:
                # The write itself already succeeded by this point (P6-S2
                # review finding S4-1): log line + metrics emission happen
                # outside the `try:` that guards the write, so a
                # metrics-library bug here can never be mistaken for a
                # write failure and cause an already-durable batch to be
                # reported as failed (no offset commit).
                elapsed_seconds = time.perf_counter() - started_at
                self._logger.info(
                    "stage=node-write label=%s topic=%s replica=%s run_id=%s "
                    "records=%s attempts=%s write_ms=%.3f",
                    self._node_config.label, self._topic, self._replica_id,
                    self._run_id, len(records), attempt + 1,
                    elapsed_seconds * 1000,
                )
                # One observe/inc per batch (never per record) - P6-S2
                # Story 4. Routed through safe_emit (review finding S4-1)
                # so a metrics-library exception can never be mistaken for
                # a write failure this far after the write already
                # succeeded.
                metrics_module.safe_emit(lambda: self._metrics.write_duration.labels(
                    kind="node", name=self._node_config.label, stage="node-write"
                ).observe(elapsed_seconds))
                metrics_module.safe_emit(lambda: self._metrics.records_consumed.labels(
                    kind="node", name=self._node_config.label,
                    label_or_type=self._node_config.label,
                ).inc(len(records)))
                return True

            retryable = self._is_retryable_neo4j_error(write_failure)
            if not retryable or attempt + 1 >= attempts:
                first = self._batch[0]
                self._last_write_failure = write_failure
                # A stream loader logs a failure it rides out as a WARNING;
                # write_outage logs the sustained reason=neo4j_outage ERROR.
                log_failure = (
                    self._logger.warning
                    if self._write_outage is not None and not self._in_rebalance_callback
                    and self._write_outage.rides_out(write_failure)
                    else self._logger.error
                )
                log_failure(
                    "Node batch write failed label=%s topic=%s partition=%s offset=%s "
                    "attempt=%s retryable=%s write_ms=%.3f reason=%s",
                    self._node_config.label,
                    first.topic,
                    first.partition,
                    first.offset,
                    attempt + 1,
                    retryable,
                    (time.perf_counter() - started_at) * 1000,
                    write_failure,
                )
                return False
            # A retryable failure that will actually retry (this is not yet
            # the final, exhausted attempt) - P6-S2 Story 4. Counted once
            # per retry *attempt*, never per record. Routed through
            # safe_emit (review finding S4-1): a raise here must never mask
            # or replace the original write failure already handled above.
            metrics_module.safe_emit(lambda: self._metrics.retry.labels(
                kind="node", name=self._node_config.label, reason="neo4j-transient"
            ).inc())
            # Backoff polls for group callbacks. Pause only on this rare
            # retry path so those polls do not accumulate more data while
            # the current batch is still unresolved.
            self._pause_assignments()
            delay_ms = min(
                self._loading_config.retry_base_delay_ms * (2 ** attempt),
                self._loading_config.retry_max_delay_ms,
            )
            if not self._heartbeat_backoff(delay_ms / 1000):
                self._last_write_failure = write_failure
                return False
            if not self._batch or self._batch_written:
                # A revoke during backoff already wrote and committed it.
                return True
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
            if self._partition_ledgers.get(key) is not ledger or ledger.next_offset >= next_offset:
                # A revoke during that poll already committed this prefix
                # while the partition was still owned.
                continue
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
        # The poll owner writes synchronously and cannot consume another batch
        # before this one is durable. Leave broker prefetch active on the
        # ordinary path; pausing each flush delays the next fetch.
        if not self._batch_written:
            if not self._write_batch_with_retry():
                return False
            self._batch_written = True
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
        self._batch_written = False
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
        self._running = True
        try:
            return self._run(max_messages=max_messages)
        except Exception as exc:
            if not _is_drain_abandoned(exc):
                raise
            # A stopping loader that could not get a lease within the drain
            # budget wrote nothing for that batch and committed nothing.
            return 0
        finally:
            self._running = False

    def _run(self, *, max_messages: int | None) -> int:
        processed = 0
        try:
            while max_messages is None or processed < max_messages:
                self._publish_pending_assignments()
                if self._rebalance_failure:
                    return 1
                if self._shutdown_requested.is_set():
                    self._logger.info("Node loader gracefully stopping label=%s", self._node_config.label)
                    self._begin_drain()
                    # Buffer everything already prefetched (no new polls), in
                    # unwind-sized transactions, then flush the remainder.
                    while self._deferred_messages:
                        message = self._deferred_messages.popleft()
                        if not self._buffer_message(message):
                            return 1
                        processed += 1
                        if (len(self._batch) >= self._loading_config.unwind_batch_size
                                and not self._flush_batch()):
                            return 1
                    if not self._flush_batch():
                        return 1
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


class _StoppedDuringStartup(Exception):
    """A stream node loader was asked to stop before it connected to anything."""


def _is_requested_startup_stop(exc: BaseException, shutdown_requested: Event) -> bool:
    """Only the stop itself counts: the pre-connect check, or a lease connect
    abandoned because of it (``LeaseCoordinatorError("... shutdown requested")``)."""
    if not shutdown_requested.is_set():
        return False
    return isinstance(exc, _StoppedDuringStartup) or (
        isinstance(exc, LeaseCoordinatorError) and "shutdown requested" in str(exc)
    )


def _reject_json_constant(value: str) -> None:
    """Reject non-standard JSON numeric constants such as NaN and Infinity."""
    raise ValueError(f"unsupported JSON numeric constant {value}")


def _stream_write_outage(node_config: NodeConfig, process_metrics, args):
    """The stream node loader's Neo4j outage ride-out (never built for bulk)."""
    from src.loader.write_outage import StreamWriteOutage

    return StreamWriteOutage(
        kind="node", name=node_config.label, metrics=process_metrics, event_logger=logger,
        context=f"replica={args.replica_id} run_id={args.run_id}",
    )


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


def main(argv: list[str] | None = None, *, shutdown_requested: Event | None = None) -> int:
    """Run one selected node loader and close all resources on every path.

    ``shutdown_requested`` lets a caller (tests) supply the stop event; the
    SIGTERM handler installed below sets it.
    """
    # D1: install the SIGTERM handler before anything else so a stop that
    # arrives during startup (argument parsing, schema load, connections)
    # is honoured as a graceful drain instead of killing the process.
    if shutdown_requested is None:
        shutdown_requested = Event()
    previous_sigterm_handler = signal.signal(
        signal.SIGTERM,
        lambda _signum, _frame: shutdown_requested.set(),
    )
    try:
        args = build_parser().parse_args(argv)
        configure_write_timing_capture(logger, "node-write")
        if args.max_messages is not None and args.max_messages <= 0:
            raise SystemExit("--max-messages must be positive")
    except BaseException:
        # Argument/startup rejection exits immediately; nothing is draining.
        # Never hand back the entrypoint's early handler: it would turn this
        # rejection (exit 2) into an exit 0 if a SIGTERM landed now.
        signal.signal(
            signal.SIGTERM,
            signal.SIG_DFL if previous_sigterm_handler is _exit_stopped_while_importing
            else previous_sigterm_handler,
        )
        raise
    consumer = None
    producer = None
    dlq_producer = None
    driver = None
    node_writer = None
    finalization_error = False
    process_metrics = metrics_module.process_metrics()
    metrics_module.install_dlq_routed_listener()
    metrics_server = metrics_module.start_metrics_server(process_metrics, kind="node", name=args.node_label)
    loop_started = False
    try:
        if args.mode == "stream" and shutdown_requested.is_set():
            # F1: stopped before it connected: a stream loader owns nothing yet,
            # and its lease clients would refuse to connect anyway. (Bulk keeps
            # its unchanged path: connect, drain nothing, DRAIN_COMPLETE, 0.)
            raise _StoppedDuringStartup("stop requested before the loader connected")
        schema = load_schema(args.config)
        node_config = _select_node_config(schema, args.node_label)
        metrics_module.initialize_zero_series(process_metrics, kind="node", name=node_config.label)

        kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        consumer = Consumer({
            "bootstrap.servers": kafka_bootstrap_servers,
            "group.id": os.environ.get("KAFKA_GROUP_ID", f"{schema.loading.consumer_group_id}-{node_config.label}"),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
            "session.timeout.ms": schema.loading.session_timeout_ms,
            "heartbeat.interval.ms": schema.loading.heartbeat_interval_ms,
        })
        producer = Producer({
            "bootstrap.servers": kafka_bootstrap_servers,
        })
        driver = get_neo4j_driver(*get_neo4j_credentials())

        malformed_sink, dlq_producer = build_malformed_record_sink(
            dead_letter=node_config.dead_letter,
            entity_topic=node_config.topic,
            entity_kind="node",
            rejection_log_path=os.environ.get(
                "REJECTION_LOG_PATH", schema.loading.rejection_log_path
            ),
            bootstrap_servers=kafka_bootstrap_servers,
            run_id=args.run_id,
            replica_id=args.replica_id,
            max_poll_interval_ms=schema.loading.max_poll_interval_ms,
        )

        # Stream mode: node writes share the relationship writers' bucket
        # leases so no transaction ever waits on a lock. Bulk (nodes load
        # before edges) stays lease-free. None when no lease address is set.
        node_writer = (
            build_leased_node_writer(
                node_config, driver, schema.loading,
                shutdown_requested=shutdown_requested, metrics=process_metrics,
            ) if args.mode == "stream" else None
        ) or NodeWriter(driver, node_config)
        loader = NodeLoader(
            consumer=consumer,
            writer=node_writer,
            node_config=node_config,
            topic=args.topic,
            producer=producer,
            run_id=args.run_id,
            replica_id=args.replica_id,
            shutdown_requested=shutdown_requested,
            loading_config=schema.loading,
            rejection_sink=malformed_sink,
            metrics=process_metrics,
            # Stream outage resilience: only a --mode stream loader with a leased
            # (fenced) writer rides out a Neo4j outage. A plain NodeWriter has no
            # transaction timeout and no fencing, so a retry after an ambiguous
            # failure could wait on an orphaned transaction's locks: bulk, a
            # nodes-only stream run and the --stream-clock-rotation path keep
            # failing fast.
            write_outage=(
                _stream_write_outage(node_config, process_metrics, args)
                if args.mode == "stream" and isinstance(node_writer, LeasedNodeWriter) else None
            ),
        )
        loop_started = True
        exit_code = loader.run(max_messages=args.max_messages)
    except Exception as exc:
        if args.mode == "stream" and not loop_started and _is_requested_startup_stop(exc, shutdown_requested):
            # A stop during stream startup (the pre-connect check, or the lease
            # clients' connect, which refuses to start once a stop is requested)
            # is the requested outcome, not a failure: nothing was read, written
            # or committed. Any other startup error stays an ERROR and exit 1,
            # even when a stop arrived at the same time (same rule as
            # edge_loader.main). Bulk keeps its fail-closed startup errors.
            logger.warning(
                "stage=node-startup-stopped label=%s replica=%s run_id=%s reason=stop requested during startup "
                "(%s); nothing was read, written or committed",
                args.node_label, args.replica_id, args.run_id, exc,
            )
            exit_code = 0
        else:
            logger.error("Unable to start node loader: %s", exc)
            exit_code = 1
    finally:
        try:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    finalization_error = True
                    logger.exception("Unable to close Kafka consumer during loader shutdown")
            if producer is not None:
                try:
                    producer.flush()
                except Exception:
                    finalization_error = True
                    logger.exception("Unable to flush control producer during loader shutdown")
            if dlq_producer is not None:
                try:
                    dlq_producer.flush()
                except Exception:
                    finalization_error = True
                    logger.exception("Unable to flush DLQ producer during loader shutdown")
            close_writer = getattr(node_writer, "close", None)
            if callable(close_writer):
                try:
                    close_writer()
                except Exception:
                    finalization_error = True
                    logger.exception("Unable to close lease clients during loader shutdown")
            if driver is not None:
                try:
                    driver.close()
                except Exception:
                    finalization_error = True
                    logger.exception("Unable to close Neo4j driver during loader shutdown")
        finally:
            metrics_module.write_metrics_snapshot(
                process_metrics, os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH")
            )
            metrics_module.stop_metrics_server(metrics_server)
            # F2: last statement of the shutdown path: CPython resets every Python-level handler to
            # SIG_DFL at interpreter exit (only SIG_IGN survives), so a repeated SIGTERM from a second
            # container stop would otherwise exit 143 during teardown.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
    # P6-S5 Story 4: a close/flush failure during an otherwise-successful
    # shutdown must be reflected in the exit code, matching
    # edge_loader.py's existing `finalization_error` behavior for the
    # identical scenario - previously silently swallowed here.
    return 1 if finalization_error else exit_code


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
