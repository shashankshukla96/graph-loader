"""Run-scoped Kafka and container monitoring for finite bulk node loads."""
from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import json
import logging
import time
from typing import Callable, Mapping

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient
from docker.errors import NotFound

from src.loader.control import CONTROL_TOPIC
from src.models.schema import GraphSchema


logger = logging.getLogger(__name__)

Partition = tuple[str, int]
Replica = tuple[str, str]


class BulkMonitorError(RuntimeError):
    """Base class for an unsafe or invalid bulk monitoring state."""


class AssignmentCoverageError(BulkMonitorError):
    """Raised for invalid assignment acknowledgements."""


class BulkTimeoutError(BulkMonitorError):
    """Raised when a required bulk lifecycle stage times out."""


class BulkShutdownRequested(BulkTimeoutError):
    """Raised when the operator asked to stop (SIGTERM/SIGINT) during a wait.

    A ``BulkTimeoutError`` subclass so existing ``except BulkTimeoutError``
    fail-closed call sites keep working; distinct so a caller can tell an
    operator stop apart from a deadline.
    """


class QuiescentViolationError(BulkMonitorError):
    """Raised when input changes after the fixed bulk boundary is captured."""


class LoaderCrashError(BulkMonitorError):
    """Raised when a tracked loader exits before its lifecycle permits it."""


class OffsetResolutionError(BulkMonitorError):
    """Raised when Kafka metadata or consumer offsets cannot be resolved safely."""


@dataclass(frozen=True)
class ControlAck:
    """Latest run-scoped lifecycle acknowledgement for one loader replica."""

    event_type: str
    epoch: int
    partitions: frozenset[Partition]


class BulkMonitor:
    """Validate assignment, boundary, progress, and drain for one exact run."""

    # A control topic is shared across runs. Drain immediately available
    # historical events in one lifecycle iteration so a prior-run backlog
    # cannot consume the finite bulk-stage deadline one record at a time.
    # The cap preserves a health check between very large backlogs.
    _BACKLOG_DRAIN_LIMIT = 1_000

    def __init__(
        self,
        schema: GraphSchema,
        run_id: str,
        expected_replicas: set[Replica],
        tracked_containers: Mapping[Replica, object],
        *,
        bootstrap_servers: str,
        admin_client: AdminClient | None = None,
        watermark_consumer: Consumer | None = None,
        control_consumer: Consumer | None = None,
        poll_interval_seconds: float = 0.1,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        abort_requested: Callable[[], bool] | None = None,
    ) -> None:
        if not run_id:
            raise BulkMonitorError("run_id must not be empty")
        if poll_interval_seconds <= 0:
            raise BulkMonitorError("poll_interval_seconds must be positive")
        if set(tracked_containers) != expected_replicas:
            raise BulkMonitorError("tracked containers must exactly match expected replicas")

        self._schema = schema
        self._run_id = run_id
        self._expected_replicas = expected_replicas
        self._tracked_containers = dict(tracked_containers)
        self._poll_interval_seconds = poll_interval_seconds
        self._clock = clock
        self._sleep = sleep
        # P6-S5 Story 2: polled on every wait iteration so an operator stop
        # is prompt, not bounded by the (default 300s) stage deadline.
        self._abort_requested = abort_requested
        self._admin = admin_client or AdminClient({"bootstrap.servers": bootstrap_servers})
        self._watermark_consumer = watermark_consumer or Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"graph-loader-watermark-{run_id}",
                "enable.auto.commit": False,
                # A watermark probe owns no input assignment and is not
                # polled; avoid a spurious watchdog event in long bulk runs.
                "max.poll.interval.ms": 3_600_000,
            }
        )
        self._control_consumer = control_consumer or Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"graph-loader-monitor-{run_id}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._owns_watermark_consumer = watermark_consumer is None
        self._owns_control_consumer = control_consumer is None
        self._operation_deadline: float | None = None

        try:
            self._control_consumer.subscribe([CONTROL_TOPIC])
            self._group_by_label = {
                node.label: f"{schema.loading.consumer_group_id}-{node.label}"
                for node in schema.nodes
            }
            self._topic_by_label = {node.label: node.topic for node in schema.nodes}
            topics = [node.topic for node in schema.nodes]
            if len(set(topics)) != len(topics):
                raise BulkMonitorError("each configured node label must use a distinct Kafka topic")
            missing_labels = {label for label, _ in expected_replicas} - set(self._group_by_label)
            if missing_labels:
                raise BulkMonitorError(f"expected replicas use unknown node labels: {sorted(missing_labels)}")
            self._expected_partitions, self._owner_by_partition = self._discover_topology()
        except Exception:
            self._close_owned_consumers()
            raise
        self._acks: dict[Replica, ControlAck] = {}
        self._drain_epochs: dict[Replica, int] = {}
        self._boundary: dict[Partition, int] | None = None
        self._boundary_lows: dict[Partition, int] = {}
        # Per-label boundaries captured by drive_label_readiness as soon as a
        # label alone proves exact coverage (the global boundary, when
        # captured, takes precedence).
        self._label_boundaries: dict[str, dict[Partition, int]] = {}
        self._health_executor: ThreadPoolExecutor | None = None
        self._drain_phase = False
        self._pending_drain_exits: dict[Replica, str] = {}

    @property
    def boundary(self) -> dict[Partition, int] | None:
        """Return a defensive copy of the captured fixed input boundary."""
        return None if self._boundary is None else dict(self._boundary)

    def _discover_topology(self) -> tuple[set[Partition], dict[Partition, str]]:
        """Expand schema topics into fully-qualified Kafka partitions."""
        try:
            metadata = self._admin.list_topics(timeout=10)
        except Exception as exc:
            raise OffsetResolutionError(f"unable to resolve Kafka topic metadata: {exc}") from exc

        expected: set[Partition] = set()
        owners: dict[Partition, str] = {}
        for node in self._schema.nodes:
            topic_metadata = metadata.topics.get(node.topic)
            if topic_metadata is None or getattr(topic_metadata, "error", None):
                raise OffsetResolutionError(f"Kafka topic metadata unavailable for {node.topic!r}")
            partitions = getattr(topic_metadata, "partitions", None)
            if not partitions:
                raise OffsetResolutionError(f"Kafka topic {node.topic!r} has no partitions")
            for partition in partitions:
                partition_id = int(partition)
                key = (node.topic, partition_id)
                expected.add(key)
                owners[key] = self._group_by_label[node.label]
        return expected, owners

    def _check_container_health(
        self, *, allow_drained_exit: bool = False, drained_labels: set[str] | None = None,
    ) -> None:
        """Fail with a replica attribution when a tracked container has exited.

        ``drained_labels`` narrows ``allow_drained_exit`` to the labels whose
        stop was requested; any other label's exit is an immediate crash.
        """
        to_inspect: list[tuple[Replica, object, bool]] = []
        for replica, container in self._tracked_containers.items():
            label, replica_id = replica
            allow_exit = allow_drained_exit if drained_labels is None else label in drained_labels
            if allow_exit and self._is_drain_acknowledged(replica):
                # Any status is acceptable once drain is acknowledged; skip
                # the Docker inspect round trip for finished replicas.
                continue
            to_inspect.append((replica, container, allow_exit))
        # Docker inspects are independent round trips (~5-20 ms each); issue
        # them concurrently so a 40-container fleet costs one round trip per
        # lifecycle iteration.  Results are evaluated below in fleet order.
        reload_errors = self._reload_containers([container for _, container, _ in to_inspect])
        for (replica, container, allow_exit), reload_error in zip(to_inspect, reload_errors):
            label, replica_id = replica
            try:
                if reload_error is not None:
                    raise reload_error
                state = getattr(container, "attrs", {}).get("State", {})
                status = state.get("Status", getattr(container, "status", "running"))
                exit_code = state.get("ExitCode")
            except NotFound as exc:
                # Docker ``remove=True`` can erase a container immediately after
                # its loader flushed a durable DRAIN_COMPLETE record. During the
                # drain phase, let the bounded control loop observe that record.
                if allow_exit:
                    if not self._is_drain_acknowledged(replica):
                        self._pending_drain_exits[replica] = (
                            f"loader disappeared label={label} replica={replica_id} "
                            "before drain acknowledgement"
                        )
                    continue
                raise LoaderCrashError(
                    f"loader disappeared label={label} replica={replica_id} before drain acknowledgement"
                ) from exc
            except Exception as exc:
                raise LoaderCrashError(
                    f"unable to inspect loader label={label} replica={replica_id}: {exc}"
                ) from exc

            if status in {"running", "created", "restarting"}:
                continue
            if allow_exit:
                if not self._is_drain_acknowledged(replica):
                    self._pending_drain_exits[replica] = (
                        f"loader exited label={label} replica={replica_id} "
                        f"status={status} exit_code={exit_code} before drain acknowledgement"
                    )
                continue
            raise LoaderCrashError(
                f"loader exited label={label} replica={replica_id} status={status} exit_code={exit_code}"
            )

    def _reload_containers(self, containers: list[object]) -> list[Exception | None]:
        """Reload every container, returning each one's exception (or None)."""

        def reload(container: object) -> Exception | None:
            try:
                container.reload()
            except Exception as exc:
                return exc
            return None

        if len(containers) <= 1:
            return [reload(container) for container in containers]
        if self._health_executor is None:
            self._health_executor = ThreadPoolExecutor(
                max_workers=16, thread_name_prefix="bulk-monitor-inspect",
            )
        return list(self._health_executor.map(reload, containers))

    def _decode_control(self, message: object) -> None:
        """Apply one valid, current-run lifecycle event to monitor state."""
        if message is None or message.error():
            return
        try:
            raw = message.value()
            if raw is None:
                return
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
            return
        if not isinstance(payload, dict) or payload.get("run_id") != self._run_id:
            return

        label = payload.get("node_label")
        replica_id = payload.get("replica_id")
        event_type = payload.get("type")
        epoch = payload.get("assignment_epoch")
        if not isinstance(label, str) or not label or not isinstance(replica_id, str) or not replica_id:
            return
        replica = (label, replica_id)
        if replica not in self._expected_replicas:
            return
        if event_type not in {"ASSIGNMENT", "IDLE_SURPLUS", "DRAIN_COMPLETE"}:
            return
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 0:
            raise AssignmentCoverageError(f"invalid assignment epoch for {replica}")

        prior = self._acks.get(replica)
        if prior is not None and epoch < prior.epoch:
            return
        if event_type == "DRAIN_COMPLETE":
            self._drain_epochs[replica] = max(self._drain_epochs.get(replica, -1), epoch)
            return

        partitions = self._parse_assignment(payload, event_type, replica)
        self._acks[replica] = ControlAck(event_type, epoch, partitions)

    def _parse_assignment(
        self, payload: dict[str, object], event_type: str, replica: Replica
    ) -> frozenset[Partition]:
        """Validate topic-qualified assignment pairs from one control event."""
        if event_type == "IDLE_SURPLUS":
            if payload.get("assigned_partitions", []) not in ([], None):
                raise AssignmentCoverageError(f"idle replica {replica} declared partitions")
            return frozenset()

        values = payload.get("assigned_partitions")
        if not isinstance(values, list) or not values:
            raise AssignmentCoverageError(f"assignment for {replica} has no partitions")
        pairs: set[Partition] = set()
        for value in values:
            if not isinstance(value, dict):
                raise AssignmentCoverageError(f"assignment for {replica} has invalid partition entry")
            topic, partition = value.get("topic"), value.get("partition")
            if not isinstance(topic, str) or isinstance(partition, bool) or not isinstance(partition, int):
                raise AssignmentCoverageError(f"assignment for {replica} has invalid topic/partition")
            pair = (topic, partition)
            if pair not in self._expected_partitions:
                raise AssignmentCoverageError(f"assignment for {replica} includes unexpected {pair}")
            if topic != self._topic_by_label[replica[0]]:
                raise AssignmentCoverageError(
                    f"assignment for {replica} includes topic owned by another node label: {topic}"
                )
            if pair in pairs:
                raise AssignmentCoverageError(f"assignment for {replica} duplicates {pair}")
            pairs.add(pair)
        return frozenset(pairs)

    def _poll_control(self) -> None:
        """Poll once, then drain a bounded immediately available backlog."""
        self._decode_control(
            self._control_consumer.poll(
                min(self._poll_interval_seconds, self._request_timeout(self._poll_interval_seconds))
            )
        )
        empty_drain_polls = 0
        for _ in range(self._BACKLOG_DRAIN_LIMIT):
            # librdkafka may need a positive timeout to advance its fetch queue;
            # poll(0) only drained the currently buffered record in production.
            # Keep this tiny so even a capped 1,000-record drain returns to
            # exact-container health checks within a bounded ten seconds.
            message = self._control_consumer.poll(
                min(0.01, self._request_timeout(0.01))
            )
            if message is None:
                if self._drain_phase:
                    empty_drain_polls += 1
                    if empty_drain_polls >= 5:
                        return
                # A newly assigned librdkafka consumer can report an empty
                # fetch while a historical record is still in flight. Keep
                # trying within the fixed cap instead of falling back to one
                # record per outer lifecycle poll.
                continue
            empty_drain_polls = 0
            self._decode_control(message)
            # During assignment and durable-progress phases, the complete
            # current-run cover is sufficient. Drain mode deliberately keeps
            # polling to collect every later DRAIN_COMPLETE acknowledgement.
            if not self._drain_phase and self._has_assignment_coverage():
                return

    def _has_assignment_coverage(self) -> bool:
        """Return whether all expected replicas acknowledge one exact partition cover."""
        if set(self._acks) != self._expected_replicas:
            return False
        assigned = [
            partition
            for acknowledgement in self._acks.values()
            for partition in acknowledgement.partitions
        ]
        counts = Counter(assigned)
        return set(counts) == self._expected_partitions and all(count == 1 for count in counts.values())

    def _has_label_assignment_coverage(self, label: str) -> bool:
        """Check one live label without including already drained groups."""
        replicas = {replica for replica in self._expected_replicas if replica[0] == label}
        if not replicas or not replicas.issubset(self._acks):
            return False
        topic = self._topic_by_label[label]
        expected = {partition for partition in self._expected_partitions if partition[0] == topic}
        counts = Counter(
            partition for replica in replicas for partition in self._acks[replica].partitions
        )
        return set(counts) == expected and all(count == 1 for count in counts.values())

    def _wait_until(self, timeout_seconds: float, stage: str, predicate: Callable[[], bool], *, allow_drained_exit: bool = False) -> None:
        """Run one bounded lifecycle loop with health checks on every iteration."""
        if timeout_seconds <= 0:
            raise BulkTimeoutError(f"{stage} timeout must be positive")
        deadline = self._clock() + timeout_seconds
        previous_deadline = self._operation_deadline
        self._operation_deadline = deadline
        try:
            while True:
                if self._abort_requested is not None and self._abort_requested():
                    raise BulkShutdownRequested(f"shutdown requested while waiting for {stage}")
                if self._clock() >= deadline:
                    if allow_drained_exit:
                        self._raise_pending_drain_failure()
                    raise BulkTimeoutError(f"timed out waiting for {stage}")
                # A graceful Docker stop can remove the exact container before
                # the next control poll. During drain, poll acknowledgements
                # first so an already durable record is not misclassified as a
                # NotFound crash.
                if allow_drained_exit:
                    self._poll_control()
                    self._check_container_health(allow_drained_exit=True)
                else:
                    self._check_container_health()
                    self._poll_control()
                if predicate():
                    return
                self._sleep(min(self._poll_interval_seconds, deadline - self._clock()))
        finally:
            self._operation_deadline = previous_deadline

    def wait_for_assignment_coverage(self, timeout_seconds: float) -> None:
        """Wait for every expected replica to prove exact partition coverage."""
        self._wait_until(timeout_seconds, "assignment coverage", self._has_assignment_coverage)

    def _watermarks(
        self, partitions: set[Partition] | None = None,
    ) -> tuple[dict[Partition, int], dict[Partition, int]]:
        """Resolve ``(highs, lows)`` for ``partitions`` (default: every configured one)."""
        watermarks: dict[Partition, int] = {}
        lows: dict[Partition, int] = {}
        for topic, partition in sorted(self._expected_partitions if partitions is None else partitions):
            try:
                low, high = self._watermark_consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=self._request_timeout()
                )
            except Exception as exc:
                raise OffsetResolutionError(
                    f"unable to resolve watermark topic={topic} partition={partition}: {exc}"
                ) from exc
            if (
                isinstance(low, bool)
                or isinstance(high, bool)
                or not isinstance(low, int)
                or not isinstance(high, int)
                or low < 0
                or high < low
            ):
                raise OffsetResolutionError(
                    f"invalid watermark topic={topic} partition={partition}: low={low} high={high}"
                )
            watermarks[(topic, partition)] = high
            lows[(topic, partition)] = low
        return watermarks, lows

    def _empty_at_snapshot(self, partition: Partition, boundary: int) -> bool:
        """``low == high`` when the boundary was captured (the snapshot's low, not the live one).

        An empty partition (never written, or emptied by retention) never gets a group offset, so
        Kafka's absent commit is the normal finished state; a later record is caught by
        ``_assert_quiescent`` (the high watermark moves).
        """
        return boundary == self._boundary_lows.get(partition, 0)

    def _partition_ok(self, partition: Partition, boundary: int, offset: int | None, *, exact: bool) -> bool:
        """Committed through the boundary (exactly, if ``exact``) or empty at the snapshot with nothing ahead."""
        if offset is not None and (offset == boundary or (not exact and offset > boundary)):
            return True
        return self._empty_at_snapshot(partition, boundary) and (offset is None or offset < boundary)

    def capture_boundary(self) -> dict[Partition, int]:
        """Capture the fixed high-watermark boundary after exact coverage exists."""
        if not self._has_assignment_coverage():
            raise AssignmentCoverageError("cannot capture boundary before assignment coverage")
        self._check_container_health()
        self._boundary, lows = self._watermarks()
        self._boundary_lows.update(lows)
        return dict(self._boundary)

    def _label_partitions(self, label: str) -> set[Partition]:
        topic = self._topic_by_label[label]
        return {partition for partition in self._expected_partitions if partition[0] == topic}

    def capture_label_boundary(self, label: str) -> dict[Partition, int]:
        """Capture one label's fixed high-watermark boundary after its own exact coverage.

        Equivalent to :meth:`capture_boundary` restricted to one label's
        topic, so an early label need not wait for every other label's
        consumer group to settle.  Input is still asserted quiescent against
        every captured boundary on each later lifecycle check.
        """
        if label not in self._topic_by_label:
            raise BulkMonitorError(f"unknown node label={label}")
        if not self._has_label_assignment_coverage(label):
            raise AssignmentCoverageError(
                f"cannot capture node label={label} boundary before its assignment coverage"
            )
        boundary, lows = self._watermarks(self._label_partitions(label))
        self._label_boundaries[label] = boundary
        self._boundary_lows.update(lows)
        return dict(boundary)

    def _label_boundary(self, label: str) -> dict[Partition, int] | None:
        """Return the fixed boundary that governs ``label``, if captured."""
        if self._boundary is not None:
            topic = self._topic_by_label[label]
            return {partition: offset for partition, offset in self._boundary.items() if partition[0] == topic}
        return self._label_boundaries.get(label)

    def _captured_boundary(self) -> dict[Partition, int]:
        captured: dict[Partition, int] = {}
        for boundary in self._label_boundaries.values():
            captured.update(boundary)
        if self._boundary is not None:
            captured.update(self._boundary)
        return captured

    def _assert_quiescent(self) -> None:
        """Reject any topic whose high watermark moved after boundary capture."""
        if self._boundary is None and not self._label_boundaries:
            raise BulkMonitorError("boundary has not been captured")
        captured = self._captured_boundary()
        current, _lows = self._watermarks() if not self._label_boundaries else self._watermarks(set(captured))
        for partition, boundary_offset in captured.items():
            current_offset = current[partition]
            if current_offset != boundary_offset:
                raise QuiescentViolationError(
                    f"bulk input changed topic={partition[0]} partition={partition[1]} "
                    f"boundary={boundary_offset} current={current_offset}"
                )

    def _committed_offsets(self, labels: set[str] | None = None) -> dict[Partition, int | None]:
        """Read stable committed offsets with the installed confluent-kafka API.

        ``labels`` restricts the admin requests to those labels' groups;
        partitions of other labels are then reported as ``None``.
        """
        groups = None if labels is None else {self._group_by_label[label] for label in labels}
        partitions_by_group: dict[str, list[Partition]] = defaultdict(list)
        for partition, group_id in self._owner_by_partition.items():
            if groups is None or group_id in groups:
                partitions_by_group[group_id].append(partition)

        resolved: dict[Partition, int | None] = {partition: None for partition in self._expected_partitions}
        # The admin API takes one group per request; issue every request
        # before awaiting any so the groups resolve in one round trip.
        pending: dict[str, object] = {}
        for group_id, partitions in partitions_by_group.items():
            request = ConsumerGroupTopicPartitions(
                group_id,
                [TopicPartition(topic, partition) for topic, partition in sorted(partitions)],
            )
            try:
                pending[group_id] = self._admin.list_consumer_group_offsets(
                    [request], require_stable=True,
                )[group_id]
            except Exception as exc:
                # Raised in group order below, exactly as a sequential read would.
                pending[group_id] = exc
        for group_id, future in pending.items():
            try:
                if isinstance(future, Exception):
                    raise future
                result = future.result(timeout=self._request_timeout())
                returned = result.topic_partitions
            except Exception as exc:
                raise OffsetResolutionError(
                    f"unable to resolve committed offsets for group={group_id}: {exc}"
                ) from exc
            for item in returned:
                key = (item.topic, item.partition)
                if key in resolved and isinstance(item.offset, int) and item.offset >= 0:
                    resolved[key] = item.offset
        return resolved

    def _at_boundary(self) -> bool:
        """Return whether every expected partition is durably committed through boundary."""
        if self._boundary is None:
            raise BulkMonitorError("boundary has not been captured")
        offsets = self._committed_offsets()
        # Kafka has no group offset for an assigned partition that held no record at the
        # snapshot (low == high); any other partition still requires a commit.
        return all(
            self._partition_ok(partition, boundary_offset, offsets[partition], exact=False)
            for partition, boundary_offset in self._boundary.items()
        )

    def wait_for_completion(self, timeout_seconds: float) -> None:
        """Wait for durable committed offsets through an unchanged boundary."""
        if self._boundary is None:
            raise BulkMonitorError("boundary has not been captured")

        def complete() -> bool:
            self._assert_quiescent()
            return self._has_assignment_coverage() and self._at_boundary()

        self._wait_until(timeout_seconds, "bulk completion", complete)

    def wait_for_next_label_completion(self, labels: set[str], timeout_seconds: float) -> str:
        """Return one node label whose fixed Kafka boundary is durably complete."""
        if self._boundary is None:
            raise BulkMonitorError("boundary has not been captured")
        if not labels or not labels.issubset(self._topic_by_label):
            raise BulkMonitorError("requested completion labels must be configured")
        completed: str | None = None

        def one_complete() -> bool:
            nonlocal completed
            self._assert_quiescent()
            offsets = self._committed_offsets()
            for label in sorted(labels):
                if not self._has_label_assignment_coverage(label):
                    continue
                topic = self._topic_by_label[label]
                if all(
                    self._partition_ok((name, partition), boundary, offsets[(name, partition)], exact=False)
                    for (name, partition), boundary in self._boundary.items()
                    if name == topic
                ):
                    completed = label
                    return True
            return False

        self._wait_until(
            timeout_seconds, "node label completion", one_complete,
            allow_drained_exit=self._drain_phase,
        )
        assert completed is not None
        return completed

    def _is_drain_acknowledged(self, replica: Replica) -> bool:
        """Return whether a replica acknowledged drain for its latest assignment epoch."""
        acknowledgement = self._acks.get(replica)
        return acknowledgement is not None and self._drain_epochs.get(replica, -1) >= acknowledgement.epoch

    def wait_for_drain_complete(self, timeout_seconds: float) -> None:
        """Wait for durable drain acknowledgements before accepting container exits."""
        self._drain_phase = True
        self._pending_drain_exits = {}
        self._wait_until(
            timeout_seconds,
            "drain completion",
            lambda: all(self._is_drain_acknowledged(replica) for replica in self._expected_replicas),
            allow_drained_exit=True,
        )

    def wait_for_label_drain_complete(self, label: str, timeout_seconds: float) -> None:
        """Prove the stopped replicas of one label acknowledged drain."""
        if label not in self._topic_by_label:
            raise BulkMonitorError(f"unknown node label={label}")
        self._drain_phase = True
        self._wait_until(
            timeout_seconds, f"node label={label} drain completion",
            lambda: all(
                self._is_drain_acknowledged(replica)
                for replica in self._expected_replicas if replica[0] == label
            ),
            allow_drained_exit=True,
        )

    def verify_label_zero_lag(self, label: str) -> None:
        """Require exact committed offsets for one drained label."""
        if self._boundary is None or label not in self._topic_by_label:
            raise BulkMonitorError(f"node label={label} has no captured boundary")
        self._check_container_health(allow_drained_exit=True)
        self._assert_quiescent()
        offsets = self._committed_offsets()
        topic = self._topic_by_label[label]
        for partition, boundary in self._boundary.items():
            if partition[0] != topic:
                continue
            offset = offsets[partition]
            if self._partition_ok(partition, boundary, offset, exact=True):
                continue
            detail = ""
            if offset is not None and offset > boundary and self._empty_at_snapshot(partition, boundary):
                detail = " (committed offset above the high watermark of an empty partition: topic recreated under a reused run id?)"
            raise OffsetResolutionError(
                f"nonzero lag label={label} topic={partition[0]} "
                f"partition={partition[1]} committed={offset} boundary={boundary}{detail}"
            )

    def _label_at_boundary(
        self, label: str, offsets: Mapping[Partition, int | None], *, exact: bool,
    ) -> bool:
        """Return whether one label's partitions are committed through the boundary."""
        label_boundary = self._label_boundary(label)
        assert label_boundary is not None
        for partition, boundary in label_boundary.items():
            offset = offsets.get(partition)
            if not self._partition_ok(partition, boundary, offset, exact=exact):
                return False
        return True

    def drive_label_readiness(
        self,
        labels: set[str],
        timeout_seconds: float,
        *,
        stop_label: Callable[[str], None],
        on_ready: Callable[[str], None],
        should_abort: Callable[[], bool] | None = None,
        max_concurrent_stops: int | None = None,
    ) -> None:
        """Independently cover, bound, stop, drain-prove, and zero-lag-prove every label.

        Each label advances ``joining -> running -> draining -> ready`` on its
        own.  A label's fixed boundary is captured as soon as that label alone
        proves exact assignment coverage (unless a global boundary was already
        captured), so a small label is not held behind every other label's
        consumer-group rebalances.  A label that reaches its boundary is
        stopped at once and published without waiting behind any other label.

        Only this calling thread touches the (non thread-safe) Kafka clients.
        ``stop_label`` runs on a worker pool (one worker per label by
        default, so no label's SIGTERM queues behind another's Docker stop),
        must never use Kafka, and must raise when the label's exact containers
        cannot be stopped.  ``on_ready`` runs on this thread only after every
        replica of the label acknowledged DRAIN_COMPLETE and the label's
        committed offsets, read after those acknowledgements, exactly equal
        the unchanged boundary.  Readiness does not wait for Docker to finish
        tearing a stopped container down: DRAIN_COMPLETE is only published
        after the replica's final offset commit, so the acknowledgements and
        offsets are the proof.  Every stop must still return (or its failure
        is raised) before this method returns.

        Deadlines: while any label is still running, some label must capture
        its boundary, reach its boundary, finish its stop, or become ready
        within ``timeout_seconds`` of the previous such progress; each stopped
        label must prove its drain, and its Docker stop must return, within
        ``timeout_seconds`` of the stop being requested.  Any failure raises;
        the caller owns fail-closed cleanup of every exact container.
        """
        if not labels or not labels.issubset(self._topic_by_label):
            raise BulkMonitorError("requested completion labels must be configured")
        if timeout_seconds <= 0:
            raise BulkTimeoutError("node label readiness timeout must be positive")

        replicas_by_label: dict[str, set[Replica]] = defaultdict(set)
        for replica in self._expected_replicas:
            replicas_by_label[replica[0]].add(replica)
        running = set(labels)
        stop_requested: set[str] = set()
        stopping: dict[str, tuple[Future, float]] = {}
        draining: dict[str, float] = {}
        progress_deadline = self._clock() + timeout_seconds
        previous_deadline = self._operation_deadline
        workers = len(labels) if max_concurrent_stops is None else max_concurrent_stops
        executor = ThreadPoolExecutor(
            max_workers=max(1, min(len(labels), workers)),
            thread_name_prefix="node-label-stop",
        )
        succeeded = False
        try:
            while running or stopping or draining:
                if should_abort is not None and should_abort():
                    raise BulkMonitorError("node label readiness aborted (dependent stage failure or shutdown request)")
                now = self._clock()
                for label, deadline in sorted(draining.items()):
                    if now >= deadline:
                        self._raise_pending_drain_failure(label)
                        raise BulkTimeoutError(f"timed out waiting for node label={label} drain completion")
                for label, (_, deadline) in sorted(stopping.items()):
                    if now >= deadline:
                        raise BulkTimeoutError(f"timed out waiting for node label={label} container stop")
                if running and now >= progress_deadline:
                    raise BulkTimeoutError("timed out waiting for node label completion")
                deadlines = (
                    ([progress_deadline] if running else [])
                    + list(draining.values())
                    + [deadline for _, deadline in stopping.values()]
                )
                self._operation_deadline = min(deadlines) if deadlines else None

                # As in _wait_until: once a stop may have removed an exact
                # container, read acknowledgements before inspecting Docker.
                if stop_requested:
                    self._poll_control()
                    self._check_container_health(drained_labels=stop_requested)
                else:
                    self._check_container_health()
                    self._poll_control()

                progressed = False
                for label in sorted(label for label, (future, _) in stopping.items() if future.done()):
                    stopping.pop(label)[0].result()
                    progress_deadline = self._clock() + timeout_seconds
                    progressed = True

                for label in sorted(running):
                    if self._label_boundary(label) is None and self._has_label_assignment_coverage(label):
                        self.capture_label_boundary(label)
                        progress_deadline = self._clock() + timeout_seconds
                        progressed = True
                bounded = {label for label in running if self._label_boundary(label) is not None}

                acknowledged = sorted(
                    label for label in draining
                    if all(self._is_drain_acknowledged(replica) for replica in replicas_by_label[label])
                )
                if bounded or acknowledged:
                    # Offsets are read strictly after the acknowledgements above.
                    self._assert_quiescent()
                    offsets = self._committed_offsets(bounded | set(acknowledged))
                    for label in acknowledged:
                        if not self._label_at_boundary(label, offsets, exact=True):
                            topic = self._topic_by_label[label]
                            detail = " ".join(
                                f"partition={partition} committed={offsets.get((name, partition))} "
                                f"boundary={boundary}"
                                for (name, partition), boundary in sorted(self._label_boundary(label).items())
                            )
                            raise OffsetResolutionError(f"nonzero lag label={label} topic={topic} {detail}")
                        del draining[label]
                        progress_deadline = self._clock() + timeout_seconds
                        progressed = True
                        on_ready(label)
                    for label in sorted(bounded):
                        if not self._has_label_assignment_coverage(label):
                            continue
                        if not self._label_at_boundary(label, offsets, exact=False):
                            continue
                        running.remove(label)
                        stop_requested.add(label)
                        self._drain_phase = True
                        requested_at = self._clock()
                        stopping[label] = (
                            executor.submit(stop_label, label), requested_at + timeout_seconds,
                        )
                        draining[label] = requested_at + timeout_seconds
                        progress_deadline = requested_at + timeout_seconds
                        progressed = True
                if not progressed and (running or stopping or draining):
                    remaining = [deadline - self._clock() for deadline in deadlines]
                    self._sleep(max(0.0, min([self._poll_interval_seconds, *remaining])))
            succeeded = True
        finally:
            self._operation_deadline = previous_deadline
            # On failure do not block on in-flight Docker stops: the caller
            # immediately stops every exact container anyway.
            executor.shutdown(wait=succeeded, cancel_futures=True)

    def _raise_pending_drain_failure(self, label: str | None = None) -> None:
        """Attribute an exited exact replica that never acknowledged its drain."""
        for replica in sorted(self._pending_drain_exits):
            if label is not None and replica[0] != label:
                continue
            if not self._is_drain_acknowledged(replica):
                raise LoaderCrashError(self._pending_drain_exits[replica])

    def verify_zero_lag(self) -> None:
        """Re-verify the fixed boundary and exact no-lag state after drain."""
        if self._boundary is None:
            raise BulkMonitorError("boundary has not been captured")
        self._check_container_health(allow_drained_exit=self._drain_phase)
        self._assert_quiescent()
        offsets = self._committed_offsets()
        for partition, boundary_offset in self._boundary.items():
            offset = offsets[partition]
            if self._partition_ok(partition, boundary_offset, offset, exact=True):
                continue
            detail = ""
            if offset is not None and offset > boundary_offset and self._empty_at_snapshot(partition, boundary_offset):
                detail = " (committed offset above the high watermark of an empty partition: topic recreated under a reused run id?)"
            raise OffsetResolutionError(
                f"nonzero lag topic={partition[0]} partition={partition[1]} "
                f"committed={offset} boundary={boundary_offset}{detail}"
            )

    def close(self) -> None:
        """Close only monitor-owned Kafka consumers."""
        if self._health_executor is not None:
            self._health_executor.shutdown(wait=False, cancel_futures=True)
            self._health_executor = None
        self._close_owned_consumers()

    def _close_owned_consumers(self) -> None:
        """Best-effort cleanup for partially constructed or closed monitors."""
        for consumer, owned in (
            (self._watermark_consumer, self._owns_watermark_consumer),
            (self._control_consumer, self._owns_control_consumer),
        ):
            if owned:
                try:
                    consumer.close()
                except Exception:
                    logger.exception("Unable to close bulk monitor Kafka consumer")

    def _request_timeout(self, default_seconds: float = 10.0) -> float:
        """Clip a Kafka request timeout to the active lifecycle-stage deadline."""
        if self._operation_deadline is None:
            return default_seconds
        remaining = self._operation_deadline - self._clock()
        if remaining <= 0:
            raise BulkTimeoutError("bulk monitor stage deadline elapsed")
        return min(default_seconds, remaining)
