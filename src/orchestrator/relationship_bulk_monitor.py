"""Run-scoped finite-bulk monitoring for a stage of relationship loaders."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import time
from typing import Callable, Mapping, Sequence

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient
from docker.errors import NotFound

from src.loader.control import CONTROL_TOPIC
from src.models.schema import EdgeConfig
from src.orchestrator.coordination import ClockLease, ClockProtocolError, decode_clock_lease
from src.orchestrator.rotation import RotationPlan
from src.orchestrator.bulk_monitor import (
    AssignmentCoverageError, BulkMonitorError, BulkTimeoutError, LoaderCrashError,
    OffsetResolutionError, QuiescentViolationError,
)

Replica = tuple[str, str]
WorkPartition = tuple[str, str, int]


@dataclass(frozen=True)
class EdgeControlAck:
    """Latest validated assignment acknowledgement for an edge replica."""
    event_type: str
    epoch: int
    partitions: frozenset[tuple[str, int]]


class RelationshipBulkMonitor:
    """Prove one exact relationship stage reaches a quiescent durable drain."""

    _BACKLOG_DRAIN_LIMIT = 1_000

    def __init__(
        self, edges: Sequence[EdgeConfig], run_id: str, expected_replicas: set[Replica],
        tracked_containers: Mapping[Replica, object], *, bootstrap_servers: str,
        consumer_group_prefix: str, admin_client: AdminClient | None = None,
        watermark_consumer: Consumer | None = None, control_consumer: Consumer | None = None,
        rotation_plan: RotationPlan | None = None, coordination_topic: str | None = None,
        tracked_clock: object | None = None, coordination_consumer: Consumer | None = None,
        poll_interval_seconds: float = 0.25, clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        wall_clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise BulkMonitorError("relationship bulk monitor poll interval must be positive")
        if not run_id or not edges or set(tracked_containers) != expected_replicas:
            raise BulkMonitorError("relationship bulk monitor requires one exact nonempty run stage")
        self._edges = {edge.type: edge for edge in edges}
        if len(self._edges) != len(edges):
            raise BulkMonitorError("relationship bulk stage has duplicate edge types")
        if {edge_type for edge_type, _ in expected_replicas} != set(self._edges):
            raise BulkMonitorError("expected relationship replicas do not match stage edge types")
        rotation_dependencies = (rotation_plan, coordination_topic, tracked_clock)
        if any(value is not None for value in rotation_dependencies) and not all(value is not None for value in rotation_dependencies):
            raise BulkMonitorError("relationship rotation monitor requires plan, topic, and exact clock")
        if coordination_consumer is not None and rotation_plan is None:
            raise BulkMonitorError("relationship coordination consumer requires a rotation plan")
        if rotation_plan is not None:
            if not isinstance(rotation_plan, RotationPlan) or not isinstance(coordination_topic, str) or not coordination_topic.strip():
                raise BulkMonitorError("relationship rotation monitor has invalid plan or coordination topic")
            plan_types = {edge_type for family in rotation_plan.families for edge_type in family.edge_types}
            if plan_types != set(self._edges):
                raise BulkMonitorError("relationship rotation plan does not match monitored edge fleet")
        self._run_id, self._expected_replicas = run_id, expected_replicas
        self._tracked_containers = dict(tracked_containers)
        self._groups = {edge_type: f"{consumer_group_prefix}-{edge_type}-{run_id}" for edge_type in self._edges}
        self._clock, self._sleep, self._interval = clock, sleep, poll_interval_seconds
        self._admin = admin_client or AdminClient({"bootstrap.servers": bootstrap_servers})
        self._watermark = watermark_consumer or Consumer({"bootstrap.servers": bootstrap_servers, "group.id": f"graph-loader-edge-watermark-{run_id}", "enable.auto.commit": False})
        self._control = control_consumer or Consumer({"bootstrap.servers": bootstrap_servers, "group.id": f"graph-loader-edge-monitor-{run_id}", "enable.auto.commit": False, "auto.offset.reset": "earliest"})
        self._rotation_plan = rotation_plan
        self._coordination_topic = coordination_topic
        self._tracked_clock = tracked_clock
        self._coordination = coordination_consumer or (
            Consumer({"bootstrap.servers": bootstrap_servers, "group.id": f"graph-loader-edge-clock-monitor-{run_id}", "enable.auto.commit": False, "auto.offset.reset": "earliest"})
            if rotation_plan is not None else None
        )
        self._own_watermark, self._own_control = watermark_consumer is None, control_consumer is None
        self._own_coordination = rotation_plan is not None and coordination_consumer is None
        self._operation_deadline: float | None = None
        self._wall_clock_ms = wall_clock_ms or (lambda: int(time.time() * 1000))
        try:
            self._control.subscribe([CONTROL_TOPIC])
            if self._coordination is not None:
                self._coordination.subscribe([coordination_topic])
            self._expected, self._raw_expected = self._discover_topology()
        except Exception:
            self.close()
            raise
        self._acks: dict[Replica, EdgeControlAck] = {}
        self._drains: dict[Replica, int] = {}
        self._boundary: dict[WorkPartition, int] | None = None
        self._draining = False
        self._clock_lease: ClockLease | None = None
        self._accepted_clock_epochs: set[int] = set()

    @property
    def clock_lease(self) -> ClockLease | None:
        """Return the newest validated current-run lease for diagnostics only."""
        return self._clock_lease

    def _clock_context(self) -> str:
        if self._clock_lease is None:
            return "epoch=unknown slot=unknown"
        return f"epoch={self._clock_lease.epoch} slot={self._clock_lease.slot_id}"

    def _failure(self, error_type, message: str):
        """Attach rotating-fleet attribution without changing legacy stages."""
        if self._rotation_plan is not None:
            message = f"stage=monitor run_id={self._run_id} {self._clock_context()} {message}"
        return error_type(message)

    def _has_current_clock_lease(self) -> bool:
        """Require a non-expired clock observation for rotation-sensitive work."""
        if self._rotation_plan is None:
            return True
        if self._clock_lease is None:
            return False
        try:
            now_ms = self._wall_clock_ms()
        except Exception as exc:
            raise BulkMonitorError(
                f"stage=monitor run_id={self._run_id} clock wall-time failed {self._clock_context()}: {exc}"
            ) from exc
        if isinstance(now_ms, bool) or not isinstance(now_ms, int) or now_ms >= self._clock_lease.expires_at_ms:
            raise BulkMonitorError(
                f"stage=monitor run_id={self._run_id} clock lease expired {self._clock_context()}"
            )
        return True

    def _discover_topology(self) -> tuple[set[WorkPartition], dict[str, set[tuple[str, int]]]]:
        try:
            metadata = self._admin.list_topics(timeout=self._request_timeout())
        except Exception as exc:
            raise OffsetResolutionError(f"relationship stage metadata unavailable: {exc}") from exc
        expected: set[WorkPartition] = set()
        raw: dict[str, set[tuple[str, int]]] = {}
        for edge_type, edge in self._edges.items():
            topic = metadata.topics.get(edge.topic)
            if topic is None or getattr(topic, "error", None) or not getattr(topic, "partitions", None):
                raise OffsetResolutionError(f"relationship topic metadata unavailable edge={edge_type} topic={edge.topic}")
            raw[edge_type] = {(edge.topic, int(partition)) for partition in topic.partitions}
            expected.update((edge_type, topic_name, partition) for topic_name, partition in raw[edge_type])
        return expected, raw

    def _stage(self) -> str:
        return ",".join(sorted(self._edges))

    def _decode_control(self, message: object) -> None:
        if message is None or message.error():
            return
        try:
            payload = json.loads(message.value().decode("utf-8"))
        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
            return
        if not isinstance(payload, dict) or payload.get("run_id") != self._run_id:
            return
        edge_type, replica_id = payload.get("edge_type"), payload.get("replica_id")
        replica = (edge_type, replica_id)
        event_type, epoch = payload.get("type"), payload.get("assignment_epoch")
        if replica not in self._expected_replicas or event_type not in {"ASSIGNMENT", "IDLE_SURPLUS", "DRAIN_COMPLETE"}:
            return
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 0:
            raise AssignmentCoverageError(f"invalid relationship assignment epoch replica={replica}")
        prior = self._acks.get(replica)
        if event_type == "DRAIN_COMPLETE":
            if self._rotation_plan is not None and (prior is None or epoch != prior.epoch):
                expected = "none" if prior is None else str(prior.epoch)
                raise self._failure(
                    AssignmentCoverageError,
                    f"edge={edge_type} replica={replica_id} drain epoch={epoch} does not match assignment epoch={expected}",
                )
            if prior is not None and epoch == prior.epoch:
                self._drains[replica] = epoch
            return
        partitions = self._parse_assignment(payload, event_type, replica)
        current = EdgeControlAck(event_type, epoch, partitions)
        if prior is not None and epoch < prior.epoch:
            return
        if prior is not None and epoch == prior.epoch:
            if prior != current:
                raise AssignmentCoverageError(f"conflicting same-epoch relationship assignment replica={replica}")
            return
        self._acks[replica] = current
        self._drains.pop(replica, None)

    def _parse_assignment(self, payload: dict[str, object], event_type: str, replica: Replica) -> frozenset[tuple[str, int]]:
        values = payload.get("assigned_partitions", [])
        if event_type == "IDLE_SURPLUS":
            if values not in ([], None):
                raise AssignmentCoverageError(f"idle relationship replica declared partitions replica={replica}")
            return frozenset()
        if not isinstance(values, list) or not values:
            raise AssignmentCoverageError(f"relationship assignment missing partitions replica={replica}")
        partitions: set[tuple[str, int]] = set()
        for value in values:
            if not isinstance(value, dict) or not isinstance(value.get("topic"), str) or isinstance(value.get("partition"), bool) or not isinstance(value.get("partition"), int):
                raise AssignmentCoverageError(f"invalid relationship assignment replica={replica}")
            pair = (value["topic"], value["partition"])
            if pair not in self._raw_expected[replica[0]] or pair in partitions:
                raise AssignmentCoverageError(f"unexpected or duplicate relationship assignment replica={replica} partition={pair}")
            partitions.add(pair)
        return frozenset(partitions)

    def _coverage(self) -> bool:
        if set(self._acks) != self._expected_replicas:
            return False
        claimed = [(edge_type, topic, partition) for (edge_type, _), ack in self._acks.items() for topic, partition in ack.partitions]
        counts = Counter(claimed)
        return set(counts) == self._expected and all(count == 1 for count in counts.values())

    def _check_health(self, allow_drain: bool = False) -> None:
        for replica, container in self._tracked_containers.items():
            try:
                container.reload(); state = getattr(container, "attrs", {}).get("State", {})
                status = state.get("Status", getattr(container, "status", "running"))
            except NotFound:
                status = "removed"
            except Exception as exc:
                raise self._failure(
                    LoaderCrashError,
                    f"relationship stage={self._stage()} edge={replica[0]} replica={replica[1]} inspect failed: {exc}",
                ) from exc
            if status not in {"running", "created", "restarting"}:
                ack = self._acks.get(replica)
                if allow_drain and ack is not None and self._drains.get(replica) == ack.epoch:
                    continue
                raise self._failure(
                    LoaderCrashError,
                    f"relationship stage={self._stage()} edge={replica[0]} replica={replica[1]} exited before drain acknowledgement",
                )

    def _check_clock_health(self) -> None:
        """Reload only the exact clock object retained by this fleet launch."""
        if self._tracked_clock is None:
            return
        try:
            self._tracked_clock.reload()
            state = getattr(self._tracked_clock, "attrs", {}).get("State", {})
            status = state.get("Status", getattr(self._tracked_clock, "status", "running"))
        except NotFound:
            status = "removed"
        except Exception as exc:
            raise LoaderCrashError(
                f"stage=monitor run_id={self._run_id} clock inspect failed {self._clock_context()}: {exc}"
            ) from exc
        if status not in {"running", "created", "restarting"}:
            raise LoaderCrashError(
                f"stage=monitor run_id={self._run_id} clock exited status={status} {self._clock_context()}"
            )

    def _decode_clock(self, message: object) -> None:
        if message is None or message.error():
            return
        try:
            payload = message.value()
            lease = decode_clock_lease(
                payload, expected_run_id=self._run_id, now_ms=self._wall_clock_ms(),
                rotation_plan=self._rotation_plan,
            )
        except Exception as exc:
            raise BulkMonitorError(
                f"stage=monitor run_id={self._run_id} invalid clock lease {self._clock_context()}: {exc}"
            ) from exc
        if lease is None:
            return
        prior = self._clock_lease
        if prior is not None:
            if lease.epoch < prior.epoch:
                raise BulkMonitorError(
                    f"stage=monitor run_id={self._run_id} stale clock lease "
                    f"epoch={lease.epoch} slot={lease.slot_id} last_{self._clock_context()}"
                )
            if lease.epoch == prior.epoch:
                reason = "duplicate" if lease == prior else "conflicting"
                raise BulkMonitorError(
                    f"stage=monitor run_id={self._run_id} {reason} clock lease "
                    f"epoch={lease.epoch} slot={lease.slot_id} last_{self._clock_context()}"
                )
        self._clock_lease = lease
        self._accepted_clock_epochs.add(lease.epoch)

    def _poll_bounded(self, consumer, decoder: Callable[[object], None]) -> None:
        """Decode one timed record and a bounded immediately available backlog."""
        decoder(consumer.poll(
            min(self._interval, self._request_timeout(self._interval))
        ))
        for _ in range(self._BACKLOG_DRAIN_LIMIT):
            message = consumer.poll(0)
            if message is None:
                break
            decoder(message)

    def _poll(self) -> None:
        self._poll_bounded(self._control, self._decode_control)
        if self._coordination is not None:
            self._poll_bounded(self._coordination, self._decode_clock)

    def _wait(self, timeout: float, name: str, predicate: Callable[[], bool], *, drain: bool = False) -> None:
        if timeout <= 0:
            raise BulkTimeoutError(f"relationship stage={self._stage()} {name} timeout must be positive")
        deadline = self._clock() + timeout
        previous_deadline = self._operation_deadline
        self._operation_deadline = deadline
        try:
            while self._clock() < deadline:
                self._check_clock_health()
                if drain:
                    self._poll(); self._check_health(True)
                else:
                    self._check_health(); self._poll()
                if predicate():
                    return
                self._sleep(min(self._interval, deadline - self._clock()))
            raise BulkTimeoutError(f"timed out waiting for relationship stage={self._stage()} {name}")
        finally:
            self._operation_deadline = previous_deadline

    def wait_for_assignment_coverage(self, timeout_seconds: float) -> None:
        self._wait(timeout_seconds, "assignment coverage", self._coverage)

    def wait_for_clock_lease(self, timeout_seconds: float) -> None:
        """Wait for a validated current-run lease without touching work commits."""
        self._wait(timeout_seconds, "current clock lease", self._has_current_clock_lease)

    def _watermarks(self) -> dict[WorkPartition, int]:
        result = {}
        for edge_type, topic, partition in sorted(self._expected):
            try:
                low, high = self._watermark.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=self._request_timeout()
                )
            except Exception as exc:
                raise self._failure(
                    OffsetResolutionError,
                    f"watermark failed edge={edge_type} topic={topic} partition={partition}: {exc}",
                ) from exc
            if not isinstance(low, int) or isinstance(low, bool) or not isinstance(high, int) or isinstance(high, bool) or low < 0 or high < low:
                raise self._failure(
                    OffsetResolutionError,
                    f"invalid watermark edge={edge_type} topic={topic} partition={partition}",
                )
            result[(edge_type, topic, partition)] = high
        return result

    def capture_boundary(self) -> dict[WorkPartition, int]:
        if not self._coverage():
            raise AssignmentCoverageError("cannot capture relationship boundary before coverage")
        if not self._has_current_clock_lease():
            raise BulkMonitorError(
                f"stage=monitor run_id={self._run_id} cannot capture boundary without a current clock lease"
            )
        self._check_health(); self._boundary = self._watermarks()
        return dict(self._boundary)

    def _assert_quiescent(self) -> None:
        if self._boundary is None:
            raise BulkMonitorError("relationship boundary has not been captured")
        if self._watermarks() != self._boundary:
            raise self._failure(
                QuiescentViolationError,
                f"relationship stage={self._stage()} input changed after boundary",
            )

    def _commits(self) -> dict[WorkPartition, int | None]:
        result: dict[WorkPartition, int | None] = {item: None for item in self._expected}
        for edge_type, raw in self._raw_expected.items():
            request = ConsumerGroupTopicPartitions(self._groups[edge_type], [TopicPartition(topic, partition) for topic, partition in sorted(raw)])
            try:
                response = self._admin.list_consumer_group_offsets(
                    [request], require_stable=True
                )[request.group_id].result(timeout=self._request_timeout())
            except Exception as exc:
                raise self._failure(
                    OffsetResolutionError,
                    f"committed offset failed edge={edge_type}: {exc}",
                ) from exc
            for item in response.topic_partitions:
                if (edge_type, item.topic, item.partition) in result and isinstance(item.offset, int) and item.offset >= 0:
                    result[(edge_type, item.topic, item.partition)] = item.offset
        return result

    def wait_for_completion(self, timeout_seconds: float) -> None:
        if self._boundary is None:
            raise BulkMonitorError("relationship boundary has not been captured")
        self._wait(timeout_seconds, "completion", self._at_boundary)

    def wait_for_rotating_completion(self, timeout_seconds: float) -> None:
        """Prove all fixed boundaries across live rotation slots.

        This deliberately remains distinct from the Phase 3 one-shot method:
        every decision rechecks the current lease as slots advance.  It does
        not demand an arbitrary number of epochs; a finite input may already
        be durably complete in its first valid slot.
        """
        if self._rotation_plan is None:
            raise BulkMonitorError("rotating completion requires a rotation monitor")
        if self._boundary is None:
            raise BulkMonitorError("relationship boundary has not been captured")
        self._wait(timeout_seconds, "rotating completion", self._at_rotating_boundary)

    def supervise_stream(self, shutdown_requested, *, initial_lease_timeout_seconds: float) -> None:
        """Keep one exact rotating stream fleet healthy until explicit shutdown.

        Stream supervision deliberately has no watermark/boundary or lag-based
        success path.  The supplied Event-compatible object makes termination
        deterministic for the CLI and tests alike.
        """
        if self._rotation_plan is None:
            raise BulkMonitorError("stream supervision requires a rotation monitor")
        if not hasattr(shutdown_requested, "is_set"):
            raise BulkMonitorError("stream supervision requires a shutdown event")
        try:
            self.wait_for_clock_lease(initial_lease_timeout_seconds)
        except BulkTimeoutError as exc:
            raise self._failure(
                BulkTimeoutError,
                "initial stream clock lease timed out",
            ) from exc
        while not shutdown_requested.is_set():
            self._check_clock_health()
            self._check_health()
            self._poll()
            if not self._has_current_clock_lease():
                raise BulkMonitorError(
                    f"stage=monitor run_id={self._run_id} stream has no current clock lease {self._clock_context()}"
                )
            self._sleep(self._interval)

    def _at_boundary(self) -> bool:
        """Return whether one stable offset snapshot reaches every boundary."""
        if not self._has_current_clock_lease():
            return False
        self._assert_quiescent()
        if not self._coverage() or self._boundary is None:
            return False
        commits = self._commits()
        return all(
            commits[key] is not None and commits[key] >= boundary
            for key, boundary in self._boundary.items()
        )

    def _at_rotating_boundary(self) -> bool:
        """Return true only for one live-lease all-edge durable snapshot."""
        if not self._has_current_clock_lease():
            return False
        self._assert_quiescent()
        if not self._coverage() or self._boundary is None:
            return False
        commits = self._commits()
        return all(
            commits[key] is not None and commits[key] >= boundary
            for key, boundary in self._boundary.items()
        )

    def wait_for_drain_complete(self, timeout_seconds: float) -> None:
        self._draining = True
        try:
            self._wait(timeout_seconds, "drain completion", self._drain_complete, drain=True)
        except BulkTimeoutError as exc:
            missing = [
                f"edge={edge_type} replica={replica_id}"
                for edge_type, replica_id in sorted(self._expected_replicas)
                if self._drains.get((edge_type, replica_id))
                != self._acks.get((edge_type, replica_id), EdgeControlAck("", -1, frozenset())).epoch
            ]
            if self._rotation_plan is not None:
                raise self._failure(
                    BulkTimeoutError,
                    f"drain acknowledgement incomplete {' '.join(missing) or 'unknown replica'}",
                ) from exc
            raise

    def _drain_complete(self) -> bool:
        return all(
            self._drains.get(replica)
            == self._acks.get(replica, EdgeControlAck("", -1, frozenset())).epoch
            for replica in self._expected_replicas
        )

    def verify_zero_lag(self) -> None:
        if self._boundary is None:
            raise BulkMonitorError("relationship boundary has not been captured")
        if not self._has_current_clock_lease():
            raise BulkMonitorError(
                f"stage=monitor run_id={self._run_id} cannot verify zero lag without a current clock lease"
            )
        self._check_health(self._draining); self._assert_quiescent(); commits = self._commits()
        for key, boundary in self._boundary.items():
            if commits[key] != boundary:
                raise self._failure(
                    OffsetResolutionError,
                    f"relationship stage={self._stage()} nonzero lag edge={key[0]} topic={key[1]} partition={key[2]}",
                )

    def close(self) -> None:
        for consumer, owned in (
            (self._watermark, self._own_watermark),
            (self._control, self._own_control),
            (self._coordination, self._own_coordination),
        ):
            if owned:
                try: consumer.close()
                except Exception: pass

    def _request_timeout(self, default_seconds: float = 10.0) -> float:
        """Clip a Kafka call to the current lifecycle-stage deadline."""
        if self._operation_deadline is None:
            return default_seconds
        remaining = self._operation_deadline - self._clock()
        if remaining <= 0:
            raise BulkTimeoutError("relationship bulk monitor stage deadline elapsed")
        return min(default_seconds, remaining)
