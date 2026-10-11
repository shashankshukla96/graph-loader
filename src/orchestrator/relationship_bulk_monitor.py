"""Run-scoped finite-bulk monitoring for a stage of relationship loaders."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import logging
import time
from typing import Callable, Mapping, Sequence

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient
from docker.errors import NotFound

from src.loader.control import CONTROL_TOPIC
from src.models.schema import EdgeConfig
from src.orchestrator.coordination import ClockLease, decode_clock_lease
from src.orchestrator.rotation import RotationPlan
from src.orchestrator.bulk_monitor import (
    AssignmentCoverageError, BulkMonitorError, BulkShutdownRequested, BulkTimeoutError, LoaderCrashError,
    OffsetResolutionError, QuiescentViolationError,
)
from src.utils import metrics as metrics_module
from src.utils.consumer_groups import edge_group, stream_retry_group

logger = logging.getLogger(__name__)

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
    # P6-S2 review finding S8-1: poll_interval_seconds defaults to 0.25s
    # (4 Hz) in production (none of cli.py's RelationshipBulkMonitor
    # constructions override it), so the consumer-lag refresh must be
    # throttled on its own clock, independent of the supervision tick
    # rate - refreshing on every tick would issue ~150 admin/metadata RPCs
    # per second for a 3-edge-type x 12-partition fleet, purely for one
    # gauge, and could stall the same loop's crash/lease-health checks
    # for up to _request_timeout()'s 10s per partition if the broker were
    # slow.
    _LAG_REFRESH_INTERVAL_SECONDS = 10.0
    _LAG_REFRESH_TIMEOUT_SECONDS = 2.0

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
        metrics: "metrics_module.GraphLoaderMetrics | None" = None,
        tracked_lease_server: object | None = None,
        bucket_lease_config: object | None = None,
        docker_service: object | None = None,
        lease_bucket_count: int | None = None,
        lease_keyed_labels: Sequence[str] = (),
        lease_network: str = "graph-loader-net",
        abort_requested: Callable[[], bool] | None = None,
        tracked_retry_consumers: Sequence[object] = (),
        mode: str = "bulk",
    ) -> None:
        if mode not in ("bulk", "stream"):
            raise BulkMonitorError("relationship monitor mode must be bulk or stream")
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
        # The loaders' own groups (one helper for launcher and monitor): run-scoped
        # in bulk, stable ``<prefix>-<type>`` in stream.
        self._groups = {
            edge_type: edge_group(consumer_group_prefix, edge_type, mode=mode, run_id=run_id)
            for edge_type in self._edges
        }
        self._clock, self._sleep, self._interval = clock, sleep, poll_interval_seconds
        self._admin = admin_client or AdminClient({"bootstrap.servers": bootstrap_servers})
        # This consumer only queries topic watermarks; it never owns work
        # partitions or calls poll in the lifecycle loop.  Keep its local
        # poll watchdog above the maximum supported bulk stage so a valid
        # long rotation cannot emit a misleading MAXPOLL group-leave event.
        self._watermark = watermark_consumer or Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"graph-loader-edge-watermark-{run_id}",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 3_600_000,
        })
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
        # P6-S5 Story 2: polled on every wait iteration (see BulkMonitor).
        self._abort_requested = abort_requested
        self._wall_clock_ms = wall_clock_ms or (lambda: int(time.time() * 1000))
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        # v7-S2: a relationship type may be fed by several topics; the boundary,
        # commit and lag bookkeeping is keyed by (edge_type, topic, partition).
        self._topics_by_edge_type = {edge_type: tuple(edge.topics) for edge_type, edge in self._edges.items()}
        self._next_lag_refresh: float | None = None
        # P5-S1 Story 5: supervised, restartable streaming lease server.
        # Independent of rotation_plan/tracked_clock -- a stream fleet uses
        # exactly one of the two coordination mechanisms, never both.
        self._tracked_lease_server = tracked_lease_server
        self._bucket_lease_config = bucket_lease_config
        self._docker_service = docker_service
        self._lease_bucket_count = lease_bucket_count
        self._lease_keyed_labels = tuple(lease_keyed_labels)
        self._lease_network = lease_network
        # Consecutive restarts (the cap and the backoff); reset once a
        # restarted server stays healthy for restart_count_reset_ms after its
        # quiet period. _lease_generation counts every restart of the run and
        # keeps the per-generation metrics snapshot names distinct.
        self._lease_restart_count = 0
        self._lease_generation = 0
        self._lease_healthy_reset_at: float | None = None
        # P5-S4 owner decision D2: the run's retry-consumer replicas are
        # supervised like edge loaders in stream mode -- any exit stops the
        # fleet (rc 1). Not restarted: a fatal exit (authentication, a missing
        # database) would only crash again.
        self._tracked_retry_consumers = tuple(tracked_retry_consumers)
        # Stable stream groups: the pre-launch member check is best effort (two
        # CLIs starting at once both see empty groups), so supervision checks
        # that no group has more members than this fleet's own replicas.
        self._mode = mode
        self._expected_members: dict[str, int] = {}
        if mode == "stream":
            for edge_type, _replica in expected_replicas:
                group = self._groups[edge_type]
                self._expected_members[group] = self._expected_members.get(group, 0) + 1
            if self._tracked_retry_consumers:
                self._expected_members[stream_retry_group(consumer_group_prefix)] = len(self._tracked_retry_consumers)
        self._next_member_check: float | None = None
        self._member_excess_seen: set[str] = set()
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
        self._last_lows: dict[WorkPartition, int] = {}
        self._boundary_lows: dict[WorkPartition, int] = {}
        self._draining = False
        self._clock_lease: ClockLease | None = None
        self._accepted_clock_epochs: set[int] = set()

    @property
    def clock_lease(self) -> ClockLease | None:
        """Return the newest validated current-run lease for diagnostics only."""
        return self._clock_lease

    @property
    def tracked_lease_server(self) -> object | None:
        """The *current* tracked lease-server container object (P5-S1
        Story 6, review finding M5-1): after a supervised restart, this is
        a different container than whatever the caller originally
        launched, so the caller's own shutdown/failure cleanup must stop
        *this* object, not the one it started with."""
        return self._tracked_lease_server

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
            raw[edge_type] = set()
            for topic_name in edge.topics:
                topic = metadata.topics.get(topic_name)
                if topic is None or getattr(topic, "error", None) or not getattr(topic, "partitions", None):
                    raise OffsetResolutionError(f"relationship topic metadata unavailable edge={edge_type} topic={topic_name}")
                raw[edge_type].update((topic_name, int(partition)) for partition in topic.partitions)
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
                exit_code = state.get("ExitCode") if status != "removed" else None
                log_tail = ""
                if status != "removed":
                    try:
                        raw_logs = container.logs(tail=30, stdout=True, stderr=True)
                        if isinstance(raw_logs, bytes):
                            log_tail = raw_logs.decode("utf-8", errors="replace").strip()[-2000:]
                        elif isinstance(raw_logs, str):
                            log_tail = raw_logs.strip()[-2000:]
                    except Exception:
                        pass
                diagnostic = f" status={status} exit_code={exit_code}"
                if log_tail:
                    diagnostic += f" log_tail={log_tail!r}"
                raise self._failure(
                    LoaderCrashError,
                    f"relationship stage={self._stage()} edge={replica[0]} replica={replica[1]} exited before drain acknowledgement{diagnostic}",
                )

    def _check_retry_consumer_health(self) -> None:
        """Fail the stream fleet when any retry-consumer replica is no longer
        running (owner decision D2). It only ever exits on its own after a
        crash or a fatal, non-healing write error (``RetryConsumerFatalError``);
        either way nothing would retry the DLQ any more, so the fleet stops
        with rc 1 instead of running on with a silently dead retry path."""
        for replica_id, container in enumerate(self._tracked_retry_consumers):
            try:
                container.reload()
                state = getattr(container, "attrs", {}).get("State", {})
                status = state.get("Status", getattr(container, "status", "running"))
            except NotFound:
                status, state = "removed", {}
            except Exception as exc:
                raise LoaderCrashError(
                    f"stage=retry-consumer run_id={self._run_id} replica={replica_id} inspect failed: {exc}"
                ) from exc
            if status in {"running", "created", "restarting"}:
                continue
            exit_code = state.get("ExitCode") if status != "removed" else None
            log_tail = ""
            if status != "removed":
                try:
                    raw_logs = container.logs(tail=30, stdout=True, stderr=True)
                    if isinstance(raw_logs, bytes):
                        log_tail = raw_logs.decode("utf-8", errors="replace").strip()[-2000:]
                    elif isinstance(raw_logs, str):
                        log_tail = raw_logs.strip()[-2000:]
                except Exception:
                    pass
            diagnostic = f" status={status} exit_code={exit_code}"
            if log_tail:
                diagnostic += f" log_tail={log_tail!r}"
            message = (
                f"stage=retry-consumer run_id={self._run_id} replica={replica_id} retry consumer exited "
                f"during stream supervision{diagnostic} -- stopping the stream fleet"
            )
            logger.error(message)
            raise LoaderCrashError(message)

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
            exit_code = state.get("ExitCode") if status != "removed" else None
            log_tail = ""
            if status != "removed":
                try:
                    raw_logs = self._tracked_clock.logs(tail=30, stdout=True, stderr=True)
                    if isinstance(raw_logs, bytes):
                        log_tail = raw_logs.decode("utf-8", errors="replace").strip()[-2000:]
                    elif isinstance(raw_logs, str):
                        log_tail = raw_logs.strip()[-2000:]
                except Exception:
                    pass
            diagnostic = f" exit_code={exit_code}"
            if log_tail:
                diagnostic += f" log_tail={log_tail!r}"
            raise LoaderCrashError(
                f"stage=monitor run_id={self._run_id} clock exited status={status}{diagnostic} {self._clock_context()}"
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
                if self._abort_requested is not None and self._abort_requested():
                    raise BulkShutdownRequested(
                        f"shutdown requested while waiting for relationship stage={self._stage()} {name}"
                    )
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
        self._last_lows = {}
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
            self._last_lows[(edge_type, topic, partition)] = low
        return result

    def capture_boundary(self) -> dict[WorkPartition, int]:
        if not self._coverage():
            raise AssignmentCoverageError("cannot capture relationship boundary before coverage")
        if not self._has_current_clock_lease():
            raise BulkMonitorError(
                f"stage=monitor run_id={self._run_id} cannot capture boundary without a current clock lease"
            )
        self._check_health(); self._boundary = self._watermarks()
        self._boundary_lows = dict(self._last_lows)
        return dict(self._boundary)

    def _assert_quiescent(self) -> None:
        if self._boundary is None:
            raise BulkMonitorError("relationship boundary has not been captured")
        if self._watermarks() != self._boundary:
            raise self._failure(
                QuiescentViolationError,
                f"relationship stage={self._stage()} input changed after boundary",
            )

    def _commits(self, *, timeout: float | None = None) -> dict[WorkPartition, int | None]:
        result: dict[WorkPartition, int | None] = {item: None for item in self._expected}
        for edge_type, raw in self._raw_expected.items():
            request = ConsumerGroupTopicPartitions(self._groups[edge_type], [TopicPartition(topic, partition) for topic, partition in sorted(raw)])
            try:
                response = self._admin.list_consumer_group_offsets(
                    [request], require_stable=True
                )[request.group_id].result(timeout=timeout if timeout is not None else self._request_timeout())
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

    def _pretouch_lag_series(self) -> None:
        """Create one lag series per (type, topic) so stream dashboards read 0, not absent.

        Stream supervision only: bulk stages never refresh the gauge, so they must not export it.
        """
        for edge_type, topics in self._topics_by_edge_type.items():
            for topic in topics:
                metrics_module.safe_emit(lambda edge_type=edge_type, topic=topic: self._metrics.consumer_lag.labels(
                    kind="cli", name=edge_type, topic=topic,
                ))

    def supervise_stream(self, shutdown_requested, *, initial_lease_timeout_seconds: float | None = None) -> None:
        """Keep one exact stream fleet healthy until explicit shutdown.

        Stream supervision deliberately has no watermark/boundary or lag-based
        success/exit path: whether the loop continues never depends on lag.
        The supplied Event-compatible object makes termination deterministic
        for the CLI and tests alike.

        P6-S2 Story 8: this loop also calls ``_refresh_consumer_lag()``,
        purely for observability - a genuinely new admin-API round trip
        (one watermark + one committed-offset request per edge type), not
        a reuse of an RPC this loop already issued, since streaming
        supervision had no such RPC before this story. That method
        throttles itself to once per ``_LAG_REFRESH_INTERVAL_SECONDS``
        (10s) on its own clock, independent of ``self._interval`` (P6-S2
        review finding S8-1: ``self._interval`` defaults to 0.25s in
        production, far too fast for a fleet-wide admin RPC every tick -
        it would both hammer the broker and, since the refresh runs
        synchronously in this same loop, risk stalling the crash/lease-
        health checks below it by up to several seconds per tick under a
        slow broker). Each RPC also uses a short explicit
        ``_LAG_REFRESH_TIMEOUT_SECONDS`` (2s), not ``_request_timeout()``'s
        up-to-10s, to bound that stall further. The whole refresh is
        wrapped in ``_refresh_consumer_lag``'s own broad ``except
        Exception`` so a transient admin-API failure can never raise out
        of this loop and break stream supervision itself.
        """
        if not hasattr(shutdown_requested, "is_set"):
            raise BulkMonitorError("stream supervision requires a shutdown event")
        self._pretouch_lag_series()
        if self._rotation_plan is not None:
            if initial_lease_timeout_seconds is None or initial_lease_timeout_seconds <= 0:
                raise BulkMonitorError("rotation stream supervision requires a positive initial lease timeout")
            try:
                self.wait_for_clock_lease(initial_lease_timeout_seconds)
            except BulkTimeoutError as exc:
                raise self._failure(
                    BulkTimeoutError,
                    "initial stream clock lease timed out",
                ) from exc
        while not shutdown_requested.is_set():
            if self._rotation_plan is not None:
                self._check_clock_health()
            self._check_health()
            self._check_retry_consumer_health()
            self._poll()
            self._refresh_consumer_lag()
            self._check_group_members()
            if self._rotation_plan is not None and not self._has_current_clock_lease():
                raise BulkMonitorError(
                    f"stage=monitor run_id={self._run_id} stream has no current clock lease {self._clock_context()}"
                )
            if self._tracked_lease_server is not None:
                self._check_lease_server_health(shutdown_requested)
            self._sleep(self._interval)

    def wait_for_lease_server_ready(self, timeout_seconds: float, shutdown_requested=None,
                                    client_factory=None) -> None:
        """Wait for the tracked lease server to answer PING (P5-S1 Story 5).

        No Kafka-published lease to decode (unlike the clock path) -- just
        a lightweight liveness probe, reusing Story 3/4's
        ``BucketLeaseClient.ping()``. Review finding B5-1: a *fresh*
        client every attempt, not one reused client -- a client whose
        first connect attempt fails is marked permanently broken
        (``BucketLeaseClient._fail``), so reusing it would make every
        later attempt fail immediately without ever really retrying,
        timing out even once the server comes up. Honors
        ``shutdown_requested`` so SIGTERM during this wait is not blocked
        for the whole timeout.

        Story 10 E2E finding: unlike the address handed to edge-loader
        *containers* (which must resolve by name across a mid-run lease
        server restart, since the replacement container has a new IP),
        this probe runs in the CLI's own host process. A user-defined
        network's embedded DNS only resolves container names from inside
        that network, so a host process cannot reach ``prefer_name=True``'s
        name-based address at all -- confirmed against a real Docker
        network. This call therefore always resolves by IP; there is no
        restart to be transparent to, since this function only runs once,
        before ``supervise_stream`` takes over.
        """
        from src.loader.lease_client import BucketLeaseClient

        address = self._docker_service.bucket_lease_address(
            self._tracked_lease_server, network=self._lease_network, prefer_name=False,
        )
        factory = client_factory or (lambda: BucketLeaseClient(address, connect_timeout_seconds=1.0))
        deadline = self._clock() + timeout_seconds
        while True:
            if shutdown_requested is not None and shutdown_requested.is_set():
                return
            client = factory()
            try:
                if client.ping():
                    return
            except Exception:
                pass
            finally:
                client.close()
            if self._clock() >= deadline:
                raise BulkTimeoutError(
                    f"stage=monitor run_id={self._run_id} lease server did not become ready"
                )
            self._sleep(min(self._interval, deadline - self._clock()))

    def _check_lease_server_health(self, shutdown_requested) -> None:
        """Restart the tracked lease server up to a bounded number of times.

        Restart-and-continue, unlike ``_check_clock_health``'s
        immediately-raise: a lease-server crash is exactly the bounded,
        self-healing blip this story exists to make safe, not a fatal end
        to an unattended ``--mode stream`` run.
        """
        try:
            self._tracked_lease_server.reload()
            state = getattr(self._tracked_lease_server, "attrs", {}).get("State", {})
            status = state.get("Status", getattr(self._tracked_lease_server, "status", "running"))
        except NotFound:
            status = "removed"
        except Exception as exc:
            raise LoaderCrashError(
                f"stage=monitor run_id={self._run_id} lease server inspect failed: {exc}"
            ) from exc
        config = self._bucket_lease_config
        if status in {"running", "created", "restarting"}:
            if (
                status == "running" and self._lease_restart_count
                and self._lease_healthy_reset_at is not None and self._clock() >= self._lease_healthy_reset_at
            ):
                logger.info(
                    "stage=lease-server-restart run_id=%s reason=restart_count_reset healthy_ms=%s "
                    "previous_consecutive_restarts=%s",
                    self._run_id, config.restart_count_reset_ms, self._lease_restart_count,
                )
                self._lease_restart_count = 0
                self._lease_healthy_reset_at = None
            return
        self._lease_restart_count += 1
        self._lease_healthy_reset_at = None
        if self._lease_restart_count > config.max_consecutive_restarts:
            raise LoaderCrashError(
                f"stage=monitor run_id={self._run_id} lease server exceeded "
                f"{config.max_consecutive_restarts} restarts"
            )
        delay_ms = min(
            config.restart_backoff_base_ms * 2 ** (self._lease_restart_count - 1),
            config.restart_backoff_max_ms,
        )
        # Interruptible by shutdown, in increments of self._interval, so a
        # SIGTERM mid-backoff is still honored promptly (never blocked for
        # the whole delay).
        remaining = delay_ms / 1000
        while remaining > 0 and not shutdown_requested.is_set():
            step = min(self._interval, remaining)
            self._sleep(step)
            remaining -= step
        if shutdown_requested.is_set():
            return
        try:
            self._tracked_lease_server.remove(force=True)
        except Exception:
            pass
        self._lease_generation += 1
        self._tracked_lease_server = self._docker_service.run_bucket_lease_server(
            run_id=self._run_id, bucket_count=self._lease_bucket_count, network=self._lease_network,
            keyed_labels=self._lease_keyed_labels,
            quiet_period_seconds=config.restart_quiet_period_ms / 1000,
            fence_seconds=config.restart_quiet_period_ms / 1000,
            restart_attempt=self._lease_generation,
        )
        # Healthy means running through the quiet period Q (no grants yet)
        # and then for restart_count_reset_ms of real service.
        self._lease_healthy_reset_at = (
            self._clock() + (config.restart_quiet_period_ms + config.restart_count_reset_ms) / 1000
        )
        logger.warning(
            "stage=lease-server-restart run_id=%s attempt=%s/%s generation=%s",
            self._run_id, self._lease_restart_count, config.max_consecutive_restarts, self._lease_generation,
        )

    _MEMBER_CHECK_INTERVAL_SECONDS = 10.0

    def _check_group_members(self) -> None:
        """Fail a stream fleet whose stable groups have more members than its own replicas.

        Throttled like the lag refresh. An excess must show on two consecutive
        checks (a member that just closed can still be listed once). A failed
        describe is logged and skipped: this is a guard, not a liveness signal.
        """
        if not self._expected_members:
            return
        now = self._clock()
        if self._next_member_check is not None and now < self._next_member_check:
            return
        self._next_member_check = now + self._MEMBER_CHECK_INTERVAL_SECONDS
        try:
            futures = self._admin.describe_consumer_groups(
                list(self._expected_members), request_timeout=self._LAG_REFRESH_TIMEOUT_SECONDS,
            )
            counts = {
                group: len(getattr(future.result(timeout=self._LAG_REFRESH_TIMEOUT_SECONDS), "members", None) or ())
                for group, future in futures.items()
            }
        except Exception as exc:
            logger.warning("stage=consumer-groups run_id=%s reason=member_check_failed error=%s", self._run_id, exc)
            return
        excess = {group: count for group, count in counts.items() if count > self._expected_members.get(group, 0)}
        confirmed = {group: count for group, count in excess.items() if group in self._member_excess_seen}
        self._member_excess_seen = set(excess)
        if confirmed:
            details = ", ".join(
                f"{group} members={count} expected={self._expected_members[group]}"
                for group, count in sorted(confirmed.items())
            )
            logger.error(
                "stage=consumer-groups run_id=%s reason=foreign_members %s "
                "detail=another fleet with the same schema and loading.consumer_group_id is consuming these groups",
                self._run_id, details,
            )
            raise BulkMonitorError(
                f"stage=consumer-groups run_id={self._run_id} reason=foreign_members {details}: another stream "
                "fleet with the same schema and loading.consumer_group_id is consuming these groups"
            )

    def _watermarks_low_high(self, *, timeout: float) -> dict[WorkPartition, tuple[int, int]]:
        """Like :meth:`_watermarks`, but keeps ``low`` too (S8-2) and takes
        an explicit short ``timeout`` instead of ``_request_timeout()``'s
        up-to-10s (S8-1) - used only by the lag-refresh path so the shared
        :meth:`_watermarks`/:meth:`_commits` contract every other caller
        relies on stays unchanged."""
        result: dict[WorkPartition, tuple[int, int]] = {}
        for edge_type, topic, partition in sorted(self._expected):
            low, high = self._watermark.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=timeout,
            )
            if not isinstance(low, int) or isinstance(low, bool) or not isinstance(high, int) or isinstance(high, bool) or low < 0 or high < low:
                raise OffsetResolutionError(
                    f"invalid watermark edge={edge_type} topic={topic} partition={partition}"
                )
            result[(edge_type, topic, partition)] = (low, high)
        return result

    def _refresh_consumer_lag(self) -> None:
        """Best-effort ``graph_loader_consumer_lag_records`` refresh,
        throttled to once per ``_LAG_REFRESH_INTERVAL_SECONDS`` regardless
        of how often the caller's tick runs (P6-S2 review finding S8-1).

        Never raises: a transient admin-API failure here must never break
        stream supervision. Sums high-watermark-minus-committed across
        every partition of one ``(edge_type, topic)`` pair into a single
        gauge value - never one gauge per partition. A partition with
        nothing committed yet (``None``) falls back to its *low* watermark
        (S8-2, Kafka's own consumer-lag convention) - the earliest offset
        still on the topic, not 0, since an absolute-zero fallback would
        wildly overstate lag on any topic that has aged past retention.
        """
        now = self._clock()
        if self._next_lag_refresh is not None and now < self._next_lag_refresh:
            return
        try:
            watermarks = self._watermarks_low_high(timeout=self._LAG_REFRESH_TIMEOUT_SECONDS)
            commits = self._commits(timeout=self._LAG_REFRESH_TIMEOUT_SECONDS)
        except Exception as exc:
            logger.warning(
                "stage=monitor run_id=%s reason=consumer_lag_refresh_failed error=%s",
                self._run_id, exc,
            )
            # Retry no sooner than the normal interval even after a
            # failure, so a persistently slow/unreachable broker cannot
            # turn this back into a per-tick cost.
            self._next_lag_refresh = now + self._LAG_REFRESH_INTERVAL_SECONDS
            return
        lag_by_key: dict[tuple[str, str], int] = {}
        for (edge_type, topic, _partition), (low, high) in watermarks.items():
            committed = commits.get((edge_type, topic, _partition))
            committed = committed if isinstance(committed, int) and committed >= 0 else low
            # A lagging group's committed offset can legitimately sit below
            # `low` after retention trims it - clip so lag never counts
            # records that no longer exist on the topic (S8-2 follow-up).
            committed = max(committed, low)
            key = (edge_type, topic)
            lag_by_key[key] = lag_by_key.get(key, 0) + max(0, high - committed)
        for (edge_type, topic), lag in lag_by_key.items():
            metrics_module.safe_emit(lambda edge_type=edge_type, topic=topic, lag=lag: self._metrics.consumer_lag.labels(
                kind="cli", name=edge_type, topic=topic,
            ).set(lag))
        self._next_lag_refresh = now + self._LAG_REFRESH_INTERVAL_SECONDS

    def _at_boundary(self) -> bool:
        """Return whether one stable offset snapshot reaches every boundary."""
        if not self._has_current_clock_lease():
            return False
        self._assert_quiescent()
        if not self._coverage() or self._boundary is None:
            return False
        commits = self._commits()
        return self._commits_reach_boundary(commits)

    def _at_rotating_boundary(self) -> bool:
        """Return true only for one live-lease all-edge durable snapshot."""
        if not self._has_current_clock_lease():
            return False
        self._assert_quiescent()
        if not self._coverage() or self._boundary is None:
            return False
        commits = self._commits()
        return self._commits_reach_boundary(commits)

    def _partition_done(self, key: WorkPartition, boundary: int, commit: int | None) -> bool:
        """Committed to the boundary, or empty at the snapshot (``low == high``).

        An empty partition never commits (a never-written one, or one emptied by retention); a
        commit at or below its low watermark (reused run id) is equally "nothing left to read".
        """
        if commit is not None and commit >= boundary:
            return True
        return self._empty_at_snapshot(key, boundary) and (commit is None or commit < boundary)

    def _empty_at_snapshot(self, key: WorkPartition, boundary: int) -> bool:
        """``low == high`` when the boundary was captured (low is the snapshot's, not the live one)."""
        return boundary == self._boundary_lows.get(key, 0)

    def _partition_verified(self, key: WorkPartition, boundary: int, commit: int | None) -> bool:
        """Zero-lag proof: committed exactly to the boundary, or empty at the snapshot with nothing ahead of it."""
        return commit == boundary or (
            self._empty_at_snapshot(key, boundary) and (commit is None or commit < boundary)
        )

    def _commits_reach_boundary(self, commits: Mapping[WorkPartition, int | None]) -> bool:
        """Accept Kafka's absent commit only for a captured empty partition.

        Empty means ``low == high`` at the snapshot: a never-written partition
        (``0 == 0``) or a fully retention-trimmed one. Multi-topic types make
        an empty topic alongside a busy one routine, and such a partition
        never commits, so it must not hold the stage open.
        """
        assert self._boundary is not None
        return all(
            self._partition_done(key, boundary, commits[key])
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
            if self._partition_verified(key, boundary, commits[key]):
                continue
            detail = ""
            if commits[key] is not None and commits[key] > boundary and self._empty_at_snapshot(key, boundary):
                detail = (
                    f" (committed offset {commits[key]} is above the high watermark {boundary} of an empty "
                    "partition: topic recreated under a reused run id?)"
                )
            raise self._failure(
                OffsetResolutionError,
                f"relationship stage={self._stage()} nonzero lag edge={key[0]} topic={key[1]} partition={key[2]}{detail}",
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
