"""
src/neo4j_graph_loader/cli.py
──────────
Command-line interface for the Neo4j Graph Loader.
"""
import argparse
import hashlib
import json
import sys
import logging
import os
import re
import signal
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from pathlib import Path
from threading import Condition, Event, Lock, Thread


from neo4j_graph_loader.utils.schema_loader import load_schema, SchemaLoadError
from neo4j_graph_loader._runtime import prepare_run, timing_directory, timing_kwargs
from neo4j_graph_loader.utils.logging_config import configure_logging, set_log_context, strip_log_format, structured
from neo4j_graph_loader.status import StatusTracker, model as status_model
from neo4j_graph_loader.status.telemetry import TelemetryPoller
from neo4j_graph_loader.orchestrator.schema_initializer import (
    get_neo4j_driver,
    apply_schema,
    SchemaInitializationError
)
from neo4j_graph_loader.utils import metrics as metrics_module

logger = logging.getLogger(__name__)

_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
BULK_EDGE_OVERLAP_MODES = ("labels", "shared")
_MAX_PARALLEL_CONTAINER_STOPS = 64
_MAX_PARALLEL_LABEL_LAUNCHES = 16
# Docker's default 10s stop grace is measured to the daemon finishing its
# network/container teardown, which it serializes: 32 concurrent stops of
# trivial bridge-networked containers took up to 9.8s. Give drained node
# loaders ample grace so SIGKILL never races a slow daemon.
_NODE_STOP_TIMEOUT_SECONDS = 30
# Bucket-lease mode: B buckets per label; one outer edge flush is split into
# up to B*B cells, so the flush must be large enough for sizeable cell
# transactions (32000 / 16^2 = 125 rows for two large labels, capped by
# ``mix_and_batch.batch_size``; hub cells such as Title->Genre fill it).
# P5-S1 Story 1: renamed with a ``bucket_lease_``/``BUCKET_LEASE`` prefix so
# "lease" unambiguously means this TCP coordinator, never
# ``CoordinationConfig.lease_timeout_ms`` (the unrelated Phase 4 clock slot
# lease). The old names stay as deprecated aliases for one release (this
# codebase's first deprecation convention: a log line, not a machine-enforced
# sunset - see ``_warn_deprecated_flag``).
DEFAULT_BUCKET_LEASE_BUCKETS = 16
DEFAULT_BUCKET_LEASE_OUTER_BATCH_SIZE = 32_000
# Labels with at most this many node records get one lease resource per node
# key instead of sharing B hash buckets: hub nodes such as IMDb's 'Drama' and
# 'Comedy' then never block each other merely by hashing together.
DEFAULT_BUCKET_LEASE_KEYED_MAX_NODES = 1_000
_LEASE_SERVER_STOP_TIMEOUT_SECONDS = 10
# Deprecated aliases (scheduled for removal in a later release): every
# external importer of the old names keeps working unchanged.
DEFAULT_LEASE_BUCKETS = DEFAULT_BUCKET_LEASE_BUCKETS
DEFAULT_LEASE_OUTER_BATCH_SIZE = DEFAULT_BUCKET_LEASE_OUTER_BATCH_SIZE
DEFAULT_LEASE_KEYED_MAX_NODES = DEFAULT_BUCKET_LEASE_KEYED_MAX_NODES

_deprecated_flags_logged: set[str] = set()


def _warn_deprecated_flag(old: str, new: str) -> None:
    """Log once per process that ``old`` is superseded by ``new``.

    P5-S1 Story 1 establishes this codebase's first deprecation convention.
    Called at argument-merge time only (never in a hot loop), and only once
    per distinct ``old`` flag per process even if merge logic runs more than
    once in a test.
    """
    if old in _deprecated_flags_logged:
        return
    _deprecated_flags_logged.add(old)
    logger.warning("stage=deprecation flag=%s superseded_by=%s", old, new)


def _nonnegative_int(value: str) -> int:
    """Parse an argparse integer that may be zero but never negative."""
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a nonnegative integer")
    return parsed


def _positive_float(value: str) -> float:
    """Parse an argparse number of seconds that must be positive and finite."""
    parsed = float(value)
    if not 0 < parsed < float("inf"):
        raise argparse.ArgumentTypeError("must be a positive number")
    return parsed


def _validated_run_id(value: object | None) -> str | None:
    """Accept an optional container/Kafka-safe explicit run identity."""
    if value is None:
        return None
    if not isinstance(value, str) or not _RUN_ID.fullmatch(value):
        raise ValueError("run id must be 1-128 letters, digits, dot, underscore, or hyphen")
    return value


def get_neo4j_credentials() -> tuple[str, str, str]:
    """Return Neo4j credentials, preferring the documented username variable."""
    return (
        os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        os.environ.get("NEO4J_USERNAME", os.environ.get("NEO4J_USER", "neo4j")),
        os.environ.get("NEO4J_PASSWORD", "changeme"),
    )


from neo4j_graph_loader.orchestrator.docker_service import DockerService
from neo4j_graph_loader.orchestrator.runtime import (
    DEFAULT_RUNTIME_BACKEND, REJECTION_DIR, RUNTIME_BACKENDS, RUNTIME_ENV, HandleGone, Runtime,
    wait_until_stop_signal_ready,
)
from neo4j_graph_loader.orchestrator.bulk_monitor import BulkMonitor, BulkShutdownRequested
from neo4j_graph_loader.orchestrator.heartbeat import beat as _beat
from neo4j_graph_loader.orchestrator.dependency_manager import (
    build_conflict_families,
    build_relationship_conflict_plan,
)
from neo4j_graph_loader.orchestrator.fleet_contract import select_fleet_edges
from neo4j_graph_loader.orchestrator.rotation import build_rotation_plan
from neo4j_graph_loader.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor, set_lag_listener
from neo4j_graph_loader.loader.dead_letter import ensure_dead_letter_topic, ensure_parked_pair_index_topic
from neo4j_graph_loader.models.schema import (
    EdgeConfig, dead_letter_topic_name, edge_dead_letter_topic_name, edge_parked_pair_index_topic_name,
    edge_parked_topic_name, parked_topic_name, require_no_precedence_in_stream, schema_data_topics,
)
from neo4j_graph_loader.orchestrator.isolation import build_relationship_isolation_plan
from neo4j_graph_loader.loader.fenced_tx import require_fenced_budget
from neo4j_graph_loader.orchestrator.stream_groups import StreamGroup, StreamGroupWaitStopped, prepare_stream_groups
from neo4j_graph_loader.utils.consumer_groups import (
    bulk_edge_group, legacy_retry_group, legacy_stream_edge_group, stream_edge_group, stream_retry_group,
)
from neo4j_graph_loader.orchestrator.planned_edges import (
    replace_stale_chunk_topic, run_planned_edge_phase, start_kafka_edge_planner,
)
from neo4j_graph_loader.orchestrator import auto_settings

# ``--edge-plan`` without a file: the library plans from the schema's edge topics.
AUTO_EDGE_PLAN = "auto"


# P6-S5 Story 1/2: the one CLI-wide shutdown Event.  ``_handle_start``
# installs SIGINT/SIGTERM handlers that set it (bulk and stream alike) and
# publishes it here so every nested phase - which builds its own local
# handlers for direct callers - shares the same Event instead of missing a
# signal that arrived before its own handler was installed.  One ``start``
# per process; ``_handle_start`` clears it on exit.
_cli_shutdown: Event | None = None
_cli_execution: "StatusTracker | None" = None      # the run's status tracker while the shutdown handlers are installed


def _shutdown_event() -> Event:
    """The CLI-wide shutdown Event, or a fresh one for a direct caller."""
    return _cli_shutdown if _cli_shutdown is not None else Event()


def _shutdown_requested() -> bool:
    return _cli_shutdown is not None and _cli_shutdown.is_set()


def _stop_handler(shutdown: Event, execution: StatusTracker | None):
    def handler(*_args) -> None:
        shutdown.set()
        if execution is not None:
            execution.request_stop()          # a flag only: no lock is taken inside a signal handler
    return handler


def _install_shutdown_handlers(cleanups: list, execution: StatusTracker | None = None) -> Event:
    """Route SIGINT/SIGTERM to one Event; restore the prior handlers as a cleanup.

    The handler only sets the Event (idempotent, re-entrancy safe: a second
    signal during cleanup changes nothing and cannot interrupt a remaining
    container stop).  Off the main thread ``signal.signal`` is unavailable;
    the Event still exists, it is just never set by a signal.
    """
    global _cli_shutdown, _cli_execution
    shutdown = Event()
    _cli_shutdown = shutdown
    _cli_execution = execution
    installed: list[tuple[int, object]] = []
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            installed.append((signum, signal.signal(signum, _stop_handler(shutdown, execution))))
    except ValueError:  # not the main thread
        pass

    def restore() -> None:
        global _cli_shutdown, _cli_execution
        for signum, previous in reversed(installed):
            signal.signal(signum, previous)
        if _cli_shutdown is shutdown:
            _cli_shutdown = None
            _cli_execution = None

    cleanups.append(restore)
    return shutdown


def _is_running(container: object) -> bool:
    """Return whether an exact tracked Docker container still needs a stop attempt."""
    try:
        container.reload()
    except HandleGone:
        return False
    except Exception:
        return True
    state = getattr(container, "attrs", {}).get("State", {})
    status = state.get("Status", getattr(container, "status", ""))
    return status in {"running", "created", "restarting"}


# F2/F1c: the CLI stops each container at most once. Overlapping cleanups (the
# fleet's success/failure/finally sweeps and the stream node cleanup all name
# the same node containers) used to issue a second ``container.stop()`` while
# the first was still draining, delivering a second SIGTERM (exit 143) -- and a
# stopped container must not be reported by the node-loader watcher.
_stop_registry_lock = Lock()
_stop_locks: dict[object, Lock] = {}
_stop_initiated: dict[object, object] = {}  # key -> container (strong ref: keeps id() keys unique)
_stop_done: set[object] = set()


def _container_key(container: object) -> object:
    cid = getattr(container, "id", None)
    return cid if isinstance(cid, str) and cid else id(container)


def _stop_initiated_by_cli(container: object) -> bool:
    """Whether the CLI has started (or finished) stopping this exact container."""
    with _stop_registry_lock:
        return _container_key(container) in _stop_initiated


def _stop_container_once(container: object, stop_timeout: int | None) -> None:
    """Stop one container unless a previous/concurrent call already did.

    A concurrent caller waits for the first ``stop()`` to return and then
    skips its own; a failed stop is not recorded, so the failure retry works.
    """
    key = _container_key(container)
    with _stop_registry_lock:
        lock = _stop_locks.setdefault(key, Lock())
        _stop_initiated[key] = container
    with lock:
        if key in _stop_done:
            return
        # F1: a container launched moments ago may not receive the one SIGTERM
        # ``docker stop`` sends; hold the stop until it can (docker_service).
        wait_until_stop_signal_ready(container)
        if stop_timeout is None:
            container.stop()
        else:
            container.stop(timeout=stop_timeout)
        _stop_done.add(key)


def _stop_exact_containers(
    containers: Iterable[object], *, identities: dict[int, str] | None = None,
    stop_timeout: int | None = None,
) -> list[str]:
    """Stop only supplied containers, retrying a failed exact target once if live.

    ``container.stop()`` blocks until the loader exits (SIGTERM, then SIGKILL
    after Docker's grace period).  Stops are issued concurrently so one
    label's replicas all receive SIGTERM together instead of each waiting
    for the previous replica's graceful drain and consumer-group departure.
    """
    identities = identities or {}
    targets = list(containers)

    def stop_once(container: object) -> None:
        _stop_container_once(container, stop_timeout)

    def stop(container: object) -> Exception | None:
        try:
            stop_once(container)
        except Exception as exc:
            return exc
        return None

    if len(targets) <= 1:
        errors = [stop(container) for container in targets]
    else:
        with ThreadPoolExecutor(
            max_workers=min(len(targets), _MAX_PARALLEL_CONTAINER_STOPS),
            thread_name_prefix="container-stop",
        ) as executor:
            errors = list(executor.map(stop, targets))
    failures = [(container, error) for container, error in zip(targets, errors) if error is not None]

    unresolved: list[str] = []
    for container, first_error in failures:
        if not _is_running(container):
            continue
        try:
            stop_once(container)
        except Exception as retry_error:
            if _is_running(container):
                target = identities.get(id(container), getattr(container, "name", "<unknown>"))
                unresolved.append(f"{target}: {retry_error}")
            continue
        if _is_running(container):
            target = identities.get(id(container), getattr(container, "name", "<unknown>"))
            unresolved.append(f"{target}: {first_error}")
    return unresolved


def _log_failed_loader_diagnostics(containers: Iterable[object]) -> None:
    """Log the tail of every exact loader that exited nonzero, before removal."""
    for container in containers:
        try:
            container.reload()
            state = container.attrs.get("State", {})
            exit_code = state.get("ExitCode")
            if state.get("Status") != "exited" or not isinstance(exit_code, int) or exit_code == 0:
                continue
            tail = container.logs(tail=40)
            if isinstance(tail, bytes):
                tail = tail.decode("utf-8", errors="replace")
            logger.error(
                "stage=diagnostics container=%s exit_code=%s last log lines:\n%s",
                getattr(container, "name", "<unknown>"), exit_code, tail,
            )
        except Exception:
            continue


class _NodeLoaderWatcher:
    """Stream supervision of the node loaders (the edge fleet has its own monitor).

    A node loader that is no longer running while the run was not asked to
    stop is a failure: log it, record it, and set the shared shutdown event
    so the edge fleet drains exactly like an operator stop.  The caller turns
    ``failed`` into rc 1.  Stream node containers are auto-removed, so a dead
    one has no exit code or log left to read.
    """

    def __init__(self, containers: Iterable[object], shutdown: Event, *, interval: float = 1.0,
                 on_exit: Callable[[object], None] | None = None) -> None:
        self._containers = tuple(containers)
        self._shutdown = shutdown
        self._interval = interval
        self._on_exit = on_exit             # E15: told which container died, so the run status can name the failed role
        self._done = Event()
        self.failed = False
        self._thread = Thread(target=self._run, name="node-loader-watch", daemon=True)

    def start(self) -> "_NodeLoaderWatcher":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._done.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._done.wait(self._interval):
            if self._shutdown.is_set():
                return
            for container in self._containers:
                if self._done.is_set() or self._shutdown.is_set():
                    return
                # Check the registry AFTER the reload inside _is_running: a CLI stop that began
                # between the two would otherwise be reported as a crash. A container the CLI
                # itself is stopping is not a failure.
                if not _is_running(container) and not _stop_initiated_by_cli(container):
                    name = getattr(container, "name", "<unknown>")
                    logger.error(
                        "stage=node-loader-supervision container=%s reason=node loader exited while the stream "
                        "run was active; stopping the run (rc 1)", name,
                    )
                    self.failed = True
                    if self._on_exit is not None:
                        try:
                            self._on_exit(container)
                        except Exception:                              # noqa: BLE001 - status never changes a stop
                            pass
                    self._shutdown.set()
                    return


def _container_exit_recorder(args) -> Callable[[object], None] | None:
    """For the Docker runtime: record in the run status that a supervised container is gone (its exit code is not available, the
    stream's node loaders are auto-removed), so the role shows ``failed`` and the execution says why. ``None`` when nothing records."""
    execution = _execution_of(args)
    if execution is None or _runtime_backend(args) != "docker":
        return None

    def record(container: object) -> None:
        service = getattr(args, "_docker_service", None)
        for incarnation, handle in (service.status_handles() if service is not None else {}).items():
            if handle is container or (getattr(handle, "id", None) is not None and getattr(handle, "id", None) == getattr(container, "id", None)):
                execution.role_exited(incarnation, None, "exited while the run was active",
                                      error={"type": "RoleExit", "message": "the container is gone (auto-removed), so its exit code is unavailable"})
                return
    return record


def _supervise_node_loaders_only(containers: Iterable[object], shutdown: Event, cleanups: list, *, interval: float = 1.0,
                                 on_exit: Callable[[object], None] | None = None) -> int:
    """A stream schema without relationship types, ``--runtime process`` (decision D14-5).

    Docker mode launches the node loaders and returns, leaving the containers running. Process-mode children are tied to the
    command (control-socket lifeline), so returning would end them silently with exit 0. Instead the command stays up as their
    supervisor, like every other stream shape: a stop request (SIGTERM, SIGINT, ``stop``) ends them in order and exits 0;
    a node loader that dies makes it stop the rest and exit 1.
    """
    from neo4j_graph_loader.orchestrator import heartbeat

    started = tuple(containers)
    cleanups.append(lambda: (
        _stop_exact_containers(started, stop_timeout=_NODE_STOP_TIMEOUT_SECONDS),
        _remove_exact_containers(started),
    ))
    watcher = _NodeLoaderWatcher(started, shutdown, on_exit=on_exit).start()
    logger.info("stage=supervise reason=stream schema without relationship types: supervising %s node loader(s) until stopped", len(started))
    try:
        while not shutdown.wait(interval):
            heartbeat.beat()
    finally:
        watcher.stop()
    return 1 if watcher.failed else 0


def _lease_launch_kwargs(args: argparse.Namespace) -> dict:
    """Return bucket-lease launch settings for an edge loader, if enabled."""
    address = getattr(args, "lease_address", None)
    if not address:
        return {}
    return {
        "lease_address": address,
        "lease_buckets": getattr(args, "lease_buckets", DEFAULT_BUCKET_LEASE_BUCKETS),
        "outer_batch_size": getattr(args, "lease_outer_batch_size", DEFAULT_BUCKET_LEASE_OUTER_BATCH_SIZE),
        "lease_keyed_labels": tuple(getattr(args, "lease_keyed_labels", ())),
    }


def _node_topic_record_counts(bootstrap_servers: str, topics: Iterable[str]) -> dict[str, int]:
    """Return each topic's retained record count (high minus low watermarks)."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers, "group.id": f"graph-loader-lease-count-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    try:
        counts: dict[str, int] = {}
        metadata = consumer.list_topics(timeout=10)
        for topic in topics:
            partitions = metadata.topics.get(topic)
            if partitions is None or partitions.error is not None:
                raise RuntimeError(f"topic {topic} metadata unavailable")
            total = 0
            for partition in partitions.partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
                total += max(0, high - low)
            counts[topic] = total
        return counts
    finally:
        consumer.close()


def _select_keyed_lease_labels(schema, args: argparse.Namespace) -> tuple[str, ...]:
    """Pick endpoint labels small enough to lease per node key.

    Any choice is safe as long as every loader of the run uses the same one
    (the coordinator enforces that); counting failures just disable keying.
    Retained topic records over-count nodes (updates, duplicates), which
    only makes keying more conservative.
    """
    limit = getattr(args, "lease_keyed_max_nodes", DEFAULT_BUCKET_LEASE_KEYED_MAX_NODES)
    if not limit:
        return ()
    # v7-S7: every label of every pair (the union for a multi-pair type).
    endpoint_labels = {label for edge in getattr(schema, "edges", ()) for label in edge.endpoint_labels}
    topics = {node.label: node.topic for node in schema.nodes if node.label in endpoint_labels}
    if not topics:
        return ()
    try:
        counts = _node_topic_record_counts(
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), sorted(set(topics.values())),
        )
    except Exception as exc:
        logger.warning("stage=lease-server keyed labels disabled: node topic sizes unavailable: %s", exc)
        return ()
    return tuple(sorted(label for label, topic in topics.items() if 0 < counts.get(topic, limit + 1) <= limit))


def _bucket_lease_fence_seconds(schema) -> float:
    """T + margin (review finding B4-1/B4-2): how long the lease server
    holds a disconnected owner's leases after the disconnect.
    Applies to bulk lease mode too -- a mid-run disconnect is not
    restart-specific. Defensive against a test double/partial schema
    that lacks ``loading.bucket_lease`` -- falls back to 0.0 (today's
    immediate-release behavior) rather than raising.
    """
    bucket_lease = getattr(getattr(schema, "loading", None), "bucket_lease", None)
    if bucket_lease is None:
        return 0.0
    return (bucket_lease.tx_timeout_ms + bucket_lease.restart_quiet_margin_ms) / 1000


def _start_bucket_lease_server(docker_service: "DockerService", args: argparse.Namespace, *, run_id: str,
                               schema=None):
    """Launch the run's lease coordinator and record its settings on ``args``."""
    args.lease_keyed_labels = _select_keyed_lease_labels(schema, args) if schema is not None else ()
    container = docker_service.run_bucket_lease_server(
        run_id=run_id, bucket_count=args.lease_buckets, network=args.network,
        keyed_labels=args.lease_keyed_labels, fence_seconds=_bucket_lease_fence_seconds(schema),
    )
    try:
        args.lease_address = docker_service.bucket_lease_address(container, network=args.network)
    except Exception:
        _stop_bucket_lease_server(container, **timing_kwargs(args))
        raise
    settings = {
        "address": args.lease_address, "buckets": args.lease_buckets,
        "keyed_labels": list(args.lease_keyed_labels),
        "outer_batch_size": getattr(args, "lease_outer_batch_size", DEFAULT_BUCKET_LEASE_OUTER_BATCH_SIZE),
    }
    logger.info(
        "stage=lease-server address=%s buckets=%s keyed_labels=%s outer_batch_size=%s",
        settings["address"], settings["buckets"], ",".join(settings["keyed_labels"]) or "-",
        settings["outer_batch_size"],
    )
    directory = timing_directory(args)
    if directory:
        try:
            (Path(directory) / "lease-settings.json").write_text(json.dumps(settings) + "\n", encoding="utf-8")
        except OSError as exc:
            logger.warning("stage=lease-server could not record settings: %s", exc)
    return container


def _stop_bucket_lease_server(container: object, *, timing_dir: str | None = None) -> None:
    """Stop the coordinator, surface its shutdown stats, keep its log, remove it."""
    failures = _stop_exact_containers([container], stop_timeout=_LEASE_SERVER_STOP_TIMEOUT_SECONDS)
    if failures:
        logger.error("stage=lease-server shutdown failed: %s", "; ".join(failures))
    try:
        logs = container.logs()
        if isinstance(logs, bytes):
            logs = logs.decode("utf-8", errors="replace")
        for line in logs.splitlines():
            if "stage=lease-server-stats" in line:
                logger.info("%s", line.split(": ", 1)[-1])
        directory = timing_dir if timing_dir is not None else os.environ.get("GRAPH_LOADER_TIMING_DIR")
        if directory:
            (Path(directory) / "lease-server.log").write_text(logs, encoding="utf-8")
    except Exception as exc:
        logger.warning("stage=lease-server could not read coordinator log: %s", exc)
    _remove_exact_containers([container])


def _removal_already_in_progress(exc: BaseException) -> bool:
    """True for Docker's 409 "removal of container ... is already in progress"."""
    return "already in progress" in str(exc).lower()


def _remove_exact_containers(containers: Iterable[object]) -> None:
    """Remove only retained run containers after their diagnostics were read."""
    for container in containers:
        try:
            container.remove(force=True)
        except HandleGone:
            pass
        except Exception as exc:
            if _removal_already_in_progress(exc):
                # Auto-remove containers are already being deleted by Docker.
                logger.debug("stage=shutdown container removal already in progress: %s", exc)
                continue
            logger.error("stage=shutdown container cleanup failed: %s", exc)


def _run_relationship_bulk_stages(schema, docker_service: Runtime, args: argparse.Namespace) -> int:
    """Run deterministic, exact-container relationship bulk stages."""
    edges = tuple(getattr(schema, "edges", ()))
    plan = build_relationship_conflict_plan(edges)
    run_id = uuid.uuid4().hex
    timeout = args.bulk_timeout_seconds
    edges_by_type = {edge.type: edge for edge in edges}
    for stage in plan.stages:
        stage_containers: list[object] = []
        tracked: dict[tuple[str, str], object] = {}
        monitor = None
        try:
            logger.info("Starting relationship bulk stage=%s edges=%s", stage.index, ",".join(stage.edge_types))
            for edge_type in stage.edge_types:
                edge = edges_by_type[edge_type]
                containers = docker_service.run_edge_loader(
                    edge_type=edge.type, topics=edge.topics, mode="bulk", config_path=args.config,
                    replicas=edge.replicas, network=args.network, run_id=run_id,
                    consumer_group_prefix=schema.loading.consumer_group_id,
                )
                stage_containers.extend(containers)
                if len(containers) != edge.replicas:
                    raise RuntimeError(f"relationship stage={stage.index} edge={edge.type} launch returned {len(containers)} replicas; expected {edge.replicas}")
                if len({id(container) for container in containers}) != len(containers):
                    raise RuntimeError(
                        f"relationship stage={stage.index} edge={edge.type} launch returned duplicate container objects"
                    )
                for replica_id, container in enumerate(containers):
                    if container in tracked.values():
                        raise RuntimeError(
                            f"relationship stage={stage.index} edge={edge.type} launch reused a container from another edge"
                        )
                    tracked[(edge.type, str(replica_id))] = container
            expected = {
                (edge_type, str(replica_id))
                for edge_type in stage.edge_types
                for replica_id in range(edges_by_type[edge_type].replicas)
            }
            if set(tracked) != expected:
                raise RuntimeError(f"relationship stage={stage.index} replica accounting mismatch")
            monitor = RelationshipBulkMonitor(
                [edges_by_type[edge_type] for edge_type in stage.edge_types], run_id, expected, tracked,
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                consumer_group_prefix=schema.loading.consumer_group_id,
                abort_requested=_shutdown_requested,
            )
            monitor.wait_for_assignment_coverage(timeout)
            monitor.capture_boundary()
            monitor.wait_for_completion(timeout)
            failures = _stop_exact_containers(stage_containers)
            if failures:
                raise RuntimeError(f"relationship stage={stage.index} shutdown failed: {'; '.join(failures)}")
            monitor.wait_for_drain_complete(timeout)
            monitor.verify_zero_lag()
        except Exception as exc:
            logger.error("Relationship bulk stage=%s edges=%s failed: %s", stage.index, ",".join(stage.edge_types), exc)
            _stop_exact_containers(stage_containers)
            return 1
        finally:
            if monitor is not None:
                monitor.close()
            _stop_exact_containers(stage_containers)
            _remove_exact_containers(stage_containers)
    return 0


def _has_self_reference(edge) -> bool | None:
    """Whether a relationship type touches one label at both ends (v7-S7: in ANY of its label pairs).

    Single-pair types (and lightweight edge-likes) keep today's ``nodes.is_self_referencing``."""
    if getattr(edge, "is_multi_pair", False) is True:
        return edge.has_self_reference
    return getattr(getattr(edge, "nodes", None), "is_self_referencing", None)


def _endpoint_labels_of(edge) -> frozenset[str]:
    """Every node label a relationship type touches (v7-S7: the union over its label pairs)."""
    if getattr(edge, "is_multi_pair", False) is True:
        return frozenset(edge.endpoint_labels)
    return frozenset((edge.nodes.source, edge.nodes.target))


def _refuse_multi_pair(edges, path: str) -> None:
    """Deprecated Phase 4 paths hard-code one label pair per type (v7-S7)."""
    multi = sorted(edge.type for edge in edges if getattr(edge, "is_multi_pair", False) is True)
    if multi:
        raise ValueError(
            f"edge type(s) {multi} bind several label pairs; {path} (deprecated) supports one pair per type; "
            "use the default lease fleet"
        )


def _run_bulk_relationship_edge(
    edge: EdgeConfig,
    docker_service: Runtime,
    args: argparse.Namespace,
    *,
    run_id: str,
    consumer_group_prefix: str,
    phase_index: int | None = None,
    cancelled: Event | None = None,
    on_launch: Callable[[str, tuple[object, ...]], None] | None = None,
    self_reference_co_scheduled: bool = False,
    timing_tag: str | None = None,
    boundary_out: dict | None = None,
    parent_edge: EdgeConfig | None = None,
) -> int:
    """Run one edge through its exact finite Kafka boundary and drain proof.

    ``timing_tag`` (a precedence sub-stage, ``t<rank>``) keeps the sub-stage's timing and metric
    files apart; ``boundary_out`` receives the captured boundary ``{(type, topic, partition): high}``.

    A self-referencing edge normally runs only as an isolated phase
    (``phase_index``); ``self_reference_co_scheduled`` instead admits it to
    the shared completion-driven scheduler (``--bulk-edge-overlap shared``).
    """
    edge_type = getattr(edge, "type", "<unknown>")
    context = (
        f"stage=bulk-edge run_id={run_id} edge={edge_type}"
        if phase_index is None else
        f"stage=isolation run_id={run_id} phase={phase_index} edge={edge_type}"
    )
    try:
        if _validated_run_id(run_id) is None:
            raise ValueError("run id must be nonblank")
        if phase_index is not None and (isinstance(phase_index, bool) or not isinstance(phase_index, int) or phase_index < 0):
            raise ValueError("phase must be a nonnegative integer")
        if not isinstance(consumer_group_prefix, str) or not consumer_group_prefix.strip():
            raise ValueError("consumer group prefix must be nonblank")
        if self_reference_co_scheduled and phase_index is not None:
            raise ValueError("co-scheduled self-reference cannot also be an isolated phase")
        # A precedence sub-stage is checked against its parent type: a topic view of a
        # multi-pair type may bind a pair that is not the self-referencing one (v7-S7).
        if _has_self_reference(parent_edge or edge) is not (phase_index is not None or self_reference_co_scheduled):
            raise ValueError("edge self-reference does not match bulk phase")
    except (AttributeError, ValueError) as exc:
        logger.error("%s failed: %s", context, exc)
        return 1

    if getattr(edge, "conflict_rule", None) == "precedence" and len(edge.topics) > 1:
        return _run_precedence_substages(
            edge, docker_service, args, run_id=run_id, consumer_group_prefix=consumer_group_prefix,
            phase_index=phase_index, cancelled=cancelled, on_launch=on_launch,
            self_reference_co_scheduled=self_reference_co_scheduled,
        )

    containers: list[object] = []
    tracked: dict[tuple[str, str], object] = {}
    monitor = None
    try:
        if cancelled is not None and cancelled.is_set():
            return 1
        returned = docker_service.run_edge_loader(
            edge_type=edge.type,
            topics=edge.topics,
            mode="bulk",
            config_path=args.config,
            replicas=edge.replicas,
            network=args.network,
            run_id=run_id,
            consumer_group_prefix=consumer_group_prefix,
            slot_gating=False,
            **({"timing_tag": timing_tag} if timing_tag else {}),
            **_lease_launch_kwargs(args),
        )
        containers.extend(returned)
        if on_launch is not None:
            on_launch(edge.type, tuple(returned))
        if cancelled is not None and cancelled.is_set():
            raise RuntimeError("another bulk relationship edge failed")
        if len(returned) != edge.replicas:
            raise RuntimeError(
                f"launch returned {len(returned)} replicas; expected {edge.replicas}"
            )
        if len({id(container) for container in returned}) != len(returned):
            raise RuntimeError("launch returned duplicate container objects")
        tracked = {(edge.type, str(replica_id)): container for replica_id, container in enumerate(returned)}
        expected = {(edge.type, str(replica_id)) for replica_id in range(edge.replicas)}
        if set(tracked) != expected:
            raise RuntimeError("replica accounting mismatch")
        monitor = RelationshipBulkMonitor(
            [edge], run_id, expected, tracked,
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            consumer_group_prefix=consumer_group_prefix,
            abort_requested=_shutdown_requested,
        )
        timeout = args.bulk_timeout_seconds
        monitor.wait_for_assignment_coverage(timeout)
        captured = monitor.capture_boundary()
        if boundary_out is not None:
            boundary_out.update(captured)
        monitor.wait_for_completion(timeout)
        identities = {
            id(container): f"edge={replica_edge_type} replica={replica_id}"
            for (replica_edge_type, replica_id), container in tracked.items()
        }
        failures = _stop_exact_containers(containers, identities=identities)
        if failures:
            raise RuntimeError(f"shutdown failed: {'; '.join(failures)}")
        monitor.wait_for_drain_complete(timeout)
        monitor.verify_zero_lag()
        return 0
    except Exception as exc:
        # N2: an operator stop interrupting the monitor's wait is the requested
        # outcome (still rc 1, still cleaned up below), not a failure.
        if isinstance(exc, BulkShutdownRequested) and _shutdown_requested():
            logger.warning("%s stopped by operator: %s", context, exc)
        else:
            logger.error("%s failed: %s", context, exc)
        failures = _stop_exact_containers(
            containers,
            identities={
                id(container): f"edge={replica_edge_type} replica={replica_id}"
                for (replica_edge_type, replica_id), container in tracked.items()
            },
        )
        if failures:
            logger.error("%s cleanup failed: %s", context, "; ".join(failures))
        return 1
    finally:
        if monitor is not None:
            monitor.close()
        _stop_exact_containers(
            containers,
            identities={
                id(container): f"edge={replica_edge_type} replica={replica_id}"
                for (replica_edge_type, replica_id), container in tracked.items()
            },
        )
        _remove_exact_containers(containers)


def _precedence_substage_plan(edge: EdgeConfig, run_id: str) -> list[tuple[int, str, str, EdgeConfig]]:
    """Plan the sequential sub-stages of a ``topic_conflict: precedence`` type.

    Returns ``(rank, topic, sub_run_id, sub_edge)`` ordered by ``edge.precedence_rank`` ascending
    (lowest rank first, so the later-listed topic is written last and wins). Each sub-edge reads a
    single topic; its run id (hence consumer group ``{prefix}-{type}-{sub_run_id}``, control events
    and drain proof) is distinct per topic, so every sub-stage has its own boundary, completion and
    zero-lag check. Replicas stay ``edge.replicas``.
    """
    ordered = sorted(edge.topics, key=edge.precedence_rank)
    return [
        (
            edge.precedence_rank(topic), topic,
            _derive_relationship_phase_run_id(run_id, phase_kind="topic", phase_index=index, edge_type=topic),
            _single_topic_sub_edge(edge, topic),
        )
        for index, topic in enumerate(ordered)
    ]


def _single_topic_sub_edge(edge: EdgeConfig, topic: str) -> EdgeConfig:
    """A one-topic copy of ``edge`` whose DLQ-family names stay the parent's (v7-S4 review S6).

    v7-S7: ``EdgeConfig.topic_view`` (a single-pair view of the topic's label pair; for a
    single-pair type it is exactly S4's pinned ``model_copy``, pinned by
    ``tests/golden/single_pair_multi_topic_compat.json``)."""
    return edge.topic_view(topic)


class PrecedenceInputChanged(RuntimeError):
    """A precedence type's topic received records outside its sub-stage's snapshot."""


def _topic_watermarks(topics: Iterable[str]) -> dict[tuple[str, int], tuple[int, int]]:
    """Return ``{(topic, partition): (low, high)}`` for every partition of ``topics``."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": f"graph-loader-precedence-snapshot-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    try:
        metadata = consumer.list_topics(timeout=10)
        marks: dict[tuple[str, int], tuple[int, int]] = {}
        for topic in topics:
            info = metadata.topics.get(topic)
            if info is None or info.error is not None:
                raise RuntimeError(f"topic {topic} metadata unavailable")
            for partition in info.partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
                marks[(topic, partition)] = (low, high)
        return marks
    finally:
        consumer.close()


def _group_commits(group: str, partitions: Iterable[tuple[str, int]]) -> dict[tuple[str, int], int | None]:
    """Committed offsets of ``group`` (None = none); does not join the group."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": group, "enable.auto.commit": False,
    })
    try:
        wanted = sorted(partitions)
        result = consumer.committed([TopicPartition(t, p) for t, p in wanted], timeout=10)
        return {(tp.topic, tp.partition): (tp.offset if tp.offset >= 0 else None) for tp in result}
    finally:
        consumer.close()


def _check_precedence_rerun(
    edge: EdgeConfig, plan: list[tuple[int, str, str, EdgeConfig]], marks: Mapping[tuple[str, int], tuple[int, int]],
    consumer_group_prefix: str,
) -> None:
    """Refuse a rerun with a reused run id that would write a lower topic after a higher one.

    If sub-stage k still has work (its group is behind the snapshot highs) while a higher-ranked
    sub-stage's group already has commits, running k now would overwrite the winner. A precedence
    type must be rerun with a fresh run id.
    """
    groups = [(topic, bulk_edge_group(consumer_group_prefix, edge.type, sub_run_id)) for _, topic, sub_run_id, _ in plan]
    commits = {
        topic: _group_commits(group, [key for key in marks if key[0] == topic]) for topic, group in groups
    }
    for index, (topic, _group) in enumerate(groups):
        behind = any(
            high > low and (commits[topic].get(key) is None or commits[topic][key] < high)
            for key, (low, high) in marks.items() if key[0] == topic
        )
        if behind and any(v is not None for later, _ in groups[index + 1:] for v in commits[later].values()):
            raise PrecedenceInputChanged(
                f"edge {edge.type}: topic {topic} still has work but a higher-ranked topic already has commits "
                "under this run id; rerun a precedence type with a fresh --run-id"
            )


def _assert_finished_topics_unchanged(
    edge_type: str, finished: Iterable[str], snapshot: Mapping[tuple[str, int], tuple[int, int]],
) -> None:
    """Fail closed if any finished topic got records after the pre-run snapshot."""
    finished = list(finished)
    if not finished:
        return
    now = _topic_watermarks(finished)
    changed = sorted(key for key in now if now[key][1] != snapshot[key][1])
    if changed:
        raise PrecedenceInputChanged(
            f"edge {edge_type}: records were appended to an already loaded topic during the run "
            f"(topic/partition {changed}); they were not loaded"
        )


def _run_precedence_substages(
    edge: EdgeConfig,
    docker_service: Runtime,
    args: argparse.Namespace,
    *,
    run_id: str,
    consumer_group_prefix: str,
    phase_index: int | None = None,
    cancelled: Event | None = None,
    on_launch: Callable[[str, tuple[object, ...]], None] | None = None,
    self_reference_co_scheduled: bool = False,
) -> int:
    """Run a precedence type's topics one after another, lowest rank first (v7-S5).

    Each sub-stage is a full single-topic bulk run (``_run_bulk_relationship_edge``): launch,
    assignment coverage, boundary capture, completion, container stop, DRAIN_COMPLETE and zero-lag
    proof. The next topic starts only after the previous one is proved drained, so for a pair
    present in several topics the highest-ranked topic writes last. A failed sub-stage fails the
    type and no later topic starts. Other edge types are unaffected (the caller's scheduler keeps
    running them concurrently); only the topics of this type are serial.

    Fail closed across sub-stages: every topic's watermarks are snapshotted before sub-stage 0; a
    sub-stage whose captured boundary differs from the snapshot fails, and after each sub-stage
    (and once after the last) every finished topic is re-read and the type fails if any changed.
    """
    plan = _precedence_substage_plan(edge, run_id)
    try:
        snapshot = _topic_watermarks([topic for _, topic, _, _ in plan])
        _check_precedence_rerun(edge, plan, snapshot, consumer_group_prefix)
    except Exception as exc:
        logger.error("stage=bulk-substage edge=%s failed before the first sub-stage: %s", edge.type, exc,
                     extra=structured("bulk-substage", edge=edge.type, state="failed-before-first"))
        return 1
    finished: list[str] = []
    for topic_index, (rank, topic, sub_run_id, sub_edge) in enumerate(plan):
        if cancelled is not None and cancelled.is_set():
            return 1
        if _shutdown_requested():
            logger.warning("stage=bulk-substage edge=%s stopped by operator before topic=%s", edge.type, topic,
                           extra=structured("bulk-substage", edge=edge.type, topic=topic, state="stopped-before"))
            return 1
        started = time.monotonic()
        started_utc = datetime.now(timezone.utc).isoformat()
        logger.info("stage=bulk-substage edge=%s topic=%s rank=%s run_id=%s start", edge.type, topic, rank, sub_run_id,
                    extra=structured("bulk-substage", edge=edge.type, topic=topic, rank=rank, sub_run_id=sub_run_id, state="start"))
        boundary: dict = {}
        result = _record_phase(args, sub_run_id, "topic", topic_index, edge.type, lambda: _run_bulk_relationship_edge(
            sub_edge, docker_service, args, run_id=sub_run_id, consumer_group_prefix=consumer_group_prefix,
            phase_index=phase_index, cancelled=cancelled, on_launch=on_launch,
            self_reference_co_scheduled=self_reference_co_scheduled,
            timing_tag=f"t{rank}", boundary_out=boundary, parent_edge=edge,
        ))
        if result == 0:
            try:
                captured = {(t, p): high for (_type, t, p), high in boundary.items()}
                expected = {key: high for key, (_low, high) in snapshot.items() if key[0] == topic}
                if captured != expected:
                    raise PrecedenceInputChanged(
                        f"edge {edge.type}: topic {topic} boundary differs from the pre-run snapshot "
                        "(records were appended before its sub-stage started)"
                    )
                _assert_finished_topics_unchanged(edge.type, [*finished, topic], snapshot)
            except Exception as exc:
                logger.error("stage=bulk-substage edge=%s topic=%s failed: %s", edge.type, topic, exc,
                             extra=structured("bulk-substage", edge=edge.type, topic=topic, state="failed"))
                result = 1
        finished.append(topic)
        elapsed = time.monotonic() - started
        logger.info(
            "stage=bulk-substage-complete edge=%s topic=%s rank=%s elapsed_s=%.3f outcome=%s",
            edge.type, topic, rank, elapsed, "succeeded" if result == 0 else "failed",
            extra=structured("bulk-substage-complete", edge=edge.type, topic=topic, rank=rank, elapsed_s=round(elapsed, 3),
                             outcome="succeeded" if result == 0 else "failed"),
        )
        _record_bulk_edge_stage_timing(
            edge.type, started_utc, datetime.now(timezone.utc).isoformat(), elapsed, result == 0,
            substage=(rank, topic), **timing_kwargs(args),
        )
        if result:
            if _shutdown_requested():
                logger.warning("stage=bulk-substage edge=%s topic=%s stopped by operator", edge.type, topic,
                               extra=structured("bulk-substage", edge=edge.type, topic=topic, state="stopped"))
            else:
                logger.error("stage=bulk-substage edge=%s topic=%s failed; later topics are not started", edge.type, topic,
                             extra=structured("bulk-substage", edge=edge.type, topic=topic, state="failed-later-topics-not-started"))
            return result
    return 0


def _run_isolated_relationship_bulk_phase(
    edge: EdgeConfig,
    docker_service: Runtime,
    args: argparse.Namespace,
    *,
    run_id: str,
    phase_index: int,
    consumer_group_prefix: str,
) -> int:
    """Prove one exact, clock-free self-reference relationship bulk phase drained."""
    started_monotonic = time.monotonic()
    started_utc = datetime.now(timezone.utc).isoformat()
    succeeded = False
    try:
        result = _run_bulk_relationship_edge(
            edge, docker_service, args, run_id=run_id,
            phase_index=phase_index, consumer_group_prefix=consumer_group_prefix,
        )
        succeeded = result == 0
        return result
    finally:
        elapsed_seconds = time.monotonic() - started_monotonic
        completed_utc = datetime.now(timezone.utc).isoformat()
        logger.info(
            "stage=bulk-edge-complete edge=%s elapsed_s=%.3f outcome=%s",
            edge.type, elapsed_seconds, "succeeded" if succeeded else "failed",
        )
        _record_bulk_edge_stage_timing(
            edge.type, started_utc, completed_utc, elapsed_seconds, succeeded, **timing_kwargs(args),
        )


def _co_scheduled_self_references(
    edges: Iterable[EdgeConfig], args: argparse.Namespace,
) -> frozenset[str]:
    """Return self-referencing bulk edges admitted to the shared scheduler.

    Self-references were isolated for the stream-mode bucket clock, which
    cannot partition one label across both endpoints.  Bulk runs never use
    that clock: each edge type has its own run id, consumer group, monitor
    and drain proof, and the ``python_apoc`` writer locks every endpoint in a
    single global ``elementId`` order (retrying retryable transient errors),
    so co-running with other types adds no lock-order cycle.  Only the opt-in
    ``--bulk-edge-overlap shared`` policy admits them; ``native_disjoint``
    edges have no global lock order and stay isolated.
    """
    if getattr(args, "mode", "bulk") != "bulk" or getattr(args, "bulk_edge_overlap", "labels") != "shared":
        return frozenset()
    return frozenset(
        edge.type for edge in edges
        if _has_self_reference(edge) is True
        and getattr(getattr(edge, "execution", None), "mode", None) == "python_apoc"
    )


def _record_bulk_edge_stage_timing(
    edge_type: str, started_utc: str, completed_utc: str,
    elapsed_seconds: float, succeeded: bool, substage: tuple[int, str] | None = None,
    *, timing_dir: str | None = None,
) -> None:
    """Keep a per-type lifecycle measurement beside Docker write timings.

    A precedence sub-stage (``substage=(rank, topic)``) writes its own ``substage-<type>.t<rank>.json``
    row beside the type's overall row.
    """
    directory = timing_dir if timing_dir is not None else os.environ.get("GRAPH_LOADER_TIMING_DIR")
    if not directory:
        return
    try:
        name = f"stage-{edge_type}.json" if substage is None else f"substage-{edge_type}.t{substage[0]}.json"
        path = Path(directory) / name
        record = {
            "edge_type": edge_type,
            "started_utc": started_utc,
            "completed_utc": completed_utc,
            "elapsed_seconds": elapsed_seconds,
            "succeeded": succeeded,
        }
        if substage is not None:
            record.update(rank=substage[0], topic=substage[1])
        path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    except OSError as exc:
        logger.warning("Could not record bulk edge timing edge=%s: %s", edge_type, exc)


def _run_completion_driven_bulk_edges(
    edges: tuple[EdgeConfig, ...], docker_service: Runtime,
    args: argparse.Namespace, *, parent_run_id: str, consumer_group_prefix: str,
    ready_labels: set[str] | None = None,
    ready_condition: Condition | None = None,
    cancelled: Event | None = None,
    edge_weights: Mapping[str, int] | None = None,
) -> int:
    """Start each edge once its endpoint labels are ready, then prove its drain.

    ``args.bulk_edge_overlap`` selects the overlap policy: ``labels`` (default)
    also reserves both endpoint labels until the edge drains, so conflicting
    types serialize; ``shared`` lets conflicting types run concurrently and
    relies on the writer's globally elementId-ordered ``apoc.lock.nodes``.
    ``args.max_concurrent_bulk_edges`` caps running types (0 = unlimited);
    under a cap, larger ``edge_weights`` start first, then lexical order.
    """
    if not edges:
        return 0
    overlap = getattr(args, "bulk_edge_overlap", "labels")
    cap = getattr(args, "max_concurrent_bulk_edges", 0)
    weights = dict(edge_weights or {})
    # A precedence type runs its topics serially, so under a concurrency cap it should start first.
    for edge in edges:
        if getattr(edge, "conflict_rule", None) == "precedence" and len(edge.topics) > 1:
            weights[edge.type] = max(weights.get(edge.type, 0), 1) * len(edge.topics)
    try:
        plan = build_relationship_conflict_plan(edges)
        if _validated_run_id(parent_run_id) != parent_run_id:
            raise ValueError("parent run id must be valid")
        co_scheduled = _co_scheduled_self_references(edges, args)
        if any(
            _has_self_reference(edge) is not False and edge.type not in co_scheduled
            for edge in edges
        ):
            raise ValueError("shared bulk schedule contains a self-referencing edge")
        if overlap not in BULK_EDGE_OVERLAP_MODES:
            raise ValueError(f"bulk edge overlap must be one of {', '.join(BULK_EDGE_OVERLAP_MODES)}")
        if isinstance(cap, bool) or not isinstance(cap, int) or cap < 0:
            raise ValueError("max concurrent bulk edges must be a nonnegative integer")
    except (AttributeError, ValueError) as exc:
        logger.error("stage=bulk-scheduler parent_run_id=%s failed: %s", parent_run_id, exc)
        return 1

    by_type = {edge.type: edge for edge in edges}
    pending = set(by_type)
    active: dict[object, str] = {}
    started: dict[object, tuple[float, str]] = {}
    phase_ids: dict[object, str] = {}
    launched: dict[str, tuple[object, ...]] = {}
    launch_lock = Lock()
    cancelled = cancelled or Event()
    if ready_labels is None:
        ready_labels = {label for edge in edges for label in _endpoint_labels_of(edge)}
    if ready_condition is None:
        ready_condition = Condition()

    def register(edge_type: str, containers: tuple[object, ...]) -> None:
        with launch_lock:
            launched[edge_type] = containers

    # In labels mode a future only releases its endpoint labels after zero
    # lag and DRAIN_COMPLETE are proved.
    order = sorted(by_type, key=lambda edge_type: (-weights.get(edge_type, 0), edge_type))
    with ThreadPoolExecutor(max_workers=len(edges), thread_name_prefix="bulk-edge") as executor:
        while pending or active:
            _beat()                          # the scheduler's own wait can outlast /healthz's stale limit with no monitor beating
            if cancelled.is_set():
                with launch_lock:
                    running = tuple(container for edge_type in active.values() for container in launched.get(edge_type, ()))
                _stop_exact_containers(running)
                for future in active:
                    try:
                        future.result()
                    except Exception:
                        pass
                return 1
            occupied = set(active.values())
            for edge_type in [edge_type for edge_type in order if edge_type in pending]:
                if cap and len(active) >= cap:
                    break
                with ready_condition:
                    dependencies_ready = _endpoint_labels_of(by_type[edge_type]).issubset(ready_labels)
                if not dependencies_ready:
                    continue
                if overlap == "labels" and plan.conflicts[edge_type] & occupied:
                    continue
                run_id = _derive_relationship_phase_run_id(
                    parent_run_id, phase_kind="bulk", phase_index=0, edge_type=edge_type,
                )
                logger.info(
                    "Starting bulk relationship edge=%s run_id=%s overlap=%s active=%s",
                    edge_type, run_id, overlap, len(active) + 1,
                )
                started_at = (time.monotonic(), datetime.now(timezone.utc).isoformat())
                if _execution_of(args) is not None:
                    _execution_of(args).phase_started(run_id, "bulk", 0, edge_type)
                future = executor.submit(
                    _run_bulk_relationship_edge, by_type[edge_type], docker_service, args,
                    run_id=run_id, consumer_group_prefix=consumer_group_prefix,
                    cancelled=cancelled, on_launch=register,
                    self_reference_co_scheduled=edge_type in co_scheduled,
                )
                active[future] = edge_type
                started[future] = started_at
                phase_ids[future] = run_id
                pending.remove(edge_type)
                occupied.add(edge_type)
            if not active:
                with ready_condition:
                    ready_condition.wait(timeout=0.25)
                continue
            completed, _ = wait(active, timeout=0.25, return_when=FIRST_COMPLETED)
            if not completed:
                continue
            failures = []
            for future in completed:
                edge_type = active.pop(future)
                started_monotonic, started_utc = started.pop(future)
                succeeded = False
                try:
                    succeeded = future.result() == 0
                except Exception as exc:
                    logger.error("stage=bulk-scheduler edge=%s failed: %s", edge_type, exc)
                if not succeeded:
                    failures.append(edge_type)
                if _execution_of(args) is not None:
                    _execution_of(args).phase_finished(
                        phase_ids.pop(future), status_model.SUCCEEDED if succeeded else status_model.FAILED,
                    )
                elapsed_seconds = time.monotonic() - started_monotonic
                completed_utc = datetime.now(timezone.utc).isoformat()
                logger.info(
                    "stage=bulk-edge-complete edge=%s elapsed_s=%.3f outcome=%s",
                    edge_type, elapsed_seconds, "succeeded" if succeeded else "failed",
                )
                _record_bulk_edge_stage_timing(
                    edge_type, started_utc, completed_utc, elapsed_seconds, succeeded, **timing_kwargs(args),
                )
            if failures:
                cancelled.set()
                with launch_lock:
                    running = tuple(container for edge_type in active.values() for container in launched.get(edge_type, ()))
                _stop_exact_containers(running)
                for future in active:
                    try:
                        future.result()
                    except Exception:
                        pass
                logger.error(
                    "stage=bulk-scheduler parent_run_id=%s failed edges=%s",
                    parent_run_id, ",".join(sorted(failures)),
                )
                return 1
    return 0


def _derive_relationship_phase_run_id(
    parent_run_id: str, *, phase_kind: str, phase_index: int, edge_type: str,
) -> str:
    """Derive one bounded collision-resistant child ID from an exact parent."""
    if _validated_run_id(parent_run_id) != parent_run_id:
        raise ValueError("parent run id must be valid")
    if not isinstance(phase_kind, str) or not phase_kind or not phase_kind.replace("_", "").isalnum():
        raise ValueError("phase kind must be nonblank alphanumeric text")
    if isinstance(phase_index, bool) or not isinstance(phase_index, int) or phase_index < 0:
        raise ValueError("phase index must be a nonnegative integer")
    if not isinstance(edge_type, str) or not edge_type:
        raise ValueError("phase edge type must be nonblank")
    digest = hashlib.sha256(
        f"{parent_run_id}|{phase_kind}|{phase_index}|{edge_type}".encode("utf-8")
    ).hexdigest()[:24]
    suffix = f".{phase_kind}.{phase_index}.{digest}"
    if len(suffix) >= 128:
        suffix = f".phase.{digest}"
    return f"{parent_run_id[:128 - len(suffix)]}{suffix}"


def _run_isolated_relationship_stream_phase(
    edge: EdgeConfig,
    docker_service: Runtime,
    args: argparse.Namespace,
    *,
    run_id: str,
    phase_index: int,
    consumer_group_prefix: str,
    parent_run_id: str,
) -> int:
    """Keep one exact self-reference stream alive without a clock or lease."""
    context = (
        f"stage=isolation parent_run_id={parent_run_id} phase_run_id={run_id} "
        f"phase={phase_index} edge={getattr(edge, 'type', '<unknown>')}"
    )
    if getattr(edge, "is_multi_pair", False) is True:
        logger.error("%s failed: edge binds several label pairs; this deprecated path supports one", context)
        return 1
    if edge.nodes.is_self_referencing is not True:
        logger.error("%s failed: edge is not self-referencing", context)
        return 1
    containers: list[object] = []
    tracked: dict[tuple[str, str], object] = {}
    monitor = None
    try:
        returned = docker_service.run_edge_loader(
            edge_type=edge.type, topics=edge.topics, mode="stream", config_path=args.config,
            replicas=edge.replicas, network=args.network, run_id=run_id,
            consumer_group_prefix=consumer_group_prefix, slot_gating=False,
        )
        containers.extend(returned)
        if len(returned) != edge.replicas:
            raise RuntimeError(f"launch returned {len(returned)} replicas; expected {edge.replicas}")
        if len({id(container) for container in returned}) != len(returned):
            raise RuntimeError("launch returned duplicate container objects")
        tracked = {(edge.type, str(replica_id)): container for replica_id, container in enumerate(returned)}
        expected = {(edge.type, str(replica_id)) for replica_id in range(edge.replicas)}
        if set(tracked) != expected:
            raise RuntimeError("replica accounting mismatch")
        monitor = RelationshipBulkMonitor(
            [edge], run_id, expected, tracked,
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            consumer_group_prefix=consumer_group_prefix, mode="stream",
        )
        shutdown = _shutdown_event()
        previous_int = signal.signal(signal.SIGINT, _stop_handler(shutdown, _cli_execution))
        previous_term = signal.signal(signal.SIGTERM, _stop_handler(shutdown, _cli_execution))
        try:
            monitor.supervise_stream(shutdown)
        finally:
            signal.signal(signal.SIGINT, previous_int)
            signal.signal(signal.SIGTERM, previous_term)
        failures = _stop_exact_containers(
            containers,
            identities={
                id(container): f"edge={edge_type} replica={replica_id}"
                for (edge_type, replica_id), container in tracked.items()
            },
        )
        if failures:
            raise RuntimeError(f"shutdown failed: {'; '.join(failures)}")
        return 0
    except Exception as exc:
        logger.error("%s failed: %s", context, exc)
        _stop_exact_containers(
            containers,
            identities={
                id(container): f"edge={edge_type} replica={replica_id}"
                for (edge_type, replica_id), container in tracked.items()
            },
        )
        return 1
    finally:
        if monitor is not None:
            monitor.close()
        _stop_exact_containers(containers)
        _remove_exact_containers(containers)


def _run_relationship_orchestration(
    schema, docker_service: Runtime, args: argparse.Namespace, *, parent_run_id: str,
    shared_already_loaded: bool = False,
    self_references_already_loaded: frozenset[str] = frozenset(),
) -> int:
    """Run completion-driven bulk or clocked stream edges with isolation.

    ``self_references_already_loaded`` names self-referencing bulk edges the
    caller already ran and proved inside the shared completion-driven
    scheduler (``--bulk-edge-overlap shared``); their isolated phase is
    skipped.  It is only honoured together with ``shared_already_loaded``.
    """
    try:
        if _validated_run_id(parent_run_id) != parent_run_id:
            raise ValueError("parent run id must be valid")
        plan = build_relationship_isolation_plan(tuple(getattr(schema, "edges", ())))
        edges_by_type = {edge.type: edge for edge in schema.edges}
    except (AttributeError, ValueError) as exc:
        logger.error("stage=isolation parent_run_id=%s failed: %s", parent_run_id, exc)
        return 1
    shared_edges = tuple(edges_by_type[edge_type] for edge_type in plan.shared_edge_types)
    mode = getattr(args, "mode", "bulk")
    if mode == "stream":
        if shared_edges and plan.isolated_phases:
            logger.error("stage=isolation parent_run_id=%s policy=mixed-stream shared and isolated relationships cannot run together", parent_run_id)
            return 1
        if len(plan.isolated_phases) > 1:
            logger.error("stage=isolation parent_run_id=%s policy=multi-isolated-stream phases=%s", parent_run_id, len(plan.isolated_phases))
            return 1
        if plan.isolated_phases:
            phase = plan.isolated_phases[0]
            phase_run_id = _derive_relationship_phase_run_id(
                parent_run_id, phase_kind="isolated", phase_index=phase.index, edge_type=phase.edge_type,
            )
            result = _record_phase(args, phase_run_id, "isolated", phase.index, phase.edge_type, lambda: _run_isolated_relationship_stream_phase(
                edges_by_type[phase.edge_type], docker_service, args, run_id=phase_run_id,
                phase_index=phase.index, consumer_group_prefix=schema.loading.consumer_group_id,
                parent_run_id=parent_run_id,
            ))
            if result:
                logger.error("stage=isolation parent_run_id=%s phase_run_id=%s phase=%s edge=%s failed", parent_run_id, phase_run_id, phase.index, phase.edge_type)
            return result
        if not shared_edges:
            return 0
        phase_run_id = _derive_relationship_phase_run_id(parent_run_id, phase_kind="shared", phase_index=0, edge_type="shared")
        result = _record_phase(args, phase_run_id, "shared", 0, None, lambda: _run_rotating_relationship_fleet(
            schema, docker_service, args, selected_edges=shared_edges, phase_run_id=phase_run_id,
        ))
        if result:
            logger.error("stage=isolation parent_run_id=%s phase_run_id=%s phase=0 edge=shared failed", parent_run_id, phase_run_id)
        return result

    already_loaded = frozenset(self_references_already_loaded) if shared_already_loaded else frozenset()
    if not shared_already_loaded:
        already_loaded = _co_scheduled_self_references(tuple(schema.edges), args)
        scheduled = shared_edges + tuple(
            edges_by_type[phase.edge_type] for phase in plan.isolated_phases
            if phase.edge_type in already_loaded
        )
        if scheduled:
            result = _run_completion_driven_bulk_edges(
                scheduled, docker_service, args, parent_run_id=parent_run_id,
                consumer_group_prefix=schema.loading.consumer_group_id,
            )
            if result:
                logger.error("stage=bulk-scheduler parent_run_id=%s shared relationship phase failed", parent_run_id)
                return result
    for phase in plan.isolated_phases:
        if phase.edge_type in already_loaded:
            continue
        phase_run_id = _derive_relationship_phase_run_id(
            parent_run_id, phase_kind="isolated", phase_index=phase.index, edge_type=phase.edge_type,
        )
        result = _record_phase(args, phase_run_id, "isolated", phase.index, phase.edge_type, lambda: _run_isolated_relationship_bulk_phase(
            edges_by_type[phase.edge_type], docker_service, args, run_id=phase_run_id,
            phase_index=phase.index, consumer_group_prefix=schema.loading.consumer_group_id,
        ))
        if result:
            logger.error("stage=isolation parent_run_id=%s phase_run_id=%s phase=%s edge=%s failed", parent_run_id, phase_run_id, phase.index, phase.edge_type)
            return result
    return 0


def _run_rotating_relationship_fleet(
    schema, docker_service: Runtime, args: argparse.Namespace, *,
    selected_edges: tuple[EdgeConfig, ...] | None = None, phase_run_id: str | None = None,
) -> int:
    """Run one exact eligible-edge fleet under a single run-scoped clock.

    The caller owns neither a discovered Docker set nor a global Kafka group:
    every cleanup target is retained as it is created.  Slice 4 refines the
    success boundary proof; this establishes the shared launch and fail-closed
    lifecycle it builds upon.
    """
    all_edges = tuple(getattr(schema, "edges", ()))
    try:
        _refuse_multi_pair(all_edges, "the rotating clock fleet")
        isolation_plan = build_relationship_isolation_plan(all_edges)
        if selected_edges is None:
            self_references = tuple(edge for edge in all_edges if edge.nodes.is_self_referencing)
            if self_references:
                deferred = ",".join(sorted(edge.type for edge in self_references))
                logger.error(
                    "stage=launch reason=self-referencing relationship types require Phase 4 Slice 4 policy edges=%s",
                    deferred,
                )
                return 1
            eligible = all_edges
        else:
            eligible = tuple(selected_edges)
            selected_types = tuple(sorted(edge.type for edge in eligible))
            if selected_types != isolation_plan.shared_edge_types:
                raise ValueError("selected rotating fleet must contain exactly every shared schema edge")
            if any(edge.nodes.is_self_referencing is not False for edge in eligible):
                raise ValueError("selected rotating fleet contains a self-referencing edge")
        if not eligible:
            return 0
        # Keep Phase 3's graph construction an explicit pre-launch validation.
        build_relationship_conflict_plan(eligible)
        fleet_edge_types = ",".join(sorted(edge.type for edge in eligible))
        fleet_edges = select_fleet_edges(all_edges, tuple(fleet_edge_types.split(",")))
        rotation_plan = build_rotation_plan(
            build_conflict_families(fleet_edges),
            bucket_count=schema.loading.coordination.bucket_count,
        )
        coordination_topic = schema.loading.coordination.topic
    except Exception as exc:
        logger.error("stage=launch reason=invalid rotating fleet contract: %s", exc)
        return 1

    try:
        run_id = _validated_run_id(phase_run_id if phase_run_id is not None else getattr(args, "run_id", None)) or uuid.uuid4().hex
    except ValueError as exc:
        logger.error("stage=launch reason=invalid run id: %s", exc)
        return 1
    mode = getattr(args, "mode", "bulk")
    edges_by_type = {edge.type: edge for edge in fleet_edges}
    edge_containers: list[object] = []
    tracked: dict[tuple[str, str], object] = {}
    clock_container = None
    monitor = None
    timeout = args.bulk_timeout_seconds
    try:
        # Establish the run-scoped clock before any gated edge process starts.
        # A loader may poll its coordination consumer immediately at startup;
        # launching the clock first prevents a fleet-start race from making its
        # first admissible lease depend on scheduler timing.
        try:
            clock_container = docker_service.run_global_batch_clock(
                run_id=run_id, config_path=args.config, fleet_edge_types=fleet_edge_types,
                coordination_topic=coordination_topic, network=args.network,
            )
        except Exception as exc:
            raise RuntimeError(f"stage=clock run_id={run_id} reason=launch failed: {exc}") from exc
        if clock_container is None:
            raise RuntimeError(f"stage=clock run_id={run_id} launch returned no container")
        for edge_type in fleet_edge_types.split(","):
            edge = edges_by_type[edge_type]
            try:
                containers = docker_service.run_edge_loader(
                    edge_type=edge.type, topics=edge.topics, mode=mode, config_path=args.config,
                    replicas=edge.replicas, network=args.network, run_id=run_id,
                    consumer_group_prefix=schema.loading.consumer_group_id, slot_gating=True,
                    coordination_topic=coordination_topic, fleet_edge_types=fleet_edge_types,
                )
            except Exception as exc:
                raise RuntimeError(
                    f"stage=launch run_id={run_id} edge={edge.type} reason={exc}"
                ) from exc
            edge_containers.extend(containers)
            if len(containers) != edge.replicas:
                raise RuntimeError(
                    f"stage=launch run_id={run_id} edge={edge.type} launch returned {len(containers)} replicas; expected {edge.replicas}"
                )
            for replica_id, container in enumerate(containers):
                replica = (edge.type, str(replica_id))
                if container in tracked.values():
                    raise RuntimeError(
                        f"stage=launch run_id={run_id} edge={edge.type} replica={replica_id} reused container object"
                    )
                tracked[replica] = container
        expected = {
            (edge.type, str(replica_id))
            for edge in fleet_edges
            for replica_id in range(edge.replicas)
        }
        if set(tracked) != expected:
            raise RuntimeError(f"stage=launch run_id={run_id} relationship replica accounting mismatch")
        monitor = RelationshipBulkMonitor(
            fleet_edges, run_id, expected, tracked,
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            consumer_group_prefix=schema.loading.consumer_group_id,
            rotation_plan=rotation_plan, coordination_topic=coordination_topic,
            tracked_clock=clock_container,
            abort_requested=_shutdown_requested if mode != "stream" else None,
            mode="stream" if mode == "stream" else "bulk",
        )
        if mode == "stream":
            shutdown = _shutdown_event()
            previous_int = signal.signal(signal.SIGINT, _stop_handler(shutdown, _cli_execution))
            previous_term = signal.signal(signal.SIGTERM, _stop_handler(shutdown, _cli_execution))
            try:
                initial_timeout = max(
                    schema.loading.coordination.lease_timeout_ms / 1000,
                    0.001,
                )
                monitor.supervise_stream(
                    shutdown, initial_lease_timeout_seconds=initial_timeout,
                )
            finally:
                signal.signal(signal.SIGINT, previous_int)
                signal.signal(signal.SIGTERM, previous_term)
            clock_failures = _stop_exact_containers(
                [clock_container], identities={id(clock_container): "clock"},
            )
            if clock_failures:
                raise RuntimeError(
                    f"stage=shutdown run_id={run_id} clock shutdown failed: {'; '.join(clock_failures)}"
                )
            edge_failures = _stop_exact_containers(
                edge_containers,
                identities={
                    id(container): f"edge={edge_type} replica={replica_id}"
                    for (edge_type, replica_id), container in tracked.items()
                },
            )
            if edge_failures:
                raise RuntimeError(
                    f"stage=shutdown run_id={run_id} edge shutdown failed: {'; '.join(edge_failures)}"
                )
            return 0
        monitor.wait_for_assignment_coverage(timeout)
        monitor.wait_for_clock_lease(timeout)
        monitor.capture_boundary()
        monitor.wait_for_rotating_completion(timeout)
        edge_identities = {
            id(container): f"edge={edge_type} replica={replica_id}"
            for (edge_type, replica_id), container in tracked.items()
        }
        stop_edge_failures = _stop_exact_containers(edge_containers, identities=edge_identities)
        if stop_edge_failures:
            raise RuntimeError(f"stage=shutdown run_id={run_id} edge shutdown failed: {'; '.join(stop_edge_failures)}")
        monitor.wait_for_drain_complete(timeout)
        monitor.verify_zero_lag()
        stop_clock_failures = _stop_exact_containers(
            [clock_container], identities={id(clock_container): "clock"},
        )
        if stop_clock_failures:
            raise RuntimeError(f"stage=shutdown run_id={run_id} clock shutdown failed: {'; '.join(stop_clock_failures)}")
        return 0
    except Exception as exc:
        logger.error("stage=relationship-fleet run_id=%s failed: %s", run_id, exc)
        # Failure is fail-closed: no new leases before stopping exact loaders.
        if clock_container is not None:
            clock_failures = _stop_exact_containers(
                [clock_container], identities={id(clock_container): "clock"},
            )
            if clock_failures:
                logger.error(
                    "stage=shutdown run_id=%s clock cleanup failed: %s", run_id,
                    "; ".join(clock_failures),
                )
        edge_failures = _stop_exact_containers(
            edge_containers,
            identities={
                id(container): f"edge={edge_type} replica={replica_id}"
                for (edge_type, replica_id), container in tracked.items()
            },
        )
        if edge_failures:
            logger.error(
                "stage=shutdown run_id=%s edge cleanup failed: %s", run_id,
                "; ".join(edge_failures),
            )
        return 1
    finally:
        if monitor is not None:
            monitor.close()
        if clock_container is not None:
            _stop_exact_containers([clock_container])
            _remove_exact_containers([clock_container])
        _stop_exact_containers(edge_containers)
        _remove_exact_containers(edge_containers)


def _sweep_orphaned_lease_containers(docker_service: Runtime, run_id: str) -> None:
    """Review finding M5-1 backstop: a lease-server relaunch whose
    ``containers.run()`` call raised *after* Docker already created the
    container leaves the monitor with no object to stop. Sweep by label
    so this run's lease server is never left running regardless."""
    try:
        containers = docker_service.list_run_containers(
            label_filters=(f"run_id={run_id}", "component=bucket-lease-server"),
        )
    except Exception as exc:
        logger.warning("stage=shutdown run_id=%s lease-server label sweep failed: %s", run_id, exc)
        return
    for container in containers:
        try:
            container.stop()
        except Exception:
            pass
        try:
            container.remove(force=True)
        except Exception:
            pass


def _launch_stream_lease_server(docker_service: Runtime, schema, args: argparse.Namespace, *, run_id: str):
    """Select keyed labels and launch the run's long-lived lease coordinator."""
    args.lease_keyed_labels = _select_keyed_lease_labels(schema, args)
    try:
        container = docker_service.run_bucket_lease_server(
            run_id=run_id, bucket_count=args.lease_buckets, network=args.network,
            keyed_labels=args.lease_keyed_labels,
            fence_seconds=_bucket_lease_fence_seconds(schema),
        )
    except Exception as exc:
        raise RuntimeError(f"stage=lease-server run_id={run_id} reason=launch failed: {exc}") from exc
    if container is None:
        raise RuntimeError(f"stage=lease-server run_id={run_id} launch returned no container")
    return container


def _start_stream_lease_for_nodes(schema, docker_service: Runtime, args: argparse.Namespace, *, run_id: str):
    """Start the stream lease server BEFORE the node loaders (they lease too).

    Returns ``(container, node_launch_kwargs)``; the kwargs carry the lease
    address and the bucket/keyed-label scheme every loader of the run shares.
    """
    _validate_stream_lease_settings(schema, args)
    container = _launch_stream_lease_server(docker_service, schema, args, run_id=run_id)
    address = docker_service.bucket_lease_address(container, network=args.network, prefer_name=True)
    return container, {
        "lease_address": address, "lease_buckets": args.lease_buckets,
        "lease_keyed_labels": args.lease_keyed_labels,
    }


def _retry_dlq_topics(edges: Iterable[EdgeConfig]) -> list[str]:
    """The DLQ topics a stream fleet's retry consumer reads (one per DLQ-enabled type, v7-S4)."""
    return sorted({
        edge_dead_letter_topic_name(edge)
        for edge in edges
        if getattr(edge, "dead_letter", None) is not None
        and edge_dead_letter_topic_name(edge) is not None
    })


def _legacy_stream_run_ids(schema, args: argparse.Namespace, parent_run_id: str) -> dict[str, str]:
    """``{edge type: run id}`` an earlier release put in this stream path's run-scoped group names.

    The default lease fleet used the parent run id. The deprecated
    ``--stream-clock-rotation`` path used a phase id derived from it.
    """
    edges = tuple(schema.edges)
    if not getattr(args, "stream_clock_rotation", False):
        return {edge.type: parent_run_id for edge in edges}
    plan = build_relationship_isolation_plan(edges)
    result = {
        phase.edge_type: _derive_relationship_phase_run_id(
            parent_run_id, phase_kind="isolated", phase_index=phase.index, edge_type=phase.edge_type,
        )
        for phase in plan.isolated_phases
    }
    if plan.shared_edge_types:
        shared = _derive_relationship_phase_run_id(parent_run_id, phase_kind="shared", phase_index=0, edge_type="shared")
        result.update({edge_type: shared for edge_type in plan.shared_edge_types})
    return result


def _stream_consumer_groups(schema, args: argparse.Namespace, *, explicit_run_id: str | None) -> list[StreamGroup]:
    """Every stable group a stream fleet of ``schema`` uses, with its legacy name when ``--run-id`` gives one."""
    prefix = schema.loading.consumer_group_id
    edges = tuple(schema.edges)
    node_groups = {f"{prefix}-{node.label}" for node in schema.nodes}
    clashing = sorted(edge.type for edge in edges if stream_edge_group(prefix, edge.type) in node_groups)
    if clashing:
        raise ValueError(
            f"relationship type(s) {clashing} have the same name as a node label, so their stream consumer "
            f"group {prefix}-<name> would be shared with that label's node loaders; rename the type or the label"
        )
    legacy_run_ids = _legacy_stream_run_ids(schema, args, explicit_run_id) if explicit_run_id is not None else {}
    groups = [
        StreamGroup(
            stream_edge_group(prefix, edge.type), tuple(edge.topics),
            legacy_stream_edge_group(prefix, edge.type, legacy_run_ids[edge.type])
            if edge.type in legacy_run_ids else None,
        )
        for edge in edges
    ]
    retry_topics = _retry_dlq_topics(edges)
    if retry_topics and not getattr(args, "stream_clock_rotation", False):
        groups.append(StreamGroup(
            stream_retry_group(prefix), tuple(retry_topics),
            legacy_retry_group(explicit_run_id) if explicit_run_id is not None else None,
        ))
    return groups


def _stream_group_member_wait_seconds(schema, args: argparse.Namespace) -> float:
    """``--stream-group-member-wait-seconds``, or the session timeout plus 5 s."""
    explicit = getattr(args, "stream_group_member_wait_seconds", None)
    if explicit is not None:
        return float(explicit)
    return schema.loading.session_timeout_ms / 1000 + 5.0


def _prepare_stream_consumer_groups(schema, args: argparse.Namespace, *, explicit_run_id: str | None,
                                    bootstrap_servers: str, admin_factory=None) -> None:
    """Refuse stable stream groups that still have members, then migrate legacy offsets once.

    Runs before anything of a stream fleet launches (nodes included). Waits up
    to the session timeout plus 5 s (or ``--stream-group-member-wait-seconds``) for
    the members of a crashed run to expire.
    """
    groups = _stream_consumer_groups(schema, args, explicit_run_id=explicit_run_id)
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient

        admin_factory = lambda: AdminClient({"bootstrap.servers": bootstrap_servers})  # noqa: E731
    prepare_stream_groups(
        admin_factory(), groups, explicit_run_id=explicit_run_id,
        member_timeout_seconds=_stream_group_member_wait_seconds(schema, args),
        stop_requested=_shutdown_requested,
    )


def _run_lease_relationship_fleet(
    schema, docker_service: Runtime, args: argparse.Namespace, *, parent_run_id: str,
    prestarted_lease_container: object | None = None, node_containers: Iterable[object] = (),
) -> int:
    """Run every relationship type concurrently under one long-lived
    bucket-lease coordinator -- streaming's default zero-wait mechanism
    (P5-S1 Story 6). Unlike the deprecated clock/rotation path, there is
    no shared/isolated split and no separate self-reference phase: every
    edge type (conflicting, non-conflicting, and self-referencing alike)
    launches into the same fleet, since ``BucketCellRouter`` already
    collapses a self-reference's cell to one resource.
    """
    try:
        if getattr(args, "runtime", None) is None:
            _merge_deprecated_lease_flags(args)
    except ValueError as exc:
        logger.error("stage=launch reason=%s", exc)
        return 1
    all_edges = tuple(getattr(schema, "edges", ()))
    if not all_edges:
        return 0
    try:
        _validate_stream_lease_settings(schema, args)
    except ValueError as exc:
        logger.error("stage=launch reason=invalid stream lease settings: %s", exc)
        return 1
    try:
        if _validated_run_id(parent_run_id) != parent_run_id:
            raise ValueError("parent run id must be valid")
    except ValueError as exc:
        logger.error("stage=launch reason=invalid run id: %s", exc)
        return 1
    run_id = parent_run_id
    node_containers = list(node_containers)

    edge_containers: list[object] = []
    retry_containers: list[object] = []
    tracked: dict[tuple[str, str], object] = {}
    lease_container = None
    monitor = None
    try:
        # Stream node loaders lease too, so handle_start may already have
        # started the coordinator (before the nodes) and chosen the keyed labels.
        lease_container = prestarted_lease_container or _launch_stream_lease_server(
            docker_service, schema, args, run_id=run_id,
        )
        # Restart-transparent addressing (Story 5): a relaunched lease
        # server is a new container with a new IP, so streaming resolves
        # it by name through the run's own network's embedded DNS.
        lease_address = docker_service.bucket_lease_address(
            lease_container, network=args.network, prefer_name=True,
        )
        # P5-S4 Story 6: the retry consumer, launched once per streaming
        # run and torn down with it -- Decisions §4's launch order (lease
        # server, then retry consumer, then edge loaders) is a one-line
        # addition here, not a new launch phase. Skipped entirely when no
        # edge type in this run's schema has DLQ routing enabled at all
        # (matching this codebase's existing "don't run a process with
        # structurally nothing to do" convention).
        # One DLQ per relationship type (v7-S4): a multi-topic type contributes one topic.
        retry_dlq_topics = _retry_dlq_topics(all_edges)
        if retry_dlq_topics:
            try:
                retry_containers = docker_service.run_retry_consumer(
                    run_id=run_id, config_path=args.config, lease_address=lease_address,
                    dlq_topics=retry_dlq_topics, replicas=getattr(args, "retry_consumer_replicas", 1),
                    network=args.network, lease_buckets=args.lease_buckets,
                    lease_keyed_labels=args.lease_keyed_labels,
                    consumer_group_prefix=schema.loading.consumer_group_id,
                )
            except Exception as exc:
                raise RuntimeError(f"stage=retry-consumer run_id={run_id} reason=launch failed: {exc}") from exc
        for edge in all_edges:
            try:
                containers = docker_service.run_edge_loader(
                    edge_type=edge.type, topics=edge.topics, mode="stream", config_path=args.config,
                    replicas=edge.replicas, network=args.network, run_id=run_id,
                    consumer_group_prefix=schema.loading.consumer_group_id,
                    lease_address=lease_address, lease_buckets=args.lease_buckets,
                    outer_batch_size=getattr(
                        args, "lease_outer_batch_size", DEFAULT_BUCKET_LEASE_OUTER_BATCH_SIZE,
                    ),
                    lease_keyed_labels=args.lease_keyed_labels,
                )
            except Exception as exc:
                raise RuntimeError(f"stage=launch run_id={run_id} edge={edge.type} reason={exc}") from exc
            edge_containers.extend(containers)
            if len(containers) != edge.replicas:
                raise RuntimeError(
                    f"stage=launch run_id={run_id} edge={edge.type} launch returned "
                    f"{len(containers)} replicas; expected {edge.replicas}"
                )
            for replica_id, container in enumerate(containers):
                replica = (edge.type, str(replica_id))
                if container in tracked.values():
                    raise RuntimeError(
                        f"stage=launch run_id={run_id} edge={edge.type} replica={replica_id} reused container object"
                    )
                tracked[replica] = container
        expected = {(edge.type, str(replica_id)) for edge in all_edges for replica_id in range(edge.replicas)}
        if set(tracked) != expected:
            raise RuntimeError(f"stage=launch run_id={run_id} relationship replica accounting mismatch")
        monitor = RelationshipBulkMonitor(
            all_edges, run_id, expected, tracked,
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            consumer_group_prefix=schema.loading.consumer_group_id,
            tracked_lease_server=lease_container, bucket_lease_config=schema.loading.bucket_lease,
            docker_service=docker_service, lease_bucket_count=args.lease_buckets,
            lease_keyed_labels=args.lease_keyed_labels, lease_network=args.network,
            tracked_retry_consumers=retry_containers, mode="stream",
        )
        shutdown = _shutdown_event()
        previous_int = signal.signal(signal.SIGINT, _stop_handler(shutdown, _cli_execution))
        previous_term = signal.signal(signal.SIGTERM, _stop_handler(shutdown, _cli_execution))
        try:
            monitor.wait_for_lease_server_ready(30.0, shutdown_requested=shutdown)
            monitor.supervise_stream(shutdown)
        finally:
            signal.signal(signal.SIGINT, previous_int)
            signal.signal(signal.SIGTERM, previous_term)
        # Review finding B6-1: stop the edge loaders FIRST, then the lease
        # server -- the reverse order (bulk's own mistake this story must
        # not repeat) drops every edge loader's coordinator out from under
        # it before it gets its own SIGTERM, degrading a clean drain into
        # WRITE_FAILUREs and Docker SIGKILLs after the stop timeout. The
        # retry consumer is stopped in this same pass (it is also
        # lease-fenced -- P5-S4 Decisions §4), before the lease server.
        edge_identities = {
            id(container): f"edge={edge_type} replica={replica_id}"
            for (edge_type, replica_id), container in tracked.items()
        }
        retry_identities = {id(container): f"retry-consumer replica={index}" for index, container in enumerate(retry_containers)}
        edge_failures = _stop_exact_containers(
            [*edge_containers, *retry_containers], identities={**edge_identities, **retry_identities},
        )
        if edge_failures:
            raise RuntimeError(
                f"stage=shutdown run_id={run_id} edge shutdown failed: {'; '.join(edge_failures)}"
            )
        # Stream node loaders lease too: drain them after the edges but
        # while the lease server is still up.
        node_failures = _stop_exact_containers(node_containers, stop_timeout=_NODE_STOP_TIMEOUT_SECONDS)
        if node_failures:
            logger.error("stage=shutdown run_id=%s node shutdown failed: %s", run_id, "; ".join(node_failures))
        # Review finding M5-1: stop the CURRENT (possibly relaunched)
        # lease container, not whichever object was launched originally.
        # _stop_bucket_lease_server (Story 5's design: "reuse it verbatim")
        # also captures stage=lease-server-stats and the coordinator's log.
        _stop_bucket_lease_server(monitor.tracked_lease_server, **timing_kwargs(args))
        return 0
    except Exception as exc:
        logger.error("stage=lease-relationship-fleet run_id=%s failed: %s", run_id, exc)
        edge_identities = {
            id(container): f"edge={edge_type} replica={replica_id}"
            for (edge_type, replica_id), container in tracked.items()
        }
        retry_identities = {id(container): f"retry-consumer replica={index}" for index, container in enumerate(retry_containers)}
        edge_failures = _stop_exact_containers(
            [*edge_containers, *retry_containers], identities={**edge_identities, **retry_identities},
        )
        if edge_failures:
            logger.error(
                "stage=shutdown run_id=%s edge cleanup failed: %s", run_id,
                "; ".join(edge_failures),
            )
        _stop_exact_containers(node_containers, stop_timeout=_NODE_STOP_TIMEOUT_SECONDS)
        current_lease = monitor.tracked_lease_server if monitor is not None else lease_container
        if current_lease is not None:
            lease_failures = _stop_exact_containers(
                [current_lease], identities={id(current_lease): "lease-server"},
            )
            if lease_failures:
                logger.error(
                    "stage=shutdown run_id=%s lease-server cleanup failed: %s", run_id,
                    "; ".join(lease_failures),
                )
        return 1
    finally:
        if monitor is not None:
            monitor.close()
        # Review finding B6-1: edges (and the retry consumer, also
        # lease-fenced) before the lease server here too, for the same
        # reason as the success/failure paths above -- this final sweep
        # only needs to guarantee nothing is left running, but keeping the
        # order consistent avoids a lease-less container lingering
        # (harmlessly, but confusingly) mid-teardown.
        _stop_exact_containers([*edge_containers, *retry_containers])
        _remove_exact_containers([*edge_containers, *retry_containers])
        _stop_exact_containers(node_containers)
        current_lease = monitor.tracked_lease_server if monitor is not None else lease_container
        if current_lease is not None:
            _stop_exact_containers([current_lease])
            _remove_exact_containers([current_lease])
        _sweep_orphaned_lease_containers(docker_service, run_id)


def _bucket_lease_numeric_args(args: argparse.Namespace) -> None:
    buckets = getattr(args, "lease_buckets", DEFAULT_BUCKET_LEASE_BUCKETS)
    if isinstance(buckets, bool) or not isinstance(buckets, int) or not 1 <= buckets <= 4096:
        raise ValueError("--bucket-lease-buckets must be 1-4096")
    keyed_max = getattr(args, "lease_keyed_max_nodes", DEFAULT_BUCKET_LEASE_KEYED_MAX_NODES)
    if isinstance(keyed_max, bool) or not isinstance(keyed_max, int) or keyed_max < 0:
        raise ValueError("--bucket-lease-keyed-max-nodes must be a nonnegative integer")
    outer = getattr(args, "lease_outer_batch_size", DEFAULT_BUCKET_LEASE_OUTER_BATCH_SIZE)
    if isinstance(outer, bool) or not isinstance(outer, int) or not 1 <= outer <= 1_000_000:
        raise ValueError("--bucket-lease-outer-batch-size must be 1-1000000")


def _bucket_lease_numeric_bounds(schema, args: argparse.Namespace, *, feature_flag: str) -> None:
    """Shared bucket/keyed-max/outer-batch/python_apoc checks for both bulk
    ``--edge-leases`` and the streaming lease-fleet default (P5-S1 Story 6).
    Raises with ``feature_flag`` named in the message so the two callers'
    errors stay distinguishable."""
    _bucket_lease_numeric_args(args)
    unsupported = sorted(
        edge.type for edge in getattr(schema, "edges", ())
        if getattr(getattr(edge, "execution", None), "mode", "python_apoc") != "python_apoc"
    )
    if unsupported:
        raise ValueError(f"{feature_flag} requires python_apoc edges; unsupported: {','.join(unsupported)}")
    # Leased writes are fenced (src/neo4j_graph_loader/loader/fenced_tx.py): T must leave a budget.
    # Checked here, not in the schema, so planned and classic bulk load as before.
    tx_timeout_ms = getattr(getattr(getattr(schema, "loading", None), "bucket_lease", None), "tx_timeout_ms", None)
    if isinstance(tx_timeout_ms, int) and not isinstance(tx_timeout_ms, bool):
        require_fenced_budget(tx_timeout_ms, feature_flag)


def _validate_edge_lease_settings(schema, args: argparse.Namespace) -> None:
    """Check ``--edge-leases`` preconditions; leases imply shared overlap."""
    if args.mode != "bulk":
        raise ValueError("--edge-leases requires --mode bulk")
    _bucket_lease_numeric_bounds(schema, args, feature_flag="--edge-leases")
    # Leased transactions are node-disjoint across every type, so all types,
    # including self-references, can run concurrently.
    args.bulk_edge_overlap = "shared"


def _validate_stream_lease_settings(schema, args: argparse.Namespace) -> None:
    """Check the streaming lease-fleet default's preconditions (P5-S1
    Story 6). Unlike ``_validate_edge_lease_settings``: no ``--mode bulk``
    gate (this is streaming's own default, no flag needed to enable it),
    and no ``bulk_edge_overlap`` side effect (that field is bulk-only)."""
    _bucket_lease_numeric_bounds(schema, args, feature_flag="--mode stream")


def _validate_edge_plan_flags(args: argparse.Namespace) -> None:
    """Check ``--edge-plan`` preconditions (bulk only, exclusive with leases)."""
    if getattr(args, "edge_plan", None) and args.mode != "bulk":
        raise ValueError("--edge-plan requires --mode bulk")
    if getattr(args, "edge_plan", None) and getattr(args, "edge_leases", False):
        raise ValueError("--edge-plan and --edge-leases are mutually exclusive")
    writers = getattr(args, "plan_writers", None)
    if writers is not None and (isinstance(writers, bool) or not isinstance(writers, int) or not 1 <= writers <= 64):
        raise ValueError("--plan-writers must be 1-64")
    for name, low, high in (("plan_workers", 1, 64), ("plan_buckets", 1, 4096), ("plan_hub_degree", 0, 1 << 40),
                            ("plan_chunk_rows", 1, 100_000), ("plan_partitions", 1, 1024),
                            ("plan_memory_mb", 16, 1 << 20)):
        value = getattr(args, name, None)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high):
            raise ValueError(f"--{name.replace('_', '-')} must be {low}-{high}")


def _validate_edge_plan_settings(schema, args: argparse.Namespace) -> None:
    _validate_edge_plan_flags(args)
    unsupported = sorted(
        edge.type for edge in getattr(schema, "edges", ())
        if getattr(getattr(edge, "execution", None), "mode", "python_apoc") != "python_apoc"
    )
    if unsupported:
        raise ValueError(f"--edge-plan requires python_apoc edges; unsupported: {','.join(unsupported)}")
    if args.edge_plan != AUTO_EDGE_PLAN:
        args.edge_plan = str(Path(args.edge_plan).resolve())


def _validate_edge_mode_flags(args: argparse.Namespace) -> None:
    """Reject contradictory edge-mode flags before any side effect."""
    chosen = [flag for flag, on in (
        ("--edge-plan", bool(getattr(args, "edge_plan", None))),
        ("--edge-leases", bool(getattr(args, "edge_leases", False))),
        ("--edge-classic", bool(getattr(args, "edge_classic", False))),
    ) if on]
    if len(chosen) > 1:
        raise ValueError(f"{' and '.join(chosen)} are mutually exclusive; choose one relationship mode")
    if getattr(args, "edge_leases", False) and args.mode != "bulk":
        raise ValueError("--edge-leases requires --mode bulk")
    if getattr(args, "edge_classic", False) and args.mode != "bulk":
        raise ValueError(
            "--edge-classic requires --mode bulk (stream mode uses the bucket-lease fleet; its classic "
            "scheduling is only the deprecated --stream-clock-rotation)"
        )


def _validate_early_numeric_args(args: argparse.Namespace) -> None:
    """Reject out-of-range numeric flags before any side effect."""
    if getattr(args, "edge_plan", None) or (
        args.mode == "bulk" and not getattr(args, "edge_classic", False) and not getattr(args, "edge_leases", False)
    ):
        _validate_edge_plan_flags(args)
    if args.mode == "bulk" and getattr(args, "bulk_timeout_seconds", 300.0) <= 0:
        raise ValueError("--bulk-timeout-seconds must be positive")
    if getattr(args, "edge_leases", False) or (
        args.mode == "stream" and not getattr(args, "stream_clock_rotation", False)
    ):
        _bucket_lease_numeric_args(args)
    if args.mode == "stream":
        replicas = getattr(args, "retry_consumer_replicas", 1)
        if isinstance(replicas, bool) or not isinstance(replicas, int) or not 1 <= replicas <= 64:
            raise ValueError("--retry-consumer-replicas must be 1-64")


def _dead_letter_edge_types(schema) -> list[str]:
    """Relationship types that opt into DLQ routing (``dead_letter.enabled: true``).

    Planned mode ignores ``dead_letter`` entirely and routes invalid records to a planner rejection file (never a DLQ topic), so
    any type whose DLQ topic resolves (this also covers ``route_missing_endpoints_in_bulk``,
    ``max_retry_attempts`` and ``parked_topic``, which all require ``enabled``) cannot run
    planned. A ``dead_letter`` block with ``enabled: false`` is the file-sink fallback and
    does not count. Node ``dead_letter`` is unaffected by the relationship mode.
    """
    return sorted(
        edge.type for edge in getattr(schema, "edges", ())
        if getattr(edge, "dead_letter", None) is not None
        and edge_dead_letter_topic_name(edge) is not None
    )


def _resolve_bulk_edge_mode(schema, args: argparse.Namespace) -> None:
    """Pick the bulk relationship mode (owner decision 5) and log the choice.

    Explicit ``--edge-plan`` / ``--edge-leases`` / ``--edge-classic`` win. Otherwise the default
    is planned (zero lock waits by construction, ``--edge-plan`` with every setting
    auto-derived), or the lease fleet (also zero lock waits) when a relationship type enables
    ``dead_letter``. A plain start never reaches classic: ``native_disjoint`` edges (disjoint by
    pair, not by node, so they can wait on endpoint locks) are refused unless ``--edge-classic``
    is passed. ``native_disjoint`` needs a Neo4j server with the Cypher 25 dialect and is not
    supported on Neo4j 5.26, the tested target; ``python_apoc`` is the supported mode, and
    ``--edge-classic`` exists for servers that do support Cypher 25. The classic-only tuning
    flags are warned about and ignored in every other mode.
    """
    explicit_overlap = getattr(args, "bulk_edge_overlap", None)
    explicit_cap = getattr(args, "max_concurrent_bulk_edges", None)
    if explicit_overlap is None:
        args.bulk_edge_overlap = "labels"
    if explicit_cap is None:
        args.max_concurrent_bulk_edges = 0
    if args.mode != "bulk" or not getattr(schema, "edges", None):
        return
    if getattr(args, "edge_plan", None):
        mode, reason = "planned", "flag"
    elif getattr(args, "edge_leases", False):
        mode, reason = "lease", "flag"
    elif getattr(args, "edge_classic", False):
        mode, reason = "classic", "flag"
    else:
        native = sorted(
            edge.type for edge in schema.edges
            if getattr(getattr(edge, "execution", None), "mode", "python_apoc") != "python_apoc"
        )
        if native:
            raise ValueError(
                f"relationship type(s) {', '.join(native)} use execution.mode native_disjoint: it needs a Neo4j "
                "server with the Cypher 25 dialect and is not supported on Neo4j 5.26 (the tested target); "
                "use python_apoc (--edge-classic exists only for servers that support Cypher 25)"
            )
        if _dead_letter_edge_types(schema):
            mode, reason = "lease", "dead_letter"
            args.edge_leases = True
        else:
            mode, reason = "planned", "default"
            args.edge_plan = AUTO_EDGE_PLAN
    if mode != "classic" and (explicit_overlap is not None or explicit_cap is not None):
        logger.warning(
            "--bulk-edge-overlap/--max-concurrent-bulk-edges tune classic only; ignored "
            "(add --edge-classic to use classic)"
        )
        if mode == "planned":
            args.bulk_edge_overlap = "labels"
        args.max_concurrent_bulk_edges = 0
    args.edge_mode_reason = reason
    logger.info("stage=launch edge_mode=%s reason=%s", mode, reason)


def _require_supported_topic_conflicts(schema, args: argparse.Namespace) -> None:
    """Refuse multi-topic ``topic_conflict: precedence`` where this run cannot honor it.

    Stream has no cross-topic order (``require_no_precedence_in_stream``, called with the
    resolved ``--mode``). Every bulk mode honors precedence: planned mode orders by planner task
    (v7-S3); classic and ``--edge-leases`` bulk run the type's topics as sequential sub-stages in
    ``precedence_rank`` order (v7-S5, ``_run_precedence_substages``). ``dead_letter`` on a
    multi-topic type is supported in every mode that honours it (v7-S4: one DLQ, parking lot and
    parked-pair index per type, provisioned once from the schema's parent edge).
    """
    if not getattr(schema, "edges", None):
        return
    if args.mode == "stream" and getattr(args, "stream_clock_rotation", False):
        _refuse_multi_pair(schema.edges, "--stream-clock-rotation")  # v7-S7, before any side effect
    try:
        require_no_precedence_in_stream(schema, args.mode)
    except ValueError as exc:
        raise ValueError(
            f"{exc}. Precedence runs in bulk mode only (planned, --edge-leases or --edge-classic)"
        ) from exc


def _former_dead_letter_names(edge) -> list[str]:
    """DLQ names a relationship type would have had with another topic count (v7-S4):
    ``<topic>.dlq`` for each of its topics and ``<type>.dlq``, minus the resolved one."""
    resolved = edge_dead_letter_topic_name(edge)
    names = [f"{topic}.dlq" for topic in edge.topics] + [f"{edge.type}.dlq"]
    return [name for name in dict.fromkeys(names) if name != resolved]


def _stale_dead_letter_topics(
    schema, bootstrap_servers: str, *, admin_factory=None, consumer_factory=None, accepted=(),
):
    """``(edge type, stale DLQ topic, records, resolved DLQ topic)`` for every DLQ-enabled
    relationship type WITHOUT an explicit ``dead_letter.topic`` whose former default DLQ
    name (one topic <-> several topics) still holds records (v7-S4 review S4).

    Adding a second topic renames the default DLQ from ``<topic>.dlq`` to ``<type>.dlq``
    (and going back renames it again), which would silently orphan the old DLQ, its
    parking lot and its parked-pair index. A name that is another entity's resolved
    DLQ-family topic is not a candidate."""
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient as admin_factory
    if consumer_factory is None:
        from confluent_kafka import Consumer as consumer_factory
    from confluent_kafka import TopicPartition

    owned: set[str] = set()
    for edge in getattr(schema, "edges", ()):
        owned.update(name for name in (
            edge_dead_letter_topic_name(edge), edge_parked_topic_name(edge), edge_parked_pair_index_topic_name(edge),
        ) if name)
    for node in getattr(schema, "nodes", ()):
        name = dead_letter_topic_name(node.topic, getattr(node, "dead_letter", None))
        if name:
            owned.add(name)
    candidates = [
        (edge, name) for edge in getattr(schema, "edges", ())
        if getattr(edge, "dead_letter", None) is not None and edge.dead_letter.topic is None
        and edge_dead_letter_topic_name(edge) is not None
        for name in _former_dead_letter_names(edge) if name not in owned and name not in set(accepted)
    ]
    if not candidates:
        return []
    existing = admin_factory({"bootstrap.servers": bootstrap_servers}).list_topics(timeout=10).topics
    present = [(edge, name) for edge, name in candidates if name in existing and getattr(existing[name], "error", None) is None]
    if not present:
        return []
    consumer = consumer_factory({
        "bootstrap.servers": bootstrap_servers, "group.id": f"graph-loader-dlq-preflight-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    stale = []
    try:
        for edge, name in present:
            records = 0
            for partition in sorted(getattr(existing[name], "partitions", {}) or {}):
                low, high = consumer.get_watermark_offsets(TopicPartition(name, int(partition)), timeout=10)
                records += max(0, high - low)
            if records:
                stale.append((edge.type, name, records, edge_dead_letter_topic_name(edge)))
    finally:
        consumer.close()
    return stale


def _require_no_stale_dead_letter_topics(schema, bootstrap_servers: str, *, accepted=()) -> None:
    """Refuse to start while a type's former default DLQ still holds records (v7-S4 S4), except
    the names given with ``start --accept-former-dlq``."""
    stale = _stale_dead_letter_topics(schema, bootstrap_servers, accepted=accepted)
    if stale:
        raise ValueError("; ".join(
            f"relationship type {edge_type} now resolves its DLQ to {resolved}, but its former default DLQ "
            f"{name} still holds {records} record(s): set `dead_letter.topic: {name}` on {edge_type} to keep "
            f"using it (its parking lot and parked-pair index follow), or replay/drain and delete {name} first "
            f"(or pass --accept-former-dlq {name} if it is kept for audit or owned elsewhere)"
            for edge_type, name, records, resolved in stale
        ))


def _dlq_index_leftovers(schema, bootstrap_servers: str, *, accepted=(), admin_factory=None, index_factory=None):
    """``(edge, index topic, leftovers)`` for every DLQ-enabled relationship type with a parked-pair
    index (no ``order_by``) whose index still holds entries of a topic the type no longer reads, or of
    a topic now bound to another label pair (final follow-ups S2; see
    :func:`neo4j_graph_loader.loader.parked_pair_index.index_leftovers`). Types in ``accepted`` are skipped.

    Cheap where it can be: it reads only the compacted index topic (one entry per still unresolved
    DLQ'd record, what every loader rebuilds at start), never the DLQ itself. So it cannot see the
    DLQ of an ``order_by`` type (no index), bulk opt-in rows (never indexed), or a rebound topic of a
    single-pair type (its entries carry no label pair); USAGE documents those."""
    from neo4j_graph_loader.loader.parked_pair_index import (
        ParkedPairIndex, describe_index_leftovers, index_leftovers, leftover_refuses_start,
    )

    candidates = [
        edge for edge in getattr(schema, "edges", ())
        if edge_dead_letter_topic_name(edge) is not None and edge.order_by is None and edge.type not in set(accepted)
    ]
    if not candidates:
        return []
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient as admin_factory
    existing = admin_factory({"bootstrap.servers": bootstrap_servers}).list_topics(timeout=10).topics
    if index_factory is None:
        def index_factory(edge, topic):
            return ParkedPairIndex(
                topic=topic, bootstrap_servers=bootstrap_servers, writable=False, sequenced=edge.is_multi_topic,
                pair_of_topic=edge.pair_labels_for_topic if edge.is_multi_pair else None,
            )
    found = []
    for edge in candidates:
        topic = edge_parked_pair_index_topic_name(edge)
        if topic not in existing or getattr(existing[topic], "error", None) is not None:
            continue
        index = index_factory(edge, topic)
        try:
            index.start()
            leftovers = index_leftovers(index, edge)
        finally:
            index.stop()
        refusing = [leftover for leftover in leftovers if leftover_refuses_start(edge, leftover)]
        harmless = [leftover for leftover in leftovers if not leftover_refuses_start(edge, leftover)]
        if harmless:
            # A single-pair type's leftovers of a removed topic: written through its one pair, so only a warning.
            logger.warning(
                "stage=launch edge=%s reason=index_holds_entries_the_type_no_longer_binds %s", edge.type,
                describe_index_leftovers(edge, harmless),
            )
        if refusing:
            found.append((edge, topic, refusing))
    return found


def _require_no_dlq_leftovers(schema, bootstrap_servers: str, *, accepted=()) -> None:
    """Refuse to start while a type's parked-pair index holds entries of removed or rebound topics
    whose rows the type can no longer write (final follow-ups S2; a single-pair type's leftovers of a removed
    topic only warn), except the types given with ``start --accept-dlq-leftovers``."""
    from neo4j_graph_loader.loader.parked_pair_index import describe_index_leftovers

    found = _dlq_index_leftovers(schema, bootstrap_servers, accepted=accepted)
    if found:
        raise ValueError("; ".join(
            f"relationship type {edge.type}: its parked-pair index {topic} holds "
            f"{describe_index_leftovers(edge, leftovers)}: drain the DLQ with the previous schema first, or pass "
            f"--accept-dlq-leftovers {edge.type} to start anyway"
            for edge, topic, leftovers in found
        ))


def _missing_schema_topics(schema, bootstrap_servers: str) -> list[str]:
    """Return the node and relationship topics the schema declares that the broker lacks.

    A multi-topic relationship type contributes each of its topics. Uses the same
    ``AdminClient.list_topics`` call as :func:`_ensure_control_topic`; a topic whose
    metadata carries an error (e.g. unknown-topic) counts as missing. An unreachable
    broker raises.
    """
    from confluent_kafka.admin import AdminClient

    declared = schema_data_topics(schema)
    existing = AdminClient({"bootstrap.servers": bootstrap_servers}).list_topics(timeout=10).topics
    return [
        topic for topic in declared
        if topic not in existing or getattr(existing[topic], "error", None) is not None
    ]


def _preflight_stream_topics(schema, bootstrap_servers: str) -> bool:
    """Fail fast when a stream run's input topics do not exist yet.

    Without this, the loaders start, hit ``relationship topic metadata
    unavailable`` and exit. Topics are never auto-created: the partition
    count is the producer's choice. Returns False (after logging an
    actionable error) when any declared topic is missing or the broker
    cannot be queried; the caller must then launch nothing.
    """
    # Schema-side problems (not broker problems) must surface as themselves, so the
    # declared list is built outside the broker try block.
    schema_data_topics(schema)
    try:
        missing = _missing_schema_topics(schema, bootstrap_servers)
    except Exception as exc:
        logger.error("stage=launch reason=topic preflight failed: could not list topics on %s: %s",
                     bootstrap_servers, exc)
        return False
    if missing:
        logger.error(
            "stage=launch reason=missing input topics: %s. Create or publish them first (the producer chooses "
            "the partition count; graph-loader does not create them), then rerun.",
            ", ".join(missing),
        )
        return False
    return True


def _ensure_control_topic(bootstrap_servers: str) -> None:
    """Create the loaders' control topic when the broker does not auto-create topics."""
    from confluent_kafka.admin import AdminClient, NewTopic

    from neo4j_graph_loader.loader.control import CONTROL_TOPIC

    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        if CONTROL_TOPIC in admin.list_topics(timeout=10).topics:
            return
        for future in admin.create_topics([NewTopic(CONTROL_TOPIC, num_partitions=1, replication_factor=-1)]).values():
            future.result(30)
        logger.info("stage=launch created control topic %s", CONTROL_TOPIC)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            logger.warning("stage=launch control topic %s not ensured: %s", CONTROL_TOPIC, exc)


def _ensure_dead_letter_topics(schema, bootstrap_servers: str, *, include_edges: bool = True) -> None:
    """Create every configured DLQ topic before any other side effect of ``start``.

    Unlike ``_ensure_control_topic``'s warn-and-continue (the control topic
    is best-effort; loaders tolerate its brief absence), this is
    fail-closed: any exception -- an unreachable broker, or Story 3's
    ``ensure_dead_letter_topic`` rejecting a pre-existing topic that
    violates the DLQ retention/size policy -- propagates to the caller and
    aborts ``_handle_start`` before any container starts. A run that cannot
    durably DLQ a malformed record must not start, matching bulk mode's
    existing "fail closed on setup" philosophy (DDL application). The
    caller runs this before DDL, the edge planner, and image builds, not
    just before container launch, so a misconfiguration is reported before
    minutes of unrelated work rather than after.

    ``include_edges=False`` (planned mode) skips relationship DLQ topics
    entirely: ``planned_writer.py``/``edge_planner.py`` never read
    ``dead_letter`` (docs/USAGE.md section 12), so a misconfigured or
    policy-violating edge DLQ topic that a planned run would never write to
    must not block that run. Node DLQ topics are always provisioned, since
    every mode's node loaders honor ``dead_letter``.

    Deduplicates by resolved topic name. Story 2's schema validation
    already rejects two entities sharing one DLQ topic at ``load_schema()``
    time, so this is a defensive assertion against that invariant, not a
    reachable branch in practice.
    """
    # (entity name the default DLQ name derives from, dead_letter, source topics). A
    # relationship type has ONE DLQ for all its topics (v7-S4), sized to the widest topic;
    # always the schema's parent edge, never a precedence sub-stage's single-topic copy.
    entities = [
        (dead_letter_topic_name(node.topic, getattr(node, "dead_letter", None)), [node.topic])
        for node in getattr(schema, "nodes", ())
    ]
    if include_edges:
        entities.extend(
            (edge_dead_letter_topic_name(edge) if getattr(edge, "dead_letter", None) is not None else None,
             list(edge.topics))
            for edge in getattr(schema, "edges", ())
        )
    seen: set[str] = set()
    for resolved_topic, source_topics in entities:
        if resolved_topic is None or resolved_topic in seen:
            continue
        seen.add(resolved_topic)
        ensure_dead_letter_topic(bootstrap_servers, resolved_topic, source_topics=source_topics)
        logger.info(
            "stage=launch ensured dead-letter topic %s (source=%s)", resolved_topic, ",".join(source_topics),
        )


def _ensure_retry_topics(schema, bootstrap_servers: str) -> None:
    """Provision every streaming, DLQ-enabled edge type's parking-lot
    topic, plus the parked-pair-index topic for the non-``order_by``
    subset of those (P5-S4 Story 6) -- the retry consumer's own durable
    state.

    Review finding M17: an ``order_by``-configured edge type never uses
    the parked-pair index (Decisions §10 -- it uses a conditional ``SET``
    instead), but it still parks after ``max_retry_attempts`` exactly like
    any other edge type, and the retry consumer publishes that park to
    ``parked_topic_name(...)`` regardless of ``order_by``. Skipping the
    parking-lot topic for ``order_by`` types left that publish targeting
    a topic that would never exist (``allow.auto.create.topics=False``),
    so the park would fail every time and the record would churn forever
    at max attempts, never actually parking. Only the parked-pairs index
    topic is ``order_by``-conditional now.

    Skipped entirely for a run with no DLQ-enabled edge type at all
    (Decisions §1: bulk and planned runs have no live retry consumer to
    protect). Same fail-closed philosophy as
    :func:`_ensure_dead_letter_topics`: a run that cannot durably park a
    permanently-unresolvable row must not start.
    """
    seen: set[str] = set()
    for edge in getattr(schema, "edges", ()):
        if getattr(edge, "dead_letter", None) is None:
            continue
        dlq_topic = edge_dead_letter_topic_name(edge)
        if dlq_topic is None:
            continue
        parked_topic = edge_parked_topic_name(edge)
        if parked_topic is not None and parked_topic not in seen:
            seen.add(parked_topic)
            ensure_dead_letter_topic(bootstrap_servers, parked_topic, source_topic=dlq_topic)
            logger.info("stage=launch ensured parking-lot topic %s (source=%s)", parked_topic, dlq_topic)
        if getattr(edge, "order_by", None) is not None:
            continue
        parked_pairs_topic = edge_parked_pair_index_topic_name(edge)
        if parked_pairs_topic is not None and parked_pairs_topic not in seen:
            seen.add(parked_pairs_topic)
            ensure_parked_pair_index_topic(bootstrap_servers, parked_pairs_topic, source_topic=dlq_topic)
            logger.info(
                "stage=launch ensured parked-pair index topic %s (source=%s)", parked_pairs_topic, dlq_topic,
            )


def _resolve_node_replicas(schema, args: argparse.Namespace) -> dict[str, int]:
    """Per-label bulk node-loader replicas: YAML values, else derived from topic sizes."""
    nodes = list(schema.nodes)
    min_records = getattr(args, "node_replicas_min_records", auto_settings.DEFAULT_NODE_REPLICAS_MIN_RECORDS)
    if args.mode != "bulk" or not min_records or all(auto_settings.replicas_are_explicit(node) for node in nodes):
        return {node.label: node.replicas for node in nodes}
    try:
        sizes = auto_settings.topic_sizes(
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), [node.topic for node in nodes],
        )
    except Exception as exc:
        logger.warning("stage=launch node replicas from the schema: node topic sizes unavailable: %s", exc)
        return {node.label: node.replicas for node in nodes}
    replicas = auto_settings.auto_node_replicas(
        nodes, sizes, min_records=min_records,
        max_replicas=getattr(args, "max_node_replicas", auto_settings.DEFAULT_MAX_NODE_REPLICAS),
    )
    logger.info("stage=launch node replicas %s", " ".join(f"{label}={count}" for label, count in replicas.items()))
    return replicas


def _merge_deprecated_lease_flags(args: argparse.Namespace) -> None:
    """Resolve ``--bucket-lease-*`` (current) vs. ``--lease-*`` (deprecated).

    Both flags of a pair now default to ``None`` at parse time (P6-S2/P5-S1
    review finding S1-1), so this is the *only* place either one's final
    value is decided -- every reader (``_lease_launch_kwargs``,
    ``_select_keyed_lease_labels``, ``_start_bucket_lease_server``,
    ``_validate_edge_lease_settings``, ...) sees ``args.lease_buckets`` etc.
    only after this function has run:

    * neither given -> the schema-independent ``DEFAULT_BUCKET_LEASE_*``;
    * only one given -> that value (the deprecated one logs once);
    * both given and equal -> that value (still logs once: the deprecated
      flag was used, even though it agrees with the new one);
    * both given and different -> a hard configuration error, since a
      silent "one wins" would let an operator's typo go unnoticed.
    """
    for old_flag, new_flag, deprecated_attr, current_attr, default in (
        (
            "--lease-buckets", "--bucket-lease-buckets", "lease_buckets_deprecated", "lease_buckets",
            DEFAULT_BUCKET_LEASE_BUCKETS,
        ),
        (
            "--lease-outer-batch-size", "--bucket-lease-outer-batch-size",
            "lease_outer_batch_size_deprecated", "lease_outer_batch_size",
            DEFAULT_BUCKET_LEASE_OUTER_BATCH_SIZE,
        ),
        (
            "--lease-keyed-max-nodes", "--bucket-lease-keyed-max-nodes",
            "lease_keyed_max_nodes_deprecated", "lease_keyed_max_nodes",
            DEFAULT_BUCKET_LEASE_KEYED_MAX_NODES,
        ),
    ):
        deprecated_value = getattr(args, deprecated_attr, None)
        current_value = getattr(args, current_attr, None)
        if deprecated_value is not None:
            _warn_deprecated_flag(old_flag, new_flag)
            if current_value is not None and current_value != deprecated_value:
                raise ValueError(
                    f"{old_flag} (deprecated, ={deprecated_value}) conflicts with "
                    f"{new_flag} (={current_value}); pass only one"
                )
            setattr(args, current_attr, deprecated_value)
        elif current_value is None:
            setattr(args, current_attr, default)


def handle_start(args: argparse.Namespace) -> int:
    cleanups: list[Callable[[], None]] = []
    try:
        return _handle_start(args, cleanups)
    finally:
        for cleanup in reversed(cleanups):
            try:
                cleanup()
            except Exception as exc:  # pragma: no cover - best effort
                logger.warning("stage=shutdown cleanup failed: %s", exc)


def _handle_start(args: argparse.Namespace, cleanups: list) -> int:
    # P6-S5 Story 1: one SIGINT/SIGTERM -> Event handler pair for the whole
    # run, bulk and stream alike (bulk used to have none, so SIGTERM killed
    # the CLI outright and orphaned every launched container).  Registered
    # first, so it is restored last, after every container/metrics cleanup.
    shutdown = _install_shutdown_handlers(cleanups, _execution_of(args))
    try:
        if getattr(args, "runtime", None) is None:
            _merge_deprecated_lease_flags(args)
    except ValueError as exc:
        logger.error("stage=launch reason=%s", exc)
        return 1
    logger.info(f"Starting pipeline in {args.mode} mode with config {args.config}")

    # One metrics server for the whole CLI process lifetime, covering bulk,
    # fleet-wide stream, and the isolated stream phase
    # (_run_isolated_relationship_stream_phase) alike, since they all run
    # inside this one _handle_start() call rather than in two separate
    # main() branches. Falls back to an ephemeral port if 9464 is taken
    # (the CLI is not guaranteed exclusive use of any port the way a
    # container is). The cleanup is registered in the same statement pair
    # as the start, before any of this function's many early `return 1`
    # paths, so every path - including a schema-load failure - stops it.
    #
    # P6-S5, found during P6-S3 (not the same as P6-S5's own Story 6,
    # which is the test proving a SIGTERM-driven stream run leaves a
    # parseable metrics-cli.prom snapshot - tests/test_cli_shutdown.py):
    # the CLI's metrics server used to bind 127.0.0.1 unconditionally,
    # so consumer-lag/planner metrics were never reachable from a
    # Prometheus container even over the host gateway (docs/MONITORING.md
    # Sec 7's monitoring/prometheus.yml "graph-loader-cli" job, prepared
    # but inert until this option existed). GRAPH_LOADER_METRICS_BIND_ADDR
    # is an explicit, off-by-default opt-in: 127.0.0.1 (unchanged default) stays a pure
    # loopback-only bind; a non-default value (typically "0.0.0.0" to
    # answer the host gateway) is the operator's own choice to expose an
    # unauthenticated Prometheus endpoint beyond localhost, so it is
    # logged at WARNING, not silently honored - this is the host's own
    # workstation/CI runner, not a Docker-network-isolated container, and
    # start_metrics_server's own docstring already explains why containers
    # and the CLI cannot share one default.
    # Review finding: an explicitly-set-but-empty value ("") is treated
    # as "unset" (the loopback-only default), not as whatever `addr=""`
    # would mean to the underlying bind call - an operator's shell/env
    # tooling accidentally exporting an empty string must not silently
    # become "bind every interface."
    cli_metrics_bind_addr = os.environ.get("GRAPH_LOADER_METRICS_BIND_ADDR") or "127.0.0.1"
    if cli_metrics_bind_addr != "127.0.0.1":
        logger.warning(
            "stage=metrics reason=cli_metrics_bind_addr_overridden addr=%s "
            "detail=exposes an unauthenticated Prometheus endpoint beyond localhost",
            cli_metrics_bind_addr,
        )
    cli_process_metrics = metrics_module.process_metrics()
    cli_metrics_server = metrics_module.start_metrics_server(
        cli_process_metrics, kind="cli", name="-", addr=cli_metrics_bind_addr, allow_ephemeral_fallback=True,
        **({"snapshot_path": str(args.runtime.timings / "metrics-cli.prom")} if getattr(args, "runtime", None) is not None else {}),
    )

    def _stop_cli_metrics_server() -> None:
        metrics_module.write_metrics_snapshot(
            cli_process_metrics, (str(args.runtime.timings / "metrics-cli.prom") if getattr(args, "runtime", None) is not None
                                  else metrics_module.cli_snapshot_path()),
        )
        metrics_module.stop_metrics_server(cli_metrics_server)

    cleanups.append(_stop_cli_metrics_server)

    try:
        _validate_edge_mode_flags(args)
    except ValueError as exc:
        logger.error("stage=launch reason=%s", exc)
        return 1

    # Cheap argument checks before any side effect (DLQ setup, DDL, planner, image build).
    try:
        _validate_early_numeric_args(args)
    except ValueError as exc:
        logger.error("stage=launch reason=%s", exc)
        return 1

    # 1. Load schema
    try:
        schema = load_schema(args.config)
    except Exception as e:
        logger.error(f"Failed to load schema: {e}")
        return 1

    try:
        _resolve_bulk_edge_mode(schema, args)
        _require_supported_topic_conflicts(schema, args)
    except ValueError as exc:
        logger.error("stage=launch reason=%s", exc)
        return 1

    try:
        explicit_run_id = _validated_run_id(getattr(args, "run_id", None))
    except ValueError as exc:
        logger.error("stage=launch reason=invalid run id: %s", exc)
        return 1
    if getattr(args, "edge_plan", None):
        try:
            _validate_edge_plan_settings(schema, args)
        except ValueError as exc:
            logger.error("stage=launch reason=%s", exc)
            return 1
    elif getattr(args, "edge_leases", False):
        try:
            _validate_edge_lease_settings(schema, args)
        except ValueError as exc:
            logger.error("stage=launch reason=%s", exc)
            return 1

    edge_configs = tuple(getattr(schema, "edges", ()))
    planned = bool(getattr(args, "edge_plan", None)) and bool(edge_configs)
    neo4j_cores = None
    planner = None

    # Reject input-topic/DLQ policy errors before image resolution. Provision
    # DLQs after image/topology checks and before DDL/planning: a run that cannot durably DLQ a
    # malformed record must not start, and should fail fast, not after
    # minutes of unrelated work. Planned mode's writers ignore
    # `dead_letter` entirely (docs/USAGE.md section 12), so this skips
    # provisioning edge DLQ topics for a planned run -- a misconfigured
    # edge DLQ topic that planned mode will never write to must not block
    # a planned run.
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    # Stream loaders exit on a missing input topic, so check them first --
    # before any topic is created or container launched.  Bulk is not
    # checked: its loaders are not known to fail the same way.
    if args.mode == "stream" and not _preflight_stream_topics(schema, kafka_bootstrap_servers):
        return 1
    if not planned:
        try:
            _require_no_stale_dead_letter_topics(
                schema, kafka_bootstrap_servers, accepted=tuple(getattr(args, "accept_former_dlq", None) or ()),
            )
            _require_no_dlq_leftovers(
                schema, kafka_bootstrap_servers, accepted=tuple(getattr(args, "accept_dlq_leftovers", None) or ()),
            )
        except ValueError as exc:
            logger.error("stage=launch reason=%s", exc)
            return 1
        except Exception as exc:
            logger.error("stage=launch reason=could not check former dead-letter topics: %s", exc)
            return 1
    # Resolve and pin every required component before service-side changes.
    try:
        docker_service = _docker_service_for_run(args)
        if _runtime_backend(args) == "process":
            docker_service.register_cli()
            cleanups.append(docker_service.close)        # registered first, so it runs last (after planner and node cleanups)
            docker_service.set_stopping_check(shutdown.is_set)
            docker_service.set_cli_metrics(cli_metrics_server)
            if getattr(args, "metrics_port", None) is not None:
                if not _start_aggregated_metrics(args, docker_service, cli_process_metrics, cleanups):
                    return 1
            if getattr(args, "health_addr", None):
                docker_service.start_health(args.health_addr)
        if getattr(args, "skip_image_build", False):
            roles = ["node"] + (["edge"] if edge_configs else [])
            if edge_configs and args.mode == "stream" and getattr(args, "stream_clock_rotation", False):
                roles.append("clock")
            docker_service.prepare_images(roles, local_only=True)
        else:
            docker_service.build_image()
            if edge_configs:
                docker_service.build_edge_image()
                if args.mode == "stream" and getattr(args, "stream_clock_rotation", False):
                    docker_service.build_clock_image()
    except Exception as exc:
        logger.error("stage=image reason=%s", exc)
        return 1

    if getattr(args, "runtime", None) is not None:
        try:
            docker_service.preflight_topology(
                args.network, plan_path=(args.edge_plan if getattr(args, "edge_plan", None) not in (None, AUTO_EDGE_PLAN) else None))
        except Exception as exc:
            logger.error("stage=docker-preflight reason=%s", exc)
            return 1
    _start_status_telemetry(args, schema, docker_service, cleanups)

    # 2. Get DB config
    uri, user, password = get_neo4j_credentials()

    try:
        _ensure_dead_letter_topics(schema, kafka_bootstrap_servers, include_edges=not planned)
        # P5-S4 Story 6: the retry consumer's own durable topics --
        # streaming only (Decisions §1), never for a planned/bulk run.
        if args.mode == "stream" and not planned:
            _ensure_retry_topics(schema, kafka_bootstrap_servers)
    except Exception as exc:
        logger.error("stage=launch reason=dead-letter topic setup failed: %s", exc)
        return 1

    # 3. Apply schema
    try:
        driver = get_neo4j_driver(uri, user, password)
        try:
            apply_schema(driver, schema)
            if planned and getattr(args, "plan_writers", None) is None:
                neo4j_cores = auto_settings.probe_neo4j_cores(driver)
        finally:
            driver.close()
        logger.info("Schema initialization complete. Ready for ingestion.")
    except SchemaInitializationError as e:
        logger.error(f"Schema initialization failed: {e}")
        return 1

    if shutdown.is_set():
        logger.warning("stage=shutdown reason=stop requested before launch; nothing was started")
        return 1

    parent_run_id = explicit_run_id or uuid.uuid4().hex
    if planned:
        if getattr(args, "plan_writers", None) is None:
            args.plan_writers = auto_settings.default_plan_writers(neo4j_cores)
            logger.info("stage=launch planned writers=%s (Neo4j cores=%s)", args.plan_writers,
                        neo4j_cores or f"unknown, assumed {auto_settings.DEFAULT_NEO4J_CORES}")
        if args.edge_plan == AUTO_EDGE_PLAN:
            # Plan from the edge topics while images build and nodes load.
            if _runtime_backend(args) == "process":
                from neo4j_graph_loader.orchestrator.cgroup_budget import SizingError
                try:
                    docker_service.apply_plan_budget(args, schema)
                except SizingError as exc:
                    logger.error("stage=sizing reason=%s", exc)
                    return 1
            try:
                if getattr(args, "replace_stale_plan_topic", False) and explicit_run_id is not None \
                        and not getattr(args, "plan_topic", None):
                    replace_stale_chunk_topic(
                        _docker_service_for_run(args), parent_run_id,
                        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                    )
                planner = start_kafka_edge_planner(
                    schema, args, run_id=parent_run_id, writers=args.plan_writers,
                    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                    stop_requested=_shutdown_requested,
                    **({"supervision": docker_service.planner_supervision()} if _runtime_backend(args) == "process" else {}),
                )
            except Exception as exc:
                logger.error("stage=edge-plan reason=planner could not start: %s", exc)
                return 1
            keep = bool(getattr(args, "keep_plan_topic", False))
            cleanups.append(lambda: planner.close(delete_topic=not keep))
            if getattr(args, "_status_poller", None) is not None:
                # Exact once planning has finished (0 before that is "not known", so it is reported as null).
                args._status_poller.add_source(lambda tracker, now: tracker.update_totals(rejected=(
                    status_model.observed(planner.rejected, status_model.timestamp(now), "planner")
                    if getattr(planner, "result", None) is not None else None)))
            if _runtime_backend(args) == "process":
                # An ordered planner stop comes before any process-group signal the runtime sends.
                docker_service.add_pre_signal_hook(lambda: planner.close(delete_topic=not keep))
    elif getattr(args, "edge_plan", None) == AUTO_EDGE_PLAN:
        args.edge_plan = None

    if shutdown.is_set():
        logger.warning("stage=shutdown reason=stop requested before launch; nothing was started")
        return 1

    is_bulk = args.mode == "bulk"
    bulk_timeout_seconds = getattr(args, "bulk_timeout_seconds", 300.0)  # validated early (> 0 for bulk)
    edge_leases = bool(getattr(args, "edge_leases", False)) and bool(edge_configs)
    if not is_bulk and edge_configs:
        # Stable stream groups: refuse a second live fleet of this schema and
        # migrate a legacy run-scoped group's offsets, before anything launches.
        try:
            _prepare_stream_consumer_groups(
                schema, args, explicit_run_id=explicit_run_id, bootstrap_servers=kafka_bootstrap_servers,
            )
        except StreamGroupWaitStopped as exc:
            logger.warning("stage=shutdown reason=%s; nothing was started", exc)
            return 1
        except Exception as exc:
            logger.error("stage=consumer-groups reason=%s", exc)
            return 1
    # Planned mode: one dispatcher-driven writer pool loads every edge type.
    run_id = parent_run_id if is_bulk or explicit_run_id is not None else None
    if run_id is None and edge_configs and not is_bulk:
        # E15.F2.S12: a stream with a relationship fleet and no --run-id used to let the fleet generate its own id, so the edge loaders, the
        # lease server and the retry consumer carried one that the status document and the other roles did not. The id the product would
        # generate anyway is adopted once, here, as the execution's run id (the fleet picks it up through args.run_id).
        args.run_id = parent_run_id
        run_id = parent_run_id
    if run_id is not None and _execution_of(args) is not None:
        _execution_of(args).set_run_id(run_id)
        set_log_context(run_id=run_id)
    _ensure_control_topic(kafka_bootstrap_servers)
    node_replicas = _resolve_node_replicas(schema, args)
    started_containers = []
    tracked_containers = {}
    monitor = None
    drain_started = False
    # Stream mode: node writes are leased like relationship writes (zero
    # lock waits between them), so the lease server must be up BEFORE the
    # node loaders, which receive its address and scheme.
    stream_lease_container = None
    node_lease_kwargs: dict = {}
    if not is_bulk and edge_configs and not getattr(args, "stream_clock_rotation", False):
        cleanups.append(lambda: _sweep_orphaned_lease_containers(docker_service, parent_run_id))
        try:
            stream_lease_container, node_lease_kwargs = _start_stream_lease_for_nodes(
                schema, docker_service, args, run_id=parent_run_id,
            )
        except Exception as exc:
            logger.error("stage=lease-server run_id=%s reason=%s", parent_run_id, exc)
            return 1
        cleanups.append(lambda: (
            _stop_exact_containers([stream_lease_container]),
            _remove_exact_containers([stream_lease_container]),
        ))

    def launch_label(node_config):
        return docker_service.run_node_loader(
            node_label=node_config.label,
            topic=node_config.topic,
            mode=args.mode,
            config_path=args.config,
            replicas=node_replicas[node_config.label],
            network=args.network,
            **({"run_id": run_id} if run_id is not None else {}),
            **node_lease_kwargs,
        )

    # Labels are independent: launch them concurrently (each call launches
    # its own replicas concurrently too), then account for them in schema
    # order.  Any failure stops every container that did start.
    node_configs = list(schema.nodes)
    with ThreadPoolExecutor(
        max_workers=max(1, min(len(node_configs), _MAX_PARALLEL_LABEL_LAUNCHES)),
        thread_name_prefix="node-launch",
    ) as launch_executor:
        launch_futures = [launch_executor.submit(launch_label, node_config) for node_config in node_configs]
        launch_outcomes = []
        for future in launch_futures:
            try:
                launch_outcomes.append((future.result(), None))
            except Exception as exc:
                launch_outcomes.append((None, exc))
    launch_failed = False
    for node_config, (containers, launch_error) in zip(node_configs, launch_outcomes):
        try:
            if launch_error is not None:
                raise launch_error
            # Preserve every returned object before validating the result so a
            # malformed launch response is still safely cleaned up.
            started_containers.extend(containers)
            if len(containers) != node_replicas[node_config.label]:
                raise RuntimeError(
                    f"loader launch returned {len(containers)} replicas for {node_config.label}; "
                    f"expected {node_replicas[node_config.label]}"
                )
            if is_bulk:
                for replica_index, container in enumerate(containers):
                    tracked_containers[(node_config.label, str(replica_index))] = container
            logger.info(f"Started node loader for {node_config.label}")
        except Exception as e:
            logger.error(f"Failed to launch node loader for {node_config.label}: {e}")
            launch_failed = True
    if launch_failed or shutdown.is_set():
        # A stop requested mid-launch: the in-flight launches were allowed to
        # finish (a Docker call is never cancelled halfway), so everything
        # Docker started is in started_containers and is stopped here.
        if not launch_failed:
            logger.warning("stage=shutdown reason=stop requested during node launch")
        _stop_exact_containers(started_containers)
        if is_bulk:
            _remove_exact_containers(started_containers)
        return 1

    if not is_bulk:
        if not edge_configs:
            if _runtime_backend(args) == "process":
                return _supervise_node_loaders_only(started_containers, shutdown, cleanups, on_exit=_container_exit_recorder(args))
            return 0                  # Docker mode: the containers keep running after the command returns (documented)
        # Every exit from here on - graceful stop (result 0), failure, an
        # isolation-policy rejection or an escaping exception - stops
        # exactly the node loaders this run started.  Neither relationship
        # orchestration path owns them, so the cleanup lives here.
        cleanups.append(lambda: (
            _stop_exact_containers(started_containers, stop_timeout=_NODE_STOP_TIMEOUT_SECONDS),
            _remove_exact_containers(started_containers),
        ))
        # P5-S1 Story 6: the bucket-lease fleet is streaming's default;
        # --stream-clock-rotation is the deprecated, one-release-only
        # escape hatch to the Phase 4 clock/rotation path, byte-for-byte
        # unchanged (this is the *only* call-site change touching that
        # path -- _run_relationship_orchestration and everything it calls
        # are not otherwise edited by this story).
        node_watcher = _NodeLoaderWatcher(started_containers, shutdown, on_exit=_container_exit_recorder(args)).start()
        try:
            if getattr(args, "stream_clock_rotation", False):
                logger.warning(
                    "stage=deprecation flag=--stream-clock-rotation "
                    "reason=Phase 4 clock/rotation streaming path; removed after one release"
                )
                result = _run_relationship_orchestration(
                    schema, docker_service, args, parent_run_id=parent_run_id,
                )
            else:
                result = _run_lease_relationship_fleet(
                    schema, docker_service, args, parent_run_id=parent_run_id,
                    prestarted_lease_container=stream_lease_container,
                    node_containers=started_containers,
                )
        finally:
            node_watcher.stop()
        return 1 if node_watcher.failed else result

    lease_container = None
    try:
        if edge_leases:
            lease_container = _start_bucket_lease_server(
                docker_service, args, run_id=parent_run_id, schema=schema,
            )
        monitor = BulkMonitor(
            schema,
            run_id,
            set(tracked_containers),
            tracked_containers,
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            abort_requested=shutdown.is_set,
        )
        isolation_plan = build_relationship_isolation_plan(edge_configs)
        co_scheduled_self_references = _co_scheduled_self_references(edge_configs, args)
        shared_types = set(isolation_plan.shared_edge_types) | co_scheduled_self_references
        if planned:
            # Planned chunks are node-disjoint across every type, including
            # self-references, so the whole relationship load is one phase.
            shared_types = {edge.type for edge in edge_configs}
        shared_edges = tuple(edge for edge in edge_configs if edge.type in shared_types)
        if shared_edges:
            node_labels = {node.label for node in schema.nodes}
            missing_endpoints = {
                label for edge in shared_edges
                for label in _endpoint_labels_of(edge)
                if label not in node_labels
            }
            if missing_endpoints:
                raise ValueError(f"relationship endpoints lack node loaders: {sorted(missing_endpoints)}")
            ready_labels: set[str] = set()
            ready_condition = Condition()
            scheduler_cancelled = Event()
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="bulk-scheduler") as scheduler_executor:
                if planned:
                    edge_future = scheduler_executor.submit(
                        run_planned_edge_phase, schema, docker_service, args,
                        parent_run_id=parent_run_id, ready_labels=ready_labels,
                        ready_condition=ready_condition, cancelled=scheduler_cancelled,
                        stop_requested=shutdown.is_set,
                    )
                else:
                    edge_future = scheduler_executor.submit(
                        _run_completion_driven_bulk_edges,
                        shared_edges, docker_service, args, parent_run_id=parent_run_id,
                        consumer_group_prefix=schema.loading.consumer_group_id,
                        ready_labels=ready_labels, ready_condition=ready_condition,
                        cancelled=scheduler_cancelled,
                    )
                readiness_started = time.monotonic()

                def stop_label(label: str) -> None:
                    # Runs on a monitor worker thread; Docker only, never Kafka.
                    label_replicas = {
                        replica: container for replica, container in tracked_containers.items()
                        if replica[0] == label
                    }
                    failures = _stop_exact_containers(
                        label_replicas.values(),
                        identities={
                            id(container): f"label={node_label} replica={replica_id}"
                            for (node_label, replica_id), container in label_replicas.items()
                        },
                        stop_timeout=_NODE_STOP_TIMEOUT_SECONDS,
                    )
                    if failures:
                        raise RuntimeError(
                            f"unable to stop node label={label}: {'; '.join(failures)}"
                        )

                def publish_ready(label: str) -> None:
                    logger.info(
                        "stage=node-label-ready label=%s elapsed_s=%.3f",
                        label, time.monotonic() - readiness_started,
                        extra=structured("node-label-ready", label=label, elapsed_s=round(time.monotonic() - readiness_started, 3)),
                    )
                    if _execution_of(args) is not None:
                        _execution_of(args).update_type("node", label, ready=status_model.observed(
                            True, status_model.timestamp(time.time()), "node-monitor"))
                    with ready_condition:
                        ready_labels.add(label)
                        ready_condition.notify_all()

                def scheduler_failed() -> bool:
                    # P6-S5 Story 2: an operator stop aborts the node-drain
                    # wait on its next poll and cancels the dependent
                    # scheduler/planned phase (both poll scheduler_cancelled).
                    if shutdown.is_set():
                        scheduler_cancelled.set()
                    if scheduler_cancelled.is_set():
                        return True
                    if not edge_future.done():
                        return False
                    try:
                        return edge_future.result() != 0
                    except Exception:
                        return True

                try:
                    drain_started = True
                    # Each label proves its own exact assignment coverage and
                    # captures its own fixed boundary inside the driver, so a
                    # small label is not held behind every other label's
                    # consumer-group rebalances.
                    monitor.drive_label_readiness(
                        {node.label for node in schema.nodes}, bulk_timeout_seconds,
                        stop_label=stop_label, on_ready=publish_ready,
                        should_abort=scheduler_failed,
                    )
                    # Nodes normally finish long before edges do: keep bridging an
                    # operator stop to the dependent phase while waiting for it,
                    # or a signal here would be ignored until the phase ends.
                    while True:
                        try:
                            edge_result = edge_future.result(timeout=0.25)
                            break
                        except FuturesTimeoutError:
                            _beat()          # nodes done, waiting for the dependent phase: still supervising
                            if shutdown.is_set():
                                scheduler_cancelled.set()
                                with ready_condition:
                                    ready_condition.notify_all()
                    if edge_result != 0:
                        raise RuntimeError(
                            "planned bulk relationship phase failed" if planned
                            else "shared bulk relationship scheduler failed"
                        )
                except Exception:
                    scheduler_cancelled.set()
                    with ready_condition:
                        ready_condition.notify_all()
                    _stop_exact_containers(started_containers)
                    edge_future.result()
                    raise
            if planned:
                rejected = getattr(planner, "rejected", 0)
                if isinstance(rejected, int) and rejected:
                    logger.warning(
                        "stage=outcome rejected=%s malformed relationship record(s) were skipped; "
                        "see %s/%s-planner-*.jsonl", rejected, (args.runtime.rejections if getattr(args, "runtime", None) is not None else REJECTION_DIR), parent_run_id,
                        extra=structured("outcome", rejected=rejected, rejections_dir=str(args.runtime.rejections if getattr(args, "runtime", None) is not None else REJECTION_DIR)),
                    )
                return 0
            return _run_relationship_orchestration(
                schema, docker_service, args, parent_run_id=parent_run_id,
                shared_already_loaded=True,
                self_references_already_loaded=co_scheduled_self_references,
            )
        monitor.wait_for_assignment_coverage(bulk_timeout_seconds)
        monitor.capture_boundary()
        monitor.wait_for_completion(bulk_timeout_seconds)
        drain_started = True
        stop_failures = _stop_exact_containers(started_containers)
        if stop_failures:
            raise RuntimeError(f"unable to stop bulk loader fleet: {'; '.join(stop_failures)}")
        monitor.wait_for_drain_complete(bulk_timeout_seconds)
        monitor.verify_zero_lag()
        return _run_relationship_orchestration(
            schema, docker_service, args, parent_run_id=parent_run_id,
        ) if edge_configs else 0
    except Exception as exc:
        if shutdown.is_set():
            # An operator/signal stop is an expected outcome, not a failure.
            logger.warning("stage=shutdown Bulk run stopped by operator/signal request: %s", exc)
        else:
            logger.error("Bulk run failed: %s", exc)
        if not drain_started:
            _stop_exact_containers(started_containers)
        _log_failed_loader_diagnostics(started_containers)
        return 1
    finally:
        if monitor is not None:
            monitor.close()
        if lease_container is not None:
            _stop_bucket_lease_server(lease_container, **timing_kwargs(args))
        # Bulk node loaders run without Docker auto-remove (see
        # DockerService.run_node_loader) so a crashed replica's exit code and
        # logs survive for diagnosis; remove them once the run is over.
        _remove_exact_containers(started_containers)


def _default_run_dir() -> str:
    import tempfile
    return tempfile.gettempdir()


def _runtime_backend(args) -> str:
    return getattr(args, "runtime_backend", None) or DEFAULT_RUNTIME_BACKEND


def _build_runtime(backend: str, paths, args=None) -> Runtime:
    if backend == "docker":
        return DockerService(runtime=paths) if paths is not None else DockerService()
    if backend == "process":
        from neo4j_graph_loader.orchestrator.process_service import ProcessService
        return ProcessService(paths, metrics_addr=getattr(args, "metrics_addr", None) or "127.0.0.1",
                              child_logs=getattr(args, "child_logs", None) or "relay",
                              health_stale_after=getattr(args, "health_stale_after", None) or 30.0)
    raise RuntimeError(f"unknown runtime backend {backend!r}")


def _start_aggregated_metrics(args, service, cli_metrics, cleanups: list) -> bool:
    """``--metrics-port``: the one merged endpoint (CLI registry plus a scrape of every role); False after an error line."""
    from prometheus_client import generate_latest
    from neo4j_graph_loader.orchestrator.aggregated_metrics import AggregatedMetrics
    port = args.metrics_port
    if not 0 <= port <= 65535:
        logger.error("stage=runtime reason=--metrics-port must be between 0 and 65535, got %s", port,
                     extra=structured("runtime", reason="invalid --metrics-port", value=str(port)))
        return False
    host = service.metrics_host or "127.0.0.1"
    aggregate = AggregatedMetrics(host, port, own=lambda: generate_latest(cli_metrics.exposition_registry()),
                                  targets=service.lifecycle.build_targets)
    try:
        bound = aggregate.start()
    except OSError as exc:
        logger.error("stage=runtime reason=cannot bind the aggregated metrics endpoint %s:%s: %s", host, port, exc,
                     extra=structured("runtime", reason="aggregated metrics endpoint cannot bind", host=host, port=port))
        return False
    cleanups.append(aggregate.close)
    logger.info("stage=metrics-aggregate reason=ready url=http://%s:%s/metrics", host, bound,
                extra=structured("metrics-aggregate", host=host, port=bound, url=f"http://{host}:{bound}/metrics"))
    return True


def _docker_service_for_run(args) -> Runtime:
    """The run's runtime (backend chosen by ``--runtime``), built once per run."""
    paths = getattr(args, "runtime", None)
    service = getattr(args, "_docker_service", None)
    if service is None:
        service = _build_runtime(_runtime_backend(args), paths, args)
        if _execution_of(args) is not None:
            service.status = _execution_of(args)
            if hasattr(service, "lifecycle"):
                tracker = _execution_of(args)
                service.lifecycle.identity_provider = lambda: {"execution_id": tracker.execution_id, "run_id": tracker.run_id or ""}
        args._docker_service = service
    return service


def _docker_service_or_none(args=None) -> Runtime | None:
    """The selected runtime, or None (after a clear error) when it is unreachable."""
    try:
        return _build_runtime(_runtime_backend(args), None)
    except Exception as exc:
        logger.error("stage=docker reason=unreachable detail=%s", exc)
        return None


def _second_shell_registry_error(exc: Exception) -> int:
    logger.error("stage=runtime reason=%s", exc, extra=structured("runtime", reason=str(exc)))
    return 1


def handle_stop(args: argparse.Namespace) -> int:
    loader = args.loader
    logger.info("Stopping loader(s)...")
    if _runtime_backend(args) == "process" and getattr(args, "recover", False):
        from neo4j_graph_loader.orchestrator.process_service import recover_registered
        from neo4j_graph_loader.orchestrator.run_registry import RegistryError
        try:
            report = recover_registered(getattr(args, "run_dir", None) or _default_run_dir())
        except (RegistryError, OSError) as exc:
            return _second_shell_registry_error(exc)
        logger.info("stage=recover targets_cleared=%s (metrics/targets.json emptied so Prometheus file_sd drops the dead endpoints)", report["targets_cleared"])
        logger.info("stage=recover attempt=%s signalled=%s killed=%s remaining=%s", report["attempt_dir"], report["signalled"],
                    report["killed"], report["remaining"])
        retained = report["retained"]
        shown = ", ".join(retained[:50]) + (f", ... and {len(retained) - 50} more" if len(retained) > 50 else "")
        logger.info("stage=recover retained (never deleted) files=%s: %s", len(retained), shown or "nothing beyond the registry and logs")
        return 1 if report["remaining"] else 0
    if _runtime_backend(args) == "process":
        from neo4j_graph_loader.orchestrator.process_service import stop_registered
        from neo4j_graph_loader.orchestrator.run_registry import RegistryError
        try:
            stopped = stop_registered(getattr(args, "run_dir", None) or _default_run_dir(), loader)
        except (RegistryError, OSError) as exc:
            return _second_shell_registry_error(exc)
        if not stopped:
            logger.info("No matching loaders running.")
        for item in stopped:
            logger.info("Stop requested: %s", item)
        return 0
    docker_service = _docker_service_or_none(args)
    if docker_service is None:
        return 1
    docker_service.stop_node_loaders(node_label=loader)
    logger.info("Loaders stopped.")
    return 0


def _wants_run_status(args: argparse.Namespace) -> bool:
    """Run status (the new view) only when one of its options asks for it. Everything else, ``--run-dir`` alone
    included, is the container listing exactly as before (E14 documents ``status --run-dir`` as the registry listing)."""
    return any(getattr(args, name, None) for name in ("run", "execution_id", "json", "watch", "refresh_lag", "list"))


def _load_status(args: argparse.Namespace) -> tuple[dict, Path | None]:
    from neo4j_graph_loader.status import reader
    if getattr(args, "status_store", "json") == "sql" and not getattr(args, "run_dir", None) and args.execution_id:
        from neo4j_graph_loader.status import sql_store
        database = sql_store.SqlStatusStore()
        try:
            return database.read_status(args.execution_id), None
        except LookupError as exc:
            raise reader.StatusNotFound(str(exc)) from None
        finally:
            database.close()
    directory, _locator = reader.resolve(getattr(args, "run_dir", None), execution_id=args.execution_id)
    return reader.read_status(directory), directory


def handle_run_status(args: argparse.Namespace) -> int:
    """``graph-loader status`` for run status: one execution, a list, or a live view. See ``status/view.py`` for the
    exit codes (0 running or succeeded, 1 failed, 2 stale/dead/unknown, 3 stopped)."""
    from neo4j_graph_loader.status import lag as status_lag, listing, reader, view
    try:
        if args.list:
            rows = listing.list_runs(store=args.status_store, base=getattr(args, "run_dir", None), active=args.active,
                                     stale_after=args.stale_after)
            if args.json:
                sys.stdout.write(json.dumps(rows) + "\n")
            else:
                sys.stdout.write("".join(view.list_line(row) + "\n" for row in rows) or "no runs found\n")
            return 0
        previous = None
        while True:
            status, attempt = _load_status(args)
            extra_lines: list[str] = []
            extra: dict = {}
            if args.refresh_lag and attempt is not None:
                try:
                    refreshed = status_lag.refresh_lag(attempt, status, bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                                                       timeout=args.refresh_timeout)
                    status = status_lag.overlay(status, refreshed)
                    extra["lag_refreshed"] = True
                except status_lag.LagRefreshError as exc:
                    extra.update(lag_refreshed=False, lag_refresh_error=str(exc))
                    extra_lines.append(f"lag       not refreshed: {exc}")
            assessment = view.assess(status, stale_after=args.stale_after, attempt=attempt)
            document = view.with_reader(status, assessment, extra)
            if args.json:
                from neo4j_graph_loader.status import schemas
                from neo4j_graph_loader.status.validate import validate as validate_document
                validate_document(document, schemas.STATUS_SCHEMA)
            frame = json.dumps(document, separators=(",", ":")) + "\n" if args.json else view.render_text(status, assessment, extra_lines=extra_lines)
            if frame != previous:
                sys.stdout.write(frame if args.json or not args.watch or previous is None else "\n" + frame)
                sys.stdout.flush()
                previous = frame
            if not args.watch or status["state"] in ("succeeded", "failed", "stopped") or assessment["stale"]:
                return assessment["exit_code"]
            time.sleep(max(0.05, args.interval))
    except KeyboardInterrupt:
        return 130
    except reader.StatusNotFound as exc:
        logger.error("stage=status reason=%s", exc)
        return 2
    except Exception as exc:  # noqa: BLE001 - StatusDataError, StatusStoreError, database errors: unknown, not a crash
        logger.error("stage=status reason=%s", exc)
        return 2


def handle_status(args: argparse.Namespace) -> int:
    if _wants_run_status(args):
        code = handle_run_status(args)
        if not getattr(args, "containers", False):
            return code
        legacy = _handle_container_status(args)
        return code or legacy
    return _handle_container_status(args)


def _handle_container_status(args: argparse.Namespace) -> int:
    logger.info(f"Checking status for config: {args.config}")
    if _runtime_backend(args) == "process":
        from neo4j_graph_loader.orchestrator.process_service import describe_registered
        from neo4j_graph_loader.orchestrator.run_registry import RegistryError
        try:
            containers = describe_registered(getattr(args, "run_dir", None) or _default_run_dir())
        except (RegistryError, OSError) as exc:
            return _second_shell_registry_error(exc)
    else:
        docker_service = _docker_service_or_none(args)
        if docker_service is None:
            return 1
        containers = docker_service.list_node_loaders()
    if not containers:
        logger.info("No loaders running.")
    for c in containers:
        logger.info(f"Loader {c['name']} ({c['id']}): {c['status']} [Node: {c['node_label']}]")
    return 0


def _parsed_iso8601_ms(value: str | None, flag: str) -> int | None:
    """Parse an optional ISO-8601 timestamp into epoch milliseconds."""
    if value is None:
        return None
    import datetime

    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{flag} must be an ISO-8601 timestamp: {exc}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return int(parsed.timestamp() * 1000)


def handle_replay_dlq(args: argparse.Namespace) -> int:
    """Read a DLQ topic and republish matching envelopes to their source topic."""
    from neo4j_graph_loader.loader.dead_letter_replay import DeadLetterReplayError, ReplayFilter, run_replay

    try:
        schema = load_schema(args.config)
    except Exception as exc:
        logger.error("stage=replay-dlq reason=failed to load schema: %s", exc)
        return 1

    if args.node_label is not None:
        matches = [node for node in schema.nodes if node.label == args.node_label]
        if len(matches) != 1:
            logger.error("stage=replay-dlq reason=no configured node label named '%s'", args.node_label)
            return 1
        entity = matches[0]
        entity_topics = [entity.topic]
        dlq_entity = entity.topic
        entity_kind, entity_name = "node", args.node_label
    else:
        matches = [edge for edge in schema.edges if edge.type == args.edge_type]
        if len(matches) != 1:
            logger.error("stage=replay-dlq reason=no configured edge type named '%s'", args.edge_type)
            return 1
        entity = matches[0]
        # v7-S4: a relationship type has one DLQ for all of its topics; default names
        # derive from the topic (one topic) or the type (several), see dead_letter_entity.
        entity_topics = list(entity.topics)
        dlq_entity = entity.dead_letter_entity
        entity_kind, entity_name = "edge", args.edge_type

    source_topics = list(dict.fromkeys(getattr(args, "source_topic", None) or ()))
    for topic in source_topics:
        if entity_kind == "edge":
            owner = schema.edge_for_topic(topic)
            belongs = owner is not None and owner.type == entity_name
        else:
            belongs = topic == entity.topic
        if not belongs:
            logger.error(
                "stage=replay-dlq reason=--source-topic %s is not a topic of %s %s (its topics: %s)",
                topic, entity_kind, entity_name, ", ".join(entity_topics),
            )
            return 1

    # Resolve the DLQ topic whenever a dead_letter block is present, even if
    # enabled=false: the migration path (docs/USAGE.md section 10) promises
    # the enabled switch is reversible in either direction, and an operator
    # who flipped an entity back to the file sink must still be able to
    # replay a backlog left sitting in its DLQ topic from before the flip.
    # --dlq-topic overrides both, for a DLQ topic that no longer has a
    # live schema entry at all.
    if args.dlq_topic:
        dlq_topic = args.dlq_topic
    elif entity.dead_letter is not None:
        # P5-S4 Story 8: --parked resolves the parking-lot topic instead of
        # the ordinary DLQ topic, so an operator does not have to compute or
        # paste its name (`<dlq topic>.parked` unless overridden) by hand.
        # A replayed parked record republishes onto the original entity
        # topic and re-enters the pipeline from the top with a fresh
        # attempt_count=0 -- a deliberate, human-approved new attempt, not a
        # continuation of the attempt count that led to parking.
        if getattr(args, "parked", False):
            resolved_parked_topic = parked_topic_name(dlq_entity, entity.dead_letter)
            if resolved_parked_topic is None:
                logger.error(
                    "stage=replay-dlq reason=%s has dead_letter.enabled=false -- no parking-lot "
                    "topic to replay from",
                    entity_name,
                )
                return 1
            dlq_topic = resolved_parked_topic
        else:
            dlq_topic = entity.dead_letter.topic or f"{dlq_entity}.dlq"
    else:
        logger.error(
            "stage=replay-dlq reason=%s has no dead_letter block configured and no --dlq-topic "
            "override was given -- nothing to replay",
            entity_name,
        )
        return 1
    # One topic: republish to it (unchanged). Several topics (v7-S4): republish every
    # envelope to its own original_topic, which must be one of the type's topics.
    # --target-topic overrides both.
    if args.target_topic:
        target_topic = args.target_topic
        if target_topic not in entity_topics:
            # Final follow-ups nit: allowed (a staging topic), but loud: no loader of this entity reads it.
            logger.warning(
                "stage=replay-dlq reason=target_outside_entity %s %s does not read --target-topic %s (its "
                "topics: %s): the replayed records reach the schema only if something consumes %s; use a "
                "topic of the %s to re-load them",
                entity_kind, entity_name, target_topic, ", ".join(entity_topics), target_topic,
                "type" if entity_kind == "edge" else "label",
            )
    elif len(entity_topics) == 1:
        target_topic = entity_topics[0]
    else:
        target_topic = None
    if entity_kind == "edge" and args.target_topic and getattr(entity, "is_multi_pair", False) is True:
        # v7-S7: a loader takes the label pair from the topic it reads, so replaying records into
        # a topic of another pair would write them as that pair. Refused before reading anything.
        pairs = {entity.pair_for_topic(topic) for topic in source_topics}
        if len(pairs) != 1:
            logger.error(
                "stage=replay-dlq reason=edge type %s binds several label pairs; --target-topic needs "
                "--source-topic values that all bind one pair (got %s)", entity_name, source_topics or "none",
            )
            return 1
        [pair] = pairs
        if args.target_topic not in entity.topics:
            logger.warning(
                "stage=replay-dlq edge=%s target=%s is not a topic of the type: the records keep their labelled "
                "key [%s, %s, sk, tk]; the loader of %s decides their pair", entity_name, args.target_topic,
                pair.source_label, pair.target_label, args.target_topic,
            )
        if args.target_topic in entity.topics and entity.pair_for_topic(args.target_topic) != pair:
            target_pair = entity.pair_for_topic(args.target_topic)
            logger.error(
                "stage=replay-dlq reason=replaying %s records into %s would write them as %s->%s",
                ",".join(source_topics), args.target_topic, target_pair.source_label, target_pair.target_label,
            )
            return 1
    if entity_kind == "edge" and entity.is_multi_topic and entity.conflict_rule in ("precedence", "arrival"):
        logger.warning(
            "stage=replay-dlq edge=%s topic_conflict=%s: a replayed record is applied when it is read "
            "again, so it can overwrite a newer value of its pair from any of the type's topics (under "
            "precedence also a higher-ranked topic's value); a replay preserves neither arrival nor "
            "precedence order (only order_by protects newer values)",
            entity_name, entity.conflict_rule,
        )

    if args.limit is not None and args.limit <= 0:
        logger.error("stage=replay-dlq reason=--limit must be positive")
        return 1
    try:
        since_ms = _parsed_iso8601_ms(args.since, "--since")
        until_ms = _parsed_iso8601_ms(args.until, "--until")
    except ValueError as exc:
        logger.error("stage=replay-dlq reason=%s", exc)
        return 1
    if since_ms is not None and until_ms is not None and until_ms < since_ms:
        logger.error("stage=replay-dlq reason=--until must not be earlier than --since")
        return 1

    replay_filter = ReplayFilter(
        reason_code=args.reason_code, since_ms=since_ms, until_ms=until_ms, limit=args.limit,
        entity_kind=entity_kind, entity_name=entity_name,
        original_topics=frozenset(source_topics) if source_topics else None,
    )
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    replay_kwargs = {} if target_topic is not None else {"original_topic_targets": frozenset(entity_topics)}
    try:
        summary = run_replay(
            dlq_topic=dlq_topic, target_topic=target_topic, bootstrap_servers=bootstrap_servers,
            replay_filter=replay_filter, dry_run=bool(args.dry_run), **replay_kwargs,
        )
    except DeadLetterReplayError as exc:
        logger.error("stage=replay-dlq reason=%s", exc)
        return 1
    logger.info(
        "stage=replay-dlq matched=%s replayed=%s dry_run=%s dlq_topic=%s target_topic=%s",
        summary.matched, summary.replayed, summary.dry_run, dlq_topic,
        target_topic if target_topic is not None else "original_topic",
    )
    return 0


def _status_store_for_command(args: argparse.Namespace):
    """The SQL status store of the ``status-store`` commands (URL from the environment only: a password on a
    command line is visible to every process on the host)."""
    try:
        from neo4j_graph_loader.status import sql_store
    except ImportError as exc:
        logger.error("stage=status-store reason=this command needs the optional SQL support (install "
                     "neo4j-graph-loader[sql], and [postgres] for PostgreSQL): %s", exc)
        return None, None
    try:
        return sql_store, sql_store.SqlStatusStore(schema=getattr(args, "schema", None))
    except sql_store.StatusDatabaseError as exc:
        logger.error("stage=status-store reason=%s", exc)
        return None, None


def locator_module_errors():
    from neo4j_graph_loader.status.locator import LocatorError
    return (LocatorError, ValueError)


def handle_monitoring_export(args: argparse.Namespace) -> int:
    from neo4j_graph_loader.monitoring import export
    try:
        written = export.write_bundle(export.ExportOptions(
            out=Path(args.out), discovery=args.discovery, scrape_interval=args.scrape_interval, job_name=args.job_name,
            extra_labels=export.parse_extra_labels(args.extra_labels), metrics_port=args.metrics_port,
            targets=tuple(item for item in (args.targets or "").split(",") if item.strip()), targets_file=args.targets_file,
            docker_network=args.docker_network, namespace=args.namespace, service_name=args.service_name,
            prometheus_url=args.prometheus_url, datasource_uid=args.datasource_uid, values=args.values, force=args.force))
    except export.ExportError as exc:
        logger.error("stage=monitoring reason=%s", exc, extra=structured("monitoring", reason=str(exc)))
        return 1
    except OSError as exc:
        logger.error("stage=monitoring reason=could not write %s: %s", args.out, exc, extra=structured("monitoring", reason=str(exc)))
        return 1
    for name in written:
        print(name)
    if args.datasource_uid is None:
        logger.info("stage=monitoring reason=grafana/dashboards/ leaves the datasource variable unset (Grafana uses its default Prometheus "
                    "datasource); pass --datasource-uid when Grafana has several Prometheus datasources",
                    extra=structured("monitoring", datasource_uid=None))
    logger.info("stage=monitoring reason=wrote %s file(s) to %s", len(written), args.out,
                extra=structured("monitoring", files=len(written), out=str(args.out), discovery=args.discovery))
    return 0


def handle_images_build(args: argparse.Namespace) -> int:
    from neo4j_graph_loader.image_build import builder
    registry = args.registry or os.environ.get("GRAPH_LOADER_IMAGE_REGISTRY") or ""
    if not registry:
        logger.error("stage=images reason=give --registry (or set GRAPH_LOADER_IMAGE_REGISTRY): the registry prefix the images are named under, "
                     "for example registry.example.com:5000/team", extra=structured("images", reason="no registry"))
        return 1
    settings = builder.Settings(
        registry=registry, version=args.package_version or builder.__version__, package_wheel=args.package_wheel, base_image=args.base_image,
        index_url=args.pip_index_url, index_secret=args.pip_index_secret, wheelhouse=args.wheelhouse, ca_bundle=args.pip_ca_bundle, proxy=args.proxy,
        docker_config=args.docker_config, debian_mirror=args.debian_mirror, tag_suffix=args.tag_suffix, push=args.push, dry_run=args.dry_run,
        output=args.output, replace=args.replace)
    try:
        result = builder.build(settings)
    except builder.BuildError as exc:
        logger.error("stage=images reason=%s", exc, extra=structured("images", reason=str(exc)))
        return 1
    if args.dry_run:
        print(json.dumps(result, indent=2))
        logger.info("stage=images reason=dry run: nothing was built", extra=structured("images", dry_run=True))
        return 0
    for row in result["images"]:
        print(f"{row['component']} {row['local_tag']} {row['id']}" + (f" {row['registry_digest']}" if row.get("registry_digest") else ""))
    logger.info("stage=images reason=built %s image(s) for %s on %s", len(result["images"]), result["version"], result["platform"],
                extra=structured("images", images=len(result["images"]), version=result["version"], platform=result["platform"]))
    return 0


def handle_status_store(args: argparse.Namespace) -> int:
    sql_store, store = _status_store_for_command(args)
    if store is None:
        return 1
    try:
        if args.status_store_command == "migrate":
            version = store.migrate()
            sys.stdout.write(f"status schema is at version {version} in {store.describe()}"
                             f"{'' if store.schema is None else ' (schema ' + store.schema + ')'}\n")
            return 0
        if args.status_store_command == "flush":
            from neo4j_graph_loader.status import outbox
            location = Path(args.run_dir)
            attempts = [location] if (location / "status-locator.json").exists() else outbox.pending_attempts(location)
            if not attempts:
                sys.stdout.write("nothing to deliver\n")
                return 0
            code = 0
            for attempt in attempts:
                try:
                    result = outbox.recover_attempt(attempt, lock_timeout=args.wait)
                    sys.stdout.write(f"{attempt}: delivered {result['events']} event(s)"
                                     f"{', the latest snapshot' if result['snapshot'] else ''}"
                                     f"{'; history flagged incomplete' if result['history_incomplete'] else ''}\n")
                except outbox.OutboxLocked as exc:
                    logger.error("stage=status-store reason=%s", exc)
                    code = 1
                except (sql_store.StatusDatabaseError, *locator_module_errors()) as exc:
                    logger.error("stage=status-store reason=%s", exc)
                    code = 1
            return code
        if args.status_store_command == "prune":
            try:
                older = sql_store.parse_duration(args.older_than)
            except ValueError as exc:
                logger.error("stage=status-store reason=%s", exc)
                return 1
            store.check_schema()
            counts = store.prune(older)
            sys.stdout.write(f"pruned {counts['executions']} finished execution(s) older than {args.older_than} "
                             f"and {counts['events']} event(s)\n")
            return 0
        logger.error("stage=status-store reason=unknown command %s", args.status_store_command)
        return 1
    except sql_store.StatusDatabaseError as exc:
        logger.error("stage=status-store reason=%s", exc)
        return 1
    finally:
        store.close()


def handle_schema_example(args: argparse.Namespace) -> int:
    try:
        content = Path(args.config).read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        logger.error("stage=schema reason=%s", exc)
        return 1
    sys.stdout.write(content)
    return 0


def handle_validate_schema(args: argparse.Namespace) -> int:
    from neo4j_graph_loader.utils.schema_loader import SchemaLoadError

    try:
        schema = load_schema(args.config)
    except SchemaLoadError as exc:
        logger.error("stage=schema reason=%s", exc)
        return 1
    print(f"Schema valid: {len(schema.nodes)} node types, {len(schema.edges)} relationship types")
    return 0


class _ExplicitStore(argparse.Action):
    """``store`` that also records the option was given (``args._explicit_<dest>``), whatever its value."""

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)
        setattr(namespace, f"_explicit_{self.dest}", True)


def _add_runtime_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--runtime", dest="runtime_backend", choices=RUNTIME_BACKENDS,
        default=os.environ.get(RUNTIME_ENV) or DEFAULT_RUNTIME_BACKEND,
        help="How roles are run: 'docker' (one container per role, default; env %s) or 'process' "
        "(child processes of this command, Linux only; no Docker daemon, network or images)" % RUNTIME_ENV,
    )


def build_parser() -> argparse.ArgumentParser:
    from neo4j_graph_loader._version import __version__

    parser = argparse.ArgumentParser(prog="graph-loader", description="Neo4j Graph Loader CLI")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--log-format", dest="log_format", choices=("text", "json"), default=os.environ.get("GRAPH_LOADER_LOG_FORMAT") or "text",
                        help="how this command and its roles log: 'text' (default) or 'json' (one JSON object per line on stderr; "
                             "env GRAPH_LOADER_LOG_FORMAT). Accepted anywhere on the command line; command output (YAML, status --json) is never affected")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_store = subparsers.add_parser(
        "status-store", help="Maintain the SQL run status database (URL from GRAPH_LOADER_STATUS_DB_URL, never on the command line)")
    status_store_commands = status_store.add_subparsers(dest="status_store_command", required=True)
    migrate = status_store_commands.add_parser("migrate", help="Create or upgrade the status schema (idempotent)")
    prune = status_store_commands.add_parser("prune", help="Delete FINISHED executions older than a duration (never running ones)")
    prune.add_argument("--older-than", required=True, metavar="DURATION", help="for example 30d, 12h, 90m")
    flush = status_store_commands.add_parser(
        "flush", help="Deliver the events a run left in its local outbox (a killed run, or a database that was down)")
    flush.add_argument("--run-dir", required=True, metavar="DIR", help="the run's attempt directory (or the base holding several)")
    flush.add_argument("--wait", type=float, default=30.0, metavar="SECONDS",
                       help="how long to wait for a live process that owns the outbox (default 30)")
    for sub in (migrate, prune, flush):
        sub.add_argument("--schema", default=None, help="PostgreSQL schema (env GRAPH_LOADER_STATUS_SCHEMA, default graph_loader)")
    status_store.set_defaults(func=handle_status_store)

    monitoring = subparsers.add_parser(
        "monitoring", help="Monitoring assets for your own Prometheus and Grafana (alert rules, dashboards, scrape configuration)")
    monitoring_commands = monitoring.add_subparsers(dest="monitoring_command", required=True)
    export = monitoring_commands.add_parser(
        "export", help="Write the monitoring bundle (scrape config, alert rules and promtool tests, dashboards, provisioning, "
        "Kubernetes manifests) for one discovery mode")
    export.add_argument("--out", required=True, metavar="DIR", help="directory to write the bundle into (created; existing files are not overwritten without --force)")
    export.add_argument("--discovery", required=True, choices=("docker", "file", "kubernetes", "static"),
                        help="how Prometheus finds the exporters: docker (Docker service discovery), file (the process runtime's "
                        "metrics/targets.json), static or kubernetes (the aggregated --metrics-port endpoint of the process runtime)")
    export.add_argument("--scrape-interval", default="15s", metavar="DURATION", help="Prometheus scrape interval (default 15s)")
    export.add_argument("--job-name", default="graph-loader", help="Prometheus job name (default graph-loader)")
    export.add_argument("--extra-labels", default=None, metavar="K=V[,K=V...]",
                        help="labels added to every target and alert; run_id, execution_id, exporter and runtime are reserved and refused")
    export.add_argument("--metrics-port", type=int, default=None, metavar="PORT",
                        help="the aggregated endpoint port (`start --runtime process --metrics-port`); required for static and kubernetes")
    export.add_argument("--targets", default=None, metavar="HOST:PORT[,...]", help="static discovery: scrape these instead of localhost:--metrics-port")
    export.add_argument("--targets-file", default=None, metavar="PATH", help="file discovery: the targets.json Prometheus reads")
    export.add_argument("--docker-network", default="graph-loader-net", help="docker discovery: the network the roles run on")
    export.add_argument("--namespace", default="default", help="kubernetes discovery: namespace of the Service")
    export.add_argument("--service-name", default="graph-loader", help="kubernetes discovery: Service (and selector) name")
    export.add_argument("--prometheus-url", default="http://prometheus:9090", help="Grafana datasource URL written to the provisioning file")
    export.add_argument("--datasource-uid", default=None,
                        help="Grafana Prometheus datasource uid: written to the provisioning file (default prometheus) and set as the "
                             "dashboard's datasource variable in grafana/dashboards/; give it when Grafana has several Prometheus datasources")
    export.add_argument("--values", default=None, metavar="FILE", help="YAML file: job_name, labels, per-alert threshold/for/severity, disable")
    export.add_argument("--force", action="store_true", help="overwrite files that already exist in --out")
    export.set_defaults(func=handle_monitoring_export)

    images = subparsers.add_parser("images", help="Build the loader images from this installed package, for a registry you control")
    images_commands = images.add_subparsers(dest="images_command", required=True)
    build = images_commands.add_parser(
        "build", help="Build the four images (cli, node, edge, clock) for this package version with docker buildx, from your own mirrors or offline",
        description="Builds graph-loader-{cli,node,edge,clock}:<version> under --registry from this installed package. Needs docker with buildx (BuildKit). "
                    "The dependencies come from your PyPI mirror (--pip-index-url, or --pip-index-secret for a URL with credentials) or from a wheelhouse "
                    "prepared for the build platform (--wheelhouse); nothing is fetched from a public index otherwise. The base image should be in your registry.")
    build.add_argument("--registry", default=None, metavar="PREFIX",
                       help="registry prefix the images are named under: host[:port]/path (default: env GRAPH_LOADER_IMAGE_REGISTRY); no scheme or credentials")
    build.add_argument("--package-version", default=None, metavar="VERSION", help="must equal the installed package version (the default); anything else is refused")
    build.add_argument("--package-wheel", type=Path, default=None, metavar="FILE",
                       help="the wheel of this installed package (required with an index; with --wheelhouse it is taken from the wheelhouse)")
    build.add_argument("--base-image", default="python:3.12-slim", metavar="REF",
                       help="base image, CPython 3.12 on Debian (default python:3.12-slim); give your mirror's reference, it is resolved to a digest first")
    dependencies = build.add_mutually_exclusive_group(required=True)
    dependencies.add_argument("--pip-index-url", default=None, metavar="URL", help="your PyPI mirror, without credentials")
    dependencies.add_argument("--pip-index-secret", type=Path, default=None, metavar="FILE",
                              help="a file whose only content is your PyPI mirror's URL, credentials included; passed as a BuildKit secret, never on a command line")
    dependencies.add_argument("--wheelhouse", type=Path, default=None, metavar="DIR",
                              help="offline build: a directory with exactly the locked wheels for the build platform plus the package wheel")
    build.add_argument("--pip-ca-bundle", type=Path, default=None, metavar="FILE", help="CA bundle (PEM) for an https index with a private certificate")
    build.add_argument("--debian-mirror", default=None, metavar="URL", help="your Debian mirror (archive layout) for the tini install in the CLI image")
    build.add_argument("--proxy", default=None, metavar="URL", help="HTTP(S) proxy for the build steps; none is passed unless you give this")
    build.add_argument("--docker-config", type=Path, default=None, metavar="DIR",
                       help="Docker config directory whose registry logins for the base image and the target registry are used (default: your own)")
    build.add_argument("--tag-suffix", default=None, metavar="SUFFIX", help="development builds: tag <version>-<suffix>; cannot be pushed")
    build.add_argument("--push", action="store_true", help="push the images with docker push after building (uses the same scoped logins)")
    build.add_argument("--dry-run", action="store_true", help="validate everything and print the plan as JSON; build and push nothing")
    build.add_argument("--output", type=Path, default=None, metavar="DIR", help="write the build manifest (images.json) into this new directory")
    build.add_argument("--replace", action="store_true", help="overwrite an image tag that already exists locally (refused otherwise)")
    build.set_defaults(func=handle_images_build)
    example = subparsers.add_parser("schema-example", help="Print a bundled schema YAML")
    example.add_argument("--name", dest="sample", choices=("default", "social"), default="default")
    example.set_defaults(func=handle_schema_example, config=None)
    validate = subparsers.add_parser("validate-schema", help="Validate a schema without services")
    inputs = validate.add_mutually_exclusive_group()
    inputs.add_argument("--config", help="Caller schema YAML path")
    inputs.add_argument("--sample", choices=("default", "social"), default="default")
    validate.set_defaults(func=handle_validate_schema)

    # Start command
    parser_start = subparsers.add_parser("start", help="Start the ingestion pipeline")
    parser_start.add_argument("--config", default=None, help="Path to schema YAML (default: bundled canonical schema)")
    parser_start.add_argument("--run-dir", metavar="BASE", help="Create a private persistent attempt under BASE (default: OS temporary directory)")
    parser_start.add_argument("--mode", choices=["bulk", "stream"], required=True, help="Ingestion mode")
    parser_start.add_argument(
        "--status-store-auto-migrate", dest="status_store_auto_migrate", action="store_true",
        default=os.environ.get("GRAPH_LOADER_STATUS_AUTO_MIGRATE", "").strip().lower() in ("1", "true", "yes"),
        help="With --status-store sql: create or upgrade the status schema at start instead of refusing an older one "
             "(env GRAPH_LOADER_STATUS_AUTO_MIGRATE=1)")
    parser_start.add_argument(
        "--status-store", dest="status_store", default=os.environ.get(STATUS_STORE_ENV) or "json",
        help="Where this run's status and event history are kept: 'json' (status.json and events.*.jsonl in the run "
             "directory, default) or 'sql' (a database; env %s). Env %s." % ("GRAPH_LOADER_STATUS_DB_URL", STATUS_STORE_ENV))
    parser_start.add_argument("--network", default="graph-loader-net", action=_ExplicitStore, help="Docker network for containers")
    _add_runtime_option(parser_start)
    parser_start.add_argument("--metrics-addr", default=None, metavar="HOST[:PORT]",
                              help="process runtime: where every role binds its metrics endpoint (default 127.0.0.1, an OS-assigned "
                              "port per role, listed in <run dir>/metrics/targets.json); use 0.0.0.0 inside a pod")
    parser_start.add_argument("--metrics-port", type=int, default=None, metavar="PORT",
                              help="process runtime: serve ONE aggregated /metrics on --metrics-addr's host at this port, merging this command's "
                              "metrics and every role's (one stable scrape target; scrape it OR the per-role file_sd targets, not both)")
    parser_start.add_argument("--health-addr", default=None, metavar="HOST:PORT",
                              help="process runtime: serve /healthz and /readyz here (off by default)")
    parser_start.add_argument("--health-stale-after", type=float, default=None, metavar="SECONDS",
                              help="process runtime: /healthz turns unhealthy when the supervision loop has not ticked for "
                              "this long (default 30)")
    parser_start.add_argument("--child-logs", choices=("relay", "files"), default=None,
                              help="process runtime: 'relay' (default) also prints each role's output to this command's stdout "
                              "with a [name] prefix; 'files' keeps it in <run dir>/logs only")
    parser_start.add_argument(
        "--skip-image-build",
        action="store_true",
        help="require compatible preloaded images; never pull (start never builds images)",
    )
    parser_start.add_argument(
        "--accept-former-dlq", action="append", default=None, metavar="TOPIC",
        help="Start although TOPIC, a relationship type's former default DLQ name (<topic>.dlq or "
        "<type>.dlq), still holds records -- e.g. a drained DLQ kept for audit, or a topic another "
        "deployment owns. Repeatable. Deleting the old topics is the expected step instead",
    )
    parser_start.add_argument(
        "--accept-dlq-leftovers", action="append", default=None, metavar="TYPE",
        help="Start although relationship type TYPE's parked-pair index still holds records of a topic "
        "it no longer reads, or of a topic now bound to another label pair (they park after "
        "max_retry_attempts, blocking records of the same keys until then). Repeatable. Draining the "
        "DLQ with the previous schema first is the expected step instead",
    )
    parser_start.add_argument(
        "--bulk-timeout-seconds",
        type=float,
        default=300.0,
        help="Maximum seconds for each bulk lifecycle stage; progress-based where stages run long: "
        "node draining, waiting for the edge plan (planner heartbeat) and the planned dispatch fail "
        "only after this many seconds without progress",
    )
    parser_start.add_argument(
        "--bulk-edge-overlap",
        choices=BULK_EDGE_OVERLAP_MODES,
        default=None,
        help="bulk, classic mode only (ignored with a warning unless --edge-classic is given; "
        "default: labels): 'labels' serializes edge types sharing an endpoint label; "
        "'shared' runs them concurrently under globally ordered APOC node locks "
        "and also schedules python_apoc self-referencing edges alongside them "
        "instead of in a final isolated phase",
    )
    parser_start.add_argument(
        "--edge-classic",
        action="store_true",
        help="bulk only: use the classic per-type APOC-locked relationship loaders. This is the one "
        "bulk mode that can wait on Neo4j locks. Without any of --edge-plan, --edge-leases and "
        "--edge-classic, bulk uses planned mode (or the lease mode when the schema enables "
        "relationship dead_letter)",
    )
    parser_start.add_argument(
        "--edge-leases",
        action="store_true",
        help="bulk only: run a global bucket-lease coordinator so concurrent relationship "
        "transactions are node-disjoint across all edge types and containers (no Neo4j "
        "lock waits); implies --bulk-edge-overlap shared",
    )
    parser_start.add_argument(
        "--bucket-lease-buckets",
        dest="lease_buckets",
        type=int,
        default=None,
        help="with --edge-leases (bulk), or by default with --mode stream: stable buckets per "
        f"node label (default: {DEFAULT_BUCKET_LEASE_BUCKETS})",
    )
    parser_start.add_argument(
        "--lease-buckets",
        dest="lease_buckets_deprecated",
        type=int,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser_start.add_argument(
        "--bucket-lease-outer-batch-size",
        dest="lease_outer_batch_size",
        type=int,
        default=None,
        help="with --edge-leases (bulk), or by default with --mode stream: edge outer flush "
        f"size, split into bucket cells (default: {DEFAULT_BUCKET_LEASE_OUTER_BATCH_SIZE}; "
        "overrides edge_unwind_batch_size)",
    )
    parser_start.add_argument(
        "--lease-outer-batch-size",
        dest="lease_outer_batch_size_deprecated",
        type=int,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser_start.add_argument(
        "--bucket-lease-keyed-max-nodes",
        dest="lease_keyed_max_nodes",
        type=_nonnegative_int,
        default=None,
        help="with --edge-leases (bulk), or by default with --mode stream: endpoint labels "
        "whose node topic holds at most this many records are leased per node key instead of "
        f"per hash bucket (default: {DEFAULT_BUCKET_LEASE_KEYED_MAX_NODES}; 0 = never)",
    )
    parser_start.add_argument(
        "--lease-keyed-max-nodes",
        dest="lease_keyed_max_nodes_deprecated",
        type=_nonnegative_int,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser_start.add_argument(
        "--retry-consumer-replicas",
        type=int,
        default=1,
        help="--mode stream only (P5-S4): replica count for the fleet-wide DLQ retry consumer, "
        "launched automatically once any edge type has DLQ routing enabled (default: 1)",
    )
    parser_start.add_argument(
        "--stream-clock-rotation",
        action="store_true",
        help="DEPRECATED, removed after one release: for --mode stream, use the legacy global batch "
        "clock with rotating relationship scheduling instead of the default bucket-lease fleet",
    )
    parser_start.add_argument(
        "--edge-plan", nargs="?", const=AUTO_EDGE_PLAN, metavar="PLAN_FILE",
        help="bulk only: load every relationship as globally planned, node-disjoint chunks "
        "(no Neo4j lock waits) with one dispatcher and a pool of --plan-writers writers; a "
        "chunk runs only once both endpoint labels are loaded. Without a file the loader "
        "plans from the schema's relationship topics (up to the bulk boundary) while the "
        "nodes load; with a file it loads the plan a producer wrote (waited for up to "
        "--bulk-timeout-seconds)",
    )
    parser_start.add_argument(
        "--plan-writers", type=int, default=None,
        help="with --edge-plan: writer containers across all edge types (default: Neo4j "
        f"available processors + 2; {auto_settings.default_plan_writers(None)} if the server "
        "cannot be probed; never more than the plan's chunks)",
    )
    parser_start.add_argument(
        "--plan-workers", type=int, default=None,
        help="with --edge-plan (no file): planner processes reading the edge topics "
        f"(default: min({auto_settings.DEFAULT_MAX_PLAN_WORKERS}, CPUs))",
    )
    parser_start.add_argument(
        "--plan-buckets", type=int, default=None,
        help="with --edge-plan (no file): hash buckets per node label (default: clamp(writers, 8, 64), "
        "at most isqrt(records / (edge types * 500)), at least 2)",
    )
    parser_start.add_argument(
        "--plan-hub-degree", type=int, default=None,
        help="with --edge-plan (no file): nodes with at least this total degree over all edge "
        "types get their own resource (default: the chunk size; 0 disables hubs)",
    )
    parser_start.add_argument(
        "--plan-chunk-rows", type=int, default=None,
        help="with --edge-plan (no file): maximum rows per chunk transaction "
        "(default: clamp(records / (20 * writers), 500, 5000))",
    )
    parser_start.add_argument(
        "--plan-partitions", type=int, default=None,
        help="with --edge-plan (no file): partitions of the run's chunk topic (default: 4)",
    )
    parser_start.add_argument(
        "--plan-topic", default=None,
        help="with --edge-plan (no file): new chunk topic to create (default: graph-loader-edge-chunks-<run id>)",
    )
    parser_start.add_argument(
        "--plan-dir", default=None,
        help="with --edge-plan (no file): directory for the plan file and its completion ledger "
        "(default: private attempt plans directory)",
    )
    parser_start.add_argument(
        "--plan-memory-mb", type=int, default=None,
        help="with --edge-plan (no file): memory budget of each planner process in MiB for buffered "
        "relationship rows, endpoint keys and Kafka queues; beyond it the planner spills to "
        "--plan-spill-dir, so inputs far larger than RAM plan with bounded memory (default: a quarter "
        "of the available RAM over the planner processes, 256-4096 MiB)",
    )
    parser_start.add_argument(
        "--plan-spill-dir", default=None,
        help="with --edge-plan (no file): directory for the planner's temporary spill files, removed "
        "after workers reap; needs about the relationship topics' uncompressed size free (default: private attempt spill directory)",
    )
    parser_start.add_argument(
        "--replace-stale-plan-topic", action="store_true",
        help="with --edge-plan (no file) and an explicit --run-id: delete the run's chunk topic "
        "left behind by a killed run with the same run id, provided no container of that run is alive "
        "(default: refuse to start, since the topic may still be in use)",
    )
    parser_start.add_argument(
        "--keep-plan-topic", action="store_true",
        help="with --edge-plan (no file): keep the run's chunk topic afterwards (default: delete it)",
    )
    parser_start.add_argument(
        "--node-replicas-min-records", type=_nonnegative_int,
        default=auto_settings.DEFAULT_NODE_REPLICAS_MIN_RECORDS,
        help="bulk only: a node label whose YAML omits 'replicas' gets one loader when its topic "
        "retains fewer records, else one per topic partition up to --max-node-replicas "
        f"(default: {auto_settings.DEFAULT_NODE_REPLICAS_MIN_RECORDS}; 0 = always the YAML value)",
    )
    parser_start.add_argument(
        "--max-node-replicas", type=int, default=auto_settings.DEFAULT_MAX_NODE_REPLICAS,
        help=f"bulk only: cap for derived node-loader replicas (default: {auto_settings.DEFAULT_MAX_NODE_REPLICAS})",
    )
    parser_start.add_argument(
        "--max-concurrent-bulk-edges",
        type=_nonnegative_int,
        default=None,
        help="bulk, classic mode only (ignored with a warning unless --edge-classic is given): "
        "maximum concurrently running shared edge types (0 = unlimited, the default)",
    )
    parser_start.add_argument(
        "--run-id",
        help="optional validated run identity. Names the run and scopes bulk relationship consumer "
        "groups, container names and labels, the plan topic and the lease "
        "server. Stream consumer groups are stable and do not use it, except for one-time legacy "
        "offset migration",
    )
    parser_start.add_argument(
        "--stream-group-member-wait-seconds", type=_positive_float, default=None, metavar="S",
        help="stream mode: how long to wait for a stable consumer group's members to expire before "
             "refusing to start (default: loading.session_timeout_ms + 5 s; a member that died during "
             "a rebalance can stay up to max_poll_interval_ms + session_timeout_ms)",
    )
    parser_start.set_defaults(func=handle_start)

    # Stop command
    parser_stop = subparsers.add_parser("stop", help="Stop running ingestion containers")
    parser_stop.add_argument("--loader", help="Specific loader name to stop")
    _add_runtime_option(parser_stop)
    parser_stop.add_argument("--run-dir", metavar="DIR", help="process runtime: the run's attempt directory, or the base given to start "
                             "(default: the OS temporary directory)")
    parser_stop.add_argument("--recover", action="store_true",
                             help="process runtime: after the command died abruptly, stop what it left behind and report what is "
                             "retained (refused while it is alive)")
    parser_stop.set_defaults(func=handle_stop)

    # Status command
    parser_status = subparsers.add_parser("status", help="Check pipeline status and lags")
    parser_status.add_argument("--config", default=None, help="Path to schema YAML (default: bundled canonical schema)")
    _add_runtime_option(parser_status)
    parser_status.add_argument("--run-dir", metavar="DIR", help="process runtime: the run's attempt directory, or the base given to start "
                               "(default: the OS temporary directory)")
    parser_status.add_argument("--run", action="store_true", help="show the run status of the newest run (or of --execution-id) found under "
                               "--run-dir or in the OS temporary directory, instead of the container listing")
    parser_status.add_argument("--execution-id", default=None, metavar="ID", help="show this execution (default: the newest one found under --run-dir, or in the OS temporary directory; --run-dir may be either runtime's attempt directory)")
    parser_status.add_argument("--json", action="store_true", help="print the run status document as JSON (plus a `reader` member)")
    parser_status.add_argument("--watch", action="store_true", help="keep refreshing until the run ends")
    parser_status.add_argument("--interval", type=float, default=2.0, metavar="SECONDS", help="--watch refresh interval (default 2)")
    parser_status.add_argument("--refresh-lag", action="store_true",
                               help="ask Kafka once, within a deadline, for the current consumer lag (never blocks otherwise)")
    parser_status.add_argument("--refresh-timeout", type=float, default=5.0, metavar="SECONDS", help="deadline of --refresh-lag (default 5)")
    parser_status.add_argument("--list", action="store_true", help="list the runs of the status store instead of showing one")
    parser_status.add_argument("--active", action="store_true", help="with --list: only runs that have not finished")
    parser_status.add_argument("--containers", action="store_true", help="also print the container listing `status` showed before run status existed")
    parser_status.add_argument("--stale-after", type=float, default=None, metavar="SECONDS",
                               help="a running execution whose heartbeat is older than this is stale (default 30; env GRAPH_LOADER_STATUS_STALE_AFTER_S)")
    parser_status.add_argument("--status-store", dest="status_store", default=os.environ.get(STATUS_STORE_ENV) or "json",
                               help="which store --list and --execution-id read when no run directory is given: json or sql")
    parser_status.set_defaults(func=handle_status)

    # Replay-DLQ command
    parser_replay = subparsers.add_parser(
        "replay-dlq", help="Read a DLQ topic and republish matching envelopes to their source topic",
    )
    parser_replay.add_argument("--config", default=None, help="Path to schema YAML (default: bundled canonical schema)")
    _add_runtime_option(parser_replay)
    replay_entity = parser_replay.add_mutually_exclusive_group(required=True)
    replay_entity.add_argument("--node-label", help="Replay this node label's DLQ topic")
    replay_entity.add_argument("--edge-type", help="Replay this relationship type's DLQ topic")
    parser_replay.add_argument("--reason-code", help="Only replay envelopes with this exact reason_code")
    parser_replay.add_argument("--since", help="Only replay envelopes at or after this ISO-8601 timestamp")
    parser_replay.add_argument("--until", help="Only replay envelopes at or before this ISO-8601 timestamp")
    parser_replay.add_argument(
        "--limit", type=int, default=None,
        help="Replay at most this many matching envelopes (not messages scanned)",
    )
    parser_replay.add_argument(
        "--target-topic", default=None,
        help="Republish destination (default: the entity's own source topic; for a relationship "
        "type with several topics, each envelope's original_topic)",
    )
    parser_replay.add_argument(
        "--source-topic", action="append", default=None, metavar="TOPIC",
        help="Only replay envelopes whose original_topic is TOPIC (repeatable); TOPIC must be one "
        "of the entity's topics",
    )
    parser_replay.add_argument(
        "--dlq-topic", default=None,
        help="Override the DLQ topic to read (default: resolved from the entity's dead_letter "
        "block, even if enabled=false -- so a backlog from before switching back to the file "
        "sink stays replayable). Required if the entity has no dead_letter block at all",
    )
    parser_replay.add_argument(
        "--parked", action="store_true",
        help="Replay from the entity's parking-lot topic instead of its ordinary DLQ topic "
        "(P5-S4). Ignored when --dlq-topic is given. A replayed parked record republishes onto "
        "the original entity topic and re-enters the pipeline with a fresh attempt_count=0.",
    )
    parser_replay.add_argument(
        "--dry-run", action="store_true",
        help="Count and log matching envelopes without publishing anything",
    )
    parser_replay.set_defaults(func=handle_replay_dlq)

    return parser


STATUS_STORE_ENV = "GRAPH_LOADER_STATUS_STORE"
STATUS_DB_URL_ENV = "GRAPH_LOADER_STATUS_DB_URL"
STATUS_DB_SCHEMA_ENV = "GRAPH_LOADER_STATUS_SCHEMA"


def _validated_status_store(args: argparse.Namespace) -> None:
    from neo4j_graph_loader.status.locator import STORES
    if getattr(args, "status_store", "json") not in STORES:
        raise ValueError(f"status store must be one of {', '.join(STORES)}, got {args.status_store!r} (env {STATUS_STORE_ENV})")


def _begin_execution(args: argparse.Namespace) -> StatusTracker | None:
    """The execution's identity and status model: the first act of ``start``, before any validation or schema load.
    It lives in memory until :func:`_open_status_store` gives it a directory."""
    if getattr(args, "command", None) != "start":
        return None
    try:
        run_id = getattr(args, "run_id", None)
        backend = _runtime_backend(args)
        execution = StatusTracker(
            run_id=run_id if isinstance(run_id, str) and _RUN_ID.fullmatch(run_id) else None,
            mode=getattr(args, "mode", None) if getattr(args, "mode", None) in status_model.MODES else None,
            runtime=backend if backend in status_model.RUNTIMES else DEFAULT_RUNTIME_BACKEND,
        )
    except Exception as exc:          # status tracking must never stop a load
        logger.warning("stage=status reason=could not create the execution record: %s", exc)
        return None
    set_log_context(execution_id=execution.execution_id, run_id=execution.run_id, runtime=backend)   # the CLI's own samples and log lines
    return execution


def _open_status_store(args: argparse.Namespace, execution: StatusTracker) -> None:
    """Create the attempt directory and its locator from the arguments alone, then attach the chosen store.

    This runs after the arguments are validated and before the schema is loaded, so a schema-load failure is
    recorded in the attempt directory too (a rejected flag or a missing schema file still creates no state). Any
    failure here disables status for the run with one log line; it never fails the load (the load's own directory
    preflight reports an unusable base exactly as before).
    """
    from neo4j_graph_loader import _runtime
    from neo4j_graph_loader.status import json_store, locator as status_locator
    store = getattr(args, "status_store", "json")
    store = store if store in status_locator.STORES else "json"
    try:
        root = _runtime.create_attempt_root(args)
    except Exception as exc:
        logger.warning("stage=status reason=status is disabled for this run, the attempt directory cannot be created: %s", exc)
        return
    args._attempt_root = root

    def write_locator() -> None:
        status_locator.write_locator(root, status_locator.build_locator(
            store=store, execution_id=execution.execution_id, run_id=execution.run_id,
            created_at=execution.snapshot()["started_at"],
            db_url=os.environ.get(STATUS_DB_URL_ENV) if store == "sql" else None,
            schema=(os.environ.get(STATUS_DB_SCHEMA_ENV) or "graph_loader") if store == "sql" else None))

    try:
        write_locator()
    except Exception as exc:
        logger.warning("stage=status reason=status is disabled for this run, the locator cannot be written in %s: %s", root, exc)
        return
    if store == "sql":
        journal = _open_sql_journal(args, root)
        if journal is None:
            return
    else:
        try:
            journal = json_store.JsonStatusStore(root, **json_store.settings_from_environment())
        except Exception as exc:
            logger.warning("stage=status reason=status is disabled for this run, the JSON store cannot start in %s: %s", root, exc)
            return
    args._status_store = journal

    def on_event(event: dict) -> None:
        if event.get("type") == "execution.run_id":
            write_locator()

    if execution.attach_journal(journal):
        execution.subscribe(on_event=on_event, on_snapshot=journal.offer_snapshot)
        if hasattr(journal, "write_snapshot"):
            journal.write_snapshot()


class StatusStoreRefused(RuntimeError):
    """The chosen status store must not be used as configured (for example an older schema): the start is refused."""


def _open_sql_journal(args: argparse.Namespace, root: Path):
    """The SQL status store's journal, or ``None`` (status disabled, one log line). Raises :class:`StatusStoreRefused`
    when the database is reachable but its schema is not this version's and auto-migration was not asked for."""
    try:
        from neo4j_graph_loader.status import sql_store
    except ImportError as exc:
        logger.warning("stage=status reason=status is disabled for this run: the SQL store needs the optional SQL support "
                       "(install neo4j-graph-loader[sql], and [postgres] for PostgreSQL): %s", exc)
        return None
    if not os.environ.get(STATUS_DB_URL_ENV):
        logger.warning("stage=status reason=status is disabled for this run: --status-store sql needs %s", STATUS_DB_URL_ENV)
        return None

    def count(reason: str) -> None:
        metrics_module.safe_emit(lambda: metrics_module.process_metrics().status_store_errors.labels(kind="cli", reason=reason).inc())

    try:
        sql = sql_store.SqlStatusStore(metrics_listener=count)
    except sql_store.StatusDatabaseError as exc:
        logger.warning("stage=status reason=status is disabled for this run: %s", exc)
        return None
    try:
        sql.check_schema()
    except sql_store.SchemaNotReady as exc:
        auto = getattr(args, "status_store_auto_migrate", False)
        if not auto:
            sql.close()
            raise StatusStoreRefused(str(exc)) from None
        try:
            sql.migrate()
        except sql_store.StatusDatabaseError as migration:
            sql.close()
            raise StatusStoreRefused(str(migration)) from None
        logger.info("stage=status reason=the status database schema was migrated to version %s", sql_store.DB_SCHEMA_VERSION)
    except sql_store.StatusDatabaseError as exc:
        # Not reachable now: the run goes on and the writer keeps trying (a durable outbox arrives with E15.F1.S5).
        logger.warning("stage=status reason=the status database is not reachable yet, events will be written when it is: %s", exc)
    from neo4j_graph_loader.status import json_store, outbox
    lock = outbox.AttemptLock(root)
    try:
        lock.acquire()
    except (OSError, outbox.OutboxLocked) as exc:
        logger.warning("stage=status reason=the status outbox lock could not be taken (%s); events are queued in memory only", exc)
        lock = None
    _recover_in_background(root.parent, root, sql_store, outbox)
    execution = _execution_of(args)
    return outbox.OutboxJournal(
        root, sql, execution.execution_id, snapshot_interval=max(2.0, json_store.settings_from_environment()["snapshot_interval"]),
        on_degraded=execution.mark_history_incomplete, on_error=count, lock=lock)


def _recover_in_background(base: Path, current: Path, sql_store, outbox) -> None:
    """Deliver what earlier runs under the same base left in their outboxes (their lock must be free). Never blocks the
    start and never fails it."""
    def work() -> None:
        try:
            for attempt in outbox.pending_attempts(base, exclude=current):
                try:
                    result = outbox.recover_attempt(attempt, lock_timeout=0)
                    logger.info("stage=status reason=delivered %s event(s) left in the outbox of %s", result["events"], attempt)
                except outbox.OutboxLocked:
                    logger.info("stage=status reason=the outbox of %s belongs to a live process; left alone", attempt)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("stage=status reason=could not deliver the outbox of %s yet: %s", attempt, exc)
        except Exception as exc:  # noqa: BLE001
            logger.debug("stage=status reason=outbox recovery scan failed: %s", exc)

    Thread(target=work, name="status-outbox-recovery", daemon=True).start()


def _close_status_store(args: argparse.Namespace) -> None:
    store = getattr(args, "_status_store", None)
    if store is not None:
        store.close()


def _execution_of(args) -> StatusTracker | None:
    execution = getattr(args, "execution", None)
    return execution if isinstance(execution, StatusTracker) else None


def _record_phase(args, phase_id: str, kind: str, index: int | None, edge_type: str | None, call: Callable[[], int]) -> int:
    """Run ``call`` as a recorded phase of the execution: started before, finished by its result."""
    execution = _execution_of(args)
    if execution is None:
        return call()
    execution.phase_started(phase_id, kind, index, edge_type)
    try:
        result = call()
    except BaseException:
        execution.phase_finished(phase_id, status_model.FAILED)
        raise
    execution.phase_finished(phase_id, status_model.SUCCEEDED if result == 0 else status_model.FAILED)
    return result


def _start_status_telemetry(args, schema, runtime, cleanups: list) -> None:
    """Start the run's telemetry acquisition: role snapshots and exits, the relationship lag poll, process OOMs."""
    execution = _execution_of(args)
    paths = getattr(args, "runtime", None)
    if execution is None or paths is None:
        return
    try:
        interval = metrics_module.status_interval_seconds()
        entities = [("node", node.label) for node in getattr(schema, "nodes", ())] + \
                   [("edge", edge.type) for edge in getattr(schema, "edges", ())]
        poller = TelemetryPoller(execution, paths.timings, interval=interval, entities=entities,
                                 handles=getattr(runtime, "status_handles", None))
        poller.start()
        cleanups.append(poller.stop)                  # after the roles stopped (their cleanups were registered later)
        args._status_poller = poller

        def on_lag(lag_by_edge_type: dict) -> None:
            observed_at = status_model.timestamp(time.time())
            for edge_type, lag in lag_by_edge_type.items():
                execution.update_type("edge", edge_type, lag=None if lag is None else status_model.observed(
                    lag, observed_at, "cli-lag-poll"))

        set_lag_listener(on_lag)
        cleanups.append(lambda: set_lag_listener(None))
        lifecycle = getattr(runtime, "lifecycle", None)
        if lifecycle is not None and hasattr(lifecycle, "add_oom_listener"):
            lifecycle.add_oom_listener(lambda before, after: execution.group_oom(before, after))
    except Exception as exc:  # noqa: BLE001 - status never stops a load
        logger.warning("stage=status reason=telemetry could not start: %s", exc)


def main(args_list: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if args_list is None else args_list)
    configure_logging(kind="cli", argv=argv)          # text: logging.basicConfig as before; json: JSON lines (read before parsing)
    parser = build_parser()
    args = parser.parse_args(strip_log_format(argv))
    execution = _begin_execution(args)
    if execution is None:
        return _dispatch(args)
    args.execution = execution
    try:
        return _run_tracked(args, execution)
    finally:
        set_log_context(execution_id=None, run_id=None, runtime=None)     # an in-process caller's next command is a new execution


def _run_tracked(args: argparse.Namespace, execution: StatusTracker) -> int:
    try:
        code = _dispatch(args)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else (0 if exc.code is None else 1)
        execution.finish(code)
        _close_status_store(args)
        raise
    except KeyboardInterrupt:
        execution.request_stop()
        execution.finish(130, reason="interrupted")
        _close_status_store(args)
        raise
    except BaseException as exc:
        execution.finish(1, error=exc)
        _close_status_store(args)
        raise
    execution.finish(code)
    _close_status_store(args)
    return code


def _dispatch(args: argparse.Namespace) -> int:
    if args.command in {"start", "stop", "status", "replay-dlq"}:
        if args.runtime_backend not in RUNTIME_BACKENDS:
            logger.error("stage=runtime reason=%s must be one of %s, got %r", RUNTIME_ENV, ", ".join(RUNTIME_BACKENDS), args.runtime_backend,
                         extra=structured("runtime", reason="invalid runtime", setting=RUNTIME_ENV, value=str(args.runtime_backend)))
            return 1
        if args.runtime_backend == "process" and args.command != "replay-dlq":
            from neo4j_graph_loader.orchestrator.process_service import ProcessPreflightError, check_platform
            try:
                check_platform()
            except ProcessPreflightError as exc:
                logger.error("stage=runtime reason=%s", exc, extra=structured("runtime", reason=str(exc)))
                return 1
            if args.command == "start":
                for given, option in ((getattr(args, "_explicit_network", False), "--network"),
                                      (getattr(args, "skip_image_build", False), "--skip-image-build")):
                    if given:
                        logger.error("stage=runtime reason=%s applies to Docker containers and is not accepted with --runtime process", option,
                                     extra=structured("runtime", reason="option not accepted with --runtime process", option=option))
                        return 1
    if args.command in {"stop", "status"} and args.runtime_backend != "process":
        run_status = args.command == "status" and _wants_run_status(args)          # run status reads any runtime's attempt directory
        for given, option in (((None if run_status else getattr(args, "run_dir", None)), "--run-dir"), (getattr(args, "recover", False), "--recover")):
            if given:
                logger.error("stage=runtime reason=%s applies to --runtime process only", option,
                         extra=structured("runtime", reason="option applies to --runtime process only", option=option))
                return 1
    if args.command == "start" and args.runtime_backend != "process":
        for given, option in ((args.metrics_addr, "--metrics-addr"), (args.metrics_port, "--metrics-port"), (args.health_addr, "--health-addr"), (args.child_logs, "--child-logs"),
                              (args.health_stale_after, "--health-stale-after")):
            if given:
                logger.error("stage=runtime reason=%s applies to --runtime process only", option,
                         extra=structured("runtime", reason="option applies to --runtime process only", option=option))
                return 1
    if args.command == "start":
        try:
            _merge_deprecated_lease_flags(args)
            _validate_edge_mode_flags(args)
            _validate_early_numeric_args(args)
            _validated_run_id(args.run_id)
            _validated_status_store(args)
        except ValueError as exc:
            logger.error("stage=launch reason=%s", exc)
            return 1
    if hasattr(args, "config"):
        from contextlib import ExitStack
        from neo4j_graph_loader._resources import schema_path

        with ExitStack() as stack:
            try:
                args.config = str(stack.enter_context(schema_path(args.config, sample=getattr(args, "sample", "default"))))
            except OSError as exc:
                logger.error("stage=schema reason=%s", exc)
                return 1
            if args.command == "start":
                # Arguments are valid and the schema file exists (an invalid flag or a missing file creates no state,
                # as ever): from here a start-up failure is recorded in the attempt directory, schema load included.
                if _execution_of(args) is not None and os.path.exists(args.config):
                    try:
                        _open_status_store(args, _execution_of(args))
                    except StatusStoreRefused as exc:
                        logger.error("stage=status reason=%s", exc)
                        return 1
                try:
                    schema = load_schema(args.config)
                    if (args.mode == "bulk" and not getattr(args, "edge_classic", False)
                            and not getattr(args, "edge_plan", None) and _dead_letter_edge_types(schema)):
                        _bucket_lease_numeric_args(args)
                    args.runtime = prepare_run(args, args.config)
                except (OSError, ValueError, SchemaLoadError) as exc:
                    logger.error("stage=runtime directory=%s reason=%s", getattr(args, "runtime_root", "-"), exc,
                                 extra=structured("runtime", directory=str(getattr(args, "runtime_root", "-")), reason=str(exc)))
                    return 1
                args.config = str(args.runtime.schema)
            return args.func(args)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
