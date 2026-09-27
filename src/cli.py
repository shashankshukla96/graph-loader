"""
src/cli.py
──────────
Command-line interface for the Neo4j Graph Loader.
"""
import argparse
import hashlib
import sys
import logging
import os
import re
import signal
import uuid
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from threading import Condition, Event, Lock

from docker.errors import NotFound

from src.utils.schema_loader import load_schema
from src.orchestrator.schema_initializer import (
    get_neo4j_driver,
    apply_schema,
    SchemaInitializationError
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


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


from src.orchestrator.docker_service import DockerService
from src.orchestrator.bulk_monitor import BulkMonitor
from src.orchestrator.dependency_manager import (
    build_conflict_families,
    build_relationship_conflict_plan,
)
from src.orchestrator.fleet_contract import select_fleet_edges
from src.orchestrator.rotation import build_rotation_plan
from src.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor
from src.models.schema import EdgeConfig
from src.orchestrator.isolation import build_relationship_isolation_plan


def _is_running(container: object) -> bool:
    """Return whether an exact tracked Docker container still needs a stop attempt."""
    try:
        container.reload()
    except NotFound:
        return False
    except Exception:
        return True
    state = getattr(container, "attrs", {}).get("State", {})
    status = state.get("Status", getattr(container, "status", ""))
    return status in {"running", "created", "restarting"}


def _stop_exact_containers(
    containers: Iterable[object], *, identities: dict[int, str] | None = None,
) -> list[str]:
    """Stop only supplied containers, retrying a failed exact target once if live."""
    identities = identities or {}
    failures: list[tuple[object, Exception]] = []
    for container in containers:
        try:
            container.stop()
        except Exception as exc:
            failures.append((container, exc))

    unresolved: list[str] = []
    for container, first_error in failures:
        if not _is_running(container):
            continue
        try:
            container.stop()
        except Exception as retry_error:
            if _is_running(container):
                target = identities.get(id(container), getattr(container, "name", "<unknown>"))
                unresolved.append(f"{target}: {retry_error}")
            continue
        if _is_running(container):
            target = identities.get(id(container), getattr(container, "name", "<unknown>"))
            unresolved.append(f"{target}: {first_error}")
    return unresolved


def _remove_exact_containers(containers: Iterable[object]) -> None:
    """Remove only retained run containers after their diagnostics were read."""
    for container in containers:
        try:
            container.remove(force=True)
        except NotFound:
            pass
        except Exception as exc:
            logger.error("stage=shutdown container cleanup failed: %s", exc)


def _run_relationship_bulk_stages(schema, docker_service: DockerService, args: argparse.Namespace) -> int:
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
                    edge_type=edge.type, topic=edge.topic, mode="bulk", config_path=args.config,
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


def _run_bulk_relationship_edge(
    edge: EdgeConfig,
    docker_service: DockerService,
    args: argparse.Namespace,
    *,
    run_id: str,
    consumer_group_prefix: str,
    phase_index: int | None = None,
    cancelled: Event | None = None,
    on_launch: Callable[[str, tuple[object, ...]], None] | None = None,
) -> int:
    """Run one edge through its exact finite Kafka boundary and drain proof."""
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
        if edge.nodes.is_self_referencing is not (phase_index is not None):
            raise ValueError("edge self-reference does not match bulk phase")
    except (AttributeError, ValueError) as exc:
        logger.error("%s failed: %s", context, exc)
        return 1

    containers: list[object] = []
    tracked: dict[tuple[str, str], object] = {}
    monitor = None
    try:
        if cancelled is not None and cancelled.is_set():
            return 1
        returned = docker_service.run_edge_loader(
            edge_type=edge.type,
            topic=edge.topic,
            mode="bulk",
            config_path=args.config,
            replicas=edge.replicas,
            network=args.network,
            run_id=run_id,
            consumer_group_prefix=consumer_group_prefix,
            slot_gating=False,
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
        )
        timeout = args.bulk_timeout_seconds
        monitor.wait_for_assignment_coverage(timeout)
        monitor.capture_boundary()
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


def _run_isolated_relationship_bulk_phase(
    edge: EdgeConfig,
    docker_service: DockerService,
    args: argparse.Namespace,
    *,
    run_id: str,
    phase_index: int,
    consumer_group_prefix: str,
) -> int:
    """Prove one exact, clock-free self-reference relationship bulk phase drained."""
    return _run_bulk_relationship_edge(
        edge, docker_service, args, run_id=run_id,
        phase_index=phase_index, consumer_group_prefix=consumer_group_prefix,
    )


def _run_completion_driven_bulk_edges(
    edges: tuple[EdgeConfig, ...], docker_service: DockerService,
    args: argparse.Namespace, *, parent_run_id: str, consumer_group_prefix: str,
    ready_labels: set[str] | None = None,
    ready_condition: Condition | None = None,
    cancelled: Event | None = None,
) -> int:
    """Run disjoint endpoint-label edges concurrently; release labels after drain."""
    if not edges:
        return 0
    try:
        plan = build_relationship_conflict_plan(edges)
        if _validated_run_id(parent_run_id) != parent_run_id:
            raise ValueError("parent run id must be valid")
        if any(edge.nodes.is_self_referencing is not False for edge in edges):
            raise ValueError("shared bulk schedule contains a self-referencing edge")
    except (AttributeError, ValueError) as exc:
        logger.error("stage=bulk-scheduler parent_run_id=%s failed: %s", parent_run_id, exc)
        return 1

    by_type = {edge.type: edge for edge in edges}
    pending = set(by_type)
    active: dict[object, str] = {}
    launched: dict[str, tuple[object, ...]] = {}
    launch_lock = Lock()
    cancelled = cancelled or Event()
    if ready_labels is None:
        ready_labels = {
            label for edge in edges for label in (edge.nodes.source, edge.nodes.target)
        }
    if ready_condition is None:
        ready_condition = Condition()

    def register(edge_type: str, containers: tuple[object, ...]) -> None:
        with launch_lock:
            launched[edge_type] = containers

    # A future only releases its endpoint labels after zero lag and
    # DRAIN_COMPLETE are proved.
    with ThreadPoolExecutor(max_workers=len(edges), thread_name_prefix="bulk-edge") as executor:
        while pending or active:
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
            for edge_type in sorted(pending):
                with ready_condition:
                    dependencies_ready = {
                        by_type[edge_type].nodes.source,
                        by_type[edge_type].nodes.target,
                    }.issubset(ready_labels)
                if not dependencies_ready:
                    continue
                if plan.conflicts[edge_type] & occupied:
                    continue
                run_id = _derive_relationship_phase_run_id(
                    parent_run_id, phase_kind="bulk", phase_index=0, edge_type=edge_type,
                )
                logger.info("Starting bulk relationship edge=%s run_id=%s", edge_type, run_id)
                future = executor.submit(
                    _run_bulk_relationship_edge, by_type[edge_type], docker_service, args,
                    run_id=run_id, consumer_group_prefix=consumer_group_prefix,
                    cancelled=cancelled, on_launch=register,
                )
                active[future] = edge_type
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
                try:
                    if future.result() != 0:
                        failures.append(edge_type)
                except Exception as exc:
                    logger.error("stage=bulk-scheduler edge=%s failed: %s", edge_type, exc)
                    failures.append(edge_type)
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
    docker_service: DockerService,
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
    if edge.nodes.is_self_referencing is not True:
        logger.error("%s failed: edge is not self-referencing", context)
        return 1
    containers: list[object] = []
    tracked: dict[tuple[str, str], object] = {}
    monitor = None
    try:
        returned = docker_service.run_edge_loader(
            edge_type=edge.type, topic=edge.topic, mode="stream", config_path=args.config,
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
            consumer_group_prefix=consumer_group_prefix,
        )
        shutdown = Event()
        previous_int = signal.signal(signal.SIGINT, lambda *_args: shutdown.set())
        previous_term = signal.signal(signal.SIGTERM, lambda *_args: shutdown.set())
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
    schema, docker_service: DockerService, args: argparse.Namespace, *, parent_run_id: str,
    shared_already_loaded: bool = False,
) -> int:
    """Run completion-driven bulk or clocked stream edges with isolation."""
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
            result = _run_isolated_relationship_stream_phase(
                edges_by_type[phase.edge_type], docker_service, args, run_id=phase_run_id,
                phase_index=phase.index, consumer_group_prefix=schema.loading.consumer_group_id,
                parent_run_id=parent_run_id,
            )
            if result:
                logger.error("stage=isolation parent_run_id=%s phase_run_id=%s phase=%s edge=%s failed", parent_run_id, phase_run_id, phase.index, phase.edge_type)
            return result
        if not shared_edges:
            return 0
        phase_run_id = _derive_relationship_phase_run_id(parent_run_id, phase_kind="shared", phase_index=0, edge_type="shared")
        result = _run_rotating_relationship_fleet(
            schema, docker_service, args, selected_edges=shared_edges, phase_run_id=phase_run_id,
        )
        if result:
            logger.error("stage=isolation parent_run_id=%s phase_run_id=%s phase=0 edge=shared failed", parent_run_id, phase_run_id)
        return result

    if shared_edges and not shared_already_loaded:
        result = _run_completion_driven_bulk_edges(
            shared_edges, docker_service, args, parent_run_id=parent_run_id,
            consumer_group_prefix=schema.loading.consumer_group_id,
        )
        if result:
            logger.error("stage=bulk-scheduler parent_run_id=%s shared relationship phase failed", parent_run_id)
            return result
    for phase in plan.isolated_phases:
        phase_run_id = _derive_relationship_phase_run_id(
            parent_run_id, phase_kind="isolated", phase_index=phase.index, edge_type=phase.edge_type,
        )
        result = _run_isolated_relationship_bulk_phase(
            edges_by_type[phase.edge_type], docker_service, args, run_id=phase_run_id,
            phase_index=phase.index, consumer_group_prefix=schema.loading.consumer_group_id,
        )
        if result:
            logger.error("stage=isolation parent_run_id=%s phase_run_id=%s phase=%s edge=%s failed", parent_run_id, phase_run_id, phase.index, phase.edge_type)
            return result
    return 0


def _run_rotating_relationship_fleet(
    schema, docker_service: DockerService, args: argparse.Namespace, *,
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
                    edge_type=edge.type, topic=edge.topic, mode=mode, config_path=args.config,
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
        )
        if mode == "stream":
            shutdown = Event()
            previous_int = signal.signal(signal.SIGINT, lambda *_args: shutdown.set())
            previous_term = signal.signal(signal.SIGTERM, lambda *_args: shutdown.set())
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

def handle_start(args: argparse.Namespace) -> int:
    logger.info(f"Starting pipeline in {args.mode} mode with config {args.config}")

    # 1. Load schema
    try:
        schema = load_schema(args.config)
    except Exception as e:
        logger.error(f"Failed to load schema: {e}")
        return 1

    try:
        explicit_run_id = _validated_run_id(getattr(args, "run_id", None))
    except ValueError as exc:
        logger.error("stage=launch reason=invalid run id: %s", exc)
        return 1

    # 2. Get DB config
    uri, user, password = get_neo4j_credentials()

    # 3. Apply schema
    try:
        driver = get_neo4j_driver(uri, user, password)
        try:
            apply_schema(driver, schema)
        finally:
            driver.close()
        logger.info("Schema initialization complete. Ready for ingestion.")
    except SchemaInitializationError as e:
        logger.error(f"Schema initialization failed: {e}")
        return 1

    docker_service = DockerService()
    edge_configs = tuple(getattr(schema, "edges", ()))
    if getattr(args, "skip_image_build", False):
        logger.info("Using the pre-built node loader image.")
    else:
        logger.info("Building node loader image...")
        try:
            docker_service.build_image()
        except Exception as e:
            logger.error(f"Failed to build node loader image: {e}")
            return 1
        if edge_configs:
            logger.info("Building relationship loader image...")
            try:
                docker_service.build_edge_image()
                if args.mode == "stream":
                    docker_service.build_clock_image()
            except Exception as e:
                logger.error(f"stage=clock Failed to build relationship loader image: {e}")
                return 1

    is_bulk = args.mode == "bulk"
    bulk_timeout_seconds = getattr(args, "bulk_timeout_seconds", 300.0)
    if is_bulk and bulk_timeout_seconds <= 0:
        logger.error("--bulk-timeout-seconds must be positive")
        return 1
    parent_run_id = explicit_run_id or uuid.uuid4().hex
    run_id = parent_run_id if is_bulk or explicit_run_id is not None else None
    started_containers = []
    tracked_containers = {}
    monitor = None
    drain_started = False
    for node_config in schema.nodes:
        try:
            containers = docker_service.run_node_loader(
                node_label=node_config.label,
                topic=node_config.topic,
                mode=args.mode,
                config_path=args.config,
                replicas=node_config.replicas,
                network=args.network,
                **({"run_id": run_id} if run_id is not None else {}),
            )
            # Preserve every returned object before validating the result so a
            # malformed launch response is still safely cleaned up.
            started_containers.extend(containers)
            if len(containers) != node_config.replicas:
                raise RuntimeError(
                    f"loader launch returned {len(containers)} replicas for {node_config.label}; "
                    f"expected {node_config.replicas}"
                )
            if is_bulk:
                for replica_index, container in enumerate(containers):
                    tracked_containers[(node_config.label, str(replica_index))] = container
            logger.info(f"Started node loader for {node_config.label}")
        except Exception as e:
            logger.error(f"Failed to launch node loader for {node_config.label}: {e}")
            _stop_exact_containers(started_containers)
            return 1

    if not is_bulk:
        return _run_relationship_orchestration(
            schema, docker_service, args, parent_run_id=parent_run_id,
        ) if edge_configs else 0

    try:
        monitor = BulkMonitor(
            schema,
            run_id,
            set(tracked_containers),
            tracked_containers,
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        )
        monitor.wait_for_assignment_coverage(bulk_timeout_seconds)
        monitor.capture_boundary()
        isolation_plan = build_relationship_isolation_plan(edge_configs)
        shared_types = set(isolation_plan.shared_edge_types)
        shared_edges = tuple(edge for edge in edge_configs if edge.type in shared_types)
        if shared_edges:
            node_labels = {node.label for node in schema.nodes}
            missing_endpoints = {
                label for edge in shared_edges
                for label in (edge.nodes.source, edge.nodes.target)
                if label not in node_labels
            }
            if missing_endpoints:
                raise ValueError(f"relationship endpoints lack node loaders: {sorted(missing_endpoints)}")
            ready_labels: set[str] = set()
            ready_condition = Condition()
            scheduler_cancelled = Event()
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="bulk-scheduler") as scheduler_executor:
                edge_future = scheduler_executor.submit(
                    _run_completion_driven_bulk_edges,
                    shared_edges, docker_service, args, parent_run_id=parent_run_id,
                    consumer_group_prefix=schema.loading.consumer_group_id,
                    ready_labels=ready_labels, ready_condition=ready_condition,
                    cancelled=scheduler_cancelled,
                )
                try:
                    remaining_labels = {node.label for node in schema.nodes}
                    while remaining_labels:
                        label = monitor.wait_for_next_label_completion(
                            remaining_labels, bulk_timeout_seconds,
                        )
                        label_containers = [
                            container for (node_label, _), container in tracked_containers.items()
                            if node_label == label
                        ]
                        drain_started = True
                        failures = _stop_exact_containers(label_containers)
                        if failures:
                            raise RuntimeError(
                                f"unable to stop node label={label}: {'; '.join(failures)}"
                            )
                        monitor.wait_for_label_drain_complete(label, bulk_timeout_seconds)
                        monitor.verify_label_zero_lag(label)
                        with ready_condition:
                            ready_labels.add(label)
                            ready_condition.notify_all()
                        remaining_labels.remove(label)
                    if edge_future.result() != 0:
                        raise RuntimeError("shared bulk relationship scheduler failed")
                except Exception:
                    scheduler_cancelled.set()
                    with ready_condition:
                        ready_condition.notify_all()
                    _stop_exact_containers(started_containers)
                    edge_future.result()
                    raise
            return _run_relationship_orchestration(
                schema, docker_service, args, parent_run_id=parent_run_id,
                shared_already_loaded=True,
            )
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
        logger.error("Bulk run failed: %s", exc)
        if not drain_started:
            _stop_exact_containers(started_containers)
        return 1
    finally:
        if monitor is not None:
            monitor.close()


def handle_stop(args: argparse.Namespace) -> int:
    loader = args.loader
    logger.info(f"Stopping loader(s)...")
    docker_service = DockerService()
    docker_service.stop_node_loaders(node_label=loader)
    logger.info("Loaders stopped.")
    return 0


def handle_status(args: argparse.Namespace) -> int:
    logger.info(f"Checking status for config: {args.config}")
    docker_service = DockerService()
    containers = docker_service.list_node_loaders()
    if not containers:
        logger.info("No loaders running.")
    for c in containers:
        logger.info(f"Loader {c['name']} ({c['id']}): {c['status']} [Node: {c['node_label']}]")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Neo4j Graph Loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Start command
    parser_start = subparsers.add_parser("start", help="Start the ingestion pipeline")
    parser_start.add_argument("--config", default="config/graph_schema.yaml", help="Path to schema YAML")
    parser_start.add_argument("--mode", choices=["bulk", "stream"], required=True, help="Ingestion mode")
    parser_start.add_argument("--network", default="graph-loader-net", help="Docker network for containers")
    parser_start.add_argument(
        "--skip-image-build",
        action="store_true",
        help="use an image built earlier by an external orchestrator",
    )
    parser_start.add_argument(
        "--bulk-timeout-seconds",
        type=float,
        default=300.0,
        help="Maximum seconds for each bulk lifecycle stage",
    )
    parser_start.add_argument(
        "--run-id",
        help="optional validated run identity shared by the rotating relationship fleet",
    )
    parser_start.set_defaults(func=handle_start)

    # Stop command
    parser_stop = subparsers.add_parser("stop", help="Stop running ingestion containers")
    parser_stop.add_argument("--loader", help="Specific loader name to stop")
    parser_stop.set_defaults(func=handle_stop)

    # Status command
    parser_status = subparsers.add_parser("status", help="Check pipeline status and lags")
    parser_status.add_argument("--config", default="config/graph_schema.yaml", help="Path to schema YAML")
    parser_status.set_defaults(func=handle_status)

    return parser


def main(args_list: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(args_list)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
