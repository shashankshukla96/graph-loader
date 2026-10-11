"""Bulk orchestration of a globally planned relationship load (``--edge-plan``).

Runs on the bulk scheduler thread while the main thread drives node-label
readiness.  It waits for the plan file -- written by the library planner
(:func:`start_kafka_edge_planner`, which reads the schema's relationship
topics while the nodes load) or by a producer -- validates it against the
schema and the planned Kafka topic, launches the run-scoped dispatcher and a
pool of ``K`` writer containers, forwards each node label as it becomes ready
(the dispatcher only grants chunks whose two endpoint labels are ready), and
succeeds only when:

* the dispatcher reports ``complete``;
* its completion ledger holds every planned chunk exactly once, each with its
  planned row count, and the rows sum to the plan's per-type and total rows
  (which the publisher counted from the relationship files);
* every writer container exited 0 after ``END``.

Anything else aborts the dispatcher, stops every exact container of the
phase, and fails the run.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from threading import Condition, Event
from typing import Callable, Iterable

from src.loader.edge_plan import EdgePlan, EdgePlanError, plan_failure_marker, plan_progress_path, read_plan_file
from src.loader.planned_writer import STOP_REASON, DispatcherClient
from src.orchestrator.docker_service import REJECTION_DIR


logger = logging.getLogger(__name__)

DEFAULT_PLAN_WRITERS = 10
_POLL_SECONDS = 0.1
_PROGRESS_CHECK_SECONDS = 1.0
_STOP_TIMEOUT_SECONDS = 15


class PlannedEdgeError(RuntimeError):
    """The planned relationship phase cannot prove a complete, exact load."""


def replace_stale_chunk_topic(docker_service, run_id: str, bootstrap_servers: str, *, admin_factory=None,
                              clock: Callable[[], float] = time.monotonic,
                              sleep: Callable[[float], None] = time.sleep, gone_timeout: float = 30.0) -> bool:
    """Delete a run-scoped chunk topic left behind by a killed run with the same run id.

    Refuses (``PlannedEdgeError``) while any container labelled with the run
    id is still running: a live writer may still be reading the topic.
    Returns True when a stale topic was deleted.
    """
    from src.loader.edge_planner import delete_chunk_topic, run_scoped_chunk_topic

    topic = run_scoped_chunk_topic(run_id)
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient as admin_factory
    admin = admin_factory({"bootstrap.servers": bootstrap_servers})
    if topic not in admin.list_topics(timeout=30.0).topics:
        return False
    alive = [c for c in docker_service.list_run_containers(label_filters=("app=graph-loader", f"run_id={run_id}"))
             if getattr(c, "status", "") in ("running", "restarting", "paused", "created")]
    if alive:
        raise PlannedEdgeError(
            f"chunk topic {topic} exists and {len(alive)} container(s) of run {run_id} are still alive "
            f"({', '.join(sorted(c.name for c in alive)[:5])}); stop them first "
            f"(docker rm -f $(docker ps -aq --filter label=run_id={run_id}))"
        )
    delete_chunk_topic(bootstrap_servers, topic, admin_factory=admin_factory)
    # Kafka deletes asynchronously: wait until the name is really gone, or the
    # planner's create_chunk_topic would still see (or collide with) it.
    deadline = clock() + gone_timeout
    while topic in admin.list_topics(timeout=30.0).topics:
        if clock() >= deadline:
            raise PlannedEdgeError(f"chunk topic {topic} was deleted but is still listed after {gone_timeout:.0f}s")
        sleep(0.2)
    logger.warning("stage=edge-plan reason=deleted stale chunk topic %s left by an earlier run with the same run id", topic)
    return True


def start_kafka_edge_planner(schema, args, *, run_id: str, writers: int, bootstrap_servers: str,
                             planner_factory=None, stop_requested=None):
    """Start planning every relationship from the schema's edge topics in the background.

    Planning parameters not given on ``args`` are derived from the number of
    relationship records at the boundary, the share of them in
    self-referencing label pairs and the writer count
    (:func:`~src.orchestrator.auto_settings.derive_plan_parameters`); the
    ``stage=edge-plan-settings`` line logs the bucket count's reason.
    Returns the running :class:`~src.loader.edge_planner.KafkaEdgePlanner`;
    ``args.edge_plan`` becomes the plan path the planned phase waits for.
    ``stop_requested`` (``() -> bool``) lets the background plan log an
    operator-stop abort as a WARNING instead of an ERROR (N2).
    """
    from src.loader.edge_planner import KafkaEdgePlanner, PlanSettings, run_scoped_chunk_topic
    from src.orchestrator import auto_settings

    directory = getattr(args, "plan_dir", None) or os.environ.get("GRAPH_LOADER_TIMING_DIR") or (
        Path("var") / "edge-plans" / run_id
    )
    plan_path = (Path(directory) / "edge-plan.json").resolve()
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    for stale in (plan_path, plan_failure_marker(plan_path), plan_progress_path(plan_path), ledger_path(plan_path)):
        stale.unlink(missing_ok=True)
    # A rerun with the same run id re-reads everything: drop its earlier planner rejections instead of appending.
    for stale in Path(REJECTION_DIR).glob(f"{run_id}-planner-*.jsonl"):
        stale.unlink(missing_ok=True)

    def settings_for(records: int, edge_types: int, *, self_ref_records: int = 0) -> PlanSettings:
        buckets = getattr(args, "plan_buckets", None)
        if buckets is None:
            buckets, buckets_reason = auto_settings.derive_plan_buckets(
                records, writers, edge_types=edge_types, self_ref_records=self_ref_records)
        else:  # --plan-buckets always wins
            buckets_reason = "explicit"
        derived = auto_settings.derive_plan_parameters(
            records, writers, edge_types=edge_types, buckets=buckets,
            hub_degree=getattr(args, "plan_hub_degree", None), chunk_rows=getattr(args, "plan_chunk_rows", None),
        )
        settings = PlanSettings(
            **derived, topic=getattr(args, "plan_topic", None) or run_scoped_chunk_topic(run_id),
            partitions=getattr(args, "plan_partitions", None) or 4,
            workers=getattr(args, "plan_workers", None) or auto_settings.default_plan_workers(),
            memory_mb=getattr(args, "plan_memory_mb", None) or 0,
            spill_dir=getattr(args, "plan_spill_dir", None) or "",
        )
        logger.info(
            "stage=edge-plan-settings records=%s edge_types=%s writers=%s buckets=%s hub_degree=%s chunk_rows=%s "
            "planner_workers=%s memory_mb=%s spill_dir=%s topic=%s plan=%s self_ref_records=%s buckets_reason=%s",
            records, edge_types, writers,
            settings.buckets, settings.hub_degree, settings.chunk_rows, settings.workers,
            settings.memory_mb or "auto", settings.spill_dir or plan_path.parent, settings.topic, plan_path,
            self_ref_records, buckets_reason,
        )
        return settings

    # Malformed relationship records go to the same rejection directory the classic loaders use
    # (one file per planner worker) and are skipped instead of failing the run.
    planner = (planner_factory or KafkaEdgePlanner)(schema, bootstrap_servers, settings_for, plan_id=run_id,
                                                    plan_path=plan_path, rejection_dir=Path(REJECTION_DIR))
    try:
        planner.start()
    except BaseException:
        planner.close(delete_topic=True)
        raise
    if stop_requested is not None:
        planner.stop_requested = stop_requested
    planner.run_in_background(plan_path)
    args.edge_plan = str(plan_path)
    return planner


def _read_progress(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def wait_for_plan(path: Path, timeout_seconds: float, *, cancelled: Event | None = None,
                  clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep) -> None:
    """Wait for the plan file (or the planner's failure marker) while the planner makes progress.

    The timeout is progress-based: a running planner rewrites ``<plan>.progress``
    whenever it read rows, counted degrees or published chunks, and every
    change restarts the ``timeout_seconds`` window.  A long plan of a large
    input never times out; a planner that stalls (or never starts) does.
    """
    marker = plan_failure_marker(path)
    progress_path = plan_progress_path(path)
    deadline = clock() + timeout_seconds
    last_progress = None
    last_check = None
    while True:
        if marker.exists():
            try:
                reason = marker.read_text(encoding="utf-8").strip()
            except OSError:
                reason = "unreadable failure marker"
            raise PlannedEdgeError(f"edge planner failed: {reason}")
        if path.is_file():
            return
        if cancelled is not None and cancelled.is_set():
            raise PlannedEdgeError("cancelled while waiting for the edge plan")
        now = clock()
        if last_check is None or now - last_check >= _PROGRESS_CHECK_SECONDS:
            last_check = now
            progress = _read_progress(progress_path)
            if progress is not None and progress != last_progress:
                last_progress = progress
                deadline = now + timeout_seconds
        if now >= deadline:
            detail = f"; last progress {last_progress.strip()}" if last_progress else ""
            raise PlannedEdgeError(
                f"edge plan {path} did not appear and the planner made no progress within "
                f"{timeout_seconds:.0f}s{detail}"
            )
        sleep(_POLL_SECONDS)


def validate_plan_against_schema(plan: EdgePlan, schema) -> None:
    """The plan must cover exactly the schema's edge types with matching labels."""
    edges = {edge.type: edge for edge in getattr(schema, "edges", ())}
    if set(plan.types) != set(edges):
        missing, extra = sorted(set(edges) - set(plan.types)), sorted(set(plan.types) - set(edges))
        raise PlannedEdgeError(f"plan edge types differ from the schema: missing={missing} unknown={extra}")
    node_labels = {node.label for node in schema.nodes}
    # Plans built from Kafka record the topic list of each type (its order is the precedence order of
    # a multi-topic type).  Only producer-source plans carry no topic information.  A Kafka plan
    # without the record predates multi-topic types: it read exactly one topic per type.
    stats = plan.stats if isinstance(plan.stats, dict) else {}
    plan_topics = stats.get("type_topics")
    if plan_topics is not None:
        if not isinstance(plan_topics, dict):
            raise PlannedEdgeError("plan type topics are malformed")
        for name, edge in edges.items():
            recorded = plan_topics.get(name)
            if not isinstance(recorded, list):
                raise PlannedEdgeError(
                    f"plan records no topic list for edge type {name} (got {recorded!r}); re-plan the load"
                )
            if recorded != list(edge.topics):
                raise PlannedEdgeError(
                    f"plan edge type {name} was planned from topics {recorded}; the schema lists "
                    f"{list(edge.topics)} (the topic order decides which topic wins under precedence); "
                    "re-plan the load"
                )
    elif stats.get("source") != "producer":
        boundary = stats.get("boundary")
        for name, edge in edges.items():
            if len(edge.topics) > 1:
                raise PlannedEdgeError(
                    f"plan predates multi-topic types but edge type {name} lists topics {list(edge.topics)}; "
                    "re-plan the load"
                )
            if isinstance(boundary, dict) and edge.topics[0] not in boundary:
                raise PlannedEdgeError(
                    f"plan edge type {name} was not planned from topic {edge.topics[0]}; re-plan the load"
                )
    for name, item in plan.types.items():
        edge = edges[name]
        if getattr(edge, "is_multi_pair", False) is True or item.is_multi_pair:
            # v7-S7: every topic must still bind the pair it was planned with.
            planned = {topic: pair.labels for pair in item.pairs for topic in pair.topics}
            schema_pairs = {topic: pair.labels for pair in getattr(edge, "endpoint_pairs", ())
                            for topic in pair.topics}
            if not item.is_multi_pair or not getattr(edge, "is_multi_pair", False):
                raise PlannedEdgeError(
                    f"plan edge type {name} was planned with {len(item.pairs) or 1} label pair(s); the schema "
                    f"binds {len(getattr(edge, 'endpoint_pairs', ())) or 1}; re-plan the load"
                )
            for topic in sorted(set(planned) | set(schema_pairs)):
                if planned.get(topic) != schema_pairs.get(topic):
                    plan_side = "->".join(planned[topic]) if topic in planned else "nothing"
                    schema_side = "->".join(schema_pairs[topic]) if topic in schema_pairs else "nothing"
                    raise PlannedEdgeError(
                        f"plan binds {topic} to {plan_side}; the schema binds it to {schema_side}; re-plan the load"
                    )
            if [pair.labels for pair in item.pairs] != [pair.labels for pair in edge.endpoint_pairs]:
                raise PlannedEdgeError(f"plan edge type {name} label pairs differ from the schema; re-plan the load")
        elif (edge.nodes.source, edge.nodes.target) != (item.source_label, item.target_label):
            raise PlannedEdgeError(f"plan edge type {name} endpoint labels differ from the schema")
        if getattr(edge.execution, "mode", "python_apoc") != "python_apoc":
            raise PlannedEdgeError(f"planned mode requires python_apoc edges; {name} is {edge.execution.mode}")
    missing_loaders = sorted(set(plan.resources.labels) - node_labels)
    if missing_loaders:
        raise PlannedEdgeError(f"plan labels lack node loaders: {missing_loaders}")


def verify_plan_topic(plan: EdgePlan, bootstrap_servers: str, *, consumer_factory=None) -> None:
    """The planned topic must hold exactly the plan's chunk messages."""
    if consumer_factory is None:
        from confluent_kafka import Consumer

        def consumer_factory(config):
            return Consumer(config)
    from confluent_kafka import TopicPartition

    consumer = consumer_factory({
        "bootstrap.servers": bootstrap_servers, "group.id": f"graph-loader-plan-verify-{plan.plan_id}",
        "enable.auto.commit": False,
    })
    try:
        metadata = consumer.list_topics(topic=plan.topic, timeout=10)
        topic = metadata.topics.get(plan.topic)
        if topic is None or getattr(topic, "error", None) is not None:
            raise PlannedEdgeError(f"planned topic {plan.topic} metadata unavailable")
        if set(topic.partitions) != set(range(plan.partitions)):
            raise PlannedEdgeError(
                f"planned topic {plan.topic} has partitions {sorted(topic.partitions)}; plan expects {plan.partitions}"
            )
        ranges = {}
        for partition in range(plan.partitions):
            ranges[partition] = consumer.get_watermark_offsets(TopicPartition(plan.topic, partition), timeout=10)
    finally:
        consumer.close()
    retained = sum(high - low for low, high in ranges.values())
    if retained != len(plan.chunks):
        raise PlannedEdgeError(
            f"planned topic {plan.topic} retains {retained} message(s); plan has {len(plan.chunks)} chunk(s)"
        )
    for chunk in plan.chunks:
        low, high = ranges[chunk.partition]
        if not low <= chunk.offset < high:
            raise PlannedEdgeError(f"chunk {chunk.id} offset {chunk.partition}:{chunk.offset} is not retained")


def verify_ledger(plan: EdgePlan, summary: dict) -> dict[str, dict]:
    """Prove every planned chunk committed exactly once with its planned rows."""
    if summary.get("state") != "complete":
        raise PlannedEdgeError(f"dispatcher is {summary.get('state')}: {summary.get('failure')}")
    if summary.get("plan_id") != plan.plan_id:
        raise PlannedEdgeError("dispatcher summary belongs to another plan")
    ledger = summary.get("ledger")
    if not isinstance(ledger, list):
        raise PlannedEdgeError("dispatcher summary has no ledger")
    seen: set[int] = set()
    rows_by_type = {name: 0 for name in plan.types}
    for entry in ledger:
        if not isinstance(entry, list) or len(entry) != 6:
            raise PlannedEdgeError("malformed ledger entry")
        chunk_id, _writer, rows = entry[0], entry[1], entry[2]
        if not isinstance(chunk_id, int) or not 0 <= chunk_id < len(plan.chunks):
            raise PlannedEdgeError(f"ledger names unknown chunk {chunk_id}")
        if chunk_id in seen:
            raise PlannedEdgeError(f"ledger records chunk {chunk_id} twice")
        seen.add(chunk_id)
        chunk = plan.chunks[chunk_id]
        if rows != chunk.rows:
            raise PlannedEdgeError(f"ledger records {rows} rows for chunk {chunk_id}; plan has {chunk.rows}")
        rows_by_type[chunk.edge_type] += rows
    if len(seen) != len(plan.chunks):
        raise PlannedEdgeError(f"ledger holds {len(seen)} of {len(plan.chunks)} chunks")
    for name, item in plan.types.items():
        if rows_by_type[name] != item.rows:
            raise PlannedEdgeError(f"edge type {name} committed {rows_by_type[name]} rows; plan has {item.rows}")
    if sum(rows_by_type.values()) != plan.total_rows:
        raise PlannedEdgeError("committed rows differ from the planned total")
    types = summary.get("types") or {}
    return {
        name: {"rows": rows_by_type[name], "chunks": item.chunks, **{
            key: value for key, value in (types.get(name) or {}).items() if key not in {"rows", "chunks"}
        }}
        for name, item in plan.types.items()
    }


def _container_exit_code(container) -> int | None:
    """Return the exit code of an exited container, or None while it runs."""
    try:
        container.reload()
    except Exception as exc:
        raise PlannedEdgeError(f"container {getattr(container, 'name', '?')} disappeared: {exc}") from exc
    state = getattr(container, "attrs", {}).get("State", {})
    if state.get("Status") in {"exited", "dead"}:
        code = state.get("ExitCode")
        return code if isinstance(code, int) else 1
    return None


def ledger_path(plan_path: Path) -> Path:
    return plan_path.with_name(plan_path.name + ".ledger.json")


def persist_ledger(plan_path: Path, plan: EdgePlan, digest: str, summary: dict) -> Path:
    """Durably write the verified completion ledger beside the plan (fsync'd)."""
    path = ledger_path(plan_path)
    data = json.dumps({
        "plan_id": plan.plan_id, "plan_digest": digest, "state": summary.get("state"),
        "chunks": len(plan.chunks), "rows": plan.total_rows, "ledger": summary.get("ledger"),
    }, separators=(",", ":")).encode("utf-8") + b"\n"
    temporary = path.with_name(f".{path.name}.tmp")
    with open(temporary, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return path


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def _record_timings(plan: EdgePlan, summary: dict, per_type: dict[str, dict], writers: int,
                    elapsed_seconds: float) -> None:
    directory = os.environ.get("GRAPH_LOADER_TIMING_DIR")
    if not directory:
        return
    try:
        root = Path(directory)
        for name, item in per_type.items():
            if "first_grant" not in item:
                continue
            (root / f"stage-{name}.json").write_text(json.dumps({
                "edge_type": name, "started_utc": _iso(item["first_grant"]),
                "completed_utc": _iso(item["last_done"]),
                "elapsed_seconds": item["last_done"] - item["first_grant"], "succeeded": True,
            }) + "\n", encoding="utf-8")
        ledger = summary.get("ledger") or []
        first = min((entry[3] for entry in ledger), default=None)
        last = max((entry[4] for entry in ledger), default=None)
        (root / "planned-summary.json").write_text(json.dumps({
            "plan_id": plan.plan_id, "topic": plan.topic, "writers": writers, "settings": plan.settings,
            "plan_stats": plan.stats, "chunks": len(plan.chunks), "rows": plan.total_rows,
            "dispatcher_stats": summary.get("stats"), "types": per_type,
            "first_grant_utc": None if first is None else _iso(first),
            "last_done_utc": None if last is None else _iso(last),
            "dispatch_seconds": None if first is None else last - first,
            "phase_seconds": elapsed_seconds,
        }, indent=1) + "\n", encoding="utf-8")
        (root / "planned-ledger.json").write_text(json.dumps(ledger) + "\n", encoding="utf-8")
    except OSError as exc:
        logger.warning("stage=planned-edges could not record timings: %s", exc)


@dataclass
class _PhaseContainers:
    dispatcher: object | None = None
    writers: tuple = ()


def _stop_phase(containers: _PhaseContainers, stop_exact, remove_exact) -> None:
    if containers.writers:
        failures = stop_exact(containers.writers)
        if failures:
            logger.error("stage=planned-edges writer shutdown failed: %s", "; ".join(failures))
    if containers.dispatcher is not None:
        failures = stop_exact([containers.dispatcher])
        if failures:
            logger.error("stage=planned-edges dispatcher shutdown failed: %s", "; ".join(failures))
        try:
            logs = containers.dispatcher.logs()
            if isinstance(logs, bytes):
                logs = logs.decode("utf-8", errors="replace")
            for line in logs.splitlines():
                if "stage=planned-dispatcher-stats" in line or "stage=planned-dispatch-failed" in line:
                    logger.info("%s", line.split(": ", 1)[-1])
            directory = os.environ.get("GRAPH_LOADER_TIMING_DIR")
            if directory:
                (Path(directory) / "planned-dispatcher.log").write_text(logs, encoding="utf-8")
        except Exception as exc:
            logger.warning("stage=planned-edges could not read dispatcher log: %s", exc)
    remove_exact([*containers.writers, *([containers.dispatcher] if containers.dispatcher is not None else [])])


def _log_writer_failures(writers: Iterable[object]) -> None:
    for container in writers:
        try:
            code = _container_exit_code(container)
            if code not in (None, 0):
                tail = container.logs(tail=30)
                if isinstance(tail, bytes):
                    tail = tail.decode("utf-8", errors="replace")
                logger.error("stage=diagnostics container=%s exit_code=%s last log lines:\n%s",
                             getattr(container, "name", "?"), code, tail)
        except Exception:
            continue


def run_planned_edge_phase(
    schema, docker_service, args, *, parent_run_id: str, ready_labels: set[str], ready_condition: Condition,
    cancelled: Event, control_factory=DispatcherClient, stop_exact=None, remove_exact=None,
    clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
    bootstrap_servers: str | None = None, verify_topic=verify_plan_topic,
    stop_requested: Callable[[], bool] = lambda: False,
) -> int:
    """Run the whole planned relationship phase; 0 only on a proven exact load."""
    if stop_exact is None or remove_exact is None:
        from src.cli import _remove_exact_containers, _stop_exact_containers
        stop_exact = stop_exact or _stop_exact_containers
        remove_exact = remove_exact or _remove_exact_containers
    started = clock()
    timeout = float(getattr(args, "bulk_timeout_seconds", 300.0))
    writers_count = int(getattr(args, "plan_writers", None) or DEFAULT_PLAN_WRITERS)
    plan_path = Path(args.edge_plan).resolve()
    containers = _PhaseContainers()
    control = None
    try:
        wait_for_plan(plan_path, timeout, cancelled=cancelled, clock=clock, sleep=sleep)
        plan, digest = read_plan_file(plan_path)
        validate_plan_against_schema(plan, schema)
        verify_topic(plan, bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        if not plan.chunks:
            logger.info("stage=planned-edges plan_id=%s has no relationship rows", plan.plan_id)
            return 0
        # More writers than chunks would only idle.
        writers_count = max(1, min(writers_count, len(plan.chunks)))
        logger.info(
            "stage=planned-edges plan_id=%s chunks=%s rows=%s resources=%s hubs=%s writers=%s wait_s=%.3f",
            plan.plan_id, len(plan.chunks), plan.total_rows, plan.resources.count, plan.resources.hub_count,
            writers_count, clock() - started,
        )
        with ready_condition:
            sent = set(ready_labels)
        containers.dispatcher = docker_service.run_planned_dispatcher(
            run_id=parent_run_id, plan_path=str(plan_path), network=args.network, ready_labels=sorted(sent),
        )
        internal, host = docker_service.planned_dispatcher_addresses(containers.dispatcher, network=args.network)
        control = control_factory(host, digest=digest)
        control.connect()
        containers.writers = tuple(docker_service.run_planned_edge_writers(
            run_id=parent_run_id, plan_path=str(plan_path), config_path=args.config,
            dispatcher_address=internal, writers=writers_count, network=args.network,
        ))
        if len(containers.writers) != writers_count:
            raise PlannedEdgeError(f"launched {len(containers.writers)} writers; expected {writers_count}")
        with ready_condition:
            current = set(ready_labels)
        control.ready(current)
        sent |= current
        plan_labels = {label for chunk in plan.chunks for label in plan.labels_of(chunk)}
        progress_deadline = clock() + timeout
        done = -1
        while True:
            if cancelled.is_set():
                raise PlannedEdgeError(
                    "cancelled: stop requested" if stop_requested() else "cancelled by a node-label failure"
                )
            with ready_condition:
                if set(ready_labels) <= sent:
                    ready_condition.wait(timeout=_POLL_SECONDS)
                new = set(ready_labels) - sent
            if new:
                control.ready(new)
                sent |= new
                progress_deadline = clock() + timeout
            status = control.status()
            if status.get("state") == "failed":
                raise PlannedEdgeError(f"dispatcher failed: {status.get('failure')}")
            if status.get("done") != done:
                done = status.get("done")
                progress_deadline = clock() + timeout
            elif not status.get("granted") and not plan_labels <= sent:
                # Nothing runs because endpoint labels still load: the node-readiness
                # monitor times that out on its own (progress-based) clock.
                progress_deadline = clock() + timeout
            if status.get("state") == "complete":
                break
            for container in containers.writers:
                code = _container_exit_code(container)
                if code is not None:
                    raise PlannedEdgeError(
                        f"writer {getattr(container, 'name', '?')} exited with code {code} before the plan completed"
                    )
            if clock() >= progress_deadline:
                raise PlannedEdgeError(
                    f"no planned chunk completed within {timeout:.0f}s (done={done}/{len(plan.chunks)})"
                )
        summary = control.summary()
        per_type = verify_ledger(plan, summary)
        writer_deadline = clock() + min(timeout, 60.0)
        pending = list(containers.writers)
        while pending:
            still = []
            for container in pending:
                code = _container_exit_code(container)
                if code is None:
                    still.append(container)
                elif code != 0:
                    raise PlannedEdgeError(f"writer {getattr(container, 'name', '?')} exited with code {code}")
            pending = still
            if pending:
                if cancelled.is_set():
                    raise PlannedEdgeError("cancelled while writers were exiting")
                if clock() >= writer_deadline:
                    raise PlannedEdgeError(f"{len(pending)} writer(s) did not exit after the plan completed")
                sleep(_POLL_SECONDS)
        durable = persist_ledger(plan_path, plan, digest, summary)
        elapsed = clock() - started
        _record_timings(plan, summary, per_type, writers_count, elapsed)
        logger.info("stage=planned-edges ledger=%s", durable)
        logger.info(
            "stage=planned-edges-complete plan_id=%s chunks=%s rows=%s elapsed_s=%.3f %s",
            plan.plan_id, len(plan.chunks), plan.total_rows, elapsed,
            " ".join(f"{name}={item['rows']}" for name, item in sorted(per_type.items())),
        )
        return 0
    except (PlannedEdgeError, EdgePlanError, Exception) as exc:
        stopped = stop_requested() and str(exc).startswith("cancelled")
        if stopped:
            # An operator/signal stop: expected, not a failure.  Writers read
            # the STOP_REASON marker and exit 0 between chunks.
            logger.warning("stage=planned-edges run_id=%s stopped: operator/signal stop requested (%s)",
                           parent_run_id, exc)
        else:
            logger.error("stage=planned-edges run_id=%s failed: %s", parent_run_id, exc)
        if control is not None:
            try:
                control.abort(STOP_REASON if stopped else str(exc))
            except Exception:
                pass
        _log_writer_failures(containers.writers)
        return 1
    finally:
        if control is not None:
            control.close()
        _stop_phase(containers, stop_exact, remove_exact)
