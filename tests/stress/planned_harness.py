"""In-process planned-mode harness: the real planner -> dispatcher -> writer pipeline.

Runs `KafkaEdgePlanner -> PlannedDispatcher -> N PlannedEdgeWriterWorker`
threads against a real Neo4j/Kafka pair, with no Docker container beyond
those two, and returns the same completion proof the CLI's own
`run_planned_edge_phase` checks (`src.orchestrator.planned_edges.verify_ledger`).
Every star-topology and correctness test in this slice that exercises
planned mode shares this one harness, so a bug in the wiring is fixed once.

Correction vs. the Cartographer's sketch (`plan/v6/slice-4/stories.md`
Story 3): writers are NOT built through
`src.loader.edge_execution.build_edge_execution` -- traced
`src/loader/planned_writer.py::main()` (the real planned-writer process
entry point), which builds them directly as ``ApocLockedEdgeWriter(driver,
edge_config, explicit_locks=False, sort_rows=False,
missing_endpoint_error_factory=MissingRelationshipEndpointError)``. This
harness matches that exact call.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import logging
from pathlib import Path
import threading
import time
from typing import Any
import uuid

from src.loader.edge_execution import ApocLockedEdgeWriter
from src.loader.edge_loader import MissingRelationshipEndpointError
from src.loader.edge_plan import DEFAULT_CHUNK_ROWS, DEFAULT_MAX_MESSAGE_BYTES, EdgePlan
from src.loader.edge_planner import DEFAULT_HUB_DEGREE, DEFAULT_PLAN_BUCKETS, KafkaEdgePlanner, PlanSettings
from src.loader.planned_writer import (
    ChunkDecoder, ChunkFetcher, DispatcherClient, PlannedEdgeWriterWorker, build_planned_writers, planned_consumer_config,
)
from src.orchestrator.planned_dispatcher import PlannedDispatcher
from src.orchestrator.planned_edges import verify_ledger

logger = logging.getLogger(__name__)

_ATTEMPTS_PREFIX = "stage=edge-write "
_WRITER_LOGGER_NAME = "src.loader.planned_writer"


class PlannedHarnessError(RuntimeError):
    """Raised when the in-process planned-mode harness itself fails to complete a run."""


@dataclass
class PlannedRunResult:
    plan: EdgePlan
    ledger: dict = field(default_factory=dict)
    total_attempts_over_one: int = 0
    wall_seconds: float = 0.0
    attempt_lines: int = 0


class _AttemptsHandler(logging.Handler):
    """Collects the ``attempts=%s`` field off every planned-writer edge-write log line."""

    def __init__(self) -> None:
        super().__init__()
        self.attempts: list[int] = []

    def emit(self, record: logging.LogRecord) -> None:
        if not record.getMessage().startswith(_ATTEMPTS_PREFIX) or not record.args:
            return
        try:
            self.attempts.append(int(record.args[-1]))
        except (TypeError, ValueError):  # pragma: no cover - defensive only
            pass


def _dispatcher_event_loop() -> tuple[asyncio.AbstractEventLoop, threading.Thread]:
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="planned-harness-dispatcher", daemon=True)
    thread.start()
    return loop, thread


def _run_on_loop(loop: asyncio.AbstractEventLoop, coroutine, timeout: float = 30.0) -> Any:
    return asyncio.run_coroutine_threadsafe(coroutine, loop).result(timeout)


def run_planned_mode(
    schema, driver, kafka_bootstrap: str, *, plan_dir: Path, writers: int = 4,
    plan_hub_degree: int | None = None, plan_buckets: int | None = None,
    chunk_rows: int | None = None, plan_workers: int = 2, timeout_s: float | None = None,
) -> PlannedRunResult:
    """Plan, dispatch, and write every relationship of ``schema``'s declared edge topics.

    Seeds nothing: the caller has already written every node (this
    package's Decision 5: node-loading correctness is
    `test_node_loader_integration.py`'s job, not this harness's). Records
    must already be published to the schema's edge topics before calling
    this. Raises `PlannedHarnessError`/`PlannedEdgeError` on anything
    `run_planned_edge_phase` would also fail on: an incomplete ledger, a
    writer exiting nonzero, a chunk written twice, or (when `timeout_s` is
    given) any writer still running past the deadline -- in which case
    every writer is asked to stop and the dispatcher is aborted (so a
    genuinely stuck writer's peers exit quickly instead of hanging too)
    before raising.
    """
    started = time.monotonic()
    run_id = uuid.uuid4().hex[:12]
    plan_path = Path(plan_dir) / f"edge-plan-{run_id}.json"
    settings = PlanSettings(
        topic=f"graph-loader-plan-chunks-{run_id}",
        hub_degree=DEFAULT_HUB_DEGREE if plan_hub_degree is None else plan_hub_degree,
        buckets=DEFAULT_PLAN_BUCKETS if plan_buckets is None else plan_buckets,
        chunk_rows=DEFAULT_CHUNK_ROWS if chunk_rows is None else chunk_rows,
        workers=plan_workers,
    ).validate()
    planner = KafkaEdgePlanner(schema, kafka_bootstrap, settings, plan_id=run_id, plan_path=plan_path)
    loop: asyncio.AbstractEventLoop | None = None
    loop_thread: threading.Thread | None = None
    dispatcher: PlannedDispatcher | None = None
    fetchers: list[ChunkFetcher] = []
    consumers: list[Any] = []
    clients: list[DispatcherClient] = []
    control: DispatcherClient | None = None
    pool: ThreadPoolExecutor | None = None
    stop = threading.Event()
    handler = _AttemptsHandler()
    writer_logger = logging.getLogger(_WRITER_LOGGER_NAME)
    previous_level = writer_logger.level
    writer_logger.addHandler(handler)
    writer_logger.setLevel(logging.INFO)
    try:
        result = planner.finish(plan_path)
        plan, digest = result.plan, result.digest
        if not plan.chunks:
            return PlannedRunResult(plan, {}, 0, time.monotonic() - started)

        loop, loop_thread = _dispatcher_event_loop()
        dispatcher = PlannedDispatcher(plan, digest, ready_labels=plan.resources.labels)
        host, port = _run_on_loop(loop, dispatcher.start("127.0.0.1", 0))
        address = f"{host}:{port}"

        from confluent_kafka import Consumer

        edge_configs = {edge.type: edge for edge in schema.edges}
        # Keyed like planned_writer.main: by type, or by (type, pair) for a type of several label pairs.
        edge_writers = build_planned_writers(plan, edge_configs, lambda config: ApocLockedEdgeWriter(
            driver, config, explicit_locks=False, sort_rows=False,
            missing_endpoint_error_factory=MissingRelationshipEndpointError,
        ))
        max_message_bytes = int(plan.settings.get("max_message_bytes", DEFAULT_MAX_MESSAGE_BYTES))

        writer_ids: list[str] = []
        worker_objects: list[PlannedEdgeWriterWorker] = []
        for index in range(writers):
            writer_id = f"harness-{run_id}-w{index}"
            writer_ids.append(writer_id)
            consumer = Consumer(planned_consumer_config(
                kafka_bootstrap, f"graph-loader-planned-{run_id}-{writer_id}", max_message_bytes,
            ))
            consumers.append(consumer)
            decoder = ChunkDecoder(plan, edge_configs)
            fetcher = ChunkFetcher(consumer, plan.topic, decoder)
            fetchers.append(fetcher)
            client = DispatcherClient(address, digest=digest, writer_id=writer_id)
            clients.append(client)
            worker_objects.append(PlannedEdgeWriterWorker(
                plan, client, fetcher, edge_writers, writer_id=writer_id, shutdown_requested=stop.is_set,
            ))

        pool = ThreadPoolExecutor(max_workers=writers)
        futures = {pool.submit(worker.run): writer_ids[index] for index, worker in enumerate(worker_objects)}
        done, pending = concurrent.futures.wait(futures, timeout=timeout_s)
        if pending:
            stop.set()
            try:
                abort_client = DispatcherClient(address, digest=digest)
                abort_client.connect()
                abort_client.abort(f"planned harness timeout after {timeout_s}s")
                abort_client.close()
            except Exception:  # pragma: no cover - best-effort: raise the timeout regardless
                pass
            still_pending = [futures[future] for future in pending]
            raise PlannedHarnessError(
                f"planned run did not complete within {timeout_s}s; still running: {still_pending}"
            )
        codes = {futures[future]: future.result() for future in done}
        failed = [writer_id for writer_id, code in codes.items() if code != 0]
        if failed:
            raise PlannedHarnessError(f"planned writer(s) exited nonzero: {failed}")

        control = DispatcherClient(address, digest=digest)
        control.connect()
        summary = control.summary()
        per_type = verify_ledger(plan, summary)  # raises PlannedEdgeError on any inconsistency

        # A vacuous "0 attempts over 1" is indistinguishable from "the log
        # handler never saw anything" (e.g. its logger name or prefix
        # drifted) unless the captured line count is checked against the
        # plan's own chunk count -- exactly one edge-write line per chunk.
        if len(handler.attempts) != len(plan.chunks):
            raise PlannedHarnessError(
                f"expected {len(plan.chunks)} edge-write log lines, captured {len(handler.attempts)} -- "
                "the attempts handler may not be observing src.loader.planned_writer, which would make "
                "total_attempts_over_one vacuously trustworthy"
            )

        return PlannedRunResult(
            plan=plan, ledger=per_type,
            total_attempts_over_one=sum(max(0, attempts - 1) for attempts in handler.attempts),
            wall_seconds=time.monotonic() - started,
            attempt_lines=len(handler.attempts),
        )
    finally:
        writer_logger.removeHandler(handler)
        writer_logger.setLevel(previous_level)
        if pool is not None:
            pool.shutdown(wait=False)
        for client in clients:
            try:
                client.close()
            except Exception:  # pragma: no cover - best-effort cleanup
                pass
        if control is not None:
            try:
                control.close()
            except Exception:  # pragma: no cover
                pass
        for fetcher in fetchers:
            try:
                fetcher.close()
            except Exception:  # pragma: no cover
                pass
        for consumer in consumers:
            try:
                consumer.close()
            except Exception:  # pragma: no cover
                pass
        if dispatcher is not None and loop is not None:
            try:
                _run_on_loop(loop, dispatcher.close())
            except Exception:  # pragma: no cover
                pass
        if loop is not None and loop_thread is not None:
            loop.call_soon_threadsafe(loop.stop)
            loop_thread.join(timeout=10)
            if not loop_thread.is_alive():
                loop.close()
            else:  # pragma: no cover - defensive: closing a still-running loop would raise
                logger.warning("stage=planned-harness dispatcher event loop thread did not stop in time")
        try:
            planner.close(delete_topic=True)
        except Exception:  # pragma: no cover
            logger.warning("stage=planned-harness could not clean up plan %s", run_id, exc_info=True)
