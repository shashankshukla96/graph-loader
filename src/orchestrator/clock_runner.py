"""Container entry point for one canonical rotating relationship clock.

P5-S1 Story 6: retired from ``--mode stream``'s *default* dispatch path.
Reachable only via the deprecated ``--stream-clock-rotation`` flag,
scheduled for removal one release after ``plan/v5/slice-1``. Not used by
bulk mode.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import time
from threading import Event

from confluent_kafka import Producer

from src.orchestrator.coordination import GlobalBatchClock
from src.orchestrator.dependency_manager import build_conflict_families
from src.orchestrator.fleet_contract import parse_fleet_edge_types, select_shared_fleet_edges
from src.orchestrator.rotation import build_rotation_plan
from src.utils.schema_loader import load_schema
from src.utils import metrics as metrics_module


logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one shared relationship rotation clock")
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--fleet-edge-types", required=True)
    parser.add_argument("--coordination-topic", required=True)
    args = parser.parse_args(argv)
    # P6-S5 Story 3: install the handler immediately after argparse, before
    # `load_schema`, `build_rotation_plan`, or `Producer(...)` construction —
    # matching node_loader.py/edge_loader.py/planned_writer.py's own
    # "handler right after argparse" shape. This closes the avoidable window
    # (a schema-file read plus a Kafka client construction) during which a
    # SIGTERM used to terminate the process immediately with no `finally`
    # running at all. The irreducible window before this line — before any
    # Python bytecode in this function has run at all — cannot be closed by
    # any process kind; that residual risk is accepted everywhere, not just
    # here.
    shutdown = Event()
    signal.signal(signal.SIGTERM, lambda *_args: shutdown.set())
    producer = None
    process_metrics = metrics_module.process_metrics()
    metrics_server = metrics_module.start_metrics_server(process_metrics, kind="clock", name="-")
    try:
        if not isinstance(args.run_id, str) or not args.run_id.strip():
            raise ValueError("stage=clock reason=run id must be nonblank")
        types = parse_fleet_edge_types(args.fleet_edge_types)
        schema = load_schema(args.config)
        if args.coordination_topic != schema.loading.coordination.topic:
            raise ValueError("stage=clock reason=coordination topic does not match schema")
        edges = select_shared_fleet_edges(schema.edges, types)
        plan = build_rotation_plan(build_conflict_families(edges), bucket_count=schema.loading.coordination.bucket_count)
        producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
        return GlobalBatchClock(run_id=args.run_id, edges=list(edges), config=schema.loading.coordination,
            producer=producer, health_probe=lambda: None, wall_clock_ms=lambda: int(time.time() * 1000),
            sleeper=time.sleep, rotation_plan=plan).run(shutdown)
    except Exception as exc:
        logger.error("stage=clock run_id=%s reason=%s", args.run_id or "unknown", exc)
        return 1
    finally:
        # Nested finally (P6-S2 review R5): a raise from `producer.flush()`
        # must never skip the handler-restore/snapshot/metrics-server
        # shutdown below.
        try:
            if producer is not None:
                producer.flush()
        finally:
            metrics_module.write_metrics_snapshot(
                process_metrics, os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH")
            )
            metrics_module.stop_metrics_server(metrics_server)
            # F2: last statement of the shutdown path: CPython resets every Python-level handler to
            # SIG_DFL at interpreter exit (only SIG_IGN survives), so a repeated SIGTERM from a second
            # container stop would otherwise exit 143 during teardown.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)


if __name__ == "__main__":
    raise SystemExit(main())
