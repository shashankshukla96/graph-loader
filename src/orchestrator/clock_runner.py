"""Container entry point for one canonical rotating relationship clock."""
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
from src.orchestrator.fleet_contract import parse_fleet_edge_types, select_fleet_edges
from src.orchestrator.rotation import build_rotation_plan
from src.utils.schema_loader import load_schema


logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one shared relationship rotation clock")
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--fleet-edge-types", required=True)
    parser.add_argument("--coordination-topic", required=True)
    args = parser.parse_args(argv)
    producer = None
    try:
        if not isinstance(args.run_id, str) or not args.run_id.strip():
            raise ValueError("stage=clock reason=run id must be nonblank")
        types = parse_fleet_edge_types(args.fleet_edge_types)
        schema = load_schema(args.config)
        if args.coordination_topic != schema.loading.coordination.topic:
            raise ValueError("stage=clock reason=coordination topic does not match schema")
        edges = select_fleet_edges(schema.edges, types)
        plan = build_rotation_plan(build_conflict_families(edges), bucket_count=schema.loading.coordination.bucket_count)
        producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
        shutdown = Event()
        previous = signal.signal(signal.SIGTERM, lambda *_args: shutdown.set())
        try:
            return GlobalBatchClock(run_id=args.run_id, edges=list(edges), config=schema.loading.coordination,
                producer=producer, health_probe=lambda: None, wall_clock_ms=lambda: int(time.time() * 1000),
                sleeper=time.sleep, rotation_plan=plan).run(shutdown)
        finally:
            signal.signal(signal.SIGTERM, previous)
    except Exception as exc:
        logger.error("stage=clock run_id=%s reason=%s", args.run_id or "unknown", exc)
        return 1
    finally:
        if producer is not None:
            producer.flush()


if __name__ == "__main__":
    raise SystemExit(main())
