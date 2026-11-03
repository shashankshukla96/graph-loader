"""Stand-in CLI that starts real planner worker processes on a scan that never finishes.

usage: e14_planner_standin.py ROOT WATCH(0|1)     prints one JSON line: {"pid", "workers": [...], "helper": pid|null}
"""
import json
import multiprocessing
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))       # the repository root, for ``tests.fixtures``

from neo4j_graph_loader.loader.edge_planner import KafkaReadTask, ParallelEdgePlanner, PlanSettings, PlannerSupervision
from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime
from neo4j_graph_loader.utils.schema_loader import load_schema
from tests.fixtures.e14_planner_fake import BlockingConsumerFactory


def main():
    root, watch = Path(sys.argv[1]), sys.argv[2] == "1"
    schema = load_schema(Path(__file__).resolve().parents[2] / "config" / "graph_schema.yaml")
    edge = next(item for item in schema.edges if item.type == "WORKS_AT")
    runtime = ProcessRuntime(root)
    runtime.register_cli()
    seen = []

    def on_process(info):
        seen.append(info)
        runtime.register_external(info["role"], info["pid"], info["index"])

    tasks = [KafkaReadTask(edge, edge.topics[0], ((partition, 0, 100),), "kafka:9092", "e14-standin",
                           read_timeout_seconds=3600.0) for partition in (0, 1)]
    planner = ParallelEdgePlanner(
        tasks, PlanSettings(topic="chunks", workers=2, memory_mb=64, spill_dir=str(root / "spill")), "kafka:9092",
        types={edge.type: (edge.nodes.source, edge.nodes.target)}, plan_id="standin",
        context=multiprocessing.get_context("spawn"), consumer_factory=BlockingConsumerFactory(),
        supervision=PlannerSupervision(parent_watch=watch, on_process=on_process),
    )
    planner.start()
    workers = [item["pid"] for item in seen if item["role"] == "planner-worker"]
    helper = next((item["pid"] for item in seen if item["role"] == "planner-helper"), None)
    print(json.dumps({"pid": os.getpid(), "workers": workers, "helper": helper}), flush=True)
    while True:
        time.sleep(0.2)


if __name__ == "__main__":
    main()
