"""A process that records a running execution (JSON store, heartbeat thread, optional E14 registry ``cli`` record).

``python e15_status_child.py ATTEMPT registry|noregistry`` prints ``ready`` once its first status is on disk and then
heartbeats every 0.2 s until it is stopped, paused (SIGSTOP) or killed (SIGKILL) by the test.
"""
import os
import sys
import threading
import time
from pathlib import Path

from neo4j_graph_loader.orchestrator import run_registry
from neo4j_graph_loader.status import StatusTracker, json_store, locator

attempt, registry = Path(sys.argv[1]), sys.argv[2] == "registry"
execution = StatusTracker(mode="stream", run_id="child-run", runtime="process")
store = json_store.JsonStatusStore(attempt, snapshot_interval=0.0, fsync=False)
execution.attach_journal(store)
execution.subscribe(on_snapshot=store.offer_snapshot)
locator.write_locator(attempt, locator.build_locator(store="json", execution_id=execution.execution_id, run_id="child-run",
                                                    created_at=execution.snapshot()["started_at"]))
if registry:
    run_registry.RunRegistry(attempt / "registry.jsonl").append(
        "cli", pid=os.getpid(), pgid=os.getpgid(0), **(run_registry.process_identity(os.getpid()) or {}))
execution.role_started("node-loader", "Person", 0, container_id="c1")
store.write_snapshot()
print("ready", flush=True)


def beat() -> None:
    while True:
        execution.heartbeat()
        time.sleep(0.2)


threading.Thread(target=beat, daemon=True).start()
while True:
    time.sleep(1)
