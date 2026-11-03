"""A stand-in CLI for process-runtime tests: registers itself, starts idle and busy role children, and on
SIGTERM performs an "ordered stop" (stops every child, leaves a marker). Prints one JSON line when ready."""
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime

root, idle, busy, deadline = Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
role = str(Path(__file__).with_name("e14_role.py"))
runtime = ProcessRuntime(root, lifeline_deadline=deadline)
runtime.register_cli()
markers = root / "markers"
markers.mkdir()
handles = []
for index in range(idle):
    handles.append(runtime.spawn("idle", "script:" + role, ["ready", str(markers / f"idle-{index}")],
                                 {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"}))
for index in range(busy):
    handles.append(runtime.spawn("busy", "script:" + role, ["busy", str(markers / f"busy-{index}")],
                                 {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"}))
stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())
print(json.dumps({"pid": os.getpid(), "children": [h.pid for h in handles]}), flush=True)
while not stop.is_set():
    time.sleep(0.05)
for handle in handles:
    handle.stop(timeout=5)
(root / "ordered-stop").write_text("done")
runtime.close()
