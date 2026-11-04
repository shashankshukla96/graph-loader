"""A stand-in for ``graph-loader start`` on a stream schema without relationship types, ``--runtime process`` (decision D14-5):
real ProcessRuntime children, the real ``cli._supervise_node_loaders_only``, SIGTERM -> shutdown Event. Prints one JSON line when
the loaders are up and its own exit code is the supervision result."""
import json
import os
import signal
import sys
from pathlib import Path
from threading import Event

from neo4j_graph_loader import cli
from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime

root, count = Path(sys.argv[1]), int(sys.argv[2])
role = str(Path(__file__).with_name("e14_role.py"))
runtime = ProcessRuntime(root, lifeline_deadline=10)
runtime.register_cli()
shutdown = Event()
signal.signal(signal.SIGTERM, lambda *_: shutdown.set())
handles = [runtime.spawn("node-loader", "script:" + role, ["ready", str(root / f"node-{i}")], {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"},
                         name=f"node-X-{i}", labels={"component": "node-loader"}) for i in range(count)]
cleanups = []
print(json.dumps({"pid": os.getpid(), "children": [h.pid for h in handles]}), flush=True)
try:
    code = cli._supervise_node_loaders_only(handles, shutdown, cleanups, interval=0.1)
finally:
    for cleanup in reversed(cleanups):
        cleanup()
    runtime.close()
sys.exit(code)
