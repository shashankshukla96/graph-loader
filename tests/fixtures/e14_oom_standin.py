"""A stand-in command for the cgroup-OOM live cells: a ProcessRuntime (real OomWatch on its own cgroup) with an idle child and a
second child that is either a memory hog (the kernel's OOM kill, under a MemoryMax scope) or killed by hand (a manual ``kill -9``).
Prints one JSON line: exit codes of the children and the registry's oom records."""
import json
import sys
import time
from pathlib import Path

from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime
from neo4j_graph_loader.orchestrator.run_registry import RunRegistry

root, mode = Path(sys.argv[1]), sys.argv[2]
role = "script:" + str(Path(__file__).with_name("e14_role.py"))
allow = {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"}
runtime = ProcessRuntime(root, lifeline_deadline=10)
runtime.register_cli()
idle = runtime.spawn("idle", role, ["ready", str(root / "idle-marker")], allow, name="idle-0")
if mode == "hog":
    victim = runtime.spawn("hog", role, ["hog", "4000", "0.002"], allow, name="hog-0")
    code = victim.wait(120)
else:
    victim = runtime.spawn("manual", role, ["ready", str(root / "manual-marker")], allow, name="manual-0")
    victim.kill()
    code = victim.wait(30)
time.sleep(1.0)
oom = [r for r in RunRegistry.read(root / "registry.jsonl") if r["kind"] == "oom"]
idle_alive = idle.status == "running"
print(json.dumps({"victim_code": code, "idle_alive": idle_alive, "oom_records": oom}), flush=True)
idle.stop(timeout=5)
runtime.close()
if len(sys.argv) > 3 and sys.argv[3] == "fail" and code not in (0, None):
    sys.exit(1)                  # what the command does when a role is lost: stop the run, exit 1 (the service manager restarts it)
