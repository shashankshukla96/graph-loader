#!/usr/bin/env python3
"""Run E00's exact pytest command with independent process-group supervision.

Testcontainers/Ryuk owns Docker cleanup. This supervisor terminates only its
own child process group, including an orphaned retry child after pytest exits.
It never stops/removes containers discovered from the host's global inventory.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
COMMAND = [sys.executable, "-m", "pytest",
           "tests/integration/test_multi_topic_relationship.py::"
           "test_dead_letter_arrival_stream_type_wide_dlq_drained_by_the_retry_consumer",
           "-m", "integration", "-v"]


def inventory():
    result = subprocess.run(["docker", "ps", "-a", "--format", "{{.ID}} {{.Image}} {{.Names}}"],
                            capture_output=True, text=True, timeout=15)
    return {"exit_code": result.returncode, "containers": result.stdout.splitlines()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()
    COMMAND[0] = str(Path(args.python).absolute())
    output = args.output
    output.mkdir(parents=True, exist_ok=False)
    before = inventory()
    started = time.monotonic()
    expired = False
    with (output / "pytest.log").open("w") as log:
        child = subprocess.Popen(COMMAND, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=1200)
        except subprocess.TimeoutExpired:
            expired, code = True, 124
        finally:
            # The group belongs exclusively to this invocation. A pytest hard
            # timeout can bypass finally blocks and leave a retry subprocess.
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            else:
                time.sleep(5)
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            child.wait(timeout=15)
    result = {"command": COMMAND, "cwd": str(ROOT), "exit_code": code, "timeout": expired,
              "elapsed_seconds": round(time.monotonic() - started, 3),
              "containers_before": before, "containers_after": inventory()}
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
