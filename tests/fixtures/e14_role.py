"""A configurable stand-in role for process-runtime tests (run through ``_child`` as ``script:<this file>``).

usage: e14_role.py MODE [ARG...]   (a marker file path, where a mode takes one, is always the last argument)
"""
import json
import os
import signal
import sys
import time

from neo4j_graph_loader._child_protocol import runtime_ready

mode, args = sys.argv[1], sys.argv[2:]


def marker(text, path):
    with open(path, "a") as handle:
        handle.write(text + "\n")


def graceful(path, code=0):
    def handler(_signum, _frame):
        marker("term", path)
        sys.exit(code)
    signal.signal(signal.SIGTERM, handler)


if mode == "ready":                       # graceful role: marker on SIGTERM, exit 0
    graceful(args[0])
    runtime_ready({"control": 7777})
    while True:
        time.sleep(0.05)
elif mode == "slow-ready":                # imports for a while, then installs its handler and reports ready
    time.sleep(float(args[0]))
    graceful(args[1])
    runtime_ready()
    while True:
        time.sleep(0.05)
elif mode == "exit-early":
    sys.exit(int(args[0]))
elif mode == "never-ready":
    graceful(args[0])
    while True:
        time.sleep(0.05)
elif mode == "ignore-term":               # a stuck role: ignores SIGTERM, so a stop ends in SIGKILL
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    runtime_ready()
    while True:
        time.sleep(0.05)
elif mode == "exit-after-ready":
    runtime_ready()
    sys.exit(int(args[0]))
elif mode == "self-signal":               # dies by a signal, not by an exit
    runtime_ready()
    os.kill(os.getpid(), int(args[0]))
    time.sleep(30)
elif mode == "dump-env":
    runtime_ready()
    with open(args[0], "w") as handle:
        json.dump({"env": dict(os.environ), "isolated": sys.flags.isolated, "ignore_environment": sys.flags.ignore_environment,
                   "cwd": os.getcwd(), "pid": os.getpid(), "argv": sys.argv,
                   "affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None}, handle)
    while True:
        time.sleep(0.05)
elif mode == "busy":                      # CPU-bound main thread; the handler still runs between bytecodes
    graceful(args[0])
    runtime_ready()
    while True:
        sum(range(10000))
elif mode == "hog":                       # allocates and touches memory until it is killed (args: megabytes to try, step pause)
    runtime_ready()
    held = []
    for _ in range(int(args[0])):
        held.append(bytearray(b"x" * (1 << 20)))
        time.sleep(float(args[1]) if len(args) > 1 else 0.005)
    time.sleep(60)
elif mode == "chatty":
    runtime_ready()
    for index in range(int(args[0])):
        print(f"line {index}")
        if index % 2:
            print(f"err {index}", file=sys.stderr)
    sys.stdout.flush()
    while True:
        time.sleep(0.05)
elif mode == "flood":                     # ready, then N lines as fast as possible (stdout and stderr), then exit 0
    runtime_ready()
    for index in range(int(args[0])):
        print(f"flood {index}")
    sys.stdout.flush()
elif mode == "quick":                     # ready, then exit 0 at once (launch-throughput tests)
    runtime_ready()
else:
    sys.exit(64)
