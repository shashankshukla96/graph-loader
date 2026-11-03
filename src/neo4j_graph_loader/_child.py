"""Entry point of a process-runtime role: ``python -I -u -m neo4j_graph_loader._child <module> [args...]``.

Sequence: apply ``LOADER_CPUSET``, adopt the control socket, start the lifeline thread, then run the
role module exactly as ``python -m <module>`` would (``runpy`` with ``__main__`` semantics), so the role's
own early signal handlers, logging set-up and exit codes are unchanged. The role signals readiness through
``neo4j_graph_loader._child_protocol.runtime_ready``.

Lifeline: end of file on the control socket means the parent died. The wrapper then redirects stdout/stderr to
/dev/null (their reader is gone), sends SIGTERM to its own process so the role's ordinary graceful shutdown runs,
and exits hard after a bounded deadline.
"""
from __future__ import annotations

import os
import runpy
import signal
import socket
import sys
import threading
import time

from neo4j_graph_loader import _child_protocol as protocol

DEFAULT_LIFELINE_DEADLINE_S = 30.0
HARD_EXIT_CODE = 137        # what a SIGKILL after the grace would have reported


def parse_cpuset(spec: str) -> set[int]:
    """``"12-15,18"`` -> {12, 13, 14, 15, 18}; raises ValueError on anything else."""
    cpus: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            raise ValueError(f"empty element in cpuset {spec!r}")
        low, dash, high = part.partition("-")
        first = int(low)
        last = int(high) if dash else first
        if first < 0 or last < first:
            raise ValueError(f"invalid cpuset range {part!r}")
        cpus.update(range(first, last + 1))
    return cpus


def apply_cpuset(spec: str | None) -> set[int] | None:
    if not spec:
        return None
    cpus = parse_cpuset(spec)
    if not hasattr(os, "sched_setaffinity"):
        sys.stderr.write("WARNING: stage=runtime-child LOADER_CPUSET ignored: this platform has no sched_setaffinity\n")
        return None
    os.sched_setaffinity(0, cpus)
    return cpus


def _lifeline(channel: socket.socket, deadline: float) -> None:
    try:
        while channel.recv(4096):
            pass
    except OSError:
        pass
    # The parent is gone, and with it the reader of our stdout/stderr pipe: any further write would raise
    # BrokenPipeError inside the role's graceful shutdown. Point both at /dev/null first, then stop the role.
    try:
        null = os.open(os.devnull, os.O_WRONLY)
        os.dup2(null, 1)
        os.dup2(null, 2)
    except OSError:
        pass
    os.kill(os.getpid(), signal.SIGTERM)
    time.sleep(deadline)
    os._exit(HARD_EXIT_CODE)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        sys.stderr.write("usage: python -m neo4j_graph_loader._child <module|script:PATH> [args...]\n")
        return 2
    target, role_args = argv[0], argv[1:]
    script = None
    if target.startswith("script:"):
        if os.environ.get(protocol.ALLOW_SCRIPT_ENV) != "1":
            sys.stderr.write("script targets are a test seam and need GRAPH_LOADER_CHILD_ALLOW_SCRIPT=1\n")
            return 2
        script = target[len("script:"):]
    elif not target.startswith("neo4j_graph_loader."):
        sys.stderr.write(f"refusing to run a module outside neo4j_graph_loader: {target}\n")
        return 2
    apply_cpuset(os.environ.get("LOADER_CPUSET"))
    descriptor = os.environ.pop(protocol.CONTROL_FD_ENV, None)
    if descriptor is not None:
        channel = socket.socket(fileno=int(descriptor))
        protocol.install(channel)
        deadline = float(os.environ.pop(protocol.LIFELINE_DEADLINE_ENV, DEFAULT_LIFELINE_DEADLINE_S))
        threading.Thread(target=_lifeline, args=(channel, deadline), name="parent-lifeline", daemon=True).start()
    if script is not None:
        sys.argv = [script, *role_args]
        runpy.run_path(script, run_name="__main__")
    else:
        sys.argv = [target, *role_args]
        runpy.run_module(target, run_name="__main__", alter_sys=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
