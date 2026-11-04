"""Control channel between the process runtime and a role child.

The parent hands each child one end of a socketpair (``GRAPH_LOADER_CONTROL_FD``). A role calls
:func:`runtime_ready` once its signal handlers are installed and its servers are bound; the
parent treats that message as "this child can take a SIGTERM and here are its ports". End of
file on the socket means the parent died. Outside a ``_child`` wrapper nothing is installed and
:func:`runtime_ready` does nothing, so Docker mode and direct ``python -m`` runs are unaffected.
"""
from __future__ import annotations

import json
import socket
import threading
from typing import Mapping

CONTROL_FD_ENV = "GRAPH_LOADER_CONTROL_FD"
LIFELINE_DEADLINE_ENV = "GRAPH_LOADER_LIFELINE_DEADLINE_S"
ALLOW_SCRIPT_ENV = "GRAPH_LOADER_CHILD_ALLOW_SCRIPT"

_lock = threading.Lock()
_channel: socket.socket | None = None
_ready_sent = False


def install(channel: socket.socket) -> None:
    global _channel, _ready_sent
    with _lock:
        _channel, _ready_sent = channel, False


def runtime_ready(bound_ports: Mapping[str, int] | None = None, metrics: Mapping[str, object] | None = None) -> bool:
    """Tell the parent this role is ready (handlers installed, endpoints bound). Idempotent; best effort.

    ``metrics`` is the role's metrics endpoint status (``metrics.metrics_status``): listening with its port, disabled or failed."""
    global _ready_sent
    with _lock:
        if _channel is None or _ready_sent:
            return False
        message = json.dumps({"type": "ready", "ports": dict(bound_ports or {}), "metrics": dict(metrics or {})}, sort_keys=True) + "\n"
        try:
            _channel.sendall(message.encode())
        except OSError:
            return False        # the parent is gone; the lifeline thread is already stopping this process
        _ready_sent = True
        return True
