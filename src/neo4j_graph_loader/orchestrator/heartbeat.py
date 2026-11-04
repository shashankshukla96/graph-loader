"""Process-wide supervision-loop heartbeat for ``/healthz``.

The CLI's supervision loops (the bulk and relationship monitors' wait/supervise ticks, the planned phase's poll loop)
call :func:`beat` once per tick. ``/healthz`` is unhealthy when the newest beat is older than a threshold: a loop that
is blocked or dead no longer beats. Before the first beat (start-up, schema apply, launching) there is nothing to judge.
"""
from __future__ import annotations

import threading
import time

_lock = threading.Lock()
_last: float | None = None
_clock = time.monotonic


def beat() -> None:
    global _last
    with _lock:
        _last = _clock()


def reset() -> None:
    global _last
    with _lock:
        _last = None


def age() -> float | None:
    """Seconds since the newest beat, or None when no loop has beaten yet."""
    with _lock:
        return None if _last is None else _clock() - _last
