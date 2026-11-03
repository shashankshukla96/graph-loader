"""The in-memory status model of one execution (E15.F1.S1).

A :class:`StatusTracker` is created as the very first act of ``graph-loader start`` and lives for the CLI process.
It owns the identity (``execution_id``, optional ``run_id``, phases, role incarnations), the overall state machine,
the overlapping activities, and a gap-free event history. It performs no I/O; a store (E15.F1.S3 and later)
subscribes to receive every event and every new snapshot. A subscriber failure is logged once and never reaches the
load.
"""
from __future__ import annotations

import copy
import logging
import os
import platform
import re
import socket
import threading
import time
from collections import deque
from typing import Any, Callable

from neo4j_graph_loader.status import model

logger = logging.getLogger(__name__)

MAX_ERROR_MESSAGE = 2000
MAX_EVENTS_IN_MEMORY = 10_000

_URL_PASSWORD = re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)(?P<user>[^/\s:@]*):(?P<password>[^@\s/]*)@")
_SECRET_PAIR = re.compile(r"(?i)(?P<key>pass(?:word|wd)?|secret|token|api[_-]?key|credential)(?P<sep>\s*[=:]\s*)\S+")


_ABSENT = object()


class InvalidTransition(ValueError):
    """The requested state change is not in the allowed transition table."""


def redact(text: str) -> str:
    """Mask credentials in free text before it enters a status document or event."""
    text = _URL_PASSWORD.sub(r"\g<scheme>\g<user>:***@", text)
    return _SECRET_PAIR.sub(r"\g<key>\g<sep>***", text)


def _error(error: BaseException | dict | None) -> dict | None:
    if error is None:
        return None
    if isinstance(error, BaseException):
        kind, message = type(error).__name__, str(error)
    else:
        kind, message = str(error.get("type") or "Error"), str(error.get("message") or "")
    return {"type": kind, "message": redact(message)[:MAX_ERROR_MESSAGE]}


class StatusTracker:
    """State, activities, phases, roles and events of one execution. Thread-safe; mutations take one lock."""

    def __init__(self, *, run_id: str | None = None, mode: str | None = None, runtime: str = "docker",
                 labels: dict[str, str] | None = None, execution_id: str | None = None,
                 clock: Callable[[], float] = time.time, host: str | None = None, pid: int | None = None,
                 package_version: str | None = None):
        if mode is not None and mode not in model.MODES:
            raise ValueError(f"mode must be one of {model.MODES}, got {mode!r}")
        if runtime not in model.RUNTIMES:
            raise ValueError(f"runtime must be one of {model.RUNTIMES}, got {runtime!r}")
        if run_id is not None and not re.fullmatch(model.RUN_ID_PATTERN, run_id):
            raise ValueError("run id must be 1-128 letters, digits, dot, underscore, or hyphen")
        if package_version is None:
            from neo4j_graph_loader._version import __version__ as package_version
        self._clock = clock
        self._lock = threading.RLock()
        self._stop_requested = threading.Event()
        self._on_event: list[Callable[[dict], None]] = []
        self._on_snapshot: list[Callable[[dict], None]] = []
        self._warned = False
        self._journal = None               # a durable event journal (JSON store, SQL outbox), once attached
        self._journal_warned = False
        self._role_failed = False          # a role failed (non-zero exit or OOM) before any stop was requested
        self._seq = 0
        self._events: deque[dict] = deque(maxlen=MAX_EVENTS_IN_MEMORY)
        now = self._now()
        self.execution_id = execution_id or model.new_id()
        self._doc: dict[str, Any] = {
            "schema_version": model.SCHEMA_VERSION, "kind": "status", "execution_id": self.execution_id,
            "run_id": run_id, "revision": 0, "state": model.STARTING, "outcome": None, "mode": mode,
            "runtime": runtime, "host": host or socket.gethostname() or "unknown", "pid": pid or os.getpid(),
            "versions": {"graph_loader": package_version, "python": platform.python_version()},
            "started_at": now, "updated_at": now, "heartbeat_at": now, "finished_at": None,
            "activities": {}, "phases": [], "roles": [], "labels": dict(labels or {}), "types": [], "totals": {},
            "telemetry": {"available": True, "reason": None}, "history_incomplete": False,
        }
        self._emit("execution.started", run_id=run_id, mode=mode, runtime=runtime, host=self._doc["host"],
                   pid=self._doc["pid"], versions=dict(self._doc["versions"]))

    # -- subscription ------------------------------------------------------------------------------------------
    def subscribe(self, *, on_event: Callable[[dict], None] | None = None,
                  on_snapshot: Callable[[dict], None] | None = None) -> None:
        """Receive every event (the history so far is replayed first, in order) and every new snapshot.

        Callbacks run under the tracker lock, so they see changes in order and must be quick; they never raise
        into the caller.
        """
        with self._lock:
            if on_event is not None:
                self._on_event.append(on_event)
                for event in list(self._events):
                    self._call(on_event, copy.deepcopy(event))
            if on_snapshot is not None:
                self._on_snapshot.append(on_snapshot)
                self._call(on_snapshot, self._snapshot_locked())

    def _call(self, callback: Callable[[dict], None], payload: dict) -> None:
        try:
            callback(payload)
        except Exception as exc:                        # a store must never fail the load
            if not self._warned:
                self._warned = True
                logger.warning("stage=status reason=subscriber failed: %s", redact(str(exc)))

    # -- internals ------------------------------------------------------------------------------------------------
    def _now(self) -> str:
        return model.timestamp(self._clock())

    @property
    def _finished(self) -> bool:
        return self._doc["state"] in model.TERMINAL_STATES

    def attach_journal(self, journal) -> bool:
        """Make ``journal`` the owner of ``seq``: from now on an event gets its ``seq`` when the journal durably
        appends it (``journal.append(make_event) -> event``, ``make_event(seq)`` builds the whole event). Events
        already recorded (at least ``execution.started``) are replayed into it first, in order, and must come back
        with the same ``seq``. A journal failure (now or later) detaches it, flags ``history_incomplete`` and the run
        goes on with in-memory ``seq``. Returns whether the journal is attached."""
        with self._lock:
            try:
                for event in list(self._events):
                    stored = journal.append(lambda seq, event=event: {**event, "seq": seq})
                    if stored["seq"] != event["seq"]:
                        raise ValueError(f"journal seq {stored['seq']} differs from event seq {event['seq']}")
            except Exception as exc:
                self._journal_failed(exc)
                self._changed()
                return False
            self._journal = journal
            return True

    def _journal_failed(self, exc: BaseException) -> None:
        self._journal = None
        self._doc["history_incomplete"] = True
        if not self._journal_warned:
            self._journal_warned = True
            logger.warning("stage=status reason=event journal failed, history is incomplete: %s", redact(str(exc)))

    def _emit(self, event_type: str, **fields: Any) -> dict:
        """Record an event (gap-free ``seq``), then publish the new snapshot. Caller holds the lock."""
        def make(seq: int) -> dict:
            return {"schema_version": model.SCHEMA_VERSION, "execution_id": self.execution_id, "seq": seq,
                    "ts": self._now(), "type": event_type, **fields}

        event = None
        if self._journal is not None:
            try:
                event = self._journal.append(make)
            except Exception as exc:
                self._journal_failed(exc)
        if event is None:
            event = make(self._seq + 1)
        self._seq = event["seq"]
        self._events.append(event)
        for callback in self._on_event:
            self._call(callback, copy.deepcopy(event))
        self._changed()
        return event

    def _changed(self) -> None:
        self._doc["revision"] += 1
        self._doc["updated_at"] = self._now()
        if self._on_snapshot:
            snapshot = self._snapshot_locked()
            for callback in self._on_snapshot:
                self._call(callback, snapshot)

    def _snapshot_locked(self) -> dict:
        return copy.deepcopy(self._doc)

    # -- reads ------------------------------------------------------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            return self._snapshot_locked()

    def events(self) -> list[dict]:
        """The in-memory event history (the most recent ``MAX_EVENTS_IN_MEMORY`` events)."""
        with self._lock:
            return copy.deepcopy(list(self._events))

    @property
    def state(self) -> str:
        return self._doc["state"]

    @property
    def run_id(self) -> str | None:
        return self._doc["run_id"]

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested.is_set()

    # -- identity ---------------------------------------------------------------------------------------------------
    def set_run_id(self, run_id: str) -> None:
        """Assign the logical run id once it is known (it may be generated after start-up)."""
        if not re.fullmatch(model.RUN_ID_PATTERN, run_id):
            raise ValueError("run id must be 1-128 letters, digits, dot, underscore, or hyphen")
        with self._lock:
            current = self._doc["run_id"]
            if current == run_id:
                return
            if current is not None:
                raise ValueError(f"run id is already {current!r}; an execution has one run id")
            self._doc["run_id"] = run_id
            self._emit("execution.run_id", run_id=run_id)

    def set_mode(self, mode: str) -> None:
        if mode not in model.MODES:
            raise ValueError(f"mode must be one of {model.MODES}, got {mode!r}")
        with self._lock:
            if self._doc["mode"] != mode:
                self._doc["mode"] = mode
                self._changed()

    # -- state machine --------------------------------------------------------------------------------------------
    def transition(self, state: str, reason: str | None = None) -> None:
        """Move the execution to ``state``; a terminal state also stamps ``finished_at`` (use :meth:`finish`)."""
        with self._lock:
            current = self._doc["state"]
            if state == current:
                return
            if state not in model.TRANSITIONS.get(current, ()):
                raise InvalidTransition(f"execution cannot move from {current!r} to {state!r}")
            self._doc["state"] = state
            if state in model.TERMINAL_STATES:
                self._doc["finished_at"] = self._now()
            self._emit("execution.state", **{"from": current, "to": state, "reason": reason})

    def request_stop(self) -> None:
        """Note that a stop was requested (safe from a signal handler: sets a flag, takes no lock)."""
        self._stop_requested.set()

    def heartbeat(self) -> None:
        """Refresh ``heartbeat_at``; a reader treats a silent execution as stale (the store decides the threshold)."""
        with self._lock:
            if self._finished:
                return
            self._doc["heartbeat_at"] = self._now()
            self._changed()

    # -- activities -----------------------------------------------------------------------------------------------
    def declare_activity(self, name: str) -> None:
        """Declare an activity that will run later (state ``pending``)."""
        self._set_activity(name, model.ACTIVITY_PENDING, only_if_absent=True)

    def activity(self, name: str, state: str) -> None:
        """Set an activity's state. Activities overlap freely; each keeps its own timestamps."""
        self._set_activity(name, state)

    def _set_activity(self, name: str, state: str, *, only_if_absent: bool = False) -> None:
        if name not in model.ACTIVITIES:
            raise ValueError(f"activity must be one of {model.ACTIVITIES}, got {name!r}")
        if state not in model.ACTIVITY_STATES:
            raise ValueError(f"activity state must be one of {model.ACTIVITY_STATES}, got {state!r}")
        with self._lock:
            entry = self._doc["activities"].get(name)
            if entry is not None and only_if_absent:
                return
            if self._finished and (entry is None or entry["state"] != model.RUNNING):
                return
            previous = entry["state"] if entry else model.ACTIVITY_PENDING
            if entry is not None and previous == state:
                return
            now = self._now()
            entry = entry or {"state": model.ACTIVITY_PENDING, "started_at": None, "finished_at": None, "updated_at": now}
            entry["state"], entry["updated_at"] = state, now
            if state == model.RUNNING and entry["started_at"] is None:
                entry["started_at"] = now
            if state in (model.SUCCEEDED, model.FAILED, model.STOPPED):
                entry["finished_at"] = now
                entry["started_at"] = entry["started_at"] or now
            self._doc["activities"][name] = entry
            self._emit("activity.state", activity=name, **{"from": previous, "to": state})

    # -- phases -----------------------------------------------------------------------------------------------------
    def phase_started(self, phase_id: str, kind: str, index: int | None = None, edge_type: str | None = None) -> bool:
        """Record a phase sub-run (a derived run id) as a child of the execution. False when it is already known."""
        if not re.fullmatch(model.RUN_ID_PATTERN, phase_id):
            raise ValueError("phase id must be 1-128 letters, digits, dot, underscore, or hyphen")
        with self._lock:
            if self._finished or any(phase["phase_id"] == phase_id for phase in self._doc["phases"]):
                return False
            self._doc["phases"].append({
                "phase_id": phase_id, "kind": kind, "index": index, "edge_type": edge_type,
                "state": model.RUNNING, "started_at": self._now(), "finished_at": None,
            })
            self._emit("phase.started", phase_id=phase_id, kind=kind, index=index, edge_type=edge_type)
            return True

    def phase_finished(self, phase_id: str, state: str) -> None:
        if state not in (model.SUCCEEDED, model.FAILED, model.STOPPED):
            raise ValueError(f"a phase ends as succeeded, failed or stopped, got {state!r}")
        with self._lock:
            for phase in self._doc["phases"]:
                if phase["phase_id"] == phase_id and phase["state"] == model.RUNNING:
                    phase["state"], phase["finished_at"] = state, self._now()
                    self._emit("phase.finished", phase_id=phase_id, state=state)
                    return

    # -- roles ----------------------------------------------------------------------------------------------------
    def role_started(self, role: str, name: str, replica: int | None = None, *, incarnation: str | None = None,
                     container_id: str | None = None, pid: int | None = None) -> str:
        """Record one role process start (a container or a child) and return its incarnation.

        A second start of the same ``(role, name, replica)`` is a restart: it gets a new incarnation and
        ``restart_count`` counts the earlier ones. Pass ``incarnation`` to reuse an id the runtime already
        assigned (the process runtime's registry id).
        """
        if incarnation is not None and not model.is_id(incarnation):
            raise ValueError("incarnation must be 32 lowercase hex digits")
        with self._lock:
            incarnation = incarnation or model.new_id()
            if self._finished:
                return incarnation
            if any(entry["incarnation"] == incarnation for entry in self._doc["roles"]):
                return incarnation
            restarts = sum(1 for entry in self._doc["roles"]
                           if (entry["role"], entry["name"], entry["replica"]) == (role, name, replica))
            self._doc["roles"].append({
                "role": role, "name": name, "replica": replica, "incarnation": incarnation,
                "restart_count": restarts, "state": model.RUNNING, "container_id": container_id, "pid": pid,
                "started_at": self._now(), "finished_at": None, "exit_code": None, "exit_reason": None,
                "last_error": None,
            })
            self._emit("role.started", role=role, name=name, replica=replica, incarnation=incarnation,
                       restart_count=restarts, container_id=container_id, pid=pid)
            if self._doc["state"] == model.STARTING:
                self._start_running()
            return incarnation

    def _role(self, incarnation: str) -> dict | None:
        return next((entry for entry in self._doc["roles"] if entry["incarnation"] == incarnation), None)

    def role_exited(self, incarnation: str, exit_code: int | None = None, exit_reason: str | None = None,
                    error: BaseException | dict | None = None) -> None:
        with self._lock:
            entry = self._role(incarnation)
            if entry is None or entry["state"] in ("exited", model.FAILED):
                return
            failed = bool(error) or bool(exit_code)
            if failed and not self.stop_requested:
                self._role_failed = True
            entry["state"] = model.FAILED if failed else "exited"
            entry["finished_at"] = self._now()
            entry["exit_code"] = exit_code
            entry["exit_reason"] = exit_reason or entry["exit_reason"]
            entry["last_error"] = _error(error)
            self._emit("role.exited", role=entry["role"], name=entry["name"], replica=entry["replica"],
                       incarnation=incarnation, exit_code=exit_code, exit_reason=entry["exit_reason"],
                       error=entry["last_error"])

    def group_oom(self, before: int, after: int) -> None:
        """A process-runtime cgroup OOM kill that no single child can be blamed for (the E14 ``stage=oom`` report)."""
        with self._lock:
            if self._finished:
                return
            if not self.stop_requested:
                self._role_failed = True
            self._emit("execution.oom", oom_kill_before=before, oom_kill_after=after)

    def role_oom(self, incarnation: str) -> None:
        """An out-of-memory kill of a role (the E14 ``stage=oom`` line / ``oom`` registry record)."""
        with self._lock:
            entry = self._role(incarnation)
            if entry is None:
                return
            entry["exit_reason"] = "oom"
            if not self.stop_requested:
                self._role_failed = True
            self._emit("role.oom", role=entry["role"], name=entry["name"], replica=entry["replica"],
                       incarnation=incarnation)

    # -- telemetry fields (acquired by E15.F1.S2; the shape is fixed here) ------------------------------------------------------
    def update_type(self, kind: str, name: str, **fields: dict | None) -> None:
        """Set progress fields of a node label or relationship type; a value is `{value, observed_at, source}`."""
        if kind not in model.TYPE_KINDS:
            raise ValueError(f"kind must be one of {model.TYPE_KINDS}, got {kind!r}")
        self._check_fields(fields)
        with self._lock:
            entry = next((item for item in self._doc["types"] if (item["kind"], item["name"]) == (kind, name)), None)
            if entry is None:
                entry = {"kind": kind, "name": name}
                self._doc["types"].append(entry)
            if all(entry.get(name, _ABSENT) == value for name, value in fields.items()):
                return                                # nothing new: no revision, no snapshot churn
            entry.update(copy.deepcopy(fields))
            self._changed()

    def update_totals(self, **fields: dict | None) -> None:
        self._check_fields(fields)
        with self._lock:
            if all(self._doc["totals"].get(name, _ABSENT) == value for name, value in fields.items()):
                return
            self._doc["totals"].update(copy.deepcopy(fields))
            self._changed()

    def set_telemetry(self, available: bool, reason: str | None = None) -> None:
        with self._lock:
            if self._doc["telemetry"] != {"available": available, "reason": reason}:
                self._doc["telemetry"] = {"available": available, "reason": reason}
                self._changed()

    @staticmethod
    def _check_fields(fields: dict) -> None:
        unknown = sorted(set(fields) - set(model.PROGRESS_FIELDS))
        if unknown:
            raise ValueError(f"unknown progress field(s) {unknown}; known: {list(model.PROGRESS_FIELDS)}")

    # -- errors and the end of the execution ------------------------------------------------------------------------------
    def error(self, stage: str, message: str) -> None:
        with self._lock:
            self._emit("error", stage=stage, message=redact(message)[:MAX_ERROR_MESSAGE])

    def _start_running(self) -> None:
        self._doc["state"] = model.RUNNING
        self._emit("execution.state", **{"from": model.STARTING, "to": model.RUNNING, "reason": None})
        if self._doc["mode"] == "stream":
            self._set_activity("stream", model.RUNNING)

    def finish(self, exit_code: int, *, error: BaseException | dict | None = None, reason: str | None = None) -> str:
        """End the execution (idempotent) and return its terminal state.

        An uncaught ``error`` means ``failed``, and so does a role failure (a non-zero exit that our own stop did
        not cause, or an OOM) recorded before a stop was requested. Otherwise a requested stop means ``stopped``
        (whatever the exit code, which is kept in ``outcome``); anything else is ``succeeded`` for exit code 0 and
        ``failed`` for any other. Phases and activities still running end in the same state; roles still running are marked
        ``exited`` with reason ``execution-finished``.
        """
        with self._lock:
            if self._finished:
                return self._doc["state"]
            captured = _error(error)
            if captured is not None or (self.stop_requested and self._role_failed):
                final = model.FAILED
            elif self.stop_requested:
                final = model.STOPPED
            else:
                final = model.SUCCEEDED if exit_code == 0 else model.FAILED
            if reason is None and final == model.STOPPED:
                reason = "stop requested"
            elif reason is None and final == model.FAILED and self._role_failed:
                reason = "role failure"
            if self.stop_requested and self._doc["state"] in (model.STARTING, model.RUNNING):
                self.transition(model.STOPPING, "stop requested")
            for entry in list(self._doc["roles"]):
                if entry["state"] in (model.RUNNING, model.STARTING, model.STOPPING):
                    self.role_exited(entry["incarnation"], None, "execution-finished")
            for phase in self._doc["phases"]:
                if phase["state"] == model.RUNNING:
                    self.phase_finished(phase["phase_id"], final)
            for name, entry in list(self._doc["activities"].items()):
                if entry["state"] == model.RUNNING:
                    self._set_activity(name, final)
            self._doc["outcome"] = {"exit_code": exit_code, "reason": reason, "error": captured}
            previous = self._doc["state"]
            self._doc["state"], self._doc["finished_at"] = final, self._now()
            self._emit("execution.state", **{"from": previous, "to": final, "reason": reason})
            self._emit("execution.finished", state=final, exit_code=exit_code, reason=reason, error=captured)
            return final

