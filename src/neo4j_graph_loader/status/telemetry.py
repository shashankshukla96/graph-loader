"""Telemetry acquisition for run status (E15.F1.S2): read the roles' status snapshots, fill the status model.

Transport. Every role writes ``<timings>/<execution_id>/<incarnation>/metrics.prom`` every status interval and a
``final.json`` marker after its last snapshot (``utils.metrics.write_status_snapshot``). The CLI lists the roles from
the :class:`~neo4j_graph_loader.status.tracker.StatusTracker` (which knows every incarnation), reads each one's
snapshot and computes the fields below. Nothing here talks to Kafka, Neo4j or Docker.

Rules, in force for every field:

* A value is ``{value, observed_at, source}`` or ``None``. A missing, unreadable or corrupt snapshot, a gauge whose
  companion ``*_observed_at_seconds`` is older than ``max_age`` (a fresh *file* carrying a stale *observation*), or a
  gauge that was never observed is ``None``; never ``0``, never the stale value.
* A counter total is the sum of every incarnation's last snapshot (a restarted role is a new incarnation, its
  predecessor's last snapshot still counts). It is marked ``lower_bound`` when some contributing incarnation ended
  without a final marker (killed before it could write one) or has no readable snapshot at all.
* A gauge that describes the present (lag, DLQ backlog) comes from running incarnations only.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

from neo4j_graph_loader.status import model

logger = logging.getLogger(__name__)

SNAPSHOT_NAME = "metrics.prom"          # = utils.metrics.STATUS_SNAPSHOT_NAME (kept apart: this package is stdlib-only)
FINAL_NAME = "final.json"               # = utils.metrics.STATUS_FINAL_NAME
DEFAULT_INTERVAL_SECONDS = 5.0
GAUGE_MAX_AGE_FACTOR = 3.0              # a gauge older than this many intervals is unknown
BACKLOG_MAX_AGE_SECONDS = 180.0         # the retry consumer recomputes its backlog on its own (slower) interval
STARTUP_GRACE_FACTOR = 3.0              # intervals a new role may take to write its first snapshot

NODE_ROLES = ("node-loader",)
EDGE_ROLES = ("edge-loader", "planned-edge-writer")
RETRY_ROLE = "retry-consumer"
PLANNED_ROLES = ("planned-edge-writer", "planned-dispatcher")
_RETRY_FIELDS = {"written": "retry_written", "re_dlq": "retry_redlq", "parked": "retry_parked"}
_RUNNING_STATES = (model.STARTING, model.RUNNING, model.STOPPING)

Sample = tuple[str, dict[str, str], float]


@dataclass
class Reading:
    """One incarnation's snapshot: its samples, when they were written, and whether it was its last one."""
    samples: list[Sample] | None            # None: missing or unreadable
    written_at: float | None = None
    final: bool = False
    problem: str | None = None              # "missing" or "corrupt" when samples is None


def incarnation_dir(root: Path | str, execution_id: str, incarnation: str) -> Path:
    return Path(root) / execution_id / incarnation


def parse_samples(text: str) -> list[Sample]:
    """The samples of a Prometheus text exposition. Raises ``ValueError`` on anything malformed."""
    from prometheus_client.parser import text_string_to_metric_families

    samples: list[Sample] = []
    try:
        for family in text_string_to_metric_families(text):
            for sample in family.samples:
                samples.append((sample.name, dict(sample.labels), float(sample.value)))
    except Exception as exc:                          # the parser raises several types
        raise ValueError(f"unparsable exposition: {exc}") from exc
    return samples


def read_incarnation(root: Path | str, execution_id: str, incarnation: str) -> Reading:
    directory = incarnation_dir(root, execution_id, incarnation)
    path = directory / SNAPSHOT_NAME
    try:
        info = path.stat()
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return Reading(None, problem="missing")
    except (OSError, UnicodeDecodeError):
        return Reading(None, problem="corrupt")
    try:
        samples = parse_samples(text)
    except ValueError:
        return Reading(None, problem="corrupt")
    if not text.strip():
        return Reading(None, problem="corrupt")
    return Reading(samples, written_at=info.st_mtime, final=(directory / FINAL_NAME).exists())


@dataclass
class Telemetry:
    """What one acquisition computed: per-entity fields, run totals and whether snapshots are flowing at all."""
    types: dict[tuple[str, str], dict[str, dict | None]] = field(default_factory=dict)
    totals: dict[str, dict | None] = field(default_factory=dict)
    available: bool = True
    reason: str | None = None


def _value(number: float, observed_at: float, source: str, lower_bound: bool | None = None) -> dict:
    return model.observed(int(number), model.timestamp(observed_at), source, lower_bound=lower_bound)


def _sum_counter(readings: list[tuple[dict, Reading]], family: str, match: Callable[[dict[str, str]], bool],
                 *, source: str) -> dict | None:
    """Total of a counter over ``readings`` (the incarnations that can write it), or ``None`` if none is readable."""
    readable = [(role, reading) for role, reading in readings if reading.samples is not None]
    if not readable:
        return None
    total, newest = 0.0, 0.0
    for _role, reading in readable:
        total += sum(value for name, labels, value in reading.samples if name == family and match(labels))
        newest = max(newest, reading.written_at or 0.0)
    unknown = [r for r, reading in readings if reading.samples is None]
    lost = [role for role, reading in readable if role["state"] not in _RUNNING_STATES and not reading.final]
    return _value(total, newest, source, lower_bound=True if (unknown or lost) else None)


def _gauge(readings: list[tuple[dict, Reading]], family: str, companion: str, match: Callable[[dict[str, str]], bool],
           *, now: float, max_age: float, source: str, combine: str = "sum") -> dict | None:
    """A present-tense gauge from running incarnations, each with a fresh companion observation; else ``None``.

    ``sum`` adds the running replicas (every one of them must have a fresh observation, else the sum would
    understate); ``latest`` takes the freshest observation (replicas that compute the same quantity).
    """
    running = [(role, reading) for role, reading in readings if role["state"] in _RUNNING_STATES]
    if not running:
        return None
    values: list[tuple[float, float]] = []           # (value, observed_at)
    for _role, reading in running:
        if reading.samples is None:
            if combine == "sum":
                return None
            continue
        found = [(labels, value) for name, labels, value in reading.samples if name == family and match(labels)]
        seen = [value for name, labels, value in reading.samples if name == companion and match(labels)]
        if not found or not seen or max(seen) <= 0 or now - max(seen) > max_age:
            if combine == "sum":
                return None
            continue
        values.append((sum(value for _l, value in found), max(seen)))
    if not values:
        return None
    if combine == "sum":
        return _value(sum(v for v, _t in values), min(t for _v, t in values), source)
    best = max(values, key=lambda item: item[1])
    return _value(best[0], best[1], source)


def acquire(roles: Iterable[dict], root: Path | str, execution_id: str, *, now: float | None = None,
            interval: float = DEFAULT_INTERVAL_SECONDS, entities: Iterable[tuple[str, str]] = (),
            reader: Callable[[Path | str, str, str], Reading] = read_incarnation,
            backlog_max_age: float = BACKLOG_MAX_AGE_SECONDS) -> Telemetry:
    """Compute the snapshot-derived fields for every entity from the roles' snapshots (see the module docstring)."""
    now = time.time() if now is None else now
    roles = list(roles)
    readings = [(role, reader(root, execution_id, role["incarnation"])) for role in roles]
    max_age = GAUGE_MAX_AGE_FACTOR * interval
    telemetry = Telemetry()

    grace = STARTUP_GRACE_FACTOR * interval
    running = [(role, reading) for role, reading in readings if role["state"] in _RUNNING_STATES]
    old_enough = [role for role, _ in running if now - _to_epoch(role["started_at"]) > grace]
    if running and old_enough and len(old_enough) == len(running) and all(r.samples is None for _, r in running):
        telemetry.available = False
        telemetry.reason = ("no role has written a readable status snapshot (the timing directory may not be "
                            "writable by the roles, or status snapshots are not being produced)")

    writers = {kind: [(role, reading) for role, reading in readings if role["role"] in names]
               for kind, names in (("node", NODE_ROLES), ("edge", EDGE_ROLES))}
    retry = [(role, reading) for role, reading in readings if role["role"] == RETRY_ROLE]
    planned = any(role["role"] in PLANNED_ROLES for role in roles)
    known = {(kind, name) for kind, name in entities}
    for kind in ("node", "edge"):
        for role, reading in writers[kind]:
            if role["role"] == "edge-loader" or kind == "node":
                known.add((kind, role["name"]))
            for sample_name, labels, _ in reading.samples or ():
                if sample_name == "graph_loader_records_consumed_total" and labels.get("kind") in ("node", "edge", "planned-writer"):
                    known.add(("node" if labels["kind"] == "node" else "edge", labels.get("label_or_type", "")))
    for _role, reading in retry:
        for sample_name, labels, _ in reading.samples or ():
            if sample_name in ("graph_loader_dlq_backlog", "graph_loader_retry_outcome_total") and labels.get("kind") == "edge":
                known.add(("edge", labels.get("name", "")))
    known.discard(("node", "-"))
    known.discard(("edge", "-"))

    for kind, name in sorted(known):
        # Only the roles that can write this entity count: a node label's own loaders; an edge type's loaders and
        # every planned writer (one pool serves every planned type).
        by_kind = [(role, reading) for role, reading in writers[kind]
                   if (role["role"] == "planned-edge-writer" or role["name"] == name)]
        match = (lambda labels, name=name: labels.get("label_or_type") == name)
        fields: dict[str, dict | None] = {
            "records_written": _sum_counter(by_kind, "graph_loader_records_consumed_total", match, source="role-snapshots"),
        }
        if not planned or kind == "node":
            reject = lambda labels, kind=kind, name=name: labels.get("kind") == kind and labels.get("name") == name
            fields["rejected"] = _sum_counter(by_kind, "graph_loader_rejected_total", reject, source="role-snapshots")
        if kind == "node":
            lag_match = lambda labels, name=name: labels.get("kind") == "node" and labels.get("name") == name
            fields["lag"] = _gauge(
                [(r, x) for r, x in by_kind if r["name"] == name], "graph_loader_consumer_lag_records",
                "graph_loader_consumer_lag_observed_at_seconds", lag_match, now=now, max_age=max_age,
                source="node-loader")
        else:
            if retry:
                for outcome, field_name in _RETRY_FIELDS.items():
                    outcome_match = lambda labels, name=name, outcome=outcome: (
                        labels.get("kind") == "edge" and labels.get("name") == name and labels.get("outcome") == outcome)
                    fields[field_name] = _sum_counter(retry, "graph_loader_retry_outcome_total", outcome_match,
                                                      source="retry-consumer")
                for queue, field_name in (("dlq", "dlq_lag"), ("parked", "parked_size")):
                    queue_match = lambda labels, name=name, queue=queue: (
                        labels.get("kind") == "edge" and labels.get("name") == name and labels.get("queue") == queue)
                    fields[field_name] = _gauge(retry, "graph_loader_dlq_backlog", "graph_loader_dlq_backlog_observed_at_seconds",
                                                queue_match, now=now, max_age=backlog_max_age, source="retry-consumer",
                                                combine="latest")
        telemetry.types[(kind, name)] = fields

    for field_name in ("records_written", "rejected"):
        parts = [fields[field_name] for fields in telemetry.types.values() if field_name in fields]
        if field_name == "rejected" and planned:
            continue                                  # planned: the planner's exact total, set by the CLI
        if parts and all(part is not None for part in parts):
            telemetry.totals[field_name] = {
                "value": sum(part["value"] for part in parts), "observed_at": max(part["observed_at"] for part in parts),
                "source": "role-snapshots",
                **({"lower_bound": True} if any(part.get("lower_bound") for part in parts) else {}),
            }
        elif parts:
            telemetry.totals[field_name] = None
    return telemetry


def _to_epoch(timestamp: str) -> float:
    from datetime import datetime, timezone
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp()


def observe_roles(tracker, handles: dict[str, object]) -> None:
    """Move tracker roles that have exited: exit code, OOM kill (Docker's ``OOMKilled``), last error.

    ``handles`` maps an incarnation to the runtime handle launched for it (``RoleLauncher.status_handles``). This
    reads ``handle.attrs`` only and never calls ``reload()``: the container API sees no extra traffic. A process
    handle's attributes are live; a Docker handle's are as fresh as the last ``reload()`` by the run's own monitors,
    which inspect every tracked container on each tick. A handle whose state cannot be read leaves the role as it
    was: that is unknown, not "exited".
    """
    for role in tracker.snapshot()["roles"]:
        if role["state"] not in _RUNNING_STATES:
            continue
        handle = handles.get(role["incarnation"])
        if handle is None:
            continue
        try:
            state = dict(handle.attrs.get("State") or {})
        except Exception:                             # not readable: unknown
            continue
        if state.get("Status") not in ("exited", "dead"):
            continue
        code = state.get("ExitCode")
        code = code if isinstance(code, int) and not isinstance(code, bool) else None
        if state.get("OOMKilled") is True:
            tracker.role_oom(role["incarnation"])
        tracker.role_exited(role["incarnation"], code, "oom" if state.get("OOMKilled") is True else (
            f"signal {code - 128}" if code is not None and code > 128 else None),
            error={"type": "RoleExit", "message": f"exited with code {code}"} if code not in (0, None) else None)


class TelemetryPoller:
    """Every ``interval`` seconds: acquire the snapshot fields into the tracker and refresh its heartbeat.

    A failure of one acquisition is logged once and skipped; the next tick tries again. ``entities`` are the
    schema's node labels and relationship types, so a type nobody has written yet shows ``null`` fields instead of
    being absent.
    """

    def __init__(self, tracker, root: Path | str, *, interval: float = DEFAULT_INTERVAL_SECONDS,
                 entities: Iterable[tuple[str, str]] = (), clock: Callable[[], float] = time.time,
                 handles: Callable[[], dict[str, object]] | None = None):
        self._tracker, self._root, self._interval = tracker, root, interval
        self._handles = handles
        self._sources: list[Callable] = []
        self._entities, self._clock = list(entities), clock
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="status-telemetry", daemon=True)
        self._warned = False

    def add_source(self, source: Callable) -> None:
        """Another per-tick contribution: ``source(tracker, now)``; a failing source is skipped, never fatal."""
        self._sources.append(source)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout=self._interval + 5)
        self.poll_once()                              # the last word, after the roles' final snapshots

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            self.poll_once()

    def poll_once(self) -> Telemetry | None:
        try:
            if self._handles is not None:
                observe_roles(self._tracker, self._handles())
            snapshot = self._tracker.snapshot()
            result = acquire(snapshot["roles"], self._root, self._tracker.execution_id, now=self._clock(),
                             interval=self._interval, entities=self._entities)
            for (kind, name), fields in result.types.items():
                self._tracker.update_type(kind, name, **fields)
            if result.totals:
                self._tracker.update_totals(**result.totals)
            for source in self._sources:
                try:
                    source(self._tracker, self._clock())
                except Exception as exc:              # noqa: BLE001
                    logger.debug("stage=status reason=telemetry source failed: %s", exc)
            self._tracker.set_telemetry(result.available, result.reason)
            self._tracker.heartbeat()
            return result
        except Exception as exc:                      # noqa: BLE001 - status never fails the load
            if not self._warned:
                self._warned = True
                logger.warning("stage=status reason=telemetry acquisition failed: %s", exc)
            return None
