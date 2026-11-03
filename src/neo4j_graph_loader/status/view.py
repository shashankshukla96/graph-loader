"""What ``graph-loader status`` tells a person or a script about a run (E15.F1.S6).

The status document says what the CLI last wrote. This module adds the reader's judgement and nothing else:

* **stale**: a run that is still ``starting``/``running``/``stopping`` but whose ``heartbeat_at`` is older than
  ``stale_after`` seconds (default 30 = six missed beats of the 5 s status interval; the CLI refreshes the heartbeat from
  its own telemetry thread, so a long planning or schema phase does not look stale);
* **dead**: stale AND the controlling CLI process is proven gone on this host: E14's run registry has a ``cli`` record
  whose pid no longer exists or now has a different start time / boot id; without a registry (Docker runtime) the status
  document's own ``host`` and ``pid`` are used, and only when the host is this host and that pid does not exist (a live
  pid proves nothing, pids are reused). Never claimed for a host that cannot be checked.

Exit codes: ``0`` running (fresh) or succeeded, ``1`` failed, ``2`` stale, dead or unknown (nothing found, unreadable),
``3`` stopped.
"""
from __future__ import annotations

import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from neo4j_graph_loader.status import model

DEFAULT_STALE_AFTER_SECONDS = 30.0
STALE_AFTER_ENV = "GRAPH_LOADER_STATUS_STALE_AFTER_S"
ACTIVE_STATES = (model.STARTING, model.RUNNING, model.STOPPING)

EXIT_OK, EXIT_FAILED, EXIT_STALE, EXIT_STOPPED = 0, 1, 2, 3


def stale_after_seconds(explicit: float | None = None) -> float:
    if explicit is not None and explicit > 0:
        return explicit
    try:
        value = float(os.environ.get(STALE_AFTER_ENV, ""))
    except ValueError:
        return DEFAULT_STALE_AFTER_SECONDS
    return value if value > 0 else DEFAULT_STALE_AFTER_SECONDS


def _epoch(stamp: str) -> float:
    return datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp()


def cli_is_gone(attempt: Path | str | None, status: dict) -> bool | None:
    """``True`` when the controlling CLI is proven gone on this host, ``False`` when it is proven there (same pid and
    start identity), ``None`` when this host cannot tell."""
    if attempt is not None:
        registry = Path(attempt) / "registry.jsonl"
        if registry.exists():
            from neo4j_graph_loader.orchestrator import run_registry
            try:
                record = run_registry.cli_record(run_registry.RunRegistry.read(registry))
            except run_registry.RegistryError:
                record = None
            if record is not None and "pid" in record:
                current = run_registry.process_identity(int(record["pid"]))
                if current is None:
                    return True
                same = (current.get("start_time"), current.get("boot_id")) == (record.get("start_time"), record.get("boot_id"))
                return not same
    if status.get("host") == socket.gethostname() and isinstance(status.get("pid"), int):
        try:
            os.kill(status["pid"], 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return None
        return None                                                      # alive, but a recycled pid cannot be excluded
    return None


def assess(status: dict, *, now: float | None = None, stale_after: float | None = None, attempt: Path | str | None = None) -> dict:
    """The reader's judgement: ``{stale, dead, heartbeat_age_seconds, stale_after_seconds, exit_code, label}``."""
    now = time.time() if now is None else now
    threshold = stale_after_seconds(stale_after)
    state = status["state"]
    age = max(0.0, now - _epoch(status["heartbeat_at"]))
    stale = state in ACTIVE_STATES and age > threshold
    dead = None
    if stale:
        dead = bool(cli_is_gone(attempt, status))
    if state == model.SUCCEEDED:
        code, label = EXIT_OK, "succeeded"
    elif state == model.FAILED:
        code, label = EXIT_FAILED, "failed"
    elif state == model.STOPPED:
        code, label = EXIT_STOPPED, "stopped"
    elif stale:
        code, label = EXIT_STALE, "dead" if dead else "stale"
    else:
        code, label = EXIT_OK, state
    return {"stale": stale, "dead": dead, "heartbeat_age_seconds": round(age, 3), "stale_after_seconds": threshold,
            "exit_code": code, "label": label}


def with_reader(status: dict, assessment: dict, extra: dict | None = None) -> dict:
    """``status`` plus a ``reader`` member (open schema: the document still validates)."""
    return {**status, "reader": {**{key: assessment[key] for key in ("stale", "dead", "heartbeat_age_seconds", "stale_after_seconds",
                                                                      "exit_code", "label")}, **(extra or {})}}


# -- text ----------------------------------------------------------------------------------------------------------------------------
def _value(field) -> str:
    if field is None:
        return "?"
    value = field["value"]
    text = ("yes" if value else "no") if isinstance(value, bool) else f"{value:,}"
    return text + ("+" if field.get("lower_bound") else "")


def _progress(entry: dict) -> str:
    names = [("records_written", "written"), ("rejected", "rejected"), ("lag", "lag"), ("ready", "ready"),
             ("retry_written", "retry-written"), ("retry_redlq", "retry-redlq"), ("retry_parked", "retry-parked"),
             ("dlq_lag", "dlq-lag"), ("parked_size", "parked")]
    parts = [f"{label} {_value(entry[key])}" for key, label in names if key in entry]
    if "plan_chunks_done" in entry or "plan_chunks_total" in entry:
        parts.append(f"chunks {_value(entry.get('plan_chunks_done'))}/{_value(entry.get('plan_chunks_total'))}")
    return ", ".join(parts) or "-"


def render_text(status: dict, assessment: dict, *, extra_lines: list[str] | None = None) -> str:
    lines = [f"execution {status['execution_id']}  run {status['run_id'] or '-'}  {status['mode'] or '-'}/{status['runtime']}",
             f"state     {assessment['label']}" + (f"  (last heartbeat {assessment['heartbeat_age_seconds']:.0f}s ago, stale after "
                                                    f"{assessment['stale_after_seconds']:.0f}s{'; the controlling CLI process is gone' if assessment['dead'] else ''})"
                                                    if assessment["stale"] else ""),
             f"started   {status['started_at']}   updated {status['updated_at']}" + (f"   finished {status['finished_at']}" if status["finished_at"] else "")]
    outcome = status["outcome"]
    if outcome:
        lines.append(f"outcome   exit code {outcome['exit_code']}" + (f", {outcome['reason']}" if outcome["reason"] else "")
                     + (f", {outcome['error']['type']}: {outcome['error']['message']}" if outcome["error"] else ""))
    if status["history_incomplete"]:
        lines.append("history   incomplete (some events may be missing from the store)")
    if not status["telemetry"]["available"]:
        lines.append(f"telemetry unavailable: {status['telemetry']['reason']}")
    if status["activities"]:
        lines.append("activities " + ", ".join(f"{name} {item['state']}" for name, item in status["activities"].items()))
    if status["phases"]:
        lines.append(f"phases    {len(status['phases'])}: " + ", ".join(f"{p['kind']}{'/' + p['edge_type'] if p['edge_type'] else ''} {p['state']}" for p in status["phases"][:8])
                     + (" ..." if len(status["phases"]) > 8 else ""))
    if status["roles"]:
        counts: dict[str, int] = {}
        for role in status["roles"]:
            counts[role["state"]] = counts.get(role["state"], 0) + 1
        lines.append(f"roles     {len(status['roles'])}: " + ", ".join(f"{n} {state}" for state, n in sorted(counts.items())))
        for role in status["roles"]:
            if role["state"] in ("failed",) or role["restart_count"]:
                lines.append(f"          {role['role']} {role['name']}#{role['replica']} restarts {role['restart_count']} "
                             f"{role['state']}{' exit ' + str(role['exit_code']) if role['exit_code'] is not None else ''}"
                             f"{' (' + role['exit_reason'] + ')' if role['exit_reason'] else ''}")
    for entry in status["types"]:
        lines.append(f"  {entry['kind']:4} {entry['name']:<24} {_progress(entry)}")
    if status["totals"]:
        lines.append(f"  total {'':<24} {_progress(status['totals'])}")
    lines += extra_lines or []
    return "\n".join(lines) + "\n"


def list_line(row: dict) -> str:
    return (f"{row['execution_id']}  {row['label']:<9} {row['run_id'] or '-':<24} {row['mode'] or '-':<6} {row['runtime']:<7} "
            f"started {row['started_at']}  heartbeat {row['heartbeat_age_seconds']:.0f}s ago")
