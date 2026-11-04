"""Vocabulary of the run status model (E15.F1.S1): identifiers, states, allowed transitions, timestamps.

Everything here is pure data and stdlib; nothing touches the filesystem or the network. The JSON Schemas in
:mod:`neo4j_graph_loader.status.schemas` are written from the same vocabulary.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone

SCHEMA_VERSION = 1

# Overall execution state.
STARTING, RUNNING, STOPPING = "starting", "running", "stopping"
SUCCEEDED, FAILED, STOPPED = "succeeded", "failed", "stopped"
TERMINAL_STATES = (SUCCEEDED, FAILED, STOPPED)
EXECUTION_STATES = (STARTING, RUNNING, STOPPING, *TERMINAL_STATES)

# state -> states it may move to; terminal states are final.
TRANSITIONS: dict[str, tuple[str, ...]] = {
    STARTING: (RUNNING, STOPPING, *TERMINAL_STATES),
    RUNNING: (STOPPING, *TERMINAL_STATES),
    STOPPING: TERMINAL_STATES,
    SUCCEEDED: (), FAILED: (), STOPPED: (),
}

# Overlapping activities (no serial phase sequence): which exist depends on the mode.
ACTIVITIES = ("planning", "nodes", "edges", "draining", "stream")
ACTIVITY_PENDING = "pending"
ACTIVITY_STATES = (ACTIVITY_PENDING, RUNNING, SUCCEEDED, FAILED, STOPPED)

PHASE_STATES = (RUNNING, SUCCEEDED, FAILED, STOPPED)
ROLE_STATES = (STARTING, RUNNING, STOPPING, "exited", FAILED)
MODES = ("bulk", "stream")
RUNTIMES = ("docker", "process")

# Telemetry fields carried per type (node label / relationship type) and for the run as a whole. A field that is
# absent is "not applicable in this mode"; ``null`` is "not known"; a value is {value, observed_at, source}.
PROGRESS_NUMBER_FIELDS = (
    "records_written", "retry_written", "retry_redlq", "retry_parked", "lag", "plan_chunks_done", "plan_chunks_total",
    "rejected", "dlq_lag", "parked_size",
)
PROGRESS_BOOLEAN_FIELDS = ("ready",)
PROGRESS_FIELDS = PROGRESS_NUMBER_FIELDS + PROGRESS_BOOLEAN_FIELDS
TYPE_KINDS = ("node", "edge")

ID_PATTERN = r"^[0-9a-f]{32}$"
RUN_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"          # the CLI's --run-id charset (cli._RUN_ID)
TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

_ID = re.compile(ID_PATTERN)


def new_id() -> str:
    """A fresh identifier: a UUID4 as 32 lowercase hex digits (the form E14 already uses for incarnations)."""
    return uuid.uuid4().hex


def is_id(value: object) -> bool:
    return isinstance(value, str) and _ID.fullmatch(value) is not None


def timestamp(epoch_seconds: float) -> str:
    """RFC 3339 UTC with millisecond precision, e.g. ``2026-11-03T12:00:00.123Z``."""
    moment = datetime.fromtimestamp(epoch_seconds, timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def observed(value, observed_at: str, source: str, *, lower_bound: bool | None = None) -> dict:
    """One telemetry value with its observation time and source. ``lower_bound`` marks a total known to be low."""
    result = {"value": value, "observed_at": observed_at, "source": source}
    if lower_bound is not None:
        result["lower_bound"] = lower_bound
    return result
