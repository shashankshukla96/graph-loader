"""Static types of the status documents the accessor returns (the JSON Schemas are the contract; these help callers)."""
from __future__ import annotations

from typing import Any, Literal, TypedDict


class Observed(TypedDict, total=False):
    value: int | bool
    observed_at: str
    source: str
    lower_bound: bool


class Role(TypedDict):
    role: str
    name: str
    replica: int | None
    incarnation: str
    restart_count: int
    state: str
    container_id: str | None
    pid: int | None
    started_at: str
    finished_at: str | None
    exit_code: int | None
    exit_reason: str | None
    last_error: dict[str, str] | None


class Phase(TypedDict):
    phase_id: str
    kind: str
    index: int | None
    edge_type: str | None
    state: str
    started_at: str
    finished_at: str | None


class StatusDocument(TypedDict, total=False):
    """A run status snapshot (``status.schema.json``). Members a newer additive version adds appear as well."""
    schema_version: int
    kind: Literal["status"]
    execution_id: str
    run_id: str | None
    revision: int
    state: Literal["starting", "running", "stopping", "succeeded", "failed", "stopped"]
    outcome: dict[str, Any] | None
    mode: Literal["bulk", "stream"] | None
    runtime: Literal["docker", "process"]
    host: str
    pid: int
    versions: dict[str, str]
    started_at: str
    updated_at: str
    heartbeat_at: str
    finished_at: str | None
    activities: dict[str, dict[str, Any]]
    phases: list[Phase]
    roles: list[Role]
    labels: dict[str, str]
    types: list[dict[str, Any]]
    totals: dict[str, Observed | None]
    telemetry: dict[str, Any]
    history_incomplete: bool


class StatusEvent(TypedDict, total=False):
    """One event (``event.schema.json``); the members beyond the first five depend on ``type``."""
    schema_version: int
    execution_id: str
    seq: int
    ts: str
    type: str
