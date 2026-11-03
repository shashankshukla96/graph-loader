"""JSON Schemas (draft 2020-12) of the run status snapshot and the event record: the public, versioned contract.

Within ``schema_version`` 1 changes are additive only (new optional members, new event types), so the PUBLISHED
schemas (``STATUS_SCHEMA``, ``EVENT_SCHEMA``, the files under ``docs/status-schemas/``) are open: a v1 reader accepts
a later additive v1 document, including an event of a type it does not know (any well-formed envelope whose
``type`` is not a known one). The ``STRICT_*`` variants are closed and exist for the tests of this version's own
writer; they are not published. The Python dictionaries are the source; the files are generated copies
(``python -m neo4j_graph_loader.status.schemas <directory>``) and a test keeps them equal.
"""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

from neo4j_graph_loader.status import model

STATUS_SCHEMA_ID = "urn:neo4j-graph-loader:schema:status:1"
EVENT_SCHEMA_ID = "urn:neo4j-graph-loader:schema:event:1"
_DRAFT = "https://json-schema.org/draft/2020-12/schema"


def _ref(name: str) -> dict:
    return {"$ref": f"#/$defs/{name}"}


def _nullable(schema: dict) -> dict:
    return {"anyOf": [schema, {"type": "null"}]}


def _object(properties: dict, required: tuple[str, ...] | None = None) -> dict:
    """A closed object whose members are all required unless ``required`` narrows them."""
    return {
        "type": "object", "properties": properties,
        "required": list(properties if required is None else required), "additionalProperties": False,
    }


_STRING = {"type": "string", "minLength": 1}
_NULLABLE_STRING = _nullable(_STRING)
_COUNT = {"type": "integer", "minimum": 0}

_DEFS = {
    "id": {"type": "string", "pattern": model.ID_PATTERN},
    "run_id": {"type": "string", "pattern": model.RUN_ID_PATTERN},
    "timestamp": {"type": "string", "pattern": model.TIMESTAMP_PATTERN},
    "error_info": _object({"type": _STRING, "message": {"type": "string"}}),
    "observed_number": _object(
        {"value": _COUNT, "observed_at": _ref("timestamp"), "source": _STRING, "lower_bound": {"type": "boolean"}},
        ("value", "observed_at", "source"),
    ),
    "observed_boolean": _object(
        {"value": {"type": "boolean"}, "observed_at": _ref("timestamp"), "source": _STRING},
    ),
    "progress": _object(
        {**{field: _nullable(_ref("observed_number")) for field in model.PROGRESS_NUMBER_FIELDS},
         **{field: _nullable(_ref("observed_boolean")) for field in model.PROGRESS_BOOLEAN_FIELDS}},
        required=(),
    ),
    "activity": _object({
        "state": {"enum": list(model.ACTIVITY_STATES)},
        "started_at": _nullable(_ref("timestamp")),
        "finished_at": _nullable(_ref("timestamp")),
        "updated_at": _ref("timestamp"),
    }),
    "phase": _object({
        "phase_id": _ref("run_id"), "kind": _STRING, "index": _nullable(_COUNT), "edge_type": _NULLABLE_STRING,
        "state": {"enum": list(model.PHASE_STATES)},
        "started_at": _ref("timestamp"), "finished_at": _nullable(_ref("timestamp")),
    }),
    "role": _object({
        "role": _STRING, "name": _STRING, "replica": _nullable(_COUNT), "incarnation": _ref("id"),
        "restart_count": _COUNT, "state": {"enum": list(model.ROLE_STATES)},
        "container_id": _NULLABLE_STRING, "pid": _nullable(_COUNT),
        "started_at": _ref("timestamp"), "finished_at": _nullable(_ref("timestamp")),
        "exit_code": _nullable({"type": "integer"}), "exit_reason": _NULLABLE_STRING,
        "last_error": _nullable(_ref("error_info")),
    }),
}
# A type entry is the identity members plus the progress members; one closed object keeps validation simple.
_DEFS["type_progress"] = _object(
    {"kind": {"enum": list(model.TYPE_KINDS)}, "name": _STRING, **_DEFS["progress"]["properties"]},
    required=("kind", "name"),
)

STATUS_SCHEMA = {
    "$schema": _DRAFT,
    "$id": STATUS_SCHEMA_ID,
    "title": "Graph Loader run status snapshot",
    "description": (
        "State of one execution (one `graph-loader start`). Numeric telemetry is `{value, observed_at, source}` "
        "or null (unknown); a member that is absent is not applicable in this mode."
    ),
    **_object({
        "schema_version": {"const": model.SCHEMA_VERSION},
        "kind": {"const": "status"},
        "execution_id": _ref("id"),
        "run_id": _nullable(_ref("run_id")),
        "revision": {"type": "integer", "minimum": 1},
        "state": {"enum": list(model.EXECUTION_STATES)},
        "outcome": _nullable(_object(
            {"exit_code": {"type": "integer"}, "reason": _NULLABLE_STRING, "error": _nullable(_ref("error_info"))},
        )),
        "mode": _nullable({"enum": list(model.MODES)}),
        "runtime": {"enum": list(model.RUNTIMES)},
        "host": _STRING,
        "pid": {"type": "integer", "minimum": 1},
        "versions": _object({"graph_loader": _STRING, "python": _STRING}),
        "started_at": _ref("timestamp"),
        "updated_at": _ref("timestamp"),
        "heartbeat_at": _ref("timestamp"),
        "finished_at": _nullable(_ref("timestamp")),
        "activities": _object({name: _ref("activity") for name in model.ACTIVITIES}, required=()),
        "phases": {"type": "array", "items": _ref("phase")},
        "roles": {"type": "array", "items": _ref("role")},
        "labels": {"type": "object", "additionalProperties": {"type": "string"}},
        "types": {"type": "array", "items": _ref("type_progress")},
        "totals": _ref("progress"),
        "telemetry": _object({"available": {"type": "boolean"}, "reason": _NULLABLE_STRING}),
        "history_incomplete": {"type": "boolean"},
    }),
    "$defs": {name: _DEFS[name] for name in (
        "id", "run_id", "timestamp", "error_info", "observed_number", "observed_boolean", "progress", "activity",
        "phase", "role", "type_progress",
    )},
}


def _event(type_name: str, fields: dict) -> dict:
    members = {
        "schema_version": {"const": model.SCHEMA_VERSION},
        "execution_id": _ref("id"),
        "seq": {"type": "integer", "minimum": 1},
        "ts": _ref("timestamp"),
        "type": {"const": type_name},
        **fields,
    }
    return _object(members)


_TERMINAL = {"enum": list(model.TERMINAL_STATES)}
_ROLE_KEY = {"role": _STRING, "name": _STRING, "replica": _nullable(_COUNT), "incarnation": _ref("id")}

EVENT_TYPES = {
    "execution.started": _event("execution.started", {
        "run_id": _nullable(_ref("run_id")), "mode": _nullable({"enum": list(model.MODES)}),
        "runtime": {"enum": list(model.RUNTIMES)}, "host": _STRING, "pid": {"type": "integer", "minimum": 1},
        "versions": _object({"graph_loader": _STRING, "python": _STRING}),
    }),
    "execution.run_id": _event("execution.run_id", {"run_id": _ref("run_id")}),
    "execution.state": _event("execution.state", {
        "from": {"enum": list(model.EXECUTION_STATES)}, "to": {"enum": list(model.EXECUTION_STATES)},
        "reason": _NULLABLE_STRING,
    }),
    "execution.finished": _event("execution.finished", {
        "state": _TERMINAL, "exit_code": {"type": "integer"}, "reason": _NULLABLE_STRING,
        "error": _nullable(_ref("error_info")),
    }),
    "activity.state": _event("activity.state", {
        "activity": {"enum": list(model.ACTIVITIES)}, "from": {"enum": list(model.ACTIVITY_STATES)},
        "to": {"enum": list(model.ACTIVITY_STATES)},
    }),
    "phase.started": _event("phase.started", {
        "phase_id": _ref("run_id"), "kind": _STRING, "index": _nullable(_COUNT), "edge_type": _NULLABLE_STRING,
    }),
    "phase.finished": _event("phase.finished", {"phase_id": _ref("run_id"), "state": {"enum": ["succeeded", "failed", "stopped"]}}),
    "role.started": _event("role.started", {
        **_ROLE_KEY, "restart_count": _COUNT, "container_id": _NULLABLE_STRING, "pid": _nullable(_COUNT),
    }),
    "role.exited": _event("role.exited", {
        **_ROLE_KEY, "exit_code": _nullable({"type": "integer"}), "exit_reason": _NULLABLE_STRING,
        "error": _nullable(_ref("error_info")),
    }),
    "role.oom": _event("role.oom", dict(_ROLE_KEY)),
    "execution.oom": _event("execution.oom", {"oom_kill_before": _COUNT, "oom_kill_after": _COUNT}),
    "error": _event("error", {"stage": _STRING, "message": {"type": "string"}}),
    "gap": _event("gap", {
        "from_seq": {"type": "integer", "minimum": 1}, "to_seq": {"type": "integer", "minimum": 1},
        "count": {"type": "integer", "minimum": 1}, "reason": _STRING,
    }),
}

EVENT_SCHEMA = {
    "$schema": _DRAFT,
    "$id": EVENT_SCHEMA_ID,
    "title": "Graph Loader run event",
    "description": (
        "One entry of an execution's event history. `seq` is gap-free per execution, starting at 1; a `gap` event "
        "stands in for the missing range `from_seq`..`to_seq` and carries `seq == from_seq`."
    ),
    "oneOf": [_ref(name) for name in EVENT_TYPES],
    "$defs": {
        "id": _DEFS["id"], "run_id": _DEFS["run_id"], "timestamp": _DEFS["timestamp"], "error_info": _DEFS["error_info"],
        **EVENT_TYPES,
    },
}


STRICT_STATUS_SCHEMA = STATUS_SCHEMA
STRICT_EVENT_SCHEMA = EVENT_SCHEMA


def _open(node):
    """A copy of a schema with every `additionalProperties: false` removed."""
    if isinstance(node, dict):
        return {key: _open(value) for key, value in node.items() if not (key == "additionalProperties" and value is False)}
    if isinstance(node, list):
        return [_open(item) for item in node]
    return node


def _published_event_schema() -> dict:
    schema = _open(STRICT_EVENT_SCHEMA)
    schema["$defs"]["unknown_event"] = {
        "type": "object",
        "properties": {
            "schema_version": {"const": model.SCHEMA_VERSION}, "execution_id": _ref("id"),
            "seq": {"type": "integer", "minimum": 1}, "ts": _ref("timestamp"),
            "type": {"type": "string", "pattern": "^[a-z][a-z0-9_.]*$", "not": {"enum": list(EVENT_TYPES)}},
        },
        "required": ["schema_version", "execution_id", "seq", "ts", "type"],
    }
    schema["oneOf"].append(_ref("unknown_event"))
    return schema


STATUS_SCHEMA = _open(STRICT_STATUS_SCHEMA)
EVENT_SCHEMA = _published_event_schema()


def schemas() -> dict[str, dict]:
    """File name -> published (open) schema, for the generated copies."""
    return {"status.schema.json": copy.deepcopy(STATUS_SCHEMA), "event.schema.json": copy.deepcopy(EVENT_SCHEMA)}


def render(schema: dict) -> str:
    return json.dumps(schema, indent=2) + "\n"


def write(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name, schema in schemas().items():
        (directory / name).write_text(render(schema), encoding="utf-8")


if __name__ == "__main__":
    write(Path(sys.argv[1]))
