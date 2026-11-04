"""Metric identity (E15.F2.S8): the labels every exported sample carries, and the one name of an exporter.

In a tracked run (one started by ``graph-loader start``, which gives every role ``GRAPH_LOADER_EXECUTION_ID`` and
``GRAPH_LOADER_INCARNATION``) every sample a process exposes (HTTP endpoint and snapshots alike) carries:

``run_id``       the logical run; the empty string when the run has none (a stream started without ``--run-id``), which
                 Prometheus treats as an absent label. Reusable: dashboards and rules never key on it alone.
``execution_id`` one ``graph-loader start``; with ``exporter`` it is what separates two runs that share a ``run_id``.
``exporter``     unique per role INCARNATION: ``<kind>-<name>-r<replica>-<incarnation[:8]>`` (``cli-<execution[:8]>`` for the CLI).
``runtime``      ``docker`` or ``process``.

These four names are reserved: ``--extra-labels`` of ``monitoring export`` rejects them. A sample's own labels always win,
so a family that already has an ``exporter`` label (``graph_loader_exporter_up``) keeps it. Constant labels add no series
within one exporter; what grows is the number of exporters over the retention window (see ``docs`` and the S8 evidence).
Without the identity environment (an untracked process, a test) nothing is added.
"""
from __future__ import annotations

import os
from typing import Iterable

RESERVED_LABELS = ("run_id", "execution_id", "exporter", "runtime")
EXECUTION_ENV, INCARNATION_ENV, RUN_ID_ENV = "GRAPH_LOADER_EXECUTION_ID", "GRAPH_LOADER_INCARNATION", "GRAPH_LOADER_RUN_ID"
RUNTIME_ENV = "GRAPH_LOADER_RUNTIME"                                # the process runtime sets "process" for every child; absent means docker

# launcher component label -> the process ``kind`` its metrics carry (utils.metrics.PROCESS_KINDS)
COMPONENT_KIND = {
    "node-loader": "node", "edge-loader": "edge", "planned-edge-writer": "planned-writer", "planned-dispatcher": "planned-dispatcher",
    "bucket-lease-server": "lease-server", "global-batch-clock": "clock", "retry-consumer": "retry-consumer", "cli": "cli",
}


class ReservedLabelError(ValueError):
    """An extra label uses a name this version reserves for run identity."""


def validate_extra_labels(labels: dict[str, str] | Iterable[str]) -> None:
    """Raise :class:`ReservedLabelError` when any of ``labels`` (names) is reserved."""
    clashes = sorted(set(labels) & set(RESERVED_LABELS))
    if clashes:
        raise ReservedLabelError(f"label name(s) {', '.join(clashes)} are reserved for run identity ({', '.join(RESERVED_LABELS)})")


def exporter_name(kind: str, name: str | None, replica: int | str | None, incarnation: str | None, *, execution_id: str | None = None) -> str:
    """The unique exporter of one process incarnation."""
    if kind == "cli":
        return f"cli-{(execution_id or incarnation or '')[:8]}".rstrip("-")
    parts = [kind]
    if name and name != "-":
        parts.append(str(name))
    if replica is not None:
        parts.append(f"r{replica}")
    parts.append((incarnation or "")[:8])
    return "-".join(part for part in parts if part)


def component_exporter(labels: dict, incarnation: str) -> str:
    """The exporter of a launched role, from its launch labels (the CLI side of :func:`exporter_name`)."""
    kind = COMPONENT_KIND.get(str(labels.get("component")), str(labels.get("component")))
    name = labels.get("node_label") or labels.get("edge_type") or "-"
    replica = labels.get("replica_id")
    if replica is None and str(labels.get("writer_id", "")).startswith("w"):
        replica = str(labels["writer_id"])[1:]
    return exporter_name(kind, name, int(replica) if replica is not None and str(replica).isdigit() else None, incarnation)


def identity_labels(kind: str | None, name: str | None, replica: int | str | None = None, *, context: dict | None = None) -> dict[str, str]:
    """The identity of this process, or ``{}`` outside a tracked run. ``context`` (the logging context) wins over the environment."""
    context = context or {}
    execution_id = context.get("execution_id") or os.environ.get(EXECUTION_ENV)
    if not execution_id or not kind:
        return {}
    incarnation = context.get("incarnation") or os.environ.get(INCARNATION_ENV)
    runtime = context.get("runtime") or ("process" if os.environ.get(RUNTIME_ENV) == "process" else "docker")
    return {
        "run_id": context.get("run_id") or os.environ.get(RUN_ID_ENV) or "", "execution_id": execution_id,
        "exporter": exporter_name(kind, name, replica if replica is not None else context.get("replica"), incarnation, execution_id=execution_id),
        "runtime": runtime,
    }
