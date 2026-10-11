# ruff: noqa: F811  (imported pytest fixture `edge` is requested by name)
"""Bulk monitors (relationship and node): a partition that was empty at the snapshot (low == high) never
commits and must not block; every other case still needs its commit, and late records fail closed."""
from __future__ import annotations

from pathlib import Path

import pytest

from src.orchestrator.bulk_monitor import OffsetResolutionError, QuiescentViolationError
from src.utils.schema_loader import load_schema
from test_bulk_monitor import _cover_all_partitions, _monitor as _node_monitor
from test_multi_topic_stream_bulk import MAPPING, T1, T2, _ack, _monitor as _edge_monitor, edge  # noqa: F401

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PERSON_1 = ("person-events", 1)


# --- relationship monitor ------------------------------------------------------------------


def _edge_ready(edge, marks, committed):
    monitor, state, _ = _edge_monitor(edge, mapping=MAPPING, watermarks=marks, committed=committed)
    _ack(monitor, [(t, p) for t in (T1, T2) for p in range(3)])
    monitor.capture_boundary()
    return monitor, state


@pytest.mark.parametrize("empty", [False, True])
def test_edge_verify_zero_lag_raises_when_commit_is_above_the_boundary(edge, empty) -> None:
    marks = {(T1, p): (0, 10) for p in range(3)}
    marks.update({(T2, p): (50, 50) for p in range(3)})
    committed = {(T1, p): 10 for p in range(3)}
    committed.update({(T2, p): 50 for p in range(3)})
    monitor, state = _edge_ready(edge, marks, committed)
    monitor.verify_zero_lag()
    state["committed"][(T2 if empty else T1, 0)] = 99
    with pytest.raises(OffsetResolutionError, match="nonzero lag") as info:
        monitor.verify_zero_lag()
    assert ("reused run id" in str(info.value)) is empty


def test_edge_partition_trimmed_after_the_snapshot_does_not_reach_the_boundary(edge) -> None:
    marks = {(t, p): (0, 10) for t in (T1, T2) for p in range(3)}
    committed = {(T1, p): 10 for p in range(3)}
    monitor, _ = _edge_ready(edge, marks, committed)
    marks[(T2, 0)] = (10, 10)  # retention ate the data after the (0, 10) snapshot
    assert monitor._at_boundary() is False


# --- node monitor ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def schema():
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


def _node_ready(schema, person1_marks, person1_commit):
    watermarks = {("person-events", 0): (0, 3), PERSON_1: person1_marks, ("company-events", 0): (0, 4)}
    person = {("person-events", 0): 3}
    if person1_commit is not None:
        person[PERSON_1] = person1_commit
    offsets = {"graph-loader-Person": person, "graph-loader-Company": {("company-events", 0): 4}}
    monitor, *_ = _node_monitor(schema, watermarks=watermarks, offsets=offsets)
    _cover_all_partitions(monitor)
    monitor.capture_boundary()
    return monitor, watermarks, offsets


def test_node_retention_emptied_partition_completes_and_verifies(schema) -> None:
    monitor, *_ = _node_ready(schema, (50, 50), None)
    monitor.wait_for_completion(1)
    assert monitor.wait_for_next_label_completion({"Person"}, 1) == "Person"
    monitor.verify_label_zero_lag("Person")
    monitor.verify_zero_lag()
    assert monitor._label_at_boundary("Person", monitor._committed_offsets(), exact=True)


def test_node_late_record_in_an_empty_partition_fails_closed(schema) -> None:
    monitor, watermarks, _ = _node_ready(schema, (50, 50), None)
    watermarks[PERSON_1] = (50, 51)
    with pytest.raises(QuiescentViolationError):
        monitor.wait_for_completion(1)
    with pytest.raises(QuiescentViolationError):
        monitor.verify_label_zero_lag("Person")


def test_node_partition_trimmed_after_the_snapshot_still_needs_its_commit(schema) -> None:
    monitor, watermarks, _ = _node_ready(schema, (0, 2), None)
    watermarks[PERSON_1] = (2, 2)
    assert monitor._at_boundary() is False


@pytest.mark.parametrize("empty", [False, True])
def test_node_commit_above_the_boundary_fails_zero_lag(schema, empty) -> None:
    monitor, _, offsets = _node_ready(schema, (50, 50) if empty else (0, 2), 50 if empty else 2)
    monitor.verify_label_zero_lag("Person")
    monitor.verify_zero_lag()
    offsets["graph-loader-Person"][PERSON_1] = 99
    with pytest.raises(OffsetResolutionError, match="nonzero lag") as info:
        monitor.verify_label_zero_lag("Person")
    assert ("reused run id" in str(info.value)) is empty
    with pytest.raises(OffsetResolutionError, match="nonzero lag") as info:
        monitor.verify_zero_lag()
    assert ("reused run id" in str(info.value)) is empty
