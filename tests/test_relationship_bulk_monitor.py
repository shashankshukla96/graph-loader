"""Unit tests for one run-scoped relationship bulk stage monitor."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from confluent_kafka import TopicPartition
from docker.errors import NotFound

from src.orchestrator.bulk_monitor import AssignmentCoverageError, QuiescentViolationError
from src.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor
from src.utils.schema_loader import load_schema


ROOT = Path(__file__).resolve().parents[1]


def _message(payload):
    message = MagicMock(); message.error.return_value = None
    message.value.return_value = json.dumps(payload).encode()
    return message


def _monitor():
    schema = load_schema(ROOT / "config" / "graph_schema.yaml")
    edge = next(edge for edge in schema.edges if edge.type == "WORKS_AT")
    admin, watermark, control = MagicMock(), MagicMock(), MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={
        "works-at-events": SimpleNamespace(error=None, partitions={0: SimpleNamespace()}),
    })
    watermark.get_watermark_offsets.return_value = (0, 3)
    future = MagicMock(); future.result.return_value = SimpleNamespace(topic_partitions=[TopicPartition("works-at-events", 0, 3)])
    admin.list_consumer_group_offsets.return_value = {"graph-loader-WORKS_AT-run-1": future}
    control.poll.return_value = None
    container = MagicMock(); container.attrs = {"State": {"Status": "running"}}
    monitor = RelationshipBulkMonitor(
        [edge], "run-1", {("WORKS_AT", "0")}, {("WORKS_AT", "0"): container},
        bootstrap_servers="kafka:9092", consumer_group_prefix="graph-loader",
        admin_client=admin, watermark_consumer=watermark, control_consumer=control,
        poll_interval_seconds=0.01,
    )
    return monitor, admin, watermark, control, container


def _assignment(epoch=1, *, partitions=None, run_id="run-1", edge_type="WORKS_AT"):
    if partitions is None:
        partitions = [{"topic": "works-at-events", "partition": 0}]
    return {"run_id": run_id, "edge_type": edge_type, "replica_id": "0", "type": "ASSIGNMENT", "assignment_epoch": epoch, "assigned_partitions": partitions}


def test_stage_requires_current_run_and_edge_then_tracks_exact_group():
    monitor, admin, _, _, _ = _monitor()
    monitor._decode_control(_message(_assignment(run_id="old")))
    monitor._decode_control(_message(_assignment(edge_type="OTHER")))
    assert not monitor._coverage()
    monitor._decode_control(_message(_assignment()))
    assert monitor._coverage()
    monitor.capture_boundary()
    monitor.wait_for_completion(1)
    assert admin.list_consumer_group_offsets.call_args.args[0][0].group_id == "graph-loader-WORKS_AT-run-1"


def test_conflicting_same_epoch_and_future_drain_are_rejected_or_ignored():
    monitor, *_ = _monitor()
    monitor._decode_control(_message(_assignment()))
    idle = {**_assignment(), "type": "IDLE_SURPLUS"}
    idle.pop("assigned_partitions")
    with pytest.raises(AssignmentCoverageError, match="conflicting same-epoch"):
        monitor._decode_control(_message(idle))
    monitor._decode_control(_message({**_assignment(), "type": "DRAIN_COMPLETE", "assignment_epoch": 2}))
    assert monitor._drains == {}


def test_late_input_after_boundary_is_rejected():
    monitor, _, watermark, _, _ = _monitor()
    monitor._decode_control(_message(_assignment()))
    monitor.capture_boundary()
    watermark.get_watermark_offsets.return_value = (0, 4)
    with pytest.raises(QuiescentViolationError):
        monitor._assert_quiescent()


def test_stage_monitor_uses_one_commit_snapshot_and_clipped_deadline():
    monitor, admin, _, _, _ = _monitor()
    monitor._decode_control(_message(_assignment()))
    monitor.capture_boundary()
    monitor._operation_deadline = monitor._clock() + 0.01
    assert monitor._request_timeout() <= 0.01
    assert monitor._at_boundary()
    assert admin.list_consumer_group_offsets.call_count == 1


def test_control_poll_timeout_is_clipped_to_stage_deadline():
    monitor, _, _, control, _ = _monitor()
    monitor._operation_deadline = monitor._clock() + 0.001
    monitor._poll()
    assert control.poll.call_args.args[0] <= 0.001


def test_exited_and_removed_edge_container_require_current_drain_ack():
    monitor, _, _, _, container = _monitor()
    monitor._decode_control(_message(_assignment()))
    container.attrs = {"State": {"Status": "exited"}}
    with pytest.raises(Exception, match="exited before drain"):
        monitor._check_health()
    monitor._decode_control(_message({**_assignment(), "type": "DRAIN_COMPLETE"}))
    monitor._check_health(True)
    container.reload.side_effect = NotFound("removed")
    monitor._check_health(True)


def test_missing_committed_offset_fails_zero_lag():
    monitor, admin, _, _, _ = _monitor()
    monitor._decode_control(_message(_assignment()))
    monitor.capture_boundary()
    future = MagicMock(); future.result.return_value = SimpleNamespace(topic_partitions=[])
    admin.list_consumer_group_offsets.return_value = {"graph-loader-WORKS_AT-run-1": future}
    with pytest.raises(Exception, match="nonzero lag"):
        monitor.verify_zero_lag()
