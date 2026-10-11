"""D6: stopping a run aborts a still-scanning planner promptly and cleans up after it."""
from __future__ import annotations

from unittest.mock import MagicMock

from src.loader.edge_plan import plan_failure_marker, plan_progress_path
from src.loader.edge_planner import KafkaEdgePlanner, _ProcessWorker


def test_process_worker_close_with_terminate_does_not_wait_for_the_scan() -> None:
    worker = object.__new__(_ProcessWorker)
    worker.process = MagicMock()
    worker.process.is_alive.side_effect = [True, False]
    worker.connection = MagicMock()
    worker._stop_sent = False
    worker.close(terminate=True)
    worker.process.terminate.assert_called_once()
    # The first join is the short post-terminate one, not the 30 s graceful one.
    assert worker.process.join.call_args_list[0].kwargs == {"timeout": 5}


def test_kafka_planner_close_aborts_an_unfinished_plan_and_removes_its_files(tmp_path) -> None:
    planner = object.__new__(KafkaEdgePlanner)
    plan_path = tmp_path / "edge-plan.json"
    plan_failure_marker(plan_path).write_text("x")
    plan_progress_path(plan_path).write_text("y")
    planner.plan_path, planner.result, planner.topic_created = plan_path, None, False
    planner.planner = MagicMock()
    planner._thread = MagicMock()
    planner._thread.is_alive.return_value = True
    planner.close(delete_topic=False)
    planner.planner.close.assert_called_once_with(terminate=True)
    assert not plan_failure_marker(plan_path).exists()
    assert not plan_progress_path(plan_path).exists()


def test_kafka_planner_close_after_a_finished_plan_is_graceful() -> None:
    planner = object.__new__(KafkaEdgePlanner)
    planner.plan_path, planner.result, planner.topic_created = None, object(), False
    planner.planner = MagicMock()
    planner._thread = None
    planner.close(delete_topic=False)
    planner.planner.close.assert_called_once_with(terminate=False)
