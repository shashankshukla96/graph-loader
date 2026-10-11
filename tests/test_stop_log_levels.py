"""Operator-requested stops log WARNINGs, real failures stay ERRORs (N2, N3, revoke-on-close quirk).

Return codes are unchanged everywhere except N3 (a retry consumer stopped while starting exits 0).
"""
from __future__ import annotations

import argparse
import logging
import signal
from pathlib import Path
from threading import Event
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import TopicPartition

from src import cli
from src.loader.edge_loader import EdgeLoader
from src.loader.edge_planner import KafkaEdgePlanner
from src.loader.lease_client import LeaseCoordinatorError
from src.loader.retry_consumer import main as retry_consumer_main
from src.orchestrator.bulk_monitor import BulkShutdownRequested
from src.utils.schema_loader import load_schema

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def schema():
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


@pytest.fixture(scope="module")
def works_at(schema):
    return next(edge for edge in schema.edges if edge.type == "WORKS_AT")


def _levels(caplog, needle: str) -> list[int]:
    return [record.levelno for record in caplog.records if needle in record.getMessage()]


# -- quirk: consumer.close() revoke after a stop -------------------------------------------------

def _message(topic: str, offset: int):
    message = MagicMock()
    message.value.return_value = (
        b'{"source":{"personId":"p-1"},"target":{"companyId":"c-1"},"properties":{"since":"2020-01-02"}}'
    )
    message.error.return_value = None
    message.topic.return_value = topic
    message.partition.return_value = 0
    message.offset.return_value = offset
    return message


def _loader_holding_unwritten_work(works_at, *, stop: bool):
    consumer, writer = MagicMock(), MagicMock()
    shutdown = Event()
    loader = EdgeLoader(consumer, writer, works_at, shutdown_requested=shutdown)
    topic = works_at.topics[0]
    partition = TopicPartition(topic, 0)
    loader._on_assign(consumer, [partition])
    assert loader._buffer_message(_message(topic, 7))
    if stop:
        shutdown.set()
    return loader, consumer, writer, partition


@pytest.mark.parametrize("stop, level", [(True, logging.WARNING), (False, logging.ERROR)])
def test_revoke_on_close_logs_a_warning_only_after_a_requested_stop(works_at, caplog, stop, level) -> None:
    loader, consumer, writer, partition = _loader_holding_unwritten_work(works_at, stop=stop)
    # run() has returned (``_running`` is False): consumer.close() fires the revoke callback.
    with caplog.at_level(logging.INFO):
        loader._on_revoke(consumer, [partition])
    assert _levels(caplog, "Kafka partitions revoked with unresolved relationship work") == [level]
    writer.write_batch.assert_not_called()   # nothing written ...
    consumer.commit.assert_not_called()      # ... and nothing committed inside the callback
    assert loader._rebalance_failure is True  # still fail-closed internally
    consumer.unassign.assert_called_once()


def test_a_live_revoke_with_unresolved_work_stays_an_error_even_when_stopping(works_at, caplog) -> None:
    """Inside run() (``_running``) a revoke that leaves work behind is a real fail-closed event."""
    loader, consumer, writer, partition = _loader_holding_unwritten_work(works_at, stop=True)
    loader._running = True
    loader._rebalance_failure = True  # settlement is skipped, so the work stays pending
    with caplog.at_level(logging.INFO):
        loader._on_revoke(consumer, [partition])
    assert _levels(caplog, "Kafka partitions revoked with unresolved relationship work") == [logging.ERROR]


# -- N2: classic bulk monitor wait interrupted by an operator stop ---------------------------------

def _run_bulk_edge(works_at, monitor_error, *, cli_stop: bool, caplog):
    docker = MagicMock()
    docker.run_edge_loader.side_effect = lambda **kw: [MagicMock() for _ in range(kw["replicas"])]
    monitor = MagicMock()
    monitor.wait_for_completion.side_effect = monitor_error
    stop = Event()
    if cli_stop:
        stop.set()
    edge = works_at.model_copy(update={"replicas": 1})
    args = argparse.Namespace(config="c.yaml", network="n", bulk_timeout_seconds=1)
    with patch.object(cli, "RelationshipBulkMonitor", return_value=monitor), \
            patch.object(cli, "_cli_shutdown", stop), \
            patch.object(cli, "_stop_exact_containers", return_value=[]), \
            patch.object(cli, "_remove_exact_containers"), \
            caplog.at_level(logging.INFO):
        return cli._run_bulk_relationship_edge(edge, docker, args, run_id="run1", consumer_group_prefix="g")


def test_bulk_edge_stopped_by_the_operator_logs_a_warning_and_still_fails_the_run(works_at, caplog) -> None:
    rc = _run_bulk_edge(
        works_at, BulkShutdownRequested("shutdown requested while waiting for relationship stage=x completion"),
        cli_stop=True, caplog=caplog,
    )
    assert rc == 1
    assert _levels(caplog, "stage=bulk-edge run_id=run1 edge=WORKS_AT stopped by operator") == [logging.WARNING]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.parametrize(
    "error, cli_stop",
    [(RuntimeError("loader crashed"), True), (RuntimeError("loader crashed"), False),
     (BulkShutdownRequested("shutdown requested"), False)],
)
def test_bulk_edge_real_failures_stay_errors(works_at, caplog, error, cli_stop) -> None:
    assert _run_bulk_edge(works_at, error, cli_stop=cli_stop, caplog=caplog) == 1
    assert _levels(caplog, "stage=bulk-edge run_id=run1 edge=WORKS_AT failed") == [logging.ERROR]


# -- N2: planner aborted by close() after a stop ---------------------------------------------------

def _background_planner(error: BaseException, stop_requested, *, aborted: bool = False) -> KafkaEdgePlanner:
    planner = KafkaEdgePlanner.__new__(KafkaEdgePlanner)
    planner.plan_id = "shut03"
    planner.error = None
    planner._thread = None
    planner.stop_requested = stop_requested
    planner._aborted = aborted

    def finish(_path):
        raise error

    planner.finish = finish
    return planner


@pytest.mark.parametrize(
    "stopped, aborted, level, text",
    [
        (True, True, logging.WARNING, "stopped by operator"),  # close() aborted the plan after a stop
        (True, False, logging.ERROR, "failed"),     # a real planner failure that coincides with a stop
        (False, True, logging.ERROR, "failed"),     # aborted because another part of the run failed
        (False, False, logging.ERROR, "failed"),
    ],
)
def test_planner_failure_is_a_warning_only_for_an_abort_after_a_stop(
    caplog, tmp_path, stopped, aborted, level, text,
) -> None:
    planner = _background_planner(
        ConnectionResetError(104, "Connection reset by peer"), lambda: stopped, aborted=aborted,
    )
    with caplog.at_level(logging.INFO):
        planner.run_in_background(tmp_path / "edge-plan.json").join(5)
    assert _levels(caplog, f"stage=edge-plan plan_id=shut03 {text}") == [level]
    assert isinstance(planner.error, ConnectionResetError)  # the phase still sees the failure


def test_planner_without_a_stop_predicate_keeps_the_error(caplog, tmp_path) -> None:
    planner = _background_planner(RuntimeError("scan failed"), None, aborted=True)
    with caplog.at_level(logging.INFO):
        planner.run_in_background(tmp_path / "edge-plan.json").join(5)
    assert _levels(caplog, "stage=edge-plan plan_id=shut03 failed: scan failed") == [logging.ERROR]


def test_close_marks_the_abort_before_terminating_the_planner_processes(tmp_path) -> None:
    planner = KafkaEdgePlanner.__new__(KafkaEdgePlanner)
    planner.result, planner.plan_path, planner.topic_created = None, None, False
    planner._aborted = False
    seen = {}
    planner.planner = MagicMock()
    planner.planner.close.side_effect = lambda terminate: seen.update(terminate=terminate, aborted=planner._aborted)
    planner._thread = MagicMock(**{"is_alive.return_value": True})
    planner.close(delete_topic=False)
    assert seen == {"terminate": True, "aborted": True}


def test_close_after_a_finished_plan_is_not_an_abort(tmp_path) -> None:
    planner = KafkaEdgePlanner.__new__(KafkaEdgePlanner)
    planner.result, planner.plan_path, planner.topic_created = object(), None, False
    planner._aborted = False
    planner.planner = MagicMock()
    planner._thread = MagicMock(**{"is_alive.return_value": False})
    planner.close(delete_topic=False)
    planner.planner.close.assert_called_once_with(terminate=False)
    assert planner._aborted is False


def test_start_kafka_edge_planner_hands_the_stop_predicate_to_the_planner(schema, tmp_path, monkeypatch) -> None:
    from src.orchestrator.planned_edges import start_kafka_edge_planner

    monkeypatch.chdir(tmp_path)
    created = {}

    class FakePlanner:
        def __init__(self, *args, **kwargs):
            created["planner"] = self

        def start(self):
            pass

        def run_in_background(self, path):
            created["predicate_at_start"] = getattr(self, "stop_requested", None)

    predicate = lambda: False  # noqa: E731
    args = argparse.Namespace(plan_dir=str(tmp_path))
    start_kafka_edge_planner(schema, args, run_id="r1", writers=2, bootstrap_servers="k:9",
                             planner_factory=FakePlanner, stop_requested=predicate)
    assert created["predicate_at_start"] is predicate


# -- N3: retry consumer stopped during startup -----------------------------------------------------

def _stop_then_refuse(*_args, **_kwargs):
    signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)  # the stop arrives during startup
    raise LeaseCoordinatorError("lease reconnect abandoned: shutdown requested")


def test_retry_consumer_stopped_during_startup_exits_zero(caplog) -> None:
    with patch("src.loader.retry_consumer.load_schema", side_effect=_stop_then_refuse), \
            caplog.at_level(logging.INFO):
        assert retry_consumer_main(["--dlq-topics", "social-works-at.dlq"]) == 0
    assert _levels(caplog, "stage=retry-consumer reason=stopped_during_startup") == [logging.WARNING]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_retry_consumer_startup_failure_without_a_stop_still_exits_one(caplog) -> None:
    with patch("src.loader.retry_consumer.load_schema",
               side_effect=LeaseCoordinatorError("lease coordinator unreachable")), \
            caplog.at_level(logging.INFO):
        assert retry_consumer_main(["--dlq-topics", "social-works-at.dlq"]) == 1
    assert _levels(caplog, "stage=retry-consumer reason=startup_failed") == [logging.ERROR]


def test_retry_consumer_real_startup_failure_during_a_stop_stays_an_error(caplog) -> None:
    def stop_then_fail(*_args, **_kwargs):
        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        raise RuntimeError("schema file unreadable")

    with patch("src.loader.retry_consumer.load_schema", side_effect=stop_then_fail), \
            caplog.at_level(logging.INFO):
        assert retry_consumer_main(["--dlq-topics", "social-works-at.dlq"]) == 1
    assert _levels(caplog, "stage=retry-consumer reason=startup_failed error=schema file unreadable") == [logging.ERROR]
    assert not _levels(caplog, "stopped_during_startup")
