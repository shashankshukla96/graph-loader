"""Unit tests (no Docker) for tests/stress/lock_wait.py (Phase 6 Slice 4, Story 2).

A dedicated `@pytest.mark.integration` test proving the real query-log
wait-detection AC lives in tests/test_lock_wait_integration.py (its own
function-scoped Neo4jContainer, per the story's note that the session-scoped
`neo4j_driver` fixture must not be mutated).
"""
from __future__ import annotations

import socket
from unittest.mock import MagicMock

import pytest
from neo4j.exceptions import TransientError

from src.loader.edge_execution import ApocLockedEdgeWriter
from src.loader.edge_loader import EdgeRecord, MissingRelationshipEndpointError
from src.utils.schema_loader import load_schema
from tests.stress import lock_wait


def _edge():
    return next(e for e in load_schema("config/graph_schema.yaml").edges if e.type == "WORKS_AT")


def _ok(rows=1):
    result = MagicMock()
    result.__iter__.return_value = iter([{"checked_rows": rows, "invalid_rows": 0, "merged_rows": rows}])
    return result


def _transient_writer(tx_runs, *, attempts=3):
    """An APOC writer whose transactions replay ``tx_runs`` in order (mirrors test_edge_execution_writer.py)."""
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = tx_runs
    edge = _edge().model_copy(update={"retry": _edge().retry.model_copy(update={
        "max_attempts": attempts, "base_delay_seconds": 0.001, "max_delay_seconds": 0.002,
    })})
    writer = ApocLockedEdgeWriter(
        driver, edge, missing_endpoint_error_factory=MissingRelationshipEndpointError, sleep=lambda _s: None,
    )
    return writer


# -- total_lock_wait_ms / any_lock_wait / assert_zero_lock_wait -------------------


def _fake_container(log_text: str, *, exit_code: int = 0):
    container = MagicMock()
    exec_result = MagicMock()
    exec_result.output = log_text.encode("utf-8")
    exec_result.exit_code = exit_code
    container.get_wrapped_container.return_value.exec_run.return_value = exec_result
    return container


def test_total_lock_wait_ms_sums_every_match() -> None:
    log = (
        "... 44 ms: (planning: 2, waiting: 0) - ...\n"
        "... 3028 ms: (planning: 26, waiting: 0) - ...\n"
        "... 2636 ms: (planning: 51, waiting: 2579) - ...\n"
    )
    assert lock_wait.total_lock_wait_ms(_fake_container(log)) == 2579


def test_wait_regex_matches_the_real_log_shape_not_a_hypothetical_ms_suffix() -> None:
    """The real query log format has no 'ms' token right after 'waiting: N'.

    A regex that (wrongly) required "waiting: N ms" would never match the
    real line shape below and would make every zero-wait assertion vacuous.
    """
    assert lock_wait._WAIT_RE.search("(planning: 51, waiting: 2579)") is not None
    assert lock_wait._WAIT_RE.search("(planning: 51, waiting: 2579 ms)") is None


def test_any_lock_wait_returns_first_nonzero_or_none() -> None:
    assert lock_wait.any_lock_wait(_fake_container("(planning: 1, waiting: 0)\n")) is None
    assert lock_wait.any_lock_wait(_fake_container(
        "(planning: 1, waiting: 0)\n(planning: 2, waiting: 17)\n(planning: 1, waiting: 9)\n"
    )) == 17


def test_assert_zero_lock_wait_passes_and_fails_correctly() -> None:
    lock_wait.assert_zero_lock_wait(_fake_container("(planning: 1, waiting: 0)\n"))
    with pytest.raises(AssertionError):
        lock_wait.assert_zero_lock_wait(_fake_container("(planning: 1, waiting: 42)\n"))


def test_assert_zero_lock_wait_rejects_an_empty_or_query_free_log() -> None:
    """An empty log must never look like a passing 'zero lock waits' result."""
    with pytest.raises(AssertionError, match="found 0"):
        lock_wait.assert_zero_lock_wait(_fake_container(""))
    with pytest.raises(AssertionError, match="found 0"):
        lock_wait.assert_zero_lock_wait(_fake_container("some unrelated log line with no waiting field\n"))


def test_query_log_read_raises_on_nonzero_exit_code() -> None:
    """A missing/unreadable query log must raise loudly, not silently report zero."""
    with pytest.raises(lock_wait.QueryLogUnavailable):
        lock_wait.total_lock_wait_ms(_fake_container("cat: /logs/query.log*: No such file or directory\n", exit_code=1))


def test_since_offset_windows_out_earlier_waits() -> None:
    early = "(planning: 1, waiting: 999)\n"
    container = _fake_container(early + "(planning: 1, waiting: 0)\n")
    mark = lock_wait.query_log_mark(_fake_container(early))
    assert lock_wait.total_lock_wait_ms(container) == 999
    assert lock_wait.total_lock_wait_ms(container, since=mark) == 0
    lock_wait.assert_zero_lock_wait(container, since=mark)


def test_since_out_of_range_raises() -> None:
    container = _fake_container("(planning: 1, waiting: 0)\n")
    with pytest.raises(ValueError):
        lock_wait.total_lock_wait_ms(container, since=10_000)


# -- count_retry_log_lines --------------------------------------------------------


def test_count_retry_log_lines_counts_real_retries_from_the_real_writer(caplog) -> None:
    deadlock = TransientError("Neo.TransientError.Transaction.DeadlockDetected")
    writer = _transient_writer([deadlock, deadlock, _ok()], attempts=3)
    with caplog.at_level("WARNING", logger="src.loader.edge_execution"):
        writer.write_batch([EdgeRecord("p-1", "c-1", {"since": None})])
    assert lock_wait.count_retry_log_lines(caplog) == 2
    assert lock_wait.count_retry_log_lines(caplog, edge_type="WORKS_AT") == 2
    assert lock_wait.count_retry_log_lines(caplog, edge_type="KNOWS") == 0


def test_count_retry_log_lines_zero_when_no_retry_happened(caplog) -> None:
    writer = _transient_writer([_ok()], attempts=3)
    with caplog.at_level("WARNING", logger="src.loader.edge_execution"):
        writer.write_batch([EdgeRecord("p-1", "c-1", {"since": None})])
    assert lock_wait.count_retry_log_lines(caplog) == 0


def test_count_retry_log_lines_ignores_a_lookalike_message_from_another_logger(caplog) -> None:
    import logging

    other = logging.getLogger("some.other.module")
    with caplog.at_level("WARNING", logger="some.other.module"):
        other.warning("stage=edge-write-retry edge=%s records=%s attempt=%s/%s delay_s=%.3f code=%s",
                       "KNOWS", 10, 1, 3, 0.1, "Deadlock")
    assert lock_wait.count_retry_log_lines(caplog) == 0


# -- assert_no_unhandled_deadlock --------------------------------------------------


def test_assert_no_unhandled_deadlock_returns_the_attempt_count() -> None:
    assert lock_wait.assert_no_unhandled_deadlock(lambda: 2) == 2


def test_assert_no_unhandled_deadlock_reraises_unchanged_on_exhausted_retries() -> None:
    def _boom():
        raise TransientError("Neo.TransientError.Transaction.DeadlockDetected")

    with pytest.raises(TransientError):
        lock_wait.assert_no_unhandled_deadlock(_boom)


def test_assert_no_unhandled_deadlock_rejects_a_non_int_return() -> None:
    with pytest.raises(AssertionError):
        lock_wait.assert_no_unhandled_deadlock(lambda: "not-a-count")


# -- try_scrape_metric --------------------------------------------------------------


def test_try_scrape_metric_returns_none_when_nothing_listens() -> None:
    # A closed local port: bind briefly to find a free one, then close it.
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    assert lock_wait.try_scrape_metric(f"http://127.0.0.1:{port}/metrics", "graph_loader_records_consumed_total") is None


def test_try_scrape_metric_sums_matching_series_across_label_sets(monkeypatch) -> None:
    body = (
        "graph_loader_records_consumed_total{kind=\"edge\",name=\"POINTS_AT\"} 7\n"
        "graph_loader_records_consumed_total{kind=\"node\",name=\"Hub\"} 3\n"
        "graph_loader_other_metric 99\n"
    )

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return body.encode("utf-8")

    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: _Resp())
    assert lock_wait.try_scrape_metric("http://x/metrics", "graph_loader_records_consumed_total") == 10.0
    assert lock_wait.try_scrape_metric("http://x/metrics", "graph_loader_missing_metric") is None
    assert lock_wait.try_scrape_metric(
        "http://x/metrics", "graph_loader_records_consumed_total", labels={"kind": "edge"},
    ) == 7.0
    assert lock_wait.try_scrape_metric(
        "http://x/metrics", "graph_loader_records_consumed_total", labels={"kind": "missing"},
    ) is None
