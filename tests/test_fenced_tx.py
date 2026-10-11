"""Lease-fenced BEGIN: a leased transaction never outlives its lease deadline (src/loader/fenced_tx.py).

The model: the client picks the Neo4j timeout just before ``begin_transaction()``; the server begins the
transaction at some time no later than when ``begin_transaction()`` returns; Neo4j ends it at most
``TX_MONITOR_SLACK_SECONDS`` after its timeout. Every accepted BEGIN must therefore satisfy
``returned + timeout + slack <= deadline``.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager

import pytest

from src.loader.fenced_tx import (
    BEGIN_ALLOWANCE_SECONDS, MIN_LEASED_TX_TIMEOUT_MS, MIN_TX_TIMEOUT_SECONDS, TX_MONITOR_SLACK_SECONDS,
    LeaseDeadlineExceeded, begin_fenced_transaction,
)
from src.loader.lease_client import LeaseCoordinatorError


class _Clock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class _Tx:
    def __init__(self, timeout, asked_at, begun_at) -> None:
        self.timeout, self.asked_at, self.begun_at = timeout, asked_at, begun_at
        self.statements = 0
        self.committed = self.rolled_back = False

    def run(self, *_a, **_k):
        self.statements += 1

    def commit(self):
        self.committed = True


class _SlowSession:
    """``begin_transaction`` takes ``delays[i]`` seconds of the fake clock (connection acquisition + BEGIN)."""

    def __init__(self, clock: _Clock, delays) -> None:
        self.clock, self.delays = clock, list(delays)
        self.transactions: list[_Tx] = []
        self.kwargs: list[dict] = []

    @contextmanager
    def begin_transaction(self, **kwargs):
        self.kwargs.append(kwargs)
        asked = self.clock.now
        self.clock.now += self.delays.pop(0) if self.delays else 0.0
        tx = _Tx(kwargs.get("timeout"), asked, self.clock.now)  # worst case: the server began it as late as possible
        self.transactions.append(tx)
        try:
            yield tx
        finally:
            if not tx.committed:
                tx.rolled_back = True


def _write(session, clock, deadline, *, tx_timeout=None):
    with begin_fenced_transaction(session, deadline=deadline, tx_timeout_seconds=tx_timeout, clock=clock,
                                  what="edge=T") as tx:
        tx.run("MERGE ...")
        clock.now += 0.01  # the write itself
        tx.commit()


@pytest.mark.parametrize("delay", [0.0, 0.0004, 0.5, 0.99, 1.0, 1.01, 2.5, 5.0, 12.0, 25.0])
@pytest.mark.parametrize("left", [5.0, 10.0, 30.0])
def test_an_accepted_transaction_never_outlives_the_deadline(delay, left) -> None:
    clock = _Clock()
    deadline = clock.now + left
    session = _SlowSession(clock, [delay, delay, delay, delay, delay, delay])
    try:
        _write(session, clock, deadline)
    except LeaseDeadlineExceeded:
        pass
    for tx in session.transactions:
        if tx.statements:
            # Neo4j ends it by begun + timeout + monitor lag; that is within the lease deadline.
            assert tx.begun_at + tx.timeout + TX_MONITOR_SLACK_SECONDS <= deadline + 1e-9
            assert tx.begun_at - tx.asked_at <= BEGIN_ALLOWANCE_SECONDS
        else:
            assert tx.rolled_back and not tx.committed  # a late BEGIN ran no statement
    accepted = [tx for tx in session.transactions if tx.statements]
    assert len(accepted) <= 1


def test_a_late_begin_is_rolled_back_before_any_statement_and_retried(caplog) -> None:
    clock = _Clock()
    session = _SlowSession(clock, [4.0, 0.0])  # a slow new connection, then a pooled one
    with caplog.at_level(logging.INFO, logger="src.loader.fenced_tx"):
        _write(session, clock, clock.now + 30.0)
    first, second = session.transactions
    assert first.statements == 0 and first.rolled_back
    assert second.statements == 1 and second.committed
    assert second.timeout == pytest.approx(30.0 - 4.0 - TX_MONITOR_SLACK_SECONDS - BEGIN_ALLOWANCE_SECONDS)
    assert any("late_transaction_begin" in record.getMessage() for record in caplog.records)


def test_no_budget_left_raises_before_beginning() -> None:
    clock = _Clock()
    session = _SlowSession(clock, [])
    floor = TX_MONITOR_SLACK_SECONDS + BEGIN_ALLOWANCE_SECONDS + MIN_TX_TIMEOUT_SECONDS
    with pytest.raises(LeaseDeadlineExceeded, match="lease deadline exceeded"):
        _write(session, clock, clock.now + floor)
    assert session.transactions == []
    assert issubclass(LeaseDeadlineExceeded, LeaseCoordinatorError)  # existing handling still applies


def test_late_begins_that_use_up_the_deadline_end_in_lease_deadline_exceeded() -> None:
    clock = _Clock()
    session = _SlowSession(clock, [3.0] * 20)
    with pytest.raises(LeaseDeadlineExceeded):
        _write(session, clock, clock.now + 12.0)
    assert all(tx.statements == 0 and tx.rolled_back for tx in session.transactions)


def test_the_normal_pooled_path_is_one_begin() -> None:
    clock = _Clock()
    session = _SlowSession(clock, [0.0003])
    _write(session, clock, clock.now + 30.0, tx_timeout=60.0)
    (tx,) = session.transactions
    assert tx.committed and tx.statements == 1
    assert session.kwargs == [{"timeout": 30.0 - TX_MONITOR_SLACK_SECONDS - BEGIN_ALLOWANCE_SECONDS}]


def test_without_a_deadline_the_begin_is_unchanged() -> None:
    clock = _Clock()
    session = _SlowSession(clock, [7.0, 7.0, 7.0])
    with begin_fenced_transaction(session, deadline=None, tx_timeout_seconds=30.0, clock=clock, what="x",
                                  metadata={"chunk": 1}) as tx:
        tx.run("q")
        tx.commit()
    with begin_fenced_transaction(session, deadline=None, tx_timeout_seconds=None, clock=clock, what="x") as tx:
        tx.commit()
    assert session.kwargs == [{"metadata": {"chunk": 1}, "timeout": 30.0}, {}]
    assert all(tx.committed for tx in session.transactions)


def test_a_failing_statement_rolls_back_and_propagates() -> None:
    clock = _Clock()
    session = _SlowSession(clock, [0.0])
    with pytest.raises(RuntimeError, match="boom"):
        with begin_fenced_transaction(session, deadline=clock.now + 30, tx_timeout_seconds=None, clock=clock,
                                      what="x"):
            raise RuntimeError("boom")
    assert session.transactions[0].rolled_back


def test_the_driver_does_not_pipeline_an_explicit_begin() -> None:
    """The bound needs begin_transaction() to return only after the server acknowledged BEGIN."""
    import inspect

    from neo4j._sync.work import session as session_module
    from neo4j._sync.work import transaction as transaction_module

    assert "send_all()" in inspect.getsource(transaction_module.Transaction._begin)
    assert "if not pipelined" in inspect.getsource(transaction_module.Transaction._begin)
    source = inspect.getsource(session_module.Session.__init__)
    assert "self._pipelined_begin = ContextBool()" in source
    assert not session_module.ContextBool()  # the default is falsy: not pipelined


def _schema_with_tx_timeout(tmp_path, tx_timeout_ms: int):
    import yaml

    from src.utils.schema_loader import load_schema

    raw = yaml.safe_load(open("config/graph_schema.yaml", encoding="utf-8"))
    raw["loading"]["bucket_lease"].update({"tx_timeout_ms": tx_timeout_ms, "expected_chunk_commit_ms": 100})
    path = tmp_path / "schema.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return load_schema(path)


def test_a_short_tx_timeout_still_loads_and_plans_but_lease_modes_refuse_it(tmp_path) -> None:
    """The fenced budget is a lease-mode rule only: planned and classic bulk keep T as a flat timeout."""
    import argparse

    from src.cli import _validate_edge_lease_settings, _validate_edge_plan_settings, _validate_stream_lease_settings
    from src.models.schema import BucketLeaseStreamConfig
    from test_single_topic_compat import _plan_fingerprint

    assert MIN_LEASED_TX_TIMEOUT_MS == 6_000
    BucketLeaseStreamConfig(tx_timeout_ms=4_000, expected_chunk_commit_ms=100)  # loads, as at 4ff068e
    schema = _schema_with_tx_timeout(tmp_path, 4_000)
    assert schema.loading.bucket_lease.tx_timeout_ms == 4_000
    planned_args = argparse.Namespace(mode="bulk", edge_plan="auto", edge_leases=False, plan_writers=None)
    _validate_edge_plan_settings(schema, planned_args)
    assert _plan_fingerprint(schema, tmp_path)["chunks"] > 0  # it plans
    with pytest.raises(ValueError, match=r"--edge-leases requires loading.bucket_lease.tx_timeout_ms >= 6000 "
                                         r"\(got 4000\)"):
        _validate_edge_lease_settings(schema, argparse.Namespace(mode="bulk"))
    with pytest.raises(ValueError, match=r"--mode stream requires loading.bucket_lease.tx_timeout_ms >= 6000"):
        _validate_stream_lease_settings(schema, argparse.Namespace(mode="stream"))
    ok = _schema_with_tx_timeout(tmp_path, 6_000)
    _validate_edge_lease_settings(ok, argparse.Namespace(mode="bulk"))
    _validate_stream_lease_settings(ok, argparse.Namespace(mode="stream"))


def test_handle_start_refuses_a_short_tx_timeout_for_stream_before_any_side_effect(tmp_path, caplog) -> None:
    import argparse
    from unittest.mock import patch

    from src.cli import handle_start

    schema = _schema_with_tx_timeout(tmp_path, 4_000)
    args = argparse.Namespace(mode="bulk", config="c.yaml", network="net", skip_image_build=True, edge_leases=True)
    with patch("src.cli.load_schema", return_value=schema), patch("src.cli.DockerService") as docker, \
            patch("src.cli.apply_schema") as apply_schema, caplog.at_level(logging.ERROR):
        assert handle_start(args) == 1
    assert any("tx_timeout_ms >= 6000" in record.getMessage() for record in caplog.records)
    apply_schema.assert_not_called()
    docker.assert_not_called()


# -- real writers: a slow BEGIN never yields a transaction that can outlive the deadline ----------------------------

def _apoc_writer(clock):
    from unittest.mock import MagicMock

    from src.loader.edge_execution import ApocLockedEdgeWriter
    from src.loader.edge_loader import MissingRelationshipEndpointError
    from test_edge_execution_writer import _edge, _ok

    driver = MagicMock()
    session = _SlowSession(clock, [])
    driver.session.return_value.__enter__.return_value = session
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
                                  clock=clock)
    session.ok = _ok
    return writer, session


def test_edge_writer_retries_a_late_begin_within_the_lease(monkeypatch) -> None:
    clock = _Clock()
    writer, session = _apoc_writer(clock)
    session.delays = [6.0, 0.0]
    monkeypatch.setattr(_Tx, "run", lambda self, *_a, **_k: (setattr(self, "statements", self.statements + 1),
                                                           session.ok())[1])
    deadline = clock.now + 30.0
    writer.write_rows([{"source_key": "p-1", "target_key": "c-1", "properties": {}}], deadline=deadline)
    late, accepted = session.transactions
    assert late.statements == 0 and late.rolled_back
    assert accepted.committed and accepted.begun_at + accepted.timeout + TX_MONITOR_SLACK_SECONDS <= deadline


def test_node_writer_never_begins_past_the_budget() -> None:
    from unittest.mock import MagicMock

    from src.loader.node_loader import NodeWriter
    from src.models.schema import NodeConfig

    clock = _Clock()
    driver = MagicMock()
    session = _SlowSession(clock, [9.0, 9.0, 9.0])
    driver.session.return_value.__enter__.return_value = session
    config = NodeConfig(label="Person", topic="people", key_property="id",
                        properties={"id": {"type": "string", "required": True}})
    writer = NodeWriter(driver, config, clock=clock)
    from src.loader.node_loader import NodeRecord

    with pytest.raises(LeaseDeadlineExceeded):
        writer.write_batch([NodeRecord(key="p-1", properties={})], deadline=clock.now + 20.0)
    assert session.transactions and all(tx.statements == 0 for tx in session.transactions)


# -- the coordinator re-queues the cell and writes it under a fresh grant --------------------------------------------

class _FencedWriter:
    """Begins fenced transactions on a slow session: the first BEGIN of the flush eats the whole deadline."""

    def __init__(self, clock: _Clock, first_delay: float) -> None:
        self.clock = clock
        self.session = _SlowSession(clock, [first_delay])
        self.deadlines: list[float] = []
        self.written: list = []

    def write_batch(self, records, *, deadline=None, lease_liveness_check=None) -> None:
        self.deadlines.append(deadline)
        with begin_fenced_transaction(self.session, deadline=deadline, tx_timeout_seconds=None, clock=self.clock,
                                      what="edge=T") as tx:
            tx.run("MERGE")
            tx.commit()
        self.written.extend(record.properties["seq"] for record in records)


def test_coordinator_requeues_a_cell_whose_deadline_ran_out_and_regrants_it() -> None:
    from test_bucket_lease_execution import _Hub, _coordinator, _pending

    from src.loader.lease_client import BucketCellRouter

    clock = _Clock()
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    # 40 s connection acquisition: longer than T, so the late-begin retries cannot fit; the cell must be re-granted.
    writer = _FencedWriter(clock, first_delay=40.0)
    coordinator, _clients = _coordinator(router, writer, hub, workers=1, tx_timeout_seconds=30.0, clock=clock,
                                         batch_size=10)
    pending = [_pending("solo-a", "solo-b", seq) for seq in range(3)]
    results = coordinator.execute_pending(pending)
    assert all(result.success for result in results)
    assert writer.written == [0, 1, 2]  # written once, in order
    assert hub.grants == 2  # the first grant was released and a fresh one taken
    assert writer.deadlines[1] > writer.deadlines[0]  # fresh grant, fresh deadline
    late = writer.session.transactions[0]
    assert late.statements == 0 and late.rolled_back
    for tx in writer.session.transactions:
        if tx.statements:
            assert tx.begun_at + tx.timeout + TX_MONITOR_SLACK_SECONDS <= writer.deadlines[-1]
    assert not hub.holder  # every lease released


def test_coordinator_fails_the_flush_after_too_many_requeues() -> None:
    from test_bucket_lease_execution import _Hub, _coordinator, _pending

    from src.loader.edge_execution import BucketLeaseExecutionCoordinator
    from src.loader.lease_client import BucketCellRouter

    clock = _Clock()
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _FencedWriter(clock, first_delay=0.0)
    writer.session.delays = [40.0] * 1000  # Neo4j never answers in time
    coordinator, _ = _coordinator(router, writer, hub, workers=1, tx_timeout_seconds=30.0, clock=clock)
    results = coordinator.execute_pending([_pending("solo-a", "solo-b", 0)])
    assert not any(result.success for result in results)
    assert isinstance(results[0].exception, LeaseDeadlineExceeded)
    assert hub.grants == BucketLeaseExecutionCoordinator._MAX_DEADLINE_REQUEUES + 1
    assert all(tx.statements == 0 for tx in writer.session.transactions)
    assert not hub.holder
