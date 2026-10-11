"""Stream outage resilience: live stream loaders ride out a Neo4j outage; bulk paths are untouched.

The stream edge loader (the default lease fleet) and the stream node loader
run through the real ``BucketLeaseExecutionCoordinator``, the real writers
(``ApocLockedEdgeWriter``/``NodeWriter``) and the real loaders. The fakes
underneath are a Neo4j driver whose outage can be switched on and off and
which logs every committed transaction in order, a lease client that tracks
every held lease, and a Kafka consumer that records pause, resume, poll and
commit calls. The ride-out's clock and sleep are faked, so outages of minutes
take no wall time.
"""
from __future__ import annotations

import ast
from collections import deque
import itertools
import json
import logging
from pathlib import Path
import threading
from threading import Event
import time
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import TopicPartition
from neo4j.exceptions import ClientError, ServiceUnavailable, SessionExpired, TransientError

from src.loader import write_outage as wo
from src.loader.edge_execution import (
    ApocLockedEdgeWriter, BucketLeaseExecutionCoordinator, LaneExecutionCoordinator, build_edge_execution,
)
from src.loader.edge_loader import EdgeLoader, MissingRelationshipEndpointError, main as edge_loader_main
from src.loader.lease_client import BucketCellRouter, LeaseCoordinatorError
from src.loader.node_loader import LeasedNodeWriter, NodeLoader, NodeWriter, main as node_loader_main
from src.loader.write_outage import StreamWriteOutage, WriteOutageDrainAbandoned
from src.models.schema import LoadingConfig
from src.utils.metrics import GraphLoaderMetrics
from src.utils.schema_loader import load_schema

ROOT = Path(__file__).resolve().parents[1]
EDGE_TOPIC = "works-at-events"
NODE_TOPIC = "person-events"


@pytest.fixture(scope="module")
def schema():
    return load_schema(ROOT / "config" / "graph_schema.yaml")


@pytest.fixture(scope="module")
def works_at(schema):
    return next(edge for edge in schema.edges if edge.type == "WORKS_AT")


@pytest.fixture(scope="module")
def person(schema):
    return next(node for node in schema.nodes if node.label == "Person")


def _neo_error(code: str, message: str = "boom") -> ClientError:
    return ClientError._hydrate_neo4j(code=code, message=message)


def _unavailable() -> Exception:
    return ServiceUnavailable("Failed to read from defunct connection (Neo4j stopped)")


# -- fakes --------------------------------------------------------------------

class FakeClock:
    """The ride-out's clock and sleep. ``on_sleep`` hooks run after each sleep."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []
        self.on_sleep: list = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
        for hook in list(self.on_sleep):
            hook(self.now)


class FakeGraph:
    """A Neo4j driver double: explicit transactions, an ordered commit log, a switchable outage.

    While an outage is armed, a transaction raises at ``stage`` (``begin``,
    ``run`` or ``commit``), once ``after_commits`` transactions committed
    since it was armed (to fail in the middle of a flush). A transaction
    that is not committed counts as rolled back."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.committed: list[tuple[str, tuple]] = []
        self.rollbacks = 0
        self.begins = 0
        self.failed_attempts = 0
        self._error = None
        self._stage = "run"
        self._fail_after: int | None = None
        # source keys that do not resolve (row-level DLQ tests)
        self.missing_sources: set = set()

    def outage(self, error=_unavailable, *, stage: str = "run", after_commits: int = 0) -> None:
        with self._lock:
            self._error, self._stage = error, stage
            self._fail_after = len(self.committed) + after_commits

    def recover(self) -> None:
        with self._lock:
            self._error = None

    @property
    def down(self) -> bool:
        return self._error is not None

    def _maybe_fail(self, stage: str) -> None:
        with self._lock:
            if self._error is None or stage != self._stage or len(self.committed) < self._fail_after:
                return
            self.failed_attempts += 1
            error = self._error()
        raise error

    def session(self, **_kwargs):
        return _FakeSession(self)

    def close(self) -> None:
        pass

    # graph state: last write wins, applied in commit order
    def edges(self) -> dict:
        state: dict = {}
        for kind, rows in self.committed:
            if kind == "edge":
                for row in rows:
                    state.setdefault((row["source_key"], row["target_key"]), {})["role"] = row["properties"].get("role")
        return state

    def nodes(self) -> dict:
        state: dict = {}
        for kind, rows in self.committed:
            if kind == "node":
                for row in rows:
                    state.setdefault(row["key"], {}).update(row["properties"])
        return state

    def history(self, pair) -> list:
        """Every committed write of one relationship pair, in commit order."""
        return [
            row["properties"].get("role") for kind, rows in self.committed if kind == "edge"
            for row in rows if (row["source_key"], row["target_key"]) == pair
        ]


class _FakeSession:
    def __init__(self, graph: FakeGraph) -> None:
        self._graph = graph

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def begin_transaction(self, **_kwargs):
        self._graph._maybe_fail("begin")
        with self._graph._lock:
            self._graph.begins += 1
        return _FakeTx(self._graph)


class _FakeResult:
    def __init__(self, rows) -> None:
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def consume(self) -> None:
        pass


class _FakeTx:
    def __init__(self, graph: FakeGraph) -> None:
        self._graph = graph
        self._pending: tuple[str, tuple] | None = None
        self._committed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if not self._committed:
            with self._graph._lock:
                self._graph.rollbacks += 1
        return False

    def run(self, query, **params):
        self._graph._maybe_fail("run")
        if "rows" in params:
            rows = tuple(params["rows"])
            missing = self._graph.missing_sources
            if "RETURN row_index, source_matches" in query:  # the read-only classification query
                return _FakeResult([
                    {"row_index": i, "source_matches": 0 if row["source_key"] in missing else 1, "target_matches": 1}
                    for i, row in enumerate(rows)
                ])
            resolved = tuple(row for row in rows if row["source_key"] not in missing)
            self._pending = ("edge", resolved)
            invalid = len(rows) - len(resolved)
            return _FakeResult([{"checked_rows": len(resolved), "invalid_rows": invalid, "merged_rows": len(resolved)}])
        self._pending = ("node", tuple(params["batch"]))
        return _FakeResult([])

    def commit(self) -> None:
        self._graph._maybe_fail("commit")
        with self._graph._lock:
            self._graph.committed.append(self._pending or ("edge", ()))  # a classification-only transaction
        self._committed = True


class LeaseLedger:
    """The lease server double: exclusive grants, and the B4-1 disconnect quarantine.

    A grant is exclusive: a resource set is granted only when none of its
    resources is held by another lease or still quarantined. A disconnect
    (``close``) moves the connection's leases to quarantine for
    ``hold_seconds`` (``T + margin`` on the real server), during which their
    resources are denied to everyone. ``quarantined`` is the cumulative list of
    quarantined resource sets; ``released`` counts explicit RELs (confirmed
    outcomes only)."""

    def __init__(self, hold_seconds: float = 0.05) -> None:
        self.lock = threading.Lock()
        self.held: dict[str, tuple] = {}
        self.quarantined: list[tuple] = []
        self._fenced: list[tuple[float, frozenset]] = []
        self.hold_seconds = hold_seconds
        self.released = 0
        self.denied_while_fenced = 0
        self.server_down = False
        self.on_acquire = None  # hook(ledger) -> "deny" | None
        self._ids = itertools.count(1)

    def client(self) -> "LedgerClient":
        return LedgerClient(self)

    def _grantable(self, resources) -> bool:
        now = time.monotonic()
        self._fenced = [(until, fenced) for until, fenced in self._fenced if until > now]
        wanted = set(resources)
        if any(wanted & fenced for _until, fenced in self._fenced):
            self.denied_while_fenced += 1
            return False
        return not any(wanted & set(held) for held in self.held.values())


class LedgerClient:
    def __init__(self, ledger: LeaseLedger) -> None:
        self._ledger = ledger
        self.broken = False
        self._mine: set[str] = set()

    def connect(self) -> None:
        if self._ledger.server_down:
            self.broken = True
            raise LeaseCoordinatorError("lease server unavailable")

    def acquire_any(self, resource_sets, *, wait_ms):
        if self._ledger.server_down:
            self.broken = True
            raise LeaseCoordinatorError("lease server connection lost")
        hook = self._ledger.on_acquire
        if hook is not None and hook(self._ledger) == "deny":
            time.sleep(0.002)
            return None
        with self._ledger.lock:
            index = next((i for i, rs in enumerate(resource_sets) if self._ledger._grantable(rs)), None)
            if index is None:
                granted = None
            else:
                granted = f"lease-{next(self._ledger._ids)}"
                self._ledger.held[granted] = tuple(resource_sets[index])
        if granted is None:
            time.sleep(0.002)  # the real server waits up to wait_ms for a release
            return None
        self._mine.add(granted)
        return granted, index

    def extend(self, lease_id, weighted_sets, *, budget):
        return []

    def release(self, lease_id) -> None:
        with self._ledger.lock:
            self._ledger.held.pop(lease_id, None)
            self._ledger.released += 1
        self._mine.discard(lease_id)

    def ping(self) -> bool:
        return not self.broken and not self._ledger.server_down

    def close(self) -> None:
        # A disconnect: the server quarantines whatever this connection held (B4-1).
        with self._ledger.lock:
            for lease_id in self._mine:
                if lease_id in self._ledger.held:
                    resources = self._ledger.held.pop(lease_id)
                    self._ledger.quarantined.append(resources)
                    self._ledger._fenced.append((time.monotonic() + self._ledger.hold_seconds, frozenset(resources)))
        self._mine.clear()
        self.broken = True


class FakeConsumer:
    """A Kafka consumer double that honours pause/resume and records polls and commits."""

    def __init__(self, messages=(), clock=None) -> None:
        self.queue: deque = deque(messages)
        self.commits: list[list[tuple[str, int, int]]] = []
        self.paused: set[tuple[str, int]] = set()
        self.pause_calls: list = []
        self.resume_calls: list = []
        self.poll_times: list[float] = []
        self.poll_hook = None
        self.on_assign = self.on_revoke = None
        self._clock = clock or time.monotonic

    def subscribe(self, topics, on_assign=None, on_revoke=None) -> None:
        self.on_assign, self.on_revoke = on_assign, on_revoke

    def assign_now(self, topic: str, partitions=(0,)) -> None:
        self.on_assign(self, [TopicPartition(topic, partition) for partition in partitions])

    def revoke_now(self, topic: str, partitions=(0,)) -> None:
        self.on_revoke(self, [TopicPartition(topic, partition) for partition in partitions])

    def assign(self, partitions) -> None:
        pass

    def unassign(self) -> None:
        self.paused.clear()

    def poll(self, timeout=None):
        self.poll_times.append(self._clock())
        if self.poll_hook is not None:
            hook, self.poll_hook = self.poll_hook, None
            hook()
        for index, message in enumerate(self.queue):
            if (message.topic(), message.partition()) not in self.paused:
                del self.queue[index]
                return message
        return None

    def pause(self, partitions) -> None:
        keys = {(tp.topic, tp.partition) for tp in partitions}
        self.paused.update(keys)
        self.pause_calls.append(sorted(keys))

    def resume(self, partitions) -> None:
        keys = {(tp.topic, tp.partition) for tp in partitions}
        self.paused.difference_update(keys)
        self.resume_calls.append(sorted(keys))

    def commit(self, offsets=None, asynchronous=False, message=None):
        self.commits.append([(tp.topic, tp.partition, tp.offset) for tp in offsets])
        return [TopicPartition(tp.topic, tp.partition, tp.offset) for tp in offsets]

    def close(self) -> None:
        pass


def _message(payload: dict, *, topic: str, offset: int, partition: int = 0):
    message = MagicMock()
    message.value.return_value = json.dumps(payload).encode()
    message.error.return_value = None
    message.key.return_value = None
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    return message


def _edge_message(person: str, company: str, role: str, offset: int):
    return _message(
        {"source": {"personId": person}, "target": {"companyId": company},
         "properties": {"since": "2020-01-02", "role": role}},
        topic=EDGE_TOPIC, offset=offset,
    )


def _node_message(person_id: str, name: str, offset: int):
    return _message({"personId": person_id, "name": name}, topic=NODE_TOPIC, offset=offset)


# Offsets 10..15 of one partition. Pair (p-1, c-1) is written three times, so its
# final role proves last-write-wins order; the four pairs spread over several cells.
EDGE_BATCH = [
    ("p-1", "c-1", "r1"), ("p-2", "c-2", "r1"), ("p-1", "c-1", "r2"),
    ("p-3", "c-1", "r1"), ("p-4", "c-3", "r1"), ("p-1", "c-1", "r3"),
]


def _edge_messages(start: int = 10):
    return [_edge_message(p, c, r, start + i) for i, (p, c, r) in enumerate(EDGE_BATCH)]


def _expected_edges() -> dict:
    state: dict = {}
    for p, c, r in EDGE_BATCH:
        state[(p, c)] = {"role": r}
    return state


def _outage(clock: FakeClock, metrics=None, **kwargs) -> StreamWriteOutage:
    return StreamWriteOutage(
        kind=kwargs.pop("kind", "edge"), name=kwargs.pop("name", "WORKS_AT"),
        metrics=metrics or GraphLoaderMetrics(), clock=clock, sleep=clock.sleep, **kwargs,
    )


def _gauge(metrics: GraphLoaderMetrics, kind: str, name: str) -> float:
    """The gauge as a scrape sees it (it is computed at collection time)."""
    return metrics.registry.get_sample_value("graph_loader_write_outage_seconds", {"kind": kind, "name": name})


def _stream_edge_loader(works_at, graph, ledger, consumer, *, outage, stop=None, drain_budget=5.0, buckets=4):
    stop = stop or Event()
    writer = ApocLockedEdgeWriter(
        graph, works_at, explicit_locks=False, sort_rows=False, sleep=lambda _s: None,
        missing_endpoint_error_factory=MissingRelationshipEndpointError,
    )
    coordinator = BucketLeaseExecutionCoordinator(
        writer, router=BucketCellRouter("Person", "Company", buckets),
        worker_count=2, batch_size=2, merge_target_rows=0, client_factory=ledger.client,
        stop_event=stop, drain_budget_seconds=drain_budget,
    )
    coordinator.prime()
    loader = EdgeLoader(
        consumer, writer, works_at, rejection_sink=MagicMock(), coordinator=coordinator,
        loading_config=LoadingConfig(unwind_batch_size=100, flush_interval_ms=60_000),
        shutdown_requested=stop, write_outage=outage, run_id="r1",
    )
    return loader, coordinator


def _stream_node_loader(person, graph, ledger, consumer, *, outage, stop=None, drain_budget=5.0):
    stop = stop or Event()
    coordinator = BucketLeaseExecutionCoordinator(
        NodeWriter(graph, person), router=BucketCellRouter("Person", "Person", 4),
        worker_count=2, batch_size=2, merge_target_rows=0, client_factory=ledger.client,
        edge_type="Person", kind="node", stop_event=stop, drain_budget_seconds=drain_budget,
    )
    coordinator.prime()
    producer = MagicMock()
    producer.flush.return_value = 0
    loader = NodeLoader(
        consumer, LeasedNodeWriter(coordinator), person, producer=producer, run_id="r1",
        shutdown_requested=stop,
        loading_config=LoadingConfig(unwind_batch_size=100, flush_interval_ms=60_000, retry_base_delay_ms=1),
        sleeper=lambda _s: None, write_outage=outage,
    )
    return loader


def _record_held_leases_while_waiting(clock: FakeClock, ledger: LeaseLedger, graph: FakeGraph | None = None) -> list:
    """Snapshot (held leases, explicit RELs, quarantined sets, commits) at every wait slice."""
    snapshots: list = []
    clock.on_sleep.append(lambda _now: snapshots.append((
        dict(ledger.held), ledger.released, len(ledger.quarantined),
        None if graph is None else len(graph.committed),
    )))
    return snapshots


def _assert_fenced_not_released(snapshots) -> None:
    """P5-Q1/B4-2: at the first wait the commit-stage failure's leases were quarantined by the
    disconnect, never RELeased; every explicit REL so far belongs to a confirmed commit."""
    held, released, quarantined, committed = snapshots[0]
    assert held == {}
    assert quarantined >= 1
    assert released == committed
    assert all(snapshot[0] == {} for snapshot in snapshots)


# -- StreamWriteOutage unit behaviour -----------------------------------------

def test_backoff_doubles_from_half_a_second_to_a_30_second_cap() -> None:
    clock = FakeClock()
    outage = _outage(clock)
    delays = [outage.on_failure(_unavailable()).delay_seconds for _ in range(9)]
    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]


@pytest.mark.parametrize("error, action", [
    (_unavailable(), wo.RETRY),
    (SessionExpired("session expired"), wo.RETRY),
    (TransientError("transient"), wo.RETRY),
    (_neo_error("Neo.TransientError.General.DatabaseUnavailable"), wo.RETRY),
    (_neo_error("Neo.ClientError.General.WriteOnReadOnlyAccessDatabase"), wo.RETRY),
    (_neo_error("Neo.ClientError.Security.Unauthorized"), wo.RETRY),  # fatal only once it persists
    (_neo_error("Neo.ClientError.Database.DatabaseNotFound"), wo.RETRY),
    (LeaseCoordinatorError("lease server unavailable"), wo.RETRY),
    (_neo_error("Neo.ClientError.Schema.ConstraintValidationFailed"), wo.FAIL),  # row
    (_neo_error("Neo.ClientError.Statement.SyntaxError"), wo.FAIL),  # unknown
    (RuntimeError("bug"), wo.FAIL),  # unknown
    (_neo_error("Neo.ClientError.Transaction.TransactionTimedOut"), wo.FAIL),  # capacity
    (_neo_error("Neo.TransientError.General.TransactionMemoryLimit"), wo.FAIL),  # capacity
    (_neo_error("Neo.TransientError.General.MemoryPoolOutOfMemoryError"), wo.FAIL),  # server
    # S3: transient by Neo4j's code, but one huge batch can cause them: not ridden out
    (_neo_error("Neo.TransientError.General.OutOfMemoryError"), wo.FAIL),
    (_neo_error("Neo.TransientError.General.StackOverFlowError"), wo.FAIL),
    (MissingRelationshipEndpointError("missing"), wo.FAIL),
])
def test_only_connectivity_class_errors_are_ridden_out(error, action) -> None:
    outage = _outage(FakeClock())
    assert outage.on_failure(error).action == action
    assert outage.active is (action == wo.RETRY)


def test_gauge_counts_outage_seconds_and_returns_to_zero_on_recovery(caplog) -> None:
    clock, metrics = FakeClock(), GraphLoaderMetrics()
    outage = _outage(clock, metrics)
    assert _gauge(metrics, "edge", "WORKS_AT") == 0.0  # pre-touched: a real 0 sample
    outage.on_failure(_unavailable())
    clock.now += 12.5
    outage.refresh()
    assert _gauge(metrics, "edge", "WORKS_AT") == pytest.approx(12.5)
    with caplog.at_level(logging.INFO):
        outage.recovered()
    assert _gauge(metrics, "edge", "WORKS_AT") == 0.0
    assert not outage.active
    assert any("stage=write-outage-recovered" in r.getMessage() for r in caplog.records)


def test_sustained_outage_error_is_logged_at_threshold_then_rate_limited(caplog) -> None:
    clock = FakeClock()
    outage = _outage(clock, error_after_seconds=30.0, error_interval_seconds=300.0)
    with caplog.at_level(logging.WARNING):
        outage.on_failure(_unavailable())
        for _ in range(100):  # 10 minutes in 6 s steps
            clock.now += 6.0
            outage.refresh()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR and "reason=neo4j_outage" in r.getMessage()]
    # first at 30 s, then at most every 300 s: 30, 330 (630 > 600)
    assert len(errors) == 2
    assert "kind=edge name=WORKS_AT" in errors[0].getMessage()
    assert "ServiceUnavailable" in errors[0].getMessage()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("reason=neo4j_unavailable" in r.getMessage() for r in warnings)


def test_fatal_rule_mirrors_the_retry_consumer() -> None:
    """Auth/database-not-found continuously for fatal_after: fatal. A gap restarts the
    run; a non-fatal Neo4j answer resets it; a driver error without a code neither
    resets nor extends it (the retry consumer's F2 rule)."""
    clock = FakeClock()
    auth = _neo_error("Neo.ClientError.Security.Unauthorized")
    outage = _outage(clock, fatal_after_seconds=600.0, fatal_gap_seconds=60.0)

    assert outage.on_failure(auth).action == wo.RETRY
    for _ in range(9):
        clock.now += 50
        assert outage.on_failure(auth).action == wo.RETRY  # 450 s
        clock.now += 10
        assert outage.on_failure(_unavailable()).action == wo.RETRY  # no code: neither resets nor extends
    clock.now += 50
    assert outage.on_failure(auth).action == wo.RETRY  # 590 s
    clock.now += 10
    assert outage.on_failure(auth).action == wo.FATAL  # 600 s

    outage = _outage(clock, fatal_after_seconds=600.0, fatal_gap_seconds=60.0)
    outage.on_failure(auth)
    clock.now += 500
    outage.on_failure(auth)  # gap of 500 s > 60 s: a new run starts here
    clock.now += 200
    assert outage.on_failure(auth).action == wo.RETRY

    outage = _outage(clock, fatal_after_seconds=600.0, fatal_gap_seconds=60.0)
    outage.on_failure(_neo_error("Neo.ClientError.Database.DatabaseNotFound"))
    for _ in range(11):
        clock.now += 30
        outage.on_failure(_neo_error("Neo.ClientError.Database.DatabaseNotFound"))
    clock.now += 30
    outage.on_failure(_neo_error("Neo.TransientError.General.DatabaseUnavailable"))  # Neo4j answered: reset
    clock.now += 30
    assert outage.on_failure(_neo_error("Neo.ClientError.Database.DatabaseNotFound")).action == wo.RETRY


def test_wait_polls_every_slice_and_stops_promptly() -> None:
    clock = FakeClock()
    outage = _outage(clock, wait_slice_seconds=0.2)
    outage.on_failure(_unavailable())
    polls: list[float] = []

    def poll() -> bool:
        polls.append(clock.now)
        return True

    assert outage.wait(2.0, poll=poll, stop=Event(), pending=lambda: True) == wo.WAIT_ELAPSED
    assert max(b - a for a, b in zip(polls, polls[1:])) <= 0.2 + 1e-9
    assert polls[-1] - polls[0] == pytest.approx(2.0)

    stop = Event()
    clock.on_sleep.append(lambda now: stop.set() if len(clock.sleeps) >= 3 else None)
    assert outage.wait(30.0, poll=lambda: True, stop=stop, pending=lambda: True) == wo.WAIT_STOPPED
    assert outage.wait(30.0, poll=lambda: False, stop=Event(), pending=lambda: True) == wo.WAIT_POLL_FAILED
    assert outage.wait(30.0, poll=lambda: True, stop=Event(), pending=lambda: False) == wo.WAIT_DROPPED


def test_drain_abandon_is_a_lease_drain_abandon() -> None:
    from src.loader.edge_execution import LeaseDrainAbandoned

    assert issubclass(WriteOutageDrainAbandoned, LeaseDrainAbandoned)


# -- live stream edge loader --------------------------------------------------

def test_edge_outage_mid_flush_rolls_back_releases_leases_commits_nothing_and_retries_in_order(works_at, caplog) -> None:
    """Neo4j stops after one cell transaction of a flush committed. Every later attempt
    rolls back, the loader holds no lease while it waits, commits no offset, keeps
    polling with its partition paused, and after recovery rewrites the same batch in
    order: the graph is exact (last write wins per pair) and the offset commits once."""
    clock, metrics = FakeClock(), GraphLoaderMetrics()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_edge_messages(), clock=clock)
    outage = _outage(clock, metrics, error_after_seconds=30.0)
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=outage)
    consumer.assign_now(EDGE_TOPIC)
    held_while_waiting = _record_held_leases_while_waiting(clock, ledger, graph)
    commits_while_down: list = []
    clock.on_sleep.append(lambda _now: commits_while_down.append(len(consumer.commits)))
    gauge_seen: list[float] = []
    clock.on_sleep.append(lambda _now: gauge_seen.append(_gauge(metrics, "edge", "WORKS_AT")))
    graph.outage(stage="commit", after_commits=1)  # the 2nd cell transaction of the flush dies
    clock_start = clock.now
    clock.on_sleep.append(lambda now: graph.recover() if graph.down and now >= clock_start + 45 else None)

    with caplog.at_level(logging.INFO):
        assert loader.run(max_messages=len(EDGE_BATCH)) == 0
    coordinator.close()

    assert graph.failed_attempts >= 5  # 0.5+1+2+4+8+16+30 s of backoff spans the 45 s outage
    assert graph.edges() == _expected_edges()
    assert graph.history(("p-1", "c-1"))[-3:] == ["r1", "r2", "r3"]  # the final attempt, in order
    assert held_while_waiting
    _assert_fenced_not_released(held_while_waiting)
    assert ledger.denied_while_fenced >= 1  # a retry waited for the quarantine (a lease wait, not a lock wait)
    assert ledger.held == {}
    assert set(commits_while_down) == {0}  # no offset committed while Neo4j was down
    assert consumer.commits == [[(EDGE_TOPIC, 0, 16)]]  # once, past the whole batch
    assert graph.rollbacks == graph.failed_attempts
    assert consumer.pause_calls and consumer.pause_calls[0] == [(EDGE_TOPIC, 0)]
    assert max(gauge_seen) >= 30.0 and _gauge(metrics, "edge", "WORKS_AT") == 0.0
    messages = [r.getMessage() for r in caplog.records]
    assert any(r.levelno == logging.ERROR and "reason=neo4j_outage" in r.getMessage() for r in caplog.records)
    assert any("stage=write-outage-recovered" in m for m in messages)
    # every per-attempt failure is a WARNING: the only ERRORs are the sustained-outage lines
    assert all("reason=neo4j_outage" in r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR)


def test_edge_outage_polls_with_partitions_paused_and_never_rebalances(works_at) -> None:
    """While waiting the loader polls at least every WAIT_SLICE_SECONDS (far inside
    max.poll.interval.ms) with its partitions paused; a record prefetched before the
    pause is kept and written after the held batch, in order; partitions resume after."""
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(clock=clock)
    outage = _outage(clock)
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=outage)
    consumer.assign_now(EDGE_TOPIC)
    for message in _edge_messages()[:3]:
        assert loader._buffer_message(message) is True
    # one record is already prefetched; the paused poll still hands it over once
    prefetched = _edge_message("p-1", "c-1", "r9", 13)
    consumer.poll_hook = lambda: consumer.queue.append(prefetched) or consumer.paused.discard((EDGE_TOPIC, 0))
    graph.outage()
    polls_before = len(consumer.poll_times)
    clock.on_sleep.append(lambda now: graph.recover() if graph.down and len(clock.sleeps) >= 60 else None)

    assert loader._flush_batch() is True
    wait_polls = consumer.poll_times[polls_before:]
    assert len(wait_polls) >= 50
    assert max(b - a for a, b in zip(wait_polls, wait_polls[1:])) <= wo.WAIT_SLICE_SECONDS + 1e-9
    assert loader._rebalance_failure is False and loader._assigned_partitions == {(EDGE_TOPIC, 0)}
    assert consumer.resume_calls == [[(EDGE_TOPIC, 0)]]
    assert [m.offset() for m in loader._deferred_messages] == [13]  # deferred, not dropped
    assert consumer.commits == [[(EDGE_TOPIC, 0, 13)]]
    assert loader._flush_batch() is True  # nothing buffered: no-op
    assert loader._buffer_message(loader._deferred_messages.popleft()) is True
    assert loader._flush_batch() is True
    coordinator.close()
    assert graph.history(("p-1", "c-1"))[-1] == "r9"
    assert consumer.commits[-1] == [(EDGE_TOPIC, 0, 14)]


def test_edge_outage_during_lease_wait_neo4j_goes_down_while_waiting(works_at) -> None:
    """Neo4j stops while the loader is waiting for a lease (another holder has it):
    the grant arrives, the write fails and is ridden out like any other outage."""
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(clock=clock)
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=_outage(clock))
    consumer.assign_now(EDGE_TOPIC)
    for message in _edge_messages():
        loader._buffer_message(message)
    denials = {"n": 0}

    def busy_then_down(_ledger):
        denials["n"] += 1
        if denials["n"] <= 5:
            if denials["n"] == 5:
                graph.outage(stage="begin")
            return "deny"
        return None

    ledger.on_acquire = busy_then_down
    clock.on_sleep.append(lambda now: graph.recover() if graph.down and len(clock.sleeps) >= 20 else None)
    assert loader._flush_batch() is True
    coordinator.close()
    assert graph.failed_attempts >= 1
    assert graph.edges() == _expected_edges()
    assert ledger.held == {}
    assert consumer.commits == [[(EDGE_TOPIC, 0, 16)]]


def test_edge_lease_server_unavailable_during_lease_wait_is_ridden_out(works_at) -> None:
    """The lease server drops while the loader asks for leases and cannot be reached
    on reconnect: nothing was written without a lease, the flush is retried once it is back."""
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(clock=clock)
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=_outage(clock))
    consumer.assign_now(EDGE_TOPIC)
    for message in _edge_messages():
        loader._buffer_message(message)
    ledger.server_down = True
    clock.on_sleep.append(
        lambda now: setattr(ledger, "server_down", False) if len(clock.sleeps) >= 15 else None
    )
    assert loader._flush_batch() is True
    coordinator.close()
    assert graph.begins > 0 and graph.edges() == _expected_edges()
    assert consumer.commits == [[(EDGE_TOPIC, 0, 16)]]


def test_edge_sigterm_during_lease_wait_of_an_outage_retry_exits_zero_within_the_drain_budget(works_at, caplog) -> None:
    """After an outage, the retry waits for leases that are still quarantined. A
    SIGTERM then abandons it within the drain budget: exit 0, nothing committed."""
    clock = FakeClock()
    stop = Event()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_edge_messages(), clock=clock)
    loader, coordinator = _stream_edge_loader(
        works_at, graph, ledger, consumer, outage=_outage(clock), stop=stop, drain_budget=0.3,
    )
    consumer.assign_now(EDGE_TOPIC)
    graph.outage(stage="begin")

    def quarantined_after_recovery(now):
        if graph.down and len(clock.sleeps) >= 3:
            graph.recover()
            ledger.on_acquire = lambda _l: (stop.set(), "deny")[1]

    clock.on_sleep.append(quarantined_after_recovery)
    started = time.monotonic()
    with caplog.at_level(logging.WARNING):
        assert loader.run(max_messages=len(EDGE_BATCH)) == 0
    coordinator.close()
    assert time.monotonic() - started < 5.0
    assert consumer.commits == []
    assert graph.committed == []
    assert ledger.held == {}
    assert any("stage=edge-drain-abandoned" in r.getMessage() for r in caplog.records)


def test_edge_sigterm_during_an_outage_exits_zero_promptly_without_committing(works_at, caplog) -> None:
    """Real clock and sleep: Neo4j stays down; a SIGTERM during the backoff is honoured
    within one wait slice. Exit 0 (a drain abandon), no offset, no write committed."""
    stop = Event()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_edge_messages())
    outage = StreamWriteOutage(kind="edge", name="WORKS_AT", metrics=GraphLoaderMetrics())
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=outage, stop=stop)
    consumer.assign_now(EDGE_TOPIC)
    graph.outage(stage="begin")
    timer = threading.Timer(1.2, stop.set)
    timer.start()
    started = time.monotonic()
    with caplog.at_level(logging.WARNING):
        code = loader.run(max_messages=len(EDGE_BATCH))
    elapsed = time.monotonic() - started
    timer.cancel()
    coordinator.close()
    assert code == 0
    assert elapsed < 1.2 + 1.0  # one 0.2 s wait slice after the stop, plus a fast failing attempt
    assert graph.failed_attempts >= 2 and graph.committed == []
    assert consumer.commits == []
    assert ledger.held == {}
    assert any(
        "stage=edge-drain-abandoned" in r.getMessage() and "reason=neo4j_outage" in r.getMessage()
        for r in caplog.records
    )


def test_edge_sigterm_while_draining_into_an_outage_exits_zero(works_at) -> None:
    """The stop arrives first; the final drain flush then hits a down Neo4j: exit 0, no commit."""
    clock = FakeClock()
    stop = Event()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(clock=clock)
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=_outage(clock), stop=stop)
    consumer.assign_now(EDGE_TOPIC)
    for message in _edge_messages():
        loader._buffer_message(message)
    graph.outage()
    stop.set()
    assert loader.run() == 0
    coordinator.close()
    assert clock.sleeps == []  # no backoff once stopping
    assert consumer.commits == [] and graph.committed == []


@pytest.mark.parametrize("code", ["Neo.ClientError.Security.Unauthorized", "Neo.ClientError.Database.DatabaseNotFound"])
def test_edge_persistent_auth_or_missing_database_becomes_fatal(works_at, code, caplog) -> None:
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_edge_messages(), clock=clock)
    outage = _outage(clock, fatal_after_seconds=120.0)
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=outage)
    consumer.assign_now(EDGE_TOPIC)
    graph.outage(lambda: _neo_error(code, "denied"), stage="begin")
    with caplog.at_level(logging.WARNING):
        assert loader.run(max_messages=len(EDGE_BATCH)) == 1
    coordinator.close()
    assert sum(clock.sleeps) >= 120.0 - 30.0  # it waited out the bounded time first
    assert consumer.commits == [] and graph.committed == []
    assert ledger.held == {}
    assert any(r.levelno == logging.ERROR and "reason=neo4j_fatal" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("error", [
    lambda: _neo_error("Neo.ClientError.Schema.ConstraintValidationFailed"),
    lambda: _neo_error("Neo.ClientError.Statement.SyntaxError"),
    lambda: RuntimeError("unexpected"),
    lambda: _neo_error("Neo.ClientError.Transaction.TransactionTimedOut"),
    lambda: _neo_error("Neo.TransientError.General.MemoryPoolOutOfMemoryError"),
], ids=["row", "unknown-code", "unknown", "capacity", "server"])
def test_edge_row_unknown_capacity_and_server_errors_still_fail_as_before(works_at, error, caplog) -> None:
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_edge_messages(), clock=clock)
    outage = _outage(clock)
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=outage)
    consumer.assign_now(EDGE_TOPIC)
    graph.outage(error)
    with caplog.at_level(logging.ERROR):
        assert loader.run(max_messages=len(EDGE_BATCH)) == 1
    coordinator.close()
    assert clock.sleeps == [] and not outage.active
    assert consumer.commits == []
    assert any("Relationship write failed" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)


def test_edge_revoke_during_an_outage_hands_the_batch_over_uncommitted(works_at, caplog) -> None:
    """A rebalance during the outage never writes inside the callback: the unwritten
    batch is dropped uncommitted for the next owner and the loader carries on."""
    clock, metrics = FakeClock(), GraphLoaderMetrics()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(clock=clock)
    outage = _outage(clock, metrics)
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=outage)
    loader._running = True  # revoke-time settlement runs only inside run()
    consumer.assign_now(EDGE_TOPIC)
    for message in _edge_messages():
        loader._buffer_message(message)
    graph.outage()
    begins_before = graph.begins
    clock.on_sleep.append(
        lambda now: setattr(consumer, "poll_hook", lambda: consumer.revoke_now(EDGE_TOPIC))
        if len(clock.sleeps) == 3 else None
    )
    with caplog.at_level(logging.WARNING):
        assert loader._flush_batch() is True
    coordinator.close()
    assert graph.begins == begins_before + 1  # the first failed attempt only: none inside the callback
    assert loader._batch == [] and loader._partition_ledgers == {}
    assert loader._rebalance_failure is False
    assert consumer.commits == []
    # S2: the outage is not over: its clock and gauge carry on, only the backoff restarts
    assert outage.active and outage.failures == 0 and _gauge(metrics, "edge", "WORKS_AT") > 0
    assert any("reason=neo4j_outage revoked during a Neo4j outage" in r.getMessage() for r in caplog.records)


def test_repeated_revokes_during_a_long_outage_never_reset_its_clock_gauge_or_error(caplog) -> None:
    """S2: a rebalance every minute during a 10-minute outage must not keep the gauge
    below the alert threshold or silence the sustained ERROR; a successful write ends it."""
    clock, metrics = FakeClock(), GraphLoaderMetrics()
    outage = _outage(clock, metrics, error_after_seconds=30.0, error_interval_seconds=300.0)
    with caplog.at_level(logging.WARNING):
        outage.on_failure(_unavailable())
        for _minute in range(10):
            for _ in range(6):
                clock.now += 10
                outage.on_failure(_unavailable())
            outage.abandon("partitions revoked")
            assert outage.failures == 0  # only the backoff restarts
    assert outage.outage_seconds == pytest.approx(600.0)
    assert _gauge(metrics, "edge", "WORKS_AT") == pytest.approx(600.0)  # above the 120 s alert threshold
    errors = [r for r in caplog.records if r.levelno == logging.ERROR and "reason=neo4j_outage" in r.getMessage()]
    assert len(errors) == 2  # at 30 s and 330 s: the cadence survived the revokes
    assert outage.on_failure(_unavailable()).delay_seconds == 0.5  # the backoff restarted at the last abandon
    outage.recovered()
    assert not outage.active and _gauge(metrics, "edge", "WORKS_AT") == 0.0


@pytest.mark.parametrize("code", [
    "Neo.TransientError.General.OutOfMemoryError", "Neo.TransientError.General.StackOverFlowError",
])
def test_oom_and_stack_overflow_are_not_ridden_out_anywhere(works_at, code, caplog) -> None:
    """S3 follow-up: rides_out() agrees with on_failure(), so the final failure line is an
    ERROR and, inside a revoke callback, the loader fails closed instead of handing over."""
    assert StreamWriteOutage.rides_out(_neo_error(code)) is False
    assert StreamWriteOutage.rides_out(_unavailable()) is True
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(clock=clock)
    loader, coordinator = _stream_edge_loader(works_at, graph, ledger, consumer, outage=_outage(clock))
    loader._running = True
    consumer.assign_now(EDGE_TOPIC)
    for message in _edge_messages():
        loader._buffer_message(message)
    graph.outage(lambda: _neo_error(code), stage="begin")
    with caplog.at_level(logging.WARNING):
        consumer.revoke_now(EDGE_TOPIC)  # the settlement write inside the callback hits it
    coordinator.close()
    assert loader._rebalance_failure is True  # failed closed, not handed over
    assert len(loader._batch) == len(EDGE_BATCH) and clock.sleeps == []
    assert consumer.commits == []
    assert any(r.levelno == logging.ERROR and "Relationship write failed" in r.getMessage() for r in caplog.records)
    assert not any("handed" in r.getMessage() for r in caplog.records)


def test_gauge_keeps_rising_while_an_attempt_blocks() -> None:
    """The gauge is computed at scrape time: it does not freeze while a loader is blocked
    inside one long attempt (a lease quarantine wait, a driver connect timeout)."""
    clock, metrics = FakeClock(), GraphLoaderMetrics()
    outage = _outage(clock, metrics)
    outage.on_failure(_unavailable())
    clock.now += 37.0  # no refresh, no wait: an attempt is blocked
    assert _gauge(metrics, "edge", "WORKS_AT") == pytest.approx(37.0)


def test_edge_loader_refuses_the_ride_out_with_static_assignment_or_slot_gating(works_at) -> None:
    outage = _outage(FakeClock())
    with pytest.raises(ValueError, match="unsliced stream loaders only"):
        EdgeLoader(MagicMock(), MagicMock(), works_at, static_assignment=(0, 1), write_outage=outage)
    with pytest.raises(ValueError, match="unsliced stream loaders only"):
        EdgeLoader(
            MagicMock(), MagicMock(), works_at, write_outage=outage, slot_admission=MagicMock(),
            slot_buffer=MagicMock(), coordination_poll=MagicMock(), wall_clock_ms=lambda: 0,
        )


# -- live stream node loader --------------------------------------------------

NODE_BATCH = [("p-1", "a"), ("p-2", "b"), ("p-1", "c"), ("p-3", "d"), ("p-1", "e")]


def _node_messages():
    return [_node_message(key, name, 20 + i) for i, (key, name) in enumerate(NODE_BATCH)]


def test_node_outage_mid_flush_releases_leases_commits_nothing_and_retries_in_order(person, caplog) -> None:
    clock, metrics = FakeClock(), GraphLoaderMetrics()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_node_messages(), clock=clock)
    outage = _outage(clock, metrics, kind="node", name="Person", error_after_seconds=30.0)
    loader = _stream_node_loader(person, graph, ledger, consumer, outage=outage)
    consumer.assign_now(NODE_TOPIC)
    held_while_waiting = _record_held_leases_while_waiting(clock, ledger, graph)
    commits_while_down: list = []
    clock.on_sleep.append(lambda _now: commits_while_down.append(len(consumer.commits)))
    graph.outage(stage="commit", after_commits=1)
    clock.on_sleep.append(lambda now: graph.recover() if graph.down and sum(clock.sleeps) >= 45 else None)

    with caplog.at_level(logging.INFO):
        assert loader.run(max_messages=len(NODE_BATCH)) == 0
    loader._writer.close()

    assert graph.failed_attempts >= 5
    assert graph.nodes() == {"p-1": {"personId": "p-1", "name": "e"}, "p-2": {"personId": "p-2", "name": "b"},
                             "p-3": {"personId": "p-3", "name": "d"}}
    assert held_while_waiting
    _assert_fenced_not_released(held_while_waiting)
    assert ledger.denied_while_fenced >= 1
    assert set(commits_while_down) == {0}
    assert consumer.commits == [[(NODE_TOPIC, 0, 25)]]
    assert consumer.pause_calls and consumer.resume_calls
    assert _gauge(metrics, "node", "Person") == 0.0
    assert any(r.levelno == logging.ERROR and "reason=neo4j_outage" in r.getMessage() for r in caplog.records)
    assert all("reason=neo4j_outage" in r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR)


def test_node_outage_polls_with_partitions_paused(person) -> None:
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(clock=clock)
    loader = _stream_node_loader(person, graph, ledger, consumer, outage=_outage(clock, kind="node", name="Person"))
    consumer.assign_now(NODE_TOPIC)
    for message in _node_messages():
        loader._buffer_message(message)
    graph.outage()
    polls_before = len(consumer.poll_times)
    clock.on_sleep.append(lambda now: graph.recover() if graph.down and len(clock.sleeps) >= 40 else None)
    assert loader._flush_batch() is True
    loader._writer.close()
    wait_polls = consumer.poll_times[polls_before:]
    assert len(wait_polls) >= 30
    assert max(b - a for a, b in zip(wait_polls, wait_polls[1:])) <= wo.WAIT_SLICE_SECONDS + 1e-9
    assert consumer.pause_calls[0] == [(NODE_TOPIC, 0)]
    assert loader._rebalance_failure is False
    assert consumer.commits == [[(NODE_TOPIC, 0, 25)]]


def test_node_sigterm_during_an_outage_exits_zero_without_committing(person, caplog) -> None:
    stop = Event()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_node_messages())
    outage = StreamWriteOutage(kind="node", name="Person", metrics=GraphLoaderMetrics())
    loader = _stream_node_loader(person, graph, ledger, consumer, outage=outage, stop=stop)
    consumer.assign_now(NODE_TOPIC)
    graph.outage(stage="begin")
    timer = threading.Timer(1.2, stop.set)
    timer.start()
    started = time.monotonic()
    with caplog.at_level(logging.WARNING):
        code = loader.run(max_messages=len(NODE_BATCH))
    elapsed = time.monotonic() - started
    timer.cancel()
    loader._writer.close()
    assert code == 0
    assert elapsed < 1.2 + 1.0
    assert consumer.commits == [] and graph.committed == []
    assert ledger.held == {}
    assert any(
        "stage=node-drain-abandoned" in r.getMessage() and "reason=neo4j_outage" in r.getMessage()
        for r in caplog.records
    )


def test_node_sigterm_during_lease_wait_of_an_outage_retry_exits_zero(person) -> None:
    clock = FakeClock()
    stop = Event()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_node_messages(), clock=clock)
    loader = _stream_node_loader(
        person, graph, ledger, consumer, outage=_outage(clock, kind="node", name="Person"),
        stop=stop, drain_budget=0.3,
    )
    consumer.assign_now(NODE_TOPIC)
    graph.outage(stage="begin")

    def quarantined_after_recovery(now):
        if graph.down and len(clock.sleeps) >= 3:
            graph.recover()
            ledger.on_acquire = lambda _l: (stop.set(), "deny")[1]

    clock.on_sleep.append(quarantined_after_recovery)
    assert loader.run(max_messages=len(NODE_BATCH)) == 0
    loader._writer.close()
    assert consumer.commits == [] and graph.committed == []


@pytest.mark.parametrize("code", ["Neo.ClientError.Security.Unauthorized", "Neo.ClientError.Database.DatabaseNotFound"])
def test_node_persistent_auth_or_missing_database_becomes_fatal(person, code, caplog) -> None:
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_node_messages(), clock=clock)
    outage = _outage(clock, kind="node", name="Person", fatal_after_seconds=120.0)
    loader = _stream_node_loader(person, graph, ledger, consumer, outage=outage)
    consumer.assign_now(NODE_TOPIC)
    graph.outage(lambda: _neo_error(code, "denied"), stage="begin")
    with caplog.at_level(logging.WARNING):
        assert loader.run(max_messages=len(NODE_BATCH)) == 1
    loader._writer.close()
    assert sum(clock.sleeps) >= 120.0 - 30.0
    assert consumer.commits == [] and graph.committed == []
    assert any(r.levelno == logging.ERROR and "reason=neo4j_fatal" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("error", [
    lambda: _neo_error("Neo.ClientError.Schema.ConstraintValidationFailed"),
    lambda: RuntimeError("unexpected"),
    lambda: _neo_error("Neo.ClientError.Transaction.TransactionTimedOut"),
], ids=["row", "unknown", "capacity"])
def test_node_row_unknown_and_capacity_errors_still_fail_as_before(person, error, caplog) -> None:
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(_node_messages(), clock=clock)
    outage = _outage(clock, kind="node", name="Person")
    loader = _stream_node_loader(person, graph, ledger, consumer, outage=outage)
    consumer.assign_now(NODE_TOPIC)
    graph.outage(error)
    with caplog.at_level(logging.ERROR):
        assert loader.run(max_messages=len(NODE_BATCH)) == 1
    loader._writer.close()
    assert clock.sleeps == [] and not outage.active
    assert consumer.commits == []
    assert any("Node batch write failed" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)


def test_node_revoke_during_an_outage_hands_the_batch_over_uncommitted(person) -> None:
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(clock=clock)
    outage = _outage(clock, kind="node", name="Person")
    loader = _stream_node_loader(person, graph, ledger, consumer, outage=outage)
    loader._running = True
    consumer.assign_now(NODE_TOPIC)
    for message in _node_messages():
        loader._buffer_message(message)
    graph.outage()
    begins_before = graph.begins
    clock.on_sleep.append(
        lambda now: setattr(consumer, "poll_hook", lambda: consumer.revoke_now(NODE_TOPIC))
        if len(clock.sleeps) == 3 else None
    )
    assert loader._flush_batch() is True
    loader._writer.close()
    assert graph.begins == begins_before + 1
    assert loader._batch == [] and loader._partition_ledgers == {}
    assert loader._rebalance_failure is False
    assert consumer.commits == []
    assert outage.active and outage.failures == 0  # S2: the outage clock keeps running


# -- BULK is untouched: no ride-out installed, connectivity errors fail fast as before

def _edge_main_loader_kwargs(schema, argv, environ=None):
    consumer, driver, loader = MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.Producer", return_value=MagicMock()), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.build_edge_execution",
               return_value=(MagicMock(), MagicMock(), MagicMock(), MagicMock())), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader) as loader_cls, \
         patch.dict("os.environ", environ or {}):
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--max-messages", "1", *argv]) == 0
    return loader_cls.call_args.kwargs


@pytest.mark.parametrize("argv, environ", [
    (["--mode", "bulk"], {}),  # bulk classic
    (["--mode", "bulk"], {"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7700"}),  # bulk --edge-leases
    (["--mode", "bulk", "--replica-count", "2", "--replica-id", "1"], {}),  # bulk static replicas
], ids=["bulk-classic", "bulk-edge-leases", "bulk-static-replicas"])
def test_bulk_edge_loader_main_installs_no_ride_out(schema, argv, environ) -> None:
    kwargs = _edge_main_loader_kwargs(schema, argv, environ)
    assert kwargs["write_outage"] is None


def test_stream_edge_loader_main_installs_the_ride_out_only_for_the_lease_fleet(schema) -> None:
    kwargs = _edge_main_loader_kwargs(
        schema, ["--mode", "stream", "--run-id", "r1"], {"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7700"},
    )
    assert isinstance(kwargs["write_outage"], StreamWriteOutage)


def test_unleased_stream_edge_loader_installs_no_ride_out(schema, monkeypatch) -> None:
    """S1: without a bucket-lease address (the --stream-clock-rotation isolated
    self-reference phase, or a direct unleased stream run) the writer has no
    transaction timeout T and no fencing, so it keeps failing fast."""
    monkeypatch.delenv("GRAPH_LOADER_BUCKET_LEASE_ADDR", raising=False)
    monkeypatch.delenv("GRAPH_LOADER_LEASE_ADDR", raising=False)
    kwargs = _edge_main_loader_kwargs(schema, ["--mode", "stream", "--run-id", "r1"])
    assert kwargs["write_outage"] is None


def test_deprecated_slot_gated_stream_path_installs_no_ride_out(schema) -> None:
    kwargs = _edge_main_loader_kwargs(schema, [
        "--mode", "stream", "--slot-gating", "--run-id", "r1",
        "--coordination-topic", schema.loading.coordination.topic, "--fleet-edge-types", "WORKS_AT",
    ], {"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7700"})
    assert kwargs["write_outage"] is None


def test_stream_edge_main_wraps_the_dlq_sink_and_the_writers_index_with_one_memory(schema) -> None:
    """M1: build_edge_execution gets the real DLQ sink and the parked-pair index, each wrapped by
    the held batch's DLQ memory (one memory for both), so a row an earlier attempt already
    dead-lettered skips both its index re-add and its publish. The admission barrier keeps the
    plain index."""
    from src.loader.dead_letter import DeadLetterRecordSink
    from src.models.schema import DeadLetterConfig

    dlq_schema = schema.model_copy(deep=True)
    index = next(i for i, edge in enumerate(dlq_schema.edges) if edge.type == "WORKS_AT")
    dlq_schema.edges[index] = dlq_schema.edges[index].model_copy(
        update={"dead_letter": DeadLetterConfig(enabled=True, topic="dlq-works-at")}
    )
    real_sink = MagicMock(spec=DeadLetterRecordSink)
    loader = MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=MagicMock()), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=MagicMock()), \
         patch("src.loader.edge_loader.load_schema", return_value=dlq_schema), \
         patch("src.loader.edge_loader.build_malformed_record_sink", return_value=(real_sink, MagicMock())), \
         patch("src.loader.edge_loader.ParkedPairIndex") as index_cls, \
         patch("src.loader.edge_loader.warn_index_leftovers"), \
         patch("src.loader.edge_loader.build_edge_execution",
               return_value=(MagicMock(), MagicMock(), MagicMock(), MagicMock())) as build, \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader) as loader_cls, \
         patch.dict("os.environ", {"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7700"}):
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--mode", "stream", "--max-messages", "1"]) == 0
    sink, index = build.call_args.kwargs["missing_endpoint_sink"], build.call_args.kwargs["parked_pair_index"]
    assert isinstance(sink, wo._DlqAckRecordingSink) and sink._sink is real_sink
    assert isinstance(index, wo._DlqAckAwareIndex) and index._index is index_cls.return_value
    assert sink._memory is index._memory
    assert loader_cls.call_args.kwargs["admission_index"] is index_cls.return_value
    assert isinstance(loader_cls.call_args.kwargs["write_outage"], StreamWriteOutage)


def _bulk_edge_loader(works_at, graph, *, environ=None, ledger=None):
    """A bulk EdgeLoader exactly as edge_loader.main builds one: no write_outage."""
    writer, partitioner, lane_batcher, coordinator = build_edge_execution(
        works_at, graph, environ=environ or {}, mode="bulk",
        lease_client_factory=None if ledger is None else ledger.client,
    )
    consumer = FakeConsumer(_edge_messages())
    loader = EdgeLoader(
        consumer, writer, works_at, rejection_sink=MagicMock(), partitioner=partitioner,
        lane_batcher=lane_batcher, coordinator=coordinator,
        loading_config=LoadingConfig(unwind_batch_size=100, flush_interval_ms=60_000),
    )
    return loader, consumer, coordinator


@pytest.mark.parametrize("path", ["bulk-classic", "bulk-edge-leases"])
def test_bulk_edge_connectivity_error_fails_fast_exactly_as_before(works_at, path, caplog) -> None:
    graph = FakeGraph()
    ledger = LeaseLedger() if path == "bulk-edge-leases" else None
    environ = {"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7700"} if ledger else None
    loader, consumer, coordinator = _bulk_edge_loader(works_at, graph, environ=environ, ledger=ledger)
    assert loader._write_outage is None
    assert isinstance(coordinator, BucketLeaseExecutionCoordinator if ledger else LaneExecutionCoordinator)
    graph.outage(stage="begin")
    consumer.assign_now(EDGE_TOPIC)
    started = time.monotonic()
    with caplog.at_level(logging.ERROR):
        assert loader.run(max_messages=len(EDGE_BATCH)) == 1
    assert time.monotonic() - started < 5.0  # no wait, no hang
    if ledger:
        coordinator.close()
    assert consumer.commits == [] and graph.committed == []
    assert consumer.resume_calls == []  # never paused-and-resumed for an outage
    # one write attempt only (the lease path stops at the first failed cell)
    assert graph.failed_attempts <= (len(EDGE_BATCH) if not ledger else works_at.execution.worker_count)
    assert any("Relationship write failed" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
    assert not any("stage=write-outage" in r.getMessage() for r in caplog.records)


def test_bulk_static_replica_loader_has_no_ride_out_and_fails_fast(works_at, caplog) -> None:
    graph = FakeGraph()
    writer, partitioner, lane_batcher, coordinator = build_edge_execution(works_at, graph, environ={}, mode="bulk")
    consumer = FakeConsumer(_edge_messages())
    consumer.list_topics = MagicMock(return_value=MagicMock(
        topics={EDGE_TOPIC: MagicMock(error=None, partitions={0: None})}))
    consumer.committed = MagicMock(return_value=[TopicPartition(EDGE_TOPIC, 0, -1001)])
    loader = EdgeLoader(
        consumer, writer, works_at, rejection_sink=MagicMock(), partitioner=partitioner,
        lane_batcher=lane_batcher, coordinator=coordinator, static_assignment=(0, 1),
        loading_config=LoadingConfig(unwind_batch_size=100, flush_interval_ms=60_000),
    )
    assert loader._write_outage is None
    graph.outage(stage="begin")
    with caplog.at_level(logging.ERROR):
        assert loader.run(max_messages=len(EDGE_BATCH)) == 1
    assert consumer.commits == []


def test_node_loader_main_installs_the_ride_out_only_for_a_leased_stream_writer(schema) -> None:
    """Bulk never gets it. S1: a stream node loader gets it only with a LeasedNodeWriter;
    a plain NodeWriter (a nodes-only stream run or --stream-clock-rotation: no lease
    server, so no transaction timeout and no fencing) keeps failing fast."""
    def run(mode, leased):
        consumer, driver, loader, producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
        loader.run.return_value = 0
        with patch("src.loader.node_loader.Consumer", return_value=consumer), \
             patch("src.loader.node_loader.Producer", return_value=producer), \
             patch("src.loader.node_loader.get_neo4j_driver", return_value=driver), \
             patch("src.loader.node_loader.load_schema", return_value=schema), \
             patch("src.loader.node_loader.build_leased_node_writer", return_value=leased), \
             patch("src.loader.node_loader.NodeLoader", return_value=loader) as node_loader_cls:
            assert node_loader_main(["--node-label", "Person", "--mode", mode, "--max-messages", "1"]) == 0
        return node_loader_cls.call_args.kwargs

    bulk = run("bulk", MagicMock(spec=LeasedNodeWriter))
    assert bulk["write_outage"] is None
    unleased = run("stream", None)
    assert isinstance(unleased["writer"], NodeWriter) and unleased["write_outage"] is None
    leased = run("stream", MagicMock(spec=LeasedNodeWriter))
    assert isinstance(leased["write_outage"], StreamWriteOutage)


def test_bulk_node_connectivity_error_keeps_its_bounded_retry_then_fails(person, caplog) -> None:
    graph = FakeGraph()
    consumer = FakeConsumer(_node_messages())
    producer = MagicMock()
    producer.flush.return_value = 0
    sleeps: list[float] = []
    loader = NodeLoader(
        consumer, NodeWriter(graph, person), person, producer=producer,
        loading_config=LoadingConfig(
            unwind_batch_size=100, flush_interval_ms=60_000, retry_max_attempts=3, retry_base_delay_ms=1,
        ),
        sleeper=sleeps.append,
    )
    assert loader._write_outage is None
    consumer.assign_now(NODE_TOPIC)
    graph.outage(stage="begin")
    with caplog.at_level(logging.ERROR):
        assert loader.run(max_messages=len(NODE_BATCH)) == 1
    # ServiceUnavailable is a driver error, not a retryable Neo4jError: one attempt, then fail, as before
    assert graph.failed_attempts == 1
    assert consumer.commits == []
    assert any("Node batch write failed" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
    assert not any("stage=write-outage" in r.getMessage() for r in caplog.records)


def test_planned_writer_connectivity_error_still_fails_closed() -> None:
    from src.loader.planned_writer import PlannedEdgeWriterWorker
    from tests.test_planned_writer import FakeClient, FakeFetcher, _rows_in_one_cell, make_plan

    plan, _ = make_plan([_rows_in_one_cell(2)])
    writer = MagicMock()
    writer.write_rows.side_effect = _unavailable()
    client = FakeClient([(0, None)])
    worker = PlannedEdgeWriterWorker(plan, client, FakeFetcher({0: [{}, {}]}), {"ACTED_IN": writer}, writer_id="w0")
    assert worker.run() == 1
    assert writer.write_rows.call_count == 1
    assert client.done_calls == [] and client.fail_calls and "ServiceUnavailable" in client.fail_calls[0][1]


def test_the_ride_out_is_reachable_only_from_the_stream_entrypoints() -> None:
    """Static proof for every bulk path: only edge_loader.py and node_loader.py import the
    ride-out module (edge_loader at module level, so the bulk edge process imports it but
    never constructs it), and each constructs it in exactly one place, gated on --mode
    stream with a leased writer. The planner, dispatcher, planned writers, bulk monitors
    and the CLI never reference it."""
    src = ROOT / "src"
    importers = sorted(
        str(path.relative_to(ROOT)) for path in src.rglob("*.py")
        if path.name != "write_outage.py" and "write_outage" in path.read_text()
        and ("from src.loader.write_outage import" in path.read_text() or "import write_outage" in path.read_text())
    )
    assert importers == ["src/loader/edge_loader.py", "src/loader/node_loader.py"]
    for name in ("planned_writer.py", "edge_planner.py", "planned_dispatcher.py", "planned_edges.py",
                 "bulk_monitor.py", "relationship_bulk_monitor.py", "cli.py"):
        for path in src.rglob(name):
            assert "write_outage" not in path.read_text() and "StreamWriteOutage" not in path.read_text(), path
    for module, gate in (
        ("edge_loader.py", 'args.mode == "stream" and not args.slot_gating and lease_mode_settings(os.environ) is not None'),
        ("node_loader.py", 'args.mode == "stream" and isinstance(node_writer, LeasedNodeWriter)'),
    ):
        tree = ast.parse((src / "loader" / module).read_text())
        constructions = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "StreamWriteOutage"
        ]
        assert len(constructions) == 1, module
        assert gate in (src / "loader" / module).read_text()


# -- DLQ copies of a retried batch (review S4, then M1) ----------------------------

class _FakeDurableIndex:
    """The parked-pair index as the live writer sees it: idempotent add, is_blocked by pair,
    and remove (what the retry consumer does once it resolves a copy)."""

    def __init__(self) -> None:
        self.adds: list[tuple] = []
        self.entries: dict[tuple, tuple] = {}  # record identity -> pair
        self.snapshot_calls = 0
        self.ready = False

    def is_blocked(self, source_key, target_key) -> bool:
        return (source_key, target_key) in self.entries.values()

    def add(self, source_key, target_key, *, original_topic, original_partition, original_offset,
            first_failure_at_ms) -> None:
        identity = (original_topic, original_partition, original_offset)
        self.adds.append(identity)
        self.entries[identity] = (source_key, target_key)

    def remove(self, identity: tuple) -> None:
        self.entries.pop(identity, None)

    # admission barrier protocol (v7-S4)
    def watermark_snapshot(self):
        self.snapshot_calls += 1
        return {"snapshot": self.snapshot_calls}

    def caught_up(self, _snapshot) -> bool:
        return self.ready


def _row_level_stream_loader(works_at, graph, ledger, consumer, clock, *, admission_index=None):
    """The DLQ-enabled stream lease path as edge_loader.main and build_edge_execution build it:
    IndexedMissingEndpointSink over the memory-wrapped DLQ sink and the memory-wrapped index."""
    from src.loader.edge_execution import IndexedMissingEndpointSink

    outage = _outage(clock)
    inner = MagicMock()
    index = _FakeDurableIndex()
    memory = outage.remember_dlq_acks()
    write_index = memory.wrap_index(index)
    sink = IndexedMissingEndpointSink(memory.wrap_sink(inner), write_index)
    writer = ApocLockedEdgeWriter(
        graph, works_at, explicit_locks=False, sort_rows=False, sleep=lambda _s: None,
        missing_endpoint_error_factory=MissingRelationshipEndpointError, missing_endpoint_sink=sink,
    )
    coordinator = BucketLeaseExecutionCoordinator(
        writer, router=BucketCellRouter("Person", "Company", 1), worker_count=1, batch_size=2,
        merge_target_rows=0, client_factory=ledger.client, parked_pair_index=write_index,
    )
    coordinator.prime()
    loader = EdgeLoader(
        consumer, writer, works_at, rejection_sink=MagicMock(), coordinator=coordinator,
        loading_config=LoadingConfig(unwind_batch_size=100, flush_interval_ms=60_000),
        write_outage=outage, run_id="r1", admission_index=admission_index,
    )
    return loader, coordinator, outage, inner, index


def _route_kwargs(offset: int, source: str = "p", target: str = "c") -> dict:
    from types import SimpleNamespace

    return dict(
        topic=EDGE_TOPIC, partition=0, offset=offset,
        record=SimpleNamespace(source_key=source, target_key=target), edge_type="WORKS_AT",
        source_key_property="personId", target_key_property="companyId", failure_class="missing_endpoint",
        reason_code="blocked_by_earlier_unresolved_row", error_text="x", retry_after_seconds=1.0,
    )


def _wired(memory_outage=None):
    """IndexedMissingEndpointSink over a memory-wrapped DLQ and index, as edge_loader.main wires it."""
    from src.loader.edge_execution import IndexedMissingEndpointSink

    outage = memory_outage or _outage(FakeClock())
    memory = outage.remember_dlq_acks()
    dlq, index = MagicMock(), _FakeDurableIndex()
    return IndexedMissingEndpointSink(memory.wrap_sink(dlq), memory.wrap_index(index)), dlq, index, outage


def _published(dlq) -> list:
    return [call.kwargs["offset"] for call in dlq.route_unresolved_endpoint.call_args_list]


def test_m1_probe_a_rerouted_row_whose_copy_was_resolved_is_neither_readded_nor_orphaned() -> None:
    """The reviewer's probe as a test: attempt 1 dead-letters r1 and r2 of one pair; the retry
    consumer resolves r1 and removes its entry; attempt 2 routes r1 again (r2 still blocks the
    pair). r1 must not come back: no re-add (which would orphan its entry, or resurrect it with a
    new copy), no second copy. r2's entry still has its one outstanding copy."""
    sink, dlq, index, _outage_state = _wired()
    sink.route_unresolved_endpoint(**_route_kwargs(10))
    sink.route_unresolved_endpoint(**_route_kwargs(11))
    index.remove((EDGE_TOPIC, 0, 10))  # the retry consumer wrote r1
    sink.route_unresolved_endpoint(**_route_kwargs(10))  # attempt 2
    sink.route_unresolved_endpoint(**_route_kwargs(11))
    assert (EDGE_TOPIC, 0, 10) not in index.entries  # no orphan, no resurrection
    assert set(index.entries) == {(EDGE_TOPIC, 0, 11)}
    assert _published(dlq) == [10, 11]
    assert index.adds == [(EDGE_TOPIC, 0, 10), (EDGE_TOPIC, 0, 11)]


@pytest.mark.parametrize("with_memory", [True, False], ids=["memory", "e9d60a7-wiring"])
def test_m1_the_docker_interleaving_applies_the_newer_row_last_only_with_the_memory(with_memory) -> None:
    """rk-ride3 ran e9d60a7, which re-added and republished. The retry consumer wrote r1's
    copy. The live retry then re-added r1 and r2 with new copies. The consumer wrote r2 and
    removed its entry. r1's new copy was still indexed, so it was applied last: old over
    new (48 WORKS_AT pairs). A retry-consumer stand-in applies a copy only while its row is
    indexed (M12). Without the memory the old row wins; with it the newer row is last."""
    from src.loader.edge_execution import IndexedMissingEndpointSink

    if with_memory:
        sink, dlq, index, _outage_state = _wired()
    else:
        dlq, index = MagicMock(), _FakeDurableIndex()
        sink = IndexedMissingEndpointSink(dlq, index)
    applied: list[int] = []

    def consume(copy_index: int) -> None:
        offset = _published(dlq)[copy_index]
        if (EDGE_TOPIC, 0, offset) in index.entries:
            applied.append(offset)
            index.remove((EDGE_TOPIC, 0, offset))

    sink.route_unresolved_endpoint(**_route_kwargs(10))  # attempt 1: r1 (older)
    sink.route_unresolved_endpoint(**_route_kwargs(11))  # attempt 1: r2 (newer)
    consume(0)  # the consumer writes r1's copy
    sink.route_unresolved_endpoint(**_route_kwargs(10))  # the live retry attempt routes both again
    sink.route_unresolved_endpoint(**_route_kwargs(11))
    for copy_index in range(1, len(_published(dlq))):
        consume(copy_index)
    if with_memory:
        assert applied == [10, 11] and _published(dlq) == [10, 11]
    else:
        assert applied[-1] == 10  # the reproduced bug: the older row is applied last
    assert index.entries == {}


def test_m1_a_row_whose_earlier_publish_failed_or_never_happened_is_routed_in_full() -> None:
    """The memory only remembers acknowledged publishes: a failed publish is retried in full."""
    sink, dlq, index, _outage_state = _wired()
    dlq.route_unresolved_endpoint.side_effect = [RuntimeError("DLQ publish failed"), None]
    with pytest.raises(RuntimeError):
        sink.route_unresolved_endpoint(**_route_kwargs(10))
    sink.route_unresolved_endpoint(**_route_kwargs(10))
    assert _published(dlq) == [10, 10]
    assert index.adds == [(EDGE_TOPIC, 0, 10), (EDGE_TOPIC, 0, 10)]


def test_m1_memory_covers_scoped_multi_pair_views_and_is_forgotten_per_batch() -> None:
    outage = _outage(FakeClock())
    memory = outage.remember_dlq_acks()
    scoped_index = MagicMock()
    base = MagicMock()
    base.scoped.return_value = scoped_index
    wrapped = memory.wrap_index(base).scoped(("Person", "Post"))
    kwargs = dict(original_topic=EDGE_TOPIC, original_partition=0, original_offset=10, first_failure_at_ms=1)
    sink = memory.wrap_sink(MagicMock())
    wrapped.add("p", "c", **kwargs)
    sink.route_unresolved_endpoint(topic=EDGE_TOPIC, partition=0, offset=10)
    wrapped.add("p", "c", **kwargs)  # a retry: skipped
    assert scoped_index.add.call_count == 1 and memory.skipped_adds == 1
    wrapped.is_blocked("p", "c")  # everything else is the index's own
    scoped_index.is_blocked.assert_called_once_with("p", "c")
    outage.recovered()  # the batch committed: forgotten
    wrapped.add("p", "c", **kwargs)
    assert scoped_index.add.call_count == 2


def test_a_retried_row_level_dlq_batch_neither_republishes_nor_resurrects(works_at) -> None:
    """Through the real loader, lease coordinator and writer: transaction 1 commits and
    dead-letters both rows of the missing pair; transaction 2 dies; between attempts the
    retry consumer resolves r1. The retries route both rows again (still blocked) but neither
    re-adds nor republishes them: one copy each, r1 is not resurrected, r2 keeps its copy."""
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    graph.missing_sources = {"p-missing"}
    consumer = FakeConsumer(clock=clock)
    loader, coordinator, outage, inner, index = _row_level_stream_loader(works_at, graph, ledger, consumer, clock)
    consumer.assign_now(EDGE_TOPIC)
    rows = [("p-missing", "c-1", "r1"), ("p-missing", "c-1", "r2"), ("p-3", "c-1", "r1"), ("p-4", "c-3", "r1")]
    for i, (p, c, r) in enumerate(rows):
        assert loader._buffer_message(_edge_message(p, c, r, 10 + i)) is True
    graph.outage(stage="commit", after_commits=1)  # transaction 1 ([r1, r2] of the missing pair) commits, then down

    def resolve_first_copy_then_recover(now):
        if len(clock.sleeps) == 2:
            index.remove((EDGE_TOPIC, 0, 10))  # the retry consumer wrote r1's copy
        if graph.down and len(clock.sleeps) >= 25:
            graph.recover()

    clock.on_sleep.append(resolve_first_copy_then_recover)
    memory = outage.remember_dlq_acks()
    assert loader._flush_batch() is True
    coordinator.close()
    assert graph.failed_attempts >= 2
    assert _published(inner) == [10, 11]  # one copy each, however many attempts re-routed them
    assert index.adds == [(EDGE_TOPIC, 0, 10), (EDGE_TOPIC, 0, 11)]
    assert set(index.entries) == {(EDGE_TOPIC, 0, 11)}  # r1 stays resolved; r2's entry has its copy
    assert memory.skipped_adds >= 2 and memory.skipped_publishes >= 2
    assert ("p-missing", "c-1") not in graph.edges() and graph.edges()[("p-4", "c-3")] == {"role": "r1"}
    assert consumer.commits == [[(EDGE_TOPIC, 0, 14)]]
    assert memory.acked((EDGE_TOPIC, 0, 10)) is False  # forgotten once the batch committed


def test_a_hard_outage_publishes_no_extra_dlq_copies(works_at) -> None:
    """DLQ copies are published only after a cell commits, so a retry adds copies only when an
    earlier attempt partly committed. With Neo4j down from the first transaction, the batch's
    unresolved row is published exactly once, by the attempt that finally succeeds."""
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    graph.missing_sources = {"p-missing"}
    consumer = FakeConsumer(clock=clock)
    loader, coordinator, _outage_state, inner, index = _row_level_stream_loader(works_at, graph, ledger, consumer, clock)
    consumer.assign_now(EDGE_TOPIC)
    for i, (p, c, r) in enumerate([("p-missing", "c-1", "r1"), ("p-2", "c-2", "r1"), ("p-3", "c-1", "r1")]):
        loader._buffer_message(_edge_message(p, c, r, 10 + i))
    graph.outage(stage="begin")
    clock.on_sleep.append(lambda now: graph.recover() if graph.down and len(clock.sleeps) >= 25 else None)
    assert loader._flush_batch() is True
    coordinator.close()
    assert graph.failed_attempts >= 3
    assert [call.kwargs["offset"] for call in inner.route_unresolved_endpoint.call_args_list] == [10]
    assert index.adds == [(EDGE_TOPIC, 0, 10)]


def test_edge_revoke_then_assign_during_an_outage_waits_for_the_admission_index(works_at) -> None:
    """S5: a revoke during the outage hands the batch over; the re-assignment raises the
    v7-S4 admission barrier, so nothing is read or written until the parked-pair index has
    caught up; then the re-read batch is written once Neo4j is back, in order, exactly."""
    clock = FakeClock()
    graph, ledger = FakeGraph(), LeaseLedger()
    consumer = FakeConsumer(clock=clock)
    index = _FakeDurableIndex()
    loader, coordinator, outage, _inner, _index = _row_level_stream_loader(
        works_at, graph, ledger, consumer, clock, admission_index=index,
    )
    loader._running = True
    consumer.assign_now(EDGE_TOPIC)
    index.ready = True
    assert loader._admission_ready() is True
    for message in _edge_messages():
        loader._buffer_message(message)
    graph.outage()
    clock.on_sleep.append(
        lambda now: setattr(consumer, "poll_hook", lambda: consumer.revoke_now(EDGE_TOPIC))
        if len(clock.sleeps) == 3 else None
    )
    assert loader._flush_batch() is True  # handed over
    assert loader._batch == [] and consumer.commits == []

    index.ready = False
    consumer.assign_now(EDGE_TOPIC)  # the same replica gets the partition back
    assert loader._admission_barrier is not None
    assert (EDGE_TOPIC, 0) in consumer.paused  # held behind the barrier
    begins = graph.begins
    assert loader._admission_ready() is False
    assert graph.begins == begins  # nothing written behind the barrier
    index.ready = True
    assert loader._admission_ready() is True
    assert (EDGE_TOPIC, 0) not in consumer.paused

    # the next owner (this replica) re-reads from the committed offset; Neo4j comes back meanwhile
    consumer.queue.extend(_edge_messages())
    clock.on_sleep.append(lambda now: graph.recover() if graph.down else None)
    assert loader.run(max_messages=len(EDGE_BATCH)) == 0
    coordinator.close()
    assert graph.edges() == _expected_edges()
    assert graph.history(("p-1", "c-1"))[-3:] == ["r1", "r2", "r3"]
    assert consumer.commits == [[(EDGE_TOPIC, 0, 16)]]
    assert not outage.active
