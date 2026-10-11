"""Unit tests for the global bucket-lease coordinator and its blocking client."""
from __future__ import annotations

import asyncio
import random
import re
import socket
from pathlib import Path
import threading
import time

import pytest

from src.loader.lease_client import (
    BUCKET_LEASE_ADDRESS_ENV, BucketLeaseClient, DEFAULT_BUCKET_LEASE_BUCKETS,
    DEFAULT_LEASE_BUCKETS, LEASE_ADDRESS_ENV, LeaseCoordinatorError, lease_scheme_token,
    parse_keyed_labels, parse_lease_address, parse_lease_bucket_count,
    resolve_bucket_lease_address,
)
from src.orchestrator import lease_server
from src.orchestrator.lease_server import (
    BucketLeaseServer, DEFAULT_BUCKET_LEASE_PORT, DEFAULT_LEASE_PORT, LeaseProtocolError, LeaseTable,
)


class _RunningServer:
    """A coordinator served from a background event loop thread."""

    def __init__(self, **kwargs) -> None:
        self.loop = asyncio.new_event_loop()
        self.server = BucketLeaseServer(**kwargs)
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        host, port = self.call(self.server.start("127.0.0.1", 0))
        self.address = f"{host}:{port}"

    def call(self, coroutine, timeout: float = 5.0):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout)

    def client(self, **kwargs) -> BucketLeaseClient:
        client = BucketLeaseClient(self.address, connect_timeout_seconds=2.0, op_timeout_seconds=5.0, **kwargs)
        client.connect()
        return client

    def stats(self) -> dict[str, str]:
        client = self.client()
        try:
            return dict(item.split("=", 1) for item in client.stats().split())
        finally:
            client.close()

    def stop(self) -> None:
        self.call(self.server.close())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()


@pytest.fixture
def running():
    servers: list[_RunningServer] = []

    def start(**kwargs) -> _RunningServer:
        server = _RunningServer(**kwargs)
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.stop()


def _wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition not reached")
        time.sleep(0.01)


# -- pure lease table ----------------------------------------------------------

def test_table_grants_whole_sets_atomically_and_releases_them() -> None:
    table = LeaseTable()
    assert table.try_acquire(1, "a", frozenset({"Person:1", "Title:2"}))
    assert not table.try_acquire(2, "b", frozenset({"Title:2", "Title:3"}))
    # All-or-nothing: the denied set left its free resource untouched.
    assert not table.is_held("Title:3")
    assert table.try_acquire(2, "c", frozenset({"Title:3"}))
    assert table.release(1, "a") == frozenset({"Person:1", "Title:2"})
    assert table.try_acquire(2, "b", frozenset({"Title:2"}))
    assert table.stats.max_concurrent == 2 and table.stats.active == 2


def test_table_rejects_duplicate_ids_and_unknown_release() -> None:
    table = LeaseTable()
    table.try_acquire(1, "a", frozenset({"X:1"}))
    with pytest.raises(LeaseProtocolError):
        table.try_acquire(1, "a", frozenset({"X:2"}))
    with pytest.raises(KeyError):
        table.release(1, "missing")
    # Ids are scoped per connection.
    assert table.try_acquire(2, "a", frozenset({"X:2"}))


def test_table_extend_adds_only_free_or_own_resources() -> None:
    table = LeaseTable()
    table.try_acquire(1, "a", frozenset({"Title:1", "Genre#x"}))
    table.try_acquire(2, "b", frozenset({"Genre#y"}))
    assert table.extend(1, "a", frozenset({"Title:1", "Genre#z"}))
    assert not table.extend(1, "a", frozenset({"Title:1", "Genre#y"}))
    assert table.release(1, "a") == frozenset({"Title:1", "Genre#x", "Genre#z"})
    with pytest.raises(LeaseProtocolError):
        table.extend(1, "a", frozenset({"Q:1"}))


def test_table_release_owner_frees_every_lease_of_a_connection() -> None:
    table = LeaseTable()
    table.try_acquire(7, "a", frozenset({"X:1"}))
    table.try_acquire(7, "b", frozenset({"X:2", "Y:1"}))
    table.try_acquire(8, "a", frozenset({"X:3"}))
    assert table.release_owner(7) == frozenset({"X:1", "X:2", "Y:1"})
    assert table.stats.disconnect_releases == 2
    assert table.is_held("X:3") and not table.is_held("X:1")


# -- client parsing --------------------------------------------------------------

def test_address_bucket_and_scheme_parsing() -> None:
    assert parse_lease_address("lease:7700") == ("lease", 7700)
    assert parse_lease_address("10.0.0.5:1") == ("10.0.0.5", 1)
    for bad in ("", "lease", "lease:", ":7700", "lease:0", "lease:99999", "lease:x"):
        with pytest.raises(ValueError):
            parse_lease_address(bad)
    assert parse_lease_bucket_count(None) == 16
    assert parse_lease_bucket_count("8") == 8
    for bad in (0, 4097, "x", True):
        with pytest.raises(ValueError):
            parse_lease_bucket_count(bad)
    assert parse_keyed_labels(" Genre, ,Region ") == frozenset({"Genre", "Region"})
    with pytest.raises(ValueError):
        parse_keyed_labels("Bad Label")
    assert lease_scheme_token(["Region", "Genre"]) == "Genre,Region"
    assert lease_scheme_token([]) == "-"


# -- TCP server ------------------------------------------------------------------

def test_acquire_denial_and_release_over_tcp(running) -> None:
    server = running(bucket_count=16)
    first, second = server.client(bucket_count=16), server.client(bucket_count=16)
    lease = first.acquire(["Person:1", "Title:2"])
    assert lease is not None
    assert second.acquire(["Title:2", "Title:5"]) is None
    other = second.acquire(["Title:5"])
    assert other is not None
    first.release(lease)
    assert second.acquire(["Title:2"]) is not None
    stats = server.stats()
    assert stats["grants"] == "3" and stats["denials"] == "1" and stats["max_concurrent"] == "2"


def test_release_of_unknown_or_foreign_lease_fails_closed(running) -> None:
    server = running()
    first, second = server.client(), server.client()
    lease = first.acquire(["X:1"])
    with pytest.raises(LeaseCoordinatorError, match="unknown lease"):
        second.release(lease)
    assert second.broken
    with pytest.raises(LeaseCoordinatorError):
        second.acquire(["X:2"])
    first.release(lease)


def test_hello_rejects_bucket_or_scheme_mismatch(running) -> None:
    server = running(bucket_count=16, scheme="Genre")
    server.client(bucket_count=16, keyed_labels=["Genre"]).close()
    with pytest.raises(LeaseCoordinatorError, match="bucket count mismatch"):
        server.client(bucket_count=8, keyed_labels=["Genre"])
    with pytest.raises(LeaseCoordinatorError, match="scheme mismatch"):
        server.client(bucket_count=16)


def test_malformed_requests_get_errors(running) -> None:
    server = running()
    with socket.create_connection(tuple(parse_lease_address(server.address)), timeout=5) as sock:
        reader = sock.makefile("rb")
        for request in (b"BOGUS\n", b"ACQ\n", b"ACQ id\n", b"ANY id x A:1\n", b"REL\n",
                        b"ACQ bad/id A:1\n", b"EXT id 5 A:1\n", b"HELLO x\n", b"\n"):
            sock.sendall(request)
            assert reader.readline().startswith(b"ERR"), request
        sock.sendall(b"PING\n")
        assert reader.readline() == b"OK\n"


def test_disconnect_releases_every_lease(running) -> None:
    server = running()
    holder, other = server.client(), server.client()
    assert holder.acquire(["Person:1"]) is not None
    assert holder.acquire(["Title:1", "Title:2"]) is not None
    assert other.acquire(["Title:2"]) is None
    holder.close()
    _wait_until(lambda: server.stats()["active"] == "0")
    assert other.acquire(["Person:1", "Title:1", "Title:2"]) is not None
    assert server.stats()["disconnect_releases"] == "2"


def test_any_grants_first_free_set_immediately(running) -> None:
    server = running()
    first, second = server.client(), server.client()
    assert first.acquire(["A:1"]) is not None
    grant = second.acquire_any([["A:1", "B:1"], ["A:2", "B:1"], ["A:3"]], wait_ms=0)
    assert grant is not None and grant[1] == 1
    assert second.acquire_any([["A:1"], ["B:1"]], wait_ms=0) is None


def test_any_waits_for_release_then_times_out(running) -> None:
    server = running()
    holder, waiter = server.client(), server.client()
    lease = holder.acquire(["A:1", "B:1"])
    outcome = {}
    thread = threading.Thread(
        target=lambda: outcome.setdefault("grant", waiter.acquire_any([["C:9", "A:1"]], wait_ms=5_000)),
    )
    thread.start()
    _wait_until(lambda: server.stats()["waiters"] == "1")
    assert "grant" not in outcome
    holder.release(lease)
    thread.join(5)
    assert outcome["grant"] is not None and outcome["grant"][1] == 0
    started = time.monotonic()
    assert holder.acquire_any([["A:1"]], wait_ms=100) is None
    assert time.monotonic() - started >= 0.09
    stats = server.stats()
    assert (stats["wait_timeouts"], stats["waited_grants"], stats["waiters"]) == ("1", "1", "0")


# ── Metrics instrumentation (P6-S2 Story 7) ──────────────────────────────────


def test_lease_grants_total_counts_immediate_and_waited_grants(running) -> None:
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    server = running(metrics=metrics)
    first = server.client()
    assert first.acquire(["A:1"]) is not None  # immediate grant via ACQ -> table.grant().
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_grants_total", {"kind": "lease-server"}
    ) == 1.0

    # P6-S2 review finding S7-3: the criterion says "whether granted
    # immediately or after a wait" - exercise an immediate ANY grant too
    # (not just ACQ), since ANY and ACQ share the same table.grant() call
    # but the AC's literal wording is about ANY specifically.
    second = server.client()
    assert second.acquire_any([["Z:1"]], wait_ms=0) is not None
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_grants_total", {"kind": "lease-server"}
    ) == 2.0

    holder, waiter = server.client(), server.client()
    lease = holder.acquire(["B:1"])
    outcome = {}
    thread = threading.Thread(
        target=lambda: outcome.setdefault("grant", waiter.acquire_any([["B:1"]], wait_ms=5_000)),
    )
    thread.start()
    _wait_until(lambda: server.stats()["waiters"] == "1")
    holder.release(lease)
    thread.join(5)
    assert outcome["grant"] is not None
    # first.acquire() (1) + second.acquire_any(immediate) (1) +
    # holder.acquire() (1, also immediate) + waiter's grant after the
    # wait (1) = 4: one grant per granted request, whether ACQ or ANY,
    # immediate or after a wait.
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_grants_total", {"kind": "lease-server"}
    ) == 4.0


def test_lease_wait_timeouts_total_and_waiters_gauge_return_to_zero(running) -> None:
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    server = running(metrics=metrics)
    holder, waiter = server.client(), server.client()
    lease = holder.acquire(["A:1", "B:1"])
    outcome = {}
    thread = threading.Thread(
        target=lambda: outcome.setdefault("grant", waiter.acquire_any([["C:9", "A:1"]], wait_ms=200)),
    )
    thread.start()
    _wait_until(lambda: server.stats()["waiters"] == "1")
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_waiters", {"kind": "lease-server"}
    ) == 1.0
    thread.join(5)
    assert outcome["grant"] is None  # timed out, never resolved by holder.release.
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_wait_timeouts_total", {"kind": "lease-server"}
    ) == 1.0
    # No leaked waiter count once every outstanding request is resolved.
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_waiters", {"kind": "lease-server"}
    ) == 0.0
    holder.release(lease)


def test_lease_held_resources_tracks_grant_extend_and_release(running) -> None:
    """P6-S2 review finding S7-2: the epic's literal '<= bucket_count' bound
    is wrong (resources are label:bucket tokens, so the real bound is
    len(keyed_labels) * bucket_count, documented in the completion doc) -
    this test proves what the gauge actually does: track grant, EXT, and
    release exactly, which is the property Story 7's AC actually needs."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    server = running(metrics=metrics, bucket_count=4)
    client = server.client(bucket_count=4)
    lease = client.acquire(["A:1", "A:2", "A:3"])
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_held_resources", {"kind": "lease-server"}
    ) == 3.0
    client.release(lease)
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_held_resources", {"kind": "lease-server"}
    ) == 0.0


def test_lease_held_resources_updates_on_extend_not_only_grant_and_release(running) -> None:
    """P6-S2 review finding S7-1: EXT (lease_client.py's merged-cell
    extension) mutates self._holder directly and must update the gauge
    too - grant()/release() alone would under-report between an
    extension and the next grant/release."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    server = running(metrics=metrics, bucket_count=4)
    client = server.client(bucket_count=4)
    lease = client.acquire(["A:1"])
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_held_resources", {"kind": "lease-server"}
    ) == 1.0
    added = client.extend(lease, [(1, ["A:2"])], budget=10)
    assert added == [0]
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_held_resources", {"kind": "lease-server"}
    ) == 2.0
    client.release(lease)
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_held_resources", {"kind": "lease-server"}
    ) == 0.0


def test_lease_denials_total_increments_on_acq_and_any_denial(running) -> None:
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    server = running(metrics=metrics)
    holder, other = server.client(), server.client()
    holder.acquire(["A:1"])
    assert other.acquire(["A:1"]) is None  # ACQ denial.
    assert other.acquire_any([["A:1"]], wait_ms=0) is None  # ANY, wait_ms=0, denial.
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_denials_total", {"kind": "lease-server"}
    ) == 2.0


def test_parked_waiters_are_served_in_arrival_order(running) -> None:
    server = running()
    holder = server.client()
    lease = holder.acquire(["A:1"])
    order: list[int] = []
    lock = threading.Lock()
    clients = [server.client() for _ in range(3)]

    def wait(index: int) -> None:
        grant = clients[index].acquire_any([["A:1"]], wait_ms=5_000)
        with lock:
            order.append(index)
        clients[index].release(grant[0])

    threads = []
    for index in range(3):
        thread = threading.Thread(target=wait, args=(index,))
        thread.start()
        threads.append(thread)
        _wait_until(lambda: server.stats()["waiters"] == str(index + 1))
    holder.release(lease)
    for thread in threads:
        thread.join(5)
    assert order == [0, 1, 2]


def test_release_hands_resources_only_to_waiters_whose_whole_set_is_free(running) -> None:
    server = running()
    a, b, c = server.client(), server.client(), server.client()
    lease_a = a.acquire(["T:1"])
    lease_b = b.acquire(["P:1"])
    result = {}
    thread = threading.Thread(target=lambda: result.setdefault("grant", c.acquire_any([["P:1", "T:1"]], wait_ms=5_000)))
    thread.start()
    _wait_until(lambda: server.stats()["waiters"] == "1")
    a.release(lease_a)
    time.sleep(0.05)
    assert "grant" not in result  # P:1 is still held
    b.release(lease_b)
    thread.join(5)
    assert result["grant"] is not None and result["grant"][1] == 0


def test_extend_merges_free_sets_within_budget(running) -> None:
    server = running()
    owner, other = server.client(), server.client()
    lease = owner.acquire(["Title:1", "Genre#a"])
    assert other.acquire(["Genre#b"]) is not None
    added = owner.extend(lease, [
        (10, ["Title:1", "Genre#b"]),   # held by another lease
        (30, ["Title:1", "Genre#c"]),
        (80, ["Title:1", "Genre#d"]),   # would exceed the budget
        (20, ["Title:2", "Genre#a"]),
    ], budget=60)
    assert added == [1, 3]
    assert other.acquire(["Genre#c"]) is None and other.acquire(["Title:2"]) is None
    owner.release(lease)
    assert other.acquire(["Genre#c", "Title:2", "Genre#d"]) is not None


def test_unreachable_coordinator_fails_closed_after_connect_deadline() -> None:
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    clock = iter([0.0, 0.1, 5.0, 10.0])
    client = BucketLeaseClient(
        f"127.0.0.1:{port}", connect_timeout_seconds=1.0, sleep=lambda _s: None, clock=lambda: next(clock),
    )
    with pytest.raises(LeaseCoordinatorError, match="unreachable"):
        client.acquire(["A:1"])
    assert client.broken


# -- P5-S1 Story 3: client generation tracking + bounded backoff ------------

def test_backoff_none_is_byte_for_byte_unchanged_bulk_behavior() -> None:
    """AC: with backoff_ms=None (bulk's default), reconnect timing is
    unchanged from today -- a fixed 0.2s poll (asserted via ``sleep`` calls,
    no real time elapsed) up to the 30s default deadline."""
    attempts = {"count": 0}

    def flaky_connect(*_args, **_kwargs):
        attempts["count"] += 1
        raise OSError("refused")

    sleeps: list[float] = []
    clock = iter([0.0, 0.1, 0.2, 40.0])  # last value exceeds the 30s deadline
    client = BucketLeaseClient(
        "fake:1", connect=flaky_connect, sleep=sleeps.append, clock=lambda: next(clock),
    )
    with pytest.raises(LeaseCoordinatorError, match="unreachable"):
        client.acquire(["A:1"])
    assert sleeps == [0.2, 0.2]  # fixed poll, no growth
    assert attempts["count"] == 3


def test_backoff_ms_grows_capped_exponentially_with_unbounded_timeout() -> None:
    """AC: with backoff_ms=(500, 30_000) and connect_timeout_seconds=None
    (unbounded), a client whose first 6 connection attempts fail and whose
    7th succeeds retries with strictly increasing (capped) delays and
    eventually connects -- an injected fake clock/sleeper, no real sockets
    or real time elapsed."""
    attempts = {"count": 0}
    real_socket = object()

    def flaky_connect(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] <= 6:
            raise OSError("refused")
        return real_socket

    class _FakeSocket:
        def setsockopt(self, *_a, **_k):
            pass

        def sendall(self, _data):
            pass

        def makefile(self, _mode):
            class _R:
                def readline(self_inner):
                    return b"OK\n"
            return _R()

        def settimeout(self, *_a, **_k):
            pass

    def connect(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] <= 6:
            raise OSError("refused")
        return _FakeSocket()

    sleeps: list[float] = []
    client = BucketLeaseClient(
        "fake:1", connect_timeout_seconds=None, backoff_ms=(500, 30_000),
        connect=connect, sleep=sleeps.append, clock=time.monotonic,
    )
    client.connect()
    assert attempts["count"] == 7
    assert sleeps == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0]  # min(500*2**(n-1), 30_000) ms, capped
    assert all(earlier < later for earlier, later in zip(sleeps, sleeps[1:]))


def test_backoff_ms_caps_at_max_ms() -> None:
    """Delays grow 1000, 2000, then cap at 3000ms and stay there."""
    attempts = {"count": 0}

    class _FakeSocket:
        def setsockopt(self, *_a, **_k):
            pass

        def sendall(self, _data):
            pass

        def makefile(self, _mode):
            class _R:
                def readline(self_inner):
                    return b"OK\n"
            return _R()

        def settimeout(self, *_a, **_k):
            pass

    def connect(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] <= 9:
            raise OSError("refused")
        return _FakeSocket()

    sleeps: list[float] = []
    client = BucketLeaseClient(
        "fake:1", connect_timeout_seconds=None, backoff_ms=(1_000, 3_000),
        connect=connect, sleep=sleeps.append, clock=time.monotonic,
    )
    client.connect()
    assert attempts["count"] == 10
    assert sleeps == [1.0, 2.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0]


def test_hello_bucket_mismatch_fails_on_first_attempt_regardless_of_backoff() -> None:
    """AC: a HELLO bucket-count mismatch (a config bug, not a transient
    outage) still fails closed immediately with no retry, regardless of
    backoff_ms -- the fake connect function is called exactly once."""
    calls = {"count": 0}

    class _FakeReader:
        def readline(self) -> bytes:
            return b"ERR bucket count mismatch server=16 client=8\n"

        def close(self):
            pass

    class _FakeSocket:
        def setsockopt(self, *_a, **_k):
            pass

        def sendall(self, _data):
            pass

        def makefile(self, _mode):
            return _FakeReader()

        def settimeout(self, *_a, **_k):
            pass

        def close(self):
            pass

    def connect(*_args, **_kwargs):
        calls["count"] += 1
        return _FakeSocket()

    client = BucketLeaseClient(
        "fake:1", bucket_count=8, connect_timeout_seconds=None, backoff_ms=(500, 30_000),
        connect=connect,
    )
    with pytest.raises(LeaseCoordinatorError, match="HELLO rejected"):
        client.connect()
    assert calls["count"] == 1


def test_generation_property_reflects_the_most_recent_hello(running) -> None:
    """The client exposes the last HELLO's generation as a read-only
    diagnostic. (Review finding S3-1: this class does *not* itself log a
    "previous vs. new generation" transition -- a client that hits any
    request failure is marked permanently broken and never reconnects
    itself, so a same-instance comparison could never fire in production.
    Where a coordinator restart is actually observed across the
    connection *pool* is Story 4's job, once the pool is restructured to
    survive one -- see the class docstring.)"""
    server = running(bucket_count=16)
    client = server.client(bucket_count=16)
    assert client.generation is not None and len(client.generation) == 32
    # A second HELLO on the same still-connected client is a no-op (the
    # existing connect() early-return), so the generation is unchanged.
    client.connect()
    assert client.generation is not None


def test_stop_event_interrupts_an_unbounded_backoff_wait() -> None:
    """S3-2: an unbounded (connect_timeout_seconds=None) reconnect loop
    must be interruptible by shutdown, not block in a plain time.sleep()
    until Docker's stop grace expires."""
    stop_event = threading.Event()
    attempts = {"count": 0}

    def flaky_connect(*_args, **_kwargs):
        attempts["count"] += 1
        raise OSError("refused")

    client = BucketLeaseClient(
        "fake:1", connect_timeout_seconds=None, backoff_ms=(60_000, 60_000),
        connect=flaky_connect, stop_event=stop_event,
    )
    started = time.monotonic()
    threading.Timer(0.05, stop_event.set).start()
    with pytest.raises(LeaseCoordinatorError, match="shutdown requested"):
        client.connect()
    elapsed = time.monotonic() - started
    # Returns promptly (well under the 60s backoff delay), not permanently
    # "broken" -- a clean abandonment, not a configuration failure.
    assert elapsed < 5.0
    assert not client.broken
    assert attempts["count"] >= 1


def test_stop_event_checked_before_the_first_connect_attempt() -> None:
    stop_event = threading.Event()
    stop_event.set()
    client = BucketLeaseClient(
        "fake:1", connect_timeout_seconds=None,
        connect=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("should not attempt to connect")),
        stop_event=stop_event,
    )
    with pytest.raises(LeaseCoordinatorError, match="shutdown requested"):
        client.connect()


def test_backoff_ms_is_validated_at_construction() -> None:
    """N3-3: direct constructors (tests, bulk overrides) bypass the
    schema's own bounds, so the client validates its own inputs."""
    with pytest.raises(ValueError, match="backoff_ms"):
        BucketLeaseClient("fake:1", backoff_ms=(0, 1_000))
    with pytest.raises(ValueError, match="backoff_ms"):
        BucketLeaseClient("fake:1", backoff_ms=(1_000, 500))
    BucketLeaseClient("fake:1", backoff_ms=(1, 1))  # boundary: valid


def test_reconnect_wait_logs_first_attempt_and_capped_attempts(caplog) -> None:
    """N3-2: an operator must see something during an unbounded outage
    (the first attempt, and every attempt that has reached the cap), not
    a silent stall."""
    attempts = {"count": 0}

    class _FakeSocket:
        def setsockopt(self, *_a, **_k):
            pass

        def sendall(self, _data):
            pass

        def makefile(self, _mode):
            class _R:
                def readline(self_inner):
                    return b"OK\n"
            return _R()

        def settimeout(self, *_a, **_k):
            pass

    def connect(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] <= 4:
            raise OSError("refused")
        return _FakeSocket()

    client = BucketLeaseClient(
        "fake:1", connect_timeout_seconds=None, backoff_ms=(500, 1_000),
        connect=connect, sleep=lambda _s: None, clock=time.monotonic,
    )
    with caplog.at_level("WARNING"):
        client.connect()
    # Delays: 500, 1000 (cap), 1000 (cap), 1000 (cap) -- attempt 1 and every
    # capped attempt (2, 3, 4) logs; nothing else does.
    wait_lines = [r for r in caplog.records if "stage=lease-reconnect-wait" in r.message]
    assert len(wait_lines) == 4
    assert "attempt=1" in wait_lines[0].message


def test_coordinator_shutdown_breaks_clients(running) -> None:
    server = running()
    client = server.client()
    assert client.acquire(["A:1"]) is not None
    server.call(server.server.close())
    with pytest.raises(LeaseCoordinatorError):
        client.acquire(["A:2"])


def test_close_resolves_a_parked_any_waiter_promptly(running) -> None:
    """P6-S5 Story 5: `close()` must resolve a client already parked in
    ANY, not leave it hanging until its own (long) `wait_ms` elapses.
    `close()` writes a best-effort "ERR shutting down" line to the
    waiter's own connection before force-cancelling its future and
    force-closing the socket; the client's `acquire_any` turns that
    unexpected-but-present reply into a prompt `LeaseCoordinatorError`.
    Per the binding P5-Q1 fencing amendment, a writer must already treat
    any lease-connection loss as "fence, roll back," so either a clean
    ERR reply or a bare disconnect is a correct outcome; this test guards
    that it stays *prompt* either way, and that today's ERR line is
    actually the one delivered.
    """
    server = running()
    holder, waiter = server.client(), server.client()
    assert holder.acquire(["A:1"]) is not None
    outcome: dict[str, object] = {}

    def park() -> None:
        try:
            waiter.acquire_any([["A:1"]], wait_ms=30_000)
        except LeaseCoordinatorError as exc:
            outcome["error"] = exc

    thread = threading.Thread(target=park)
    thread.start()
    _wait_until(lambda: server.stats()["waiters"] == "1")

    started = time.monotonic()
    server.call(server.server.close())
    close_elapsed = time.monotonic() - started

    thread.join(5)
    assert not thread.is_alive()
    assert isinstance(outcome.get("error"), LeaseCoordinatorError)
    assert "shutting down" in str(outcome["error"])
    assert close_elapsed < 5.0


def test_close_resolves_several_parked_any_waiters_promptly(running) -> None:
    """Same property as above, for several concurrent parked waiters: none
    of them queues behind another, and `close()` itself returns promptly
    regardless of how many were parked."""
    server = running()
    holder = server.client()
    assert holder.acquire(["A:1"]) is not None
    waiters = [server.client() for _ in range(3)]
    outcomes: list[dict[str, object]] = [{} for _ in waiters]

    def park(index: int, client) -> None:
        try:
            client.acquire_any([["A:1"]], wait_ms=30_000)
        except LeaseCoordinatorError as exc:
            outcomes[index]["error"] = exc

    threads = [threading.Thread(target=park, args=(index, client)) for index, client in enumerate(waiters)]
    for thread in threads:
        thread.start()
    _wait_until(lambda: server.stats()["waiters"] == "3")

    started = time.monotonic()
    server.call(server.server.close())
    close_elapsed = time.monotonic() - started

    for thread in threads:
        thread.join(5)
    assert all(not thread.is_alive() for thread in threads)
    assert all(isinstance(outcome.get("error"), LeaseCoordinatorError) for outcome in outcomes)
    assert close_elapsed < 5.0


def test_concurrent_holders_never_overlap(running) -> None:
    """Many threads contend for random resource sets; no two holders overlap."""
    server = running()
    resources = [f"Person:{index}" for index in range(6)] + [f"Title:{index}" for index in range(6)]
    owners: dict[str, int] = {}
    owners_lock = threading.Lock()
    violations: list[str] = []
    grants = [0]

    def worker(seed: int) -> None:
        rng = random.Random(seed)
        client = server.client()
        try:
            for _ in range(60):
                sets = [sorted(rng.sample(resources, rng.randint(1, 3))) for _ in range(rng.randint(1, 4))]
                grant = client.acquire_any(sets, wait_ms=rng.choice((0, 20)))
                if grant is None:
                    continue
                lease_id, index = grant
                held = sets[index]
                with owners_lock:
                    for resource in held:
                        if resource in owners:
                            violations.append(resource)
                        owners[resource] = seed
                    grants[0] += 1
                time.sleep(rng.random() * 0.001)
                with owners_lock:
                    for resource in held:
                        owners.pop(resource, None)
                client.release(lease_id)
        finally:
            client.close()

    threads = [threading.Thread(target=worker, args=(seed,)) for seed in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert not violations
    assert grants[0] > 100
    _wait_until(lambda: server.stats()["active"] == "0")


def test_main_serves_until_sigterm_and_logs_stats(monkeypatch, caplog) -> None:
    captured = {}

    async def fake_serve(host, port, *, bucket_count, stop, scheme=None, quiet_period_seconds=0.0,
                          disconnect_hold_seconds=0.0):
        captured.update(
            host=host, port=port, bucket_count=bucket_count, scheme=scheme,
            quiet_period_seconds=quiet_period_seconds, disconnect_hold_seconds=disconnect_hold_seconds,
        )
        return lease_server.LeaseStats()

    monkeypatch.setattr(lease_server, "serve", fake_serve)
    assert lease_server.main(["--port", "7701", "--buckets", "8", "--scheme", "Genre"]) == 0
    assert captured == {
        "host": "0.0.0.0", "port": 7701, "bucket_count": 8, "scheme": "Genre", "quiet_period_seconds": 0.0,
        "disconnect_hold_seconds": 0.0,
    }
    with pytest.raises(SystemExit):
        lease_server.main(["--buckets", "0"])


def test_serve_logs_final_stats(caplog) -> None:
    async def run() -> lease_server.LeaseStats:
        stop = asyncio.Event()
        task = asyncio.create_task(lease_server.serve("127.0.0.1", 0, bucket_count=4, stop=stop))
        await asyncio.sleep(0.05)
        stop.set()
        return await task

    with caplog.at_level("INFO", logger="src.orchestrator.lease_server"):
        stats = asyncio.run(run())
    assert stats.grants == 0
    assert any("stage=lease-server-stats" in record.getMessage() for record in caplog.records)


def test_serve_sigterm_leaves_snapshot_with_accumulated_counters(tmp_path, monkeypatch) -> None:
    """P6-S2 Story 9/10 (review finding S9-1): a SIGTERM (here: the async
    stop Event serve() already watches, exactly what the real SIGTERM
    handler installed in main() sets) arriving mid-run, after a real
    grant, still leaves a metrics-*.prom snapshot with that grant's
    counters."""
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    # P6-S2 review finding S10-2: pin to a free ephemeral port instead of
    # binding the real 0.0.0.0:9464 during the unit suite.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _s:
        _s.bind(("127.0.0.1", 0))
        monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(_s.getsockname()[1]))
    snapshot_path = tmp_path / "metrics-lease-server.prom"
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(snapshot_path))

    captured: dict[str, object] = {}
    real_cls = lease_server.BucketLeaseServer

    class CapturingServer(real_cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            captured["server"] = self

    monkeypatch.setattr(lease_server, "BucketLeaseServer", CapturingServer)

    async def run() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(lease_server.serve("127.0.0.1", 0, bucket_count=None, stop=stop))
        while "server" not in captured:
            await asyncio.sleep(0.01)
        # A real grant through the same LeaseTable.grant() the wire
        # protocol uses - increments lease_grants_total and sets
        # lease_held_resources, exactly as an ACQ over TCP would.
        captured["server"].table.grant(("owner-1", None), "lease-1", frozenset({"A:1"}))
        stop.set()
        await task

    asyncio.run(run())

    assert snapshot_path.exists()
    from prometheus_client.parser import text_string_to_metric_families

    families = {family.name: family for family in text_string_to_metric_families(snapshot_path.read_text())}
    samples = families["graph_loader_lease_grants"].samples
    assert any(sample.value > 0 for sample in samples)


@pytest.mark.parametrize("sets, other_held, expected", [
    ([["A:1", "B:1"], ["A:1", "C:1"]], ["B:1"], 1),
    ([["A:1", "C:1"], ["A:1", "D:1"], ["A:1", "C:1"]], [], 0),
])
def test_woken_waiter_gets_its_lowest_index_free_set(running, sets, other_held, expected) -> None:
    server = running()
    holder, blocker, waiter = server.client(), server.client(), server.client()
    lease = holder.acquire(["A:1"])
    for resource in other_held:
        assert blocker.acquire([resource]) is not None
    result = {}
    thread = threading.Thread(target=lambda: result.setdefault("grant", waiter.acquire_any(sets, wait_ms=5_000)))
    thread.start()
    _wait_until(lambda: server.stats()["waiters"] == "1")
    holder.release(lease)
    thread.join(5)
    assert result["grant"][1] == expected
    waiter.release(result["grant"][0])
    _wait_until(lambda: server.stats()["active"] == str(len(other_held)))


# -- P5-S1 Story 1: bucket_lease_* rename ------------------------------------

def test_deprecated_default_aliases_still_resolve() -> None:
    """``grep -rn DEFAULT_LEASE_PORT|DEFAULT_LEASE_BUCKETS`` must still
    resolve as back-compat aliases equal to the new names."""
    assert DEFAULT_LEASE_PORT == DEFAULT_BUCKET_LEASE_PORT == 7700
    assert DEFAULT_LEASE_BUCKETS == DEFAULT_BUCKET_LEASE_BUCKETS == 16


def test_resolve_bucket_lease_address_prefers_current_name() -> None:
    assert resolve_bucket_lease_address({}) == ""
    assert resolve_bucket_lease_address({LEASE_ADDRESS_ENV: "old:7700"}) == "old:7700"
    assert resolve_bucket_lease_address({BUCKET_LEASE_ADDRESS_ENV: "new:7700"}) == "new:7700"
    assert resolve_bucket_lease_address(
        {BUCKET_LEASE_ADDRESS_ENV: "new:7700", LEASE_ADDRESS_ENV: "old:7700"}
    ) == "new:7700"


def test_resolve_bucket_lease_address_deprecation_log_once_per_process(caplog) -> None:
    import src.loader.lease_client as lease_client_module

    lease_client_module._bucket_lease_addr_fallback_logged = False
    with caplog.at_level("WARNING"):
        resolve_bucket_lease_address({LEASE_ADDRESS_ENV: "old:7700"})
        resolve_bucket_lease_address({LEASE_ADDRESS_ENV: "old:7700"})
        resolve_bucket_lease_address({LEASE_ADDRESS_ENV: "old:7700"})
    deprecation_lines = [r for r in caplog.records if "stage=deprecation" in r.message]
    assert len(deprecation_lines) == 1
    assert f"flag={LEASE_ADDRESS_ENV}" in deprecation_lines[0].message
    assert f"superseded_by={BUCKET_LEASE_ADDRESS_ENV}" in deprecation_lines[0].message
    # A resolution that never falls back logs nothing.
    lease_client_module._bucket_lease_addr_fallback_logged = False
    caplog.clear()
    with caplog.at_level("WARNING"):
        resolve_bucket_lease_address({BUCKET_LEASE_ADDRESS_ENV: "new:7700"})
    assert not [r for r in caplog.records if "stage=deprecation" in r.message]


def test_no_old_lease_setting_names_used_internally_in_src() -> None:
    """Source-scan guard (mirrors the P6-S2 metrics cardinality-guard test):
    every *internal* ``src/`` call site uses the renamed
    ``bucket_lease_*``/``BUCKET_LEASE`` identifiers, not the deprecated
    aliases -- only the alias *definitions* themselves may still mention the
    old names."""
    root = Path(__file__).resolve().parents[1] / "src"
    old_names = re.compile(
        r"\bDEFAULT_LEASE_PORT\b|\bDEFAULT_LEASE_BUCKETS\b|\bDEFAULT_LEASE_OUTER_BATCH_SIZE\b|"
        r"\bDEFAULT_LEASE_KEYED_MAX_NODES\b"
    )
    allowed_alias_lines = {
        # "<old> = <new>" alias-definition lines only.
        "DEFAULT_LEASE_PORT = DEFAULT_BUCKET_LEASE_PORT",
        "DEFAULT_LEASE_BUCKETS = DEFAULT_BUCKET_LEASE_BUCKETS",
        "DEFAULT_LEASE_OUTER_BATCH_SIZE = DEFAULT_BUCKET_LEASE_OUTER_BATCH_SIZE",
        "DEFAULT_LEASE_KEYED_MAX_NODES = DEFAULT_BUCKET_LEASE_KEYED_MAX_NODES",
    }
    violations = []
    for path in root.rglob("*.py"):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if old_names.search(line) and line.strip() not in allowed_alias_lines:
                violations.append(f"{path.relative_to(root.parent)}:{lineno}: {line.strip()}")
    assert not violations, "deprecated lease setting names used outside their alias definitions:\n" + "\n".join(violations)


def test_lease_address_env_string_only_referenced_where_expected() -> None:
    """N3: LEASE_ADDRESS_ENV (the deprecated env var *name*) must only be
    referenced in lease_client.py (definition + fallback resolution) and
    docker_service.py (the dual-set), never re-implemented ad hoc
    elsewhere."""
    root = Path(__file__).resolve().parents[1] / "src"
    allowed_files = {"loader/lease_client.py", "orchestrator/docker_service.py"}
    violations = []
    for path in root.rglob("*.py"):
        relative = str(path.relative_to(root))
        if relative in allowed_files:
            continue
        if "LEASE_ADDRESS_ENV" in path.read_text(encoding="utf-8"):
            violations.append(relative)
    assert not violations, f"unexpected LEASE_ADDRESS_ENV reference outside {allowed_files}: {violations}"


# -- P5-S1 Story 2: server-side generation ------------------------------------

def _raw_request(address: str, line: str) -> str:
    host, port = parse_lease_address(address)
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(line.encode("ascii") + b"\n")
        return sock.makefile("rb").readline().decode("ascii").strip()


def test_hello_returns_a_stable_32_hex_char_generation_token(running) -> None:
    server = running(bucket_count=16, scheme="-")
    reply = _raw_request(server.address, "HELLO 16 -")
    head, _, token = reply.partition(" ")
    assert head == "OK"
    assert len(token) == 32
    assert all(c in "0123456789abcdef" for c in token)
    # Stable across every subsequent HELLO/STATS on the same running instance.
    again = _raw_request(server.address, "HELLO 16 -")
    assert again == reply
    stats_reply = _raw_request(server.address, "STATS")
    assert f"generation={token}" in stats_reply


def test_two_server_instances_never_share_a_generation_token() -> None:
    """Uses a monotonic-counter test double, not uuid4's statistical
    uniqueness, per the story's acceptance criterion (review finding
    N2-3: the counter is an actual factory invoked by the code under
    test, not two hard-coded literals asserted against each other)."""
    counter = iter(f"gen-{index}" for index in range(1, 1_000))
    first = BucketLeaseServer(generation=next(counter))
    second = BucketLeaseServer(generation=next(counter))
    assert first.generation != second.generation
    assert (first.generation, second.generation) == ("gen-1", "gen-2")
    # The real (no injected generation) path still produces distinct tokens.
    real_a, real_b = BucketLeaseServer(), BucketLeaseServer()
    assert real_a.generation != real_b.generation


def test_generation_token_is_validated_against_the_lease_id_charset() -> None:
    """N2-2: a test-supplied generation with whitespace/newlines would
    corrupt the single-line HELLO/STATS wire protocol."""
    with pytest.raises(ValueError, match="invalid generation token"):
        BucketLeaseServer(generation="has space")
    with pytest.raises(ValueError, match="invalid generation token"):
        BucketLeaseServer(generation="has\nnewline")
    with pytest.raises(ValueError, match="invalid generation token"):
        BucketLeaseServer(generation="")
    BucketLeaseServer(generation="valid-token_123.ok")


def test_stats_reply_keeps_every_existing_key_and_appends_generation(running) -> None:
    server = running()
    reply = _raw_request(server.address, "STATS")
    assert re.match(
        r"^OK connections=\d+ grants=\d+ extensions=\d+ waited_grants=\d+ denials=\d+ "
        r"wait_timeouts=\d+ releases=\d+ disconnect_releases=\d+ active=\d+ "
        r"max_concurrent=\d+ max_waiters=\d+ total_wait_ms=[\d.]+ waiters=\d+ "
        r"generation=[0-9a-f]{32}$",
        reply,
    ), reply


def test_client_built_against_old_bare_ok_hello_reply_still_accepted() -> None:
    """This story alone (before Story 3 parses/uses the token) must not
    break bulk mode's existing behavior: a fake server replying the old
    bare ``OK`` still connects."""
    calls = {"count": 0}

    class _FakeReader:
        def readline(self) -> bytes:
            return b"OK\n"

    class _FakeSocket:
        def setsockopt(self, *_args, **_kwargs):
            pass

        def sendall(self, data: bytes) -> None:
            calls["count"] += 1

        def makefile(self, _mode):
            return _FakeReader()

        def settimeout(self, *_args, **_kwargs):
            pass

        def close(self):
            pass

    client = BucketLeaseClient(
        "fake:1", bucket_count=16, connect=lambda *_a, **_k: _FakeSocket(),
    )
    client.connect()
    assert calls["count"] == 1


def test_client_accepts_generation_reply_and_still_rejects_mismatch_over_tcp(running) -> None:
    """N2-4: pins, against the real server, that the relaxed check accepts
    the new "OK <generation>" reply *and* still rejects an "ERR ..." bucket
    mismatch on the same code path. (Story 2 is server-only: the client
    does not yet expose a .generation attribute -- that is Story 3 -- so
    this asserts via the raw wire reply instead.)"""
    server = running(bucket_count=16)
    accepted = server.client(bucket_count=16)  # must not raise
    accepted.close()
    reply = _raw_request(server.address, "HELLO 16 -")
    head, _, token = reply.partition(" ")
    assert head == "OK" and len(token) == 32
    with pytest.raises(LeaseCoordinatorError, match="bucket count mismatch"):
        server.client(bucket_count=8)


# -- P5-S1 Story 5: post-restart quiet period Q ------------------------------

def test_quiet_period_refuses_every_acq_and_any_grant_until_it_elapses(running) -> None:
    clock = iter([0.0] + [0.1 * i for i in range(1, 200)])
    server = running(quiet_period_seconds=5.0, clock=lambda: next(clock))
    client, other = server.client(), server.client()
    assert _raw_request(server.address, "") or True  # keep _raw_request imported/used elsewhere
    assert client.acquire(["A:1"]) is None  # ACQ: "NO quiet", not a protocol error
    assert client.acquire_any([["A:1"]], wait_ms=0) is None  # ANY, wait_ms=0: "NO quiet"
    # Nothing was actually granted -- no denials/wait_timeouts counted either.
    stats = server.stats()
    assert stats["grants"] == "0" and stats["denials"] == "0" and stats["wait_timeouts"] == "0"
    assert other.acquire(["A:1"]) is None


def test_quiet_period_elapses_and_then_grants_normally(running) -> None:
    # A tiny quiet period that elapses almost immediately, using the real
    # clock (deterministic ordering, not exact timing).
    server = running(quiet_period_seconds=0.05)
    client = server.client()
    _wait_until(lambda: client.acquire(["A:1"]) is not None, timeout=2.0)


def test_zero_quiet_period_preserves_existing_behavior(running) -> None:
    """Every existing test (and bulk mode, which never restarts this
    server) constructs BucketLeaseServer with no quiet_period_seconds at
    all -- must behave exactly as before this story."""
    server = running()  # quiet_period_seconds defaults to 0.0
    client = server.client()
    assert client.acquire(["A:1"]) is not None


def test_raw_hello_reply_unaffected_by_quiet_period(running) -> None:
    """HELLO/PING/STATS are informational, never grant-issuing, so the
    quiet period must not gate them at all."""
    server = running(quiet_period_seconds=30.0)
    reply = _raw_request(server.address, "HELLO 16 -")
    assert reply.startswith("OK ")


# -- P5-S1 Story 4 review finding B4-1: disconnect quarantine ---------------

def test_disconnect_holds_the_lease_until_disconnect_time_plus_fence_seconds(running) -> None:
    """A bare TCP EOF (not an explicit REL) must not free resources until
    grant_time + fence_seconds -- a real writer's Neo4j transaction may
    still be open when its lease socket closes. Real (small) timing,
    since asyncio's call_later always runs on the event loop's own clock,
    not an injected test clock."""
    server = running(disconnect_hold_seconds=0.2)
    holder = server.client()
    assert holder.acquire(["A:1"]) is not None
    holder.close()  # bare disconnect, no REL
    # Not yet freed: the hold (0.2s) hasn't elapsed.
    other = server.client()
    assert other.acquire(["A:1"]) is None
    _wait_until(lambda: server.stats()["active"] == "0", timeout=2.0)
    assert other.acquire(["A:1"]) is not None


def test_zero_fence_seconds_frees_immediately_on_disconnect(running) -> None:
    """Preserves today's behavior exactly when fencing is not configured."""
    server = running()  # disconnect_hold_seconds defaults to 0.0
    holder = server.client()
    assert holder.acquire(["A:1"]) is not None
    holder.close()
    _wait_until(lambda: server.stats()["active"] == "0")
    other = server.client()
    assert other.acquire(["A:1"]) is not None


def test_explicit_release_is_immediate_even_with_fence_seconds_configured(running) -> None:
    server = running(disconnect_hold_seconds=30.0)
    holder = server.client()
    lease = holder.acquire(["A:1"])
    holder.release(lease)
    other = server.client()
    assert other.acquire(["A:1"]) is not None


def test_disconnect_fence_is_measured_from_the_disconnect_not_the_grant(running) -> None:
    """Hardening: the fence is measured from the disconnect, so a lease held
    (and extended) for longer than the hold still stays fenced for the full
    hold after the disconnect, independent of how long it was held. (A
    grant-based hold was already safe for writes -- their deadline is grant +
    T and EXT never extends it.) Never re-granted earlier than disconnect + hold."""
    hold = 0.6
    server = running(disconnect_hold_seconds=hold)
    holder = server.client()
    lease = holder.acquire(["A:1"])
    added = holder.extend(lease, [(1, ["B:1"])], budget=10)
    assert added == [0]
    time.sleep(hold + 0.2)  # held longer than the hold itself
    disconnected_at = time.monotonic()
    holder.close()  # bare disconnect holding both A:1 and B:1 (EXT'd)
    other = server.client()
    time.sleep(0.2)
    assert other.acquire(["A:1"]) is None
    assert other.acquire(["B:1"]) is None
    _wait_until(lambda: server.stats()["active"] == "0", timeout=3.0)
    assert time.monotonic() - disconnected_at >= hold - 0.05
    assert server.stats()["disconnect_releases"] == "1"  # one lease, extended
    assert other.acquire(["A:1"]) is not None
    assert other.acquire(["B:1"]) is not None


# -- P5-S1 Story 8: confirm streaming lease metrics ---------------------------

def test_lease_server_metrics_populate_for_a_contended_and_a_disjoint_run(running) -> None:
    """AC: a contended run produces nonzero grants/waits; a disjoint run's
    denials stay at exactly 0. No new label value or series is introduced
    anywhere in this slice -- kind="lease-server" already covers both
    bulk and streaming, unconditionally."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    server = running(metrics=metrics)
    first, second = server.client(), server.client()
    lease = first.acquire(["A:1"])
    assert second.acquire(["A:1"]) is None  # contended: a denial
    first.release(lease)
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_grants_total", {"kind": "lease-server"}
    ) == 1.0
    assert metrics.registry.get_sample_value(
        "graph_loader_lease_denials_total", {"kind": "lease-server"}
    ) == 1.0

    disjoint_metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    disjoint_server = running(metrics=disjoint_metrics)
    solo = disjoint_server.client()
    assert solo.acquire(["B:1"]) is not None
    # Never denied at all -- the labeled series may not even exist yet
    # (prometheus_client only creates a child metric on first .inc()),
    # which is itself the proof of "stays at exactly 0."
    denials = disjoint_metrics.registry.get_sample_value(
        "graph_loader_lease_denials_total", {"kind": "lease-server"}
    )
    assert denials in (None, 0.0)
    assert disjoint_server.stats()["denials"] == "0"


def test_lease_server_counters_reset_to_zero_on_a_fresh_generation() -> None:
    """A supervised restart's fresh process gets a fresh registry (P6-S2's
    registry-per-process design) -- every lease-server counter reads 0
    again, even though the previous generation's grants were nonzero.
    Prometheus's own rate()/increase() handle this drop correctly; a
    dashboard subtracting two raw scrapes would not (documented in
    docs/USAGE.md)."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    old_generation_metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    old_server = BucketLeaseServer(metrics=old_generation_metrics)
    old_server.table.grant(1, "lease-1", frozenset({"A:1"}))
    assert old_generation_metrics.registry.get_sample_value(
        "graph_loader_lease_grants_total", {"kind": "lease-server"}
    ) == 1.0

    new_generation_metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    new_server = BucketLeaseServer(metrics=new_generation_metrics)
    assert new_server.generation != old_server.generation
    # A brand-new process's registry has never observed a single grant --
    # prometheus_client only creates the labeled child metric on first
    # .inc(), so "not present yet" is exactly what "reset to 0" means for
    # a fresh generation (in production, initialize_zero_series forces
    # this to an explicit 0.0 sample at process start instead).
    reset_value = new_generation_metrics.registry.get_sample_value(
        "graph_loader_lease_grants_total", {"kind": "lease-server"}
    )
    assert reset_value in (None, 0.0)
    assert new_server.stats.grants == 0


def test_sigterm_storm_to_the_lease_server_process_still_exits_zero() -> None:
    """F2: a container may be stopped twice; SIGTERM keeps arriving while the process
    tears down. CPython resets Python-level handlers to SIG_DFL at interpreter exit and
    asyncio resets loop handlers when the loop closes, so only SIG_IGN set as the last
    step keeps every signal harmless (exit 0, never -15/143). Signals are sent every
    ~1 ms until the process exits, several runs, to hit the teardown window."""
    import os
    import signal
    import socket
    import subprocess
    import sys

    for _ in range(10):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        env = dict(os.environ, GRAPH_LOADER_METRICS_ENABLED="0")
        proc = subprocess.Popen(
            [sys.executable, "-m", "src.orchestrator.lease_server", "--host", "127.0.0.1", "--port", str(port)],
            cwd=Path(__file__).resolve().parents[1], env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                    break
                except OSError:
                    time.sleep(0.02)
            else:
                pytest.fail("lease server did not start")
            deadline = time.monotonic() + 10
            while proc.poll() is None and time.monotonic() < deadline:
                try:
                    proc.send_signal(signal.SIGTERM)
                except ProcessLookupError:
                    break
                time.sleep(0.001)
            assert proc.wait(timeout=10) == 0
        finally:
            if proc.poll() is None:
                proc.kill()
