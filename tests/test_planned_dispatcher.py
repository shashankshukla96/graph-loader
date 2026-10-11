"""Unit tests for the planned-edge dispatcher state machine and TCP server."""
from __future__ import annotations

import asyncio
import random
import threading
import time

import pytest

from src.loader.edge_plan import ChunkSpec, ResourceTable, assemble_plan, resource_pair, write_plan_file
from src.loader.planned_writer import DispatcherClient, DispatcherError
from src.orchestrator import planned_dispatcher as planned_dispatcher_module
from src.orchestrator.planned_dispatcher import END, DispatchError, DispatchState, PlannedDispatcher

DIGEST = "a" * 64


def make_plan(units, *, labels=("A", "B"), buckets=4, types=None):
    """``units``: list of (edge_type, source_bucket, target_bucket, [rows per chunk])."""
    table = ResourceTable(labels, buckets)
    types = types or {"T": ("A", "B")}
    specs, offset, totals = [], 0, {name: 0 for name in types}
    for index, (edge_type, source, target, chunks) in enumerate(units):
        source_label, target_label = types[edge_type]
        pair = resource_pair(table.bucket_resource(source_label, source), table.bucket_resource(target_label, target))
        for seq, rows in enumerate(chunks):
            specs.append(ChunkSpec(f"{edge_type}:{index}:{seq}", edge_type, (edge_type, index), seq, pair, rows,
                                   0, offset))
            offset += 1
            totals[edge_type] += rows
    return assemble_plan(
        plan_id="plan-1", topic="chunks", partitions=1, resources=table,
        types={name: (*types[name], totals[name]) for name in types}, specs=specs,
    )


def test_grants_are_resource_disjoint_ranked_and_end_after_last_done() -> None:
    plan = make_plan([("T", 0, 0, [10]), ("T", 0, 1, [10]), ("T", 1, 1, [10])])
    state = DispatchState(plan, ready_labels=["A", "B"])
    first, _ = state.grant("w1")
    second, _ = state.grant("w2")
    assert set(plan.chunks[first].resources).isdisjoint(plan.chunks[second].resources)
    # The third unit overlaps both granted ones.
    assert state.grant("w3") is None
    state.done("w1", first, 10)
    assert state.grant("w3") is None  # still overlaps w2's grant
    state.done("w2", second, 10)
    third, _ = state.grant("w3")
    assert plan.chunks[third].resources == (0, 5)
    state.done("w3", third, 10)
    assert state.complete and state.grant("w1") == END
    assert sorted(entry.chunk for entry in state.ledger) == [0, 1, 2]


# ── Metrics instrumentation (P6-S2 Story 6) ──────────────────────────────────


def test_dispatch_state_gauges_reflect_grant_and_done_mutations() -> None:
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    plan = make_plan([("T", 0, 0, [10]), ("T", 0, 1, [10]), ("T", 1, 1, [10])])
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    state = DispatchState(plan, ready_labels=["A", "B"], metrics=metrics)

    # Set once, at construction, to the plan's total chunk count - never
    # changes for the life of the run.
    assert metrics.registry.get_sample_value(
        "graph_loader_plan_chunks_planned", {"kind": "planned-dispatcher"}
    ) == 3.0

    first, _ = state.grant("w1")
    assert metrics.registry.get_sample_value(
        "graph_loader_plan_active_writers", {"kind": "planned-dispatcher"}
    ) == 1.0
    second, _ = state.grant("w2")
    assert metrics.registry.get_sample_value(
        "graph_loader_plan_active_writers", {"kind": "planned-dispatcher"}
    ) == 2.0

    state.done("w1", first, 10)
    assert metrics.registry.get_sample_value(
        "graph_loader_plan_active_writers", {"kind": "planned-dispatcher"}
    ) == 1.0
    assert metrics.registry.get_sample_value(
        "graph_loader_plan_chunks_committed", {"kind": "planned-dispatcher"}
    ) == 1.0

    state.done("w2", second, 10)
    third, _ = state.grant("w3")
    state.done("w3", third, 10)
    assert state.complete
    assert metrics.registry.get_sample_value(
        "graph_loader_plan_chunks_committed", {"kind": "planned-dispatcher"}
    ) == metrics.registry.get_sample_value(
        "graph_loader_plan_chunks_planned", {"kind": "planned-dispatcher"}
    ) == 3.0
    assert metrics.registry.get_sample_value(
        "graph_loader_plan_active_writers", {"kind": "planned-dispatcher"}
    ) == 0.0


def test_plan_active_writers_never_exceeds_launched_writer_count() -> None:
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    plan = make_plan([("T", 0, 0, [10]), ("T", 0, 1, [10]), ("T", 1, 1, [10])])
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    state = DispatchState(plan, ready_labels=["A", "B"], metrics=metrics)
    launched_writers = 2
    state.grant("w1")
    state.grant("w2")
    # A third writer would overlap both already-granted units, so it never
    # gets a grant - active writers stays within the launched count.
    assert state.grant("w3") is None
    active = metrics.registry.get_sample_value(
        "graph_loader_plan_active_writers", {"kind": "planned-dispatcher"}
    )
    assert active <= launched_writers


def test_unit_chain_runs_in_order_and_the_successor_is_the_holders_hint() -> None:
    plan = make_plan([("T", 0, 0, [5, 5, 5])])
    state = DispatchState(plan, ready_labels=["A", "B"])
    chunk, hint = state.grant("w1")
    assert chunk == 0 and hint == 1
    assert state.grant("w2") is None  # successor waits for chunk 0's commit
    state.done("w1", 0, 5)
    # The successor is hinted to w1, so w2 only gets it when nothing else is eligible.
    assert state.grant("w2") == (1, 2)
    assert state.stats.stolen_hints == 1


def test_own_hint_is_preferred_within_slack() -> None:
    plan = make_plan([("T", 0, 0, [5]), ("T", 1, 1, [5]), ("T", 2, 2, [5]), ("T", 3, 3, [5])])
    state = DispatchState(plan, ready_labels=["A", "B"], hint_slack=8)
    chunk, hint = state.grant("w1")
    assert hint is not None and hint != chunk
    state.done("w1", chunk, 5)
    assert state.grant("w1")[0] == hint
    assert state.stats.hint_grants == 1


def test_label_readiness_gates_chunks() -> None:
    plan = make_plan(
        [("T", 0, 0, [5]), ("U", 1, 1, [5])],
        labels=("A", "B", "C"), types={"T": ("A", "B"), "U": ("A", "C")},
    )
    state = DispatchState(plan, ready_labels=["A", "B"])
    granted, _ = state.grant("w1")
    assert plan.chunks[granted].edge_type == "T"
    assert state.grant("w2") is None
    assert state.mark_ready(["C"]) is True and state.mark_ready(["C"]) is False
    other, _ = state.grant("w2")
    assert plan.chunks[other].edge_type == "U"


def test_node_phase_limit_caps_active_chunks_until_every_plan_label_is_ready() -> None:
    plan = make_plan(
        [("T", 0, 0, [5]), ("T", 1, 1, [5]), ("T", 2, 2, [5]), ("U", 3, 3, [5])],
        labels=("A", "B", "C"), types={"T": ("A", "B"), "U": ("A", "C")},
    )
    state = DispatchState(plan, ready_labels=["A", "B"], node_phase_limit=1)
    first, _ = state.grant("w1")
    assert state.grant("w2") is None and state.stats.node_phase_deferrals == 1
    state.done("w1", first, 5)
    second, _ = state.grant("w2")
    assert state.grant("w3") is None
    # Once C (the last planned label) is ready the cap no longer applies.
    state.mark_ready(["C"])
    assert state.grant("w3") is not None and state.grant("w4") is not None


def test_node_phase_limit_defaults_from_environment(monkeypatch) -> None:
    plan = make_plan([("T", 0, 0, [5])])
    monkeypatch.setenv("GRAPH_LOADER_PLAN_NODE_PHASE_WRITERS", "3")
    assert DispatchState(plan).node_phase_limit == 3
    monkeypatch.delenv("GRAPH_LOADER_PLAN_NODE_PHASE_WRITERS")
    assert DispatchState(plan).node_phase_limit == 0
    with pytest.raises(ValueError):
        DispatchState(plan, node_phase_limit=-1)


def test_done_must_match_the_grant_and_planned_rows() -> None:
    plan = make_plan([("T", 0, 0, [5]), ("T", 1, 1, [5])])
    state = DispatchState(plan, ready_labels=["A", "B"])
    chunk, _ = state.grant("w1")
    with pytest.raises(DispatchError, match="does not hold"):
        state.done("w2", chunk, 5)
    assert state.state == "failed"
    with pytest.raises(DispatchError, match="failed"):
        state.grant("w3")
    state = DispatchState(plan, ready_labels=["A", "B"])
    chunk, _ = state.grant("w1")
    with pytest.raises(DispatchError, match="committed 4 rows"):
        state.done("w1", chunk, 4)
    state = DispatchState(plan, ready_labels=["A", "B"])
    state.grant("w1")
    with pytest.raises(DispatchError, match="already holds"):
        state.grant("w1")


def test_disconnect_while_holding_fails_closed_and_keeps_resources_held() -> None:
    plan = make_plan([("T", 0, 0, [5]), ("T", 0, 1, [5])])
    state = DispatchState(plan, ready_labels=["A", "B"])
    chunk, _ = state.grant("w1")
    held = state.held_resources()
    state.disconnect("w1")
    assert state.state == "failed" and "disconnected while holding" in state.failure
    assert state.held_resources() == held
    with pytest.raises(DispatchError):
        state.grant("w2")
    # A writer without a grant may leave freely.
    state = DispatchState(plan, ready_labels=["A", "B"])
    state.disconnect("w9")
    assert state.state == "running"


@pytest.mark.parametrize("seed", range(12))
def test_randomized_stress_never_grants_overlapping_resources(seed) -> None:
    rng = random.Random(seed)
    buckets = 3
    units = [
        (rng.choice(["T", "S"]), rng.randrange(buckets), rng.randrange(buckets),
         [rng.randint(1, 9) for _ in range(rng.randint(1, 4))])
        for _ in range(rng.randint(5, 40))
    ]
    plan = make_plan(units, labels=("A", "B"), buckets=buckets, types={"T": ("A", "B"), "S": ("A", "A")})
    state = DispatchState(plan, ready_labels=["A"], hint_slack=rng.randint(0, 5))
    writers = [f"w{index}" for index in range(rng.randint(1, 6))]
    active: dict[str, int] = {}
    done_order: list[int] = []
    for _step in range(10_000):
        if state.complete:
            break
        if "B" not in state.ready_labels and rng.random() < 0.05:
            state.mark_ready(["B"])
        idle = [writer for writer in writers if writer not in active]
        if idle and (not active or rng.random() < 0.5):
            writer = rng.choice(idle)
            result = state.grant(writer)
            if result is None or result == END:
                if not active and "B" not in state.ready_labels:
                    state.mark_ready(["B"])
                continue
            chunk_id, _hint = result
            chunk = plan.chunks[chunk_id]
            assert "B" in state.ready_labels or "B" not in plan.labels_of(chunk)
            assert chunk.after == -1 or chunk.after in done_order
            for other in active.values():
                assert set(plan.chunks[other].resources).isdisjoint(chunk.resources)
            active[writer] = chunk_id
        else:
            writer = rng.choice(sorted(active))
            chunk_id = active.pop(writer)
            state.done(writer, chunk_id, plan.chunks[chunk_id].rows)
            done_order.append(chunk_id)
    assert state.complete
    assert sorted(done_order) == list(range(len(plan.chunks)))
    assert state.rows_done == plan.total_rows
    positions = {chunk_id: index for index, chunk_id in enumerate(done_order)}
    for chunk in plan.chunks:
        if chunk.after != -1:
            assert positions[chunk.after] < positions[chunk.id]


# -- TCP server ------------------------------------------------------------------


class _Server:
    def __init__(self, plan, **kwargs) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.dispatcher = PlannedDispatcher(plan, DIGEST, **kwargs)
        host, port = asyncio.run_coroutine_threadsafe(self.dispatcher.start("127.0.0.1", 0), self.loop).result(5)
        self.address = f"{host}:{port}"

    def client(self, writer_id=None, digest=DIGEST) -> DispatcherClient:
        client = DispatcherClient(self.address, digest=digest, writer_id=writer_id,
                                  connect_timeout_seconds=2, op_timeout_seconds=5)
        client.connect()
        return client

    def stop(self) -> None:
        asyncio.run_coroutine_threadsafe(self.dispatcher.close(), self.loop).result(5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)


@pytest.fixture
def server():
    started = []

    def start(plan, **kwargs):
        instance = _Server(plan, **kwargs)
        started.append(instance)
        return instance

    yield start
    for instance in started:
        instance.stop()


def test_hello_rejects_a_different_plan_digest(server) -> None:
    running = server(make_plan([("T", 0, 0, [1])]), ready_labels=["A", "B"])
    with pytest.raises(DispatcherError, match="digest mismatch"):
        running.client("w1", digest="b" * 64)
    assert running.client("w1").plan_id == "plan-1"


def test_parked_next_is_granted_on_done_and_all_writers_get_end(server) -> None:
    plan = make_plan([("T", 0, 0, [3, 4])])
    running = server(plan, ready_labels=["A", "B"])
    first, second = running.client("w1"), running.client("w2")
    chunk, hint = first.next(0)
    assert (chunk, hint) == (0, 1)
    assert second.next(0) == "WAIT"
    results = {}
    waiter = threading.Thread(target=lambda: results.setdefault("second", second.next(5000)))
    waiter.start()
    first.done(0, 3, 1.5)
    waiter.join(5)
    assert results["second"] == (1, None)
    assert first.next(0) == "WAIT"
    second.done(1, 4, 2.0)
    assert first.next(1000) == "END" and second.next(0) == "END"
    control = running.client()
    summary = control.summary()
    assert summary["state"] == "complete" and [entry[0] for entry in summary["ledger"]] == [0, 1]
    assert summary["types"]["T"]["rows"] == 7


def test_plan_resource_wait_seconds_observes_once_per_wait_resolution(server) -> None:
    """One observation per wait *resolution* (Story 6), not accumulated
    into a running total the way total_wait_ms already is."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    plan = make_plan([("T", 0, 0, [3, 4])])
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    running = server(plan, ready_labels=["A", "B"], metrics=metrics)
    first, second = running.client("w1"), running.client("w2")
    first.next(0)
    assert second.next(0) == "WAIT"  # immediate WAIT (wait_ms=0): no waiter parked, no resolution yet.
    results = {}
    waiter = threading.Thread(target=lambda: results.setdefault("second", second.next(5000)))
    waiter.start()
    # Deterministically wait until w2 has actually parked as a waiter on
    # the dispatcher's event loop before releasing w1's chunk - otherwise
    # this is a race: if first.done() below runs before the event loop has
    # processed w2's NEXT request, w2's own immediate grant check inside
    # _next() can succeed without ever parking (bypassing _resolve/this
    # story's metric entirely), which is a real, correct dispatcher
    # behavior but makes this specific test flaky, not the instrumentation.
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not running.dispatcher._waiters:
        time.sleep(0.005)
    assert running.dispatcher._waiters, "writer w2 never parked as a waiter"
    first.done(0, 3, 1.5)
    waiter.join(5)
    assert results["second"] == (1, None)

    assert metrics.registry.get_sample_value(
        "graph_loader_plan_resource_wait_seconds_count", {"kind": "planned-dispatcher"}
    ) == 1.0
    assert metrics.registry.get_sample_value(
        "graph_loader_plan_resource_wait_seconds_sum", {"kind": "planned-dispatcher"}
    ) >= 0.0


def test_serve_sigterm_leaves_snapshot_with_accumulated_counters(tmp_path, monkeypatch) -> None:
    """P6-S2 Story 9/10 (review finding S9-1): a SIGTERM (the async stop
    Event serve() already watches - exactly what main()'s real SIGTERM
    handler sets) arriving mid-run, after a real grant+done, still leaves
    a metrics-*.prom snapshot with those counters."""
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    # P6-S2 review finding S10-2: pin to a free ephemeral port instead of
    # binding the real 0.0.0.0:9464 during the unit suite.
    import socket as _socket_module
    with _socket_module.socket(_socket_module.AF_INET, _socket_module.SOCK_STREAM) as _s:
        _s.bind(("127.0.0.1", 0))
        monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(_s.getsockname()[1]))
    snapshot_path = tmp_path / "metrics-planned-dispatcher.prom"
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(snapshot_path))

    plan = make_plan([("T", 0, 0, [10])])
    plan_path = tmp_path / "edge-plan.json"
    write_plan_file(plan_path, plan)

    captured: dict[str, object] = {}
    real_cls = planned_dispatcher_module.PlannedDispatcher

    class CapturingDispatcher(real_cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            captured["dispatcher"] = self

    monkeypatch.setattr(planned_dispatcher_module, "PlannedDispatcher", CapturingDispatcher)

    async def run() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(
            planned_dispatcher_module.serve(
                str(plan_path), "127.0.0.1", 0, stop=stop, ready_labels=["A", "B"],
            )
        )
        while "dispatcher" not in captured:
            await asyncio.sleep(0.01)
        # A real grant+done through the same DispatchState methods the
        # wire protocol uses - sets plan_active_writers/
        # plan_chunks_committed, exactly as a NEXT/DONE exchange would.
        state = captured["dispatcher"].state
        chunk_id, _hint = state.grant("w1")
        state.done("w1", chunk_id, plan.chunks[chunk_id].rows)
        stop.set()
        await task

    asyncio.run(run())

    assert snapshot_path.exists()
    from prometheus_client.parser import text_string_to_metric_families

    families = {family.name: family for family in text_string_to_metric_families(snapshot_path.read_text())}
    assert any(sample.value > 0 for sample in families["graph_loader_plan_chunks_committed"].samples)


def test_ready_from_control_releases_gated_writers(server) -> None:
    plan = make_plan([("T", 0, 0, [2])])
    running = server(plan, ready_labels=["A"])
    writer = running.client("w1")
    results = {}
    waiter = threading.Thread(target=lambda: results.setdefault("grant", writer.next(5000)))
    waiter.start()
    control = running.client()
    assert control.status()["ready_labels"] == ["A"]
    control.ready(["B"])
    waiter.join(5)
    assert results["grant"] == (0, None)


def test_writer_disconnect_with_a_grant_fails_the_run_for_everyone(server) -> None:
    plan = make_plan([("T", 0, 0, [2]), ("T", 1, 1, [2])])
    running = server(plan, ready_labels=["A", "B"])
    doomed, survivor, control = running.client("w1"), running.client("w2"), running.client()
    doomed.next(0)
    doomed.close()
    for _ in range(50):
        if control.status()["state"] == "failed":
            break
        threading.Event().wait(0.02)
    status = control.status()
    assert status["state"] == "failed" and "disconnected while holding" in status["failure"]
    with pytest.raises(DispatcherError, match="failed"):
        survivor.next(0)


def test_fail_abort_and_protocol_violations_fail_closed(server) -> None:
    plan = make_plan([("T", 0, 0, [2]), ("T", 1, 1, [2])])
    running = server(plan, ready_labels=["A", "B"])
    writer = running.client("w1")
    chunk, _ = writer.next(0)
    writer.fail(chunk, "Neo4j said no")
    control = running.client()
    assert "Neo4j said no" in control.status()["failure"]
    running2 = server(plan, ready_labels=["A", "B"])
    writer2 = running2.client("w1")
    with pytest.raises(DispatcherError, match="does not hold"):
        writer2.done(0, 2, 1.0)
    assert running2.client().status()["state"] == "failed"
    running3 = server(plan, ready_labels=["A", "B"])
    running3.client().abort("node label failed")
    assert "aborted by orchestrator" in running3.client().status()["failure"]


def test_duplicate_writer_ids_are_rejected(server) -> None:
    running = server(make_plan([("T", 0, 0, [1])]), ready_labels=["A", "B"])
    _first = running.client("w1")
    with pytest.raises(DispatcherError, match="already connected"):
        running.client("w1")
