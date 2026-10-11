"""v7-S7 story 7.4: each label pair's endpoints are leased as their own resources, through the unchanged server."""
from __future__ import annotations

import asyncio
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry

from multi_pair_support import BACKFILL, COMMENTS, POSTS, GraphStore, likes, likes_schema
from src import cli
from src.loader.edge_execution import (
    LaneExecutionResult,
    BucketLeaseExecutionCoordinator,
    LeaseDrainAbandoned,
    PairSplitLeaseCoordinator,
    build_edge_execution,
)
from src.loader.edge_loader import PendingEdgeRecord
from src.loader.edge_records import normalize_edge_record
from src.loader.lease_client import BucketCellRouter, BucketLeaseClient
from src.loader.mix_and_batch import endpoint_bucket
from src.orchestrator.lease_server import BucketLeaseServer
from src.utils.metrics import GraphLoaderMetrics
from multi_pair_support import record

ROOT = Path(__file__).resolve().parents[1]


def _pending(edge, topic: str, person: int, other: int, offset: int, partition: int = 0) -> PendingEdgeRecord:
    return PendingEdgeRecord(normalize_edge_record(record(topic, person, other), edge.topic_view(topic)),
                             topic, partition, offset)


class _LeaseServer:
    def __init__(self, buckets: int) -> None:
        self.loop = asyncio.new_event_loop()
        self.server = BucketLeaseServer(bucket_count=buckets, scheme="-")
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)

    def __enter__(self) -> str:
        self.thread.start()
        host, port = asyncio.run_coroutine_threadsafe(self.server.start("127.0.0.1", 0), self.loop).result(5)
        return f"{host}:{port}"

    def __exit__(self, *exc) -> None:
        asyncio.run_coroutine_threadsafe(self.server.close(), self.loop).result(5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()


# ── AC 1: router cells ────────────────────────────────────────────────────────


@pytest.mark.parametrize("buckets", [1, 64])
def test_cells_of_two_pairs_share_only_the_common_label(buckets: int) -> None:
    posts = BucketCellRouter("Person", "Post", buckets)
    comments = BucketCellRouter("Person", "Comment", buckets)
    post_cell, comment_cell = posts.cell(1, 5), comments.cell(1, 5)
    assert post_cell != comment_cell
    assert set(BucketCellRouter.resources(post_cell)) & set(BucketCellRouter.resources(comment_cell)) == {
        f"Person:{endpoint_bucket(1, buckets)}"}


def test_lease_execution_routes_each_pair_through_its_own_router() -> None:
    edge = likes()
    writer, _p, _b, coordinator = build_edge_execution(
        edge, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700", "GRAPH_LOADER_LEASE_BUCKETS": "8"},
        lease_client_factory=lambda: MagicMock(),
    )
    assert isinstance(coordinator, PairSplitLeaseCoordinator) and coordinator.routes_records is True
    subs = coordinator.coordinators
    assert [(sub._router.source_label, sub._router.target_label, sub._router.bucket_count) for sub in subs.values()] == [
        ("Person", "Post", 8), ("Person", "Comment", 8)]
    assert all(isinstance(sub, BucketLeaseExecutionCoordinator) and sub._observe_lease_wait is False
               for sub in subs.values())
    assert all(sub._writer._edge_config.nodes.target in ("Post", "Comment") for sub in subs.values())


# ── AC 2: against the real, unchanged lease server ───────────────────────────


def _flush(edge, store: GraphStore, pending: list, *, buckets: int, merge_rows: str | None = None):
    registry = GraphLoaderMetrics(registry=CollectorRegistry())
    environ = {"GRAPH_LOADER_LEASE_ADDR": "", "GRAPH_LOADER_LEASE_BUCKETS": str(buckets)}
    if merge_rows is not None:
        environ["GRAPH_LOADER_LEASE_MERGE_ROWS"] = merge_rows
    with _LeaseServer(buckets) as address:
        environ["GRAPH_LOADER_LEASE_ADDR"] = address
        _w, _p, _b, coordinator = build_edge_execution(
            edge, store.driver(), metrics=registry, environ=environ,
            lease_client_factory=lambda: BucketLeaseClient(address, bucket_count=buckets),
        )
        try:
            results = coordinator.execute_pending(tuple(pending))
        finally:
            coordinator.close()
    assert all(result.success for result in results), [repr(r.exception) for r in results if not r.success][:3]
    return coordinator, registry


def _mixed_pending(edge, count: int = 240) -> list:
    return [_pending(edge, (POSTS, BACKFILL, COMMENTS)[i % 3], i % 40, i % 10, offset=i // 3, partition=i % 2)
            for i in range(count)]


def test_two_pairs_against_a_real_lease_server_are_node_disjoint_and_never_mixed() -> None:
    """Default EXT merging on: colliding Person/Post/Comment keys, 3 workers per pair."""
    edge = likes(execution={"worker_count": 3})
    buckets = 4
    store = GraphStore({"Person": set(range(40)), "Post": set(range(10)), "Comment": set(range(10))})
    store.resource_of = BucketCellRouter("Person", "Post", buckets).resource
    pending = _mixed_pending(edge)
    coordinator, registry = _flush(edge, store, pending, buckets=buckets)
    assert not store.overlaps, store.overlaps[:5]  # no two concurrent holders of a resource token
    assert all(len(tx["pairs"]) == 1 for tx in store.transactions)  # no transaction mixes pairs, EXT merges included
    expected = {("LIKES", "Person", p.record.source_key, "Comment" if p.topic == COMMENTS else "Post",
                 p.record.target_key) for p in pending}
    assert set(store.rels) == expected
    [observed] = [s for f in registry.registry.collect() if f.name == "graph_loader_lease_wait_seconds"
                  for s in f.samples if s.name.endswith("_count")]
    assert observed.value == 1  # AC 3: one lease_wait observation per outer flush
    assert "pair=Person>Post cells=" in coordinator.describe_last_flush()
    assert " | pair=Person>Comment cells=" in coordinator.describe_last_flush()


def test_post_and_comment_cells_are_held_at_the_same_time() -> None:
    """``Post:x`` and ``Comment:x`` are different resources: the first write of each pair waits for the
    other's, which can only complete if both pairs hold leases together. EXT merging is off so one write
    holds one Person bucket (with merging, one transaction may legitimately hold every Person bucket)."""
    edge = likes(execution={"worker_count": 3})
    buckets = 4
    store = GraphStore({"Person": set(range(40)), "Post": set(range(10)), "Comment": set(range(10))})
    store.resource_of = BucketCellRouter("Person", "Post", buckets).resource
    rendezvous = threading.Barrier(2, timeout=20)
    gate = {"posts": False, "comments": False}
    gate_lock = threading.Lock()

    def rendezvous_hook(pair) -> None:
        key = "posts" if pair == ("Person", "Post") else "comments"
        with gate_lock:
            first_of_pair = not gate[key]
            gate[key] = True
        if first_of_pair:
            rendezvous.wait()

    store.before_write = rendezvous_hook
    _flush(edge, store, _mixed_pending(edge), buckets=buckets, merge_rows="0")
    assert store.max_open_pairs == {("Person", "Post"), ("Person", "Comment")}
    assert not store.overlaps and all(len(tx["pairs"]) == 1 for tx in store.transactions)


# ── AC 4: keyed labels are the union over pairs ──────────────────────────────


def test_a_small_comment_topic_makes_comment_keyed(monkeypatch) -> None:
    schema = likes_schema()
    monkeypatch.setattr(cli, "_node_topic_record_counts",
                        lambda _bootstrap, topics: {"persons": 10 ** 6, "posts": 10 ** 6, "comments": 12})
    assert cli._select_keyed_lease_labels(schema, SimpleNamespace(lease_keyed_max_nodes=100)) == ("Comment",)


# ── AC 5: a stop abandons every pair's group ─────────────────────────────────


class _DenyClient:
    broken = False

    def connect(self) -> None:
        return None

    def acquire_any(self, sets, *, wait_ms):
        time.sleep(0.001)
        return None

    def close(self) -> None:
        return None


def test_a_stop_during_a_two_pair_flush_abandons_both_groups() -> None:
    edge = likes()
    stop = threading.Event()
    subs = {
        pair.index: BucketLeaseExecutionCoordinator(
            MagicMock(), router=BucketCellRouter(*pair.labels, 4), worker_count=2, batch_size=10,
            client_factory=_DenyClient, stop_event=stop, drain_budget_seconds=0.2, observe_lease_wait=False,
        )
        for pair in edge.endpoint_pairs
    }
    pair_of = {topic: pair.index for pair in edge.endpoint_pairs for topic in pair.topics}
    coordinator = PairSplitLeaseCoordinator(subs, lambda item: pair_of[item.topic], edge_type="LIKES",
                                            pair_names={0: "Person>Post", 1: "Person>Comment"},
                                            metrics=GraphLoaderMetrics(registry=CollectorRegistry()))
    stop.set()
    started = time.monotonic()
    results = coordinator.execute_pending((_pending(edge, POSTS, 1, 1, 1), _pending(edge, COMMENTS, 1, 1, 1)))
    assert time.monotonic() - started < 3
    assert len(results) == 2 and all(isinstance(r.exception, LeaseDrainAbandoned) for r in results)
    assert {r.routed_records[0].topic for r in results} == {POSTS, COMMENTS}


def test_a_single_pair_flush_is_a_direct_call_and_an_empty_flush_observes_nothing() -> None:
    edge = likes()
    ok = LaneExecutionResult(0, (), True)
    sub = MagicMock()
    sub.execute_pending.return_value = (ok,)
    sub.last_lease_wait_seconds = 0.25
    sub.describe_last_flush.return_value = "cells=1"
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    coordinator = PairSplitLeaseCoordinator({0: sub, 1: MagicMock()}, lambda item: 0, edge_type="LIKES",
                                            pair_names={0: "Person>Post", 1: "Person>Comment"}, metrics=metrics)
    assert coordinator.execute_pending(()) == ()
    assert coordinator.execute_pending((_pending(edge, POSTS, 1, 1, 1),), budget_seconds=3) == (ok,)
    assert sub.execute_pending.call_args.kwargs == {"budget_seconds": 3}
    assert coordinator.last_lease_wait_seconds == 0.25
    assert coordinator.describe_last_flush() == "pair=Person>Post cells=1"


# ── review round 2 ───────────────────────────────────────────────────────────


def _sub(result: LaneExecutionResult) -> MagicMock:
    sub = MagicMock()
    sub.execute_pending.return_value = (result,)
    sub.last_lease_wait_seconds = 0.0
    sub.describe_last_flush.return_value = "cells=1"
    return sub


@pytest.mark.parametrize("abandoned_pair", [0, 1])
def test_a_real_error_beats_a_drain_abandon_whatever_the_pair_order(abandoned_pair: int, caplog) -> None:
    from confluent_kafka import TopicPartition

    from src.loader.edge_loader import EdgeLoader
    from src.models.schema import LoadingConfig

    edge = likes()
    real = RuntimeError("neo4j write failed")
    outcomes = {abandoned_pair: LaneExecutionResult(0, (), False, LeaseDrainAbandoned("budget")),
                1 - abandoned_pair: LaneExecutionResult(0, (), False, real)}
    pair_of = {topic: pair.index for pair in edge.endpoint_pairs for topic in pair.topics}
    coordinator = PairSplitLeaseCoordinator(
        {index: _sub(result) for index, result in outcomes.items()}, lambda item: pair_of[item.topic],
        edge_type="LIKES", pair_names={0: "Person>Post", 1: "Person>Comment"},
        metrics=GraphLoaderMetrics(registry=CollectorRegistry()))
    results = coordinator.execute_pending((_pending(edge, POSTS, 1, 1, 1), _pending(edge, COMMENTS, 1, 1, 1)))
    assert [r.exception for r in results] == [real, real]
    consumer = MagicMock()
    loader = EdgeLoader(consumer, MagicMock(), edge, coordinator=coordinator,
                        loading_config=LoadingConfig(unwind_batch_size=100))
    loader._running = True
    loader._on_assign(consumer, [TopicPartition(POSTS, 0), TopicPartition(COMMENTS, 0)])
    loader._batch.extend([_pending(edge, POSTS, 1, 1, 1), _pending(edge, COMMENTS, 1, 1, 1)])
    assert loader._flush_batch() is False  # logged as a write failure, not a drain abandon
    assert "neo4j write failed" in caplog.text
    consumer.commit.assert_not_called()


def test_the_stop_drain_budget_is_shared_by_every_pair_of_the_type() -> None:
    edge = likes()
    stop = threading.Event()
    clock = [100.0]
    _w, _p, _b, coordinator = build_edge_execution(
        edge, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700"}, stop_event=stop,
        lease_client_factory=lambda: MagicMock(),
    )
    posts, comments = coordinator.coordinators.values()
    assert posts._drain is comments._drain
    posts._clock = comments._clock = lambda: clock[0]
    stop.set()
    assert posts._drain_expired() is False  # the budget starts now, for both pairs
    clock[0] += posts._drain_budget + 0.1
    assert comments._drain_expired() is True  # a later Comment-only flush does not get a fresh budget


def test_single_pair_lease_execution_is_todays_coordinator() -> None:
    from src.utils.schema_loader import load_schema

    for edge in load_schema(ROOT / "examples/social/graph_schema.yaml").edges:
        writer, partitioner, batcher, coordinator = build_edge_execution(
            edge, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700", "GRAPH_LOADER_LEASE_BUCKETS": "8"},
            lease_client_factory=lambda: MagicMock(),
        )
        assert type(coordinator) is BucketLeaseExecutionCoordinator and coordinator._observe_lease_wait is True
        assert partitioner is None and batcher is None
        assert coordinator._router == BucketCellRouter(edge.nodes.source, edge.nodes.target, 8)
        assert writer._edge_config is edge


# ── AC 6: the lease server and client are unchanged ──────────────────────────


def test_the_slice_does_not_touch_the_lease_server_or_client() -> None:
    changed = subprocess.run(
        ["git", "diff", "--name-only", "8748274", "--", "src/orchestrator/lease_server.py",
         "src/loader/lease_client.py", "tests/test_lease_server.py"],
        cwd=ROOT, capture_output=True, text=True,
    )
    if changed.returncode != 0:
        pytest.skip("not a git checkout")
    assert changed.stdout.strip() == ""
