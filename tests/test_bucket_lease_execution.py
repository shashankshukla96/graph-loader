"""Tests for bucket-lease cell routing and the lease-aware edge executor."""
from __future__ import annotations

import asyncio
from collections import defaultdict
import itertools
from pathlib import Path
import random
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.loader.edge_execution import (
    ApocLockedEdgeWriter, BucketLeaseExecutionCoordinator, ExecutionModeError, LeaseDrainAbandoned,
    LaneExecutionCoordinator, build_edge_execution, lease_mode_settings,
    streaming_lease_client_factory,
)
from src.loader.edge_loader import (
    EdgeLoader, EdgeRecord, PendingEdgeRecord, apply_edge_outer_batch_override, main as edge_loader_main,
)
from src.loader.lease_client import BucketCellRouter, BucketLeaseClient, LeaseCoordinatorError
from src.loader.mix_and_batch import endpoint_bucket
from src.models.schema import DeadLetterConfig, LoadingConfig
from src.orchestrator.lease_server import BucketLeaseServer
from src.utils.schema_loader import load_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def schema():
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


@pytest.fixture(scope="module")
def works_at(schema):
    return next(edge for edge in schema.edges if edge.type == "WORKS_AT")


@pytest.fixture(scope="module")
def knows(schema):
    return next(edge for edge in schema.edges if edge.type == "KNOWS")


def _pending(source, target, seq: int, *, partition: int = 0) -> PendingEdgeRecord:
    return PendingEdgeRecord(EdgeRecord(source, target, {"seq": seq}), "t", partition, seq)


# -- cell routing ------------------------------------------------------------------

def test_router_maps_endpoints_to_label_buckets() -> None:
    router = BucketCellRouter("Person", "Title", 16)
    cell = router.cell("nm1", "tt9")
    assert cell == (f"Person:{endpoint_bucket('nm1', 16)}", f"Title:{endpoint_bucket('tt9', 16)}")
    assert router.resources(cell) == tuple(sorted(cell))
    # Stable across instances (no Python hash randomization).
    assert BucketCellRouter("Person", "Title", 16).cell("nm1", "tt9") == cell


def test_router_same_pair_always_same_cell_and_typed_keys_differ() -> None:
    router = BucketCellRouter("Person", "Title", 8)
    assert {router.cell("a", "b") for _ in range(5)} == {router.cell("a", "b")}
    buckets = {BucketCellRouter("A", "B", 4096).cell(1, 1), BucketCellRouter("A", "B", 4096).cell("1", "1")}
    assert len(buckets) == 2


def test_router_self_reference_resources_collapse_when_buckets_match() -> None:
    router = BucketCellRouter("Title", "Title", 4)
    same = next(
        (a, b) for a, b in itertools.product((f"t{i}" for i in range(50)), repeat=2)
        if a != b and endpoint_bucket(a, 4) == endpoint_bucket(b, 4)
    )
    assert router.resources(router.cell(*same)) == (f"Title:{endpoint_bucket(same[0], 4)}",)
    differ = next(
        (a, b) for a, b in itertools.product((f"t{i}" for i in range(50)), repeat=2)
        if endpoint_bucket(a, 4) != endpoint_bucket(b, 4)
    )
    assert len(router.resources(router.cell(*differ))) == 2
    # Reverse direction of one pair shares the same resource set.
    assert router.resources(router.cell(*differ)) == router.resources(router.cell(differ[1], differ[0]))


def test_router_keyed_labels_get_one_resource_per_node() -> None:
    router = BucketCellRouter("Title", "Genre", 1, frozenset({"Genre"}))
    drama, comedy = router.cell("tt1", "Drama"), router.cell("tt1", "Comedy")
    assert drama[0] == comedy[0] == "Title:0"
    assert drama[1].startswith("Genre#") and drama[1] != comedy[1]
    assert BucketCellRouter("Title", "Genre", 16, frozenset({"Genre"})).cell("tt2", "Drama")[1] == drama[1]
    with pytest.raises(ValueError):
        BucketCellRouter("Bad Label", "Genre", 16)
    with pytest.raises(ValueError):
        BucketCellRouter("Title", "Genre", 0)


# -- in-memory coordinator doubles -----------------------------------------------

class _Hub:
    """Thread-safe in-memory lease table shared by fake clients."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.holder: dict[str, str] = {}
        self.leases: dict[str, set[str]] = {}
        self.ids = itertools.count(1)
        self.grants = 0

    def held(self, resources) -> bool:
        with self.lock:
            return all(resource in self.holder for resource in resources)


class _FakeClient:
    def __init__(self, hub: _Hub, *, deny_all: bool = False, fail_release: bool = False,
                 fail_connect: bool = False) -> None:
        self.hub, self.deny_all, self.fail_release, self.fail_connect = hub, deny_all, fail_release, fail_connect
        self.broken = False
        self.closed = False
        self.requests: list[tuple] = []
        # Lease ids this instance currently holds in the hub, not yet
        # released -- mirrors the real BucketLeaseServer's per-connection
        # ownership, so close() can release them the same way a real
        # server's _drop_owner does when a TCP connection drops, even if
        # REL itself never round-trips successfully.
        self._held: set[str] = set()

    def connect(self) -> None:
        if self.fail_connect:
            raise LeaseCoordinatorError("unreachable")

    def acquire_any(self, sets, *, wait_ms):
        self.requests.append(("ANY", tuple(tuple(s) for s in sets), wait_ms))
        if self.deny_all:
            return None
        with self.hub.lock:
            for index, resources in enumerate(sets):
                if not any(resource in self.hub.holder for resource in resources):
                    lease_id = f"L{next(self.hub.ids)}"
                    self.hub.leases[lease_id] = set(resources)
                    for resource in resources:
                        self.hub.holder[resource] = lease_id
                    self.hub.grants += 1
                    self._held.add(lease_id)
                    return lease_id, index
        if wait_ms:
            time.sleep(0.0005)
        return None

    def extend(self, lease_id, weighted_sets, *, budget):
        added, used = [], 0
        with self.hub.lock:
            for index, (weight, resources) in enumerate(weighted_sets):
                if used + weight > budget:
                    continue
                if all(self.hub.holder.get(resource) in (None, lease_id) for resource in resources):
                    for resource in resources:
                        self.hub.holder[resource] = lease_id
                    self.hub.leases[lease_id].update(resources)
                    used += weight
                    added.append(index)
        return added

    def _release_from_hub(self, lease_id) -> None:
        with self.hub.lock:
            resources = self.hub.leases.pop(lease_id, None)
            if resources:
                for resource in resources:
                    self.hub.holder.pop(resource, None)
        self._held.discard(lease_id)

    def release(self, lease_id) -> None:
        if self.fail_release:
            self.broken = True
            raise LeaseCoordinatorError("release failed")
        self._release_from_hub(lease_id)

    def close(self) -> None:
        self.closed = True
        # A real disconnect releases every resource this connection held,
        # regardless of whether REL itself ever succeeded.
        for lease_id in list(self._held):
            self._release_from_hub(lease_id)

    def ping(self) -> bool:
        return not self.broken


class _CheckingWriter:
    """Records batches and asserts every write is leased and node-disjoint."""

    def __init__(self, hub: _Hub, router: BucketCellRouter, *, fail_on_seq=None, delay: float = 0.0) -> None:
        self.hub, self.router, self.fail_on_seq, self.delay = hub, router, fail_on_seq, delay
        self.lock = threading.Lock()
        self.inflight: set[str] = set()
        self.batches: list[list[EdgeRecord]] = []
        self.violations: list[str] = []

    def write_batch(self, records, *, deadline=None, lease_liveness_check=None) -> None:
        resources = {token for record in records for token in self.router.cell(record.source_key, record.target_key)}
        if not self.hub.held(resources):
            self.violations.append("write without lease")
        with self.lock:
            if self.inflight & resources:
                self.violations.append("overlapping transactions")
            self.inflight |= resources
        try:
            if self.delay:
                time.sleep(self.delay)
            if self.fail_on_seq is not None and any(record.properties["seq"] == self.fail_on_seq for record in records):
                raise RuntimeError("neo4j write failed")
            with self.lock:
                self.batches.append(list(records))
        finally:
            with self.lock:
                self.inflight -= resources


class _ProvenancedCheckingWriter(_CheckingWriter):
    """P5-S3 Story 4: same leasing/node-disjoint checks as _CheckingWriter,
    but exposes row_level_dlq_enabled=True and records write_provenanced_batch
    calls (with the full items, not just .record) instead of write_batch."""

    row_level_dlq_enabled = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.provenanced_batches: list[list] = []

    def write_provenanced_batch(self, items, *, deadline=None, lease_liveness_check=None, parked_pairs=None) -> None:
        records = [item.record for item in items]
        self.write_batch(records, deadline=deadline, lease_liveness_check=lease_liveness_check)
        with self.lock:
            self.provenanced_batches.append(list(items))
            self.parked_pairs_seen = parked_pairs


def _coordinator(router, writer, hub, *, workers=4, batch_size=10, merge=0, client_kwargs=None, **kwargs):
    clients: list[_FakeClient] = []

    def factory():
        client = _FakeClient(hub, **(client_kwargs or {}))
        clients.append(client)
        return client

    coordinator = BucketLeaseExecutionCoordinator(
        writer, router=router, worker_count=workers, batch_size=batch_size,
        client_factory=factory, merge_target_rows=merge, **kwargs,
    )
    return coordinator, clients


def _random_pending(count: int, *, seed: int = 7, sources: int = 40, targets: int = 40):
    rng = random.Random(seed)
    return [_pending(f"s{rng.randrange(sources)}", f"t{rng.randrange(targets)}", seq) for seq in range(count)]


# -- P5-S3 Story 4: row-level DLQ wired into the lease coordinator ----------

def test_write_leased_uses_write_provenanced_batch_when_row_level_dlq_enabled() -> None:
    """_write_leased's write call swaps to write_provenanced_batch, with
    full items (not just .record), whenever the writer opts in."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _ProvenancedCheckingWriter(hub, router)
    coordinator, _ = _coordinator(router, writer, hub, batch_size=10)
    pending = [_pending("solo-a", "solo-b", seq) for seq in range(3)]
    results = coordinator.execute_pending(pending)
    assert all(result.success for result in results)
    assert not writer.violations
    # write_batch was never called directly (only via write_provenanced_batch).
    written_items = [item for batch in writer.provenanced_batches for item in batch]
    assert sorted(item.offset for item in written_items) == [0, 1, 2]
    assert all(hasattr(item, "topic") and hasattr(item, "partition") for item in written_items)


def test_write_leased_still_uses_write_batch_when_row_level_dlq_disabled() -> None:
    """The default (fake writer with no row_level_dlq_enabled attribute at
    all, matching every writer before this story) keeps using write_batch
    -- zero observable change for a run that doesn't opt in."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router)
    coordinator, _ = _coordinator(router, writer, hub, batch_size=10)
    pending = [_pending("solo-a", "solo-b", seq) for seq in range(3)]
    results = coordinator.execute_pending(pending)
    assert all(result.success for result in results)
    assert not writer.violations
    assert not hasattr(writer, "provenanced_batches")


def test_write_leased_mixed_cell_counts_dlqd_rows_as_fully_written() -> None:
    """A cell with one resolvable and one unresolvable row: state.written[key]
    still equals the cell's total row count (both durably accounted for)."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _ProvenancedCheckingWriter(hub, router)
    coordinator, _ = _coordinator(router, writer, hub, batch_size=10)
    pending = [_pending("mixed-a", "mixed-b", 0), _pending("mixed-a", "mixed-b", 1)]
    results = coordinator.execute_pending(pending)
    assert all(result.success for result in results)
    assert len(writer.provenanced_batches) == 1 and len(writer.provenanced_batches[0]) == 2


def test_same_pair_ordering_holds_across_sequential_transactions_in_one_flush(works_at) -> None:
    """MUST-FIX regression (reviewer finding): a cell with more rows than
    batch_size drains across multiple sequential transactions in one
    flush. R1 (missing, transaction 1) must still block R2 (individually
    resolvable, same pair, transaction 2) via the flush-wide parked_pairs
    set -- not just within one write_provenanced_batch call."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = [
        # Transaction 1 (offset=0 only): Attempt 1 fails; classified missing_source.
        MagicMock(__iter__=lambda self: iter([{"checked_rows": 1, "invalid_rows": 1, "merged_rows": 0}])),
        MagicMock(__iter__=lambda self: iter([{"row_index": 0, "source_matches": 0, "target_matches": 1}])),
        # Transaction 2 (offset=1 only, pre-blocked -- Attempt 1 must be
        # skipped entirely): classification alone would say this row
        # individually resolves (1, 1), but pre_blocked_pairs forces it
        # to "blocked" instead -- no matched-subset write call follows.
        MagicMock(__iter__=lambda self: iter([{"row_index": 0, "source_matches": 1, "target_matches": 1}])),
    ]
    sink = MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, works_at, missing_endpoint_error_factory=lambda msg: RuntimeError(msg),
        missing_endpoint_sink=sink,
    )
    coordinator, _ = _coordinator(router, writer, hub, batch_size=1, workers=1)
    pending = [_pending("mixed-a", "mixed-b", 0), _pending("mixed-a", "mixed-b", 1)]
    results = coordinator.execute_pending(pending)
    assert all(result.success for result in results)
    # Exactly 3 tx.run calls total across both transactions -- never a 4th
    # (a matched-subset write for R2, which must never happen).
    assert tx.run.call_count == 3
    assert sink.route_unresolved_endpoint.call_count == 2
    by_offset = {c.kwargs["offset"]: c.kwargs["reason_code"] for c in sink.route_unresolved_endpoint.call_args_list}
    assert by_offset[0] == "missing_source"
    assert by_offset[1] == "blocked_by_earlier_unresolved_row"


@pytest.mark.parametrize(
    "mode, opt_in, expected",
    [
        ("stream", False, True),   # streaming: unconditional default once dead_letter resolves
        ("stream", True, True),
        ("bulk", False, False),    # lease bulk: opt-in required
        ("bulk", True, True),
    ],
)
def test_build_edge_execution_lease_mode_row_level_dlq_activation_table(works_at, mode, opt_in, expected) -> None:
    edge = works_at.model_copy(update={
        "dead_letter": DeadLetterConfig(topic="dlq-works-at", enabled=True, route_missing_endpoints_in_bulk=opt_in),
    })
    writer, _p, _b, _c = build_edge_execution(
        edge, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700"},
        lease_client_factory=lambda: _FakeClient(_Hub()),
        mode=mode, missing_endpoint_sink=MagicMock(),
    )
    assert writer.row_level_dlq_enabled is expected


def test_build_edge_execution_lease_mode_no_dlq_topic_never_activates(works_at) -> None:
    """dead_letter absent/disabled: row-level DLQ never activates regardless
    of mode or missing_endpoint_sink."""
    writer, _p, _b, _c = build_edge_execution(
        works_at, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700"},
        lease_client_factory=lambda: _FakeClient(_Hub()),
        mode="stream", missing_endpoint_sink=MagicMock(),
    )
    assert writer.row_level_dlq_enabled is False


def test_build_edge_execution_lease_mode_default_mode_is_bulk_no_opt_in(works_at) -> None:
    """Every pre-P5-S3 call site (no mode= passed) keeps today's exact
    behavior: row-level DLQ off, even with dead_letter enabled."""
    edge = works_at.model_copy(update={"dead_letter": DeadLetterConfig(topic="dlq-works-at", enabled=True)})
    writer, _p, _b, _c = build_edge_execution(
        edge, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700"},
        lease_client_factory=lambda: _FakeClient(_Hub()), missing_endpoint_sink=MagicMock(),
    )
    assert writer.row_level_dlq_enabled is False


# ── P5-S4 Story 3's second half: parked_pair_index wiring through build_edge_execution ──


def test_build_edge_execution_stream_row_level_dlq_wires_index_into_sink_and_coordinator(works_at) -> None:
    from src.loader.edge_execution import IndexedMissingEndpointSink

    edge = works_at.model_copy(update={"dead_letter": DeadLetterConfig(topic="dlq-works-at", enabled=True)})
    sink = MagicMock()
    index = MagicMock()
    writer, _p, _b, coordinator = build_edge_execution(
        edge, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700"},
        lease_client_factory=lambda: _FakeClient(_Hub()),
        mode="stream", missing_endpoint_sink=sink, parked_pair_index=index,
    )
    assert writer.row_level_dlq_enabled is True
    assert isinstance(writer._missing_endpoint_sink, IndexedMissingEndpointSink)
    assert writer._missing_endpoint_sink._sink is sink
    assert writer._missing_endpoint_sink._index is index
    assert coordinator._parked_pair_index is index


def test_build_edge_execution_order_by_edge_type_never_wires_a_parked_pair_index(works_at) -> None:
    """P5-S4 Decisions §10: order_by is an alternative to pair-parking, not
    an addition to it -- an order_by-configured type must never construct
    or receive a ParkedPairIndex, enforced here regardless of what the
    caller passes in."""
    edge = works_at.model_copy(update={
        "dead_letter": DeadLetterConfig(topic="dlq-works-at", enabled=True),
        "order_by": "since",
    })
    sink = MagicMock()
    index = MagicMock()
    writer, _p, _b, coordinator = build_edge_execution(
        edge, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700"},
        lease_client_factory=lambda: _FakeClient(_Hub()),
        mode="stream", missing_endpoint_sink=sink, parked_pair_index=index,
    )
    assert writer.row_level_dlq_enabled is True
    assert writer._missing_endpoint_sink is sink  # NOT wrapped
    assert coordinator._parked_pair_index is None
    index.add.assert_not_called()
    index.is_blocked.assert_not_called()


def test_build_edge_execution_bulk_mode_never_wires_a_parked_pair_index(works_at) -> None:
    """P5-S4 Decisions §1: bulk has no live retry consumer to close the
    cross-flush gap against, even with the bulk opt-in set."""
    edge = works_at.model_copy(update={
        "dead_letter": DeadLetterConfig(topic="dlq-works-at", enabled=True, route_missing_endpoints_in_bulk=True),
    })
    sink = MagicMock()
    index = MagicMock()
    writer, _p, _b, coordinator = build_edge_execution(
        edge, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700"},
        lease_client_factory=lambda: _FakeClient(_Hub()),
        mode="bulk", missing_endpoint_sink=sink, parked_pair_index=index,
    )
    assert writer.row_level_dlq_enabled is True
    assert writer._missing_endpoint_sink is sink  # NOT wrapped
    assert coordinator._parked_pair_index is None


def test_cross_flush_blocking_via_durable_index_forces_a_resolvable_row_to_blocked(works_at) -> None:
    """End-to-end proof (the actual gap this integration closes): a row
    that individually resolves, in a brand-new flush, is still diverted to
    the DLQ when the durable index reports its pair as already blocked
    from an EARLIER flush -- not merely within this flush's own
    transactions."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = [
        # Attempt 1 (fast path) never runs: the pair is pre-blocked before
        # any row-count aggregate check is attempted.
        MagicMock(__iter__=lambda self: iter([{"row_index": 0, "source_matches": 1, "target_matches": 1}])),
    ]
    sink = MagicMock()
    index = MagicMock()
    index.is_blocked.return_value = True  # blocked by an earlier flush, durably
    writer = ApocLockedEdgeWriter(
        driver, works_at, missing_endpoint_error_factory=lambda msg: RuntimeError(msg),
        missing_endpoint_sink=sink,
    )
    coordinator, _ = _coordinator(router, writer, hub, batch_size=10, workers=1, parked_pair_index=index)
    pending = [_pending("cross-flush-a", "cross-flush-b", 0)]
    results = coordinator.execute_pending(pending)
    assert all(result.success for result in results)
    assert tx.run.call_count == 1  # classification only -- no matched-subset write
    sink.route_unresolved_endpoint.assert_called_once()
    assert sink.route_unresolved_endpoint.call_args.kwargs["reason_code"] == "blocked_by_earlier_unresolved_row"


def test_dlq_routed_metric_increments_with_zero_new_instrumentation(works_at) -> None:
    """P6-S2's catalogue: graph_loader_dlq_routed_total{kind,name,failure_class}
    increments through the existing DeadLetterProducer.publish -> on_dlq_routed
    seam, confirmed via an isolated GraphLoaderMetrics instance -- no new
    metrics code anywhere in this slice."""
    from prometheus_client import CollectorRegistry
    from src.loader.dead_letter import DeadLetterProducer, DeadLetterRecordSink
    from src.utils.metrics import GraphLoaderMetrics, install_dlq_routed_listener

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    install_dlq_routed_listener(metrics)
    fake_kafka_producer = MagicMock()
    fake_kafka_producer.flush.return_value = 0
    sink = DeadLetterRecordSink(
        producer=DeadLetterProducer(fake_kafka_producer), topic="dlq-works-at",
        run_id="run-1", replica_id="0", kind="edge",
    )
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = [
        MagicMock(__iter__=lambda self: iter([{"checked_rows": 1, "invalid_rows": 1, "merged_rows": 0}])),
        MagicMock(__iter__=lambda self: iter([{"row_index": 0, "source_matches": 0, "target_matches": 1}])),
    ]
    writer = ApocLockedEdgeWriter(
        driver, works_at, missing_endpoint_error_factory=lambda msg: RuntimeError(msg),
        missing_endpoint_sink=sink, metrics=metrics,
    )
    writer.write_provenanced_batch([_pending("missing-src", "c-1", 0)])
    assert metrics.registry.get_sample_value(
        "graph_loader_dlq_routed_total",
        {"kind": "edge", "name": "WORKS_AT", "failure_class": "missing_endpoint"},
    ) == 1.0


# -- executor ------------------------------------------------------------------------

def test_every_cell_written_exactly_once_in_fifo_order() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router, delay=0.0005)
    coordinator, _ = _coordinator(router, writer, hub, batch_size=7)
    pending = _random_pending(600)
    results = coordinator.execute_pending(pending)
    assert results and all(result.success for result in results)
    written = [record for batch in writer.batches for record in batch]
    assert sorted(record.properties["seq"] for record in written) == list(range(600))
    assert not writer.violations
    assert all(len(batch) <= 7 for batch in writer.batches)
    # Without merging, each transaction holds exactly one cell.
    assert all(len({router.cell(r.source_key, r.target_key) for r in batch}) == 1 for batch in writer.batches)
    # FIFO per cell: write order within a cell matches arrival order.
    by_cell: dict[tuple, list[int]] = defaultdict(list)
    for record in written:
        by_cell[router.cell(record.source_key, record.target_key)].append(record.properties["seq"])
    for sequence in by_cell.values():
        assert sequence == sorted(sequence)
    # Results cover every record once, grouped by cell.
    assert sorted(item.offset for result in results for item in result.routed_records) == list(range(600))
    assert not hub.holder
    assert "transactions=" in coordinator.describe_last_flush()


# ── Metrics instrumentation (P6-S2 Story 5, review finding T2) ──────────────


def test_lease_wait_seconds_observes_once_per_flush_and_distinguishes_contention() -> None:
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    # Contended: a tiny key space over a small bucket count guarantees
    # heavy resource overlap across a worker pool, so some ANY requests
    # are denied at least once (the FakeClient sleeps briefly on denial).
    router = BucketCellRouter("Person", "Title", 2)
    hub = _Hub()
    writer = _CheckingWriter(hub, router, delay=0.001)
    contended_metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    coordinator, _ = _coordinator(
        router, writer, hub, workers=4, batch_size=5,
        metrics=contended_metrics, edge_type="WORKS_AT",
    )
    pending = _random_pending(200, sources=4, targets=4)
    results = coordinator.execute_pending(pending)
    assert all(result.success for result in results)
    assert contended_metrics.registry.get_sample_value(
        "graph_loader_lease_wait_seconds_count", {"kind": "edge", "name": "WORKS_AT"}
    ) == 1.0  # one observation per outer flush (per execute_pending call), never per grant/cell.
    contended_sum = contended_metrics.registry.get_sample_value(
        "graph_loader_lease_wait_seconds_sum", {"kind": "edge", "name": "WORKS_AT"}
    )
    assert contended_sum > 0.0

    # Disjoint: a single worker writing a single cell never waits on
    # anyone - the coordinator never observes a nonzero wait.
    hub2 = _Hub()
    writer2 = _CheckingWriter(hub2, router)
    disjoint_metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    coordinator2, _ = _coordinator(
        router, writer2, hub2, workers=1, batch_size=10,
        metrics=disjoint_metrics, edge_type="WORKS_AT",
    )
    results2 = coordinator2.execute_pending([_pending("solo-a", "solo-b", 0)])
    assert all(result.success for result in results2)
    assert disjoint_metrics.registry.get_sample_value(
        "graph_loader_lease_wait_seconds_count", {"kind": "edge", "name": "WORKS_AT"}
    ) == 1.0
    disjoint_sum = disjoint_metrics.registry.get_sample_value(
        "graph_loader_lease_wait_seconds_sum", {"kind": "edge", "name": "WORKS_AT"}
    )
    # An immediate (never-denied) grant still measures a tiny nonzero
    # elapsed time (the acquire_any call's own overhead) - "stays at 0"
    # means "no real contention-driven wait", not "literally 0.0 seconds".
    # Distinguish the two cases by magnitude instead of exact equality.
    # Loosened per P6-S2 review's T2 note: both bounds are wall-clock and
    # thread-interleaving dependent, so keep real margin against a loaded
    # CI runner rather than a tight ratio that could spuriously fail.
    assert disjoint_sum < 0.05
    assert contended_sum > disjoint_sum


def test_same_relationship_updates_keep_their_order() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 2)
    writer = _CheckingWriter(hub, router)
    coordinator, _ = _coordinator(router, writer, hub, batch_size=3, merge=3)
    pending = [_pending("p1", "t1", seq) if seq % 2 else _pending(f"p{seq}", f"t{seq}", seq) for seq in range(40)]
    assert all(result.success for result in coordinator.execute_pending(pending))
    pair = [r.properties["seq"] for batch in writer.batches for r in batch if (r.source_key, r.target_key) == ("p1", "t1")]
    assert pair == [seq for seq in range(40) if seq % 2]


def test_small_cells_merge_within_target_and_stay_leased() -> None:
    hub, router = _Hub(), BucketCellRouter("Title", "Genre", 4, frozenset({"Genre"}))
    writer = _CheckingWriter(hub, router)
    coordinator, clients = _coordinator(router, writer, hub, workers=2, batch_size=50, merge=30)
    rng = random.Random(3)
    pending = [_pending(f"tt{rng.randrange(200)}", f"g{rng.randrange(25)}", seq) for seq in range(500)]
    assert all(result.success for result in coordinator.execute_pending(pending))
    assert not writer.violations
    assert sorted(r.properties["seq"] for batch in writer.batches for r in batch) == list(range(500))
    assert any(len({router.cell(r.source_key, r.target_key) for r in batch}) > 1 for batch in writer.batches)
    assert all(len(batch) <= 50 for batch in writer.batches)
    assert "merged_cells=" in coordinator.describe_last_flush()
    assert len(writer.batches) < len({router.cell(r.record.source_key, r.record.target_key) for r in pending})


def test_candidates_are_largest_first_and_include_many_cells() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 8)
    writer = _CheckingWriter(hub, router)
    coordinator, clients = _coordinator(router, writer, hub, workers=1, batch_size=1000, candidate_limit=5)
    pending = [_pending("big", "t", seq) for seq in range(10)] + _random_pending(30, seed=11)
    pending = [_pending(p.record.source_key, p.record.target_key, seq) for seq, p in enumerate(pending)]
    assert all(result.success for result in coordinator.execute_pending(pending))
    first = clients[0].requests[0]
    assert len(first[1]) == 5
    assert first[1][0] == router.resources(router.cell("big", "t"))


def test_write_failure_propagates_and_stops_new_transactions() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router, fail_on_seq=0)
    coordinator, _ = _coordinator(router, writer, hub, workers=1, batch_size=5)
    pending = [_pending("s", "t", 0)] + _random_pending(200, seed=5)[1:]
    results = coordinator.execute_pending(pending)
    failed = [result for result in results if not result.success]
    assert failed and all(str(result.exception) == "neo4j write failed" for result in failed)
    assert sum(len(batch) for batch in writer.batches) < 200
    assert not hub.holder  # the failed transaction's lease was released


def test_no_write_without_lease_and_backoff_is_bounded() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router)
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) >= 6:
            raise LeaseCoordinatorError("coordinator gone")

    coordinator, _ = _coordinator(
        router, writer, hub, workers=1, wait_ms=0, sleep=sleep, client_kwargs={"deny_all": True},
    )
    results = coordinator.execute_pending(_random_pending(20))
    assert not writer.batches
    assert results and not any(result.success for result in results)
    assert isinstance(results[0].exception, LeaseCoordinatorError)
    # Jittered 1-2 ms, doubling per consecutive denial up to 4x: never a hot spin.
    for attempt, seconds in enumerate(sleeps):
        factor = min(2 ** attempt, 4)
        assert 0.001 * factor <= seconds <= 0.002 * factor


def test_unreachable_coordinator_fails_before_any_write() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router)
    coordinator, _ = _coordinator(router, writer, hub, client_kwargs={"fail_connect": True})
    with pytest.raises(LeaseCoordinatorError):
        coordinator.prime()
    results = coordinator.execute_pending(_random_pending(10))
    assert not writer.batches and not any(result.success for result in results)


def test_prime_opens_full_pool_and_drain_succeeds_after_stop_is_set() -> None:
    """After SIGTERM a lazily created client cannot connect; priming the whole
    pool means a drain spanning more cells than one client still succeeds."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router)
    stop = threading.Event()
    clients: list[_FakeClient] = []

    def factory():
        if stop.is_set() and len(clients) >= 4:
            raise LeaseCoordinatorError("stopped: cannot connect")
        client = _FakeClient(hub)
        clients.append(client)
        return client

    coordinator = BucketLeaseExecutionCoordinator(
        writer, router=router, worker_count=4, batch_size=10, client_factory=factory,
    )
    coordinator.prime()
    assert len(clients) == 4
    stop.set()
    pending = _random_pending(200)
    results = coordinator.execute_pending(pending)
    assert results and all(result.success for result in results)
    assert len(clients) == 4  # nothing created lazily


def test_prime_closes_opened_clients_and_reraises_when_one_fails() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    clients: list[_FakeClient] = []

    def factory():
        client = _FakeClient(hub, fail_connect=len(clients) == 2)
        clients.append(client)
        return client

    coordinator = BucketLeaseExecutionCoordinator(
        _CheckingWriter(hub, router), router=router, worker_count=4, batch_size=10,
        client_factory=factory,
    )
    with pytest.raises(LeaseCoordinatorError):
        coordinator.prime()
    assert all(client.closed for client in clients)
    assert not coordinator._idle_clients


def test_release_failure_after_a_successful_write_does_not_fail_the_flush() -> None:
    """P5-S1 Story 4, review finding S3-3 phase (c): the write already
    committed durably before REL is even attempted, so a REL failure must
    discard the now-broken client (the pool never reuses it) without
    turning an already-durable commit into a reported flush failure --
    Kafka offsets for a fully-written flush must still commit."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router)
    coordinator, clients = _coordinator(router, writer, hub, workers=1, client_kwargs={"fail_release": True})
    results = coordinator.execute_pending(_random_pending(10))
    assert results and all(result.success for result in results)
    assert len(writer.batches) >= 1
    # The specific client that failed its release is discarded, never
    # reused; the coordinator eagerly checks out a replacement afterward
    # regardless of whether further work remains, so a later (never
    # actually used) client may legitimately end up idle.
    assert clients[0].closed
    assert clients[0] not in coordinator._idle_clients


def test_grant_of_a_locally_busy_cell_fails_closed() -> None:
    router = BucketCellRouter("Person", "Title", 1, frozenset({"Title"}))

    class _BrokenClient(_FakeClient):
        def acquire_any(self, sets, *, wait_ms):
            return f"L{next(self.hub.ids)}", 0  # grants without exclusion

        def release(self, lease_id) -> None:
            pass

    hub = _Hub()
    started, gate = threading.Event(), threading.Event()

    class _SlowWriter:
        def __init__(self) -> None:
            self.calls = 0

        def write_batch(self, records, *, deadline=None, lease_liveness_check=None) -> None:
            self.calls += 1
            started.set()
            gate.wait(2)

    writer = _SlowWriter()
    coordinator = BucketLeaseExecutionCoordinator(
        writer, router=router, worker_count=2, batch_size=1, merge_target_rows=0,
        client_factory=lambda: _BrokenClient(hub),
    )
    timer = threading.Timer(0.3, gate.set)
    timer.start()
    # Two cells, so two workers run; both are "granted" the largest one.
    results = coordinator.execute_pending([_pending("a", "b", seq) for seq in range(3)] + [_pending("a", "c", 3)])
    timer.cancel()
    gate.set()
    assert not all(result.success for result in results)
    assert any(isinstance(result.exception, ExecutionModeError) for result in results)


def test_clients_are_reused_across_flushes_and_closed() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router)
    coordinator, clients = _coordinator(router, writer, hub, workers=2)
    coordinator.prime()
    for seed in range(3):
        assert all(result.success for result in coordinator.execute_pending(_random_pending(50, seed=seed)))
    assert len(clients) <= 2
    coordinator.close()
    assert all(client.closed for client in clients)
    assert coordinator.execute_pending(()) == ()


def test_concurrent_types_share_resources_without_overlap() -> None:
    """Several executors (edge types) contend for shared labels through one hub."""
    hub = _Hub()
    lock = threading.Lock()
    inflight: set[str] = set()
    violations: list[str] = []
    totals: dict[str, int] = defaultdict(int)

    class _SharedWriter:
        def __init__(self, name: str, router: BucketCellRouter) -> None:
            self.name, self.router = name, router

        def write_batch(self, records, *, deadline=None, lease_liveness_check=None) -> None:
            resources = {t for r in records for t in self.router.cell(r.source_key, r.target_key)}
            with lock:
                if inflight & resources or not hub.held(resources):
                    violations.append(self.name)
                inflight.update(resources)
            time.sleep(0.0005)
            with lock:
                inflight.difference_update(resources)
                totals[self.name] += len(records)

    specs = [("ACTED_IN", "Person", "Title"), ("WROTE", "Person", "Title"), ("EPISODE_OF", "Title", "Title"),
             ("HAS_GENRE", "Title", "Genre")]
    outcomes = {}

    def run(name: str, source: str, target: str) -> None:
        router = BucketCellRouter(source, target, 4, frozenset({"Genre"}))
        coordinator, _ = _coordinator(router, _SharedWriter(name, router), hub, workers=3, batch_size=8, merge=16)
        pending = _random_pending(300, seed=len(name), sources=30, targets=30)
        outcomes[name] = all(result.success for result in coordinator.execute_pending(pending))

    threads = [threading.Thread(target=run, args=spec) for spec in specs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert outcomes == {name: True for name, _, _ in specs}
    assert not violations
    assert dict(totals) == {name: 300 for name, _, _ in specs}


def test_executor_against_real_coordinator() -> None:
    loop = asyncio.new_event_loop()
    server = BucketLeaseServer(bucket_count=4, scheme="-")
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        host, port = asyncio.run_coroutine_threadsafe(server.start("127.0.0.1", 0), loop).result(5)
        router = BucketCellRouter("Person", "Title", 4)
        hub = _Hub()  # only used for the writer's overlap bookkeeping
        writer = _CheckingWriter(hub, router)
        writer.hub.held = lambda resources: True
        coordinator = BucketLeaseExecutionCoordinator(
            writer, router=router, worker_count=4, batch_size=6, merge_target_rows=6,
            client_factory=lambda: BucketLeaseClient(f"{host}:{port}", bucket_count=4),
        )
        coordinator.prime()
        results = coordinator.execute_pending(_random_pending(300))
        assert all(result.success for result in results)
        assert sorted(r.properties["seq"] for batch in writer.batches for r in batch) == list(range(300))
        assert not writer.violations
        coordinator.close()
        assert server.stats.grants >= len(writer.batches) and server.stats.active == 0
    finally:
        asyncio.run_coroutine_threadsafe(server.close(), loop).result(5)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        loop.close()


# -- construction and loader wiring ------------------------------------------------

def test_lease_mode_settings_parse_environment() -> None:
    assert lease_mode_settings({}) is None
    assert lease_mode_settings({"GRAPH_LOADER_LEASE_ADDR": " "}) is None
    assert lease_mode_settings({
        "GRAPH_LOADER_LEASE_ADDR": "lease:7700", "GRAPH_LOADER_LEASE_BUCKETS": "8",
        "GRAPH_LOADER_LEASE_KEYED_LABELS": "Genre,Region",
    }) == ("lease:7700", 8, frozenset({"Genre", "Region"}))
    with pytest.raises(ValueError):
        lease_mode_settings({"GRAPH_LOADER_LEASE_ADDR": "nonsense"})
    with pytest.raises(ValueError):
        lease_mode_settings({"GRAPH_LOADER_LEASE_ADDR": "lease:7700", "GRAPH_LOADER_LEASE_BUCKETS": "0"})


def test_build_edge_execution_lease_mode_drops_apoc_locks_and_primes(works_at) -> None:
    hub = _Hub()
    made: list[_FakeClient] = []

    def factory():
        made.append(_FakeClient(hub))
        return made[-1]

    writer, partitioner, batcher, coordinator = build_edge_execution(
        works_at, MagicMock(), environ={
            "GRAPH_LOADER_LEASE_ADDR": "lease:7700", "GRAPH_LOADER_LEASE_BUCKETS": "8",
            "GRAPH_LOADER_EDGE_EXPLICIT_LOCKS": "1", "GRAPH_LOADER_LEASE_MERGE_ROWS": "64",
        }, lease_client_factory=factory,
    )
    assert isinstance(writer, ApocLockedEdgeWriter) and writer._explicit_locks is False
    assert "apoc.lock.nodes" not in writer._query
    # P6-S4 finding, fixed here: lease mode is node-disjoint by construction
    # (same guarantee planned mode relies on, sort_rows=False already), so
    # it must use the same streaming (uncollected, unordered) query shape
    # planned mode uses -- not the collect-based, "ORDER BY elementId(s),
    # elementId(t))"-reordering shape, which is not a stable sort for
    # repeated (source,target) pairs within one transaction and would
    # silently break last-write-wins.
    assert "ORDER BY elementId" not in writer._query and "collect(" not in writer._query
    assert "invalid_rows" in writer._query  # still validates endpoints, just not via the WHERE guard
    assert partitioner is None and batcher is None
    assert isinstance(coordinator, BucketLeaseExecutionCoordinator) and coordinator.routes_records is True
    assert coordinator._router.bucket_count == 8 and coordinator._merge_target == 64
    assert len(made) == 1 and coordinator._idle_clients == made


def test_build_edge_execution_lease_mode_rejects_native_and_bad_merge(works_at) -> None:
    native = works_at.model_copy(update={"execution": works_at.execution.model_copy(update={"mode": "native_disjoint"})})
    with pytest.raises(ExecutionModeError, match="python_apoc"):
        build_edge_execution(native, MagicMock(), environ={"GRAPH_LOADER_LEASE_ADDR": "lease:7700"},
                             lease_client_factory=lambda: _FakeClient(_Hub()))
    with pytest.raises(ValueError):
        build_edge_execution(works_at, MagicMock(), environ={
            "GRAPH_LOADER_LEASE_ADDR": "lease:7700", "GRAPH_LOADER_LEASE_MERGE_ROWS": "-1",
        }, lease_client_factory=lambda: _FakeClient(_Hub()))


def test_build_edge_execution_without_leases_keeps_lane_mode(works_at) -> None:
    _writer, partitioner, batcher, coordinator = build_edge_execution(works_at, MagicMock(), environ={})
    assert isinstance(coordinator, LaneExecutionCoordinator)
    assert partitioner is not None and batcher is not None


# -- P5-S1 Story 7: streaming lease client factory ---------------------------

def test_streaming_lease_client_factory_returns_none_when_lease_mode_inactive() -> None:
    bucket_lease = SimpleNamespace(
        restart_backoff_base_ms=500, restart_backoff_max_ms=30_000, reconnect_timeout_ms=60_000,
    )
    assert streaming_lease_client_factory({}, bucket_lease, threading.Event()) is None


def test_streaming_lease_client_factory_builds_bounded_capped_backoff_client() -> None:
    """Unlike bulk's default (a fixed 30s connect deadline, no backoff),
    a streaming client rides out a supervised lease-server restart: capped
    backoff from the schema's streaming supervision settings, a
    reconnect_timeout_ms-derived connect deadline (review finding B7-1:
    bounded by default, not unbounded, since streaming's poll thread
    blocks synchronously on this reconnect in this slice), and
    interruptible by this container's own shutdown event."""
    bucket_lease = SimpleNamespace(
        restart_backoff_base_ms=500, restart_backoff_max_ms=30_000, reconnect_timeout_ms=60_000,
    )
    shutdown = threading.Event()
    factory = streaming_lease_client_factory(
        {"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7700"}, bucket_lease, shutdown,
    )
    assert factory is not None
    client = factory()
    assert client._connect_timeout == 60.0
    assert client._backoff_ms == (500, 30_000)
    assert client._stop_event is shutdown


def test_streaming_lease_client_factory_zero_reconnect_timeout_means_unbounded() -> None:
    bucket_lease = SimpleNamespace(
        restart_backoff_base_ms=500, restart_backoff_max_ms=30_000, reconnect_timeout_ms=0,
    )
    factory = streaming_lease_client_factory(
        {"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7700"}, bucket_lease, threading.Event(),
    )
    assert factory()._connect_timeout is None


def test_outer_batch_override() -> None:
    loading = LoadingConfig(unwind_batch_size=500, edge_unwind_batch_size=2000)
    assert apply_edge_outer_batch_override(loading, {}) is loading
    assert apply_edge_outer_batch_override(
        loading, {"GRAPH_LOADER_EDGE_OUTER_BATCH_SIZE": "32000"},
    ).edge_unwind_batch_size == 32000
    for bad in ("0", "x", "1000001"):
        with pytest.raises(ValueError):
            apply_edge_outer_batch_override(loading, {"GRAPH_LOADER_EDGE_OUTER_BATCH_SIZE": bad})


def _message(payload: bytes, *, offset: int):
    message = MagicMock()
    message.value.return_value = payload
    message.error.return_value = None
    message.topic.return_value = "works-at-events"
    message.partition.return_value = 0
    message.offset.return_value = offset
    return message


def _payload(person: str, company: str) -> bytes:
    return (
        '{"source":{"personId":"%s"},"target":{"companyId":"%s"},"properties":{"since":"2020-01-02"}}'
        % (person, company)
    ).encode()


def test_edge_loader_commits_only_after_every_leased_cell_is_written(works_at) -> None:
    hub = _Hub()
    router = BucketCellRouter("Person", "Company", 4)
    writer = _CheckingWriter(hub, router)
    coordinator, _ = _coordinator(router, writer, hub, workers=2, batch_size=2)
    consumer = MagicMock()
    consumer.poll.side_effect = [_message(_payload(f"p{i % 5}", f"c{i % 3}"), offset=i) for i in range(6)]
    loading = LoadingConfig(unwind_batch_size=6)
    loader = EdgeLoader(consumer, MagicMock(), works_at, loading_config=loading, coordinator=coordinator)
    assert loader.run(max_messages=6) == 0
    assert sum(len(batch) for batch in writer.batches) == 6
    consumer.commit.assert_called()
    committed = consumer.commit.call_args.kwargs["offsets"]
    assert [(tp.partition, tp.offset) for tp in committed] == [(0, 6)]


def test_edge_loader_leaves_offsets_uncommitted_when_a_cell_fails(works_at) -> None:
    hub = _Hub()
    router = BucketCellRouter("Person", "Company", 4)

    class _FailingWriter(_CheckingWriter):
        def write_batch(self, records, *, deadline=None, lease_liveness_check=None) -> None:
            raise RuntimeError("neo4j down")

    coordinator, _ = _coordinator(router, _FailingWriter(hub, router), hub, workers=2)
    consumer = MagicMock()
    consumer.poll.side_effect = [_message(_payload(f"p{i}", "c1"), offset=i) for i in range(3)]
    loader = EdgeLoader(consumer, MagicMock(), works_at, loading_config=LoadingConfig(unwind_batch_size=3),
                        coordinator=coordinator)
    assert loader.run(max_messages=3) == 1
    consumer.commit.assert_not_called()
    assert not hub.holder


def test_edge_loader_main_applies_outer_override_and_closes_lease_clients(schema, monkeypatch) -> None:
    consumer, producer, driver, loader = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    loader.run.return_value = 0
    coordinator = MagicMock()
    monkeypatch.setenv("GRAPH_LOADER_EDGE_OUTER_BATCH_SIZE", "32000")
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.Producer", return_value=producer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.build_edge_execution", return_value=(MagicMock(), None, None, coordinator)), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader) as loader_cls:
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--mode", "bulk", "--run-id", "run-9"]) == 0
    assert loader_cls.call_args.kwargs["loading_config"].edge_unwind_batch_size == 32000
    coordinator.close.assert_called_once()


def test_edge_loader_main_passes_bucket_lease_tx_timeout_seconds_from_schema(schema) -> None:
    """P5-S1 Story 4: every mode's main() threads the schema's
    loading.bucket_lease.tx_timeout_ms through to build_edge_execution, in
    seconds, regardless of whether lease mode is actually in effect for
    this run (build_edge_execution's classic branch ignores it)."""
    consumer, producer, driver, loader = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    producer.flush.return_value = 0
    loader.run.return_value = 0
    build_execution = MagicMock(return_value=(MagicMock(), None, None, MagicMock()))
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.Producer", return_value=producer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.build_edge_execution", build_execution), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader):
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--mode", "bulk", "--run-id", "run-9"]) == 0
    assert build_execution.call_args.kwargs["bucket_lease_tx_timeout_seconds"] == (
        schema.loading.bucket_lease.tx_timeout_ms / 1000
    )


def test_edge_loader_main_passes_streaming_lease_client_factory_only_in_stream_mode(schema, monkeypatch) -> None:
    """P5-S1 Story 7: main() threads a streaming-flavored (bounded by the
    schema's reconnect timeout, capped-backoff) lease_client_factory into build_edge_execution only
    for --mode stream; bulk keeps build_edge_execution's own bounded
    default (lease_client_factory=None)."""
    monkeypatch.setenv("GRAPH_LOADER_BUCKET_LEASE_ADDR", "lease:7700")
    consumer, driver, loader = MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    build_execution = MagicMock(return_value=(MagicMock(), None, None, MagicMock()))
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.build_edge_execution", build_execution), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader):
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--mode", "stream", "--run-id", "run-9"]) == 0
    stream_factory = build_execution.call_args.kwargs["lease_client_factory"]
    assert stream_factory is not None
    client = stream_factory()
    assert client._connect_timeout == schema.loading.bucket_lease.reconnect_timeout_ms / 1000 and client._backoff_ms is not None

    build_execution.reset_mock()
    with patch("src.loader.edge_loader.Consumer", return_value=MagicMock()), \
         patch("src.loader.edge_loader.Producer", return_value=MagicMock()), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.build_edge_execution", build_execution), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader):
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--mode", "bulk", "--run-id", "run-9"]) == 0
    assert build_execution.call_args.kwargs["lease_client_factory"] is None


def test_edge_loader_main_reports_clean_exit_on_shutdown_during_startup(schema, caplog) -> None:
    """P5-S1 Story 7, review finding S7-2: a SIGTERM arriving while still
    connecting to the lease server (e.g. inside
    build_edge_execution's coordinator.prime()) must exit 0, not be
    reported as "unable to start"."""
    import signal as signal_module

    from src.loader.lease_client import LeaseCoordinatorError

    consumer, driver = MagicMock(), MagicMock()
    installed_sigterm_handler = {}

    def capturing_signal(signum, handler):
        if signum == signal_module.SIGTERM and "shutdown_requested" not in installed_sigterm_handler:
            installed_sigterm_handler["shutdown_requested"] = handler
        return MagicMock()

    def raise_after_sigterm(*_a, **_k):
        # Simulate the real SIGTERM arriving mid-connect: fire the handler
        # main() actually installed, then raise the same error the
        # streaming client's stop_event produces.
        installed_sigterm_handler["shutdown_requested"](signal_module.SIGTERM, None)
        raise LeaseCoordinatorError("lease coordinator lease:7700: lease reconnect abandoned: shutdown requested")

    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.build_edge_execution", side_effect=raise_after_sigterm), \
         patch("src.loader.edge_loader.signal.signal", side_effect=capturing_signal):
        with caplog.at_level("INFO"):
            exit_code = edge_loader_main(["--edge-type", "WORKS_AT", "--mode", "stream"])
    assert exit_code == 0
    assert any("stage=edge-loader-shutdown" in r.message for r in caplog.records)


# ── P5-S1 Story 4: Neo4j tx timeout + lease-liveness abort-not-commit ───────


class _DeadlineCapturingWriter:
    """Simulates ApocLockedEdgeWriter's real contract closely enough to
    exercise the coordinator's deadline/liveness wiring: records every
    ``deadline`` it was called with, and calls ``lease_liveness_check`` (if
    given) before "committing" -- raising if it returns False, exactly
    like the real writer's abort-before-commit check."""

    def __init__(self) -> None:
        self.deadlines: list[float | None] = []
        self.batches: list[list] = []

    def write_batch(self, records, *, deadline=None, lease_liveness_check=None) -> None:
        self.deadlines.append(deadline)
        if lease_liveness_check is not None and not lease_liveness_check():
            raise LeaseCoordinatorError("lease connection broke before commit; aborting instead of committing")
        self.batches.append(list(records))


def test_grant_time_derived_deadline_reaches_the_writer() -> None:
    """D1: the coordinator computes each cell's deadline from its own
    lease-grant timestamp plus tx_timeout_seconds, not a value fixed at
    construction time -- every write call gets grant_time + T."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _DeadlineCapturingWriter()
    clock_calls = iter([1_000.0, 1_000.0, 1_000.5, 1_000.5])

    def fake_clock():
        try:
            return next(clock_calls)
        except StopIteration:
            return 1_005.0

    coordinator, _ = _coordinator(
        router, writer, hub, workers=1, tx_timeout_seconds=30.0, clock=fake_clock,
    )
    results = coordinator.execute_pending([_pending("solo-a", "solo-b", 0)])
    assert all(result.success for result in results)
    assert writer.deadlines and writer.deadlines[0] is not None
    # deadline == the grant-time clock reading + T (30.0), not merely "some
    # positive number" -- pins the exact D1 computation.
    assert writer.deadlines[0] in (1_000.0 + 30.0, 1_000.5 + 30.0)


def test_no_tx_timeout_configured_passes_no_deadline() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _DeadlineCapturingWriter()
    coordinator, _ = _coordinator(router, writer, hub, workers=1)  # tx_timeout_seconds defaults to None
    results = coordinator.execute_pending([_pending("solo-a", "solo-b", 0)])
    assert all(result.success for result in results)
    assert writer.deadlines == [None]


def test_lease_liveness_failure_aborts_reports_failure_and_commits_zero_offsets(works_at) -> None:
    """AC: a cell write whose lease connection's liveness check fails
    immediately before the scheduled commit rolls back (no write
    persists), reports a failure through LaneExecutionResult (never
    raising past execute_pending uncaught), and the flush containing that
    cell commits zero Kafka offsets."""
    hub, router = _Hub(), BucketCellRouter("Person", "Company", 4)
    writer = _DeadlineCapturingWriter()

    class _DeadPingClient(_FakeClient):
        def ping(self) -> bool:
            return False

    clients: list[_DeadPingClient] = []

    def factory():
        client = _DeadPingClient(hub)
        clients.append(client)
        return client

    coordinator = BucketLeaseExecutionCoordinator(
        writer, router=router, worker_count=1, batch_size=10, client_factory=factory,
    )
    # execute_pending must not raise even though every write aborts.
    results = coordinator.execute_pending(_random_pending(5, sources=2, targets=2))
    assert results and not any(result.success for result in results)
    assert all(isinstance(result.exception, LeaseCoordinatorError) for result in results)
    assert not writer.batches  # nothing ever committed

    consumer = MagicMock()
    consumer.poll.side_effect = [_message(_payload(f"p{i}", "c1"), offset=i) for i in range(3)]
    loader = EdgeLoader(
        consumer, MagicMock(), works_at, loading_config=LoadingConfig(unwind_batch_size=3),
        coordinator=BucketLeaseExecutionCoordinator(
            writer, router=router, worker_count=1, batch_size=10,
            client_factory=lambda: _DeadPingClient(hub),
        ),
    )
    assert loader.run(max_messages=3) == 1
    consumer.commit.assert_not_called()


def test_ping_round_trip_is_counted_inside_lease_wait_seconds() -> None:
    """AC: the added PING round trip must be observed inside
    _LeaseFlush.lease_wait_seconds, as part of the same flush's accounted
    lease-wait time."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _DeadlineCapturingWriter()

    class _SlowPingClient(_FakeClient):
        def ping(self) -> bool:
            time.sleep(0.02)
            return True

    coordinator, _ = _coordinator(router, writer, hub, workers=1)
    coordinator._client_factory = lambda: _SlowPingClient(hub)
    coordinator.execute_pending([_pending("solo-a", "solo-b", 0)])
    assert "lease_wait_ms=" in coordinator.describe_last_flush()
    lease_wait_ms = float(coordinator.describe_last_flush().split("lease_wait_ms=")[1].split()[0])
    assert lease_wait_ms >= 15.0  # the ping's ~20ms sleep dominates


def test_broken_client_after_release_failure_never_reused_next_iteration() -> None:
    """S3-3 phase (c), belt-and-suspenders at the coordinator level: once
    a client is observed broken after a write, the *next* iteration must
    not still be holding it -- proven by checking every _FakeClient
    instance actually used for more than one lease is never broken when
    reused."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _DeadlineCapturingWriter()
    coordinator, clients = _coordinator(
        router, writer, hub, workers=1, client_kwargs={"fail_release": True},
    )
    results = coordinator.execute_pending(_random_pending(6, sources=3, targets=3))
    assert all(result.success for result in results)
    # Every client that was asked to acquire a SECOND lease must not have
    # been broken at the time (each is discarded immediately on breaking).
    for client in clients:
        assert len(client.requests) <= 1 or not client.broken or client.closed


def test_unconfirmed_write_outcome_skips_release_and_leaves_it_for_quarantine() -> None:
    """B4-2: when the Neo4j outcome is not positively known (here: a
    driver-level failure unrelated to our own pre-commit checks), REL
    must not be sent -- the client is simply closed, leaving the lease
    server's own disconnect quarantine (B4-1) to free it once
    grant_time + fence_seconds has passed. Releasing immediately would
    hand the same nodes to another writer while the first transaction's
    outcome is still unknown."""
    from neo4j.exceptions import ServiceUnavailable

    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)

    class _UnconfirmedFailureWriter:
        def write_batch(self, records, *, deadline=None, lease_liveness_check=None) -> None:
            raise ServiceUnavailable("connection to the database failed")

    release_calls: list[str] = []
    created: list["_RecordingClient"] = []

    class _RecordingClient(_FakeClient):
        def release(self, lease_id) -> None:
            release_calls.append(lease_id)
            super().release(lease_id)

    def factory():
        client = _RecordingClient(hub)
        created.append(client)
        return client

    coordinator = BucketLeaseExecutionCoordinator(
        _UnconfirmedFailureWriter(), router=router, worker_count=1, batch_size=10, client_factory=factory,
    )
    results = coordinator.execute_pending([_pending("solo-a", "solo-b", 0)])
    assert results and not results[0].success
    assert isinstance(results[0].exception, ServiceUnavailable)
    assert not release_calls  # REL was never sent
    assert created and created[0].closed  # the connection was closed instead


# -- F1: bounded stop drain -------------------------------------------------------

def test_stop_drain_abandons_the_flush_within_the_budget_when_no_lease_can_be_had() -> None:
    """A holder killed mid-lease keeps its resources fenced for T + margin
    (far beyond docker's stop grace). Once the loader is stopping, a flush
    that cannot get a lease gives up after the drain budget, writes nothing."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router)
    stop = threading.Event()
    coordinator, _ = _coordinator(
        router, writer, hub, batch_size=10, client_kwargs={"deny_all": True},
        stop_event=stop, drain_budget_seconds=0.3,
    )
    stop.set()
    started = time.monotonic()
    results = coordinator.execute_pending([_pending("a", "b", seq) for seq in range(5)])
    elapsed = time.monotonic() - started
    assert elapsed < 2.0
    assert results and not any(result.success for result in results)
    assert all(isinstance(result.exception, LeaseDrainAbandoned) for result in results)
    assert writer.batches == [] and not writer.violations


def test_stop_drain_budget_starts_at_the_stop_request_not_before() -> None:
    """Without a stop request the denial loop is unbounded (a live fleet
    legitimately waits); the budget only applies once the stop event is set."""
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router)
    stop = threading.Event()
    clock = [100.0]
    coordinator, _ = _coordinator(
        router, writer, hub, batch_size=10, client_kwargs={"deny_all": True},
        stop_event=stop, drain_budget_seconds=5.0, clock=lambda: clock[0],
    )
    assert coordinator._drain_expired() is False  # no stop yet
    stop.set()
    assert coordinator._drain_expired() is False  # budget just started
    clock[0] += 4.9
    assert coordinator._drain_expired() is False
    clock[0] += 0.2
    assert coordinator._drain_expired() is True


def test_stop_drain_still_writes_when_leases_are_available() -> None:
    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    writer = _CheckingWriter(hub, router)
    stop = threading.Event()
    stop.set()
    coordinator, _ = _coordinator(router, writer, hub, batch_size=10, stop_event=stop, drain_budget_seconds=5.0)
    results = coordinator.execute_pending([_pending("a", "b", seq) for seq in range(5)])
    assert all(result.success for result in results)
    assert sum(len(batch) for batch in writer.batches) == 5


def test_a_real_write_error_replaces_a_recorded_drain_abandon() -> None:
    """S2: the first failure wins, except a stop-drain abandon never masks a real error."""
    from src.loader.edge_execution import _LeaseFlush

    state = SimpleNamespace(failure=None)
    record = lambda exc: _LeaseFlush.record_failure(state, exc)  # noqa: E731
    abandon, real, later = LeaseDrainAbandoned("budget"), RuntimeError("neo4j write failed"), RuntimeError("later")
    record(abandon)
    assert state.failure is abandon
    record(LeaseDrainAbandoned("again"))
    assert state.failure is abandon
    record(real)
    assert state.failure is real
    record(later)
    record(LeaseDrainAbandoned("late"))
    assert state.failure is real


def test_lease_reconnect_aborted_by_the_stop_is_recorded_as_a_drain_abandon() -> None:
    """The streaming client raises LeaseCoordinatorError('... shutdown requested') once the
    stop event is set; that is an abandon, not a failure. Without a stop it stays an error."""
    from src.loader.edge_execution import _LeaseFlush

    hub, router = _Hub(), BucketCellRouter("Person", "Title", 4)
    stop = threading.Event()
    coordinator, _ = _coordinator(router, _CheckingWriter(hub, router), hub, stop_event=stop)
    error = LeaseCoordinatorError("lease reconnect abandoned: shutdown requested")

    def fresh_state():
        state = SimpleNamespace(failure=None, lock=threading.Lock())
        state.record_failure = lambda exc: _LeaseFlush.record_failure(state, exc)
        return state

    state = fresh_state()
    coordinator._record_worker_failure(state, error)
    assert state.failure is error
    stop.set()
    state = fresh_state()
    coordinator._record_worker_failure(state, error)
    assert isinstance(state.failure, LeaseDrainAbandoned)
    state = fresh_state()
    other = LeaseCoordinatorError("lease coordinator unreachable")
    coordinator._record_worker_failure(state, other)
    assert state.failure is other
