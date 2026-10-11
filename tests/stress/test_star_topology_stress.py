"""Star-topology / hub-node stress tests (Phase 6 Slice 4, Stories 4-7).

Every test in this file drives the real write-path classes directly
(planned mode via `tests.stress.planned_harness`, contended modes via
`src.loader.edge_execution` directly -- Stories 5-6), multi-threaded,
against real testcontainers-backed Neo4j/Kafka, per Decision 1 in
`plan/v6/slice-4/stories.md`: this proves the epic's "no deadlocks/lock
waits under contention" claim at the level that actually decides it (the
Cypher query shape and which transactions run concurrently), without
needing built Docker images or `docker_service.py`.

Every test builds its own function-scoped Neo4jContainer (`stress_neo4j`
fixture below) rather than the session-scoped `neo4j_driver` fixture,
because query logging must be applied via `.with_env(...)` before
`.start()` (Story 2's own note). The session-scoped `kafka_bootstrap`
fixture from `tests/conftest.py` is reused as-is.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from pathlib import Path
import threading
import uuid

import docker
import pytest
import yaml
from neo4j import GraphDatabase

from src.loader.edge_execution import ApocLockedEdgeWriter, build_edge_execution
from src.loader.edge_loader import EdgeRecord, MissingRelationshipEndpointError, PendingEdgeRecord
from src.loader.lease_client import LEASE_ADDRESS_ENV, LEASE_BUCKETS_ENV
from src.orchestrator.lease_server import BucketLeaseServer
from src.utils.schema_loader import load_schema
from tests.stress import lock_wait, planned_harness
from tests.stress import star_topology_data as std

# 100k is a literal, not GRAPH_LOADER_STRESS_SCALE: that env var is read only
# by the 1M tier (Story 7), read only inside that one test function. If this
# 100k tier read the same env var, setting GRAPH_LOADER_STRESS_SCALE=1000000
# for Story 8's dedicated 1M job would silently balloon *every* test in this
# file to 1M rows under a 100k-sized timeout the moment anyone also selects
# `-m integration` in that job -- exactly the tier confusion P6-Q3 exists to
# prevent.
HUB_EDGES_100K = 100_000


def _docker_reachable() -> bool:
    try:
        docker.from_env().ping()
        return True
    except Exception:
        return False


def _load_schema(topology: str, run_id: str, tmp_path: Path):
    path = tmp_path / f"schema-{topology}-{run_id}.yaml"
    path.write_text(yaml.safe_dump(std.schema_dict(topology, run_id)), encoding="utf-8")
    return load_schema(path)


# (Historical: the shared tests/conftest.py fixture was pinned to
# neo4j:5.21-enterprise until P6-S5 moved it to 5.26; the finding below is
# why. Minimum supported server is 5.23.) neo4j:5.21-enterprise CANNOT run ApocLockedEdgeWriter's
# explicit_locks=True (classic) or sort_rows=True (bulk-lease default)
# write path at all: build_apoc_locked_upsert_query's CALL (vars) { ... }
# scoped-subquery syntax (edge_execution.py) is a Cypher feature that
# landed after 5.21 (confirmed empirically: "WITH 1 AS x CALL (x) { RETURN
# x AS y } RETURN y" is a syntax error on 5.21.2, and succeeds on 5.26.31).
# docker-compose.yaml's own production/dev stack already pins the newer
# "neo4j:5-enterprise" floating tag, so this is not a gap this slice is
# introducing -- it is an existing version-pin mismatch between
# tests/conftest.py's fixture (which no pre-existing integration test
# happens to exercise this query path against) and the real deployment
# target. Recorded as a residual risk in plan/v6/completion_slice-4.md;
# fixing tests/conftest.py or src/ is out of scope for this tests-only
# slice. Every container in this file uses this newer, pinned-minor tag
# (not the fully-floating "5-enterprise") for reproducibility, including
# the planned-mode tests above, which do not need it but are unaffected by
# it either.
STRESS_NEO4J_IMAGE = "neo4j:5.26-enterprise"


def _start_stress_container(*, apoc: bool):
    """Build and start a dedicated, query-logged (+ optionally APOC-enabled) Neo4j container.

    Skips only when Docker itself is unreachable (matching
    `tests/conftest.py`'s own skip discipline). Once Docker is reachable, a
    container start failure is a real test failure, never a skip -- a
    stress test that reports green by skipping on a broken Neo4j start
    would violate this slice's own "fails loudly if Docker is reachable"
    AC. `hard_timeout` (used by every test in this file) calls
    `faulthandler`'s `exit=True`, i.e. `os._exit`, on a genuine hang, which
    skips a fixture's own teardown entirely -- but testcontainers' Ryuk
    reaper (enabled by default; `TESTCONTAINERS_RYUK_DISABLED` is unset
    throughout this repo) still removes the container from outside the
    killed process.
    """
    if not _docker_reachable():
        pytest.skip("Docker is required for star-topology stress tests")
    from testcontainers.community.neo4j import Neo4jContainer

    container = Neo4jContainer(STRESS_NEO4J_IMAGE, password="test-password") \
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes") \
        .with_env("NEO4J_server_memory_heap_initial__size", "2G") \
        .with_env("NEO4J_server_memory_heap_max__size", "2G") \
        .with_env("NEO4J_server_memory_pagecache_size", "1G")
    # Neo4j auto-sizes its heap/page cache from the *host's* RAM (64GB+
    # locally; a GitHub-hosted runner has 7-16GB total). Pinning these
    # explicitly keeps the measured 1M-scale timing and behaviour honest
    # across very different hosts, rather than silently running with a
    # different (and on a shared runner, possibly starved) memory budget.
    container = lock_wait.with_transaction_timeout(lock_wait.with_verbose_query_log(container))
    if apoc:
        container = lock_wait.with_apoc(container)
    try:
        container.start()
    except Exception:
        try:
            container.stop()
        except Exception:  # pragma: no cover - best-effort cleanup after a failed start
            pass
        raise
    return container


@pytest.fixture
def stress_neo4j():
    """A dedicated, query-logged Neo4j container (no APOC -- planned/lease mode never calls it)."""
    container = _start_stress_container(apoc=False)
    driver = GraphDatabase.driver(container.get_connection_url(), auth=(container.username, container.password))
    try:
        driver.verify_connectivity()
        yield container, driver
    finally:
        driver.close()
        container.stop()


@pytest.fixture
def stress_neo4j_apoc():
    """A dedicated, query-logged, APOC-enabled Neo4j container (classic shared-overlap mode needs apoc.lock.nodes)."""
    container = _start_stress_container(apoc=True)
    driver = GraphDatabase.driver(container.get_connection_url(), auth=(container.username, container.password))
    try:
        driver.verify_connectivity()
        yield container, driver
    finally:
        driver.close()
        container.stop()


@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_into_one_hub_planned_mode_zero_lock_waits(stress_neo4j, kafka_bootstrap, tmp_path) -> None:
    """The epic's headline AC: 100k relationships into one hub, planned mode, zero Neo4j lock waits.

    Measured wall-clock at this scale (First Mate's own local run, before
    fixing Story 8's CI timeout): see plan/v6/completion_slice-4.md.
    """
    container, driver = stress_neo4j
    with lock_wait.hard_timeout(600):
        run_id = uuid.uuid4().hex[:8]
        schema = _load_schema("target_hub", run_id, tmp_path)
        dataset = std.generate(HUB_EDGES_100K, "target_hub")
        std.seed_nodes(driver, dataset, schema)
        # The measurement window starts only now: node seeding (including
        # constraint creation) must never be blamed for a lock wait it
        # didn't cause, and must never let an earlier phase's wait hide
        # inside this test's own "zero lock waits" claim either way.
        mark = lock_wait.query_log_mark(container)
        try:
            std.publish(dataset, kafka_bootstrap, schema)
            # No plan_hub_degree override: 100k on one key already exceeds
            # DEFAULT_HUB_DEGREE (5000) by construction, so the *stock*
            # planner config is what gets exercised here, not a
            # test-lowered threshold.
            result = planned_harness.run_planned_mode(
                schema, driver, kafka_bootstrap, plan_dir=tmp_path, writers=4, timeout_s=540,
            )
            # Precondition: the plan really used exactly one hub resource,
            # for this key -- every Leaf has degree 1 (well under the
            # threshold), so a passing lock-wait assertion on a plan with
            # zero (or more than one, accidental) hubs would prove nothing
            # about the epic's actual claim.
            assert result.plan.resources.hub_count == 1
            assert result.plan.resources.hub_resource(std.HUB_LABEL, std.HUB_KEY) is not None
            problems = std.compare(std.expected_graph(dataset), std.read_graph(driver, schema))
            assert problems == []
            lock_wait.assert_zero_lock_wait(container, since=mark, min_queries=len(result.plan.chunks))
            assert result.total_attempts_over_one == 0
        finally:
            std.delete_topics(kafka_bootstrap, std.edge_topic_names(schema))


def _resource_of_label(plan, chunk, label: str) -> int:
    """Return the one resource in ``chunk.resources`` whose label is ``label``.

    ``PlannedChunk`` has no ``source_resource``/``target_resource`` field --
    only a sorted, deduplicated ``resources`` tuple (confirmed reading
    ``src/loader/edge_plan.py``'s dataclass directly). This slice's
    topologies always pair two distinct labels, so a resource's label
    unambiguously identifies its endpoint role.
    """
    matches = [resource for resource in chunk.resources if plan.resources.label_of(resource) == label]
    assert len(matches) == 1, f"expected exactly one {label!r} resource in chunk {chunk.id}, found {matches}"
    return matches[0]


@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_from_one_hub_planned_mode_zero_lock_waits(stress_neo4j, kafka_bootstrap, tmp_path) -> None:
    """The reverse direction: one Hub node is every record's SOURCE (topology="source_hub").

    The planner's hub rule keys on a node's total degree regardless of
    whether it appears as a unit's source or target resource
    (`docs/USAGE.md` Section 6); this is the one assertion Story 4 doesn't
    need (its hub is always the target) and is the actual proof the
    reverse direction was exercised structurally, not just published.
    """
    container, driver = stress_neo4j
    with lock_wait.hard_timeout(600):
        run_id = uuid.uuid4().hex[:8]
        schema = _load_schema("source_hub", run_id, tmp_path)
        dataset = std.generate(HUB_EDGES_100K, "source_hub")
        std.seed_nodes(driver, dataset, schema)
        mark = lock_wait.query_log_mark(container)
        try:
            std.publish(dataset, kafka_bootstrap, schema)
            result = planned_harness.run_planned_mode(
                schema, driver, kafka_bootstrap, plan_dir=tmp_path, writers=4, timeout_s=540,
            )
            assert result.plan.resources.hub_count == 1
            hub_resource = result.plan.resources.hub_resource(std.HUB_LABEL, std.HUB_KEY)
            assert hub_resource is not None
            points_from_chunks = [chunk for chunk in result.plan.chunks if chunk.edge_type == "POINTS_FROM"]
            assert points_from_chunks
            for chunk in points_from_chunks:
                source_label, _target_label = result.plan.labels_of(chunk)
                assert source_label == std.HUB_LABEL
                assert _resource_of_label(result.plan, chunk, std.HUB_LABEL) == hub_resource
            problems = std.compare(std.expected_graph(dataset), std.read_graph(driver, schema))
            assert problems == []
            lock_wait.assert_zero_lock_wait(container, since=mark, min_queries=len(result.plan.chunks))
            assert result.total_attempts_over_one == 0
        finally:
            std.delete_topics(kafka_bootstrap, std.edge_topic_names(schema))


@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_five_types_into_one_hub_planned_mode_zero_lock_waits(stress_neo4j, kafka_bootstrap, tmp_path) -> None:
    """Five relationship types, all into one hub (topology="multi_type_hub").

    Proves the planner treats a hub's *total* degree across every type, not
    per type, as the trigger: the hub's plan resource must be shared by
    chunks of more than one type, or the per-type-lane contention planned
    mode exists to avoid would silently be reintroduced.

    The production default (`DEFAULT_HUB_DEGREE=5000`) does NOT distinguish
    a total-degree rule from a per-type rule at this scale: each of the
    five types alone carries ~20,200 records into the hub, already above
    5000, so a (hypothetical) per-type rule would also flag it, and hub
    resources are keyed per node (`label, key`), not per type
    (`edge_plan.py`'s `ResourceTable`), so either rule would still produce
    `hub_count == 1` with that one resource shared by all five types --
    every assertion below would pass under *either* rule, testing nothing
    about which one is real. `plan_hub_degree=50_000` is deliberately
    chosen (and asserted as a precondition) so the largest single type
    stays under the threshold while the five-type total clears it: only a
    true total-degree rule can pass this test.
    """
    container, driver = stress_neo4j
    with lock_wait.hard_timeout(600):
        run_id = uuid.uuid4().hex[:8]
        schema = _load_schema("multi_type_hub", run_id, tmp_path)
        dataset = std.generate(HUB_EDGES_100K, "multi_type_hub")
        plan_hub_degree = 50_000
        largest_single_type = max(len(rows) for rows in dataset.edges.values())
        assert largest_single_type < plan_hub_degree <= dataset.edge_count, (
            f"the multi_type_hub threshold must sit strictly between the largest single type's "
            f"record count ({largest_single_type}) and the five-type total ({dataset.edge_count}) "
            f"for this test to discriminate a total-degree rule from a per-type one"
        )
        std.seed_nodes(driver, dataset, schema)
        mark = lock_wait.query_log_mark(container)
        try:
            std.publish(dataset, kafka_bootstrap, schema)
            result = planned_harness.run_planned_mode(
                schema, driver, kafka_bootstrap, plan_dir=tmp_path, writers=4, timeout_s=540,
                plan_hub_degree=plan_hub_degree,
            )
            assert result.plan.resources.hub_count == 1
            hub_resource = result.plan.resources.hub_resource(std.HUB_LABEL, std.HUB_KEY)
            assert hub_resource is not None
            types_sharing_hub = {
                chunk.edge_type for chunk in result.plan.chunks if hub_resource in chunk.resources
            }
            assert len(types_sharing_hub) > 1, (
                f"expected the hub's resource to be shared by more than one edge type's chunks, "
                f"got {types_sharing_hub}"
            )
            problems = std.compare(std.expected_graph(dataset), std.read_graph(driver, schema))
            assert problems == []
            lock_wait.assert_zero_lock_wait(container, since=mark, min_queries=len(result.plan.chunks))
            assert result.total_attempts_over_one == 0
        finally:
            std.delete_topics(kafka_bootstrap, std.edge_topic_names(schema))


def _classic_rows(dataset: std.Dataset, edge_type: str) -> list[dict]:
    """Rows in the ``{"source_key", "target_key", "properties"}`` shape ``write_rows`` expects."""
    return [
        {"source_key": source, "target_key": target, "properties": properties}
        for source, target, properties in dataset.edges[edge_type]
    ]


def _event_loop_thread() -> tuple[asyncio.AbstractEventLoop, threading.Thread]:
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="lease-server-loop", daemon=True)
    thread.start()
    return loop, thread


@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_into_one_hub_shared_overlap_zero_unhandled_deadlocks(stress_neo4j_apoc, tmp_path, caplog) -> None:
    """Classic mode, `--bulk-edge-overlap shared` equivalent: many threads, one hot Hub node.

    Correction vs. the story doc's sketch (which expected
    `count_retry_log_lines(...) > 0` here): `docs/USAGE.md`'s own "Why lock
    waits used to dominate" section states plainly that "Sorted explicit
    locks (apoc.lock.nodes) prevent deadlocks, but not waiting" -- i.e. this
    mode's whole design point is contention *without* deadlock, proven
    empirically too (a real run with genuine measured contention below
    produced exactly 0 retries, matching the doc). Asserting
    `count_retry_log_lines(...) > 0` here would either never pass (fighting
    a documented, intentional safety property) or -- if it somehow did --
    be evidence of a real regression, not of "healthy contention". This
    test instead asserts the two claims that actually hold for this mode:
    real lock *waiting* occurred (`total_lock_wait_ms(...) > 0`, the
    opposite of Stories 4-5's planned-mode claim) and exactly zero
    deadlocks were ever recovered (`count_retry_log_lines(...) == 0`,
    which *would* fail if a future regression broke the ordered-lock
    guarantee and reintroduced real deadlocks -- this is the meaningful
    regression check, not a doomed attempt to force one). No Kafka: rows
    are built directly from the dataset (Decision 1 -- this drives the
    writer class, not the Kafka-consuming loader).
    """
    container, driver = stress_neo4j_apoc
    with lock_wait.hard_timeout(600):
        run_id = uuid.uuid4().hex[:8]
        schema = _load_schema("target_hub", run_id, tmp_path)
        dataset = std.generate(HUB_EDGES_100K, "target_hub")
        std.seed_nodes(driver, dataset, schema)
        mark = lock_wait.query_log_mark(container)
        edge_config = next(edge for edge in schema.edges if edge.type == "POINTS_AT")
        writer = ApocLockedEdgeWriter(
            driver, edge_config, missing_endpoint_error_factory=MissingRelationshipEndpointError,
        )
        rows = _classic_rows(dataset, "POINTS_AT")
        # Tuned locally (see plan/v6/completion_slice-4.md for the measured
        # evidence) so 16 threads on 100k rows against one hot node reliably
        # reproduce real contention: a batch size too large serializes
        # everything through fewer, bigger transactions (less overlap
        # opportunity); too small and each transaction is too cheap for two
        # threads to ever land on the hub at the same instant.
        workers, batch_size = 16, 25
        batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
        with caplog.at_level(logging.WARNING, logger="src.loader.edge_execution"):
            with ThreadPoolExecutor(max_workers=workers) as pool:
                attempts = list(pool.map(writer.write_rows, batches))
        # Every call returned normally: no unhandled DeadlockDetectedException/
        # TransientError reached this test.
        assert all(isinstance(attempt, int) and not isinstance(attempt, bool) for attempt in attempts)
        # write_rows's own return value (attempt count) is a caplog-independent
        # signal that no retry happened at all -- every attempt must be
        # exactly 1, which can't misreport just because a logger level was
        # misconfigured somewhere.
        assert all(attempt == 1 for attempt in attempts), (
            f"expected every write_rows call to succeed on its first attempt, got {attempts}"
        )
        # Precondition sanity check: prove contention was real, not assumed --
        # a test that can never fail on the thing it claims to test is worse
        # than no test.
        contended_wait_ms = lock_wait.total_lock_wait_ms(container, since=mark)
        assert contended_wait_ms > 0, (
            "expected real lock contention (waiting > 0) in shared-overlap mode; "
            "16 threads x 25-row batches against one hub apparently never overlapped -- "
            "raise the thread count or lower the batch size (see this test's own tuning note)"
        )
        # See this test's own docstring: ordered explicit locks provably
        # prevent deadlocks, so this is a regression check, not a "did we
        # force one" check.
        assert lock_wait.count_retry_log_lines(caplog, edge_type="POINTS_AT") == 0
        with driver.session() as session:
            count = session.run("MATCH ()-[r:POINTS_AT]->(:Hub) RETURN count(r) AS c").single()["c"]
        assert count == len(std.distinct_pairs(dataset, "POINTS_AT"))


@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_into_one_hub_bulk_lease_mode_zero_lock_waits(stress_neo4j, tmp_path) -> None:
    """Bulk lease mode: an in-process BucketLeaseServer, zero lock-wait AND zero unhandled deadlocks.

    Bucket leases make concurrent transactions node-disjoint by
    construction (`docs/USAGE.md`'s relationship-modes table: "Bucket
    leases ... none"), independent of Phase 5 Slice 1 (which only extends
    leases to *streaming*; bulk lease mode is not gated on that slice).
    Asserts both zero lock-wait (leases claim zero, not just "recovered")
    and zero unhandled deadlocks. No Kafka: rows are fed directly through
    the coordinator (Decision 1).

    Review fix: `schema_dict` does not set `execution`, so
    `EdgeExecutionConfig.worker_count` defaults to 1
    (`src/models/schema.py`), which alone would make the coordinator run
    `min(1, len(cells))` = 1 worker (`edge_execution.py`) -- a single
    worker can never contend with itself, so "zero lock wait" would be
    true whether or not the lease coordinator did anything. This test
    overrides `worker_count` to 8 and asserts it took effect, and adds a
    counterfactual: writing the SAME rows without the lease coordinator
    (bare `explicit_locks=False, sort_rows=True`, no ordering, no leasing,
    matching the coordinator's own 8-worker count) DOES produce real lock
    waiting -- proving the zero-wait result below comes from the lease
    coordinator, not from a setup that could never contend at all.

    Second review fix: the counterfactual and the real lease-coordinated
    write must operate on *disjoint* rows, both genuinely new creates. An
    earlier version ran the counterfactual over the *entire* dataset
    first, leaving the lease-coordinated write with nothing but
    already-existing rows to (idempotently) re-merge -- `MERGE`-matching
    an existing relationship apparently needs far less locking than
    `MERGE`-creating a new one (confirmed empirically while writing this
    test: re-running the exact same contended workload against
    already-written rows measured ~0 ms of waiting even without leasing),
    so that version's "zero lock wait" claim would have passed even with
    leasing completely broken. Splitting by `leaf_id` below keeps both
    halves genuine, disjoint, fresh creates.
    """
    container, driver = stress_neo4j
    with lock_wait.hard_timeout(600):
        run_id = uuid.uuid4().hex[:8]
        schema = _load_schema("target_hub", run_id, tmp_path)
        dataset = std.generate(HUB_EDGES_100K, "target_hub")
        std.seed_nodes(driver, dataset, schema)
        base_edge_config = next(edge for edge in schema.edges if edge.type == "POINTS_AT")
        # 8 concurrent workers -- the default worker_count of 1 would make
        # "zero lock wait" vacuously true (a single worker can't contend
        # with itself) regardless of whether leasing did anything.
        edge_config = base_edge_config.model_copy(update={
            "execution": base_edge_config.execution.model_copy(update={"worker_count": 8}),
        })

        leaf_boundary = 20_000
        counterfactual_rows = [row for row in dataset.edges["POINTS_AT"] if row[0] < leaf_boundary]
        lease_rows = [row for row in dataset.edges["POINTS_AT"] if row[0] >= leaf_boundary]
        counterfactual_pairs = {(source, target) for source, target, _ in counterfactual_rows}
        lease_pairs = {(source, target) for source, target, _ in lease_rows}
        assert counterfactual_pairs.isdisjoint(lease_pairs)
        assert counterfactual_pairs | lease_pairs == std.distinct_pairs(dataset, "POINTS_AT")

        # Counterfactual FIRST, on its own disjoint half of fresh
        # (not-yet-written) rows, using the same worker count (8) the
        # real lease coordinator below is configured with, and the same
        # small batch size already proven (in the shared-overlap test) to
        # make overlap on this hub reliable.
        naive_writer = ApocLockedEdgeWriter(
            driver, edge_config, explicit_locks=False, sort_rows=True,
            missing_endpoint_error_factory=MissingRelationshipEndpointError,
        )
        naive_rows = [
            {"source_key": source, "target_key": target, "properties": properties}
            for source, target, properties in counterfactual_rows
        ]
        naive_batches = [naive_rows[i:i + 25] for i in range(0, len(naive_rows), 25)]
        counterfactual_mark = lock_wait.query_log_mark(container)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(naive_writer.write_rows, naive_batches))
        counterfactual_wait_ms = lock_wait.total_lock_wait_ms(container, since=counterfactual_mark)
        assert counterfactual_wait_ms > 0, (
            "counterfactual expected real lock contention without lease coordination "
            "(disjoint fresh rows, same 8-worker count as the real lease coordinator below, "
            "no leasing); measured 0 ms -- either the helper cannot detect a real wait here, "
            "or this setup can never contend, which would make the lease-coordinated "
            "zero-wait result below meaningless"
        )
        with driver.session() as session:
            counterfactual_count = session.run(
                "MATCH (l:Leaf)-[r:POINTS_AT]->(:Hub) WHERE l.leaf_id < $boundary RETURN count(r) AS c",
                boundary=leaf_boundary,
            ).single()["c"]
        assert counterfactual_count == len(counterfactual_pairs)

        # The measurement window for the REAL, lease-coordinated claim
        # starts only now, excluding the counterfactual's own (expected)
        # waiting.
        mark = lock_wait.query_log_mark(container)
        loop, loop_thread = _event_loop_thread()
        server = BucketLeaseServer()
        coordinator = None
        try:
            host, port = asyncio.run_coroutine_threadsafe(server.start("127.0.0.1", 0), loop).result(10)
            # Bucket-lease addresses are plain host:port -- no "tcp://" scheme
            # (confirmed against src.loader.lease_client.parse_lease_address).
            _writer, _partitioner, _batcher, coordinator = build_edge_execution(
                edge_config, driver,
                environ={LEASE_ADDRESS_ENV: f"{host}:{port}", LEASE_BUCKETS_ENV: "16"},
            )
            assert coordinator._workers == 8, f"expected 8 lease workers, coordinator built {coordinator._workers}"
            # Only the OTHER, disjoint half -- every one of these rows is a
            # genuinely fresh create, never touched by the counterfactual.
            pending = [
                PendingEdgeRecord(EdgeRecord(source, target, properties), edge_config.topic, 0, index)
                for index, (source, target, properties) in enumerate(lease_rows)
            ]
            results = coordinator.execute_pending(pending)
            assert all(result.success for result in results), (
                f"lease-coordinated write(s) failed: "
                f"{[result.exception for result in results if not result.success][:3]}"
            )
            with driver.session() as session:
                count = session.run("MATCH ()-[r:POINTS_AT]->(:Hub) RETURN count(r) AS c").single()["c"]
            # Together with the counterfactual's half, the full dataset.
            assert count == len(std.distinct_pairs(dataset, "POINTS_AT"))
            # Leases claim zero lock-wait outright, not just "recovered from
            # a deadlock" -- the same hard assertion Stories 4-5 make.
            # min_queries=len(results): a safe lower bound (>=1 transaction
            # per written cell; cells may be merged into fewer transactions
            # via EXT, never more), well below the true ~100+ transactions
            # expected at this scale with 16 lease buckets.
            lock_wait.assert_zero_lock_wait(container, since=mark, min_queries=len(results))
        finally:
            # A broken/unreachable coordinator fails closed at connect via
            # BucketLeaseClient's own existing deadline behaviour
            # (tests/test_lease_server.py::
            # test_unreachable_coordinator_fails_closed_after_connect_deadline);
            # this test's own hard_timeout is the backstop if it somehow
            # still hung.
            if coordinator is not None:
                try:
                    coordinator.close()
                except Exception:  # pragma: no cover - best-effort cleanup
                    pass
            try:
                asyncio.run_coroutine_threadsafe(server.close(), loop).result(10)
            except Exception:  # pragma: no cover - best-effort cleanup
                pass
            loop.call_soon_threadsafe(loop.stop)
            loop_thread.join(timeout=10)
            if not loop_thread.is_alive():
                loop.close()


@pytest.mark.integration
@pytest.mark.stress
@pytest.mark.skipif(
    os.environ.get("GRAPH_LOADER_STRESS_SCALE") != "1000000",
    reason=(
        "1M-edge tier only runs when GRAPH_LOADER_STRESS_SCALE=1000000 is set "
        "(the nightly/manual star-topology-stress-1m CI job, or an explicit local "
        "override) -- never as part of an ordinary `pytest -m integration` run, "
        "per P6-Q3's tiering decision."
    ),
)
def test_1m_edges_into_one_hub_planned_mode_zero_lock_waits(stress_neo4j, kafka_bootstrap, tmp_path) -> None:
    """The epic's literal 1,000,000-edge scale, gated to the nightly/manual CI job only.

    A runtime check keyed to `GRAPH_LOADER_STRESS_SCALE`, not a second
    pytest marker (Decision 2 in `plan/v6/slice-4/stories.md`): a
    dedicated marker would need every CI invocation to remember to
    *exclude* it (`-m "stress and not stress_1m"`); one missed `-m` filter
    anywhere and this would silently run inside the ordinary nightly
    `integration` job. Implemented as `skipif` rather than a `pytest.skip()`
    call inside the body (this story's own literal sketch) so an ordinary
    run never pays for `stress_neo4j`'s own container startup/teardown
    (~15s) just to then skip -- `skipif`'s condition is still keyed to the
    same env var, evaluated the same way, so nothing about the tiering
    contract changes. `GRAPH_LOADER_STRESS_SCALE` is consumed only by this
    test's own gate (`skipif` is evaluated once, at collection, but no
    other test in this file reads or is affected by that variable --
    see `HUB_EDGES_100K`'s own comment above for why that distinction
    matters).
    """
    container, driver = stress_neo4j
    # Sized with headroom above the measured 1M wall-clock -- see
    # plan/v6/completion_slice-4.md for the actual local measurement that
    # justified these two numbers (Story 7's own DoD: measure, don't guess).
    with lock_wait.hard_timeout(1800):
        run_id = uuid.uuid4().hex[:8]
        schema = _load_schema("target_hub", run_id, tmp_path)
        dataset = std.generate(1_000_000, "target_hub")
        std.seed_nodes(driver, dataset, schema)
        mark = lock_wait.query_log_mark(container)
        try:
            std.publish(dataset, kafka_bootstrap, schema)
            result = planned_harness.run_planned_mode(
                schema, driver, kafka_bootstrap, plan_dir=tmp_path, writers=4, timeout_s=1500,
            )
            assert result.plan.resources.hub_count == 1
            assert result.plan.resources.hub_resource(std.HUB_LABEL, std.HUB_KEY) is not None
            problems = std.compare(std.expected_graph(dataset), std.read_graph(driver, schema))
            assert problems == []
            lock_wait.assert_zero_lock_wait(container, since=mark, min_queries=len(result.plan.chunks))
            assert result.total_attempts_over_one == 0
        finally:
            std.delete_topics(kafka_bootstrap, std.edge_topic_names(schema))
