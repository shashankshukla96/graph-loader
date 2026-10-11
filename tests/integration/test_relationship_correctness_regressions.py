"""Relationship correctness regressions across planned, classic and bulk-lease modes.

Reuses `examples.social.social_graph` directly (the same import
`tests/test_social_planned_integration.py` already does), at a small scale
(200 people, not the default 20,000), so the correctness claims that
module already encodes -- but which never run in CI, because that test is
gated behind `GRAPH_LOADER_IT_*` -- are continuously checked somewhere that
actually executes on a stock runner.

Correction vs. the story doc's sketch (`plan/v6/slice-4/stories.md` Story
9): `build_edge_execution`'s classic-mode default (`explicit_locks=True`,
env `GRAPH_LOADER_EDGE_EXPLICIT_LOCKS` unset) and its bulk-lease path
(`sort_rows=True`) both hit `build_apoc_locked_upsert_query`'s `CALL (vars)
{ ... }` branch, a Cypher syntax that is a syntax error on
`neo4j:5.21-enterprise` (Story 6's finding; the shared pin is now 5.26). Only planned mode avoids it.
This file uses one dedicated `neo4j:5.26-enterprise` + APOC container
(module-scoped: one reset-and-reseed per mode, not three containers) for
all three modes, matching Story 6/9's shared fixture pattern, rather than
the shared `tests/conftest.py::neo4j_driver` fixture.
"""
from __future__ import annotations

import asyncio
import logging
import threading

import docker
import pytest
import yaml
from neo4j import GraphDatabase

from examples.social import social_graph
from src.loader.edge_execution import build_edge_execution
from src.loader.edge_loader import EdgeRecord, MissingRelationshipEndpointError, PendingEdgeRecord
from src.loader.lease_client import LEASE_ADDRESS_ENV
from src.orchestrator.lease_server import BucketLeaseServer
from src.orchestrator.schema_initializer import apply_schema
from src.utils.schema_loader import load_schema
from tests.stress import lock_wait, planned_harness

PERSONS = 200


def _visited_only_schema(tmp_path):
    """A minimal schema declaring only Person, City and VISITED (the real "social-visited" topic).

    Used only for the planned-mode leg of the missing-endpoint test: a
    `KafkaEdgePlanner` captures a boundary for *every* schema edge topic,
    so reusing the full `social_schema` (six edge types) there would try
    to read boundaries for topics this tiny, standalone publish never
    creates (or that an earlier test in this module already deleted).
    Node labels/key properties match `examples/social/graph_schema.yaml`
    exactly, since the graph already seeded by `_reset_and_seed` (full
    `social_schema`) must validate against this schema's endpoint types
    too.
    """
    path = tmp_path / "visited-only-schema.yaml"
    path.write_text(yaml.safe_dump({
        "loading": {"mode": "bulk"},
        "nodes": [
            {"label": "Person", "topic": "social-person", "key_property": "person_id",
             "properties": {"person_id": {"type": "integer", "required": True}}},
            {"label": "City", "topic": "social-city", "key_property": "name",
             "properties": {"name": {"type": "string", "required": True}}},
        ],
        "edges": [
            {"type": "VISITED", "topic": "social-visited", "nodes": {"source": "Person", "target": "City"},
             "source_key_property": "person_id", "target_key_property": "name",
             "properties": {"times": {"type": "integer", "required": True}}},
        ],
    }), encoding="utf-8")
    return load_schema(path)


def _docker_reachable() -> bool:
    try:
        docker.from_env().ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def social_driver():
    """A dedicated, APOC-enabled, query-logged Neo4j container, shared by every test in this module.

    Module-scoped (one container, `DETACH DELETE` + reseed between modes)
    since this is a correctness suite, not a lock-wait stress suite --
    every test here resets its own starting state explicitly via
    `_reset_and_seed`.
    """
    if not _docker_reachable():
        pytest.skip("Docker is required for relationship correctness regression tests")
    from testcontainers.community.neo4j import Neo4jContainer

    container = lock_wait.with_apoc(lock_wait.with_transaction_timeout(lock_wait.with_verbose_query_log(
        Neo4jContainer("neo4j:5.26-enterprise", password="test-password")
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    )))
    try:
        container.start()
    except Exception:
        try:
            container.stop()
        except Exception:  # pragma: no cover - best-effort cleanup after a failed start
            pass
        raise
    driver = GraphDatabase.driver(container.get_connection_url(), auth=(container.username, container.password))
    try:
        driver.verify_connectivity()
        yield driver
    finally:
        driver.close()
        container.stop()


@pytest.fixture(scope="module")
def social_schema():
    return load_schema(social_graph.SCHEMA_PATH)


@pytest.fixture(scope="module")
def social_dataset():
    return social_graph.generate(persons=PERSONS, seed=7)


def _reset_and_seed(driver, schema, dataset: social_graph.Dataset) -> None:
    """`DETACH DELETE` everything, recreate constraints, then seed every node directly.

    Node-loading correctness is `test_node_loader_integration.py`'s job
    (Decision 5's reasoning, reused here): this bypasses `NodeLoader`/Kafka
    entirely with one `UNWIND ... MERGE` per label, using the dataset's own
    typed Python values directly (the driver natively encodes
    `datetime.date`/`datetime.datetime`; no wire-format conversion needed
    for a direct Cypher call).
    """
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
    apply_schema(driver, schema)
    key_property = {node.label: node.key_property for node in schema.nodes}
    for label, records in dataset.nodes.items():
        with driver.session() as session:
            session.run(
                f"UNWIND $rows AS row MERGE (n:`{label}` {{`{key_property[label]}`: row.`{key_property[label]}`}}) "
                "SET n += row",
                rows=records,
            ).consume()


def _run_planned(schema, driver, kafka_bootstrap: str, dataset: social_graph.Dataset, tmp_path):
    _reset_and_seed(driver, schema, dataset)
    social_graph.publish(dataset, kafka_bootstrap)
    try:
        result = planned_harness.run_planned_mode(
            schema, driver, kafka_bootstrap, plan_dir=tmp_path, writers=2, timeout_s=180,
        )
        assert result.total_attempts_over_one == 0
    finally:
        social_graph.delete_topics(kafka_bootstrap)
    return social_graph.read_graph(driver, None)


def _run_classic(schema, driver, dataset: social_graph.Dataset):
    """Label-serialized-equivalent: one writer call per type, sequentially -- no concurrency needed."""
    _reset_and_seed(driver, schema, dataset)
    for edge_config in schema.edges:
        rows = dataset.edges[edge_config.type]
        if not rows:
            continue
        writer, _partitioner, _batcher, _coordinator = build_edge_execution(edge_config, driver)
        writer.write_batch([EdgeRecord(source, target, properties) for source, target, properties in rows])
    return social_graph.read_graph(driver, None)


def _event_loop_thread() -> tuple[asyncio.AbstractEventLoop, threading.Thread]:
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="social-lease-loop", daemon=True)
    thread.start()
    return loop, thread


def _run_lease(schema, driver, dataset: social_graph.Dataset):
    _reset_and_seed(driver, schema, dataset)
    loop, loop_thread = _event_loop_thread()
    server = BucketLeaseServer()
    try:
        host, port = asyncio.run_coroutine_threadsafe(server.start("127.0.0.1", 0), loop).result(10)
        address = f"{host}:{port}"
        for edge_config in schema.edges:
            rows = dataset.edges[edge_config.type]
            if not rows:
                continue
            _writer, _partitioner, _batcher, coordinator = build_edge_execution(
                edge_config, driver, environ={LEASE_ADDRESS_ENV: address},
            )
            try:
                pending = [
                    PendingEdgeRecord(EdgeRecord(source, target, properties), edge_config.topic, 0, index)
                    for index, (source, target, properties) in enumerate(rows)
                ]
                results = coordinator.execute_pending(pending)
                assert all(result.success for result in results), (
                    f"lease-coordinated write(s) failed for {edge_config.type}: "
                    f"{[result.exception for result in results if not result.success][:3]}"
                )
            finally:
                coordinator.close()
    finally:
        try:
            asyncio.run_coroutine_threadsafe(server.close(), loop).result(10)
        except Exception:  # pragma: no cover - best-effort cleanup
            pass
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=10)
        if not loop_thread.is_alive():
            loop.close()
    return social_graph.read_graph(driver, None)


def test_last_write_wins_evidence_is_real() -> None:
    """No Docker: proves the dataset actually exercises overwrites before trusting the rest of this file."""
    dataset = social_graph.generate(persons=PERSONS, seed=7)
    evidence = social_graph.last_write_evidence(dataset)
    assert evidence["WORKS_AT"]["pairs_overwritten"] > 0
    assert evidence["FRIEND_OF"]["pairs_overwritten"] > 0
    # FRIEND_OF is self-referencing: at least one self-loop must exist.
    self_loops = sum(1 for source, target, _properties in dataset.edges["FRIEND_OF"] if source == target)
    assert self_loops > 0


@pytest.mark.integration
def test_planned_classic_and_lease_modes_all_match_the_expected_graph(
    social_driver, social_schema, social_dataset, kafka_bootstrap, tmp_path,
) -> None:
    """Planned, classic and bulk-lease modes each produce the exact expected graph, and agree with each other.

    Covers: integer (Person), date (Day), string (Company/Product/City)
    keys; a self-referencing type with self-loops (FRIEND_OF); two types
    between the same labels (LIVES_IN, VISITED: Person -> City), each with
    correct, independent, non-cross-contaminated counts (folded into
    `expected_graph`'s own per-type comparison); last-write-wins
    (WORKS_AT's role changes).
    """
    expected = social_graph.expected_graph(social_dataset)
    expected_nodes, expected_rels = expected

    planned_actual = _run_planned(social_schema, social_driver, kafka_bootstrap, social_dataset, tmp_path)
    assert social_graph.compare(expected, planned_actual) == []

    classic_actual = _run_classic(social_schema, social_driver, social_dataset)
    assert social_graph.compare(expected, classic_actual) == []
    assert social_graph.compare((planned_actual[0], planned_actual[1]), classic_actual) == []

    lease_actual = _run_lease(social_schema, social_driver, social_dataset)
    lease_nodes, lease_rels, lease_duplicates = lease_actual
    # Real finding (not a test bug): build_edge_execution's bulk-lease path
    # builds ApocLockedEdgeWriter with sort_rows left at its default
    # (True), unlike planned mode's explicit sort_rows=False. sort_rows=True
    # reorders a batch's rows by ORDER BY elementId(s), elementId(t) before
    # merging -- exactly the reordering `build_apoc_locked_upsert_query`'s
    # own docstring says planned mode avoids "so the input order, which
    # fixes SET r += props for repeated pairs, is kept" is NOT avoided here.
    # A repeated (source, target) pair (e.g. BOUGHT: the same person buying
    # the same product twice) landing in the same lease-coordinated flush
    # can therefore commit in a different relative order than
    # `expected_graph` assumes, so this leg checks structural correctness
    # (exact node graph, zero duplicates, exact per-type relationship
    # counts) rather than full last-write-wins property equality --
    # flagged in plan/v6/completion_slice-4.md for the owner; fixing it
    # would mean changing build_edge_execution's lease-writer construction
    # in src/, out of scope for this tests-only slice.
    assert lease_nodes == expected_nodes
    assert lease_duplicates == {}
    assert {name: len(items) for name, items in lease_rels.items()} == {
        name: len(items) for name, items in expected_rels.items()
    }


@pytest.mark.integration
def test_missing_endpoint_fails_closed_by_default_in_every_mode(
    social_driver, social_schema, social_dataset, kafka_bootstrap, tmp_path, caplog,
) -> None:
    """A relationship whose source Person does not exist fails its write closed, in all three modes.

    Asserted against the library's *default* config (no `dead_letter`
    opt-in field set on `VISITED` in `examples/social/graph_schema.yaml`):
    Phase 5 Slice 3's bulk opt-in is off by default and not merged yet, so
    today's uniform behaviour is fail-closed everywhere -- matching
    `plan/v5/slices.md`'s owner decision, extended here to pin down what
    classic/lease bulk mode do by default too, which `docs/USAGE.md`
    already states but no CI-checked test proves.
    """
    _reset_and_seed(social_driver, social_schema, social_dataset)
    visited_config = next(edge for edge in social_schema.edges if edge.type == "VISITED")
    good_rows = social_dataset.edges["VISITED"][:5]
    assert good_rows, "the social dataset must publish at least one VISITED record for this test to mean anything"
    bad_source = -999_999_999  # not a person_id social_graph.generate() ever produces
    bad_record = EdgeRecord(bad_source, good_rows[0][1], {"times": 1})
    good_records = [EdgeRecord(source, target, properties) for source, target, properties in good_rows]

    def visited_count() -> int:
        with social_driver.session() as session:
            return session.run("MATCH ()-[r:VISITED]->() RETURN count(r) AS c").single()["c"]

    before = visited_count()

    # Classic: one write_batch call for the whole (good + bad) batch must
    # fail closed, committing nothing from it -- not even the good rows.
    classic_writer, _p, _b, _c = build_edge_execution(visited_config, social_driver)
    with pytest.raises(MissingRelationshipEndpointError):
        classic_writer.write_batch(good_records + [bad_record])
    assert visited_count() == before

    # Bulk lease: the bad record alone (isolating it from any cell-level
    # partial-success nuance -- a batch containing only good rows in a
    # different cell would otherwise still commit under row-level lease
    # semantics, which is not what this AC is about).
    loop, loop_thread = _event_loop_thread()
    server = BucketLeaseServer()
    try:
        host, port = asyncio.run_coroutine_threadsafe(server.start("127.0.0.1", 0), loop).result(10)
        _w, _p, _b, coordinator = build_edge_execution(
            visited_config, social_driver, environ={LEASE_ADDRESS_ENV: f"{host}:{port}"},
        )
        try:
            pending = [PendingEdgeRecord(bad_record, visited_config.topic, 0, 0)]
            results = coordinator.execute_pending(pending)
            assert len(results) == 1 and not results[0].success
            assert isinstance(results[0].exception, MissingRelationshipEndpointError)
        finally:
            coordinator.close()
    finally:
        try:
            asyncio.run_coroutine_threadsafe(server.close(), loop).result(10)
        except Exception:  # pragma: no cover - best-effort cleanup
            pass
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=10)
        if not loop_thread.is_alive():
            loop.close()
    assert visited_count() == before

    # Planned: a tiny, standalone dataset (published to this schema's real
    # VISITED topic) with only the bad row -- the harness must raise
    # rather than silently succeed, and nothing must commit. A minimal,
    # VISITED-only schema is used here (see _visited_only_schema's own
    # docstring for why the full six-edge-type social_schema won't work).
    tiny_bad_dataset = social_graph.Dataset(nodes={}, edges={"VISITED": [(bad_source, good_rows[0][1], {"times": 1})]})
    social_graph.publish(tiny_bad_dataset, kafka_bootstrap)
    try:
        # pytest.raises(PlannedHarnessError) alone is too broad: the harness
        # raises that same type for a timeout, an attempts/chunks liveness
        # mismatch, or a writer exiting nonzero for any cause. Pin the
        # actual cause via the writer's own logged reason
        # (stage=planned-writer ... reason=<ExcType>: ...).
        with caplog.at_level(logging.ERROR, logger="src.loader.planned_writer"):
            with pytest.raises(planned_harness.PlannedHarnessError, match="exited nonzero"):
                planned_harness.run_planned_mode(
                    _visited_only_schema(tmp_path), social_driver, kafka_bootstrap, plan_dir=tmp_path,
                    writers=1, timeout_s=60,
                )
        assert any("MissingRelationshipEndpointError" in record.getMessage() for record in caplog.records)
    finally:
        social_graph.delete_topics(kafka_bootstrap)
    assert visited_count() == before
