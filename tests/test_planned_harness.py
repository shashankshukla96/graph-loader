"""Integration test for tests/stress/planned_harness.py (Phase 6 Slice 4, Story 3).

Small, non-hub scale -- proving the harness's own wiring, not any
star-topology/hub claim (Stories 4-7 own that).
"""
from __future__ import annotations

from pathlib import Path
import threading
import uuid

import pytest
import yaml
from confluent_kafka.admin import AdminClient

from src.loader.edge_execution import ApocLockedEdgeWriter
from src.utils.schema_loader import load_schema
from tests.stress import planned_harness, star_topology_data as std


def _small_schema(tmp_path: Path, run_id: str):
    path = tmp_path / f"schema-{run_id}.yaml"
    path.write_text(yaml.safe_dump(std.schema_dict("target_hub", run_id)), encoding="utf-8")
    return load_schema(path)


@pytest.mark.integration
def test_run_planned_mode_matches_the_independently_computed_expected_graph(
    neo4j_driver, clean_neo4j, kafka_bootstrap, tmp_path,
) -> None:
    run_id = uuid.uuid4().hex[:8]
    schema = _small_schema(tmp_path, run_id)
    dataset = std.generate(200, "target_hub", repeat_fraction=0.02)
    std.seed_nodes(neo4j_driver, dataset, schema)
    std.publish(dataset, kafka_bootstrap, schema)
    try:
        result = planned_harness.run_planned_mode(schema, neo4j_driver, kafka_bootstrap, plan_dir=tmp_path, writers=2)
        assert result.total_attempts_over_one == 0
        assert result.attempt_lines == len(result.plan.chunks)
        assert result.ledger["POINTS_AT"]["rows"] == len(dataset.edges["POINTS_AT"])
        actual = std.read_graph(neo4j_driver, schema)
        # Deliberate-fault check on the real Neo4j-backed path: deleting one
        # real relationship must make compare() report it, proving the
        # comparison below isn't vacuously empty for this exact call shape.
        # Use expected_graph's own (last-write-wins-merged) properties to
        # restore it exactly, since the raw first record for this pair may
        # not be its final property set if this pair was one of the
        # repeat_fraction pairs sent twice.
        expected_nodes, expected_rels = std.expected_graph(dataset)
        pair_source, pair_target = next(iter(expected_rels["POINTS_AT"]))
        pair_props = expected_rels["POINTS_AT"][(pair_source, pair_target)]
        with neo4j_driver.session() as session:
            session.run(
                "MATCH (l:Leaf {leaf_id: $leaf_id})-[r:POINTS_AT]->(h:Hub {hub_id: $hub_id}) DELETE r",
                leaf_id=pair_source, hub_id=pair_target,
            ).consume()
        tampered = std.read_graph(neo4j_driver, schema)
        assert std.compare((expected_nodes, expected_rels), tampered) != []
        with neo4j_driver.session() as session:
            session.run(
                "MATCH (l:Leaf {leaf_id: $leaf_id}), (h:Hub {hub_id: $hub_id}) "
                "MERGE (l)-[r:POINTS_AT]->(h) SET r += $props",
                leaf_id=pair_source, hub_id=pair_target, props=pair_props,
            ).consume()
        actual = std.read_graph(neo4j_driver, schema)
        problems = std.compare((expected_nodes, expected_rels), actual)
        assert problems == []
    finally:
        std.delete_topics(kafka_bootstrap, std.edge_topic_names(schema))


@pytest.mark.integration
def test_run_planned_mode_raises_on_a_writer_failure_instead_of_a_partial_result(
    neo4j_driver, clean_neo4j, kafka_bootstrap, tmp_path, monkeypatch,
) -> None:
    """One write fails; the run must still fail closed as a whole, not silently succeed partially.

    Only the *first* write_rows call (across every worker thread) fails --
    proving one bad chunk fails the whole run, not that every writer is
    broken (which would prove nothing about partial-result handling). The
    dispatcher is itself fail-fast: once one chunk reports FAIL, every other
    writer's next `NEXT` gets `ERR failed ...` and exits promptly (not
    "keeps writing more chunks" -- production's plan-level semantics are
    fail-closed the instant any chunk fails, confirmed by this test: only
    one write_rows call ever happens).
    """
    run_id = uuid.uuid4().hex[:8]
    schema = _small_schema(tmp_path, run_id)
    dataset = std.generate(200, "target_hub", repeat_fraction=0.0)
    std.seed_nodes(neo4j_driver, dataset, schema)
    std.publish(dataset, kafka_bootstrap, schema)

    original_write_rows = ApocLockedEdgeWriter.write_rows
    call_count = {"n": 0}
    lock = threading.Lock()

    def _boom_once(self, rows, *, metadata=None):
        with lock:
            call_count["n"] += 1
            is_first_call = call_count["n"] == 1
        if is_first_call:
            raise RuntimeError("deliberately broken writer for test_run_planned_mode_raises_on_a_writer_failure")
        return original_write_rows(self, rows, metadata=metadata)

    monkeypatch.setattr(ApocLockedEdgeWriter, "write_rows", _boom_once)
    try:
        with pytest.raises(planned_harness.PlannedHarnessError):
            planned_harness.run_planned_mode(
                schema, neo4j_driver, kafka_bootstrap, plan_dir=tmp_path, writers=3, chunk_rows=10, timeout_s=60,
            )
        # Fail-fast: the dispatcher marks the whole plan failed the instant
        # one chunk fails, so no other writer ever calls write_rows.
        assert call_count["n"] == 1, "expected exactly one write attempt before the plan failed closed"
    finally:
        std.delete_topics(kafka_bootstrap, std.edge_topic_names(schema))


@pytest.mark.integration
def test_run_planned_mode_twice_leaves_no_leftover_chunk_topic(
    neo4j_driver, clean_neo4j, kafka_bootstrap, tmp_path,
) -> None:
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    before = set(admin.list_topics(timeout=20).topics)
    for _ in range(2):
        run_id = uuid.uuid4().hex[:8]
        schema = _small_schema(tmp_path, run_id)
        dataset = std.generate(30, "target_hub", repeat_fraction=0.0)
        std.seed_nodes(neo4j_driver, dataset, schema)
        std.publish(dataset, kafka_bootstrap, schema)
        try:
            planned_harness.run_planned_mode(schema, neo4j_driver, kafka_bootstrap, plan_dir=tmp_path, writers=1)
        finally:
            std.delete_topics(kafka_bootstrap, std.edge_topic_names(schema))
        after = set(admin.list_topics(timeout=20).topics)
        leftover_chunk_topics = {topic for topic in after - before if topic.startswith("graph-loader-plan-chunks-")}
        assert not leftover_chunk_topics, f"leftover chunk topic(s) after run {run_id}: {leftover_chunk_topics}"


def test_harness_never_imports_docker_service() -> None:
    source = Path(planned_harness.__file__).read_text(encoding="utf-8")
    assert "docker_service" not in source
