"""v7-S7 story 7.6: planned mode plans each label pair, counts hub degrees per label, and records the pair."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from multi_pair_support import BACKFILL, COMMENTS, POSTS, GraphStore, likes_schema, likes_schema_dict, record
from src.loader.edge_execution import build_edge_execution
from src.loader.edge_loader import PendingEdgeRecord
from src.loader.edge_plan import EdgePlan, EdgePlanError
from src.loader.edge_planner import (
    EdgePlanError as PlannerError,
    IterableEdgeSource,
    KafkaEdgePlanner,
    PlanSettings,
    source_tasks,
)
from src.loader.edge_records import normalize_edge_record
from src.loader.edge_execution import ApocLockedEdgeWriter
from src.loader.planned_writer import ChunkDecoder, build_planned_writers, writer_for
from src.models.schema import GraphSchema
from src.orchestrator.planned_edges import PlannedEdgeError, validate_plan_against_schema
from test_edge_planner import FakeCluster, FakeProducer

GOLDEN = Path(__file__).resolve().parent / "golden" / "multi_pair_plan.json"


def _plan(schema, records: dict[str, list[dict]], tmp_path: Path, *, hub_degree: int = 0, workers: int = 1,
          chunk_rows: int = 3):
    cluster = FakeCluster()
    for edge in schema.edges:
        for topic in edge.topics:
            cluster.topic(topic, 2)
            for index, value in enumerate(records.get(topic, [])):
                cluster.append(topic, index % 2, json.dumps(value, sort_keys=True).encode())
    settings = PlanSettings(buckets=4, hub_degree=hub_degree, chunk_rows=chunk_rows, topic="chunks", partitions=2,
                            workers=workers, memory_mb=0, spill_dir=str(tmp_path / "spill"))
    planner = KafkaEdgePlanner(SimpleNamespace(nodes=schema.nodes, edges=list(schema.edges)), "kafka:1", settings,
                               plan_id="run-1", context="inline", consumer_factory=cluster.consumer,
                               producer_factory=lambda: FakeProducer(cluster), admin_factory=cluster.admin)
    planner.start()
    result = planner.finish(tmp_path / "plan.json")
    return result.plan, cluster


def _records() -> dict[str, list[dict]]:
    """Colliding ids: Person p likes Post i and Comment i; a newer value in the backfill for some."""
    out = {POSTS: [], BACKFILL: [], COMMENTS: []}
    for i in range(24):
        out[POSTS].append(record(POSTS, i % 6, i % 8, f"2024-01-01T00:00:{i:02d}+00:00", note=f"p{i}"))
        out[COMMENTS].append(record(COMMENTS, i % 6, i % 8, f"2024-01-01T00:01:{i:02d}+00:00", note=f"c{i}"))
        if i % 3 == 0:
            out[BACKFILL].append(record(BACKFILL, i % 6, i % 8, f"2024-01-02T00:00:{i:02d}+00:00", note=f"b{i}"))
    return out


def _write_plan(plan: EdgePlan, cluster: FakeCluster, schema, store: GraphStore) -> None:
    """Write every chunk in id order (a chunk's predecessor always has a lower id) like the planned writers."""
    edges = {edge.type: edge for edge in schema.edges}
    decoder = ChunkDecoder(plan, edges)
    writers = build_planned_writers(plan, edges, lambda config: ApocLockedEdgeWriter(
        store.driver(), config, explicit_locks=False, sort_rows=False, missing_endpoint_error_factory=RuntimeError))
    for chunk in plan.chunks:
        rows = decoder.decode(chunk, cluster.logs[plan.topic][chunk.partition][chunk.offset])
        writer_for(writers, chunk).write_rows(rows)


def _classic(schema, records: dict[str, list[dict]], store: GraphStore) -> None:
    edge = schema.edges[0]
    _w, _p, _b, coordinator = build_edge_execution(edge, store.driver(), mode="bulk")
    for topic in edge.topics:  # per pair, order_by makes the result independent of cross-topic order
        pending = [PendingEdgeRecord(normalize_edge_record(raw, edge.topic_view(topic)), topic, index % 2, index)
                   for index, raw in enumerate(records[topic])]
        assert all(result.success for result in coordinator.execute_pending(tuple(pending)))


def _nodes_store() -> GraphStore:
    return GraphStore({"Person": set(range(6)), "Post": set(range(8)), "Comment": set(range(8))})


# ── AC 1, 6 ──────────────────────────────────────────────────────────────────


def test_a_two_pair_plan_has_chunks_of_both_pairs_with_their_own_resources(tmp_path) -> None:
    schema = likes_schema()
    plan, cluster = _plan(schema, _records(), tmp_path)
    item = plan.types["LIKES"]
    assert item.is_multi_pair and [pair.labels for pair in item.pairs] == [("Person", "Post"), ("Person", "Comment")]
    assert [pair.topics for pair in item.pairs] == [(POSTS, BACKFILL), (COMMENTS,)]
    assert item.pairs[0].rows == 24 + 8 and item.pairs[1].rows == 24
    assert {chunk.pair for chunk in plan.chunks} == {0, 1}
    for chunk in plan.chunks:
        labels = {plan.resources.label_of(resource) for resource in chunk.resources}
        assert labels == set(plan.labels_of(chunk)) == set(item.pairs[chunk.pair].labels)
    # Post 7 and Comment 7 (same key) are in different units: their chunks never share a resource set.
    post_units = {chunk.resources for chunk in plan.chunks if chunk.pair == 0}
    comment_units = {chunk.resources for chunk in plan.chunks if chunk.pair == 1}
    assert not post_units & comment_units
    planned, classic = _nodes_store(), _nodes_store()
    _write_plan(plan, cluster, schema, planned)
    _classic(schema, _records(), classic)
    assert planned.rels == classic.rels and len(planned.rels) == 2 * 24  # 6 persons x 8 ids per pair, 24 distinct
    assert all(len(tx["pairs"]) == 1 for tx in planned.transactions)


# ── AC 2: hubs per label ──────────────────────────────────────────────────────


def test_hub_degrees_are_counted_per_label_over_both_pairs(tmp_path) -> None:
    schema = likes_schema()
    records = {POSTS: [record(POSTS, 0, i) for i in range(6)], BACKFILL: [],
               COMMENTS: [record(COMMENTS, 0, i) for i in range(6)]}
    records[POSTS] += [record(POSTS, p, 0) for p in range(1, 6)]
    records[COMMENTS] += [record(COMMENTS, p, 0) for p in range(1, 6)]  # Post 0 and Comment 0: 6 each
    plan, _cluster = _plan(schema, records, tmp_path, hub_degree=10)
    hubs = {plan.resources.describe(r) for r in range(plan.resources.count) if "hub" in plan.resources.describe(r)}
    assert hubs == {"Person#hub=0"}


# ── AC 3: reversed pairs ─────────────────────────────────────────────────────


def test_reversed_pairs_form_two_chains_each_written_in_its_direction(tmp_path) -> None:
    data = likes_schema_dict(topics=(POSTS,))
    data["edges"][0]["topics"].append({"name": "posts-like-persons", "nodes": {"source": "Post", "target": "Person"},
                                       "source_key_property": "post_id", "target_key_property": "person_id"})
    schema = GraphSchema.model_validate(data)
    reverse = {"source": {"post_id": 1}, "target": {"person_id": 2},
               "properties": {"liked_at": "2024-01-01T00:00:00+00:00"}}
    records = {POSTS: [record(POSTS, 2, 1)] * 2, "posts-like-persons": [reverse] * 2}
    plan, cluster = _plan(schema, records, tmp_path, chunk_rows=1)
    by_pair = {pair: [c for c in plan.chunks if c.pair == pair] for pair in (0, 1)}
    assert by_pair[0][0].resources == by_pair[1][0].resources  # the same resource set ...
    assert {c.after for c in by_pair[0]} - {-1} <= {c.id for c in by_pair[0]}  # ... but two chains
    assert {c.after for c in by_pair[1]} - {-1} <= {c.id for c in by_pair[1]}
    store = GraphStore({"Person": {2}, "Post": {1}})
    _write_plan(plan, cluster, schema, store)
    assert set(store.rels) == {("LIKES", "Person", 2, "Post", 1), ("LIKES", "Post", 1, "Person", 2)}


# ── AC 4, 5: plan file and plan-vs-schema ────────────────────────────────────


def test_the_plan_json_round_trips_with_pairs_and_eleven_field_chunks(tmp_path) -> None:
    plan, _cluster = _plan(likes_schema(), _records(), tmp_path)
    payload = plan.to_json()
    assert "source" not in payload["types"]["LIKES"] and len(payload["types"]["LIKES"]["pairs"]) == 2
    assert {len(entry) for entry in payload["chunks"]} == {11}
    again = EdgePlan.from_json(json.loads(json.dumps(payload)))
    assert again.to_json() == payload
    bad = copy.deepcopy(payload)
    bad["chunks"][0] = bad["chunks"][0][:10]
    with pytest.raises(EdgePlanError, match="10 fields"):
        EdgePlan.from_json(bad)
    bad = copy.deepcopy(payload)
    bad["chunks"][0][10] = 5
    with pytest.raises(EdgePlanError, match="no label pair 5"):
        EdgePlan.from_json(bad)
    old_reader = copy.deepcopy(payload["types"]["LIKES"])
    with pytest.raises(KeyError):
        old_reader["source"]  # a pre-S7 reader ("source"/"target" required) fails closed


def test_the_two_pair_plan_json_golden(tmp_path) -> None:
    """New in S7 (no pre-S7 counterpart): pins the multi-pair plan format and chunk layout."""
    plan, _cluster = _plan(likes_schema(), _records(), tmp_path)
    payload = plan.to_json()
    payload.pop("stats")
    # Recorded on the S7 tree with: json.dumps(payload, indent=1, sort_keys=True) (stats removed).
    assert payload == json.loads(GOLDEN.read_text(encoding="utf-8"))


def test_plan_vs_schema_refuses_a_rebound_topic_a_removed_pair_and_a_pair_field_on_a_single_pair_type(tmp_path):
    schema = likes_schema()
    plan, _cluster = _plan(schema, _records(), tmp_path)
    validate_plan_against_schema(plan, schema)
    rebound = likes_schema_dict()
    rebound["edges"][0]["topics"][1]["nodes"]["target"] = "Comment"
    rebound["edges"][0]["topics"][1]["target_key_property"] = "comment_id"
    with pytest.raises(PlannedEdgeError, match="plan binds likes-posts-backfill to Person->Post; the schema binds it "
                                               "to Person->Comment; re-plan the load"):
        validate_plan_against_schema(plan, GraphSchema.model_validate(rebound))
    single = likes_schema(topics=(POSTS, BACKFILL))
    with pytest.raises(PlannedEdgeError):
        validate_plan_against_schema(plan, single)  # the Comment pair (and its topic) was removed
    payload = _plan(single, _records(), tmp_path / "single")[0].to_json()
    payload["chunks"][0].append(0)
    with pytest.raises(EdgePlanError, match="10 fields"):
        EdgePlan.from_json(payload)


# ── AC 7: producer sources ───────────────────────────────────────────────────


def test_a_producer_source_of_a_multi_pair_type_must_name_its_topic() -> None:
    schema = likes_schema()
    with pytest.raises(PlannerError, match="source for LIKES must name its topic"):
        source_tasks(schema, [IterableEdgeSource("LIKES", [record(POSTS, 1, 1)])])
    with pytest.raises(PlannerError, match=r"\{type: rows\} mapping"):
        source_tasks(schema, {"LIKES": [record(POSTS, 1, 1)]})
    [task] = source_tasks(schema, [IterableEdgeSource("LIKES", [record(COMMENTS, 1, 1)], topic=COMMENTS)])
    assert task.pair == 1 and task.edge.nodes.target == "Comment"


def test_producer_sources_of_two_pairs_may_share_an_order(tmp_path) -> None:
    """Review round 4 N1: orders are distinct per (type, pair), and the plan matches the Kafka planner's."""
    from src.loader.edge_planner import plan_and_publish_edges

    schema = likes_schema()
    records = _records()
    cluster = FakeCluster()
    settings = PlanSettings(buckets=4, hub_degree=0, chunk_rows=3, topic="chunks", partitions=2, workers=1,
                            memory_mb=0, spill_dir=str(tmp_path / "spill"))
    sources = [IterableEdgeSource("LIKES", records[POSTS], topic=POSTS),
               IterableEdgeSource("LIKES", records[BACKFILL], order=1, topic=BACKFILL),
               IterableEdgeSource("LIKES", records[COMMENTS], topic=COMMENTS)]
    result = plan_and_publish_edges(schema, sources, bootstrap_servers="kafka:1", plan_path=tmp_path / "p.json",
                                    settings=settings, plan_id="run-1", context="inline",
                                    admin_factory=cluster.admin, producer_factory=lambda: FakeProducer(cluster))
    assert [pair.rows for pair in result.plan.types["LIKES"].pairs] == [32, 24]
    store = _nodes_store()
    _write_plan(result.plan, cluster, schema, store)
    expected = _nodes_store()
    _classic(schema, records, expected)
    assert store.rels == expected.rels


def _normalize_in_child(task, raw):
    from src.loader.planned_writer import compile_record_normalizer

    return task.pair, task.edge.pair_index, compile_record_normalizer(task.edge)(raw)


def test_pair_view_tasks_survive_a_spawned_planner_worker() -> None:
    """Review round 4 N4: read tasks of a multi-pair type are pickled to planner processes."""
    import multiprocessing

    from src.loader.edge_planner import kafka_read_tasks

    schema = likes_schema(comment_key_type="string")
    boundary = {topic: [(0, 0, 1)] for topic in schema.edges[0].topics}
    tasks = kafka_read_tasks(schema.edges, boundary, 1, "kafka:1", "g")
    comment_task = next(task for task in tasks if task.topic == COMMENTS)
    with multiprocessing.get_context("spawn").Pool(1) as pool:
        pair, index, (source_key, target_key, _props) = pool.apply(
            _normalize_in_child, (comment_task, record(COMMENTS, 1, "7")))
    assert (pair, index, source_key, target_key) == (1, 1, 1, "7")
    assert [task.order >> 20 for task in tasks] == [0, 1, 2]  # the parent's precedence ranks
