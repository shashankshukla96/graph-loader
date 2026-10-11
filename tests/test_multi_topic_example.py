"""The multi-topic example: schema, generator, expected graph (no Kafka or Neo4j), plus a planner replay."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from examples.multi_topic import multi_topic_graph as mt  # noqa: E402
from src.loader.edge_planner import KafkaEdgePlanner, PlanSettings  # noqa: E402
from src.loader.edge_records import normalize_edge_record  # noqa: E402
from src.models.schema import require_no_precedence_in_stream  # noqa: E402
from src.utils.schema_loader import load_schema  # noqa: E402
from test_edge_planner import FakeCluster, FakeProducer, chains  # noqa: E402


@pytest.fixture(scope="module")
def schema():
    return load_schema(mt.SCHEMA_PATH)


def test_schema_declares_the_two_rules_and_the_topics_the_generator_publishes(schema) -> None:
    edges = {edge.type: edge for edge in schema.edges}
    assert edges["RATED"].conflict_rule == "order_by" and edges["RATED"].order_by == "rated_at"
    assert edges["WISHLISTED"].conflict_rule == "precedence"
    for edge_type, edge in edges.items():
        assert tuple(edge.topics) == mt.EDGE_TOPICS[edge_type][0]
        assert edge.is_multi_topic
    assert {node.topic for node in schema.nodes} == {topic for topic, _ in mt.NODE_TOPICS.values()}


def test_precedence_type_is_refused_in_stream_and_order_by_alone_is_not(schema) -> None:
    with pytest.raises(ValueError, match="precedence"):
        require_no_precedence_in_stream(schema, "stream")
    require_no_precedence_in_stream(schema, "bulk")
    only_order_by = schema.model_copy(update={"edges": [e for e in schema.edges if e.type == "RATED"]})
    require_no_precedence_in_stream(only_order_by, "stream")


def test_generator_is_deterministic_and_exercises_both_rules() -> None:
    first, second = mt.generate(300, seed=5), mt.generate(300, seed=5)
    assert first.edges == second.edges and first.nodes == second.nodes
    evidence = mt.conflict_evidence(first)
    assert evidence["RATED"]["pairs_in_both_topics"] > 0 and evidence["WISHLISTED"]["pairs_in_both_topics"] > 0
    app = {(s, t): p["rated_at"] for s, t, p in first.edges["RATED"]["mt-rated-app"]}
    imported = {(s, t): p["rated_at"] for s, t, p in first.edges["RATED"]["mt-rated-import"]}
    both = app.keys() & imported.keys()
    assert any(app[pair] > imported[pair] for pair in both), "newer value must sit in the earlier topic sometimes"
    assert any(app[pair] < imported[pair] for pair in both), "and in the later topic sometimes"
    assert all(app[pair] != imported[pair] for pair in both)  # no ties
    corrections = first.edges["WISHLISTED"]["mt-wishlist-corrections"]
    assert any("note" not in props for _, _, props in corrections), "a property the later topic omits must survive"


def test_wire_records_pass_the_library_normalization(schema) -> None:
    edges = {edge.type: edge for edge in schema.edges}
    dataset = mt.generate(100, seed=3)
    for edge_type, by_topic in dataset.edges.items():
        for rows in by_topic.values():
            for source, target, properties in rows[:30]:
                record = normalize_edge_record(mt.edge_record(edge_type, source, target, properties), edges[edge_type])
                assert (record.source_key, record.target_key) == (source, target)


def test_expected_graph_applies_order_by_and_precedence() -> None:
    dataset = mt.generate(400, seed=11)
    _nodes, rels = mt.expected_graph(dataset)
    app = {(s, t): p for s, t, p in dataset.edges["RATED"]["mt-rated-app"]}
    imported = {(s, t): p for s, t, p in dataset.edges["RATED"]["mt-rated-import"]}
    for pair in app.keys() & imported.keys():
        winner = max(app[pair], imported[pair], key=lambda p: p["rated_at"])
        assert rels["RATED"][pair]["rated_at"] == winner["rated_at"]
        assert rels["RATED"][pair]["channel"] == winner["channel"]
    base = {(s, t): p for s, t, p in dataset.edges["WISHLISTED"]["mt-wishlist-base"]}
    for s, t, props in dataset.edges["WISHLISTED"]["mt-wishlist-corrections"]:
        merged = rels["WISHLISTED"][(s, t)]
        assert merged["priority"] == props["priority"]
        if (s, t) in base:
            assert merged["note"] == props.get("note", base[(s, t)]["note"])
    assert len(rels["RATED"]) == mt.conflict_evidence(dataset)["RATED"]["pairs"]


def test_planned_chain_replay_of_the_precedence_type_equals_the_expected_graph(schema, tmp_path) -> None:
    """Real planner over fake Kafka: replaying each unit's chain with ``SET r += props`` gives the expected rels."""
    dataset = mt.generate(150, seed=2)
    edge = next(e for e in schema.edges if e.type == "WISHLISTED")
    cluster = FakeCluster()
    for topic in reversed(edge.topics):  # published in the opposite order on purpose
        cluster.topic(topic, 3)
        for source, target, properties in dataset.edges["WISHLISTED"][topic]:
            record = mt.edge_record("WISHLISTED", source, target, properties)
            cluster.append(topic, source % 3, json.dumps(record).encode())
    settings = PlanSettings(buckets=3, hub_degree=0, chunk_rows=5, topic="chunks", partitions=2, workers=2,
                            memory_mb=0, spill_dir=str(tmp_path / "spill"))
    planner = KafkaEdgePlanner(SimpleNamespace(nodes=schema.nodes, edges=[edge]), "kafka:1", settings, plan_id="r",
                               context="inline", consumer_factory=cluster.consumer,
                               producer_factory=lambda: FakeProducer(cluster), admin_factory=cluster.admin)
    planner.start()
    plan = planner.finish(tmp_path / "plan.json").plan
    stored: dict = {}
    for records in chains(plan, cluster).values():
        for record in records:
            pair = (record["source"]["customer_id"], record["target"]["sku"])
            stored.setdefault(pair, {}).update(record["properties"])
    assert stored == mt.expected_graph(dataset)[1]["WISHLISTED"]


def test_compare_reports_missing_wrong_and_duplicate_entities() -> None:
    dataset = mt.generate(200, seed=9)
    nodes, rels = mt.expected_graph(dataset)
    assert mt.compare((nodes, rels), (nodes, rels, {})) == []
    wrong = copy.deepcopy(rels)
    pair = next(iter(wrong["RATED"]))
    wrong["RATED"][pair]["stars"] = -1
    assert "wrong properties 1" in mt.compare((nodes, rels), (nodes, wrong, {}))[0]
    assert mt.compare((nodes, rels), (nodes, rels, {"RATED": 2})) == ["RATED: 2 duplicate entities"]


# -- dead-letter variant (v7-S4) ------------------------------------------------


def test_dead_letter_variant_has_one_dlq_for_the_two_topic_type_and_runs_in_stream() -> None:
    from src.models.schema import edge_dead_letter_topic_name, edge_parked_pair_index_topic_name, edge_parked_topic_name

    variant = load_schema(Path(mt.SCHEMA_PATH).with_name("graph_schema_dead_letter.yaml"))
    require_no_precedence_in_stream(variant, "stream")
    (rated,) = variant.edges
    assert rated.topics == list(mt.EDGE_TOPICS["RATED"][0]) and rated.order_by == "rated_at"
    assert edge_dead_letter_topic_name(rated) == "RATED.dlq"
    assert edge_parked_topic_name(rated) == "RATED.dlq.parked"
    assert edge_parked_pair_index_topic_name(rated) == "RATED.dlq.parked-pairs"  # unused: order_by


def test_publish_only_edges_or_nodes_and_compare_limited_to_types(monkeypatch) -> None:
    import confluent_kafka
    import confluent_kafka.admin

    dataset = mt.generate(50, 7)
    produced: list = []

    class _Admin:
        def __init__(self, config):
            pass

        def list_topics(self, timeout=None):
            return SimpleNamespace(topics={t: None for t in mt.all_topics()})

    class _Producer:
        def __init__(self, config):
            pass

        def produce(self, topic, key=None, value=None, **_kw):
            produced.append(topic)

        def poll(self, *_a):
            return 0

        def flush(self, *_a):
            return 0

    monkeypatch.setattr(confluent_kafka, "Producer", _Producer)
    monkeypatch.setattr(confluent_kafka.admin, "AdminClient", _Admin)
    mt.publish(dataset, "k", only="edges")
    assert produced and not set(produced) & {"mt-customer", "mt-product"}
    produced.clear()
    mt.publish(dataset, "k", only="nodes")
    assert set(produced) == {"mt-customer", "mt-product"}
    expected = mt.expected_graph(dataset)
    nodes, rels = expected
    actual = (copy.deepcopy(nodes), {"RATED": copy.deepcopy(rels["RATED"]), "WISHLISTED": {}}, {})
    assert mt.compare(expected, actual, {"RATED"}) == []
    assert mt.compare(expected, actual) != []


# -- multi-pair variant (v7-S7) ------------------------------------------------


def test_multi_pair_variant_schema_binds_one_pair_per_topic() -> None:
    variant = load_schema(mt.MULTI_PAIR_SCHEMA_PATH)
    (likes,) = variant.edges
    assert likes.is_multi_pair and likes.conflict_rule == "order_by"
    assert list(likes.topics) == list(mt.MP_EDGE_TOPICS)
    assert [pair.labels for pair in likes.endpoint_pairs] == list(mt.MP_PAIRS)
    for topic, (target_label, target_key) in mt.MP_EDGE_TOPICS.items():
        pair = likes.pair_for_topic(topic)
        assert (pair.target_label, pair.target_key_property) == (target_label, target_key)
    assert {node.topic for node in variant.nodes} == {topic for topic, _ in mt.MP_NODE_TOPICS.values()}


def test_multi_pair_generator_has_colliding_ids_both_pairs_and_a_hub() -> None:
    first, second = mt.generate_multi_pair(300, seed=4), mt.generate_multi_pair(300, seed=4)
    assert first.edges == second.edges and first.nodes == second.nodes
    rows = first.edges["LIKES"]
    assert {p["post_id"] for p in first.nodes["Post"]} == {c["comment_id"] for c in first.nodes["Comment"]}
    posts = {(s, t) for topic in ("mt-likes-posts", "mt-likes-posts-import") for s, t, _ in rows[topic]}
    comments = {(s, t) for s, t, _ in rows["mt-likes-comments"]}
    assert len(posts & comments) > 50  # Customer c likes Post i AND Comment i
    live = {(s, t): p["liked_at"] for s, t, p in rows["mt-likes-posts"]}
    imported = {(s, t): p["liked_at"] for s, t, p in rows["mt-likes-posts-import"]}
    both = live.keys() & imported.keys()
    assert any(live[k] > imported[k] for k in both) and any(live[k] < imported[k] for k in both)
    items = len(first.nodes["Post"])
    assert {t for s, t in posts if s == 0} == set(range(items)) == {t for s, t in comments if s == 0}


def test_multi_pair_expected_graph_is_per_labelled_pair_and_records_normalize() -> None:
    variant = load_schema(mt.MULTI_PAIR_SCHEMA_PATH)
    (likes,) = variant.edges
    dataset = mt.generate_multi_pair(200, seed=3)
    for topic, rows in dataset.edges["LIKES"].items():
        for source, target, properties in rows[:20]:
            normalize_edge_record(mt.multi_pair_record(topic, source, target, properties), likes.topic_view(topic))
    nodes, rels = mt.expected_multi_pair(dataset)
    shared = set(rels[("Customer", "Post")]) & set(rels[("Customer", "Comment")])
    assert shared and any(rels[("Customer", "Post")][k] != rels[("Customer", "Comment")][k] for k in shared)
    assert mt.compare_multi_pair((nodes, rels), (nodes, rels, {})) == []
    assert mt.compare_multi_pair((nodes, rels), (nodes, rels, {"cross_pair": 3})) == ["cross_pair: 3 duplicate entities"]


def test_multi_pair_real_planner_replay_equals_the_expected_graph(tmp_path) -> None:
    variant = load_schema(mt.MULTI_PAIR_SCHEMA_PATH)
    (likes,) = variant.edges
    dataset = mt.generate_multi_pair(150, seed=6)
    cluster = FakeCluster()
    for topic in reversed(likes.topics):
        cluster.topic(topic, 3)
        for source, target, properties in dataset.edges["LIKES"][topic]:
            cluster.append(topic, source % 3, json.dumps(mt.multi_pair_record(topic, source, target, properties)).encode())
    settings = PlanSettings(buckets=3, hub_degree=40, chunk_rows=7, topic="chunks", partitions=2, workers=2,
                            memory_mb=0, spill_dir=str(tmp_path / "spill"))
    planner = KafkaEdgePlanner(SimpleNamespace(nodes=variant.nodes, edges=[likes]), "kafka:1", settings, plan_id="r",
                               context="inline", consumer_factory=cluster.consumer,
                               producer_factory=lambda: FakeProducer(cluster), admin_factory=cluster.admin)
    planner.start()
    plan = planner.finish(tmp_path / "plan.json").plan
    assert len(plan.types["LIKES"].pairs) == 2
    head_pair = {tuple(chunk.key.split(":")[:3]): plan.labels_of(chunk) for chunk in plan.chunks if chunk.after == -1}
    stored: dict = {pair: {} for pair in mt.MP_PAIRS}
    for unit, records in chains(plan, cluster).items():
        pair = head_pair[unit]
        target_key = "post_id" if pair == ("Customer", "Post") else "comment_id"
        for record in records:
            key = (record["source"]["customer_id"], record["target"][target_key])
            properties = normalize_edge_record(record, likes.pair_view(likes.endpoint_pairs[mt.MP_PAIRS.index(pair)])
                                               ).properties
            current = stored[pair].get(key)
            if current is None or properties["liked_at"] > current["liked_at"]:
                stored[pair][key] = {name: mt._canonical(value) for name, value in properties.items()}
    assert stored == mt.expected_multi_pair(dataset)[1]
    hubs = [plan.resources.describe(r) for r in range(plan.resources.count) if "hub" in plan.resources.describe(r)]
    assert "Customer#hub=0" in hubs
