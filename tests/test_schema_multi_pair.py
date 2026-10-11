"""v7-S7 story 7.2: one relationship type over several label pairs, each topic binding its own pair."""
from __future__ import annotations

import copy
import logging
import pickle
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from multi_pair_support import BACKFILL, COMMENTS, POSTS, likes, likes_edge, likes_schema, likes_schema_dict
from src.models.schema import (
    EdgeConfig,
    EndpointPair,
    GraphSchema,
    MultiPairEdgeError,
    MultiTopicEdgeError,
    arrival_conflict_warnings,
    edge_dead_letter_topic_name,
    edge_parked_pair_index_topic_name,
    edge_parked_topic_name,
    precedence_edge_types,
    require_no_precedence_in_stream,
)
from src.utils.schema_loader import load_schema

REPO = Path(__file__).resolve().parent.parent

EXPECTED_PAIRS = (
    EndpointPair(0, "Person", "Post", "person_id", "post_id", (POSTS, BACKFILL)),
    EndpointPair(1, "Person", "Comment", "person_id", "comment_id", (COMMENTS,)),
)

TOP_LEVEL_FORM = """
nodes:
  - {label: Person, topic: persons, key_property: person_id,
     properties: {person_id: {type: integer, required: true}}}
  - {label: Post, topic: posts, key_property: post_id, properties: {post_id: {type: integer, required: true}}}
  - {label: Comment, topic: comments, key_property: comment_id,
     properties: {comment_id: {type: integer, required: true}}}
edges:
  - type: LIKES
    nodes: {source: Person, target: Post}
    source_key_property: person_id
    target_key_property: post_id
    order_by: liked_at
    topics:
      - likes-posts
      - likes-posts-backfill
      - {name: likes-comments, nodes: {source: Person, target: Comment},
         source_key_property: person_id, target_key_property: comment_id}
    properties:
      liked_at: {type: datetime, required: true}
"""


def _schema_with(edge: dict) -> dict:
    data = likes_schema_dict()
    data["edges"] = [edge]
    return data


def _single_topic_sub_edge_s4(edge: EdgeConfig, topic: str) -> EdgeConfig:
    """S4's ``cli._single_topic_sub_edge`` as of d334cdd, kept here to compare ``topic_view`` against."""
    update: dict = {"topics": [topic], "topic_conflict": None}
    parent_dlq = edge_dead_letter_topic_name(edge)
    if edge.dead_letter is not None and parent_dlq is not None:
        pinned = {"topic": parent_dlq}
        if edge.dead_letter.parked_topic is None:
            pinned["parked_topic"] = edge_parked_topic_name(edge)
        update["dead_letter"] = edge.dead_letter.model_copy(update=pinned)
    return edge.model_copy(update=update)


# ── AC 1 ─────────────────────────────────────────────────────────────────────


def test_mapping_form_loads_with_pairs_in_first_appearance_order() -> None:
    edge = likes()
    assert edge.is_multi_pair and edge.is_multi_topic
    assert edge.endpoint_pairs == EXPECTED_PAIRS
    assert edge.topics == [POSTS, BACKFILL, COMMENTS]
    assert edge.endpoint_labels == frozenset({"Person", "Post", "Comment"})
    assert edge.pair_for_topic(BACKFILL) is edge.endpoint_pairs[0]
    assert edge.pair_for_topic(COMMENTS).labels == ("Person", "Comment")
    assert edge.pair_labels_for_topic("removed-topic") == ("?", "removed-topic")
    with pytest.raises(ValueError, match="does not read topic 'nope'"):
        edge.pair_for_topic("nope")


def test_top_level_pair_is_the_default_for_string_entries() -> None:
    schema = GraphSchema.model_validate(yaml.safe_load(TOP_LEVEL_FORM))
    assert schema.edges[0].endpoint_pairs == EXPECTED_PAIRS
    found = schema.pair_for_topic(COMMENTS)
    assert found is not None and found[0] is schema.edges[0] and found[1] == EXPECTED_PAIRS[1]
    assert schema.pair_for_topic("persons") is None


# ── AC 2, 3 ──────────────────────────────────────────────────────────────────


def _shipped_schema_paths() -> list[str]:
    paths = sorted(str(p.relative_to(REPO)) for p in [*REPO.glob("config/*.yaml"), *REPO.glob("examples/**/*.yaml"),
                                                       *REPO.glob("var/*.yaml"), *REPO.glob("tests/golden/fixtures/*.yaml")])
    out = []
    for path in paths:
        data = yaml.safe_load((REPO / path).read_text(encoding="utf-8"))
        if isinstance(data, dict) and "edges" in data and "nodes" in data:
            out.append(path)
    return out


@pytest.mark.parametrize("path", _shipped_schema_paths())
def test_every_shipped_schema_stays_single_pair(path: str) -> None:
    if "multi_pair" in path:
        pytest.skip("the multi-pair example is multi-pair on purpose")
    schema = load_schema(REPO / path)
    raw = yaml.safe_load((REPO / path).read_text(encoding="utf-8"))
    for edge, raw_edge in zip(schema.edges, raw["edges"]):
        assert edge.is_multi_pair is False
        assert edge.topic_endpoints is None
        assert edge.nodes.source == raw_edge["nodes"]["source"] and edge.nodes.target == raw_edge["nodes"]["target"]
        assert (edge.source_key_property, edge.target_key_property) == (
            raw_edge["source_key_property"], raw_edge["target_key_property"])
        assert edge.source_key_config is not None and edge.target_key_config is not None
        assert edge.single_pair.labels == (edge.nodes.source, edge.nodes.target)
        assert edge.pair_index == 0
        dump = edge.model_dump(exclude_unset=True)
        assert "topic_endpoints" not in dump
        assert {"nodes", "source_key_property", "target_key_property"} <= dump.keys()
    again = GraphSchema.model_validate(schema.model_dump(exclude_unset=True))
    assert again == schema


def test_mapping_entries_of_one_pair_normalise_to_the_single_pair_shape() -> None:
    edge = likes_edge(topics=(POSTS, BACKFILL))
    schema = GraphSchema.model_validate(_schema_with(edge))
    loaded = schema.edges[0]
    assert loaded.is_multi_pair is False
    assert loaded.nodes.source == "Person" and loaded.nodes.target == "Post"
    assert (loaded.source_key_property, loaded.target_key_property) == ("person_id", "post_id")
    assert "topic_endpoints" not in loaded.model_dump(exclude_unset=True)
    assert loaded.source_key_config.type == "integer"
    reference = copy.deepcopy(edge)
    reference.update(nodes={"source": "Person", "target": "Post"}, source_key_property="person_id",
                     target_key_property="post_id", topics=[POSTS, BACKFILL])
    assert loaded == GraphSchema.model_validate(_schema_with(reference)).edges[0]


# ── AC 4 ─────────────────────────────────────────────────────────────────────


def _broken(kind: str) -> dict:
    edge = likes_edge()
    if kind == "unknown label":
        edge["topics"][2]["nodes"]["target"] = "Komment"
    elif kind == "key differs from node key":
        edge["topics"][2]["target_key_property"] = "id"
    elif kind == "key not required":
        pass  # handled by the schema below
    elif kind == "entry without nodes":
        del edge["topics"][2]["nodes"]
    elif kind == "entry with unknown key":
        edge["topics"][2]["partitions"] = 3
    elif kind == "string entry without a top-level pair":
        edge["topics"][1] = BACKFILL
    elif kind == "unused top-level pair":
        edge.update(nodes={"source": "Person", "target": "Post"}, source_key_property="person_id",
                    target_key_property="post_id")
    elif kind == "same pair, other keys":
        edge["topics"][1]["target_key_property"] = "comment_id"
    elif kind == "topic given a mapping":
        edge = {**edge, "topic": edge["topics"][2]}
        del edge["topics"]
    elif kind == "topic_endpoints keys differ":
        dumped = likes().model_dump(exclude_unset=True)
        dumped["topic_endpoints"].pop(BACKFILL)
        edge = dumped
    return edge


@pytest.mark.parametrize("kind, match", [
    ("unknown label", r"Edge 'LIKES' topic 'likes-comments' target label 'Komment' does not name a declared node"),
    ("key differs from node key", r"Edge 'LIKES' topic 'likes-comments' target_key_property 'id' must match"),
    ("entry without nodes", r"Edge 'LIKES' topic 'likes-comments': a topics mapping entry needs \['nodes'\]"),
    ("entry with unknown key", r"Edge 'LIKES' topic 'likes-comments': unknown key\(s\) \['partitions'\]"),
    ("string entry without a top-level pair", r"Edge 'LIKES' topic 'likes-posts-backfill' binds no label pair"),
    ("unused top-level pair", r"Edge 'LIKES': top-level .* is unused"),
    ("same pair, other keys", r"Edge 'LIKES': topics 'likes-posts' and 'likes-posts-backfill' bind the same label "
                              r"pair Person->Post with different key properties"),
    ("topic given a mapping", r"Edge 'LIKES': 'topic' takes one topic name"),
    ("topic_endpoints keys differ", r"Edge 'LIKES': topic_endpoints keys .* differ from topics"),
])
def test_invalid_multi_pair_schemas_fail_at_load_naming_type_and_topic(kind: str, match: str) -> None:
    with pytest.raises(ValidationError, match=match):
        GraphSchema.model_validate(_schema_with(_broken(kind)))


def test_an_optional_key_property_is_refused_by_the_node_first() -> None:
    data = likes_schema_dict()
    comment = data["nodes"][2]
    comment["properties"] = {"comment_id": {"type": "integer"}, "uid": {"type": "integer", "required": True}}
    comment["key_property"] = "comment_id"
    with pytest.raises(ValidationError):  # the node itself already refuses an optional key property
        GraphSchema.model_validate(data)


def test_single_pair_error_messages_are_unchanged() -> None:
    data = likes_schema_dict()
    data["edges"] = [{"type": "WROTE", "topic": "wrote", "nodes": {"source": "Person", "target": "Poster"},
                      "source_key_property": "person_id", "target_key_property": "post_id",
                      "properties": {"at": {"type": "date"}}}]
    with pytest.raises(ValidationError, match=r"Edge 'WROTE' target label 'Poster' does not name a declared node"):
        GraphSchema.model_validate(data)


# ── AC 5, 6 ──────────────────────────────────────────────────────────────────


def test_reversed_pairs_and_a_self_reference_beside_a_normal_pair_load() -> None:
    edge = likes_edge(topics=(POSTS,))
    edge["topics"].append({"name": "posts-like-persons", "nodes": {"source": "Post", "target": "Person"},
                           "source_key_property": "post_id", "target_key_property": "person_id"})
    loaded = GraphSchema.model_validate(_schema_with(edge)).edges[0]
    assert [pair.labels for pair in loaded.endpoint_pairs] == [("Person", "Post"), ("Post", "Person")]
    assert loaded.has_self_reference is False
    edge["topics"].append({"name": "persons-like-persons", "nodes": {"source": "Person", "target": "Person"},
                           "source_key_property": "person_id", "target_key_property": "person_id"})
    loaded = GraphSchema.model_validate(_schema_with(edge)).edges[0]
    assert loaded.has_self_reference is True
    assert [pair.is_self_referencing for pair in loaded.endpoint_pairs] == [False, False, True]


def test_multi_pair_parent_has_no_single_pair() -> None:
    edge = likes()
    assert edge.nodes is None and edge.source_key_property is None and edge.target_key_property is None
    assert edge.source_key_config is None and edge.target_key_config is None
    with pytest.raises(MultiPairEdgeError, match=r"Edge 'LIKES' binds 2 label pairs \(Person>Post, Person>Comment\)"):
        edge.single_pair
    with pytest.raises(MultiTopicEdgeError):
        edge.topic


# ── AC 7 ─────────────────────────────────────────────────────────────────────


def test_pair_and_topic_views_are_bound_single_pair_configs_with_the_parents_dlq_names() -> None:
    edge = likes(dead_letter={"enabled": True, "max_retry_attempts": 4})
    parent_names = (edge_dead_letter_topic_name(edge), edge_parked_topic_name(edge),
                    edge_parked_pair_index_topic_name(edge))
    assert parent_names == ("LIKES.dlq", "LIKES.dlq.parked", "LIKES.dlq.parked-pairs")
    posts, comments = edge.pair_view(0), edge.pair_view(edge.endpoint_pairs[1])
    for view, index, target, key, topics in ((posts, 0, "Post", "post_id", [POSTS, BACKFILL]),
                                             (comments, 1, "Comment", "comment_id", [COMMENTS])):
        assert view.is_multi_pair is False and view.topic_endpoints is None
        assert (view.nodes.source, view.nodes.target) == ("Person", target)
        assert view.target_key_property == key and view.source_key_property == "person_id"
        assert view.source_key_config.type == "integer" and view.target_key_config.type == "integer"
        assert view.pair_index == index and view.single_pair.index == index
        assert view.topics == topics
        assert (edge_dead_letter_topic_name(view), edge_parked_topic_name(view),
                edge_parked_pair_index_topic_name(view)) == parent_names
        assert view.dead_letter.max_retry_attempts == 4
    assert posts.topic_conflict is None and posts.conflict_rule == "order_by"
    assert edge.pair_view(0) is posts  # cached per parent
    backfill = edge.topic_view(BACKFILL)
    assert backfill.topics == [BACKFILL] and backfill.pair_index == 0
    assert edge.topic_view(COMMENTS) is comments  # the pair's only topic: the same view
    with pytest.raises(ValueError, match="are not distinct topics of label pair Person>Comment"):
        edge.pair_view(1, topics=[POSTS])
    with pytest.raises(ValueError, match="has no label pair 2"):
        edge.pair_view(2)


def test_a_view_of_a_contended_pair_keeps_the_rule() -> None:
    edge = likes(order_by=False, topic_conflict="arrival")
    assert edge.pair_view(0).topic_conflict == "arrival" and edge.pair_view(0).conflict_rule == "arrival"
    assert edge.pair_view(1).topic_conflict is None and edge.pair_view(1).conflict_rule == "single"


@pytest.mark.parametrize("path", ["examples/social/graph_schema.yaml", "tests/golden/fixtures/single_topic_dlq.yaml"])
def test_single_pair_topic_view_equals_the_s4_sub_edge(path: str) -> None:
    schema = load_schema(REPO / path)
    for edge in schema.edges:
        view = edge.topic_view(edge.topic)
        reference = _single_topic_sub_edge_s4(edge, edge.topic)
        assert view == reference
        assert view.model_dump() == reference.model_dump()
        assert (view.source_key_config, view.target_key_config) == (edge.source_key_config, edge.target_key_config)


def test_single_pair_multi_topic_topic_view_equals_the_s4_sub_edge() -> None:
    edge = likes(topics=(POSTS, BACKFILL), dead_letter={"enabled": True})
    for topic in edge.topics:
        assert edge.topic_view(topic) == _single_topic_sub_edge_s4(edge, topic)


# ── AC 8 ─────────────────────────────────────────────────────────────────────


def test_pickle_copy_and_dump_round_trip_keep_pairs_and_bindings() -> None:
    schema = likes_schema(dead_letter={"enabled": True})
    edge = schema.edges[0]
    restored = pickle.loads(pickle.dumps(edge))
    assert restored == edge and restored.endpoint_pairs == EXPECTED_PAIRS
    assert restored.pair_view(1).target_key_config == edge.pair_view(1).target_key_config
    copied = edge.model_copy()
    assert copied.endpoint_pairs == EXPECTED_PAIRS
    assert copied.pair_view(1) is not edge.pair_view(1) and copied.pair_view(1) == edge.pair_view(1)
    dumped = edge.model_dump(exclude_unset=True)
    assert set(dumped["topic_endpoints"]) == {POSTS, BACKFILL, COMMENTS}
    assert dumped["topics"] == [POSTS, BACKFILL, COMMENTS]
    assert "nodes" not in dumped
    again = GraphSchema.model_validate({**likes_schema_dict(), "edges": [dumped]})
    assert again.edges[0] == edge
    assert again.edges[0].pair_view(1).target_key_config.type == "integer"
    full = GraphSchema.model_validate({**likes_schema_dict(), "edges": [edge.model_dump()]})
    assert full.edges[0].endpoint_pairs == EXPECTED_PAIRS
    assert pickle.loads(pickle.dumps(edge.pair_view(1))) == edge.pair_view(1)


# ── AC 9: Q1, the conflict rule per label pair ───────────────────────────────


def test_pairs_with_one_topic_each_need_no_rule() -> None:
    edge = likes(order_by=False, topics=(POSTS, COMMENTS))
    assert edge.is_multi_topic and edge.conflict_rule == "single" and edge.contended_pairs == ()


@pytest.mark.parametrize("rule", ["precedence", "arrival"])
def test_a_rule_on_a_type_without_a_contended_pair_has_no_effect(rule: str, caplog, monkeypatch) -> None:
    import src.models.schema as schema_module

    monkeypatch.setattr(schema_module, "_WARNED_ONCE", set())
    caplog.set_level(logging.WARNING)
    schema = likes_schema(order_by=False, topic_conflict=rule, topics=(POSTS, COMMENTS),
                          loading={"mode": "stream"})
    edge = schema.edges[0]
    assert edge.conflict_rule == "single" and edge.topic_conflict == rule
    assert any(f"topic_conflict={rule} has no effect" in r.getMessage() for r in caplog.records)
    assert precedence_edge_types(schema) == [] and arrival_conflict_warnings(schema) == []
    require_no_precedence_in_stream(schema, "stream")
    # Final follow-ups nit: once per process, not on every load, copy or view of the schema.
    caplog.clear()
    for _ in range(3):
        likes_schema(order_by=False, topic_conflict=rule, topics=(POSTS, COMMENTS), loading={"mode": "stream"})
    edge.topic_view(POSTS)
    assert not any("has no effect" in r.getMessage() for r in caplog.records)


def test_a_contended_pair_without_a_rule_fails_naming_the_pair() -> None:
    with pytest.raises(ValidationError, match=r"must declare how records for the same pair are reconciled.*"
                                              r"label pair\(s\) fed by several topics: Person->Post "
                                              r"\['likes-posts', 'likes-posts-backfill'\]"):
        likes_schema(order_by=False)


def test_precedence_on_a_contended_multi_pair_type_is_still_refused_in_stream() -> None:
    with pytest.raises(ValidationError, match="not supported in stream mode"):
        likes_schema(order_by=False, topic_conflict="precedence", loading={"mode": "stream"})
    schema = likes_schema(order_by=False, topic_conflict="precedence")
    assert precedence_edge_types(schema) == ["LIKES"]
    with pytest.raises(ValueError, match="not supported in stream mode"):
        require_no_precedence_in_stream(schema, "stream")


def test_arrival_warning_names_the_pair_groups() -> None:
    schema = likes_schema(order_by=False, topic_conflict="arrival")
    [message] = arrival_conflict_warnings(schema)
    assert "['likes-posts', 'likes-posts-backfill'] (label pair Person->Post)" in message
    assert COMMENTS not in message


# ── review round 1 (M1, S1, S2, N2) ──────────────────────────────────────────


def test_views_of_a_later_pair_can_be_split_further() -> None:
    edge = likes()
    comments = edge.pair_view(1)
    assert comments.topic_view(COMMENTS) == edge.topic_view(COMMENTS)
    assert comments.topic_view(COMMENTS).pair_index == 1
    again = comments.pair_view(comments.single_pair)
    assert again.pair_index == 1 and again.nodes.target == "Comment"
    with pytest.raises(ValueError, match="has no label pair 0"):
        comments.pair_view(0)


def test_a_topic_subset_keeps_the_parents_order() -> None:
    edge = likes(order_by=False, topic_conflict="precedence")
    view = edge.pair_view(0, topics=[BACKFILL, POSTS])
    assert view.topics == [POSTS, BACKFILL]
    assert view is edge.pair_view(0)
    assert view.precedence_rank(BACKFILL) == edge.precedence_rank(BACKFILL)


def test_concurrent_view_builds_return_one_object() -> None:
    import threading

    edge = likes()
    barrier = threading.Barrier(8)
    seen: list = []

    def build() -> None:
        barrier.wait()
        seen.append(edge.topic_view(BACKFILL))

    threads = [threading.Thread(target=build) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len({id(view) for view in seen}) == 1 and seen[0] is edge.topic_view(BACKFILL)


def test_a_non_string_topic_keeps_the_string_message() -> None:
    edge = {**likes_edge(), "topic": 123}
    del edge["topics"]
    with pytest.raises(ValidationError, match="must be a non-blank string"):
        GraphSchema.model_validate(_schema_with(edge))
