"""v7-S1: multi-topic relationship types, conflict rule, DLQ naming, N1/N2 strictness."""
from __future__ import annotations

import itertools
import logging
import pickle
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from src.models.schema import (
    BucketLeaseStreamConfig,
    CoordinationConfig,
    EdgeConfig,
    EdgeExecutionConfig,
    GraphSchema,
    LoadingConfig,
    MixAndBatchConfig,
    MultiTopicEdgeError,
    NodeConfig,
    RetryConfig,
    RetryConsumerConfig,
    arrival_conflict_warnings,
    edge_dead_letter_topic_name,
    edge_parked_pair_index_topic_name,
    edge_parked_topic_name,
    precedence_edge_types,
    require_no_precedence_in_stream,
    schema_data_topics,
)
from src.utils.schema_loader import load_schema

REPO = Path(__file__).resolve().parent.parent


def _edge(**overrides) -> dict:
    data = {
        "type": "KNOWS",
        "topic": "knows-events",
        "nodes": {"source": "Person", "target": "Person"},
        "source_key_property": "personId",
        "target_key_property": "personId",
        "properties": {"since": {"type": "date", "required": True}, "note": {"type": "string"}},
    }
    data.update(overrides)
    return data


def _swap_topics(**overrides) -> dict:
    data = _edge(**overrides)
    if "topics" in data:
        del data["topic"]
    return data


def _schema(edge: dict, **loading) -> dict:
    data = {
        "nodes": [
            {
                "label": "Person",
                "topic": "person-events",
                "key_property": "personId",
                "properties": {"personId": {"type": "string", "required": True}},
            }
        ],
        "edges": [edge],
    }
    if loading:
        data["loading"] = loading
    return data


# ── Story 1.1 ────────────────────────────────────────────────────────────────


def test_topic_shorthand_equals_topics_list() -> None:
    a = EdgeConfig(**_edge(topic="a"))
    b = EdgeConfig(**_swap_topics(topics=["a"]))
    assert a == b
    assert a.topic == "a" and a.topics == ["a"]
    assert a.conflict_rule == "single"


def test_multi_topic_property_raises_and_order_is_preserved() -> None:
    edge = EdgeConfig(**_swap_topics(topics=["b", "a"], topic_conflict="arrival"))
    assert edge.topics == ["b", "a"]
    with pytest.raises(MultiTopicEdgeError, match="KNOWS"):
        edge.topic
    assert isinstance(MultiTopicEdgeError("x"), ValueError)


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"topics": ["a"]}, "either 'topic' or 'topics'"),  # both keys
        ({"topics": []}, "must not be empty"),
        ({"topics": ["a", "a"], "topic_conflict": "arrival"}, "duplicate topics"),
        ({"topics": ["a", " "], "topic_conflict": "arrival"}, "must not be blank"),
        ({"topics": ["a", 3], "topic_conflict": "arrival"}, "non-blank string"),
        ({"topics": "a"}, "must be a list"),
    ],
)
def test_invalid_topics_name_the_type(overrides: dict, match: str) -> None:
    data = _edge(**overrides)
    if "topics" in overrides and overrides["topics"] == ["a"]:
        pass  # keeps both 'topic' and 'topics'
    elif "topics" in overrides:
        del data["topic"]
    with pytest.raises(ValidationError, match=match) as info:
        EdgeConfig(**data)
    assert "KNOWS" in str(info.value)


def test_neither_topic_key_is_rejected() -> None:
    data = _edge()
    del data["topic"]
    with pytest.raises(ValidationError, match="'topic' .* is required"):
        EdgeConfig(**data)


def test_topics_are_stripped() -> None:
    assert EdgeConfig(**_edge(topic=" a ")).topics == ["a"]


def test_pickle_copy_and_dump_round_trip() -> None:
    edge = EdgeConfig(**_swap_topics(topics=["a", "b"], order_by="since"))
    assert pickle.loads(pickle.dumps(edge)) == edge
    assert edge.model_copy(deep=True) == edge
    dumped = edge.model_dump()
    assert "topic" not in dumped and dumped["topics"] == ["a", "b"]
    assert EdgeConfig.model_validate(dumped) == edge
    single = EdgeConfig(**_edge())
    assert "topic" not in single.model_dump()
    assert EdgeConfig.model_validate(single.model_dump()) == single


# ── Story 1.2 ────────────────────────────────────────────────────────────────


def _two_edge_schema(topics_a, topics_b) -> dict:
    data = _schema(_swap_topics(type="A", topics=topics_a, order_by="since"))
    data["edges"].append(_swap_topics(type="B", topics=topics_b, order_by="since"))
    return data


def test_topic_shared_between_types_fails() -> None:
    with pytest.raises(ValidationError, match="Topic 'shared' is declared by both edge 'A' and edge 'B'"):
        GraphSchema.model_validate(_two_edge_schema(["x", "shared"], ["shared", "y"]))


def test_edge_topic_equal_to_node_topic_fails() -> None:
    with pytest.raises(ValidationError, match="Topic 'person-events' is declared by both node 'Person'"):
        GraphSchema.model_validate(_two_edge_schema(["x", "person-events"], ["y"]))


def test_disjoint_multi_topic_types_load() -> None:
    schema = GraphSchema.model_validate(_two_edge_schema(["a1", "a2"], ["b1"]))
    assert [e.topics for e in schema.edges] == [["a1", "a2"], ["b1"]]


def test_duplicate_types_still_rejected_with_unchanged_message() -> None:
    data = _two_edge_schema(["a1", "a2"], ["b1"])
    data["edges"][1]["type"] = "A"
    with pytest.raises(ValidationError, match="Duplicate edge types detected in schema."):
        GraphSchema.model_validate(data)


def test_dlq_default_of_multi_topic_type_collides_with_data_topic() -> None:
    data = _two_edge_schema(["a1", "KNOWS.dlq"], ["b1"])
    data["edges"][0]["type"] = "KNOWS"
    with pytest.raises(ValidationError, match="collides with its own source topic"):
        GraphSchema.model_validate({**data, "edges": [{**data["edges"][0], "dead_letter": {}}, data["edges"][1]]})


# ── Story 1.3 conflict rule ──────────────────────────────────────────────────


def _edge_for(n_topics: int, order_by: bool, conflict: str | None) -> dict:
    data = _swap_topics(topics=[f"t{i}" for i in range(n_topics)])
    if order_by:
        data["order_by"] = "since"
    if conflict is not None:
        data["topic_conflict"] = conflict
    return data


def _expected(n: int, order_by: bool, conflict: str | None) -> str:
    """Return the expected conflict_rule, or 'error'."""
    if conflict == "order_by" and not order_by:
        return "error"
    if order_by and conflict in ("precedence", "arrival"):
        return "error"
    if n == 1:
        return "order_by" if order_by else "single"
    if order_by:
        return "order_by" if conflict in (None, "order_by") else "error"
    return "error" if conflict in (None, "order_by") else conflict


@pytest.mark.parametrize(
    ("n", "order_by", "conflict"),
    list(itertools.product([1, 2, 3], [False, True], [None, "order_by", "precedence", "arrival"])),
)
@pytest.mark.parametrize("mode", [None, "bulk", "stream"])
def test_conflict_rule_matrix(n: int, order_by: bool, conflict: str | None, mode: str | None) -> None:
    expected = _expected(n, order_by, conflict)
    loading = {} if mode is None else {"mode": mode}
    data = _schema(_edge_for(n, order_by, conflict), **loading)
    if expected == "error" or (expected == "precedence" and mode == "stream"):
        with pytest.raises(ValidationError):
            GraphSchema.model_validate(data)
        return
    schema = GraphSchema.model_validate(data)
    assert schema.edges[0].conflict_rule == expected


def test_missing_rule_error_lists_the_three_options() -> None:
    with pytest.raises(ValidationError) as info:
        EdgeConfig(**_edge_for(2, False, None))
    text = str(info.value)
    assert "'KNOWS'" in text and "order_by" in text and "precedence" in text and "arrival" in text


def test_order_by_with_other_rule_says_pick_one() -> None:
    with pytest.raises(ValidationError, match="mutually exclusive; pick one"):
        EdgeConfig(**_edge_for(2, True, "arrival"))


def test_topic_conflict_order_by_needs_order_by() -> None:
    with pytest.raises(ValidationError, match="requires 'order_by"):
        EdgeConfig(**_edge_for(2, False, "order_by"))


def test_precedence_in_explicit_stream_mode_rejected_but_default_mode_deferred() -> None:
    edge = _edge_for(2, False, "precedence")
    with pytest.raises(ValidationError, match="not supported in stream mode"):
        GraphSchema.model_validate(_schema(edge, mode="stream"))
    # loading.mode absent: the run mode may come from --mode, so load succeeds ...
    schema = GraphSchema.model_validate(_schema(edge))
    assert precedence_edge_types(schema) == ["KNOWS"]
    # ... and the start-time helper rejects stream and accepts bulk.
    with pytest.raises(ValueError, match=r"\['KNOWS'\]"):
        require_no_precedence_in_stream(schema, "stream")
    require_no_precedence_in_stream(schema, "bulk")


def test_single_topic_with_precedence_is_ignored() -> None:
    schema = GraphSchema.model_validate(_schema(_edge(topic_conflict="precedence"), mode="stream"))
    assert schema.edges[0].conflict_rule == "single"
    assert precedence_edge_types(schema) == []


def test_arrival_warns_once_per_type_and_order_by_does_not(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="src.models.schema"):
        schema = GraphSchema.model_validate(_schema(_edge_for(2, False, "arrival")))
    warnings = [r.getMessage() for r in caplog.records if "arrival" in r.getMessage()]
    assert len(warnings) == 1 and "KNOWS" in warnings[0] and "t0" in warnings[0]
    assert arrival_conflict_warnings(schema) == warnings
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="src.models.schema"):
        ok = GraphSchema.model_validate(_schema(_edge_for(2, True, None)))
    assert not caplog.records
    assert arrival_conflict_warnings(ok) == []


def test_order_by_string_property_warns_lexicographic(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="src.models.schema"):
        EdgeConfig(**_edge(order_by="note"))
    assert any("lexicographically" in r.getMessage() and "ISO-8601" in r.getMessage() for r in caplog.records)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="src.models.schema"):
        EdgeConfig(**_edge(order_by="since"))
    assert not caplog.records


# ── DLQ naming ───────────────────────────────────────────────────────────────


def test_single_topic_dlq_names_unchanged() -> None:
    edge = EdgeConfig(**_edge(dead_letter={}))
    assert edge_dead_letter_topic_name(edge) == "knows-events.dlq"
    assert edge_parked_topic_name(edge) == "knows-events.dlq.parked"
    assert edge_parked_pair_index_topic_name(edge) == "knows-events.dlq.parked-pairs"


def test_multi_topic_dlq_names_default_to_type_and_can_be_overridden() -> None:
    edge = EdgeConfig(**{**_edge_for(2, True, None), "dead_letter": {}})
    assert edge_dead_letter_topic_name(edge) == "KNOWS.dlq"
    assert edge_parked_topic_name(edge) == "KNOWS.dlq.parked"
    assert edge_parked_pair_index_topic_name(edge) == "KNOWS.dlq.parked-pairs"
    old = EdgeConfig(**{**_edge_for(2, True, None), "dead_letter": {"topic": "t0.dlq"}})
    assert edge_dead_letter_topic_name(old) == "t0.dlq"
    assert edge_parked_topic_name(old) == "t0.dlq.parked"
    assert edge_parked_pair_index_topic_name(old) == "t0.dlq.parked-pairs"


def test_dlq_disabled_returns_none() -> None:
    edge = EdgeConfig(**_edge())
    assert edge_dead_letter_topic_name(edge) is edge_parked_topic_name(edge) is None
    assert edge_parked_pair_index_topic_name(edge) is None


# ── N1: node dead_letter rejects edge-only fields ────────────────────────────


def _node(dead_letter: dict) -> dict:
    return {
        "label": "Person",
        "topic": "person-events",
        "key_property": "pid",
        "properties": {"pid": {"type": "string", "required": True}},
        "dead_letter": dead_letter,
    }


@pytest.mark.parametrize(
    "field_value",
    [
        {"max_retry_attempts": 7},
        {"parked_topic": "p.parked"},
        {"route_missing_endpoints_in_bulk": True},
    ],
)
def test_node_rejects_edge_only_dead_letter_fields(field_value: dict) -> None:
    with pytest.raises(ValidationError, match="relationship types only") as info:
        NodeConfig(**_node(field_value))
    assert "Person" in str(info.value) and next(iter(field_value)) in str(info.value)


def test_node_accepts_default_valued_edge_only_fields_from_a_dump() -> None:
    node = NodeConfig(**_node({"topic": "x.dlq"}))
    assert NodeConfig.model_validate(node.model_dump()) == node


def test_node_accepts_topic_and_enabled_and_edge_keeps_fields() -> None:
    node = NodeConfig(**_node({"topic": "x.dlq", "enabled": True}))
    assert node.dead_letter.topic == "x.dlq"
    edge = EdgeConfig(**_edge(dead_letter={"max_retry_attempts": 7, "parked_topic": "p"}))
    assert edge.dead_letter.max_retry_attempts == 7


# ── N2: typo-rejecting loading config ────────────────────────────────────────


@pytest.mark.parametrize(
    ("model", "field"),
    [
        (RetryConsumerConfig, "poll_timeout_ms"),
        (BucketLeaseStreamConfig, "tx_timeout_ms"),
        (CoordinationConfig, "bucket_count"),
    ],
)
def test_frozen_loading_configs_reject_unknown_keys_and_stay_frozen(model, field) -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        model(not_a_real_key=1)
    with pytest.raises(ValidationError, match="frozen"):
        setattr(model(), field, getattr(model(), field))


@pytest.mark.parametrize("model", [LoadingConfig, MixAndBatchConfig, RetryConfig, EdgeExecutionConfig])
def test_other_loading_models_reject_unknown_keys(model) -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        model(not_a_real_key=1)


def test_typo_in_yaml_loading_section_fails_load() -> None:
    data = _schema(_edge())
    data["loading"] = {"retry_consumer": {"poll_timeout": 100}}
    with pytest.raises(ValidationError, match="poll_timeout"):
        GraphSchema.model_validate(data)
    data["loading"] = {"bucket_lease": {"tx_timeout": 100}}
    with pytest.raises(ValidationError, match="tx_timeout"):
        GraphSchema.model_validate(data)
    with pytest.raises(ValidationError, match="Extra inputs"):
        LoadingConfig(bogus=1)


# ── Story 1.4: backward compatibility ────────────────────────────────────────


@pytest.mark.parametrize(
    "path", ["config/graph_schema.yaml", "examples/imdb/graph_schema.yaml", "examples/social/graph_schema.yaml"]
)
def test_shipped_schemas_load_with_unchanged_topics(path: str) -> None:
    raw = yaml.safe_load((REPO / path).read_text())
    schema = load_schema(REPO / path)
    for raw_edge, edge in zip(raw["edges"], schema.edges):
        assert edge.topic == raw_edge["topic"]
        assert edge.topics == [raw_edge["topic"]]
        assert edge.conflict_rule == ("order_by" if raw_edge.get("order_by") else "single")
        assert "topic" not in edge.model_dump()


@pytest.mark.parametrize(
    "path", ["config/graph_schema.yaml", "examples/imdb/graph_schema.yaml", "examples/social/graph_schema.yaml"]
)
def test_shipped_schemas_round_trip_through_dump(path: str) -> None:
    schema = load_schema(REPO / path)
    assert GraphSchema.model_validate(schema.model_dump()) == schema
    assert GraphSchema.model_validate(schema.model_dump(exclude_unset=True)) == schema


def test_exclude_unset_is_the_serialization_contract_for_precedence() -> None:
    schema = GraphSchema.model_validate(_schema(_edge_for(2, False, "precedence")))
    assert GraphSchema.model_validate(schema.model_dump(exclude_unset=True)) == schema
    # a full dump makes loading.mode: stream explicit, which precedence refuses
    with pytest.raises(ValidationError, match="stream mode"):
        GraphSchema.model_validate(schema.model_dump())


def test_single_topic_rule_combination_errors_apply_regardless_of_topic_count() -> None:
    with pytest.raises(ValidationError, match="pick one"):
        EdgeConfig(**_edge(order_by="since", topic_conflict="arrival"))
    with pytest.raises(ValidationError, match="requires 'order_by"):
        EdgeConfig(**_edge(topic_conflict="order_by"))


def test_precedence_rank_edge_for_topic_and_data_topics() -> None:
    edge = EdgeConfig(**_edge_for(3, False, "precedence"))
    assert [edge.precedence_rank(t) for t in ("t0", "t1", "t2")] == [0, 1, 2]
    with pytest.raises(ValueError, match="does not read topic 'zz'"):
        edge.precedence_rank("zz")
    schema = GraphSchema.model_validate(_two_edge_schema(["a1", "a2"], ["b1"]))
    assert schema.edge_for_topic("a2").type == "A"
    assert schema.edge_for_topic("b1").type == "B"
    assert schema.edge_for_topic("person-events") is None
    assert schema_data_topics(schema) == ["person-events", "a1", "a2", "b1"]


# ── DLQ collision coverage for multi-topic types ─────────────────────────────


def _multi(etype: str, topics: list[str], **dead_letter) -> dict:
    data = _swap_topics(type=etype, topics=topics, order_by="since")
    data["dead_letter"] = dead_letter
    return data


def _with_edges(*edges: dict) -> dict:
    data = _schema(edges[0])
    data["edges"].extend(edges[1:])
    return data


def test_multi_topic_default_dlq_collides_with_other_types_single_topic_dlq() -> None:
    # type "A" defaults to A.dlq; type B's only topic is "A" so its DLQ is also A.dlq
    data = _with_edges(_multi("A", ["a1", "a2"]), _multi("B", ["A"]))
    with pytest.raises(ValidationError, match="'A.dlq'"):
        GraphSchema.model_validate(data)


def test_multi_topic_parked_and_index_collisions() -> None:
    data = _with_edges(_multi("A", ["a1", "a2"]), _multi("B", ["b1", "b2"], parked_topic="A.dlq.parked"))
    with pytest.raises(ValidationError, match="Parking-lot topic 'A.dlq.parked'"):
        GraphSchema.model_validate(data)
    # B's explicit DLQ "A.dlq.parked" makes its index A.dlq.parked.parked-pairs; choose B.topic so
    # B's DLQ equals A's parked-pairs topic
    data = _with_edges(_multi("A", ["a1", "a2"]), _multi("B", ["b1", "b2"], topic="A.dlq.parked-pairs"))
    with pytest.raises(ValidationError, match="A.dlq.parked-pairs"):
        GraphSchema.model_validate(data)


def test_explicit_dead_letter_topic_override_collision() -> None:
    data = _with_edges(_multi("A", ["a1", "a2"], topic="shared.dlq"), _multi("B", ["b1", "b2"], topic="shared.dlq"))
    with pytest.raises(ValidationError, match="'shared.dlq' is declared by both"):
        GraphSchema.model_validate(data)


def test_node_topic_equal_to_multi_topic_type_dlq_is_rejected() -> None:
    data = _with_edges(_multi("A", ["a1", "a2"]))
    data["nodes"][0]["topic"] = "A.dlq"
    with pytest.raises(ValidationError, match="collides with node 'Person'"):
        GraphSchema.model_validate(data)
