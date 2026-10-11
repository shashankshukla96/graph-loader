"""Unit tests (no Docker) for the star-topology dataset generator (Phase 6 Slice 4, Story 1)."""
from __future__ import annotations

import sys
from unittest.mock import patch

import pytest

from tests.stress import star_topology_data as std


def test_target_hub_small_scale_produces_exact_records_and_one_hub() -> None:
    dataset = std.generate(1000, "target_hub", repeat_fraction=0.0)
    assert len(dataset.edges["POINTS_AT"]) == 1000
    assert list(dataset.edges.keys()) == ["POINTS_AT"]
    assert len(dataset.nodes["Hub"]) == 1
    assert dataset.nodes["Hub"][0]["hub_id"] == std.HUB_KEY
    assert len(dataset.nodes["Leaf"]) == 1000


def test_source_hub_small_scale_all_records_share_one_source() -> None:
    dataset = std.generate(1000, "source_hub", repeat_fraction=0.0)
    rows = dataset.edges["POINTS_FROM"]
    assert len(rows) == 1000
    assert {source for source, _target, _props in rows} == {std.HUB_KEY}


def test_multi_type_hub_small_scale_sums_and_every_type_nonempty() -> None:
    dataset = std.generate(1000, "multi_type_hub", repeat_fraction=0.0)
    assert set(dataset.edges) == set(std.MULTI_TYPES)
    assert sum(len(rows) for rows in dataset.edges.values()) == 1000
    for edge_type in std.MULTI_TYPES:
        assert len(dataset.edges[edge_type]) >= 1
    # every leaf id used is unique across types (no cross-type collision)
    seen: set[int] = set()
    for rows in dataset.edges.values():
        for source, _target, _props in rows:
            assert source not in seen
            seen.add(source)


@pytest.mark.parametrize("topology,edge_type", [("target_hub", "POINTS_AT"), ("source_hub", "POINTS_FROM")])
def test_last_write_wins_collapse_matches_injected_repeat_count(topology: str, edge_type: str) -> None:
    dataset = std.generate(2000, topology, repeat_fraction=0.01)
    rows = dataset.edges[edge_type]
    pairs = std.distinct_pairs(dataset, edge_type)
    expected_repeats = round(2000 * 0.01)
    assert expected_repeats > 0
    assert len(rows) - len(pairs) == expected_repeats
    nodes, rels = std.expected_graph(dataset)
    assert len(rels[edge_type]) == len(pairs)
    assert len(rels[edge_type]) < len(rows)


def test_expected_graph_last_write_wins_keeps_highest_seq() -> None:
    dataset = std.generate(500, "target_hub", repeat_fraction=0.02)
    _, rels = std.expected_graph(dataset)
    evidence = std.last_write_evidence(dataset)["POINTS_AT"]
    assert evidence["pairs_overwritten"] > 0
    assert evidence["pairs"] == len(rels["POINTS_AT"])
    # every repeated pair's expected seq is the *later* record's seq, not the first
    by_pair: dict[tuple, list[int]] = {}
    for source, target, props in dataset.edges["POINTS_AT"]:
        by_pair.setdefault((source, target), []).append(props["seq"])
    overwritten = [pair for pair, seqs in by_pair.items() if len(seqs) > 1]
    assert overwritten
    for pair in overwritten:
        assert rels["POINTS_AT"][pair]["seq"] == max(by_pair[pair])


def test_unknown_topology_raises() -> None:
    with pytest.raises(std.StarTopologyError):
        std.generate(10, "not_a_topology")


def test_nonpositive_hub_edges_raises() -> None:
    with pytest.raises(std.StarTopologyError):
        std.generate(0, "target_hub")


def test_schema_dict_is_load_schema_compatible(tmp_path) -> None:
    import yaml

    from src.utils.schema_loader import load_schema

    for topology in ("target_hub", "source_hub", "multi_type_hub"):
        path = tmp_path / f"{topology}.yaml"
        path.write_text(yaml.safe_dump(std.schema_dict(topology, "runA")), encoding="utf-8")
        schema = load_schema(path)
        labels = {node.label for node in schema.nodes}
        assert labels == {std.HUB_LABEL, std.LEAF_LABEL}
        types = {edge.type for edge in schema.edges}
        expected_types = {
            "target_hub": {"POINTS_AT"}, "source_hub": {"POINTS_FROM"}, "multi_type_hub": set(std.MULTI_TYPES),
        }[topology]
        assert types == expected_types


def test_schema_dict_run_id_isolates_topics_and_group() -> None:
    first = std.schema_dict("target_hub", "runA")
    second = std.schema_dict("target_hub", "runB")
    first_topics = {node["topic"] for node in first["nodes"]} | {edge["topic"] for edge in first["edges"]}
    second_topics = {node["topic"] for node in second["nodes"]} | {edge["topic"] for edge in second["edges"]}
    assert first_topics.isdisjoint(second_topics)
    assert first["loading"]["consumer_group_id"] != second["loading"]["consumer_group_id"]


def test_schema_dict_rejects_blank_run_id() -> None:
    with pytest.raises(std.StarTopologyError):
        std.schema_dict("target_hub", "")


def test_multi_type_hub_below_minimum_scale_raises() -> None:
    with pytest.raises(std.StarTopologyError):
        std.generate(4, "multi_type_hub")


def test_repeat_fraction_out_of_range_raises() -> None:
    with pytest.raises(std.StarTopologyError):
        std.generate(100, "target_hub", repeat_fraction=1.5)


def test_kafka_key_uses_the_leaf_endpoint_for_every_topology(tmp_path) -> None:
    import yaml

    from src.utils.schema_loader import load_schema

    for topology, edge_type in (("target_hub", "POINTS_AT"), ("source_hub", "POINTS_FROM")):
        path = tmp_path / f"{topology}.yaml"
        path.write_text(yaml.safe_dump(std.schema_dict(topology, "runK")), encoding="utf-8")
        schema = load_schema(path)
        config = next(edge for edge in schema.edges if edge.type == edge_type)
        dataset = std.generate(50, topology, repeat_fraction=0.0)
        keys = {std._kafka_key(config, source, target) for source, target, _ in dataset.edges[edge_type]}
        # A hub-as-source topology (source_hub) must not collapse every
        # record onto the hub's single key ("H") -- that would spread zero
        # records across Kafka partitions and defeat any concurrency test.
        assert len(keys) > 1
        assert std.HUB_KEY not in keys


def test_module_imports_without_confluent_kafka_or_neo4j() -> None:
    """`import tests.stress.star_topology_data` must not import Kafka/Neo4j at module scope."""
    import importlib

    module_name = "tests.stress.star_topology_data"
    saved = sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"confluent_kafka": None, "neo4j": None}):
            reloaded = importlib.import_module(module_name)
            assert reloaded is not None
    finally:
        sys.modules.pop(module_name, None)
        if saved is not None:
            sys.modules[module_name] = saved
        importlib.import_module(module_name)


def test_publish_imports_confluent_kafka_lazily_and_fails_without_it() -> None:
    """`publish` itself needs confluent_kafka, but only when called, not at import time."""
    with patch.dict(sys.modules, {"confluent_kafka": None}):
        with pytest.raises(ImportError):
            std.publish(std.generate(10, "target_hub"), "localhost:9092", schema=object())


# -- compare()/read_graph() fault injection (Story 3 review requirement) ---------
#
# compare() is the exact-match proof every planned/classic/lease-mode test in
# this slice trusts. If it always returned [], every "matches the expected
# graph exactly" assertion downstream would be vacuous -- each fault below
# must produce a nonempty problem list.


def _rels_copy(rels: dict) -> dict:
    return {edge_type: dict(pairs) for edge_type, pairs in rels.items()}


def _expected_for_compare_tests():
    dataset = std.generate(50, "target_hub", repeat_fraction=0.0)
    return std.expected_graph(dataset)


def test_compare_is_empty_for_two_identical_graphs() -> None:
    nodes, rels = _expected_for_compare_tests()
    assert std.compare((nodes, rels), (nodes, _rels_copy(rels), {})) == []


def test_compare_detects_a_missing_relationship() -> None:
    nodes, rels = _expected_for_compare_tests()
    got_rels = _rels_copy(rels)
    pair = next(iter(got_rels["POINTS_AT"]))
    del got_rels["POINTS_AT"][pair]
    assert std.compare((nodes, rels), (nodes, got_rels, {})) != []


def test_compare_detects_an_extra_relationship() -> None:
    nodes, rels = _expected_for_compare_tests()
    got_rels = _rels_copy(rels)
    got_rels["POINTS_AT"][("not-a-real-leaf", std.HUB_KEY)] = {"seq": 0}
    assert std.compare((nodes, rels), (nodes, got_rels, {})) != []


def test_compare_detects_a_wrong_property_value() -> None:
    nodes, rels = _expected_for_compare_tests()
    got_rels = _rels_copy(rels)
    pair = next(iter(got_rels["POINTS_AT"]))
    got_rels["POINTS_AT"][pair] = {**got_rels["POINTS_AT"][pair], "seq": got_rels["POINTS_AT"][pair]["seq"] + 999}
    assert std.compare((nodes, rels), (nodes, got_rels, {})) != []


def test_compare_detects_a_reported_duplicate() -> None:
    nodes, rels = _expected_for_compare_tests()
    assert std.compare((nodes, rels), (nodes, _rels_copy(rels), {"POINTS_AT": 1})) != []


def test_compare_detects_a_missing_node() -> None:
    nodes, rels = _expected_for_compare_tests()
    got_nodes = {label: dict(items) for label, items in nodes.items()}
    key = next(iter(got_nodes["Leaf"]))
    del got_nodes["Leaf"][key]
    assert std.compare((nodes, rels), (got_nodes, rels, {})) != []
