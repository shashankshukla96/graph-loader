"""The social example's generator and exact graph comparison (no Kafka or Neo4j)."""
from __future__ import annotations

import copy
from datetime import date, datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from examples.social import social_graph  # noqa: E402
from src.loader.edge_records import normalize_edge_record  # noqa: E402
from src.utils.schema_loader import load_schema  # noqa: E402


def test_generator_is_deterministic_and_exercises_the_contract() -> None:
    first, second = social_graph.generate(300, seed=5), social_graph.generate(300, seed=5)
    assert first.edges == second.edges and first.nodes == second.nodes
    evidence = social_graph.last_write_evidence(first)
    assert evidence["WORKS_AT"]["records"] > evidence["WORKS_AT"]["pairs"]
    assert sum(item["pairs_overwritten"] for item in evidence.values()) > 0
    assert any(source == target for source, target, _ in first.edges["FRIEND_OF"])
    assert any(isinstance(target, date) for _, target, _ in first.edges["ACTIVE_ON"])


def test_wire_records_pass_the_library_normalization() -> None:
    schema = load_schema(social_graph.SCHEMA_PATH)
    edges = {edge.type: edge for edge in schema.edges}
    dataset = social_graph.generate(100, seed=3)
    for edge_type, rows in dataset.edges.items():
        for source, target, properties in rows[:50]:
            record = normalize_edge_record(social_graph.edge_record(edge_type, source, target, properties),
                                           edges[edge_type])
            assert (record.source_key, record.target_key) == (source, target)
            assert record.properties == properties


def test_compare_reports_missing_wrong_and_duplicate_entities() -> None:
    dataset = social_graph.generate(200, seed=9)
    nodes, rels = social_graph.expected_graph(dataset)
    assert social_graph.compare((nodes, rels), (nodes, rels, {})) == []
    wrong = copy.deepcopy(rels)
    pair = next(iter(wrong["BOUGHT"]))
    wrong["BOUGHT"][pair] = {**wrong["BOUGHT"][pair], "quantity": -1}
    assert "wrong properties 1" in social_graph.compare((nodes, rels), (nodes, wrong, {}))[0]
    missing = copy.deepcopy(rels)
    missing["LIVES_IN"].pop(next(iter(missing["LIVES_IN"])))
    assert "missing 1" in social_graph.compare((nodes, rels), (nodes, missing, {}))[0]
    assert social_graph.compare((nodes, rels), (nodes, rels, {"FRIEND_OF": 2})) == ["FRIEND_OF: 2 duplicate entities"]
    # Datetimes compare by instant; -0.0 equals 0.0 (as Neo4j returns them).
    at = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
    assert social_graph._canonical(at) == social_graph._canonical(at.astimezone())
    assert social_graph._canonical(-0.0) == 0.0
