"""Tests for deterministic relationship conflict planning."""
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from src.orchestrator.dependency_manager import (
    build_conflict_families,
    build_relationship_conflict_plan,
)


def _edge(edge_type: str, source: str, target: str):
    return SimpleNamespace(
        type=edge_type,
        nodes=SimpleNamespace(source=source, target=target),
    )


def _stage_types(plan):
    return tuple(stage.edge_types for stage in plan.stages)


def test_empty_input_has_no_vertices_or_stages():
    plan = build_relationship_conflict_plan([])

    assert dict(plan.conflicts) == {}
    assert plan.stages == ()


def test_independent_relationships_share_a_stage():
    plan = build_relationship_conflict_plan([
        _edge("VISITED", "Person", "Place"),
        _edge("SUPPLIES", "Vendor", "Product"),
    ])

    assert dict(plan.conflicts) == {"SUPPLIES": frozenset(), "VISITED": frozenset()}
    assert _stage_types(plan) == (("SUPPLIES", "VISITED"),)


def test_shared_endpoint_label_creates_conflict_and_serial_stages():
    plan = build_relationship_conflict_plan([
        _edge("WORKS_AT", "Person", "Company"),
        _edge("BOUGHT", "Person", "Product"),
    ])

    assert plan.conflicts["BOUGHT"] == frozenset({"WORKS_AT"})
    assert plan.conflicts["WORKS_AT"] == frozenset({"BOUGHT"})
    assert _stage_types(plan) == (("BOUGHT",), ("WORKS_AT",))


def test_transitive_chain_has_only_pairwise_safe_stages():
    edges = [
        _edge("AB", "A", "B"),
        _edge("BC", "B", "C"),
        _edge("CD", "C", "D"),
    ]
    plan = build_relationship_conflict_plan(edges)
    labels = {edge.type: {edge.nodes.source, edge.nodes.target} for edge in edges}

    assert _stage_types(plan) == (("AB", "CD"), ("BC",))
    for stage in plan.stages:
        for index, left in enumerate(stage.edge_types):
            for right in stage.edge_types[index + 1 :]:
                assert labels[left].isdisjoint(labels[right])


def test_self_reference_conflicts_only_when_its_label_is_used():
    plan = build_relationship_conflict_plan([
        _edge("KNOWS", "Person", "Person"),
        _edge("WORKS_AT", "Person", "Company"),
        _edge("SUPPLIES", "Vendor", "Product"),
    ])

    assert _stage_types(plan) == (("KNOWS", "SUPPLIES"), ("WORKS_AT",))


def test_plan_does_not_depend_on_input_order():
    edges = [
        _edge("WORKS_AT", "Person", "Company"),
        _edge("BOUGHT", "Person", "Product"),
        _edge("SUPPLIES", "Vendor", "Product"),
    ]

    assert build_relationship_conflict_plan(edges) == build_relationship_conflict_plan(list(reversed(edges)))


def test_public_plan_state_is_immutable():
    plan = build_relationship_conflict_plan([_edge("WORKS_AT", "Person", "Company")])

    with pytest.raises(TypeError):
        plan.conflicts["WORKS_AT"] = frozenset()
    with pytest.raises(AttributeError):
        plan.conflicts["WORKS_AT"].add("OTHER")
    with pytest.raises(FrozenInstanceError):
        plan.stages[0].edge_types = ("OTHER",)


def test_duplicate_types_are_rejected_for_direct_callers():
    with pytest.raises(ValueError, match="duplicate relationship edge types"):
        build_relationship_conflict_plan([
            _edge("WORKS_AT", "Person", "Company"),
            _edge("WORKS_AT", "Person", "Vendor"),
        ])


def test_conflict_families_preserve_only_component_shared_labels():
    families = build_conflict_families([
        _edge("WORKS_AT", "Person", "Company"),
        _edge("BOUGHT", "Person", "Product"),
        _edge("SUPPLIES", "Vendor", "Stock"),
    ])

    assert [family.edge_types for family in families] == [("BOUGHT", "WORKS_AT"), ("SUPPLIES",)]
    shared, isolated = families
    assert shared.shared_labels == ("Person",)
    assert dict(shared.edge_endpoint_labels) == {
        "BOUGHT": frozenset({"Person", "Product"}),
        "WORKS_AT": frozenset({"Company", "Person"}),
    }
    assert dict(shared.edge_shared_labels) == {
        "BOUGHT": frozenset({"Person"}), "WORKS_AT": frozenset({"Person"}),
    }
    assert isolated.shared_labels == ()


def test_conflict_families_are_stable_for_transitive_components_and_reject_self_reference():
    edges = [_edge("AB", "A", "B"), _edge("BC", "B", "C"), _edge("CD", "C", "D")]
    family = build_conflict_families(edges)[0]
    assert family.edge_types == ("AB", "BC", "CD")
    assert family.shared_labels == ("B", "C")
    assert family == build_conflict_families(list(reversed(edges)))[0]
    with pytest.raises(ValueError, match="edge=KNOWS is self-referencing"):
        build_conflict_families([_edge("KNOWS", "Person", "Person")])
