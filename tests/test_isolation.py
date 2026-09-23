"""Unit tests for deterministic self-reference isolation planning."""
from types import SimpleNamespace

import pytest

from src.orchestrator.isolation import (
    IsolatedRelationshipPhase,
    RelationshipIsolationPlan,
    build_relationship_isolation_plan,
)


def _edge(edge_type: str, source: str, target: str):
    return SimpleNamespace(
        type=edge_type,
        nodes=SimpleNamespace(
            source=source,
            target=target,
            is_self_referencing=source == target,
        ),
    )


def test_empty_and_shared_only_plans_are_canonical():
    assert build_relationship_isolation_plan(() ) == RelationshipIsolationPlan((), ())
    plan = build_relationship_isolation_plan((_edge("WORKS_AT", "Person", "Company"), _edge("BOUGHT", "Person", "Product")))
    assert plan.shared_edge_types == ("BOUGHT", "WORKS_AT")
    assert plan.isolated_phases == ()


def test_self_reference_and_mixed_types_are_partitioned_deterministically():
    plan = build_relationship_isolation_plan((
        _edge("WORKS_AT", "Person", "Company"),
        _edge("KNOWS", "Person", "Person"),
        _edge("BOUGHT", "Person", "Product"),
    ))
    assert plan.shared_edge_types == ("BOUGHT", "WORKS_AT")
    assert plan.isolated_phases == (IsolatedRelationshipPhase(0, "KNOWS", "Person"),)


def test_multiple_self_reference_phases_and_shuffled_input_are_lexical():
    edges = (_edge("Z_LINK", "Zed", "Zed"), _edge("A_LINK", "Alpha", "Alpha"), _edge("WORKS_AT", "Person", "Company"))
    first = build_relationship_isolation_plan(edges)
    second = build_relationship_isolation_plan(tuple(reversed(edges)))
    assert first == second
    assert [(phase.index, phase.edge_type, phase.label) for phase in first.isolated_phases] == [
        (0, "A_LINK", "Alpha"), (1, "Z_LINK", "Zed"),
    ]


def test_duplicate_and_forged_inputs_fail_with_attribution():
    with pytest.raises(ValueError, match=r"duplicate relationship isolation edge type=KNOWS labels=Person,Person"):
        build_relationship_isolation_plan((_edge("KNOWS", "Person", "Person"), _edge("KNOWS", "Person", "Person")))
    forged = _edge("FORGED", "Person", "Company")
    forged.nodes.is_self_referencing = True
    with pytest.raises(ValueError, match=r"edge=FORGED.*labels=Person,Company"):
        build_relationship_isolation_plan((forged,))
    inverse = _edge("INVERSE", "Person", "Person")
    inverse.nodes.is_self_referencing = False
    with pytest.raises(ValueError, match=r"edge=INVERSE.*labels=Person,Person"):
        build_relationship_isolation_plan((inverse,))


def test_plan_invariants_reject_noncanonical_overlap_and_indices():
    phase = IsolatedRelationshipPhase(1, "KNOWS", "Person")
    with pytest.raises(ValueError, match="phase indices"):
        RelationshipIsolationPlan((), (phase,))
    with pytest.raises(ValueError, match="shared and isolated"):
        RelationshipIsolationPlan(("KNOWS",), (IsolatedRelationshipPhase(0, "KNOWS", "Person"),))
    with pytest.raises(ValueError, match="sorted and unique"):
        RelationshipIsolationPlan(("WORKS_AT", "BOUGHT"), ())
