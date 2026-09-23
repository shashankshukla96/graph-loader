"""Tests for exact canonical shared-fleet process contracts."""
from types import SimpleNamespace

import pytest

from src.orchestrator.fleet_contract import parse_fleet_edge_types, select_fleet_edges, select_shared_fleet_edges


def _edge(edge_type, source, target):
    return SimpleNamespace(type=edge_type, nodes=SimpleNamespace(
        source=source, target=target, is_self_referencing=source == target,
    ))


def test_fleet_contract_requires_every_eligible_edge_and_excludes_self_references():
    edges = [_edge("BOUGHT", "Person", "Product"), _edge("KNOWS", "Person", "Person"), _edge("WORKS_AT", "Person", "Company")]
    contract = parse_fleet_edge_types("BOUGHT,WORKS_AT")
    assert [edge.type for edge in select_fleet_edges(edges, contract)] == ["BOUGHT", "WORKS_AT"]
    with pytest.raises(ValueError, match="every eligible"):
        select_fleet_edges(edges, parse_fleet_edge_types("BOUGHT"))
    with pytest.raises(ValueError, match="every eligible"):
        select_fleet_edges(edges, parse_fleet_edge_types("BOUGHT,KNOWS,WORKS_AT"))


@pytest.mark.parametrize("value", ["", "WORKS_AT,BOUGHT", "BOUGHT,BOUGHT", "BOUGHT, WORKS_AT"])
def test_fleet_contract_rejects_noncanonical_values(value):
    with pytest.raises(ValueError):
        parse_fleet_edge_types(value)


def test_shared_selector_rejects_self_reference_and_preserves_shared_order():
    edges = [_edge("BOUGHT", "Person", "Product"), _edge("KNOWS", "Person", "Person"), _edge("WORKS_AT", "Person", "Company")]
    assert [edge.type for edge in select_shared_fleet_edges(edges, ("BOUGHT", "WORKS_AT"))] == ["BOUGHT", "WORKS_AT"]
    with pytest.raises(ValueError, match="every eligible"):
        select_shared_fleet_edges(edges, ("BOUGHT", "KNOWS", "WORKS_AT"))


@pytest.mark.parametrize("fleet_types", [("WORKS_AT", "BOUGHT"), ("BOUGHT", "BOUGHT")])
def test_shared_selector_rejects_noncanonical_tuple_directly(fleet_types):
    edges = [_edge("BOUGHT", "Person", "Product"), _edge("WORKS_AT", "Person", "Company")]
    with pytest.raises(ValueError, match="canonical sorted unique"):
        select_shared_fleet_edges(edges, fleet_types)
