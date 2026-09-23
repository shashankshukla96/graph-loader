"""Canonical shared-fleet selection used by edge loaders and the clock."""
from __future__ import annotations

from typing import Sequence

from src.models.schema import EdgeConfig
from src.orchestrator.isolation import build_relationship_isolation_plan


def parse_fleet_edge_types(value: str) -> tuple[str, ...]:
    """Parse one canonical comma-delimited, lexically sorted type contract."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("fleet edge types must be nonblank")
    types = tuple(value.split(","))
    if any(not item or item.strip() != item for item in types):
        raise ValueError("fleet edge types must not contain blank or spaced entries")
    if tuple(sorted(types)) != types or len(set(types)) != len(types):
        raise ValueError("fleet edge types must be canonical sorted unique values")
    return types


def select_shared_fleet_edges(edges: Sequence[EdgeConfig], fleet_types: tuple[str, ...]) -> tuple[EdgeConfig, ...]:
    """Return exactly the canonical non-self-reference edges of one schema."""
    if not isinstance(fleet_types, tuple) or not fleet_types:
        raise ValueError("fleet edge types must be a nonempty canonical tuple")
    if any(not isinstance(item, str) or not item or item.strip() != item for item in fleet_types):
        raise ValueError("fleet edge types must not contain blank or spaced entries")
    if tuple(sorted(fleet_types)) != fleet_types or len(set(fleet_types)) != len(fleet_types):
        raise ValueError("fleet edge types must be canonical sorted unique values")
    by_type = {edge.type: edge for edge in edges}
    if len(by_type) != len(edges) or set(fleet_types) != set(by_type) & set(fleet_types):
        raise ValueError("fleet edge types are absent or duplicate in schema")
    plan = build_relationship_isolation_plan(edges)
    if fleet_types != plan.shared_edge_types:
        raise ValueError("fleet edge types must include every eligible schema edge")
    selected = tuple(by_type[edge_type] for edge_type in fleet_types)
    if len(selected) != len(fleet_types):
        raise ValueError("fleet edge types are incomplete")
    if any(edge.nodes.is_self_referencing for edge in selected):
        raise ValueError("self-referencing edge cannot join shared fleet")
    return selected


def select_fleet_edges(edges: Sequence[EdgeConfig], fleet_types: tuple[str, ...]) -> tuple[EdgeConfig, ...]:
    """Compatibility alias for the canonical shared-fleet selector."""
    return select_shared_fleet_edges(edges, fleet_types)
