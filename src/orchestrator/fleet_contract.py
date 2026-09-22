"""Canonical shared-fleet selection used by edge loaders and the clock."""
from __future__ import annotations

from typing import Sequence

from src.models.schema import EdgeConfig


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


def select_fleet_edges(edges: Sequence[EdgeConfig], fleet_types: tuple[str, ...]) -> tuple[EdgeConfig, ...]:
    """Return exactly the declared eligible schema edges named by one contract."""
    by_type = {edge.type: edge for edge in edges}
    if len(by_type) != len(edges) or set(fleet_types) != set(by_type) & set(fleet_types):
        raise ValueError("fleet edge types are absent or duplicate in schema")
    eligible_types = {edge.type for edge in edges if not edge.nodes.is_self_referencing}
    if set(fleet_types) != eligible_types:
        raise ValueError("fleet edge types must include every eligible schema edge")
    selected = tuple(by_type[edge_type] for edge_type in fleet_types)
    if len(selected) != len(fleet_types):
        raise ValueError("fleet edge types are incomplete")
    if any(edge.nodes.is_self_referencing for edge in selected):
        raise ValueError("self-referencing edge cannot join shared fleet")
    return selected
