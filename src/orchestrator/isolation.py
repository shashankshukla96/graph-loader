"""Pure deterministic planning for absolutely isolated self-reference edges."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from src.models.schema import EdgeConfig


@dataclass(frozen=True)
class IsolatedRelationshipPhase:
    """One lexically ordered, clock-free self-reference relationship phase."""

    index: int
    edge_type: str
    label: str

    def __post_init__(self) -> None:
        if isinstance(self.index, bool) or not isinstance(self.index, int) or self.index < 0:
            raise ValueError("isolated relationship phase index must be a nonnegative integer")
        if not isinstance(self.edge_type, str) or not self.edge_type.strip():
            raise ValueError("isolated relationship phase edge type must be nonblank")
        if not isinstance(self.label, str) or not self.label.strip():
            raise ValueError(f"isolated relationship phase edge={self.edge_type} label must be nonblank")


@dataclass(frozen=True)
class RelationshipIsolationPlan:
    """Canonical split between shared-clock edges and absolute isolation phases."""

    shared_edge_types: tuple[str, ...]
    isolated_phases: tuple[IsolatedRelationshipPhase, ...]

    def __post_init__(self) -> None:
        shared = tuple(self.shared_edge_types)
        phases = tuple(self.isolated_phases)
        if any(not isinstance(edge_type, str) or not edge_type.strip() for edge_type in shared):
            raise ValueError("relationship isolation plan shared edge type must be nonblank")
        if shared != tuple(sorted(shared)) or len(set(shared)) != len(shared):
            raise ValueError("relationship isolation plan shared edge types must be sorted and unique")
        if any(not isinstance(phase, IsolatedRelationshipPhase) for phase in phases):
            raise ValueError("relationship isolation plan phases must be isolated relationship phases")
        isolated_types = tuple(phase.edge_type for phase in phases)
        if isolated_types != tuple(sorted(isolated_types)) or len(set(isolated_types)) != len(isolated_types):
            raise ValueError("relationship isolation plan phases must be sorted and unique")
        if tuple(phase.index for phase in phases) != tuple(range(len(phases))):
            raise ValueError("relationship isolation plan phase indices must be contiguous")
        overlap = set(shared) & set(isolated_types)
        if overlap:
            raise ValueError(
                f"relationship isolation plan edge type appears in shared and isolated phases edge={min(overlap)}"
            )
        object.__setattr__(self, "shared_edge_types", shared)
        object.__setattr__(self, "isolated_phases", phases)


def build_relationship_isolation_plan(edges: Sequence[EdgeConfig]) -> RelationshipIsolationPlan:
    """Return canonical shared types and one absolute phase per self-reference.

    Classification exclusively uses ``is_self_referencing``.  The redundant
    source/target consistency check deliberately protects callers that supply
    lightweight edge-like values in unit tests or orchestration validation.
    """
    by_type: dict[str, EdgeConfig] = {}
    for edge in edges:
        edge_type = getattr(edge, "type", None)
        nodes = getattr(edge, "nodes", None)
        source, target = getattr(nodes, "source", None), getattr(nodes, "target", None)
        if not isinstance(edge_type, str) or not edge_type.strip():
            raise ValueError("relationship isolation edge type must be nonblank")
        if edge_type in by_type:
            raise ValueError(
                f"duplicate relationship isolation edge type={edge_type} labels={source},{target}"
            )
        by_type[edge_type] = edge

    shared: list[str] = []
    isolated: list[tuple[str, str]] = []
    for edge_type in sorted(by_type):
        edge = by_type[edge_type]
        nodes = edge.nodes
        is_self_referencing = getattr(nodes, "is_self_referencing", None)
        if not isinstance(is_self_referencing, bool):
            raise ValueError(f"relationship isolation edge={edge_type} has invalid self-reference classification")
        if is_self_referencing != (nodes.source == nodes.target):
            raise ValueError(
                f"relationship isolation edge={edge_type} has inconsistent self-reference labels={nodes.source},{nodes.target}"
            )
        if is_self_referencing:
            isolated.append((edge_type, nodes.source))
        else:
            shared.append(edge_type)
    return RelationshipIsolationPlan(
        shared_edge_types=tuple(shared),
        isolated_phases=tuple(
            IsolatedRelationshipPhase(index, edge_type, label)
            for index, (edge_type, label) in enumerate(isolated)
        ),
    )
