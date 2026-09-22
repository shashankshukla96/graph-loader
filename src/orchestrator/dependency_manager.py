"""Deterministic safe bulk stages for schema-declared relationship loaders."""
from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping, Sequence

from src.models.schema import EdgeConfig


@dataclass(frozen=True)
class RelationshipStage:
    """One deterministic, pairwise endpoint-label-disjoint bulk stage."""

    index: int
    edge_types: tuple[str, ...]


@dataclass(frozen=True)
class RelationshipConflictPlan:
    """Immutable conflict adjacency and ordered safe relationship stages."""

    conflicts: Mapping[str, frozenset[str]]
    stages: tuple[RelationshipStage, ...]


@dataclass(frozen=True)
class ConflictFamily:
    """One immutable connected component of eligible relationship conflicts."""

    edge_types: tuple[str, ...]
    shared_labels: tuple[str, ...]
    edge_endpoint_labels: Mapping[str, frozenset[str]]
    edge_shared_labels: Mapping[str, frozenset[str]]
    edge_endpoint_order: Mapping[str, tuple[str, str]] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        types = tuple(self.edge_types)
        labels = tuple(self.shared_labels)
        if not types or tuple(sorted(types)) != types or len(set(types)) != len(types):
            raise ValueError("conflict family edge types must be sorted and unique")
        if any(not isinstance(edge_type, str) or not edge_type.strip() for edge_type in types):
            raise ValueError("conflict family edge type must be nonblank")
        if tuple(sorted(labels)) != labels or len(set(labels)) != len(labels):
            raise ValueError("conflict family shared labels must be sorted and unique")
        if any(not isinstance(label, str) or not label.strip() for label in labels):
            raise ValueError("conflict family label must be nonblank")
        endpoint_labels = {
            edge_type: frozenset(values)
            for edge_type, values in self.edge_endpoint_labels.items()
        }
        edge_shared_labels = {
            edge_type: frozenset(values)
            for edge_type, values in self.edge_shared_labels.items()
        }
        endpoint_order = {
            edge_type: tuple(values)
            for edge_type, values in self.edge_endpoint_order.items()
        }
        if set(endpoint_labels) != set(types) or set(edge_shared_labels) != set(types):
            raise ValueError("conflict family edge metadata must cover every edge type")
        label_counts: dict[str, int] = {}
        for edge_type in types:
            endpoints = endpoint_labels[edge_type]
            if not endpoints or any(not isinstance(label, str) or not label.strip() for label in endpoints):
                raise ValueError(f"conflict family edge={edge_type} has invalid endpoint labels")
            for label in endpoints:
                label_counts[label] = label_counts.get(label, 0) + 1
        expected_shared = tuple(sorted(label for label, count in label_counts.items() if count >= 2))
        if labels != expected_shared:
            raise ValueError("conflict family shared labels are incomplete")
        for edge_type in types:
            expected_edge_shared = endpoint_labels[edge_type] & frozenset(labels)
            if edge_shared_labels[edge_type] != expected_edge_shared:
                raise ValueError(f"conflict family edge={edge_type} has incomplete shared labels")
        if endpoint_order:
            if set(endpoint_order) != set(types) or any(
                len(endpoint_order[edge_type]) != 2
                or frozenset(endpoint_order[edge_type]) != endpoint_labels[edge_type]
                for edge_type in types
            ):
                raise ValueError("conflict family endpoint order is inconsistent")
        object.__setattr__(self, "edge_types", types)
        object.__setattr__(self, "shared_labels", labels)
        object.__setattr__(self, "edge_endpoint_labels", MappingProxyType(endpoint_labels))
        object.__setattr__(self, "edge_shared_labels", MappingProxyType(edge_shared_labels))
        object.__setattr__(self, "edge_endpoint_order", MappingProxyType(endpoint_order))


def _endpoint_labels(edge: EdgeConfig) -> frozenset[str]:
    """Return the node-label resources used by one declared edge type."""
    return frozenset((edge.nodes.source, edge.nodes.target))


def build_relationship_conflict_plan(
    edges: Sequence[EdgeConfig],
) -> RelationshipConflictPlan:
    """Return deterministic pairwise-label-disjoint bulk stages.

    Two relationship types conflict precisely when their source/target label
    resource sets intersect.  Types are processed lexically and assigned to
    the first stage whose accumulated resources are disjoint, which gives a
    reproducible first-fit plan regardless of YAML declaration order.
    """
    edges_by_type = {edge.type: edge for edge in edges}
    if len(edges_by_type) != len(edges):
        raise ValueError("duplicate relationship edge types are not schedulable")

    ordered_types = tuple(sorted(edges_by_type))
    labels_by_type = {
        edge_type: _endpoint_labels(edges_by_type[edge_type])
        for edge_type in ordered_types
    }
    mutable_conflicts: dict[str, set[str]] = {edge_type: set() for edge_type in ordered_types}
    for index, left_type in enumerate(ordered_types):
        for right_type in ordered_types[index + 1 :]:
            if labels_by_type[left_type] & labels_by_type[right_type]:
                mutable_conflicts[left_type].add(right_type)
                mutable_conflicts[right_type].add(left_type)

    stage_types: list[list[str]] = []
    stage_labels: list[set[str]] = []
    for edge_type in ordered_types:
        labels = labels_by_type[edge_type]
        for types, occupied_labels in zip(stage_types, stage_labels):
            if labels.isdisjoint(occupied_labels):
                types.append(edge_type)
                occupied_labels.update(labels)
                break
        else:
            stage_types.append([edge_type])
            stage_labels.append(set(labels))

    immutable_conflicts = MappingProxyType(
        {
            edge_type: frozenset(sorted(neighbours))
            for edge_type, neighbours in mutable_conflicts.items()
        }
    )
    stages = tuple(
        RelationshipStage(index=index, edge_types=tuple(types))
        for index, types in enumerate(stage_types)
    )
    return RelationshipConflictPlan(conflicts=immutable_conflicts, stages=stages)


def build_conflict_families(edges: Sequence[EdgeConfig]) -> tuple[ConflictFamily, ...]:
    """Return deterministic connected conflict families for shared rotation.

    A self-referencing edge is deliberately rejected here.  Phase 4 Slice 4
    owns its isolation execution policy; shared-clock rotation must not admit
    it accidentally.
    """
    plan = build_relationship_conflict_plan(edges)
    edges_by_type = {edge.type: edge for edge in edges}
    for edge_type, edge in edges_by_type.items():
        if edge.nodes.source == edge.nodes.target:
            raise ValueError(f"conflict family edge={edge_type} is self-referencing")

    unvisited = set(plan.conflicts)
    families: list[ConflictFamily] = []
    while unvisited:
        root = min(unvisited)
        component: set[str] = set()
        pending = [root]
        while pending:
            edge_type = pending.pop()
            if edge_type in component:
                continue
            component.add(edge_type)
            unvisited.discard(edge_type)
            pending.extend(sorted(plan.conflicts[edge_type] - component, reverse=True))
        edge_types = tuple(sorted(component))
        endpoints = {
            edge_type: frozenset((edges_by_type[edge_type].nodes.source, edges_by_type[edge_type].nodes.target))
            for edge_type in edge_types
        }
        endpoint_order = {
            edge_type: (edges_by_type[edge_type].nodes.source, edges_by_type[edge_type].nodes.target)
            for edge_type in edge_types
        }
        label_counts: dict[str, int] = {}
        for labels in endpoints.values():
            for label in labels:
                label_counts[label] = label_counts.get(label, 0) + 1
        shared_labels = tuple(sorted(label for label, count in label_counts.items() if count > 1))
        shared = {
            edge_type: frozenset(endpoints[edge_type] & set(shared_labels))
            for edge_type in edge_types
        }
        families.append(ConflictFamily(edge_types, shared_labels, endpoints, shared, endpoint_order))
    return tuple(sorted(families, key=lambda family: family.edge_types))
