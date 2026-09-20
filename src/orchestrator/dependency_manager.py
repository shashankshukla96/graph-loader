"""Deterministic safe bulk stages for schema-declared relationship loaders."""
from __future__ import annotations

from dataclasses import dataclass
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
