"""Pure deterministic label-scoped ownership rotation for relationship fleets."""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping, Sequence

from src.orchestrator.dependency_manager import ConflictFamily


@dataclass(frozen=True)
class SharedLabelResource:
    """One contested endpoint-label bucket, independent of Kafka records."""

    label: str
    bucket: int

    def __post_init__(self) -> None:
        if not isinstance(self.label, str) or not self.label.strip():
            raise ValueError("shared resource label must be nonblank")
        if isinstance(self.bucket, bool) or not isinstance(self.bucket, int) or self.bucket < 0:
            raise ValueError("shared resource bucket must be a nonnegative integer")


@dataclass(frozen=True)
class RotationPlan:
    """Validated fair ownership schedule for all conflicted label resources."""

    families: tuple[ConflictFamily, ...]
    bucket_count: int

    def __post_init__(self) -> None:
        if isinstance(self.bucket_count, bool) or not isinstance(self.bucket_count, int) or not 1 <= self.bucket_count <= 4096:
            raise ValueError("rotation bucket count must be between 1 and 4096")
        families = tuple(self.families)
        if tuple(sorted(families, key=lambda family: family.edge_types)) != families:
            raise ValueError("rotation families must be canonical")
        edge_types: set[str] = set()
        labels: set[str] = set()
        for family in families:
            if not isinstance(family, ConflictFamily):
                raise ValueError("rotation family has invalid type")
            overlap = edge_types & set(family.edge_types)
            if overlap:
                raise ValueError(f"rotation family has duplicate edge type={min(overlap)}")
            edge_types.update(family.edge_types)
            shared_overlap = labels & set(family.shared_labels)
            if shared_overlap:
                raise ValueError(f"rotation family has duplicate shared label={min(shared_overlap)}")
            labels.update(family.shared_labels)
            for edge_type, endpoints in family.edge_endpoint_labels.items():
                if len(endpoints) == 1:
                    raise ValueError(f"rotation family edge={edge_type} is self-referencing")
                if not family.edge_shared_labels[edge_type].issubset(endpoints):
                    raise ValueError(f"rotation family edge={edge_type} has partial metadata")
        object.__setattr__(self, "families", families)

    @staticmethod
    def _epoch(epoch: int) -> int:
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 0:
            raise ValueError("rotation epoch must be a nonnegative integer")
        return epoch

    def owners_for_epoch(self, epoch: int) -> Mapping[str, Mapping[int, str]]:
        """Return complete immutable owner maps for every contested resource."""
        epoch = self._epoch(epoch)
        owners: dict[str, Mapping[int, str]] = {}
        for family in self.families:
            if not family.shared_labels:
                continue
            owner = family.edge_types[epoch % len(family.edge_types)]
            for label in family.shared_labels:
                owners[label] = MappingProxyType({bucket: owner for bucket in range(self.bucket_count)})
        return MappingProxyType(owners)

    def owner_for(self, resource: SharedLabelResource, epoch: int) -> str:
        """Return the sole scheduled owner of one valid shared resource."""
        if resource.bucket >= self.bucket_count:
            raise ValueError(f"shared resource label={resource.label} bucket={resource.bucket} is outside rotation")
        owners = self.owners_for_epoch(epoch)
        try:
            return owners[resource.label][resource.bucket]
        except KeyError as exc:
            raise ValueError(f"shared resource label={resource.label} is not in rotation") from exc

    def max_wait_epochs(self, edge_type: str, label: str) -> int:
        """Return the finite wait bound for an edge's actually shared label."""
        for family in self.families:
            if edge_type in family.edge_types:
                if label not in family.edge_shared_labels[edge_type]:
                    raise ValueError(f"rotation edge={edge_type} does not use shared label={label}")
                return len(family.edge_types)
        raise ValueError(f"rotation has no edge type={edge_type}")


def build_rotation_plan(families: Sequence[ConflictFamily], *, bucket_count: int) -> RotationPlan:
    """Build a validated canonical schedule without Kafka, Docker, or Neo4j I/O."""
    return RotationPlan(tuple(sorted(families, key=lambda family: family.edge_types)), bucket_count)
