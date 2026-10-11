"""Simulation tests for deterministic shared-label ownership rotation."""
from types import MappingProxyType

import pytest

from src.orchestrator.dependency_manager import ConflictFamily, build_conflict_families
from src.orchestrator.rotation import SharedLabelResource, build_rotation_plan


class _Edge:
    def __init__(self, edge_type, source, target):
        self.type = edge_type
        self.nodes = type("Nodes", (), {"source": source, "target": target})()


def _shared_person_plan():
    families = build_conflict_families([
        _Edge("WORKS_AT", "Person", "Company"),
        _Edge("BOUGHT", "Person", "Product"),
    ])
    return build_rotation_plan(families, bucket_count=4)


def test_shared_person_rotation_is_exclusive_deterministic_and_fair():
    plan = _shared_person_plan()
    seen = {"BOUGHT": [], "WORKS_AT": []}
    for epoch in range(6):
        owners = plan.owners_for_epoch(epoch)
        assert set(owners) == {"Person"}
        assert set(owners["Person"]) == {0, 1, 2, 3}
        assert len(set(owners["Person"].values())) == 1
        owner = plan.owner_for(SharedLabelResource("Person", 3), epoch)
        assert owner == owners["Person"][3]
        seen[owner].append(epoch)
    assert seen == {"BOUGHT": [0, 2, 4], "WORKS_AT": [1, 3, 5]}
    assert plan.max_wait_epochs("WORKS_AT", "Person") == 2
    assert plan.owners_for_epoch(3) == plan.owners_for_epoch(3)


def test_multi_label_family_grants_one_owner_all_shared_labels_together():
    family = ConflictFamily(
        ("LEFT", "RIGHT"), ("A", "B"),
        MappingProxyType({"LEFT": frozenset({"A", "B"}), "RIGHT": frozenset({"A", "B"})}),
        MappingProxyType({"LEFT": frozenset({"A", "B"}), "RIGHT": frozenset({"A", "B"})}),
    )
    plan = build_rotation_plan([family], bucket_count=2)
    assert set(plan.owners_for_epoch(0)["A"].values()) == {"LEFT"}
    assert set(plan.owners_for_epoch(0)["B"].values()) == {"LEFT"}
    assert set(plan.owners_for_epoch(1)["A"].values()) == {"RIGHT"}
    assert set(plan.owners_for_epoch(1)["B"].values()) == {"RIGHT"}


def test_rotation_rejects_unsafe_resources_and_malformed_families():
    with pytest.raises(ValueError, match="bucket count"):
        build_rotation_plan([], bucket_count=True)
    with pytest.raises(ValueError, match="epoch"):
        _shared_person_plan().owners_for_epoch(True)
    with pytest.raises(ValueError, match="outside rotation"):
        _shared_person_plan().owner_for(SharedLabelResource("Person", 4), 0)
    with pytest.raises(ValueError, match="not in rotation"):
        _shared_person_plan().owner_for(SharedLabelResource("Other", 0), 0)
    self_reference = ConflictFamily(
        ("KNOWS",), (),
        MappingProxyType({"KNOWS": frozenset({"Person"})}),
        MappingProxyType({"KNOWS": frozenset()}),
    )
    with pytest.raises(ValueError, match="self-referencing"):
        build_rotation_plan([self_reference], bucket_count=1)


def test_conflict_family_rejects_omitted_shared_metadata_and_is_immutable():
    endpoints = MappingProxyType({
        "LEFT": frozenset({"A", "B"}), "RIGHT": frozenset({"A", "C"}),
    })
    with pytest.raises(ValueError, match="shared labels are incomplete"):
        ConflictFamily(("LEFT", "RIGHT"), (), endpoints, MappingProxyType({
            "LEFT": frozenset(), "RIGHT": frozenset(),
        }))
    with pytest.raises(ValueError, match="edge=RIGHT has incomplete"):
        ConflictFamily(("LEFT", "RIGHT"), ("A",), endpoints, MappingProxyType({
            "LEFT": frozenset({"A"}), "RIGHT": frozenset(),
        }))
    family = ConflictFamily(("LEFT", "RIGHT"), ("A",), endpoints, MappingProxyType({
        "LEFT": frozenset({"A"}), "RIGHT": frozenset({"A"}),
    }))
    with pytest.raises(TypeError):
        family.edge_endpoint_labels["LEFT"] = frozenset()
    with pytest.raises(AttributeError):
        family.edge_shared_labels["LEFT"].add("B")


def test_rotation_rejects_cross_family_duplicate_shared_label():
    def family(prefix):
        return ConflictFamily(
            (f"{prefix}1", f"{prefix}2"), ("A",),
            MappingProxyType({
                f"{prefix}1": frozenset({"A", "B"}),
                f"{prefix}2": frozenset({"A", "C"}),
            }),
            MappingProxyType({f"{prefix}1": frozenset({"A"}), f"{prefix}2": frozenset({"A"})}),
        )

    with pytest.raises(ValueError, match="duplicate shared label=A"):
        build_rotation_plan([family("LEFT"), family("RIGHT")], bucket_count=2)
