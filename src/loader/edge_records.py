"""Schema-driven validation of canonical relationship records (no I/O).

Kept free of Kafka, Neo4j and Docker imports so planner processes load it
cheaply; :mod:`src.loader.edge_loader` re-exports every name.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from src.loader.record_validation import normalize_property_value
from src.models.schema import EdgeConfig, PropertyConfig


class EdgeRecordValidationError(ValueError):
    """Raised when a relationship event violates its declared schema."""


@dataclass(frozen=True)
class EdgeRecord:
    """Normalized keys and allowlisted properties for one relationship event."""

    source_key: object
    target_key: object
    properties: dict[str, object]


def _raise_error(edge_config: EdgeConfig, field: str, reason: str) -> None:
    raise EdgeRecordValidationError(f"Edge '{edge_config.type}' field '{field}' {reason}")


def _normalize_endpoint(
    section: Mapping[str, object],
    *,
    section_name: str,
    key_property: str,
    property_config: PropertyConfig,
    edge_config: EdgeConfig,
) -> object:
    unknown = set(section) - {key_property}
    if unknown:
        _raise_error(edge_config, f"{section_name}.{sorted(unknown)[0]}", "is not declared")
    if key_property not in section or section[key_property] is None:
        _raise_error(edge_config, f"{section_name}.{key_property}", "is required")
    return normalize_property_value(
        section[key_property],
        property_config,
        entity_description=f"Edge '{edge_config.type}' {section_name}",
        property_name=key_property,
        error_factory=EdgeRecordValidationError,
    )


def normalize_edge_record(raw_record: object, edge_config: EdgeConfig) -> EdgeRecord:
    """Validate the canonical envelope and normalize its declared values.

    Edge configs must come from a fully validated :class:`GraphSchema`, which
    binds endpoint scalar definitions.  Direct ``EdgeConfig`` construction is
    rejected deterministically rather than accepting unconstrained values.
    """
    if not isinstance(raw_record, Mapping):
        _raise_error(edge_config, "event", "must be a mapping")

    source_property = edge_config.source_key_config
    target_property = edge_config.target_key_config
    if source_property is None or target_property is None:
        edge_config.single_pair  # v7-S7: a multi-pair parent raises MultiPairEdgeError (pass a topic view)
        raise EdgeRecordValidationError(
            f"Edge '{edge_config.type}' has unresolved endpoint schema bindings"
        )

    expected_sections = {"source", "target", "properties"}
    unknown_sections = set(raw_record) - expected_sections
    if unknown_sections:
        _raise_error(edge_config, sorted(unknown_sections)[0], "is not declared")
    for section_name in expected_sections:
        section = raw_record.get(section_name)
        if not isinstance(section, Mapping):
            _raise_error(edge_config, section_name, "must be a mapping")

    source = raw_record["source"]
    target = raw_record["target"]
    properties = raw_record["properties"]
    assert isinstance(source, Mapping) and isinstance(target, Mapping) and isinstance(properties, Mapping)
    source_key = _normalize_endpoint(
        source,
        section_name="source",
        key_property=edge_config.source_key_property,
        property_config=source_property,
        edge_config=edge_config,
    )
    target_key = _normalize_endpoint(
        target,
        section_name="target",
        key_property=edge_config.target_key_property,
        property_config=target_property,
        edge_config=edge_config,
    )

    declared_properties = edge_config.properties
    for property_name in properties:
        if property_name not in declared_properties:
            _raise_error(edge_config, f"properties.{property_name}", "is not declared")
    normalized_properties: dict[str, object] = {}
    for property_name, property_config in declared_properties.items():
        value = properties.get(property_name)
        if property_name not in properties or value is None:
            if property_config.required:
                _raise_error(edge_config, f"properties.{property_name}", "is required")
            continue
        normalized_properties[property_name] = normalize_property_value(
            value,
            property_config,
            entity_description=f"Edge '{edge_config.type}' properties",
            property_name=property_name,
            error_factory=EdgeRecordValidationError,
        )
    return EdgeRecord(source_key=source_key, target_key=target_key, properties=normalized_properties)
