"""
tests/test_schema_validation.py
────────────────────────────────
Unit tests for:
  - src/models/schema.py  (Pydantic v2 models)
  - src/utils/schema_loader.py  (YAML → GraphSchema pipeline)

All SchemaLoader tests use load_schema() exclusively.
No test in TestSchemaLoader imports GraphSchema directly.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from src.models.schema import (
    ConstraintConfig,
    EdgeConfig,
    GraphSchema,
    IndexConfig,
    LoadingConfig,
    MixAndBatchConfig,
    NodeConfig,
    PropertyConfig,
    RetryConfig,
    SourceTargetConfig,
)
from src.utils.schema_loader import SchemaLoadError, load_schema

# ── Project root — anchors path-dependent tests to the repo root ──────────────
# Follows the same convention as tests/test_dev_environment.py.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _minimal_node(label: str = "Person", key: str = "pid") -> dict:
    """Return the smallest valid NodeConfig dict for the given label."""
    return {
        "label": label,
        "topic": f"{label.lower()}-events",
        "key_property": key,
        "properties": {key: {"type": "string", "required": True}},
    }


def _minimal_edge(etype: str = "KNOWS") -> dict:
    """Return the smallest valid EdgeConfig dict for the given relationship type."""
    return {
        "type": etype,
        "topic": f"{etype.lower()}-events",
        "nodes": {"source": "Person", "target": "Person"},
        "properties": {"since": {"type": "date", "required": True}},
    }


def _valid_schema_dict() -> dict:
    """Return a minimal but fully valid GraphSchema dict (2 nodes, 2 edges)."""
    return {
        "nodes": [
            _minimal_node("Person", "personId"),
            _minimal_node("Company", "companyId"),
        ],
        "edges": [
            _minimal_edge("WORKS_AT"),
            _minimal_edge("KNOWS"),
        ],
    }


# ── TestPropertyConfig ────────────────────────────────────────────────────────


class TestPropertyConfig:
    def test_valid_string_property(self) -> None:
        p = PropertyConfig(type="string")
        assert p.type == "string"
        assert p.required is False

    @pytest.mark.parametrize("ptype", ["string", "integer", "float", "date", "datetime"])
    def test_all_valid_types_accepted(self, ptype: str) -> None:
        PropertyConfig(type=ptype)  # must not raise

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValidationError, match="type"):
            PropertyConfig(type="blob")  # type: ignore[arg-type]

    def test_constraint_embedded(self) -> None:
        p = PropertyConfig(type="string", constraint={"type": "uniqueness"})
        assert p.constraint is not None
        assert p.constraint.type == "uniqueness"

    def test_index_embedded(self) -> None:
        p = PropertyConfig(type="integer", index={"type": "range"})
        assert p.index is not None
        assert p.index.type == "range"


# ── TestConstraintConfig ──────────────────────────────────────────────────────


class TestConstraintConfig:
    @pytest.mark.parametrize("ctype", ["uniqueness", "not_null"])
    def test_valid_constraint_types(self, ctype: str) -> None:
        ConstraintConfig(type=ctype)  # must not raise

    def test_invalid_constraint_type_raises(self) -> None:
        with pytest.raises(ValidationError):
            ConstraintConfig(type="foreign_key")  # type: ignore[arg-type]


# ── TestNodeConfig ────────────────────────────────────────────────────────────


class TestNodeConfig:
    def test_valid_node(self) -> None:
        node = NodeConfig(**_minimal_node())
        assert node.label == "Person"

    def test_missing_key_property_raises(self) -> None:
        bad = _minimal_node()
        bad["key_property"] = "nonexistent"
        with pytest.raises(ValidationError, match="key_property"):
            NodeConfig(**bad)

    def test_empty_properties_raises(self) -> None:
        bad = _minimal_node()
        bad["properties"] = {}
        with pytest.raises(ValidationError, match="at least one property"):
            NodeConfig(**bad)

    def test_missing_topic_raises(self) -> None:
        bad = _minimal_node()
        del bad["topic"]
        with pytest.raises(ValidationError):
            NodeConfig(**bad)


# ── TestSourceTargetConfig ────────────────────────────────────────────────────


class TestSourceTargetConfig:
    def test_self_referencing_detected(self) -> None:
        st = SourceTargetConfig(source="Person", target="Person")
        assert st.is_self_referencing is True

    def test_non_self_referencing(self) -> None:
        st = SourceTargetConfig(source="Person", target="Company")
        assert st.is_self_referencing is False


# ── TestEdgeConfig ────────────────────────────────────────────────────────────


class TestEdgeConfig:
    def test_valid_edge(self) -> None:
        edge = EdgeConfig(**_minimal_edge())
        assert edge.type == "KNOWS"

    def test_empty_properties_raises(self) -> None:
        bad = _minimal_edge()
        bad["properties"] = {}
        with pytest.raises(ValidationError, match="at least one property"):
            EdgeConfig(**bad)

    def test_mix_and_batch_defaults(self) -> None:
        edge = EdgeConfig(**_minimal_edge())
        assert edge.mix_and_batch.batch_size == 1000

    def test_self_referencing_edge_detectable(self) -> None:
        edge = EdgeConfig(**_minimal_edge("KNOWS"))
        assert edge.nodes.is_self_referencing is True


# ── TestRetryConfig ───────────────────────────────────────────────────────────


class TestRetryConfig:
    def test_valid_retry_config(self) -> None:
        r = RetryConfig(max_attempts=3, base_delay_seconds=1.0, max_delay_seconds=60.0)
        assert r.max_attempts == 3

    def test_cross_field_guard(self) -> None:
        """max_delay_seconds must be >= base_delay_seconds."""
        with pytest.raises(ValidationError):
            RetryConfig(base_delay_seconds=30.0, max_delay_seconds=1.0)


# ── TestGraphSchema ───────────────────────────────────────────────────────────


class TestGraphSchema:
    def test_valid_schema_parses(self) -> None:
        schema = GraphSchema.model_validate(_valid_schema_dict())
        assert len(schema.nodes) == 2
        assert len(schema.edges) == 2

    def test_duplicate_node_labels_raises(self) -> None:
        data = _valid_schema_dict()
        data["nodes"].append(_minimal_node("Person", "pid2"))  # duplicate label
        with pytest.raises(ValidationError, match="Duplicate node labels"):
            GraphSchema.model_validate(data)

    def test_duplicate_edge_types_raises(self) -> None:
        data = _valid_schema_dict()
        data["edges"].append(_minimal_edge("KNOWS"))  # duplicate type
        with pytest.raises(ValidationError, match="Duplicate edge types"):
            GraphSchema.model_validate(data)

    def test_loading_config_defaults(self) -> None:
        schema = GraphSchema.model_validate(_valid_schema_dict())
        assert schema.loading.mode == "stream"
        assert schema.loading.consumer_group_id == "graph-loader"


# ── TestSchemaLoader ──────────────────────────────────────────────────────────
# All tests in this class use load_schema() exclusively.
# GraphSchema is NOT imported here — load_schema() is the only authorised path.


class TestSchemaLoader:
    def test_loads_canonical_yaml(self) -> None:
        schema = load_schema(_PROJECT_ROOT / "config" / "graph_schema.yaml")
        assert len(schema.nodes) == 2
        assert len(schema.edges) == 2

    def test_missing_file_raises_schema_load_error(self, tmp_path: Path) -> None:
        with pytest.raises(SchemaLoadError, match="not found"):
            load_schema(tmp_path / "nonexistent.yaml")

    def test_invalid_yaml_raises_schema_load_error(self, tmp_path: Path) -> None:
        bad_yaml = tmp_path / "bad.yaml"
        # "key: [\n  - unclosed\n" reliably triggers yaml.ScannerError in PyYAML 6.x
        bad_yaml.write_text("key: [\n  - unclosed\n", encoding="utf-8")
        with pytest.raises(SchemaLoadError, match="parse") as exc_info:
            load_schema(bad_yaml)
        assert isinstance(exc_info.value.__cause__, yaml.YAMLError)

    def test_pydantic_failure_raises_schema_load_error(self, tmp_path: Path) -> None:
        bad_schema = tmp_path / "bad_schema.yaml"
        bad_schema.write_text(
            yaml.dump({
                "nodes": [{"label": "X", "topic": "t", "key_property": "MISSING",
                           "properties": {"id": {"type": "string"}}}],
                "edges": [],
            }),
            encoding="utf-8",
        )
        with pytest.raises(SchemaLoadError, match="validation failed") as exc_info:
            load_schema(bad_schema)
        assert isinstance(exc_info.value.__cause__, ValidationError)

    def test_non_mapping_yaml_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "list.yaml"
        bad.write_text("- item1\n- item2\n", encoding="utf-8")
        with pytest.raises(SchemaLoadError, match="mapping"):
            load_schema(bad)
