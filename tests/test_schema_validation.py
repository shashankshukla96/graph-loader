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

import copy
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from src.models.schema import (
    BucketLeaseStreamConfig,
    ConstraintConfig,
    DeadLetterConfig,
    EdgeConfig,
    EdgeExecutionConfig,
    GraphSchema,
    LoadingConfig,
    NodeConfig,
    PropertyConfig,
    RetryConfig,
    RetryConsumerConfig,
    SourceTargetConfig,
    CoordinationConfig,
    parked_pair_index_topic_name,
    parked_topic_name,
)
from src.loader.dead_letter import dead_letter_topic_name
from src.utils.schema_loader import SchemaLoadError, load_schema

# ── Project root — anchors path-dependent tests to the repo root ──────────────
# Follows the same convention as tests/test_dev_environment.py.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_coordination_defaults_and_validation() -> None:
    assert CoordinationConfig().bucket_count == 64
    with pytest.raises(ValidationError):
        CoordinationConfig().bucket_count = 8
    with pytest.raises(ValidationError):
        CoordinationConfig(bucket_count=True)
    with pytest.raises(ValidationError):
        CoordinationConfig(topic=" ")
    with pytest.raises(ValidationError):
        CoordinationConfig(slot_duration_ms=20, lease_timeout_ms=10)


@pytest.mark.parametrize("field,value", [
    ("bucket_count", True), ("slot_duration_ms", True), ("lease_timeout_ms", True),
    ("bucket_count", 0), ("bucket_count", -1), ("bucket_count", 4097),
    ("slot_duration_ms", 0), ("slot_duration_ms", -1), ("slot_duration_ms", 3_600_001),
    ("lease_timeout_ms", 0), ("lease_timeout_ms", -1), ("lease_timeout_ms", 3_600_001),
])
def test_coordination_integer_bounds(field, value) -> None:
    with pytest.raises(ValidationError):
        CoordinationConfig(**{field: value})


def test_loading_coordination_defaults_and_custom_yaml(tmp_path: Path) -> None:
    data = _valid_schema_dict()
    assert GraphSchema.model_validate(data).loading.coordination.topic == "graph.loader.coordination"
    data.setdefault("loading", {})["coordination"] = {"topic": "clock-x", "bucket_count": 8, "slot_duration_ms": 10, "lease_timeout_ms": 20}
    path = tmp_path / "clock.yaml"; path.write_text(yaml.safe_dump(data), encoding="utf-8")
    loaded = load_schema(path)
    assert loaded.loading.coordination.bucket_count == 8
    data["loading"]["coordination"]["bucket_count"] = 0
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(SchemaLoadError):
        load_schema(path)


def test_bucket_lease_stream_config_defaults_and_validation() -> None:
    """P5-S1 Story 1: BucketLeaseStreamConfig defaults, mirroring CoordinationConfig's tests."""
    config = BucketLeaseStreamConfig()
    assert config.health_check_interval_ms == 2_000
    assert config.max_consecutive_restarts == 5
    assert config.restart_backoff_base_ms == 500
    assert config.restart_backoff_max_ms == 30_000
    # P5-S1 Story 7, review finding B7-1: bounded by default (60s), not
    # unbounded -- streaming's poll thread blocks synchronously on lease
    # reconnect in this slice, so an unbounded default risked exceeding
    # max.poll.interval.ms. 0 stays available as an explicit override.
    assert config.reconnect_timeout_ms == 60_000
    assert config.tx_timeout_ms == 30_000
    assert config.expected_chunk_commit_ms == 1_000
    assert config.restart_quiet_margin_ms == 5_000
    assert config.restart_quiet_period_ms == 35_000
    with pytest.raises(ValidationError):
        config.max_consecutive_restarts = 1
    with pytest.raises(ValidationError):
        BucketLeaseStreamConfig(max_consecutive_restarts=True)
    with pytest.raises(ValidationError):
        BucketLeaseStreamConfig(restart_backoff_base_ms=1_000, restart_backoff_max_ms=500)
    with pytest.raises(ValidationError):
        # P5-Q1 fencing amendment: T must be >= 10x expected_chunk_commit_ms.
        BucketLeaseStreamConfig(tx_timeout_ms=1_000, expected_chunk_commit_ms=1_000)
    # Exactly 10x is the boundary and must be accepted.
    BucketLeaseStreamConfig(tx_timeout_ms=10_000, expected_chunk_commit_ms=1_000)


@pytest.mark.parametrize("field,value", [
    ("health_check_interval_ms", True), ("max_consecutive_restarts", True),
    ("restart_backoff_base_ms", True), ("restart_backoff_max_ms", True),
    ("reconnect_timeout_ms", True), ("tx_timeout_ms", True),
    ("expected_chunk_commit_ms", True), ("restart_quiet_margin_ms", True),
    ("health_check_interval_ms", 0), ("health_check_interval_ms", 60_001),
    ("max_consecutive_restarts", 0), ("max_consecutive_restarts", 101),
    ("reconnect_timeout_ms", -1),
    # S1-4: margin must be >= Neo4j's default transaction-monitor check
    # interval (2s), so Q cannot elapse before a timed-out transaction is
    # noticed and terminated.
    ("restart_quiet_margin_ms", 1_999),
])
def test_bucket_lease_stream_config_integer_bounds(field, value) -> None:
    with pytest.raises(ValidationError):
        BucketLeaseStreamConfig(**{field: value})


def test_loading_bucket_lease_defaults_and_custom_yaml(tmp_path: Path) -> None:
    data = _valid_schema_dict()
    assert GraphSchema.model_validate(data).loading.bucket_lease.tx_timeout_ms == 30_000
    data.setdefault("loading", {})["bucket_lease"] = {
        "health_check_interval_ms": 500, "max_consecutive_restarts": 3,
        "restart_backoff_base_ms": 100, "restart_backoff_max_ms": 1_000,
        "tx_timeout_ms": 20_000, "expected_chunk_commit_ms": 500,
    }
    path = tmp_path / "bucket_lease.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    loaded = load_schema(path)
    assert loaded.loading.bucket_lease.max_consecutive_restarts == 3
    assert loaded.loading.bucket_lease.restart_quiet_period_ms == 20_000 + 5_000
    data["loading"]["bucket_lease"]["restart_backoff_max_ms"] = 1
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(SchemaLoadError):
        load_schema(path)


def test_bucket_lease_reconnect_budget_must_stay_under_max_poll_interval() -> None:
    """P5-S1 Story 7, review finding B7-1: reconnect_timeout_ms +
    tx_timeout_ms + restart_quiet_period_ms must stay comfortably under
    max_poll_interval_ms, since streaming's poll thread blocks
    synchronously on the reconnect and the write in this slice. Skipped
    when reconnect_timeout_ms == 0 (an explicit unbounded override)."""
    # Defaults are safe.
    LoadingConfig()
    # A reconnect budget that eats almost the whole poll interval is rejected.
    with pytest.raises(ValidationError):
        LoadingConfig(
            max_poll_interval_ms=100_000,
            bucket_lease=BucketLeaseStreamConfig(
                reconnect_timeout_ms=90_000, tx_timeout_ms=10_000, restart_quiet_margin_ms=2_000,
            ),
        )
    # The same oversized reconnect budget is accepted when explicitly
    # unbounded (0) -- the validator only guards the bounded case.
    LoadingConfig(
        max_poll_interval_ms=100_000,
        bucket_lease=BucketLeaseStreamConfig(
            reconnect_timeout_ms=0, tx_timeout_ms=10_000, restart_quiet_margin_ms=2_000,
        ),
    )


def test_slot_admission_capacity_defaults_bounds_and_field_immutability() -> None:
    config = LoadingConfig()
    assert (config.slot_buffer_max_records, config.slot_worker_queue_max_batches) == (10_000, 32)
    with pytest.raises(ValidationError):
        config.slot_buffer_max_records = 3
    with pytest.raises(ValidationError):
        config.slot_worker_queue_max_batches = 3
    for field, value in (
        ("slot_buffer_max_records", True), ("slot_buffer_max_records", 0),
        ("slot_buffer_max_records", 100_001), ("slot_worker_queue_max_batches", True),
        ("slot_worker_queue_max_batches", 0), ("slot_worker_queue_max_batches", 1_025),
    ):
        with pytest.raises(ValidationError):
            LoadingConfig(**{field: value})


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
        "source_key_property": "personId",
        "target_key_property": "personId",
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

    def test_optional_key_property_raises(self) -> None:
        bad = _minimal_node()
        bad["properties"]["pid"]["required"] = False
        with pytest.raises(ValidationError, match="must be required"):
            NodeConfig(**bad)

    @pytest.mark.parametrize(
        ("field", "value"),
        [("label", "Person;DELETE"), ("key_property", "person-id")],
    )
    def test_invalid_node_identifier_raises(self, field: str, value: str) -> None:
        bad = _minimal_node()
        bad[field] = value
        with pytest.raises(ValidationError, match="Cypher identifier"):
            NodeConfig(**bad)

    def test_invalid_node_property_identifier_raises(self) -> None:
        bad = _minimal_node()
        bad["properties"] = {"person-id": {"type": "string", "required": True}}
        bad["key_property"] = "person-id"
        with pytest.raises(ValidationError, match="Cypher identifier"):
            NodeConfig(**bad)


# ── TestDeadLetterConfig ──────────────────────────────────────────────────────


class TestDeadLetterConfig:
    def test_topic_optional_defers_to_default_naming(self) -> None:
        config = DeadLetterConfig()
        assert config.topic is None
        assert config.enabled is True
        assert dead_letter_topic_name("person-events", config) == "person-events.dlq"

    def test_explicit_topic_used_verbatim(self) -> None:
        config = DeadLetterConfig(topic="custom-dlq")
        assert dead_letter_topic_name("person-events", config) == "custom-dlq"

    def test_blank_topic_rejected_when_set(self) -> None:
        with pytest.raises(ValidationError, match="must not be blank"):
            DeadLetterConfig(topic="  ")

    def test_disabled_and_none_are_identical_to_the_naming_helper(self) -> None:
        assert dead_letter_topic_name("t", None) is None
        assert dead_letter_topic_name("t", DeadLetterConfig(enabled=False)) is None
        assert dead_letter_topic_name("t", DeadLetterConfig(topic="x", enabled=False)) is None

    def test_node_config_accepts_dead_letter_like_edge_config(self) -> None:
        node = NodeConfig(**_minimal_node(), dead_letter={"enabled": True})
        assert node.dead_letter is not None
        assert node.dead_letter.enabled is True
        assert node.dead_letter.topic is None

    def test_node_config_without_dead_letter_is_none_by_default(self) -> None:
        node = NodeConfig(**_minimal_node())
        assert node.dead_letter is None

    def test_route_missing_endpoints_in_bulk_defaults_false(self) -> None:
        config = DeadLetterConfig()
        assert config.route_missing_endpoints_in_bulk is False

    def test_route_missing_endpoints_in_bulk_explicit_true_with_enabled(self) -> None:
        config = DeadLetterConfig(enabled=True, route_missing_endpoints_in_bulk=True)
        assert config.route_missing_endpoints_in_bulk is True

    def test_route_missing_endpoints_in_bulk_requires_enabled(self) -> None:
        with pytest.raises(ValidationError, match="requires enabled=True"):
            DeadLetterConfig(enabled=False, route_missing_endpoints_in_bulk=True)

    def test_route_missing_endpoints_in_bulk_rejects_string_typo(self) -> None:
        with pytest.raises(ValidationError, match="must be a boolean"):
            DeadLetterConfig(enabled=True, route_missing_endpoints_in_bulk="true")

    def test_extra_forbid_still_rejects_unrelated_typo(self) -> None:
        with pytest.raises(ValidationError):
            DeadLetterConfig(enabled=True, route_missing_endpoint_in_bulk=True)

    def test_unknown_key_is_rejected_not_silently_ignored(self) -> None:
        # A typo like `enable` (missing the trailing 'd') must not silently
        # leave `enabled` at its True default -- that would flip DLQ routing
        # on when the author meant to turn it off.
        with pytest.raises(ValidationError, match="enable"):
            DeadLetterConfig(enable=False)

    # ── P5-S4 Story 1: max_retry_attempts / parked_topic ─────────────────

    def test_max_retry_attempts_defaults_to_five(self) -> None:
        assert DeadLetterConfig().max_retry_attempts == 5

    def test_max_retry_attempts_and_parked_topic_load_cleanly(self) -> None:
        config = DeadLetterConfig(enabled=True, max_retry_attempts=8, parked_topic="orders.parked")
        assert config.max_retry_attempts == 8
        assert config.parked_topic == "orders.parked"

    def test_max_retry_attempts_requires_enabled(self) -> None:
        with pytest.raises(ValidationError, match="requires enabled=True"):
            DeadLetterConfig(enabled=False, max_retry_attempts=8)

    def test_parked_topic_requires_enabled(self) -> None:
        with pytest.raises(ValidationError, match="requires enabled=True"):
            DeadLetterConfig(enabled=False, parked_topic="x")

    def test_max_retry_attempts_rejects_bool(self) -> None:
        with pytest.raises(ValidationError, match="must be an integer"):
            DeadLetterConfig(max_retry_attempts=True)  # type: ignore[arg-type]

    def test_max_retry_attempts_rejects_zero(self) -> None:
        with pytest.raises(ValidationError):
            DeadLetterConfig(max_retry_attempts=0)

    def test_parked_topic_rejects_blank_when_set(self) -> None:
        with pytest.raises(ValidationError, match="must not be blank"):
            DeadLetterConfig(parked_topic="   ")

    def test_parked_topic_name_defaults_from_resolved_dlq_topic(self) -> None:
        config = DeadLetterConfig(enabled=True)
        assert parked_topic_name("orders", config) == "orders.dlq.parked"

    def test_parked_topic_name_respects_explicit_dlq_topic_override(self) -> None:
        config = DeadLetterConfig(enabled=True, topic="custom-dlq")
        assert parked_topic_name("orders", config) == "custom-dlq.parked"

    def test_parked_topic_name_respects_explicit_parked_override(self) -> None:
        config = DeadLetterConfig(enabled=True, parked_topic="orders.parked")
        assert parked_topic_name("orders", config) == "orders.parked"

    def test_parked_topic_name_none_when_dlq_disabled(self) -> None:
        assert parked_topic_name("orders", None) is None
        assert parked_topic_name("orders", DeadLetterConfig(enabled=False)) is None

    def test_parked_pair_index_topic_name_derives_from_resolved_dlq(self) -> None:
        config = DeadLetterConfig(enabled=True)
        assert parked_pair_index_topic_name("orders", config) == "orders.dlq.parked-pairs"

    def test_parked_pair_index_topic_name_none_when_dlq_disabled(self) -> None:
        assert parked_pair_index_topic_name("orders", None) is None


# ── TestSourceTargetConfig ────────────────────────────────────────────────────


class TestSourceTargetConfig:
    def test_self_referencing_detected(self) -> None:
        st = SourceTargetConfig(source="Person", target="Person")
        assert st.is_self_referencing is True

    def test_non_self_referencing(self) -> None:
        st = SourceTargetConfig(source="Person", target="Company")
        assert st.is_self_referencing is False

    @pytest.mark.parametrize("field", ["source", "target"])
    def test_invalid_endpoint_identifier_raises(self, field: str) -> None:
        values = {"source": "Person", "target": "Company"}
        values[field] = "bad-label"
        with pytest.raises(ValidationError, match="Cypher identifier"):
            SourceTargetConfig(**values)


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
        assert edge.mix_and_batch.lane_count == 1
        assert edge.mix_and_batch.batch_size == 1000

    def test_edge_execution_defaults_and_validates_worker_count(self) -> None:
        edge = EdgeConfig(**_minimal_edge())
        assert edge.execution.mode == "python_apoc"
        assert edge.execution.worker_count == 1
        assert EdgeExecutionConfig(mode="native_disjoint", worker_count=2).worker_count == 2
        for value in (0, 65, True):
            with pytest.raises(ValidationError):
                EdgeExecutionConfig(worker_count=value)  # type: ignore[arg-type]

    @pytest.mark.parametrize("lane_count", [0, -1, 4097])
    def test_mix_and_batch_rejects_invalid_lane_count(self, lane_count: int) -> None:
        bad = _minimal_edge()
        bad["mix_and_batch"] = {"lane_count": lane_count}
        with pytest.raises(ValidationError, match="lane_count"):
            EdgeConfig(**bad)

    def test_self_referencing_edge_detectable(self) -> None:
        edge = EdgeConfig(**_minimal_edge("KNOWS"))
        assert edge.nodes.is_self_referencing is True

    def test_invalid_edge_type_identifier_raises(self) -> None:
        bad = _minimal_edge()
        bad["type"] = "KNOWS;DELETE"
        with pytest.raises(ValidationError, match="Cypher identifier"):
            EdgeConfig(**bad)

    def test_invalid_edge_property_identifier_raises(self) -> None:
        bad = _minimal_edge()
        bad["properties"] = {"since-date": {"type": "date", "required": True}}
        with pytest.raises(ValidationError, match="Cypher identifier"):
            EdgeConfig(**bad)

    def test_order_by_none_by_default(self) -> None:
        assert EdgeConfig(**_minimal_edge()).order_by is None

    def test_order_by_declared_property_loads_cleanly(self) -> None:
        data = _minimal_edge()
        data["properties"]["updated_at"] = {"type": "integer", "required": False}
        data["order_by"] = "updated_at"
        edge = EdgeConfig(**data)
        assert edge.order_by == "updated_at"

    def test_order_by_undeclared_property_raises(self) -> None:
        data = _minimal_edge()
        data["order_by"] = "updated_at"
        with pytest.raises(ValidationError, match="updated_at.*not one of"):
            EdgeConfig(**data)


# ── TestRetryConsumerConfig ───────────────────────────────────────────────────


class TestRetryConsumerConfig:
    def test_defaults(self) -> None:
        config = RetryConsumerConfig()
        assert config.poll_timeout_ms == 500
        assert config.backlog_check_interval_ms == 10_000
        assert config.due_batch_coalesce_ms == 200

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("poll_timeout_ms", 5),
            ("poll_timeout_ms", 60_001),
            ("backlog_check_interval_ms", 999),
            ("backlog_check_interval_ms", 300_001),
            ("due_batch_coalesce_ms", -1),
            ("due_batch_coalesce_ms", 10_001),
        ],
    )
    def test_out_of_bounds_rejected(self, field: str, value: int) -> None:
        with pytest.raises(ValidationError):
            RetryConsumerConfig(**{field: value})

    def test_bool_rejected(self) -> None:
        with pytest.raises(ValidationError, match="must be integers"):
            RetryConsumerConfig(poll_timeout_ms=True)  # type: ignore[arg-type]

    def test_frozen(self) -> None:
        config = RetryConsumerConfig()
        with pytest.raises(ValidationError):
            config.poll_timeout_ms = 1000  # type: ignore[misc]

    def test_loading_config_defaults_retry_consumer(self) -> None:
        assert LoadingConfig().retry_consumer == RetryConsumerConfig()


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

    @pytest.mark.parametrize(
        ("field", "value", "match"),
        [
            ("source", "Missing", "source label 'Missing'"),
            ("target", "Missing", "target label 'Missing'"),
        ],
    )
    def test_unknown_edge_endpoint_label_raises(self, field: str, value: str, match: str) -> None:
        data = _valid_schema_dict()
        data["edges"][0]["nodes"][field] = value
        with pytest.raises(ValidationError, match=match):
            GraphSchema.model_validate(data)

    @pytest.mark.parametrize(
        ("field", "value", "match"),
        [
            ("source_key_property", "otherId", "source_key_property 'otherId'"),
            ("target_key_property", "otherId", "target_key_property 'otherId'"),
        ],
    )
    def test_edge_endpoint_key_must_match_declared_node_key(
        self, field: str, value: str, match: str
    ) -> None:
        data = _valid_schema_dict()
        data["edges"][0][field] = value
        with pytest.raises(ValidationError, match=match):
            GraphSchema.model_validate(data)

    def test_validated_schema_binds_endpoint_property_configs(self) -> None:
        schema = GraphSchema.model_validate(_valid_schema_dict())
        edge = schema.edges[0]
        assert edge.source_key_config is not None
        assert edge.source_key_config.type == "string"
        assert edge.target_key_config is not None

    def test_no_dead_letter_blocks_is_a_regression_no_op(self) -> None:
        schema = GraphSchema.model_validate(_valid_schema_dict())
        assert all(node.dead_letter is None for node in schema.nodes)
        assert all(edge.dead_letter is None for edge in schema.edges)

    def test_default_named_dlq_topic_colliding_with_another_entitys_source_topic_raises(self) -> None:
        # Person's defaulted DLQ topic ("person-events.dlq") collides with a
        # third entity whose own *source* topic is that literal string.
        data = _valid_schema_dict()
        data["nodes"][0]["dead_letter"] = {"enabled": True}
        data["nodes"].append(_minimal_node("Extra", "extraId"))
        data["nodes"][2]["topic"] = "person-events.dlq"
        with pytest.raises(ValidationError, match="Dead-letter topic 'person-events.dlq'"):
            GraphSchema.model_validate(data)

    def test_explicit_dlq_topic_colliding_with_another_source_topic_raises(self) -> None:
        data = _valid_schema_dict()
        data["edges"][0]["dead_letter"] = {"topic": "companyid-events"}
        data["nodes"][1]["topic"] = "companyid-events"
        with pytest.raises(ValidationError, match="Dead-letter topic 'companyid-events'"):
            GraphSchema.model_validate(data)

    def test_two_entities_defaulting_to_the_same_dlq_topic_name_raises(self) -> None:
        data = _valid_schema_dict()
        data["nodes"][0]["topic"] = "shared-base"
        data["nodes"][0]["dead_letter"] = {"enabled": True}
        # WORKS_AT keeps its own distinct source topic, but its explicit DLQ
        # topic collides with Person's *defaulted* DLQ topic.
        data["edges"][0]["dead_letter"] = {"topic": "shared-base.dlq"}
        with pytest.raises(ValidationError, match="Dead-letter topic 'shared-base.dlq' is declared by both"):
            GraphSchema.model_validate(data)

    def test_entity_dlq_topic_equal_to_its_own_source_topic_raises(self) -> None:
        data = _valid_schema_dict()
        data["nodes"][0]["dead_letter"] = {"topic": "person-events"}
        with pytest.raises(ValidationError, match="Dead-letter topic 'person-events'"):
            GraphSchema.model_validate(data)

    def test_dlq_topic_colliding_with_coordination_topic_raises(self) -> None:
        data = _valid_schema_dict()
        data["nodes"][0]["dead_letter"] = {"topic": "graph.loader.coordination"}
        with pytest.raises(ValidationError, match="Dead-letter topic 'graph.loader.coordination'"):
            GraphSchema.model_validate(data)

    def test_dlq_topic_colliding_with_control_topic_raises(self) -> None:
        data = _valid_schema_dict()
        data["nodes"][0]["dead_letter"] = {"topic": "__graph_loader_control"}
        with pytest.raises(ValidationError, match="Dead-letter topic '__graph_loader_control'"):
            GraphSchema.model_validate(data)

    def test_explicit_dlq_topic_with_illegal_characters_raises(self) -> None:
        data = _valid_schema_dict()
        data["nodes"][0]["dead_letter"] = {"topic": "not a legal topic!"}
        with pytest.raises(ValidationError, match="not a legal Kafka topic name"):
            GraphSchema.model_validate(data)

    def test_defaulted_dlq_topic_over_249_chars_raises(self) -> None:
        data = _valid_schema_dict()
        long_topic = "p" * 248  # + ".dlq" pushes the default past 249 chars
        data["nodes"][0]["topic"] = long_topic
        data["nodes"][0]["dead_letter"] = {"enabled": True}
        with pytest.raises(ValidationError, match="not a legal Kafka topic name"):
            GraphSchema.model_validate(data)

    def test_own_source_topic_collision_message_names_it_explicitly(self) -> None:
        data = _valid_schema_dict()
        data["nodes"][0]["dead_letter"] = {"topic": "person-events"}
        with pytest.raises(ValidationError, match="collides with its own source topic"):
            GraphSchema.model_validate(data)

    # ── P5-S4 Story 1: parked_topic / parked-pairs collision set ──────────

    def test_two_edges_resolving_to_the_same_explicit_parked_topic_raises(self) -> None:
        data = _valid_schema_dict()
        data["edges"][0]["dead_letter"] = {"enabled": True, "parked_topic": "shared.parked"}
        data["edges"][1]["dead_letter"] = {"enabled": True, "parked_topic": "shared.parked"}
        with pytest.raises(ValidationError, match="Parking-lot topic 'shared.parked' is declared by both"):
            GraphSchema.model_validate(data)

    def test_parked_topic_colliding_with_another_source_topic_raises(self) -> None:
        data = _valid_schema_dict()
        data["edges"][0]["dead_letter"] = {"enabled": True}
        # WORKS_AT's defaulted parked topic is "works_at-events.dlq.parked";
        # give KNOWS's source topic that exact literal name.
        data["edges"][1]["topic"] = f"{data['edges'][0]['topic']}.dlq.parked"
        with pytest.raises(ValidationError, match="Parking-lot topic"):
            GraphSchema.model_validate(data)

    def test_loading_config_defaults(self) -> None:
        schema = GraphSchema.model_validate(_valid_schema_dict())
        assert schema.loading.mode == "stream"
        assert schema.loading.consumer_group_id == "graph-loader"
        assert schema.loading.unwind_batch_size == 500
        assert schema.loading.edge_unwind_batch_size is None
        assert schema.loading.flush_interval_ms == 1000
        assert schema.loading.retry_max_attempts == 3
        assert schema.loading.retry_base_delay_ms == 100
        assert schema.loading.retry_max_delay_ms == 5000
        assert schema.loading.rejection_log_path == "var/rejections/node-loader.jsonl"

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("unwind_batch_size", 0), ("edge_unwind_batch_size", 0), ("flush_interval_ms", 0),
            ("retry_max_attempts", 0), ("retry_base_delay_ms", 0),
            ("retry_max_delay_ms", 0), ("rejection_log_path", "  "),
            ("unwind_batch_size", -1), ("unwind_batch_size", 10_001),
            ("edge_unwind_batch_size", 1_000_001),
            ("flush_interval_ms", 60_001), ("retry_max_attempts", 11),
            ("retry_base_delay_ms", 60_001), ("retry_max_delay_ms", 300_001),
        ],
    )
    def test_loading_runtime_settings_reject_invalid_values(self, field, value) -> None:
        data = _valid_schema_dict()
        data.setdefault("loading", {})[field] = value
        with pytest.raises(ValidationError):
            GraphSchema.model_validate(data)

    def test_loading_runtime_settings_validate_retry_order_and_custom_values(self) -> None:
        data = _valid_schema_dict()
        data.setdefault("loading", {}).update({"retry_base_delay_ms": 1000, "retry_max_delay_ms": 100})
        with pytest.raises(ValidationError, match="retry_max_delay_ms"):
            GraphSchema.model_validate(data)
        data["loading"].update({"unwind_batch_size": 25, "edge_unwind_batch_size": 2000, "flush_interval_ms": 50, "retry_base_delay_ms": 10, "retry_max_delay_ms": 100, "rejection_log_path": "tmp/reject.jsonl"})
        schema = GraphSchema.model_validate(data)
        assert schema.loading.unwind_batch_size == 25
        assert schema.loading.edge_unwind_batch_size == 2000
        assert schema.loading.rejection_log_path == "tmp/reject.jsonl"


# ── TestSchemaLoader ──────────────────────────────────────────────────────────
# All tests in this class use load_schema() exclusively.
# GraphSchema is NOT imported here — load_schema() is the only authorised path.


class TestSchemaLoader:
    def test_loads_canonical_yaml(self) -> None:
        schema = load_schema(_PROJECT_ROOT / "config" / "graph_schema.yaml")
        assert len(schema.nodes) == 2
        assert len(schema.edges) == 2
        assert schema.loading.unwind_batch_size == 500
        assert schema.loading.flush_interval_ms == 1000
        assert schema.loading.retry_max_attempts == 3
        assert schema.loading.retry_base_delay_ms == 100
        assert schema.loading.retry_max_delay_ms == 5000
        assert schema.loading.rejection_log_path == "var/rejections/node-loader.jsonl"

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

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda data: data["nodes"][0].__setitem__("label", "bad-label"),
            lambda data: data["nodes"][0].__setitem__("key_property", "bad-key"),
            lambda data: data["nodes"][0].__setitem__(
                "properties", {"bad-key": {"type": "string", "required": True}}
            ),
            lambda data: data["edges"][0].__setitem__("type", "BAD-TYPE"),
            lambda data: data["edges"][0].__setitem__(
                "properties", {"bad-prop": {"type": "date", "required": True}}
            ),
            lambda data: data["edges"][0]["nodes"].__setitem__("source", "bad-label"),
            lambda data: data["edges"][0]["nodes"].__setitem__("target", "bad-label"),
        ],
    )
    def test_unsafe_identifier_from_yaml_raises_schema_load_error(self, tmp_path: Path, mutate) -> None:
        data = copy.deepcopy(_valid_schema_dict())
        mutate(data)
        path = tmp_path / "unsafe_identifier.yaml"
        path.write_text(yaml.dump(data), encoding="utf-8")

        with pytest.raises(SchemaLoadError, match="validation failed") as exc_info:
            load_schema(path)
        assert isinstance(exc_info.value.__cause__, ValidationError)


def test_heartbeat_interval_defaults_fast_and_must_be_below_session_timeout():
    from src.models.schema import LoadingConfig

    assert LoadingConfig().heartbeat_interval_ms == 500
    assert LoadingConfig(heartbeat_interval_ms=3_000).heartbeat_interval_ms == 3_000
    with pytest.raises(ValidationError, match="heartbeat_interval_ms"):
        LoadingConfig(session_timeout_ms=6_000, heartbeat_interval_ms=6_000)
