"""
src/models/schema.py
─────────────────────
Pydantic v2 models for the Neo4j Graph Loader YAML schema.

Every construct in ``config/graph_schema.yaml`` maps 1-to-1 to a model here.
Load the schema exclusively via ``src.utils.schema_loader.load_schema()`` —
do not import this module and call ``GraphSchema.model_validate()`` directly
from application code.
"""
from __future__ import annotations

import re
from typing import Literal, List, Optional

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator


CYPHER_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_cypher_identifier(value: str, field_name: str) -> str:
    """Return a Cypher-safe identifier or raise a field-specific error.

    Labels, relationship types, and property names are interpolated into
    Cypher by the DDL generator and later ingestion code.  Values must be
    constrained at the schema boundary because Cypher does not parameterize
    identifiers.
    """
    if not CYPHER_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"{field_name} must be a Cypher identifier matching "
            "^[A-Za-z_][A-Za-z0-9_]*$"
        )
    return value


# ── Property & constraint primitives ─────────────────────────────────────────


class ConstraintConfig(BaseModel):
    """One constraint declaration on a single property.

    Attributes:
        type: ``"uniqueness"`` generates ``CREATE CONSTRAINT … IS UNIQUE``;
              ``"not_null"`` generates ``CREATE CONSTRAINT … IS NOT NULL``.
    """

    type: Literal["uniqueness", "not_null"]


class IndexConfig(BaseModel):
    """One index declaration on a single property.

    Attributes:
        type: ``"range"`` generates ``CREATE RANGE INDEX …``;
              ``"text"`` generates ``CREATE TEXT INDEX …``.
    """

    type: Literal["range", "text"]


class PropertyConfig(BaseModel):
    """A single property definition on a node label or relationship type.

    Attributes:
        type: The Neo4j-compatible data type for this property.
        required: Whether the property must be present on every record.
            Informational only in Phase 1; enforced by constraints in Neo4j.
        constraint: Optional constraint to create for this property.
        index: Optional index to create for this property.
    """

    type: Literal["string", "integer", "float", "date", "datetime"]
    required: bool = False
    constraint: Optional[ConstraintConfig] = None
    index: Optional[IndexConfig] = None


# ── Node & Edge configs ───────────────────────────────────────────────────────


class NodeConfig(BaseModel):
    """Configuration for a single node label.

    Attributes:
        label: The Neo4j node label (e.g. ``"Person"``).
        topic: Kafka topic that carries records for this node type.
        key_property: Property used as the unique node identifier.
            Must be present as a key in ``properties``.
        properties: Mapping of property name → :class:`PropertyConfig`.
            Must contain at least one entry.
    """

    label: str
    topic: str
    key_property: str
    replicas: int = Field(default=1, ge=1)
    properties: dict[str, PropertyConfig] = Field(default_factory=dict)

    @field_validator("label", "key_property")
    @classmethod
    def cypher_identifier_fields(cls, value: str, info) -> str:
        """Validate node identifiers that are interpolated into Cypher."""
        return _validate_cypher_identifier(value, info.field_name)

    @field_validator("properties")
    @classmethod
    def property_keys_are_cypher_identifiers(
        cls, value: dict[str, PropertyConfig]
    ) -> dict[str, PropertyConfig]:
        """Validate every schema property key before Cypher generation."""
        for property_name in value:
            _validate_cypher_identifier(property_name, "properties")
        return value

    @model_validator(mode="after")
    def at_least_one_property(self) -> "NodeConfig":
        """Enforce that every node defines at least one property."""
        if not self.properties:
            raise ValueError(f"Node '{self.label}' must have at least one property.")
        return self

    @model_validator(mode="after")
    def key_property_must_exist(self) -> "NodeConfig":
        """Enforce that the key is declared and required for every record."""
        if self.key_property not in self.properties:
            raise ValueError(
                f"key_property '{self.key_property}' not found in "
                f"properties of node '{self.label}'"
            )
        if not self.properties[self.key_property].required:
            raise ValueError(
                f"key_property '{self.key_property}' for node '{self.label}' "
                "must be required"
            )
        return self


class SourceTargetConfig(BaseModel):
    """The source and target node labels for a relationship type.

    Attributes:
        source: Label of the source node (e.g. ``"Person"``).
        target: Label of the target node (e.g. ``"Company"``).
    """

    source: str
    target: str

    @field_validator("source", "target")
    @classmethod
    def cypher_identifier_fields(cls, value: str, info) -> str:
        """Validate endpoint label aliases before later edge Cypher uses them."""
        return _validate_cypher_identifier(value, info.field_name)

    @property
    def is_self_referencing(self) -> bool:
        """Return ``True`` when source and target labels are identical.

        Self-referencing edges (e.g. ``Person-KNOWS-Person``) must be loaded
        in isolation to prevent deadlocks — the loader uses this flag to
        schedule them separately.
        """
        return self.source == self.target


class MixAndBatchConfig(BaseModel):
    """Deadlock-prevention batching parameters for edge loading.

    The Mix-and-Batch technique splits a batch of edges into forward
    (``src_id < tgt_id``) and backward slots to eliminate circular
    lock dependencies between concurrent loaders.

    Attributes:
        lane_count: Number of deterministic directional source-hash lanes.
        batch_size: Number of edges per write batch. Must be > 0.
        forward_slot_ms: Duration in milliseconds for the forward-edge slot.
        backward_slot_ms: Duration in milliseconds for the backward-edge slot.
    """

    lane_count: int = Field(default=1, ge=1, le=4096)
    batch_size: int = Field(default=1000, gt=0)
    forward_slot_ms: int = Field(default=500, gt=0)
    backward_slot_ms: int = Field(default=500, gt=0)


class RetryConfig(BaseModel):
    """Exponential-backoff retry settings for failed writes.

    Attributes:
        max_attempts: Maximum number of delivery attempts (including the first).
        base_delay_seconds: Initial backoff delay in seconds.
        max_delay_seconds: Upper bound on backoff delay in seconds.
            Must be greater than or equal to ``base_delay_seconds``.
    """

    max_attempts: int = Field(default=3, ge=1)
    base_delay_seconds: float = Field(default=1.0, gt=0)
    max_delay_seconds: float = Field(default=60.0, gt=0)

    @model_validator(mode="after")
    def max_delay_exceeds_base(self) -> "RetryConfig":
        """Enforce that the backoff ceiling is not below the starting delay."""
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError(
                f"max_delay_seconds ({self.max_delay_seconds}) must be >= "
                f"base_delay_seconds ({self.base_delay_seconds})"
            )
        return self


class DeadLetterConfig(BaseModel):
    """Dead-letter queue routing for messages that exhaust all retries.

    Attributes:
        topic: Kafka topic name to publish failed messages to.
        enabled: When ``False``, failed messages are logged and dropped
            instead of being routed to the DLQ.
    """

    topic: str
    enabled: bool = True


class EdgeExecutionConfig(BaseModel):
    """Select relationship execution strategy and bounded concurrency."""
    mode: Literal["python_apoc", "native_disjoint"] = "python_apoc"
    worker_count: int = Field(default=1, ge=1, le=64)

    @field_validator("worker_count", mode="before")
    @classmethod
    def worker_count_is_not_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("worker_count must be an integer")
        return value


class CoordinationConfig(BaseModel):
    """Bounded run-scoped settings for the Phase 4 global batch clock."""

    model_config = ConfigDict(frozen=True)

    topic: str = "graph.loader.coordination"
    bucket_count: int = Field(default=64, ge=1, le=4096)
    slot_duration_ms: int = Field(default=60_000, ge=1, le=3_600_000)
    lease_timeout_ms: int = Field(default=120_000, ge=1, le=3_600_000)

    @field_validator("topic")
    @classmethod
    def topic_is_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("coordination topic must not be blank")
        return value

    @field_validator("bucket_count", "slot_duration_ms", "lease_timeout_ms", mode="before")
    @classmethod
    def integer_fields_are_not_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("coordination values must be integers")
        return value

    @model_validator(mode="after")
    def lease_covers_slot(self) -> "CoordinationConfig":
        if self.lease_timeout_ms <= self.slot_duration_ms:
            raise ValueError("lease_timeout_ms must be > slot_duration_ms")
        return self


class LoadingConfig(BaseModel):
    """Global loading behaviour shared across all loaders.

    Attributes:
        mode: ``"bulk"`` for one-shot historical loads;
              ``"stream"`` for continuous Kafka consumption.
        consumer_group_id: Kafka consumer group identifier.
        max_poll_interval_ms: Maximum time between Kafka ``poll()`` calls
            before the broker considers the consumer dead (milliseconds).
        session_timeout_ms: Kafka session timeout (milliseconds).
        unwind_batch_size: Maximum records in one node UNWIND transaction (default 500).
        flush_interval_ms: Idle partial-batch flush interval in milliseconds (default 1000).
        retry_max_attempts: Node-write attempts including the first (default 3).
        retry_base_delay_ms: Initial node-write retry delay in milliseconds (default 100).
        retry_max_delay_ms: Maximum node-write retry delay in milliseconds (default 5000).
        rejection_log_path: Fsync'd interim malformed-record sink; Phase 5 replaces it with DLQ.
    """

    mode: Literal["bulk", "stream"] = "stream"
    consumer_group_id: str = "graph-loader"
    max_poll_interval_ms: int = Field(default=300_000, gt=0)
    session_timeout_ms: int = Field(default=45_000, gt=0)
    unwind_batch_size: int = Field(default=500, gt=0, le=10_000)
    flush_interval_ms: int = Field(default=1_000, gt=0, le=60_000)
    retry_max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_ms: int = Field(default=100, gt=0, le=60_000)
    retry_max_delay_ms: int = Field(default=5_000, gt=0, le=300_000)
    rejection_log_path: str = "var/rejections/node-loader.jsonl"
    coordination: CoordinationConfig = Field(default_factory=CoordinationConfig)
    slot_buffer_max_records: int = Field(default=10_000, ge=1, le=100_000, frozen=True)
    slot_worker_queue_max_batches: int = Field(default=32, ge=1, le=1_024, frozen=True)

    @field_validator("rejection_log_path")
    @classmethod
    def rejection_log_path_is_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("rejection_log_path must not be blank")
        return value

    @field_validator("slot_buffer_max_records", "slot_worker_queue_max_batches", mode="before")
    @classmethod
    def slot_capacity_fields_are_not_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("slot capacity values must be integers")
        return value

    @model_validator(mode="after")
    def node_retry_delay_is_ordered(self) -> "LoadingConfig":
        if self.retry_max_delay_ms < self.retry_base_delay_ms:
            raise ValueError("retry_max_delay_ms must be >= retry_base_delay_ms")
        return self


class EdgeConfig(BaseModel):
    """Configuration for a single relationship type.

    Attributes:
        type: The Neo4j relationship type (e.g. ``"WORKS_AT"``).
        topic: Kafka topic that carries records for this relationship type.
        nodes: Source and target node label configuration.
        properties: Mapping of property name → :class:`PropertyConfig`.
            Must contain at least one entry.
        mix_and_batch: Batching and slot-timing parameters.
        retry: Retry and backoff configuration.
        dead_letter: Optional DLQ routing. ``None`` disables DLQ.
    """

    type: str
    topic: str
    nodes: SourceTargetConfig
    source_key_property: str
    target_key_property: str
    replicas: int = Field(default=1, ge=1, le=64)
    properties: dict[str, PropertyConfig] = Field(default_factory=dict)
    mix_and_batch: MixAndBatchConfig = Field(default_factory=MixAndBatchConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    dead_letter: Optional[DeadLetterConfig] = None
    execution: EdgeExecutionConfig = Field(default_factory=EdgeExecutionConfig)
    _source_key_config: PropertyConfig | None = PrivateAttr(default=None)
    _target_key_config: PropertyConfig | None = PrivateAttr(default=None)

    @field_validator("type", "source_key_property", "target_key_property")
    @classmethod
    def edge_type_is_cypher_identifier(cls, value: str, info) -> str:
        """Validate relationship identifiers interpolated into Cypher."""
        return _validate_cypher_identifier(value, info.field_name)

    @field_validator("replicas", mode="before")
    @classmethod
    def replicas_are_not_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("replicas must be an integer")
        return value

    def bind_endpoint_properties(
        self,
        source_property: PropertyConfig,
        target_property: PropertyConfig,
    ) -> None:
        """Bind endpoint property definitions after complete graph validation.

        Only :class:`GraphSchema` may call this method. The bindings are
        internal and non-serialized; callers must use a config obtained from
        ``load_schema()`` before normalizing relationship events.
        """
        self._source_key_config = source_property
        self._target_key_config = target_property

    @property
    def source_key_config(self) -> PropertyConfig | None:
        """Return the graph-validated source endpoint definition, if bound."""
        return self._source_key_config

    @property
    def target_key_config(self) -> PropertyConfig | None:
        """Return the graph-validated target endpoint definition, if bound."""
        return self._target_key_config

    @field_validator("properties")
    @classmethod
    def property_keys_are_cypher_identifiers(
        cls, value: dict[str, PropertyConfig]
    ) -> dict[str, PropertyConfig]:
        """Validate relationship property names before DDL generation."""
        for property_name in value:
            _validate_cypher_identifier(property_name, "properties")
        return value

    @model_validator(mode="after")
    def at_least_one_property(self) -> "EdgeConfig":
        """Enforce that every edge defines at least one property."""
        if not self.properties:
            raise ValueError(f"Edge '{self.type}' must have at least one property.")
        return self


# ── Top-level schema ──────────────────────────────────────────────────────────


class GraphSchema(BaseModel):
    """Root model — the complete graph schema loaded from YAML.

    Attributes:
        nodes: List of node label configurations. Labels must be unique.
        edges: List of relationship type configurations. Types must be unique.
        loading: Global loading behaviour defaults.
    """

    nodes: List[NodeConfig]
    edges: List[EdgeConfig]
    loading: LoadingConfig = Field(default_factory=LoadingConfig)

    @model_validator(mode="after")
    def validate_edge_endpoints(self) -> "GraphSchema":
        """Enforce unique types and bind each edge to declared required keys."""
        labels = [n.label for n in self.nodes]
        if len(labels) != len(set(labels)):
            raise ValueError("Duplicate node labels detected in schema.")
        types = [e.type for e in self.edges]
        if len(types) != len(set(types)):
            raise ValueError("Duplicate edge types detected in schema.")

        nodes_by_label = {node.label: node for node in self.nodes}
        for edge in self.edges:
            for endpoint_name, label, key_property in (
                ("source", edge.nodes.source, edge.source_key_property),
                ("target", edge.nodes.target, edge.target_key_property),
            ):
                node = nodes_by_label.get(label)
                if node is None:
                    raise ValueError(
                        f"Edge '{edge.type}' {endpoint_name} label '{label}' "
                        "does not name a declared node"
                    )
                if key_property != node.key_property:
                    raise ValueError(
                        f"Edge '{edge.type}' {endpoint_name}_key_property "
                        f"'{key_property}' must match node '{label}' key_property "
                        f"'{node.key_property}'"
                    )
                property_config = node.properties.get(key_property)
                if property_config is None or not property_config.required:
                    raise ValueError(
                        f"Edge '{edge.type}' {endpoint_name}_key_property "
                        f"'{key_property}' must be a required property of node '{label}'"
                    )

            source_node = nodes_by_label[edge.nodes.source]
            target_node = nodes_by_label[edge.nodes.target]
            edge.bind_endpoint_properties(
                source_node.properties[edge.source_key_property],
                target_node.properties[edge.target_key_property],
            )
        return self
