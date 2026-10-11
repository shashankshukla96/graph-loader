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

import logging
import re
import threading
import weakref
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, List, Optional

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator

from src.loader.control import CONTROL_TOPIC


logger = logging.getLogger(__name__)

_WARNED_ONCE: set = set()
_WARNED_ONCE_LOCK = threading.Lock()


def _warn_once(key: tuple, message: str, *args) -> None:
    """Log ``message`` as a WARNING once per process for ``key`` (final follow-ups nit: the schema is
    validated many times per process -- loads, model copies, views -- and the same advice repeated)."""
    with _WARNED_ONCE_LOCK:
        if key in _WARNED_ONCE:
            return
        _WARNED_ONCE.add(key)
    logger.warning(message, *args)


CYPHER_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Kafka's own topic-name rule (letters, digits, '.', '_', '-'; 1-249 chars).
# Applied here only to a computed dead-letter topic name (explicit or
# defaulted) so a naming problem fails at load_schema() time rather than at
# topic-creation time against a live broker. Source topics aren't checked
# against this today (pre-existing, out of scope for this slice).
KAFKA_TOPIC_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


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
            A record missing it, or with a null value, is rejected by record
            validation.
        constraint: Optional constraint to create for this property.
        index: Optional index to create for this property.
    """

    type: Literal["string", "integer", "float", "date", "datetime"]
    required: bool = False
    constraint: Optional[ConstraintConfig] = None
    index: Optional[IndexConfig] = None


# ── Node & Edge configs ───────────────────────────────────────────────────────


_DEFAULT_MAX_RETRY_ATTEMPTS = 5


class DeadLetterConfig(BaseModel):
    """Dead-letter queue routing for malformed or otherwise undeliverable records.

    Attributes:
        topic: Explicit Kafka DLQ topic name. When ``None``, the effective
            topic defaults to ``f"{entity_topic}.dlq"`` (see
            :func:`dead_letter_topic_name` below). For a relationship type
            with two or more topics the entity name is the type name, so the
            default is ``f"{type}.dlq"`` (one DLQ per type; see
            :func:`edge_dead_letter_topic_name`).
        enabled: When ``False``, malformed records keep using the file-based
            rejection sink (today's behavior) instead of being routed to a
            DLQ topic. ``None``/absent and ``enabled=False`` are equivalent.
        route_missing_endpoints_in_bulk: Bulk-only opt-in (P5-Q3, P5-S3).
            Streaming always routes a missing/ambiguous relationship endpoint
            to this entity's DLQ when ``enabled=True``; classic and lease
            *bulk* keep today's fail-closed ``MissingRelationshipEndpointError``
            for the whole batch unless this is explicitly set ``True``.
            Planned mode (``--edge-plan``) never reads this field — it stays
            fail-closed unconditionally, per P5-Q3. Relationship-only: there
            is no equivalent on ``NodeConfig.dead_letter``: the model is
            shared with nodes, so ``NodeConfig`` rejects it (and
            ``max_retry_attempts``/``parked_topic``) when set explicitly.
        max_retry_attempts: P5-S4. Relationship-only (mirrors
            ``route_missing_endpoints_in_bulk``'s own scoping): how many times
            the retry consumer re-attempts a DLQ'd missing/ambiguous-endpoint
            row before parking it. Distinct from ``RetryConfig.max_attempts``,
            which bounds a *single write call*'s in-process Neo4j
            ``TransientError`` retries (milliseconds), not this slice's
            across-backoff-cycles bound (potentially hours).
        parked_topic: Explicit parking-lot topic override. When ``None``, the
            effective topic defaults to ``f"{resolved_dlq_topic}.parked"``
            (see :func:`parked_topic_name` below) — derived from the
            *resolved* DLQ topic, not the raw entity topic, so an overridden
            ``topic`` above is respected.
    """

    model_config = ConfigDict(extra="forbid")

    topic: Optional[str] = None
    enabled: bool = True
    route_missing_endpoints_in_bulk: bool = False
    max_retry_attempts: int = Field(default=_DEFAULT_MAX_RETRY_ATTEMPTS, ge=1, le=1000)
    parked_topic: Optional[str] = None

    @field_validator("topic")
    @classmethod
    def topic_is_not_blank_when_set(cls, value: Optional[str]) -> Optional[str]:
        """Allow an unset topic (defer to default naming) but reject blank."""
        if value is not None and not value.strip():
            raise ValueError("dead_letter topic must not be blank when set")
        return value

    @field_validator("parked_topic")
    @classmethod
    def parked_topic_is_not_blank_when_set(cls, value: Optional[str]) -> Optional[str]:
        """Allow an unset parked_topic (defer to default naming) but reject blank."""
        if value is not None and not value.strip():
            raise ValueError("dead_letter parked_topic must not be blank when set")
        return value

    @field_validator("route_missing_endpoints_in_bulk", mode="before")
    @classmethod
    def _route_missing_endpoints_in_bulk_is_not_a_string(cls, value: object) -> object:
        """Reject a YAML string ``"true"``/``"false"`` rather than coerce it.

        Mirrors every other boolean-toggle validator in this module: pydantic
        would otherwise silently coerce a plausible operator typo (quoting a
        boolean) into the corresponding ``bool``.
        """
        if isinstance(value, str):
            raise ValueError("route_missing_endpoints_in_bulk must be a boolean")
        return value

    @field_validator("max_retry_attempts", mode="before")
    @classmethod
    def _reject_bool_max_retry_attempts(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("max_retry_attempts must be an integer")
        return value

    @model_validator(mode="after")
    def _bulk_opt_in_requires_enabled(self) -> "DeadLetterConfig":
        if self.route_missing_endpoints_in_bulk and not self.enabled:
            raise ValueError(
                "route_missing_endpoints_in_bulk=True requires enabled=True "
                "(there is no DLQ topic to route to otherwise)"
            )
        return self

    @model_validator(mode="after")
    def _retry_consumer_fields_require_enabled(self) -> "DeadLetterConfig":
        # P5-S4 Story 1: max_retry_attempts/parked_topic are meaningless
        # without a DLQ topic to retry from, mirroring the identical
        # enabled=False rejection already applied to
        # route_missing_endpoints_in_bulk above.
        if not self.enabled:
            # Differs-from-default, not merely "set": model_dump() emits the
            # default, and the dump must re-validate.
            if self.max_retry_attempts != _DEFAULT_MAX_RETRY_ATTEMPTS:
                raise ValueError(
                    "max_retry_attempts requires enabled=True "
                    "(there is no DLQ topic to retry from otherwise)"
                )
            if self.parked_topic is not None:
                raise ValueError(
                    "parked_topic requires enabled=True "
                    "(there is no DLQ topic to park records from otherwise)"
                )
        return self


def dead_letter_topic_name(
    entity_topic: str, dead_letter: Optional[DeadLetterConfig]
) -> Optional[str]:
    """Return the effective DLQ topic, or ``None`` when DLQ routing is off.

    ``dead_letter is None`` and ``enabled=False`` both mean "use the file-sink
    fallback" and must return the identical ``None`` so every downstream
    caller (schema validation, the loaders, the CLI, the replay command)
    treats them the same way. Lives here (not in ``src/loader/dead_letter.py``)
    so this module never has to import the loader package: the loader module
    imports and re-exports this function instead, keeping the dependency
    direction model -> loader, never the reverse.
    """
    if dead_letter is None or not dead_letter.enabled:
        return None
    return dead_letter.topic or f"{entity_topic}.dlq"


def parked_topic_name(
    entity_topic: str, dead_letter: Optional[DeadLetterConfig]
) -> Optional[str]:
    """Return the effective parking-lot topic, or ``None`` when DLQ routing is off.

    Derived from the *resolved* DLQ topic (P5-S4 Decisions §9), not the raw
    entity topic, so an overridden ``dead_letter.topic`` is respected. Mirrors
    :func:`dead_letter_topic_name`'s placement and one-directional dependency
    (this module never imports the loader package).
    """
    resolved_dlq = dead_letter_topic_name(entity_topic, dead_letter)
    if resolved_dlq is None:
        return None
    if dead_letter.parked_topic is not None:
        return dead_letter.parked_topic
    return f"{resolved_dlq}.parked"


def parked_pair_index_topic_name(
    entity_topic: str, dead_letter: Optional[DeadLetterConfig]
) -> Optional[str]:
    """Return the internal parked-pair compacted index topic, or ``None``.

    ``f"{resolved_dlq_topic}.parked-pairs"`` (P5-S4 Decisions §6). No override
    field exists for this topic — it is a purely internal accounting
    structure, never read by an operator or ``replay-dlq``.
    """
    resolved_dlq = dead_letter_topic_name(entity_topic, dead_letter)
    if resolved_dlq is None:
        return None
    return f"{resolved_dlq}.parked-pairs"


def edge_dead_letter_topic_name(edge: "EdgeConfig") -> Optional[str]:
    """Effective DLQ topic for a relationship type (one DLQ per type).

    Single-topic types are unchanged (``<topic>.dlq``); types with two or more
    topics default to ``<type>.dlq``. ``dead_letter.topic`` overrides both.
    """
    return dead_letter_topic_name(edge.dead_letter_entity, edge.dead_letter)


def edge_parked_topic_name(edge: "EdgeConfig") -> Optional[str]:
    """Effective parking-lot topic for a relationship type, or ``None``."""
    return parked_topic_name(edge.dead_letter_entity, edge.dead_letter)


def edge_parked_pair_index_topic_name(edge: "EdgeConfig") -> Optional[str]:
    """Effective parked-pair index topic for a relationship type, or ``None``."""
    return parked_pair_index_topic_name(edge.dead_letter_entity, edge.dead_letter)


_EDGE_ONLY_DEAD_LETTER_FIELDS = frozenset(
    {"max_retry_attempts", "parked_topic", "route_missing_endpoints_in_bulk"}
)


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
    dead_letter: Optional[DeadLetterConfig] = None

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
    def _dead_letter_has_no_edge_only_fields(self) -> "NodeConfig":
        """Reject relationship-only DLQ fields on a node (N1).

        ``DeadLetterConfig`` is shared with edges; these fields would
        otherwise load silently and do nothing for a node.
        """
        if self.dead_letter is not None:
            # Set AND non-default: a full model_dump() carries the defaults
            # and must still re-validate.
            defaults = DeadLetterConfig.model_fields
            bad = sorted(
                name
                for name in self.dead_letter.model_fields_set & _EDGE_ONLY_DEAD_LETTER_FIELDS
                if getattr(self.dead_letter, name) != defaults[name].default
            )
            if bad:
                raise ValueError(
                    f"Node '{self.label}': dead_letter field(s) {bad} apply to "
                    "relationship types only and have no effect on nodes; remove them"
                )
        return self

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

        Self-referencing edges (e.g. ``Person-KNOWS-Person``) are isolated
        in classic bulk with ``--bulk-edge-overlap labels`` and on the clock
        path; planned, lease and stream modes schedule them together with
        the other types.
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

    model_config = ConfigDict(extra="forbid")

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

    model_config = ConfigDict(extra="forbid")

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


class EdgeExecutionConfig(BaseModel):
    """Select relationship execution strategy and bounded concurrency."""

    model_config = ConfigDict(extra="forbid")
    mode: Literal["python_apoc", "native_disjoint"] = "python_apoc"
    worker_count: int = Field(default=1, ge=1, le=64)

    @field_validator("worker_count", mode="before")
    @classmethod
    def worker_count_is_not_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("worker_count must be an integer")
        return value


class CoordinationConfig(BaseModel):
    """Bounded run-scoped settings for the deprecated Phase 4 global batch clock (``--stream-clock-rotation`` only)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

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


class BucketLeaseStreamConfig(BaseModel):
    """Tuning for the bucket-lease coordinator and the P5-Q1 fencing amendment.

    ``health_check_interval_ms``/``max_consecutive_restarts``/
    ``restart_count_reset_ms``/
    ``restart_backoff_base_ms``/``restart_backoff_max_ms``/
    ``reconnect_timeout_ms`` are streaming-only (Story 5's supervisor and
    Story 3's client reconnect; bulk mode's coordinator is unsupervised and
    keeps its own CLI-only settings). ``tx_timeout_ms``/
    ``expected_chunk_commit_ms``/``restart_quiet_margin_ms`` are NOT
    streaming-only: per the P5-Q1 fencing amendment
    (``plan/v5-v6-orchestration.md``), every leased relationship write
    (streaming or bulk lease mode) is opened with a fenced BEGIN whose Neo4j
    timeout is at most ``tx_timeout_ms`` (``T``) minus 4 s, and planned
    writes use the flat ``T``, so Neo4j itself terminates any transaction
    older than its lease allows (see ``src/loader/fenced_tx.py``). Lease modes
    need ``T >= 6000`` (checked by the CLI). This block lives
    on ``LoadingConfig`` rather than only under a "stream" section because
    both bulk and edge containers already load the whole schema, so
    ``loading.bucket_lease.tx_timeout_ms`` is readable from every writer,
    not just streaming ones.

    A lease server (re)starting with a new generation waits a quiet period
    ``Q = tx_timeout_ms + restart_quiet_margin_ms`` before its first grant,
    so every old-generation transaction has committed or been terminated by
    Neo4j by then -- this is why margin must exceed Neo4j's own transaction
    monitor granularity (``db.transaction.monitor.check.interval``, default
    2s): a transaction Neo4j only *notices* is over ``T`` up to one check
    interval later could otherwise still be inside ``COMMIT`` when ``Q``
    elapses. ``PING`` stays a real-time optimisation that shortens the
    common-case abort window; the quiet period is the correctness
    guarantee, not ``PING``.

    Distinct from ``CoordinationConfig`` (the deprecated Phase 4 clock's
    slot lease).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")
    health_check_interval_ms: int = Field(default=2_000, ge=100, le=60_000)
    max_consecutive_restarts: int = Field(default=5, ge=1, le=100)
    # Streaming-only: a restarted lease server that stays up this long after
    # its quiet period Q resets the consecutive-restart counter (and the
    # backoff), so deaths far apart never add up to the cap of a long-lived fleet.
    restart_count_reset_ms: int = Field(default=600_000, ge=1_000, le=86_400_000)
    restart_backoff_base_ms: int = Field(default=500, ge=1, le=60_000)
    restart_backoff_max_ms: int = Field(default=30_000, ge=1, le=600_000)
    # P5-S1 Story 7, review finding B7-1: streaming's poll thread blocks
    # synchronously on lease acquisition and the write (no dual-thread
    # worker model in this slice -- an accepted, documented DoD
    # deviation), so an unbounded reconnect could exceed
    # max.poll.interval.ms across a few lease-server restarts and stall
    # every group member's rebalance. Bounded by default (60s: comfortably
    # under the 300s default max.poll.interval.ms even after the tx
    # timeout and quiet period below); 0 stays available as an explicit,
    # documented-risk override that skips the max.poll.interval.ms
    # validator below.
    reconnect_timeout_ms: int = Field(default=60_000, ge=0, le=3_600_000)
    tx_timeout_ms: int = Field(default=30_000, ge=1_000, le=600_000)
    expected_chunk_commit_ms: int = Field(default=1_000, ge=1, le=600_000)
    # >= 2s (Neo4j's own default db.transaction.monitor.check.interval): a
    # margin below that could let Q elapse while a timed-out transaction is
    # still waiting for Neo4j's periodic monitor to notice and terminate it
    # (review finding S1-4).
    restart_quiet_margin_ms: int = Field(default=5_000, ge=2_000, le=600_000)

    @field_validator(
        "health_check_interval_ms", "max_consecutive_restarts", "restart_count_reset_ms",
        "restart_backoff_base_ms", "restart_backoff_max_ms", "reconnect_timeout_ms",
        "tx_timeout_ms", "expected_chunk_commit_ms", "restart_quiet_margin_ms",
        mode="before",
    )
    @classmethod
    def _bucket_lease_fields_are_not_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("bucket_lease values must be integers")
        return value

    @model_validator(mode="after")
    def _backoff_covers_base(self) -> "BucketLeaseStreamConfig":
        if self.restart_backoff_max_ms < self.restart_backoff_base_ms:
            raise ValueError("restart_backoff_max_ms must be >= restart_backoff_base_ms")
        return self

    @model_validator(mode="after")
    def _tx_timeout_covers_expected_commit(self) -> "BucketLeaseStreamConfig":
        # P5-Q1 fencing amendment: T must be validated to be at least 10x
        # the expected chunk commit time, so a normally-fast commit never
        # spuriously hits Neo4j's own transaction-timeout termination.
        if self.tx_timeout_ms < 10 * self.expected_chunk_commit_ms:
            raise ValueError(
                "tx_timeout_ms must be at least 10x expected_chunk_commit_ms"
            )
        return self

    @property
    def restart_quiet_period_ms(self) -> int:
        """``Q = T + margin``: the delay a new lease-server generation waits
        before its first grant (P5-Q1 fencing amendment)."""
        return self.tx_timeout_ms + self.restart_quiet_margin_ms


class RetryConsumerConfig(BaseModel):
    """Streaming-only tuning for the DLQ retry consumer (P5-S4).

    ``due_batch_coalesce_ms`` is the small window the retry consumer's
    scheduler waits after the *first* record in a tick becomes due, to let
    near-simultaneous due records from other partitions join the same write
    batch rather than writing single-row batches back-to-back — bounded low
    so it never meaningfully delays a resolution.

    ``max_records_per_partition`` bounds how many due, unresolved records one
    DLQ partition may hold (the partition is paused at the bound);
    ``max_batch_records`` is the due-record count that flushes a batch
    immediately instead of waiting out the coalescing window (and the most
    records read per poll tick); ``write_workers`` is the number of
    concurrent lease-holding write threads per edge type (live-fleet mode).

    ``max_overflow_held_per_partition`` (v7-S4) bounds the blocked rows one DLQ
    partition may hold locally beyond ``max_records_per_partition`` (they do not
    count towards the pause, so the partition keeps fetching its roots' next
    copies); beyond it a blocked row is re-published. ``None`` means 10 x
    ``max_records_per_partition``. Each held row keeps its envelope in memory.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")
    poll_timeout_ms: int = Field(default=500, ge=10, le=60_000)
    backlog_check_interval_ms: int = Field(default=10_000, ge=1_000, le=300_000)
    due_batch_coalesce_ms: int = Field(default=200, ge=0, le=10_000)
    max_records_per_partition: int = Field(default=1_000, ge=1, le=100_000)
    max_batch_records: int = Field(default=2_000, ge=1, le=200_000)
    write_workers: int = Field(default=4, ge=1, le=64)
    max_overflow_held_per_partition: Optional[int] = Field(default=None, ge=0, le=10_000_000)

    @field_validator(
        "poll_timeout_ms", "backlog_check_interval_ms", "due_batch_coalesce_ms",
        "max_records_per_partition", "max_batch_records", "write_workers",
        "max_overflow_held_per_partition", mode="before",
    )
    @classmethod
    def _reject_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("RetryConsumerConfig fields must be integers")
        return value


class LoadingConfig(BaseModel):
    """Global loading behaviour shared across all loaders.

    Attributes:
        mode: ``"bulk"`` for one-shot historical loads;
              ``"stream"`` for continuous Kafka consumption.
        consumer_group_id: Prefix of every consumer group (node groups ``<id>-<label>``,
            stream edge groups ``<id>-<type>``, retry ``<id>-retry-consumer``).
        max_poll_interval_ms: Maximum time between Kafka ``poll()`` calls
            before the broker considers the consumer dead (milliseconds).
        session_timeout_ms: Kafka session timeout (milliseconds).
        heartbeat_interval_ms: Kafka group heartbeat interval (milliseconds,
            default 500; librdkafka's default is 3000). Members learn that a
            rebalance started only from a heartbeat response, so each rebalance
            while staggered replicas join costs up to one interval. Must be
            below ``session_timeout_ms``.
        unwind_batch_size: Maximum records in a node batch, and the classic
            edge outer batch when no edge-specific size is configured (default 500);
            lease modes replace the edge outer batch with
            ``--bucket-lease-outer-batch-size``.
        edge_unwind_batch_size: Optional edge outer batch size for classic loaders (default: use
            ``unwind_batch_size``; at most 1,000,000; lease modes replace
            it with ``--bucket-lease-outer-batch-size``). The per-lane or per-cell
            Neo4j transaction limit is ``mix_and_batch.batch_size``.
        flush_interval_ms: Idle partial-batch flush interval in milliseconds (default 1000).
        retry_max_attempts: Node-write attempts including the first (default 3).
        retry_base_delay_ms: Initial node-write retry delay in milliseconds (default 100).
        retry_max_delay_ms: Maximum node-write retry delay in milliseconds (default 5000).
        rejection_log_path: Fsync'd malformed-record sink used when no ``dead_letter`` is enabled.
        coordination: Deprecated Phase 4 global batch clock settings, reachable only via
            ``--mode stream --stream-clock-rotation`` after P5-S1.
        bucket_lease: Bucket-lease coordinator tuning (P5-S1): streaming's default
            relationship-write concurrency mechanism, plus the P5-Q1 fencing amendment's
            Neo4j transaction-timeout settings, which also apply to bulk lease and planned
            writers (see ``BucketLeaseStreamConfig``).
        retry_consumer: DLQ retry-consumer tuning (P5-S4): poll cadence, backlog-gauge
            check interval, and due-batch coalescing window (see ``RetryConsumerConfig``).
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["bulk", "stream"] = "stream"
    consumer_group_id: str = "graph-loader"
    max_poll_interval_ms: int = Field(default=300_000, gt=0)
    session_timeout_ms: int = Field(default=45_000, gt=0)
    heartbeat_interval_ms: int = Field(default=500, gt=0)
    unwind_batch_size: int = Field(default=500, gt=0, le=10_000)
    edge_unwind_batch_size: int | None = Field(default=None, gt=0, le=1_000_000)
    flush_interval_ms: int = Field(default=1_000, gt=0, le=60_000)
    retry_max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_ms: int = Field(default=100, gt=0, le=60_000)
    retry_max_delay_ms: int = Field(default=5_000, gt=0, le=300_000)
    rejection_log_path: str = "var/rejections/node-loader.jsonl"
    coordination: CoordinationConfig = Field(default_factory=CoordinationConfig)
    bucket_lease: BucketLeaseStreamConfig = Field(default_factory=BucketLeaseStreamConfig)
    retry_consumer: RetryConsumerConfig = Field(default_factory=RetryConsumerConfig)
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
    def _bucket_lease_reconnect_stays_under_max_poll_interval(self) -> "LoadingConfig":
        # P5-S1 Story 7, review finding B7-1: streaming's poll thread
        # blocks synchronously on lease reconnect + the Neo4j write (no
        # dual-thread worker model for the lease path in this slice), so
        # an unbounded-looking reconnect budget could exceed
        # max.poll.interval.ms and get the consumer evicted mid-batch,
        # stalling every group member's rebalance. Skipped when
        # reconnect_timeout_ms == 0 (an explicit, documented-risk
        # override -- see the field's own docstring).
        reconnect_ms = self.bucket_lease.reconnect_timeout_ms
        if reconnect_ms == 0:
            return self
        margin_ms = 5_000
        budget_ms = reconnect_ms + self.bucket_lease.tx_timeout_ms + self.bucket_lease.restart_quiet_period_ms
        if budget_ms >= self.max_poll_interval_ms - margin_ms:
            raise ValueError(
                "bucket_lease.reconnect_timeout_ms + tx_timeout_ms + restart_quiet_period_ms "
                f"({budget_ms}ms) must stay under max_poll_interval_ms - {margin_ms}ms "
                f"({self.max_poll_interval_ms - margin_ms}ms), or set reconnect_timeout_ms=0 "
                "to explicitly accept the unbounded-reconnect risk"
            )
        return self

    @model_validator(mode="after")
    def node_retry_delay_is_ordered(self) -> "LoadingConfig":
        if self.retry_max_delay_ms < self.retry_base_delay_ms:
            raise ValueError("retry_max_delay_ms must be >= retry_base_delay_ms")
        return self

    @model_validator(mode="after")
    def heartbeat_is_within_session(self) -> "LoadingConfig":
        if self.heartbeat_interval_ms >= self.session_timeout_ms:
            raise ValueError("heartbeat_interval_ms must be < session_timeout_ms")
        return self


class MultiTopicEdgeError(ValueError):
    """``EdgeConfig.topic`` was read on a relationship type with several topics."""


class MultiPairEdgeError(ValueError):
    """Single-pair code received a relationship type that binds several label pairs (v7-S7).

    Cypher builders, normalisers, writers and routers work on ONE label pair. They must be given a
    single-pair view (:meth:`EdgeConfig.pair_view` / :meth:`EdgeConfig.topic_view`), never the parent."""


ConflictRule = Literal["single", "order_by", "precedence", "arrival"]

_TOPIC_ENTRY_KEYS = ("name", "nodes", "source_key_property", "target_key_property")


class TopicEndpoints(BaseModel):
    """The label pair one topic of a multi-pair relationship type binds (v7-S7).

    Each topic binds its own ``(source label, target label)`` and key properties; the pair is never
    inferred from a record (owner decision 3)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    nodes: SourceTargetConfig
    source_key_property: str
    target_key_property: str

    @field_validator("source_key_property", "target_key_property")
    @classmethod
    def key_properties_are_cypher_identifiers(cls, value: str, info) -> str:
        return _validate_cypher_identifier(value, info.field_name)


@dataclass(frozen=True)
class EndpointPair:
    """One label pair of a relationship type and the topics that feed it, in ``topics`` order.

    ``index`` is the pair's position in order of first appearance in ``topics`` (0 for a single-pair
    type). Within one type a pair is identified by its labels: the same ``(S, T)`` with different key
    properties is rejected at load."""

    index: int
    source_label: str
    target_label: str
    source_key_property: str
    target_key_property: str
    topics: tuple[str, ...]

    @property
    def labels(self) -> tuple[str, str]:
        return (self.source_label, self.target_label)

    @property
    def is_self_referencing(self) -> bool:
        return self.source_label == self.target_label

    @property
    def name(self) -> str:
        """``Source>Target``: how a pair appears in logs and plan statistics (never in metric labels)."""
        return f"{self.source_label}>{self.target_label}"


# Views of a parent EdgeConfig, keyed by id(parent) and dropped with it. Kept outside the model:
# pydantic compares (and copies) private attributes, so a cache there would change equality and
# leak into model_copy() copies.
# Every access holds _PAIR_VIEWS_LOCK: pair coordinators and the retry consumer build views from
# several threads, and a view must be one object per (parent, pair, topics).
_PAIR_VIEWS: dict[int, tuple["weakref.ref", dict]] = {}
_PAIR_VIEWS_LOCK = threading.RLock()


def _views_of(edge: "EdgeConfig") -> dict:
    """The view cache of ``edge``; callers read and write it under ``_PAIR_VIEWS_LOCK``."""
    key = id(edge)
    with _PAIR_VIEWS_LOCK:
        entry = _PAIR_VIEWS.get(key)
        if entry is not None and entry[0]() is edge:
            return entry[1]
        views: dict = {}

        def _drop(ref, key=key) -> None:
            with _PAIR_VIEWS_LOCK:
                current = _PAIR_VIEWS.get(key)
                if current is not None and current[0] is ref:
                    del _PAIR_VIEWS[key]

        _PAIR_VIEWS[key] = (weakref.ref(edge, _drop), views)
        return views


def _resolve_topic_entries(name: object, data: dict) -> None:
    """v7-S7 rules R2-R8: resolve every ``topics`` entry to its label pair, in place.

    Single-pair result: top-level ``nodes``/keys set, no ``topic_endpoints`` (today's shape).
    Multi-pair result: top-level fields removed, ``topic_endpoints`` covers every topic."""
    entries = data["topics"]
    given_endpoints = data.pop("topic_endpoints", None)
    top = {key: data.get(key) for key in _TOPIC_ENTRY_KEYS[1:]}
    has_top = {key: value is not None for key, value in top.items()}
    names: list[str] = []
    bindings: dict[str, Any] = {}
    for entry in entries:
        if isinstance(entry, str):
            if not entry.strip():
                raise ValueError(f"Edge '{name}' topic must not be blank")
            topic = entry.strip()
            names.append(topic)
            bindings.setdefault(topic, None)
            continue
        if not isinstance(entry, Mapping):
            raise ValueError(
                f"Edge '{name}': each topics entry must be a non-blank string or a mapping "
                f"{{name, nodes, source_key_property, target_key_property}}"
            )
        topic = entry.get("name")
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError(f"Edge '{name}': a topics mapping entry needs a non-blank 'name'")
        topic = topic.strip()
        unknown = sorted(set(entry) - set(_TOPIC_ENTRY_KEYS))
        if unknown:
            raise ValueError(
                f"Edge '{name}' topic '{topic}': unknown key(s) {unknown}; a topics entry takes "
                "name, nodes, source_key_property and target_key_property"
            )
        missing = [key for key in _TOPIC_ENTRY_KEYS[1:] if entry.get(key) is None]
        if missing:
            raise ValueError(
                f"Edge '{name}' topic '{topic}': a topics mapping entry needs {missing} "
                "(each topic binds its own label pair)"
            )
        names.append(topic)
        bindings.setdefault(topic, {key: entry[key] for key in _TOPIC_ENTRY_KEYS[1:]})
    if not names:
        raise ValueError(f"Edge '{name}': topics must not be empty")
    duplicates = sorted({t for t in names if names.count(t) > 1})
    if duplicates:
        raise ValueError(f"Edge '{name}': duplicate topics {duplicates} in topics list")
    if given_endpoints is not None:  # R8: the canonical dump form
        if not isinstance(given_endpoints, Mapping):
            raise ValueError(f"Edge '{name}': topic_endpoints must map each topic to its label pair")
        if any(binding is not None for binding in bindings.values()) or any(has_top.values()):
            raise ValueError(
                f"Edge '{name}': topic_endpoints cannot be combined with topics mapping entries or "
                "top-level nodes/source_key_property/target_key_property"
            )
        if set(given_endpoints) != set(names):
            raise ValueError(
                f"Edge '{name}': topic_endpoints keys {sorted(given_endpoints)} differ from topics {names}"
            )
        bindings = {topic: given_endpoints[topic] for topic in names}
    inherits = [topic for topic in names if bindings[topic] is None]
    if inherits and not all(has_top.values()):
        absent = [key for key, present in has_top.items() if not present]
        raise ValueError(
            f"Edge '{name}' topic '{inherits[0]}' binds no label pair: set {absent} at the top level "
            "or give the topic a mapping entry {name, nodes, source_key_property, target_key_property}"
        )
    if not inherits and any(has_top.values()):
        present = [key for key, value in has_top.items() if value]
        raise ValueError(
            f"Edge '{name}': top-level {present} is unused (every topics entry binds its own label pair); "
            "remove it"
        )
    resolved: dict[str, TopicEndpoints] = {}
    for topic in names:
        source = bindings[topic] if bindings[topic] is not None else top
        if isinstance(source, TopicEndpoints):
            resolved[topic] = source
            continue
        if not isinstance(source, Mapping):
            raise ValueError(f"Edge '{name}' topic '{topic}': the label pair must be a mapping")
        try:
            resolved[topic] = TopicEndpoints.model_validate(dict(source))
        except ValueError as exc:
            raise ValueError(f"Edge '{name}' topic '{topic}': invalid label pair: {exc}") from None
    by_labels: dict[tuple[str, str], tuple[str, TopicEndpoints]] = {}
    for topic in names:
        endpoints = resolved[topic]
        labels = (endpoints.nodes.source, endpoints.nodes.target)
        first = by_labels.setdefault(labels, (topic, endpoints))
        if (first[1].source_key_property, first[1].target_key_property) != (
            endpoints.source_key_property, endpoints.target_key_property,
        ):
            raise ValueError(
                f"Edge '{name}': topics '{first[0]}' and '{topic}' bind the same label pair "
                f"{labels[0]}->{labels[1]} with different key properties "
                f"({first[1].source_key_property}, {first[1].target_key_property}) and "
                f"({endpoints.source_key_property}, {endpoints.target_key_property})"
            )
    data["topics"] = names
    if len(by_labels) == 1:  # R5: one pair, today's shape
        if not inherits:  # every entry was a mapping: lift the pair to the top level
            only = resolved[names[0]]
            data["nodes"] = only.nodes
            data["source_key_property"] = only.source_key_property
            data["target_key_property"] = only.target_key_property
        return
    for key in _TOPIC_ENTRY_KEYS[1:]:  # R6: several pairs
        data.pop(key, None)
    data["topic_endpoints"] = resolved


class EdgeConfig(BaseModel):
    """Configuration for a single relationship type.

    Attributes:
        type: The Neo4j relationship type (e.g. ``"WORKS_AT"``).
        topics: Kafka topics that carry records for this relationship type
            (non-empty, unique; order is meaningful for ``precedence``).
            ``topic: x`` is accepted on input as shorthand for ``topics: [x]``.
            The read property ``topic`` returns the single topic and raises
            :class:`MultiTopicEdgeError` for multi-topic types.
        topic_conflict: Required (unless ``order_by`` is set) when ``topics``
            has two or more entries: ``order_by`` | ``precedence`` |
            ``arrival``. Ignored for single-topic types.
        nodes: Source and target node label configuration. ``None`` for a
            multi-pair type (v7-S7), whose topics each bind their own pair in
            ``topic_endpoints``; single-pair code receives a
            :meth:`pair_view` / :meth:`topic_view` instead.
        topic_endpoints: Multi-pair types only: ``{topic: TopicEndpoints}``
            for every topic. A ``topics`` entry may be a mapping
            ``{name, nodes, source_key_property, target_key_property}``; a
            string entry inherits the top-level pair. Types whose topics all
            bind one pair normalise to the single-pair shape.
        properties: Mapping of property name → :class:`PropertyConfig`.
            Must contain at least one entry.
        mix_and_batch: Batching and slot-timing parameters.
        retry: Retry and backoff configuration.
        dead_letter: Optional DLQ routing. ``None`` disables DLQ.
    """

    type: str
    topics: list[str]
    topic_conflict: Optional[Literal["order_by", "precedence", "arrival"]] = None
    nodes: Optional[SourceTargetConfig] = None
    source_key_property: Optional[str] = None
    target_key_property: Optional[str] = None
    topic_endpoints: Optional[dict[str, TopicEndpoints]] = None
    replicas: int = Field(default=1, ge=1, le=64)
    properties: dict[str, PropertyConfig] = Field(default_factory=dict)
    mix_and_batch: MixAndBatchConfig = Field(default_factory=MixAndBatchConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    dead_letter: Optional[DeadLetterConfig] = None
    execution: EdgeExecutionConfig = Field(default_factory=EdgeExecutionConfig)
    order_by: Optional[str] = None
    """P5-S4: when set, must name one of this edge type's own declared
    ``properties`` keys. Retried and live writes for this type use a
    conditional ``SET`` (only applied when the incoming value is newer)
    instead of pair-parking — an alternative mechanism, not an addition to
    it (P5-S4 Decisions §10): an ``order_by``-configured type never builds a
    ``ParkedPairIndex``.
    """
    _source_key_config: PropertyConfig | None = PrivateAttr(default=None)
    _target_key_config: PropertyConfig | None = PrivateAttr(default=None)
    # v7-S7, multi-pair parents: {(source label, target label): (source, target) key configs}.
    _pair_key_configs: dict | None = PrivateAttr(default=None)
    # v7-S7: the pair a view was built for (0 for a single-pair type).
    _pair_index: int = PrivateAttr(default=0)

    @model_validator(mode="before")
    @classmethod
    def _normalize_topic_shorthand(cls, data: Any) -> Any:
        """Map ``topic: x`` to ``topics: [x]``; require exactly one of the two.

        v7-S7: ``topics`` entries may also be mappings that bind their own label
        pair (see :func:`_resolve_topic_entries`)."""
        if not isinstance(data, dict):
            return data
        name = data.get("type")
        has_topic = "topic" in data
        has_topics = "topics" in data
        if has_topic and has_topics:
            raise ValueError(
                f"Edge '{name}': declare either 'topic' or 'topics', not both"
            )
        if not has_topic and not has_topics:
            raise ValueError(
                f"Edge '{name}': 'topic' (or 'topics: [..]') is required"
            )
        data = dict(data)
        if has_topic:
            if isinstance(data["topic"], Mapping):
                raise ValueError(
                    f"Edge '{name}': 'topic' takes one topic name; use 'topics: [..]' for "
                    "entries that bind their own label pair"
                )
            data["topics"] = [data.pop("topic")]
        topics = data["topics"]
        if not isinstance(topics, (list, tuple)):
            raise ValueError(f"Edge '{name}': topics must be a list of topic names")
        if all(isinstance(entry, str) for entry in topics) and data.get("topic_endpoints") is None:
            # Today's path, unchanged: string entries only.
            data.pop("topic_endpoints", None)
            cleaned: list[str] = []
            for entry in topics:
                if not entry.strip():
                    raise ValueError(f"Edge '{name}' topic must not be blank")
                cleaned.append(entry.strip())
            if not cleaned:
                raise ValueError(f"Edge '{name}': topics must not be empty")
            duplicates = sorted({t for t in cleaned if cleaned.count(t) > 1})
            if duplicates:
                raise ValueError(f"Edge '{name}': duplicate topics {duplicates} in topics list")
            missing = [key for key in _TOPIC_ENTRY_KEYS[1:] if data.get(key) is None]
            if missing:
                raise ValueError(
                    f"Edge '{name}' topic '{cleaned[0]}' binds no label pair: set {missing} at the "
                    "top level or give the topic a mapping entry {name, nodes, source_key_property, "
                    "target_key_property}"
                )
            data["topics"] = cleaned
            return data
        _resolve_topic_entries(name, data)
        return data

    @property
    def topic(self) -> str:
        """The single topic of a single-topic type.

        Raises :class:`MultiTopicEdgeError` when the type has several topics,
        so code that still assumes one topic per type fails loudly.
        """
        if len(self.topics) != 1:
            raise MultiTopicEdgeError(
                f"Edge '{self.type}' has {len(self.topics)} topics {self.topics}; "
                "use EdgeConfig.topics instead of EdgeConfig.topic"
            )
        return self.topics[0]

    @property
    def is_multi_topic(self) -> bool:
        return len(self.topics) > 1

    @property
    def conflict_rule(self) -> ConflictRule:
        """Effective cross-topic conflict rule.

        ``order_by`` whenever ``order_by`` is set; ``single`` for a one-topic
        type without it; otherwise the declared ``topic_conflict``
        (``precedence`` | ``arrival``) of a multi-topic type.

        v7-S7 (orchestrator decision on Q1): the rule is evaluated per label
        pair. A multi-pair type none of whose pairs is fed by two or more
        topics is ``single`` (records of different pairs never conflict); a
        declared ``precedence``/``arrival`` there has no effect (warned).
        """
        if self.order_by is not None:
            return "order_by"
        if len(self.topics) == 1:
            return "single"
        if self.topic_endpoints is not None and not self.contended_pairs:
            return "single"
        return self.topic_conflict  # validated non-None and not "order_by"

    # ── v7-S7: label pairs ────────────────────────────────────────────────

    @property
    def is_multi_pair(self) -> bool:
        """``True`` when the topics of this type bind two or more label pairs."""
        return self.topic_endpoints is not None

    @property
    def endpoint_pairs(self) -> tuple[EndpointPair, ...]:
        """Every label pair, in order of first appearance in ``topics``.

        A single-pair type (and every view) has exactly one pair, whose index
        is :attr:`pair_index`."""
        if self.topic_endpoints is None:
            if self.nodes is None:
                return ()
            return (EndpointPair(
                index=self._pair_index, source_label=self.nodes.source, target_label=self.nodes.target,
                source_key_property=self.source_key_property, target_key_property=self.target_key_property,
                topics=tuple(self.topics),
            ),)
        views = _views_of(self)
        with _PAIR_VIEWS_LOCK:
            pairs = views.get("pairs")
        if pairs is None:
            grouped: dict[tuple[str, str], list[str]] = {}
            first: dict[tuple[str, str], TopicEndpoints] = {}
            for topic in self.topics:
                endpoints = self.topic_endpoints[topic]
                labels = (endpoints.nodes.source, endpoints.nodes.target)
                grouped.setdefault(labels, []).append(topic)
                first.setdefault(labels, endpoints)
            pairs = tuple(
                EndpointPair(
                    index=index, source_label=labels[0], target_label=labels[1],
                    source_key_property=first[labels].source_key_property,
                    target_key_property=first[labels].target_key_property, topics=tuple(topics),
                )
                for index, (labels, topics) in enumerate(grouped.items())
            )
            with _PAIR_VIEWS_LOCK:
                pairs = views.setdefault("pairs", pairs)
        return pairs

    @property
    def contended_pairs(self) -> tuple[EndpointPair, ...]:
        """Pairs fed by two or more topics: where a cross-topic conflict rule matters."""
        return tuple(pair for pair in self.endpoint_pairs if len(pair.topics) > 1)

    def pair_for_topic(self, topic: str) -> EndpointPair:
        """The label pair ``topic`` binds. ``ValueError`` for a topic this type does not read."""
        for pair in self.endpoint_pairs:
            if topic in pair.topics:
                return pair
        raise ValueError(f"Edge '{self.type}' does not read topic '{topic}'")

    def pair_labels_for_topic(self, topic: str) -> tuple[str, ...]:
        """``(S, T)`` of the pair ``topic`` binds; ``("?", topic)`` for a topic the type does not read.

        The pseudo pair keeps an unknown topic (for example a DLQ envelope of a removed topic) apart
        from every real pair, so it can never block or be blocked by one."""
        for pair in self.endpoint_pairs:
            if topic in pair.topics:
                return pair.labels
        return ("?", topic)

    @property
    def endpoint_labels(self) -> frozenset[str]:
        """Every node label this type touches (the union over its pairs)."""
        return frozenset(label for pair in self.endpoint_pairs for label in pair.labels)

    @property
    def has_self_reference(self) -> bool:
        """``True`` when any pair is self-referencing (source label == target label)."""
        return any(pair.is_self_referencing for pair in self.endpoint_pairs)

    @property
    def single_pair(self) -> EndpointPair:
        """The one pair of a single-pair type or view; :class:`MultiPairEdgeError` for a multi-pair parent."""
        if self.topic_endpoints is not None:
            raise MultiPairEdgeError(
                f"Edge '{self.type}' binds {len(self.endpoint_pairs)} label pairs "
                f"({', '.join(pair.name for pair in self.endpoint_pairs)}); single-pair code must use "
                "EdgeConfig.pair_view() / topic_view()"
            )
        return self.endpoint_pairs[0]

    @property
    def pair_index(self) -> int:
        """Index of this config's pair in its parent's :attr:`endpoint_pairs` (0 for a single-pair type)."""
        return self._pair_index

    def pair_view(self, pair: "EndpointPair | int", *, topics: Sequence[str] | None = None) -> "EdgeConfig":
        """A single-pair copy of this type for one of its label pairs.

        ``nodes`` and key properties are the pair's (key configs bound),
        ``topics`` is ``topics`` (a subset of the pair's, kept in the parent's
        order) or all of the pair's topics, ``topic_conflict`` is kept only
        when the view has two or more topics, and the DLQ, parking-lot and
        parked-pair index names are pinned to this type's (one DLQ per type;
        v7-S4 review S6). Cached per parent (one object per pair and topics).

        A view is an in-process object: ``pair_index`` and the key bindings are
        private and do not survive ``model_dump``/``model_validate`` (pickle
        keeps them). Decisions that depend on the whole type (``is_multi_topic``
        for the sequenced index, ``conflict_rule``, ``dead_letter_entity``,
        ``precedence_rank``) must be taken on the parent, never on a view.
        """
        pairs = self.endpoint_pairs
        index = pair if isinstance(pair, int) else pair.index
        # A view's one pair keeps its parent's index, so look the pair up by index, not position.
        chosen = None if isinstance(index, bool) else next((p for p in pairs if p.index == index), None)
        if chosen is None:
            raise ValueError(f"Edge '{self.type}' has no label pair {pair!r}")
        if not isinstance(pair, int) and pair != chosen:
            raise ValueError(f"Edge '{self.type}': {pair!r} is not one of its label pairs")
        requested = tuple(chosen.topics if topics is None else topics)
        if not requested or not set(requested) <= set(chosen.topics) or len(set(requested)) != len(requested):
            raise ValueError(
                f"Edge '{self.type}': topics {list(requested)} are not distinct topics of label pair "
                f"{chosen.name} {list(chosen.topics)}"
            )
        # Always in the parent's order, so a view never changes precedence_rank.
        view_topics = tuple(topic for topic in chosen.topics if topic in set(requested))
        cache_key = (index, view_topics)
        views = _views_of(self)
        with _PAIR_VIEWS_LOCK:
            cached = views.get(cache_key)
        if cached is not None:
            return cached
        update: dict = {
            "topics": list(view_topics),
            "topic_conflict": self.topic_conflict if len(view_topics) > 1 else None,
        }
        if self.topic_endpoints is not None:
            update.update({
                "nodes": SourceTargetConfig(source=chosen.source_label, target=chosen.target_label),
                "source_key_property": chosen.source_key_property,
                "target_key_property": chosen.target_key_property,
                "topic_endpoints": None,
            })
        parent_names = (edge_dead_letter_topic_name(self), edge_parked_topic_name(self),
                        edge_parked_pair_index_topic_name(self))
        if self.dead_letter is not None and parent_names[0] is not None:
            pinned = {"topic": parent_names[0]}
            if self.dead_letter.parked_topic is None:
                pinned["parked_topic"] = parent_names[1]
            update["dead_letter"] = self.dead_letter.model_copy(update=pinned)
        view = self.model_copy(update=update)
        if self.topic_endpoints is not None:
            configs = (self._pair_key_configs or {}).get(chosen.labels)
            view._source_key_config, view._target_key_config = configs if configs is not None else (None, None)
            view._pair_key_configs = None
            view._pair_index = index
        view_names = (edge_dead_letter_topic_name(view), edge_parked_topic_name(view),
                      edge_parked_pair_index_topic_name(view))
        if view_names != parent_names:
            raise RuntimeError(
                f"edge={self.type} pair={chosen.name} topics={list(view_topics)}: view DLQ names {view_names} "
                f"differ from the type's {parent_names}"
            )
        with _PAIR_VIEWS_LOCK:
            return views.setdefault(cache_key, view)

    def topic_view(self, topic: str) -> "EdgeConfig":
        """The single-topic, single-pair view of ``topic``: ``pair_view(pair_for_topic(topic), topics=[topic])``."""
        return self.pair_view(self.pair_for_topic(topic), topics=[topic])

    def precedence_rank(self, topic: str) -> int:
        """Index of ``topic`` in ``topics``; a higher rank wins under ``precedence``.

        Raises ``ValueError`` for a topic this type does not read.
        """
        try:
            return self.topics.index(topic)
        except ValueError:
            raise ValueError(f"Edge '{self.type}' does not read topic '{topic}'") from None

    @property
    def dead_letter_entity(self) -> str:
        """Name the default DLQ topics derive from: the topic for a
        single-topic type (compat), the type name for a multi-topic type."""
        return self.topics[0] if len(self.topics) == 1 else self.type

    @field_validator("type", "source_key_property", "target_key_property")
    @classmethod
    def edge_type_is_cypher_identifier(cls, value: Optional[str], info) -> Optional[str]:
        """Validate relationship identifiers interpolated into Cypher (key properties are
        ``None`` on a multi-pair type, whose pairs are validated in ``TopicEndpoints``)."""
        if value is None and info.field_name != "type":
            return value
        return _validate_cypher_identifier(value, info.field_name)

    @model_validator(mode="after")
    def _one_pair_shape(self) -> "EdgeConfig":
        """Exactly one of the single-pair fields and ``topic_endpoints`` describes the pairs."""
        single = (self.nodes, self.source_key_property, self.target_key_property)
        if self.topic_endpoints is None:
            if any(value is None for value in single):
                raise ValueError(
                    f"Edge '{self.type}' binds no label pair: set nodes, source_key_property and "
                    "target_key_property"
                )
            return self
        if any(value is not None for value in single):
            raise ValueError(
                f"Edge '{self.type}': top-level nodes/source_key_property/target_key_property cannot be "
                "combined with topic_endpoints"
            )
        if set(self.topic_endpoints) != set(self.topics):
            raise ValueError(
                f"Edge '{self.type}': topic_endpoints keys {sorted(self.topic_endpoints)} differ from "
                f"topics {self.topics}"
            )
        if len({(e.nodes.source, e.nodes.target) for e in self.topic_endpoints.values()}) < 2:
            raise ValueError(
                f"Edge '{self.type}': topic_endpoints binds one label pair; use the single-pair form"
            )
        return self

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
        with _PAIR_VIEWS_LOCK:
            _views_of(self).clear()  # views copy the bindings when they are built

    def _bind_pair_key_configs(self, configs: dict) -> None:
        """Multi-pair counterpart of :meth:`bind_endpoint_properties`: ``{(S, T): (source, target)}``."""
        self._pair_key_configs = dict(configs)
        with _PAIR_VIEWS_LOCK:
            _views_of(self).clear()

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

    @model_validator(mode="after")
    def _order_by_is_a_declared_property(self) -> "EdgeConfig":
        """P5-S4 Story 1: ``order_by`` must name a property this type declares."""
        if self.order_by is not None and self.order_by not in self.properties:
            raise ValueError(
                f"Edge '{self.type}': order_by={self.order_by!r} is not one of "
                f"this type's declared properties {sorted(self.properties)}"
            )
        return self

    @model_validator(mode="after")
    def _conflict_rule_is_declared(self) -> "EdgeConfig":
        """Multi-topic types must pick exactly one conflict rule."""
        if self.order_by is not None and self.topic_conflict in ("precedence", "arrival"):
            raise ValueError(
                f"Edge '{self.type}': order_by and topic_conflict="
                f"{self.topic_conflict!r} are mutually exclusive; pick one"
            )
        if self.order_by is None and self.topic_conflict == "order_by":
            raise ValueError(
                f"Edge '{self.type}': topic_conflict='order_by' requires 'order_by: <property>'"
            )
        if len(self.topics) < 2 or self.order_by is not None:
            return self  # a single topic needs no rule
        contended = self.contended_pairs if self.topic_endpoints is not None else None
        if contended is not None and not contended:
            # v7-S7 Q1 (orchestrator decision): every label pair has one topic, so no
            # two records of one relationship can come from different topics.
            if self.topic_conflict is not None:
                pairs = ", ".join(pair.name for pair in self.endpoint_pairs)
                _warn_once(
                    ("topic_conflict_has_no_effect", self.type, self.topic_conflict, pairs),
                    "Edge '%s': topic_conflict=%s has no effect: each of its label pairs (%s) is fed "
                    "by one topic, so records of one relationship never come from two topics",
                    self.type, self.topic_conflict, pairs,
                )
            return self
        if self.topic_conflict is None:
            pairs = "" if contended is None else "; label pair(s) fed by several topics: " + ", ".join(
                f"{pair.source_label}->{pair.target_label} {list(pair.topics)}" for pair in contended
            )
            raise ValueError(
                f"Edge '{self.type}' reads {len(self.topics)} topics {self.topics} and must "
                "declare how records for the same pair are reconciled across topics. "
                "Set one of: 'order_by: <property>' (correct in every mode), "
                "'topic_conflict: precedence' (later-listed topic wins; bulk modes only), "
                "or 'topic_conflict: arrival' (nondeterministic)" + pairs
            )
        return self

    @model_validator(mode="after")
    def _warn_order_by_property_type(self) -> "EdgeConfig":
        if self.order_by is not None:
            prop_type = self.properties[self.order_by].type
            if prop_type not in _ORDERABLE_PROPERTY_TYPES:
                logger.warning(
                    "Edge '%s': order_by property '%s' has type '%s'; values compare "
                    "lexicographically, which is only correct for ISO-8601 timestamps "
                    "or zero-padded values (date, datetime, integer or float is preferred)",
                    self.type, self.order_by, prop_type,
                )
        return self


_ORDERABLE_PROPERTY_TYPES = frozenset({"date", "datetime", "integer", "float"})


def precedence_edge_types(schema: "GraphSchema") -> list[str]:
    """Types whose effective conflict rule is ``precedence`` (multi-topic only)."""
    return [e.type for e in schema.edges if e.is_multi_topic and e.conflict_rule == "precedence"]


def require_no_precedence_in_stream(schema: "GraphSchema", mode: str) -> None:
    """Raise ``ValueError`` if ``mode == "stream"`` and any type uses ``precedence``.

    ``GraphSchema`` calls this at load time only when ``loading.mode`` is
    explicit in the YAML; the effective run mode may come from ``--mode``, so
    ``start`` must call it again with the resolved mode.
    """
    if mode != "stream":
        return
    types = precedence_edge_types(schema)
    if types:
        raise ValueError(
            f"topic_conflict='precedence' is not supported in stream mode (no cross-topic "
            f"order exists): edge type(s) {types}. Use 'order_by: <property>' instead, or "
            "set `loading.mode: bulk` (or remove it) and run `start --mode bulk`"
        )


def schema_data_topics(schema: "GraphSchema") -> list[str]:
    """Every node and edge data topic, deduplicated, nodes first in schema order."""
    return list(dict.fromkeys(
        [n.topic for n in schema.nodes] + [t for e in schema.edges for t in e.topics]
    ))


def arrival_conflict_warnings(schema: "GraphSchema") -> list[str]:
    """One warning message per multi-topic type using ``topic_conflict: arrival``.

    For a multi-pair type (v7-S7) the message names the label pairs fed by several topics."""
    return [
        f"Edge '{e.type}': topic_conflict=arrival; pairs present in several topics "
        + (
            f"{e.topics}" if not e.is_multi_pair else "; ".join(
                f"{list(pair.topics)} (label pair {pair.source_label}->{pair.target_label})"
                for pair in e.contended_pairs
            )
        )
        + " resolve in arrival order, which is nondeterministic. "
        "Use order_by for a deterministic result"
        for e in schema.edges
        if e.is_multi_topic and e.conflict_rule == "arrival"
    ]


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
    _edges_by_topic: dict[str, EdgeConfig] = PrivateAttr(default_factory=dict)

    def edge_for_topic(self, topic: str) -> "EdgeConfig | None":
        """The relationship type that reads ``topic``, or ``None``."""
        return self._edges_by_topic.get(topic)

    @model_validator(mode="after")
    def validate_edge_endpoints(self) -> "GraphSchema":
        """Enforce unique types and bind each edge to declared required keys."""
        labels = [n.label for n in self.nodes]
        if len(labels) != len(set(labels)):
            raise ValueError("Duplicate node labels detected in schema.")
        types = [e.type for e in self.edges]
        if len(types) != len(set(types)):
            raise ValueError("Duplicate edge types detected in schema.")

        owners: dict[str, str] = {}
        for kind, name, topic in (
            *(("node", node.label, node.topic) for node in self.nodes),
            *(("edge", edge.type, topic) for edge in self.edges for topic in edge.topics),
        ):
            if not isinstance(topic, str) or not topic.strip():
                raise ValueError(f"{kind.capitalize()} '{name}' topic must not be blank")
            if topic in owners:
                raise ValueError(
                    f"Topic '{topic}' is declared by both {owners[topic]} and {kind} '{name}'; "
                    "every node label and relationship type needs its own Kafka topic"
                )
            owners[topic] = f"{kind} '{name}'"
        self._edges_by_topic = {t: e for e in self.edges for t in e.topics}
        if self.loading.coordination.topic in owners:
            raise ValueError(
                f"Coordination topic '{self.loading.coordination.topic}' is also the data topic of "
                f"{owners[self.loading.coordination.topic]}"
            )

        # Also guard every DLQ topic against the coordination and internal
        # control topics, not only against node/edge data topics (the
        # reverse direction, coordination-vs-data, is already covered above).
        owners_with_coordination = dict(owners)
        owners_with_coordination.setdefault(
            self.loading.coordination.topic, "the coordination topic"
        )
        owners_with_coordination.setdefault(CONTROL_TOPIC, "the internal control topic")

        dlq_owners: dict[str, str] = {}
        # P5-S4 Story 1: every resolved parked_topic_name/parked-pairs index
        # topic joins the same collision set as the DLQ topics themselves —
        # nodes never resolve either (NodeConfig rejects the edge-only
        # parked_topic field, so those two are only meaningful for edges).
        # Edges resolve names from dead_letter_entity: the topic (single-topic,
        # unchanged) or the type name (multi-topic, one DLQ per type).
        dlq_candidates = (
            *(("node", node.label, node.topic, node.dead_letter, "Dead-letter") for node in self.nodes),
            *(("edge", edge.type, edge.dead_letter_entity, edge.dead_letter, "Dead-letter") for edge in self.edges),
            *(("edge", edge.type, edge.dead_letter_entity, edge.dead_letter, "Parking-lot") for edge in self.edges),
            *(("edge", edge.type, edge.dead_letter_entity, edge.dead_letter, "Parked-pair index") for edge in self.edges),
        )
        for kind, name, topic, dead_letter, topic_kind_label in dlq_candidates:
            if topic_kind_label == "Dead-letter":
                dlq_topic = dead_letter_topic_name(topic, dead_letter)
            elif topic_kind_label == "Parking-lot":
                dlq_topic = parked_topic_name(topic, dead_letter)
            else:
                dlq_topic = parked_pair_index_topic_name(topic, dead_letter)
            if dlq_topic is None:
                continue
            if not KAFKA_TOPIC_NAME_RE.fullmatch(dlq_topic):
                raise ValueError(
                    f"{topic_kind_label} topic '{dlq_topic}' for {kind} '{name}' is not a legal "
                    "Kafka topic name (letters, digits, '.', '_', '-' only, 1-249 characters)"
                )
            if dlq_topic in owners_with_coordination:
                this_entity = f"{kind} '{name}'"
                collides_with = owners_with_coordination[dlq_topic]
                description = (
                    "its own source topic" if collides_with == this_entity else collides_with
                )
                raise ValueError(
                    f"{topic_kind_label} topic '{dlq_topic}' for {kind} '{name}' collides with "
                    f"{description}; every dead-letter/parking-lot/parked-pair-index topic must "
                    "be distinct from every node/edge/coordination/control topic"
                )
            if dlq_topic in dlq_owners:
                raise ValueError(
                    f"{topic_kind_label} topic '{dlq_topic}' is declared by both "
                    f"{dlq_owners[dlq_topic]} and {kind} '{name}'; every entity's dead-letter/"
                    "parking-lot/parked-pair-index topic (explicit or defaulted) must be distinct"
                )
            dlq_owners[dlq_topic] = f"{kind} '{name}'"

        if "mode" in self.loading.model_fields_set:
            require_no_precedence_in_stream(self, self.loading.mode)
        for message in arrival_conflict_warnings(self):
            logger.warning(message)

        nodes_by_label = {node.label: node for node in self.nodes}
        for edge in self.edges:
            pair_configs: dict = {}
            for pair in edge.endpoint_pairs:
                # Single-pair messages are unchanged; multi-pair ones also name the pair's topics.
                where = "" if not edge.is_multi_pair else (
                    f" topic '{pair.topics[0]}'" if len(pair.topics) == 1 else f" topics {list(pair.topics)}"
                )
                for endpoint_name, label, key_property in (
                    ("source", pair.source_label, pair.source_key_property),
                    ("target", pair.target_label, pair.target_key_property),
                ):
                    node = nodes_by_label.get(label)
                    if node is None:
                        raise ValueError(
                            f"Edge '{edge.type}'{where} {endpoint_name} label '{label}' "
                            "does not name a declared node"
                        )
                    if key_property != node.key_property:
                        raise ValueError(
                            f"Edge '{edge.type}'{where} {endpoint_name}_key_property "
                            f"'{key_property}' must match node '{label}' key_property "
                            f"'{node.key_property}'"
                        )
                    property_config = node.properties.get(key_property)
                    if property_config is None or not property_config.required:
                        raise ValueError(
                            f"Edge '{edge.type}'{where} {endpoint_name}_key_property "
                            f"'{key_property}' must be a required property of node '{label}'"
                        )
                pair_configs[pair.labels] = (
                    nodes_by_label[pair.source_label].properties[pair.source_key_property],
                    nodes_by_label[pair.target_label].properties[pair.target_key_property],
                )

            if edge.is_multi_pair:
                edge._bind_pair_key_configs(pair_configs)
            else:
                edge.bind_endpoint_properties(*pair_configs[edge.single_pair.labels])
        return self

    def pair_for_topic(self, topic: str) -> "tuple[EdgeConfig, EndpointPair] | None":
        """The relationship type that reads ``topic`` and the label pair the topic binds, or ``None``."""
        edge = self._edges_by_topic.get(topic)
        return None if edge is None else (edge, edge.pair_for_topic(topic))
