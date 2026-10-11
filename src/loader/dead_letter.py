"""Dead-letter queue envelope, wire codec, producer and topic provisioning.

Phase 5 Slice 2 replaces ``RejectionSink``'s fsync'd JSONL file with a Kafka
DLQ topic as the durable home for malformed node and relationship records.
This module is the shared foundation every later story in the slice (and
Slice 3's row-level missing-endpoint DLQ) builds on:

* :class:`DeadLetterEnvelope` / :func:`encode_envelope` / :func:`decode_envelope`
  (Story 1, this module today) — a versioned, self-describing, replayable
  record of one failure. "Versioned" is a two-version *decode* contract as
  of P5-S3 Story 2: a schema-version-1 payload (P5-S2's original shape, no
  ``retry_after_ms``) and a schema-version-2 payload (adds
  ``retry_after_ms``, required for ``failure_class`` other than
  ``"malformed"``) both decode; :func:`encode_envelope` only ever produces
  the newest version (``SCHEMA_VERSION``). A future version 3 should extend
  this same "old versions still decode, only the newest encodes" pattern.
* :func:`dead_letter_topic_name` (Story 2) — defined in
  ``src/models/schema.py`` (it validates ``DeadLetterConfig`` collisions at
  schema-load time, so it needs to live beside that model) and re-exported
  here so loader code has one import path. Keeping this module's dependency
  on the model one-directional (``dead_letter.py`` -> ``schema.py``, never
  the reverse) matters once this module also imports Kafka clients and
  ``RejectionSink``-adjacent loader code below: ``schema.py`` must stay
  importable by every schema-only consumer (``auto_settings``,
  ``cypher_generator``, ``schema_loader``, the CLI status path) without
  pulling in ``confluent_kafka`` at all.
* :class:`DeadLetterProducer` / :func:`ensure_dead_letter_topic` /
  :func:`on_dlq_routed` (Story 3, this module today) — a fail-closed
  publisher, idempotent topic provisioning with unlimited retention, and a
  dependency-free observability seam.
* :class:`MalformedRecordSink` / :class:`RejectionSink` /
  :class:`DeadLetterRecordSink` / :func:`build_malformed_record_sink`
  (Stories 4/5, this module today) — the loader-facing sink abstraction
  shared by ``node_loader.py`` and ``edge_loader.py``. ``RejectionSink``
  moved here from ``node_loader.py`` (which re-exports it for backward
  compatibility) so this module can offer both sinks and the factory that
  picks between them without ``node_loader.py`` and this module importing
  each other in a cycle.
* :func:`ensure_parked_pair_index_topic` / :meth:`DeadLetterRecordSink.reroute_or_park`
  / :func:`on_parked` (P5-S4 Story 2) — the parking-lot half of the retry
  consumer: a compacted topic for the cross-flush parked-pair index
  (``src/loader/parked_pair_index.py`` reads/writes it), and a sink method
  that republishes a DLQ'd row with an incremented attempt count or moves
  it to the parking-lot topic once ``max_retry_attempts`` is reached. The
  parking-lot topic itself reuses :func:`ensure_dead_letter_topic` verbatim
  (append-only, unlimited retention) — no new provisioning function needed
  for it.
"""
from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
import datetime as _datetime
import json
import logging
import math
import os
from pathlib import Path
import time
from typing import TYPE_CHECKING, Callable, Literal, Mapping, Protocol, Sequence

from src.models.schema import dead_letter_topic_name  # noqa: F401 - re-exported

if TYPE_CHECKING:
    # Import-cycle-free: src.loader.edge_records never imports this module.
    # Only used for a type annotation (route_unresolved_endpoint's `record`
    # parameter, P5-S3 Story 2) -- no runtime dependency added.
    from src.loader.edge_records import EdgeRecord

logger = logging.getLogger(__name__)


# ── Envelope schema (Story 1; versioned to 2 in P5-S3 Story 2) ──────────────
#
# Versioning contract (the first this module has ever needed): OLD versions
# stay decodable forever, only the NEWEST version is ever encoded. A schema
# version already sitting in a production DLQ topic (P5-S2 shipped v1 with
# no retry_after_ms) must keep decoding after this module moves on to a
# newer version. A future v3 should extend this same pattern (an
# `_ENVELOPE_FIELDS_V<N>` tuple plus a version-dispatch branch in
# decode_envelope), not invent a new one.

SCHEMA_VERSION = 2  # was 1 (P5-S2); v1 payloads still decode via _decode dispatch below.

# The complete, fixed set of DLQ failure classes. "malformed" is the only one
# P5-S2 ever produces; "missing_endpoint" and "ambiguous_endpoint" are
# produced starting in P5-S3 for a relationship row whose endpoint(s) did not
# resolve within one write attempt.
FAILURE_CLASSES = ("malformed", "missing_endpoint", "ambiguous_endpoint")

ENTITY_KINDS = ("node", "edge")

# v1's field set (P5-S2), unchanged forever -- a v1 wire payload must contain
# exactly these fields, no more, no less (no retry_after_ms).
_ENVELOPE_FIELDS = (
    "schema_version", "failure_class", "entity_kind", "entity_name",
    "original_topic", "original_partition", "original_offset",
    "key_bytes", "value_bytes", "reason_code", "error_text",
    "loader_identity", "first_failure_at_ms", "attempt_count",
    "dlq_routed_at_ms",
)
# v2's field set (P5-S3): v1's fields plus retry_after_ms. Every envelope
# this module encodes from this slice forward uses this field set.
_ENVELOPE_FIELDS_V2 = _ENVELOPE_FIELDS + ("retry_after_ms",)
_LOADER_IDENTITY_FIELDS = ("run_id", "replica_id", "kind")


class DeadLetterEnvelopeError(ValueError):
    """Raised when a dead-letter envelope is malformed, unsafe, or unreadable."""


@dataclass(frozen=True)
class LoaderIdentity:
    """The loader replica that first observed a dead-lettered record.

    Bounded, structured identity (never a free-text container name) so this
    stays as bounded as Phase 6's metric labels must be.
    """

    run_id: str
    replica_id: str
    kind: Literal["node", "edge"]


@dataclass(frozen=True)
class DeadLetterEnvelope:
    """A versioned, self-describing, replayable record of one failed message.

    Carries everything needed to reconstruct the original Kafka message
    (``key_bytes``/``value_bytes``, unmodified) and to know why and where it
    failed, without depending on any schema registry or this library's code
    to be read back.
    """

    schema_version: int
    failure_class: str
    entity_kind: str
    entity_name: str
    original_topic: str
    original_partition: int
    original_offset: int
    key_bytes: bytes | None
    value_bytes: bytes | None
    reason_code: str
    error_text: str
    loader_identity: LoaderIdentity
    first_failure_at_ms: int
    attempt_count: int
    dlq_routed_at_ms: int
    retry_after_ms: int | None = None
    """Earliest wall-clock ms P5-S4's retry consumer may re-attempt this row.

    ``None`` only for ``failure_class="malformed"`` (P5-Q2: a malformed
    record is terminal, never auto-retried -- there is nothing to compute a
    retry time for). Required (a nonnegative int, ``>= first_failure_at_ms``)
    for ``"missing_endpoint"``/``"ambiguous_endpoint"``. Absent entirely on a
    decoded schema-version-1 payload, which predates this field.
    """


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DeadLetterEnvelopeError(message)


def _validated_nonnegative_int(value: object, field: str) -> int:
    # Reject bool explicitly: bool is an int subclass in Python, and a stray
    # ``True``/``False`` must never silently become 1/0 on the wire.
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeadLetterEnvelopeError(f"{field} must be a nonnegative integer")
    return value


def _validated_nonblank_str(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeadLetterEnvelopeError(f"{field} must be a nonblank string")
    return value


def _validated_optional_bytes(value: object, field: str) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, bytes):
        raise DeadLetterEnvelopeError(f"{field} must be bytes or None")
    return value


def _validated_loader_identity(value: object) -> LoaderIdentity:
    if isinstance(value, LoaderIdentity):
        run_id, replica_id, kind = value.run_id, value.replica_id, value.kind
    elif isinstance(value, Mapping):
        if set(value) != set(_LOADER_IDENTITY_FIELDS):
            raise DeadLetterEnvelopeError("loader_identity has missing or unexpected field")
        run_id, replica_id, kind = value["run_id"], value["replica_id"], value["kind"]
    else:
        raise DeadLetterEnvelopeError("loader_identity must be a LoaderIdentity or mapping")
    run_id = _validated_nonblank_str(run_id, "loader_identity.run_id")
    replica_id = _validated_nonblank_str(replica_id, "loader_identity.replica_id")
    _require(kind in ENTITY_KINDS, "loader_identity.kind must be 'node' or 'edge'")
    return LoaderIdentity(run_id=run_id, replica_id=replica_id, kind=kind)


def _validated_envelope(
    *,
    schema_version: object,
    failure_class: object,
    entity_kind: object,
    entity_name: object,
    original_topic: object,
    original_partition: object,
    original_offset: object,
    key_bytes: object,
    value_bytes: object,
    reason_code: object,
    error_text: object,
    loader_identity: object,
    first_failure_at_ms: object,
    attempt_count: object,
    dlq_routed_at_ms: object,
    retry_after_ms: object = None,
) -> DeadLetterEnvelope:
    """Validate every field of a candidate envelope, raising on the first defect.

    ``schema_version`` is a *range* concept, not a single equality check:
    every version this module has ever shipped (1 and 2 today) is accepted
    here, since ``decode_envelope`` calls this after it has already picked
    the right field set for that version. ``encode_envelope`` always passes
    ``SCHEMA_VERSION`` (the newest), so this never actually sees anything
    older than that from the encode path.
    """
    schema_version = _validated_nonnegative_int(schema_version, "schema_version")
    _require(schema_version in (1, 2), "schema_version must be 1 or 2")
    _require(
        isinstance(failure_class, str) and failure_class in FAILURE_CLASSES,
        f"failure_class must be one of {FAILURE_CLASSES}",
    )
    _require(isinstance(entity_kind, str) and entity_kind in ENTITY_KINDS,
              "entity_kind must be 'node' or 'edge'")
    entity_name = _validated_nonblank_str(entity_name, "entity_name")
    original_topic = _validated_nonblank_str(original_topic, "original_topic")
    original_partition = _validated_nonnegative_int(original_partition, "original_partition")
    original_offset = _validated_nonnegative_int(original_offset, "original_offset")
    key_bytes = _validated_optional_bytes(key_bytes, "key_bytes")
    value_bytes = _validated_optional_bytes(value_bytes, "value_bytes")
    reason_code = _validated_nonblank_str(reason_code, "reason_code")
    if not isinstance(error_text, str):
        raise DeadLetterEnvelopeError("error_text must be a string")
    identity = _validated_loader_identity(loader_identity)
    _require(
        identity.kind == entity_kind,
        "loader_identity.kind must match entity_kind: a node loader cannot "
        "produce an edge-kind envelope and vice versa",
    )
    first_failure_at_ms = _validated_nonnegative_int(first_failure_at_ms, "first_failure_at_ms")
    attempt_count = _validated_nonnegative_int(attempt_count, "attempt_count")
    dlq_routed_at_ms = _validated_nonnegative_int(dlq_routed_at_ms, "dlq_routed_at_ms")
    if failure_class == "malformed":
        _require(
            retry_after_ms is None,
            "retry_after_ms must be None for failure_class='malformed' "
            "(a malformed record is terminal and is never auto-retried)",
        )
    else:
        _require(
            retry_after_ms is not None,
            f"retry_after_ms is required for failure_class={failure_class!r}",
        )
        retry_after_ms = _validated_nonnegative_int(retry_after_ms, "retry_after_ms")
        _require(
            retry_after_ms >= first_failure_at_ms,
            "retry_after_ms must be >= first_failure_at_ms",
        )
    return DeadLetterEnvelope(
        schema_version=schema_version,
        failure_class=failure_class,
        entity_kind=entity_kind,
        entity_name=entity_name,
        original_topic=original_topic,
        original_partition=original_partition,
        original_offset=original_offset,
        key_bytes=key_bytes,
        value_bytes=value_bytes,
        reason_code=reason_code,
        error_text=error_text,
        loader_identity=identity,
        first_failure_at_ms=first_failure_at_ms,
        attempt_count=attempt_count,
        dlq_routed_at_ms=dlq_routed_at_ms,
        retry_after_ms=retry_after_ms,
    )


def _b64_encode(value: bytes | None) -> str | None:
    return None if value is None else base64.b64encode(value).decode("ascii")


def _b64_decode(value: object, field: str) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DeadLetterEnvelopeError(f"{field} must be a base64 string or null")
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise DeadLetterEnvelopeError(f"{field} is not valid base64") from exc


def encode_envelope(envelope: DeadLetterEnvelope) -> bytes:
    """Return the canonical UTF-8 JSON wire form of a fully validated envelope.

    Encoding always re-validates every field (never trusts an
    already-constructed dataclass instance blindly) so a caller can never
    publish a malformed envelope, and encodes twice to the identical bytes
    for the same input (canonical: sorted keys, no whitespace).

    Always writes ``schema_version=SCHEMA_VERSION`` (2) and includes
    ``retry_after_ms`` in the wire payload (``null`` for
    ``failure_class="malformed"``). ``envelope.schema_version`` must equal
    ``SCHEMA_VERSION`` exactly (strict, unlike ``decode_envelope``'s
    two-version acceptance) — this module never *re-encodes* an old-version
    envelope; it only ever decodes one. Building a fresh envelope to publish
    always uses ``SCHEMA_VERSION`` for this field (every constructor call
    site in this module already does).
    """
    schema_version = _validated_nonnegative_int(envelope.schema_version, "schema_version")
    _require(schema_version == SCHEMA_VERSION, f"schema_version must be {SCHEMA_VERSION}")
    valid = _validated_envelope(
        schema_version=schema_version,
        failure_class=envelope.failure_class,
        entity_kind=envelope.entity_kind,
        entity_name=envelope.entity_name,
        original_topic=envelope.original_topic,
        original_partition=envelope.original_partition,
        original_offset=envelope.original_offset,
        key_bytes=envelope.key_bytes,
        value_bytes=envelope.value_bytes,
        reason_code=envelope.reason_code,
        error_text=envelope.error_text,
        loader_identity=envelope.loader_identity,
        first_failure_at_ms=envelope.first_failure_at_ms,
        attempt_count=envelope.attempt_count,
        dlq_routed_at_ms=envelope.dlq_routed_at_ms,
        retry_after_ms=envelope.retry_after_ms,
    )
    payload = {
        "schema_version": valid.schema_version,
        "failure_class": valid.failure_class,
        "entity_kind": valid.entity_kind,
        "entity_name": valid.entity_name,
        "original_topic": valid.original_topic,
        "original_partition": valid.original_partition,
        "original_offset": valid.original_offset,
        "key_bytes": _b64_encode(valid.key_bytes),
        "value_bytes": _b64_encode(valid.value_bytes),
        "reason_code": valid.reason_code,
        "error_text": valid.error_text,
        "loader_identity": {
            "run_id": valid.loader_identity.run_id,
            "replica_id": valid.loader_identity.replica_id,
            "kind": valid.loader_identity.kind,
        },
        "first_failure_at_ms": valid.first_failure_at_ms,
        "attempt_count": valid.attempt_count,
        "dlq_routed_at_ms": valid.dlq_routed_at_ms,
        "retry_after_ms": valid.retry_after_ms,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def decode_envelope(payload: bytes) -> DeadLetterEnvelope:
    """Validate and parse a wire-form envelope, raising ``DeadLetterEnvelopeError``.

    Never raises a raw ``KeyError``/``TypeError``/``json.JSONDecodeError`` —
    every defect is reported as a ``DeadLetterEnvelopeError`` so the replay
    command (Story 7) can read a DLQ topic back deterministically and fail
    closed on anything it does not understand.

    Dispatches on the wire payload's own ``schema_version`` *before* the
    strict field-set check: a schema-version-1 payload (P5-S2, no
    ``retry_after_ms``) must still decode against v1's exact field set, and
    a schema-version-2 payload must decode against v2's (with
    ``retry_after_ms``). A v1 payload carrying a stray ``retry_after_ms`` key
    is rejected (it would mean the v1 field-set contract was violated), and
    likewise a v2 payload missing it.
    """
    if not isinstance(payload, bytes):
        raise DeadLetterEnvelopeError("envelope payload must be bytes")

    def no_duplicates(pairs):
        result: dict = {}
        for key, value in pairs:
            if key in result:
                raise DeadLetterEnvelopeError("envelope payload has a duplicate key")
            result[key] = value
        return result

    try:
        data = json.loads(payload.decode("utf-8"), object_pairs_hook=no_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeadLetterEnvelopeError("envelope payload is not valid JSON") from exc
    except RecursionError as exc:
        raise DeadLetterEnvelopeError("envelope payload is too deeply nested") from exc
    if not isinstance(data, dict):
        raise DeadLetterEnvelopeError("envelope payload must be a JSON object")

    version = data.get("schema_version")
    if version == 1:
        if set(data) != set(_ENVELOPE_FIELDS):
            raise DeadLetterEnvelopeError("envelope payload has a missing or unexpected field")
        retry_after_ms = None
    else:
        if set(data) != set(_ENVELOPE_FIELDS_V2):
            raise DeadLetterEnvelopeError("envelope payload has a missing or unexpected field")
        retry_after_ms = data["retry_after_ms"]

    loader_identity_raw = data["loader_identity"]
    if not isinstance(loader_identity_raw, dict):
        raise DeadLetterEnvelopeError("loader_identity must be an object")

    return _validated_envelope(
        schema_version=data["schema_version"],
        failure_class=data["failure_class"],
        entity_kind=data["entity_kind"],
        entity_name=data["entity_name"],
        original_topic=data["original_topic"],
        original_partition=data["original_partition"],
        original_offset=data["original_offset"],
        key_bytes=_b64_decode(data["key_bytes"], "key_bytes"),
        value_bytes=_b64_decode(data["value_bytes"], "value_bytes"),
        reason_code=data["reason_code"],
        error_text=data["error_text"],
        loader_identity=loader_identity_raw,
        first_failure_at_ms=data["first_failure_at_ms"],
        attempt_count=data["attempt_count"],
        dlq_routed_at_ms=data["dlq_routed_at_ms"],
        retry_after_ms=retry_after_ms,
    )


# ── Producer, topic provisioning, observability hook (Story 3) ──────────────

# Base64 inflates a payload by ~4/3, and the envelope adds a small fixed JSON
# overhead (topic/partition/offset/timestamps/identity). 8 MiB comfortably
# covers a source message up to Kafka's common 1 MiB broker default with
# headroom, without this code having to probe the source topic's own
# max.message.bytes (which may not even be set — brokers fall back to the
# cluster default, and it can differ per topic). The check in
# DeadLetterProducer.publish covers only the envelope value; the Kafka
# record's key (the original message key, carried unmodified) adds a little
# more on top, which the "+ 65536" headroom below absorbs for ordinary keys.
DEAD_LETTER_MAX_ENVELOPE_BYTES = 8 * 1024 * 1024

# The "+ 65536" mirrors edge_planner.chunk_topic_config's own headroom
# constant for the same reason: broker-side framing overhead (plus the
# record key) beyond the envelope value itself. All three of the following
# must stay consistent so the pre-flight check in DeadLetterProducer.publish
# is never stricter than what the client or the topic will actually accept:
# DEAD_LETTER_MAX_ENVELOPE_BYTES <= producer's "message.max.bytes"
#                                 <= topic's "max.message.bytes".
DEAD_LETTER_TOPIC_CONFIG: dict[str, str] = {
    "retention.ms": "-1",
    "retention.bytes": "-1",
    "cleanup.policy": "delete",
    "max.message.bytes": str(DEAD_LETTER_MAX_ENVELOPE_BYTES + 65536),
}

DEAD_LETTER_PRODUCER_CONFIG: dict[str, object] = {
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
    "delivery.timeout.ms": 120_000,
    # Without this, librdkafka's own default (1,000,000 bytes) rejects a
    # base64-inflated envelope well under DEAD_LETTER_MAX_ENVELOPE_BYTES,
    # synchronously from produce(), before the topic's own limit is ever
    # reached.
    "message.max.bytes": DEAD_LETTER_MAX_ENVELOPE_BYTES + 65536,
    # librdkafka's Producer defaults this to True (unlike the Consumer,
    # which defaults to False). Many dev/CI Kafka clusters set
    # auto.create.topics.enable=true broker-side; if a loader ever produces
    # a DLQ message before ensure_dead_letter_topic has run (Story 6 runs it
    # at CLI launch, but a loader started standalone would not wait for
    # that), an auto-created topic gets the broker's default retention
    # (commonly 7 days), silently violating the "never auto-deleted"
    # decision. Disabling this makes a missing DLQ topic a loud, fail-closed
    # publish error instead.
    "allow.auto.create.topics": False,
}


class DeadLetterPublishError(RuntimeError):
    """Raised when a dead-letter envelope cannot be durably published."""


# kind, name, topic, failure_class
DlqRoutedListener = Callable[[str, str, str, str], None]


def on_dlq_routed(kind: str, name: str, topic: str, failure_class: str) -> None:
    """No-op observability seam fired exactly once per acknowledged DLQ publish.

    Phase 6 (P6-S2) replaces or wraps this module attribute to increment
    ``graph_loader_dlq_routed_total{kind,name}``. This is deliberately not a
    metrics exporter itself — no dependency, no port, no ``/metrics``
    endpoint here — just a dependency-free callable seam, like a
    :mod:`logging` handler. :class:`DeadLetterProducer` looks this attribute
    up on the module at call time (not at construction time), so replacing
    ``dead_letter.on_dlq_routed`` after a producer already exists still
    takes effect.
    """
    return None


# kind, name, parked_topic
OnParkedListener = Callable[[str, str, str], None]


def on_parked(kind: str, name: str, parked_topic: str) -> None:
    """No-op observability seam fired exactly once per acknowledged parking-lot publish.

    P5-S4's Phase 6 metrics wiring (P6-S2's catalogue) replaces this module
    attribute to increment ``graph_loader_parked_total{kind,name}``. Fired
    directly by :meth:`DeadLetterRecordSink.reroute_or_park` after a
    successful parking-lot publish — never by :class:`DeadLetterProducer`
    itself, and never for a re-DLQ publish (that publish fires the
    pre-existing :func:`on_dlq_routed` instead; the two are distinct
    conceptual events and must never both fire for the same outcome).
    """
    return None


class DeadLetterProducer:
    """Synchronously publish a dead-letter envelope, acknowledged or not at all.

    Wraps a caller-owned ``confluent_kafka.Producer`` (same ownership model
    as ``NodeLoader``/``EdgeLoader``'s existing producers: constructed once
    in ``main()``, flushed/closed in the ``finally`` block). Never shares a
    ``Producer`` instance with control-topic publishing
    (``NodeLoader._publish_control``) — mixing their ``flush()`` undelivered
    counts would misattribute one stream's failure to the other.
    """

    def __init__(self, producer) -> None:
        self._producer = producer

    def publish(
        self, topic: str, envelope: DeadLetterEnvelope, *, fire_dlq_routed_hook: bool = True,
    ) -> None:
        """Synchronously publish and block until Kafka acknowledges.

        Raises :class:`DeadLetterEnvelopeError` if ``envelope`` itself is
        invalid (checked first, before any Kafka call), or
        :class:`DeadLetterPublishError` on any delivery failure, an
        unflushed message, or an envelope over the DLQ size cap. Calls
        :func:`on_dlq_routed` exactly once, and only after a proven ack —
        never before, since a hook fired before the ack would claim a
        routing that might still fail. A hook exception is caught and
        logged, never raised: observability must never change delivery
        semantics — the publish already succeeded by the time the hook runs.

        ``fire_dlq_routed_hook=False`` (P5-S4 Story 2): used exclusively by
        :meth:`DeadLetterRecordSink.reroute_or_park`'s parking-lot publish —
        a park is a *distinct* observability event (``on_parked``, fired by
        the caller directly, not by this method) from an ordinary DLQ
        publish, and must never also increment
        ``graph_loader_dlq_routed_total`` for the same conceptual event.
        Every other call site keeps the default (fires the hook).
        """
        encoded = encode_envelope(envelope)
        if len(encoded) > DEAD_LETTER_MAX_ENVELOPE_BYTES:
            raise DeadLetterPublishError(
                f"envelope for {envelope.entity_kind} '{envelope.entity_name}' "
                f"topic={envelope.original_topic} partition={envelope.original_partition} "
                f"offset={envelope.original_offset} is {len(encoded)} bytes, over the "
                f"{DEAD_LETTER_MAX_ENVELOPE_BYTES}-byte DLQ envelope limit"
            )
        delivery_error: Exception | None = None

        def on_delivery(error, _message) -> None:
            nonlocal delivery_error
            if error is not None:
                delivery_error = error

        try:
            self._producer.produce(
                topic, key=envelope.key_bytes, value=encoded, on_delivery=on_delivery,
            )
            undelivered_count = self._producer.flush()
        except Exception as exc:
            raise DeadLetterPublishError(f"DLQ publish to {topic} failed: {exc}") from exc
        if undelivered_count:
            raise DeadLetterPublishError(
                f"DLQ publish to {topic} has {undelivered_count} undelivered message(s)"
            )
        if delivery_error is not None:
            raise DeadLetterPublishError(f"DLQ publish to {topic} failed: {delivery_error}")
        if not fire_dlq_routed_hook:
            return
        try:
            on_dlq_routed(envelope.entity_kind, envelope.entity_name, topic, envelope.failure_class)
        except Exception:
            logger.exception(
                "on_dlq_routed listener raised after a successful DLQ publish to %s "
                "(entity_kind=%s entity_name=%s) — ignoring, the publish already succeeded",
                topic, envelope.entity_kind, envelope.entity_name,
            )


    def publish_many(
        self, items: "Sequence[tuple[str, DeadLetterEnvelope, bool]]",
    ) -> "list[Exception | None]":
        """Publish many envelopes with ONE ``flush()``; return one result per item.

        ``items``: ``(topic, envelope, fire_dlq_routed_hook)``. The result
        list is positional: ``None`` for an item whose own delivery was
        acknowledged (its routed hook has fired, exactly as :meth:`publish`
        would), otherwise the exception :meth:`publish` would have raised
        for it (invalid/oversize envelope, failed delivery, no delivery
        report). A per-item failure never raises, so the caller can resolve
        the acknowledged items and leave the rest unresolved. Same
        durability contract as :meth:`publish` -- an item counts as
        published only after its own proven ack -- at one broker round trip
        per call instead of one per item.
        """
        results: list[Exception | None] = [None] * len(items)
        delivered: set[int] = set()
        failed: dict[int, Exception] = {}
        produced: list[int] = []
        for position, (topic, envelope, _hook) in enumerate(items):
            try:
                encoded = encode_envelope(envelope)
                if len(encoded) > DEAD_LETTER_MAX_ENVELOPE_BYTES:
                    raise DeadLetterPublishError(
                        f"envelope for {envelope.entity_kind} '{envelope.entity_name}' "
                        f"topic={envelope.original_topic} partition={envelope.original_partition} "
                        f"offset={envelope.original_offset} is {len(encoded)} bytes, over the "
                        f"{DEAD_LETTER_MAX_ENVELOPE_BYTES}-byte DLQ envelope limit"
                    )

                def on_delivery(error, _message, position=position, topic=topic) -> None:
                    if error is None:
                        delivered.add(position)
                    else:
                        failed[position] = DeadLetterPublishError(f"DLQ publish to {topic} failed: {error}")

                self._producer.produce(topic, key=envelope.key_bytes, value=encoded, on_delivery=on_delivery)
                produced.append(position)
            except (DeadLetterEnvelopeError, DeadLetterPublishError) as exc:
                results[position] = exc
            except Exception as exc:
                results[position] = DeadLetterPublishError(f"DLQ publish to {topic} failed: {exc}")
        if produced:
            flush_error: Exception | None = None
            try:
                self._producer.flush()
            except Exception as exc:
                flush_error = DeadLetterPublishError(f"DLQ publish flush failed: {exc}")
            for position in produced:
                if position in failed:
                    results[position] = failed[position]
                elif position not in delivered:
                    results[position] = flush_error or DeadLetterPublishError(
                        f"DLQ publish to {items[position][0]} has an undelivered message"
                    )
        for position, (topic, envelope, fire_hook) in enumerate(items):
            if results[position] is not None or not fire_hook:
                continue
            try:
                on_dlq_routed(envelope.entity_kind, envelope.entity_name, topic, envelope.failure_class)
            except Exception:
                logger.exception(
                    "on_dlq_routed listener raised after a successful DLQ publish to %s "
                    "(entity_kind=%s entity_name=%s) -- ignoring, the publish already succeeded",
                    topic, envelope.entity_kind, envelope.entity_name,
                )
        return results


def _source_partition_count(metadata, sources: "Sequence[str]", *, kind: str, topic: str) -> int:
    """Partition count for a new DLQ-family topic: the maximum over ``sources``.

    A multi-topic relationship type has one DLQ for all its topics (v7-S4), so
    the DLQ gets as many partitions as its widest source topic. A source with
    no partition metadata yet is skipped with a warning; with none at all the
    fallback is 1 partition (also warned).
    """
    counts = []
    for source in sources:
        item = metadata.topics.get(source)
        partitions = getattr(item, "partitions", None) if item is not None else None
        if partitions:
            counts.append(len(partitions))
        elif len(sources) > 1:
            logger.warning(
                "%s topic %s: source topic %s has no partition metadata yet; sizing from the "
                "type's other topics", kind, topic, source,
            )
    if counts:
        return max(counts)
    logger.warning(
        "%s topic %s: source topic(s) %s have no partition metadata yet; creating the "
        "%s topic with 1 partition (this is a fallback and may not match the source "
        "topic's eventual partition count)",
        kind, topic, ", ".join(sources), kind,
    )
    return 1


def _warn_if_narrower(metadata, topic: str, sources: "Sequence[str]") -> None:
    """An existing DLQ is never resized (v7-S4 review S7): say so when it has fewer
    partitions than the widest of its source topics (for example after a wider topic
    was added to the type). Correctness is unaffected (a pair's records still share
    one DLQ partition); only the retry consumer's parallelism is lower."""
    existing = getattr(metadata.topics.get(topic), "partitions", None) or {}
    widest = max(
        (len(getattr(metadata.topics.get(source), "partitions", None) or {}) for source in sources), default=0,
    )
    if existing and widest > len(existing):
        logger.warning(
            "DLQ topic %s has %d partition(s), fewer than its widest source topic (%d, of %s); it is "
            "never resized automatically -- add partitions with kafka-topics --alter if retry "
            "parallelism matters",
            topic, len(existing), widest, ", ".join(sources),
        )


def _resolved_sources(source_topic: "str | None", source_topics: "Sequence[str] | None") -> list[str]:
    if (source_topic is None) == (source_topics is None):
        raise ValueError("give exactly one of source_topic and source_topics")
    sources = [source_topic] if source_topic is not None else list(source_topics)
    if not sources or any(not isinstance(s, str) or not s.strip() for s in sources):
        raise ValueError("source topics must be a non-empty list of nonblank names")
    return sources


def ensure_dead_letter_topic(
    bootstrap_servers: str, topic: str, *, source_topic: str | None = None,
    source_topics: "Sequence[str] | None" = None, admin_factory=None, timeout: float = 30.0,
) -> None:
    """Create a DLQ topic with unlimited retention, unless it already exists.

    Mirrors ``src.cli._ensure_control_topic`` and
    ``src.loader.edge_planner.create_chunk_topic``: ``list_topics`` first,
    no-op if the topic is already present (never overwrites an existing
    topic's config), else ``create_topics`` with
    :data:`DEAD_LETTER_TOPIC_CONFIG`, tolerating a concurrent
    ``TOPIC_ALREADY_EXISTS`` from a racing sibling replica or CLI launch. If
    the topic is already present, its configuration is verified (never
    modified) against the DLQ retention/size policy and this function fails
    closed if it does not comply -- see
    :func:`_verify_existing_dead_letter_topic_config`.

    Unlike ``_ensure_control_topic``'s log-and-continue pattern, this
    function does not swallow a genuine failure (broker unreachable,
    permission denied): Story 6's caller is fail-closed, so any exception
    other than a same-topic creation race propagates to the caller.

    ``source_topics`` (v7-S4): a multi-topic relationship type's one DLQ is
    created with the maximum partition count over all of its topics. Give
    exactly one of ``source_topic`` / ``source_topics``. An existing topic is
    never resized (only its retention/size policy is verified).
    """
    sources = _resolved_sources(source_topic, source_topics)
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient as admin_factory
    from confluent_kafka.admin import NewTopic

    admin = admin_factory({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=timeout)
    if topic in metadata.topics:
        _verify_existing_dead_letter_topic_config(admin, topic, timeout=timeout)
        _warn_if_narrower(metadata, topic, sources)
        return
    partitions = _source_partition_count(metadata, sources, kind="DLQ", topic=topic)
    try:
        futures = admin.create_topics([NewTopic(
            topic, num_partitions=partitions, replication_factor=-1, config=DEAD_LETTER_TOPIC_CONFIG,
        )])
        for future in futures.values():
            future.result(timeout)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise


def _verify_existing_dead_letter_topic_config(admin, topic: str, *, timeout: float) -> None:
    """Fail closed if a pre-existing DLQ topic violates the retention/size policy.

    A DLQ topic can already exist because an operator pre-created it, or
    because the broker auto-created it on first produce (``auto.create.
    topics.enable`` is common on dev clusters) before this function ever
    ran. ``ensure_dead_letter_topic`` never overwrites an existing topic's
    config (by contract), so the only safe response to a policy violation
    -- retention that would eventually delete undelivered failures, or a
    size limit smaller than an envelope this code might publish -- is to
    fail closed here rather than silently accept it.
    """
    from confluent_kafka.admin import ConfigResource

    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    futures = admin.describe_configs([resource])
    config = next(iter(futures.values())).result(timeout)

    def entry(name: str) -> str | None:
        item = config.get(name) if hasattr(config, "get") else None
        return None if item is None else item.value

    retention_ms = entry("retention.ms")
    retention_bytes = entry("retention.bytes")
    max_message_bytes = entry("max.message.bytes")
    problems = []
    if retention_ms != "-1":
        problems.append(f"retention.ms={retention_ms!r} (must be -1: never auto-delete DLQ records)")
    if retention_bytes != "-1":
        problems.append(f"retention.bytes={retention_bytes!r} (must be -1: never auto-delete DLQ records)")
    if max_message_bytes is not None and int(max_message_bytes) < DEAD_LETTER_MAX_ENVELOPE_BYTES:
        problems.append(
            f"max.message.bytes={max_message_bytes!r} "
            f"(must be >= {DEAD_LETTER_MAX_ENVELOPE_BYTES} to accept a full-size envelope)"
        )
    if problems:
        raise RuntimeError(
            f"DLQ topic {topic} already exists but violates the DLQ policy: "
            f"{'; '.join(problems)}. This topic is never overwritten automatically; "
            "fix its configuration, or delete it and rerun so it is recreated correctly."
        )


# ── Parking-lot and parked-pair-index topic provisioning (P5-S4 Story 2) ────
#
# The parking-lot topic itself reuses ensure_dead_letter_topic verbatim (same
# retention.ms=-1/retention.bytes=-1 append-only policy) — no new function
# needed, only a new call site (Story 6) passing parked_topic_name(...)
# instead of dead_letter_topic_name(...). The parked-*pair* index topic below
# is provisioned differently: compacted, not append-only, so its size is
# bounded by the number of distinct pairs currently blocked, not by the
# number of DLQ events ever published (P5-S4 Decisions §6).

PARKED_PAIR_INDEX_TOPIC_CONFIG: dict[str, str] = {
    "cleanup.policy": "compact",
    "min.cleanable.dirty.ratio": "0.1",
    # 5 min tombstone-visibility window: long enough for every consumer of
    # this topic (every ParkedPairIndex replica) to observe a tombstone
    # before compaction physically removes it.
    "delete.retention.ms": "300000",
    # Review finding N3: compaction never touches the *active* segment, and
    # the broker/cluster defaults (segment.ms=7 days, segment.bytes=1 GiB)
    # would leave up to a week's worth of superseded records uncompacted --
    # defeating the "bounded by pairs currently outstanding" claim this
    # topic exists for. 1 hour keeps the active segment small and rotating
    # quickly so compaction can actually reclaim superseded/tombstoned keys.
    "segment.ms": "3600000",
    # A canonical-JSON per-record key (pair + original topic/partition/offset)
    # plus a small marker value never approaches the DLQ envelope's 8 MiB cap.
    "max.message.bytes": "65536",
}


def ensure_parked_pair_index_topic(
    bootstrap_servers: str, topic: str, *, source_topic: str | None = None,
    source_topics: "Sequence[str] | None" = None, admin_factory=None, timeout: float = 30.0,
) -> None:
    """Create the compacted parked-pair index topic, unless it already exists.

    Mirrors :func:`ensure_dead_letter_topic`'s list-then-create-tolerating-
    races shape, but never calls :func:`_verify_existing_dead_letter_topic_config`
    (that function's ``retention.ms=-1`` check does not apply to a compacted
    topic) — :func:`_verify_existing_parked_pair_index_topic_config` confirms
    only ``cleanup.policy=compact`` on an existing topic instead, failing
    closed the same way on a violation.
    """
    sources = _resolved_sources(source_topic, source_topics)
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient as admin_factory
    from confluent_kafka.admin import NewTopic

    admin = admin_factory({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=timeout)
    if topic in metadata.topics:
        _verify_existing_parked_pair_index_topic_config(admin, topic, timeout=timeout)
        return
    partitions = _source_partition_count(metadata, sources, kind="Parked-pair index", topic=topic)
    try:
        futures = admin.create_topics([NewTopic(
            topic, num_partitions=partitions, replication_factor=-1,
            config=PARKED_PAIR_INDEX_TOPIC_CONFIG,
        )])
        for future in futures.values():
            future.result(timeout)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise


def _verify_existing_parked_pair_index_topic_config(admin, topic: str, *, timeout: float) -> None:
    """Fail closed if a pre-existing parked-pair index topic isn't compacted."""
    from confluent_kafka.admin import ConfigResource

    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    futures = admin.describe_configs([resource])
    config = next(iter(futures.values())).result(timeout)

    def entry(name: str) -> str | None:
        item = config.get(name) if hasattr(config, "get") else None
        return None if item is None else item.value

    cleanup_policy = entry("cleanup.policy")
    if cleanup_policy != "compact":
        raise RuntimeError(
            f"Parked-pair index topic {topic} already exists but violates the required "
            f"policy: cleanup.policy={cleanup_policy!r} (must be 'compact'). This topic is "
            "never overwritten automatically; fix its configuration, or delete it and rerun "
            "so it is recreated correctly."
        )


# ── Malformed-record sinks and factory (Stories 4/5) ─────────────────────────


class MalformedRecordSink(Protocol):
    """Where a loader durably routes one malformed node or relationship record."""

    def route(
        self, *, topic: str, partition: int, offset: int,
        key: bytes | None, value: bytes | None, reason: str,
        reason_code: str, node_label: str | None = None,
        edge_type: str | None = None,
    ) -> None: ...


def _validate_exactly_one_discriminator(node_label: str | None, edge_type: str | None) -> None:
    if (node_label is None) == (edge_type is None):
        raise ValueError("exactly one of node_label or edge_type is required")


class RejectionSink:
    """Durably log malformed records to a local JSONL file (the pre-DLQ default).

    Kept as the documented compatibility fallback for a schema entity that
    does not opt into DLQ routing (``dead_letter`` absent or
    ``enabled=False``), and for local/offline runs without a DLQ topic.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    @property
    def path(self) -> Path:
        """Return the configured JSONL path."""
        return self._path

    def route(
        self, *, topic: str, partition: int, offset: int,
        key: bytes | None = None, value: bytes | None = None, reason: str,
        reason_code: str = "", node_label: str | None = None,
        edge_type: str | None = None,
    ) -> None:
        """Append and fsync one metadata-only node or relationship rejection.

        ``key``/``value``/``reason_code`` are accepted (to satisfy
        :class:`MalformedRecordSink`) but ignored: the on-disk format does
        not change in this slice, preserving compatibility for anyone still
        parsing it. Exactly one discriminator is retained so existing node
        JSONL records remain compatible while relationship records identify
        their edge type.
        """
        _validate_exactly_one_discriminator(node_label, edge_type)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "reason": reason,
        }
        if node_label is not None:
            entry["node_label"] = node_label
        else:
            entry["edge_type"] = edge_type
        with self._path.open("a", encoding="utf-8") as rejection_file:
            rejection_file.write(json.dumps(entry, separators=(",", ":"), sort_keys=True))
            rejection_file.write("\n")
            rejection_file.flush()
            os.fsync(rejection_file.fileno())

    def append(
        self, *, topic: str, partition: int, offset: int, reason: str,
        node_label: str | None = None, edge_type: str | None = None,
    ) -> None:
        """Deprecated alias for :meth:`route` with no raw payload (metadata only)."""
        self.route(
            topic=topic, partition=partition, offset=offset, key=None, value=None,
            reason=reason, reason_code="", node_label=node_label, edge_type=edge_type,
        )


def _dlq_json_safe(value: object) -> object:
    """Return a JSON-encodable value that replays byte-for-byte through the
    loader's own normalizer (P5-S3 Story 2's fix for the reviewer-found
    date/datetime corruption bug).

    ``normalize_property_value`` (``src/loader/record_validation.py``) only
    ever returns ``str``, ``int``, ``float``, ``bool``, ``datetime.date``,
    or ``datetime.datetime`` for a validated property or endpoint key value.
    ``datetime.datetime`` is checked before ``datetime.date`` because it is
    a subclass of it. Encoding either with ``.isoformat()`` reproduces the
    exact ISO-8601 shape (``T`` separator, explicit offset) the loader's own
    parser requires; the previous ``json.dumps(..., default=str)`` instead
    wrote ``str(datetime)``, which uses a space separator and is rejected by
    that same parser -- silently making a parked ``date``/``datetime`` row
    unreplayable forever once its v2 envelope existed in a DLQ topic.
    Anything else raises ``TypeError`` (a bug in the caller, since no other
    type can reach here from a normalized ``EdgeRecord``) rather than
    silently stringifying an unexpected type.
    """
    if isinstance(value, _datetime.datetime):
        return value.isoformat()
    if isinstance(value, _datetime.date):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported DLQ value type: {type(value).__name__}")


class DeadLetterRecordSink:
    """Route one malformed record's full envelope to its DLQ topic.

    Built once per loader (Story 4/5's construction-time pick, not a
    per-message branch) from a :class:`DeadLetterProducer`, the resolved
    DLQ topic, and this replica's identity.
    """

    def __init__(
        self, *, producer: DeadLetterProducer, topic: str, run_id: str, replica_id: str,
        kind: Literal["node", "edge"], clock: Callable[[], int] | None = None,
    ) -> None:
        """Validate identity/topic once, at construction, not on the first record.

        A blank ``run_id``/``replica_id``/``topic`` or an invalid ``kind``
        would otherwise only surface as a ``DeadLetterEnvelopeError`` on the
        first malformed record this sink sees -- leaving that record's
        offset permanently uncommitted (a poison pill that fails again on
        every restart). Fail loudly here instead, at loader startup.
        """
        self._topic = _validated_nonblank_str(topic, "topic")
        self._run_id = _validated_nonblank_str(run_id, "run_id")
        self._replica_id = _validated_nonblank_str(replica_id, "replica_id")
        _require(kind in ENTITY_KINDS, "kind must be 'node' or 'edge'")
        self._producer = producer
        self._kind = kind
        self._clock = clock or (lambda: int(time.time() * 1000))

    def route(
        self, *, topic: str, partition: int, offset: int,
        key: bytes | None, value: bytes | None, reason: str,
        reason_code: str, node_label: str | None = None,
        edge_type: str | None = None,
    ) -> None:
        """Publish a full :class:`DeadLetterEnvelope` for one malformed record.

        Malformed records are terminal (P5-Q2): ``attempt_count`` starts and
        stays at 0 here, and ``first_failure_at_ms``/``dlq_routed_at_ms`` are
        the same wall-clock reading, since this is the only attempt.
        """
        _validate_exactly_one_discriminator(node_label, edge_type)
        if self._kind == "node" and node_label is None:
            raise DeadLetterEnvelopeError(
                "a 'node'-kind DeadLetterRecordSink was routed a record with edge_type "
                "instead of node_label -- this sink and its caller disagree on entity kind"
            )
        if self._kind == "edge" and edge_type is None:
            raise DeadLetterEnvelopeError(
                "an 'edge'-kind DeadLetterRecordSink was routed a record with node_label "
                "instead of edge_type -- this sink and its caller disagree on entity kind"
            )
        now = self._clock()
        envelope = DeadLetterEnvelope(
            schema_version=SCHEMA_VERSION,
            failure_class="malformed",
            entity_kind=self._kind,
            entity_name=node_label if node_label is not None else edge_type,
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            key_bytes=key,
            value_bytes=value,
            reason_code=reason_code,
            error_text=reason,
            loader_identity=LoaderIdentity(
                run_id=self._run_id, replica_id=self._replica_id, kind=self._kind,
            ),
            first_failure_at_ms=now,
            attempt_count=0,
            dlq_routed_at_ms=now,
        )
        self._producer.publish(self._topic, envelope)

    def route_unresolved_endpoint(
        self, *, topic: str, partition: int, offset: int,
        record: "EdgeRecord", edge_type: str,
        source_key_property: str, target_key_property: str,
        failure_class: str, reason_code: str, error_text: str,
        retry_after_seconds: float, endpoint_labels: "tuple[str, str] | None" = None,
    ) -> None:
        """Publish a :class:`DeadLetterEnvelope` for one relationship row whose
        endpoint(s) did not resolve within one write attempt (P5-S3).

        ``endpoint_labels`` (v7-S7): the ``(source label, target label)`` the
        row's topic binds, given for multi-pair types only. The key is then
        ``[S, T, source_key, target_key]``, so the pair identity every DLQ
        consumer groups by includes the label pair; without it the key stays
        ``[source_key, target_key]`` byte for byte.

        ``failure_class`` is ``"missing_endpoint"`` or ``"ambiguous_endpoint"``
        (never ``"malformed"`` -- a malformed record is routed through
        :meth:`route` instead). ``attempt_count`` starts at 0: this method
        only ever produces the *first* attempt's envelope; P5-S4's retry
        consumer re-publishes with an incremented ``attempt_count`` and a
        recomputed ``retry_after_ms`` on each further failure.

        ``value_bytes`` carries the already-*normalized* ``EdgeRecord``, but
        reshaped into the loader's own wire **input** contract --
        ``{"source": {source_key_property: ...}, "target": {target_key_property:
        ...}, "properties": {...}}`` -- rather than the flatter
        ``{"source_key", "target_key", "properties"}`` shape
        ``plan/v5/slice-3/stories.md``'s Decisions point 3 originally sketched.
        This is a deliberate deviation from that sketch (recorded in this
        slice's completion doc): the flatter shape cannot be fed back through
        ``normalize_edge_record``/``dead_letter_replay.py`` (a bare
        ``"source_key"`` field is not a declared section and would itself be
        rejected as malformed), which is exactly the manual-replay path this
        slice's own Story 7 documents. The wire-input shape keeps Decisions
        point 3's actual goal -- a *normalized*, already-validated record
        rather than raw undecoded bytes -- while staying directly replayable
        through the existing generic ``replay-dlq`` command. ``key_bytes`` is
        ``[source_key, target_key]`` (not the original Kafka key, which is
        never threaded through the hot-path batching dataclasses -- see
        Decisions point 3): a replay's Kafka partition assignment may differ
        from the original message's, which is safe because the loader never
        parses a relationship message's key, only its value.

        Every value (``record.source_key``/``record.target_key``/each
        property value) is encoded with :func:`_dlq_json_safe`, which
        round-trips ``date``/``datetime`` the same ISO-8601 way the loader's
        own normalizer produced them from, instead of ``str()`` (Python's
        ``str(datetime)`` uses a space separator, which the loader's
        datetime parser rejects as "must use a 'T' datetime separator" --
        that would make a parked ``date``/``datetime`` row silently
        unreplayable forever, since this envelope shape is durable the
        moment it lands in a production DLQ topic).
        """
        _require(self._kind == "edge", "route_unresolved_endpoint is edge-only")
        _require(
            failure_class in ("missing_endpoint", "ambiguous_endpoint"),
            f"failure_class must be missing_endpoint or ambiguous_endpoint, got {failure_class!r}",
        )
        now = self._clock()
        value = json.dumps(
            {
                "source": {source_key_property: _dlq_json_safe(record.source_key)},
                "target": {target_key_property: _dlq_json_safe(record.target_key)},
                "properties": {
                    name: _dlq_json_safe(prop_value) for name, prop_value in record.properties.items()
                },
            },
            sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        key_items = [_dlq_json_safe(record.source_key), _dlq_json_safe(record.target_key)]
        if endpoint_labels is not None:
            source_label, target_label = endpoint_labels
            key_items = [source_label, target_label, *key_items]
        key = json.dumps(key_items, separators=(",", ":")).encode("utf-8")
        if not math.isfinite(retry_after_seconds):
            raise DeadLetterEnvelopeError("retry_after_seconds must be a finite number")
        envelope = DeadLetterEnvelope(
            schema_version=SCHEMA_VERSION,
            failure_class=failure_class,
            entity_kind="edge",
            entity_name=edge_type,
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            key_bytes=key,
            value_bytes=value,
            reason_code=reason_code,
            error_text=error_text,
            loader_identity=LoaderIdentity(
                run_id=self._run_id, replica_id=self._replica_id, kind="edge",
            ),
            first_failure_at_ms=now,
            attempt_count=0,
            dlq_routed_at_ms=now,
            retry_after_ms=now + int(retry_after_seconds * 1000),
        )
        self._producer.publish(self._topic, envelope)

    def reroute_or_park(
        self, *, envelope: DeadLetterEnvelope, reason_code: str, error_text: str,
        retry_after_seconds_for_reattempt: float, parked_topic: str | None,
        increment_attempt: bool = True,
    ) -> Literal["re_dlq", "parked"]:
        """Republish ``envelope`` with ``attempt_count + 1``, or park it (P5-S4).

        The caller (the retry consumer, Story 5) has already decided
        re-DLQ vs. parking — by comparing ``envelope.attempt_count + 1``
        against the edge type's own ``max_retry_attempts`` — and passes that
        decision as *which topic to publish to*: ``parked_topic is not
        None`` means "park", ``None`` means "re-DLQ to this sink's own DLQ
        topic". This method never looks up a threshold itself, keeping this
        module free of any per-edge-type config lookup.

        ``increment_attempt=False`` (review finding M1): a row that is only
        blocked behind an earlier outstanding record of the same pair
        (``reason_code`` ``blocked_by_earlier_unresolved_row``) is **not** a resolution
        failure of its own — its own endpoint(s) may resolve fine. Re-DLQing
        it at an unchanged ``attempt_count`` (the caller passes
        ``increment_attempt=False`` and ``parked_topic=None`` — a blocked
        row is never parked purely for being blocked) means waiting on
        another record's own resolution never consumes this row's own
        attempt budget or compounds its own backoff. The default
        (``True``) is every ordinary endpoint-resolution retry.

        Preserves ``original_topic``/``original_partition``/``original_offset``,
        ``entity_kind``/``entity_name``, ``loader_identity``, and
        ``first_failure_at_ms`` byte-for-byte from ``envelope``: only
        ``attempt_count`` (incremented unless ``increment_attempt=False``),
        ``retry_after_ms`` (recomputed on re-DLQ), ``reason_code``, and
        ``error_text`` change. ``dlq_routed_at_ms`` is refreshed to the
        current publish time either way.

        A parked envelope carries its *last* ``retry_after_ms`` unchanged
        rather than ``None``: unlike ``failure_class="malformed"`` (whose
        envelope is never eligible for a nonnegative ``retry_after_ms`` in
        the first place, so ``None`` is the only valid encoding),
        ``"missing_endpoint"``/``"ambiguous_endpoint"`` envelopes require a
        nonnegative ``retry_after_ms`` unconditionally (``encode_envelope``'s
        existing, unrelaxed contract) — a parked envelope is never read for
        scheduling (only the DLQ topic is; the retry consumer never reads
        the parking-lot topic), so this field is purely informational there.

        A park publish fires :func:`on_parked` exactly once, directly (not
        via :class:`DeadLetterProducer`, and with
        ``fire_dlq_routed_hook=False`` so the underlying publish never also
        fires :func:`on_dlq_routed` for the same conceptual event). A re-DLQ
        publish fires the pre-existing :func:`on_dlq_routed` instead, via the
        normal :meth:`DeadLetterProducer.publish` path both outcomes share.

        Never called for ``failure_class="malformed"`` — a malformed record
        is terminal (P5-Q2) and is never retried; a caller attempting to do
        so is a caller bug, enforced here, before any Kafka call.
        """
        _require(
            envelope.failure_class != "malformed",
            "reroute_or_park must never be called for failure_class='malformed' "
            "(a malformed record is terminal and is never retried)",
        )
        kind, topic, outgoing = self._build_reroute(
            envelope=envelope, reason_code=reason_code, error_text=error_text,
            retry_after_seconds_for_reattempt=retry_after_seconds_for_reattempt,
            parked_topic=parked_topic, increment_attempt=increment_attempt,
        )
        self._producer.publish(topic, outgoing, fire_dlq_routed_hook=kind == "re_dlq")
        if kind == "parked":
            self._fire_on_parked(envelope, topic)
        return kind

    def _fire_on_parked(self, envelope: DeadLetterEnvelope, parked_topic: str) -> None:
        try:
            on_parked(envelope.entity_kind, envelope.entity_name, parked_topic)
        except Exception:
            logger.exception(
                "on_parked listener raised after a successful parking-lot publish to "
                "%s (entity_kind=%s entity_name=%s) — ignoring, the publish already "
                "succeeded",
                parked_topic, envelope.entity_kind, envelope.entity_name,
            )

    def reroute_or_park_many(
        self, requests: "Sequence[Mapping[str, object]]",
    ) -> "list[Literal['re_dlq', 'parked'] | Exception]":
        """:meth:`reroute_or_park` for many envelopes with ONE producer flush.

        ``requests``: the keyword arguments of :meth:`reroute_or_park`, one
        mapping per envelope. Returns one positional result per request:
        ``"re_dlq"``/``"parked"`` once that request's own publish is
        acknowledged (its hook fired exactly as the single-item call would
        fire it), or the exception it would have raised. Nothing raises for
        a per-request failure, so a caller commits only what is durable.
        """
        results: list = [None] * len(requests)
        items: list[tuple[str, DeadLetterEnvelope, bool]] = []
        built: list[tuple[int, str, DeadLetterEnvelope, str]] = []
        for position, request in enumerate(requests):
            try:
                kind, topic, outgoing = self._build_reroute(**request)
            except Exception as exc:
                results[position] = exc
                continue
            built.append((position, kind, outgoing, topic))
            items.append((topic, outgoing, kind == "re_dlq"))
        published = self._producer.publish_many(items)
        for (position, kind, _outgoing, topic), error in zip(built, published):
            if error is not None:
                results[position] = error
                continue
            results[position] = kind
            if kind == "parked":
                self._fire_on_parked(requests[position]["envelope"], topic)
        return results

    def _build_reroute(
        self, *, envelope: DeadLetterEnvelope, reason_code: str, error_text: str,
        retry_after_seconds_for_reattempt: float, parked_topic: str | None,
        increment_attempt: bool = True,
    ) -> "tuple[str, str, DeadLetterEnvelope]":
        """Return ``(kind, topic, outgoing_envelope)`` for one reroute/park."""
        _require(
            envelope.failure_class != "malformed",
            "reroute_or_park must never be called for failure_class='malformed' "
            "(a malformed record is terminal and is never retried)",
        )
        next_attempt = envelope.attempt_count + 1 if increment_attempt else envelope.attempt_count
        now = self._clock()
        if parked_topic is not None:
            return "parked", parked_topic, DeadLetterEnvelope(
                schema_version=SCHEMA_VERSION,
                failure_class=envelope.failure_class,
                entity_kind=envelope.entity_kind,
                entity_name=envelope.entity_name,
                original_topic=envelope.original_topic,
                original_partition=envelope.original_partition,
                original_offset=envelope.original_offset,
                key_bytes=envelope.key_bytes,
                value_bytes=envelope.value_bytes,
                reason_code=reason_code,
                error_text=error_text,
                loader_identity=envelope.loader_identity,
                first_failure_at_ms=envelope.first_failure_at_ms,
                attempt_count=next_attempt,
                dlq_routed_at_ms=now,
                retry_after_ms=envelope.retry_after_ms,
            )
        if not math.isfinite(retry_after_seconds_for_reattempt):
            raise DeadLetterEnvelopeError("retry_after_seconds_for_reattempt must be a finite number")
        return "re_dlq", self._topic, DeadLetterEnvelope(
            schema_version=SCHEMA_VERSION,
            failure_class=envelope.failure_class,
            entity_kind=envelope.entity_kind,
            entity_name=envelope.entity_name,
            original_topic=envelope.original_topic,
            original_partition=envelope.original_partition,
            original_offset=envelope.original_offset,
            key_bytes=envelope.key_bytes,
            value_bytes=envelope.value_bytes,
            reason_code=reason_code,
            error_text=error_text,
            loader_identity=envelope.loader_identity,
            first_failure_at_ms=envelope.first_failure_at_ms,
            attempt_count=next_attempt,
            dlq_routed_at_ms=now,
            retry_after_ms=now + int(retry_after_seconds_for_reattempt * 1000),
        )


def _reason_code(exc: Exception) -> str:
    """Classify a malformed-record exception into a short, stable reason code.

    Generic over the exception's *type and message text* — never imports
    ``NodeRecordValidationError``/``EdgeRecordValidationError`` (both are
    plain ``ValueError`` subclasses), so this stays reusable by both loaders
    without creating an import cycle. Always returns a nonblank string: a
    blank ``reason_code`` would fail :class:`DeadLetterEnvelope` validation
    and leave the record's offset permanently uncommitted (a poison pill).
    """
    if isinstance(exc, UnicodeDecodeError):
        return "decode_error"
    if isinstance(exc, json.JSONDecodeError):
        return "invalid_json"
    if isinstance(exc, ValueError):
        text = str(exc)
        if "is not declared" in text:
            return "undeclared_property"
        if "is required" in text:
            return "missing_required"
        if "payload is empty" in text:
            return "empty_payload"
        if "must be a top-level object" in text:
            return "not_an_object"
    return "invalid_value"


def build_malformed_record_sink(
    *,
    dead_letter,
    entity_topic: str,
    entity_kind: Literal["node", "edge"],
    rejection_log_path: str,
    bootstrap_servers: str,
    run_id: str,
    replica_id: str,
    max_poll_interval_ms: int | None = None,
    producer_factory=None,
    clock: Callable[[], int] | None = None,
):
    """Build the malformed-record sink for one loader, and its owned producer if any.

    Returns ``(sink, producer)``. ``producer`` is ``None`` for the
    :class:`RejectionSink` (file) fallback; the caller must flush it in its
    own ``finally`` block when not ``None``, exactly like the existing
    control-topic producer -- this producer is never shared with that one.

    ``max_poll_interval_ms``, when given, bounds the producer's
    ``delivery.timeout.ms`` at half of it (floored at 1000 ms -- librdkafka
    rejects a value at or below its default ``linger.ms``, and this keeps
    Producer construction itself from failing on an aggressively small
    ``max_poll_interval_ms``): a synchronous DLQ publish runs on the same
    poll thread that must keep heartbeating within ``max_poll_interval_ms``
    or the broker evicts the consumer and triggers a rebalance. A publish
    that is merely slow (not failed) should not, by itself, cost the
    loader its group membership.
    """
    resolved_topic = dead_letter_topic_name(entity_topic, dead_letter)
    if resolved_topic is None:
        return RejectionSink(rejection_log_path), None
    # Validate identity/topic before spending a Kafka connection on a
    # Producer that would be built only to be discarded unflushed.
    _validated_nonblank_str(resolved_topic, "topic")
    _validated_nonblank_str(run_id, "run_id")
    _validated_nonblank_str(replica_id, "replica_id")
    _require(entity_kind in ENTITY_KINDS, "entity_kind must be 'node' or 'edge'")
    if producer_factory is None:
        from confluent_kafka import Producer as producer_factory
    producer_config = dict(DEAD_LETTER_PRODUCER_CONFIG)
    if max_poll_interval_ms is not None:
        producer_config["delivery.timeout.ms"] = max(
            1_000, min(producer_config["delivery.timeout.ms"], max_poll_interval_ms // 2),
        )
    producer = producer_factory({
        **producer_config, "bootstrap.servers": bootstrap_servers,
    })
    sink = DeadLetterRecordSink(
        producer=DeadLetterProducer(producer),
        topic=resolved_topic,
        run_id=run_id,
        replica_id=replica_id,
        kind=entity_kind,
        clock=clock,
    )
    return sink, producer
