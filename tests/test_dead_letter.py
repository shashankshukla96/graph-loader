"""Dead-letter envelope codec: round-trips, determinism, and validation."""
from __future__ import annotations

import base64
from datetime import date, datetime
import json
from unittest.mock import MagicMock

import pytest

from src.loader import dead_letter
from src.loader.edge_records import EdgeRecord
from src.loader.dead_letter import (
    DEAD_LETTER_MAX_ENVELOPE_BYTES,
    DEAD_LETTER_TOPIC_CONFIG,
    PARKED_PAIR_INDEX_TOPIC_CONFIG,
    SCHEMA_VERSION,
    DeadLetterEnvelope,
    DeadLetterEnvelopeError,
    DeadLetterProducer,
    DeadLetterPublishError,
    DeadLetterRecordSink,
    LoaderIdentity,
    RejectionSink,
    _reason_code,
    build_malformed_record_sink,
    decode_envelope,
    encode_envelope,
    ensure_dead_letter_topic,
    ensure_parked_pair_index_topic,
)


def _envelope(**overrides) -> DeadLetterEnvelope:
    fields = dict(
        schema_version=SCHEMA_VERSION,
        failure_class="malformed",
        entity_kind="node",
        entity_name="Person",
        original_topic="person-events",
        original_partition=2,
        original_offset=17,
        key_bytes=b"p-1",
        value_bytes=b"{bad",
        reason_code="invalid_json",
        error_text="Expecting value: line 1 column 1 (char 0)",
        loader_identity=LoaderIdentity(run_id="run-1", replica_id="0", kind="node"),
        first_failure_at_ms=1_700_000_000_000,
        attempt_count=0,
        dlq_routed_at_ms=1_700_000_000_500,
        retry_after_ms=None,
    )
    fields.update(overrides)
    return DeadLetterEnvelope(**fields)


def test_round_trip_preserves_non_utf8_value_and_none_key() -> None:
    envelope = _envelope(key_bytes=None, value_bytes=b"\xff\xfe\x00bad-bytes")
    decoded = decode_envelope(encode_envelope(envelope))
    assert decoded == envelope


def test_round_trip_preserves_empty_value_and_present_key() -> None:
    envelope = _envelope(key_bytes=b"k", value_bytes=b"")
    decoded = decode_envelope(encode_envelope(envelope))
    assert decoded == envelope
    assert decoded.value_bytes == b""


def test_encoding_is_deterministic() -> None:
    envelope = _envelope()
    assert encode_envelope(envelope) == encode_envelope(envelope)


def test_encoded_envelope_is_plain_json_readable_without_this_library() -> None:
    envelope = _envelope()
    parsed = json.loads(encode_envelope(envelope))
    assert isinstance(parsed, dict)
    assert parsed["reason_code"] == "invalid_json"
    assert base64.b64decode(parsed["value_bytes"]) == b"{bad"
    assert parsed["key_bytes"] == base64.b64encode(b"p-1").decode("ascii")
    assert parsed["loader_identity"] == {"run_id": "run-1", "replica_id": "0", "kind": "node"}


@pytest.mark.parametrize("field, value", [
    ("schema_version", 3),
    ("schema_version", 0),
    ("failure_class", "bogus"),
    ("original_partition", -1),
    ("original_offset", -1),
    ("attempt_count", -1),
    ("first_failure_at_ms", -1),
    ("dlq_routed_at_ms", -1),
    ("entity_kind", "relationship"),
    ("entity_name", ""),
    ("reason_code", ""),
])
def test_encode_rejects_every_invalid_field(field, value) -> None:
    envelope = _envelope(**{field: value})
    with pytest.raises(DeadLetterEnvelopeError):
        encode_envelope(envelope)


def test_encode_rejects_bool_masquerading_as_int() -> None:
    envelope = _envelope(original_offset=True)
    with pytest.raises(DeadLetterEnvelopeError):
        encode_envelope(envelope)


def test_encode_rejects_bad_loader_identity_kind() -> None:
    envelope = _envelope(loader_identity=LoaderIdentity(run_id="r", replica_id="0", kind="bogus"))
    with pytest.raises(DeadLetterEnvelopeError):
        encode_envelope(envelope)


def test_decode_rejects_non_json() -> None:
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(b"not json at all {")


def test_decode_rejects_non_utf8_payload() -> None:
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(b"\xff\xfe\x00\x01")


def test_decode_rejects_top_level_array() -> None:
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(b"[1, 2, 3]")


def test_decode_rejects_missing_field() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    del payload["reason_code"]
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_unexpected_extra_field() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["unexpected_field"] = "surprise"
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_duplicate_top_level_keys() -> None:
    duplicated = b'{"schema_version": 1, "schema_version": 1}'
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(duplicated)


def test_decode_rejects_truncated_base64() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["value_bytes"] = "not-valid-base64!!!"
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_unknown_schema_version() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["schema_version"] = 99
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_unrecognized_failure_class() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["failure_class"] = "not_a_real_class"
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_negative_offset_or_partition() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["original_offset"] = -5
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_bad_loader_identity_shape() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["loader_identity"] = {"run_id": "r", "replica_id": "0", "kind": "carrier_pigeon"}
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_non_dict_payload() -> None:
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(b"42")


def test_decode_rejects_non_bytes_payload() -> None:
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope("not bytes")  # type: ignore[arg-type]


def test_missing_endpoint_and_ambiguous_endpoint_are_reserved_literals() -> None:
    for failure_class in ("missing_endpoint", "ambiguous_endpoint"):
        envelope = _envelope(failure_class=failure_class, retry_after_ms=1_700_000_001_000)
        decoded = decode_envelope(encode_envelope(envelope))
        assert decoded.failure_class == failure_class


def test_round_trip_with_both_key_and_value_none() -> None:
    envelope = _envelope(key_bytes=None, value_bytes=None)
    decoded = decode_envelope(encode_envelope(envelope))
    assert decoded.key_bytes is None
    assert decoded.value_bytes is None


@pytest.mark.parametrize("bad_version", [True, False, 1.0])
def test_encode_rejects_schema_version_bool_or_float_masquerade(bad_version) -> None:
    envelope = _envelope(schema_version=bad_version)
    with pytest.raises(DeadLetterEnvelopeError):
        encode_envelope(envelope)


@pytest.mark.parametrize("bad_version", [True, 1.0, "1", 3, 0])
def test_decode_rejects_schema_version_bool_float_or_wrong_value(bad_version) -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["schema_version"] = bad_version
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_entity_kind_loader_identity_kind_mismatch() -> None:
    payload = json.loads(encode_envelope(_envelope(entity_kind="node")))
    payload["loader_identity"]["kind"] = "edge"
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_encode_rejects_entity_kind_loader_identity_kind_mismatch() -> None:
    envelope = _envelope(
        entity_kind="node",
        loader_identity=LoaderIdentity(run_id="r", replica_id="0", kind="edge"),
    )
    with pytest.raises(DeadLetterEnvelopeError):
        encode_envelope(envelope)


def test_decode_raises_dead_letter_error_on_pathologically_nested_json() -> None:
    pathological = b"[" * 100_000 + b"]" * 100_000
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(pathological)


def test_decode_rejects_truly_truncated_base64() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["value_bytes"] = "QUJ"  # valid alphabet, wrong length/padding
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_base64_with_trailing_data_after_padding() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["value_bytes"] = "QQ==QQ=="
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_bad_base64_in_key_bytes() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["key_bytes"] = "not-valid-base64!!!"
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


@pytest.mark.parametrize("field, value", [
    ("original_offset", "17"),
    ("original_partition", 1.5),
    ("attempt_count", True),
    ("first_failure_at_ms", "not-a-number"),
    ("dlq_routed_at_ms", None),
])
def test_decode_rejects_wrong_type_int_fields(field, value) -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload[field] = value
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_null_error_text() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["error_text"] = None
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_value_bytes_as_int() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["value_bytes"] = 12345
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_loader_identity_as_list() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["loader_identity"] = ["run-1", "0", "node"]
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_loader_identity_as_string() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["loader_identity"] = "run-1"
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_loader_identity_missing_key() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    del payload["loader_identity"]["replica_id"]
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_loader_identity_extra_key() -> None:
    payload = json.loads(encode_envelope(_envelope()))
    payload["loader_identity"]["extra"] = "surprise"
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(json.dumps(payload).encode("utf-8"))


def test_decode_rejects_duplicate_key_inside_nested_loader_identity() -> None:
    duplicated = (
        b'{"schema_version":1,"failure_class":"malformed","entity_kind":"node",'
        b'"entity_name":"Person","original_topic":"t","original_partition":0,'
        b'"original_offset":0,"key_bytes":null,"value_bytes":null,'
        b'"reason_code":"x","error_text":"y",'
        b'"loader_identity":{"run_id":"r","run_id":"r2","replica_id":"0","kind":"node"},'
        b'"first_failure_at_ms":0,"attempt_count":0,"dlq_routed_at_ms":0}'
    )
    with pytest.raises(DeadLetterEnvelopeError):
        decode_envelope(duplicated)


def test_encode_rejects_non_bytes_key_and_value() -> None:
    envelope = _envelope(key_bytes="not-bytes")
    with pytest.raises(DeadLetterEnvelopeError):
        encode_envelope(envelope)
    envelope = _envelope(value_bytes="not-bytes")
    with pytest.raises(DeadLetterEnvelopeError):
        encode_envelope(envelope)


def test_encode_rejects_blank_original_topic() -> None:
    envelope = _envelope(original_topic="   ")
    with pytest.raises(DeadLetterEnvelopeError):
        encode_envelope(envelope)


# ── DeadLetterProducer (Story 3) ─────────────────────────────────────────────


def _delivering_producer() -> MagicMock:
    """A mocked Producer whose flush() is where delivery is confirmed.

    Matches real librdkafka: the delivery callback fires during flush(),
    not synchronously inside produce().
    """
    producer = MagicMock()
    state = {"on_delivery": None}

    def produce(topic, key, value, on_delivery):
        state["on_delivery"] = on_delivery

    def flush():
        state["on_delivery"](None, MagicMock())
        return 0

    producer.produce.side_effect = produce
    producer.flush.side_effect = flush
    return producer


def test_publish_success_calls_on_dlq_routed_once_after_ack(monkeypatch) -> None:
    producer = _delivering_producer()
    envelope = _envelope()
    listener = MagicMock()
    monkeypatch.setattr(dead_letter, "on_dlq_routed", listener)

    DeadLetterProducer(producer).publish("person-events.dlq", envelope)

    args, kwargs = producer.produce.call_args
    assert args[0] == "person-events.dlq"
    assert kwargs["key"] == envelope.key_bytes
    assert kwargs["value"] == encode_envelope(envelope)
    listener.assert_called_once_with("node", "Person", "person-events.dlq", "malformed")


def test_publish_delivery_failure_raises_and_does_not_route(monkeypatch) -> None:
    producer = MagicMock()
    state = {"on_delivery": None}

    def produce(topic, key, value, on_delivery):
        state["on_delivery"] = on_delivery

    def flush():
        state["on_delivery"](RuntimeError("broker down"), MagicMock())
        return 0

    producer.produce.side_effect = produce
    producer.flush.side_effect = flush
    listener = MagicMock()
    monkeypatch.setattr(dead_letter, "on_dlq_routed", listener)

    with pytest.raises(DeadLetterPublishError):
        DeadLetterProducer(producer).publish("person-events.dlq", _envelope())
    listener.assert_not_called()


def test_publish_unflushed_message_raises_and_does_not_route(monkeypatch) -> None:
    producer = _delivering_producer()
    producer.flush.side_effect = None
    producer.flush.return_value = 1
    listener = MagicMock()
    monkeypatch.setattr(dead_letter, "on_dlq_routed", listener)

    with pytest.raises(DeadLetterPublishError):
        DeadLetterProducer(producer).publish("person-events.dlq", _envelope())
    listener.assert_not_called()


def test_publish_rejects_oversized_envelope_before_producing(monkeypatch) -> None:
    producer = _delivering_producer()
    listener = MagicMock()
    monkeypatch.setattr(dead_letter, "on_dlq_routed", listener)
    envelope = _envelope(value_bytes=b"x" * (DEAD_LETTER_MAX_ENVELOPE_BYTES + 1))

    with pytest.raises(DeadLetterPublishError):
        DeadLetterProducer(producer).publish("person-events.dlq", envelope)
    producer.produce.assert_not_called()
    listener.assert_not_called()


def test_publish_rejects_invalid_envelope_before_producing() -> None:
    producer = _delivering_producer()
    envelope = _envelope(reason_code="")

    with pytest.raises(DeadLetterEnvelopeError):
        DeadLetterProducer(producer).publish("person-events.dlq", envelope)
    producer.produce.assert_not_called()


def test_publish_wraps_produce_exception_as_publish_error(monkeypatch) -> None:
    producer = MagicMock()
    producer.produce.side_effect = BufferError("local queue full")
    listener = MagicMock()
    monkeypatch.setattr(dead_letter, "on_dlq_routed", listener)

    with pytest.raises(DeadLetterPublishError):
        DeadLetterProducer(producer).publish("person-events.dlq", _envelope())
    listener.assert_not_called()


def test_publish_wraps_flush_exception_as_publish_error(monkeypatch) -> None:
    producer = MagicMock()
    producer.produce.side_effect = lambda topic, key, value, on_delivery: None
    producer.flush.side_effect = RuntimeError("KafkaException: fatal idempotent producer error")
    listener = MagicMock()
    monkeypatch.setattr(dead_letter, "on_dlq_routed", listener)

    with pytest.raises(DeadLetterPublishError):
        DeadLetterProducer(producer).publish("person-events.dlq", _envelope())
    listener.assert_not_called()


def test_publish_succeeds_even_if_on_dlq_routed_listener_raises(monkeypatch) -> None:
    producer = _delivering_producer()
    listener = MagicMock(side_effect=RuntimeError("metrics bug"))
    monkeypatch.setattr(dead_letter, "on_dlq_routed", listener)

    # Must not raise: a hook bug must never turn an acknowledged publish
    # into a reported failure (that would stall the partition forever).
    DeadLetterProducer(producer).publish("person-events.dlq", _envelope())
    listener.assert_called_once()


def test_producer_config_has_safety_critical_settings() -> None:
    from src.loader.dead_letter import DEAD_LETTER_PRODUCER_CONFIG, DEAD_LETTER_MAX_ENVELOPE_BYTES

    assert DEAD_LETTER_PRODUCER_CONFIG["acks"] == "all"
    assert DEAD_LETTER_PRODUCER_CONFIG["enable.idempotence"] is True
    assert DEAD_LETTER_PRODUCER_CONFIG["allow.auto.create.topics"] is False
    assert DEAD_LETTER_PRODUCER_CONFIG["message.max.bytes"] >= DEAD_LETTER_MAX_ENVELOPE_BYTES
    assert int(DEAD_LETTER_TOPIC_CONFIG["max.message.bytes"]) >= DEAD_LETTER_MAX_ENVELOPE_BYTES


# ── ensure_dead_letter_topic (Story 3) ───────────────────────────────────────


def _admin_factory(admin: MagicMock):
    return lambda config: admin


def _compliant_describe_configs(admin: MagicMock) -> None:
    """Configure a MagicMock admin's describe_configs() to report a fully compliant topic."""
    entries = {
        "retention.ms": MagicMock(value="-1"),
        "retention.bytes": MagicMock(value="-1"),
        "max.message.bytes": MagicMock(value=str(DEAD_LETTER_MAX_ENVELOPE_BYTES + 65536)),
    }
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: entries.get(key, default)
    future = MagicMock()
    future.result.return_value = config
    admin.describe_configs.return_value = {"resource": future}


def test_ensure_dead_letter_topic_noops_when_present_and_compliant() -> None:
    admin = MagicMock()
    admin.list_topics.return_value.topics = {"person-events.dlq": object()}
    _compliant_describe_configs(admin)

    ensure_dead_letter_topic(
        "kafka:9092", "person-events.dlq", source_topic="person-events",
        admin_factory=_admin_factory(admin),
    )
    admin.create_topics.assert_not_called()
    admin.describe_configs.assert_called_once()


def test_ensure_dead_letter_topic_fails_closed_when_existing_topic_has_finite_retention() -> None:
    admin = MagicMock()
    admin.list_topics.return_value.topics = {"person-events.dlq": object()}
    entries = {
        "retention.ms": MagicMock(value="604800000"),  # 7 days, not -1
        "retention.bytes": MagicMock(value="-1"),
        "max.message.bytes": MagicMock(value=str(DEAD_LETTER_MAX_ENVELOPE_BYTES + 65536)),
    }
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: entries.get(key, default)
    future = MagicMock()
    future.result.return_value = config
    admin.describe_configs.return_value = {"resource": future}

    with pytest.raises(RuntimeError, match="retention.ms"):
        ensure_dead_letter_topic(
            "kafka:9092", "person-events.dlq", source_topic="person-events",
            admin_factory=_admin_factory(admin),
        )


def test_ensure_dead_letter_topic_fails_closed_when_existing_topic_max_message_bytes_too_small() -> None:
    admin = MagicMock()
    admin.list_topics.return_value.topics = {"person-events.dlq": object()}
    entries = {
        "retention.ms": MagicMock(value="-1"),
        "retention.bytes": MagicMock(value="-1"),
        "max.message.bytes": MagicMock(value="1000000"),
    }
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: entries.get(key, default)
    future = MagicMock()
    future.result.return_value = config
    admin.describe_configs.return_value = {"resource": future}

    with pytest.raises(RuntimeError, match="max.message.bytes"):
        ensure_dead_letter_topic(
            "kafka:9092", "person-events.dlq", source_topic="person-events",
            admin_factory=_admin_factory(admin),
        )


def test_ensure_dead_letter_topic_creates_with_exact_retention_config_and_source_partitions() -> None:
    admin = MagicMock()
    source_topic_metadata = MagicMock()
    source_topic_metadata.partitions = {0: object(), 1: object(), 2: object()}
    admin.list_topics.return_value.topics = {"person-events": source_topic_metadata}
    future = MagicMock()
    future.result.return_value = None
    admin.create_topics.return_value = {"person-events.dlq": future}

    ensure_dead_letter_topic(
        "kafka:9092", "person-events.dlq", source_topic="person-events",
        admin_factory=_admin_factory(admin),
    )

    admin.create_topics.assert_called_once()
    (new_topics,), _ = admin.create_topics.call_args
    new_topic = new_topics[0]
    assert new_topic.topic == "person-events.dlq"
    assert new_topic.num_partitions == 3
    assert new_topic.replication_factor == -1
    assert new_topic.config == DEAD_LETTER_TOPIC_CONFIG


def test_ensure_dead_letter_topic_defaults_to_one_partition_when_source_metadata_absent() -> None:
    admin = MagicMock()
    admin.list_topics.return_value.topics = {}
    future = MagicMock()
    future.result.return_value = None
    admin.create_topics.return_value = {"person-events.dlq": future}

    ensure_dead_letter_topic(
        "kafka:9092", "person-events.dlq", source_topic="person-events",
        admin_factory=_admin_factory(admin),
    )

    (new_topics,), _ = admin.create_topics.call_args
    assert new_topics[0].num_partitions == 1


def test_ensure_dead_letter_topic_tolerates_concurrent_creation_race_from_create_topics() -> None:
    admin = MagicMock()
    admin.list_topics.return_value.topics = {}
    admin.create_topics.side_effect = RuntimeError("TOPIC_ALREADY_EXISTS")

    ensure_dead_letter_topic(
        "kafka:9092", "person-events.dlq", source_topic="person-events",
        admin_factory=_admin_factory(admin),
    )  # must not raise


def test_ensure_dead_letter_topic_tolerates_concurrent_creation_race_from_future_result() -> None:
    """The real race arrives from future.result(), not from create_topics() itself.

    Two sibling callers both see the topic absent from list_topics(), both
    call create_topics(); the loser's future raises TOPIC_ALREADY_EXISTS
    when awaited.
    """
    admin = MagicMock()
    admin.list_topics.return_value.topics = {}
    future = MagicMock()
    future.result.side_effect = RuntimeError(
        "KafkaException(KafkaError{code=TOPIC_ALREADY_EXISTS,val=36,str=\"Topic 'person-events.dlq' already exists.\"})"
    )
    admin.create_topics.return_value = {"person-events.dlq": future}

    ensure_dead_letter_topic(
        "kafka:9092", "person-events.dlq", source_topic="person-events",
        admin_factory=_admin_factory(admin),
    )  # must not raise


def test_ensure_dead_letter_topic_two_concurrent_callers_both_succeed() -> None:
    """Both racing callers observe a successful outcome (Story 3 AC).

    Both admins' list_topics() reports the topic absent (the race window:
    neither has created it yet), independent of the shared ``broker_topics``
    registry that create_topics() mutates -- so the second caller's
    create_topics() call, not its list_topics() call, is where the race is
    observed and tolerated.
    """
    broker_topics: dict = {}

    def make_admin() -> MagicMock:
        admin = MagicMock()
        admin.list_topics.return_value.topics = {}
        future = MagicMock()

        def create_topics(new_topics):
            name = new_topics[0].topic
            if name in broker_topics:
                future.result.side_effect = RuntimeError("TOPIC_ALREADY_EXISTS")
            else:
                broker_topics[name] = object()
                future.result.side_effect = None
                future.result.return_value = None
            return {name: future}

        admin.create_topics.side_effect = create_topics
        return admin

    first_admin, second_admin = make_admin(), make_admin()
    ensure_dead_letter_topic(
        "kafka:9092", "person-events.dlq", source_topic="person-events",
        admin_factory=_admin_factory(first_admin),
    )
    ensure_dead_letter_topic(
        "kafka:9092", "person-events.dlq", source_topic="person-events",
        admin_factory=_admin_factory(second_admin),
    )  # neither call raises


def test_ensure_dead_letter_topic_propagates_other_failures() -> None:
    admin = MagicMock()
    admin.list_topics.return_value.topics = {}
    admin.create_topics.side_effect = RuntimeError("SASL authentication failed")

    with pytest.raises(RuntimeError, match="SASL authentication failed"):
        ensure_dead_letter_topic(
            "kafka:9092", "person-events.dlq", source_topic="person-events",
            admin_factory=_admin_factory(admin),
        )


def test_ensure_dead_letter_topic_propagates_list_topics_failure() -> None:
    admin = MagicMock()
    admin.list_topics.side_effect = RuntimeError("Local: Broker transport failure")

    with pytest.raises(RuntimeError, match="Broker transport failure"):
        ensure_dead_letter_topic(
            "kafka:9092", "person-events.dlq", source_topic="person-events",
            admin_factory=_admin_factory(admin),
        )


def test_on_dlq_routed_default_is_a_silent_no_op() -> None:
    assert dead_letter.on_dlq_routed("node", "Person", "person-events.dlq", "malformed") is None


# ── RejectionSink / DeadLetterRecordSink / _reason_code (Stories 4/5) ───────


def test_rejection_sink_route_writes_metadata_only_jsonl_line(tmp_path) -> None:
    path = tmp_path / "rejections.jsonl"
    sink = RejectionSink(path)
    sink.route(
        topic="t", partition=1, offset=2, key=b"raw-key", value=b"raw-value",
        reason="bad record", reason_code="invalid_value", node_label="Person",
    )
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == {
        "topic": "t", "partition": 1, "offset": 2, "reason": "bad record", "node_label": "Person",
    }


def test_rejection_sink_route_edge_type_discriminator(tmp_path) -> None:
    path = tmp_path / "rejections.jsonl"
    sink = RejectionSink(path)
    sink.route(
        topic="t", partition=0, offset=0, key=None, value=None,
        reason="bad edge", reason_code="invalid_value", edge_type="KNOWS",
    )
    entry = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert entry == {"topic": "t", "partition": 0, "offset": 0, "reason": "bad edge", "edge_type": "KNOWS"}


def test_rejection_sink_route_rejects_both_or_neither_discriminator(tmp_path) -> None:
    sink = RejectionSink(tmp_path / "r.jsonl")
    with pytest.raises(ValueError):
        sink.route(topic="t", partition=0, offset=0, key=None, value=None, reason="x", reason_code="y")
    with pytest.raises(ValueError):
        sink.route(
            topic="t", partition=0, offset=0, key=None, value=None, reason="x", reason_code="y",
            node_label="Person", edge_type="KNOWS",
        )


def test_rejection_sink_append_delegates_to_route_with_no_payload(tmp_path) -> None:
    path = tmp_path / "rejections.jsonl"
    sink = RejectionSink(path)
    sink.append(topic="t", partition=1, offset=2, reason="bad record", node_label="Person")
    entry = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert entry == {"topic": "t", "partition": 1, "offset": 2, "reason": "bad record", "node_label": "Person"}


def test_dead_letter_record_sink_builds_terminal_malformed_envelope() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="person-events.dlq", run_id="run-1", replica_id="0",
        kind="node", clock=lambda: 1_700_000_000_000,
    )
    sink.route(
        topic="person-events", partition=2, offset=17, key=b"p-1", value=b"{bad",
        reason="bad json", reason_code="invalid_json", node_label="Person",
    )
    producer.publish.assert_called_once()
    (topic, envelope), _ = producer.publish.call_args
    assert topic == "person-events.dlq"
    assert envelope.failure_class == "malformed"
    assert envelope.entity_kind == "node"
    assert envelope.entity_name == "Person"
    assert envelope.attempt_count == 0
    assert envelope.first_failure_at_ms == envelope.dlq_routed_at_ms == 1_700_000_000_000
    assert envelope.loader_identity.run_id == "run-1"
    assert envelope.loader_identity.kind == "node"


def test_dead_letter_record_sink_edge_kind() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows.dlq", run_id="run-1", replica_id="0",
        kind="edge", clock=lambda: 1,
    )
    sink.route(
        topic="knows-events", partition=0, offset=0, key=None, value=b"{bad",
        reason="bad json", reason_code="invalid_json", edge_type="KNOWS",
    )
    envelope = producer.publish.call_args[0][1]
    assert envelope.entity_kind == "edge"
    assert envelope.entity_name == "KNOWS"


@pytest.mark.parametrize("field, value", [
    ("run_id", ""), ("run_id", "  "), ("replica_id", ""), ("topic", ""),
])
def test_dead_letter_record_sink_rejects_blank_identity_at_construction(field, value) -> None:
    kwargs = dict(producer=MagicMock(), topic="t.dlq", run_id="r", replica_id="0", kind="node")
    kwargs[field] = value
    with pytest.raises(DeadLetterEnvelopeError):
        DeadLetterRecordSink(**kwargs)


def test_dead_letter_record_sink_rejects_bad_kind_at_construction() -> None:
    with pytest.raises(DeadLetterEnvelopeError):
        DeadLetterRecordSink(producer=MagicMock(), topic="t.dlq", run_id="r", replica_id="0", kind="bogus")


def test_dead_letter_record_sink_rejects_edge_type_on_a_node_kind_sink() -> None:
    sink = DeadLetterRecordSink(
        producer=MagicMock(), topic="t.dlq", run_id="r", replica_id="0", kind="node",
    )
    with pytest.raises(DeadLetterEnvelopeError):
        sink.route(
            topic="t", partition=0, offset=0, key=None, value=None,
            reason="x", reason_code="y", edge_type="KNOWS",
        )


def test_dead_letter_record_sink_rejects_node_label_on_an_edge_kind_sink() -> None:
    sink = DeadLetterRecordSink(
        producer=MagicMock(), topic="t.dlq", run_id="r", replica_id="0", kind="edge",
    )
    with pytest.raises(DeadLetterEnvelopeError):
        sink.route(
            topic="t", partition=0, offset=0, key=None, value=None,
            reason="x", reason_code="y", node_label="Person",
        )


# ── P5-S3 Story 2: v1/v2 envelope migration, retry_after_ms ─────────────────


def _v1_payload(**overrides) -> bytes:
    """A hand-built schema-version-1 payload (P5-S2's exact shape, no retry_after_ms)."""
    fields = dict(
        schema_version=1,
        failure_class="malformed",
        entity_kind="node",
        entity_name="Person",
        original_topic="person-events",
        original_partition=0,
        original_offset=0,
        key_bytes=None,
        value_bytes=None,
        reason_code="invalid_json",
        error_text="bad record",
        loader_identity={"run_id": "run-1", "replica_id": "0", "kind": "node"},
        first_failure_at_ms=1_000,
        attempt_count=0,
        dlq_routed_at_ms=1_000,
    )
    fields.update(overrides)
    return json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")


def test_decode_accepts_a_hand_built_schema_version_1_payload() -> None:
    decoded = decode_envelope(_v1_payload())
    assert decoded.schema_version == 1
    assert decoded.retry_after_ms is None
    assert decoded.failure_class == "malformed"


def test_decode_rejects_v1_payload_with_a_stray_retry_after_ms_key() -> None:
    fields = json.loads(_v1_payload())
    fields["retry_after_ms"] = 2_000
    payload = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with pytest.raises(DeadLetterEnvelopeError, match="missing or unexpected field"):
        decode_envelope(payload)


def test_decode_rejects_v2_payload_missing_retry_after_ms() -> None:
    fields = json.loads(encode_envelope(_envelope()))
    del fields["retry_after_ms"]
    payload = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with pytest.raises(DeadLetterEnvelopeError, match="missing or unexpected field"):
        decode_envelope(payload)


def test_retry_after_ms_none_with_non_malformed_failure_class_is_rejected() -> None:
    envelope = _envelope(failure_class="missing_endpoint", retry_after_ms=None)
    with pytest.raises(DeadLetterEnvelopeError, match="retry_after_ms is required"):
        encode_envelope(envelope)


def test_retry_after_ms_set_with_malformed_failure_class_is_rejected() -> None:
    envelope = _envelope(failure_class="malformed", retry_after_ms=1_700_000_001_000)
    with pytest.raises(DeadLetterEnvelopeError, match="must be None"):
        encode_envelope(envelope)


def test_retry_after_ms_before_first_failure_at_ms_is_rejected() -> None:
    envelope = _envelope(
        failure_class="missing_endpoint",
        first_failure_at_ms=1_700_000_001_000,
        retry_after_ms=1_700_000_000_000,
    )
    with pytest.raises(DeadLetterEnvelopeError, match="must be >= first_failure_at_ms"):
        encode_envelope(envelope)


def test_route_unresolved_endpoint_round_trips_retry_after_ms() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0",
        kind="edge", clock=lambda: 1_700_000_000_000,
    )
    record = EdgeRecord(source_key="p1", target_key="p2", properties={"since": "2020-01-01"})
    sink.route_unresolved_endpoint(
        topic="knows-events", partition=0, offset=42, record=record, edge_type="KNOWS",
        source_key_property="personId", target_key_property="personId",
        failure_class="missing_endpoint", reason_code="missing_target",
        error_text="target not found", retry_after_seconds=1.5,
    )
    envelope = producer.publish.call_args[0][1]
    assert envelope.failure_class == "missing_endpoint"
    assert envelope.entity_kind == "edge"
    assert envelope.entity_name == "KNOWS"
    assert envelope.reason_code == "missing_target"
    assert envelope.attempt_count == 0
    assert envelope.first_failure_at_ms == 1_700_000_000_000
    assert envelope.retry_after_ms == 1_700_000_001_500


def test_route_unresolved_endpoint_value_bytes_is_replayable_through_normalize_edge_record() -> None:
    """value_bytes must be the loader's own wire *input* shape (Story 2 fix,
    reviewer-found): dead_letter_replay.py republishes it verbatim to the
    source topic, where normalize_edge_record reads it as a fresh message.
    """
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0",
        kind="edge", clock=lambda: 1_700_000_000_000,
    )
    record = EdgeRecord(
        source_key="p1", target_key="p2",
        properties={"weight": 0.5, "note": None},
    )
    sink.route_unresolved_endpoint(
        topic="knows-events", partition=0, offset=1, record=record, edge_type="KNOWS",
        source_key_property="personId", target_key_property="personId",
        failure_class="ambiguous_endpoint", reason_code="ambiguous_source",
        error_text="multiple sources matched", retry_after_seconds=2.0,
    )
    envelope = producer.publish.call_args[0][1]
    decoded_value = json.loads(envelope.value_bytes.decode("utf-8"))
    assert decoded_value == {
        "source": {"personId": "p1"}, "target": {"personId": "p2"},
        "properties": {"weight": 0.5, "note": None},
    }
    assert json.loads(envelope.key_bytes.decode("utf-8")) == ["p1", "p2"]


def test_route_unresolved_endpoint_value_bytes_round_trips_date_and_datetime() -> None:
    """MUST-FIX (reviewer): json.dumps(default=str) wrote a space-separated
    datetime the loader's own parser rejects. This proves the fix produces
    exactly what normalize_property_value would accept back.
    """
    import datetime as dt

    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0",
        kind="edge", clock=lambda: 1_700_000_000_000,
    )
    record = EdgeRecord(
        source_key=dt.date(2024, 1, 2),
        target_key="p2",
        properties={
            "since": dt.date(2024, 1, 2),
            "created_at": dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc),
        },
    )
    sink.route_unresolved_endpoint(
        topic="knows-events", partition=0, offset=1, record=record, edge_type="KNOWS",
        source_key_property="birthDate", target_key_property="personId",
        failure_class="missing_endpoint", reason_code="missing_source",
        error_text="source not found", retry_after_seconds=1.0,
    )
    envelope = producer.publish.call_args[0][1]
    decoded_value = json.loads(envelope.value_bytes.decode("utf-8"))
    assert decoded_value["source"]["birthDate"] == "2024-01-02"
    assert decoded_value["properties"]["since"] == "2024-01-02"
    assert decoded_value["properties"]["created_at"] == "2024-01-02T03:04:05+00:00"
    # And every value re-parses through the loader's own normalizer exactly
    # as it was before being dead-lettered.
    assert date.fromisoformat(decoded_value["source"]["birthDate"]) == dt.date(2024, 1, 2)
    assert datetime.fromisoformat(decoded_value["properties"]["created_at"]) == record.properties["created_at"]


def test_route_unresolved_endpoint_rejects_malformed_failure_class_before_any_kafka_call() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0", kind="edge",
    )
    record = EdgeRecord(source_key="p1", target_key="p2", properties={})
    with pytest.raises(DeadLetterEnvelopeError, match="missing_endpoint or ambiguous_endpoint"):
        sink.route_unresolved_endpoint(
            topic="knows-events", partition=0, offset=1, record=record, edge_type="KNOWS",
            source_key_property="personId", target_key_property="personId",
            failure_class="malformed", reason_code="x", error_text="y", retry_after_seconds=1.0,
        )
    producer.publish.assert_not_called()


def test_route_unresolved_endpoint_is_edge_only() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="person-events.dlq", run_id="run-1", replica_id="0", kind="node",
    )
    record = EdgeRecord(source_key="p1", target_key="p2", properties={})
    with pytest.raises(DeadLetterEnvelopeError, match="edge-only"):
        sink.route_unresolved_endpoint(
            topic="knows-events", partition=0, offset=1, record=record, edge_type="KNOWS",
            source_key_property="personId", target_key_property="personId",
            failure_class="missing_endpoint", reason_code="x", error_text="y", retry_after_seconds=1.0,
        )


def test_route_unresolved_endpoint_rejects_non_finite_retry_after_seconds() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0", kind="edge",
    )
    record = EdgeRecord(source_key="p1", target_key="p2", properties={})
    for bad in (float("inf"), float("nan")):
        with pytest.raises(DeadLetterEnvelopeError, match="finite"):
            sink.route_unresolved_endpoint(
                topic="knows-events", partition=0, offset=1, record=record, edge_type="KNOWS",
                source_key_property="personId", target_key_property="personId",
                failure_class="missing_endpoint", reason_code="x", error_text="y",
                retry_after_seconds=bad,
            )
    producer.publish.assert_not_called()


def test_publish_ordering_uses_real_producer_produce_then_flush(monkeypatch) -> None:
    """A real DeadLetterProducer wired around a mocked confluent Producer.

    Proves the sequence is produce(), then flush() (where delivery is
    confirmed in real librdkafka), and that on_dlq_routed only fires after
    that -- not the loader-level mocks used elsewhere in this file.
    """

    manager = MagicMock()
    kafka_producer = MagicMock()
    manager.attach_mock(kafka_producer.produce, "produce")
    manager.attach_mock(kafka_producer.flush, "flush")
    listener = MagicMock()
    manager.attach_mock(listener, "on_dlq_routed")
    monkeypatch.setattr(dead_letter, "on_dlq_routed", listener)

    state = {"on_delivery": None}

    def produce(topic, key, value, on_delivery):
        state["on_delivery"] = on_delivery

    def flush():
        state["on_delivery"](None, MagicMock())
        return 0

    kafka_producer.produce.side_effect = produce
    kafka_producer.flush.side_effect = flush

    DeadLetterProducer(kafka_producer).publish("person-events.dlq", _envelope())

    assert [entry[0] for entry in manager.mock_calls] == ["produce", "flush", "on_dlq_routed"]


def test_publish_ordering_failure_never_reaches_on_dlq_routed(monkeypatch) -> None:
    kafka_producer = MagicMock()
    listener = MagicMock()
    monkeypatch.setattr(dead_letter, "on_dlq_routed", listener)

    def produce(topic, key, value, on_delivery):
        pass

    def flush():
        return 1  # undelivered

    kafka_producer.produce.side_effect = produce
    kafka_producer.flush.side_effect = flush

    with pytest.raises(DeadLetterPublishError):
        DeadLetterProducer(kafka_producer).publish("person-events.dlq", _envelope())
    listener.assert_not_called()


@pytest.mark.parametrize("exc, expected", [
    (UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad"), "decode_error"),
    (json.JSONDecodeError("bad", "{", 0), "invalid_json"),
    (ValueError("property 'x' is not declared in the schema"), "undeclared_property"),
    (ValueError("property 'x' is required"), "missing_required"),
    (ValueError("message payload is empty"), "empty_payload"),
    (ValueError("JSON payload must be a top-level object"), "not_an_object"),
    (ValueError("something else entirely"), "invalid_value"),
    (RuntimeError("unexpected"), "invalid_value"),
])
def test_reason_code_classifies_every_branch_and_never_blank(exc, expected) -> None:
    code = _reason_code(exc)
    assert code == expected
    assert code.strip() != ""


def test_build_malformed_record_sink_returns_rejection_sink_when_disabled(tmp_path) -> None:
    sink, producer = build_malformed_record_sink(
        dead_letter=None,
        entity_topic="person-events",
        entity_kind="node",
        rejection_log_path=str(tmp_path / "r.jsonl"),
        bootstrap_servers="kafka:9092",
        run_id="run-1",
        replica_id="0",
    )
    assert isinstance(sink, RejectionSink)
    assert producer is None


def test_build_malformed_record_sink_returns_dead_letter_sink_when_enabled() -> None:
    from src.models.schema import DeadLetterConfig

    fake_producer = MagicMock()
    sink, producer = build_malformed_record_sink(
        dead_letter=DeadLetterConfig(enabled=True, topic="person-events.dlq"),
        entity_topic="person-events",
        entity_kind="node",
        rejection_log_path="unused.jsonl",
        bootstrap_servers="kafka:9092",
        run_id="run-1",
        replica_id="0",
        producer_factory=lambda config: fake_producer,
    )
    assert isinstance(sink, DeadLetterRecordSink)
    assert producer is fake_producer
    assert sink._topic == "person-events.dlq"


def test_build_malformed_record_sink_uses_default_topic_naming_when_topic_unset() -> None:
    from src.models.schema import DeadLetterConfig

    fake_producer = MagicMock()
    sink, producer = build_malformed_record_sink(
        dead_letter=DeadLetterConfig(enabled=True),
        entity_topic="person-events",
        entity_kind="node",
        rejection_log_path="unused.jsonl",
        bootstrap_servers="kafka:9092",
        run_id="run-1",
        replica_id="0",
        producer_factory=lambda config: fake_producer,
    )
    assert sink._topic == "person-events.dlq"


# ── P5-S4 Story 2: parking-lot topic provisioning, reroute_or_park, on_parked ──


def test_ensure_parked_pair_index_topic_creates_compacted_topic() -> None:
    admin = MagicMock()
    source_topic_metadata = MagicMock()
    source_topic_metadata.partitions = {0: object(), 1: object()}
    admin.list_topics.return_value.topics = {"knows-events.dlq": source_topic_metadata}
    future = MagicMock()
    future.result.return_value = None
    admin.create_topics.return_value = {"knows-events.dlq.parked-pairs": future}

    ensure_parked_pair_index_topic(
        "kafka:9092", "knows-events.dlq.parked-pairs", source_topic="knows-events.dlq",
        admin_factory=_admin_factory(admin),
    )

    admin.create_topics.assert_called_once()
    (new_topics,), _ = admin.create_topics.call_args
    new_topic = new_topics[0]
    assert new_topic.topic == "knows-events.dlq.parked-pairs"
    assert new_topic.num_partitions == 2
    assert new_topic.config == PARKED_PAIR_INDEX_TOPIC_CONFIG
    assert PARKED_PAIR_INDEX_TOPIC_CONFIG["cleanup.policy"] == "compact"


def test_ensure_parked_pair_index_topic_noops_when_present_and_compact() -> None:
    admin = MagicMock()
    admin.list_topics.return_value.topics = {"knows-events.dlq.parked-pairs": object()}
    entries = {"cleanup.policy": MagicMock(value="compact")}
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: entries.get(key, default)
    future = MagicMock()
    future.result.return_value = config
    admin.describe_configs.return_value = {"resource": future}

    ensure_parked_pair_index_topic(
        "kafka:9092", "knows-events.dlq.parked-pairs", source_topic="knows-events.dlq",
        admin_factory=_admin_factory(admin),
    )
    admin.create_topics.assert_not_called()


def test_ensure_parked_pair_index_topic_fails_closed_when_existing_topic_not_compacted() -> None:
    admin = MagicMock()
    admin.list_topics.return_value.topics = {"knows-events.dlq.parked-pairs": object()}
    entries = {"cleanup.policy": MagicMock(value="delete")}
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: entries.get(key, default)
    future = MagicMock()
    future.result.return_value = config
    admin.describe_configs.return_value = {"resource": future}

    with pytest.raises(RuntimeError, match="cleanup.policy"):
        ensure_parked_pair_index_topic(
            "kafka:9092", "knows-events.dlq.parked-pairs", source_topic="knows-events.dlq",
            admin_factory=_admin_factory(admin),
        )


def test_ensure_parked_pair_index_topic_tolerates_concurrent_creation_race() -> None:
    admin = MagicMock()
    admin.list_topics.return_value.topics = {}
    admin.create_topics.side_effect = RuntimeError("TOPIC_ALREADY_EXISTS")

    ensure_parked_pair_index_topic(
        "kafka:9092", "knows-events.dlq.parked-pairs", source_topic="knows-events.dlq",
        admin_factory=_admin_factory(admin),
    )  # must not raise


def test_on_parked_default_is_a_silent_no_op() -> None:
    assert dead_letter.on_parked("edge", "KNOWS", "knows-events.dlq.parked") is None


def _unresolved_envelope(**overrides) -> DeadLetterEnvelope:
    fields = dict(
        schema_version=SCHEMA_VERSION,
        failure_class="missing_endpoint",
        entity_kind="edge",
        entity_name="KNOWS",
        original_topic="knows-events",
        original_partition=0,
        original_offset=5,
        key_bytes=b'["p1","p2"]',
        value_bytes=b'{"source":{"personId":"p1"},"target":{"personId":"p2"},"properties":{}}',
        reason_code="missing_target",
        error_text="target not found",
        loader_identity=LoaderIdentity(run_id="run-1", replica_id="0", kind="edge"),
        first_failure_at_ms=1_700_000_000_000,
        attempt_count=2,
        dlq_routed_at_ms=1_700_000_000_500,
        retry_after_ms=1_700_000_001_500,
    )
    fields.update(overrides)
    return DeadLetterEnvelope(**fields)


def test_reroute_or_park_re_dlq_increments_attempt_and_recomputes_retry_after() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0",
        kind="edge", clock=lambda: 1_700_000_010_000,
    )
    envelope = _unresolved_envelope(attempt_count=2)
    outcome = sink.reroute_or_park(
        envelope=envelope, reason_code="missing_target", error_text="still missing",
        retry_after_seconds_for_reattempt=4.0, parked_topic=None,
    )
    assert outcome == "re_dlq"
    (topic, published), kwargs = producer.publish.call_args
    assert topic == "knows-events.dlq"
    assert published.attempt_count == 3
    assert published.retry_after_ms == 1_700_000_014_000
    assert published.reason_code == "missing_target"
    assert published.error_text == "still missing"
    assert kwargs.get("fire_dlq_routed_hook", True) is True
    # Untouched fields preserved byte-for-byte.
    assert published.original_topic == envelope.original_topic
    assert published.original_partition == envelope.original_partition
    assert published.original_offset == envelope.original_offset
    assert published.first_failure_at_ms == envelope.first_failure_at_ms
    assert published.loader_identity == envelope.loader_identity


def test_reroute_or_park_parks_when_caller_passes_parked_topic() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0",
        kind="edge", clock=lambda: 1_700_000_010_000,
    )
    envelope = _unresolved_envelope(attempt_count=4)
    outcome = sink.reroute_or_park(
        envelope=envelope, reason_code="missing_target", error_text="still missing",
        retry_after_seconds_for_reattempt=4.0, parked_topic="knows-events.dlq.parked",
    )
    assert outcome == "parked"
    (topic, published), kwargs = producer.publish.call_args
    assert topic == "knows-events.dlq.parked"
    assert published.attempt_count == 5
    # retry_after_ms is carried over unchanged (purely informational once
    # parked): the envelope contract requires a nonnegative retry_after_ms
    # for any non-"malformed" failure_class, and the parking-lot topic is
    # never read for scheduling.
    assert published.retry_after_ms == envelope.retry_after_ms
    assert kwargs.get("fire_dlq_routed_hook") is False
    assert published.original_topic == envelope.original_topic
    assert published.original_partition == envelope.original_partition
    assert published.original_offset == envelope.original_offset
    assert published.first_failure_at_ms == envelope.first_failure_at_ms
    assert published.loader_identity == envelope.loader_identity


def test_reroute_or_park_increment_attempt_false_leaves_attempt_count_unchanged() -> None:
    """Review finding M1: a row blocked purely behind an earlier outstanding
    record of the same pair (blocked_by_parked_pair/
    blocked_by_earlier_unresolved_row) is not itself a resolution failure --
    re-DLQing it must not consume its own attempt budget."""
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0",
        kind="edge", clock=lambda: 1_700_000_010_000,
    )
    envelope = _unresolved_envelope(attempt_count=2)
    outcome = sink.reroute_or_park(
        envelope=envelope, reason_code="blocked_by_parked_pair", error_text="waiting on an earlier row",
        retry_after_seconds_for_reattempt=1.0, parked_topic=None, increment_attempt=False,
    )
    assert outcome == "re_dlq"
    (_topic, published), _kwargs = producer.publish.call_args
    assert published.attempt_count == envelope.attempt_count == 2


def test_reroute_or_park_fires_on_parked_exactly_once_for_park_only(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(dead_letter, "on_parked", lambda kind, name, topic: calls.append((kind, name, topic)))
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0",
        kind="edge", clock=lambda: 1_700_000_010_000,
    )
    envelope = _unresolved_envelope(attempt_count=4)

    sink.reroute_or_park(
        envelope=envelope, reason_code="x", error_text="y",
        retry_after_seconds_for_reattempt=1.0, parked_topic="knows-events.dlq.parked",
    )
    assert calls == [("edge", "KNOWS", "knows-events.dlq.parked")]

    calls.clear()
    sink.reroute_or_park(
        envelope=envelope, reason_code="x", error_text="y",
        retry_after_seconds_for_reattempt=1.0, parked_topic=None,
    )
    assert calls == []  # never fired for a re-DLQ publish


def test_reroute_or_park_rejects_malformed_before_any_kafka_call() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0", kind="edge",
    )
    envelope = _unresolved_envelope(failure_class="malformed", retry_after_ms=None)
    with pytest.raises(DeadLetterEnvelopeError, match="never be called for failure_class='malformed'"):
        sink.reroute_or_park(
            envelope=envelope, reason_code="x", error_text="y",
            retry_after_seconds_for_reattempt=1.0, parked_topic=None,
        )
    producer.publish.assert_not_called()


def test_reroute_or_park_rejects_non_finite_retry_after_seconds() -> None:
    producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=producer, topic="knows-events.dlq", run_id="run-1", replica_id="0", kind="edge",
    )
    envelope = _unresolved_envelope()
    for bad in (float("inf"), float("nan")):
        with pytest.raises(DeadLetterEnvelopeError, match="finite"):
            sink.reroute_or_park(
                envelope=envelope, reason_code="x", error_text="y",
                retry_after_seconds_for_reattempt=bad, parked_topic=None,
            )


def test_publish_fire_dlq_routed_hook_false_suppresses_on_dlq_routed(monkeypatch) -> None:
    """Direct unit test of the flag on DeadLetterProducer itself, using the
    real class (not a bare MagicMock sink) so the suppression is proven at
    the actual call site, not merely asserted via kwargs."""
    calls = []
    monkeypatch.setattr(dead_letter, "on_dlq_routed", lambda *args: calls.append(args))
    fake_confluent_producer = MagicMock()
    fake_confluent_producer.flush.return_value = 0
    producer = DeadLetterProducer(fake_confluent_producer)
    envelope = _unresolved_envelope(attempt_count=5)

    producer.publish("knows-events.dlq.parked", envelope, fire_dlq_routed_hook=False)
    assert calls == []

    producer.publish("knows-events.dlq", envelope, fire_dlq_routed_hook=True)
    assert calls == [("edge", "KNOWS", "knows-events.dlq", "missing_endpoint")]


# ── Batched publish: publish_many / reroute_or_park_many (retry drain throughput) ──


class _AckingProducer:
    """A producer that acknowledges (or rejects) each message via its own
    delivery callback on flush(), like librdkafka does."""

    def __init__(self, reject_keys=()) -> None:
        self.produced: list[tuple[str, bytes | None]] = []
        self.flush_calls = 0
        self._pending: list = []
        self._reject = set(reject_keys)

    def produce(self, topic, key=None, value=None, on_delivery=None, **kwargs) -> None:
        self.produced.append((topic, key))
        self._pending.append((key, on_delivery))

    def flush(self, *args, **kwargs) -> int:
        self.flush_calls += 1
        pending, self._pending = self._pending, []
        for key, on_delivery in pending:
            on_delivery(RuntimeError("rejected") if key in self._reject else None, None)
        return 0


def test_publish_many_uses_one_flush_and_reports_each_items_own_result(monkeypatch) -> None:
    producer = _AckingProducer(reject_keys={b'["bad","x"]'})
    routed = []
    monkeypatch.setattr(dead_letter, "on_dlq_routed", lambda *args: routed.append(args))
    good1 = _envelope(key_bytes=b'["a","b"]')
    bad = _envelope(key_bytes=b'["bad","x"]')
    good2 = _envelope(key_bytes=b'["c","d"]')

    results = dead_letter.DeadLetterProducer(producer).publish_many([
        ("t.dlq", good1, True), ("t.dlq", bad, True), ("t.parked", good2, False),
    ])

    assert producer.flush_calls == 1
    assert results[0] is None and results[2] is None
    assert isinstance(results[1], dead_letter.DeadLetterPublishError)
    assert len(routed) == 1  # only the acknowledged item that asked for the hook; none for the rejected one


def test_publish_many_reports_an_undeliverable_envelope_without_raising() -> None:
    producer = _AckingProducer()
    results = dead_letter.DeadLetterProducer(producer).publish_many([
        ("t.dlq", _envelope(), True), ("t.dlq", _envelope(value_bytes=b"x" * (DEAD_LETTER_MAX_ENVELOPE_BYTES + 1)), True),
    ])
    assert results[0] is None and isinstance(results[1], dead_letter.DeadLetterPublishError)
    assert len(producer.produced) == 1 and producer.flush_calls == 1


def test_reroute_or_park_many_matches_the_single_item_calls(monkeypatch) -> None:
    producer = _AckingProducer()
    parked_hooks = []
    monkeypatch.setattr(dead_letter, "on_parked", lambda *args: parked_hooks.append(args))
    sink = DeadLetterRecordSink(
        producer=dead_letter.DeadLetterProducer(producer), topic="knows-events.dlq", run_id="run-1",
        replica_id="0", kind="edge", clock=lambda: 1_700_000_010_000,
    )
    retried = _unresolved_envelope(attempt_count=1, original_offset=1)
    parked = _unresolved_envelope(attempt_count=4, original_offset=2)
    blocked = _unresolved_envelope(attempt_count=1, original_offset=3)

    results = sink.reroute_or_park_many([
        dict(envelope=retried, reason_code="missing_target", error_text="e",
             retry_after_seconds_for_reattempt=4.0, parked_topic=None),
        dict(envelope=parked, reason_code="missing_target", error_text="e",
             retry_after_seconds_for_reattempt=4.0, parked_topic="knows-events.dlq.parked"),
        dict(envelope=blocked, reason_code="blocked_by_earlier_unresolved_row", error_text="e",
             retry_after_seconds_for_reattempt=0.5, parked_topic=None, increment_attempt=False),
    ])

    assert results == ["re_dlq", "parked", "re_dlq"]
    assert producer.flush_calls == 1
    assert [topic for topic, _key in producer.produced] == [
        "knows-events.dlq", "knows-events.dlq.parked", "knows-events.dlq",
    ]
    assert parked_hooks == [("edge", "KNOWS", "knows-events.dlq.parked")]  # only the park fired on_parked


def test_reroute_or_park_many_reports_a_bad_request_and_a_rejected_publish_per_item() -> None:
    producer = _AckingProducer(reject_keys={b'["p9","p9"]'})
    sink = DeadLetterRecordSink(
        producer=dead_letter.DeadLetterProducer(producer), topic="knows-events.dlq", run_id="run-1",
        replica_id="0", kind="edge",
    )
    malformed = _envelope(failure_class="malformed", retry_after_ms=None)
    rejected = _unresolved_envelope(key_bytes=b'["p9","p9"]')
    fine = _unresolved_envelope()
    common = dict(reason_code="r", error_text="e", retry_after_seconds_for_reattempt=1.0, parked_topic=None)

    results = sink.reroute_or_park_many([
        dict(envelope=malformed, **common), dict(envelope=rejected, **common), dict(envelope=fine, **common),
    ])

    assert isinstance(results[0], Exception) and isinstance(results[1], Exception)
    assert results[2] == "re_dlq"


# ── v7-S4: one DLQ per multi-topic relationship type ─────────────────────────


def _topics_metadata(sizes: dict[str, int]) -> dict:
    out = {}
    for name, count in sizes.items():
        item = MagicMock()
        item.partitions = {p: object() for p in range(count)}
        out[name] = item
    return out


class _FakeAdmin:
    """Tracks created topics so a rerun sees them (idempotency)."""

    def __init__(self, sizes: dict[str, int]) -> None:
        self.sizes = dict(sizes)
        self.created: list = []

    def list_topics(self, timeout=None):
        return MagicMock(topics=_topics_metadata(self.sizes))

    def create_topics(self, new_topics):
        future = MagicMock()
        future.result.return_value = None
        for topic in new_topics:
            self.created.append(topic)
            self.sizes[topic.topic] = topic.num_partitions
        return {topic.topic: future for topic in new_topics}

    def describe_configs(self, resources):
        entries = {
            "retention.ms": MagicMock(value="-1"), "retention.bytes": MagicMock(value="-1"),
            "max.message.bytes": MagicMock(value=str(DEAD_LETTER_MAX_ENVELOPE_BYTES + 65536)),
            "cleanup.policy": MagicMock(value="compact"),
        }
        config = MagicMock()
        config.get.side_effect = lambda key, default=None: entries.get(key, default)
        future = MagicMock()
        future.result.return_value = config
        return {"resource": future}


def test_type_wide_dlq_gets_the_maximum_partition_count_and_is_idempotent() -> None:
    admin = _FakeAdmin({"works-at-a": 3, "works-at-b": 8})
    for _ in range(2):
        ensure_dead_letter_topic(
            "kafka:9092", "WORKS_AT.dlq", source_topics=["works-at-a", "works-at-b"],
            admin_factory=lambda config: admin,
        )
    assert [(t.topic, t.num_partitions) for t in admin.created] == [("WORKS_AT.dlq", 8)]
    assert admin.created[0].config == DEAD_LETTER_TOPIC_CONFIG


def test_type_wide_dlq_skips_a_topic_without_metadata(caplog) -> None:
    admin = _FakeAdmin({"works-at-b": 5})
    ensure_dead_letter_topic(
        "kafka:9092", "WORKS_AT.dlq", source_topics=["works-at-a", "works-at-b"],
        admin_factory=lambda config: admin,
    )
    assert admin.created[0].num_partitions == 5
    assert "works-at-a has no partition metadata" in caplog.text


def test_parked_pair_index_accepts_source_topics_too() -> None:
    admin = _FakeAdmin({"works-at-a": 2, "works-at-b": 6})
    ensure_parked_pair_index_topic(
        "kafka:9092", "WORKS_AT.dlq.parked-pairs", source_topics=["works-at-a", "works-at-b"],
        admin_factory=lambda config: admin,
    )
    assert admin.created[0].num_partitions == 6


@pytest.mark.parametrize("kwargs", [{}, {"source_topic": "a", "source_topics": ["a"]}, {"source_topics": []}])
def test_dlq_provisioning_needs_exactly_one_source_argument(kwargs) -> None:
    with pytest.raises(ValueError):
        ensure_dead_letter_topic("kafka:9092", "x.dlq", admin_factory=lambda config: _FakeAdmin({}), **kwargs)


def test_an_existing_narrower_dlq_is_kept_and_warned_about(caplog) -> None:
    admin = _FakeAdmin({"works-at-a": 3, "works-at-b": 8, "WORKS_AT.dlq": 3})
    ensure_dead_letter_topic(
        "kafka:9092", "WORKS_AT.dlq", source_topics=["works-at-a", "works-at-b"],
        admin_factory=lambda config: admin,
    )
    assert admin.created == []  # never resized or recreated
    assert "WORKS_AT.dlq has 3 partition(s), fewer than its widest source topic (8" in caplog.text


def test_an_existing_dlq_as_wide_as_its_topics_is_not_warned_about(caplog) -> None:
    admin = _FakeAdmin({"works-at-a": 3, "works-at-b": 8, "WORKS_AT.dlq": 8})
    ensure_dead_letter_topic(
        "kafka:9092", "WORKS_AT.dlq", source_topics=["works-at-a", "works-at-b"],
        admin_factory=lambda config: admin,
    )
    assert "fewer than its widest" not in caplog.text
