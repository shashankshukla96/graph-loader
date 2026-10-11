# Story 3 implementation plan: Dead-Letter Producer, Topic Auto-Creation, Observability Hook

## Files

- Edit: `src/loader/dead_letter.py` (append below the Story 1/2 codec)
- Edit: `tests/test_dead_letter.py`

## Design

### Constants and errors

```python
DEAD_LETTER_PRODUCER_CONFIG: dict[str, object] = {
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
    "delivery.timeout.ms": 120_000,
}

# Base64 inflates a payload by ~4/3; the envelope adds a small fixed JSON
# overhead (topic/partition/offset/timestamps/identity). 8 MiB comfortably
# covers a source message up to Kafka's common 1 MiB broker default with
# headroom for the base64 inflation and JSON scaffolding, without requiring
# this code to probe the source topic's own `max.message.bytes` (which may
# not even be set — brokers fall back to the cluster default).
DEAD_LETTER_MAX_ENVELOPE_BYTES = 8 * 1024 * 1024

DEAD_LETTER_TOPIC_CONFIG: dict[str, str] = {
    "retention.ms": "-1",
    "retention.bytes": "-1",
    "cleanup.policy": "delete",
    "max.message.bytes": str(DEAD_LETTER_MAX_ENVELOPE_BYTES + 65536),
}

class DeadLetterPublishError(RuntimeError): ...
```

The `+ 65536` mirrors `edge_planner.chunk_topic_config`'s own headroom
constant for the same reason (broker-side framing overhead beyond the
payload itself).

### `DeadLetterProducer`

```python
class DeadLetterProducer:
    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    def publish(self, topic: str, envelope: DeadLetterEnvelope) -> None:
        encoded = encode_envelope(envelope)  # raises DeadLetterEnvelopeError first
        if len(encoded) > DEAD_LETTER_MAX_ENVELOPE_BYTES:
            raise DeadLetterPublishError(
                f"envelope for {envelope.entity_kind} '{envelope.entity_name}' "
                f"topic={envelope.original_topic} partition={envelope.original_partition} "
                f"offset={envelope.original_offset} is {len(encoded)} bytes, over the "
                f"{DEAD_LETTER_MAX_ENVELOPE_BYTES}-byte DLQ envelope limit"
            )
        delivery_error: Exception | None = None

        def on_delivery(error, _message):
            nonlocal delivery_error
            if error is not None:
                delivery_error = error

        self._producer.produce(topic, key=envelope.key_bytes, value=encoded, on_delivery=on_delivery)
        undelivered = self._producer.flush()
        if undelivered:
            raise DeadLetterPublishError(
                f"DLQ publish to {topic} has {undelivered} undelivered message(s)"
            )
        if delivery_error is not None:
            raise DeadLetterPublishError(f"DLQ publish to {topic} failed: {delivery_error}")
        on_dlq_routed(envelope.entity_kind, envelope.entity_name, topic, envelope.failure_class)
```

This is a deliberate, dedicated producer — never sharing a Kafka `Producer`
instance with `_publish_control`'s control-topic producer (mixing their
`flush()` undelivered counts would misattribute a control-message failure
to a DLQ publish or vice versa). `build_malformed_record_sink` (Story 4)
constructs its own `Producer`.

The pre-flight size check runs before `produce()` so an oversized envelope
fails with a clear, attributable message instead of an opaque broker
`MSG_SIZE_TOO_LARGE` delivery error days later. `on_dlq_routed` fires
strictly after a proven ack, matching Story 3's stated ordering contract.

### `on_dlq_routed` hook

```python
DlqRoutedListener = Callable[[str, str, str, str], None]  # kind, name, topic, failure_class

def on_dlq_routed(kind: str, name: str, topic: str, failure_class: str) -> None:
    """No-op by default. P6-S2 replaces/wraps this module attribute to
    increment graph_loader_dlq_routed_total{kind,name}. Not a metrics
    exporter itself — no dependency, no port, no /metrics endpoint here."""
    return None
```

Module-level function reference (like `logging` handlers), so a test or
P6-S2 can do `dead_letter.on_dlq_routed = replacement` without touching
loader code. `DeadLetterProducer.publish` calls `on_dlq_routed` (the
current module attribute, looked up at call time — not captured at
`__init__` time) so monkeypatching after construction still works.

### `ensure_dead_letter_topic`

```python
def ensure_dead_letter_topic(bootstrap_servers: str, topic: str, *, source_topic: str,
                              admin_factory=None, timeout: float = 30.0) -> None:
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient as admin_factory
    from confluent_kafka.admin import NewTopic

    admin = admin_factory({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=timeout)
    if topic in metadata.topics:
        return
    source_item = metadata.topics.get(source_topic)
    partitions = len(source_item.partitions) if source_item is not None and getattr(source_item, "partitions", None) else 1
    try:
        futures = admin.create_topics([NewTopic(
            topic, num_partitions=partitions, replication_factor=-1, config=DEAD_LETTER_TOPIC_CONFIG,
        )])
        for future in futures.values():
            future.result(timeout)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
```

Mirrors `_ensure_control_topic`/`create_chunk_topic`: `list_topics` first
(no-op if present — never overwrites an existing topic's config, per the
acceptance criterion), else `create_topics`, tolerating a concurrent
`TOPIC_ALREADY_EXISTS` race from a sibling replica/CLI launch. Unlike
`_ensure_control_topic`'s "log and continue" pattern, this function does
**not** swallow other failures — Story 6's caller (`_handle_start`) is
fail-closed per the story text, so this function must propagate a real
failure (broker unreachable, permission denied) rather than warn-and-continue.
`source_topic`'s partition count comes from the same `list_topics` metadata
call already made (no second round-trip), falling back to 1 partition if
the source topic's metadata is unexpectedly absent (defensive; schema
validation already guarantees the source topic is declared).

## Tests (extend `tests/test_dead_letter.py`)

- `DeadLetterProducer.publish` success: mocked `Producer` whose `produce`
  immediately invokes `on_delivery(None, message)`; `flush` returns 0.
  Assert `produce` was called with `topic`, `key=envelope.key_bytes`,
  `value=encode_envelope(envelope)`, and that a patched `on_dlq_routed`
  fires exactly once with `(entity_kind, entity_name, topic, failure_class)`.
- Delivery failure (mocked `on_delivery(error, message)` with a non-`None`
  error): `publish` raises `DeadLetterPublishError`; patched `on_dlq_routed`
  is called zero times.
- Nonzero `flush()` undelivered count: same assertion.
- Oversized envelope (construct one with `value_bytes` larger than
  `DEAD_LETTER_MAX_ENVELOPE_BYTES`): `publish` raises `DeadLetterPublishError`
  before calling `produce()` at all (assert `produce.assert_not_called()`).
- `ensure_dead_letter_topic`: no-ops when the topic is already in
  `list_topics()` (assert `create_topics` not called); creates with exactly
  `DEAD_LETTER_TOPIC_CONFIG` and the source topic's partition count when
  absent; tolerates a `TOPIC_ALREADY_EXISTS`-raising `create_topics` (a
  fake `admin_factory` raising that on the create call, not just the
  `list_topics` no-op path) and returns normally; propagates a different
  exception (e.g. a broker-auth failure) rather than swallowing it.
- `on_dlq_routed` default is a no-op returning `None` and doesn't raise.

## Dependencies

Depends on Story 1 (envelope/codec). Blocks Stories 4, 5, 6.
