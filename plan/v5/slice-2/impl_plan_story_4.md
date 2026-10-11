# Story 4 implementation plan: Node Loader — Route Malformed Records to the DLQ

(Also lays the shared groundwork Story 5 reuses verbatim, per the reviewer's
forward note on Story 2: to avoid a circular import — `node_loader.py`
would need `MalformedRecordSink`/`DeadLetterRecordSink` from `dead_letter.py`,
while `dead_letter.py` would need `RejectionSink` from `node_loader.py` —
`RejectionSink` moves into `dead_letter.py` now, with `node_loader.py`
re-exporting it for backward compatibility. `build_malformed_record_sink`
is placed directly in `dead_letter.py` from the start (Story 5's stated end
state) rather than in `node_loader.py` first and moved later.)

## Files

- Edit: `src/loader/dead_letter.py` (append `MalformedRecordSink`,
  `RejectionSink` (moved from `node_loader.py`), `DeadLetterRecordSink`,
  `_reason_code`, `build_malformed_record_sink`)
- Edit: `src/loader/node_loader.py`
- Edit: `tests/test_dead_letter.py`
- Edit: `tests/test_node_loader.py`

## Design

### `dead_letter.py` additions

```python
class MalformedRecordSink(Protocol):
    def route(self, *, topic: str, partition: int, offset: int,
              key: bytes | None, value: bytes | None, reason: str,
              reason_code: str, node_label: str | None = None,
              edge_type: str | None = None) -> None: ...
```

`RejectionSink` (moved verbatim from `node_loader.py`, plus the protocol
method): `.route(...)` writes today's metadata-only JSONL line unchanged
(ignoring `key`/`value`/`reason_code` — on-disk format does not change).
`.append(...)` becomes a thin deprecated alias: `self.route(topic=topic,
partition=partition, offset=offset, key=None, value=None, reason=reason,
reason_code="", node_label=node_label, edge_type=edge_type)`. A shared
`_validate_exactly_one_discriminator(node_label, edge_type)` helper (the
existing `(node_label is None) == (edge_type is None)` check) is used by
both `RejectionSink.route` and `DeadLetterRecordSink.route`.

`DeadLetterRecordSink`: constructed with a `DeadLetterProducer`, resolved
`topic`, `run_id`, `replica_id`, `kind` ("node"/"edge"), and a wall-clock
callable. `.route(...)` builds a `DeadLetterEnvelope` (`failure_class=
"malformed"`, `attempt_count=0`, `first_failure_at_ms=dlq_routed_at_ms=
clock()`) and calls `self._producer.publish(self._topic, envelope)`. No
extra `on_dlq_routed` call here — `DeadLetterProducer.publish` already
calls it after the ack (Story 3).

`_reason_code(exc: Exception) -> str`: classifies generically over
exception *type and message text*, never importing
`NodeRecordValidationError`/`EdgeRecordValidationError` (both are
`ValueError` subclasses, so no import is needed and no cycle is created):
`UnicodeDecodeError` -> `"decode_error"`; `json.JSONDecodeError` ->
`"invalid_json"`; else if `ValueError` and `"is not declared"` in the
message -> `"undeclared_property"`; `"is required"` in the message ->
`"missing_required"`; else `"invalid_value"`. Always returns a nonblank
string (Story 1 review's poison-pill warning: `DeadLetterEnvelope.
reason_code` must never be blank, or `encode_envelope` raises and the
record's offset never commits) — the final `else` branch is unconditional,
so this is structurally guaranteed.

`build_malformed_record_sink(*, dead_letter, entity_topic, entity_kind,
entity_name, rejection_log_path, bootstrap_servers, run_id, replica_id,
producer_factory=None, clock=None) -> tuple[MalformedRecordSink, object |
None]`: resolves the effective DLQ topic via `dead_letter_topic_name`; `None`
-> `(RejectionSink(rejection_log_path), None)` (no producer to clean up);
otherwise constructs a **dedicated** `Producer` (never shared with the
control-topic producer) using `DEAD_LETTER_PRODUCER_CONFIG` plus
`bootstrap.servers`, wraps it in `DeadLetterProducer`, and returns
`(DeadLetterRecordSink(...), producer)`. The caller (`main()`) owns the
returned producer and must flush it in its own `finally` block, exactly
like the existing control-topic producer.

### `node_loader.py`

- Delete the `RejectionSink` class body; replace with `from
  src.loader.dead_letter import RejectionSink  # noqa: F401 - re-exported`
  (so `edge_loader.py`'s existing `from src.loader.node_loader import
  RejectionSink` and any test/script importing it keep working).
- `NodeLoader.__init__`'s `rejection_sink` parameter keeps its name and
  default (`RejectionSink(self._loading_config.rejection_log_path)`) for
  direct/test construction; its declared type becomes `MalformedRecordSink
  | None`. No branching added inside `__init__` — the sink is fixed once,
  by whichever object the caller passes (or the default), matching "no
  per-message branching." `main()` is the one call site that branches, via
  the factory below.
- `_buffer_message`: capture `key = message.key()` alongside the existing
  `payload = message.value()`; add `reason_code = _reason_code(exc)`; call
  `self._rejection_sink.route(topic=topic, partition=partition,
  offset=offset, key=key, value=payload, reason=reason,
  reason_code=reason_code, node_label=self._node_config.label)` in place of
  `.append(...)`.
- `main()`: replace the unconditional `RejectionSink(...)` construction
  with `sink, dlq_producer = build_malformed_record_sink(dead_letter=
  node_config.dead_letter, entity_topic=node_config.topic,
  entity_kind="node", entity_name=node_config.label,
  rejection_log_path=os.environ.get("REJECTION_LOG_PATH",
  schema.loading.rejection_log_path), bootstrap_servers=
  kafka_bootstrap_servers, run_id=args.run_id, replica_id=args.replica_id)`;
  pass `rejection_sink=sink` to `NodeLoader(...)`; add `dlq_producer` to the
  existing `finally` block's flush logic (flush if not `None`, mirroring the
  existing control-topic producer's cleanup, wrapped in its own
  try/except/log so a flush failure doesn't mask the loader's real exit code).

## Tests

`tests/test_dead_letter.py`: `_reason_code` classification table (each
exception type/message -> expected code, including the catch-all default);
`RejectionSink.route` writes the identical JSONL entry `.append` used to
(byte-for-byte regression against a captured pre-slice fixture line);
`RejectionSink.append` delegates to `.route` with `key=None, value=None,
reason_code=""`; `DeadLetterRecordSink.route` builds an envelope with
`failure_class="malformed"`, `attempt_count=0`, and calls
`producer.publish` with the resolved topic (mocked `DeadLetterProducer`);
`build_malformed_record_sink` returns `(RejectionSink, None)` when
`dead_letter` is `None`/disabled, and `(DeadLetterRecordSink, producer)`
otherwise, with the producer configured via `DEAD_LETTER_PRODUCER_CONFIG`.

`tests/test_node_loader.py` (extends the existing malformed-record tests):
- `dead_letter` enabled (a `DeadLetterRecordSink` passed as `rejection_sink`
  with a mocked inner `DeadLetterProducer`): a malformed record's exact raw
  key/value bytes appear in the published envelope; the offset commits only
  after `publish` returns without raising (mock raises -> offset stays
  uncommitted, mirroring the existing "sink failure leaves offset
  uncommitted" test).
- Mixed batch: one malformed record DLQ'd immediately + two valid records
  written later in one transaction; the malformed offset resolves
  independent of the batch flush; the final committed offset covers both
  once the batch flushes (regression of the existing gap-filling test, sink
  swapped for a mock `MalformedRecordSink`).
- `dead_letter=None`/disabled path (existing `RejectionSink` tests, updated
  to assert `.route(...)` was called with `key=..., value=..., reason_code=
  ...` instead of the old `.append(...)` signature) — same byte-identical
  on-disk JSONL output as before (unchanged `.route` file-write logic).
- Rebalance-mid-DLQ-publish: reuse the existing `_settle_before_revoke`
  fixtures with a mocked sink; assert the existing revoke-safety invariants
  are unaffected by the sink swap.
- At-least-once redelivery note: a restart re-delivering an
  already-DLQ'd-but-uncommitted message calls `.route(...)` a second time
  with a new set of `key`/`value` (same message) — documented, not
  deduplicated.

## Dependencies

Depends on Stories 1, 2, 3. Blocks Story 6. Shares the
`RejectionSink`/`MalformedRecordSink`/`build_malformed_record_sink`
groundwork with Story 5, done here once.
