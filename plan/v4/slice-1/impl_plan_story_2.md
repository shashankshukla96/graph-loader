# Implementation Plan — Phase 4 / Slice 1 / Story 2

## Scope

Create a pure coordination lease protocol only—no producer, consumer loop,
Docker access, or loader gating. Runtime publishing is Story 3.

## Files

- Create `src/orchestrator/coordination.py` with `ClockLease`,
  `ClockProtocolError`, `encode_clock_lease`, and `decode_clock_lease`.
- Create `tests/test_coordination.py` for protocol tests.

## Contract

`ClockLease` is frozen and holds `run_id`, nonnegative `epoch`, nonnegative
`slot_id`, `issued_at_ms`, `expires_at_ms`, tuple `active_edge_types`, and an
immutable mapping of every bucket integer to one active edge type. The payload
is canonical UTF-8 JSON (`sort_keys`, compact separators), with `bucket_owners`
serialized as sorted bucket-string keys.

One private full validator constructs/validates a lease and is used by both
`encode_clock_lease` and `decode_clock_lease`; direct frozen dataclass
construction can therefore never cause an invalid publish. `expected_run_id`
must be nonblank and `now_ms` a non-bool nonnegative integer. Every integer
field rejects JSON booleans.

`decode_clock_lease` fully parses and validates a lease before comparing its
run id; it returns `None` only for a valid foreign run. It raises
`ClockProtocolError` for decode/type/identifier errors, duplicate or unsorted
active types, empty active set, missing/extra/noninteger bucket keys, owner not
active, negative epoch/slot/time, `expires_at_ms <= issued_at_ms`, or
`now_ms >= expires_at_ms`. Errors name only fields/reasons, never raw payload.
Since the protocol itself has no config parameter, bucket keys are required to
be a contiguous zero-based range inferred from the payload; Story 3 checks it
against `CoordinationConfig.bucket_count` before publishing.

## Tests

- Canonical byte-stable round trip and frozen state.
- Foreign run returns `None`; expired lease is rejected.
- Invalid UTF-8/JSON, bad types, bad timing, invalid identifiers, duplicate or
  unsorted types, noncontiguous/duplicate bucket keys, and inactive owners.
- Decode errors contain no sentinel payload value.
- Invalid direct leases passed to encode, malformed foreign payloads, and bad
  expected run/time arguments fail closed.

Run `.venv/bin/python -m pytest tests/test_coordination.py -v`.

## Rollback

Pure module/tests only; no Kafka state or external resources.
