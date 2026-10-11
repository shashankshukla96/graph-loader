# Story 5 implementation plan: Edge Loader — Route Malformed Relationship Records to the DLQ

(Shared groundwork -- `MalformedRecordSink`, `RejectionSink`,
`DeadLetterRecordSink`, `_reason_code`, `build_malformed_record_sink` -- was
already placed in `src/loader/dead_letter.py` during Story 4. This story
only wires `edge_loader.py` to it; no further shared-module changes.)

## Files

- Edit: `src/loader/edge_loader.py`
- Edit: `docs/USAGE.md` (§12 Limitations)
- Edit: `tests/test_edge_loader.py`

## Design

`edge_loader.py`'s `_buffer_message` malformed branch (identical shape to
`node_loader.py`'s, confirmed at Story 3/4 review time: it fires in the
`except (UnicodeDecodeError, json.JSONDecodeError, EdgeRecordValidationError,
ValueError)` branch, strictly *before* `self._slot_gating`'s
`SlotAwareAdmissionBuffer.add(...)` call -- so a malformed record never
reaches lease/slot admission regardless of which relationship-execution
mode is active):

- Swap `self._rejection_sink.append(topic=..., partition=..., offset=...,
  edge_type=self._edge_config.type, reason=reason)` for `.route(topic=...,
  partition=..., offset=..., key=message.key(), value=payload,
  reason=reason, reason_code=_reason_code(exc), edge_type=
  self._edge_config.type)`. `edge_loader.py` already imports
  `RejectionSink` from `src.loader.node_loader` (which now re-exports it
  from `dead_letter.py`) -- add `_reason_code` and
  `build_malformed_record_sink` to that same import line (still importing
  through `node_loader.py`'s re-export, not duplicating an import path, to
  keep the diff minimal; both names resolve to the identical objects
  `dead_letter.py` defines).
- `EdgeLoader.__init__`'s `rejection_sink` parameter type becomes
  `MalformedRecordSink | None`, same as `NodeLoader` -- no other change; no
  branching added.
- `main()`: mirrors `node_loader.py`'s change exactly -- call
  `build_malformed_record_sink(dead_letter=edge_config.dead_letter,
  entity_topic=edge_config.topic, entity_kind="edge",
  rejection_log_path=os.environ.get("REJECTION_LOG_PATH",
  schema.loading.rejection_log_path), bootstrap_servers=bootstrap_servers,
  run_id=run_id, replica_id=args.replica_id)`, pass the sink to
  `EdgeLoader(...)`, and flush the returned `dlq_producer` (if not `None`)
  in the existing `finally` block, alongside (not merged with) the existing
  bulk-mode control/lease producer's own flush.
- No change to `planned_writer.py` or `edge_planner.py` (explicitly out of
  scope, confirmed by grep: neither module is touched by this story).
- `docs/USAGE.md` §12: add the sentence Story 5's spec text specifies,
  directly under the existing "Planned mode fails closed..." bullet.

## Tests (`tests/test_edge_loader.py`)

Mirrors Story 4's list for relationship records:
- envelope byte-fidelity (raw key/value bytes preserved, edge_type as
  entity_name);
- DLQ publish-failure leaves the offset uncommitted;
- mixed batch (malformed DLQ'd immediately + valid rows written later in
  one transaction) resolves independently, final offset covers both;
- `dead_letter` absent/disabled is byte-identical to today's JSONL output;
- rebalance-mid-publish safety, reusing the existing
  `_settle_before_revoke`/`_on_revoke` edge fixtures with a mocked sink;
- at-least-once redelivery note (two `.route()` calls for the same offset
  produce two envelopes with different `dlq_routed_at_ms`).

Edge-specific addition: a malformed relationship record under **slot-gated**
(lease) mode is DLQ'd at buffer time and never reaches
`SlotAwareAdmissionBuffer` -- assert the buffer's size/record count is
unchanged after processing a malformed record with slot gating configured
(construct an `EdgeLoader` with `slot_admission`/`slot_buffer` set, feed one
malformed message, assert `slot_buffer` was never touched).

## Dependencies

Depends on Stories 1, 2, 3, and reuses Story 4's shared factory/classifier
verbatim (no new shared code). Blocks Story 6.
