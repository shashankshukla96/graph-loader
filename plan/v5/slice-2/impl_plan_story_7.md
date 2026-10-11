# Story 7 implementation plan: Dead-Letter Replay Command

## Files

- New: `src/loader/dead_letter_replay.py`
- Edit: `src/cli.py` (new `replay-dlq` subcommand)
- Edit: `docs/USAGE.md` (new subsection near §10, cross-referenced from the
  migration procedure)
- New: `tests/test_dead_letter_replay.py`

## Design

### `src/loader/dead_letter_replay.py`

```python
@dataclass(frozen=True)
class ReplayFilter:
    reason_code: str | None
    since_ms: int | None
    until_ms: int | None
    limit: int | None

@dataclass(frozen=True)
class ReplaySummary:
    matched: int
    replayed: int
    dry_run: bool

class DeadLetterReplayError(RuntimeError): ...
```

**Resolution**: caller (the CLI) resolves `--node-label`/`--edge-type` to a
`NodeConfig`/`EdgeConfig`, reads its `dead_letter` field, computes the DLQ
topic via `dead_letter_topic_name` (raising if `dead_letter` is
`None`/disabled -- there is nothing to replay), and the default target
topic (`config.topic`, overridable by `--target-topic`). This module's
entry point takes the already-resolved `dlq_topic`/`target_topic` plus
`bootstrap_servers` and the filter -- it does not know about `NodeConfig`/
`EdgeConfig` at all, keeping it schema-agnostic like `edge_planner.py`'s
boundary-capture code.

**Reader** (`_read_dlq_boundary`, mirrors `edge_planner.capture_edge_boundary`
and `docs/USAGE.md`'s existing "bulk boundary" pattern): a dedicated,
non-group consumer (`group.id` a fresh `uuid4` string, matching
`capture_edge_boundary`'s precedent), `assign()`'d to every partition of the
DLQ topic. Low watermark by default; `--since` resolves to an offset via
`offsets_for_times` instead (per-partition, clamped to that partition's low
watermark if the timestamp precedes all its records, and to the high
watermark -- i.e. skip the partition -- if it's `None`, meaning no message
at or after that timestamp). High watermark captured once at the start,
per partition, as the read boundary -- a DLQ topic still receiving new
failures during a replay run does not turn into an unbounded tail. **Never
commits an offset on the DLQ topic** (no `enable.auto.commit` even needed,
since this consumer's `group.id` isn't shared with anything and it never
calls `.commit()`).

**Filtering**: `decode_envelope` (Story 1) every message in
`[low_or_since, high)` for each partition, in offset order per partition
(partitions interleaved is fine -- there is no cross-partition ordering
guarantee to preserve, matching the DLQ's own no-order-guarantee nature).
Keep an envelope iff: `reason_code` filter is `None` or equals
`envelope.reason_code` exactly; `since_ms`/`until_ms` filters are `None` or
`since_ms <= envelope.first_failure_at_ms <= until_ms` (`until_ms` defaults
to no upper bound). `--limit` caps the number of *matching* envelopes
returned (checked after the reason/time filters, stopping the scan early
once reached) -- not the number of messages scanned.

**Dry-run**: for each matching envelope, log one line: `topic/partition/
offset`, `reason_code`, `error_text` truncated to e.g. 200 chars (never the
raw payload -- it may be sensitive). Print a summary (`matched=N
dry_run=true`). Zero `produce()` calls -- the dry-run code path does not
even construct a `Producer`.

**Live replay**: construct one `Producer` (synchronous
produce-then-flush-then-check, the same pattern
`DeadLetterProducer.publish` already uses -- this module reuses that exact
helper shape rather than reinventing it, calling into a small shared
`_synchronous_produce(producer, topic, key, value) -> None` used by both).
For each matching envelope in order: produce `envelope.value_bytes` with
key `envelope.key_bytes` to `target_topic`. **Fail closed on the batch**:
the first unacknowledged replay stops immediately; report how many
succeeded (`ReplaySummary.replayed`) and exit non-zero -- never skip a
record and claim overall success.

**Idempotency note** (module docstring and `docs/USAGE.md`): downstream
writes are `MERGE`-based, so replaying an already-fixed-and-reproduced
record, or running the same replay command twice, is safe -- no
deduplication bookkeeping is implemented or needed (at-least-once,
matching P5-Q5's precedent).

### `src/cli.py`

New subcommand:
```
python -m src.cli replay-dlq --config graph.yaml
  (--node-label LABEL | --edge-type TYPE) [--reason-code CODE]
  [--since ISO8601] [--until ISO8601] [--limit N] [--target-topic TOPIC]
  [--dry-run]
```
`handle_replay_dlq(args) -> int`: loads the schema, resolves the entity
(mutually exclusive `--node-label`/`--edge-type`, exactly one required --
`argparse` mutually-exclusive-group with `required=True`), rejects
`dead_letter` absent/disabled with a clear message (nothing to replay),
resolves `dlq_topic`/`target_topic`, parses `--since`/`--until` as ISO-8601
into epoch ms, calls `dead_letter_replay.run_replay(...)`, logs the
summary, returns `0` normally or `1` on a replay failure or bad input.

### Tests (`tests/test_dead_letter_replay.py`)

- Filter combinations (reason code, time range, limit) select the exact
  expected subset from a fixture set of encoded envelopes across 2+
  partitions.
- Dry-run makes zero `produce()` calls under every filter combination
  exercised.
- A live replay reproduces the original `key_bytes`/`value_bytes` exactly
  on the target topic (mocked producer, assert `produce()` args) and only
  reports success once every replayed record is acknowledged.
- A simulated mid-batch publish failure stops replay, reports the exact
  partial count, and returns non-zero without attempting the remaining
  records.
- The DLQ topic's own contents/offsets are unchanged by a replay run
  (assert the mocked consumer's `commit`-equivalent is never called; no
  `enable.auto.commit` in its config).
- `--since` resolution via `offsets_for_times`, including the "no message
  at or after this timestamp on this partition" (`None` result) case.

## Dependencies

Depends on Story 1 (envelope codec) and Story 3 (the synchronous
publish-and-verify pattern it reuses). Independently mergeable after those
two; does not depend on Stories 4-6.
