# Phase 5 / Slice 2 Completion — Dead-Letter Routing for Malformed Node and Relationship Records

## Delivered scope vs. Definition of Done

All 5 slice-level DoD items from `plan/v5/slices.md` and all 7 stories'
individual DoDs (`plan/v5/slice-2/stories.md`) are met:

- [x] Every malformed node record and every malformed relationship record
      is durably published to a DLQ topic (or the compat file, if
      explicitly configured) before its offset commits. Proven in the unit
      suite (`_buffer_message`/offset-commit-only-after-`DeadLetterProducer
      .publish`-returns tests in `test_node_loader.py`/`test_edge_loader.py`)
      and in the scratch Docker run (see below).
- [x] `DeadLetterConfig.enabled=False` preserves today's log-and-continue
      behavior with no DLQ publish attempted — byte-identical JSONL
      regression tests in both loader test files.
- [x] The DLQ envelope is sufficient, on its own, to reconstruct the
      original record and know why and where it failed — `DeadLetterEnvelope`
      carries the raw `key_bytes`/`value_bytes`, `original_topic/partition/
      offset`, `reason_code`, `error_text`, `loader_identity`, and
      timestamps; confirmed byte-for-byte in the scratch run.
- [x] A documented migration path exists from the JSONL rejection file to
      DLQ-topic routing, with no silent behavior change for a deployment
      that does not opt in — `docs/USAGE.md` §10 "Migrating a
      rejection-file entity to a DLQ topic"; `config/graph_schema.yaml`
      ships both existing edge `dead_letter` blocks present but
      `enabled: false` (see Deviations).
- [x] Unit tests cover both node and relationship malformed-record paths
      and the offset-commit-after-ack ordering, including rebalance
      interactions (classic and slot-gated/bucket-lease execution modes).

### Story-by-story delivery

1. **Envelope + wire codec** (`src/loader/dead_letter.py`): `DeadLetterEnvelope`,
   `LoaderIdentity`, `encode_envelope`/`decode_envelope`,
   `DeadLetterEnvelopeError`. Canonical JSON, base64 payload fields,
   exhaustive field validation shared between encode and decode.
2. **Schema — node `dead_letter` + topic naming** (`src/models/schema.py`):
   `DeadLetterConfig.topic` now optional; `NodeConfig.dead_letter` mirrors
   `EdgeConfig.dead_letter`; `dead_letter_topic_name` helper; schema-wide
   collision validation (against every node/edge/coordination/control
   topic and every other entity's DLQ topic), Kafka topic-name legality
   check, and `extra="forbid"` on `DeadLetterConfig` (rejects typos like
   `enable:` instead of `enabled:`).
3. **DLQ producer, topic provisioning, observability hook**
   (`src/loader/dead_letter.py`): `DeadLetterProducer` (synchronous
   produce-flush-check, mirroring `NodeLoader._publish_control`),
   `ensure_dead_letter_topic` (idempotent creation with
   `retention.ms=-1`/`retention.bytes=-1`, fails closed if a pre-existing
   topic violates that policy via `describe_configs`), `on_dlq_routed`
   no-op hook for Phase 6.
4. **Node loader wiring** (`src/loader/node_loader.py`): `RejectionSink`
   moved into `dead_letter.py` (re-exported for compatibility) alongside
   the new `MalformedRecordSink` protocol, `DeadLetterRecordSink`,
   `_reason_code` classifier, and `build_malformed_record_sink` factory —
   placed there from the start (not deferred to Story 5) to avoid a
   circular import the reviewer flagged during Story 2.
5. **Edge loader wiring** (`src/loader/edge_loader.py`): identical pattern,
   reusing Story 4's shared code verbatim; malformed-record classification
   narrowed to decode/JSON/normalize only, so slot-admission/lease-machinery
   exceptions can never be misclassified as a malformed record.
6. **Orchestrator wiring + migration docs** (`src/cli.py`):
   `_ensure_dead_letter_topics`, called immediately after schema load
   (before DDL, the edge planner, and image builds — not merely before
   container launch), fail-closed; skips edge DLQ topics for planned-mode
   runs (`include_edges=not planned`).
7. **Replay command** (`src/loader/dead_letter_replay.py`,
   `python -m src.cli replay-dlq`): a non-group, non-committing reader
   with a bounded (stall-timeout-protected, offset-gap-tolerant) scan;
   streamed dry-run/live replay; fail-closed on the first unacknowledged
   republish; `--dlq-topic` override so a backlog stays replayable after
   an entity is switched back to the file sink.

## Files changed

- `src/loader/dead_letter.py` (new)
- `src/loader/dead_letter_replay.py` (new)
- `src/loader/node_loader.py`
- `src/loader/edge_loader.py`
- `src/models/schema.py`
- `src/cli.py`
- `config/graph_schema.yaml`
- `docs/USAGE.md`
- `tests/test_dead_letter.py` (new)
- `tests/test_dead_letter_replay.py` (new)
- `tests/test_cli_dead_letter.py` (new)
- `tests/test_node_loader.py`
- `tests/test_edge_loader.py`
- `tests/test_schema_validation.py`
- `tests/test_auto_settings.py`
- `tests/test_cli.py` (unaffected by content, verified for regressions)
- `plan/v5/slice-2/impl_plan_story_1.md` through `impl_plan_story_7.md`

## Deviations from the plan

1. **Stories 1 and 2 landed in one commit** (`31f4db2`). The reviewer's
   fixes for both stories touched the same files
   (`src/loader/dead_letter.py`, `tests/test_dead_letter.py`) closely
   enough in sequence — most notably the Story 2 fix that moved
   `dead_letter_topic_name` out of `dead_letter.py` and into
   `src/models/schema.py` to avoid a circular import — that a clean
   per-story split was no longer practical without an artificial
   file-surgery exercise. Every other story landed as its own
   feat-then-fixes commit pair.
2. **`RejectionSink`/`build_malformed_record_sink`/the `MalformedRecordSink`
   protocol were placed in `dead_letter.py` during Story 4**, not deferred
   to Story 5 as the stories document originally sketched. The reviewer
   identified during the Story 2 review that Story 5's planned refactor
   (moving `RejectionSink` out of `node_loader.py` so `dead_letter.py`
   could offer a factory without a circular import) needed to happen a
   story earlier than planned, so Story 4 did it once and Story 5 reused
   it verbatim.
3. **`config/graph_schema.yaml`'s two existing edge `dead_letter` blocks
   (`WORKS_AT`, `KNOWS`) ship with `enabled: false`.** They were dead code
   before this slice (no loader read them); once live, leaving them
   `enabled: true` would have made this repository's own default schema
   opt every relationship type into DLQ routing with zero explicit
   operator action — the reviewer judged that inconsistent with "zero
   silent behavior change for a deployment that does not opt in," and the
   conservative reading was chosen. The blocks are kept present (with a
   comment) as a runnable example; flipping `enabled: true` is the
   documented one-line opt-in.
4. **DLQ topic provisioning (`_ensure_dead_letter_topics`) runs immediately
   after schema load**, before DDL/`apply_schema`, the edge planner, and
   image builds — earlier than the stories document's "right beside
   `_ensure_control_topic`" placement. The reviewer pointed out that
   placement, while still technically "before any container launch,"
   left several minutes of unrelated work (DDL, planning, three image
   builds) between a DLQ misconfiguration and its failure being reported.
   `_ensure_control_topic` itself was left where it was.
5. **Planned-mode edge DLQ topics are skipped**
   (`_ensure_dead_letter_topics(..., include_edges=not planned)`), an
   addition beyond the stories document's original text, because
   `planned_writer.py`/`edge_planner.py` never read `dead_letter` at all —
   provisioning or policy-validating an edge DLQ topic a planned run will
   never write to serves no purpose and could needlessly fail a planned
   run over an unrelated topic's misconfiguration.

## Known limitations

- **At-least-once DLQ delivery.** A duplicate envelope for the same
  `(topic, partition, offset)` is expected, not a bug, matching P5-Q5's
  existing counters precedent. Documented sources (docs/USAGE.md §11):
  a crash between the DLQ publish's ack and the source offset commit; a
  slot-gated (bucket-lease) streaming revoke, which always fails closed
  without settling; and a slow-but-eventually-successful publish after
  the consumer has already been evicted from its group.
- **An envelope over the 8 MiB cap stalls its partition.** Fail-closed by
  design (`DEAD_LETTER_MAX_ENVELOPE_BYTES`); documented in
  `docs/USAGE.md` §11 with the recovery procedure.
- **Replay is idempotent, not order-safe.** `MERGE` makes replaying an
  already-fixed record or re-running the same replay command safe, but
  replaying an old DLQ'd envelope for a key a newer valid record has
  since updated will overwrite that node's/relationship's properties
  back to the old values. Documented in `docs/USAGE.md` and the
  `dead_letter_replay.py` module docstring, with the recommended
  dry-run-then-staging-topic workflow.
- **`NodeLoader.process_message`** (the non-buffered, one-message-at-a-time
  path) is not used by `main()` — only the batched `_buffer_message` path
  is — and does not route a malformed record to a DLQ or the file sink;
  it logs and returns failure. Pre-existing, unchanged by this slice,
  documented in `docs/USAGE.md` §12.
- **Planned mode is entirely unaffected**, by design (owner decision
  P5-Q3): no line of `planned_writer.py`/`edge_planner.py` changed in
  this slice, confirmed by diff against the slice's base commit.
- **DLQ durability depends on the topic's `min.insync.replicas`**, which
  the broker's default (often `1`) leaves no stronger than `acks=1`
  despite the producer's `acks=all`. Documented in `docs/USAGE.md` next
  to the migration procedure.

## Test evidence

### Full unit suite (required by the workflow)

```
.venv/bin/python -m pytest tests examples/imdb/tests \
  -m "not integration and not smoke" --ignore=tests/test_dev_environment.py -q

1087 passed, 13 deselected
```

### This slice's own test files

```
.venv/bin/python -m pytest \
  tests/test_dead_letter.py tests/test_node_loader.py tests/test_edge_loader.py \
  tests/test_schema_validation.py tests/test_auto_settings.py tests/test_cli.py \
  tests/test_cli_dead_letter.py tests/test_dead_letter_replay.py -q

565 passed
```

### Lint

```
ruff check src tests examples   # (installed into a /tmp venv, not the repo venv)
All checks passed!
```

### Scratch Docker end-to-end validation

Isolated scratch containers on their own network, never touching the
shared `graph-loader-kafka`/`graph-loader-neo4j`/`graph-loader-net` stack
or the `:latest` image tags:

- `scratch-p5s2-kafka` (`confluentinc/cp-kafka:7.7.0`, `--cpuset-cpus 28-31`,
  bind-mounted data, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` matching the
  shared compose default — deliberately, to prove the DLQ producer's own
  `allow.auto.create.topics=False` guard, not broker config, is what
  prevents auto-creation).
- `scratch-p5s2-neo4j` (`neo4j:5-enterprise`, `--cpuset-cpus 28-31`,
  bind-mounted data).
- A scratch schema (`ScratchPerson` node, self-referencing `SCRATCH_KNOWS`
  edge, both `dead_letter` enabled with explicit topic names) run directly
  via `python -m src.loader.node_loader` / `python -m src.loader.edge_loader`
  against the scratch broker/database (bypassing Docker-orchestrated
  container launch — the code paths under test are the loader processes
  themselves, identical to what `docker_service.py` would run inside a
  container; this avoided any risk of rebuilding the shared `:latest`
  image tags).

Findings, each independently confirmed via `kafka-consumer-groups
--describe`, `kafka-configs --describe`, `kafka-console-consumer`, and a
Python `decode_envelope` inspection script:

1. **DLQ topic policy** (`_ensure_dead_letter_topics` run directly):
   `kafka-configs --describe` on both `scratch-person-events.dlq` and
   `scratch-knows-events.dlq` confirmed `retention.ms=-1`,
   `retention.bytes=-1`, `max.message.bytes=8454144` (exactly
   `DEAD_LETTER_MAX_ENVELOPE_BYTES + 65536`).
2. **Standalone loader before the DLQ topic exists, against an
   auto-create-enabled broker, does not auto-create it.** Ran
   `node_loader.py` against a malformed record before provisioning any
   DLQ topic: `kafka-topics --list` before and after showed no
   `.dlq` topic was created; the loader's `DeadLetterProducer.publish`
   raised `DeadLetterPublishError` (`UNKNOWN_TOPIC_OR_PART`), the loader
   logged the failure and did not commit past the malformed offset
   (`kafka-consumer-groups --describe` showed `CURRENT-OFFSET` unchanged
   across the failed attempt).
3. **DLQ-unavailable fails closed without committing.** Same run as (2):
   the malformed record's offset never advanced past it until the DLQ
   topic was provisioned and the loader was rerun.
4. **Exact envelope bytes; offset commits only after DLQ ack.** After
   provisioning the DLQ topics and rerunning both loaders to completion
   (exit 0, zero consumer-group lag), a standalone Python script read
   back every DLQ envelope with `decode_envelope` and confirmed, for all
   4 malformed records (2 node: invalid JSON and an undeclared property;
   2 edge: invalid JSON and a missing `source` field): `value_bytes`
   byte-identical to the original published payload, correct
   `reason_code` (`invalid_json`, `undeclared_property`, `invalid_value`),
   correct `entity_kind`/`entity_name`, `attempt_count=0`, and a populated
   `loader_identity`. The two valid records (1 node pair + 1 relationship)
   were confirmed written into Neo4j via `cypher-shell`.
5. **`replay-dlq --dry-run` then a live replay into a staging topic.**
   Dry-run reported `matched=2 replayed=0` with zero `produce()` calls
   (by construction — the dry-run path never even builds a producer). A
   live replay into a newly created staging topic
   (`scratch-person-staging`) reported `matched=2 replayed=2`;
   `kafka-console-consumer` on the staging topic showed both records with
   byte-identical keys/values to the original DLQ'd payloads. A
   subsequent inspection of the DLQ topic showed its contents and
   watermarks unchanged (still exactly 2 messages, same offsets), and
   `kafka-consumer-groups --list` showed no lingering consumer group for
   the replay's ephemeral, non-committing reader.

All scratch containers, the scratch Docker network, and the bind-mounted
scratch data directory were removed after validation. No shared
container, network, or `:latest` image tag was touched.

## Reviewer findings and resolutions

A single dedicated independent reviewer subagent (Opus) was kept for the
entire slice and reviewed every story plus every fix round. Summary by
story (full detail is in the commit messages of each `fix(dlq): ...`
commit):

- **Story 1** (envelope codec): `schema_version` accepted `bool`/`float`
  masquerading as `1`; deeply nested JSON crashed with a raw
  `RecursionError`; `entity_kind`/`loader_identity.kind` were never
  cross-checked. All fixed; ~30 additional edge-case tests added.
- **Story 2** (schema): `dead_letter_topic_name`'s original placement in
  `dead_letter.py` would have created a circular import once Stories 3-5
  landed — moved to `src/models/schema.py`. `config/graph_schema.yaml`'s
  default-enabled edge DLQ blocks (see Deviation 3). A typo like `enable:`
  silently left DLQ routing on — fixed with `extra="forbid"`. DLQ topic
  names weren't checked against Kafka's naming rules — fixed.
- **Story 3** (producer/topic provisioning): the 8 MiB size cap never
  took effect because the producer's own `message.max.bytes` default
  (1 MB) rejected the envelope first — fixed. An `on_dlq_routed` hook
  exception could turn an already-acknowledged publish into a reported
  failure — fixed (caught and logged). A pre-existing DLQ topic's
  retention was never checked — fixed via `describe_configs`, fail-closed.
- **Story 4** (node loader): `allow.auto.create.topics` defaulting to
  `True` on the producer, combined with the shared compose stack's
  broker-level auto-create, could let a loader silently create a
  7-day-retention DLQ topic before the CLI's provisioning ever ran —
  fixed. The Story 4 acceptance criterion for a rebalance mid-DLQ-publish
  had no test — added. A blank `run_id`/`replica_id` only failed on the
  first malformed record (a poison pill) — fixed with construction-time
  validation.
- **Story 5** (edge loader): the malformed-record `except` clause also
  wrapped the slot-admission/lease machinery, so a `ValueError` bug in
  that code (not the record) could DLQ and commit a structurally valid
  record as a terminal, never-retried failure — fixed by narrowing the
  `try` to decode/JSON/normalize only. Missing rebalance and bucket-lease
  coordinator-mode tests — added.
- **Story 6** (orchestrator wiring): DLQ provisioning originally ran
  after DDL, the planner, and image builds — moved earlier (Deviation 4).
  Planned-mode edge DLQ topics were provisioned/validated unnecessarily —
  fixed (Deviation 5). Integration tests didn't prove call order or the
  full launch-call surface — fixed with an attached parent mock and a
  broader `run_*`/`start_*` method-call assertion.
- **Story 7** (replay command): the scan could hang forever on an offset
  gap or an unreachable broker — fixed with offset-based progress
  tracking and a stall timeout. The full matching set was materialized
  in memory before any replay — fixed by streaming. An entity switched
  back to `enabled: false` became unreplayable, contradicting the
  documented reversibility — fixed by resolving the DLQ topic whenever a
  `dead_letter` block is present, plus a `--dlq-topic` override. The
  idempotency documentation overstated safety (order-safety, not just
  idempotency) — corrected. A late spot-check caught one more edge case
  (a message landing at/past the captured high watermark right after an
  offset gap) — fixed in a follow-up commit.

No must-fix item was ever left unresolved; the reviewer's final message
confirmed nothing was blocking merge.

## Sign-off

The dedicated independent reviewer subagent (Opus, read-only, no
repository access) reviewed all 7 stories and every fix round across the
slice and reported no outstanding must-fix or should-fix items as of the
final Story 7 fix commit (`a8a7164`) and the subsequent boundary-read fix
(`f547788`).
