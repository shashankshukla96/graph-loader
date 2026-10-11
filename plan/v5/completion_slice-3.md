# Phase 5 / Slice 3 Completion — Row-Level DLQ for Missing-Endpoint Relationship Rows

## Delivered scope vs. Definition of Done

All 4 slice-level DoD items from `plan/v5/slices.md` and all 7 stories'
individual DoDs (`plan/v5/slice-3/stories.md`) are met:

- [x] A batch with some rows whose endpoints exist and some whose endpoints
      don't commits the resolvable rows and DLQs only the rest, in one
      transaction. Proven at the writer level (Story 3), the lease- and
      classic-coordinator level (Stories 4-5), the full `EdgeLoader` level
      with a fake Neo4j driver (Story 6), and against a real scratch Neo4j
      + Kafka (Story 7).
- [x] The batch's Kafka offset commits only after every row in it is either
      written or durably DLQ'd; a crash between the two never double-commits
      or drops a row. Proven in Story 6's DLQ-produce-failure scenario
      (zero offsets resolve, including rows already committed to Neo4j
      moments earlier) and its successful-replay follow-up.
- [x] Missing vs. ambiguous endpoint failures are distinguishable in the DLQ
      envelope (`reason_code` — `missing_source`/`missing_target`/
      `missing_both`/`ambiguous_source`/`ambiguous_target`/`ambiguous_both`,
      plus `blocked_by_earlier_unresolved_row` for the same-pair ordering
      guard).
- [x] Bulk mode's default (fail-closed) behavior is provably unchanged
      unless the new opt-in (`route_missing_endpoints_in_bulk`) is set —
      proven by regression tests pinned against the pre-slice fixture
      shape in Story 5, and by every pre-existing test in
      `tests/test_edge_execution_writer.py`/`tests/test_bucket_lease_execution.py`/
      `tests/test_edge_loader.py` passing unmodified.
- [x] A streaming E2E scenario publishes a relationship before its node,
      confirms the row lands in the DLQ, then publishes the node and
      confirms a manual replay resolves it without data loss or
      duplication (Story 7's scratch proof; P5-S4, which would automate
      this replay, does not exist yet).

### Story-by-story delivery

1. **Schema — per-entity bulk opt-in** (`src/models/schema.py`):
   `DeadLetterConfig.route_missing_endpoints_in_bulk: bool = False`, with a
   string-typo guard and a `model_validator` requiring `enabled=True`.
   Relationship-only; `NodeConfig.dead_letter` has no equivalent (documented
   as a no-op there, matching P5-S2's precedent of an inert-but-accepted
   field).
2. **DLQ envelope — `retry_after_ms`, v1→v2 decode** (`src/loader/dead_letter.py`):
   `SCHEMA_VERSION` 1→2 with a two-version decode contract (old envelopes
   keep decoding; only v2 ever encodes). New
   `DeadLetterRecordSink.route_unresolved_endpoint()`. Two reviewer-found
   fixes landed *before* commit (see Deviations below): a `date`/`datetime`
   corruption bug in the value encoder, and the envelope's `value_bytes`
   shape.
3. **Row-level endpoint classification and the two-attempt write**
   (`src/loader/edge_execution.py`, `ApocLockedEdgeWriter`): a new
   `write_provenanced_batch()` — Attempt 1 is today's exact query
   (byte-identical cost/query-object-identity for an all-resolved batch);
   only on that aggregate check failing does a fresh transaction run a
   read-only per-row classification query and re-write the matched subset.
   `_classify_rows()` adds the same-pair ordering guard. A TOCTOU mismatch
   or a lease-loss abort fails the whole original batch closed, uncaught.
4. **Wired into the streaming lease coordinator (default path)**
   (`BucketLeaseExecutionCoordinator._write_leased`, `build_edge_execution`):
   activates unconditionally in streaming once a DLQ topic resolves; needs
   the bulk opt-in too for lease-bulk (`--edge-leases`). Reviewer-found
   must-fix: the same-pair ordering guard didn't span the multiple
   sequential transactions one lease flush can split a cell into — fixed
   with a `ParkedPairs` protocol (`_LeaseFlush.is_parked`/`mark_parked`).
5. **Wired into classic bulk (opt-in) and `edge_loader.py` construction**:
   `LaneExecutionCoordinator.execute`'s classic branch gets the identical
   cross-transaction-splitting fix (`_ParkedPairSet`, one per `execute()`
   call) — the reviewer's carry-over question from Story 4 confirmed the
   same gap exists on `LaneBatcher.drain_all()`'s multi-batch-per-lane
   splitting. `build_edge_execution` gains `mode`/`missing_endpoint_sink`
   (defaulted, not required — required would have broken ~10 existing call
   sites). `edge_loader.py:main()` reorders construction so the
   malformed-record sink is built first and reused as
   `missing_endpoint_sink` when it resolves a real `DeadLetterRecordSink`.
6. **Offset-ledger, fail-closed, and ordering correctness tests**
   (test-only, `tests/test_edge_loader.py`): proves Decisions points 5-7's
   "falls out of the existing `WorkerResult`/`PartitionLedger` contract for
   free" claim through the real `EdgeLoader`/`LaneExecutionCoordinator`/
   `BucketLeaseExecutionCoordinator`/`ApocLockedEdgeWriter` stack (fake
   Neo4j driver only) — mixed-batch commit, DLQ-produce-failure-then-replay,
   rebalance mid-batch, lease-loss through the *real* lease coordinator
   (not a mocked one — a reviewer must-fix after the first attempt proved
   nothing new), and same-pair ordering end to end. A reviewer should-fix
   added a lease-backed variant of the mixed-batch test, since streaming's
   default coordinator path was otherwise only exercised at the Story-4
   coordinator level, never through the full loader into the offset ledger.
7. **Scratch integration proof and documentation** (this story): fixed
   P6-S4's auto-activating scaffold
   (`tests/integration/test_edge_before_node_dlq.py`) to drive the real,
   now-committed interface instead of probing for one that didn't exist
   yet at its authorship; ran the full scratch proof described below;
   updated `docs/USAGE.md` (§3, §5, §10, §12).

## Files changed

- `src/models/schema.py`
- `config/graph_schema.yaml`
- `src/loader/dead_letter.py`
- `src/loader/edge_execution.py`
- `src/loader/edge_loader.py`
- `docs/USAGE.md`
- `tests/test_schema_validation.py`
- `tests/test_dead_letter.py`
- `tests/test_dead_letter_replay.py`
- `tests/test_edge_execution_writer.py`
- `tests/test_bucket_lease_execution.py`
- `tests/test_edge_loader.py`
- `tests/integration/test_edge_before_node_dlq.py` (P6-S4's scaffold,
  updated to this slice's real interface)
- `tests/test_edge_before_node_dlq_probes.py` (P6-S4's scaffold's own unit
  tests, updated to match)
- `plan/v5/completion_slice-3.md` (this file)

## Deviations from the plan

1. **`route_unresolved_endpoint`'s `value_bytes` shape deviates from
   stories.md Decisions point 3's sketch** (a reviewer "decide now" during
   Story 2). The stories doc sketched a flat `{"source_key","target_key",
   "properties"}` shape. That shape cannot be fed back through
   `normalize_edge_record`/`dead_letter_replay.py` — a bare `"source_key"`
   field is not a declared section of the loader's wire input contract and
   would itself be rejected as malformed on replay, directly contradicting
   Story 7's own documented manual-replay workflow. Switched to the
   loader's own wire **input** shape (`{"source": {key_prop: v}, "target":
   {key_prop: v}, "properties": {...}}`), which keeps Decisions point 3's
   actual goal (a normalized record, not raw undecoded bytes) while staying
   directly replayable. `key_bytes` stays `[source_key, target_key]`, not
   the original Kafka key (never threaded through the hot-path batching
   dataclasses) — safe, since the loader never parses a relationship
   message's key, only its value; a replay's partition assignment may
   differ from the original message's.
2. **`_dlq_json_safe()` encoder added** (a reviewer must-fix during Story 2).
   The stories doc's sketch used `json.dumps(..., default=str)`, which
   would have written `str(datetime)` — a space-separated form the
   loader's own datetime parser rejects ("must use a 'T' datetime
   separator"). This would have silently made every parked `date`/
   `datetime` row unreplayable forever, once a v2 envelope existed in a
   production DLQ topic. Fixed before the encoder ever shipped.
3. **`build_edge_execution`'s new `mode` parameter defaults to `"bulk"`**
   rather than being a required keyword-only parameter as stories.md's
   Decisions point 2 sketched. Required would have broken roughly 10
   existing call sites across `tests/`, `tests/integration/`, and
   `tests/stress/` that never pass it. Defaulting to `"bulk"` is the
   conservative choice: bulk always needs the explicit opt-in flag too, so
   every existing caller keeps byte-identical behavior regardless.
4. **Same-pair ordering across sequential transactions within one flush**
   was not in stories.md's original design at all — a reviewer-found gap in
   the *stories document itself*, not a deviation by the implementer, for
   both the lease coordinator (Story 4) and the classic
   `LaneExecutionCoordinator` (Story 5, the reviewer's own carry-over
   question). `BucketLeaseExecutionCoordinator` can split one cell's rows
   across several sequential `write_provenanced_batch` transactions when
   the cell holds more rows than `batch_size`/the merge budget;
   `LaneBatcher.drain_all()` does the identical thing per lane. A pair
   always routes to the same cell/lane, so without a per-flush `ParkedPairs`
   (implemented by `_LeaseFlush` and a new `_ParkedPairSet`), an earlier
   transaction's parked row and a later transaction's resolvable row of the
   *same* pair, in the *same* flush, could both exist — the exact
   last-write-wins violation Decisions point 4 says a single flush must
   never introduce. Fixed in both coordinators with dedicated regression
   tests (`batch_size=1`, two same-pair rows split across transactions).

## Known limitations

All documented in `docs/USAGE.md` §3/§10/§12 (row-level DLQ subsections):

- **Same-pair ordering is scoped to one flush, not the whole run.** A pair
  whose rows straddle two different polls/flushes is not covered — this is
  the residual gap explicitly handed off to P5-S4 (`plan/v5-v6-orchestration.md`'s
  P5-S3→P5-S4 handoff), which is better positioned to close it since it
  already re-reads and re-attempts DLQ'd rows in whatever order the DLQ
  topic delivers them.
- **`native_disjoint` execution mode is not supported by row-level DLQ on
  the classic branch, and cannot reach the lease branch at all.**
  `NativeDisjointEdgeWriter` already has its own `ON ERROR CONTINUE REPORT
  STATUS` per-transaction reporting — a structurally different mechanism —
  and row-level DLQ is not wired into it. Setting
  `route_missing_endpoints_in_bulk: true` on a classic, `native_disjoint`
  edge type has no effect and is not rejected at schema-load time (silent,
  not loud). Under bucket leases (streaming's default, or bulk
  `--edge-leases`) the outcome is different and pre-existing (P5-S1, not
  new here): `build_edge_execution`'s lease branch requires
  `execution.mode: python_apoc` and raises `ExecutionModeError` at startup
  for `native_disjoint` before Kafka subscription even begins — confirmed
  both by the pre-existing test
  `test_build_edge_execution_lease_mode_rejects_native_and_bad_merge`
  (`tests/test_bucket_lease_execution.py`) and by a direct reproduction
  during this story's review. **Reviewer note:** the first round of this
  story's review flagged this as a doc-accuracy must-fix in the opposite
  direction (claiming row-level DLQ silently activates for `native_disjoint`
  under leases); a runtime reproduction and the pre-existing test above
  showed that reading was incorrect — leases reject `native_disjoint`
  outright, they do not silently run it with DLQ active — so the fix
  applied here clarifies both branches' actual behavior rather than
  inverting the original claim.
- **The deprecated `--stream-clock-rotation` path never activates row-level
  DLQ**, since it reaches the classic (non-lease) writer branch with
  `mode="stream"`, which `build_edge_execution`'s activation table
  deliberately excludes (Decisions point 2) — that path is scheduled for
  removal and row-level DLQ is new behavior this slice must not silently
  graft onto it.
- **A row parked only because an earlier row of its pair was unresolved
  always gets `failure_class="missing_endpoint"`**, even when the row that
  blocked it was itself ambiguous. Harmless (the discriminator that matters
  operationally is "why nothing was written for this pair yet"), but worth
  knowing when filtering the DLQ topic by `failure_class`.
- **Lease hold time grows with the number of unresolved rows in a cell.**
  DLQ publishes for a cell's unresolved rows are synchronous, one at a
  time, after the Neo4j commit and before the lease releases. Never a lock
  wait (no Neo4j write happens after commit) — only Kafka acknowledgement
  latency, and only for the rare batches that actually contain an
  edge-before-node race.
- **At-least-once DLQ delivery, from two sources — not just a crash.**
  Matching every other counter/DLQ path in this system (P5-Q5): (1) a
  crash between a DLQ publish's ack and the source offset commit, and (2)
  a DLQ publish failing partway through a multi-row-unresolved batch,
  where the already-published envelopes republish again when the whole
  batch replays (Decisions point 6's fail-closed contract). Both are safe
  for Neo4j (`MERGE` is idempotent) but **not ordering-safe**: a duplicate,
  older copy of a row retried after a newer row of the same pair has
  committed in a later flush overwrites it — the same last-write-wins risk
  the cross-flush same-pair-ordering gap above already describes, now also
  reachable through a duplicate. **Explicit P5-S4 handoff item:** the
  retry consumer must dedupe, or compare offsets/timestamps before
  applying, a retried row rather than replaying in bare receipt order.
- **A `TransientError` inside the fallback (classification + matched-subset
  write) is not retried** — a deliberate Story 3 scope decision (decisions
  points 5/6 already require any Attempt-2 failure to resolve zero offsets
  and replay the whole batch from Kafka, which a genuinely transient Neo4j
  error would also eventually recover through, just on the outer Kafka
  redelivery cadence rather than `RetryConfig`'s tight backoff). Only
  Attempt 1's identical fast-path statement gets the existing
  backoff-and-retry loop. Flagged by the reviewer as a P5-S4-adjacent
  follow-up if the coarser Kafka-level retry cadence proves too slow for
  the fallback path specifically in production.
- **The 3 reviewer-recorded nice-to-haves from Stories 1-2, left unfixed
  per the owner's LEAN directive:**
  1. `schema_version in (1, 2)` is hardcoded in `_validated_envelope` rather
     than derived from `SCHEMA_VERSION` as a range — a future v3 will need
     to touch this line again.
  2. `key_bytes` in `route_unresolved_endpoint` is not built with
     `sort_keys` — irrelevant while keys are scalars, would matter if a
     composite/keyed label's key ever became a dict.
  3. A YAML `1`/`1.0`/`0` for `route_missing_endpoints_in_bulk` still
     coerces to a bool (only a `str` is explicitly rejected) — parity with
     every other boolean flag in this codebase, not tightened further.
- **Test coverage gaps recorded (not closed) by the reviewer during Story
  3, folded into Story 6/7's scope where practical:**
  - `*_target`/`*_both` reason codes are exercised in the classic-bulk and
    lease-coordinator tests but not exhaustively in every scenario; only
    `*_source` variants appear in some of the earlier Story-3-era tests.
  - A DLQ publish failing partway through a multi-row batch (not just the
    single-row case) is covered once (Story 6's produce-failure test), not
    parametrized over batch position.
  - "No duplicate relationship after replay" (stories.md's AC 3) can only
    be meaningfully proven against a real Neo4j instance — covered in
    Story 7's scratch proof (replaying twice, confirming the count stays
    at 1), not in any fake-driver unit test.

## Test evidence

### Full unit suite (required by the workflow, run once)

```
.venv/bin/python -m pytest tests examples/imdb/tests \
  -m "not integration and not smoke" --ignore=tests/test_dev_environment.py -q

1406 passed, 29 deselected
```

### This slice's own test files (run once)

```
.venv/bin/python -m pytest \
  tests/test_schema_validation.py tests/test_dead_letter.py tests/test_dead_letter_replay.py \
  tests/test_edge_execution_writer.py tests/test_bucket_lease_execution.py tests/test_edge_loader.py \
  tests/test_edge_before_node_dlq_probes.py -q

513 passed
```

### Lint

```
ruff check src tests examples   # (/tmp/lintvenv, not the repo venv)
All checks passed!
```

### P6-S4 scaffold integration test (real ephemeral testcontainers Neo4j + Kafka)

```
.venv/bin/python -m pytest tests/integration/test_edge_before_node_dlq.py -v

test_edge_before_node_is_dlqd_row_level_and_retried_once_the_node_arrives SKIPPED
  (Phase 5 Slice 4 retry consumer not merged yet -- the DLQ'd-row half of
  this test, which this slice's own row-level DLQ path drives, passed;
  only the Slice-4 retry-and-verify half is legitimately skipped)
1 skipped in 15.55s
```

This is a genuine pass of every assertion up to the Slice-4 skip point: the
writer is constructed with a real `DeadLetterRecordSink` and driven through
`write_provenanced_batch` (not the old sink-less `write_batch`), the missing
row is confirmed absent from Neo4j, the DLQ envelope is read back from a
real (ephemeral) Kafka topic and decoded (`failure_class=missing_endpoint`,
`reason_code=missing_source`, `retry_after_ms >= first_failure_at_ms`,
`value_bytes` round-trips the exact wire-input shape including a `date`
property through `_dlq_json_safe`).

### Scratch Docker end-to-end validation

Isolated scratch containers on their own network, never touching the shared
`graph-loader-kafka`/`graph-loader-neo4j`/`graph-loader-net` stack or any
`:latest`/`:generic` image tag:

- `scratch-p5s3-neo4j` (`neo4j:5-enterprise`, `--cpuset-cpus 28-31`,
  bind-mounted data, `dbms.logs.query.enabled=VERBOSE` and
  `dbms.logs.query.threshold=0` so every query's `waiting:` field is
  logged).
- `scratch-p5s3-kafka` (`confluentinc/cp-kafka:7.7.0`, KRaft mode,
  `--cpuset-cpus 28-31`, bind-mounted data, `auto.create.topics.enable=false`).
- Ran the loader code directly via a Python script (`python -m src.loader...`
  equivalent, calling `ApocLockedEdgeWriter`/`DeadLetterRecordSink` exactly
  as `edge_loader.py:main()` would construct them) against the scratch
  broker/database — the same convention P5-S2's own scratch validation
  used, avoiding any need to rebuild the shared `:latest` image tags.

Findings:

1. **Mixed-batch commit, real Neo4j.** A batch of two rows — one
   referencing a Person that does not exist yet, one referencing an
   already-existing Person — both targeting an existing Company. The
   unresolved row's `write_provenanced_batch` call correctly fell through
   Attempt 1 (aggregate check failed) into the classification + matched-
   subset write; the resolvable row committed (`resolved_count=1`) and the
   missing row did not (`missing_count=0`), in one transaction.
2. **Zero lock waits.** `docker exec scratch-p5s3-neo4j grep -o "waiting: [0-9]*" /logs/query.log | sort -u`
   returned exactly `waiting: 0` — the only distinct value across every
   query in the run (the mixed-batch write, the classification query, the
   verification reads, the replay writes, and a separate all-resolved
   fast-path write).
3. **PROFILE evidence: the classification query takes no locks.**
   `PROFILE <classification query>` was run against the real database; the
   returned plan (`ResultSummary.profile`, serialized) contains no `Lock`
   operator anywhere in its plan tree (`assert "Lock" not in plan_text`
   passed).
4. **Fast path confirmed unchanged for an all-resolved batch, against real
   Neo4j.** A second writer was constructed with `missing_endpoint_sink=
   object()` (a bare object with no `route_unresolved_endpoint` method) and
   given a single fully-resolvable row: `write_provenanced_batch` completed
   without raising `AttributeError`, proving Attempt 1's fast path
   succeeded and the classification/fallback code path — which would have
   tried to call a method that does not exist on `object()` — was never
   reached. The row committed (`fast_path_count=1`).
5. **DLQ envelope byte verification.** The one envelope published to
   `scratch-works-at-events.dlq` was read back with a real `confluent_kafka`
   consumer and `decode_envelope`: `failure_class=missing_endpoint`,
   `reason_code=missing_source`, `retry_after_ms > first_failure_at_ms`,
   and `value_bytes` decoded to exactly
   `{"source": {"personId": "scratch-missing-person-1"}, "target":
   {"companyId": "scratch-co-1"}, "properties": {"since": "2024-01-01",
   "role": "eng"}}` — the ISO date string, confirming `_dlq_json_safe`'s
   fix round-trips correctly against a real encode/decode/Kafka cycle, not
   just the unit-level fixture in Story 2's own tests.
6. **Manual replay resolves cleanly, with no duplicate.** After merging the
   previously-missing Person node, the decoded envelope's value was passed
   through `normalize_edge_record` and written via `write_batch` (the
   Story 3-era manual-replay path Story 7's own docstring describes, since
   P5-S4's retry consumer does not exist yet): the relationship count went
   from 0 to 1. Replaying the *same* envelope a second time (simulating an
   at-least-once duplicate delivery) left the count at 1 — confirmed via
   `cypher-shell`-equivalent count queries, proving idempotent `MERGE`
   safety end to end against a real database, not a mock.

All scratch containers, the scratch Docker network, and the bind-mounted
scratch data directory (`/mnt/graph-loader/scratch/p5s3/`) were removed
after validation. No shared container, network, or `:latest`/`:generic`
image tag was touched or rebuilt.

## Reviewer findings and resolutions

A single dedicated independent reviewer subagent (Opus, read-only, no
repository write access) was kept for the entire slice and reviewed every
story plus every fix round. Summary by story:

- **Story 1** (schema opt-in): approved with 3 nice-to-haves (a lax
  boolean check allowing `1`/`1.0`/`0`; `NodeConfig.dead_letter` silently
  accepting the no-op field; test coverage stopping at the model level
  rather than the full `load_schema` path) — all recorded above, none
  fixed, per the owner's LEAN directive.
- **Story 2** (envelope): one must-fix (the `date`/`datetime` corruption
  bug) and one "decide now" (the `value_bytes` shape, resolved in favor of
  replayability) — both fixed before commit; see Deviations 1-2 above.
- **Story 3** (classification/two-attempt write): approved with no
  must-fix; several test-gap nice-to-haves folded into Stories 6/7 above.
- **Story 4** (lease coordinator wiring): one must-fix — the same-pair
  ordering guard did not span a lease flush's multiple sequential
  transactions per cell; fixed with the `ParkedPairs` protocol (Deviation
  4).
- **Story 5** (classic bulk + `edge_loader.py` wiring): approved with no
  must-fix; the reviewer's own carry-over question ("does the classic path
  have the same gap Story 4 just fixed?") was answered yes and fixed in
  the same story, with a dedicated regression test.
- **Story 6** (offset/ordering tests): one must-fix (the lease-loss test
  used a mocked coordinator that already carried the exception, proving
  nothing new) and one should-fix (the spec's mixed-batch scenario named
  the lease coordinator specifically, but the test used the classic one)
  — both fixed; a final required one-line tweak
  (`assert tx.run.call_count == len(tx_run_side_effect)`) closed a
  remaining vacuity hole in the lease-loss test.
- **Story 7** (scratch proof + docs): reviewed via this completion
  document and the diffs referenced in it; the reviewer's four specific
  checks (the P6-S4 scaffold using the real interface, the real-Neo4j
  no-duplicate check, PROFILE evidence, and the five specific USAGE.md
  topics) are all addressed above. Two doc-accuracy must-fixes came back:
  (1) the at-least-once bullet understated its two duplicate sources and
  the ordering risk — fixed as suggested, with an explicit P5-S4 handoff
  item added; (2) a claim that `native_disjoint` silently gets row-level
  DLQ under bucket leases — a runtime reproduction and the pre-existing
  `test_build_edge_execution_lease_mode_rejects_native_and_bad_merge` test
  showed the opposite is true (leases reject `native_disjoint` outright
  with `ExecutionModeError`, before Kafka subscription), so this was fixed
  by clarifying both branches' actual behavior rather than inverting the
  original claim to something false — see the `native_disjoint` bullet
  above for the full explanation and evidence.

No must-fix item was ever left unresolved.

## Sign-off

The dedicated independent reviewer subagent (Opus, read-only) reviewed all
7 stories and every fix round across the slice.
