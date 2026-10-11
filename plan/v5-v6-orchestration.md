# Phases 5 and 6: orchestration decisions and execution order

The orchestrator owns this record. It resolves the open questions raised in
`plan/v5/slices.md` and `plan/v6/slices.md`, and it fixes the execution order.
Where a slice document and this file disagree, this file wins. Every Slice
Cartographer and First Mate must read it.

## Owner decisions (binding)

1. **Phase 5 improved design.** Streaming relationship writes use the bucket-lease
   coordinator, which gives zero Neo4j lock waits. The APOC-locked query in
   `plan/phase_5_streaming_resilience/technical.md` is not used.
   * Relationship dead-lettering is row-level: only the unresolved rows go to
     the DLQ.
   * Malformed node and relationship records go to a DLQ, which replaces the
     local rejection file.
2. **After the maximum number of retries, records go to a parking-lot topic.**
   The default is `<topic>.dlq.parked`. Records are never dropped, and they can
   be replayed.
3. **Models.** Sonnet 5.5 does the Navigator, Cartographer and First Mate
   implementation work. Opus 5.5 is each slice's dedicated reviewer.
4. **Fully autonomous.** Escalate to the owner only when blocked, when manual
   intervention is needed, or on a genuine architecture or product doubt.
   Decide with product value and scalability in mind.
5. **Parallelism.** Run slices in parallel (separate worktrees) only when their
   dependencies allow it and they are unlikely to conflict when merged.
   Otherwise run them in sequence, each branched from the latest integration
   branch `feat/streaming-observability`.

## Resolved open questions

### Phase 5
* **P5-Q1, clock and rotation.** Leases become the streaming default for
  relationship write concurrency, including self-referencing types. The Phase 4
  clock and rotation stay available behind a deprecated opt-in flag for one
  release; they are not deleted in P5-S1. Streaming needs a supervised and
  restartable lease server:
  * loaders reconnect with bounded backoff;
  * on a lease-server disconnect, a writer aborts (rolls back) any transaction
    that is still open instead of committing it, so a restarted server cannot
    grant resources that are still held;
  * held resources are never handed out while a holder may still commit.

  Rename the lease-related config to avoid the collision with
  `CoordinationConfig.lease_timeout_ms`. For example, name the TCP coordinator's
  settings `bucket_lease_*`, and document both.
* **P5-Q1 amendment: fencing across lease-server restarts (binding for
  P5-S1).** A "PING before commit" check alone is not enough. A restart
  between the PING and the commit could re-grant a resource that is still
  being committed, and the second writer would wait on a lock, which breaks
  the owner's rule that nothing waits on a lock. Use time-based fencing
  instead:
  1. Every relationship write transaction, streaming or bulk, in lease and
     planned modes, is opened with a Neo4j transaction timeout `T`
     (`bucket_lease_tx_timeout`, default 30s, validated to be at least 10× the
     expected chunk commit time). Neo4j terminates any transaction older than
     `T`.
  2. After a (re)start, a lease server with a new generation waits a quiet
     period `Q ≥ T + margin` (default margin 5s) before its first grant, so
     every transaction from the old generation has committed or been
     terminated by then.
  3. On a lease-connection loss or generation change, writers roll back
     immediately. The pre-commit PING stays as an optimisation, not as the
     guarantee.
  4. Tests must cover a server kill between PING and commit, with zero lock
     waits observed in Neo4j's query log.
* **P5-Q1 amendment 2: fenced BEGIN (owner decision, 2026-10-01).** Neo4j
  counts a transaction's timeout from the server-side BEGIN (verified on the
  scratch stack), so a timeout chosen before `begin_transaction()` let a slow
  connection acquisition or BEGIN, plus the transaction monitor's late
  termination, carry a transaction past grant + `T`. Every write with a lease
  deadline (stream edge, node and retry-consumer writes, and bulk
  `--edge-leases`) now begins through `src/loader/fenced_tx.py`: timeout
  `D - t0 - 3 s (monitor slack) - 1 s (BEGIN allowance)`; a BEGIN slower than
  1 s is rolled back before any statement and begun again; with no budget left
  the cell is re-granted. **The owner keeps this fence in bulk
  `--edge-leases` too.** It needs `tx_timeout_ms >= 6000`, checked only for
  stream mode and `--edge-leases`; planned and classic bulk keep `T` as a flat
  timeout and load any value the schema allows.
* **P5-Q2, what the retry consumer retries.** It only retries missing-endpoint
  relationship rows (P5-S3). A malformed record (P5-S2) is terminal: it goes to
  the DLQ with its reason, and a separate replay tool or command re-injects it
  after a fix. No automatic retry.
* **P5-Q3, bulk DLQ opt-in.** Only the classic and lease bulk modes get the
  opt-in. Planned mode (`--edge-plan`) stays fail-closed in Phase 5; that is
  documented, not changed.
* **P5-Q4, retention and backlog.** DLQ and parking-lot topics are created with
  `retention.ms=-1` and `retention.bytes=-1`; they are never auto-deleted.
  Backlog is observable as a Phase 6 metric (DLQ lag or depth per topic), and
  P6-S3 adds an example alert rule for it. Retention policy is an ops
  decision, documented in `docs/USAGE.md`.
* **P5-Q4 follow-up, DLQ backlog gauge.** P5-S4 must add a true DLQ backlog
  gauge to the P6-S2 catalogue, for example `graph_loader_dlq_backlog` labelled
  `{kind,name}`: DLQ high watermark minus the retry consumer's committed offset,
  plus the parking-lot size. P6-S3's backlog alert, which currently uses the
  routed and parked growth rate as a proxy, then switches to it.
* **P5-S3 → P5-S4 handoff, ordering across flushes (binding for P5-S4).**
  P5-S3 parks later rows of a pair only within one flush. Across flushes, a
  parked older record can be retried after a newer record of the same pair has
  committed, and would then overwrite newer properties with older ones. P5-S4
  must close this gap:
  * **Default: park by pair.** While any record of a (type, source key, target
    key) pair is waiting in the DLQ, later records of that pair are also
    routed to the DLQ behind it, in order. That needs a bounded, durable
    parked-pair index, for example a compacted Kafka topic, or DLQ-derived
    state rebuilt on start. Retries replay a pair's records in their original
    order.
  * **Optional per edge type:** a schema-declared ordering property (for
    example `order_by: updated_at`). Writes then use a conditional SET, so a
    retried record only applies if it is newer.
  * Document the chosen semantics, and prove both with tests.
  * *Orchestrator decision under this handoff (replay resurrection,
    2026-10-01, option B):* a crash, a stop-drain abandon or a failed revoke
    settlement redelivers a partly committed live flush. The redelivered flush
    re-adds and republishes DLQ'd rows that the retry consumer may already
    have written or parked and tombstoned, and the older row was then applied
    after a newer one. The retry consumer now keeps, per (pair key including
    labels, original topic, original partition), the highest source offset it
    wrote or parked (LRU-bounded). It resolves each pair in order, so a copy
    still indexed at or below that offset can only be a resurrection: it is
    tombstoned and committed past (`stale_resurrected_copy_skipped`), never
    written. "At or below" includes the resolved record itself: a sequenced
    index (several topics) gives a re-added record a new seq after every
    outstanding row of its pair. It also covers a written or parked copy whose
    tombstone failed: it is tombstoned again, no longer rewritten. A revoke drops the entries of its DLQ partitions, like the
    superseded-copy memory. Offsets are never committed past an unresolved
    row (the skipped copy is committed only after its tombstone is
    acknowledged). Replays (`replay-dlq`, `--parked` too) come back at new
    source offsets and are unaffected. Accepted gaps, documented in USAGE §10
    and §12: a retry-consumer restart between the two resolutions (the
    memory is per process); order across topics of a multi-topic type
    (offsets of different topics are not comparable); LRU eviction. The
    operational mitigation is `order_by`. The durable fix, a resolved marker
    per record identity that the live writer reads (option A), is an
    owner-level follow-up.
* **P5-S4 retry-consumer write failures: owner decisions (2026-10-01,
  binding).**
  * **D1.** Retry consumer never parks a row because of a system-wide
    condition. A row's failure counts toward its attempt budget only with
    evidence that the failure is the row's own: an allowlisted row error, or
    a same-edge-type WRITTEN outcome after the row's first uncounted failure.
    With no such evidence rows back off and wait indefinitely; a
    sustained-stall ERROR and a metric/alert fire instead.
    *Owner-confirmed decision under D1 (round 5 review; confirmed 2026-10-02):* a row failing alone
    with `MemoryPoolOutOfMemoryError` (server-wide memory pressure) is
    counted under the same evidence rule as a capacity error, never without
    evidence; this supersedes round 4's F1 "never count", so a row that
    exhausts the pool by itself terminates while same-type writes succeed.
    *Owner-confirmed decision under D1 (v7 final follow-ups, S4; confirmed 2026-10-02):* for a
    relationship type of several label pairs (v7-S7), the same-edge-type
    WRITTEN evidence is keyed by (type, label pair): a written Person->Post
    row is no evidence that a failing Person->Comment row is at fault.
    Single-pair types are unchanged. A pair whose rows fail with none of
    them written is reported as a stall of its type (the gauge stays per
    type, the ERROR names the pair) even while the type's other pairs write.
  * **D2.** Retry-consumer containers are supervised like edge loaders in
    stream mode: a crash or fatal exit stops the fleet with rc 1; plus an
    alert when the DLQ backlog metric goes absent.
* **Found during P6-S2 (assigned):**
  1. **Owned by P6-S5.** When the CLI receives SIGTERM during stream-mode
     relationship orchestration, it stops the edge and clock containers but not
     the node-loader containers that are already running.
  2. **Owned by P6-S5.** When a stream-mode schema mixes shared and
     self-referencing edge types, the isolation check fails after the node
     containers have started, and that failure path doesn't stop them.
     Every failure path must stop exactly the containers it started.
  3. **Owned by P5-S1.** P5-S1 removes the mixed shared plus self-reference
     restriction in stream mode (a single lease fleet). After it, the unmodified
     `examples/social/graph_schema.yaml`, which includes the self-referencing
     `FRIEND_OF`, must run in `--mode stream`.
* **Found during P6-S3 (assigned):**
  1. **Orchestrator, after P5-S1 merges.** Apply the deferred one-line link to
     `docs/MONITORING.md` in `docs/USAGE.md`; the text is in
     `plan/v6/slice-3/deferred_usage_link.md`.
  2. **Owned by P6-S5.** The CLI's host-side metrics endpoint binds
     `127.0.0.1`, so the monitoring stack never scrapes consumer lag or planner
     metrics. Make them reachable in a safe way: an opt-in bind address, or a
     host-gateway scrape target, plus the matching Prometheus job. Update
     `docs/MONITORING.md` §7.
  3. **Owned by P6-S5.** `tests/test_clock_runner.py` is flaky: it failed once
     and passed on re-run. Fix it with correct synchronisation. Retries, sleeps
     and bigger timeouts are not acceptable fixes.
  4. **Owned by P6-S5.** After Phase 5 lands, re-run P6-S3's full 8-step
     scratch monitoring proof against a real fleet: edge loaders, lease server,
     retry consumer, two concurrent runs with the same node label, and a
     browser check of the dashboard.
* **Found during P6-S4 (assigned):**
  1. **Owned by P5-S1, must-fix; sent to it.** The bulk-lease writer keeps
     `sort_rows=True`, and `ORDER BY elementId()` is not a stable sort, so
     last-write-wins breaks for repeated pairs. Lease transactions are
     node-disjoint, so use `sort_rows=False` and add a last-write-wins test.
  2. **Owned by P6-S5.** `tests/conftest.py` pins `neo4j:5.21-enterprise`,
     which cannot parse `CALL (vars) { ... }` scoped subqueries, so the
     classic and lease writers fail on it. Bump the pin to 5.26, and document
     the minimum supported Neo4j version (the first version with scoped
     subqueries) in `docs/USAGE.md` §2.
  3. **Owned by P5-S3; sent to it.** Switch the edge-before-node DLQ scaffold
     to P5-S3's real opt-in interface.
* **P5-Q5, double counting.** Counters are at-least-once. A crash can
  double-count a retry, while the graph stays exact thanks to idempotent MERGE.
  Document this next to the metric catalogue.
* **P5-S5 folds into Phase 6.** It is not a standalone slice. The metric names
  are fixed in P6-S2's catalogue. Every Phase 5 slice that lands after P6-S2
  emits its own counters through that catalogue as it is built. P5-S2 lands
  before P6-S2, so P6-S2 wires its DLQ counter.

### Phase 6
* **P6-Q1, Pushgateway.** Not built. Live metrics come from pull scraping with
  Docker service discovery via container labels. For short bulk runs, every
  process writes its final metric values into the run's timing directory when
  it exits (the report stays the durable record for a run), so nothing is lost
  if a container exits between scrapes.
* **P6-Q2, Neo4j Enterprise in CI.** The integration job is nightly or manual
  (`workflow_dispatch`) and non-blocking. Every PR runs lint, unit tests and
  the image builds.
* **P6-Q3, star-topology scale.** Tiered: 100k edges into one node in the CI
  integration job, 1M in the nightly or manual stress job.
* **P6-Q4, alerts.** P6-S3 ships example Prometheus alert rules with
  conservative defaults, clearly marked as starting points. It does not claim
  SLOs.
* **P6-Q5, metric names.** P6-S2's catalogue is the single source of truth,
  and Phase 5 slices use it.
* **P6-Q6, CI images.** Build all three images: node, edge and clock.
* **Lint baseline.** CI lint must pass on the current code without a mass
  reformat: pick a rule set that matches the codebase and fix only genuine
  errors. A repository-wide reformat would conflict with every other branch.

## Execution order

| Wave | Slices | Parallel | Reason |
| --- | --- | --- | --- |
| 1 | P6-S1 CI pipeline, P5-S2 malformed-record DLQ | yes | disjoint files: `.github/` and lint config vs `src/` |
| 2 | P6-S2 core metrics instrumentation | no | touches every process entry point |
| 3 | P5-S1 zero-wait streaming via bucket leases | no | `edge_loader`, `edge_execution`, `cli`, lease server |
| 4 | P5-S3 row-level DLQ for missing-endpoint rows | no | same hot files; depends on wave 3 |
| 5 | P5-S4 retry consumer and parking lot | no | depends on wave 4 |
| 6 | P6-S3 metrics delivery, P6-S4 regression and stress tests | yes | compose and monitoring files vs `tests/` |
| 7 | P6-S5 graceful-shutdown audit | no | touches every process entry point |

**Owner directive (added mid-programme):** maximise parallelism wherever
dependencies allow and merge conflicts are unlikely. The revised order is:

* P6-S3 (metrics delivery) and P6-S4 (regression and stress tests) run
  **alongside wave 2 and the Phase 5 waves**:
  * P6-S3 is confined to new files: `monitoring/`,
    `docker-compose.monitoring.yml` and `docs/MONITORING.md`. At the end it adds
    a one-line link to `docs/USAGE.md`.
  * P6-S4 is confined to new test files, plus additive changes to
    `.github/workflows/ci-integration.yml` and the pytest markers.
  * Both take metric names from the P6-S2 catalogue, and their final validation
    runs after P6-S2 has merged.
* Planning documents (cartographers) for later slices run as early as their
  design inputs exist. P5-S1 is running during wave 2; P5-S3 follows as soon as
  the P5-S1 stories exist, and P5-S4 after P5-S3.
* The source-heavy slices keep their sequence, because they share
  `edge_loader.py`, `edge_execution.py`, `cli.py` and the lease server:
  P6-S2 → P5-S1 → P5-S3 → P5-S4 → P6-S5.

Otherwise, each slice's cartographer runs just before its wave, against the
code as it stands then. Each slice ends with `plan/vN/completion_slice-K.md` and a
merge into `feat/streaming-observability`, followed by the full unit suite.
