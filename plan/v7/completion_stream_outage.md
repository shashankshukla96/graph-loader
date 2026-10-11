# Completion: live stream loaders ride out a Neo4j outage

Branch `feat/stream-outage-resilience`, branched from `feat/streaming-observability` 3a94537. Not pushed, not merged.
Binding: `plan/v5-v6-orchestration.md` (zero Neo4j lock waits, P5-Q1 time-based fencing, offsets never committed
past unwritten rows, per-pair order, D1/D2) and the owner constraint that the BULK path is not affected.

Before: a live stream edge loader whose Neo4j write failed exited 1 by design, and stream supervision stopped the
whole fleet with rc 1 (bench/p0/retest4 `rf-out3`: `Failed to read from defunct connection ...`, then
`exited before drain acknowledgement ... exit_code=1`). After: it holds the batch and retries it until Neo4j is back.

## Design

* `src/loader/write_errors.py` (new): the retry consumer's error taxonomy (connectivity, server, capacity, row,
  unknown), its code tables and its fatal rule constants, moved verbatim. `retry_consumer.py` keeps every old private
  name as an alias (`_classify_write_error`, `_is_fatal_error`, `_ROW_ERROR_CODES`, `_FATAL_OUTAGE_SECONDS`, ...), so
  its behaviour and its tests are unchanged (its classification table test passes as is).
* `src/loader/write_outage.py` (new): `StreamWriteOutage`, the outage state of one stream loader process. On a
  failed write it classifies the error. Anything but `connectivity` returns FAIL (the caller does exactly what it did
  before). `connectivity` returns RETRY with a capped exponential delay (0.5 s doubling to 30 s), or FATAL once
  fatal-class errors (authentication, a missing database) have persisted for 600 s with gaps under 60 s. This is the
  retry consumer's F2/D2 rule with the same constants: only a non-fatal Neo4j-coded answer or a success resets it, and
  a driver error without a code neither resets nor extends it. `wait()` sleeps in 0.2 s slices and calls the
  loader's Kafka poll every slice. It returns early on a stop, a rebalance failure, or when a revoke took the batch
  away. Logs: a WARNING at the start and one per retry, an ERROR with `reason=neo4j_outage` at 30 s (then every
  5 minutes), an INFO `stage=write-outage-recovered`, an ERROR with `reason=neo4j_fatal`. Gauge:
  `graph_loader_write_outage_seconds{kind,name}`. A stop raises `WriteOutageDrainAbandoned`, a subclass of
  `LeaseDrainAbandoned`, which both loaders already turn into exit 0 with nothing committed.
* `EdgeLoader`: new constructor argument `write_outage=None`. `_flush_batch` dispatches to the unchanged body (renamed
  `_flush_batch_once`) unless a ride-out is installed. The only edits inside that body: the two failure log calls go
  through `_report_write_failure`, which records the exception. It logs an ERROR, as before, when there is no
  ride-out, and a WARNING when the failure will be ridden out. `_flush_batch_through_outage` loops: one attempt;
  on a connectivity failure, pause the assigned partitions (only if nothing else, such as a finite drain, already
  paused them), wait while polling, then retry the same `self._batch` (same records, same order). It resumes only
  the pause it added. The constructor refuses a ride-out combined with a static assignment (bulk replicas) or with
  slot gating (the deprecated clock path).
* `NodeLoader`: the same pattern around the existing bounded retry (`_write_batch_with_retry` dispatches to the
  unchanged, renamed `_write_batch_with_bounded_retry`). Each ride-out attempt is one full bounded retry.
* Revoke during an outage (both loaders): the loader never writes inside the Kafka callback. The unwritten batch of
  the released partitions is dropped uncommitted for the next owner. Ledgers and prefetched messages of those
  partitions are dropped too, and the outage state is reset. This happens whether the outage was already active, or
  the settlement write itself hit the outage. It is the redelivery a crash causes. Without a ride-out (bulk), the
  revoke path is unchanged.
* Injection (review S1): only lease-fenced stream writers get it. `edge_loader.main` constructs it for
  `--mode stream`, no `--slot-gating` and `lease_mode_settings(os.environ) is not None`. `node_loader.main` does
  for `--mode stream` and `isinstance(node_writer, LeasedNodeWriter)`. An unleased writer has no transaction
  timeout T and no fencing, so a retry after an ambiguous failure could wait on an orphaned transaction's locks. It
  keeps failing fast: `--stream-clock-rotation` edge loaders, its isolated self-reference phase and its node
  loaders, the node loaders of a nodes-only stream run, and any stream loader without a lease address.
  `edge_loader.py` imports the module at module level, so a bulk edge process imports it but never constructs it.
  `node_loader.py` imports it lazily, and no other module imports it.

### Why a retry is safe (fencing, offsets, order, zero lock waits)

* Rollback and no lease held across an outage: the failed attempt's transactions are closed without commit. The
  lease coordinator then gives up every lease of that flush before `execute_pending` returns. It releases a lease
  whose outcome is confirmed. When the outcome is ambiguous (ServiceUnavailable, SessionExpired, a lost commit
  reply), it closes the lease connection, and the lease server quarantines those resources for `T + margin` (B4-2,
  unchanged). The ride-out's wait therefore always runs with zero leases held. The unit tests snapshot the lease
  ledger at every wait slice. A transaction that might still be committing on the server can't overlap another
  writer, and the next attempt waits for its leases (a lease wait, never a Neo4j lock wait).
* Offsets: the batch's offsets are resolved only after a successful attempt, as before. Nothing is committed while
  Neo4j is down (asserted at every wait slice in the tests).
* Order: the retry rewrites the identical batch. A pair always routes to the same cell and is drained FIFO, so the
  final attempt writes each pair's rows in offset order. Cells that committed before the outage are rewritten; MERGE
  plus `SET +=` of the same rows in the same order leaves the same state. Rows of a DLQ-enabled type that the failed
  attempt routed to the DLQ are found in the parked-pair index on the retry, so they stay blocked. They are routed
  again by the writer, but `HeldBatchDlqMemory` (final design, see the M1 sections below) skips both the index
  re-add and the publish for every row whose copy an earlier attempt of the held batch had acknowledged. The first
  copy is then either outstanding or already resolved, so nothing is orphaned and nothing is resurrected.
* Kafka: the partitions are paused and polled every 0.2 s, far inside `max.poll.interval.ms` (300 s). A record
  already prefetched is deferred, not dropped. One attempt blocks the poll thread, as before, for at most:
  * the lease quarantine wait (`T + margin`, 35 s by default);
  * a lease-server reconnect while that server is down (`reconnect_timeout_ms`, 60 s by default);
  * a driver call: immediate against a stopped container, up to the driver's 30 s connect timeout against a
    blackholed host.

  A stop is honoured within the 5 s drain budget during a lease wait, but not inside a blocked connect or
  reconnect. Those can outlast Docker's 10 s stop grace for edge loaders, which ends in SIGKILL (137), with nothing
  committed. This is documented in USAGE section 5.4, "How long one attempt can block".

## Requirements

| Requirement | Resolution |
| --- | --- |
| 1. Stream only; rollback, release leases, commit nothing, capped backoff, same batch, same order | `StreamWriteOutage` plus the two loader loops above. Connectivity class = the retry consumer's: ServiceUnavailable, SessionExpired, pool/acquisition errors, TimeoutError/OSError, any TransientError (after the writer's own bounded retries), `Neo.ClientError.{Security,Database,Cluster}.*`, the read-only codes, LeaseCoordinatorError (the lease server down after the streaming client's bounded reconnect; the server is supervised and restarted, and its quiet period fences the new generation). |
| 1. `max.poll.interval.ms`, no rebalance storm | Paused partitions plus a 0.2 s poll cadence; tests assert the largest gap between polls and an unchanged assignment. |
| 2. row and unknown errors | FAIL: today's path (row-level DLQ routing where configured, otherwise exit 1). |
| 2. auth and database-not-found persisting | Ridden out for 600 s (D2 constants), then `reason=neo4j_fatal` and exit 1, so supervision stops the fleet. |
| 2. capacity and server | Kept failing as today (not clearly safe: one huge row could stall a live loader forever). |
| 3. ERROR at a sustained threshold | `reason=neo4j_outage` at 30 s, every 5 min after. |
| 3. bounded gauge | `graph_loader_write_outage_seconds{kind,name}` (kind node or edge, name = schema label or type); catalogue (26 families), `validate_monitoring.py` copy, test tables; the cardinality guard passes. Pre-touched to 0 only by stream processes. |
| 3. example alert with promtool tests | `alerts/write_outage.yml` `GraphLoaderWriteOutage` (`> 120` for 1m, critical); `alert_tests/write_outage_test.yml` (fires at 4m, a 90 s blip never fires, recovery clears it); added to `make monitoring-validate`. Dashboard panel "Stream write outage" in the Errors row. |
| 3. docs | USAGE section 5.4 "Riding out a Neo4j outage", the section 8 metrics bullet, the section 10 stop bullet, a section 11 troubleshooting row; MONITORING section 6 (file table, a "Stream write outage" paragraph) and section 8 (a troubleshooting row). |
| 4. SIGTERM during an outage | A stop is checked every wait slice: `stage=edge-drain-abandoned`/`node-drain-abandoned reason=neo4j_outage`, exit 0, nothing committed. A retry waiting for quarantined leases is still bounded by the 5 s stop-drain budget. A stop that arrives first, followed by a failing drain flush, also exits 0 without any backoff. |
| 5. BULK untouched | See below. |

## BULK is unaffected: evidence

* Gate: only the two stream entrypoints construct the ride-out, for leased writers only. Bulk classic, bulk
  `--edge-leases`, static bulk replicas, planned mode (planner, dispatcher, planned writers), bulk node loaders and
  the bulk monitors never construct it. Planned mode, the dispatcher, the bulk monitors and the CLI never import it.
  A bulk edge process imports it, through `edge_loader.py`'s module-level import, but never constructs it. An
  AST/static test enforces that exactly two modules import it, each with exactly one construction behind its
  stream-and-leased gate. With `write_outage=None`, `_flush_batch` is `_flush_batch_once` and `_write_batch_with_retry` is
  `_write_batch_with_bounded_retry`. Those are the previous bodies; the only change is that each records the last
  failure next to the identical `logger.error` call.
* (a) Unit tests, one per bulk path (`tests/test_stream_outage_resilience.py`, "BULK is untouched" section):
  `main(--mode bulk)` passes `write_outage=None` for classic, `--edge-leases` and static replicas, and node
  `--mode bulk` does the same. A connectivity error still fails fast for bulk classic and `--edge-leases` (rc 1, one
  attempt, no wait, no `stage=write-outage` line), for a static replica (rc 1), and for the bulk node loader (rc 1
  after exactly one attempt: ServiceUnavailable is not a retryable Neo4jError, unchanged). The planned writer fails
  closed with FAIL on a ServiceUnavailable (rc 1, one attempt). The deprecated slot-gated stream path also gets None,
  and EdgeLoader refuses a ride-out with a static assignment or slot gating.
* (b) `tests/test_single_topic_compat.py`, `test_single_pair_compat.py` and `test_single_pair_multi_topic_compat.py`
  pass, and `tests/golden/*` are byte-identical (no diff).
* (c) Full suite: see below.
* (d) Docker bulk control: see below.

## Review round (Opus, on a535c80): S1-S5 and the nit, fixed in a253b1f

| Item | Resolution |
| --- | --- |
| S1 ride-out on unleased writers | It is now gated on a leased writer (see Injection above). Tests: the stream edge main with a lease address installs it; without one (`--stream-clock-rotation`'s isolated self-reference phase, or a direct unleased stream run) it does not, and neither does the slot-gated path, even with an address. For node main: bulk gets None, stream with a plain `NodeWriter` gets None, stream with a `LeasedNodeWriter` gets it. The static gate test checks the new gate strings. USAGE section 5.4 and this note were corrected, including the import wording. |
| S2 a revoke reset the outage | `abandon()` now restarts only the backoff (`_failures = 0`). `_since` and `_error_logged_at` carry on until a write succeeds. The gauge is also computed at scrape time (`set_function`), so it keeps rising while an attempt is blocked: the first Docker SIGTERM run showed a frozen 1.6 s value during a long lease wait. Tests: ten revokes during a 10-minute outage leave the gauge at 600 s and keep the ERROR at 30 s and 330 s; the gauge rises during a blocked attempt; the edge and node revoke tests assert the outage stays active. |
| S3 OOM and StackOverflow ridden out | `StreamWriteOutage.on_failure` returns FAIL for `Neo.TransientError.General.OutOfMemoryError` and `StackOverFlowError`. The shared taxonomy and the retry consumer are untouched. Classification test cases were added. |
| S4 DLQ republished on retries | Superseded by M1 below (removed). Original fix: `DlqAckMemorySink`, installed by `edge_loader.main` via `remember_dlq_acks` around the real sink, inside `IndexedMissingEndpointSink`. It is keyed by (topic, partition, offset), thread-safe, and forgotten on commit (`recovered`) and on handover (`abandon`). Tests: row-level DLQ with a durable index, where transaction 1 commits and DLQs its row and transaction 2 dies; over every retry the envelope is published once (`[10]`; without the memory, `[10, 10]`), while the row stays blocked and the index re-add is idempotent. Also a main wiring test and a forget test. |
| S5 vacuous fencing tests | The `LeaseLedger` double is now exclusive (no two holders of a resource) and quarantines a closed connection's resources for a hold time, denying them meanwhile. Both mid-flush tests assert, at the first wait, `quarantined >= 1`, `released == commits` (only confirmed commits RELeased) and no lease held, and that a retry was denied while fenced. The reviewer's mutant (`_outcome_is_confirmed` forced True) now fails both tests (`assert 0 >= 1`). A new revoke-then-assign test with an admission index shows the barrier holds the partition paused with no write until the index catches up, then the re-read batch is written exactly. |
| Nit: per-attempt bound | Corrected here and in USAGE section 5.4, "How long one attempt can block": a lease quarantine wait up to T + margin (35 s), a lease reconnect up to `reconnect_timeout_ms` (60 s), a driver connect up to 30 s against a blackholed host. A stop is not honoured inside the last two, which can exceed Docker's 10 s stop grace (SIGKILL, 137); nothing is committed. |

## Re-review (on a253b1f): M1 and the S3 follow-up, fixed in the commit after 9ba6f0e

| Item | Resolution |
| --- | --- |
| M1 the S4 ack memory can orphan an index entry | (e9d60a7, superseded by the next section) Chose the fallback: **the memory is removed**. Retries publish a copy with every index re-add, as a crash and redelivery does, and duplicates are documented as safe. Why not the `contains`-gated skip: (1) the memory would have to sit outside `IndexedMissingEndpointSink`, which `build_edge_execution` builds once per label pair (with scoped indexes for multi-pair types), so it would need new hooks in the lease builder that bulk `--edge-leases` also uses; (2) its safety would rest on how fresh this process's index view is when `contains` is read, a new and subtle argument, whereas duplicate copies are the already-proven crash-replay path; (3) the volume the memory saved is small: a copy is published only after a cell commits, so retries add copies only in attempts that partly committed, never in a hard outage. Tests: the reviewer's probe as a sink-level test (r1's first copy resolved, then re-routed: it is re-added and republished, so no orphan); a loader-level row-level-DLQ test with the durable index and a resolve between attempts (every index add has its publish); a hard-outage test (exactly one publish); and a main test (the real sink reaches `build_edge_execution` unwrapped). |
| S3 follow-up: `rides_out()` still True for OOM/StackOverflow | `rides_out()` now returns False for `NOT_RIDDEN_TRANSIENT_CODE_PREFIXES`, and `on_failure` uses it, so the two agree. Test: both codes are not ridden out; inside a revoke callback the settlement write that hits them fails closed (rebalance failure, batch kept, no handover), and the failure line is an ERROR. |

## M1, final: the fallback was wrong too; skip both the re-add and the publish

The rk-ride3 Docker proof on e9d60a7 (the fallback: re-add and republish) was **not exact**. 48 WORKS_AT pairs
held an older value. Trace for pair (41817753, C00363), records social-works-at:3:5115 (old) and :6221 (new):

1. Attempt 1 dead-lettered both (DLQ p1 o243 and o246, `missing_source`) in a committed cell.
2. After recovery, the retry re-added both and published new copies (o579 and o582,
   `blocked_by_earlier_unresolved_row`).
3. The retry consumer wrote 5115 from o243 and removed its entry before step 2's re-add.
4. It wrote 6221 from o246, then found o579 indexed again (resurrected by the re-add) and wrote 5115 last: old over
   new. o582 was skipped because its entry was gone.

So the two simple designs fail in opposite directions:

* skip only the publish (a253b1f): orphans the entry, and the pair blocks forever;
* re-add and republish (e9d60a7): resurrects a resolved row after a newer one.

The reviewer's option, "skip both only while `contains`, otherwise delegate fully", resurrects in exactly this
case: the row is no longer contained because it was resolved.

**Final rule (`HeldBatchDlqMemory`, this commit):** for a row whose DLQ copy an earlier attempt of the same held
batch had acknowledged, skip both the index re-add and the publish, unconditionally.

* If the row is still indexed, its first copy is outstanding and will be retried.
* If it is not, the retry consumer has written or parked it, and doing nothing is correct.
* A row is remembered only after its publish was acknowledged, so a row whose cell did not commit, or whose
  publish failed, is routed in full next time.

Wiring: `edge_loader.main` (stream ride-out only) wraps the DLQ sink and the parked-pair index passed to
`build_edge_execution` with one memory. The index wrapper overrides `add` and `scoped` (multi-pair views) and
delegates everything else; the admission barrier keeps the plain index. `edge_execution.py`, bulk and the retry
consumer are unchanged. The memory is cleared on commit (`recovered`) and on handover (`abandon`).

Tests:
* the reviewer's probe as a test: no orphan, no resurrection;
* the rk-ride3 interleaving, parametrised: with the e9d60a7 wiring the older row is applied last (the bug
  reproduced); with the memory the newer row is last;
* a failed publish is routed in full again;
* scoped multi-pair views, forgotten per batch;
* loader level, with the real coordinator and writer and a resolve between attempts: one copy each, and r1 is not
  re-added;
* hard outage: one copy;
* main wiring: one memory for the sink and the index; the admission barrier gets the plain index.

**Found by this trace, predates the ride-out, not changed:** a crash, a stop-drain abandon or a handover can also
redeliver a flush in which some cells had dead-lettered rows that the retry consumer resolved before the
redelivery. The next owner then re-routes them, with the same resurrection risk. Without the ride-out this needs a
loader exit after a partly committed flush, plus the retry consumer winning the race; it is rare, but it is real.
A durable fix needs the live writer to recognise a resolved record identity across processes, for example with a
per-record "resolved" marker in the parked-pair index. That is a separate change for the owner.
(Follow-up on branch `fix/replay-resurrection`: the retry consumer now skips such a resurrected copy within one
retry-consumer process, option B; see the orchestrator decision under the P5-S3 -> P5-S4 handoff in
`plan/v5-v6-orchestration.md`. The durable marker, option A, remains the owner's.)

## Tests and counts (final: 531f3b8)

* 531f3b8: `tests/test_stream_outage_resilience.py` 75 tests. Full suite 2711 passed, 9 skipped, 2 failed
  (`test_dev_environment.py`, local credentials); with those rerun via `bench/local_env.sh` (5 passed),
  **2713 passed, 9 skipped**. `ruff`: clean. `make monitoring-validate`: SUCCESS.

### Earlier counts (a253b1f)

* `tests/test_stream_outage_resilience.py`: 68 tests. They cover the unit behaviour (backoff sequence,
  classification per class including S3, the gauge at scrape time, the rate-limited ERROR, the fatal rule with gap
  and reset, revokes not resetting the clock, the wait semantics). For the edge loader:
  * an outage mid-flush through the real lease coordinator and writer: rollback; quarantined, not released, leases
    and no lease held at any wait slice; no commit while down; recovery; an exact last-write-wins graph; the final
    attempt in offset order; one commit;
  * paused partitions with a poll every 0.2 s or less, and a prefetched record deferred;
  * Neo4j going down during a lease wait, and the lease server unavailable during a lease wait;
  * SIGTERM during the lease wait of a retry, during the backoff (real clock, under 1 s), and before a failing drain;
  * auth and database-not-found becoming fatal; row, unknown, capacity and server errors failing as before;
  * a revoke during the outage, and a revoke then assign with an admission index;
  * the S4 DLQ memory.

  The node loader has the same cases, followed by the bulk and gating assertions.
* Full suite `python -m pytest tests`: 2704 passed, 9 skipped, 2 failed. The 2 failures are
  `test_dev_environment.py`, which needs the local stack's credentials exported; it passes rerun with
  `bench/local_env.sh` (5 passed, read-only). That makes **2706 passed, 9 skipped**. The stress file passed in
  this run. On a535c80: 2697 passed, 9 skipped (the stress file passed when rerun alone after the host OOM).
  `examples/imdb/tests`: 77 passed (a535c80). Goldens: no diff.
* `ruff check src tests examples scripts`: clean. `make monitoring-validate`: SUCCESS (promtool config, every rule
  file, 4 test files; 23 panels, 17 alert expressions, all metric names known).

## Docker proofs (scratch stack p0rk-*, run 2026-10-01 17:17-17:54Z after p0rj-* was gone)

Stack: p0rk-net; p0rk-kafka (cp-kafka 7.7.0, cpuset 30, :29992); p0rk-neo4j (neo4j:5-enterprise plus APOC,
2G heap and 2G page cache, cpuset 26-29, :28387, query-log mount); `LOADER_CPUSET=31`. Images were
`graph-loader-{node,edge,clock}:resil`, built from the commit under test; for the final runs they were rebuilt from
a253b1f and their source hashes match the tree. The connection variables are explicit, and each run printed its
endpoints first. Data: social, 40000 persons, 617017 relationships, the `dlq-edges-fast` schema (every type has a
DLQ; the retry consumer runs). Scripts and per-run directories are in `/mnt/graph-loader/bench/p0/rk/`.

| Run | Code | Result |
| --- | --- | --- |
| (1) ride `rk-ride` | a535c80 | Neo4j was stopped at 84060 relationships, with the live edge loaders writing, and was down 46 s. All 14 containers and the CLI stayed up. The outage gauge read 37-46 s on 4 edge types, and every edge type logged the `reason=neo4j_outage` ERROR and `stage=write-outage-recovered` (6 of 6). No loader exited. The graph went to 617017 within 67 s of Neo4j coming online, with the DLQ at 0 and parked at 0 (topic total 0). The node loaders were idle, so their gauge stayed 0. The CLI got SIGTERM: rc 0 in 6.9 s, 0 containers left. `verify`: `exact_match: true`. Query log for the whole run: 5574 entries, all `waiting: 0`. |
| (1) ride `rk-ride2` | a253b1f (final) | The stop came at 66190 relationships; Neo4j was down 45 s. 14 containers and the CLI stayed up. The scrape-time gauge read 53-56 s, the full outage including Neo4j's 11 s shutdown. ERROR and recovered lines: 6 of 6; no loader exited; 617017 relationships, DLQ 0, parked 0. CLI SIGTERM: rc 0 in 9.0 s, 0 containers left. `exact_match: true`. Query log: 6912 entries, all `waiting: 0`. |
| (2) SIGTERM `rk-term` | a535c80 | The CLI got SIGTERM 20 s into the outage: rc 0 in 11.5 s, 0 containers left. (The loader logs were lost to auto-removal; hence the rerun with log followers.) |
| (2) SIGTERM `rk-term2` | a253b1f | The CLI got SIGTERM 20 s into the outage, with the gauge at about 30 s: rc 0 in 10.9 s, 0 containers left. All 6 edge loaders logged `stage=edge-drain-abandoned ... reason=neo4j_outage`, and every node loader stopped gracefully. Nothing was committed for the held batches (redelivered on the next run). |
| (3) bulk `rk-bulk` | a535c80 | `--mode bulk --edge-leases`; Neo4j was stopped at 87328 relationships during the edge phase. rc 1, fail-closed ("exited before drain acknowledgement ... exit_code=1", "Bulk run failed"), with the CLI exiting before `docker stop` returned. No `stage=write-outage` line. 0 containers of the run left. |
| (3) bulk `rk-bulk2` | a253b1f (final) | The same with 16 containers of the run (6 edge loaders): rc 1 10.7 s after the stop began, which is the `docker stop` duration itself, so no hang. Same fail-closed errors, no `stage=write-outage` line, 0 containers left. |

Runs after the re-review (p0rk stack rebuilt 18:0xZ; images `resil2` = e9d60a7 and `resil3` = 531f3b8, sources
checked against the tree; `:resil` was left untouched for the orchestrator's bulk A/B benchmark):

| Run | Code | Result |
| --- | --- | --- |
| ride `rk-ride3` | e9d60a7 (M1 fallback: re-add and republish) | Fleet up through a 45 s outage, recovered, 617017 relationships, DLQ 0, parked 0, every query-log entry `waiting: 0`, CLI rc 0 in 7.0 s. **`exact_match: false`**: 48 WORKS_AT pairs held an older value. Traced to the DLQ resurrection described in "M1, final" (DLQ p1 o243/o246, then o579/o582 for social-works-at:3:5115/6221). That commit is superseded. |
| ride `rk-ride4` | 531f3b8 (final) | Stop at 107416 relationships, down 46 s. 14 containers and the CLI stayed up; gauge 51-52 s; 6/6 edge ERROR and recovered lines; no loader exit; 617017 relationships, DLQ 0, parked 0. CLI SIGTERM: rc 0 in 8.8 s, 0 containers left. `exact_match: true`. Query log: 5262 entries, all `waiting: 0`. (This run had no DLQ routing: the nodes landed before the edges.) |
| ride `rk-ride5` | 531f3b8 (final), `EDGES_FIRST=1` | Edges published 9 s before nodes, so the DLQ and the retry consumer were busy during the outage (about 96k DLQ envelopes over the six types). Stop at 62004 relationships, down 47 s. Fleet up; gauge about 56 s; 6/6 edge plus 1 node ERROR and recovered lines; no loader exit; 617017 relationships, DLQ 0, parked 0. CLI SIGTERM: rc 0 in 8.5 s, 0 containers left. **`exact_match: true`**. Query log: 12751 entries, all `waiting: 0`. |

One finding, pre-existing and not changed: after any edge loader exit that holds unresolved work, `consumer.close()`
fires the revoke callback, which logs `ERROR: Kafka partitions revoked with unresolved relationship work`. This
includes the existing lease drain abandon (see `bench/p0/final_p0/*`) and the new outage drain abandon. The exit
code stays 0 for a stop, as `rk-term2` shows, and nothing is committed; only the log level is misleading.

Cleanup: p0rk-neo4j, p0rk-kafka and p0rk-net were removed, and no container of any rk-* run is left. p0rj-* was
never touched.
