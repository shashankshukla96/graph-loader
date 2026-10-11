# Phase 5 / Slice 4 Completion — Retry Consumer with Backoff and a Parking Lot

Status: Stories 1-8 implemented, reviewed, and merged into
`feat/streaming-observability` (merge commit `968fce0`; slice branch
`feat/p5-s4-retry`). **Story 9 (the real Docker/Kafka/Neo4j scratch proof) has
not been run.** The First Mate stopped before writing this report and before
Story 9; see "Pending owner e2e (Story 9)" below. Everything else in this
document is derived from the commit history, the code, and `docs/USAGE.md`,
not from a scratch run.

## Delivered scope vs. Definition of Done

Binding decisions from `plan/v5-v6-orchestration.md` and their status:

- [x] **P5-Q2** — only `missing_endpoint`/`ambiguous_endpoint` relationship
      rows are retried; `malformed` rows stay terminal and on the manual
      `replay-dlq` path.
- [x] **Owner decision 2** — after `max_retry_attempts`, a row is published to
      the parking-lot topic (`<resolved dlq topic>.parked`, overridable via
      `dead_letter.parked_topic`), never dropped, replayable with
      `replay-dlq --parked`.
- [x] **P5-Q4 follow-up** — `graph_loader_dlq_backlog{kind,name}` gauge added to
      the P6-S2 catalogue (23 families now); `PROCESS_KINDS` 7 -> 8
      (`retry-consumer`). Retry/park counters (`graph_loader_retry_total{reason="missing-endpoint"}`,
      `graph_loader_parked_total`) are live instead of "held at zero".
- [x] **P5-S3 -> P5-S4 ordering handoff** — default pair-parking with a durable
      `ParkedPairIndex` (cross-flush guard) plus ordered per-pair retry, and the
      optional per-edge-type `order_by` conditional-SET alternative. The
      duplicate-DLQ-copy hazard `completion_slice-3.md` handed off (a stale
      duplicate overwriting a newer commit) is closed by M12 (see Deviations).
- [x] **P5-Q1 fencing** — retry writes acquire the union of the batch's
      bucket-lease resources through the run's live lease server
      (`LeaseFencedRetryWriter`) and use the same tx-timeout write path.
      Unit-proven; the "zero lock waits" claim against real Neo4j is Story 9,
      pending.
- [x] **P5-Q5** — counters remain at-least-once.

### Story-by-story delivery

1. **Schema** (`796dc2a`, `src/models/schema.py`): `DeadLetterConfig.max_retry_attempts`
   (default 5, 1-1000) and `parked_topic`; `EdgeConfig.order_by` (must name a
   declared property); `RetryConsumerConfig` under `loading.retry_consumer`
   (`poll_timeout_ms`, `backlog_check_interval_ms`, `due_batch_coalesce_ms`);
   `parked_topic_name()` / `parked_pair_index_topic_name()` helpers; the
   DLQ-topic collision check now also covers parking-lot and parked-pairs
   topic names.
2. **Dead-letter module** (`2a573e2`, `src/loader/dead_letter.py`):
   `ensure_parked_pair_index_topic()` (compacted, fails closed on a
   non-compacted pre-existing topic); `DeadLetterRecordSink.reroute_or_park()`;
   the `on_parked` observability seam; `DeadLetterProducer.publish(fire_dlq_routed_hook=...)`
   so a park never double-counts as `dlq_routed`.
3. **Parked-pair index** (`0ab2d7b`, `aad56a1`, `5127b1b`; Phase B wiring
   `d0edf31`, `a07c378`): `src/loader/parked_pair_index.py` (see Deviations),
   wired into the live writer through P5-S3's existing `ParkedPairs` protocol
   (`IndexedMissingEndpointSink`, `_LeaseFlush.is_parked` durable fallback,
   `BucketLeaseExecutionCoordinator`/`build_edge_execution` `parked_pair_index`).
4. **`order_by` conditional-SET writes** (`48c1179`, `2149fd0`, `479c962`):
   `FOREACH`-guarded `SET r += props` only when the incoming value is strictly
   newer (a tie is a no-op); skipped rows counted as `skipped_as_not_newer_rows`
   so batch accounting is unchanged; query text byte-identical when
   `order_by` is unset.
5. **Retry consumer core** (`44426c6`, `be1810b` fixup M8-M10, `b6b1d6d` M11,
   `src/loader/retry_consumer.py`): per-partition pause/resume state machine
   with a wake heap (poll never sleeps), per-edge-type due batches, blocking
   check, read barrier, retry-or-park, per-record commit.
6. **Launch, fleet wiring, standalone invocation** (`d7e040a`, `9824d7d`,
   `ce9bba1`, `0d13a3d`, `bac9c62`, fixups `f71d7ea` M15-M17, `961e950` M18):
   `ApocLockedEdgeWriter.retry_provenanced_batch` (the real `RetryWriter`
   adapter); `LeaseFencedRetryWriter`; `DockerService.run_retry_consumer`
   (same edge image, new entrypoint `python -m src.loader.retry_consumer`, no
   fourth image); `_ensure_retry_topics` in `src/cli.py`; `main()` with
   `--config --run-id --dlq-topics [--lease-address]`; `KafkaBacklogSource`;
   `_run_lease_relationship_fleet` launches it once per streaming run (lease
   server -> retry consumer -> edge loaders), gated on at least one DLQ-enabled
   edge type; CLI flag `--retry-consumer-replicas` (default 1).
7. **Backlog gauge and catalogue amendment** (`be1810b`, `src/utils/metrics.py`):
   `graph_loader_dlq_backlog` (Gauge, `kind`/`name`); `install_parked_listener()`;
   `compute_dlq_backlog()` plus a dedicated backlog thread
   (`start_backlog_thread`/`stop_backlog_thread`) that never runs on the poll
   thread and, on failure, logs and keeps the last value.
   `scripts/validate_monitoring.py`'s catalogue copy updated to 23 families.
8. **Parking-lot replay and docs** (`3cd7adb`, `961e950` M19):
   `replay-dlq --parked`; `dead_letter_replay.py` needed no code change;
   `docs/USAGE.md` sections 3, 5.3, 8, 10, 12 updated.
9. **Scratch integration proof**: **not delivered** (pending owner).

## Files changed (principal)

- `src/models/schema.py`, `src/loader/dead_letter.py`,
  `src/loader/parked_pair_index.py` (new), `src/loader/retry_consumer.py` (new),
  `src/loader/edge_execution.py`, `src/loader/edge_loader.py`,
  `src/orchestrator/docker_service.py`, `src/cli.py`, `src/utils/metrics.py`,
  `scripts/validate_monitoring.py`, `docs/USAGE.md`, `tests/conftest.py`
- Tests: `tests/test_retry_consumer.py` (new), `tests/test_parked_pair_index.py`
  (new), and additions to the schema, dead-letter, dead-letter-replay,
  edge-execution-writer, bucket-lease-execution, edge-loader, docker-service and
  `test_cli_dead_letter.py` suites.
- `plan/v5/completion_slice-4.md` (this file)

(Exact file list: `git diff --stat 1efb97b..968fce0`.)

## Deviations from the plan

Most of these were forced by reviewer findings (M-numbers) on the stories
document's own sketches.

1. **Per-record `ParkedPairIndex`, not a per-pair `outstanding` counter**
   (M2; stories Decisions §6 sketched `{"outstanding": n}`). A counter cannot
   give "live path blocks on any outstanding record, retry path blocks only on
   an *earlier* one": excluding a retried record's own count livelocks or
   re-opens the re-DLQ-to-the-back ordering bug, and a counter loses
   idempotency across a crash/re-read (a pair could be stuck blocked forever).
   The index tracks record identity `(source_key, target_key, original_topic,
   original_partition, original_offset)`; `is_blocked(pair)` (live path),
   `is_blocked_by_earlier(record)` (retry path), and idempotent `add()`/`remove()`
   (overwrite / tombstone, no read-modify-write). "Earlier" = lower offset in the
   same partition, else lower `first_failure_at_ms` across partitions
   (best-effort). Consequently **both** the live writer (`add`, before its DLQ
   publish) and the retry consumer (`remove`) write the index, dropping the
   original "only the retry consumer writes it" asymmetry.
2. **`increment_attempt` flag on `reroute_or_park()`** (M1). A row that is only
   blocked behind an earlier outstanding record of its pair is not itself a
   resolution failure and must not consume its attempt budget; it is re-DLQ'd at
   unchanged `attempt_count` with a small fixed delay
   (`blocked_retry_delay_seconds`), never the exponential formula.
3. **Confirmed, fail-closed index publishes** (M3): `acks=all`,
   `enable.idempotence`, delivery callback, undelivered-count check; in-memory
   view updated only after a confirmed publish (a failed `remove` leaves the pair
   blocked locally: the safe direction).
4. **EOF-based index rebuild** (M4): `enable.partition.eof` /
   `_PARTITION_EOF` instead of waiting for a captured high watermark, which can
   be compacted away and would stall every restart until timeout.
5. **Watermark snapshot read barrier** (M5, M7, M8): `watermark_snapshot()` /
   `caught_up()` hold a due batch paused (never sleeping in `poll()`) until the
   consumer's own index view has caught up. Each edge type's due batch is frozen
   and its snapshot taken exactly once (`_FrozenBatch`), so a moving target
   cannot starve it and a record that became due later cannot slip under the
   barrier. `get_watermark_offsets` is bounded (`watermark_timeout_seconds`,
   default 5 s); a timeout is treated as "not caught up this tick".
6. **Throwaway `group.id` on the index's own consumers** (M6) — a real
   confluent_kafka consumer refuses to construct without one; fake-client unit
   tests could not catch it.
7. **Per-record offset commits** (M9), not batch commits: a partial-batch failure
   must not leave already-resolved records uncommitted, because republishing an
   already-resolved re-DLQ/park can reopen the ordering bug. Each record commits
   and resumes its own partition once its own outcome is durable.
8. **Poison-pill validation at read time** (M10): bad `key_bytes`, or an edge
   type this consumer has no configuration for (renamed/removed since the record
   was DLQ'd), is logged and committed past instead of crash-looping the whole
   fleet-wide consumer. The record stays in the DLQ topic (`retention.ms=-1`) for
   `replay-dlq`.
9. **Revoked partitions stripped from frozen batches** (M11) so a member never
   processes a record on a partition it no longer owns.
10. **Duplicate-copy detection via `ParkedPairIndex.contains()`** (M12). Idempotent
    MERGE and idempotent index add/remove are not enough: a DLQ publish failing
    partway through a batch republishes already-published envelopes, so two copies
    of one record can exist with no crash; the first to resolve calls `remove()`,
    unblocking a newer live record, and the stale second copy then overwrites it.
    A record whose own index entry is gone is committed past with a distinct
    `duplicate_skipped` outcome. This is the P5-S3 -> P5-S4 duplicate handoff item.
11. **`order_by` (M13, M14)**: the `$order_by` parameter is bound on every write
    (previously never bound: every write for an `order_by` type would have failed
    with `ParameterMissing`, replaying forever); and same-pair rows within one
    batch are collapsed (`_collapse_same_pair_rows_for_order_by`) because Neo4j's
    planner puts an eager barrier between the `WHERE` read and `SET` write, so
    every UNWIND row would see the pre-batch value. Losing rows have their
    properties replaced with `{}` (a harmless merge, never dropped from the
    batch). Only the batch's greatest value among *value-carrying* rows keeps its
    properties; a missing-value row is never emptied and never participates in the
    comparison (fixup 2 corrected a first version that let a missing-value row
    empty the max and regress the stored value).
    Semantics: strictly newer wins; tie is a no-op; a missing incoming or stored
    value is applied unconditionally (with a batched WARNING, N20).
12. **`LeaseFencedRetryWriter`** (`9824d7d`): acquires the union of every
    envelope's bucket-lease resources in one `acquire_any` call (server-side
    wait, no busy-retry), delegates, releases in `finally`. It does not add a
    pre-commit PING; the guarantee is the tx-timeout-plus-quiet-period mechanism
    from P5-S1.
13. **Live edge loaders never built an index** (M16, blocker). Without it
    `IndexedMissingEndpointSink` was never active on the live path, so nothing was
    indexed and the default pair-parking mode had zero retries and zero
    cross-flush blocking even though each half tested green in isolation.
    `edge_loader.py` now builds, starts, passes, and stops a writable index for
    streaming, DLQ-enabled, non-`order_by` edge types only.
14. **Parking-lot topic provisioned for `order_by` types too** (M17); only the
    parked-pairs index topic is `order_by`-conditional.
15. **Per-envelope decode in `retry_provenanced_batch`** (M15): one unreadable
    envelope is reported `invalid_on_retry` and goes through bounded attempts and
    parking instead of failing its healthy batch-mates forever.
16. **Standalone mode builds no index** (M18): with no `--lease-address` the
    bulk-routed rows have no index entries, so the M12 check would skip every
    candidate while still advancing offsets. Standalone mode therefore gives no
    cross-record ordering guarantee (documented).
17. **Docs corrections** (M19): `order_by` is "strictly newer"; the retry consumer
    is streaming-only; the index race note rewritten to state only what is
    guaranteed.
18. **Other small deviations**: a parked envelope keeps its last `retry_after_ms`
    (the encoder requires it for non-malformed classes); index topic gets
    `segment.ms=3600000` (N3) so compaction actually applies; `RetryWriter` is a
    duck-typed protocol so `edge_execution.py` keeps its one-directional layering;
    live cross-flush diversions reuse `reason_code="blocked_by_earlier_unresolved_row"`
    (there is no separate `blocked_by_parked_pair` code, although the stories'
    Scenario 3 text uses that name).

## Known limitations

- **Streaming-only.** Bulk's opt-in `route_missing_endpoints_in_bulk` has no
  automatic retry consumer; `replay-dlq` remains the bulk path. A bulk-routed
  row sharing a DLQ topic with a live streaming fleet's retry consumer is
  committed past (M12: no index entry), not retried.
- **Standalone retry consumer (no `--lease-address`)** is not lease-fenced-index
  aware: no same-pair cross-record ordering guarantee.
- **The index race between two live writer replicas** racing on the same pair is
  best-effort (a lagging tombstone only over-blocks, the safe direction; the real
  risk is a stale view on the *add* side across replicas). Documented in
  `docs/USAGE.md` section 12.
- **"Earlier" across source partitions** falls back to `first_failure_at_ms`, a
  best-effort comparison, not a strict guarantee.
- **`order_by`**: ties and type mismatches are silent no-ops; the semantics are
  strictly-newer only.
- **Retry counter scope**: `graph_loader_retry_total{reason="missing-endpoint"}`
  counts only `written`/`re_dlq`/`parked` outcomes (N10); `blocked` and
  `duplicate_skipped` are not counted.
- **`native_disjoint` and the deprecated `--stream-clock-rotation` path** are
  unaffected by this slice (see `completion_slice-3.md`).
- **Retry consumer does not retry `TransientError`s inside the fallback beyond**
  the existing Kafka-level replay cadence (carried from P5-S3); not revisited.
- **No throughput benchmark** was run; `docs/USAGE.md` section 9 is untouched.

## Test evidence

Full unit suite on `feat/streaming-observability` after the merge (reported by
the caller; not re-run for this docs-only commit):

```
1583 passed, 29 deselected, 1 xfailed
```

The one xfail is P6-S5's pending orphan test, unrelated to this slice.
`make lint` is clean.

Suite counts at intermediate commits, from commit messages: 1514 (Phase B
wiring), 1521 (M12), 1540/1542 (Story 4 fixups), 1548-1561 (Story 6 parts),
1570 (Story 6 3/3b), 1580 (M18/M19 round 2). ruff clean at each.

Not run: `tests/integration/test_edge_before_node_dlq.py` (P6-S4 scaffold) with
the retry half enabled, and the Story 9 scratch scenarios. To verify.

## Reviewer findings and resolutions

A dedicated independent Opus reviewer reviewed the slice in rounds (multiple
blocking review passes on Stories 3, 5, 6 and 8, per the commit history). All
must-fix items recorded in commits were resolved before merge:

| Item | Story | Resolution |
| --- | --- | --- |
| M1 | 3 | `increment_attempt` flag on `reroute_or_park` |
| M2 | 3 | Per-record index instead of per-pair counter |
| M3-M5 | 3 | Confirmed publish, EOF rebuild, watermark read barrier (`aad56a1`) |
| M6, M7 | 3 | Consumer `group.id`; bounded watermark timeout (`5127b1b`) |
| M8-M10 | 5 | Frozen per-batch snapshot, per-record commit, poison-pill validation (`be1810b`) |
| M11 | 5 | Revoked partitions stripped from frozen batches (`b6b1d6d`) |
| M12 | 3 (Phase B) | Duplicate-copy detection via `contains()` (`a07c378`) |
| M13, M14 | 4 | `$order_by` bound; same-pair collapse; missing-value refinement (`2149fd0`, `479c962`) |
| M15-M17 | 6 | Per-envelope decode; live loader builds index; parking-lot topic for `order_by` (`f71d7ea`) |
| M18, M19 | 8 | Standalone builds no index; docs corrections (`961e950`) |

Carried non-blocking findings (N-numbers). Status was first inferred from
commit messages; it was updated when the remaining items were closed out on
branch `fix/retry-outage-and-nitems`.

| Item | Status |
| --- | --- |
| N1, N2 | Fixed on branch `feat/v7-s1-schema` (`src/models/schema.py`) |
| N3 | Fixed (`segment.ms=3600000` on the index topic) |
| N4 | Documented (`docs/USAGE.md`: crash after a re-DLQ publish leaves two DLQ copies; same at-least-once contract as P5-Q5) |
| N5 | Documented (`docs/USAGE.md` section 10: order warning before replaying a parked row) |
| N6, N8 | Fixed |
| N7 | Documented (already in `docs/USAGE.md`) |
| N9 | Fixed (`ParkedPairIndex` poll loop waits on its stop event after a poll error: 0.5 s doubling to 30 s, reset by a good poll; test added) |
| N10 | Fixed ("blocked" outcome distinct from "re_dlq"; retry_total counts real attempts only) |
| N11 | Fixed (`_revoked_topic_partitions` revoke-callback adapter) |
| N12 | Fixed (`KafkaBacklogSource` uses one overall deadline per call and passes the shrinking remainder to each broker call; raises on expiry so the gauge keeps its last value; test added) |
| N13 | Fixed (`run_forever` guarded, non-zero exit, resources closed in `finally`) |
| N14 | Resolved (commit-failure-during-skip test in `a07c378`; the redelivery guard now also ignores resolved-but-uncommitted offsets) |
| N15 | Fixed (`KafkaBacklogSource` maps `OFFSET_INVALID` to the low watermark) |
| N16 | Fixed |
| N17 | Documented (`docs/MONITORING.md`: `queue="dlq"` counts DLQ records, not distinct rows) |
| N18 | Moot |
| N19 | Documented (stale `blocked_by_parked_pair` dropped from the `dead_letter.py` docstring) |
| N20 | Fixed (`_warn_missing_order_by` logs once per batch) |

## Pending owner e2e (Story 9)

Story 9 requires a real Kafka + Neo4j and was not run. Run it with the owner on
isolated scratch containers (`scratch-p5s4-kafka`, `scratch-p5s4-neo4j`, on their
own network, never the shared stack or any `:latest`/`:generic` tag), with Neo4j
`dbms.logs.query.enabled=VERBOSE` and `dbms.logs.query.threshold=0`, and remove
everything afterwards. Attach results to this file.

### Setup

Copy `examples/social/graph_schema.yaml` to a scratch schema and, for
`WORKS_AT` (Person -> Company; topic `social-works-at`), add:

```yaml
    dead_letter:
      enabled: true
      max_retry_attempts: 3
```

Add an `order_by` edge type for scenario 3b, e.g. give `FRIEND_OF` a
`updated_at: {type: datetime}` property (declared) plus `order_by: updated_at`
and its own `dead_letter` block (leave it out of the WORKS_AT schema copy if you
want the scenarios isolated). Shorten the backoff with a `retry:` block using a
small `base_delay_seconds` if the schema exposes it (see `docs/USAGE.md`
section 3); the stories assumed `base_delay_seconds: 1`. Streaming needs
`loading.mode: stream` style behavior; the `start` command's `--mode stream`
selects it.

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092 NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j NEO4J_PASSWORD=...      # do not paste secrets into logs
export LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 LOADER_NEO4J_URI=bolt://neo4j:7687
export GRAPH_LOADER_METRICS_PORT=9464
SCHEMA=/tmp/p5s4_social.yaml

# create the social-* topics and the generated dataset's nodes/edges are NOT wanted
# here: create topics only, then publish hand-made records (below).
python examples/social/social_graph.py publish --persons 1 --seed 7   # creates topics
python -m src.cli start --mode stream --config $SCHEMA --retry-consumer-replicas 1 &
```

`start` provisions `social-works-at.dlq`, `social-works-at.dlq.parked` and
`social-works-at.dlq.parked-pairs` itself (`_ensure_retry_topics`); stream mode
auto-launches the lease server, the retry consumer, then the edge loaders. To
publish hand-made records, reuse the example's own helpers so the wire shape
stays exact:

```bash
python - <<'PY'
import json, os
from confluent_kafka import Producer
from examples.social.social_graph import edge_record, node_record
p = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
def edge(source, target, props):
    p.produce("social-works-at", key=str(source),
              value=json.dumps(edge_record("WORKS_AT", source, target, props)).encode())
def node(topic, rec, key):
    p.produce(topic, key=str(key), value=json.dumps(node_record(rec)).encode())
# edit and run one scenario at a time (below)
p.flush()
PY
```

### 1. Edge before node, node arrives later: retried and written

```bash
# a) edge first (Person 900001 does not exist yet; Company c-1 does)
#    node("social-company", {"company_id": "c-1", "name": "Acme"}, "c-1")
#    edge(900001, "c-1", {"since": __import__("datetime").date(2024,1,1), "role": "eng"})
# b) confirm it lands in the DLQ
kafka-console-consumer --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --topic social-works-at.dlq --from-beginning --max-messages 1
# c) publish the node within the first backoff window
#    node("social-person", {"person_id": 900001, "name": "P"}, 900001)
# d) expect the relationship, exactly once
cypher-shell -a $NEO4J_URI -u $NEO4J_USERNAME \
  "MATCH (:Person {person_id:900001})-[r:WORKS_AT]->(:Company {company_id:'c-1'}) RETURN count(r)"
curl -s localhost:9464/metrics | grep -E 'graph_loader_retry_total|graph_loader_parked_total'
```

Expect: count 1; `graph_loader_retry_total{reason="missing-endpoint"}` up by the number of
attempts actually taken; `graph_loader_parked_total` unchanged.

### 2. Node never arrives: parked after max attempts, replayable

```bash
# publish an edge whose Person (900002) is never published
kafka-console-consumer --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --topic social-works-at.dlq --from-beginning          # attempt_count 1..3, growing retry_after_ms
kafka-console-consumer --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --topic social-works-at.dlq.parked --from-beginning --max-messages 1
curl -s localhost:9464/metrics | grep graph_loader_parked_total    # +1 exactly
# DLQ high watermark must stop advancing for that row:
kafka-get-offsets --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --topic social-works-at.dlq
# publish the missing node, then replay from the parking lot
python -m src.cli replay-dlq --config $SCHEMA --edge-type WORKS_AT --parked --dry-run
python -m src.cli replay-dlq --config $SCHEMA --edge-type WORKS_AT --parked
```

Expect: exactly `max_retry_attempts` DLQ cycles then one park; after replay the
row re-enters with `attempt_count=0` and is written once the node exists (count 1;
run replay twice to confirm no duplicate).

### 3. Ordering preserved across flushes (pair-parking) and with `order_by`

(a) Pair-parking, `WORKS_AT`: publish R1 for pair (P, C) with Person P missing
(properties `role: "old"`); wait for the flush; then in a **separate later
publish** publish R2 for the same pair with `role: "new"` (P still missing, then
create P). Expect R2 is diverted to the DLQ while R1 is outstanding
(`reason_code=blocked_by_earlier_unresolved_row`; not written), R1 resolves first,
R2 next, and the final relationship has `role = "new"`:

```bash
cypher-shell -a $NEO4J_URI -u $NEO4J_USERNAME \
  "MATCH (:Person {person_id:900003})-[r:WORKS_AT]->(:Company {company_id:'c-1'}) RETURN r.role"
```

To make R2's endpoints individually resolvable while R1 is still parked, use a pair
whose *target* is the missing endpoint for R1 only if the schema lets R2 resolve
differently; otherwise create the node right after R2 is diverted (R2 must be
diverted, not written, before the node exists).

(b) `order_by`: repeat with the `order_by` edge type (`FRIEND_OF`, `updated_at`).
R1 (`updated_at` older) parks on a missing endpoint; R2 (newer) for the same pair
in a later flush must write **immediately** (no diversion; the pair-parked index
topic is not even created for this type). After the endpoint arrives and R1
resolves, expect R1 does not overwrite R2's value (final properties match R2).
Also inspect that its `.parked-pairs` topic does not exist and its `.parked`
topic does.

### 4. Zero lock waits

With the retry consumer and at least one edge-loader replica writing concurrently
over overlapping node sets (e.g. scenario 1/3 while a bulk of ordinary
`social_graph.py` edges streams in), use the query log and live transactions:

```bash
docker exec scratch-p5s4-neo4j sh -c \
  'grep -o "waiting: [0-9]*" /logs/query.log | sort | uniq -c'      # only "waiting: 0"
cypher-shell -a $NEO4J_URI -u $NEO4J_USERNAME \
  "SHOW TRANSACTIONS YIELD status WHERE status STARTS WITH 'Blocked' RETURN count(*)"  # 0, sampled repeatedly
```

Expect only `waiting: 0` and no `Blocked by ...` transactions (`docs/USAGE.md` section 8).

### 5. Backlog gauge rises and drains

```bash
watch -n1 "curl -s localhost:9464/metrics | grep graph_loader_dlq_backlog"
```

Publish a burst of, say, 200 edges whose Person nodes do not exist yet; expect
`graph_loader_dlq_backlog{kind="edge",name="WORKS_AT"}` to rise (within one
`backlog_check_interval_ms`, default 10 s), then publish the nodes and expect it to
drain to 0; repeat with nodes withheld so the rows park and confirm the gauge
also drains (parked rows leave the backlog). Note: the retry consumer is a
separate container with its own `/metrics`; if the host CLI's port does not show
the gauge, scrape the retry-consumer container instead (its metrics port is
published per `docs/MONITORING.md`).

### Also record

- Backoff-to-resolution latency per scenario (reported here, not folded into
  `docs/USAGE.md` section 9).
- Re-run `tests/integration/test_edge_before_node_dlq.py`; its retry-and-verify
  half was skipped in P5-S3 pending this slice.
- Cleanup: remove the scratch containers, network, and bind-mounted data.

Note: the commands above are written from the CLI/code as merged; exact scratch
schema keys for backoff and the retry-consumer metrics endpoint should be
confirmed against `docs/USAGE.md` when running.

## Sign-off

Stories 1-8 reviewed by the dedicated independent Opus reviewer with all recorded
must-fix items resolved. Every carried N-item is now fixed, resolved, documented
or moot (table above; N1 and N2 on `feat/v7-s1-schema`). Only Story 9 (the
owner e2e) remains with the owner.

## Post-merge fix: BUG-1 (retry writer wiring)

Functional testing found the retry consumer could never retry a DLQ'd
relationship: `RetryConsumer` calls `writer.retry_batch(...)`, but `main()`
wired the bare `ApocLockedEdgeWriter`, whose method is
`retry_provenanced_batch`, so every tick raised `AttributeError`
(`write_failed -- leaving N candidate record(s) paused`). Unit tests used a
fake writer that had `retry_batch`, so nothing caught it.

Fix: explicit `ApocRetryWriterAdapter` (retry_consumer.py) mapping
`retry_batch` -> `retry_provenanced_batch` one-to-one (same envelopes in,
same `{index: (outcome, reason_code)}` out, exceptions propagate), plus
`build_retry_writer()`, which `main()` now uses in both lease-fenced and
standalone modes. `RetryWriter`/`BacklogSource` are `runtime_checkable`.
Tests: real `ApocLockedEdgeWriter` (fake driver) + adapter (+ lease fence)
driven through `RetryConsumer.tick()`; protocol-conformance check;
signature-binding checks of the real `ParkedPairIndex`,
`DeadLetterRecordSink.reroute_or_park`, `KafkaBacklogSource`, and the revoke
adapter against real `TopicPartition`s. `tests/integration/
test_edge_before_node_dlq.py` now probes `src.loader.retry_consumer` and
implements the retry half (edge DLQ'd, node published, real retry ticks write
the edge).

## Post-test fix: BUG-2 / MET-07

BUG-2: `ApocLockedEdgeWriter` built its endpoint-classification query only when
a `missing_endpoint_sink` was supplied, so the retry consumer's writer (no sink)
ran `transaction.run(None, ...)` on every tick (Cypher `SyntaxError ... Invalid
input 'None'`). The query is now always built (row-level DLQ stays gated by the
sink); main() builds the writer through `build_retry_base_writer`, which fails
fast if the query is missing. Tests now use that same factory with a strict fake
driver asserting every `run()` query is a non-empty string, and the BUG-1 main()
wiring test uses the real writer instead of a spec'd double.

## Post-test fix (final)

- Retry-consumer tick tests with the real writer now use the strict driver
  (every `run()` query must be a non-empty classification/merge query).
- Edge loaders pre-open their whole bucket-lease client pool
  (`BucketLeaseExecutionCoordinator.prime()` connects `worker_count` clients;
  on failure it closes those opened and re-raises), so a post-SIGTERM drain
  never needs a lazy reconnect (owner-approved; mirrors the node-loader fix).
