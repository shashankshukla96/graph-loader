# Option A: durable resolved markers. Design notes, not implemented

Status: approved by the owner on 2026-10-02. Work stopped before any code was
written, so this file records the design reached so far for whoever picks it up.
Option B (the retry consumer's in-process memory of resolved offsets, commit
`bb47900`) is the shipped mitigation. Declaring `order_by` on a type avoids the
problem entirely.

## Problem

A crash, a stop-drain abandon or a failed revoke settlement redelivers a
partly committed flush. Meanwhile the retry consumer may already have resolved
a dead-lettered row of that flush and tombstoned its index entry. The
redelivered flush then re-adds and republishes that row, and an older value
can land after a newer one.

Option B skips such copies only when the same retry-consumer process saw the
newer resolution. It loses that knowledge across a restart, a revoke, an LRU
eviction, and across topics. Under fault injection with restarts, 58 of 600
fuzz runs were still bad.

## Design

1. **Marker topic, one per type: `<dlq>.resolved`.**
   - Use one name helper, the same way `.dlq.parked` and `.dlq.parked-pairs` are named. `dead_letter.resolved_topic` overrides the name.
   - `cleanup.policy=compact,delete`. The new setting `dead_letter.resolved_retention_ms` defaults to 7 days. That default is the orchestrator's choice and the owner can change it.
   - The topic has the same partition count as the DLQ.
   - Add it to the schema's DLQ-family name collision check.
   - Provision it in `_handle_start`, from the parent edge. Only stream mode needs it, and only for indexed (non-`order_by`) DLQ types: bulk opt-in rows are never indexed, and a standalone retry consumer has no index.
2. **Marker format.**
   - Key: canonical JSON `["resolved", topic, partition, offset]`, with `S, T` appended for multi-pair types. Index keys always have 5 elements, so a marker key can never be read as an index key.
   - Value: `{topic_id, outcome, resolved_at_ms}`.
3. **Topic-ID guard. This is required.**
   - Each marker stores the source topic's Kafka topic ID, taken from AdminClient `describe_topics`. A marker counts only while that ID matches the topic's current ID.
   - Without the guard, markers kept for 7 days would silently drop records after the documented "delete and recreate your input topics" step followed by a rerun, because new records reuse the same (topic, partition, offset).
   - The retry consumer refuses to start if a type's topic has no ID.
   - Recreating a topic mid-run stays unsupported.
4. **Retry consumer.**
   - A background poller reads the whole marker topic into memory, following the parked-pair index pattern.
   - Store the view as offset ranges per (topic, topic ID, partition), so loads that dead-letter everything first stay small. Put a hard cap on it that evicts the oldest entries first.
   - Each batch takes a snapshot at freeze time and waits until the view has caught up, next to the index read barrier.
   - New rule: write a row only if it is contained in the index AND not resolved. A resolved copy is committed past, and its re-added index entry is tombstoned, so it can't orphan.
   - Publish a marker, and wait for its ack, before each of the three tombstone sites: `flush_tombstones`, the chain levels, and the stale copies. A failed marker publish is handled like a failed tombstone: the row stays outstanding and its pair stalls.
5. **Keep option B as the fast path.** It also covers one rare case that markers alone miss: an earlier record written directly by the live loader and then re-added behind a blocker. In practice that needs ambiguous endpoints.
6. **No skip in the live writer.** Correctness rests on the retry consumer either way. Without the skip, a resurrected entry blocks its pair only until one consumer batch tombstones it. Adding the skip would make every edge-loader replica read the whole marker topic.
7. **Remaining bound after option A:** a resurrection is still possible only if a redelivered copy arrives more than `resolved_retention_ms` after the resolution, or after its marker was evicted under the memory cap.

## Work still to do

- **Schema and provisioning:** the name helper, the two settings and their validation, and provisioning.
- **Retry consumer:** the marker view module, and its wiring into the retry consumer (barrier, publish before tombstone, the selection rule).
- **Metrics:** marker publishes, view size and barrier wait. Use bounded labels.
- **Docs:** update USAGE §10 and §12 (the replay-resurrection limit becomes the retention bound), INCREMENTAL_LOAD, ARCHITECTURE and CHANGELOG, and record the 7-day default in plan/v5-v6-orchestration.md.
- **Tests:**
  - unit tests;
  - the reviewer's probes as tests;
  - the redelivery fuzz *with* retry-consumer restarts must reach 0 bad runs, with a markers-off control that still fails;
  - re-run `test_retry_pair_order_fuzz`, `test_retry_liveness` and `test_retry_replay_resurrection`.
- **Proofs:**
  - a Docker proof that kills the retry consumer mid-drain and restarts it;
  - bulk `--edge-leases` and planned control runs, which must be unchanged;
  - an Opus review.
