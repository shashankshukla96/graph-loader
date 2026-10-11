# v7 and Phase 5/6 follow-ups: final report (2026-10-01)

Branch `feat/streaming-observability` and `feat/multi-topic` are both at `9ddf9ab`. Everything is local: nothing has been pushed.

Since `4382960`: 78 commits, 133 files changed (+31k/−0.9k lines).
- Unit suite: 2660 passed, 1 skipped.
- `ruff` is clean, `make monitoring-validate` succeeds, and actionlint is clean.
- Integration tests (testcontainers): 9 passed for multi-topic and 6 for multi-pair.
- The `:latest` images were rebuilt from `9ddf9ab`.

## What was delivered

| Area | Result |
| --- | --- |
| Multi-topic relationship types (v7 S1–S3, S5) | `topics: [..]` per type. Every type with 2 or more topics needs a conflict rule (`order_by`, `precedence` or `arrival`). Supported in stream, lease, classic and planned mode. `precedence` runs in sequential per-topic stages in bulk and is refused in stream. |
| Per-type DLQ (S4) | One `<type>.dlq` per type, partitioned at the widest topic's partition count. The parked-pair index orders a pair's records by a per-pair sequence number. Rebalances use an admission barrier, so a stale view is never used. `replay-dlq --source-topic` replays each record once, in first-copy order. A preflight before `start` refuses when a DLQ from an earlier topic layout still holds records. |
| Multi-pair types (item B, S7) | One type can connect several label pairs, with one pair per topic. Pair identity carries through leases, the DLQ, the index, retries, the planner and replay. Single-pair types are byte-identical to before (golden files recorded on the pre-v7 tree). |
| Bulk default = planned | Plain `--mode bulk` runs in planned mode. A schema with a DLQ defaults to `--edge-leases`. Classic runs only with `--edge-classic`. In planned mode invalid records go to the rejection log. |
| Retry consumer reliability | System-wide problems never park rows (D1): only evidence that the row itself is at fault counts. A crash or fatal exit stops the fleet (D2). Stall and absent-backlog alerts were added. Pair order holds through duplicates, failed tombstones and crashes. Drain speed went from about 22 to about 13k rows/s. DLQ growth is bounded: blocked copies are held locally, with an overflow budget. Two latent liveness bugs were fixed: parallel re-DLQ lineages, and starvation in capped batches. |
| Slice 6 polish | Multi-topic example, dashboard rows, runbook, zero-lock-wait integration test, single-topic compatibility golden, and a manual CI workflow for the Docker proofs. |

## Final smoke on `:latest` (local stack, `bench/p0/final_smoke/RESULTS.md`)
All runs were exact and every query-log entry showed `waiting: 0`.
- IMDb 20k planned: 71 s, counts identical to the reference.
- SIGTERM to the IMDb example: the script forwards it to the CLI and leaves no orphans.
- Social, stream with DLQ (edges published 60 s before nodes): the DLQ drained and the parking lot is empty.
- multi_topic examples (planned; stream with DLQ; multi-pair stream with DLQ): exact, no relationships crossing pairs.
- Stream shutdowns exit with rc 0 in 4–9 s.
- The dev-environment test passes against the local stack.

## Owner decisions taken while you were away (please confirm)
1. **The conflict rule applies per label pair.** A rule is required only when one label pair is fed by 2 or more topics. To revert, change `_conflict_rule_is_declared` and `conflict_rule` in `src/models/schema.py`.
2. **D1 evidence is per label pair** for multi-pair types: a written Post row is not evidence against a failing Comment row. A stalled pair is reported as a stall of its type.
3. **`server`-class errors** (MemoryPool) count under the evidence rule, like `capacity` errors.

## Open items and known limits
- Precedence also loads pairs that have only one topic in sequence: correct, just slower.
- Going back from multi-pair to single-pair: detected only where a cheap check exists (the index topic). The documented gaps are `order_by` types, bulk opt-in rows and rebinding a single-pair topic.
- A lone row with an unknown error, on a type that writes nothing else, waits forever under D1. The stall alert fires.
- None of the new CI workflows has run on GitHub yet; they need a larger runner, about 16 GB.
- The project `.env` points at remote StateFarm systems. Every local run used `bench/local_env.sh` instead. `make monitoring-up` sources `.env` over exported variables.
- Live stream edge loaders exit on a Neo4j error, which stops the fleet. That is safe but not resilient. Making them ride out an outage would be a separate change.
- Leftover proof data in RAM-backed `/tmp` (about 15 GiB) was kept, as you asked.
