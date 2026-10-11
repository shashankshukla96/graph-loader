# Work log: 2026-09-29 to 2026-10-02

This is the record of everything done in this Claude Code session on the graph-loader library: what was asked, what was decided, what was built, how it was tested, and what is still open.

- **Branch:** `feat/streaming-observability`. `feat/multi-topic` points at the same commit.
- **Commits:** 340+ ahead of `main`.
- **Pushed:** nothing. The owner's standing rule is never push.

Related files:

| Source | Location |
| --- | --- |
| Full conversation, readable | `/mnt/graph-loader/bench/transcripts/TRANSCRIPT.md` (your messages, my replies, one line per tool call) |
| Raw session and subagent logs | `/mnt/graph-loader/bench/transcripts/raw/` |
| Change list with upgrade notes | `CHANGELOG.md` |
| Phase 5/6 final report | `plan/v5-v6-final-report.md` |
| v7 final report | `plan/v7/final-report.md` |
| Per-slice completion reports | `plan/v5/`, `plan/v6/`, `plan/v7/` |
| Test results | `/mnt/graph-loader/bench/p0/*/RESULTS.md` |

---

## 1. Ground rules the owner set

| Rule | Where it came from |
| --- | --- |
| Never push. | Stated at the start and several times later. |
| No relationship (or node) write ever waits on a Neo4j lock. Anything that comes up against this goes to the owner. | Early in the performance work. The whole design rests on it. |
| "Best" means least load time and maximum performance. | Performance phase. |
| It is a generic library: you give it a YAML schema and it works out the scheduling. | 40k-seed phase. |
| Neo4j: 8 cores, 12 GB page cache, 8 GB heap. Kafka on cores 8-11. | Pinned resources for benchmarks. |
| I act as orchestrator: plan, delegate and review; subagents write the code. Sonnet does implementation and planning, Opus reviews. Opus also implements where Sonnet missed correctness issues. | Phase 5/6. |
| Run work in parallel only in separate worktrees, and only when merge conflicts are unlikely. Always check dependencies first. | Phase 5/6. |
| Test once and move on; finish fast. | Throughout testing. |
| Use the local Neo4j and Kafka only. Never read or source `.env`, which points at remote systems. | 2026-10-01. Enforced with `bench/local_env.sh` and `bench/p0/COMMON_RULES.md`. |
| Docker data lives on the instance store (`/mnt`). Don't touch the root EBS volume. | Infrastructure. |
| Nothing may break or affect the bulk ingestion path. | Stream-outage work. Every later change carried tests proving bulk was unchanged. |

---

## 2. Performance work (2026-09-29)

**Goal:** speed up the IMDb Kafka-to-Neo4j load.

**Pinned resources:** Neo4j on cpuset 0-7, Kafka on 8-11, loaders on 12-17.

**Steps:**
- **Baseline and profiling:** a benchmark harness in `/mnt/graph-loader/bench/` (`run_bench.sh`, fingerprints, query-log lock-wait counting, samplers).
- **Classic-mode tuning:**
  - shared endpoint labels can overlap;
  - validate, lock and merge in one statement;
  - concurrent label readiness and replica launch;
  - a faster multi-process publisher.
- **The lock problem:** classic mode used APOC sorted locks, and transactions waited on each other. The owner's rule required zero waits, so two zero-wait designs were built:
  - **Bucket-lease coordinator** (`--edge-leases`). A TCP lease server hands out node-disjoint resources (label buckets) to writers.
  - **Globally planned bulk mode** (`--edge-plan`).
    - The planner maps every node to a resource: a label hash bucket, or its own resource for a hub of degree ≥ H.
    - It cuts each (type, source resource, target resource) unit into chunk chains of at most C rows.
    - A dispatcher grants node-disjoint chunks to K writers.
    - The result is zero lock waits by construction.
- **Turned into a library:**
  - a planner that reads the schema's topics, with generic keys;
  - K, B, C and H derived automatically from the server and the data volume;
  - an external-memory spill planner for inputs larger than RAM.
- **Results** (planned mode, all exact, 0 lock waits):

  | Load | Time |
  | --- | --- |
  | IMDb 20k | 525 s → about 60 s |
  | IMDb 40k | 107.8 s |
  | 80k seed, depth 8, incremental offset batches | exact (no single timing) |

- **Neo4j at 16 cores:** 20-27% faster, but only once the writer count was derived from the server.
- **Research:** a Flink pre-scheduling layer was evaluated and not adopted.

---

## 3. Phases 5 and 6 (2026-09-29 to 09-30)

The phases were split into 10 slices. Each had a cartographer plan, a First Mate implementation and a dedicated Opus reviewer. The workflow came from `workflows/`.

| Slice | Delivered |
| --- | --- |
| P6-S1 CI | Blocking `ci.yml` (lint, unit tests, image builds), nightly or manual `ci-integration.yml`, and a ruff baseline. |
| P5-S2 Malformed-record DLQ | Per-type `<topic>.dlq` with an envelope codec, and `replay-dlq`. |
| P6-S2 Metrics | Prometheus endpoint on :9464, a metric catalogue, and per-process snapshots. |
| P6-S3 Monitoring | `docker-compose.monitoring.yml` (Prometheus with docker_sd, plus Grafana), a dashboard, alerts with promtool tests, and `make monitoring-*`. |
| P6-S4 Stress and regression tests | Star topology at 100k (CI) and 1M (nightly), and correctness regressions across modes. |
| P5-S1 Streaming leases | Lease fleet as the stream default; a supervised, restartable lease server; time-based fencing (transaction timeout T, quiet period Q ≥ T + margin, rollback on lease loss); the clock retained behind `--stream-clock-rotation`. |
| P5-S3 Row-level DLQ | Rows with missing endpoints are dead-lettered; the rest of the batch commits; same-pair ordering is preserved. |
| P5-S4 Retry consumer and parking lot | Retry with backoff, the parked-pair index, an `order_by` conditional SET, `<dlq>.parked`, `replay-dlq --parked`, and the backlog gauge. |
| P6-S5 Graceful shutdown | Shutdown event, SIGINT/SIGTERM handled in every phase, no orphaned containers. |

Then a 155-case functional test plan (`plan/v5-v6-functional-test-plan.md`), a smoke run, and the P0 cases.

**Bugs found by P0 and fixed:**
- **BUG-1:** the retry writer adapter was missing.
- **BUG-2:** the classification query was never built.
- **MET-07:** snapshots were only written at exit. They are now periodic.
- **Shutdown defects D1-D6:** init in containers, the drain loop, 409 errors on cleanup, stale plan topics, exit codes, and planner termination.
- **ROW-11:** node writes waited on edge writes. Fixed by leasing stream node writes too (owner decision).

**Retry consumer: rewritten in five review rounds.**
- **Throughput:** about 22 rows/s → about 13k rows/s, using batched, lease-coordinated drains.
- **Failure handling:** an evidence-based rule.
  - **D1:** a system-wide condition never parks a row; a row's failure counts only when there is evidence the problem is the row itself.
  - **D2:** the retry consumer is supervised; a crash stops the fleet.
- **Also covered:** stall and fatal handling, per-pair order across duplicates, tombstones and crashes, overflow holds, and the lineage memory for superseded copies.

**Stream ride-out:** stream edge and node loaders now ride out a Neo4j outage instead of stopping the fleet. Bulk is unchanged.

---

## 4. v7: multi-topic and multi-pair relationship types (2026-09-30 to 10-01)

**Owner decisions:**
- A relationship type fed by 2 or more topics must declare a conflict rule: `order_by`, `precedence` or `arrival`.
- One DLQ per type.
- Item B (multi-pair types) comes after multi-topic.
- Stream ordering by timestamp is deferred.
- Planned mode is the bulk default:
  - `--edge-classic` is the only mode that can wait on locks;
  - a schema with a DLQ defaults to `--edge-leases`.
- Confirmed on 10-02:
  - the conflict rule applies per label pair;
  - D1 evidence is per label pair;
  - MemoryPool errors count against a row only with evidence.

| Slice | Delivered |
| --- | --- |
| S1 | `topics: [...]` and `topic_conflict` in the schema; per-type DLQ names; `extra="forbid"` on loading configs. |
| S2 | Stream and bulk classic/lease across all topics of a type. |
| S3 | Planned mode: precedence made exact through plan-chain order. |
| S4 | One DLQ per type; a sequenced parked-pair index; `replay-dlq --source-topic`; DLQ growth bounded with local holds and overflow holds. |
| S5 | Precedence in classic and lease bulk, as sequential per-topic sub-stages. |
| S6 | Example, dashboards, runbook, zero-lock-wait integration test, single-topic compatibility goldens. |
| S7 (item B) | One type spanning several label pairs. Pair identity runs through leases, DLQ, index, retry, planner and replay. Single-pair types stay byte-identical. |
| Bulk default | Planned mode by default; malformed records go to planner rejection files. |

---

## 5. Testing and fixes on the final code (2026-10-01 to 10-02)

- **Integration and smoke tests:** all pass, including 1M-edge stress, streaming lease e2e, rotation, multi-topic and multi-pair. The 3 stale tests were fixed.
- **P0 functional re-run on the final code:**
  - every case that ran passed;
  - 6 cases are marked plan-outdated;
  - the cleanup pass fixed the intermittent SIGTERM loss (F1) and minor items.
- **Bulk performance A/B** (baseline against the stream-outage branch): no regression in any mode.
- **Replay resurrection:**
  - **Problem:** an older row could come back after a redelivery.
  - **Option B (shipped):** the retry consumer skips stale copies using its in-process memory.
  - **Option A:** a durable marker topic. Approved, but stopped before implementation; its design is in `plan/option-a-resolved-markers.md`.
- **Stable stream consumer groups:** `<prefix>-<type>` and `<prefix>-retry-consumer`, so a stream restart resumes. Migration from the old run-scoped groups and a one-live-fleet guard are included.
- **Lease restarts:**
  - The restart cap now resets after a healthy period.
  - The fenced BEGIN means no leased transaction outlives its lease. The minimum T in lease modes is 6000 ms.
  - The owner chose to keep the fence in bulk `--edge-leases`.
  - The Neo4j driver is pinned below 7.
- **Self-referencing bucket count:** B = clamp(round(K × (1 + s)), 8, 64) when same-label pairs exceed 10% of rows.

  | Dataset | Edge dispatch | Wall time |
  | --- | --- | --- |
  | FRIEND_OF-heavy | −25 to −40% | −10 to −22% |
  | IMDb and mixed social | unchanged | unchanged |

- **Final smoke on `:latest`:** every graph exact and every query-log entry `waiting: 0`. Covered:
  - IMDb planned and lease runs;
  - stream with DLQ;
  - a restart that resumes;
  - refusing a second fleet;
  - multi-topic and multi-pair;
  - a 46 s Neo4j outage ride-out;
  - the integration tests.

---

## 6. Documentation

| Doc | Content |
| --- | --- |
| `README.md` | Entry point and a quick start that was run live and works as written. |
| `docs/BULK_LOAD.md`, `docs/INCREMENTAL_LOAD.md` | Step-by-step guides. |
| `docs/CLI.md` | Every command, flag, exit code, log line and environment variable. |
| `docs/ARCHITECTURE.md` | Components, data flows, and how zero lock waits is achieved in each mode. |
| `CHANGELOG.md` | Every change since `main`, with 22 upgrade notes. |
| `docs/USAGE.md`, `docs/MONITORING.md` | Full reference, corrected. |
| `docs/HISTORY.md` | Old phase-era design notes. |

Every doc was checked against the code by an Opus accuracy review and corrected.

A comparison with `neo4j-parallel-spark-loader` found the same core grouping idea, about 5% of our code. Our dynamic dispatcher, hub handling and planning across relationship types are what make us faster. The self-referencing bucket fix came from that comparison.

---

## 7. Final state

- **Code:** `feat/streaming-observability` = `feat/multi-topic` (see `git log -1`).
- **Tests:** about 3350 unit tests pass. Lint and monitoring validation are clean.
- **Images:** `:latest` rebuilt from the final code.
- **Infrastructure:**
  - Docker data root on `/mnt/docker`; root EBS grown to 40G; Neo4j query-log rotation at 512 MB × 20.
  - Old proof data deleted on 10-02 at the owner's request.
- **Not done:**
  - **Option A:** design notes only.
  - **Bulk node-readiness delay:** about 10 s; predates this work.
  - **Remaining Spark-comparison ideas:** ledger resume, sorting rows inside chunks, starting writes before planning finishes.
  - **P1/P2 functional cases:** never run.
  - **CI on GitHub:** never run; needs about a 16 GB runner, and nothing is pushed.
  - **Grafana:** checked through its API only, not in a browser.
