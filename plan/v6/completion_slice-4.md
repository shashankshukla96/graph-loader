# Phase 6 / Slice 4 Completion — Regression & Stress Test Suite

## Summary

All 10 stories in `plan/v6/slice-4/stories.md` are implemented, reviewed
(dedicated Opus reviewer, one continuous session across every story), and
committed on `feat/p6-s4-stress`, branched from and cleanly stacking on
`feat/streaming-observability` at `e0c735a` (the merge-base; that branch has
since moved forward with other slices' work, which is expected and does not
affect this diff). **Zero changes to `src/`.** Every change is a new file
under `tests/`, plus strictly additive edits to `.github/workflows/
ci-integration.yml`, `pyproject.toml` (the `stress` pytest marker) and
`Makefile` (two targets appended at the end), exactly as scoped.

```
git diff --stat e0c735a..feat/p6-s4-stress
26 files changed, 3744 insertions(+), 3 deletions(-)
```

15 commits (10 story commits + 5 review-driven fix commits, since real bugs
were found by actually running these tests against Neo4j, not just by
review):

```
13e0104 test(stress): star-topology dataset generator and expected-graph module (P6-S4 Story 1)
ab79b26 test(stress): lock-wait and deadlock-recovery assertion helpers (P6-S4 Story 2)
3e98994 fix(stress): lock-wait helpers must fail loudly on an empty or unreadable query log
708dde0 test(stress): in-process planned-mode harness (P6-S4 Story 3)
07d34ea test(stress): 100k target-hub planned-mode stress test, zero lock waits (P6-S4 Story 4)
5a1cba5 test(stress): reverse fan-out and multi-type hub stress tests (P6-S4 Story 5)
e512f57 fix(stress): multi_type_hub test must discriminate total-degree from per-type hub rules
af08ba5 test(stress): deliberately contended modes -- shared-overlap and bulk lease (P6-S4 Story 6)
e2da451 test(stress): 1M-edge nightly/manual tier, plus bulk-lease test fixes (P6-S4 Story 7)
47cc526 ci(stress): wire the 100k/1M tiers into ci-integration.yml and Makefile (P6-S4 Story 8)
0b2cec5 fix(stress): 1M CI job must fail if the gated test is skipped, not just pass
b64cc01 test(integration): relationship correctness regressions across all three modes (P6-S4 Story 9)
5116a94 fix(integration): pin the planned-mode missing-endpoint check to its real cause
f239d3e test(integration): edge-before-node DLQ scaffold, auto-activating (P6-S4 Story 10)
ec6872e fix(integration): the DLQ scaffold must actually opt in, or it can never auto-activate
```

## Files

New:
- `tests/stress/__init__.py`, `tests/stress/star_topology_data.py`,
  `tests/stress/lock_wait.py`, `tests/stress/planned_harness.py`,
  `tests/stress/test_star_topology_stress.py`
- `tests/test_star_topology_data.py`, `tests/test_lock_wait_helpers.py`,
  `tests/test_lock_wait_integration.py`, `tests/test_planned_harness.py`
- `tests/integration/__init__.py`,
  `tests/integration/test_relationship_correctness_regressions.py`,
  `tests/integration/test_edge_before_node_dlq.py`
- `tests/test_edge_before_node_dlq_probes.py`
- `plan/v6/slice-4/impl_plan_story_{1..10}.md`

Additive edits:
- `.github/workflows/ci-integration.yml` (`integration` job's
  `timeout-minutes` 45→60, single line; one new `star-topology-stress-1m`
  job)
- `pyproject.toml` (`stress` marker)
- `Makefile` (`stress-100k`/`stress-1m` targets, `.PHONY` updated)

## Evidence

### 100k tier — zero lock waits, exact counts (Stories 4-5)

Three planned-mode tests (target-hub, source-hub, multi-type-hub), each
100,000 relationships into one hub, run together:

```
5 passed in 108.96s (0:01:49)   # includes the two contended-mode tests below
```

Each asserts, against the real query log (not a sampling poll): `plan.
resources.hub_count == 1` and the hub key actually got that resource;
`lock_wait.assert_zero_lock_wait(container, since=mark, min_queries=len(
plan.chunks))` (a liveness floor, so an empty/broken log can't pass
vacuously); `total_attempts_over_one == 0`; and a **full**
`star_topology_data.compare(expected_graph(...), read_graph(...)) == []`
(not just a count — catches a wrong last-write-wins `seq`, not just a wrong
total). The `multi_type_hub` test deliberately forces
`plan_hub_degree=50_000` (asserted as a precondition to sit strictly
between the largest single type's count and the five-type total) so the
test can't pass under a hypothetical per-type hub rule as easily as the
real total-degree rule.

### Contended modes — no unhandled deadlocks (Story 6)

Shared-overlap (classic, `explicit_locks=True`, 16 threads × 25-row
batches against the hub) and bulk-lease mode (in-process
`BucketLeaseServer`, 8 workers) both included in the 108.96s run above.
Shared-overlap: `total_lock_wait_ms(...) > 0` measured as a precondition
(contention was real, not assumed) and `count_retry_log_lines(...) == 0`
— **not** `> 0` as the story sketch guessed: `docs/USAGE.md`'s own "Why
lock waits used to dominate" section states sorted explicit locks
(`apoc.lock.nodes`) prevent deadlocks, but not waiting, confirmed
empirically (3 separate local runs, all: real contention, exactly 0
retries). Bulk-lease: split the dataset by `leaf_id` into two disjoint
halves so both the counterfactual (no leasing, same worker count) and the
real lease-coordinated write are on genuinely fresh creates; the
counterfactual measures real contention, the real run measures zero.

### 1M tier — measured once, not guessed (Story 7)

```
1 passed in 79.70s (0:01:19)   # `make stress-1m`, matching the direct-pytest 79.07s measurement
```

Machine: this session's sandbox, 32 vCPUs visible, ~64GB-class RAM (exact
host spec not otherwise available; recorded so a future re-measurement on
a different host isn't compared blind). `hard_timeout(1800)` and
`run_planned_mode(timeout_s=1500)` are sized with generous headroom above
that measurement. **Per the owner's later "finish fast" directive, the 1M
tier was measured exactly once and is not re-run** — it stays wired as the
nightly/manual-only `star-topology-stress-1m` CI job; its 25-minute job
timeout is a large (~15-20x) multiple over the local measurement to absorb
a GitHub-hosted runner's much slower, shared 2-4 vCPUs.

### Correctness regressions (Story 9)

`examples.social.social_graph` at `persons=200`: planned and classic modes
each match the independently-computed expected graph exactly (`compare(...)
== []`) and match each other exactly; bulk lease matches structurally
(exact node graph, zero duplicates, exact per-type relationship counts —
see the real finding below for why not full property equality). A
missing-endpoint `VISITED` record fails closed with zero relationships
committed, in all three modes, using the library's default (no DLQ
opt-in) config; the planned-mode leg's failure is pinned to
`MissingRelationshipEndpointError` specifically (not just "the harness
raised something"), via the worker's own logged reason.

### Edge-before-node DLQ scaffold (Story 10)

Collects cleanly with zero Docker/Phase-5-specific imports at module scope;
skips today with a documented reason (`1 skipped in ...s`); 6 probe unit
tests (no Docker) prove both capability probes are correct in isolation,
including that `_row_level_dlq_still_missing` never swallows an unrelated
exception and `_retry_consumer_available` never raises even if `find_spec`
itself does.

### Fault-injection proof for the lock-wait helper (Story 2)

`tests/test_lock_wait_integration.py`: two real threads, one holding a
transaction through `apoc.util.sleep(2500)`, produce a **genuine** measured
wait (`total_lock_wait_ms(container) > 0`) — the non-negotiable proof that
`assert_zero_lock_wait` isn't vacuously true. `2 passed in 26.19s`.

### CI validation, without pushing (Story 8)

- `make validate-workflows` (YAML parse + `actionlint`): clean.
- Teeth-check: a deliberately broken `needs:` value in the new job was
  caught by `actionlint` (`job-needs`) and reverted.
- `pytest --collect-only -m stress`: lists all 6 Story 4-7 tests, zero
  Docker required.
- `git diff` on the `integration` job: exactly one line changed
  (`timeout-minutes`).
- `make -n stress-100k` / `make -n stress-1m`: each prints exactly one
  recipe line.
- `make stress-100k` (109.43s) and `make stress-1m` (79.70s) both run
  literally against scratch containers and pass.

### Final unit-suite and lint sanity

```
ruff check .                                                  -> All checks passed!
python -m pytest tests examples/imdb/tests -m "not integration and not smoke"
-> 1134 passed, 31 deselected
```

No stray containers left behind (`docker ps -a` checked after every
Docker-backed run in this session); only the pre-existing, untouched
`graph-loader-neo4j` dev container from another session was ever present.

## Real findings for the owner (not fixed here — all out of `src/` scope for this tests-only slice)

1. **`neo4j:5.21-enterprise` cannot run `ApocLockedEdgeWriter`'s
   `explicit_locks=True` (classic) or `sort_rows=True` (bulk-lease
   default) write path at all.** `build_apoc_locked_upsert_query`'s
   `CALL (vars) { ... }` scoped-subquery syntax (`src/loader/
   edge_execution.py`) is a Cypher feature that landed after Neo4j 5.21
   (confirmed empirically: a syntax error on 5.21.2, works on 5.26.31,
   both via direct `cypher-shell` probes against throwaway containers).
   `docker-compose.yaml`'s own production/dev stack already floats on the
   newer `neo4j:5-enterprise` tag, so this is a pre-existing version-pin
   gap between `tests/conftest.py`'s shared fixture (pinned to 5.21) and
   the real deployment target — no pre-existing integration test happens
   to exercise this exact query path against a live instance, so it was
   never caught before this slice. Every stress/correctness test this
   slice added that needs the classic or lease path uses
   `neo4j:5.26-enterprise` instead (Stories 6, 9); Stories 4, 5, 7 and 10
   (planned mode, or the DLQ scaffold's `explicit_locks=False, sort_rows=
   False` construction) are unaffected and stay on 5.21.
2. **Bulk-lease mode's last-write-wins guarantee is not actually
   guaranteed.** `build_edge_execution`'s bulk-lease path builds its
   writer with `sort_rows` left at its default (`True`) — only planned
   mode explicitly passes `sort_rows=False`. `sort_rows=True`'s `ORDER BY
   elementId(s), elementId(t)` is not a stable sort for two rows of the
   *same* pair (identical sort key), so a repeated `(source, target)` pair
   landing in the same lease-coordinated flush can commit in a different
   relative order than `expected_graph` (or any caller) assumes. Confirmed
   empirically while writing Story 9 (a `BOUGHT` record — a person buying
   the same product twice — showed a wrong-properties mismatch on the
   first run) and confirmed from source by the reviewer independently.
   Every lease-mode test in this slice (Stories 6, 7, 9) is deliberately
   structural- or count-only because of this, documented inline at each
   site. **Suggested owner fix:** build the bulk-lease writer with
   `sort_rows=False` (leases already make transactions node-disjoint, the
   same reasoning `docs/USAGE.md`/the writer's own docstring already give
   for planned mode), or add a stable tiebreaker (e.g. original row index)
   to the `ORDER BY`.
3. **The edge-before-node DLQ scaffold's auto-activation is a best guess,
   not a guarantee.** Phase 5 Slice 3's bulk-mode opt-in flag name isn't
   committed anywhere yet (no `plan/v5/slice-3/` stories doc exists at
   this slice's authorship). The scaffold (`tests/integration/
   test_edge_before_node_dlq.py`) opts in via `dead_letter=
   DeadLetterConfig(enabled=True)` on the edge config, guessing Slice 3's
   bulk opt-in will key off that same field. **Handoff item for whoever
   implements Phase 5 Slice 3:** if Slice 3's real opt-in is a different
   flag, or if row-level DLQ only ever gets wired into the streaming/lease
   write path (not the bulk `ApocLockedEdgeWriter.write_batch` call this
   scaffold drives), this test will keep skipping forever and must be
   updated by hand — check `_works_at_edge_config_with_dlq_opt_in` and the
   module's own "Known limitation" docstring first.
4. **`tests/conftest.py::kafka_bootstrap` still `pytest.skip`s on a Kafka
   startup `TimeoutError`.** This pre-existing fixture (not editable under
   this slice's file-ownership rule) means a Kafka-start failure reports
   skip, not fail, in a bare local run. In CI specifically this is already
   mitigated: both the `integration` and `star-topology-stress-1m` jobs'
   "Fail if Docker-backed tests were skipped" guard greps for the exact
   `"Docker is required"` text that fixture's skip reason contains, so a
   Kafka-start failure in either CI job still fails the job. Only a bare
   local `pytest` invocation (outside `make`/CI) would silently report
   green-by-skip.
5. **Neo4j memory is pinned in every stress container** (`2G` heap /
   `1G` page cache) so the measured 100k/1M timings and behaviour carry
   over to a GitHub-hosted runner's much smaller RAM instead of Neo4j
   auto-sizing off this sandbox's full (very different) memory — but the
   1M job's real CI timing is still unmeasured (see below).
6. **No `pytest-timeout` dependency exists in this repo.** Checked
   `requirements.txt`/`requirements-dev.txt` first, per this slice's own
   file-ownership rule; used `lock_wait.hard_timeout` (stdlib
   `faulthandler.dump_traceback_later(exit=True)`) instead of adding one.
   `hard_timeout` calls `os._exit` on a genuine hang, which skips a
   fixture's own teardown, but testcontainers' Ryuk reaper (enabled by
   default throughout this repo) still removes the container from outside
   the killed process. Flagged here as a candidate dependency for a
   future slice if a native pytest-level timeout is ever wanted instead.
7. **The 1M tier's real CI timing is unmeasured.** Per the owner's later
   "finish fast" directive, the 1M tier was not run on a GitHub-hosted
   runner (only locally, once, on this sandbox's much larger machine). The
   25-minute `star-topology-stress-1m` job timeout is an extrapolation
   with a large safety margin, not a second measurement; the first real
   run of that job will be the actual validation of that number.
8. **Base branch for the eventual PR/merge must be
   `feat/streaming-observability`, not `main`** — `git diff` against
   `main` would show unrelated `src/` changes from other, already-merged
   Phase 5/6 slices that predate this branch's fork point. This slice's
   own diff (`git diff e0c735a..feat/p6-s4-stress`, `e0c735a` being the
   documented merge-base) is the correct, purely-additive scope shown
   above.

## Reviewer summary

One dedicated Opus reviewer (general-purpose agent, read-only, not in a
worktree) reviewed every story's plan and diff across the full slice in one
continuous session, retaining context story-to-story. Every REQUIRED fix it
raised was applied and re-verified before moving on; RECOMMENDED items are
folded into the findings list above rather than deferred silently. Notable
review catches (beyond the two production findings above, which the
reviewer helped pin down from source): the bulk-lease test's original
`worker_count` defaulted to 1 (vacuous — a single worker can't contend with
itself); the same test's counterfactual initially wrote the *entire*
dataset before the real run, leaving nothing fresh to create; the 1M test
originally paid for a full container startup/teardown just to skip
(switched to `@pytest.mark.skipif`); the 1M CI job's own skip-guard didn't
actually check that its one test ran; and the `multi_type_hub` test's
original hub-degree threshold couldn't distinguish a total-degree rule from
a (hypothetical) per-type one.

## Before merging

- Rebase/merge target is `feat/streaming-observability`.
- Nothing in this slice touches `src/`, `docs/`, `docker-compose*.yml`, or
  `requirements*.txt`.
- The `stress` pytest marker and the `Makefile`/`ci-integration.yml`
  additive edits were confirmed to have no collision with any other
  in-flight slice's own additive edits to the same files at the time this
  slice was authored; re-confirm at actual merge time per Story 8's own
  dependency note.
- See "Real findings" #2 and #3 above before Phase 5 Slice 3/4 land — they
  are the two places this slice's own tests may need a follow-up commit
  once those slices' real interfaces exist.
