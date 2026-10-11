# Story 2 implementation plan: The Metric Catalogue

## Corrections applied vs. `stories.md`

1. **22 series, not 19.** The stories document's own table lists 22 distinct
   metric names (it undercounts itself); `EXPECTED_METRIC_FAMILIES` in
   `src/utils/metrics.py` lists all 22, and the completeness test asserts
   the registry contains exactly that set.
2. **`records_consumed_total` does not count a DLQ'd record.** Story 2's
   prose ("records durably resolved (written, or DLQ'd)") conflicts with
   Story 4's explicit, checkbox-level acceptance criterion ("does **not**
   increment records_consumed_total"). The specific AC wins: a DLQ'd record
   increments only `dlq_routed_total`, never `records_consumed_total`.
   Recorded as a deviation in `plan/v6/completion_slice-2.md`.
3. **Zero-touch initialization.** A `Counter`/`Histogram` only creates a
   labelled child (and so only appears in a scrape) once `.labels(...)` is
   called for that combination — merely constructing the parent metric does
   not make `parked_total{kind="node",name="Person"}` read `0`. Added
   `initialize_zero_series(metrics, kind=, name=)` (called once per process
   at startup, in Stories 4/5/6) to pre-touch `parked_total`, every
   `retry_total{reason=...}`, and `dlq_routed_total{failure_class="malformed"}`
   for a given `(kind, name)`. The "kind only" series (lease-server/
   dispatcher/planner/CLI-scoped) are pre-touched by
   `start_metrics_server`'s own process-info triad the same way.

## Files

- Edit: `src/utils/metrics.py` (catalogue added to `GraphLoaderMetrics.__init__`,
  `EXPECTED_METRIC_FAMILIES`, `FAILURE_CLASSES`, `initialize_zero_series`).
- Edit: `tests/test_metrics.py` (completeness, bucket-sharing,
  `FAILURE_CLASSES` cross-check, zero-touch, both cardinality-guard scans).

## Design

See `src/utils/metrics.py` catalogue block (22 series, grouped: per-entity
write-path counters/histograms; lease-server-scoped; planned-dispatcher-
scoped; CLI/planner-scoped; process-info triad). `WRITE_DURATION_BUCKETS`
(Story 1) is the single bucket set shared by every `_seconds` histogram —
verified by a test reading each `Histogram`'s private `_upper_bounds`.

`FAILURE_CLASSES` is duplicated (not imported) from
`src.loader.dead_letter.FAILURE_CLASSES`, with a test
(`test_failure_class_matches_dead_letter_enum`) asserting the two tuples
stay equal — this keeps `src/utils/metrics.py` free of any project-internal
import at module scope while still catching drift if either enum changes.

Cardinality guard implemented as two independent `ast`-based source scans
(cheaper and more reliable than a runtime check, per the story's own
guidance):

1. No `.labels(...)` call site under `src/` passes a keyword value
   containing an `ast.Subscript` (a record/row/message index) anywhere in
   its expression tree.
2. No `.inc()`/`.observe()`/`.set()` call sits inside a `for record in
   ...`/`for row in ...`/`for msg in ...`/`for message in ...` loop
   (heuristic: loop variable name, not iterable shape — matches this
   codebase's existing singular-loop-variable style).

Both scans run today (before Stories 4-8 add any real call sites) and pass
vacuously; they become load-bearing once those stories land.

## Acceptance Criteria / DoD

Both satisfied by `tests/test_metrics.py`'s Story 2 section (see file for
the 8 new tests): completeness, bucket sharing, `FAILURE_CLASSES` parity,
`RETRY_REASONS` closure, zero-touch reads, both scan guards.
