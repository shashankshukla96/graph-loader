# Story 9 implementation plan: Final-Values Snapshot on Exit and Run-Report Totals

## Scope note (binding deviation, already recorded in `impl_plan_story_3.md`)

This story's first half — every process writing its final metric values
to `GRAPH_LOADER_TIMING_DIR`/`GRAPH_LOADER_METRICS_SNAPSHOT_PATH` on exit
(normal or SIGTERM) — was landed together with Story 3, because both
touch the same six processes' `finally` blocks and `docker_service.py`'s
three `_mount_write_timing_directory` call sites, and `stories.md`'s own
"Cross-Cutting Notes" warns against touching each twice. That work is
complete: `write_metrics_snapshot`/`stop_metrics_server` are called in
every process's `finally`, `GRAPH_LOADER_METRICS_SNAPSHOT_PATH` is set by
`docker_service.py` for all six launchers (extended beyond the three
launchers `stories.md`'s literal text named, per P6-Q1's binding "every
process" wording), and the CLI writes its own snapshot directly to
`$GRAPH_LOADER_TIMING_DIR/metrics-cli.prom` (Story 8).

**This story's remaining, actual scope is the second half**: the IMDb
example driver's run-report "Metrics totals" section.

## Files

- Edit: `examples/imdb/publish_and_load_nodes.py`
- Edit: `examples/imdb/tests/test_publish_and_load_nodes.py`

## Design

`read_metrics_snapshots(directory: Path) -> dict[str, float]` globs
`metrics-*.prom`, parses each with
`prometheus_client.parser.text_string_to_metric_families` (already a
transitive dependency — no new one), and sums every *sample* (not
family) by its full exposition name (e.g.
`graph_loader_records_consumed_total`, matching a Counter's real name in
the text — verified empirically that `family.name` strips the `_total`
suffix but `sample.name` does not) across every replica's file.
Deliberately coarse — not broken out by label — since this is a run
*total*, not a live per-label dashboard. Returns `{}`, not an error, when
no snapshot files exist (e.g. `GRAPH_LOADER_METRICS_ENABLED=0` for the
whole run, or a run that predates this slice).

`write_run_report` gains a "## Metrics totals" section, inserted right
before the closing summary lines, populated only when
`read_metrics_snapshots(timing_directory)` returns anything — a clean
omission (no section, no crash, no fake zero rows) when it doesn't. Only
the four series Story 9's own AC names are shown (records consumed, DLQ
routed, deadlocks recovered, retries), and only when present in the
totals dict — a series absent from every snapshot (e.g. `dlq_routed`
when nothing was ever malformed) is omitted from the table, not shown as
a manufactured `0` row, since `read_metrics_snapshots` itself cannot
distinguish "genuinely zero" from "never touched" the same way
`initialize_zero_series` does inside a live process. The section repeats
the catalogue's own at-least-once caveat (P5-Q5) so a reader doesn't
mistake a summed delta for an exact count.

## Re-scoping (binding, confirmed with the reviewer)

Story 9's headline criterion — a SIGTERM to a running node/edge/planned-
writer/lease-server/planned-dispatcher/clock container still leaves a
`metrics-*.prom` snapshot with counters accumulated up to shutdown, one
test per process kind — had no tests when this story's report-section
work landed (review finding S9-1). The `finally`-block wiring itself was
already reviewed and merged as part of Story 3. Rather than write six
near-identical SIGTERM-snapshot tests here and then rewrite them again for
Story 10's own explicitly-scoped "full six-process matrix… confirming
every process kind's SIGTERM path reaches its snapshot write" (`stories.md`'s
own words for Story 10), the reviewer agreed to move that matrix into
Story 10, where it belongs natively. **Story 9 is not fully closed by this
commit alone** — see `impl_plan_story_10.md` for the six-process SIGTERM
matrix and the CLI-specific `metrics-cli.prom` case that complete it.

## Acceptance Criteria / DoD

- [x] `read_metrics_snapshots` sums samples across multiple replica
      files correctly (`test_read_metrics_snapshots_sums_samples_across_replica_files`)
      and returns `{}` for an empty/nonexistent directory
      (`test_read_metrics_snapshots_empty_when_no_files`).
- [x] The report gains the section when snapshots exist
      (`test_run_report_includes_metrics_totals_section_when_snapshots_exist`)
      and omits it cleanly otherwise, including when `timing_directory`
      itself is `None`
      (`test_run_report_omits_metrics_totals_section_cleanly_when_no_snapshots`).
- [x] A series absent from the snapshots is omitted from the table, not
      shown as a fake `0` (asserted in the same test above).

Reviewed and merged in this story's own commit.
