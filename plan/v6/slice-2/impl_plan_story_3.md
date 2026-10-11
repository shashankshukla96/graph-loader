# Story 3 implementation plan: HTTP Exposition — Six Container Entry Points and Docker Plumbing

## Corrections vs. `stories.md`

1. **Port 9464, not 9090**, throughout (see impl_plan_story_1.md).
2. **`name="-"` for lease-server, planned-dispatcher, clock, and cli`,
   overriding `stories.md`'s own inconsistent text** (Story 3 gave the
   clock `name=run_id` and the dispatcher `name=plan.plan_id`; Story 8 gave
   the CLI `name=run_id`). The label-policy table in Story 1 — itself
   marked "verbatim... single source of truth" — already lists these four
   kinds as always `"-"`, and Decision 2 explicitly forbids a `run_id`
   label. Confirmed with the reviewer before implementing. A `plan_id` is
   exactly as unbounded over time as a `run_id`, so it gets the same
   treatment.
3. **Story 9 (final-values snapshot on exit) folded into this story.**
   Both stories touch the same six `finally` blocks; `stories.md`'s own
   "Cross-Cutting Notes" warns against touching each twice. Landed
   together; `write_metrics_snapshot`/`GRAPH_LOADER_METRICS_SNAPSHOT_PATH`
   wiring is complete for all six process kinds as part of this story.
4. **`GRAPH_LOADER_METRICS_SNAPSHOT_PATH`/write-timing-directory mount
   extended to all six launchers**, not only the three call sites
   `stories.md`'s Story 9 text names (node/edge/planned-writer). P6-Q1's
   binding text says "every process writes its final metric values... so
   nothing is lost" — lease-server, planned-dispatcher, and clock get the
   same treatment for consistency with that binding decision, at
   negligible extra cost (the mount is already conditional on
   `GRAPH_LOADER_TIMING_DIR` being set).

## Files

- Edit: `src/loader/node_loader.py`, `src/loader/edge_loader.py`,
  `src/loader/planned_writer.py`, `src/orchestrator/lease_server.py`,
  `src/orchestrator/planned_dispatcher.py`, `src/orchestrator/clock_runner.py`
  (metrics-server start/stop + snapshot wiring in each `main()`/`serve()`).
- Edit: `src/orchestrator/docker_service.py` (ports/labels/env passthrough
  helpers `_metrics_port`, `_metrics_env_passthrough`, `_metrics_labels`,
  `_metrics_ports_entry`; applied to all six launchers).
- Edit: `tests/conftest.py` (autouse `GRAPH_LOADER_METRICS_ENABLED=0` +
  metrics-globals reset for the whole unit suite).
- Edit: `tests/test_docker_service.py`, `tests/test_planned_edges_cli.py`
  (new assertions + two pre-existing exact-dict assertions widened).

## Design

Each process's `main()`/`serve()`:

```python
process_metrics = metrics_module.process_metrics()
metrics_module.install_dlq_routed_listener()   # node/edge loaders only
metrics_server = metrics_module.start_metrics_server(process_metrics, kind=..., name=...)
metrics_module.initialize_zero_series(process_metrics, kind=..., name=...)
try:
    ...unchanged existing logic...
finally:
    ...unchanged existing cleanup...
    metrics_module.write_metrics_snapshot(process_metrics, os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH"))
    metrics_module.stop_metrics_server(metrics_server)
```

Async processes (`lease_server.py`, `planned_dispatcher.py`) wrap their
existing close-and-log step in a *nested* `try/finally` so a raise from
`server.close()`/`dispatcher.close()` can never skip the snapshot/stop
(review finding R5).

`docker_service.py` helpers resolve the metrics port from the *host's*
`GRAPH_LOADER_METRICS_PORT` (falling back to `DEFAULT_METRICS_PORT` on an
unparseable value, never raising) so the docker-published port and every
container's own in-container `GRAPH_LOADER_METRICS_PORT` agree.

## Deviation: test suite defaults to metrics disabled

Added an autouse `tests/conftest.py` fixture setting
`GRAPH_LOADER_METRICS_ENABLED=0` for the whole unit suite (hundreds of
existing tests call a loader's `main()` directly and would otherwise each
bind a real socket) — `tests/test_metrics.py` explicitly re-enables where
it needs a live server. A second autouse fixture resets
`process_metrics()`'s singleton and the DLQ-routed-listener's target/
attribute before and after every test in the whole repo, not only inside
`tests/test_metrics.py` (review finding B3a: these are process-wide
globals that would otherwise leak between test files).

## Acceptance Criteria / DoD

All of Story 3's ACs verified in `tests/test_metrics.py` (server
start/stop/disable/fallback/never-raises) and `tests/test_docker_service.py`
/ `tests/test_planned_edges_cli.py` (ports/labels/env passthrough, two
replicas never collide on a host port).
