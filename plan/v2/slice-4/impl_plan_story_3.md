# Story 3 Plan — Scoped Bulk CLI Orchestration and Verification

Modify `src/orchestrator/docker_service.py`, `src/cli.py`,
`tests/test_docker_service.py`, `tests/test_cli.py`, and create
`tests/test_bulk_e2e.py`.

- Extend `DockerService.run_node_loader()` with optional `run_id`. For a bulk
  run, set `GRAPH_LOADER_RUN_ID`, append `--run-id <run_id>` and
  `--replica-id <index>` to the loader command, and label each created container
  with `run_id` and `replica_id`. Include a collision-free run suffix in bulk
  container names while retaining deterministic label/replica identity. The
  returned container objects remain the sole stop targets for that run.
- Add `--bulk-timeout-seconds` to the CLI start command (positive, default 300).
  In `handle_start`, stream mode retains the existing launch-and-return behavior.
  Bulk mode generates one UUID run id, launches every schema node with that id,
  verifies each returned replica count, and maps exact returned objects to
  `(node_label, replica_id)`.
- Instantiate `BulkMonitor` with that exact mapping and the explicit host-side
  Kafka bootstrap address `os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")`
  (never DockerService's container-side `kafka:29092`). Execute the safe sequence: wait for assignment coverage →
  capture boundary → wait through boundary → send `stop()` only to the tracked
  objects → await DRAIN_COMPLETE acknowledgements → verify post-drain zero lag.
  Return success only after all stages succeed. Close the monitor in `finally`.
- On launch, monitor, boundary, quiescence, health, drain, verification, or stop
  failure, report a nonzero result and stop only exact containers created for the
  active bulk run. Never call label-filtered/global stop on a run failure, so an
  overlapping older fleet cannot be touched. The intentional drain loop attempts
  `stop()` once for every tracked target even if an earlier stop raises, collecting
  failures after all attempts. For a failed exact target, reload it and, if it is
  still running, make one bounded follow-up `stop()` attempt on that same object;
  report a cleanup failure if it remains running. Cleanup never touches an
  untracked object or invokes label-filtered/global stop.
- Add unit tests for Docker run scoping (labels, environment, arguments, and
  unique bulk names), stream-mode no-monitor behavior, happy-path ordered bulk
  calls (including the host bootstrap argument), idle-surplus monitor support,
  quiescent violation cleanup, launch rollback, multi-replica stop failure that
  remains running and receives an exact-target follow-up, and
  post-drain verification failure. Mocks assert all
  stop calls target only containers returned by that invocation. Add a focused
  `test_bulk_e2e.py` module for the CLI lifecycle contract; actual Kafka/Neo4j
  boundary semantics are covered by Story 2 monitor tests.
- Update README bulk operation guidance with the finite-bulk contract and note
  that stream mode does not auto-stop at zero lag.

Rollback removes the optional run-scoping arguments, monitor invocation, and
tests; it does not alter Story 1 loader drain acknowledgements or Story 2 monitor
semantics.
