# Story 10 implementation plan: Performance Budget, Benchmark, and Cross-Cutting Edge-Case Tests

## Scope absorbed from Story 9 (binding, per the reviewer's agreed
re-scoping in `impl_plan_story_9.md`)

Story 9's own headline criterion — a SIGTERM to a running node/edge/
planned-writer/lease-server/planned-dispatcher/clock process still leaves
a `metrics-*.prom` snapshot with counters accumulated up to the moment of
shutdown — had no tests when Story 9's report-section work landed
(review finding S9-1). `stories.md`'s own text already scopes "a full
six-process matrix … confirming every process kind's SIGTERM path
reaches its snapshot write" into *this* story, so rather than write the
same scaffolding twice, that matrix is delivered here.

## Files

- New: `tests/test_metrics_overhead.py` (benchmark + the port-conflict/
  never-raises/multi-replica cross-cutting cases)
- Edit: `tests/test_node_loader.py`, `tests/test_edge_loader.py`,
  `tests/test_planned_writer.py`, `tests/test_lease_server.py`,
  `tests/test_planned_dispatcher.py`, `tests/test_clock_runner.py` (one
  SIGTERM-snapshot test each)
- Edit: `tests/test_cli.py` (the CLI's own `metrics-cli.prom` case, plus a
  read-only-`GRAPH_LOADER_TIMING_DIR` case)

## Design

**Six-process SIGTERM matrix.** Rather than one parametrized test (the
six processes' real entry points differ too much in shape - three are
synchronous Kafka-polling loops, two are asyncio TCP servers, one
delegates to a mocked `GlobalBatchClock`), each process kind gets its own
test in its own existing test file, following one shared recipe:

1. Capture the process's real installed SIGTERM handler by wrapping
   `signal.signal` (synchronous processes) or by injecting the same
   `asyncio.Event` `serve()` already watches (the two async servers,
   where "SIGTERM" is simulated by setting that Event directly - exactly
   what `main()`'s own real `loop.add_signal_handler` callback does).
2. Let at least one real, metrics-emitting operation complete first (a
   node/edge write, a chunk grant, a lease grant, a dispatcher
   grant+done) - captured via the real classes, with only the I/O
   boundary (Kafka `Consumer`, the Neo4j driver, `DispatcherClient`/
   `ChunkFetcher`, `ApocLockedEdgeWriter`) faked.
3. Fire the captured handler (or set the Event), let the process's own
   loop observe it and unwind through its normal exit path.
4. Parse the resulting `metrics-*.prom` file with
   `prometheus_client.parser.text_string_to_metric_families` and assert
   the relevant counter/gauge is `> 0` - not merely that the file exists.

For the two asyncio servers (lease server, planned dispatcher), rather
than drive a full ACQ/NEXT wire exchange over a real socket, the test
captures the constructed `BucketLeaseServer`/`PlannedDispatcher` instance
(subclassing it to record `self` in `__init__`) and calls
`server.table.grant(...)` / `dispatcher.state.grant(...)`+`.done(...)`
directly - the exact same methods the wire protocol calls, so the
metrics-emitting code path under test is identical, without the
complexity of a real TCP round trip in a unit test.

**CLI's own snapshot.** `metrics-cli.prom` is the one snapshot that
bypasses `GRAPH_LOADER_METRICS_SNAPSHOT_PATH` entirely (it is not a
Docker-launched container, so `docker_service.py` never sets that env
var for it) - it goes straight to `$GRAPH_LOADER_TIMING_DIR/metrics-cli.prom`
when that host env var is set (Story 8's own `_stop_cli_metrics_server`
cleanup), and is absent, not an error, when it isn't. A third test proves
a read-only/nonexistent `GRAPH_LOADER_TIMING_DIR` doesn't change
`handle_start`'s exit code, matching every other best-effort
`GRAPH_LOADER_TIMING_DIR` writer's contract in this codebase.

**Overhead benchmark.** Measures the marginal cost of exactly the calls
Stories 4-8 add beside an already-timed write (one `Counter.inc()` + one
`Histogram.observe()`, both through a label lookup) directly at the
`prometheus_client` facade level, not through the full loader/writer
machinery - deliberately, to keep the signal this benchmark exists to
catch (an accidental per-record call slipping into a future change)
undiluted by unrelated setup cost. Uses an absolute microseconds-per-call
ceiling (100us; ~3us measured empirically on a shared dev host) rather
than a percentage-of-total-time threshold, per the story's own "either/or"
guidance - looser than a percentage would be flaky against, tighter than
what an accidental per-record loop (which would push this to
milliseconds) could hide inside.

**Port conflict, "never raises", multi-replica independence.** Three
smaller cross-cutting tests, each restating an AC that's either already
proven per-story (multi-replica: each replica's registry is already a
separate object, so this is one explicit confirmation, not new
machinery) or needed a `kind=`-parametrized sweep across all six process
kinds ("never raises": Story 3's own AC only exercised `OSError`; this
extends it to any exception, for every `kind`).

## Reviewer findings and resolutions

(To be filled in after review; six-process matrix + benchmark sent as one
batch given how repetitive the per-process wiring is.)

## Definition of Done

- [x] Every one of the six process kinds has a passing SIGTERM-leaves-a-
      snapshot test with a real, nonzero counter assertion (not just file
      existence).
- [x] The CLI's own snapshot path is covered (present when
      `GRAPH_LOADER_TIMING_DIR` is set, absent when it isn't, and a write
      failure doesn't change the exit code).
- [x] The benchmark passes with a wide margin under its ceiling.
- [x] `start_metrics_server` never raises for any exception, across all
      six `kind=` values.
- [x] Three replicas report independent, non-aggregated counters.
- [x] Full unit suite (`pytest -m "not integration and not smoke"`)
      passes with this story's own tests properly excluded from that run
      (marked `integration`, per this repo's heavy-test convention) and
      included when run with `-m integration`.
