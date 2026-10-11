# Story 3 implementation plan: in-process planned-mode harness

File: `tests/stress/planned_harness.py` (new). Unit/integration test:
`tests/test_planned_harness.py` (new, `@pytest.mark.integration`, small scale).

## Corrections vs. the story doc's sketch (verified against current source)

- **Writer construction.** The story sketch says writers are built by
  `src.loader.edge_execution.build_edge_execution(edge_config, driver)`.
  Traced `src/loader/planned_writer.py::main()` (the actual planned-writer
  process entry point) line by line: it builds writers directly as
  `ApocLockedEdgeWriter(driver, edges[name], explicit_locks=False,
  sort_rows=False, missing_endpoint_error_factory=MissingRelationshipEndpointError)`
  -- it never calls `build_edge_execution` at all. The harness must match
  this exact call, not the story's sketch.
- **Per-writer resources, not shared.** The story's Technical Context says
  "each with its own DispatcherClient, ChunkFetcher(...)" -- confirmed this
  is required, not optional: each writer *thread* here stands in for one
  writer *process* in production, and each production process owns its own
  Kafka `Consumer`. `ChunkFetcher` itself is internally thread-safe (one
  background thread drains a `Queue`), but giving every harness worker
  thread its own `Consumer`/`ChunkFetcher`/group id (mirroring
  `planned_writer.main()`'s `group.id = f"graph-loader-planned-{plan_id}-
  {writer_id}"`) matches production exactly and avoids ever having to
  reason about whether sharing one `Consumer` object across threads is
  safe. The `writers: Mapping[str, object]` (edge type -> `ApocLockedEdgeWriter`)
  dict, by contrast, is safe to share across worker threads: each call
  opens its own Neo4j session and the writer object carries no
  cross-call mutable state.
- **`verify_ledger`'s summary source.** `PlannedDispatcher` is a plain Python
  object here (no Docker), so the harness could read `dispatcher.state.
  summary()` directly -- but that races with the asyncio event-loop thread
  still mutating `state` for in-flight connections. Instead, after every
  worker thread has been joined, the harness opens one more `DispatcherClient`
  in the CONTROL role (`writer_id=None`) and calls `.summary()` over the
  same TCP protocol the CLI's own orchestrator uses -- serialized through
  the event loop like every other command, matching production exactly.
- **`PlanSettings.hub_degree`** (not `plan_hub_degree` as a `PlanSettings`
  field name -- confirmed against `src/loader/edge_planner.py:171-211`); the
  harness's own `plan_hub_degree` parameter maps onto that field.
- **`KafkaEdgePlanner.finish(plan_path)`** can be called without first
  calling `.start()` (it calls `.start()` internally if `self.planner is
  None` -- confirmed at `edge_planner.py:1628`); it returns a `PlanResult`
  with `.plan` and `.digest`.

## `tests/stress/planned_harness.py` contents

```python
@dataclass
class PlannedRunResult:
    plan: EdgePlan
    ledger: dict          # DispatchState.summary()'s "ledger" list, as recorded
    total_attempts_over_one: int
    wall_seconds: float

class PlannedHarnessError(RuntimeError): ...

def run_planned_mode(
    schema, driver, kafka_bootstrap: str, *, plan_dir: Path, writers: int = 4,
    plan_hub_degree: int | None = None, plan_buckets: int | None = None,
    chunk_rows: int | None = None, plan_workers: int = 2,
) -> PlannedRunResult: ...
```

- Builds a `PlanSettings(topic=f"graph-loader-plan-chunks-{run_id}",
  hub_degree=..., buckets=..., chunk_rows=..., workers=plan_workers).validate()`
  (only the fields the caller overrode differ from the dataclass defaults),
  a fresh `run_id = uuid.uuid4().hex[:12]` (also used as `plan_id`, matching
  `_PLAN_ID`'s regex), and `plan_path = plan_dir / f"edge-plan-{run_id}.json"`.
- `KafkaEdgePlanner(schema, kafka_bootstrap, settings, plan_id=run_id,
  plan_path=plan_path).finish(plan_path)` -> `(plan, digest)`. If
  `plan.chunks` is empty, returns a `PlannedRunResult` with an empty ledger
  and 0 attempts (nothing to dispatch) instead of starting a dispatcher for
  nothing.
- Dispatcher on its own asyncio-event-loop thread (the `_Server`/
  `_RunningServer` pattern from `tests/test_planned_dispatcher.py` and
  `tests/test_lease_server.py`): `PlannedDispatcher(plan, digest,
  ready_labels=plan.resources.labels)` (every node was already seeded by
  the caller -- Decision 5 -- so every label is ready from the start; no
  node-phase gating is exercised by this harness).
- One `ApocLockedEdgeWriter` per plan edge type, shared by every worker
  thread, built exactly as `planned_writer.main()` builds them.
- Per writer thread: its own `confluent_kafka.Consumer`
  (`planned_consumer_config(kafka_bootstrap, f"graph-loader-planned-harness-
  {run_id}-{writer_id}", max_message_bytes)`), its own `ChunkFetcher`, its
  own `DispatcherClient(address, digest=digest, writer_id=writer_id)`, and
  its own `PlannedEdgeWriterWorker`. Run every worker's `.run()` in a
  `ThreadPoolExecutor(max_workers=writers)`; a nonzero return from any
  worker raises `PlannedHarnessError` (with the writer id) instead of
  "silently returns a partial result" (Story 3's own AC).
- **Attempt capture.** A dedicated `logging.Handler` (not the caller's
  `caplog`, so the harness has no pytest dependency and every stress test
  in Stories 4-9 doesn't have to remember to configure a logger level
  itself) is attached to `logging.getLogger("src.loader.planned_writer")`
  for the duration of the run; it parses the `attempts=%s` field off every
  `"stage=edge-write "` INFO line (`record.args[-1]`, matching
  `planned_writer.py`'s own positional log call) and is detached (and the
  logger's previous level restored) in a `finally`, even on failure.
  `total_attempts_over_one = sum(max(0, attempts - 1) for attempts in
  captured)`.
- **Verification.** After every worker thread is joined (success or
  raise), a CONTROL `DispatcherClient(address, digest=digest)` (no
  `writer_id`) calls `.summary()`; `src.orchestrator.planned_edges.
  verify_ledger(plan, summary)` is called and its result is not swallowed
  -- any `PlannedEdgeError` propagates.
- **Cleanup (`finally`, unconditional, matching Story 3's own AC on leaked
  resources).** Close every worker's `DispatcherClient`/`ChunkFetcher`/
  `Consumer`; close the CONTROL client; `asyncio.run_coroutine_threadsafe(
  dispatcher.close(), loop).result(10)`; stop and join the event-loop
  thread; `planner.close(delete_topic=True)` (deletes the run's chunk
  topic); detach the attempts-capturing log handler.
- Never imports or calls `src.orchestrator.docker_service`; never starts a
  Docker container beyond the two the `neo4j_driver`/`kafka_bootstrap`
  fixtures already own (Story 3's own AC).

## `tests/test_planned_harness.py`

`@pytest.mark.integration`, small scale (a few hundred rows), using a
non-hub schema built from `tests.stress.star_topology_data.schema_dict(
"target_hub", run_id)` at `hub_edges` low enough that no hub is created
(e.g. 200 rows, well under `DEFAULT_HUB_DEGREE`):
- Seed nodes via direct `UNWIND ... MERGE` (Decision 5), publish edges via
  `star_topology_data.publish`, run `run_planned_mode`, compare the
  resulting graph against `star_topology_data.expected_graph` exactly (AC).
- A deliberately broken writer (monkeypatched to have one
  `PlannedEdgeWriterWorker` instance's `writers` mapping point at a stub
  writer whose `write_rows` raises) proves `run_planned_mode` raises rather
  than returning a partial result (AC).
- Calling `run_planned_mode` twice in the same test process (two different
  `run_id`s, same Kafka/Neo4j containers) and asserting Kafka has no
  leftover chunk topic after either call, via
  `admin.list_topics().topics` (AC).
- Asserts no test here ever imports/touches `src.orchestrator.docker_service`
  (a static check: `"docker_service" not in sys.modules` after import, or
  simply the absence of any such import in the harness source) (AC).
