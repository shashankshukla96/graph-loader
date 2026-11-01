# Public schema and producer planning API (development alpha)

The explicit supported modules are `neo4j_graph_loader.schema` and
`neo4j_graph_loader.planning`. The package root exports the version only.
Schema/planning class reexports preserve the canonical existing class identity
and spawn/pickle imports. There is no generic `src` shim. Fleet, retry, index,
metrics, worker/transport and test dependency-injection hooks remain internal.
All declared runtime dependencies remain mandatory; there are no extras.

`schema` exports `load_schema`, `GraphSchema`, `LoadingConfig`, `NodeConfig`,
`EdgeConfig`, `PropertyConfig`, `SourceTargetConfig`, `DeadLetterConfig`,
`RetryConfig`, `SchemaLoadError`, `MultiTopicEdgeError`, `MultiPairEdgeError`.
`load_schema(str | os.PathLike) -> GraphSchema` wraps missing/read/decode/YAML/
root/model errors in `SchemaLoadError` with an inspectable cause. Direct model
construction/validation retains Pydantic `ValidationError`. Schema meaning and
stored plan/chunk/DLQ formats are unchanged.

```python
from neo4j_graph_loader.schema import load_schema
from neo4j_graph_loader.planning import PlanSettings, plan_and_publish_edges

schema = load_schema('my-graph.yaml')
result = plan_and_publish_edges(
    schema, {'VISITED': []}, bootstrap_servers='localhost:9092',
    plan_path='run/edge-plan.json', settings=PlanSettings(topic='run-chunks'),
)
print(result.path, result.digest, result.plan.total_rows)
```

Producer planning **requires Kafka**, even for in-memory sources: it publishes
chunks and records their delivered locations. The schema's edge names must match
source names; the sample name above must exist in the supplied schema.
`PlanResult` contains the `EdgePlan`, digest/path, scan/hub/emit/total timings,
rows and per-lane summaries. `PlanSettings.validate()` rejects invalid settings
with `ValueError`; source binding/record/plan problems use `EdgePlanError` or
`EdgePlanInputError`, and ordinary client/source failure diagnostics are retained.

`planning` exports `PlanSettings`, `EdgeRecordSource`, `IterableEdgeSource`,
`ParallelEdgePlanner`, `PlanResult`, `EdgePlan`, `producer_edge_planner`,
`plan_and_publish_edges`, `EdgePlanError`, `EdgePlanInputError`,
`EdgePlanCancelledError`, `EdgePlanTimeoutError`.
Construct planners through `producer_edge_planner`; use the returned canonical
`ParallelEdgePlanner` lifecycle. With `plan_and_publish_edges`, mapping `edge_type -> iterable` inputs default
to synchronous inline execution. The `producer_edge_planner` factory defaults
to spawn even for mappings; pass `context="inline"` there for caller-local
iterables/generators. Explicit concrete `EdgeRecordSource` sequences also default
to spawn. Process sources/subclasses must be picklable and defined in importable
modules. `EdgeRecordSource`
is a concrete base class with `records(edge)` and `describe()`, not a structural
Protocol. Each returning row is a canonical mapping/JSON str/bytes, or a prepared
`(source_key, target_key, record_json)` tuple. Prepared payload validation still
finishes in writers. For multi-pair types, use sources naming their `topic`;
a plain type mapping cannot identify the pair. Ordering follows source order
and schema rules; no restart-safe arrival or exactly-once guarantee is added.

The installed console script and bundled default/social resources are available; existing module
commands still work with external configuration. This is an alpha (`0.1.0a1`); the supported environment is in
[Support and limits](../README.md#support-and-limits). Cancellation and cleanup behavior is described below.

## Ownership and lifecycle

Use `producer_edge_planner(schema, sources, settings, broker, plan_path=...)` to
obtain the canonical planner. `start()` is idempotent while active and begins
planning; `finish(path)` starts it if needed, returns one result and closes it.
A completed/closed planner cannot restart or finish again; `close()` is idempotent.
The configured finish path cannot change. Calls to `start`/`finish`/`close` on
one instance must not overlap across threads. Independent instances may run
concurrently using distinct output paths. Explicit ownership is:

```python
from neo4j_graph_loader.planning import producer_edge_planner

planner = producer_edge_planner(schema, sources, settings, broker,
                                plan_path='run/edge-plan.json')
try:
    planner.start()
    result = planner.finish('run/edge-plan.json')
finally:
    planner.close()
```

This factory does **not** create/delete the chunk topic: the caller provisions
and owns it. The convenience `plan_and_publish_edges(create_topic=True)` owns
the topic it creates, retains it on success for loaders, and attempts deletion
on failure after workers have stopped. If cleanup cannot establish that, the
owned topic is retained and named in the error log. With `create_topic=False`, it never deletes the caller's topic.
Deletion failures are logged without replacing the original planning error;
topic/admin operations have their own timeouts and are outside coordinator
cancellation. A retained success topic needs explicit caller cleanup after use.

The planner exclusively creates `<plan>.lock` before clearing stale result,
`.failed` and `.progress` files. Other attempts cannot overwrite a live owner's
output or evidence. Never remove a lock based only on a PID: confirm its owner
has stopped before manual recovery. Owned spill directories are unique even
for identical plan IDs. Successful cleanup releases the lock/spill/progress;
source/startup errors write `<plan>.failed` while output ownership is held.
The original failure cause is retained inline; process failures retain remote
type/message/traceback diagnostics as a local error cause, without deserializing
arbitrary remote exception objects. Once a worker failure reply is received, it
is raised immediately with that cause and enters shared cleanup; later replies
or a following timeout/cancellation check cannot replace the observed error.
Receives still follow lane order: an error behind an earlier blocked lane may
remain unobserved until that receive returns or the operation is interrupted.
A wrong finish path leaves the owner active.
Lifecycle/ownership alone does not promise interruption of blocked caller code.

## Cancellation and one operation clock

The three public planner interfaces accept keyword-only `cancel_event=None` and
`timeout_seconds=None`. The signal is caller-owned and supplies a nonblocking
`is_set()` method; another thread may set it without overlapping lifecycle calls.
Timeouts must be positive finite seconds (booleans/zero/nonfinite values reject
before client side effects). `EdgePlanCancelledError` and `EdgePlanTimeoutError`
are explicit planning exports under `EdgePlanError`. If both apply, cancellation
is observed first. Ordinary inline source errors retain their original cause;
process errors retain remote diagnostic causes without arbitrary error unpickling.

One absolute monotonic deadline covers all scan/hub/emit/result phases, never
one timer per wait. Process execution anchors after initial source serialization/
submission completes; a gap between `start()` and `finish()` counts. Inline mode
anchors immediately before its first source iteration and checks between
returning records. Cancellation/deadline checks cover complete-frame header/body
reads, later command writes, codec and phase transitions; parent selector slices
are at most100ms. Large partial replies and full send buffers cannot turn a
readiness result into an unbounded receive/send. Codec/frame limits are64MiB
frames and aggregate blob bytes,1MiB scalar,32MiB numeric counter,1M nodes/depth64;
unsupported/oversized/corrupt/truncated values fail explicitly. No receiver
helper thread/process is used.

This is cooperative coordinator interruption, not CPU or OS preemption. Initial
source construction/pickling/submission, Kafka topic/admin/client callbacks,
arbitrary blocking inline `records()`/`next()` and OS-uninterruptible work retain
their own timeouts/limits. Deadline checks resume after returning callbacks and
the elapsed budget is not reset. Numeric/codec/assembly computation is checked
at bounded traversal/operation boundaries, not preempted inside C functions.
Use picklable process sources to interrupt blocked `records()`; never rely on
inline mode to interrupt code that does not return. The tests of this behavior do not
cover every architecture, start method or worker count; the measured worker counts are 1-8, 16, 32 and 64.

## Cleanup budget and retained ownership

After coordinator observation, cleanup uses one separate aggregate 10-second
monotonic budget for all workers and heartbeat waits. Normal close gives stop
messages one shared 100ms window, then a shared grace period; failure signals all
workers first. Remaining children are terminated, then killed and joined under
the same final cutoff. No worker gets another ten seconds. Cleanup closes parent
endpoints even when delivery was interrupted and creates no receiver helper.
Initial submission, caller callbacks and OS-uninterruptible calls remain the
exclusions above; scheduling can add latency, so this is a supported-platform
cooperative target rather than a universal hard real-time promise.

If a child remains alive/unknown or the heartbeat cannot stop, cleanup raises
`EdgePlanError` (or adds a note to the original planning error), names its PID
and retained paths, and preserves the owned lock, spill and any owned topic.
The `.failed` marker retains diagnostics. The closed planner cannot retry cleanup
with another implicit budget. Confirm all owners have stopped before explicit
manual recovery; another planner will refuse the retained lock. Caller-owned
topics and signals are never reclaimed. Successful cleanup removes only the
planner's owned spill, progress and lock; failure markers remain inspectable.


## Imports and independent calls

Public and CLI imports do not configure caller signals, logging, environment,
process start methods, servers, client connections or threads. Public producer
planners use a fresh private metrics registry by default; repeated independent
calls do not initialize the internal process metrics singleton or share counters.
No metrics server is started by constructing or calling a public planner.
CLI process setup and its explicit internal metrics ownership remain at entry
functions. Use distinct plan paths/topics for independent runs; importing a
public module does not provision their resources. All runtime dependencies remain
mandatory; this boundary adds no optional extras or lazy dependency promise.
