# E04.F2.S2 independent import-effects review

Verdict: **accepted after one documentation repair**. No remaining actionable
S2 finding identified. This accepts the story's revised source/tests/docs, not
whole-E04 completion, final artifact/image/B3 proof, or E08 platform acceptance.

Review inspected the actual dirty/untracked working tree at HEAD
`98cd267db5c70d3517a89ec68c1a4cedf2c254fd`, independently of prior review labels.
Read `handoff-e04.md`, `execution-e04.md`, canonical E04 and the cancellation
contract. No applicable AGENTS.md was found. No implementation/test edits were
made by this reviewer. No `.env`, remote IMDb, Docker or foreign resources used.

## F1 — P2: documented mapping default did not match the planner factory

Original location: `docs/LIBRARY_API.md:42–46`. Immediately after recommending
`producer_edge_planner`, the guide said mapping inputs default to synchronous
inline execution. The factory actually preserves `context=None`, and
`ParallelEdgePlanner.start()` chooses spawn; only `plan_and_publish_edges`
automatically selects inline execution for mappings. This matters for generators
and other caller-local iterables, and for which cancellation limitations apply.

Independently reproduced using the installed development interpreter, the social
schema, an empty generator in `{'VISITED': generator}`, default factory options
and a temporary owned plan path. No broker was contacted:

```text
factory_context: None
mapping_generator_start: TypeError cannot pickle 'generator' object
```

The failed start and temporary resources were closed and cleaned. The author
repaired the guide during review. Revised lines 43–48 explicitly limit automatic
mapping-to-inline selection to `plan_and_publish_edges`, state the factory's
spawn default even for mappings, and explain `context="inline"` for caller-local
iterables/generators. Independently rechecked the actual factory contexts and
convenience selection (with only `finish` replaced to observe its selected
context, `create_topic=False` and a temporary owned path): default factory context
is `None`, explicit inline is `"inline"`, and the convenience mapping default is
`"inline"`. The revised wording matches implementation. **F1 closed.**

## Implementation and test assessment

`ParallelEdgePlanner` now constructs `GraphLoaderMetrics()` for an omitted
metrics argument; this creates a private `CollectorRegistry`, without starting a
server or installing global listeners. Explicit supplied metrics ownership is
preserved. Internal `KafkaEdgePlanner` still selects its process metrics and
passes them explicitly into the underlying planner, preserving CLI aggregation.
No runtime dependency bounds/extras changed, no lazy optional dependency promise
was introduced, and no worker/fleet/retry API was added to the public planning
exports. Existing approved schema configuration exports remain unchanged.

The fresh `-I` import fixture snapshots environment values without printing
them, all 29 catchable signal handlers, threads, process start method, root
logging and an existing configured caller logger. It traps Python socket
creation/connect/bind/listen, subprocess/process/thread starts, signal and
start-method setters and common logging setup entry points. Public schema,
planning and CLI imports preserve these snapshots and leave `_process_metrics`
uninitialized. Independently reran the fixture with CLI imported first as well:
exit 0, one thread, 29 handlers, metrics uninitialized. Actual import paths were
also inspected; no hidden setup call was identified.

Repeated successful public calls assert independent registry identity and
counters, unchanged one-row results, and no process singleton initialization.
The self-contained public fixture exercises schema success/error with cause,
planning success/input error, approved cancellation/timeout errors and reclaimed
lock/spill state. Tests use public imports for the behavior under review.
Existing CLI shutdown/stop-readiness and metrics regressions remain in the
focused suite rather than being replaced by these new probes.

The artifact validator retains its strict dynamic runtime inventory, per-member
source equality, metadata/dependency/no-extras checks and sdist leak allowlist.
Its additions execute the two self-contained fixtures using `-I -c` in the
selected installed interpreter/cwd, retain code hashes/results and do not add
source-path rescue. The existing origin probe still checks every runtime module
against clean site-packages. This is source review of the validator; final
rebuilt wheel/sdist/image execution remains the subsequent whole-E04 gate.

## Independent validation

Interpreter: `var/e04-validation-20261018/dev/bin/python`.

```text
python -m pytest tests/test_planner_imports.py tests/test_cli_shutdown.py tests/test_stop_signal_ready.py tests/test_metrics.py tests/test_public_api.py tests/test_public_lifecycle.py tests/test_public_cleanup.py -q -rs
142 passed in 55.16s
```

Exit 0; no skips or warnings reported. Independently executed the mapping
generator failure reproduction, the revised context distinction control, and
the CLI-first fresh isolated import probe described above; all expected
assertions passed. Author's separate focused log reports 142 passes in 55.31s;
acceptance here is based on the independent rerun and code inspection.

Reviewed final SHA-256 identities:

- `src/neo4j_graph_loader/loader/edge_planner.py`: `e9dca64614546e35a244b8b1a03fbbca32e8bebb35a41ded367eca174bd1980b`
- `tests/test_planner_imports.py`: `30a702a0a6692c4e43c587cb6e343307c503f9b8c44d5bb4d3ee01c60e9a60ea`
- `tests/fixtures/e04_imports.py`: `3665a0227b67167c5aa34c1ee0af0bd6f6d1e5dd725b86d73aabffe6c41c90f9`
- `tests/fixtures/e04_public_example.py`: `6453960c629ffd20a630fc057fcceb2f877118a9ca334168698af37078947c6a`
- `docs/LIBRARY_API.md`: `7cd357739c4facf05e1e0795ed359e0e6a93ae5aa58418edccc5a1a9c79c3757`
- `plan/library-implementation/tools/verify_e03_artifacts.py`: `f2ff08f66b148215bde76176d327c381b9060c509a89ea43bd6f9a1ed1d65d79`
