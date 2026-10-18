# Independent E03.F1.S1 runtime review

Reviewed 2026-10-17/18 against branch `codex/pypi-library-checklist`, HEAD
`6de3562083cc7079091d056b89d66f18b69931e8`, including the required preexisting
dirty/untracked E00–E02 work and the current untracked migrated implementation.
Read the handoff, execution plan, E03 acceptance criteria and decision register;
inspected actual current files and entry backup rather than relying on status
labels. No product files were edited by this reviewer.

**Result: no actionable findings in E03.F1.S1.** The atomic namespace migration,
development bootstrap and existing Docker launch paths satisfy the reviewed
story scope. This does not approve publication or future E04–E12 behavior.

## Independent checks

- Compared all 49 current runtime module ASTs against the actual safe entry
  archive `/tmp/graph-loader-e03-work/entry-source.tar.gz`, applying explicit
  `src.*` → `neo4j_graph_loader.*` import/string substitutions, corresponding
  source-path text substitutions, the relative metrics version import and the
  top-level package docstring change only. All 49 matched. The existing literal
  `_version.py` is unchanged. No algorithm, schema/default, Kafka group/key,
  plan/chunk or DLQ format alteration was found.
- Independently compared entry/current test, example and script ASTs after the
  mechanical namespace substitutions. The remaining changes were removal of
  application-only example path insertions and their unused `sys` imports,
  updating the metrics-test description, and updating the two lease guards and
  outage guard to scan the migrated runtime root. Assertions were preserved.
- Ran with `/tmp/graph-loader-e03-work/dev/bin/python`:
  `-m pytest tests/test_namespace_migration.py tests/test_external_planner.py
  tests/test_docker_service.py tests/test_lease_server.py
  tests/test_stream_outage_resilience.py tests/test_metrics.py
  examples/imdb/tests/test_planned_edges.py
  examples/imdb/tests/test_publish_and_load_nodes.py
  -m "not integration and not smoke" -q -rs`.
  **335 passed in 18.27s**, four existing macOS fork warnings, no skips.
  This exercises canonical spawn/pickle class identity, real planner process
  execution, dynamic patch targets, launch arguments, runtime source guards
  and the executable publisher/planner helpers.
- Ran a second lane:
  `-m pytest tests/test_distribution_foundation.py
  tests/test_library_preflight.py tests/test_node_loader_early_stop.py
  tests/test_dlq_proof_harness.py tests/test_single_pair_compat.py
  tests/test_single_topic_compat.py tests/test_single_pair_multi_topic_compat.py
  -q -rs`. **67 passed in 1.28s**, no skips. This includes import lifecycle,
  metrics/version authority, shutdown during heavy imports and compatibility
  golden comparisons. `pip check`, requirements parity and `git diff --check`
  also passed.
- During review, the implementer hardened the isolated namespace-test child to
  remove `GRAPH_LOADER_VERSION`, ensuring the default-version assertion is
  independent of the reviewer's deployment override. Inspected that test-only
  change and reran `GRAPH_LOADER_VERSION=review-deployment-override
  /tmp/graph-loader-e03-work/dev/bin/python -m pytest
  tests/test_namespace_migration.py -q -rs`: **7 passed in 0.70s**. The README's
  stale E02 scaffold row was also corrected. Neither change alters runtime
  source or the image/source identities above; the final full suite is still
  the implementer's subsequent freeze gate.
- Inspected `pyproject.toml`, the documented fresh editable-install instructions,
  helper-only path insertions, every Dockerfile and DockerService's explicit
  lease/dispatcher/writer/retry module commands. There is no runtime path rescue
  or generic `src` compatibility package.

## Independent image probes

Used the immutable image IDs recorded in `e03-images-build.json`, without
rebuilding or modifying them:

| Image | ID |
| --- | --- |
| CLI | `sha256:fd3a00bffc3f612aef98fdf6d22f08eb2d66ce3a62930a30627d8e1a3eb60b53` |
| Node | `sha256:828c34279e161f193d9ddad4f5f7e00fc5ae3ad6d9859c16499097efe21577c0` |
| Edge | `sha256:77ec74dd246176590e48fa780f98c80201684b13be57d8775ea9583618f8792a` |
| Clock | `sha256:45ef1d0fa82809109289c86fa3bc025d6b2ea89f7155949f5b5e4e09d281a27b` |

Every probe used `docker run --rm --network none --workdir /tmp`, no source
mounts, and `--entrypoint python <id> -I` for module checks. In each image:
all 49 runtime modules imported from that interpreter's site-packages and their
SHA-256 bytes matched current checkout source; metadata version was
`0.1.0a1.dev0`; `src.cli` was rejected; all eight module helps (CLI, node, edge,
retry, planned writer, dispatcher, lease, clock) exited 0 and emitted usage;
the unmodified default entrypoint also accepted `--help`. Thus **32 independent
module helps and four default entrypoint helps passed**.

The reviewed runtime inventory identity is
`6e97765595b5046b4d2c8163490371abfd84cefd107aefeab1e7f656041e3121`, SHA-256 of
compact sorted-key JSON for path-sorted `{path, sha256}` records over all 49
`src/neo4j_graph_loader/**/*.py` files. Changes to those bytes require rechecking
the image/source relationship.

## Evidence and residual classification

Inspected the complete full-suite log, whose SHA-256
`b1d664696c51a7b80e3d878589cbcb2b58961ff625ebe88a5503cca678806c74` matches the
record: 3,387 passed, four inherited skips, 49 deselected and 19 fork warnings.
Did not rerun that entire suite during this focused review.

Inspected the actual supervised B3 log (SHA-256
`d404c11f7b87144ee262d38e1ce27362bc0c5352105931c61541dece51833277`): the exact
test was collected and passed in 22.29s using the fresh development interpreter,
with no skip. Inspected the current test's real migrated retry subprocess,
dataset, exact final property/offset/drain assertions and zero-lock-wait proof,
plus supervisor interpreter selection and bounded owned-process-group cleanup.
These assertions match the entry implementation after namespace replacement.
B3 was not rerun by this reviewer; the supplied live proof is directly checked
evidence, separate from the reviewer's independently rerun unit/process lanes.

Fresh searches across executable runtime/tests/examples/scripts/Dockerfiles
found no missed legacy import, patch or module launch target. Remaining old
namespace mentions are intentional rejection tests/migration prose and
historical Git slice references. Remaining `src/` occurrences include the
physical source root, comments and historical documentation; none supplies an
old runtime import. Repository-root helper paths resolve `examples` or test
helpers, while the migrated application requires installation.

The historical Git guards' unavailable commits, platform-specific shell skip,
intentional schema example skip and existing fork warnings remain baseline
limitations. Clean wheel/sdist reproduction belongs to the separately reviewed
E03.F2.S2 gate. The broader Linux architecture/start-method/service support
matrix and public API/resource/console redesign remain future epics; no new
missing coverage blocks this mechanical migration story.
