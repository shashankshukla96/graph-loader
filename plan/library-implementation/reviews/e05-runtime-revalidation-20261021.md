# E05 runtime and launcher revalidation — 2026-10-21

**Verdict: accepted for the reviewed runtime/lifecycle/launcher scope after independent re-review of the narrow repair. No open actionable finding remains in this scope.** The original low-priority error-handling finding and its reproductions remain below; no blocking runtime/lifecycle defect was found. This independent review inspected actual source changes and executed fresh controls. Earlier approval labels were not treated as proof.

Scope: current HEAD `0638322f3b84471f63ee867861fb75b13c97760b`, E05 implementation `2265e4fd1d9f76eb2813856157dea89a80b9f216`, baseline `14fe4934976f62f421086e541e6bc00dbc1ceec8`. Working tree was clean when review began. No applicable AGENTS.md was found in the repository's ancestor chain. Read the canonical E05, execution-e05, E06 handoff, CLI_RUNTIME documentation, actual `_runtime.py`/CLI/DockerService/planned-edge/metrics/retry changes, and focused tests. Full suite, artifact/image identity, and broader delivery verification belong to the parent review.

## R1 — P3, closed after independent repair re-review: catch symlink-resolution failures at runtime preflight

Actionable locations: `src/neo4j_graph_loader/_runtime.py:21` and `:71`; runtime error boundary `src/neo4j_graph_loader/cli.py:3607`.

On the supported CPython 3.12 interpreter, `Path.resolve()` raises `RuntimeError` for a symlink loop. Runtime preparation resolves caller directory paths and the external plan path, but `main()` catches only `OSError`, `ValueError`, and `SchemaLoadError`. Consequently `--run-dir`, `--plan-dir`, `--plan-spill-dir`, `GRAPH_LOADER_TIMING_DIR`, and `--edge-plan` all let a loop error escape the public `main()` boundary. Ordinary invalid paths return a logged `stage=runtime ... reason=...` failure; these paths produce a Python traceback instead. Schema loading already explicitly handles the same `RuntimeError` category at `utils/schema_loader.py:64`.

Minimal independent reproduction from repository root, using the existing development interpreter:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from neo4j_graph_loader import cli

with TemporaryDirectory(prefix="e05-loop-repro-") as temporary:
    loop = Path(temporary) / "loop"
    loop.symlink_to(loop)
    with patch.object(cli, "handle_start", side_effect=AssertionError("dispatch")):
        result = cli.main(["start", "--mode", "bulk", "--run-dir", str(loop)])
        assert result == 1
```

Run the snippet with `var/e05-validation-20261021/dev/bin/python`. Original, pre-repair result: uncaught `RuntimeError: Symlink loop from ...`; the assertion was never reached. After the repair the same call returns 1. Replace the directory flag with `--plan-dir` or `--plan-spill-dir`, or pass the loop through `GRAPH_LOADER_TIMING_DIR` / `--edge-plan`, for the same result. Actual `python -m neo4j_graph_loader start ...` subprocesses were also exercised from an unrelated temporary directory: all five exit **1**, with a traceback and without the normal runtime `reason` diagnostic.

Severity is **P3** because the numerical process exit status remains 1, all cases fail before ingestion, no service was contacted, and no foreign file was modified. The difference affects predictable public-main error handling and CLI diagnostics, not ingestion safety or a successful run. Normalize resolution `RuntimeError` to an existing handled error near the resolution sites, or explicitly handle this category at the runtime preflight boundary. A focused negative test should cover both directory overrides and external input resolution.

Raw results: `var/e05-runtime-revalidation-20261021/probe-results.json`, `cli-results.json`, and five `cli-*.stderr` files. `probe.py` includes the controlled public-main reproductions. The timing subprocess case additionally set only `GRAPH_LOADER_TIMING_DIR` to the owned loop; that environment override is represented by its case name and documented here.

## Repair re-review and closure

The parent added private `_resolve_path` in `_runtime.py`, wrapping only `Path.resolve()`'s `RuntimeError` as `ValueError` with the original cause. Directory preflight and external plan resolution use it. The existing main boundary and ingestion/cleanup behavior were not broadened. Five public-main regressions use real self-referential links, forbid handler/Docker dispatch, require the runtime diagnostic to contain the caller path, and retain the link. The final diagnostic assertion does not depend on a Python/platform-specific error phrase.

The parent retained the pre-fix red test run: all five new cases failed in 0.44s, `var/e05-revalidation-20261021/red.log`; that output was independently inspected. The reviewer read the actual patch and independently reran the final **35 runtime tests**, all passing in **0.33s**, no skips/warnings. Original custom controls were rerun after the repair: all five loops now return 1 before dispatch, all 21 output blockers remain rejected without sentinel changes, eight concurrent runs remain isolated, and 14 launcher calls retain their mount/environment contracts.

Five actual package-module subprocesses from unrelated cwd now exit 1 with a `stage=runtime ... reason=...` diagnostic and no traceback, preserving the caller's symlink. Direct helper controls additionally prove the original `RuntimeError` is preserved as `ValueError.__cause__`, ordinary dangling directory symlinks still resolve and permit owned target creation, and unrelated `PermissionError`/`OSError` instances pass through unchanged. These results support closing R1.

The repaired product/test bytes reviewed here are recorded in `var/e05-runtime-revalidation-20261021/repair-reviewed-inputs.json`. The result binds the current uncommitted repair on the HEAD stated above; parent delivery verification owns any later commit/artifact identity claim. Repair evidence is in `repair-probe-results.json`, `repair-preflight-results.json`, `repair-cause-results.json`, `repair-cli-results.json`, five `repair-cli-*.stderr` files, and `repair-runtime-tests.log`, with executable probes alongside them. An initial cause-probe assertion compared macOS `/var` against its canonical `/private/var` spelling; the reviewer corrected only the probe's base normalization and the rerun passed.

## Fresh executed validation

Interpreter: `/Users/shashank/Desktop/Code/Graph/graph-loader/var/e05-validation-20261021/dev/bin/python`, discovered from committed `e05-results.json`.

- Focused runtime/resources/Docker/planned-phase/shutdown/environment/metrics suite: **216 passed in 48.01s**, no skips or warnings. Invocation: `python -m pytest tests/test_runtime_paths.py tests/test_cli_resources.py tests/test_docker_service.py tests/test_planned_edges_cli.py tests/test_cli_shutdown.py tests/test_cli_env_polish.py tests/test_metrics.py -q -rs`.
- Additional CLI/default-mode/schema-topic/DLQ/installed-entry suite: **449 passed in 44.03s**, no skips or warnings. Invocation: `python -m pytest tests/test_cli.py tests/test_auto_settings.py tests/test_cli_dead_letter.py tests/test_cli_topic_preflight.py tests/test_installed_cli.py -q -rs`. Combined with the separate suite above: **665 tests passed**.
- Independent eight-call concurrent public-main probe with the same run identity: eight distinct private roots, root permissions 0700, schema copies 0600, unchanged process environment.
- Independent 14 launcher calls, covering all seven roles on two runtime instances: absolute instance-owned bind sources; schema/plan read-only; timing/rejection outputs read-write; container-visible snapshot paths under `/app/write-timings`; changed ambient timing environment did not redirect either instance or create the ambient directory.
- Independent **21** invalid-output cases: directory, symlink, and read-only blockers across plan, ledger, hidden ledger temporary, progress, hidden progress temporary, failure marker, and CLI metrics output. Every case returned 1 before dispatch and retained sentinel content/type/link. Native read-only permission probes ran as the ordinary non-root workstation user.
- Independently reproduced all five symlink-loop failures through both controlled `main()` calls and actual package-module subprocesses, as described in R1.

The review checked timing/snapshot threading, planner spill/rejection defaults, explicit output priority, private schema copying, resource lifetime enclosing handler cleanup, per-instance Docker state, cleanup ordering, and persistent failure diagnostics. External plan symlink handling uses consistent resolved paths in preparation, handler validation, and planned execution; an initial suspicion of a ledger-location mismatch was disproved by reading those call sites and is not a finding. Schema node/edge identifiers are validated before they become timing filename components.

Fresh raw controls live only under the new ignored `var/e05-runtime-revalidation-20261021/` directory. A first launcher-probe attempt omitted its required owned plan fixture and failed before the dispatcher launch; its original `probe-first-attempt.log` is retained, the fixture was corrected, and the complete rerun passed. This was a reviewer harness failure, not a product finding.

## Limits and preservation

The reviewer made no product edits, commits, pushes, private `.env` reads, remote IMDb access, or cleanup of other owners' files/resources occurred. Only owned temporary test paths were removed. All Docker calls in these independent controls used recording clients; no live service ingestion or new container/rootfs proof is claimed. Existing checked-in image evidence is not counted as an independently executed control here. The parent review handles complete-suite/artifact verification.
