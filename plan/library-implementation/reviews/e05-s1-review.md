# E05.F1.S1 independent implementation review — 2026-10-21

**Verdict: accepted for S1. No actionable finding.**

This separately initialized review inspected the actual S1 delta at HEAD
`14fe4934976f62f421086e541e6bc00dbc1ceec8`, the execution plan, handoff,
canonical E05 story and plan review. It did not inherit the implementation
author's findings. Scope is the installed command/version story only; resource,
writable-state and final E05 gates remain open. No product source was edited,
private settings read, service contacted, or delivery/publication performed.

## Source assessment

The sole `project.scripts` entry resolves to the existing
`neo4j_graph_loader.cli:main(args_list=None)`. The new package `__main__` has a
guard, so importing it during installed module inventory remains inert. The
parser uses the stable `graph-loader` name and reads the existing literal version
authority. Existing subcommand dispatch, required arguments and parser exit
codes remain unchanged. No dotenv loading or process environment mutation was
added.

The manifest and strict validator deliberately admit exactly the new entry-point
metadata. Both archive formats require its exact content; the sdist additionally
requires the full finite egg-info set. Runtime module byte checks, dependency,
license, public readme and private-file exclusions remain intact. The capture
allowlist includes the new test, while its existing package subtree includes the
new `__main__.py` without widening secret/data inclusion.

## Independently executed checks

- Fresh dev CPython 3.12 ran `tests/test_installed_cli.py`,
  `tests/test_namespace_migration.py` and `tests/test_planner_imports.py`:
  **17 passed in 7.51s**, no skips or warnings.
- Ruff passed for both touched runtime modules, the new test and both touched
  verification tools. `git diff --check` passed.
- Re-executed strict archive inspection on original and no-Git rebuilt wheels,
  comparing their members and bytes directly: **53 runtime modules**, all
  members equal. Both wheels have SHA-256
  `576da25a5340e61fb80ac66c95667d53db1c9a407de9b2601a9b86e551e77468`.
- Independently exercised **54 guarded installed invocations** in the author's
  two clean, external, noneditable environments. For the actual console script
  and both module forms, help/version, no arguments, unknown command, missing
  required start arguments and each existing subcommand's help had identical
  stdout/stderr and expected exits. Socket connection/bind/listen, process and
  thread creation were forbidden in every child. A caller `.env` sentinel stayed
  unloaded and the process environment remained unchanged. Version was exactly
  `graph-loader 0.1.0a1.dev0`.
- Independently removed `entry_points.txt` from each wheel and from the sdist:
  **all three omission controls were rejected** by the strict validator.

Independent detailed results are retained in owned ignored
`var/e05-validation-20261021/s1/independent-review-results.json`. The review also
inspected the completed author evidence: focused **139 passed in 43.80s**,
isolated builds, strict Twine, no-Git rebuild, exact archive inspection, clean
site-packages origins, mandatory dependency-only installs, inert module inventory,
public API examples and three-form entry parity in
`final-artifact-results.json` and `final-artifacts-commands.json`.

## Reviewed input identities

SHA-256 values bind this acceptance to the reviewed S1 files; later stories must
receive their own review and the full delta still requires final acceptance.

| File | SHA-256 |
| --- | --- |
| `pyproject.toml` | `7b55196fd04e80e79302c338f0ef386fee211d02a6279f1d9191fa89dea52baf` |
| `MANIFEST.in` | `678b8af94717bace27e5fdece3c6ec015d01efb7795d308544c3932556f408b8` |
| `src/neo4j_graph_loader/cli.py` | `3da80761ab089b2b13a3cc60f5d77daed6bdf928dc4a754d90c8be81ac27c07f` |
| `src/neo4j_graph_loader/__main__.py` | `2b4c81c50a4eb397b47e80df991ce4436ebae6f9d200ce6b4c2b7a9dd1503c40` |
| `tests/test_installed_cli.py` | `eadaea44f5b0a4e037ba2d521cca78474131728f69a88a7d6e7ded7e9c5af1cf` |
| `plan/library-implementation/tools/verify_e03_artifacts.py` | `c37bd641649ebc4325b2ddd2ba20ec2731ef2d3f8de70834ba916400ec3a80ac` |
| `plan/library-implementation/tools/capture_baseline.py` | `b569f5e1378dff0635f293bd2a1f0507c1dfc4cf1ce34df2c7be454df74cda98` |
