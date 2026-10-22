# E05 distribution and command revalidation — 2026-10-21

**Disposition: scoped approval. No actionable E05 command/resource/distribution finding.**

Independently reviewed actual baseline `14fe493` → clean reviewed HEAD
`0638322f3b84471f63ee867861fb75b13c97760b` (implementation `2265e4f`), the E05 epic,
and execution acceptance criteria. Prior review conclusions were not used as
approval evidence. This report covers command entry, schemas, distribution closure,
installed behavior and preserved import behavior; the parent review owns the full
suite, rebuilding/Twine gates and service/image evidence.

## Code assessment

- `pyproject.toml:28` declares the sole `graph-loader = neo4j_graph_loader.cli:main`
  executable. `__main__.py:5` guards execution; importing that newly discovered module
  does not run the command. `main(args_list=None)` remains callable without arguments.
- `cli.py:3288–3290` stabilizes the program name and reports `_version.__version__`.
  It keeps argparse error status 2; schema/runtime failures use status 1.
  `cli.py:3292–3299` implements the bounded schema helpers and mutually exclusive
  caller-path/sample inputs. Existing modes and dispatch remain present.
- `pyproject.toml:40` and `MANIFEST.in:5–7` explicitly include exactly the approved
  canonical/social schemas plus console-entry metadata. The resources match the
  reviewed checkout sample bytes; no broader config/data inclusion was added.
- `_resources.py:10–16` preserves explicit caller paths and uses `files/as_file`
  for bundled schemas. `cli.py:3594–3611` holds materialization through dispatch
  and its cleanup. `schema-example` and validation failures are handled at
  `cli.py:3263–3281`; neither helper launches services.
- `verify_e03_artifacts.py:30–83` retains exact wheel member closure, rejects duplicate
  payload paths, checks source/resource bytes and dependencies/license metadata,
  requires both schemas in both formats, and checks the exact sole entry point.
  Required sdist egg-info members are now mandatory rather than optional.

## Fresh executed verification

All new output is under ignored `var/e05-distribution-revalidation-20261021/`.
No product source was edited, and no remote services were accessed.

1. Focused existing tests:
   `var/e05-validation-20261021/dev/bin/python -m pytest tests/test_installed_cli.py tests/test_cli_resources.py tests/test_cli.py tests/test_cli_env_polish.py tests/test_cli_topic_preflight.py -q -rs`
   → **151 passed in 43.62s**, no skips. Log: `focused-tests.log`.
2. Reran the actual artifact inspector against the recorded wheel and sdist,
   comparing their runtime/resource bytes to this HEAD. Wheel SHA-256
   `99a88f8b88aa5e80495e6089c74ff4e9e5c44a0e77cba288e55922531c7ca434`;
   sdist SHA-256 `3fe448722e6d15d3c674dfad6db840648846d5f48f3d23813f746d2d9555df59`.
3. Created **13 independent archive mutations** and observed rejection of each:
   omission and corruption of each schema in wheel and sdist (8), omission and
   corruption of entry-point metadata in both formats (4), and an extra unapproved
   wheel resource (1). Controls and assertions are reproducible with
   `audit_distribution.py`; results are in `results.json`.
4. Created an owned fresh environment and performed a real noneditable wheel
   installation with `pip --no-index --no-deps`. To avoid remote package access,
   copied runtime dependencies from the prior clean original installation; excluded
   its application package, entry metadata, development tools and `.pth` files.
   The application was installed afresh into the owned environment. Installation
   output is retained in `fresh-install.log`.
5. Enforced read-only modes on both the installed package and an unrelated working
   directory. Actual attempted writes to each failed with `PermissionError`, so
   this was effective native-user enforcement rather than an unchecked chmod claim.
   From that directory, **16 argument cases** matched stdout/stderr and expected
   status across executable, package module, and existing CLI module: help/version,
   missing/unknown commands, start help/missing mode, both schema examples, both
   bundled validations, external caller validation, invalid UTF-8, missing/directory
   schema inputs, invalid sample and mutually exclusive input flags.
6. Imported **all 55 runtime modules**, including `__main__`, in an isolated child
   with socket construction/connect/bind/listen, subprocess/thread/process startup,
   signal installation, multiprocessing setup, and logging setup forbidden.
   Environment, threads, start method, logger configuration and 29 signal handlers
   were unchanged; process metrics remained uninitialized.
7. Executed the installed `e05_runtime.py` fixture from the enforced read-only cwd
   and installation. Public-main preflight created two distinct private attempts;
   controlled launchers recorded all seven role invocations, absolute binds and
   schema/plan read-only versus output read-write modes. Invalid/unwritable paths
   failed before controlled dispatch; the working directory remained empty and
   process environment unchanged. Log: `installed-runtime.log`.
8. Imported directly from the real wheel ZIP and exercised actual temporary
   materialization of both schemas, validation and a controlled status handler.
   Paths existed through dispatch/finally and were reclaimed only afterward.
   Probe: `zip_resource_probe.py`; log: `zip-resource.log`.
9. Empty and nonempty `GRAPH_LOADER_VERSION` metric overrides did not change CLI
   version identity. Results: `version-override.json`. `git diff --check` passed.

## Limits

This approval does not independently rerun a network dependency resolution, build
new archives, validate wheel RECORD cryptographic integrity, or treat the archive
inspector as a general hostile-tar extraction security boundary. The fresh install
uses copied runtime dependencies as explicitly described above. Installed runtime
launches use controlled clients; this is not a live ingestion or image-selection
proof. E06 checkout-free image selection remains deferred as documented in
`docs/CLI_RUNTIME.md:60–65`. No existing `.env` was opened, no remote IMDb service
was touched, no foreign resources were removed, and no commit/push was performed.

## Addendum — independently verified symlink-loop repair

**Disposition: scoped approval of the repaired source and rebuilt distribution.**
No new actionable finding. This addendum supersedes only the initial report's
artifact identity for the narrowly repaired runtime file; the original review and
its evidence remain retained.

Reviewed the actual uncommitted repair relative to HEAD `0638322`: only
`src/neo4j_graph_loader/_runtime.py` changes in the product/package surface, plus
five parameterized regression cases in `tests/test_runtime_paths.py`. The helper at
`_runtime.py:20–25` normalizes the Python 3.12 `Path.resolve()` `RuntimeError` into
an actionable `ValueError` with the original exception chained. It is applied at
the existing writable-directory and explicit edge-plan resolution boundaries;
`cli.main` continues to handle the resulting error with its existing runtime
failure branch. The repair does not broaden a handler around dispatch.

Independently reran the real strict artifact inspector using this working source
against the new original wheel/sdist and the sdist-rebuilt wheel listed in
`var/e05-revalidation-20261021/repaired/artifacts.json`. Checked the actual
installed bytes of all 55 runtime modules and both resources in **both fresh
installations**, rather than accepting their recorded digests as proof. The
runtime source and both installed copies have SHA-256
`2f968ec88bc4abee8645ba4228bfdb98e3ebe3139abc2dd994592a6276c15969`.

The original and rebuilt wheel have identical inventories and every member's
bytes match. Both wheel SHA-256 values are
`8912919b1a9d1c74ea4fe0d83d56d23084a75958ff2a6e698269d89b93c385d1`;
the repaired sdist SHA-256 is
`85ed813597b6dd47450847adea9ab0a12c623b84c8fdd743a6d4f1643f244c06`.
Exact archive closure still passes, with the two approved schemas and sole
console command. Both installations resolve runtime code beneath their own
purelib, have noneditable metadata, no editable `.pth` artifacts, and none of the
checked development distributions (`pytest`, `testcontainers`, `pandas`,
`dotenv`, `build`, `twine`).

Executed new owned self-loop probes for all five path settings:
`--run-dir`, `--plan-dir`, `--plan-spill-dir`, `GRAPH_LOADER_TIMING_DIR`, and
`--edge-plan`. For each setting in each fresh installation:

- A fresh isolated child called actual `cli.main` with dispatch, Docker creation,
  socket construction/connect/bind, process startup and thread startup forbidden:
  **10/10 returned 1**, logged `stage=runtime` with the affected path, and did not
  reach ingestion.
- Invoked the real installed executable, `python -I -m neo4j_graph_loader`, and
  `python -I -m neo4j_graph_loader.cli` from the unrelated outside-checkout
  directory: **30/30 exited 1**, logged the path and runtime failure, and produced
  no traceback.
- Verified each owned self-loop remained a symlink to its original target.

Reproduction is retained at
`var/e05-distribution-revalidation-20261021/repaired_probe.py`; results are in
`repaired/results.json`, with individual main/command logs under each owned case
folder and a summary in `repaired-probes.log`. The first harness attempt used the
old artifact JSON shape and stopped with `KeyError` before installed probes; its
path lookup was corrected to the new `installed_probes` field. This was a harness
format adjustment, not a product failure.

This addendum does not substitute for the parent's final full suite, B3,
image rebuilds, or mixed-client matrix. It makes no new live-service or complete
checkout-free ingestion claim. No product file was changed by this reviewer;
no commit/push, existing `.env` access, remote service access, or foreign cleanup
was performed.
