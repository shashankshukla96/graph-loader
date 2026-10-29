# Independent E08 dependency-floor review — 2026-10-27

**Disposition: approve only `PyYAML>=6.0.1,<7` in place of
`PyYAML>=6.0,<7`, with regenerated runtime requirements and the matching minimum
constraint. No actionable floor-decision finding remains.** All other direct
floors, all eight mandatory dependencies, no extras and the Neo4j/Rust `<7`
fences remain as approved. This is not E08.F1.S1 acceptance, E08 completion or
support-matrix acceptance.

The reviewer started from a task-only brief with no preceding implementation
conversation. Reviewed D05 in [decisions](../decisions.md) and the
[decision brief](../decision-brief.md), canonical [E08](../epics/e08.md), the
[execution plan](../execution-e08.md), [plan review](e08-plan-review.md), current
`pyproject.toml`, generated `requirements.txt`, their generator and the actual
evidence below. No applicable AGENTS.md was found. Only this review was written;
no dependency metadata, constraints, application code or evidence was modified.
No environment-file access, tests, services, remote access, commit, push or
upload occurred.

## Evidence and decision

- The original macOS attempt is retained in
  `var/e08-validation-20261026/declared-minimum-probe.json` and
  `declared-minimum-resolution.log`. Its joint declared-minimum dry-run exits 1
  while preparing PyYAML 6.0's source-build requirements, with
  `AttributeError: 'build_ext' object has no attribute 'cython_sources'`.
- The original native Linux arm64 attempt is retained in
  `linux-declared-minimum.json` and `linux-declared-minimum.log` in that evidence
  tree. Its joint exact-minimum dry-run reaches the same PyYAML 6.0 default
  isolated-build failure and exits 1. These are actual failed resolver/build
  attempts, not successful installs or an inference from missing wheel names.
- `linux-candidate-minimum-v4/result.json`, `install.log` and `resolution.json`
  retain a real joint install in a newly created `/tmp/base` virtual environment
  with default `system_site_packages=False`. The recorded command uses isolated
  pip and the exact eight direct targets: Neo4j 5.20.0, Rust extension 5.20.0.0,
  Kafka 2.5.0, Pydantic 2.7.0, PyYAML 6.0.1, Docker 7.1.0, NumPy 1.26.0 and
  Prometheus client 0.20.0. It completes successfully, runs `pip check` with
  “No broken requirements found,” and prints the installed inventory using
  isolated Python. The report and inventory agree on CPython 3.12.14 and Linux
  aarch64; the receipt identifies native Linux arm64 execution and the fixed
  Python image digest. All eight direct versions match the proposal.
- The recorded SHA-256 values for both original failure logs and the successful
  v4 install log match the actual files. The v4 receipt exits 0 without timeout,
  records no leftover owned processes or cleanup errors, and records the removed
  container as absent afterward.

The only candidate dependency change is 6.0 to 6.0.1, the next patch release.
It removes the demonstrated default-install obstruction while preserving the
other direct minimum targets. The successful joint install and dependency check
support the smallest proposed correction; there is no evidence here that
justifies raising any other floor or changing the Neo4j/Rust pair. The v4 report
records Neo4j 5.20.0 as a source archive, and its log records the successful
`neo4j-5.20.0-py3-none-any.whl` build. That pure-Python source build is acceptable
and must stay explicit in the later finite wheelhouse/provenance evidence.

The earlier v2 attempt records an unavailable Docker daemon and is not a package
failure. The retained v3 log fails before dependency installation with permission
denied on `/tmp/base/bin/python`; v4 explicitly mounts executable temporary
storage and succeeds. No v3 `result.json` is present. Neither infrastructure
attempt is counted as candidate compatibility proof, and neither is relabeled
as a successful run.

## Limits and remaining work

Approval permits the narrow metadata correction, generated-requirements update
and minimum-constraint selection before continuing the reviewed E08 work. The
candidate install is dependency-install evidence: it does not install the project
wheel, execute public application probes, establish active acceleration, validate
application behavior or satisfy the frozen installed test lanes. The transient
dependencies resolved in v4 must be captured in the finite reviewed lane inputs;
this receipt is not a universal minimum-transitive lock.

Rebuild the strict project artifacts and refresh their immutable inventory after
the metadata edit, then run the planned bare installed-client and separate
test-rich minimum/current lanes with exact constraints and provenance. Preserve
the original PyYAML 6.0 failures as failures. D04 still advertises native Linux
arm64 and amd64: the user has explicitly kept unavailable native amd64 proof
open. Native arm64 or labeled emulated amd64 observations cannot close that
gap. The outstanding native cells remain release blockers, and this review
cannot be cited as full matrix or E08 completion.
