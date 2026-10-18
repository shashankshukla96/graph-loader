# E03 independent completion revalidation — 2026-10-18

**Verdict: approve E03 within its namespace-migration scope. No actionable
completion blocker found.** This is an independent inspection of the canonical
acceptance criteria, actual source, retained entry source, archives and local
images. Earlier approval labels were not treated as evidence of correctness.
This review does not approve E04 implementation, publication or a broader
platform/service support claim.

## Reviewed identity and change tracker

| Commit | Role verified in this review |
| --- | --- |
| `6de3562083cc7079091d056b89d66f18b69931e8` | Parent of the implementation commit; the E03 entry manifest additionally captures required uncommitted E00–E02 work, so this commit alone is not the full migration baseline. |
| `bf5471e9243dbda90cdf066a5d811486119dba7c` | Cohesive E03 implementation plus intentionally preserved prerequisites. |
| `ffb862537b83ef7be053fb96e05d12fcbcfdd355` | Reviewed HEAD. Its two changed files are completion/postcommit evidence only; it changes no runtime/build input. |

No applicable `AGENTS.md` was found at the repository or its ancestor paths.
The governing inputs were [E03](../epics/e03.md), [decisions](../decisions.md)
and the story acceptance/validation criteria. This agent changed no product,
test or build source and made no commit, push or remote-service request.

## Independent findings and checks

1. **Migration is mechanical relative to the actual entry state.** Read the
   retained `var/e03-validation-20261017-6de3562/entry-source.tar.gz` directly,
   checked each of its 49 original runtime module bytes against the entry
   manifest/comparison hashes, and independently recomputed AST equality with
   the current modules. All 49 agree after namespace/path substitutions, the
   documented metrics import adjustment from
   `..neo4j_graph_loader._version` to `.._version`, and the package docstring
   update. No algorithm, schema/message/group identity or executable control
   flow changed in the migration. Comparing only the parent Git tree would
   conflate five inherited prerequisite changes with E03; the retained entry
   archive resolves that ambiguity.
2. **Evidence still names the current source.** Independently checked all 245
   existing source inputs from `e03-final.json` against current size and
   SHA-256; all match. The 49 current runtime hashes also match the mechanical
   comparison and artifact inventory. All four Dockerfile hashes match the
   final successful-build record. The later evidence-only commit does not
   invalidate these per-file comparisons. The old combined HEAD-dependent
   source hash is not claimed to be the current Git identity.
3. **Imports, subprocesses and tests are coherently renamed.** Inspected
   runtime imports, DockerService overrides, CLI retry subprocesses, spawn
   target/class locations, examples, patch/sys.modules strings and test setup.
   Searches found no executable old application import/module/patch target.
   Independently inspected normalized existing test/example differences:
   assertions remain intact; changes are import roots, source-layout static
   guard roots, removed application-only path insertions and documented
   version-probe identity. Historical Git slice paths intentionally retain
   their historical names. Comment-only old source paths also remain in
   monitoring files/changelog; these cannot resolve application imports.
4. **Development imports cannot silently use the checkout root.** Pytest's
   root `pythonpath` workaround is gone. Remaining IMDb/test insertions expose
   repository example/test helpers, while application code is nested under
   the new source layout. The documented workflow installs editable before
   tests. The existing repository `.venv` currently fails an uninstalled
   `import neo4j_graph_loader`; this is consistent with that workflow and was
   not repaired or used as installed-artifact evidence by this reviewer.
5. **No generic compatibility package is shipped.** D03 selects no shim;
   source, discovery and migration documentation follow it. Old pickle class
   names are explicitly unsupported, while the new canonical spawn test
   exercises the migrated class identity. Package discovery includes only
   regular `neo4j_graph_loader` packages/subpackages. A checkout source-root
   directory named `src` is not an installed legacy application package.
6. **Retained wheel/sdist are complete and correspond to source.** Independently
   verified retained wheel/sdist SHA-256 against the final artifact record,
   inspected the exact-discovery/manifest rules and reran `inspect()` from
   the E03 artifact validator against those actual archives. Passed:
   **49 runtime modules, 54 wheel members, 60 sdist files**. Exact archive
   membership and per-module source bytes pass, with one expected metadata
   root, correct version/dependencies/license/readme and no generic `src`,
   tests, fixtures or extra runtime payload. The validator's exact whole-wheel
   allowlist addresses the prior metadata-leak finding. Console command and
   packaged resources remain explicitly deferred E05 work.
7. **All four component images really import the installed package.** Each
   Dockerfile installs generated mandatory requirements, then installs the
   package through isolated setuptools using the literal file version; no
   Git-based fallback or PYTHONPATH rescue is needed. Independently reran
   the complete retained immutable-image matrix with `--pull=never`,
   `--network none`, cwd `/tmp`, no mounts, and isolated Python for import/
   module probes. All four images passed all 49 import-origin/source-byte
   assertions, eight component helps, default entry-point help and `pip check`.
   The import probe also rejects `src`, checks version agreement and guards
   network/thread/process/root-logging import side effects.

### Independently rerun image evidence

The recorded rerun completed **44 successful commands**: four import probes
covering **196 module-origin/source-byte assertions**, 32 component helps,
four default entry-point helps and four dependency checks. Component modules
are CLI, node, edge, retry, planned writer, dispatcher, lease and legacy clock.

Full exact commands, exit codes, stdout/stderr, resolved import origins, image
identities and all current runtime hashes are retained locally in
`var/e03-revalidation-independent-20261018-image-probes.json` (safe ignored
evidence output authorized by the coordinating reviewer). SHA-256:
`b4baecfc1e5cd43c60bdcdc68cb87dc9c41093a36fe2ea48de85fdf2e9389d84`.
The command payloads originate in the inspected
[final image matrix](../evidence/e03-final-image-probes.json); the rerun adds
`--pull=never` and actually executes every command again.

| Image | Independently checked immutable ID | Commands passed |
| --- | --- | --- |
| CLI | `sha256:70f0d0a4a61cfe94ee6485497402cf8cd62a02faa43a594927c593eb98cb765f` | 11 |
| Node | `sha256:74589fd928af851a5a582ef1a1fc0a3b34e4fa0185c48fc11e4ac8f3157f50f2` | 11 |
| Edge | `sha256:5ee08a7e5f83c8387803f9eee3787772f335981c4117ef51aa7b333ccb0ab183` | 11 |
| Clock | `sha256:113091b00ffed55fad6a19e7590634be61927191ca16009d1b8dc79bc16332c6` | 11 |

## Acceptance disposition and concrete limits

| E03 story | Independent disposition |
| --- | --- |
| F1.S1: cohesive runtime migration | Source/entry AST comparison, preserved tests, executable-reference inspection and four-image matrix pass. Reviewed historical full-suite and B3 records remain consistent with current source. |
| F2.S1: approved old-namespace policy | No shim shipped, new canonical identities used, migration/pickle policy documented, isolated image old-import rejection passes. |
| F2.S2: complete migrated artifacts | Retained exact archives/source closure independently passes; all component commands resolve from installed image site-packages. Reviewed two clean noneditable wheel-install records and extracted no-Git rebuild evidence. |

This agent **did not rerun** the full 3,387-test checkout suite, native clean
wheel installations/builds, Twine, archive mutation controls, Docker builds or
live B3 regression. Those historical records were inspected, not represented
as this agent's fresh executions. The coordinating reviewer is independently
running fresh development-suite, build/install, Docker-build and supervised
B3 checks; their results must be recorded separately. Historical
`/tmp/graph-loader-e03-work` environments no longer exist, so their paths are
provenance rather than presently inspectable environments. Retained archives,
entry source and the four immutable local images do exist and were checked.

The historical suite record is 3,387 passes, four inherited skips, 49
deselections and 19 inherited fork warnings; the focused record is 55 passes.
The exact historical B3 record is one pass without skip, actual/expected 783
pairs, four type-wide DLQ partitions, 4,154 envelopes/commits, no parked rows
and zero observed lock wait. These are recorded prior results, not new claims
of this review. No external IMDb, configured developer service, remote
endpoint or publication was accessed. Image checks are Linux arm64 smoke
checks; they do not establish E08/E09 platform, service or cancellation proof.

E03's implementation is suitable as the prerequisite for an E04 handoff once
the coordinating review closes its fresh required regression checks. This
review neither implements E04 nor changes its acceptance boundary.

## Final addendum — fresh coordinating evidence verified

**Final disposition: E03 accepted. The pending coordinating-test gate above is
closed; no actionable source, artifact, image or regression finding remains.**
Independently checked the coordinating
[completion verification](e03-completion-verification-20261018.md) against
[its structured evidence](../evidence/e03-revalidation-20261018.json) and the
actual raw records under `var/e03-revalidation-20261018-z_x9loyc/`.
This addendum verifies completed executions by the coordinating reviewer; it
does not claim that this agent reran those passing tests.

- All **13** retained raw-record sizes and SHA-256 values agree with the
  structured evidence. Independently recomputed both before/after source and
  environment hashes. All **245** source inputs agree across before, after,
  postcommit evidence and current files, including content, size and executable
  mode. Source identity is
  `e061a4fdc095a82d197bb93992df1a6de4a30894946ccea5e2e0ef0d102d0ecb`;
  environment identity is
  `52b35a20e5e822a5e36fcdc7709f3170fc97ddbac417136cb92551f1d17cbdc4`.
  The unstaged Git diff hash changed as planning documentation was edited;
  the summary correctly claims unchanged source/environment identities,
  not an unchanged entire working-tree diff. Current tracked changes are
  confined to plan/index/handoff documentation.
- Fresh setup records install declared development dependencies, then the
  editable application, then pass dependency checks. `full-results.json` and
  the actual `full-tests.log` agree: **3,387 passed, four inherited skips,
  49 deselected, 19 warnings**, exit zero. Pytest time is **175.44 s**;
  the separately recorded supervising wall time is **208.0 s**.
- Fresh supervised B3 result, pytest log and actual proof summary agree:
  **one passed, zero skipped**, pytest **23.36 s**, supervisor **23.967 s**,
  no timeout and empty before/after container inventories. The proof records
  **783 actual/expected pairs, 582 shared pairs, four DLQ partitions,
  4,014 envelopes/commits, zero parked rows, no per-topic DLQ, 840 LIKES
  writes and zero observed lock wait**. Independently summed partition
  watermarks to the same 4,014 count. No historical scheduling count was
  substituted for this fresh result.
- Fresh build/rebuild/Twine/install/dependency-check commands all record exit
  zero, and their logs agree. The extracted rebuild contains no `.git`.
  Original/rebuilt artifact hashes match actual retained bytes; independently
  compared **every wheel member byte** and all **49** runtime source hashes.
  Inspected both distinct clean environments on disk: all 49 actual installed
  files match source digests, paths lie under their respective site-packages,
  direct-url records are noneditable and no editable rescue file exists.
  Each recorded isolated probe passes eight component helps and six separate
  old-import rejections, with the inspected inertness/version checks intact.
- The four fresh build logs contain the newly recorded immutable IDs
  (`36d52bc4…`, `92b2b2f5…`, `212160ae…`, `4969c6de…`). Every fresh-image probe
  command names its corresponding new ID. Verified **44 zero exits** and
  **196 source/origin assertions** in the raw fresh matrix, with no pull,
  networking disabled, `/tmp` cwd and no source mounts. Recorded source maps
  match current source; each import payload contains the exact 49-member
  digest map it asserts. The retained-image executions documented earlier
  are separate evidence and are not used to stand in for these new images.

This closes the E03 prerequisite verification for an E04 handoff. The separate
handoff/tracker reviewer owns its documentation disposition. E04 remains
unimplemented, and the earlier platform/release/cancellation scope limits
remain in force. No additional test execution, product edit, commit or push
was performed for this addendum.
