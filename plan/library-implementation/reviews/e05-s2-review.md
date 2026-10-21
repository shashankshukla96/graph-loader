# E05.F1.S2 independent implementation review — 2026-10-21

**Verdict: accepted after repair. No remaining actionable S2 finding.**

This separately initialized review inspected actual tracked/untracked changes at
HEAD `14fe4934976f62f421086e541e6bc00dbc1ceec8`, the execution plan, handoff,
canonical E05 and independent S1 acceptance. Scope is S2 resources/schema helpers;
writable run state, container proof and final E05 acceptance remain open. No
product source was edited, private settings read, service contacted or publication
performed by this reviewer.

## Finding and independently verified repair

**[P2, closed] `schema-example` propagated missing/undecodable resource errors.**
The original handler read YAML outside the resource-entry exception handler, so
missing files raised `FileNotFoundError` and invalid UTF-8 raised
`UnicodeDecodeError` instead of returning a schema diagnostic. Both were reproduced
independently against the original installed S2 wheel, without package mutation.
The author repaired the handler to catch `OSError` and `UnicodeError` before
writing stdout, log `stage=schema`, and return 1. Independent source checks,
13 resource tests, direct missing/decode controls and both repaired installed
environments verify exit 1 with no partial stdout. Original reproduction remains
in `var/e05-validation-20261021/s2/independent/original-finding-reproduction.log`.

## Source assessment

The package contains exactly two small reviewed byte copies: canonical/default
and social. Exact package-data and MANIFEST entries agree with the strict
validator's resource set and byte checks. Its wheel/sdist closure still rejects
additional payloads, missing resources and changed resource bytes. Safe source
capture has only the two explicit YAML exceptions to the general data exclusion;
independent checks confirmed other data, `.env`, datasets and logs remain denied.

`schema_path` uses `importlib.resources.files/as_file`; the enclosing command
holds its context through dispatch and handler cleanup. Explicit caller paths
bypass resource resolution and retain their filesystem interpretation. Omitted
configuration uses the packaged canonical schema without cwd config or `.env`
fallback. The two new helpers require no services. The guides accurately retain
the pending writable-state and checkout image-build limitations.

## Independently executed checks

- Fresh dev CPython 3.12 ran CLI/resource/installed-entry/namespace/import checks:
  **141 passed in 44.13s**. The initial invocation named a nonexistent schema test
  file and collected nothing; the corrected command produced this result.
- Additional schema validation/multi-topic/multi-pair checks: **318 passed,
  1 inherited intentional multi-pair-example skip in 0.38s**.
- Repaired resource tests: **13 passed in 0.12s**, including missing and invalid
  UTF-8 example resources. Ruff on touched runtime/tests/fixtures/tools and
  `git diff --check` passed.
- Used an actual wheel `zipfile.Path` with real `as_file` materialization:
  the temporary schema existed throughout successful dispatch and failing-handler
  cleanup, then disappeared after context exit in both cases.
- Re-executed strict inspection on both final repaired `s2b` artifacts and directly
  compared every original/rebuilt wheel member: all member names and bytes equal.
  Final wheel SHA-256 is
  `dff5ac003ba852b5623e66d3903d0d5c773d656cb7eb54ae6a797fd4b75184bb`;
  sdist SHA-256 is
  `40ed6a4a3b573d2cf207c8d3bdc85b942d6e3c9f40ef940d9e7a0dbf6e0acdb0`.
- **10 final archive negative controls rejected**: omit/corrupt either resource
  from either wheel, and omit either resource from the sdist. The same controls
  also rejected the original pre-repair artifacts.
- **54 guarded installed invocations** across two clean external noneditable
  environments and all three entry forms passed. Controls cover default/social
  examples and validation, external caller paths containing spaces, malformed and
  missing caller files, conflicting options and unknown sample names. stdout,
  stderr and expected exit codes agree across entry forms. Socket connection,
  bind/listen, subprocess and thread creation were forbidden, and environment
  equality was checked after every invocation.
- Independently executed the final resource fixture against both installations:
  package files/directories were read-only, an actual attempted package write was
  denied under this non-root account, default/social and caller schemas worked,
  unrelated cwd was read-only, and modes were restored. Both installed versions
  also passed the four combined missing/invalid-UTF8 repaired example controls.

Detailed independent results and reviewed input hashes are retained in owned
ignored `var/e05-validation-20261021/s2/independent/`. This review also inspected
the author's repaired `s2b/final-artifact-results.json`, strict Twine/build/rebuild
command evidence, and story evidence. Native permission proof does not substitute
for the final E05 read-only-rootfs container gate.

## Reviewed input identities

SHA-256 values bind acceptance to the repaired S2 inputs. Later story edits need
their own review and full-delta acceptance.

| File | SHA-256 |
| --- | --- |
| `pyproject.toml` | `f2a3f430204f7ae911655b98cdc17793e6c10f2322dc5322d13f2eb38f947231` |
| `MANIFEST.in` | `95c4cd08d768ba7d29d0d6b0089095453adc73707cd1eedb01f2c50ec0b8ad53` |
| `src/neo4j_graph_loader/cli.py` | `001a39f1e1443800ebfbeef2bb2d11ab5e2574e6394f357e83c6c5abb880e982` |
| `src/neo4j_graph_loader/_resources.py` | `c19cf2cdebc7f4c311aa80b9799682b17a9426b675cc71f97590f2d003692551` |
| `src/neo4j_graph_loader/data/graph_schema.yaml` | `954ba4fc994cc1d16844a3cc29a1971a94042817c20f4a0a52f70e4ac932b747` |
| `src/neo4j_graph_loader/data/social_schema.yaml` | `fd558b4bb65671b9cda6514280d23e37a93adebd04e240d938c36173d3f98c74` |
| `tests/test_cli_resources.py` | `cb14ccb3136e0bba987deda9b3a7372b8f066f961fdbb31381bf72f62c382f48` |
| `tests/fixtures/e05_resources.py` | `2bb046c5ce836c4abb17a798d8fc51b715b74d8ac5272252e4a7e9055b4c0c53` |
| `tests/test_cli.py` | `027143280987e768c8d789c6f5b938390aaf5418a2d3612bd13c08223ed5aa93` |
| `plan/library-implementation/tools/verify_e03_artifacts.py` | `be0fdabb16f4fd3e5c4c344b8370cf918dc8ba2f7e5d06bfecfc02ea7d43fbfa` |
| `plan/library-implementation/tools/capture_baseline.py` | `c56883d54c5eeb2fb978d107e175a4ac0d2222663606c75770065d1727c1ed25` |
| `PACKAGE_README.md` | `be5d5915e591c2e5fea038ae15cee99884065e84214efc39d25c702c1c39c59d` |
| `docs/DISTRIBUTION_DEVELOPMENT.md` | `e67c7c66723c5499de2066ea70a0c387b15a48795d291a8e49f72d7927779464` |
| `docs/LIBRARY_API.md` | `d323f05f108e5998e6c070566c459b74941d04b676a19b0f330bd1550e4aff73` |
