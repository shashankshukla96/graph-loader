# E04.F1.S1 independent checkpoint — 2026-10-18

Status: **accepted after repairs**; R1/R2 are closed (see final recheck below).
Reviewed actual dirty/untracked
working tree against entry `98cd267`; no implementation files edited by reviewer.

Read execution-e04.md, canonical E04.F1.S1, D02's approved exports and the
cancellation contract. Inspected public schema.py/planning.py, schema loader
changes, docs/LIBRARY_API.md, test_public_api.py and the existing planner/source
implementations behind the reexports. Cancellation exports, private transport,
cleanup and import-inertness gates remain their separately ordered stories;
their absence is not a finding at S1.

## Independent validation

Used `var/e04-validation-20261018/dev/bin/python`, independently reran:

```
python -m pytest tests/test_public_api.py tests/test_library_preflight.py tests/test_schema_validation.py -q
```

Result: **183 passed in 0.73s** (exit 0). Read the retained s1-tests.log separately:
183 passed in 1.64s. No `.env`, remote IMDb, commit or push operations performed.

## Findings

### R1 [P2] Wrap errors while checking the schema file

Location: `src/neo4j_graph_loader/utils/schema_loader.py:68`.

`resolved.is_file()` remains outside both error-wrapping blocks. A filesystem
stat error (for example a permission or I/O failure) escapes as raw `OSError`
instead of the supported `SchemaLoadError` with its original cause. Clients
catching the public error therefore miss this file-loading failure, contrary to
D02 and docs/LIBRARY_API.md's read-failure contract. The adjacent new resolution
wrapper does not cover stat failures after resolution.

Independently reproduced using only the public import:

```python
from pathlib import Path
from unittest.mock import patch
from neo4j_graph_loader.schema import SchemaLoadError, load_schema
with patch.object(Path, 'is_file', side_effect=PermissionError('stat denied')):
    try:
        load_schema('schema.yaml')
    except Exception as exc:
        print(type(exc).__name__, isinstance(exc, SchemaLoadError), exc.__cause__)
```

Actual: `PermissionError False None`. Wrap this stage and add a public regression
asserting `SchemaLoadError` and identity/type of the original cause.

### R2 [P2] Add the explicitly planned public multi-pair source controls

Location: `tests/test_public_api.py:56` (source-validation coverage).

The execution plan explicitly calls for multi-pair input errors through the
public modules, but this suite only tests an unknown type and single-pair
mapping/sequence construction. No public test constructs a multi-pair schema,
rejects the ambiguous mapping, rejects a sequence source with missing/unknown
topic, or accepts an explicitly topic-bound source. Existing multi-pair tests
import implementation modules and were not run by this checkpoint command.
Consequently the new documented topic/pair contract and public error identity
are not covered by the stated S1 evidence.

Add public-only schema/planning imports for those cases and retain the diagnostic
assertions; verify a valid named topic selects its intended endpoint pair. This
is the planned S1 binding gate, separate from later lifecycle/source-failure work.

## Otherwise verified

Finite approved S1 `__all__` values match D02 and retain canonical objects;
no root API or internal worker/fleet exports were introduced. Public schema
success, Unicode/YAML/root/model errors, missing-path and resolution causes,
Pydantic direct validation, concrete source naming, representative settings,
single-pair source inputs and a genuine PlanResult all pass. Documentation states
Kafka dependence, concrete base-class semantics and future console/cancellation
boundaries accurately for this checkpoint.

## First repair recheck

Independently inspected the updated stat wrapper and new public tests. R1 is
closed: `is_file()` is inside `except OSError`, with the original exception
chained, and a public PermissionError regression passes. R2's mapping and
unknown-topic rejection plus valid construction now exist; missing-topic
coverage and verification of the selected endpoint pair remain to close it.

Repeated focused command: **185 passed in 0.59s** (exit 0). Targeted Ruff on the
public modules, schema loader and public tests passes; `git diff --check` passes.

## Final repair recheck — accepted

R2 is closed. The public multi-pair test rejects the mapping, both missing and
unknown source topics, then selects the last topic and verifies its task uses
the expected endpoint pair and differs from the first pair. R1 remains fixed.

Also inspected the additional cause-preservation change in edge_planner.py:
inline workers retain the original exception and `_gather` chains it; remote
worker diagnostics retain a diagnostic RuntimeError cause without introducing
exception-object unpickling. A new public inline-source failure regression
asserts original cause identity. No actionable issue found in that delta.

Independently reran the exact expanded checkpoint command:

```
python -m pytest tests/test_public_api.py tests/test_library_preflight.py tests/test_schema_validation.py tests/test_external_planner.py -q
```

Result: **205 passed in 2.12s** (exit 0). Repeated Ruff on public modules,
schema loader, changed planner and public tests: pass. `git diff --check`: pass.
The earlier 183/185 runs above are historical checkpoint results, not claims
about this final source. **No unresolved findings for E04.F1.S1.** Subsequent
ownership, transport, cancellation, cleanup and import-effects gates remain
separate and are not certified by this acceptance.
