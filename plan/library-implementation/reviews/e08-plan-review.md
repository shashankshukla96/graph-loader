# Independent E08 execution-plan review — 2026-10-27

**Disposition: accepted for implementation after amendment. No actionable
execution-plan finding remains.** This accepts the ordered plan, not a dependency
floor change, implementation, completed story or support-matrix result.

The reviewer began with a task-only brief and no preceding implementation
conversation. Entry HEAD is `4b246f3ab953bd9f7d266a682ac9fff39bd32032` on
`codex/pypi-library-checklist`. The amended [execution plan](../execution-e08.md)
has SHA-256
`96d85775ad69d1a5a76f2041c5878ae680c13655d90cdece66509870faaaa13a`.
Reviewed canonical [E08](../epics/e08.md), [decisions](../decisions.md),
[ordering](../ordering-matrix.md), [cancellation](../cancellation-contract.md),
the [handoff](../handoff-e08.md), current packaging, installed runner and relevant
planner/transport/API/CLI source and documentation. Inspected the retained
declared-minimum failure logs and native-arm64 discovery receipt. Only this
report was written by this reviewer; no tests/services, private environment-file
access, remote IMDb access, commit, push or upload occurred.

## Original finding and amended disposition

**P1 — The proposed eight-worker maximum omitted the existing advertised
64-worker maximum.** The original plan proposed normal counts1–8 and cleanup
at1/2/4/8. `docs/CLI.md`, `docs/BULK_LOAD.md` and actual CLI validation already
document/accept1–64. Eight-worker proof could not establish that contract.

**Resolved in the reviewed amendment.** Normal planning now covers1–8 plus
16/32/64; full failure/cleanup covers1/2/4/8/64. The existing CLI range and
serialized settings remain intact. Untested interior counts must be disclosed
without claiming exhaustive timing measurements. Positive API count acceptance
is distinct from evidence for measured counts; no arbitrary larger-count proof
or new constructor limit is inferred. The maximum-worker cells retain the
aggregate ten-second cleanup budget and original external supervisors.

## Scope, dependency evidence and runner preservation

D05's eight mandatory runtime dependencies govern the generic extra/fallback
story wording. No missing-extra API or pure-driver fallback is introduced.
Spawn and cooperative inline preserve the current process modes; other-context
observations do not expand advertised support or change the embedding global
method. The bounded spawn refactor for existing fork warnings preserves output,
source-failure assertions, IDs and per-call worker configuration; larger changes
require amended review.

The strict-constraints/finite-wheelhouse runner extension is appropriate provided
the implementation preserves the prescribed old interface/defaults and every
E07 ownership, interruption, origin, startup and post-exec receipt control.
Runtime and test-tool installation both remain constrained. Directives, URLs,
path injection and unsafe wheelhouse members are rejected; exact input bytes,
resolution/install reports and installed inventories remain evidence obligations.
New bare runtime probes precede separate test-rich lanes, and all eight frozen
installed lanes retain exact coverage and exclusions. A fake producer does not
establish Kafka/service compatibility.

The original PyYAML6.0 resolver attempts reached a real default isolated-build
metadata failure (`cython_sources`) on macOS and native Linux arm64. They are
failure evidence, not successful installs. The smallest proposed candidate6.0.1
still requires coherent joint resolution, actual install/probe evidence and an
independent floor-decision review before metadata changes. Infrastructure retries
do not approve that correction. Record source-built inputs rather than requiring
every dependency to have a CPython-specific wheel.

## Native gap, ordering and completion

The amendment records the user's2026-10-27 answer: native Linux amd64 is
unavailable and its proof stays open. Native arm64 and labeled emulated amd64
observations cannot silently close native amd64 timing/transport/cleanup cells.
The portable finite runner bundle preserves a future route to identical native
inputs without inventing access or narrowing D04. Missing native cells remain
release blockers. Any interim implementation review must identify pending
matrix evidence and cannot be represented as completed story/matrix proof.

S1 precedes S2 with independent review; actionable findings require repair,
retest and rereview. The final checkout/example, lint/parity/pip/whitespace,
strict artifact/rebuild equality, bare installs, both-wheel installed lanes,
four fresh immutable images, twelve independently supervised R1 controls,
exact no-skip B3 and installed tiny service gates remain intact. No historical
total, infrastructure skip or IMDb substitution closes them. Final independent
acceptance, delivery equality and a conditioned E09 handoff remain pending;
E08 cannot be marked complete with an advertised native cell unproved.
