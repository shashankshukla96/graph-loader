# Fresh final E06 revalidation — 2026-10-23

**Disposition: blocked by actionable compaction counterexample; final proof remains pending.**

This is a fresh independent final review from zero inherited task history. Entry
HEAD is `e78970cb310a7a265ae6af5db958edc613f1d923`; original E06 implementation is
`a034420f98a57d17c7591b23eb1011d7bc346d25`, E05 baseline `a2bc7bb`. Historical
acceptance is context, not a substitute for this review. The original full-suite
failure and independent diagnosis attribute the initial replay/local-install race
to unchanged pre-E06 index code. The diagnosing agent subsequently authored the
first offset-fence repair; this final reviewer authored neither product repair.
The parent authored the separate database normalization repair.

## Reviewed scope and independent checks so far

Read E06 epic/execution/revalidation, D01–D07, cancellation contract and ordering
matrix; inspected actual whole E06 product delta and the seven pending product/
test files. No applicable AGENTS.md was found. Reviewed image policy/candidate
COPY closure, immutable-ID selection, CLI ordering, preflight mount and container
ownership, database probe/session behavior, separate host/container coordinator
addresses, and inherited lease-server SIGTERM teardown repair. Existing positive
client doubles now provide explicit delivery coordinates instead of silently
accepting unconfirmed publication; no existing assertions are removed to make
the full-gate index failure disappear.

The database probe now calls shared `open_session(driver)`, preserving stripped
explicit database names and omitted default selection for unset/empty/blank.
Its five new controls verify subsequent Kafka metadata completion and driver
closure. Current preflight paths retain exact returned container ID before
start, exclusively created marker ownership before writing, phase-only public
diagnostics, strict read-only inputs and bounded wait/owned cleanup.

A new independent focused execution passed **289 tests in 5.09s**, covering index,
topology, image selection/candidate builder, DockerService, lease-server teardown,
multi-topic DLQ and both single-pair compatibility suites. Raw log:
`var/e06-final-revalidation-20261023/focused.log`. Interpreter:
`var/e06-validation-20261022/dev/bin/python`.

Independent safe capture contains **279 inputs**, source identity
`31a6fffe4e878450fadcf9636683ed81615899f81178cd7dd7deab0b6328d2e4`, environment
`96b072066210abc50c4d8cf2bf48460f11e2ebbea959f87983d47328e1d5135d`.
The capture is `var/e06-final-revalidation-20261023/before.json`.

## F1 — partition progress is insufficient same-key evidence after compaction

**Required repair; affects local blocking correctness.** In the first repair,
`ParkedPairIndex._install_confirmed()` drops a returning local mutation whenever
`_applied_position[partition] > acknowledged_offset`. This assumes the consumer
must have applied that record, or a newer record of the same key. A compacted
log can pass that offset while having applied neither.

Deterministic valid schedule:

1. Local add for outstanding identity A is durably acknowledged at index offset0;
   its flush/caller has not returned to the local-install step.
2. Another writer appends unrelated identity B at offset1 and a newer A at offset2.
   Kafka compaction may remove A@0 because A@2 exists.
3. The poller skips the compacted offset0 and consumes B@1 only. Its next consumed
   position is2; it has never seen A.
4. Local add resumes. The first repair treats `2 > 0` as sufficient evidence and
   skips installation. The add returns successfully while both `contains(A)` and
   `is_blocked(A)` are false, until the poller eventually consumes A@2.

This new omission can let a subsequent local same-pair row bypass its confirmed
outstanding record. It is not an out-of-order consumer, partition comparison or
stronger cross-replica ordering promise. The old unconditional post-ack install
would retain A in this schedule; the first repair introduced this specific
regression while addressing the inherited replay race.

Own ignored reproducer
`var/e06-final-revalidation-20261023/test_compacted_delayed_install.py` fails
**1 failed in 0.04s** on the frozen first repair; full failure is preserved in
`compaction-red.log`. It uses the real index with a paused poller, a delivery
callback and an interleaving inside flush, then asserts the public local view.
No product or repository test edits were made by this reviewer.

The correction needs same-key consumed evidence while local operations are in
flight, without retaining an unbounded history of tombstones. Partition progress
must continue to reflect only actual consumed records/EOF, producer I/O must
remain outside the state lock, and pending fences must still survive older
same-key replay and retire across compaction. All affected add/remove/bulk,
foreign-newer and reversed-local-install controls need renewed independent
checks, followed by a new final source freeze and affected full/artifact/image/
installed proof. The parent and repair author have been notified.

## Pending gates and limits

Fresh full suite, strict archives/outside-Git rebuild and every-member equality,
two clean noneditable installs, four newly built images/read-only and bind
probes, all twelve separately supervised installed/image R1 controls, exact
owned-service no-skip B3 and enhanced installed service proof are parent-owned
and are not yet accepted here. Prior green labels/receipts do not close this
review, particularly after an additional product correction changes source.
Final tracked reports/evidence and source/evidence identities still require
inspection after the parent supplies them.

This reviewer created only the new review report and ignored scratch. No private
`.env` was read/sourced; no remote IMDb service was contacted; no live Docker
container, image, network or foreign resource was mutated; no product/test edit,
commit, push or publication occurred.

## Independent amended-design disposition

The parent supplied the author’s amended design after F1. Independently approved
for implementation: register a bounded active context before publication, keyed
by wire key, with reference counts and consumed/installed offset evidence per
index partition; retain that context through installation/error until the last
overlapping same-key call ends. Skip installation only with actual valid same-key
consumed evidence at or beyond the acknowledgement, or newer same-key local
installation/fence evidence. Remove the global consumed-position veto; use the
global frontier only to avoid adding a pending fence behind an already-passed
position. Unrelated/EOF/malformed records must not count as same-key state
application. Retain newer installed evidence through unrelated progress/fence
pruning while an older overlapping caller is active. Precompute bulk keys before
registration, or unwind all registrations even if a later key fails; duplicates
count individually. Producer I/O remains outside the state lock. Proposed new
controls cover compacted gaps for all operations, ignored malformed newer values,
reversed local installation after pruning, duplicates and failure cleanup.
This design approval is not acceptance of unreviewed future code or proof.
