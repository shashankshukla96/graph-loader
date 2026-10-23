# Parked-pair index full-gate failure investigation — 2026-10-23

**Disposition: confirmed inherited product race; first authored repair
superseded after fresh independent review found a compaction regression.
Amended candidate authored and checked; independent actual-code acceptance
and the parent's renewed final gates remain pending. Prior author checks do
not close the gate.**
The E06 full-suite failure is reproducible without changing the failing test.
Controlled schedules reproduce both temporary stale replay and a persistent
wrong local view after newer foreign writes. No product or repository test
source was edited during the initial independent diagnosis. After that report
and a separate reviewer's approval of the design, the parent authorized this
same investigator to become the repair author. The diagnosis/proposal below
preserves its pre-repair evidence; the final section records the later author
phase. Author checks do not constitute independent acceptance of the repair.

## Attribution and scope

Entry HEAD: `e78970cb310a7a265ae6af5db958edc613f1d923`. The parent independently
owns the unrelated E06 database-normalization repair. During initial diagnosis
this investigator wrote only this report and ignored scratch under
`var/e06-index-revalidation-20261023/`. No real service, container, private `.env`,
foreign cleanup, commit or push was used.

`git diff a2bc7bb e78970c --
src/neo4j_graph_loader/loader/parked_pair_index.py
tests/test_parked_pair_index.py` is empty. Both files are byte-identical to
E03 migration commit `bf5471e`; SHA-256:

* Product: `c39be57572789434d0e618edcfb1acb0d38eb1a3995befd3d67fc4d46bb38ea4`.
* Test: `65a95c085acf76af461e0f0f5bb00308514f7648e99d0121e6d74d8e3c40bcee`.

Thus E06 validation exposed an inherited defect. This is not evidence that E06
introduced the race, nor that any historical failure with a different signature
has this cause.

Original preserved failure: `var/e06-revalidation-20261023/full-suite.log`:
`tests/test_parked_pair_index.py::test_contains_false_after_remove_the_duplicate_copy_signal`
at line 281, expected `False`, observed `True` after synchronous add/remove.
Full result: **1 failed, 3623 passed, 4 skipped, 49 deselected, 19 warnings,
196.88 seconds**.

## Causal mechanism

`add()` publishes and then installs its record directly in `_by_pair`
(`parked_pair_index.py:793–799`); `remove()` publishes and clears it directly
(`:821–828`); `remove_many()` similarly clears successfully delivered entries
after one flush (`:882–890`). Independently, `_poll_forever()` calls `_apply()`,
which unconditionally applies each consumed log entry (`:495–538`). The shared
lock prevents simultaneous dictionary writes, but supplies no ordering between
an acknowledged local mutation and a previously fetched or lagging log entry.

For one key in one Kafka partition:

1. Local add is acknowledged at index-log offset 0 and becomes locally present.
2. Local remove is acknowledged at index-log offset 1 and clears it locally.
3. Poller applies the older add at offset 0, restoring the resolved record.
4. Poller eventually applies tombstone offset 1, clearing it again.

Step 3 is exactly the observed assertion failure. The fake log delivers offsets
in order; it does not invent out-of-order broker delivery. No compaction is
needed. The original fake spins on repeated EOF, influencing scheduling and
making failures intermittent, but that does not make the local ordering defect
fake-specific. A producer that invokes successful delivery callbacks with real
message-shaped partition/offset metadata reproduces the same defect.

The inverse sequence add/remove/re-add is also wrong: an older tombstone can
temporarily clear a newer acknowledged add, allowing `is_blocked()` to return
false while a record is outstanding. The bulk removal path has the first race.

There is a second ordering window that a fence against old replay alone cannot
repair: after a local delivery callback, before its `flush()` returns, a foreign
writer can append a newer same-key record. If the poller applies both before
the caller performs its direct local install, that install overwrites the newer
broker state. Demonstrated variants:

* Local add overwrites an already-applied newer foreign tombstone.
* Local remove overwrites an already-applied newer foreign add.
* Local bulk remove overwrites an already-applied newer foreign add.

These remain wrong with **zero unread log records** and
`caught_up(watermark_snapshot()) == True`. No further record necessarily arrives
to self-correct them.

## Read-barrier and impact limits

A snapshot captured after add but before remove has high watermark 1. After
the stale add is consumed, `caught_up({0: 1})` is true even though the local
record has been resurrected. A fresh snapshot taken after the removal has high
watermark 2 and correctly remains false until the tombstone is consumed.
Therefore the ordinary frozen batch barrier can prevent some schedules, but
does not provide a general ordering rule for local mutations. The persistent
foreign-write schedule above defeats even a fresh barrier.

The production retry consumer captures snapshots while freezing batches
(`retry_consumer.py:1609`) and checks them before processing (`:1630`), then uses
`contains()` to distinguish duplicates (`:1786`). This investigation proves
incorrect index behavior, not a completed stale-write event against Neo4j.
Additional duplicate/superseded/stale-copy defenses may reduce downstream
impact in individual schedules. A broker-backed reproduction and end-to-end
data-loss demonstration were outside the authorized offline investigation.
Temporary false blocking is established; temporary false unblocking and a
persistent incorrect caught-up local view are also established.

## Independent checks and retained reproductions

Interpreter throughout: `var/e06-validation-20261022/dev/bin/python`.

1. Existing index suite: `-m pytest -q tests/test_parked_pair_index.py`:
   **39 passed in 0.46s**, saved as `index-suite.log`. This pass does not remove
   the deterministically reproduced defect.
2. `repeat_original.py` invokes the exact unchanged failing test 500 times per
   scheduling setting: **2/500 failures** at the default 5 ms switch interval;
   **41/500** at 1 microsecond. `repeat-original.jsonl` records failed iteration
   numbers. These are observation counts for this fake/test/process, not
   estimates of production failure frequency.
3. `probe_index_race.py`: eight offline scenarios with semaphore-controlled
   delivery and a completion queue. No scheduling sleeps determine the race.
   `probe-index-race.jsonl` contains original-fake and acknowledged-producer
   stale-add cases, bulk removal, inverse stale tombstone, a caught-up control,
   and the three persistent foreign-write cases. All baseline-defect assertions
   pass. Same-partition offset order is preserved throughout.
4. `test_proposed_regressions.py`: six desired-contract regression skeletons:
   single/bulk remove survives old add, re-add survives old tombstone, and three
   foreign-write-before-local-return cases. **6 failed in 0.12s** on untouched
   product source, saved as `proposed-regressions-red.log`. The foreign-write
   helper currently contains baseline-only assertions, explicitly marked for
   removal when incorporating these skeletons into repository tests.

All named logs and scripts in this section are below the owned scratch folder.

## First proposed repair design — superseded after compaction review

Retain synchronous delivery confirmation and immediate local visibility, but
order local and consumed mutations with the broker-assigned **index-topic**
partition and offset. Do not use envelope `original_partition` or
`original_offset`; those identify a different log.

1. `_publish()` captures `(message.partition(), message.offset())` from the
   successful delivery callback and returns those coordinates only after the
   existing flush/error/undelivered checks succeed. Missing callback or invalid
   metadata must fail closed. Never treat `flush() == 0` alone as coordinate
   confirmation. Failure callbacks may legitimately have no message.
2. A common local-install helper runs under `_lock`. If the poller has already
   consumed past that acknowledgement (`_applied_position[p] > ack_offset`),
   skip the direct installation: the current state already includes that echo
   and possibly a newer foreign write. Also skip if a newer pending local
   acknowledgement for that same wire key/partition already won. Otherwise
   mutate `_by_pair` and retain a pending fence for that key at `(p, offset)`.
3. `_apply()`, under the same lock, suppresses state mutation for messages of
   that wire key in that partition whose offsets precede the local fence.
   Equal/newer messages remain authoritative. Suppressed stale messages still
   advance consumed position. Do not suppress unrelated keys just because some
   local publication has a higher offset in their partition.
4. Central consumed-position advancement retires pending fences behind its
   position, including advancement from an EOF marker or a later message when
   compaction removes the exact acknowledgement echo. Fences should represent
   pending propagation, not retain every historical tombstoned key forever.
5. `add()`, `remove()`, and each successful result from `remove_many()` use the
   same helper. Bulk callbacks retain per-record coordinates and errors; only
   confirmed successes mutate locally. Preserve one bulk flush and positional
   results. Do not hold `_lock` across `produce()` or `flush()`.

Critically, producer acknowledgements **never advance** `_applied_position`.
An acknowledgement for key A at offset 20 does not mean the poller consumed
unrelated keys at offsets 1–19; pretending otherwise would break the existing
read barrier. Numeric offset comparisons are valid only within a Kafka
partition. The existing ordering contract assumes a given wire key has stable
partition placement; these proofs and the repair must not silently introduce a
cross-partition total-order claim.

Equivalent pseudocode, omitting decoding/error paths:

```python
def install_confirmed(key, ack_partition, ack_offset, mutation):
    with lock:
        if applied_position[ack_partition] > ack_offset:
            return
        if pending_same_key_partition_offset(key, ack_partition) > ack_offset:
            return
        mutation()
        pending[key] = (ack_partition, ack_offset)

def apply_consumed(message):
    with lock:
        if not older_than_pending_same_key_same_partition(message):
            apply_value_or_tombstone(message)
        advance_consumed_position_and_retire_passed_fences(message)
```

The second guard on local installation covers reversed installation order for
two already-confirmed local writes; the first guard covers equal-own-echo and
newer-foreign-message interleavings. A replay-only guard misses the latter.

## Fake compatibility and required regression coverage

Positive fake seams requiring realistic coordinates:

* `tests/test_parked_pair_index.py:33`: `_FakeTopicLog.append()` should return
  its assigned offset while holding its existing lock.
* `:116`: `_FakeProducer.produce()` should pass a `_FakeMessage` with the
  appended index-log offset to the success callback; currently it never calls
  the callback at all.
* `:657`: `_DeliveringProducer` currently passes `None` as the success message;
  let the corrected base producer deliver the callback and retain its flush
  counter without issuing a second callback.
* `tests/test_multi_topic_dlq.py:49`: `_AckingIndexProducer` currently passes an
  unconfigured `MagicMock` message. Delegate the callback through the corrected
  parent fake, fixing its transitive use in multi-pair retry/migration tests.
* `tests/test_single_pair_compat.py:148` and
  `tests/test_single_pair_multi_topic_compat.py:126`: wire-fixture capture
  replacements for `_publish()` return `None`; return assigned index-log
  coordinates while preserving captured wire key/value bytes exactly.

Negative producers reporting explicit callback errors can stay message-less.
Do not add a fake-only fallback that restores the unsafe production path.

Beyond the six baseline-red schedules, test equal own echo before return,
two confirmed local mutations installed in reverse order, newer foreign writes
after a pending local fence, unrelated keys within a partition, independent
partitions, producer acknowledgement not satisfying the read barrier, failed
and missing acknowledgements, partially successful bulk operations, and
EOF/compaction retirement when the exact echo is absent. Re-run the existing
index, multi-topic/multi-pair DLQ and retry, compatibility and migration suites,
then the full offline gate. An independent actual-diff review remains required.

## Subsequent author phase — separate from independent diagnosis

After the parent obtained independent approval of the offset-fence design,
this investigator was explicitly authorized to author the narrow repair and
its regression tests. No commit or push was performed. Root owns the separate
database fix and final artifact/image/service/full-suite gates. This section
records author verification, not an independent review of authored code.

The implementation modifies only `parked_pair_index.py` plus the four test files
listed in the fake-compatibility section. It validates successful callback
topic/partition/offset, rejects booleans and missing/invalid coordinates, retains
pending positions per `(index partition, wire key)`, and shares locked mutation
and consumed-position/pruning helpers. Equal echo or newer foreign replay that
already advanced the poller wins over a returning local caller. No broker
operation runs under the index state lock. A producer acknowledgement never
advances the consumed position. Bulk delivery preserves positional success and
failure results and one flush.

**45 new cases** were appended to the existing index test module: single/bulk
stale add replay; inverse stale tombstone; equal own echo and newer foreign
writes during flush for add/remove/bulk; older local installation resumed after
a newer acknowledgement both before and after its echo/fence retirement;
unrelated-key and barrier preservation; other-partition/equal-offset/EOF/later
record fence retirement; thirty missing/malformed acknowledgement combinations;
and a partially confirmed bulk flush. Positive fakes now report actual assigned
offsets and real-shaped messages. Compatibility capture helpers return delivery
positions while their expected wire fixture bytes remain unchanged.

Author commands and results, using the same interpreter:

* `-m pytest -q tests/test_parked_pair_index.py`: **84 passed in 0.77s**,
  `repaired-index-suite.log`.
* Existing index/multi-topic/compatibility suites before new cases were added:
  **69 passed in 1.17s**, `initial-fix.log`.
* `-m pytest -q tests/test_parked_pair_index.py tests/test_multi_topic_dlq.py
  tests/test_multi_pair_dlq.py tests/test_multi_pair_retry.py
  tests/test_dlq_migration_guards.py tests/test_retry_consumer.py
  tests/test_retry_pair_order_fuzz.py tests/test_single_pair_compat.py
  tests/test_single_pair_multi_topic_compat.py -m 'not integration and not smoke'`:
  **740 passed in 27.13s**, `repaired-focused-suite.log`.
* Targeted Ruff over the five authored files: **all checks passed**.
  `git diff --check`: clean.

The six pre-repair red regressions and all original failure/probe logs remain
preserved under the same scratch directory. The old probe helpers intentionally
assert baseline defects and are not a repaired-code acceptance suite.
`authored-inputs.sha256` captures the stable authored bytes sent for independent
actual-diff review; product SHA-256 is
`b6baf81ef4a11b4b33f1557ae4fd845ad222ad72cf4b718299c6e4b2d04959cc`.
The parent may begin its costly final gates on these stable bytes while the
separate reviewer inspects the actual diff. Any later edit requires updated
input identity and relevant refreshed checks.

## Fresh-review blocker in the first authored repair

The independent final reviewer reproduced an installation regression:
acknowledged local A at index offset 0 can be compacted by a still-unseen foreign
A at offset 2. If the consumer has applied only unrelated B at offset 1,
`_applied_position[0] == 2` does not prove that A's state was applied. The first
repair's global consumed-position guard incorrectly skips A's local installation,
leaving `contains(A)` and `is_blocked(A)` false until the foreign A arrives.
The inherited unconditional-install path retained the confirmed A in this case.

The reviewer preserved the red test and full failure at
`var/e06-final-revalidation-20261023/test_compacted_delayed_install.py` and
`compaction-red.log` (**1 failed in 0.04s**). This is a defect in the first
authored repair, not attribution to E06's original product changes. Its stable
hash and previous author-green results above are historical and superseded.
The parent has stopped acceptance of that source variant's costly final gates.

Proposed correction: track same-key valid consumed offsets and winning local
install offsets only for the lifetime of currently active local operations.
Register before producer I/O, keep shared per-key reference counts across
overlapping calls, and release in `finally` after installation or failure.
Unrelated/EOF progress and malformed ignored same-key values must not veto a
confirmed local installation. Pending replay fences still use consumed position
for retirement; same-key active history survives that retirement until the last
overlapping caller finishes. No unbounded historical tombstone map is proposed.
The precise amendment was sent to the parent for independent design review
before further product edits.

## Amended author phase — bounded active-operation evidence

The independent final reviewer approved the amended design before this author
phase resumed. This phase is author verification only. The fresh final reviewer
will independently inspect the actual amended source and tests; no earlier
design approval or first-candidate green result accepts these new bytes.

Each public add/remove call now registers its wire key before any producer I/O
and keeps that registration through its confirmed installation or failure, with
cleanup in `finally`. Bulk removal resolves every wire/pair key before
registration, counts duplicate keys separately, and releases every registration
on success or any exception. Overlapping callers share bounded per-wire-key
contexts holding valid applied offsets and winning local installation offsets
per index partition. The last caller removes the whole context, including
tombstone history. Producer `produce()` and `flush()` remain outside the state
lock; acknowledgements do not advance consumed positions.

An installation is vetoed only by actual valid same-key applied evidence at or
beyond its acknowledgement, or a newer same-key local installation/fence in the
same partition. Unrelated partition progress, EOF and ignored malformed values
do not supply same-key application evidence. Global consumed position is used
only to retire pending echo fences and avoid adding a new fence behind an
already-passed position. A newer local installation remains recorded while any
older overlapping caller is active, even after its pending fence is pruned.

The amended index module has **123 cases**, 84 more than the original 39 and
39 net additions over the superseded first repair's 84. The previous two
helper-only reverse-install tests were replaced by twelve schedules that pause
a real caller immediately before installation while its active-operation
lifetime remains open. Those schedules cover older add/remove/bulk calls and
newer state with no progress, same-key echo, unrelated later progress and EOF.
Other additions cover compacted-away add/remove/bulk echoes before only an
unrelated record is applied; no fence behind the consumed frontier; malformed
newer same-key values; same-key evidence arriving inside `produce()`; unlocked
producer I/O; duplicate bulk overlap and fence pruning; produce/flush/delivery/
installation failures; all-key precomputation failure; and final context cleanup
for missing/malformed acknowledgements and partial delivery. Positive wire
fixture expectations remain unchanged.

Interpreter: `var/e06-validation-20261022/dev/bin/python`. Actual author results:

* `-m pytest -q tests/test_parked_pair_index.py`: **123 passed in 1.14s**,
  `var/e06-index-revalidation-20261023/amended-index-suite.log`.
* The same nine-module focused command recorded in the first author phase:
  **779 passed in 26.72s**,
  `var/e06-index-revalidation-20261023/amended-focused-suite.log`.
* Targeted Ruff over the five authored files: **all checks passed**,
  `var/e06-index-revalidation-20261023/amended-ruff.log`.
* `git diff --check`: clean.

Stable amended source/test identities are recorded in
`var/e06-index-revalidation-20261023/amended-authored-inputs.sha256`:

| File | SHA-256 |
| --- | --- |
| `src/neo4j_graph_loader/loader/parked_pair_index.py` | `9e7e20cb91c4cbca576df9e5f6b0ae25badcc0d2763ad8ac537cf7c91a36ff07` |
| `tests/test_parked_pair_index.py` | `44e09c7ce2efe973f90822255798af5b757d05194dde5a9719fb3f5b6949da8d` |
| `tests/test_multi_topic_dlq.py` | `45f6929dc2fde995a3d8ce899774f32d5f583e861e81c6998921597fbf1c5129` |
| `tests/test_single_pair_compat.py` | `ccc62a6d19b09a98e3509c57c967df123396ed0dbfc328cfdd25bcbf901d22b8` |
| `tests/test_single_pair_multi_topic_compat.py` | `58c1d6cb2e5f3279d2199f29e278ca898106f17a2f305af3c13a29e7dd99d45a` |

The original full failure, six baseline-red regressions, first authored green
results and independent compaction-red result remain preserved. This author
phase started no real services, read no private environment settings, and made
no commit or push. Root owns the separate database repair and all renewed full,
artifact, image and installed-service proof.
