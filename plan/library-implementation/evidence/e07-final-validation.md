# E07 final validation — 2026-10-25

**Independently accepted for local delivery.** [Fresh final adversarial review](../reviews/e07-final-adversarial-review.md)
finds no actionable issue. All ordered stories, including [F2](../reviews/e07-f2-review.md),
are accepted. [F2 narrative](E07.F2.S1.md) and [machine proof](e07-f2.json) retain
exact validation. Local delivery and actual postcommit equality are the remaining
recording steps; no future SHA is invented.

## Result and scope

The reusable checkout tool `scripts/verify_installed.py` accepts explicit lane,
wheel and inventory arguments, stages a finite hash-bound test/helper/config
closure outside every Git ancestry, creates a fresh noneditable target
environment and records actual parent/child application origins and OS identities.
It rejects missing/changed payload or immutable distribution metadata, source
rescue, unsupported child launchers, lost pytest receipts, unapproved test losses,
startup provenance failures and owned cleanup failures. Output ownership is
exclusive; rejected foreign/existing paths retain their bytes and modes.

`test-inventory.json` freezes 3,799 collected IDs across 124 files: 3,650 installed
unit IDs, 48 explicit service IDs and 101 checkout development IDs. The original
full source/assertion baseline, recursive fixture/reference closure, exact
parametrized selections and reviewed exceptions remain recorded. Extra runtime
coverage was split into five approved bounded stories before porting.

Application source is unchanged from entry HEAD
`5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. Location adapters scan the actual
nonempty installed package and use frozen reference files. The CLI initialization
failure regression now additionally requires its intended database branch.
Original selected assertions and all E04–E06 behavior remain: public ownership,
framing/cancellation/deadlines/aggregate cleanup and first-received causes;
private persistent attempts/resources and five symlink-loop preflights; exact
image compatibility/pinning/topology/database normalization and parked-index
acknowledgement/compaction/overlap safety. No broader ordering promise is added.

## Independent ordered story gates

[S1 runner](../reviews/e07-s1-review.md) →
[S3 inventory and approved overflow](../reviews/e07-s3-review.md) →
[S2 core](../reviews/e07-s2-review.md) →
[S4 safety](../reviews/e07-s4-review.md) →
[S5 CLI](../reviews/e07-s5-review.md) →
[S11 startup repair](../reviews/e07-s11-repair-review.md) →
[S6 public](../reviews/e07-s6-review.md) →
[S7 writer](../reviews/e07-s7-review.md) →
[S8 orchestration](../reviews/e07-s8-review.md) →
[S9 examples](../reviews/e07-s9-review.md) →
[S10 service-helper units](../reviews/e07-s10-review.md).
[F2 acceptance](../reviews/e07-f2-review.md) is complete; final independent acceptance
remains a separate gate.

Each of the following selections ran against both fresh original and rebuilt
wheels in 16 distinct target environments:

| Lane | Selected per wheel | Actual outcome per wheel |
| --- | ---: | --- |
| core | 454 | 453 passed; one intentional multi-pair example skip |
| safety | 1,335 | 1,335 passed |
| CLI/runtime | 636 | 636 passed |
| public contracts | 247 | 247 passed |
| writer runtime | 540 | 540 passed |
| orchestration runtime | 282 | 282 passed |
| example contracts | 115 | 115 passed |
| service-helper units | 41 | 41 passed |
| Total | 3,650 | 3,649 passed; one approved skip |

Aggregate: **7,298 passes, two intentional core skips, 396 actual child launches
and 6,018 module-origin receipts**. Every run reconciles its own exact full/
selected IDs, documented exclusions, payload/stage/metadata bytes, parent/child
origins and cleanup. Scheduling can change actual child/module totals between
independent runs; receipts match actual processes rather than a fixed count.
The three public tool cases, one benchmark, two historical Git checks and six
live mixed-file cases retain the precise independently reviewed dispositions.

## Fresh artifact and infrastructure proof

- Isolated wheel/sdist; actual outside-Git discovery exit 128 before original
  build and extracted-sdist rebuild; strict Twine and archive inspection.
- All 65 wheel members byte-equal. Both wheel SHA-256 values are
  `ac3500e2b6f6a3907b41e47c8acee0cf22f7fb4685414b046a6596ba80919700`.
  Sdist SHA-256 is
  `a802f3cdf1d42799393d662f05a3c2a152afd7a3e85e5246bd9d4c9c8698db15`.
  Three archives are persistently retained; 11 actual archive mutations reject.
- Two clean bare noneditable runtime-only installs outside checkout pass public
  imports, command/parser/resources/example probes, origin bytes and pip check.
- Four actual no-cache rebuilt immutable image IDs, all distinct from E06,
  exact version/component/contract labels, 74 manifested build inputs. All 76
  image checks and four actual read-only-input/writable-output bind probes pass.
  Python uses site-packages, workdir `/`, read-only root and no source mount;
  images contain no test/build/IMDb tools. See the exact IDs in the machine proof.
- All 12 separately hard-supervised default/finite R1 controls pass across two
  bare installs and four images. The original client is byte-identical to E06;
  original first-received causes and uninstrumented timing/cleanup bounds remain.
- Tiny installed service proof passes with new owned Neo4j/Kafka/network,
  13 nodes/12 edges, offsets/plan/ledger, padded and blank database normalization,
  shared external plan/spill/timing paths, host/container lease readiness and
  seven expected credential/database/network/metadata/daemon-path controls.
  Owned application/infrastructure objects are removed; foreign objects are untouched.
- Exact supervised B3 reports one actual pass, no skip or timeout; before/after
  container inventories are empty. No IMDb load supplies this proof.

The finite 71-file sdist deliberately excludes checkout tests, runner, inventory,
plan, secrets, datasets and operational state. The separate checkout tool stages
reviewed test bytes; the wheel remains the sole application import source.
Runtime still has 57 Python modules, exactly two schema YAML resources, eight
mandatory dependencies, no extras, no legacy alias and root version only.
Package version remains `0.1.0a1.dev0`; release target remains `0.1.0a1`.
No registry digest, publication availability or anonymous pull has been established.

## Full checkout and limits

The independent checkout suite actually reports **3,746 passed, four inherited
skips, 49 deselected and 19 known warnings in 198.70 seconds**. The four skips are:

- Bash-only readonly SHELLOPTS behavior on this shell.
- Two historical Git-slice checks whose historical checkout condition is absent.
- The deliberately multi-pair schema example.

The 19 warnings are 15 subset-builder and four planner CPython multi-threaded-fork
deprecations, retained under A5/E08 without filtering. The isolated installed
lanes add no pytest warning or skip. Service-driver librdkafka connection-refused
diagnostics occur during expected negative/owned-broker teardown paths; graph,
offsets, exact phases and cleanup are verified before success. Full lint,
requirements parity, pip check, exact inventory and whitespace pass.

Excluded live cases remain open under E09; no required infrastructure skip is
counted as success. No E08–E12 work, remote IMDb contact, global Docker pruning,
foreign resource removal, push, upload, Git tag, publication or workflow is authorized.

## Retained findings and proof repairs

[Resolutions](../reviews/e07-resolutions.md) retain every actionable issue,
original rejection, author reproduction, fix, rerun and independent re-review.
S11's original interruption hole is closed by durable BaseException failure
records while preserving interruption types; all original E04 test/application
bytes remain. Early E07 discovery imported developer settings and read private
`.env` before that collection side effect was identified. No values were printed
or file changed. Lazy smoke-only settings and a blocked-read collection regression
repair it; subsequent audited collection/unit checks record no private-file read.

The first final-suite proof script re-entered pytest under multiprocessing; the
exact owned group was stopped, all recorded identities are gone, and the rejected
script/output is retained. Adding a main-process guard changes only that proof
script; the corrected complete suite passes. A duplicate cleanup of the already
empty rejected group produced a retained Darwin EPERM; the newer supervisor only
accepts it after verifying no live group members. The successful suite itself
exits 0. Evidence-assembler dict/list assumptions about a count field are also
retained; its corrected run independently rehashes all 59 payload files in all
16 installed environments. No application/test assertion was changed for these
proof-only errors.

## Exact identity and delivery gate

All relevant tracked and allowlisted untracked inputs are captured safely:
**307 files**, tested source
`f94b1e9e68d6bcb750752782d15c253a4b4fa28325d52ae09129c56e1e5adcf6`;
environment
`ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`;
inventory
`770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.
Current full-suite, S6–S10 and final before/after captures match; earlier stories
retain their own accepted tested snapshots. Private files, ignored
operational data and self-hashed evidence are excluded. Exact commands, exits,
origins, IDs and 154 hash-bound final receipt/proof files are referenced in
[e07-f2.json](e07-f2.json), with additional accepted story/reviewer proof retained.

Final independent acceptance is complete. E07 status is complete; render and
validate local links, inspect staging and commit locally. Then record the actual
full delivery SHA and per-file tested/committed/current bytes, sizes and Git modes
plus unchanged environment in a documentation-only follow-up. Resolve that
follow-up's own identity from Git history, without recursive self-hashing.
