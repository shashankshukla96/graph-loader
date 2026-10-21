# E04-R1 — observed source failure and blocked peer

Current disposition (2026-10-21): repair and final whole-proof review independently
accepted. [Fresh final review](../reviews/e04-r1-final-review.md) restores E04 S3/S4
completion after the new code, regression and full proof gates. E05 is not
implemented; its handoff awaits local delivery identity recording. The original
[changes-requested report](../reviews/e04-completion-verification-20261020.md)
and historical results/approvals remain unchanged.

[Plan](../execution-e04-r1.md) and [independent plan acceptance](../reviews/e04-r1-plan-review.md)
preceded product repair. Entry was clean `6508bd31779c49c0d1dd78f0ee242c91b9f9a9fc`.
Fresh owned CPython3.12 environment installed requirements-dev and editable package;
repository/revalidation environments and `.env` were preserved.

The original exact bounded reproduction was rerun before repair: default client
still waited at2.006s, with the ordinary error already received, blocked peer
entered, lock present and no marker. Finite timeout replaced that error with
`EdgePlanTimeoutError` at0.827s, cause None. Two deterministic new public tests
also failed before repair when timeout/cancellation became active immediately
after an error reply returned. Raw negative controls remain under
`var/e04-r1-20261020/before-repro` and `red-check.log`.

`_gather` now immediately raises a received non-ok reply with its original inline
cause or remote diagnostic cause, before checking interruption again or waiting
for another reply. Successful reply checks/order remain unchanged. No receive
multiplexing was added: failures behind an earlier blocked lane may remain
unobserved until its receive returns or cancellation/deadline interrupts it.
The public guide states this distinction explicitly.

New real public regressions use two actual spawned workers, coordinating failure
with the peer's entered-blocked-source file. Separate independent supervisors
bound default and finite-timeout clients and own emergency process-group cleanup.
Observation-only receive/close wrappers capture prompt cleanup entry and total
observation+aggregate-budget time. Tests assert ordinary cause/marker, both child/
endpoint deaths, no heartbeat/active child, released owned lock/spill and unchanged
caller sentinel/signal, before fixture emergency recovery. The two deterministic
controls preserve the exact original inline exception object. All existing
cancellation/timeout/partial-frame/backpressure and topic-ownership controls stay.

Author focused run:200 passed in17.96s. Independent source/test reviewer:157 passed
in15.75s, Ruff/whitespace passed, no actionable finding. Its independently run real
mixed cases entered cleanup within0.00015s of observing the failure and finished
in0.133/0.124s, reaped children/endpoints and preserved caller ownership.
[Scoped repair review](../reviews/e04-r1-repair-review.md) preserves exact identities
and executed controls; final fresh full/artifact/image/B3 evidence follows.

Fresh complete suite: **3,516 passed,4 inherited skips,49 deselected,19 known
A5/E08 fork warnings**,192.57s, exit0. Exact isolated B3:1 passed in24.88s,
no skip/timeout and no remaining containers, with the explicit fresh interpreter.
Ruff (including executable proof scripts), dependency/parity and whitespace pass.
The same original bounded reproduction now finishes default/finite cases in
0.268/0.275s with ordinary source diagnostic causes and source-bearing markers;
blocked peer entered and owned locks were reclaimed in both.

Isolated original sdist/wheel and no-Git extracted-sdist rebuild pass strict
Twine, complete52-module inventories and wheel-member byte equality. Two new
noneditable environments outside checkout pass installed origins, public/import
examples, old-import rejection,8 component helps and dependency checks. All four
new R1 component images pass52 isolated commands by immutable IDs, no mounts or
network rescue. One edge build initially hit a Docker Hub manifest HEAD EOF;
that log is preserved and only remaining edge/clock builds were resumed, without
source or Dockerfile edits. All four builds and required probes ultimately pass.

Additional checkout-free public mixed-source controls execute actual spawned
workers against BOTH clean wheel installs and EACH of the four new images,
with default/no timeout and finite timeout:12 cases pass. They use a standalone
client without checkout/test dependencies; independent process-group/container
supervision bounds them and only owned outputs/children/container IDs are cleaned.
Production resource/endpoint/child assertions run before fixture emergency cleanup.
These supplemental controls are not E07's full installed suite or E08/E09 matrices.

[Machine-readable results](e04-r1-results.json), [entry](e04-r1-entry.json),
[before](e04-r1-before.json)/[after](e04-r1-after.json) safe manifests,
[artifacts](e04-r1-artifacts.json)/[commands](e04-r1-artifact-commands.json),
[images](e04-r1-images.json)/[commands](e04-r1-image-commands.json) and
[12 installed mixed controls](e04-r1-installed-mixed.json) retain exact identities,
commands/exits and raw-record hashes. All259 source inputs and the fresh environment
are unchanged before/after the full checks: source
`3f00faeef6a887516b3501590226537e71ea1bd3a9ea1f1f76315fdf2c110a7f`, environment
`01e164fd8501e329de5a67ac77646f90a6ca3eb4c5825057afcf0c98162e3356`.
Final whole-proof independent acceptance and local delivery remain separate gates.


2026-10-21 resume: a usage limit interrupted the initial final-review attempt
before any final acceptance/report. A new zero-history final reviewer was
requested. A fresh identity capture confirms every one of the259 frozen source
inputs and the exact environment still matches; unchanged passing checks remain
applicable. No final acceptance is inferred from the interrupted review.


Final acceptance on2026-10-21 is based on the new zero-history final review, not
the usage-limited attempt. Canonical E04 S3/S4 completion and the E05 prerequisite
are restored; final render/link/staged checks and local delivery identity follow.
No E05 work or push/publication is included.


Local repair delivery: `aa03f2c0d71409a7ed46534ec57780e5bef966bf`.
[Postcommit proof](e04-r1-postcommit.json) confirms all259 tested delivery inputs
match actual committed blobs and current files in bytes, size and executable mode;
environment identity is unchanged. The following tracker/evidence commit records
the actual repair hash and changes no tested source/build/test inputs. Its own
hash is available in Git history without recursive bookkeeping.
