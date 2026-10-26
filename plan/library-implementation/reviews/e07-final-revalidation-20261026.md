# E07 independent final delivered-code revalidation — 2026-10-26

**Approved for local delivery. No actionable finding remains in the actual E07
source, narrow repairs, complete fresh proof, machine evidence or reviewed final
observations/handoff scope. Actual local commit and postcommit equality are still
required; E08 execution and publication remain outside this acceptance.**

This fresh reviewer independently reads the actual E07 epic, execution and
revalidation plans, handoffs, runner/guard/pytest/inventory code and baseline
test changes. Entry HEAD is `91d765d18d5711402764bb5be51e3cd9c3bbed89`,
original E07 implementation is `9b1ddd2ff24f3d86b1a19aa35ea188c66e6155d7`,
and E06 baseline is `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`.
No applicable AGENTS.md was found. Prior review labels are not acceptance evidence.

The bounded tooling corrections fit the E07 acceptance criteria: the supervisor
retains the original BaseException, performs bounded process-group and exact
receipt-owned cleanup in its finally path, appends command cleanup/interruption
receipts, and preserves an owned failed main manifest before re-raising
KeyboardInterrupt/SystemExit. Unsupported actual `os.forkpty` fails through its
OS audit event before creating a child. Existing output protection and normal
cleanup fail-closed behavior remain. The five added checkout tests exercise real
SIGINT, cleanup error retention, main interruption identity and actual forkpty
rejection. Application behavior and supported launcher scope do not expand.

Independent checks:

- Own fresh runner and inventory control execution: **37 passed in 1.68 seconds**.
- Own guarded actual source collection: **3,804 IDs in 124 files**, exact frozen
  order and recomputed dispositions, zero private-read or network audit events.
- All 3,799 original IDs remain in relative order with identical dispositions;
  precisely five checkout-tool IDs are added. All eight installed lane objects,
  original assertion baselines, staged closure/import edges and hashes remain
  identical to entry. The old runner test AST is an exact prefix of the new one.
- All **307** final frozen input bytes/sizes/executable flags match current
  files. Application `src` is byte-identical to the E06 baseline. Current source
  identity is `995e8d9cf5a89fc31eddf8d5e8c80b633e91114483097d0627fa0997df726317`,
  environment identity `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`,
  inventory `ea0a618d1f62df26ddb616ceb7db162c6aa85fc63a85bfdcbb73a066c5fbbf93`.
- Direct strict inspection of both fresh wheels and the actual fresh sdist
  passes. All **65** wheel members and their bytes match across original and
  outside-Git rebuilt wheels; payload/distribution hashes match current inventory.
- All **154** original retained raw receipt files match recorded sizes/hashes.
  All **74** image candidate inputs match current bytes. Direct read-only daemon
  inspection confirms each of the four immutable image IDs, component/version/
  contract/source labels, Linux arm64 platform and entrypoint against actual
  retained image metadata. Historical image/service proof remains historical.

The source/fixture adaptations preserve original assertions: installed source
scans use the actual imported package; schema coverage enumerates the complete
frozen reference set; early-stop children use owned temporary cwd with fake
Kafka while importing only installed application code; CLI initialization-failure
coverage gains exact intended branch assertions. No runtime test is silently
reclassified. Eight lanes total 3,650 installed IDs; 48 service IDs remain E09,
and 106 current checkout development IDs retain explicit rationales.

Owned independent evidence is under `var/e07-final-audit-20261026/`:
`static_audit.py`, `static-audit.json`, `static-audit.log`, `collect.py`,
`collection.json`, `collection-audit.json`, `collection.log`, and
`runner-inventory.log`. This reviewer made no product/test changes, installation,
service/container/network mutation, private dotenv read, remote IMDb connection,
cleanup of foreign resources, commit or push.

## Independent audit of the completed fresh proof

All sixteen fresh original/rebuilt installed lane receipts now reconcile against
current inventory and current tools. Each has exact full and selected ID order,
exact explicit exclusion reasons, no unapproved deselection, no receipt error,
no xfail or failed phase, successful supervised commands and empty owned cleanup.
The aggregate is **7,298 passes and two approved core skips**; actual records
contain **396 child launches and 6,015 module-origin receipts**. Every real child
has matching PID/OS-creation, parent, launch-anchor and startup evidence; exec
requires a newer startup. Every module-origin path lies in its target environment
and matches wheel bytes. The final pip-check adds its own audited startup after
the test-process enumeration; that extra process is checked separately.

This reviewer directly rehashed all **944 installed payload files**, every
staged/generated file, each copied receipt/log, installed guard/plugin and current
runner tool hash across all sixteen environments. No staged application source
or Git ancestry exists. All recorded process identities have exited; no matching
live owned child remains. The independent audit is retained in
`fresh_proof_audit.py`, `fresh-proof-audit.json` and `fresh-proof-audit.log`.

The full checkout/example suite has **3,751 passed, four inherited skips,
49 deselected and 19 known fork warnings in 202.16 seconds**. Its external
supervisor exits 0 without timeout; private/remote guard records zero attempts.
The four skips are Bash-only SHELLOPTS behavior, two historical Git-slice
conditions and the deliberate multi-pair example. Fifteen subset/four planner
multithreaded-fork deprecations remain E08 work. They are not filtered or new
installed-lane skips. Independent capture and final before/after proof have the
same complete307 source records and complete environment.

Fresh read-only image checks retain **76 actual successful commands**, each
hash-bound log using immutable IDs, read-only root, workdir `/`, no source mounts
and no network. They prove all57 runtime module bytes and absence of test/build
packages. Four actual binds prove EROFS for schema/plan, writable owned outputs
and unchanged resource bytes. Twelve independently supervised R1 cases use the
byte-identical historical client, with actual original/rebuilt bare targets and
four images, default/finite modes, original failure cause, dead children, closed
endpoints and reclaimed state. Fresh image execution reuses the four independently
inspected immutable E07 images after all74 inputs match; copied historical build
records are explicitly not new builds.

Exact B3 executes the named single test and reports **one actual pass**, no skip,
exit0, no timeout and22.705 supervisor seconds; before/after inventories are empty.
New tiny installed service proof verifies13 nodes/12 exact edges, source offsets,
plan/ledger, named padded database load, blank-default actual preflight, external
shared-plan/spill/timing paths, host/container lease readiness, seven expected
negative cases and12 exact command log hashes. The blank-default case is preflight,
tiny inputs do not force spill, and padded load reuses node consumer groups; the
machine-proof limits preserve those distinctions. This reviewer independently
confirms exact owned infrastructure container/network identities are absent and
the actual daemon's final container inventory is empty. Expected broker-teardown
connection-refused diagnostics remain retained.

`infrastructure_audit.py`/`infrastructure-audit.json` retain these direct checks.
All **348** current machine-proof raw file bindings independently match actual
sizes/hashes; its embedded matrix, service, B3, full-suite and tested-input records
are exactly equal to the raw receipt/freeze objects (`machine-proof-audit.json`).
The retained348 proof files and154 historical files are distinct dated evidence;
original rejected/superseded outputs have not been overwritten or relabeled.


## Final document and scope verdict

The completed current [completion report](e07-completion-verification-20261026.md),
[machine evidence](../evidence/e07-revalidation-20261026.json),
[execution observations](../execution-e07-revalidation.md), tracker/README
qualifications and conditioned [E08 handoff](../handoff-e08.md) accurately
distinguish final repaired proof from original historical acceptance. They retain
the two reproduced defects, additive five-case regression scope, exact latest
counts, immutable-image reuse, actual fresh service checks, dated platform limits
and required next delivery/equality steps. The five new controls and3,804-ID
inventory are preserved for the next agent. No E08–E12, general E09 closure or
registry/publication claim is introduced.

This fresh independent review **accepts the complete current E07 implementation,
repair/inventory assertions and final execution proof for local delivery**. The
parent may update pending-review qualifiers to this accepted verdict while
keeping actual delivery/equality pending until those actions occur. Source
changes would require refreshed affected proof; documentation-only recording of
this verdict and actual future delivery facts does not change tested inputs.
Stage only the reviewed E07 tooling, additive checkout controls, refreshed
inventory and associated plan/report/evidence/handoff files. Retain every
original red/superseded output. No push, upload, tag or publication is approved.

## Precommit scope approval

The final accepted/local-delivery-pending qualifiers, current3,804-ID/106-checkout
handoff, current3,751-pass narrative and historical-image-build distinction are
consistent with the audited source and proof. Exactly **14** changed/new files
are in scope: the two tooling repairs, additive runner tests, refreshed inventory,
execution plan, machine evidence, four current reviews, tracker/README,
historical-validation qualification and E08 handoff. There are no application,
version, dependency or unrelated changes. All307 frozen bytes/sizes/executable
flags and all348 proof size/hash bindings still match; whitespace is clean.
The index was empty at this inspection. Staging and local commit of these exact
paths are approved; actual commit/current/tested equality remains the next gate.
The exact path list and inspected hashes are in
`var/e07-final-audit-20261026/precommit-audit.json`.
