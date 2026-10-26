# Independent E07.F2.S1 review — 2026-10-25

**Disposition: accepted for rebuilt-sdist artifact parity. No actionable F2 finding remains.** Both candidate artifacts pass all eight frozen installed lanes with exact inventory dispositions and actual process provenance. A separate fresh final adversarial review and local delivery remain required; this report does not declare E07 complete or accept E09 service guarantees.

This fresh zero-history review read canonical E07, the execution plan, approved S3 overflow/disposition freeze, S11 repair acceptance, S10 acceptance, the frozen inventory, F2 narrative/machine evidence, build/inspection tooling, final build commands/logs and all sixteen installed raw receipt sets. Review used `codex/pypi-library-checklist`, HEAD `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. Only this report, reviewer-owned evidence under `var/e07-validation-20261025/f2-review/`, and new outside-Git temporary builds/environments were written. No source, test, assertion, runner, inventory or development environment was changed.

## Archives and independent rebuild

The author builds a finite public source copy outside the checkout, with actual Git discovery exiting **128**, then extracts the resulting sdist into another outside-Git source directory, repeats Git discovery **128**, and rebuilds its wheel. Both build logs show isolated build environments. Strict Twine succeeds for all three explicit archives. Persistent archive copies match their original files, sizes and hashes.

I independently extracted the retained sdist into a new reviewer-owned outside-Git directory, obtained actual Git discovery **128**, rebuilt its wheel in isolation and ran strict Twine on the original wheel, sdist, author-rebuilt wheel and reviewer-rebuilt wheel. Every one of the **65 wheel member names and bytes**, including RECORD and all six distribution files, is equal across all three wheels. ZIP container timestamps give the independently built wheel a different whole-file hash; the member-byte parity is exact. The reviewer wheel is persistently copied under `f2-review/artifacts/`.

Archive inspection independently matches the frozen contract: **57 Python modules, exactly two YAML resources, 59 application payload members, 65 total wheel members and 71 sdist files**. Source payload bytes equal current unchanged application bytes. All six frozen distribution metadata hashes remain exact. Metadata has version `0.1.0a1.dev0`, the eight mandatory runtime dependencies and no extras. The root exports only the version; the legacy `src` namespace is absent. Archive RECORD digests and sizes validate. Runtime artifacts contain no checkout tests, authoring tools, plan/review files, dotenv, datasets, private inputs or runtime state.

The finite `MANIFEST.in` deliberately excludes tests and runner/inventory tooling from the sdist. Frozen maintained test/helper bytes are separately staged by the checkout runner; application imports still come solely from the installed explicit wheel. This policy is stated in the F2 evidence and does not silently remove a selected installed regression.

I independently reran inspection against all **11 retained real archive mutations**: removed graph/social resources, removed entrypoints, unexpected wheel members, and the corresponding sdist omissions. All reject. Commands, strict build/Twine logs, inspections, retained reviewer wheel and negative-control reconciliation are in `rebuild.py`, `commands.json`, `rebuild.json`, `artifact-audit.json` and `independent-artifact-retention.json`.

## Clean installations and complete installed matrix

Two new reviewer bare environments install only the original wheel or reviewer-rebuilt wheel and runtime dependencies. Both pass pip check, all public application imports with exact target-site byte hashes, all eight component help commands, console/module CLI parity, old-namespace rejection, and all four maintained public import/resource/runtime examples. The probes reject unexpected import-time network/process/thread/logging side effects and confirm no editable install or test/build tooling. All 59 installed payload members and immutable metadata bytes independently equal the archives. Results are retained in `original-probe.json` and `independent-rebuilt-probe.json`.

The author's sixteen runs use **16 distinct new noneditable environments**, one per lane per candidate. I independently reconcile every full/selected test ID in order, exact approved exclusion/reason, setup/call/teardown outcome, allowed skip, final pytest receipt, command log digest, current runner/guard/receipt digest, staged input bytes, generated stage files, installed payload and metadata bytes, direct-wheel installation identity and install-time RECORD hashes/sizes. Real Git discovery in every stage exits128. There is no staged `src` or Git rescue. No receipt error, xfail, unapproved deselection, timeout, owned process leak or cleanup error occurs. Installed pytest logs contain no warning summary.

Each candidate has **3,650 selected IDs: 3,649 passes and the one exact inherited intentionally multi-pair schema-example skip**:

| Lane | Selected / full IDs | Outcomes per candidate | Actual children per candidate |
| --- | ---: | --- | ---: |
| core | 454 / 454 | 453 passed, one approved skip | 5 |
| safety | 1,335 / 1,335 | 1,335 passed | 10 |
| cli-runtime | 636 / 636 | 636 passed | 3 |
| public-contracts | 247 / 250 | 247 passed | 137 |
| writer-runtime | 540 / 541 | 540 passed | 0 |
| orchestration-runtime | 282 / 284 | 282 passed | 0 |
| example-contracts | 115 / 115 | 115 passed | 43 |
| service-helper-unit | 41 / 47 | 41 passed | 0 |

Across both candidates, **7,298 passes, two approved skips, 396 actual child launches and 6,018 module receipts** reconcile from raw records. Every child has matching PID/OS creation identity, actual parent, target-environment prefix and startup receipt after its pre-creation anchor within the accepted three-second bound. Every observed module origin and actual installed file hash matches the frozen wheel payload. Post-exec freshness checks pass. The two module-receipt totals differ by two import observations; each actual observation is independently valid. These are not substituted process counts or claims derived only from pytest totals. Full audit code/results are in `audit_matrix.py` and `matrix-audit.json`.

I additionally ran the unchanged prescribed runner's public-contracts lane against the independently rebuilt wheel. It passes in **32.59 seconds overall**: **247 passing selected IDs, all 250 full IDs, exactly three approved exclusions, 137 actual children, 138 regression process identities and 1,698 module receipts**. All phases, anchors, origins, staged/tool/payload/metadata bytes and install RECORD entries validate independently. There are no skips, xfails, warnings, receipt errors or cleanup failures. The independent command, copied raw receipts and verification are retained in `public-command.json`, `public-receipts/`, `verify_public.py` and `public-verification.json`.

## Evidence closure and remaining boundary

All **154 raw files** declared by `e07-f2.json` match retained sizes/hashes. Embedded artifact, archive-retention, full installed matrix, full-checkout, image, mixed-R1, service, B3 and negative-control evidence exactly matches its referenced retained file. Both per-candidate summaries independently match raw installed results. The F2 narrative reconciles with this proof. Its two disclosed evidence-assembler shape errors and the earlier full-suite harness rejection remain retained; no application/test change supplies their corrections.

I also reconciled the retained required gates without replacing the final independent acceptance: all **74 image input hashes**, four fresh immutable image identities, **76 image command/log hashes** with read-only/no-source-mount settings, four data-bind results, all **12 separate R1 log hashes/expected causes/cleanup observations**, all **12 service command logs** and seven negative outcomes, exact B3 **one pass/no skip**, and the **3,746-pass/four inherited skips/49 deselections/19 inherited warnings** full-checkout summary. The full-checkout audit reports zero blocked private/remote attempts. The exact inventory collection retains **3,799 IDs**. These checks are retained in `audit_retained_gates.py` and `retained-gates-audit.json`; broad E09 live guarantees remain open.

Independent safe before/after captures match author evidence exactly:

- Source: **307 inputs**, `f94b1e9e68d6bcb750752782d15c253a4b4fa28325d52ae09129c56e1e5adcf6`.
- Development environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.
- Both candidate wheel archives: `ac3500e2b6f6a3907b41e47c8acee0cf22f7fb4685414b046a6596ba80919700`.
- Sdist: `a802f3cdf1d42799393d662f05a3c2a152afd7a3e85e5246bd9d4c9c8698db15`.
- Reviewed F2 machine evidence: `f9c66e95df92d9e9b48b477f6f3c5f696749004b781bbbf83aac5e26bdaa576f`.
- Reviewed F2 narrative: `8761cb52a430cb45ce770deee7129978ca29b3db9ab383765be2fd87d0c3d442`.

`before.json`, `after.json`, the copied `reviewed-e07-f2.json` and `author-reconciliation.json` retain these identities. Application source has no HEAD diff; `git diff --check` passes. Prior S11/S1 protections remain unchanged and independently covered by the reviewed startup-repair acceptance; no new concern required replaying that complete mechanism matrix in this parity review.

No private environment was read, prohibited remote endpoint contacted, developer smoke run, foreign resource altered, Docker prune performed, or push/upload/tag/publication/E08+ work undertaken by this reviewer. F2 is accepted at the identities above. Separate fresh final adversarial acceptance and local delivery remain open.
