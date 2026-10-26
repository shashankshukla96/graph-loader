# E07 execution plan — 2026-10-23

Execute only [E07](epics/e07.md) in response to the supplied E07 handoff. Clean
entry is `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`, branch
`codex/pypi-library-checklist`. Latest accepted E06 repair is
`c15c43c5c4eb7c6fe6920489fd5835a91bf72377`; preserve its rejected repair,
compaction reproduction, acknowledgement/overlap controls, shared database
normalization and all original evidence. No applicable AGENTS.md was found.
No E08–E12 execution, push, upload, release tag, publication or workflow.

## Ordered implementation and independent gates

1. **F1.S1 — isolated runner.** Implement the prescribed
   `scripts/verify_installed.py --lane … --wheel … --inventory …` interface.
   Use new owned environments and staging outside the checkout/Git, an explicit
   finite test/helper/data allowlist, explicit pytest config and sanitized runtime
   environment. Never stage application sources, a Git tree, datasets or private
   files. Install only the explicit wheel and separately declared test tools.
   Check every installed application payload member, module origin and metadata;
   enforce and record origins in direct tests and actual Python children,
   including spawn and fork. Preserve output and bounded process-group failure
   supervision. S1 proves the mechanism with a small maintained probe, not a
   claim that unported lanes pass. Actual missing-module/resource wheels and
   contaminated import paths must fail. Fresh independent story review.
2. **F1.S3 — inventory before porting.** Collect exact current IDs including
   parametrization; compute recursive local imports plus explicit dynamic/data/
   conftest/config closure. Classify every test file under tests and example
   tests, retaining the full checkout suite. Record lane counts, staged closure,
   source-to-installed IDs, extra test dependencies and revised effort. No
   silent difficult-test exclusions. Initial CLI inventory contains a live
   Docker build case; separately review its actual-service disposition rather
   than execute it as a unit or give it a vacuous pass. Tests which scan source
   must scan the installed package with the same assertions. Add and review
   bounded lane/story splits before any overflow port. Freeze and review the
   collection/closure and disappearance/deselection negative controls.
3. **F1.S2 — core.** Run the enumerated schema/planner/encoding selection with
   maintained reference/golden/schema closure. Adapt only location assumptions;
   retain every selected ID/assertion and subprocess provenance. Record exact
   mapping and justified existing skips. Independent acceptance before safety.
4. **F1.S4 — safety.** Port coordination/retry/offset/index selection and explicit
   cross-test helpers, preserving outcome/fence/offset/known-counterexample
   assertions and all E06 revalidation regressions. Keep real-service guarantees
   owned by E09. Review each approved bounded split before the next one.
5. **F1.S5 — CLI/runtime.** Port selected CLI/process/shutdown/metrics tests with
   fake external services and installed child origins. Preserve genuine stop,
   failure, cleanup and metrics assertions. Source scans use installed payload,
   never empty staged src directories. Repeat actual omitted payload negatives.
   Independently review every approved bounded split.
6. **F2.S1 — rebuilt artifact parity.** Freeze source/environment, build strict
   isolated wheel/sdist, Twine strict, extract truly outside Git and rebuild;
   compare every wheel member. Run all accepted installed lanes independently
   against original/rebuilt wheels in clean environments, recording exact IDs,
   outcomes, origins, children, resources and pip checks. Keep test/runner/plan
   tooling outside runtime artifacts; document deliberate sdist inclusion policy.
   Fresh independent story review and final adversarial review.

## Preservation and final proof

### S3 approved overflow freeze — 2026-10-24

The first S6 execution exposed a startup-provenance race in deliberately
immediate worker termination. [Proposed bounded S11 repair](execution-e07-startup-provenance.md)
requires independent plan review before implementation and independent repair
acceptance before S6. Its rejected pytest-green/runner-red proof remains
retained; no origin waiver or cancellation-test change is approved.

The [independently accepted inventory evidence](evidence/E07.F1.S3.md) records five bounded stories
S6–S10 before F2.S1, without expanding E08/E09 runtime or live-service guarantees.
Each receives independent review after its frozen installed lane. Source scans
must remain nonvacuous against installed package bytes; the tracked-schema guard
must use the complete frozen schema set without a staged Git rescue. The only
pre-port legacy test edit fixes an independently testable collection side effect:
developer smoke settings no longer read private dotenv during import. Earlier
discovery loaded it without output; the evidence records that constraint breach.
All source/application behavior and E04–E06 regressions are preserved.

Never read/source/disclose private .env, contact IMDb192.168.1.75, remove foreign
resources or globally prune Docker. Use new owned output/tags/services. Preserve
E04 public API/framing/cancellation/10-second aggregate cleanup/R1 causes,
E05 five symlink-loop cases/resource lifetime/private persistent state, E06 exact
immutable selection/strict mount/coordinator/service checks and all accepted
parked-index acknowledgement/compaction/overlap regressions. No application
behavior, version, root export, namespace shim, dependency or ordering expansion
is planned. Eight runtime dependencies remain mandatory; version0.1.0a1.dev0,
release target0.1.0a1. Test tools are development/runner inputs only.

Create a fresh CPython3.12 editable development environment. Capture safe tracked
and relevant-untracked manifests, excluding secrets and self-hashed evidence.
Retain every original failure, fix, rerun and independent re-review. Required
full checkout/example suite, lint/parity/pip/whitespace, strict artifacts/true
outside-Git rebuild/two clean noneditable installs, four rebuilt immutable images
with actual read-only/no-source-mount probes, all12 separately supervised R1
controls and exact no-skip supervised B3 remain gates. Preserve original causes,
per-process hard supervision and exact-owned emergency cleanup. Required service
skips remain open; inherited known skips/warnings retain individual explanations.
Historical E06 counts are not fixed E07 totals.

A fresh zero-history reviewer independently accepts each ordered story/split and
the final exact source/evidence. Author responses do not certify acceptance.
Only then complete statuses, render plan, validate links, inspect staging and
commit locally. Record the actual full delivery SHA and every tested/committed/
current byte, size and mode in a documentation-only follow-up; obtain that
follow-up's own SHA from Git history without recursive self-hashing. Prepare a
conditioned E08 handoff without executing it.
