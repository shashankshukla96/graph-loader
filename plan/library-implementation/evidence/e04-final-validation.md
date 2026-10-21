# E04 final validation and review

The six story gates are independently accepted in execution order; their evidence
and original findings/rechecks are retained beside this file. Whole-E04 is independently accepted in the
[fresh adversarial review](../reviews/e04-final-adversarial-review.md). The full suite passed3,512 tests, with
the same four inherited skips,49 deselections and19 known A5/E08 fork warnings
in187.36s; its exit code is0. Source was
frozen in `var/e04-validation-20261018/final-before.json` before full tests/builds;
the only subsequent source-manifest difference is corrected source-default
wording in `docs/LIBRARY_API.md`, independently accepted by the S2 reviewer.
No runtime/build/test/fixture changes followed that freeze.

Isolated original sdist/wheel, no-Git extracted rebuild, strict Twine and complete
wheel-member byte equality passed. Two new noneditable environments outside the
checkout passed all52 module origin/byte checks, metadata/dependency/no-dev-tool/
old-namespace checks,8 component helps and self-contained public schema/planning
success/error/cancel/deadline/import-effect probes. All four newly built image
matrices passed by immutable IDs without mounts/network/PYTHONPATH; each checks
all52 module bytes/site-packages origins,8 component helps, default help, pip
check and the same public/import examples. These are E04 smoke matrices, not
E07's installed full regression lane or E08/E09 platform/service acceptance.

The exact B3 regression passed1 test in24.08s, no skip, through the owned
supervisor with the explicit fresh dev interpreter. Initial image/B3 attempts
failed because Docker was stopped; those logs were retained, Docker started and
both required gates passed on retry. Successful B3 supervisor result records no
timeout and empty before/after owned-service inventory. No remote IMDb, foreign
resources, global prune or publication was used.

[Machine-readable results](e04-final-results.json),
[before manifest](e04-final-before.json), [after manifest](e04-final-source.json),
[artifact identities and probes](e04-artifacts.json),
[artifact commands](e04-artifact-commands.json),
[image identities and probes](e04-images.json) and
[image commands](e04-image-commands.json) retain exact identities/commands/exits.
The manifest mutation negative control changes the digest, and environment
identity remains `921610492c278551bfe4b511a52ec637cf7799b784fdf6ee832a91482d97f349`.
Ruff, dependency checks, requirements parity and whitespace passed.
The fresh reviewer independently passed24 focused tests,1,000 seeded codec
roundtrips and four extra corrupt controls, checked all source/artifact/image/log
identities and B3 semantics, and reported no actionable findings. Final plan
render/link checks and local commit/content-equality records close delivery. Raw logs/scripts/environments remain under ignored owned
`var/e04-validation-20261018`; selected safe results/manifests are retained in
tracked evidence rather than secret/dataset/log archives.


Final canonical statuses mark E04 complete after acceptance; E05–E12 remain
unstarted. The canonical renderer passed13 epics/26 features/54 stories and48
checklist coverage items with acyclic dependencies. Local links and whitespace
passed; exact link counts and commands are in the JSON delivery checks. The
conditioned E05 handoff is prepared without implementing E05. Local commit hash
and per-file content equality are recorded by the tracker follow-up.


Staged inspection caught one trailing space in the previously untracked public
API guide; it was removed. Reviewed and delivered guide hashes are recorded
explicitly in the JSON normalization entry. This whitespace-only documentation
change leaves every runtime/build/test/fixture and artifact byte unchanged.
The separate [delivery source manifest](e04-delivery-source.json) and postcommit proof preserve per-file
identity without pretending that the guide's reviewed hash is unchanged.


Local implementation/delivery commit: `55dd4938188768090c10d851f8f7330706975ea5`.
[Postcommit proof](e04-postcommit.json) confirms all259 delivery inputs match
committed blobs and the working tree in content, size and executable mode;
environment identity remains unchanged. The following tracker/evidence commit
records that actual hash and changes no tested source/build/test inputs. Its own
hash is available in Git history, avoiding recursive bookkeeping commits.
