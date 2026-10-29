# E08 final harness focused review — 2026-10-28

**Current verdict: v4 prepared final harness accepted in the [fourth repair re-review](#fourth-repair-re-review).**
All earlier open findings and superseded acceptances below are historical evidence.
This accepts the tooling only; actual gate results and final E08 acceptance remain separate.

**Original disposition: two actionable harness findings; prepared final tooling is not yet accepted.** This is an independent zero-history review of the eleven named ignored raw helpers only. It does not accept implementation, S2, E08 or any final result. The execution plan and current E08.F2.S1 evidence require fresh actual native S2 acceptance before final execution.

No private environment file or existing IMDb dataset/service was accessed. No remote connection, Docker command, final gate, duplicate native run, package build/install, global cleanup, foreign-resource deletion, implementation/script edit or commit occurred. Only this report, one local control summary, and finite synthetic files/processes outside the checkout were created. The synthetic process was reclaimed after checking its OS creation identity.

## Actionable findings

1. **P1 — resolve the Docker executable before entering the clean controller environment.** The reviewed initial `image_driver.py`, `data_bind_proof.py` and `installed_mixed_matrix.py` pass literal `docker` into `matrix_docker.run_container`. Its `runtime_environment(Path('docker'))` supplies `PATH=.:/bin:/usr/bin`. On this machine the executable resolves through the original PATH to `/Users/shashank/.docker/bin/docker`, and cannot resolve through the controller PATH. Therefore every new Docker proof adapter fails before creating a container. A safe lookup control confirms both paths without executing Docker. Pass an explicit absolute executable or the already authorized explicit local-socket wrapper consistently, including ordinary image inspection. Retain/check the actual wrapper identity. During review the main agent began replacing these calls with an absolute `final/docker` wrapper path; that in-progress delta is not accepted here and needs focused re-review and a no-daemon synthetic executable control.

2. **P1 — guarantee installed-lane cleanup and a failure receipt on interruption.** `run_lane.py:15` starts `verify_installed.py` in its own session, but lines 16–19 reclaim that group and provenance only for `TimeoutExpired`. `KeyboardInterrupt` or enclosing-controller termination can kill the wrapper while leaving its separate runner session alive. `run_all_installed.py` itself uses `subprocess.run(timeout=1520)` for the wrapper, so that timeout can trigger this exact escape. The outer `run_checked.py` group kill also cannot reach a runner that started a new session. An exact copied `run_lane.py` control changed only the `command` AST assignment to a finite owned sleeper, sent SIGINT to the wrapper, observed exit -2, observed its child still alive, and found no wrapper JSON receipt. The sleeper was then reclaimed by checked PID/creation identity. Use interruption-safe finally supervision, preserve the original cause, write a failed/interrupted receipt, and retain the checked cleanup result; the enclosing timeout/kill path must also account for the detached runner, rather than relying only on a finally block that SIGKILL cannot execute. Controls should cover ordinary exit, deadline, handled interruption and enclosing forced termination.

## Reviewed behavior and limits

The primary `build_probe.py` copies the finite package build closure outside Git, checks failed Git discovery before both builds, performs strict Twine validation, invokes the maintained archive verifier for both wheels, compares every member name and byte, and retains original wheel/sdist plus rebuilt wheel. The current package has 57 Python modules, two resources and six distribution members, yielding 65 wheel members. Its two new bare environments install the wheel only, pip-check, then run the maintained installed origin/import/CLI/resource/public-example probes. These are prepared paths, not new observed artifact/install proof.

`run_all_installed.py` enumerates the eight preserved lanes for each of the actual original and rebuilt wheel paths. `run_lane.py` explicitly supplies the current strict constraints, including pytest 9.1.1; the maintained runner applies constraints to the application and test-tool installation and verifies observed versions. The aggregate checks exact selected IDs, errors, inventory identity and provenance violations, and retains per-lane receipts. All sixteen actual lanes remain unexecuted by this reviewer.

For ordinary completed/failed invocations, the new image/bind/R1 adapters delegate to the maintained `matrix_docker` controller, whose CID, unique owner label, immutable image and recorded creation-time checks precede cleanup. Image probes specify immutable IDs, offline networking, read-only roots, private tmpfs and no source mount. Four explicit bind cases test two read-only inputs and two writable output directories, compare unchanged input/resource bytes and inspect actual written outputs. The unchanged mixed-client file is byte-identical to the accepted E07 client; the matrix still enumerates six installed identities times two modes, with individual host/container supervision. None of these static facts substitutes for their twelve actual R1 results or four image/bind results.

The service helper retains the bounded installed tiny fixture using new Neo4j 5.26 Enterprise/Kafka 7.7 testcontainers, synthetic topics/database/schema, graph/offset/plan/ledger checks, lease host/container proof and explicit negative outcomes. It constructs service settings rather than loading developer dotenv. Its caller must supply the candidate image manifest schema it actually consumes (`local_tag`) and the matching fresh installed interpreter; the final image-results schema uses `tag` instead. No actual service version, immutable application image selection, successful load or owned service removal is claimed by this review. Exact no-skip B3 and lint/parity/input/environment/final-adversarial gates still require their separately retained actual commands/results.

The full suite wrapper installs its private-dotenv/remote-address audit before importing pytest and retains the existing complete checkout/example marker selection. `run_checked.py` supplies the external process-group deadline; negative archive helpers preserve the original resource/entry-point/unexpected-input mutation controls for both wheels and the sdist. All eleven reviewed files parse successfully. No full suite or negative archive mutation was executed.

## Retained controls and snapshot identities

Control summary: `var/e08-completion-20261028/final-harness-review-controls.json`, SHA-256 `f9613d3153cf55d95a709fa473e045db2aec3554d767ca4e4c37aff6b98fa76d`. Its raw owned directory is recorded inside the summary. The initial Docker-path failure and run-lane interruption were reproduced without accessing a daemon or a real final lane.

The table records bytes at report creation. The Docker callers were already being edited by the main agent during review; their table entries therefore identify the pending wrapper-path revision, while finding 1 explicitly describes the initial literal-Docker revision. No acceptance attaches to an unreviewed concurrent delta.

| Final helper | SHA-256 |
| --- | --- |
| `build_probe.py` | `1aae102f8acf32a3c64f1fab14c48001a4449e9c89994092ff779a660e8829b2` |
| `run_checked.py` | `3b0ff17fdbb988e0cbbd0683bbaa6b7c97d5a5bc777230fe966838a70296f9a7` |
| `full_suite.py` | `44ed19156252af3562995055654e935e693246133a4ac1194d97cbb9369ccb21` |
| `run_lane.py` | `8038913f0f9e0e764aca523af49094f63945fe1600cf25a947411f4725b45717` |
| `run_all_installed.py` | `9df56080ca1b3e68440263a488f61f2d62f20966f34fcb29dcc4f04b3c4f2815` |
| `image_driver.py` | `2a934760b3f470ec067896d2e8e5a7153e44d82b56d47c1291b711c758c9eba4` |
| `data_bind_proof.py` | `7adf3460876bdfb3aec72c1cde7bbf4e202011e86547e1bc24fdd30f42a63242` |
| `installed_mixed_matrix.py` | `e36a16779ecafdde3ff45bac9f75f581652900a744fdcf3fe4bce4d2184a283a` |
| `installed_mixed_client.py` | `2e6b3e30a65fbec4700d8ae68cd3b25691ea91e4a27385abd3b47155eb4527fd` |
| `service_proof.py` | `e7f2db9efb7043380c96a9658686b8976fe1bbc73bf0a6b1d39e878a1bbe26a0` |
| `negative_artifacts.py` | `eb7f59fb9132f21c9cba2f3f60b184f4b1cd284000513a4e6ad9ba4aaefdb27a` |


## Main-agent repair submission, pending independent re-review

The original two findings and synthetic red controls above remain unchanged.
The prepared Docker adapters now use the absolute owned `final/docker` wrapper
with its explicit local daemon socket, including ordinary image inspection.
A no-daemon absolute-executable stand-in succeeds under the same clean PATH.
`run_lane.py` now delegates external supervision to the maintained runner,
records intent before launch, retains receipts in finally on BaseException and
performs receipt-bound emergency child cleanup. SIGTERM becomes a preserved
KeyboardInterrupt for that wrapper. The enclosing final supervisor also reads
only its finite recorded owned lane/container intents after a failed/terminated
invocation, covering an inner wrapper that cannot run finally after SIGKILL.
All cleanup checks actual PID/creation or CID/owner/image/creation identity.

Actual synthetic ordinary/deadline/SIGINT/SIGTERM controls pass at
`var/e08-completion-20261028/final-harness-repair-controls.json`; all four actual
owned child identities are dead and wrapper receipts survive. The separate
`final-enclosing-recovery-control.json` forces a copied inner wrapper to ignore
TERM, lets the enclosing deadline kill it, then verifies recovery of the detached
receipt-bound synthetic child. That control passes and explicitly records the
missing inner-final receipt, actual child identity and surviving outer receipt.
No real final installed/image/service gate is executed by these controls.

The attempted reviewer continuation stopped at an account usage limit. This
submission and its controls are authored by the main agent, not independent
acceptance. Both repairs and the enclosing recovery delta require fresh focused
re-review before final execution. Actual current helper hashes are retained in
the repair-control manifest; subsequent enclosing recovery source and receipt
are retained in the named raw control directory.


## Focused repair re-review — first follow-up

**Disposition: finding 1 is resolved; finding 2 remains open for forced enclosing termination before installed provenance exists.** The exact current Docker callers use an absolute owned wrapper that invokes the absolute local Docker executable with the explicit authorized local socket. The retained executable stand-in control proves clean-environment absolute-path execution without daemon access. The four repaired lane controls reconcile their ordinary/deadline/SIGINT/SIGTERM outcomes, final receipts and command-log hashes. All retained raw file sizes/hashes and the six current helper identities in `final-harness-retention.json` independently verify. No full final gate, Docker or remote operation was executed in this follow-up.

The revised `run_lane.py` now correctly delegates ordinary/deadline/interruption cleanup to the maintained supervisor, handles SIGTERM as an interruption and writes its receipt in finally. The outer wrapper adds finite intent-based recovery. However its lane intent contains only the output path, not the detached runner's PID/OS creation identity. Its recovery calls only `cleanup_owned(output/provenance.jsonl)`. The real detached `verify_installed.py` controller starts before creating venv/pip/install state and before installing/launching the guard that supplies installed provenance; its own controller identity is not recorded there. Killing the enclosing lane wrapper during this early setup therefore leaves that controller session alive.

The supplied forced-enclosing control succeeds because its substituted child explicitly writes its own PID/creation identity into the installed provenance file. That proves recovery of an already recorded process, but misses the earlier real startup window. An independent additional control uses the same exact lane/outer sources, replaces only the child command with a finite sleeper that writes an external identity file, and makes the copied wrapper ignore SIGTERM to force the outer deadline's SIGKILL. It does **not** fabricate installed provenance. The outer command returns failure after its deadline, but the checked detached child remains alive and the installed provenance file is absent. This reviewer then reclaims only that synthetic child after confirming OS creation identity.

Persist the detached runner's own PID/OS creation identity immediately at launch (before waiting or installation), retain the intent independently of installed provenance, and have the enclosing recovery validate and reclaim that recorded runner/group as well as any later installed provenance. Re-run both the no-provenance startup control and the already recorded descendant control. Final execution remains conditioned on fresh actual native S2 acceptance.

| Follow-up input/evidence | SHA-256 |
| --- | --- |
| `var/e08-completion-20261028/final/run_lane.py` | `a5b35600680e6a584b6a90e9741e252974cb18da6c3ff6deeab5ac33ccdc3805` |
| `var/e08-completion-20261028/final/run_checked.py` | `11cddec0ce4002c855de7cd193becac6b0bde7bd61856c7272f494933775a34d` |
| `var/e08-completion-20261028/final/image_driver.py` | `49863c1949f0ee43283db2f938fc2d810dcc7f96eb2bf17f69c58cabc4acc016` |
| `var/e08-completion-20261028/final/data_bind_proof.py` | `269ffefd3a03328856ea97b20f4f6f9fc0d14ea461989223842b72037b4cb162` |
| `var/e08-completion-20261028/final/installed_mixed_matrix.py` | `3cde83f58dfc95e7dfb0efb7d79f839a44b1b3b01e21372ed4a76fc71fbaffab` |
| `var/e08-completion-20261028/final/docker` | `2d1058d9e2057833a326f858d1841d11a5d2e7d572826d8966f62d4a5f2ea92c` |
| `var/e08-completion-20261028/final-harness-repair-controls.json` | `a2daa418cc66befc04fa6996b99b793111815d594fbb14479613ad2350c977bf` |
| `var/e08-completion-20261028/final-enclosing-recovery-control.json` | `701622a190c3391128d2e46a6035da4687c2cc155679bd04092e3dcab8106d33` |
| `var/e08-completion-20261028/final-harness-retention.json` | `94984cdf264a1f78fd5a5334649a3f07a0270c0c2306a988b4da0a3a403a3592` |
| `var/e08-completion-20261028/final-no-provenance-review-control.json` | `66b196b0be0b0903da7ad067dc1bb854d591c43dca0324f83726d9bd0ec34e58` |


## Second repair re-review — durable launch handshake

**Disposition: the no-provenance forced-enclosing startup window is repaired; finding 2 remains open for a graceful lane interruption with a separate-session setup descendant.** The reviewed v2 handshake atomically records actual launcher PID/OS creation/parent/owner/command and requires parent verification, persisted intent and matching atomic acknowledgement before exec. The four v2 ordinary/deadline/SIGINT/SIGTERM controls reconcile exact startup/acknowledgement/intent identities and successful owned cleanup. The supplied forced-enclosing v2 control now reaches a real recorded controller with one actual OS-observed separate-session child, creates no installed provenance and proves both dead after outer recovery. The outer remains failed after its deadline; no successful final gate is inferred.

An independent actual `owned_launch.py` pre-acknowledgement control registers its real parent and launcher, lets the parent exit without an acknowledgement, and confirms the launcher exits without executing a command that would create a sentinel. This validates refusal to exec after pre-ack parent death without a real final controller or daemon access.

One reachable gap remains: v2 `run_lane.py` terminates the registered controller's group without first capturing its actual descendants. The real installed controller's setup launches venv/pip through the maintained supervisor, which creates separate sessions. On SIGINT/SIGTERM of the lane wrapper, it can kill that controller first and orphan such a setup subprocess before installed provenance exists. When the outer wrapper subsequently recovers, its runner lookup is already absent, so it can no longer discover the orphan.

An independent control substitutes a finite controller that starts one actual separate-session sleeper and writes only external identity evidence. The real launch handshake completes. Sending SIGINT to the copied lane wrapper yields an outer failure with the controller dead, the separate-session descendant still alive and no installed provenance. The outer receipt explicitly reports the controller already absent. This reviewer manually reclaims the remaining synthetic descendant after checking OS creation identity. The copied lane changes only the finite command assignment; all production cleanup and outer recovery remain exercised.

Before terminating the controller in the lane's own finally, capture and persist the actual descendant PID/OS creation identities, and reclaim those checked identities. The outer recovery must read persisted descendant identities even if the controller already exited. Retain a no-installed-provenance controller-plus-detached-child lane SIGINT/SIGTERM control in addition to the forced-enclosing control. Only prepared final harness scope is under review; S2 and all real final gates remain unaccepted. The main agent began a third repair while this follow-up was written; the v2 identities below identify the actual reviewed sources, not that pending concurrent delta.

| Exact reviewed v2 helper | SHA-256 |
| --- | --- |
| `run_lane.py` | `86d106be4a11a55a3004d658a201ed37612b57def491522fb63a374d81ffab0e` |
| `run_checked.py` | `b341c38cdbeecd33c273fa710f3ba196671c8d0e886aa1ac1f3e02db9a7d9dca` |
| `var/e08-completion-20261028/final/owned_launch.py` | `e71d1116af89841569ea8a64fbff6e7ecf7a5133f457bed578bf4edea30962e9` |
| `var/e08-completion-20261028/final-harness-repair-v2-controls.json` | `e1342b0c9bed48a9f03b7166d7c1eb62b944994ac76d2cae5c8769e51dd00bd6` |
| `var/e08-completion-20261028/final-no-provenance-repair-control.json` | `90270c61a365aad791553e4d9c50ad57e978bbef8ab1c41ffc23560cca601605` |
| `var/e08-completion-20261028/final-interrupt-descendant-review-control.json` | `40aa3af4ec32b322f1e5584bbfdace03b9d161489623fff607cb52962b17a6b3` |
| `var/e08-completion-20261028/final-preack-parent-death-review-control.json` | `a932e27be125bb16e5bdd3fd3b96e03905dd32f63ca2e6e6100b7ea29603474e` |


## Third repair re-review — accepted prepared harness

**Current disposition: accept the exact prepared final harness below; both actionable findings are resolved.** This supersedes the earlier open harness dispositions, while preserving every original red control and intermediate repair result. It accepts authoring tooling only. Fresh actual native S2 acceptance must still precede every real final execution; no implementation, S2, E08 story, artifact, installed-lane, image, bind, R1, B3 or live-service result is accepted by this review.

The lane now snapshots actual transitive OS descendants before terminating its recorded controller, atomically persists each PID/OS creation identity into the owned intent and reclaims those checked identities independently of installed provenance. The outer also checks those persisted descendants after the controller has exited. The retained SIGINT and SIGTERM controller-plus-separate-session-child controls have only the finite command assignment changed from production. Both retain failure causes and inner/outer receipts, persist the actual child identity before controller termination, create no installed provenance and leave both controller and child dead. Their copied-source ASTs, startup/acknowledgement identities, persisted descendants, failure receipts and outer persisted-identity checks independently reconcile.

I independently repeated the formerly red graceful SIGINT case against the exact current lane, launcher and outer wrapper. The copied lane again changes only the finite command assignment. Actual controller and separate-session child are both dead after the outer returns failure, and no installed provenance was manufactured. `final-interrupt-descendant-independent-v3.json` retains this observation. The v3 ordinary/deadline/SIGINT/SIGTERM controls and forced-enclosing no-provenance control also reconcile current helper hashes and raw command/log/identity evidence. The forced copy differs only in its finite command and deliberate ignored TERM; it exercises the real outer forced-kill and actual-descendant recovery paths. No real final lane or Docker action was run.

The unchanged owned launcher still refuses execution without a matching acknowledgement and live original parent identity. The retained no-ack, bad-ack and pre-ack parent-death controls match their actual startup records and absent execution sentinels. The independent pre-ack parent-death control from the prior follow-up remains valid for the same launcher hash. The absolute owned Docker wrapper repair remains accepted; the task-only outer environment selects that wrapper and explicit local socket without rewriting global Docker configuration. The original uninstrumented R1 client bytes remain unchanged.

All current final Python helper sources parse successfully. Static boundaries and remaining real-gate requirements described above remain in force. This follow-up changed only the report and added one finite synthetic observation; it did not read private dotenv/IMDb state, connect remotely, invoke Docker, alter helpers/implementation or create a commit.

| Exact accepted prepared helper | SHA-256 |
| --- | --- |
| `var/e08-completion-20261028/final/build_probe.py` | `1aae102f8acf32a3c64f1fab14c48001a4449e9c89994092ff779a660e8829b2` |
| `var/e08-completion-20261028/final/data_bind_proof.py` | `269ffefd3a03328856ea97b20f4f6f9fc0d14ea461989223842b72037b4cb162` |
| `var/e08-completion-20261028/final/full_suite.py` | `44ed19156252af3562995055654e935e693246133a4ac1194d97cbb9369ccb21` |
| `var/e08-completion-20261028/final/image_driver.py` | `49863c1949f0ee43283db2f938fc2d810dcc7f96eb2bf17f69c58cabc4acc016` |
| `var/e08-completion-20261028/final/installed_mixed_client.py` | `2e6b3e30a65fbec4700d8ae68cd3b25691ea91e4a27385abd3b47155eb4527fd` |
| `var/e08-completion-20261028/final/installed_mixed_matrix.py` | `3cde83f58dfc95e7dfb0efb7d79f839a44b1b3b01e21372ed4a76fc71fbaffab` |
| `var/e08-completion-20261028/final/negative_artifacts.py` | `eb7f59fb9132f21c9cba2f3f60b184f4b1cd284000513a4e6ad9ba4aaefdb27a` |
| `var/e08-completion-20261028/final/owned_launch.py` | `e71d1116af89841569ea8a64fbff6e7ecf7a5133f457bed578bf4edea30962e9` |
| `var/e08-completion-20261028/final/run_all_installed.py` | `9df56080ca1b3e68440263a488f61f2d62f20966f34fcb29dcc4f04b3c4f2815` |
| `var/e08-completion-20261028/final/run_checked.py` | `37094ed87ce5fb2f4c6344c3a62ecfe374a4308931ea45b8426127138c8df24a` |
| `var/e08-completion-20261028/final/run_lane.py` | `4fd902a405e2ec99d4b93bd3f696762088664608c255c6df8a555d3a050ab4e4` |
| `var/e08-completion-20261028/final/service_proof.py` | `e7f2db9efb7043380c96a9658686b8976fe1bbc73bf0a6b1d39e878a1bbe26a0` |
| `var/e08-completion-20261028/final/docker` | `2d1058d9e2057833a326f858d1841d11a5d2e7d572826d8966f62d4a5f2ea92c` |

| Exact reviewed repair evidence | SHA-256 |
| --- | --- |
| `var/e08-completion-20261028/final-interrupt-descendant-repair-controls.json` | `1000bf1d11f1e1e8ae6c156de7f8549600e7a8f16a1de54f696d94b56c98c075` |
| `var/e08-completion-20261028/final-harness-repair-v3-controls.json` | `70791eb76f488009d350b5eb2e6d2893182bb0cfee2b7eeab01490209a9835c8` |
| `var/e08-completion-20261028/final-no-provenance-repair-v3-control.json` | `ed2b4943113ea344cfc9ca190d26016823c15ba7a04b94d4f7b4cecdf8f91bc5` |
| `var/e08-completion-20261028/launch-handshake-controls.json` | `1b3d937f4c8e400d1100ddb54cd002c1f708e3a3371c26bfa29689c6f7069d88` |
| `var/e08-completion-20261028/final-interrupt-descendant-independent-v3.json` | `a839fa5d9e1a4cd9555488ad562af79a362668d74cb83ecfda2b1c89463fb6f4` |
| `var/e08-completion-20261028/final-preack-parent-death-review-control.json` | `a932e27be125bb16e5bdd3fd3b96e03905dd32f63ca2e6e6100b7ea29603474e` |


## Fourth repair re-review

**Disposition: accept the exact v4 prepared final harness and fresh retry driver below. No remaining actionable finding in this focused scope.** This supersedes the third tooling acceptance. It does not accept the actual full-suite/artifact/install/native/service/image/R1/B3 results or E08 completion; those require their separate result reviews. The final-gate prerequisite remains actual fresh S2 acceptance, tracked separately.

An actual final attempt exposed an additional v3 harness defect: a failed image-build gate recovered healthy installed-core controllers belonging to a concurrent gate. I independently matched both recovered PID/OS creation/owner/command identities in the failed image gate receipt to the two actual core startup records and their -15 lane exits. The image build's original log retains the unavailable `docker-credential-desktop` error under its clean PATH. These are retained failed attempts, not product regressions or successful gates. Their original helper copies, logs and associations remain preserved.

V4 assigns a fresh owner to each outer gate and propagates it only in that command's environment. Lane and image/bind/R1 container intents record this owner before launch. Both outer recovery loops reject every nonmatching owner before inspecting/signaling a process or inspecting/removing a container. The actual two-gate control proves a failed gate has empty cleanup lists while a concurrently live controller under another owner remains alive and its gate later exits zero. I independently repeated this with the exact current outer/lane/launch helpers and a command-only synthetic lane substitution: the failing gate exits one, does no lane/container cleanup, and the different-owner healthy gate completes zero. Its separate owner and actual controller identity are retained in `gate-isolation-independent-v4.json`. No real final lane, Docker build/service/action or remote operation was run by this reviewer.

The Docker wrapper now selects the explicit authorized local socket and an owned empty Docker configuration directory. I verified the directory is mode 0700, its literal empty JSON file is mode 0600, and both wrapper and SDK environment use that path. This removes dependence on the user's credential-helper configuration without changing global Docker state or reading that configuration. The retained clean-environment, read-only daemon-info command exits zero and its log hash matches. This proves daemon access with the owned configuration; a successful new image build remains an actual retry gate, not a claim from daemon-info.

All current helper sizes/hashes match `final-harness-v4-manifest.json`. The v4 ordinary/deadline/SIGINT/SIGTERM controls, forced enclosing no-installed-provenance controller-plus-separate-session-child control, and graceful SIGINT/SIGTERM detached-child controls reconcile their exact current helper identities, startup/acknowledgement/owner records, raw log hashes, retained failure causes and dead actual process identities. Copied-lane ASTs differ only by the declared finite command/deadline/ignored-TERM control substitutions. Same-gate forced and graceful recovery remain exercised after adding the isolation filter. The unchanged launcher retains the accepted no-ack/bad-ack/pre-ack-parent-death boundary proof.

`run_all_installed_v2.py` changes only fresh lane labels and matrix/receipt output names from the accepted dual-wheel driver; it still runs all eight lanes for each original/rebuilt wheel with unchanged constraints/assertions. The image driver changes to the fresh `candidates-v2` manifest/log paths. Historical failed labels and outputs are preserved. All reviewed Python helpers parse. This follow-up edits only this report and adds one finite independent synthetic isolation observation; no implementation/helper/private-data or real-gate work is performed.

| Exact accepted v4 prepared input | SHA-256 |
| --- | --- |
| `var/e08-completion-20261028/final/run_checked.py` | `cb20f23fd6756b4b4e29f694c1a61fb4528b24ef91fd9eba62662a2cfb55e9bd` |
| `var/e08-completion-20261028/final/run_lane.py` | `bdb1b1dc06ac32d1a2bbbe26bf7a38298dcb9ba93f9f27f8e103f45cc30b7e91` |
| `var/e08-completion-20261028/final/owned_launch.py` | `e71d1116af89841569ea8a64fbff6e7ecf7a5133f457bed578bf4edea30962e9` |
| `var/e08-completion-20261028/final/image_driver.py` | `d68fc468cb4f4009d92af44b75bff6d7f5f1cbc9b2ec6b268396d52e1fa6aed2` |
| `var/e08-completion-20261028/final/data_bind_proof.py` | `8f8124f1709b61c4b5b08aa13cf94018f04b1f10f29d22ecc3be1ceca1ae7548` |
| `var/e08-completion-20261028/final/installed_mixed_matrix.py` | `d000dcbf1a33dbcc1c1bf2afb6892022f441672cd9c328ec4717d52ff88013c5` |
| `var/e08-completion-20261028/final/run_all_installed_v2.py` | `0c660a44c32c08e4ad59daea0c92fed69036ae2e96f45ad7f22266ae2c250848` |
| `var/e08-completion-20261028/final/docker` | `800d43073716b1d438981f18e30788e2059a8ec01d67c08df43b34df5f9ed8d0` |
| `var/e08-completion-20261028/final/docker-config/config.json` | `ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356` |

| Exact reviewed v4 evidence | SHA-256 |
| --- | --- |
| `var/e08-completion-20261028/final-harness-v4-manifest.json` | `3d31d8f69ef83c51d158633be0a5d9fe7cd1a556790ea74eec7d3eab48dde30a` |
| `var/e08-completion-20261028/gate-isolation-original-red.json` | `f3921bfbf663c5704af54a1f6c4daa6906c6856a1457400c311006664c887958` |
| `var/e08-completion-20261028/gate-isolation-repair-controls.json` | `f6f9c26d749e8b573069b7982291876c4834a435ea6599d6cc32833ed7fbb97c` |
| `var/e08-completion-20261028/gate-isolation-independent-v4.json` | `a7186f324bba124ad81db88895998a77966d0243322915903b0cfc8203e9095c` |
| `var/e08-completion-20261028/final-harness-repair-v4-controls.json` | `4bd4ce77ddaa9f7ee2f84f1c27b9a4d3feddff7be5bc2e4f83d566658542a0a0` |
| `var/e08-completion-20261028/final-no-provenance-repair-v4-control.json` | `b258656d66f9dec9651fe78e704fd2947f774358954653d63140e9b9833b35f3` |
| `var/e08-completion-20261028/final-interrupt-descendant-repair-v4-controls.json` | `b0e84ca55fa290382c06c093d699b37b9e1a7d366018f44e087b7c0ce9ea11cb` |
| `var/e08-completion-20261028/final/owned-config-daemon-commands.json` | `47ca19fb6b9b50ebdf068252be5750b95331e60112d59c85a8397d5c4530d5dc` |
| `var/e08-completion-20261028/final/owned-config-daemon-info.json` | `e2dae06f742c98aa2cc0df4e0bb430f487da11eb398ade2349f6356a5c1e5f31` |
