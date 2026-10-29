# Independent E08 S2 native harness review — 2026-10-28

**Disposition: accept the exact reviewed native harness for bounded execution
on the intended nonroot native arm64/amd64 runners. No actionable finding remains
in these inputs. This is harness acceptance only; native S2 observations, S2
acceptance and final E08 acceptance remain pending.**

The reviewer started with a task-only brief and no implementation conversation.
Reviewed `var/e08-completion-20261028/native_matrix.py` and `native_watchdog.py`
against the accepted S1 `remote_controller_v3.py`, `remote_watchdog.py`, their
[adapter review](e08-native-workspace-review-20261028.md), the current maintained
`matrix_docker.py`, `support_matrix.py`, `verify_installed.py` and dependency
helper, the execution scope and remaining process design. Independently inspected
the four frozen bundles under
`/private/tmp/graph-loader-e08-s2-contexts-20261028-v1/`. Only this report was
written. No source edit, SSH, Docker operation, test execution, private environment
read, service/data access, commit, upload or publication occurred. The outer
scripts and embedded Python client compile without execution.

## Inputs and native execution boundary

All **758 manifested files** in the four bundles match their hashes, without
symlinks or unexpected members. Each minimum bundle has 189 files and each
current bundle has 190. All bundles carry the exact current inventory (3,873
dispositions, 3,673 installed IDs in eight lanes), and the four maintained helpers
match the current reviewed bytes. All **141 staged test/fixture files** match both
the current inventory hashes and all four frozen bundles. The constraints,
wheelhouse manifests and every wheelhouse file are byte-identical to the
corresponding retained public S1 platform preparation. This reuse supplies frozen
installation inputs; the runner still creates fresh bare/installed environments
for every cell and cannot substitute an old observation for a new one.

The application wheel is the retained
`a804d26a8130ee7fcc06256327257b7a94476896bac6b151b68f1136c6b52391`
artifact. Test and client source is finite manifested reference material; there
is no checkout application-source mount. Each dependency set receives a new
UUID-qualified local image/tag, finite context manifest and owner label. The
actual inspected scalar `identity['Id']` is saved separately from Docker's build
IID receipt and used consistently for container creation and ownership checks.
This correctly accommodates an IID receipt that denotes a manifest identity.

Both hardcoded Python base digests are genuine Linux platform entries in the
retained `python-base-manifest.json` (SHA-256
`f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f`).
The controller requires a matching host CPU, Linux daemon CPU, base-image CPU
and built-image CPU. The maintained observation runner independently requires
Linux, CPython 3.12 and the exact target CPU inside the container. Retain actual
host/interpreter/daemon and deployment endpoint identities with execution proof;
these static checks alone do not establish an observed native cell.

## Deadlines, failure behavior and retained ownership

The finite loop executes bare plus eight installed lanes for each dependency
set: 18 cells per architecture. Only `public-contracts` receives the explicit
`--process-envelope` option, which selects 134 additional sequential cases in
each dependency set. The existing installed pytest deadline remains 1,800
seconds. Ordinary observation/wrapper/container limits remain
2,400/2,600/2,700 seconds. The explicit process-envelope lane uses
14,400/14,500/14,600 seconds outside the unchanged per-case supervision. These
are enclosing harness limits; they do not alter the original fault clocks or
aggregate cleanup assertions.

The independent run watchdog retains its stricter 7,200-second interrupt
deadline, 180-second kill escalation and 7,450-second outer wait. Consequently,
the extended per-container limit does not promise 14,600 seconds of execution:
the whole run can be interrupted earlier and must remain failed if incomplete.
No deadline or count is converted to a skip or successful partial aggregate.
The current inputs supply exactly eight lanes; final zero exit requires all 18
rows observed. Any command/build/volume/identity failure aborts or records a
failed cell. Interruption is retained and re-raised. Container cleanup error,
watchdog timeout, emergency removal or nonzero controller exit prevents the
watchdog's aggregate status from becoming `observed`.

Image ownership is saved before target creation. Every cell receives a unique
owner-qualified local volume, with inspected name/label/driver/creation identity
saved before launch. The image initializes `/evidence` with mode 1777; the
target runs under the intended host user's nonroot UID/GID with an offline
network, read-only root, private tmpfs, one native volume and one owned host
export bind. Deployment must retain its actual UID/GID, fresh root, endpoint
wrapper and launch bytes, as in the accepted S1 adapter proof.

The maintained controller records complete CIDs, initial image/label/creation
identity and raw command receipts. Normal cleanup and the independent watchdog
inspect only finite recorded CIDs and require matching immutable image and owner
identity before removal; recorded creation identity is checked when available.
There is no global list/prune, foreign-resource deletion, image removal or volume
removal. The watchdog delta only selects this controller, architecture argument
and the actual authoring interpreter directory in its clean PATH.

Normal child success or nonzero exit enters the finite snapshot export. The
exclusions remain exactly the top-level bare environment, installed environment
and directories named `__pycache__`. A failed or interrupted export cannot yield
successful container completion. As in accepted v3, the persistent volume keeps
all workspace bytes, including excluded environments/caches and malformed or
partial evidence, when hard termination prevents the export handler from
finishing. Neither cleanup path removes the volume. Preserve these volumes and
their identities until verified export equality or owned recovery; this review
does not assert a new reboot/OOM experiment or reopen any remote volume.

## Required observation review

Before accepting native S2 results, independently verify the actual deployed
harness/bundle identities; daemon/host/container architecture and Python;
nonroot execution; immutable image and context identities; all 18 cell receipts
and exact full/selected IDs; both complete 134-case process envelopes; actual
parent/child startup/creation/import origins; guard restoration; original clock,
cleanup and fault assertions; normal export equality or retained-volume
recovery; complete owned cleanup; and the final watchdog result. Historical S1
or local macOS observations cannot stand in for those current native receipts.
All final E08 artifact, dual-wheel, production-image, service, R1/B3, adversarial
review and delivery/equality gates remain in force.

| Exact reviewed input | SHA-256 |
| --- | --- |
| `var/e08-completion-20261028/native_matrix.py` | `e8edac1defe059832a747734ae17e83d1bed04707b16c10bf2997af104ad9ace` |
| `var/e08-completion-20261028/native_watchdog.py` | `4f974e0e9010a3ed6e247318e83eea1731ba80872431a547ead51ff7244c7b6e` |
| `var/e08-validation-20261028/remote_controller_v3.py` | `98ecc0d0de2f3d6cd31c9b2e29a9db235492e59f7c45be98a1368311ad88892a` |
| `var/e08-validation-20261028/remote_watchdog.py` | `57091f9666af43d553db58d179457febe01c9b8168968846a2ba379155f95158` |
| `scripts/matrix_docker.py` | `ad07f49198a68bf781e2adf9c8fe050f922b03c1d94a0a9fc4eb83c66aa7b370` |
| `scripts/support_matrix.py` | `848d02ea2371a7050f8b6129440d1e8a13f4f038ed534c099f6c229679813bf3` |
| `scripts/verify_installed.py` | `21f3c0ce10a6d830f07841571c5520e9879717443f504c396128728542367222` |
| `scripts/installed_dependencies.py` | `11434237aec2be294787d7aca60cc66f5446487a775fb0c4d13386b52b52543e` |
| Current/four-bundle `inventory.json` | `eb7611c6efd394cec21f8acc6132b4935e9eb438238e25757d7ffac673470c15` |

| Frozen bundle `bundle.json` | SHA-256 |
| --- | --- |
| `arm64-minimum` | `366aac8f5e66c5a8c75bb78980d231ed64a81147ab7c12aba8c7f366f7f7537e` |
| `arm64-current` | `9f77e4ae521278eaaf63d1bfa0b93b7f5537ebfbc6666135f5ecf73d59664dc4` |
| `amd64-minimum` | `985a74cd00620a7543e0a12fc4aa5351a203a0943b43c5557f165e7e1cf338a0` |
| `amd64-current` | `ac0abccacc44b9fc4b8542f0a4fe1f1e57be23ea272160aa1f282d620252dee3` |

## V2 frozen-input follow-up

**Disposition: accept the additive worker-count validation and exact v2 bundles
below for the same bounded native execution scope. Native results and S2
acceptance remain pending.** The harness and watchdog hashes remain identical to
the first review. No SSH, Docker action, test rerun or source edit occurred in
this follow-up; only this report was updated.

The sole portable file change from each v1 bundle is
`scripts/verify_installed.py`: ten lines derive actual process-worker PIDs from
the already validated client-parent `python_child` launch records containing
`--multiprocessing-fork`, excluding the resource tracker. They retain requested
parallelism separately, require zero process workers for inline, prohibit counts
above requested capacity, and require exactly the requested count for spawn
success/source-error/worker-exit. Nonpicklable submission can fail during initial
submission before all workers are created; the recorded partial count is allowed
only for that explicit case. Existing startup/parent/creation/environment/import,
failure and cleanup gates remain in force. The updated tool compiles.

Independently replayed all 42 external cases from the retained macOS v3 raw
client/provenance/result files and matched the saved worker-count replay. Every
count passes the new checks and each counted PID has the corresponding audited
parent/launch/creation startup identity. The nonpicklable requested-4 case has
one actual worker; requested-2 and requested-8 also have one, while requested-64
has 64. This corroborates honest partial-start reporting and is a receipt replay,
not execution of the updated runner or native S2 acceptance.

All 758 v2 manifested files under
`var/e08-completion-20261028/native-input-bundles-v2/` verify without symlinks or
unexpected members. Only the stated tool hash differs from v1; inventory,
141 staged files, constraints and wheels retain the previously checked bytes.
The launch manifest's canonical source hash recomputes correctly, and all 318
listed files checked against current bytes match; the public `.env.example`
entry was excluded from this follow-up's file reads. Independently verified the
87,028,386-byte amd64 input archive hash and every one of its 383 unique regular
files against the two exact v2 bundles and unchanged harness/watchdog, with no
extra or unsafe archive member. No extraction or remote operation was performed.

| Exact follow-up input | SHA-256 |
| --- | --- |
| `scripts/verify_installed.py` | `c915abcd7abc1740597dbf2988ecc58134bb0a48b6748442670609f2f16a2267` |
| V2 `arm64-minimum/bundle.json` | `51c14c57ed6049a53bbb788b104fa7d63c944fad0f2c7f56dd19725262a4ab78` |
| V2 `arm64-current/bundle.json` | `6bfadab94b3b153cc1c74f5c907e38527ff1542a691786b3cd69c3477a14ac5a` |
| V2 `amd64-minimum/bundle.json` | `d64030f86b8842648a90bd179559d41025e9a75ce0a7ffdabd49a1d4c60f387a` |
| V2 `amd64-current/bundle.json` | `2129fae9b5993c62ed945c82ca81d6681b9f1bc31e686f47ad694ac9fd1e66ba` |
| `native-launch-inputs-v2.json` | `a094f66b5ecabc6f74f5d7268565736955700418559a76beec04a60ef70ec15c` |
| Canonical `source` identity in launch manifest | `f1a7fb0f0ac83fdbe53bd75d0bb98299c4d0d1f4aa10cd39443d7ec372cc07ec` |
| `mac-v3-worker-count-replay.json` | `bc8c174a62b63234093aa02f2ddabe718b7c704467e001e7bd6a51e2613abdc1` |
| `native-amd64-inputs-v2.tar.gz` | `588a23753eebc24b2e051e3d7ebf57b2a45562dbb72be7eefd83468e173e174a` |

## macOS deadline portability repair

**Disposition: accept the exact portable deadline helper and watchdog below for
bounded local arm64 execution. No actionable finding. This repairs host tooling
portability only; actual native cells and S2 remain unaccepted.** Only this report
was changed during review; no SSH, Docker, test rerun, private environment access
or source mutation occurred.

The retained `native-arm64-v1/watchdog.log` reports `FileNotFoundError` for
`/usr/bin/timeout` at the watchdog's child launch. Its finite root contains no
dependency/controller/cell outputs. It is a retained failed prelaunch attempt,
not an observed matrix. The earlier GNU-wrapper acceptance applies to a host
where that utility exists; it does not establish macOS launch portability.

`native_watchdog_portable.py` differs from the reviewed watchdog on exactly its
child command: the same interpreter launches `native_deadline.py` instead of
GNU timeout. Environment, new private session/group, 7,450-second independent
outer wait, group SIGKILL/10-second reap, finite CID/image/owner/creation emergency
cleanup and aggregate failure gate remain unchanged. The new independent child
layer launches the controller in that private group, records PID/OS creation
identity before waiting, waits 7,200 seconds, sends the controller SIGINT, grants
180 seconds for ordinary cleanup, then kills/reaps it within ten seconds if
necessary. It writes its result in `finally`; timeout always exits 124 even if
the controller handles SIGINT and exits zero. Errors cannot yield a zero exit.
If this independent layer itself stalls, the unchanged outer layer kills its
owned group and performs the finite Docker cleanup. No test fixture, fault clock,
application source, codec, frozen v2 bundle or native controller changes.

Independently verified the retained normal, graceful-interrupt and forced-kill
controls, their PID/creation/result identities, outer command receipts and raw
log hashes. Their deadline-helper copies differ from production only by replacing
the 7,200/180-second waits with 0.2/0.2 seconds. Normal exits zero; interruption
records the child's graceful zero exit but the helper returns 124; ignored SIGINT
records child exit -9 and helper exit 124. All three record no internal errors.
These are bounded deadline controls, not full native execution proof. The
unchanged outer-group path was statically reviewed; this paragraph does not claim
an independently inspected new outer-group runtime control. Both production
Python sources compile without execution.

| Exact portability input under `var/e08-completion-20261028/` | SHA-256 |
| --- | --- |
| `native_deadline.py` | `1ae30284afe9a871349672e53c553bc75e7750dd9793b511503b3194e76c22e1` |
| `native_watchdog_portable.py` | `cb39fc3c862aa533b5a597e86eb015797cc2f4d9bca999ee5ab584a99f9acb70` |
| `check_portable_deadline.py` | `da72f7a5b733d08894ea6a7e93a028ee47762d00611e14afaa2803defa420945` |
| `portable-deadline-controls.json` | `5a32652f7381aeef380bbdc743f8a16bef7eae90e73e513a0829d44177724c18` |
| `portable-deadline-commands.json` | `30554ade07b6e89b4e5332307e02de9420dc373330564e89d7b39efb4d7b64c5` |

### Retained outer-group runtime control follow-up

**Disposition: accept the fourth bounded control of the outer watchdog's owned
process-group kill path. No actionable finding.** The production portable
watchdog and independent deadline helper retain the exact accepted hashes above.
Only this review report was updated; no new control/test, Docker operation,
remote connection, private-file read or active native-run inspection occurred.

Independently inspected `check_outer_deadline.py`,
`outer-deadline-control.json`, `outer-deadline-commands.json` and the raw retained
root `/private/tmp/e08-outer-deadline-rorxud6t/`. The copied watchdog differs from
production only in the outer wait, reduced from 7,450 to 0.3 seconds. Its synthetic
deadline helper launches one sleeping Python child in its inherited group, writes
that child's PID, then stalls. This deliberately exercises the outer protection
when the inner layer does not terminate itself.

The actual result records the outer-timeout error, helper exit -9, no Docker
emergency operations, and aggregate **failed** status. Its run log hash matches;
the PID file matches the control's owned child, and the retained control asserts
that child is absent or zombie after group termination. The external command
receipt completed in 0.390 seconds without its own ten-second timeout or cleanup
fallback; its raw log hash matches. Its zero script exit means the watchdog
finished writing its failure record, not that the controlled run succeeded.
The decisive `run-result.json` remains failed. The synthetic helper's source,
copied watchdog's exact wait-only delta, launch PID record, raw result and summary
all agree. No Docker resource or full native matrix is claimed by this control.
This supplies the runtime control that was not yet available in the previous
paragraph; native cells and S2 still require their independent result review.

| Exact outer-control input under `var/e08-completion-20261028/` | SHA-256 |
| --- | --- |
| `check_outer_deadline.py` | `1e76c787ceaea113fe36d6651816776ac7f1a2c8037d4715e9257888d3a8825a` |
| `outer-deadline-control.json` | `7aac708d4924e1e6cb6b1ea2e537a5ae6fadfa37efb79d5a2813b3c4593a08a8` |
| `outer-deadline-commands.json` | `ea76aaddcd741c32e5cc3094699b630eca9547e6bbab62bc505452c586f3283a` |

## S2 retained-volume export and finite proof helpers

**Disposition: accept the exact five helpers below for their bounded intended
use after a completed observed native run. No actionable finding. Actual export
equality, archive integrity, native observations and S2 remain unaccepted.** This
was local source/delta review and compilation only. No helper was executed, no
Docker/SSH operation or active native-run inspection occurred, no private file
was read, and only this report was written.

The verifier is the exact previously accepted S1 verifier
(`a182cb286ad3734ce9cc1fb5635dab2534e847443c58ec739689c8b20f5f322e`)
with only explicit architecture selection, the architecture-qualified minimum
bundle helper import and matching platform argument added. It gates on the
observed run watchdog and all 18 observed cell rows. Before each verification
container, it compares the full current native-volume inspection against the
retained identity and owner label, and checks the image against the retained
owner's immutable image IDs. The bounded 120-second client runs offline/nonroot
with a read-only root, read-only retained-volume mount and one owned export bind.
No volume/image removal or foreign enumeration is introduced.

The unchanged embedded client verifies both complete snapshot hash maps, with
the same explicit bare/installed-environment/cache exclusions as export. It
requires the actual installed package's complete payload hash map (59 current
members), all six original wheel metadata hashes, the five installed metadata
files that pip does not rewrite, and actual installed RECORD SHA-256/size entries
for all payload plus those five metadata members. RECORD's own entry must remain
unhashed. It retains the rewritten installed RECORD digest and the equality map;
successful verification does not require copying virtualenv bytes into the proof
archive. Outer source and embedded Python client compile without execution.

The collector's delta from accepted S1
(`5abda4f42debeb4d3e2bda9cc7b10cf437aa837d93f5dd6bf3437cabad800882`)
replaces remote preparation/executable paths with the explicit frozen
`inputs/{architecture}-{dependency}` bundle. Context-manifest equality, every
manifested input/context file hash (including constraints), both wheelhouse maps,
exported equality hashes and installed wheelhouse equality remain mandatory.
It gates on both successful watchdogs and 18 completed verifications. The full
immutable contexts retain the actual unique wheel bytes; duplicate input copies
and installed wheelhouse snapshots are excluded only after equality checks.
Bundle manifests, raw logs, stages/provenance and the archive member hash/size
index remain included. New-run `home`/`authoring`, caches and archive copies are
excluded; target virtualenvs are already excluded from the verified export and
remain in their retained native volumes. The collector is intended only for the
fresh finite owned run root; final archive bytes/index still require actual
postcollection verification.

The Linux export watchdog changes the reviewed native watchdog's command and
receipt names and adds only the finite export-verification CID layout to the same
image/owner/creation-bound emergency cleanup. The portable variant substitutes
the reviewed independent Python deadline layer. `export_deadline.py` differs from
the accepted/tested native deadline helper only in its verifier target and export
receipt filenames. The unchanged 7,200/180/10 inner limits, 7,450-second independent
outer/group cleanup, retained-volume semantics and failed aggregate on timeout,
nonzero verifier, emergency removal or cleanup error all remain. All five helpers
compile. No new timing, codec, assertion, bundle or application change follows.

| Exact helper under `var/e08-completion-20261028/` | SHA-256 |
| --- | --- |
| `verify_native_exports.py` | `e3938adc1482bb93755ad8bbbe06333b246f7331ce8625ecd9de7505c5018bec` |
| `collect_native_proof.py` | `58657a6fb0a23cdc338738d4bd97009a5a835e392263baafd1cba576406eedbc` |
| `export_watchdog_linux.py` | `0d1f1c1c0738b9741fb1b7b47bd6fc8b525dfabd0e88ff079fa0a5dae6040c1e` |
| `export_watchdog_portable.py` | `1c8f41eec280c2e370b0e740ef8d6ab58811d682cc3375e16ab4dc67758e19ce` |
| `export_deadline.py` | `db493002368775abd55aee116ed9449585cda2c0a62620c0d7f5fddbd430cdc9` |

## Prepared postcommit delivery-proof helper

**Disposition: accept the corrected exact helper below for its intended future
documentation-only proof step after final acceptance and actual local delivery.
The inherited Git-mode finding was repaired during review; no actionable finding
remains. This review establishes no commit, tested/committed/current equality,
unchanged final environment or completed E08 gate.** Only this report was written
by the reviewer; no helper/capture execution, Git mutation, commit, private-file
read, remote action or publication occurred.

The adaptation from accepted E07 helper
`8bd2733d4b5f20a68b07a0c812551471b5902f23d9773432105b94138d4dc58c`
correctly accounts for previously untracked safe inputs becoming tracked at
delivery. It compares the complete ordered tested/current file lists projected
onto path, content SHA-256, size and executable identity. Only Git tracking
bookkeeping is omitted from that file comparison; aggregate source identities
retain their actual HEAD/tracking origins and are recorded separately. The
reviewer's pure identity-projection check accepted a tracking-only change and
rejected independent changes to each of those four identity fields. No full
proof/helper was run.

Every tested path must then exist as an actual blob under the full captured
delivery HEAD and match its tested bytes/size and exact normalized regular-file
Git mode. The current working file must separately match the same bytes/size
and executable identity. An uncommitted new tested input therefore cannot pass
merely because the projected tracked flag was ignored. The initial inherited
mode expression accepted any non-100755 mode for a nonexecutable file. The
corrected helper now explicitly requires `100644` or `100755` according to the
tested executable flag, rejecting symlink/gitlink modes. The corrected source
compiles and the exact finite mode alternatives were independently checked.

The environment hash must remain identical. The future record retains the actual
full delivery SHA, parent SHA, changed-path list, both tested/postcommit source
identities, raw postcommit manifest hash and per-file tested/Git/current rows.
Its date/count describe only the later executed proof; the proposed 319-input
final snapshot must be independently bound to actual final tests before using
this helper. The code does not infer a successful final gate from a snapshot,
manufacture a future commit identity, or perform a commit/push itself.

The self-hash boundary remains correct: the raw postcommit manifest is under the
ignored finite proof root, and the generated machine record is under
`plan/library-implementation/evidence`, which the maintained capture allowlist
excludes. Neither joins its own tested source identity. Actual follow-up delivery
identity remains obtainable from Git history after that documentation-only
follow-up, rather than recursively embedding its own eventual SHA. The safe
capture still excludes private dotenv inputs; its explicit public
`.env.example` exception does not authorize a private-file read by this review.

| Exact delivery helper under `var/e08-completion-20261028/` | SHA-256 |
| --- | --- |
| `postcommit_proof.py` before exact-mode repair (superseded) | `43bebe749812e5f5314df1e2409f01e51d6ea871046e1e1a8a43e499afe45965` |
| `postcommit_proof.py` corrected, accepted | `3e4857dfcff0467f0f4e122298f42d66aba5a4e92bc0f29b66ff8e6a114a1d14` |

### Actual temporary-Git mode control

Independently re-read the retained temporary fixture's Git index and blob, without
mutating either repository or executing the proof helper. The regular `100644`,
executable `100755` and symlink `120000` entries all point to the identical actual
blob `1fbae4bd94ef5607f411f05f6bebaf0fa685eb24`; the two regular working files and
symlink target bytes also equal that blob. Extracted the corrected helper's actual
AST mode expression and checked all six retained executable/nonexecutable cases
against these actual index modes. Only matching regular/executable cases pass;
both symlink cases and both executable-bit mismatches fail. The raw fixture/index
observations agree with `postcommit-real-mode-controls.json`, SHA-256
`ca2e05020aa573cd3358af67cca6ffba148af9a63d0b7501c367e6f07733e68a`.
This real Git control substantiates the mode repair beyond the initial truth-table
check. Corrected-helper acceptance is unchanged; actual final delivery/equality
proof remains pending.
