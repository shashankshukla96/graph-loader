# Independent E08 native amd64 workspace review — 2026-10-28

**Disposition: accept the reviewed v3 native-volume adapter for bounded E08
execution. The v2 failure-evidence retention finding below is resolved by the
v3 design, and the retained interrupted-volume runtime control is accepted.
No further actionable finding remains. Actual native amd64 matrix observations
and E08.F1.S1 acceptance remain pending.** This report does
not accept S1 results, unblock S2, complete E08 or authorize publication.

The reviewer started with a task-only brief and no implementation conversation.
Reviewed the execution plan's remote scope and S1 gates, the prior
[remote-runner review](e08-native-runner-review-20261028.md), the diff from
`remote_controller.py` to `remote_controller_v2.py`, the subsequent v3 repair,
the unchanged watchdog,
`support_matrix.py`, `matrix_docker.py`, `verify_installed.py`, and the original
resource fixture. A follow-up independently inspected the locally retrieved
restart preflight/control evidence described below. Only this report was
written. No SSH, Docker operation,
service/data access, credential inspection, code mutation, commit or upload was
performed by this reviewer. Adapter code review and bounded runtime-control
evidence review do not establish a native matrix result.

## Initial finding, resolved in v3

**[P1, v2] Persist partial target evidence before destructive container cleanup.**
In `var/e08-validation-20261028/remote_controller_v2.py:115–121`, the wrapper's
`finally` block is the only operation copying `/evidence/observation` out of the
new tmpfs. The unchanged controller's cleanup calls `docker rm --force` after a
command interruption or timeout (`scripts/matrix_docker.py:76–81`), and the
watchdog does the same on its emergency path (`remote_watchdog.py:60–64`). Neither
waits for a completed export or salvages the target workspace. A controller
interruption during a running cell, outer timeout during export, or container
OOM/SIGKILL can therefore remove the tmpfs while its installed result, stage,
pytest receipt, provenance JSONL, logs or malformed partial receipts have never
reached `/export`. The retained outer Docker logs and failure status cannot
reconstruct those files. The previous host bind preserved them as written.

Keep the unchanged finite probes in a native Linux filesystem whose evidence
survives container stop/removal, or establish a bounded preservation mechanism
before destructive cleanup for every relevant path. Preservation must cover
failure and interruption, including incomplete/malformed artifacts, without
weakening the hard deadlines or ownership checks. Demonstrate the repaired
retention path with a deliberately interrupted cell before relying on the full
native run. A successful ordinary export alone does not exercise this gap.

### V3 follow-up disposition

`remote_controller_v3.py:108–115` now explicitly creates and inspects one new
UUID/owner-qualified, labeled local Docker volume per cell, and persists the
volume's inspected identity before launching the target container. The test-only
Dockerfile initializes `/evidence` with mode 1777, and line 138 mounts that native
volume at `/evidence`; the existing single host export bind remains `/export`.
Normal controller cleanup and the unchanged watchdog remove only the owned
container. Neither removes any volume. The target workspace therefore survives
container termination/removal and can be recovered even when the wrapper's
export never runs or is interrupted. The retained volume includes the otherwise
excluded target environments and caches as well as partial logs/provenance.
This resolves the destructive-evidence-loss mechanism identified in v2.

The v3 delta changes neither the finite bundle/probes nor their assertions and
deadlines. Failure to create/inspect a volume or verify its name/owner/local
driver aborts the controller; it cannot become an observed cell. Retain the
volumes and exact inspected identities until complete export verification or
owned recovery. Future removal must validate the retained owner/name/creation
identity and must not use a broad volume list/prune operation. The requested
bounded interrupted-container control is accepted below. Actual matrix
observations remain pending.

### Retained runtime-control follow-up

Independently inspected the local evidence under
`var/e08-validation-20261028/restart-preflight-proof/`, including all three
`durable-workspace-smoke` controller records, their raw logs, volume creation
and before/after inspect records, recovered bytes and result, plus setup, the
earlier filesystem probe, readiness, deployed scripts/wrapper and watchdog
launch. Recomputed **59 stdout/stderr hashes across 32 command receipts**; all
match their retained bytes. Deployed `remote_controller.py` exactly matches
reviewed v3 and the deployed watchdog matches the unchanged reviewed watchdog.
Readiness and launch hashes also match those exact bytes. The wrapper explicitly
selects the user's local Docker Desktop socket without credential material or
persistent context mutation; launch uses the new isolated HOME.

The smoke created
`graph-loader-e08-restart-smoke-84ce36be78da43a9a0e80aae685e4c1c`, a local volume
with the matching owner label and creation time `2026-10-28T13:30:22Z`.
Initialization runs as root solely to set that new volume's mode to 1777. The
tested writer and recovery client run as UID/GID 1000, offline with read-only
roots. The writer requires EACCES when creating a file inside a mode-0500
directory, writes the retained JSON only after that assertion, then sleeps for
60 seconds. The outer 3-second timeout fires at 3.017 seconds; its controller
correctly remains **failed**, with no cleanup error. The cleanup inspection
records the writer still running before force removal. A separately launched
nonroot client subsequently mounts the same volume read-only and recovers the
exact expected JSON bytes through the owned export bind. The final volume
inspection is identical to the initial one, including owner/name/creation time.

For initialization, the interrupted writer and recovery, independently checked
the complete CID against its cidfile/create output, initial and cleanup raw
inspect records against the controller, consistent immutable image/owner/creation
identity before removal, the matching removal output and subsequent "no such
object" result. All three owned containers are absent and the volume remains
retained. This exercises the actual evidence-loss path from v2: the writer has
no ordinary export/finally handler, and the artifact is recovered after its
container is forcibly removed.

This accepts the bounded nonroot chmod and timeout/removal retention control.
It does not claim a reboot/OOM experiment or run the original full installed
resource fixture. The smoke uses the retained Python base image and a root
permission-initialization step; the matrix image's build-time mode initialization
and full probes still require their own actual observations. The earlier tmpfs
probe separately reports x86_64 and UID 1000; readiness reports the x86_64 host
and CPython 3.12.3, while these container identities record Python 3.12.14.
Those distinct observations are not conflated with final matrix identities.

## Preserved contracts and bounded conclusions

The substantive probe invocation is unchanged: the same manifested bundle,
native amd64 architecture check, explicit installed lane and frozen constraints
are passed to the same maintained runner. The original test IDs, selection,
exclusions, skip allowances, phase assertions, 1,800-second installed test
deadline, 2,400-second observation supervisor, child provenance checks, and
ownership-bound cleanup logic are not edited. The new 2,600-second wrapper
deadline sits outside the existing inner supervisor and inside the unchanged
2,700-second container deadline; no original assertion is relaxed. The
7,200-second watchdog and its escalation remain unchanged.

The original `readonly_package()` first removes write permission and then
requires actual package creation to fail with EACCES or EROFS. Moving the fresh
installed environment from a Docker Desktop host share to a native Linux
filesystem addresses the reported permission-semantics mismatch without
altering that assertion. Execution remains nonroot, offline, with a read-only
image root and a single owned writable host export bind. The workspace itself
is necessarily writable to install the fresh environments; the fixture supplies
the tested package restriction. This review has not independently reproduced
the host-share failure or observed the original full resource fixture passing
in the native replacement. The narrower chmod/retention smoke is accepted above.

On ordinary child success or nonzero exit, the wrapper attempts to export the
whole observation tree, excluding exactly the top-level `bare` environment,
`installed/environment`, and directories named `__pycache__`. Logs, install
reports, version reports, result/failure JSON, copied constraints/wheelhouse,
staged test bytes, generated stage configuration, selected/full IDs and
parent/child PID/creation/startup/post-exec provenance remain within the intended
copy. Receipt paths correctly describe the original `/evidence` execution
locations rather than the later export location. Excluding the two target
virtualenvs means the export is an evidence snapshot, not a retained runnable
environment; actual installed bytes are checked by the unchanged probes and
recorded provenance. The loss identified above concerns artifacts outside those
explicit exclusions.

The reviewed adaptation introduces no IMDb operation, foreign-resource cleanup,
global Docker configuration change, application source change, bundle change,
test assertion change or publication action. The final matrix evidence must
retain the remote endpoint wrapper, launch bytes, actual base/daemon/interpreter
identities, dependency preparations, image context manifests, every raw
target/controller receipt, export outcome, and final watchdog result for fresh
result review. The bounded preflight subset accepted here supplies no full
matrix outcome. S1 remains
open until the native cells and all other required gates are accepted.

| Exact reviewed input | SHA-256 |
| --- | --- |
| `var/e08-validation-20261028/remote_controller.py` | `e1b428b3d64e37c100e3c00abf5cb3e6a33e8d0c15821e1ab00c4437b6124e96` |
| `var/e08-validation-20261028/remote_controller_v2.py` | `f34c607900bb308175e54f4a613bc0a35036cfa7256699c5972c95e38e46e625` |
| `var/e08-validation-20261028/remote_controller_v3.py` | `98ecc0d0de2f3d6cd31c9b2e29a9db235492e59f7c45be98a1368311ad88892a` |
| `var/e08-validation-20261028/remote_watchdog.py` | `57091f9666af43d553db58d179457febe01c9b8168968846a2ba379155f95158` |
| `scripts/support_matrix.py` | `d5d534218672cc20e6a0e64079795252618909b3ecd6bdff4e2a560858de4bf8` |
| `scripts/matrix_docker.py` | `ad07f49198a68bf781e2adf9c8fe050f922b03c1d94a0a9fc4eb83c66aa7b370` |
| `scripts/verify_installed.py` | `a53730fc59a222051c124a90927fc6b6202cd934b0bab40418467bec0e0dd138` |
| `tests/fixtures/e05_resources.py` | `df574fa81e8b56d6d55d93ec3e45f2eb0ba57fc1db910b020f4c337aba943705` |

The following paths are relative to
`var/e08-validation-20261028/restart-preflight-proof/`.

| Accepted control evidence | SHA-256 |
| --- | --- |
| `readiness.json` | `236033da76c7d36dd1f7438aaa2c726b532866af2cf4166b3d2bc40ba5ab727d` |
| `watchdog-launch.json` | `c7e3ce06d22a3b5e67103c93f337896f4c3ad9107f8ab021ff63442aa07203e7` |
| `docker` | `ae395bfc6478062db8c1f6881afffa4f0f452425c786976368180a42dee514fe` |
| `durable-workspace-smoke/commands.json` | `1ee5822bd5fb5e3277e2abb41c92a0dbf91b10e5189adc5bf8279138f2cd3e59` |
| `durable-workspace-smoke/initialize/controller.json` | `b055698a53cf0ed9536bf249b30343f0bc13ed4a9d1211a7ccd81943ec627ec1` |
| `durable-workspace-smoke/interrupted/controller.json` | `aa022be64883431cbad028714ba2c6b5d5cde3078cf4f61d628867cb0497906e` |
| `durable-workspace-smoke/recover/controller.json` | `bfb8d41118ddf6657e47b567b83cae496601824a1b88d8d0ad0b9972c91d39e5` |
| `durable-workspace-smoke/volume-inspect.log` | `d7cf5055e461229a0941efb1c0832e53faa49198690d1648e112e547d4a1d22d` |
| `durable-workspace-smoke/recovered.json` | `bd4bef8cc3fd0f0a5023eaebb5decdde5c77511feadeddb8da15b6c80bc7623c` |
| `durable-workspace-smoke/result.json` | `9ac0e02aec0893e637305249574080e55f2c170383252dfabac2a732d3e69c6d` |

## Pre-execution proof-helper review

**Disposition: accept the corrected export verifier, its watchdog and finite
proof collector for their bounded intended use. Actual execution and evidence
outcomes remain pending; no S1 acceptance follows.** These helpers were reviewed
locally without executing their Docker or collection operations.

The initial export verifier (`84f5f82077e98709da3da56ddd08bbc88ccaf1aedc8492d41e65b1593beb2871`)
had three blocking defects: indentation made the embedded client fail to compile,
two installed-RECORD variables were undefined, and it compared pip's rewritten
installed RECORD directly with the wheel RECORD. Those defects were reported
before execution. The corrected verifier below resolves all three. Independently
compiled each helper's outer source and the corrected embedded client without
executing them. The verifier now checks the original six wheel metadata hashes
inside the zip, the five unchanged installed metadata files separately, and
actual pip RECORD SHA-256/size entries for all payload plus five metadata members
(64 for the frozen inventory), with the RECORD self-entry explicitly unhashed.

The verifier gates on the successful run watchdog and all 18 observed rows,
compares the current volume inspect identity with its retained identity and
owner, then uses new bounded offline/nonroot containers with a read-only volume
mount and one owned export bind. It checks the complete normal snapshot against
the native workspace under the existing explicit environment/cache exclusions,
retains equality receipts and leaves all volumes intact. Its outer watchdog
includes the finite export-verification CID layout in the existing
image/label/creation-bound emergency cleanup; emergency removal, errors or a
nonzero verifier prevent an observed watchdog result.

The collector gates on both successful watchdogs and the completed verification
count. It rechecks context manifests, executable/prepared/context wheel equality,
constraints and exported snapshot hashes before excluding duplicate wheelhouses
and executable copies. The complete context retains unique actual wheel bytes;
manifests, raw logs/provenance/stage files and the finite archive member index
remain included. Authoring/HOME/resolver environments, caches and archive copies
are excluded. No broad Docker enumeration, volume removal, private input access
or publication operation is introduced. No further actionable static finding
remains in the exact helper bytes below. Actual exported equality, archive
integrity and final target results require fresh evidence review.

| Exact helper input under `var/e08-validation-20261028/` | SHA-256 |
| --- | --- |
| `verify_native_exports.py` | `a182cb286ad3734ce9cc1fb5635dab2534e847443c58ec739689c8b20f5f322e` |
| `export_watchdog.py` | `893717b5c015712d4419e17d8d79b0ab7c26178770713a5064fe145fb3b7ad5b` |
| `collect_native_proof.py` | `5abda4f42debeb4d3e2bda9cc7b10cf437aa837d93f5dd6bf3437cabad800882` |
