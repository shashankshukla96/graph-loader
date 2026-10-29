# Independent E08 native amd64 remote runner review — 2026-10-28

**Disposition: accept the reviewed remote runner adaptation for its bounded E08
execution scope. No actionable finding in the reviewed inputs. Actual native
amd64 observations and global E08.F1.S1 acceptance remain pending.** This review
does not accept S1 results, unblock S2, complete E08 or authorize publication.

The reviewer started with a task-only brief and no implementation conversation.
Reviewed the new SSH scope paragraph in [the execution plan](../execution-e08.md),
the prior [arm64 review](e08-s1-arm64-review.md), [S1 evidence](../evidence/E08.F1.S1.md),
the local remote controller/watchdog, and their maintained support-matrix,
container and installed-runner helpers. Only this review report was written.
No SSH, Docker operation, service/data access, credential inspection, commit,
upload or new chat was performed by this review.

## Scope, inputs and failure behavior

The execution plan correctly limits the newly supplied SSH access to finite E08
inputs and owned resources. It preserves the ban on existing IMDb services/data,
private environment access, global Docker changes, foreign cleanup and
publication. The controller contains no credential material or IMDb connection
operation. Preparation executes dependency tooling only. Target execution uses
network-disabled containers with read-only roots, a nonroot user and exactly one
new evidence mount; application source is installed from the frozen wheel.

The reviewer independently recomputed the portable archive's recorded SHA-256
and size, the bundle manifest's recorded SHA-256, and all 155 finite manifested
input hashes. The bundled `support_matrix.py`, `matrix_docker.py` and
`verify_installed.py` exactly match the current reviewed helper bytes. No bundle
member was missing, changed or unexpected.

The controller checks the actual daemon's Linux/x86_64 identity and the pulled
base image's amd64 identity, pins the actual base digest, prepares both dependency
sets on that target, and requires byte-identical full version locks. Each new
image has a retained context manifest and inspected immutable image ID/owner
label. Each of the 18 bare/installed cells must return successfully; a failed
cell, preparation, build, ownership check, interruption or cleanup cannot become
an aggregate successful exit. The existing target helpers separately enforce
the interpreter architecture, finite bundle, installed versions, exact test
receipts and actual application/child provenance.

The per-command supervisor retains receipts and bounds its owned process groups.
Container cleanup uses complete CIDs, expected immutable image IDs, fresh owner
labels and observed creation identities. The independent watchdog imposes a
7,200-second interrupt deadline, a 180-second kill escalation and an additional
bounded outer wait. Emergency cleanup inspects only CIDs from the new finite
controller layout and verifies owner/image identity before removal and subsequent
absence. Emergency removal, timeout, nonzero controller exit or cleanup error
prevents its final status from being `observed`. Owned images and raw proof are
retained; no broad listing, prune or foreign removal is used.

## Required result review

The actual remote Docker wrapper, its explicit local socket, launch command and
environment were outside this local review's inputs. Retain their exact bytes
and identities alongside the observations to establish endpoint isolation and
credential-free execution. Retain actual host/interpreter/daemon identities,
both preparation reports and wheelhouse hashes, all image context bytes and
identities, every target/controller receipt, cleanup results, and the watchdog's
final result. A fresh result review must recompute that evidence and inspect
actual IDs/outcomes/provenance before accepting the native cells. This report
does not infer a pass from the scripts' intended behavior or from a launched job.

All remaining S1, ordered S2 and final retention/delivery gates remain in force.

| Exact reviewed input | SHA-256 |
| --- | --- |
| `var/e08-validation-20261028/remote_controller.py` | `e1b428b3d64e37c100e3c00abf5cb3e6a33e8d0c15821e1ab00c4437b6124e96` |
| `var/e08-validation-20261028/remote_watchdog.py` | `57091f9666af43d553db58d179457febe01c9b8168968846a2ba379155f95158` |
| `scripts/support_matrix.py` | `d5d534218672cc20e6a0e64079795252618909b3ecd6bdff4e2a560858de4bf8` |
| `scripts/matrix_docker.py` | `ad07f49198a68bf781e2adf9c8fe050f922b03c1d94a0a9fc4eb83c66aa7b370` |
| `scripts/verify_installed.py` | `a53730fc59a222051c124a90927fc6b6202cd934b0bab40418467bec0e0dd138` |
