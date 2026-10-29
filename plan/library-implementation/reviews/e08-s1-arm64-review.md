# Independent E08 S1 implementation and available arm64 review — 2026-10-27

**Disposition: accept the reviewed implementation and the available native Linux
arm64 minimum/current observations. No actionable finding remains in that limited
scope. This is not global E08.F1.S1 acceptance, full support-matrix acceptance,
E08 completion, release acceptance or permission to begin the dependent story.**
Native Linux amd64 remains unavailable and explicitly open by user direction.

The reviewer started with a task-only brief and no implementation conversation.
Reviewed the [execution plan](../execution-e08.md), [decisions](../decisions.md),
[ordering contract](../ordering-matrix.md), canonical [E08](../epics/e08.md),
[story evidence](../evidence/E08.F1.S1.md), prior plan/floor/runner reviews,
current implementation/tests/locks, and retained raw proof. Only this report was
written. No services, Docker operations, private environment files, remote IMDb,
commits, pushes, uploads or new chats were used by this review.

## Implementation and preservation

The metadata implements only the independently approved floor corrections:
PyYAML 6.0.1 and the paired Neo4j 6.0/Rust 6.0 floors, retaining the less-than-7
fence, eight mandatory dependencies and no extras/fallback. Application sources,
public exports, version and both schema resources remain unchanged. The original
red floor observations and rejected/superseded execution bundles remain retained.

The finite constraints/wheelhouse readers reject directives, paths, normalized
duplicates, symlinks, changed/unmanifested wheels and nonregular/oversized inputs.
Verified wheel bytes are snapshotted before installation. Both application and
test-tool resolution receive the same constraints; installed versions must match
them. Existing installed-runner defaults, interruption/forkpty ownership controls,
exact collection/outcome receipts and actual parent/child provenance remain.
The maintained container controller binds removal to complete CID, image, owner
label and observed creation identity, retains original failures and cleanup
errors, and verifies subsequent absence. The actual aggregate caller exits
nonzero on any failed cell.

The original testcontainers imports and assertions are restored. The explicit
pytest 9.1.1 test-tool pin preserves the source collector's exact IDs after the
retained pytest 9.2 long-ID mismatch correctly failed receipts; no test ID,
assertion, lane or skip allowance was relaxed.

Independent comparison against HEAD's accepted inventory verified all 3,804 old
IDs in their original order, all old per-ID dispositions and assertion/source
baselines, every field of all eight installed lane definitions, and all 59
application payload hashes. The 37 additional IDs are checkout tooling controls.

## Raw available-platform proof

The reviewer recomputed every one of the 99 path/hash/size references in
[machine arm64 proof](../evidence/e08-s1-arm64-20261027.json), all 379 frozen image
context file hashes across the two images, and every manifested bundle byte.
The 126 controller command receipts and 172 target command receipts have matching
actual stdout/stderr hashes, permitted exact exits, no timeout and no owned
cleanup error/leftover. Every cell controller reports successful removal and
verified absence; initial identities show network disabled, read-only root,
nonroot execution and exactly one new output mount with no checkout/source mount.
The recorded aarch64 daemon, arm64 images and actual Linux/aarch64 interpreter
observations support native arm64 scope; no amd64 execution is inferred.

All 16 installed receipts were checked against the frozen full/selected IDs and
order, explicit exclusions, stage hashes and distribution contract. Each selected
ID has its actual successful call or the original allowed core skip; no failed
phase, xfail, unexpected skip, receipt error or hidden deselection is counted.
Actual provenance contains 6,002 application module records with the expected
installed origins/payload hashes and 428 process-startup records with the expected
environment prefix/site. Child PID/creation identities, fresh post-exec startup
ordering and absence of provenance violations also pass independent inspection.

| Installed lane | Minimum passes/skips | Current passes/skips |
| --- | --- | --- |
| core | 453/1 | 453/1 |
| safety | 1,335/0 | 1,335/0 |
| cli-runtime | 636/0 | 636/0 |
| public-contracts | 247/0 | 247/0 |
| writer-runtime | 540/0 | 540/0 |
| orchestration-runtime | 282/0 | 282/0 |
| example-contracts | 115/0 | 115/0 |
| service-helper-unit | 41/0 | 41/0 |

This totals 7,298 successful installed calls and two original intentional skips.
Both additional bare runtime-only environments pass their absence, payload,
matched active Rust, import/API/resource and CLI parity probes. The bare targets
are separate new virtualenvs and do not borrow installed-lane development tools.

Both retained resolution reports match their summary hashes/artifact records;
all 30 minimum and 31 current wheelhouse member hashes match actual bytes and the
full locks match the recorded constraints. The portable archive's exact hash,
size, README hash and 155 manifested input members match its evidence. It has no
architecture wheelhouse. The actual clean arm64 portable preparation is successful
and reproduces the full minimum lock. It makes no amd64 wheel/execution claim.

Strict original/rebuilt artifact hashes match actual archives. Independent direct
comparison verifies all 65 wheel member names/bytes equal, with the rebuilt wheel
under a directory having no Git ancestor. Its original supervised build command
log hashes/exits/cleanup receipts also match. The retained audited checkout run
reports 3,788 passes/four inherited skips/49 deselections/19 existing fork warnings;
the audit predates pytest and records no blocked private-dotenv or IMDb attempt.
The subsequent 85-control run is retained. This review additionally reran current
matrix and installed-runner controls without shared conftest or pytest cache:

```text
var/e08-validation-20261026/dev/bin/python -B -m pytest --noconftest -p no:cacheprovider -q tests/test_support_matrix.py tests/test_installed_runner.py
66 passed in 1.55s
```

The final controller retry-log naming correction postdates the immutable v4
target images. Its current bytes pass these focused controls and the actual
portable-controller preparation. The v4 images/receipts retain their exact old
hashes; acceptance here does not claim those images contain the final controller
or that tested/committed/current equality has been achieved.

## Ordered continuation and remaining gates

Canonical E08.F2.S1 declares `Story dependencies: E08.F1.S1`; the approved
execution plan requires a fresh reviewer to accept S1 before S2. S1's validation
requires each advertised architecture, and native amd64 remains open. Therefore
this limited arm64 acceptance does **not** satisfy the dependency: S2 must remain
pending under the accepted ordering. Keeping unavailable proof open does not
authorize an ordering override, and this reviewer does not grant one on the
user's behalf.

Native amd64 minimum/current resolution, both bare probes and every installed
lane remain required. S2's pickling/start-method/framed-transport/worker-envelope
and 19-warning work remains unexecuted dependent work. The final artifact,
dual-wheel installed, production-image, R1/B3, live-service, adversarial-review,
local-delivery and postcommit equality gates remain required. No historical E07
or partial S1 proof substitutes for them.

## Exact key review inputs

| Input | SHA-256 |
| --- | --- |
| `scripts/support_matrix.py` | `d5d534218672cc20e6a0e64079795252618909b3ecd6bdff4e2a560858de4bf8` |
| `scripts/installed_dependencies.py` | `11434237aec2be294787d7aca60cc66f5446487a775fb0c4d13386b52b52543e` |
| `scripts/matrix_docker.py` | `ad07f49198a68bf781e2adf9c8fe050f922b03c1d94a0a9fc4eb83c66aa7b370` |
| `scripts/verify_installed.py` | `a53730fc59a222051c124a90927fc6b6202cd934b0bab40418467bec0e0dd138` |
| `tests/test_support_matrix.py` | `038fe4f5abfc1fe434052da3a3a1d003223207a0eaa8c2d69440f4cf86d42f45` |
| `tests/fixtures/e08_base_probe.py` | `f7d55e32b7a928f92e65c34fd44423b100824c5639e6142f6f5570a2ccc9df4f` |
| `plan/library-implementation/tools/verify_e03_artifacts.py` | `39a1644d1a523eb0bc1f5703b3e20c9204161bede579829a443dd85942a8b17e` |
| `scripts/freeze_installed_inventory.py` | `411d3c71992f4871d6294c340e46f44bd9207aafdc544839dd861c1ed85b7d2e` |
| `pyproject.toml` | `4d24e00871858121b9f24eb4a948562cf57b78668650f27cade5604f4641d7c7` |
| `requirements.txt` | `508cb22a1887324e0a3b996612218ef184cae2a285302b1ad75d4be18bd7ada0` |
| `evidence/e08-s1-arm64-20261027.json` | `2cd6a64cc075b6b182e95f6019c61e12cedb1c0a9e189af6ce678d9dfcd9878d` |
