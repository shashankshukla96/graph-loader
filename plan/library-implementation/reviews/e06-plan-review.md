# Independent E06 execution-plan review — 2026-10-22

**Disposition: accepted for implementation. No actionable plan finding remains.**
This accepts the ordered execution plan only. It does not accept implementation,
candidate images, service topology, artifact evidence or E06 completion.

This reviewer was initialized without conversation history. Reviewed actual HEAD
`a2bc7bbd6e82350a25e9a127454452c360c1ceda`, the pending
[execution plan](../execution-e06.md), [handoff](../handoff-e06.md), canonical
[E06](../epics/e06.md), D01–D07, relevant Dockerfiles, DockerService and CLI
launch paths, coordinator addressing, existing build/launcher tests, runtime
documentation, test inventory and E05/R1 acceptance records. Execution-plan SHA-256:
`deb9c6b10bfe794d8a452a4c319897fc1e4b64977fe7b9cc1ae44571610adb61`.
Only this report was written. No product/test edits, tests, service operations,
private environment-file access, remote IMDb access, commits or pushes occurred.

## Source assessment and required implementation evidence

The present source confirms the defect addressed by E06: all three build methods
use `path="."`; runtime defaults use `:latest`; launch methods read those mutable
tags again when creating containers. The four Dockerfiles already install the
package and use module entry points, so version/identity labeling and deliberate
development builds fit the approved prebuilt strategy without packaging a new
build context. The package's existing development version is the authority for
E06 candidate tags; D06's initial release target does not authorize changing it.

The plan explicitly covers these implementation gates:

- Build and identify all four approved component candidates. The story review
  must inspect the actual label names, exact accepted values and missing/malformed
  label behavior. Component, package version and the narrow image contract must
  all match for default references and overrides. A tag string or a user-supplied
  version label alone is insufficient evidence of the builder's installed-version
  check. Record actual local IDs, entry points, platform and input identities;
  local IDs are not registry publication digests. This contract establishes
  compatibility, not provenance authentication of arbitrary operator images.
- Normal resolution must inspect locally first and pull only an absent reference;
  incompatible local images fail rather than triggering an opportunistic build or
  silently substituting another image. `--skip-image-build` retains its spelling
  and becomes explicit local-only resolution, still checking compatibility. Test
  pull/daemon failure, missing/mismatched metadata and explicit override handling.
  Documentation/help must replace the current checkout-build description.
- Resolve and retain immutable IDs for every launch role, including lease server,
  dispatcher, planned writers, retry, clock and replica launches. A retag after
  resolution must not change the launched image. Exercise direct launcher use as
  well as CLI resolution, and concurrent replica creation; neither can provide a
  bypass back to mutable tags or hidden cwd builds. Retained `build_*` entry
  points must implement the stated resolution contract and explain obsolete
  explicit Dockerfile requests.

These are concrete realizations of the accepted plan, not additional stories or
new release guarantees. Existing mocked tests that expect `:latest` or cwd builds
must be updated for the changed contract rather than retained as false evidence
of installed behavior.

## Service and topology proof

The topology gate is necessary and appropriately scoped. Current planned
dispatcher addressing publishes a loopback host port, whereas the lease helper
returns an internal bridge IP/name. The host-side lease readiness probe calls
that helper with `prefer_name=False`; container bridge IPs do not by themselves
prove Docker Desktop host reachability. Actual evidence must exercise both the
host and writer paths for dispatcher/lease addressing and document any narrow
fix. A successful planned-mode load alone cannot certify the lease path.

The tiny installed-wheel load must actually run outside the checkout with its
selected immutable component IDs, fresh owned Kafka/Neo4j and daemon-visible
absolute inputs. It must assert resulting graph data, source offsets and exact
owned cleanup. Tests of launcher keyword arguments, raw bind probes and inherited
B3 are valuable but cannot substitute for that installed orchestration proof.
Record which real mode proves each required schema/rejection/plan/ledger/spill/
timing and coordinator path; use focused actual probes for paths not traversed
by the tiny load, without turning E06 into E09's full service matrix.

Keep host bootstrap/Neo4j addresses distinct from container addresses and verify
Kafka's advertised metadata, not just initial TCP bootstrap. Database selection
and bad credentials need observable success/failure with secrets absent from
diagnostics. Missing networks, inaccessible binds/services and unsupported remote
daemon/loopback arrangements must fail clearly. A Docker Desktop file-sharing
limitation must remain an open gate if it prevents the promised supported run;
the plan does not authorize claiming a mock or a skip as that proof.

## Completion boundaries

The plan retains the inherited full checkout/example suite, lint/parity/pip/
whitespace checks, strict wheel/sdist and outside-Git rebuild, every-member
equality, two clean noneditable outside-checkout installs, all four rebuilt
immutable images and actual read-only probes. All 12 separately supervised R1
default/finite cases and exact no-skip owned B3 remain required after final
product freeze; previous green receipts certify only their previous inputs.
Required infrastructure failures/skips remain open. Source/environment identity
and tested/committed/current byte/size/mode equality prevent silent evidence drift.

Ordered zero-history story reviews, repair/retest/re-review, a fresh final
adversarial acceptance and actual-SHA documentation follow-up are explicit.
Preserve the five E05 symlink-loop regressions and narrow exception handling,
public API/version/dependency contracts, transport/cancellation bounds and R1
first-received failure causes. E07's installed full-suite runner, broader E08/E09
platform/service matrices, publication, workflows, foreign cleanup and remote
IMDb remain outside E06. No additional actionable plan discrepancy was found.
