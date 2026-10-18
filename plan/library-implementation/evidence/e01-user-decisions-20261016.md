# Direct E01 user decisions — 2026-10-16

Source: the human's structured replies in this chat, received after E00 execution
acceptance. They are product decisions, not independent-review substitutions.
Approval-input SHA-256 values are preserved in
[e01-approved-input-hashes.json](e01-approved-input-hashes.json). The brief's
technical proposals are unchanged; status/identity notes now reflect the reply.

## D01

The question named distribution `neo4j-graph-loader`, import
`neo4j_graph_loader`, executable `graph-loader`, and requested the authorized
public maintainer/index owner and public contact. Exact user reply:

> Yes, looks good. Name: Shashank Shukla, email: shashankshukla96@gmail.com

Record those identities and the supplied public-role contact, without using
machine-local Git email or inferring account access/reservation. D01 resolved.

## D02–D05 and D07 policy

The question requested approval **exactly as proposed in decision-brief.md**:
schema/planner + existing CLI, full cancellation/restricted-ordering contracts,
0.1.0a1/no src shim and compatibility policy, CPython 3.12 Linux arm64/amd64
conditional on tests, eight mandatory dependencies including the matching Rust
extension, setuptools/file version/manual Twine/no workflows. It also requested
the release-owner name. Exact user reply:

> Yes I approve.

D02–D05 and D07's technical/publishing choices are explicitly approved, including
the brief's export/format/dependency tables and full linked contract semantics.
No separate release-owner/operator designation was supplied. The named
maintainer/index owner is known, but that additional role is not asserted as an
explicit decision. This remaining detail belongs to E01.F2.S2; do not reopen
already-approved technical choices or infer publication authority.

## D06

The question offered recommended matching versioned prebuilt images (requesting
registry namespace and image owner) versus a bundled versioned build context.
Exact user reply:

> For now, go with you recommendation, we will discuss it more later.

Record prebuilt images as the currently selected direction with public registry
namespace/image-owner details deliberately deferred. No namespace/account is
invented; no release/image push is authorized. The approved E01 exit gate still
requires ownership before the epic closes. Deferral does not silently remove it.

## Scope and verification consequence

E01.F1.S1, E01.F1.S2 and E01.F2.S1 meet their decision/investigation gates.
At the time of those initial replies, E01.F2.S2 remained incomplete for D06
registry/image ownership and D07 release-operator designation. E00 remains accepted; E02–E12 have not started.
No substantive contract, story sizing or dependency change follows from these
within-plan decisions. The user additionally requested an independent adversarial
subagent once implementation is finished; review the complete decision-record
changes and correct any findings before the final response.

## Delegated completion and per-step review

After the assistant identified the remaining registry/image-owner/release-operator
fields, the user instructed:

> finish the rest

Then the user added:

> and review after every step using the Agent

These instructions end the earlier deferral for this bounded assignment and
allow the agent to complete the concrete recommendations under the already
approved identity/prebuilt-image direction. The chosen registry is
`ghcr.io/shashankshukla96`; Shashank Shukla is designated image and release owner
as well as the already approved public maintainer/index owner. Record these as
**agent-selected under user delegation**, not as literal user-supplied strings or
proof of account control. Public repository/profile metadata corroborates the
namespace/name; see e01-registry-investigation.json.

Agent review checkpoints: (1) investigate/recommend and assess authorization;
(2) apply and review canonical decisions/status/handoff; (3) final validation and
adversarial acceptance. Checkpoint 1 approved the delegation interpretation and
registry/owner choice before application. See the separate
[completion review](../reviews/e01-completion-checkpoints.md). No upload/push,
workflow, stronger guarantee or E02–E12 implementation is authorized by this
bounded completion instruction. E01's decision gate can close without inventing
operational account-access or artifact-publication evidence.
