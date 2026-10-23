# E06 final independent adversarial review — 2026-10-23

**Verdict: accepted for E06's implementation and final validation gate. No actionable finding remains.**

This is a fresh independent review of the final E06 aggregate, not an adoption of
an earlier reviewer's unreported work. I inspected the canonical E06 story gates,
execution plan, D01–D07, cancellation contract, ordering matrix, test inventory,
all three independent story reviews and retained resolutions, current product and
test changes, final evidence and the underlying proof helpers/logs. No applicable
AGENTS.md was found. The earlier review receipts were available as historical
context; the checks below were performed independently for this disposition.

Accepted source is the exact **279-input** tested manifest
`39cb146ce8e5891c04c1ea10b02e83d2bb06582c817f043599fba757f2851d13`,
with development environment
`96b072066210abc50c4d8cf2bf48460f11e2ebbea959f87983d47328e1d5135d`.
Fresh safe captures before and after this review match the full tested source
rows, sizes, hashes and executable flags and the canonical environment identity.
The only input changed from the first finalb green snapshot is
`docs/DISTRIBUTION_DEVELOPMENT.md`; all other 278 rows, all 59 package payload
members and all 74 candidate input hashes remain exact. The corrected full run
is bound to the accepted 279-input snapshot.

## Acceptance criteria

| Boundary | Independent disposition |
| --- | --- |
| E06.F1.S1: complete matching component candidates | Accepted. All four actual local immutable IDs exist, with approved namespace/version proposals, component/contract labels, entrypoints and linux/arm64 platform matching the candidate manifest. Fresh builder enumeration equals all 74 recorded COPY/config/tool inputs. No invented registry digest or publication claim. D06 selects prebuilt images; a bundled build context is not required. |
| E06.F1.S2: deterministic installed selection | Accepted. Runtime never builds cwd. Local-first/missing-only pulls, strict local-only flag, compatible overrides and safe failures are implemented and tested. The service snapshots references, serializes resolution and pins immutable IDs across all launch roles. The constructor redaction repair remains present. Installation/import/help are inert. |
| E06.F2.S1: mounts, addresses and installed load | Accepted. Strict schema/external-plan read-only identity, separate output markers and writable rejection/timing paths, host plan/ledger/spill ownership, published host coordinator ports and container network addresses are covered. The exact installed service proof exercises Neo4j5.26 Enterprise/Kafka3.7, database isolation, both normal and explicit external-plan loads, seven real negative controls and owned cleanup. Remote daemon/path translation and unsupported networks fail clearly and are documented. |
| Preservation and final regression gates | Accepted. Current full suite, strict artifacts, outside-Git rebuild, two clean installs, four images, four real data-bind probes, all twelve supervised installed/image R1 cases and exact no-skip B3 are supported by retained exact-source receipts and independent checks below. No weaker ordering/platform/public API guarantee was substituted. |

## Independently reproduced checks

- A new focused run passed **547 tests in 57.94s**, with no skips or warnings. It
  covers image/candidate/topology/DockerService, relationship monitor, actual
  lease shutdown, all runtime-path cases, public API/lifecycle/cancellation/
  cleanup, framed planner transport/imports and CLI/planned/DLQ/environment
  behavior. The original ten-process SIGTERM storm and new real-service
  closed-loop regression both pass.
- Reinspected the original wheel and sdist and rebuilt wheel against current
  package/configuration bytes and metadata: 57 Python modules, exactly two YAML
  resources, sole console entrypoint, license and all mandatory dependencies,
  no extras or unexpected/private archive inputs. Independently compared **all
  65 wheel member bytes**. Whole wheel hashes differ because archive containers
  differ; their members are equal. All eleven retained actual mutated archives
  were independently fed to the strict inspector and rejected.
- Created **two new clean noneditable environments outside the checkout**, one
  per exact original/rebuilt wheel, using the retained runtime dependency
  constraints. Git discovery returns128. Independently reran all module
  site-packages origin/hash and inert-import checks, version/metadata, absence
  of legacy `src`/development-only dependencies, eight component helps,
  three-entry parity including error cases, public examples, resources/runtime
  ownership fixtures and pip checks. Both pass.
- Independently inspected and ran **all four actual immutable image IDs** with
  network disabled, read-only rootfs, cwd `/`, no source/data bind mounts and
  only a temporary `/tmp`. Each reports installed site-packages origin and the
  exact current hashes of **all 59 package payload members**, including repaired
  lease-server/preflight/CLI bytes. Exact labels, entrypoints and platform match.
  Each review container was retained by its returned ID and removed by that ID.
- Independently validated all **127 aggregate log size/hash identities**, all
  85 artifact/image command-log hashes and all ten service command-log hashes.
  Seven tracked aggregate proof files equal their raw finalb receipts byte for
  byte. Candidate COPY closure equals a fresh current-source enumeration.

## Final retained execution audit

The full suite was not needlessly repeated: its log matches the accepted source
snapshot and records exactly `3624 passed, 4 skipped, 49 deselected, 19 warnings
in 196.88s (0:03:16)`. The four inherited skip reasons and 19 known IMDb fork
warnings remain explicit A5/E08 limitations. Lint, requirement parity, pip check,
whitespace, isolated build and strict Twine receipts pass.

The outside-Git sdist rebuild receipt records exit128 for Git discovery and the
matching extracted build-source argument; its successful isolated rebuild log
and all65-member equality are verified. The original temporary extracted source,
installed environments and service cwd have since disappeared. They are not
represented as currently present: fresh independent install environments above
replace the import/installation rechecks, while the historical no-Git and actual
service assertions are audited through preserved source/helper/log identities.

The actual installed service proof is audited as an executed historical gate,
not claimed as rerun today. Its successful helper assertions and logs establish
13 nodes, 12 property-verified relationships, node commits12/1, edge boundary
`[0,0,12]`, two complete ledger entries/12 rows, scratch/default database isolation,
normal and external shared-plan/spill/timing runs, foreign-sentinel preservation,
actual default-factory host monitor readiness and container DNS PING. The retained
normal plan/ledger/timings and empty owned spill directory agree with the receipt.
All seven real credential/database/container-address/network/metadata/missing-bind
negatives pass with safe diagnostics. Tiny input does not force disk spilling.

The twelve R1 cases retain their individual external supervisors, default/finite
modes, exact original/rebuilt install or immutable-image identities and log hashes.
The current fixture asserts the original first-received `LookupError: mixed
installed source failure`, bounded observation/cleanup, dead children, closed
endpoints and reclaimed owned state before emergency fixture recovery. All twelve
pass. The independently hashed exact B3 log records **1 passed**, no skip or
supervisor timeout, exit0 and empty before/after container inventories; its graph,
source/DLQ/parking and zero-lock-wait test source remains byte-identical to entry.
Neither service proof is replaced by IMDb or an in-process substitute.

After local Docker Desktop startup, all four immutable image inspections/runs
above succeeded. I independently queried the exact three recorded owned service
IDs and network: all are absent. The recorded application run and all review
containers are absent. No foreign container/network cleanup was performed.

## Adversarial preservation and findings disposition

Reviewed every product/test diff for hidden weakening. Changes to Docker mocks
provide explicit compatible images and retain preexisting ownership/launch
assertions. Obsolete build assertions now require approved resolution and forbid
runtime builds; DLQ failure retains no-DDL/no-ingestion guarantees. Read-only
input-topic/DLQ policy rejection remains before image access, and image/topology
preflight remains before service mutation. The earlier S1 input-closure, S2
constructor-redaction and S3 exact-ID/marker/external-plan findings are repaired.
The narrow R3 signal repair ignores repeated SIGTERM before loop closure without
weakening the original storm test or genuine failure status. Original failed
attempts and the limited attribution of the discarded-stderr storm failure remain
honestly recorded in the story review and resolutions.

The root exports/version, package metadata/dependencies, public schema/planning,
resource/runtime ownership, planner/transport implementation, core public
lifecycle/cancellation/cleanup tests, B3 and R1 source fixtures are byte-identical
to E06 entry. All five parameterized public symlink-loop cases remain unchanged
and passed in the new run. E05 evidence is not modified. The D02 cancellation,
10-second aggregate cleanup and restricted ordering contracts are preserved.
No E07–E12 implementation, version change, source alias, release workflow,
publication, push or tag is included.

There are **no required repairs**. This acceptance allows E06 completion metadata
and local delivery to proceed. The staged delivery scope/status metadata, actual
full delivery commit and documentation-only tested/committed/current equality
follow-up remain separate checks; this report does not invent a commit SHA or
pre-accept those future records. Native macOS/local Linux arm64 proof does not
close E08's wider matrix or E09's service/ordering guarantees.

Machine-readable checks and identities are in
[the independent evidence](../evidence/e06-final-independent.json). New owned raw
review output is `var/e06-final-acceptance-20261023/`. Three early audit harness
failures are retained: JSON tuple/list comparison and two assumptions that old
temporary directories remained. They were corrected without product edits; the
complete final audit passes. The initial absent Docker socket was resolved by
starting the existing local app, followed by successful actual image checks.
Only this report, its independent evidence and owned review scratch were written.
No private `.env` was read/sourced, remote IMDb contacted, registry operation,
global prune, foreign deletion, product edit or Git commit was performed.
