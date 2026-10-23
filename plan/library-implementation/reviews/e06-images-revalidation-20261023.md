# E06 image identity and selection revalidation — 2026-10-23

**Disposition: scoped approval; no actionable findings.** This independently
revalidates E06.F1.S1/F1.S2 image construction, metadata, resolution and launch
pinning. It does not independently close the full E06 topology, artifact,
installed-load or regression gates assigned to the parent reviewer.

Reviewed HEAD `e78970c` (full identity in the new probe result), implementation
`a034420f98a57d17c7591b23eb1011d7bc346d25`, and the actual implementation diff
against E05 baseline `a2bc7bb`. The working tree was clean before this review;
the follow-up HEAD changes no reviewed product/build input. Read the E06 epic,
execution plan, E06/E07 handoffs, D06 decisions/decision brief and direct decision
record, `_images.py`, DockerService image/launch paths, CLI preparation/order,
all four Dockerfiles, candidate builder, build target, `.dockerignore`, image
documentation and relevant tests. No AGENTS.md was found. Earlier review verdicts
were not used to establish this disposition.

## Evidence produced in this review

New owned output directory: `var/e06-images-revalidation-20261023/` (Git ignored).
The interpreter is the one recorded in E06 evidence:
`var/e06-validation-20261022/dev/bin/python`.

1. Focused existing suite:

   ```text
   var/e06-validation-20261022/dev/bin/python -m pytest tests/test_image_selection.py tests/test_candidate_images.py tests/test_docker_service.py -q -rs
   86 passed in 0.23s
   ```

   Output: `var/e06-images-revalidation-20261023/focused-tests.log`.

2. Newly authored adversarial probes:

   ```text
   var/e06-validation-20261022/dev/bin/python var/e06-images-revalidation-20261023/adversarial_images.py
   86 passed; 4 real local images inspected; 74 current candidate inputs matched
   ```

   Machine-readable cases, actual inspected metadata, reviewed HEAD and limits:
   `var/e06-images-revalidation-20261023/adversarial-results.json`.
   Driver output: `var/e06-images-revalidation-20261023/adversarial-probes.log`.
   Fake-engine builder outputs underneath this directory are explicitly mock
   command-construction evidence, not real Docker build/publication results.

## Independently checked contracts

- **D06 identity and construction.** Defaults use the approved
  `ghcr.io/shashankshukla96/graph-loader-{role}` namespace and actual development
  version `0.1.0a1.dev0`, preserving the separate release target. The builder
  reads `_version.py` as data, supplies it to every Dockerfile and uses distinct
  suffixed candidate tags. Dockerfiles install the package and assert installed
  metadata against the supplied version, and define the expected component and
  contract labels. The no-argument ad-hoc build has an empty version label and
  fails runtime compatibility. Current enumeration equals every one of the 74
  recorded candidate input hashes, including nested source/resource/config files.
  Fake-engine execution exercised all four build commands, generated manifest
  and overrides, no release-tag/pull/push operation, and refusal of an existing
  candidate tag before any build. References: `scripts/build_candidate_images.py`
  lines 20–42 and 45–84; each Dockerfile lines 5–23; Makefile build-images target.
- **Local-first and strict local-only.** For each of cli/node/edge/clock, local
  compatible lookup returns the immutable ID without pull/build/container launch.
  Only `ImageNotFound` triggers the default pull. Local-only absence fails without
  pull/build. Local incompatibility fails without substitution. Incompatible
  pulled images also fail, and failed selections are never cached. Daemon and
  registry failures produce safe actionable messages. References: `_images.py`
  lines 51–69; DockerService lines 380–400; CLI lines 2644–2662.
- **Overrides and compatibility.** Overrides use the same exact version,
  component and contract rules as defaults; a release version lacking the
  development suffix, another component, or integer contract `1` fails.
  Invalid URLs/whitespace/non-string references fail before daemon access and
  are not echoed. Docker error markers do not appear in public error strings;
  existing public-main tests check constructor and resolution diagnostics.
  References: `_images.py` lines 19–22, 47–69; DockerService lines 358–364.
- **Immutable identity and lifecycle.** Short hashes, uppercase hash characters,
  mutable tags, integers and absent IDs fail. Reference snapshots survive later
  environment mutation. Forty-eight simultaneous callers per role cause only
  one local lookup and one missing-image pull. Explicit replacement after pinning
  fails. Both independent source inspection and actual controlled calls through
  all seven launch methods confirm pinned IDs after environment/tag mutation,
  including multiple replicas, dispatcher, lease coordinator, retry and clock.
  References: `_images.py` lines 29–46, 63–73; DockerService image arguments at
  lines 423, 480, 561, 643, 859, 942 and 1063.
- **No implicit checkout build/infrastructure startup.** Retained build method
  names resolve images; explicit Dockerfile requests fail and no runtime image
  build call remains. Import/help/version/bundled schema validation/example
  commands complete while Docker access is forbidden. Image preparation alone
  does not launch containers. Public CLI resolves required roles before topology
  checks and service-side provisioning/DDL/planning; the preceding topic/DLQ
  policy checks are read-only. References: DockerService lines 387–400; CLI
  lines 2616–2685; `docs/DOCKER_EXECUTION.md` lines 3–48.

## Actual local image inspection

Read-only `docker image inspect` against each manifest immutable ID confirmed
exact ID, all labels, expected entrypoint, linux/arm64 platform and recorded local
tag. All four recorded registry digest fields remain null.

| Component | Actual immutable local image ID |
| --- | --- |
| cli | `sha256:d8b76aedcc897aebd17dcd4dd2cfecb1e2570a7f625b8d0480f7122100bbc610` |
| node | `sha256:ad35a1ba5badab2aabd5fbd7da1307f655b2e498808c51b82fa791345c33d4e9` |
| edge | `sha256:e98b54e7b60796656e42869d47ae2f3b04d0c0028ee7974320dff5934b9aca5c` |
| clock | `sha256:c22c40dfd33d89faea89b90ed77659f4b12067ee294a6aad65b3144655ae9fbb` |

## Limits and ownership

This review made no real image build, pull, tag, push, container, network or
service mutation. It read no private environment file, contacted no remote IMDb
service and performed no cleanup of foreign resources. Product source/tests and
earlier evidence were unchanged. Only new ignored probe outputs and this review
were written; no commit/push occurred.

Compatibility labels are a documented trust contract for operator-supplied
images, not authentication of arbitrary image provenance. Registry publication,
registry digests/anonymous pull availability and E08's platform matrix remain
unproved here and correctly deferred. Fresh artifact/image execution, actual
installed service load and the full regression suite are the parent reviewer's
separate gates. Approval applies to the exact reviewed image-related product
bytes and locally inspected candidates; any product correction requires review
of affected checks again.
