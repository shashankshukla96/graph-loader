# Independent E06.F1.S2 review — 2026-10-22

**Final disposition: accepted for E06.F1.S2 after repair and independent re-review.**

Initial disposition was changes required. Original R1 and its historical evidence
are retained below; the repair and independent acceptance are recorded at the end.

This independent story review covers deterministic image selection and overrides
only. It does not accept the next host/container story, installed ingestion,
publication, final frozen artifacts or E06 completion. Reviewed the execution
plan, plan and S1 reviews, canonical E06 and D06, `_images.py`, DockerService/CLI
changes, compatible-image controls/installed fixture, Makefile/docs, S2 candidate
and installed evidence, and raw S2 logs. Entry is
`a2bc7bbd6e82350a25e9a127454452c360c1ceda`.

## Original R1 — Docker-client initialization bypasses diagnostic redaction (P2)

The new image-resolution preflight catches every exception and logs its string
at `cli.py:2628–2630`, but `DockerService.__init__` originally called
`docker.from_env()` before entering the safe ImageSelection exception boundary.
Docker performs a server-version request during client construction, so a
constructor failure reaches the new CLI diagnostic as a raw Docker exception
body. This is observable in the author's own `s2-clean-daemon.log`: the reason
contains `Error while fetching server API version` and the nested raw connection
exception rather than the promised stable safe diagnostic. A Docker endpoint or
proxy error can carry sensitive request/body material; the get/pull regression
only covers failures after successful client construction.

Wrap client initialization in the same safe `ImageSelectionError` policy,
preserving the original cause for programmatic diagnosis without including it in
public text. Add a public-main regression with a synthetic sensitive marker in a
constructor failure and assert status 1, no marker/traceback and no later service
access. Refresh installed wheel/candidate evidence for the changed runtime bytes,
then independently re-review. Preserve the original raw daemon proof and this
finding.

## Checks passed before repair acceptance

- Independent focused selection plus seven-role mount run: 25 passed in 0.14s
  (the author's constructor regression was present by execution).
- Full initial focused run completed with `739 passed in 111.26s`, recorded in
  `s2-final-tests.log`. It began before R1 repair; it does not certify the changed
  constructor bytes. Author's repaired 59-case subset also passed.
- Source uses exact package-version defaults under the approved GHCR namespace.
  Default and override images must match all three exact version/component/
  contract labels and a full immutable local SHA-256 ID. Local incompatibility
  fails without pull, and only ImageNotFound invokes missing-only pull. Strict
  local-only resolution neither pulls nor builds. Get/pull bodies are safely
  wrapped. No runtime image build remains, and explicit old Dockerfile arguments
  fail with an actionable compatibility-entry-point diagnostic.
- All seven direct container launch paths call the same instance resolver. CLI
  stores one DockerService on its run namespace and passes it through phases.
  Independent `s2-review-probes.py` exercised all seven launch paths concurrently,
  with 16 total replicas, after pinning and then changing environment references
  and making subsequent tag lookups fail. All 16 used the original exact role IDs;
  exactly three image lookups occurred, with no pull/build. Repeated CLI factory
  calls retained one instance. Mutable/malformed/missing IDs were rejected.
  Results are in `s2-review-probes.log`; the initial harness import-path failure
  is preserved separately as `s2-review-probes-initial.log`.
- Compatible-image mock support returns explicit actual label/id shapes rather
  than allowing MagicMock values through. The installed public-main fixture
  still preserves its original runtime-path/ownership assertions.
- Fresh outside-checkout installed evidence resolves preloaded node/edge/clock
  IDs from installed site-packages; actual public starts return 1 for missing,
  wrong-component and unavailable-daemon cases. The initial daemon diagnostic
  is exactly the R1 failure above. These are selection proofs; they do not claim
  the next story's successful installed ingestion/topology gate.
- Makefile and docs describe deliberate candidate builds, versioned selection,
  strict local-only skip behavior, ownership and unpublished registry proposals.

Only this review and owned raw review-probe files were written. No product/test
edits, private environment-file access, remote IMDb access, application service creation,
foreign cleanup, publication, commit or push was performed by this reviewer.


## Repair re-review and final acceptance

The repaired constructor uses a narrow diagnostic boundary around
`docker.from_env()`, raises a stable safe `ImageSelectionError`, and preserves
its cause without interpolating the Docker body. An explicitly supplied client
is retained as supplied. The added actual public-main regression puts a
synthetic sensitive marker in the constructor exception and verifies status 1
and its absence from captured diagnostics. Independent repaired focused run of
selection, DockerService and runtime-path checks passed **115 tests in 0.39s**,
with no skips (`s2b-review-tests.log`). The completed initial 739-case run is
retained as historical wider coverage; its single constructor repair is covered
by the new regression and independent repaired installed checks.

Independently verified repaired tracked evidence equals raw `s2b` evidence byte
for byte. All 73 candidate manifest input hashes match current source. The fresh
repaired wheel contains exact current `_images.py`, DockerService and CLI bytes.
Each of the four rebuilt immutable candidate IDs was independently run with a
read-only root, no network, no source mounts, work directory `/` and only `/tmp`
writable; installed file hashes for those three runtime files match current
source in every image. These transient owned checks created no application
infrastructure and removed their own containers.

Independently reran the fresh noneditable environment's installed resolver from
its outside-checkout directory with isolated Python, actual local images and
exact labels/IDs. It resolved all node/edge/clock IDs successfully. Then actual
installed public starts, each with a fresh owned run directory, returned status 1
for a missing local override, a wrong-component override and a nonexistent Docker
socket. All had no traceback; the daemon case displayed only the safe
`cannot access Docker daemon; check Docker endpoint and permissions` message,
with neither the old API-version body nor nested raw exception. Receipts:
`s2b-review-installed.json` and `s2b-review-installed-{resolve,missing,incompatible,daemon}.log`.

R1 is repaired and independently rechecked. No actionable S2 finding remains.
The next host/container story and all final E06 freeze/integration/release gates
remain required and outside this acceptance.

Reviewed repaired SHA-256 identities:

- `_images.py`: `a53f0d3a173c00bf24f7aca551d86a1ceb4b1258655f5b4fc70e6cb456bcf55e`
- `docker_service.py`: `ff23b2210b0669f61e256c433199bac8c863f13504a0177c641ddd7ea8e8b47a`
- `cli.py`: `0b1471874c4f689d64c7de18ce0087c2698f33780fd449289252099c70dc8f1a`
- `e06-s2b-images.json`: `4ecda80975eb6429833e4b5ff1a70a37f92acf51bf889d1175517287bda6dd4f`
- `e06-s2b-installed.json`: `27fedb6a7df6593fe9483b3b986e8467ff96d2190da1ddd5aac9fb2601c6c64d`
