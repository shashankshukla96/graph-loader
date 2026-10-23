# Independent E06.F2.S1 review — 2026-10-22

**Final disposition: accepted for E06.F2.S1 after repair and independent re-review.**

Initial disposition was changes required. Original R1/R2 and their historical
receipts remain below; final repair acceptance is recorded at the end. No
actionable S3 finding remains.

This independent story review covers the supported local Docker topology,
preflight diagnostics and lifecycle, host/container coordinator addresses and
actual installed tiny-load evidence. It does not accept final frozen artifacts,
the full inherited B3/R1 gates, publication or E06 completion. Reviewed the E06
execution plan, independent plan/S1/S2 reviews, canonical E06 and D06, the actual
preflight/CLI/DockerService/monitor/runtime paths, topology documentation, focused
regressions, service-proof helper and S3e service/image evidence. Only this report
and new owned review outputs were written; no product edits, private `.env`
access, remote IMDb access, foreign cleanup, publication, commit or push occurred.

## Original R1 — probe start failure loses ownership of created container (P2)

The initial `check()` assigns its owned container from
`client.containers.run(..., detach=True, remove=False)`. Docker SDK's actual
`ContainerCollection.run` creates a container, calls `container.start()`, and
only then returns the object. If start raises (for example a runtime, mount or
network-start failure), `check()` retains `container=None`, so its finally block
removes host markers but cannot remove the already-created owned container.
The advertised preflight cleanup guarantee is therefore false on this path.

Independently reproduced using the real SDK `ContainerCollection.run` with only
daemon creation/start mocked: `create=1`, `start=1`, `remove=0`; the stable safe
error was raised and all host markers were removed. Receipt:
`var/e06-validation-20261022/s3-review-start-failure-*/repro.txt`.
The existing create-failure mock cannot cover this boundary because it raises
before returning or retaining a created object.

Follow-up SDK inspection/reproduction also found `containers.create()` itself
receives a daemon ID from low-level create and then calls `self.get(ID)`. A
hidden inspect failure at that point similarly prevents retaining the known
owned ID. Receipt: `var/e06-validation-20261022/s3-review-sdk-create-inspect.txt`.
The repair must retain the direct create response ID before any subsequent
start/inspect/log/wait call, and clean up only that exact ID.

The same delayed-ownership pattern affects host markers: `created.append(path)`
occurs after `output.write()` and context close. If exclusive file creation
succeeds but write/flush fails, the file is omitted from cleanup. Independent
controlled write-failure reproduction observed one leftover marker; the reviewer
then removed only that owned marker. Receipt:
`var/e06-validation-20261022/s3-review-marker-write-*/repro.txt`.

Retain the ID immediately from direct low-level create, then start it, so finally
can remove that exact owned ID after a later failure without a hidden SDK
inspect boundary. Record marker ownership
immediately after successful exclusive open, before writing/flushing. Add
regressions with start failing after creation and marker write/close failure; preserve redaction and cleanup-failure behavior.
Refresh exact-source installed/image proof and independently re-review.

## Original R2 — external edge-plan input bypasses daemon mount identity check (P2)

`prepare_run()` preserves an explicit external `--edge-plan` while
`runtime.plans` remains the separate private/default plan-output directory.
Preflight checks only `runtime.plans`. A successful preflight therefore says
nothing about the actual external plan file or its parent, although the
subsequent dispatcher/writers bind that file. A shared run directory can pass
while an unshared or different daemon-side external plan fails only after
service changes/node ingestion. The launcher also uses legacy `volumes` binding
for this file, which does not provide the strict missing-source semantics used
by the probe.

Independent real `prepare_run()` + actual `check()` reproduction (only Docker
mocked) recorded a successful preflight whose five mount sources include neither
the explicit existing external plan nor its parent. Receipt:
`var/e06-validation-20261022/s3-review-external-plan-rsyvqjte/repro.json`.

Cover the resolved actual external plan parent in the daemon identity probe,
retaining E05's supported future/absent external plan input and sibling-ledger
ownership semantics. Check an existing plan's bytes read-only where feasible;
never create, overwrite or delete caller plan files. Add explicit external-plan
regressions and refresh exact-source evidence before acceptance.

## Initial checks and outstanding evidence

- Independent focused source run: **179 passed in 0.72s**, no skips, covering
  topology, monitor, runtime paths, DockerService and image selection. Receipt:
  `var/e06-validation-20261022/s3-review-focused-initial.log`.
- Author's `s3c-focused-tests.log` records **185 passed in 0.73s**. The five E05
  public symlink-loop cases remain present (run-dir, plan-dir, spill-dir,
  timing environment and edge-plan), including the narrow path error boundary.
- Network/remote-daemon rejection, strict schema/output marker checks,
  closed-set failure phases and driver/daemon secret redaction are appropriately
  scoped. The probe bounds its container wait and closes/removes retained owned
  objects; this does not change the public planner's cancellation API or its
  existing aggregate cleanup contract. Public-main preflight failure is tested
  before DDL; actual service controls return status1 before launching ingestion.
- S3e records an actual noneditable outside-checkout tiny planned load against
  owned Neo4j5.26 Enterprise/Kafka3.7, exact13 nodes/12 property-verified edges,
  node offsets12/1, selected database/default-database isolation and owned
  cleanup. The seven negative controls distinguish auth/database/container
  Neo4j/container Kafka/network/advertised metadata/missing bind. Earlier proof
  harness failures are retained. This receipt predates the last cleanup fix and
  the two findings above, so it cannot accept repaired current source.
- The successful planned load exercises dispatcher host control and writer
  paths. Separate lease host/container PING is present, but actual installed
  `RelationshipBulkMonitor.wait_for_lease_server_ready()` with its default
  client factory must additionally run against the real published host port.
  The updated unit assertion alone is insufficient for that specific regression.
- Stronger helper ledger/source-boundary/spill assertions and exact rebuilt
  current-source wheel/images must finish and be independently checked.

Repair/re-review evidence and final story disposition will be appended here;
original findings and historical failed receipts will remain preserved.


## Repair re-review and final story acceptance

The final repair uses Docker's public low-level create response to retain the
exact owned ID before start, wait or logs. It performs no high-level hidden
inspection, name deletion or broad label sweep. All subsequent known-ID failures
attempt removal of that exact ID. Marker paths are retained immediately after
successful exclusive open, before write/flush/close. Cleanup errors fail the
command with safe diagnostics. A daemon failure that loses the create response
cannot supply an ID; the attempt label permits diagnosis without pretending to
provide a universal cleanup guarantee through daemon failure.

An explicit non-auto external plan is now passed from public main through
DockerService into topology checking. Its resolved actual parent receives the
plan identity marker; an existing input additionally gets a strict read-only
file bind and SHA-256 comparison inside the probe. Future absent external plans
remain allowed and are never created by this check. Per-role marker suffixes
permit shared output directories and external parents without collisions.
Caller files, sentinels and output ownership are preserved.

Independent final focused run passed **191 tests in 0.75s**, no skips, in
`s3-review-lowlevel-focused.log`. It covers topology, monitor, runtime paths,
DockerService, image selection and candidate construction, including all five
E05 public symlink-loop regressions. Independent additional final probes checked
start-failure exact-ID cleanup, successful marker open followed by close failure,
all four output roles sharing an external plan parent, the strict read-only plan
bind, preserved foreign sentinel, the 40-second wait argument and unknown-phase
redaction. Receipt: `s3-review-lowlevel-probes.log`. Earlier intermediate
189-case and additional probe receipts remain retained; the first standalone
review helper import-path failure is retained separately and was a harness issue.
The author's wider repaired **245-case** focused receipt also passes. Read-only
input-topic/DLQ policy rejection is restored before image access; image selection
and topology still precede provisioning, DDL and planning. This preserves the
existing early-rejection behavior and meets the E06 no-mutation-before-preflight
boundary.

Independently validated the repaired actual proof in raw `s3g-service/`:

- Tracked `e06-s3-repaired-service.json` and `e06-s3-repaired-images.json` equal
  their raw receipts byte for byte. All **74** current candidate input hashes
  exactly equal a fresh `build_inputs()` enumeration; all **10** recorded
  command-log hashes match the retained logs, with no supervisor timeout.
- The actual normal installed load returns0 in16.32 seconds, with exact13 nodes,
  twelve property-verified edges, selected-database/default-database isolation,
  committed node offsets12/1 and plan source boundary `[0,0,12]`. Its completed
  two-entry ledger covers all12 planned rows. Retained timings exist and the
  planner-owned temporary spill child is gone. Tiny input does not force spill;
  actual preflight establishes shared spill-directory identity.
- A second real installed CLI load returns0 with an explicit external plan whose
  parent also contains spill/timings; its completed ledger covers12 rows and the
  foreign sentinel survives. This exercises the repaired external read-only
  file/hash branch on the actual daemon.
- `lease-addresses.log` records host loopback PING, an actual constructed
  RelationshipBulkMonitor calling default-factory readiness successfully, and
  a second container's stable-network-DNS PING. The normal planned load already
  exercises dispatcher host control and writer coordination.
- All seven actual auth/database/container-address/network/advertised-metadata/
  strict-missing-bind controls pass. Failure logs retain phase-only diagnostics
  and no credential markers or traceback. The public-main source regression
  proves failure before DDL. No ingestion fleet is launched by preflight failure.
- Independently queried each exact recorded owned infrastructure ID and network:
  all are absent. No application containers with that exact owned run ID remain.

The actual proof's new noneditable interpreter lives outside the checkout and
Git discovery from its working directory independently returns128. Its installed
version/origin and four relevant module hashes match current source. All **59**
package members in its wheel are byte-identical to current source/package data.
Each of the **four** exact S3d image IDs was independently run with a read-only
root, no network, no source mounts and only `/tmp` writable; version, installed
site-packages origin, labels and current preflight/CLI/DockerService/monitor
hashes match. These owned review containers were removed by exact returned ID.
Receipts: `s3-review-receipts.json`, `s3-review-receipts.log`,
`s3-review-images.json` and `s3-review-images.log`.

R1/R2 are repaired and independently rechecked. This accepts the S3 story only.
Final full-suite/artifact/frozen-image/B3/R1 and fresh adversarial E06 gates remain
required and outside this verdict. Earlier failed, superseded and intermediate
proofs remain preserved; no publication, foreign cleanup or E07–E12 work occurred.

Reviewed final SHA-256 identities:

- `_docker_preflight.py`: `594474078e90788e678c8d1a907991c838946bd993c21f2d3f1648df99fd645e`
- `cli.py`: `613e7114ab1f64f463fe6e6972a42f7477c3bc061478b15fc102a879c1533ff6`
- `docker_service.py`: `c8e9696fea01375dbe8b83f749e1f77ae5ab584cc8fcbab72b09826d02ccf5a4`
- `relationship_bulk_monitor.py`: `a2eebfc02bb7233f6d48d405a06dee203cfc0b02b961dd343fc46b1194e36a25`
- `DOCKER_EXECUTION.md`: `9b2479c0890b7bd6dd281e37aa7d032a4059022f3cafd8daba38715e4d44c271`
- `run_e06_service_proof.py`: `2647a7e54125579c1c59d13b22cc5c45fcb844c607e0ce804bc28c3542ed26f7`
- `e06-s3-repaired-service.json`: `42a5b9e0c29dad52498a75f2685337563dd08aa14460afc30080be8b2bfa90d3`
- `e06-s3-repaired-images.json`: `cfb038f3dc8ef10eb6d7a8d55648e783ab80952165b0aefa6a5453dc4a4b9f6b`


## Final-gate R3 — repeated SIGTERM can reach a closed event loop (P2)

**Initial post-acceptance disposition: repair/re-review required.** The later final full checkout run
retains a real failure of the unchanged existing ten-run process-storm regression:
`test_sigterm_storm_to_the_lease_server_process_still_exits_zero` observed exit1
instead of0. Its child stderr was discarded, so the original failure cause
cannot be recovered conclusively. The author's subsequent natural150-run
captured-stderr diagnostic passed; that does not explain away the original gate.

Independent source inspection found the plain SIGTERM handler still invokes
`loop.call_soon_threadsafe(stop.set)` after `asyncio.run()` closes its loop and
before `main()` reaches its final `SIG_IGN`. A second stop signal in that window
raises `RuntimeError("Event loop is closed")`; main catches it and returns1.
Two separately run deterministic independent probes confirmed this failure:

- `storm-review/closed-loop.log`: stub only the async service; wrap the real
  `asyncio.run` and deliver SIGTERM immediately after it closes the loop, before
  main's finally. Public main returns1 and logs `Event loop is closed`.
- `storm-review/real-shutdown.log`: run the actual service on a new owned
  `127.0.0.1:0` listener with metrics disabled; schedule the first real SIGTERM
  through the event loop, let service shutdown and real `asyncio.run` finish,
  then deliver the second signal in the same closing boundary. Normal first
  shutdown completes, but public main again returns1 with the closed-loop error.

These prove a concrete lifecycle defect compatible with the full-run observation;
they do not claim the discarded original stderr proved this exact cause. Both
review probes retained logs under `var/e06-validation-20261022/storm-review/` and
created no remote service or foreign resource.

A narrow repair should install `SIG_IGN` inside the service coroutine's finally,
before the coroutine exits and asyncio closes the loop, while retaining the outer
final ignore at interpreter teardown. A deterministic public-main regression must
inject the late signal and require exit0; the existing actual ten-run storm
assertions must remain intact. Independently recheck the repair and repeat every
affected exact-byte image/artifact/service final gate before final acceptance.


### R3 repair re-review

**R3 source repair accepted after independent re-review.** The repaired `run()`
coroutine wraps the existing `await serve(...)` in a nested finally that installs
SIGTERM `SIG_IGN` before coroutine exit and event-loop closure. The existing outer
main finally remains intact for interpreter teardown. Normal service operation,
actual first-signal shutdown, genuine service exceptions and their nonzero result
handling are preserved; the repair only closes the late repeated-stop window.
The original process-storm test is byte-for-byte unchanged within its function;
the additional deterministic public-main test requires the real loop to be closed
before delivering the second stop and retains the exit0 requirement.

Independently reran the review's actual-service boundary probe after repair:
`storm-review/real-shutdown-repaired.log` records normal first-SIGTERM shutdown
followed by public-main status0 after the second late signal. Independently ran
the full lease-server/topology/image-selection/relationship-monitor focused set:
**156 passed in 4.16s**, no skips, including the original ten-run actual process
storm and new deterministic regression. Receipt:
`var/e06-validation-20261022/storm-review/repaired-focused.log`.
The author's matching156-case receipt also passes. No original assertion was
weakened; the old full-run failure, natural150-run nonreproduction, deterministic
red proof and separate independent red/green logs remain preserved. The original
discarded-stderr failure still cannot be attributed conclusively beyond the
proven compatible lifecycle hole.

This narrow source repair does not certify prior-source frozen artifacts.
The planned finalb package/image/installed-service/R1/B3 checks must bind the
new lease-server bytes; final fresh adversarial acceptance remains separate.
The reviewer made no product changes or foreign-resource operations.

Reviewed R3 SHA-256 `src/neo4j_graph_loader/orchestrator/lease_server.py`: `37b25e19d72adf49546095ad32b3b6adafd27a860f76ba7cb67322d12f7594a0`.

Reviewed R3 SHA-256 `tests/test_lease_server.py`: `5184bb72f73d115ba826da35c70876100738135efb16f6190528142bf2c571b9`.
