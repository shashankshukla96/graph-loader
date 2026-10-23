# E06 topology and lifecycle revalidation — 2026-10-23

**Current disposition: R1 source repair independently accepted; final acceptance
still requires refreshed frozen artifact/image/service gates.** The original
finding and pre-repair evidence remain below.

Independently reviewed clean HEAD `e78970cb310a7a265ae6af5db958edc613f1d923`,
the E06 implementation `a034420f98a57d17c7591b23eb1011d7bc346d25`, and its
diff from E05 baseline `a2bc7bbd6e82350a25e9a127454452c360c1ceda`. Read the
epic, execution record, E06/E07 handoffs, topology documentation, new preflight,
CLI ordering, Docker coordinator addressing, relationship monitor, lease-server
signal teardown, tests and installed-service proof helper. No applicable
AGENTS.md was found. This reviewer did not edit product/test source, start
containers, read private environment files, access remote IMDb, or remove any
Docker resource.

## Required repair

### R1 [P2] Preserve database normalization in the new container probe

`src/neo4j_graph_loader/_docker_preflight.py:134` passes
`os.environ.get('NEO4J_DATABASE') or None` directly to `driver.session`.
The existing shared `utils.neo4j_session.open_session`, used by schema
initialization and ingestion, first strips whitespace and then chooses an
explicit database or the driver default. Thus `NEO4J_DATABASE=' neo4j '` worked
as `neo4j`, and `NEO4J_DATABASE='   '` worked as the default database before
E06. The new mandatory probe instead asks for the literal padded/blank database,
reports `neo4j-database`, and aborts an otherwise valid public start before DDL
or ingestion. This violates the stated consistent database-selection contract.

Independent deterministic reproduction uses the actual `open_session` helper,
actual `probe`, successful mocked mounts/driver connectivity, and a driver
whose session rejects nonexistent database names. For each input, the existing
helper selects `{'database': 'neo4j'}` or `{}`, while the probe supplies
`{'database': ' neo4j '}` or `{'database': '   '}` and returns 1 with
`{"ok": false, "phase": "neo4j-database"}`. It does not depend on a live
server or an inferred log message. The two cases are retained in
`var/e06-topology-revalidation-20261023/test_adversarial.py` as
`test_database_normalization_regression`.

Recommended repair: have the probe use the existing shared
`open_session(driver)`, preserving both whitespace normalization and default
selection. Add probe regressions that reach successful Kafka metadata completion
for unset, empty, whitespace-only, padded existing and explicit database values;
keep the real missing-database negative. Record red/green results and refresh
the required frozen artifact/image/service gates after the source change.

## Independently executed checks

Interpreter: `var/e06-validation-20261022/dev/bin/python`, as recorded by the
accepted E06 development-environment receipt. All commands ran from the
checkout, writing only new ignored scratch under
`var/e06-topology-revalidation-20261023/` plus this report.

1. Focused existing tests: `test_docker_topology.py`, `test_docker_service.py`,
   `test_relationship_bulk_monitor.py`, `test_runtime_paths.py`,
   `test_cli_dead_letter.py`, `test_cli_shutdown.py`, and `test_lease_server.py`,
   with `-m 'not integration and not smoke' -q -rs`: **255 passed in 44.94s**,
   no skips or warnings. This includes the five E05 symlink-loop cases and the
   real ten-process lease-server signal storm.
2. New ignored adversarial controls: **14 passed in 0.94s**. Auth/database/
   bootstrap/metadata failures return only closed phase names despite injected
   secret sentinels; successful bootstrap is followed by connection to the
   advertised broker address; unknown/malformed JSON payloads remain redacted
   and clean the exact owned container/markers. A public-main failure proves
   image selection occurred and provisioning, DDL, planning, control-topic
   creation and ingestion launch were not reached. The two normalization cases
   above confirm the required repair.
3. Independently loaded baseline lease-server source from Git and repeated the
   real `serve`/SIGTERM/asyncio-close boundary: baseline returns **1**, current
   implementation returns **0** under the same precisely injected second
   SIGTERM. This corroborates the narrow teardown repair without claiming the
   discarded-stderr historical storm failure had this conclusively known cause.

Raw logs: `focused.log`, `adversarial.log`; executable reproduction:
`test_adversarial.py`; baseline comparison source: `baseline_lease_server.py`;
targeted input hashes and historical log checks: `results.json`, all beneath
the owned ignored scratch directory above.

## Accepted boundaries and remaining limits

Apart from R1, the examined implementation supports its documented local
socket/Desktop + existing user-defined bridge topology. It rejects TCP/SSH
daemon and host/default-bridge configurations before creating probe resources.
Public-start topic policy checks retain their earlier read-only ordering;
image selection and topology checks precede provisioning, DDL and planning.
Schema/external-plan strict read-only binds, external/future plan parents,
per-role markers and container-to-host output writes cover the intended path
identities. Probe creation retains the daemon-returned ID before start/wait;
failures remove only that container and exclusively created markers. Cleanup
failure prevents success. A lost create response still cannot supply an ID;
the attempt label is diagnostic and does not justify a foreign/broad sweep.

Host lease readiness uses an ephemeral published loopback port, while writers
and retry consumers use network IP/stable DNS. The dispatcher also separates
host and writer addresses. Signal repair sets SIG_IGN before the event loop
closes and preserves the outer exit protection. Existing focused tests found no
cleanup/shutdown regression. Probe container diagnostics deliberately discard
raw stderr/driver/daemon bodies and admit only a fixed phase vocabulary; the
new adversarial controls cover malformed payloads as well as expected failures.

The historical installed-service proof is credible within its stated scope:
the helper asserts outside-Git installed origin, scratch/default database
isolation, 13 nodes/12 property-checked relationships, node commits 12/1, edge
source boundary, complete 12-row ledger, normal/external shared output paths,
foreign-sentinel preservation, real host-monitor readiness/container DNS ping,
negative controls and owned cleanup. Independently matched all **10** recorded
command log hashes against the retained `finalb/service/` logs; expected exit
codes and no-timeout fields agree. The missing-bind control is a direct strict
Docker API rejection rather than a full public-main run. Tiny rows do not force
spill, and the record does not claim they do.

This reviewer did **not** rerun live installed-service infrastructure; the
parent is performing a fresh proof. Historical receipts, Linux arm64 images and
native macOS checks do not establish E08's wider matrix, remote orchestration,
publication/anonymous pulls, or the future E09 service-ordering guarantees.
R1 and refreshed final gates must close before E06 revalidation is accepted.

## R1 source repair re-review

The actual repair imports `utils.neo4j_session.open_session` in the probe and
replaces only the raw `driver.session(database=...)` call with
`open_session(driver)`. It now shares exactly the database/default-selection
semantics used by schema initialization and ingestion. Topology, error phase,
mount, credential, timeout and cleanup behavior are unchanged by the two-line
repair. No product change was made by this reviewer.

The five new tracked cases exercise unset, empty, whitespace-only, padded and
explicit database values through the actual probe. They create actual owned
temporary schema/marker files, reject an incorrect session target, and require
successful metadata retrieval plus connections to both the bootstrap and
advertised broker. They are meaningful regression controls for this repair.

Independently adapted the owned adversarial controls into a separate
`test_adversarial_repaired.py`, preserving the original `test_adversarial.py`,
`adversarial.log` and `results.json` without alteration. The repaired independent
cases compare the probe's exact session keyword arguments with the actual shared
helper for all five states and require successful metadata and advertised-broker
completion. Reran those controls plus all tracked `test_docker_topology.py`
tests: **44 passed in 0.75s**, no skips or warnings. Auth/database/bootstrap/
metadata failure redaction, malformed probe-payload cleanup, public-start
ordering and the baseline-red/current-green native signal boundary still pass.
Targeted whitespace check passes. New receipts are
`repaired-adversarial.log` and `repaired-results.json` under the same owned
ignored scratch directory; the latter records the exact two repaired input
hashes and log hash.

**R1 is closed at source/unit level.** The parent will refresh the full frozen
gates and execute fresh owned installed-service proof, including a padded named
database tiny load and whitespace-only default-database probe. This re-review
does not claim those pending live gates have already passed and does not assess
the separately investigated parked-index race reported by the parent.
