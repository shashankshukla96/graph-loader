# E09 execution plan — prove service behavior and ordering limits

Reset on 2026-10-31 by decision [D08](decisions.md). The earlier, much larger plan (fenced daemon, broker with
durable settlement, sandbox bootstrap) is retired; a copy is in `var/plan-reset-backup-20261031/`.
Story acceptance criteria and validation commands are authoritative in [epics/e09.md](epics/e09.md).

## Goal

Executed evidence that the **installed** wheel and **installed** candidate images work against real, throwaway
Neo4j 5.26 Enterprise and Kafka 7.7.0 for every mode the release supports, and that the restricted ordering
contract (D02, [ordering-matrix.md](ordering-matrix.md)) is stated and tested exactly. This is checklist V6/V7
evidence for a library release, not a security sandbox.

## Substrate

- Host `shashank@192.168.1.75` (Ubuntu 24.04, amd64), native Docker Engine 29.9.0 at `/var/run/docker.sock`,
  key-auth SSH, `shashank` in group `docker`. See [migration record](evidence/e09-docker-migration-20261030.md).
- The engine is shared with the user's own stack. Never touch, stop, prune or reconfigure: containers `postgres`,
  `graph-loader-neo4j`, `graph-loader-kafka`, `graph-loader-kafka-ui`; networks `graph-loader-net`, `ws_default`;
  volumes `graphloader_neo4j_data`, `graphloader_neo4j_logs`; data under `/media/shashank/Data/graph-loader/` and
  `/volume/postgres`; host ports 5432, 7474, 7687, 8080, 9092, 9101.
- Results are labelled "native shared Docker Engine substrate".

## Rules (all stories)

1. Installed code only: fresh non-editable environment outside Git with the candidate wheel; explicit candidate
   image IDs; no checkout source on any import path. The installed runner (`scripts/verify_installed.py`) enforces it.
2. Owned resources only: owner label and `e09-<token>-` names, removal by recorded ID, and a before/after snapshot
   of every container, network, volume and image that must be unchanged apart from owned resources.
3. Bounded runs: every controller and client has a hard deadline; no unbounded waits or output.
4. Real assertions: graph contents and properties, committed offsets, ledger/plan digest where relevant, cleanup.
   A "zero" claim needs a positive control (the lock-wait detector must see a real conflicting write).
5. Evidence: one passing lane run at the final bytes (`result.json` and `command.json` from the runner), repeated
   three times only for timing-sensitive cases; red runs kept and explained; a short `evidence/<story>.md`.
6. Inventory: new service tests join a lane through `scripts/freeze_installed_inventory.py --freeze`; existing IDs
   and dispositions never change silently.
7. Review: the implementer self-checks each story; one independent review covers E09 as a whole before its exit
   gate (and earlier only if a story changes product code). No commits, pushes or uploads without instruction.

## Order and status

| Story | Scope | Status |
| --- | --- | --- |
| E09.F1.S7 | Live harness: owned stack, installed CLI, provenance, observers, lock-wait detector, `service-harness` lane | complete ([evidence](evidence/E09.F1.S7.md)) |
| E09.F1.S1 | Planned bulk end to end; worker loss fails closed; replay converges; lock-wait evidence | complete ([evidence](evidence/E09.F1.S1.md)) |
| E09.F1.S2 | Streaming leases: graph, stable group offsets, node-before-edge, graceful restart/resume | planned |
| E09.F1.S3 | Classic mode (included by D02/D04): final graph/offsets/cleanup, lock behaviour | planned |
| E09.F1.S4 | Installed DLQ: routing, identity, retry limits, parking/replay, safe offsets | planned |
| E09.F1.S5 | Outages: short Neo4j outage, lost coordinator/writer, graceful stop with work outstanding | planned |
| E09.F2.S1 | Ordering O1–O8: asserted outcome or approved exclusion per row; order_by probes | planned |

The S7 `service-harness` lane is rerun at E09's final bytes (the reset and S1 changed some of its inputs; behaviour
is unchanged). arm64 (D04) is covered by an installed-CLI smoke on the final arm64 images in E11.F1.S3; E09 runs on
the amd64 host only.

Exit gate: every supported mode and ordering case has an executed result with graph/offset/cleanup evidence;
unsupported cases are documented and excluded from guarantees (feeds U3 in E10).
