# E13 stream soak harness

Evidence that `graph-loader start --mode stream` on the frozen `0.1.0a1` candidate keeps its documented guarantees under sustained,
realistic traffic and injected faults. Plan: `plan/library-implementation/epics/e13.md`. This directory is a test harness: it is not
part of the package (`MANIFEST.in` is `global-exclude *` plus explicit includes), and it imports nothing from `src/`.

**Installed code only.** The harness runs from a venv that holds the frozen wheel (`b420c784...e199`) and talks to the four final
amd64 image IDs from `evidence/E11.F1.S3-manifest.json` (`--skip-image-build`, images selected by ID).

## Layout

| Module | Role |
| --- | --- |
| `spec.py`, `schema.yaml`, `schema.py` | The graph (3 labels, 4 relationship types, 7 topics, 11 application containers) and the stream schema rendered per run |
| `journal.py` | SQLite journal of logical records and physical sends (never enqueued / acknowledged / uncertain) |
| `generator.py` | Seeded traffic: Zipf endpoints, early relationships, updates, nulls, duplicates, out-of-order `order_by`, malformed, caps |
| `oracle.py` | The expected graph, folded from the journal only (never from Neo4j) |
| `kafka_io.py` | Publisher, offset/read-back helpers, the pure drain predicates, uncertain-send resolution by `e13-send` header |
| `graph_read.py` | Paginated reads of the scratch graph with temporal normalisation |
| `checker.py` | Quiet points and reconciliation checkpoints (pending / inconclusive handling, DLQ frontier, replay journalling) |
| `stack.py` | `tests/installed_service/live_stack.py` under the `e13` namespace, memory limits, rolling query log |
| `querylog.py` | Incremental query-log reader across rotation and restarts; lock-wait classification; coverage failures |
| `fleet.py` | One CLI incarnation + `Supervisor` (narrowed recovery, unexplained-restart accounting) |
| `faults.py` | The fault schedule's building blocks and their documented expected outcomes |
| `sampler.py`, `logs.py`, `budget.py` | 15 s sampling, host guards, container log follower, memory-budget inequality |
| `runtime.py`, `run_soak.py` | The owned run and its driver (`smoke` now; calibration and the compressed run in S2) |
| `tests/` | The harness's own tests with the negative controls (`pytest soak/tests`) |

## Running

```bash
# On the Linux host, from the harness checkout, with the frozen wheel's venv
E13_GRAPH_LOADER=~/e13-soak/venv/bin/graph-loader ~/e13-soak/venv/bin/python -m pytest soak/tests -q
~/e13-soak/venv/bin/python -m soak.run_soak smoke --manifest ~/e13-soak/manifest.json --venv-bin ~/e13-soak/venv/bin
```

Everything a run creates is named `e13-<token>-...`, labelled with the owner token, registered in an append-only
`registry.jsonl` and removed by recorded ID. A run records a before/after snapshot of every foreign container, network, volume and
image and is red if anything changed. It never touches the user's services, ports or volumes.

## Rules the harness encodes

* **Never the documented `kill -9` recovery filter.** `docker rm -f $(docker ps -aq --filter label=app=graph-loader)` removes every
  graph-loader container on a shared engine. The supervisor's recovery removes only containers its own observer adopted on the owned
  network, re-verified as owned immediately before removal, by recorded ID.
* **Parking is never modelled as "absent".** A parked record may later be written by a restarted retry consumer, and a replay
  acknowledgement does not prove a record was applied. Quiet points exclude the identities of parked records (`pending`);
  reconciliation checkpoints replay them with the generator paused and compare them strictly.
* **Coverage is proved.** Query-log segments, query ids and sampling intervals are accounted for; a gap is a coverage failure, never a
  zero. The lock-wait detector has a positive control in every fleet incarnation.
* **The database name keeps the `e09<16 hex>` form** because the strict query-log parser only tolerates the `CREATE DATABASE` of exactly
  that name; everything else the harness names is `e13-`.

## Host shapes (E14.F3.S10): the same soak on processes and on a systemd user unit

`python -m soak.run_soak host-smoke|host-compressed --shape process|service --venv-bin <venv>/bin --wheel <wheel> --wheel-sha256 <hash>`
with `SOAK_NAMESPACE=e14` (owned names become `e14-<token>-...`; the E13 default is unchanged and its tests still pass). No container
image is involved: the fleet is `graph-loader start --mode stream --runtime process` (and the bulk handover is a process-runtime bulk
run); only the owned Neo4j and Kafka are containers, reached on loopback (Neo4j on a published port that survives restarts).

| Module | Role |
| --- | --- |
| `host.py` | `verify_wheel` (wheel hash, then every installed file against the wheel's RECORD), the fleet view read from the run registry (`registry.jsonl`) and `/proc`, `ProcessFleet` (the harness starts and restarts the command, pinned with `taskset`), `Unit`/`ServiceFleet` (the shipped `examples/systemd/graph-loader.service`, substituted and linked as `e14-<owner8>-soak.service`) |
| `host_runtime.py` | `HostSoak` (preflight with the wheel check, budget from `process_peaks.json`, per-process RSS/CPU rows, `fleet_integrity` = no second fleet, Neo4j internals), `HostFaults` (SIGTERM during an outage holds the relaunch until Neo4j is back; `kill -9` of the command measures the lifeline), `FileLogCollector` (child log files; relayed copies in the command's output are not counted twice) |
| `stack.py` | adds `internals()`: JVM heap and GC (JMX), page cache and data size (Neo4j's Prometheus endpoint), `du` of the store |

Restart ownership: `process` - the harness is the only launcher; `service` - systemd is. After an exit 0 (SIGTERM during an outage,
graceful stop for the replica change) systemd does not restart, so the harness issues the explicit `systemctl --user start` and
nothing else; it never starts the command itself. Deviations of the soak unit from the shipped file (recorded in the run summary's
unit text): `CPUAffinity=4-11`, `MemoryMax=3G`, `MemorySwapMax=0`, `StartLimitBurst=30` (nine faults in a few hours would otherwise
hit the shipped 5-in-600-s limit).

Fault mapping: container kill -> SIGKILL of the child process; `docker kill`/`kill -9` of the CLI -> SIGKILL of the command
(`systemctl --user kill -s KILL --kill-whom=main` under systemd), after which the children must be gone through the lifeline/manager
(no container recovery exists; any orphan the harness has to remove is red); per-container RSS/OOM -> per-process RSS and the kernel
log plus the unit cgroup's `oom_kill` count. `process_peaks.json` holds the per-process peaks measured at 400 ops/s after the baseline
re-read (the 2x rule's reference for the host shapes).
