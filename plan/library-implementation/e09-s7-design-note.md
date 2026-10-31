# E09.F1.S7 design note — checkpoint A (2026-10-30)

Substrate label for every result: **native shared Docker Engine substrate** (Docker 29.9.0 on `192.168.1.75`, shared with the user's `postgres`/`graph-loader-*` stack). Nothing below has been built yet; the only thing run is the unchanged `live_smoke.py` (frozen sha256 `718883c2…abfa`, host run `~/e09-s7-runs/smoke-confirm-1`, exit 0, Neo4j 5.26.32 Enterprise + APOC + verbose query log, Kafka 7.7.0 round trip, exact cleanup by ID, foreign snapshot equal).

## Facts found that shape the design

1. **The wheel has no creation-observation adapter.** `grep` of `src/` finds no intent/channel hook; `scripts/service_app_channel.py` is unused primitives. The "intent before create" mechanism in `execution-e09.md` would need a product change plus rebuilt wheel/images and a review. The CLI builds `docker.from_env()` and creates app containers (preflight, node/edge/clock/lease) on `--network <owned bridge>` with labels `app=graph-loader`, `component`, `attempt=<run-dir name>`.
2. The fenced-broker/journal/launcher machinery (`installed_service_process.py`, `service_app_*`, `service_docker_wire.py`, `synthetic_broker.py`, `service_stack.py`) assumes a daemon this host does not have. I propose to **leave it in place, untouched and uncited**, and not build on it.
3. `images.json` input hashes list macOS AppleDouble files (`src/neo4j_graph_loader/._cli.py` …, hash `3b844ac6…`) in the build context, and `.dockerignore` does not exclude `._*`. The image provenance probe must therefore compare package bytes in each image against the wheel RECORD and will flag any extra `._*` file. A hit is a candidate-build defect, reported, not patched.
4. `lanes.service-*` in `test-inventory.json` are still "proposed" with no frozen IDs; `verify_installed.py` ends service lanes with `service controller integration is not complete`. The freezer/runner extension is part of S7.
5. D02 text alone is not decisive on modes, but `execution-e09.md` line 28 says D02 **includes planned/default bulk, streaming leases and classic**. I treat S3 as "execute", not "exclude", unless you say otherwise.

## Proposed mechanisms (decisions wanted)

| # | Mechanism | Simplification vs plan — what is no longer proven |
|---|---|---|
| A | **App-container observation without product change.** Controller subscribes to the Docker event stream (bounded, before the CLI starts), and records every container that attaches to the owned network or carries our run's `attempt` label; inspects image ID/labels/mounts on first sight; removes by recorded ID after verifying owned-network attachment. | No durable intent before the daemon create; a container created and auto-removed between events and inspect gets only event-attribute evidence; a lost create response cannot be settled. A foreign process with socket access could mimic labels (same-user socket; no fence). |
| B | **Run supervision.** One controller under `timeout --signal=KILL`, a registration file (fsync'd JSON lines: intent → actual IDs) written before/after every create, and a separate reaper that removes only recorded IDs and verifies owner label. Each client subprocess also under `timeout -s KILL`. | No pidfd/creation-identity fencing, no watchdog independent of the controller host, no durable settlement of in-flight Docker calls. |
| C | **Query log: structured non-rolling file appender** (reviewed `query_log_config.make_config`, run marker, immediate flush) injected into the fresh owned Neo4j before start, instead of the FIFO/registered-reader design. Continuity = stable inode + prefix equality + **contiguous query-ID sequence within the window** (verified empirically first; if IDs are not contiguous I stop and report). Controls: real synchronized conflicting write (`apoc.lock.nodes` + second writer) must show `waiting>0` for the identified query; uncontended parsed query shows 0; truncation, rotation, malformed, missing and unrelated-record logs fail closed. | Does not prove Neo4j emitted every event beyond ID contiguity; no source-to-sink fence. `window()` stays closed until the controls pass. |
| D | **Installed code only.** Fresh non-editable venv outside git from the frozen wheel; probes (a) in that venv and (b) inside each of the four candidate images (`docker run --entrypoint python <image-id>`; auto-removed, registered): imported module paths, wheel-RECORD byte/hash match, no `._*` extras. | — |
| E | **Reuse:** `installed_service.local_docker_host/validate_images`, `query_log_config`, `structured_query_log`, `query_log.waits` parsing, `observers.graph_observations/plan_ledger`. `committed_offsets` takes a `broker_guard` tied to the fence — I will give it a plain owned-prefix check. **New:** `tests/installed_service/{conftest,live_stack,live_cli,lockwait}.py`, `test_service_harness.py`, runner dispatch in `verify_installed.py`, additive freezer entries. | Existing 2198-line unit tests stay unchanged and must still pass. |

## S7 live scenarios (all on owned resources, token prefix `e09-<token>-`, loopback/engine-chosen ports only)

| ID | Scenario | Expected | Deadline |
|---|---|---|---|
| H1 | Stack up: owned bridge + Neo4j 5.26 Ent (APOC, structured query log) + Kafka 7.7.0; scratch DB/topics | versions, settings, APOC, host+container reachability | 120 s |
| H2 | Image identity: 4 candidate images by ID vs manifest, labels, mounts | exact match; `._*` extras flagged | 60 s |
| H3 | Installed-package provenance in venv and in each image | module paths/bytes match wheel | 90 s |
| H4 | Installed CLI against stack: `validate-schema`, then tiny `start --mode bulk` (≈3 rows) if feasible; else CLI preflight only — S1 owns planned-bulk assertions | app containers observed, IDs/images/mounts recorded, graph/offset/ledger inspectors return source-derived values | 300 s |
| H5 | Outage/termination: stop/start owned Neo4j 5 s; kill one observed app container | bounded, state read back by inspectors | 120 s |
| H6 | Lock-wait detector (C) positive, negative, and fail-closed controls (live + synthetic log files) | as in C row | 150 s |
| H7 | Cleanup + postflight: all recorded IDs gone, no process left, foreign snapshot (names, IDs, status, ports) byte-equal | equal | 60 s |

Lane hard deadline 1500 s. Timing-sensitive controls (H5, H6) repeated ≥5× at final bytes. Final run from a numbered, hashed copy of sources under `~/e09-s7-runs/run-NNN/`; red logs kept (`xb`).

## Questions for the orchestrator

1. **Acceptance criterion 4** (daemon fence, retained namespaces, dedicated daemon, durable broker) cannot be met on this host. The user directed the shared native engine, but changing the criterion is yours and theirs. I will record AC4 as "not met — replaced by user-directed shared-engine substrate; weakenings listed" unless you amend it.
2. Approve mechanism **A** (no product change) over the shipped adapter?
3. Approve mechanism **C** (non-rolling file + ID contiguity) over the FIFO design?
4. Approve H4's tiny bulk `start`, falling back to preflight-only if it turns out S1-sized?
5. Confirm S3 = execute classic (item 5).
