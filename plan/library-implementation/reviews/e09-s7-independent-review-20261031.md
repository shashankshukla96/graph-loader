# E09.F1.S7 independent review (zero-history), 2026-10-31

Reviewer scope: story E09.F1.S7 "Provision the live installed-service proof harness on the isolated substrate", branch `codex/pypi-library-checklist`, nothing committed. Read-only review. The only file written is this one. No container was started, no live lane was run, ssh to 192.168.1.75 was used only for `ls`/`cat`/`docker ps`/`docker images`/`grep` on retained run directories.

## Verdict: ACCEPT WITH REQUIRED REPAIRS

- Acceptance criteria 1 and 2 are supported by the retained five-run evidence, with the gaps in findings 1 and 5.
- Acceptance criterion 3 is only partly delivered (finding 3).
- AC4 is honestly declared not met. The S7 criterion text says the production blockers must close "before any live-service claim", so S7 cannot be closed against its own wording. It needs an explicit recorded amendment by the orchestrator or user (finding 4).
- No blocker found. Four majors need repair or an explicit rewording before S7 is called complete.
- I found no evidence that any foreign (user) resource was touched. I found no secret in any retained artefact.

## Findings

### 1. [major] Completeness of application-container observation is not established; the label path is dead code
- Evidence: `tests/installed_service/test_service_harness.py:159` builds `AppObserver(docker_client, registry, stack.network.id, 'unused')`. In `live_cli.py:_handle` the `attempt` label is the only way to see a container that never attaches to the owned network. With `'unused'` that path can never fire, so only containers that connect to the owned bridge are recorded.
- The design note (mechanism A) and `E09.F1.S7.md` (weakening 2: "the Docker event stream plus inspect-on-first-sight records each container") describe observation as network attachment OR run label.
- Consequence: a CLI-created container that is on another network, uses host networking, fails before connecting, or is created and removed in between is invisible to the observer. The outer postflight only catches containers that are still present. The H4 claim "5 observed/5 inspected" and "app containers inspected, image IDs match" is true for the attached set, but nothing shows the set is complete.
- The `observed` registry rows carry `name: None` for all 5 containers in every run. They come from network `connect` events, which carry no container name.
- Repair:
  - Record every container `create`/`destroy` event during the CLI window without filtering. Assert each is either in the observed/inspected set or listed as an expected foreign. Or filter on `app=graph-loader` plus a start timestamp.
  - Fill the event name from the inspect result.
  - Otherwise state in the evidence that the label path is unused and the observed set is "network-attached only".

### 2. [major] The outer reaper and the foreign-snapshot comparison have never fired against a real daemon (zero/absent claims without a positive control)
- Evidence: all five final runs and `dry-3` report `reaped: {removed: [], refused: []}` and `foreign_unchanged: true`. In the failed `dry-1`/`dry-2` the reaper also removed nothing, because the test session cleaned up itself (host `~/e09-s7-runs/dry-*/out/result.json`, `service_postflight`).
- `scripts/service_lane.py` `reap`, `snapshot` and `difference` are exercised only by fakes (`tests/test_service_lane.py`). The claim "0 leftovers reaped" and "foreign snapshots equal" therefore cannot be told apart from "reaper/snapshot never works".
- Robustness gaps seen in the code:
  - `service_lane.read_registry` raises `ValueError` on a truncated last line. `service_postflight` catches it, sets `registry_error`, and skips the reap entirely. That is exactly the SIGKILL case where reaping matters.
  - `intent` rows with no matching `created` row are never reaped by name or label. A kill between daemon create and registry write leaves an unrecorded leftover. The run then fails as unclean, but the resource stays.
  - `reap` calls `remove_container`/`remove_volume` without try/except. A Docker API error escapes, and the failure path then tries the postflight a second time and records only `service_postflight_error`.
- Repair:
  - Add one retained live negative-control run. Kill the pytest process group mid-stack, with an owned container, an owned-network-attached container and a volume present. Show the reaper removes exactly those and the postflight reports unclean with the right `removed` list.
  - Add a sacrificial canary container, created by the control before the snapshot. Mutate it (restart or add a port) and show `difference` flags it.
  - Make `read_registry` skip an unparseable final line while still marking the run unclean. Add reaping by `OWNER_LABEL` and the `e09-<owner>-` name prefix for intents with no `created` row.

### 3. [major] AC3 "reusable bounded outage/termination controls" is only partly delivered
- Evidence:
  - Outage: only `LiveStack.stop_neo4j/start_neo4j` exist (`live_stack.py`). There is no Kafka stop/start. Kafka data is on `tmpfs` (`live_stack.py` Kafka `tmpfs={'/var/lib/kafka/data': ...}`), so a Kafka outage would lose topics and offsets, and no control or note covers that.
  - Termination: H5 kills only a throwaway `python:3.12-slim sleep` container, with the kill/wait/inspect code inline in the test body (`test_service_harness.py:261-277`). No helper terminates an observed or CLI-launched container.
  - Lock-wait: `lock_conflict` lives in the test module `test_service_harness.py:281`, not in an importable helper (`query_window.waits` is importable).
- Weakening 6 admits H5 does not prove application failure semantics. It does not say Kafka outage and app-container kill controls do not exist as reusable tools. The evidence table reads as if they do.
- Repair: move these into `live_stack.py`/`live_cli.py` as helpers (`kill_container(id, expect_exit)`, `stop_kafka/start_kafka` with registry `control` rows and deadlines, and `lock_conflict`) and use them from the tests. Or re-word the AC3 status to "partial: Neo4j outage and sleeper kill only".

### 4. [major] AC4 is declared not met, but the closing wording and the list of weakenings are not complete
- The AC4 declaration itself is honest. It names the four blockers listed in the plan: syscall/descriptor fence, retained namespaces, dedicated daemon with witnessed continuity, and durable broker/settlement. S7 criterion 4 says these must close "before any live-service claim", while the evidence makes live-service claims.
- Required: an explicit recorded orchestrator/user amendment of criterion 4 (the design-note question 1 answer is "Approve as proposed" only). S7 must not be marked complete or cited as satisfying the gate until the criterion is amended.
- Weakenings that are missing from the `E09.F1.S7.md` list or are understated:
  - (a) The installed CLI under test runs with `DOCKER_HOST` set to the shared engine socket, so the product code has engine-wide authority. The only guard against it touching the user's containers is the before/after snapshot. Weakening 1 mentions "same-user processes" but not the code under test.
  - (b) The lock-wait detector is proven only on a harness-injected non-rolling structured JSON appender (`query_log_config.make_config`). It is not proven on the shipped `config/neo4j/server-logs.xml` text/rolling log. That file is only a hashed template input. The plan says positive contention "uses the same ... configuration", and the handoff asks for rotation/truncation coverage. Rotation is only rejected, never covered. The live "rotation" is a `mv` that makes the next `stat` fail (missing file), not an inode swap at the same path.
  - (c) Candidate images' Python dependencies float (build logs show a PyPI download during `docker build`) and are never compared with the venv's. The image rebuild itself fetched from PyPI, which the "Not run: registry pull" paragraph does not mention.
  - (d) `NEO4J_IMAGE = 'neo4j:5-enterprise'` is a floating tag. The image ID is recorded but not pinned. Kafka is fixed to 7.7.0.
  - (e) The detector is capped at 8 MiB per capture (`MAX_LOG`) and re-reads the whole log each time. It fails closed, but only for tiny loads such as the 5-node smoke.
- Repair: add these items to the weakenings list and record the AC4 amendment.

### 5. [minor] Numeric and wording errors in the evidence file
- "20 synthetic controls" is actually 22: `pytest.json` lists 22 `test_query_window_controls.py` IDs, and 7 + 22 + 1 = 30. The table's rows sum to 28.
- Pytest wall time is stated as 206.8–207.7 s. The five logs say 207.09, 206.87, 206.17, 206.86 and 206.74 s.
- H1 says "loopback-only engine-chosen ports not in the foreign set". Only the Neo4j port is engine-chosen. The Kafka host port comes from `live_stack.free_port()` (bind-probe then release, a time-of-check/time-of-use race). H1 checks the static `FOREIGN_PORTS` set, not the dynamic foreign set from the snapshot.
- "foreign containers byte-identical to run 1's pre-snapshot" compares five fields (name, image, state, StartedAt, RestartCount), as `postflight.py` shows.
- The Kafka version is established from the image label `release` (`live_stack.facts`), not from the running broker.
- "images' package bytes were correct" (old AppleDouble images) has no retained probe. H3 was never run against the discarded `…s6-f0104a478ab5` images.
- `E09.F1.S7.md` does not mention the passing `dry-3` run, the `exp1-*` runs or the Mac `service-helper-unit` regression run (`regress-service-helper-unit-1.out`). All are retained, and the manifest is complete over `var/.../s7`.

### 6. [minor] Weak, vacuous or timing-based assertions
- `test_service_harness.py:228` `max(r['waiting_ms'] for r in window) >= 0` is always true. The meaningful check is the next line (`WORKS_AT` appears in a completed query).
- `time.sleep(2)` after the CLI run (`:167`) stands in for event-stream drain. Wait for the observer to see `destroy`/`die` for every observed ID, or for an idle period with a bound. The loop in `AppObserver._loop` also ends silently if the stream closes without an exception. The five-component equality is the only backstop.
- `test_live_truncation_and_rotation_fail_closed` passes through `events()` "empty or incomplete" (capture of an emptied file) and a failing `stat`. It does not exercise `window()`'s truncation/prefix branch live. The `len(after.content) < len(before.content)` branch is unreachable in practice. The synthetic tests cover the branch.
- H5: `pytest.raises(Exception)` for the "connect refused" check accepts any failure. `assert snapshot['errors'] == [] and leftovers == {...}` is a bare compound assert.
- H7 is a point-in-time child count and can race with `psutil.NoSuchProcess` on `p.status()`.
- H5 hard-depends on H4's five nodes (`assert baseline == 5`). The destructive live-continuity test must be last. This holds only by file-name order (`test_query_window_controls`, `test_service_harness`, `test_service_log_continuity_live`); no marker or ordering plugin enforces it.

### 7. [minor] Cleanup-path ownership: sound, with small gaps
- `LiveStack.close` removes containers and the network by recorded object ID and verifies the owner label for volumes. `AppObserver.reap` and `service_lane.reap` verify owner label or attachment to the recorded owned network; `reap` verifies the owner label for volumes and networks.
- `service_lane.reap` `owned_container` accepts either the owner label or attachment to any recorded pending network, so a container with another run's owner label that is attached to our network would be removed. This only matters for IDs already in the registry.
- `AppObserver.reap` and `reap` act on `observed` IDs, which come from events alone. `run_cli` does `os.killpg(process.pid)` after the process was waited on (negligible PID-reuse window).
- `run_cli` starts the CLI in its own session (`start_new_session=True`). The runner's `run()` timeout kills only the pytest process group, so on a runner timeout the CLI survives until `cleanup_owned` kills it via guard provenance (Python processes only). Weakening 3 ("SIGKILL of the process group") reads as if the CLI is covered by the group kill.
- Repair: document this; optionally `setsid`-record the CLI pid in the registry so the postflight can kill it.

### 8. [note] Query-log detector soundness (`query_window.py`, `structured_query_log.py`)
- Reviewed and judged sound for the stated threat model.
- Epochs: a new epoch starts at each `Query started: id:1`. Earlier epochs are checked for contiguity without quiescence. The last epoch must be contiguous from 1 and quiescent (every started ID has a completion). A restart between marks changes `epochs` and `window()` rejects.
- `window()` requires: same inode; same epoch count; `after.content.startswith(before.content)`; record prefix equal; delta ID set exactly equal to `before.max_id+1..after.max_id`; and exactly two counted records per ID. `read_log` brackets the read with the inode and size and requires a matching trailer. `capture` needs two identical quiescent reads 0.5 s apart.
- A gap, truncation, same-size rewrite, rotated inode, restart, foreign marker, failed query (non-INFO or unsupported format), malformed or partial line is rejected. Each has a synthetic test (`test_query_window_controls.py`), and I re-ran them (pass).
- The schema-DDL exemption is narrow: completion only, identical tuple (transaction, database, user, query), `runtime=schema`, text starts with `CREATE|DROP CONSTRAINT|INDEX`, at most once per query ID. The duplicate is flagged and excluded from counts, and a triple completion still fails (tested).
  - Limits: the duplicate's `waiting` value is ignored. The regex is case-sensitive; a lower-case DDL would be rejected (fail closed).
- The old rejection stays covered: `tests/test_installed_service_structured_log.py:51` (`runtime=pipelined` duplicate raises "duplicated") passes locally (215 tests pass).
- Residual: ID contiguity is empirical for Neo4j 5.26.32 on small logs. It cannot prove nothing was dropped after the last logged ID, or that unlogged queries consume no ID. This is disclosed in weakening 4.

### 9. [note] Runner changes
- `.pth` guard loader (`scripts/installed_guard_load.py`, `e07_guard.pth`, probe in `verify_installed.py`):
  - On a non-Debian Python the behaviour is unchanged and single-load. I confirmed this in a throw-away macOS Python 3.9 venv with a counting stub `sitecustomize`: `-I -c` loaded once, plain `-c` loaded once, `-S` loaded zero times (as before).
  - On Debian/Ubuntu the retained `provenance.jsonl` has exactly one `process` record per PID (6 PIDs, 6 rows), so there is no double load.
  - Failures in a `.pth` import line are swallowed by `site`. Only the one-shot probe catches a non-loading guard, and it runs only in the runner's `python -I`.
  - The probe now runs for every lane. The guard loads earlier than before (during `.pth` processing, before later `.pth` files). It would stay out of the way if some other `sitecustomize` was already imported, and the probe would then fail the run.
  - Only `service-helper-unit` was re-run through the changed runner (on the Mac, once, `regress-service-helper-unit-1.out`, exit 0, after the final `verify_installed.py` bytes). The other unit lanes and the E08 `--process-envelope` path were not re-run on Linux with the new loader. The checkout-level `tests/test_installed_runner.py` passes locally.
- `.xml` exception: `safe_relative` allows `.xml` only when `value == 'config/neo4j/server-logs.xml'` (exact string, after the unsafe-part, absolute and normalised-path checks). It is narrow. It applies to every lane's validation, not only the service lane, but still only to that one path.
- Service branch keeps prior validation:
  - `validate_service_inventory` reuses `validate_inventory` (via a unit view), the frozen-ID/disposition/collection-hash checks, `validate_wheel` and the payload checks.
  - `preflight_service_images` compares the source build closure with the wheel payload and the image manifest, and checks the daemon ID before and after.
  - `validate_provenance` runs after pytest. The pytest receipt must equal the frozen ID list.
  - A non-service lane given `--images` or `--docker-host` is rejected.
- Failure paths: pytest non-zero, timeout, an unclean postflight or any exception returns exit 1 with `result.json` `status: failed`, and the postflight runs again in the except path. A success report requires `clean: true`, which needs no registry error, an unchanged foreign snapshot, and nothing reaped or refused. The residual gap is the SIGKILL case in finding 2.
- `docker_client.close()` is not called on the success path (process exit), and the daemon ID is not re-checked inside the pytest process. Both are minor.

### 10. [note] Inventory
- The comparison copy named in the review brief (`/private/tmp/claude-501/-Users-shashank--claude-spares-spare-pymd4n/.../test-inventory.before-s7.json`) does not exist on this machine. I could not diff against the pre-S7 file.
- What I verified instead:
  - Against `git HEAD` (3,873 IDs / 125 files): all HEAD IDs are present, in the same order, with unchanged `test_dispositions`. `file_dispositions` are unchanged for all HEAD files.
  - Beyond HEAD: 429 added IDs. 391 are the S6-era test files (`test_installed_service.py` 125, `_log` 35, `_log_config` 10, `_structured_log` 23, `test_service_app_channel` 20, `_app_journal` 34, `_docker_wire` 105, `test_service_observers` 14, `test_synthetic_broker` 25), which with HEAD gives 3,873 + 391 = 4,264 across 134 files. 38 are S7: 22 + 7 + 1 in `tests/installed_service/` and 8 in `tests/test_service_lane.py`. Total 4,302 across 138 files; the 4 new files are the S7 additions.
  - The 30 service-harness IDs carry `kind: service` dispositions. The 8 new `test_service_lane` IDs carry `checkout-development` dispositions.
  - Lanes versus HEAD: only `service-harness` has IDs (30). The seven other service lanes differ only in an S6-to-S7 owner-text `note`, and have no IDs. No unit lane differs from HEAD.
  - I ran `scripts/freeze_installed_inventory.py --collection-output <new scratch path>` myself with the E08 dev Python: `{"test_ids": 4302, "files": 138, "status": "matched"}`.
- Not independently attested: that none of the 391 S6-era dispositions or hashes were edited during S7 (no baseline). The evidence claim "4,264/134 preserved + additions" is consistent with everything I could check.
- `tests/test_installed_service.py::test_service_cli_remains_closed_until_controller_and_image_preflight_exist` keeps its frozen ID, but the body now asserts something different (failed image preflight stops the lane). The name is now misleading; the docstring discloses it.

### 11. [note] Image and candidate handling
- The rebuild is justified. The old `images.json` was unusable for preflight because the host build context was contaminated by AppleDouble `._*` files from a tar-with-xattrs transfer. The Mac working tree has none (`find . -name '._*'` returns nothing). `.dockerignore` does not exclude `._*`.
- The new manifest on the host (`/home/shashank/graph-loader-e09-s7-clean-1793416361-evidence/images.json`) is consistent with the runs:
  - It has 78 inputs, no `._*` key, version `0.1.0a1.dev0`, `published: false`, and four distinct image IDs, tags and `linux/amd64`, identical to `final-*/out/result.json` `service.images`.
  - `docker images` on the host shows the same IDs.
  - Its `input_sha256` map equals `build_inputs()` computed on the Mac working tree (78 = 78, no differing key). `src/` is unmodified in `git status`.
- The old `…s6-f0104a478ab5` images were left in place.
- Recommendation: add `._*` to `.dockerignore` (a product-adjacent file change that alters build inputs, so it needs its own decision) or transfer without xattrs, as `assemble_run.py` now does with `tar --no-xattrs`.
- The source snapshot is an uncommitted working tree. Reproducibility rests on the recorded hashes only.

### 12. [note] Secrets and consent
- No scratch password appears in `pytest.log`, `service.json`, `registry.jsonl`, `result.json`, CLI output, or `provenance.jsonl` argv. The CLI is given credentials by environment, not argv, and its output is redacted with a count asserted 0. `live_cli.project` excludes env and command.
- The password is visible to anyone with socket or same-user `/proc` access (container env and CLI environ). It is scratch-only.
- The harness sets `NEO4J_ACCEPT_LICENSE_AGREEMENT=yes`. `execution-e09.md` records the user's approval for throwaway containers.
- `psutil` was installed into the controller venv, which the evidence says was approved scope.

## Claims verified independently
- Runs: five final runs exit 0, `pytest.json` 30 collected / 30 passed / exit 0 / no errors, the exact 30 IDs and order equal the frozen lane.
- Frozen inputs: `frozen-inputs.sha256` is byte-identical across the five runs (sha256 `f9836d38579a…` matches the evidence). The 36-file overlay hashes in `final-*-overlay.json` equal the current Mac working-tree bytes and the host `frozen-inputs` lines.
- Runner and wheel identity: `result.json` `runner_tools` hashes for `verify_installed.py`, `installed_guard.py`, `installed_guard_load.py`, `installed_pytest.py`, `installed_dependencies.py` and `service_lane.py` equal the current Mac files. Wheel sha `09b8b7fc836e…` and inventory sha `81b6d631fd0e…` are the same in all runs and equal the current inventory.
- Postflight: `foreign-before.json` equals `foreign-after.json` in all five runs. The registry shows 16 containers (11 created + 5 observed), 1 network and 2 volumes, with `close.failed == []`.
- Independent host check: read-only `docker ps -a`, `docker volume ls` and `docker network ls` now show only the user's four containers, `graphloader_neo4j_data`, `graphloader_neo4j_logs`, one anonymous volume, and networks `bridge`, `graph-loader-net`, `host`, `none`, `ws_default`. No `e09-*` object remains, and no runner process is alive. `postflight-final.json` reports ok.
- Provenance: installed CLI process `…/environment/bin/graph-loader start …`, 41 module records in the CLI process and 41 in its spawn child, and module origins within the target site. One `process` row per PID.
- H4 artefacts (host `final-1/out/service-run/h4`): `edge-plan.json.ledger.json` has `state: complete, chunks 1, rows 3`; CLI log has `planned-edges-complete … chunks=1 rows=3`. The `wait_timeouts=9` dispatcher counter is not used as lock evidence.
- Evidence manifest: all 126 retained files match their sha256 and byte counts, and no file under `var/e09-validation-20261028/s7` is unlisted.
- Local checks: ruff clean on the touched files. `audited_pytest.py` on `test_service_lane`, `test_installed_service_structured_log`, `test_query_window_controls`, `test_installed_runner`, `test_installed_inventory` and `test_installed_service` gives 215 passed. The retained `local-unit-final.log` shows 260 passed.
- Inventory: freezer `--collection-output` reproduces 4,302 / 138 matched.
- Candidate manifest: equality with the Mac working-tree build closure (finding 11).
- Guard single-load on non-Debian Python (finding 9).

## Claims taken on trust (not independently reproducible here)
- That the live lane behaves the same on repeat beyond the five retained logs and registries. I read them but did not re-execute.
- That Neo4j 5.26.32 consumes no unlogged query IDs. This is the stated empirical assumption.
- The contents of the old (AppleDouble) images, the Docker 29.9.0 version string and the host Ubuntu Python layout. I saw the daemon ID in `result.json`, not the daemon itself.
- The E08/E07 history behind the runner baseline.
- That the 391 S6-era inventory entries were not altered during S7 (no pre-S7 copy).

## Not checked
- I did not run the live lane, start any container, or inspect inside any image. I did not run the E08 `--process-envelope` path or the other unit lanes through the changed runner.
- I did not verify the dependency pins of the candidate images beyond the build log excerpts, or any claim about arm64, concurrent runs or Docker Desktop.
- I did not review the unused fenced-broker modules (`installed_service_process.py`, `service_app_*`, `service_docker_wire.py`, `synthetic_broker.py`, `service_stack.py`, `service_resources.py`, `service_kafka_probe.py`) beyond confirming the S7 path does not import them and the freezer lists them as checkout-only.
- I did not review the plan-document edits (`backlog.json`, `epics/e09.md`, `CHANGE_TRACKER.md`) beyond confirming `E09.F1.S7.md` says its proposed backlog text is "not applied".
