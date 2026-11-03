# Handoff — E13 stream soak (implementer)

You implement; the orchestrator (the session that messages you) owns scope, status, review, commits and every
question to the user. Do not commit, push, tag, upload or mark stories complete. Written 2026-11-02.

## Read first

- `epics/e13.md` — scope, workload, pass criteria and story acceptance (authoritative).
- `docs/INCREMENTAL_LOAD.md` Part B (stream fleet, DLQ, ordering B5, outages B7, supervision B8, restart B10, scaling
  B11) and "Handing over from a bulk load to a stream"; `docs/BULK_LOAD.md` sections 4 and 13.
- `execution-e09.md` and `tests/installed_service/` (`live_stack.py`, `ownership.py`, `lockwait.py`, `observers.py`,
  `stream_run.py`) — reuse them; do not fork them.
- `evidence/E11.F1.S3-manifest.json` — the frozen wheel hash and the final amd64 image IDs you must use.

## Order and stop points

1. **E13.F1.S1** — build `soak/` and `soak/tests/`; run them; report.
2. **E13.F1.S2** — calibration, bulk handover, 30-minute compressed run. **Stop and report**; wait for the orchestrator.
3. **E13.F1.S3** — only after the orchestrator says go: the 8-hour soak. Run it in the background on the host with a
   hard wall-clock limit; report when it ends (do not poll in a tight loop).

## Approved for this epic

- Owned Docker resources on the Linux host (`e13-<token>-` names, owner label, recorded-ID removal) and the data
  directory `/home/shashank/e13-soak/<token>/`.
- Pulling the exact Neo4j 5.26 Enterprise image E09/E11 used and `confluentinc/cp-kafka:7.7.0`; `docker load` of the
  E11 OCI archives if the host lacks the final image IDs (verify the ID after loading).
- A fresh venv outside Git with the frozen wheel; pip installs of the harness's own test dependencies.

## Never

- Change `src/`, `pyproject.toml`, `PACKAGE_README.md`, `MANIFEST.in`, `test-inventory.json` or any collected test ID.
  A product fix means a new candidate: stop and report.
- Touch the user's resources (list in `epics/e13.md`), the orchestrator's `imdb-c7b4c997dfd08de3` stack, or the user's
  `imdb` database on the user's Neo4j. Run the documented `kill -9` recovery with its unnarrowed
  `label=app=graph-loader` filter.
- `docker ... prune` or any broad filter; sudo; passwords; installing system packages; changing Docker, firewall,
  sysctl or group settings.
- Work around a permission or classifier denial: report it.

## Stop, preserve state and ask when

- a pass criterion fails (classify harness vs product; give a minimal reproduction for a product suspect);
- the host memory guard fires, or a foreign resource changes;
- the docs and the observed behaviour disagree in a way that changes an expected outcome;
- cleanup of an owned resource is uncertain.

## Report at the end of each story

Files changed; exact commands and run directories; results per pass criterion with evidence; faults and observed
outcomes; red runs; foreign-snapshot result and cleanup receipt; docs findings; what is not covered.
