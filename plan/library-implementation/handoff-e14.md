# Handoff — E14 process runtime (implementer)

You implement; the orchestrator (the session that messages you) owns scope, status, review, commits and every question
to the user. Do not commit, push, tag, upload or mark stories complete. Written 2026-11-02.

## Where things are

- You work in your **own git worktree**. Your session opened in the main checkout, so create it first:
  `git -C /Users/shashank/Desktop/Code/Graph/graph-loader worktree add -b e14-process-runtime
  /Users/shashank/Desktop/Code/Graph/graph-loader-e14 52d9bd9` (branch cut from `codex/pypi-library-checklist` at
  `52d9bd9`), then do all work, edits, venvs and test runs inside `/Users/shashank/Desktop/Code/Graph/graph-loader-e14`.
  If the path or branch already exists, stop and report. The plan files are **not committed** yet, so read them from the
  main checkout by absolute path (read-only):
  - `/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/epics/e14.md` — authoritative scope,
    design, story acceptance and decisions (D14-1 = b: release after E14 and E15; D14-2..D14-4 as recommended).
  - `/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/epics/e15.md` — the next epic; do not
    implement it, but do not make choices that contradict it (identity labels, status store, JSON logs).
  - `/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/README.md` — plan rules (story sizing,
    evidence, freshness).
- Never edit anything in the main checkout (`/Users/shashank/Desktop/Code/Graph/graph-loader`) apart from the single
  `git worktree add` above: it holds the paused E13 harness (`soak/`, untracked), other uncommitted plan files, and
  another session works there.
- Write evidence in your worktree at `plan/library-implementation/evidence/E14.<F>.<S>.md`.
- Parity baseline: the frozen candidate source is commit `60ea29f` ("Freeze the 0.1.0a1 release candidate snapshot");
  its artifacts and images (`evidence/E11.F1.S3-manifest.json`) are preserved and must not be rebuilt or overwritten.

## Order and stop points

1. **E14.F1.S1** (offline, on the Mac) — runtime/handle protocols and the behavioural parity trace against `60ea29f`.
   **Stop and report** after S1; the orchestrator reviews it before anything else starts.
2. After the orchestrator's go: S2, S3, S4, S5, S6, S7, then S8, S9, S10, S11, reporting after **each** story and
   continuing unless a stop condition below applies or the orchestrator says wait.
3. Linux host stories (S2 acceptance onward) need the orchestrator's confirmation that the host is free: the E13 stream
   soak is paused, and no two owned stacks may run on the host at once.

## Approved for this epic

- Product changes under `src/`, new tests, docs, `Dockerfile` (adding `tini`), `test-inventory.json` additions (existing
  collected IDs must stay unchanged), in your worktree.
- Owned throwaway Docker resources on the Linux host `shashank@192.168.1.75` (key-based SSH, `ssh -o BatchMode=yes`):
  `e14-<token>-` names, owner label, private network, recorded-ID removal, data under `/home/shashank/e14/<token>/`.
- A systemd **user** manager on the host (`systemctl --user`, `systemd-run --user`) for S9 only; if it is not reachable
  over SSH, stop and report (do not enable lingering or change system settings yourself).
- Fresh venvs outside Git; pulling the exact Neo4j 5.26 Enterprise image used in E09/E11 and `confluentinc/cp-kafka:7.7.0`.

## Never

- Touch the user's resources: containers `postgres`, `graph-loader-neo4j`, `graph-loader-kafka`,
  `graph-loader-kafka-ui`; networks `graph-loader-net`, `ws_default`; volumes `graphloader_neo4j_data`,
  `graphloader_neo4j_logs`; paths `/home/shashank/graph-loader-data`, `/media/shashank/Data/**`, `/volume/postgres`;
  host ports 5432, 7474, 7687, 8080, 9092, 9101; the `imdb` database. Nor the orchestrator's `imdb-c7b4c997dfd08de3`
  stack, the E13 resources under `/home/shashank/e13-soak/`, or, on the Mac, `neburyx-*`, `neo4j:2026`/`neo4j:latest`
  images and the shared build cache.
- `docker ... prune`, broad label/name filters, sudo, passwords (the host password is never used or asked for), installing
  system packages, changing Docker, firewall, sysctl, systemd system units or group settings.
- Change `pyproject.toml` dependencies or `PACKAGE_README.md` beyond what an E14 story names; rebuild or overwrite the
  frozen `0.1.0a1` artifacts.
- Print or commit credentials (Neo4j credentials live in `~/imdb-load/neo4j.env` on the host; use owned throwaway
  credentials for owned stacks).
- Work around a permission or classifier denial: report it.

## Stop, preserve state and ask when

- an acceptance criterion fails and the cause is not a bounded harness bug (classify; minimal reproduction for a product
  suspect);
- Docker-mode behaviour would change (the S1 trace is the contract);
- a story would exceed its estimate by more than half (propose a split);
- the host memory guard fires, a foreign resource changes, or owned-resource cleanup is uncertain;
- the plan and the code disagree in a way that changes a design decision.

## Report at the end of each story

Files changed; exact commands and environments; results per acceptance criterion with evidence; negative controls and
their red results; red runs kept; Docker-mode parity status; foreign-resource snapshot and cleanup receipt (host
stories); docs findings; what is not covered; time spent versus estimate.
