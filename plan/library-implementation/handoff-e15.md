# Handoff — E15 run status and portable monitoring (implementer)

You implement. The orchestrator (the session that messages you) owns scope, status, review, commits and every question
to the user. Do not commit, push, tag, upload or mark stories complete. Written 2026-11-03; the user started E15 that day
("Start E15 in a new implementer Sonnet Session").

## Where things are

- Your worktree `/Users/shashank/Desktop/Code/Graph/graph-loader-e15` already exists: branch `e15-run-status`, cut from
  the committed E14 work `1211283` on `e14-process-runtime`. Do all work, edits, venvs and test runs there. Do not create
  another worktree or switch branches.
- The plan is committed on branch `codex/pypi-library-checklist` in the main checkout. Read it there by absolute path,
  read-only:
  - `/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/epics/e15.md` is the authority for
    scope, design, story acceptance and decisions. D15-1, D15-2, D15-3 and D15-5 are as recommended; D15-4 is
    `--status-store {json,sql}`, SQLAlchemy Core, PostgreSQL via psycopg 3, SQLite for single host, optional `[sql]` and
    `[postgres]` extras, no new base dependency.
  - `.../epics/e14.md` describes the runtime you build on: process and Docker runtimes, run registry, health,
    `stage=oom`, and D14-5.
  - `.../README.md` holds the plan rules: story sizing, evidence, freshness.
- E14 hand-offs that E15 must absorb, listed in your worktree's `plan/library-implementation/evidence/E14.F3.S11.md`:
  - `--metrics-addr 0.0.0.0` writes `127.0.0.1` targets into `targets.json`; S8 and S10 fix it with the aggregated
    endpoint and discovery;
  - the `stage=oom` line and `oom` registry record become a status event;
  - `/readyz` follows the source.
- Never edit the main checkout (`/Users/shashank/Desktop/Code/Graph/graph-loader`). It holds the paused E13 harness
  (`soak/`, untracked), and another session works there.
- Write evidence in your worktree at `plan/library-implementation/evidence/E15.<F>.<S>.md`.
- `pyproject.toml`, `MANIFEST.in` and `PACKAGE_README.md` may change only where an E15 story names them (S10, S11, S13).
  Adding the optional extras and package data there is in scope. Base dependencies must not grow.

## Order and stop points

1. **E15.F1.S1** (identity and status model, offline). **Stop and report** after S1. The orchestrator reviews the public
   contract (status and event JSON Schemas, identity fields, `schema_version`) before anything builds on it.
2. After the orchestrator's go, work in this order: S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13. Report after
   **each** story and continue unless a stop condition applies or the orchestrator says wait. S7 (JSON logs) and S8
   (metric identity) depend only on E14; the orchestrator may move them to a parallel lane. Do not start them early on
   your own.
3. Prefer offline proofs on the Mac. Use the Linux host only where an acceptance needs it: S4 owned PostgreSQL if not
   run locally, S5, S12, and the promtool/Grafana checks if not runnable locally. Before each live host run, confirm
   with the orchestrator that the host is free.

## Approved for this epic

- Product changes under `src/`, new tests, docs, `monitoring/`, `docker-compose.monitoring.yml`, `Makefile`, packaging
  as named above, `test-inventory.json` additions (existing collected IDs stay unchanged; additions only), in your
  worktree.
- Owned throwaway Docker resources on the Linux host `shashank@192.168.1.75` (key-based SSH only,
  `ssh -o BatchMode=yes`): `e15-<token>-` names, the owner label, a private network, removal by recorded ID, data under
  `/home/shashank/e15/<token>/`. This covers owned Neo4j 5.26 Enterprise, `confluentinc/cp-kafka:7.7.0`, an owned
  PostgreSQL, owned Prometheus and Grafana, and development role/CLI images built under `e15-` tags (never a release or
  candidate tag). Remove all of them by ID after each run.
- Owned throwaway PostgreSQL, Prometheus or Grafana containers on the Mac only if they run with Docker Desktop already
  running. Do not start Docker Desktop yourself; ask.
- Fresh venvs outside Git (for example in `/private/tmp` or your scratch directory).

## Never

- Touch the user's resources:
  - containers `postgres`, `graph-loader-neo4j`, `graph-loader-kafka`, `graph-loader-kafka-ui`;
  - networks `graph-loader-net`, `ws_default`;
  - volumes `graphloader_neo4j_data`, `graphloader_neo4j_logs`;
  - paths `/home/shashank/graph-loader-data`, `/media/shashank/Data/**`, `/volume/postgres`;
  - host ports 5432, 7474, 7687, 8080, 9092, 9101, and any port already listening before your run (check first; use
    other ports for owned PostgreSQL, Prometheus and Grafana);
  - the `imdb` database.

  The same goes for the orchestrator's `imdb-c7b4c997dfd08de3` stack, the E13 resources under `/home/shashank/e13-soak/`,
  E14's leftovers if any (report them, do not remove them), and on the Mac: `neburyx-*`, the `neo4j:2026` and
  `neo4j:latest` images, and the shared build cache.
- `docker ... prune`, broad label or name filters, sudo, passwords (the host password is never used or asked for),
  installing system packages, changing Docker, firewall, sysctl, systemd system units or group settings.
- Long soaks. The user decided against them for this phase. Use focused live cells only.
- Change base dependencies, rebuild or overwrite the frozen `0.1.0a1` artifacts or images, or edit E14's committed
  history.
- Print, log or commit credentials. Use owned throwaway credentials for owned services, keep them in 0600 files or in
  memory, and grep evidence and logs for them before reporting. No archive containing secrets goes into the Git tree:
  large or binary host artifacts go to `/Users/shashank/Desktop/Code/Graph/e15-host-artifacts/` (0700), redacted,
  referenced by path and sha256.
- Work around a permission or classifier denial: report it.

## Stop, preserve state and ask when

- an acceptance criterion fails and the cause is not a bounded harness bug (classify it; give a minimal reproduction
  for a product suspect);
- a public contract would change from what e15.md says (status or event schema, SQL schema, metric names or labels,
  CLI flags);
- E14 behaviour (process or Docker runtime, parity golden) would change;
- a story would exceed its estimate by more than half (propose a split);
- the host memory guard fires, a foreign resource changes, or owned-resource cleanup is uncertain;
- the plan and the code disagree in a way that changes a design decision.

## Report at the end of each story

- Files changed, and the exact commands and environments.
- Results per acceptance criterion, with evidence.
- Negative controls and their red results; red runs kept.
- Docker-mode parity status (`tests/test_docker_parity.py` stays green).
- For host stories: a foreign-resource snapshot before and after, and a cleanup receipt (storage and memory cleaned
  after each live run).
- Docs findings, what is not covered, and time spent versus the estimate.
