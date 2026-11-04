# Handoff — E19 Neburyx publishing and restricted-network images (implementer)

You implement. The orchestrator (the session that messages you) owns scope, status, review, commits and every question
to the user. Do not commit, push, tag, upload or mark stories complete. Written 2026-11-03. The user approved the plan
and D19-8 that day ("I am good with both"), then said "Start E19".

## Where things are

- Your worktree `/Users/shashank/Desktop/Code/Graph/graph-loader-e19` already exists: branch `e19-neburyx`, cut from the
  private `main` at `ad3c760` (E14 and E15 merged). Do all work, edits, venvs and test runs there. Do not create another
  worktree or switch branches.
- The plan is committed in that same tree. The authority for scope, design, story acceptance and decisions is
  `plan/library-implementation/epics/e19.md`. Also read `README.md` (plan rules), `epics/e14.md` and `epics/e15.md` for
  the runtimes and contracts you build on.
- Never edit the main checkout (`/Users/shashank/Desktop/Code/Graph/graph-loader`); the orchestrator works there.
- Write evidence in your worktree at `plan/library-implementation/evidence/E19.<F>.<S>.md`.
- `pyproject.toml`, `MANIFEST.in` and `PACKAGE_README.md` may change only where an E19 story names them. Base
  dependencies must not grow. The lock tool is dev-only.

## Order and stop points

1. **E19.F1.S1** (Neburyx move). **Stop and report.** The orchestrator checks the namespace diff, the a2 parity golden
   diff and the a1 normalization before anything builds on them.
2. **E19.F1.S2** (no product dependency on `plan/`, inventory migration). **Stop and report.** The orchestrator checks the
   inventory migration (ids, dispositions and order preserved; listed field changes) before you continue.
3. After the orchestrator's go: S3, S4, S5, S6, S7, S8, reporting after **each** and continuing unless a stop
   condition applies or the orchestrator says wait.
4. S9, S10 and S11 are live work on the Linux host. Before each host sitting, confirm with the orchestrator that the host
   is free.
5. Prefer offline proofs on the Mac. The Mac's Docker Desktop may be used for owned throwaway resources only if it is
   already running; do not start it yourself, ask.

## Approved for this epic

- Product changes, tests, docs, scripts, Dockerfiles and packaging as e19.md names them, in your worktree.
- `test-inventory.json` migration as specified in e19.md S2: every existing id, disposition and order preserved;
  additions only otherwise.
- The private tools under `plan/library-implementation/tools/` (guard, export, proof harness).
- **Reading** the public repo `https://github.com/Neburyx/graph-loader.git`: cloning it into a scratch quarantine
  directory outside both repositories is allowed. No push, no tag, no release, no settings change, no issue or PR, ever.
- Owned throwaway Docker resources on the Linux host `shashank@192.168.1.75` (key-based SSH only,
  `ssh -o BatchMode=yes`):
  - `e19-<token>-` names, the owner label, removal by recorded ID, data under `/home/shashank/e19/<token>/`;
  - **D19-8 as approved**: one `--internal` network, a privileged `docker:dind` (pinned) with its own volumes, an owned
    `registry:2` with htpasswd, `pypiserver` behind an owned TLS proxy with a throwaway CA, a static Debian repo, a
    runner container, Neo4j 5.26 and Kafka inside the inner daemon;
  - budget: 10 GB disk, 8 GB memory, 4 CPUs.
- Seeding pulls by digest (base images, `registry:2`, `pypiserver`, `docker:dind`, Neo4j 5.26 Enterprise,
  `confluentinc/cp-kafka:7.7.0`), each removed afterwards by owned tag. An image ID is removed only if it was newly
  created and no other tag or container uses it.
- Fresh venvs outside Git (in `/private/tmp` or your scratch directory). Dev-only lock tool (`pip-tools` or `uv`) in such
  a venv.

## Never

- Touch the user's resources:
  - containers `postgres`, `graph-loader-neo4j`, `graph-loader-kafka`, `graph-loader-kafka-ui`;
  - networks `graph-loader-net`, `ws_default`;
  - volumes `graphloader_neo4j_data`, `graphloader_neo4j_logs`;
  - paths `/home/shashank/graph-loader-data`, `/media/shashank/Data/**`, `/volume/postgres`;
  - host ports 5432, 7474, 7687, 8080, 9092, 9101, and any port already listening before your run (check first);
  - the `imdb` database;
  - the volumes of `imdb-c7b4c997dfd08de3`, `/home/shashank/e13-soak/`, and any E14 or E15 leftovers (report, do not
    remove).
- Run `docker ... prune` or force removals, use broad label or name filters, use sudo or passwords (the host password
  is never used or asked for), install system packages, or change the shared Docker daemon's configuration, the
  firewall, DNS, sysctl, systemd system units or group settings. The dind daemon's own arguments are owned and allowed.
- Push, tag or upload anywhere: the private remote, `Neburyx/graph-loader`, ghcr.io, PyPI or TestPyPI. Pushes in the
  proof go only to the owned registry inside the isolation.
- Put any AI attribution or AI tool name in a file, a commit message or a comment in product files: no
  `Co-Authored-By`, no "Claude", "Codex", "subagent" or similar. The guard (S7) enforces this for exports; follow it
  everywhere.
- Long soaks (the user decided against them). Use focused live cells only.
- Rebuild or overwrite the frozen `0.1.0a1` artifacts or images, or edit E14's or E15's committed history.
- Print, log or commit credentials. Owned throwaway credentials live in 0600 files or in memory. Grep evidence and logs
  for them before reporting. Large or binary host artifacts go to
  `/Users/shashank/Desktop/Code/Graph/e19-host-artifacts/` (0700), redacted, referenced by path and sha256.
- Work around a permission or classifier denial: report it.

## Stop, preserve state and ask when

- an acceptance criterion fails and the cause is not a bounded harness bug (classify it; give a minimal reproduction
  for a product suspect);
- a public contract would change beyond e19.md (CLI flags, environment variables, image labels or references, the
  artifact contract, the inventory rule);
- E14 or E15 behaviour would change (runtimes, parity golden beyond the namespace, status or metrics contracts);
- a story would exceed its estimate by more than half (propose a split);
- the host memory guard fires, a foreign resource changes, or owned-resource cleanup is uncertain;
- an isolation control shows public access from inside the D19-8 isolation;
- the plan and the code disagree in a way that changes a design decision.

## Report at the end of each story

- Files changed, and the exact commands and environments.
- Results per acceptance criterion, with evidence.
- Negative controls and their red results; red runs kept.
- Docker-mode parity status (`tests/test_docker_parity.py` green; a2 golden diff namespace-only).
- For host stories: a foreign-resource snapshot before and after, and a cleanup receipt (storage and memory cleaned
  after each live run).
- Docs findings, what is not covered, and time spent versus the estimate.

Run Mac suites with `--ignore=tests/installed_service --ignore=tests/integration --ignore=tests/stress -m "not
integration and not smoke and not stress"` unless a story needs more and the orchestrator agrees.
