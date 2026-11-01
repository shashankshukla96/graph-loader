# Handoff — E10 installed-user documentation (implementer)

You implement; the orchestrator (the session that started you) owns scope, status changes, review and commits.
Do not commit, push, tag, upload or mark stories complete yourself. Written 2026-11-01.

## Read first

- `plan/library-implementation/epics/e10.md` — acceptance criteria and validation (authoritative).
- `plan/library-implementation/decisions.md` — D01–D08, especially D02 (supported scope), D03 (version
  `0.1.0a1`, alpha policy), D04 (CPython 3.12, Linux amd64/arm64, Neo4j 5.26 / Kafka 7.7.0), D05, D06 (images in
  `ghcr.io/shashankshukla96`), D08 (plan reset: keep only what a production-ready library needs).
- `plan/library-implementation/ordering-matrix.md` and `evidence/E09.*.md` — what was actually proved. Public
  guarantees must not be stronger than this evidence.
- `docs/PYPI_LIBRARY_CHECKLIST.md` items A6, U1–U5, D6, S3, R5.

## Order

E10.F1.S1 quick starts → E10.F1.S2 README/support/limits (includes the native_disjoint docs fix) → E10.F2.S1
release notes/migration → E10.F2.S2 recovery and release runbook. Report to the orchestrator after each story.

## Known facts to carry into the docs

- `execution.mode: native_disjoint` needs the Cypher 25 dialect; on Neo4j 5.26 it fails closed at the edge loader
  (E09.F1.S3). It is not supported on the D04 target. Fix `docs/BULK_LOAD.md`, `docs/USAGE.md` and the CLI help that
  present `--edge-classic` as the way to run it on 5.23+. A CLI help text change is product code: ask first.
- Ordering: only the D02 restricted contract (O1–O8 in `evidence/E09.F2.S1.md`) is guaranteed; timestamp ordering
  was not executed and must not be claimed.
- IMDb runs (`examples/imdb/reports/imdb-load-20261101T140252Z.md`): ~14.5M nodes / ~70M relationships in about
  16.5 minutes on one host, no database lock waits. Quote it only as one measured environment, never as a general
  performance promise.

## Validation substrate

- Quick starts must run from a fresh non-editable wheel environment outside the checkout (build the wheel from the
  current tree; no `sys.path`, `PROJECT_ROOT` or checkout source on any import path).
- Services: an owned throwaway Neo4j 5.26 Enterprise + Kafka 7.7.0 stack on `shashank@192.168.1.75` (key-based SSH
  only: `ssh -o BatchMode=yes`). Reuse the owned-stack pattern from `examples/imdb` / E09 (owner label, recorded IDs,
  ports published on 127.0.0.1 only, removal by recorded ID). An IMDb stack `imdb-c7b4c997dfd08de3` is already
  running there; do not touch it, it belongs to the orchestrator.

## Never

- Touch the user's stack on that host: containers `postgres`, `graph-loader-neo4j`, `graph-loader-kafka`,
  `graph-loader-kafka-ui`; networks `graph-loader-net`, `ws_default`; volumes `graphloader_neo4j_data`,
  `graphloader_neo4j_logs`; `/media/shashank/Data/**`, `/volume/postgres`; host ports 5432, 7474, 7687, 8080, 9092,
  9101. Never run `docker system/volume/network/image prune` or broad filters.
- On the Mac, touch the `neburyx-*` containers/images or `neo4j:2026`/`neo4j:latest` (another project uses them).
- Ask for, type or store a password; use sudo. Credentials live only in process memory or 0600 files, never in
  docs, evidence, logs or argv.
- Invent public contact details, a release date or a version: those need the user's confirmation through the
  orchestrator. Use clearly marked placeholders until confirmed.
- Edit `examples/imdb/build_subset.py` or its test (an uncommitted fix the orchestrator will commit separately).
- Work around a permission or classifier denial; report it instead.
- Change product code, acceptance criteria or `backlog.json` status without the orchestrator's approval.

## Stop, preserve state and ask when

- a doc claim would need behavior E09 did not prove, or proved differently;
- a quick start fails because of a product defect (send a minimal reproduction);
- a foreign resource changes, or cleanup of an owned resource is uncertain.

## Report at the end of each story

Files changed; exact commands and environments used; results (criterion → evidence); cleanup receipt for any owned
resources; what is not covered or still needs user confirmation. Write `evidence/<story>.md`. Then continue with the
next story unless the orchestrator says otherwise.
