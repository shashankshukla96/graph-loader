# Handoff — E09 live-service proof (implementer)

You implement; the orchestrator (original session) owns scope, status changes and review. Do not commit, push,
upload, or mark stories complete yourself. Updated 2026-10-31 after the plan reset (decision D08).

## What changed in the reset

The plan now keeps only what a production-ready library needs. Removed for good: the S6 sandbox bootstrap
fixture and the shared-gate machinery (fence, broker/durable settlement, Docker wire decoder, app channel/journal,
`installed_service_process`, `service_stack`/`service_resources`, `service_kafka_probe`, synthetic broker) and their
docs/reviews/evidence. `scripts/installed_service.py` now holds only `local_docker_host` and `validate_images`.
Do not rebuild any of that. Your S7 harness and S1 work-in-progress are untouched.

## Read first

`plan/library-implementation/execution-e09.md` (rules, substrate, order), `epics/e09.md` (acceptance criteria),
`e09-s1-design-note.md` (your S1 design and the user's answers), `ordering-matrix.md`, `decisions.md` (D02, D04, D08).

## Current state

- E09.F1.S7 complete (independently accepted). E09.F1.S1 in progress: exploration runs `explore-1..4` on the host
  under `/home/shashank/e09-s1-runs/`; uncommitted edits in `scripts/freeze_installed_inventory.py`,
  `tests/installed_service/{bulk_fixture.py,test_service_bulk.py,live_cli.py,test_observer_controls.py}`.
- `test-inventory.json` was pruned of the removed tests (4,095 IDs). The only difference from a fresh collection is
  your new S1 IDs (13 observer controls, 3 bulk tests); add them with `--freeze` when S1's tests are final.
- Known defect in your harness: after a killed writer, `query_window.capture` raises "structured query event
  identity differs". Investigate before the next S1 run.

## Rules (short form; full list in execution-e09.md)

- Installed wheel and explicit candidate images only; owned, labelled resources removed by recorded ID; unchanged
  foreign-resource snapshot every run; never touch the user's stack or its ports (5432, 7474, 7687, 8080, 9092, 9101).
- Hard deadlines on every process. Real graph/offset/cleanup assertions; positive controls for any zero claim.
- Evidence per story: one passing lane run at final bytes (runner `result.json`/`command.json`), three repeats only
  for timing-sensitive cases, red runs kept, short `evidence/<story>.md` with a "Not covered" list.
- No per-story independent review rounds: self-check, then report. One independent review covers E09 before its exit
  gate. Ask the orchestrator before changing product code, acceptance criteria, system settings, or downloading
  anything beyond pip/image builds already approved.

## Never

- Put credentials anywhere but process memory (not argv, environment dumps, logs, receipts or evidence); redact errors.
- Ask for, type or store a password; use sudo (the host no longer grants it; plain `docker` works via the group).
- Publish owned container ports anywhere but `127.0.0.1`, or touch the user's containers, networks, volumes, data
  paths or ports; run `docker system/volume/network prune` or any broad filter.
- Install system packages; change firewall, sysctl, AppArmor, Docker or group settings; tag, push, upload, or run
  any GitHub action.
- Mount checkout source to rescue an import. A product-code change requires rebuilding and re-freezing the wheel and
  images (with orchestrator approval first).

## Stop, preserve state and ask when

- a foreign resource changes, a run leaves anything owned behind, or cleanup is uncertain;
- a result contradicts D02, the ordering matrix or a story's criteria, or you want to change a criterion;
- a product defect blocks a story (report it with a minimal reproduction).

## Order

S1 planned bulk → S2 streaming leases → S3 classic → S4 DLQ → S5 outages → F2.S1 ordering O1–O8.

## Report at the end of each story

Files changed; exact commands and run directory; results table (case, expected, observed); foreign-snapshot result
and cleanup receipt; red runs and defects (own vs product); not covered/weakened; what you did not run. Then continue
with the next story unless the orchestrator says otherwise.
