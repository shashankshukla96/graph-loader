# Independent review — plan reset (D08), 2026-10-31

Reviewer: independent Claude agent; read-only apart from this file and scratch files outside the repo.
Branch `codex/pypi-library-checklist`, nothing committed. Baseline for "before the reset":
`var/plan-reset-backup-20261031/pre-reset-working-tree.tar.gz`, extracted outside the repo.

## Verdict: ACCEPT WITH CHANGES

The cut is right. Nothing removed is needed by a checklist item. V6 (bulk, streaming, classic, DLQ,
failure/restart, lock-wait), V7/U3 (O1–O8) and the final-candidate gates with a single manifest are
all still owned by a story. The mechanical work checks out: the backup is complete, the tests pass,
ruff is clean, the plan renders idempotently and the inventory claim is exact. The remote host is
as claimed. Two major problems need fixing before E09 continues:

- The dry-run cut left E12 depending on dry-run evidence that no story produces any more.
- The rewritten implementer handoff dropped several guardrails at the same time as it switched to
  continuing automatically from one story to the next.

## Evidence (commands run)

| Check | Result |
| --- | --- |
| `removed-files.txt` vs `removed-files.tar.gz` contents | identical 45 paths; each byte-equal to the pre-reset tarball; none present in the repo |
| pre-reset tarball | contains old `execution-e09.md` (53,870 B) and `backlog.json` (112,269 B); every file in it is either still present or listed as removed |
| files changed by the reset (pre vs now) | only CHANGE_TRACKER, README, backlog, decisions, epics e09/e10/e11, execution-e09, both E09 handoffs, ordering-matrix, test-inventory, capture_baseline, traceability, freeze_installed_inventory (4 `CHECKOUT` lines only), installed_service.py, verify_installed.py (1 line), test_installed_service.py. The implementer's S1 files are byte-equal to the pre-reset snapshot |
| `python3 plan/library-implementation/tools/render_plan.py` | "13 epics, 26 features, 58 stories; 48 checklist items covered; acyclic". `git diff HEAD` and generated-file hashes are byte-identical before and after (idempotent) |
| grep for removed modules or story IDs in scripts/, tests/, src/, tools/, Makefile, pyproject.toml, docs/ | none |
| audited pytest (the 11 files listed in the brief) | **271 passed** in 14.3 s |
| `ruff check scripts tests/installed_service tests/test_installed_service.py plan/library-implementation/tools` | All checks passed |
| `freeze_installed_inventory.py --collection-output <scratch>` | fails by design. Collected 4,111 vs inventory 4,095: **0 missing, 16 new** (13 `test_observer_controls.py`, 3 `test_service_bulk.py`), order preserved. `sha256` of IDs matches; `test_dispositions` keys equal the IDs in order; `file_dispositions` ids, order and categories are consistent; counts `{service:113, installed:3673, checkout-development:309}` match; files 135; the only file-set difference is the new `test_service_bulk.py`; no lane references a removed ID |
| inventory structured diff (pre vs now) | only the `ordering-boundaries` owner moved to E09.F2.S1; test_installed_service 125→26 IDs; counts, files and sha changed accordingly; nothing else |
| kept `installed_service.py` functions | `local_docker_host`, `_read_regular` and `validate_images` are AST-identical to before; the 26 kept tests are AST-identical |
| remote (read-only SSH) | no `/var/tmp/e09-bootstrap-*`; `/etc/sudoers.d/e09-claude` absent (dir holds only README, mtime Oct 31 16:40); `postgres`, `graph-loader-neo4j` (healthy), `graph-loader-kafka` (healthy) and `graph-loader-kafka-ui` are Up 19 h; networks `graph-loader-net`, `ws_default`; volumes `graphloader_neo4j_*` plus one anonymous volume used by `postgres`; no `*.sh` in home |

## Findings

### 1. [major] E12 still requires the recovery dry run that the reset removed
- `backlog.json:1783` (E12.F1.S1, rendered at `epics/e12.md:30`) says: "Link the completed E10.F2.S2 runbook and **dry-run evidence**".
- `backlog.json:1858,1862,1866` (E12.F2.S2) refer to a "pre-exercised recovery runbook", an "already exercised" procedure and "Cross-check the pre-production dry run".
- `README.md:200-202` says "E10.F2.S2 prepares and dry-runs the recovery runbook".
- Consequence: the production-authorization gate asks for evidence that no story produces. It can only pass by being waived silently, which the E12 exit gate forbids.
- Fix: reword these places to "completed E10.F2.S2 runbook" and "documented procedure". R5 only asks for the procedure to be documented, so dropping the dry run itself is correct.

### 2. [major] The rewritten implementer handoff dropped guardrails and now continues automatically
Compared with the pre-reset handoff, `handoff-e09-live-implementation.md` and `execution-e09.md` together no longer state:
- Secrets: credentials stay in memory only and never appear in argv, logs, receipts or evidence.
- Never type, store or request a password, and no `sudo`. The sudoers drop-in is now gone, so any sudo attempt would ask for a password.
- Owned containers publish only to `127.0.0.1` with engine-chosen ports.
- Stop and preserve state if a foreign resource changes, a run leaves anything behind, or cleanup is uncertain.
- After any approved product change, rebuild and re-freeze the wheel and images.
- Neo4j Enterprise licence acceptance applies to throwaway containers only.
- The "not approved" list: system packages, firewall/AppArmor/sysctl, the docker group, tags and GitHub actions.

At the same time, `handoff-e09-live-implementation.md:46-48` now says "continue with the next story unless the orchestrator says otherwise".

Nothing unsafe is added, but per-story stops are gone, so these omissions matter more. Fix: add a five-line "never / stop and ask" block to the handoff (or to execution-e09 rules).

### 3. [minor] Leftover E11.F1.S4 references
- `backlog.json:1640,1644` (E11.F1.S2) still say "S3/S4".
- `backlog.json:1736` (E11.F2.S2, `epics/e11.md:109`) says "consume the **frozen S4 manifest**".
- Consequence: they point at a story that no longer exists. They should say S3.

### 4. [minor] `handoff-e09.md:122` still prescribes per-story review
It still says "zero-history internal review after each ordered story … fresh final adversarial review". This contradicts D08 and execution-e09 rule 7. An agent that starts from `handoff-e09.md` would bring back the per-story review overhead.

### 5. [minor] A test for kept code was removed
`test_fifo_evidence_is_rejected_without_waiting_for_a_writer` (pre-reset `tests/test_installed_service.py`) tested `_read_regular`. That function was kept and is the manifest reader behind `validate_images`, the service-lane image preflight. Its frozen ID is gone and the FIFO / non-blocking case is now untested. Re-add it with `--freeze` (it gets appended as a new ID).

### 6. [minor] E09.F1.S7 is "complete", but its frozen bytes have moved
The accepted `final4` overlay hashed `scripts/installed_service.py`, `scripts/verify_installed.py` and `test-inventory.json`. The reset changed all three. Behaviour is unchanged: the kept functions are AST-identical and verify_installed only changed one owner string. The S1 work also changed `live_cli.py` and `test_observer_controls.py`.

Consequence: the S7 evidence no longer describes the current bytes. Execution-e09 should say explicitly that the `service-harness` lane is rerun at E09's final bytes before the exit gate.

### 7. [minor] Stale statements in live evidence
- `evidence/E09.F1.S7.md:104` (weakening 8) says "Pre-existing fenced-broker modules remain". They have been deleted.
- `evidence/e09-docker-migration-20261030.md:48-54` (linked from execution-e09 as the substrate record) says the production blockers "remain open" and that "/etc/sudoers.d/e09-claude … remain[s]". The first is now out of scope under D08 and the second is removed.
- The host cleanup (bootstrap dirs, migration scripts, sudoers removal) is recorded in no repo document.
- **The same line records an "exposed host password". Nothing in the reset addresses it, and the user should rotate it.**

### 8. [minor] Status and wording mismatches
- `execution-e09.md` table and the E09 `implementation_status` say S1 is "in progress", but story E09.F1.S1 `status` is still "planned".
- `render_plan.py:78` keeps a fallback, "none (fixture only; V6 and D5 are carried by E09.F1.S7)", that only existed for S6. No story uses it now, and the text is stale.
- E11.F1.S3 now carries image builds plus reruns of all E07, E08 and E09 lanes, but is still estimated at 1 day (it was 1 + 1 before the merge).
- The merge dropped S4's "preserve all pass/skip/exclusion reasons". R1 needs exclusions stated explicitly, so add the clause back to E11.F1.S3.

### 9. [note] D08 says "Approved by the user"
The user asked for the re-evaluation and the cut. The specific choices (per-epic review, three repeats, no dry run, dropping AC4) were the orchestrator's. Unless the user confirmed those specifics, D08 should say "user-directed; specifics proposed by orchestrator" or record the confirmation.

### 10. [note] The test-inventory baseline still shows the old test file
`original_source_baseline["tests/test_installed_service.py"]` keeps the pre-trim assertions, which mention `service_kafka_probe`. The freezer is designed to keep the first-seen source, so this is harmless. It is the only remaining mention of a removed module in the inventory.

## Scope judgement

**Kept, and needed for production readiness:**

| Story | Checklist items |
| --- | --- |
| E09.F1.S7 | D5, V6 |
| E09.F1.S1–S5 | V6, B3, U3 |
| E09.F2.S1 | V7, U3, S3 |
| E10.F1.S1/S2 | A6, U1, U2, U3, U5, D6 |
| E10.F2.S1 | U4 |
| E10.F2.S2 | R5, U5 |
| E11 | T1, T4–T6, V1–V6 final gates |
| E12 | R1–R5 |

`traceability.md` maps every checklist item. V7 and U3 now point only at E09.F2.S1, whose acceptance covers O1–O8 row by row. `ordering-matrix.md` O4–O8 owners were moved to E09.F2.S1.

**Removed or merged, with nothing a checklist item needs lost:**
- S6 and the gate protocol: no checklist item needs a fence or a broker, and D5/V6 are carried by S7.
- E09.F2.S2: its O4–O8, IMDb-topic and order_by-boundary clauses survive in F2.S1.
- E11.F1.S4: its final reruns and single manifest survive in E11.F1.S3, apart from finding 8.
- R5: the text only asks for documentation.

**Should come back:** only the FIFO test (finding 5) and the exclusion-reasons clause (finding 8).

**Still in the plan and not needed (concrete):**
1. **The bundled-context branch.** D06 chose prebuilt ghcr images, yet bundled-context alternatives remain in E11.F1.S3 (validation and change wording), E11.F2.S1 acceptance 4, E11.F2.S2 acceptance 4 and `README.md:193-194`. Cut them.
2. **E11.F1.S1 "configure the selected publisher" (T2, T3).** T2 is decided and T3 is N/A. Reduce the story to T1 account and ownership verification.
3. **E12.F2.S2.** It duplicates E10.F2.S2 (R5, U5). Fold "fill actual release references into the runbook" into E12.F2.S1.
4. **`handoff-e09.md` process obligations.** Per-file tested/Git/current byte equality, doc-only follow-up commits and per-story zero-history reviews go beyond R1–R5. Align them with D08 (see finding 4).

**Open question (not caused by the reset):** D04 advertises Linux arm64 for CLI/container operations, but the E09 substrate is amd64 only. Either V6 needs an arm64 release-image smoke test (for example in E11.F1.S3), or a statement that the E08 arm64 evidence covers it.
