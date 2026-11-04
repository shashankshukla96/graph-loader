# Handoff — freeze the 0.1.0a2 candidate (implementer)

You implement. The orchestrator reviews, commits and owns every question to the user. Written 2026-11-04; the user said
"Merge and start freeze". The rules, never-list and report format of `handoff-e19.md` still apply, except where this
file changes them.

**Where.** The worktree `/Users/shashank/Desktop/Code/Graph/graph-loader-e19`, now on branch `freeze-0.1.0a2`, cut from
`main` `7fd63f9` (E14, E15 and E19 merged). Do not switch branches and do not commit.

**Scope.** E11.F1.S2 of `epics/e11.md`, for version `0.1.0a2`. The inputs are:
- `evidence/E19.close.md` ("Freeze list");
- `evidence/E19.F2.S6.md` ("Changelog items");
- `evidence/E14.F3.S11.md` ("Handoff list");
- `epics/e15.md` (status record);
- `evidence/E11.F1.S2.md`, which shows how the 0.1.0a1 freeze ran.

E11.F1.S3 (final images and gates) is **not** part of this; it needs a separate go.

## Part A — prepare the snapshot (uncommitted), then stop and report

1. `_version.py` → `0.1.0a2`, plus the test literals that name the real version. Keep frozen test ids; use a reviewed
   rename only with the orchestrator's approval.
2. `CHANGELOG.md`:
   - `0.1.0a1` was never published. Fold it so the first public entry is `0.1.0a2`, with the release date set to the
     freeze date.
   - The entry covers E14 (process runtime, single container), E15 (run status, stores, JSON logs, portable
     monitoring, the changed bundled-stack defaults) and E19 (the Neburyx namespace and repo default, the registry
     setting, `images build`, the restricted-network guide, the image content change, hashed dependency locks).
   - No old namespace stays in the file.
3. Docs:
   - Replace the pinned `0.1.0a1` wherever it means the current version (`PACKAGE_README.md`, `README.md`, `docs/`).
   - Fix `PACKAGE_README.md` "Support and limits" (it still says loaders run only as Docker containers).
   - Review `README.md` and `.env.example` for the E15 and E19 changes.
   - Keep yaml-fence line positions, so frozen snippet ids survive.
4. Verify the E14 hand-offs against the code and report each one:
   - `--metrics-addr 0.0.0.0` targets in `targets.json`;
   - the `oom` status event;
   - the `/readyz` wording in the docs matching its behaviour.

   Do not change behaviour; report a gap instead.
5. Locks: `make check-lock` must pass unchanged. Do not regenerate the locks unless the check says they are stale; if it
   does, stop and report.
6. Inventory:
   - Move `original_source_baseline` to the private sidecar.
   - Re-freeze on the Linux host with `pytest==9.1.1`, from the prepared tree. Report the additions and every changed
     field. No existing id, disposition or order may change, except through a reviewed rename.
   - Regenerate the exact allow-entries in `export/guard-allow.json` (a stale entry is an error).
7. Goldens: the a1 golden stays untouched. The a2 golden changes only if a real launch changes, and you must report any
   change.
8. Checks: `ruff check src tests examples scripts`, `scripts/sync_requirements.py --check`, the full Mac suite in the
   **foreground**, doc links, `test_export_guard`, and the guard on a materialized tree.

**Host.** Confirm with the orchestrator before each sitting. Use a venv under `/home/shashank/e19/<token>/` and remove it
afterwards. Take a foreign snapshot before and after. No Docker is needed for this part. The host's and the local Neo4j
password equal the old documented default, and that value stays out of the evidence.

## Part B — after the orchestrator commits Part A

Build from `git archive` of that commit, in an empty directory, with the build backend pinned as the inventory records
it:
- wheel and sdist, `twine check --strict`, hashes and metadata;
- rebuild a wheel from the sdist without `.git`, and validate its version and resources;
- run the inventory check on the host against those exact files;
- the provenance manifest for S3.

The artifacts go to `/Users/shashank/Desktop/Code/Graph/a2-candidate/` (0700), outside Git; record their hashes in the
evidence. If the check needs a re-freeze, it means a new commit and a rebuild, so report it.

**Evidence.** `plan/library-implementation/evidence/E11.F1.S2-a2.md`, with Part A and Part B kept separate.

**Never.**
- Overwrite the frozen `0.1.0a1` artifacts in `var/` or in the evidence.
- Create a tag.
- Run the public export.
- Push or upload anything.
- Add any AI attribution.
