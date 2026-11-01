# Handoff — E11 release candidate (implementer)

You implement; the orchestrator (the session that messages you) owns scope, status, review, commits and every
question to the user. Do not commit, push, tag, upload or mark stories complete. Written 2026-11-01.

## Read first

- `epics/e11.md` — acceptance criteria and validation (authoritative).
- `decisions.md` — D01 (names), D03 (`0.1.0a1`), D04 (CPython 3.12; Linux amd64 + arm64), D05, D06 (images
  `ghcr.io/shashankshukla96/graph-loader-{cli,node,edge,clock}:<version>`), D07 (file-based version, manual Twine,
  external credentials, no workflows), D08 (scope; per-epic review).
- `docs/RELEASE_RUNBOOK.md` (E10) — follow it; fix it if reality differs, and say so.
- `execution-e07.md`, `execution-e08.md`, `execution-e09.md` — how the installed lanes, support lanes and service
  lanes are run (`scripts/verify_installed.py`, `scripts/freeze_installed_inventory.py`, `scripts/service_lane.py`).

## Order and gates

1. **E11.F1.S1 — accounts (T1).** You cannot sign in to PyPI, TestPyPI or GitHub and must not try. Check only what
   is public without credentials (project-name pages on pypi.org and test.pypi.org, the ghcr.io repositories), write
   the manual Twine environment and token handling into the evidence, and send the orchestrator the list of things
   the user must confirm or do (accounts, 2FA, name ownership, API tokens kept outside the repo, GHCR package
   visibility). Continue to S2 meanwhile.
2. **E11.F1.S2 — freeze.** Prepare the candidate in the working tree: `_version.py` → `0.1.0a1`, CHANGELOG date →
   the freeze date, refreeze `test-inventory.json` with `scripts/freeze_installed_inventory.py --freeze` (existing
   IDs and dispositions unchanged and in order; report the diff). **Then stop and report:** the orchestrator gets the
   user's approval and commits the snapshot. After that, build wheel and sdist from a clean export of that commit
   (`git archive`) in an empty directory, never from the working tree or an old `dist/`. File-version mode: record
   the commit; do not create a Git tag (the tag is an E12 step).
3. **E11.F1.S3 — images and final gates.** Build the four images from the frozen wheel:
   - linux/amd64 natively on the Linux host `shashank@192.168.1.75`;
   - linux/arm64 natively on this Mac (Docker Desktop, Apple silicon). No QEMU/binfmt installation anywhere.
   Rerun on the final bytes and final amd64 image IDs: the E07 installed lanes, the E08 support lanes and the E09
   `service-harness` lane (this reruns the edited `test_service_legacy_classic.py` live). On arm64, run an
   installed-CLI real-load smoke (small planned bulk load with graph and offset checks) against owned arm64 Neo4j
   5.26 Enterprise + Kafka 7.7.0 containers on the Mac. Produce the single candidate manifest (commit, version,
   wheel/sdist names and sha256, image IDs per arch, OCI archive hashes, every gate result with pass/skip/exclusion
   reasons). Any change to bytes after that means a new candidate and rerunning the affected gates.
4. **Stop before E11.F2.** Uploads and image pushes need a new explicit instruction from the user, which only the
   orchestrator relays. Send the manifest and wait.

## Approved for this epic

- pip installs and Docker image builds; pulling `neo4j:5.26-enterprise` (or the exact Neo4j 5.26 Enterprise image E09
  used), `confluentinc/cp-kafka:7.7.0` and the Python base images the Dockerfiles name, on the Linux host and on the
  Mac. Neo4j Enterprise is for throwaway test containers only (user decision).
- Read-only HTTP checks of public PyPI/TestPyPI/GHCR pages.

## Never

- Upload, push (Git or images), tag, sign in to an index or registry, or handle a token. Ask for, type or store a
  password; use sudo.
- On the Linux host touch the user's stack: containers `postgres`, `graph-loader-neo4j`, `graph-loader-kafka`,
  `graph-loader-kafka-ui`; networks `graph-loader-net`, `ws_default`; volumes `graphloader_neo4j_data`,
  `graphloader_neo4j_logs`; `/media/shashank/Data/**`, `/volume/postgres`; host ports 5432, 7474, 7687, 8080, 9092,
  9101. Also leave the orchestrator's IMDb stack `imdb-c7b4c997dfd08de3` alone.
- On the Mac touch anything you did not create: the `neburyx-*` containers, networks, volumes and images, `neo4j:2026`
  / `neo4j:latest`, or any other foreign resource. Another project runs tests on this Docker Desktop at the same time.
- Run `docker system/volume/network/image/builder prune` or any broad filter, on either machine. Remove only owned
  resources by recorded ID, and keep a before/after snapshot of foreign resources on both machines.
- Install system packages or change Docker, firewall, sysctl or group settings.
- Carry forward E06/E09 development image IDs or development wheels as final proof.
- Work around a permission or classifier denial: report it.

## Stop, preserve state and ask when

- a gate fails on the final bytes (report own-harness vs product defect, with a minimal reproduction);
- the inventory refreeze would change an existing ID or disposition;
- a foreign resource changes, or cleanup of an owned resource is uncertain;
- a product-code change seems needed (it means a new candidate).

## Report at the end of each story

Files changed; exact commands and run directories; results (criterion → evidence); foreign-snapshot result and
cleanup receipt per machine; red runs; what is not covered; what the user must do. Write `evidence/<story>.md`.
