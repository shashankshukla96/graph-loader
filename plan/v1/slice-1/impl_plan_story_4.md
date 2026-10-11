# Implementation Plan: Story 4 - Developer Convenience Script & Full Stack Validation

## Files to Create/Modify
- `J:\Graph Loader\scripts\start_dev.sh` (Create)
- `J:\Graph Loader\scripts\start_dev.ps1` (Create)
- `J:\Graph Loader\Makefile` (Create)

## New Classes and Methods
- N/A

## Key Logic / Implementation Details
- **`scripts/start_dev.sh`**:
  - Resolve the repository root from the script's own directory (`scripts/..`) and `cd` there before checking `.env` or invoking Docker Compose, so the script works from any caller directory.
  - Fail fast if `.env` does not exist.
  - Do not source or manually export `.env`: Docker Compose loads it itself, and shell tokenization of the documented inline password comment is invalid. This prevents both errors and accidental execution of dotenv content.
  - Run `docker compose up -d`.
  - Use a Bash `wait_for_healthy(container, service)` helper that polls `docker inspect` for up to 120 seconds, exits non-zero if the status is `unhealthy` or times out, and prints the failing service logs. Use a loop rather than GNU `timeout` so the script works in macOS Bash as well as Linux/WSL/Git Bash.
  - Print the endpoints to the console once both are ready.
- **`scripts/start_dev.ps1`**:
  - Resolve the repository root from `$PSScriptRoot`, execute all checks and Compose calls there, and restore the caller's location when the script exits.
  - Replicate the logic of `start_dev.sh` natively for Windows PowerShell users (without WSL dependency).
  - Use a `Wait-ForHealthyContainer` function with `Test-Path`, `Start-Sleep`, `Write-Error`, and a 120-second deadline. Fail on the explicit `unhealthy` state as well as on timeout, and show service logs for diagnosis. Check `$LASTEXITCODE` immediately after every native `docker compose up -d` call so a Compose error fails immediately rather than becoming a readiness timeout.
- **`Makefile`**:
  - Provide `dev` (runs `start_dev.sh`), `dev-down` (`docker compose down -v`), and `dev-logs` (`docker compose logs -f`) targets.

## Tests to Write
- N/A (connectivity smoke tests are Story 5). Run `bash -n scripts/start_dev.sh` and static PowerShell syntax validation when a PowerShell runtime is available.
- Create an isolated temporary project copy containing `docker-compose.yaml` and `scripts/start_dev.sh` but no `.env`, then require its copied Bash script to exit 1 with the documented guidance; do not modify the repository's real `.env`.
- Run `make dev` twice and require both services to be healthy after each invocation and all endpoint lines to be printed.
- Run `make dev-down`, verify Compose reports no project containers or volumes, then use `make dev` to restore the stack for Story 5.

## Rollback / Cleanup
- Remove the created files if the implementation fails.
