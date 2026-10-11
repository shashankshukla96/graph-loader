# P6-S3 Story 6: Makefile Targets — Implementation Plan

## Files touched
- `Makefile` (append-only — one new `.PHONY:` line, three new targets, at
  the very end of the file; zero existing lines modified or reordered)

## Correction to `stories.md`'s literal text
`stories.md`'s draft `monitoring-validate` target runs
`prom/prometheus:v2.55.1 promtool check rules /etc/prometheus/alerts/*.yml`
as a single `docker run` with `--entrypoint promtool`, which fails twice
over: (1) `promtool` needs `--entrypoint promtool` to run at all (see
Story 5's correction), and (2) even with that fixed, `promtool` itself
does not glob `*.yml` — confirmed empirically
(`promtool: error: path '/etc/prometheus/alerts/*.yml' does not exist`).
There is also no shell inside this invocation to expand the glob
container-side, and this sandbox's own tooling refuses a `docker run`
whose entrypoint is `sh`/`/bin/sh` (flagged as an unverifiable command).
Fixed by expanding the glob **on the host**, in the Makefile recipe's own
shell (Make always runs each recipe line through a host shell regardless;
this is not a `docker run ... sh` invocation), issuing one
`docker run --rm --entrypoint promtool ...` per alert file:
```makefile
monitoring-validate:
	docker run --rm --entrypoint promtool -v "$$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml
	for f in monitoring/prometheus/alerts/*.yml; do \
		docker run --rm --entrypoint promtool -v "$$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 check rules "/etc/prometheus/alerts/$$(basename $$f)" || exit 1; \
	done
	docker run --rm --entrypoint promtool -v "$$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 test rules /etc/prometheus/alert_tests/concurrency_test.yml
	python3 scripts/validate_monitoring.py
```

## Implementation
Append (after the existing `validate-workflows` target, changing no
existing line): a second `.PHONY: monitoring-up monitoring-down
monitoring-validate` line, then the three targets:
- `monitoring-up`: `mkdir -p` both new data subdirectories (Compose does
  not reliably create host bind-mount parent directories across all
  Docker versions), then `docker compose -f docker-compose.yaml -f
  docker-compose.monitoring.yml up -d prometheus grafana`.
- `monitoring-down`: the matching `down` for the two-file invocation only.
- `monitoring-validate`: the three checks above as one command.

## Validation
- `git diff Makefile`: confirmed pure append — the diff shows only added
  lines (`+`), no `-` lines, no reordering.
- `make monitoring-validate`: ran end to end, exit 0 (`check config`:
  SUCCESS; `check rules` on all 4 files: SUCCESS each; `test rules`:
  SUCCESS; `validate_monitoring.py`: "1 dashboard(s), 15 panel(s), 13
  alert expr(s), 19 unique metric name(s), all known").
- `ruff check src tests examples`: All checks passed (unaffected by a
  Makefile-only change, confirmed anyway since `make lint` must stay
  green per the First Mate's binding brief).
- `make monitoring-up`/`monitoring-down` full bring-up/tear-down deferred
  to this slice's scratch-run proof (Story 5's finish step), since a real
  `up` needs `graph-loader-net` to exist and this slice's own validation
  uses a dedicated scratch stack instead of the shared network.

## Commit
One commit: `feat(monitoring): make monitoring-up/-down/-validate (P6-S3 Story 6)`.
