.PHONY: dev dev-down dev-logs lint test-unit build-images stress-100k stress-1m requirements check-requirements lock check-lock

requirements:
	python3 scripts/sync_requirements.py

check-requirements:
	python3 scripts/sync_requirements.py --check

# Hashed per-platform dependency locks for the loader images (developer tool; needs Docker). Each lock is resolved inside the base image on its platform.
lock:
	python3 scripts/lock_requirements.py generate --platform linux/amd64
	python3 scripts/lock_requirements.py generate --platform linux/arm64

check-lock: check-requirements
	python3 scripts/lock_requirements.py check

dev:
	@bash scripts/start_dev.sh

dev-down:
	docker compose down -v

dev-logs:
	docker compose logs -f

lint:
	ruff check src tests examples

test-unit:
	python3 -m pytest tests examples/imdb/tests -m "not integration and not smoke"

# Local development candidates are separate from proposed release tags.
# Choose a fresh suffix/output; the builder refuses to overwrite local tags.
CANDIDATE_SUFFIX ?= local
CANDIDATE_OUTPUT ?= var/candidate-images-$(CANDIDATE_SUFFIX)

# Dependencies come from the public PyPI by default; for a build without it set CANDIDATE_WHEELHOUSE=<dir prepared for this platform> (see
# docs/DISTRIBUTION_DEVELOPMENT.md) or CANDIDATE_INDEX=<your mirror>. Needs docker with buildx and the `build` package.
CANDIDATE_WHEELHOUSE ?=
CANDIDATE_INDEX ?=

build-images: check-requirements check-lock
	python3 scripts/build_candidate_images.py --suffix "$(CANDIDATE_SUFFIX)" --output "$(CANDIDATE_OUTPUT)" \
		$(if $(CANDIDATE_WHEELHOUSE),--wheelhouse "$(CANDIDATE_WHEELHOUSE)",$(if $(CANDIDATE_INDEX),--pip-index-url "$(CANDIDATE_INDEX)"))

.PHONY: monitoring-up monitoring-down monitoring-validate monitoring-bundle

# monitoring-up sources .env itself (reviewer finding, should-fix 1):
# docker-compose.yaml/.monitoring.yml are read by `docker compose`, which
# auto-loads .env for variable substitution, but `make`'s recipe shell does
# not — an operator who sets GRAPH_LOADER_DATA_DIR only in .env (as
# docs/USAGE.md's own §2 says is supported) would otherwise get mkdir/chown
# run against the wrong (default) path while Compose mounts the .env path,
# reproducing the exact permission-denied blocker (B2) this recipe exists
# to fix. All of monitoring-up's steps run in one shell invocation (`\`
# continuation) so the sourced variables stay in scope through the final
# docker compose call.
#
# Exported variables win over .env (same rule as docker compose and the
# scripts that use load_dotenv(override=False)): the recipe snapshots the
# exported environment (`export -p`), sources .env, then re-applies the
# snapshot, so .env only fills in variables that were not set (the readonly
# BASHOPTS/SHELLOPTS are left out of the snapshot: re-assigning them would
# fail when they are exported). The project
# .env may point at remote systems; export every connection variable for the
# stack you mean before running this.
monitoring-up:
	@__gl_exported="$$(export -p | grep -Ev '^(export|declare -x) (BASHOPTS|SHELLOPTS)=')"; set -a; [ -f .env ] && . ./.env; set +a; eval "$$__gl_exported"; unset __gl_exported; \
	if [ -z "$$GRAFANA_ADMIN_PASSWORD" ]; then echo "monitoring-up: GRAFANA_ADMIN_PASSWORD is required (Grafana's admin password is never defaulted); export one of your own, or put it in .env" >&2; exit 1; fi; \
	if [ "$$GRAFANA_ADMIN_PASSWORD" = "admin" ] || [ "$${#GRAFANA_ADMIN_PASSWORD}" -lt 8 ]; then echo "monitoring-up: GRAFANA_ADMIN_PASSWORD must be at least 8 characters and not 'admin'" >&2; exit 1; fi; \
	d="$${GRAPH_LOADER_DATA_DIR:-/mnt/graph-loader/data}"; \
	mkdir -p "$$d/prometheus" "$$d/grafana"; \
	chown 65534:65534 "$$d/prometheus" || { echo "monitoring-up: chown failed on $$d/prometheus - run as root, or chown it to 65534:65534 manually (prom/prometheus runs as uid 65534)" >&2; exit 1; }; \
	chown 472:472 "$$d/grafana" || { echo "monitoring-up: chown failed on $$d/grafana - run as root, or chown it to 472:472 manually (grafana/grafana-oss runs as uid 472)" >&2; exit 1; }; \
	DOCKER_GID=$${DOCKER_GID:-$$(stat -c %g /var/run/docker.sock)} docker compose -f docker-compose.yaml -f docker-compose.monitoring.yml $${MONITORING_EXTRA_COMPOSE:+-f $$MONITORING_EXTRA_COMPOSE} up -d prometheus grafana

# Scoped stop+rm, NOT `down`: with both -f files loaded, Compose treats
# neo4j/kafka/kafka-ui as part of the same project (reviewer finding
# S3-SF3) — an unscoped `down` would stop/remove those too and attempt to
# tear down graph-loader-net, which this profile declares external and
# does not own. DOCKER_GID must be set here too (reviewer finding): Compose
# interpolates the whole file, including group_add, for every subcommand,
# not just `up` — `stop`/`rm` fail the same required-variable error
# without it.
monitoring-down:
	GRAFANA_ADMIN_PASSWORD=$${GRAFANA_ADMIN_PASSWORD:-unused-for-shutdown} DOCKER_GID=$${DOCKER_GID:-$$(stat -c %g /var/run/docker.sock)} docker compose -f docker-compose.yaml -f docker-compose.monitoring.yml $${MONITORING_EXTRA_COMPOSE:+-f $$MONITORING_EXTRA_COMPOSE} stop prometheus grafana
	GRAFANA_ADMIN_PASSWORD=$${GRAFANA_ADMIN_PASSWORD:-unused-for-shutdown} DOCKER_GID=$${DOCKER_GID:-$$(stat -c %g /var/run/docker.sock)} docker compose -f docker-compose.yaml -f docker-compose.monitoring.yml $${MONITORING_EXTRA_COMPOSE:+-f $$MONITORING_EXTRA_COMPOSE} rm -f prometheus grafana

# E15.F2.S12: MONITORING_EXTRA_COMPOSE=<file> appends one more Compose override file (another network, container names or ports) to
# monitoring-up/-down, for running the quick start beside other stacks.
# E15.F2.S10: write the monitoring bundle (docker discovery, the quick start's scrape interval) with your own values, then
# `MONITORING_DIR=$$(pwd)/.monitoring-bundle make monitoring-up`. Needs the graph-loader package installed.
monitoring-bundle:
	graph-loader monitoring export --discovery docker --scrape-interval 3s --out .monitoring-bundle --force $${MONITORING_VALUES:+--values $$MONITORING_VALUES}

# --user matches the promtool container to this host user (reviewer nit):
# without it, the image's own default user may not be able to read a
# checkout with a restrictive umask.
monitoring-validate:
	docker run --rm --user "$$(id -u):$$(id -g)" --entrypoint promtool -v "$$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml
	for f in monitoring/prometheus/alerts/*.yml; do \
		docker run --rm --user "$$(id -u):$$(id -g)" --entrypoint promtool -v "$$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 check rules "/etc/prometheus/alerts/$$(basename $$f)" || exit 1; \
	done
	docker run --rm --user "$$(id -u):$$(id -g)" --entrypoint promtool -v "$$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 test rules /etc/prometheus/alert_tests/concurrency_test.yml /etc/prometheus/alert_tests/progress_test.yml /etc/prometheus/alert_tests/dlq_test.yml /etc/prometheus/alert_tests/write_outage_test.yml /etc/prometheus/alert_tests/health_test.yml /etc/prometheus/alert_tests/lag_errors_test.yml
	python3 scripts/validate_monitoring.py

.PHONY: stress-100k stress-1m

# Star-topology stress tiers (tests/stress/). Both need
# scratch/throwaway Neo4j + Kafka via testcontainers -- never the shared
# graph-loader-neo4j/graph-loader-kafka containers.
stress-100k:
	python3 -m pytest tests/stress/test_star_topology_stress.py -m "integration and stress" -k "not 1m"

stress-1m:
	GRAPH_LOADER_STRESS_SCALE=1000000 python3 -m pytest \
		"tests/stress/test_star_topology_stress.py::test_1m_edges_into_one_hub_planned_mode_zero_lock_waits" \
		-m "integration and stress"
