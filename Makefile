.PHONY: dev dev-down dev-logs lint test-unit build-images stress-100k stress-1m

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

# Tag matches the CLI default (graph-loader-*:latest); override with IMAGE_TAG=x and
# export GRAPH_LOADER_{NODE,EDGE,CLOCK}_IMAGE to match.
IMAGE_TAG ?= latest

build-images:
	docker build -f Dockerfile.node_loader -t graph-loader-node:$(IMAGE_TAG) .
	docker build -f Dockerfile.edge_loader -t graph-loader-edge:$(IMAGE_TAG) .
	docker build -f Dockerfile.clock -t graph-loader-clock:$(IMAGE_TAG) .

.PHONY: monitoring-up monitoring-down monitoring-validate

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
	d="$${GRAPH_LOADER_DATA_DIR:-/mnt/graph-loader/data}"; \
	mkdir -p "$$d/prometheus" "$$d/grafana"; \
	chown 65534:65534 "$$d/prometheus" || { echo "monitoring-up: chown failed on $$d/prometheus - run as root, or chown it to 65534:65534 manually (prom/prometheus runs as uid 65534)" >&2; exit 1; }; \
	chown 472:472 "$$d/grafana" || { echo "monitoring-up: chown failed on $$d/grafana - run as root, or chown it to 472:472 manually (grafana/grafana-oss runs as uid 472)" >&2; exit 1; }; \
	DOCKER_GID=$${DOCKER_GID:-$$(stat -c %g /var/run/docker.sock)} docker compose -f docker-compose.yaml -f docker-compose.monitoring.yml up -d prometheus grafana

# Scoped stop+rm, NOT `down`: with both -f files loaded, Compose treats
# neo4j/kafka/kafka-ui as part of the same project (reviewer finding
# S3-SF3) — an unscoped `down` would stop/remove those too and attempt to
# tear down graph-loader-net, which this profile declares external and
# does not own. DOCKER_GID must be set here too (reviewer finding): Compose
# interpolates the whole file, including group_add, for every subcommand,
# not just `up` — `stop`/`rm` fail the same required-variable error
# without it.
monitoring-down:
	DOCKER_GID=$${DOCKER_GID:-$$(stat -c %g /var/run/docker.sock)} docker compose -f docker-compose.yaml -f docker-compose.monitoring.yml stop prometheus grafana
	DOCKER_GID=$${DOCKER_GID:-$$(stat -c %g /var/run/docker.sock)} docker compose -f docker-compose.yaml -f docker-compose.monitoring.yml rm -f prometheus grafana

# --user matches the promtool container to this host user (reviewer nit):
# without it, the image's own default user may not be able to read a
# checkout with a restrictive umask.
monitoring-validate:
	docker run --rm --user "$$(id -u):$$(id -g)" --entrypoint promtool -v "$$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml
	for f in monitoring/prometheus/alerts/*.yml; do \
		docker run --rm --user "$$(id -u):$$(id -g)" --entrypoint promtool -v "$$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 check rules "/etc/prometheus/alerts/$$(basename $$f)" || exit 1; \
	done
	docker run --rm --user "$$(id -u):$$(id -g)" --entrypoint promtool -v "$$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 test rules /etc/prometheus/alert_tests/concurrency_test.yml /etc/prometheus/alert_tests/progress_test.yml /etc/prometheus/alert_tests/dlq_test.yml /etc/prometheus/alert_tests/write_outage_test.yml
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
