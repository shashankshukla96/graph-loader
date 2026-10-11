# P6-S3 Story 3: `docker-compose.monitoring.yml` — Implementation Plan

## Files touched
- New: `docker-compose.monitoring.yml` (repo root, sibling to `docker-compose.yaml`)

## Contract cross-check
- `docker-compose.yaml`'s network block: `graph-loader-net` (bridge driver,
  explicit `name: graph-loader-net`) — confirmed by reading the file
  directly. This slice's file must declare it `external: true` so Compose
  joins the existing network rather than attempting to (re)create it.
- `docker-compose.yaml`'s existing `${GRAPH_LOADER_DATA_DIR:-/mnt/graph-loader/data}`
  convention (used by `neo4j`/`kafka` volumes) — reused verbatim with new
  `prometheus/` and `grafana/` subdirectories.
- `docker-compose.yaml`'s `cpuset` convention: `NEO4J_CPUSET:-0-7`,
  `KAFKA_CPUSET:-8-11` — `MONITORING_CPUSET` defaults to `28-31`, disjoint
  from both, per the same `${VAR:-default}` pattern.
- Per the First Mate's binding brief (orchestrator constraint), scratch
  proof containers use their own network, not `graph-loader-net`; but this
  *production* config file (used by real operators, not the scratch proof)
  must join the real `graph-loader-net` — the scratch proof for this slice
  instead builds a parallel scratch compose stack (documented separately,
  not part of this file) so the real `graph-loader-net` / `graph-loader-*`
  containers are never touched by validation.

## Implementation
Create `docker-compose.monitoring.yml` per `stories.md` Story 3's draft:
`prometheus` (prom/prometheus:v2.55.1) + `grafana` (grafana/grafana-oss:11.3.1),
both on `graph-loader-net` (external), `cpuset: ${MONITORING_CPUSET:-28-31}`,
data under `${GRAPH_LOADER_DATA_DIR:-/mnt/graph-loader/data}/{prometheus,grafana}`,
`/var/run/docker.sock:ro` mounted into `prometheus` only, retention
`${MONITORING_RETENTION:-48h}`, Grafana anonymous Viewer + admin/admin
editor (both overridable), both bound `0.0.0.0` by default (overridable via
`PROMETHEUS_HOST_BIND`/`GRAFANA_HOST_BIND`), Grafana `depends_on: prometheus:
condition: service_healthy`.

## Validation
- `docker compose -f docker-compose.yaml config --services` (base only) —
  confirmed: `kafka`, `kafka-ui`, `neo4j` only, no `prometheus`/`grafana`
  (byte-for-byte unaffected AC holds).
- `docker compose -f docker-compose.yaml -f docker-compose.monitoring.yml config`
  — confirmed both services resolve.
- **Correction to `stories.md`'s Story 3 AC text**, verified empirically
  with a disposable throwaway compose file (not this slice's real file,
  and not the real `graph-loader-net`): `docker compose ... config` does
  **not** fail when an `external: true` network is missing — Compose
  version 2.x's `config` subcommand is a pure render step that never calls
  the Docker API to check external-resource existence; it exits 0
  regardless. The actual, verified failure point is `docker compose ...
  up`, which fails with `network <name> declared as external, but could
  not be found` (exit 1). The AC is retargeted to `up`, not `config`, in
  this slice's `plan/v6/completion_slice-3.md`.
- Deferred to Story 5's scratch-run proof: actually bringing the two
  containers up and confirming healthchecks pass, since that requires the
  base stack's `graph-loader-net` to exist first — done against a
  dedicated scratch network for this slice's own validation (per the
  First Mate's binding brief), not against the shared
  `graph-loader-kafka`/`graph-loader-neo4j` containers.

## Commit
One commit: `feat(monitoring): Prometheus + Grafana compose profile (Story 3)`.
