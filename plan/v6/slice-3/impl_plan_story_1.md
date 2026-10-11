# P6-S3 Story 1: Prometheus Docker Service Discovery Configuration — Implementation Plan

## Files touched
- New: `monitoring/prometheus/prometheus.yml`

## Contract cross-check (done immediately before implementing, per the
cross-cutting note to re-read `feat/p6-s2-metrics` fresh)

Read `feat/p6-s2-metrics:src/orchestrator/docker_service.py` and
`feat/p6-s2-metrics:src/utils/metrics.py` directly (not only the plan
docs). Confirmed:

- Metrics port constant: `DEFAULT_METRICS_PORT = 9464` in `src/utils/metrics.py`.
- Docker labels set by `_metrics_labels(kind, env)`: `graph_loader.metrics_port`,
  `graph_loader.kind`, `graph_loader.metrics_enabled` (dotted keys).
- Plain (undotted) labels set by each launcher: `app=graph-loader`,
  `component=<node-loader|edge-loader|planned-edge-writer|planned-dispatcher|
  bucket-lease-server|global-batch-clock>`, `node_label` (node loader only),
  `edge_type` + `replica_id` (edge loader), `replica_id` (node loader),
  `writer_id` (planned writer), `run_id` (present on edge/node loaders only
  when `--run-id` was passed; always present on planned-writer,
  planned-dispatcher, lease-server, clock).
- Control ports: `DEFAULT_LEASE_PORT = 7700` (`lease_server.py`),
  `DEFAULT_DISPATCHER_PORT = 7701` (`planned_dispatcher.py`) — never the
  metrics port.
- Network: every launcher's default is `graph-loader-net` (node loader's
  own function signature default `graph_loader_default` is legacy/unused —
  `cli.py`'s `--network` argparse default is `graph-loader-net` and is
  always passed explicitly to every launcher call site, confirmed by
  grepping `src/cli.py` for `network=args.network`).
- `docker-compose.yaml`'s network block: `graph-loader-net` (bridge,
  external name identical to the Compose service name) — Story 3 will
  join this by `external: true`.

No drift from `stories.md`'s Story 1 draft found. Implementing the file
verbatim as specified there, with the `prometheus` self-scrape job added
per the story's own text.

## Implementation

Create `monitoring/prometheus/prometheus.yml` with:
1. `global.scrape_interval: 3s`, `evaluation_interval: 15s`.
2. `rule_files: ["alerts/*.yml"]` (Story 2's files; safe to reference before
   they exist since Story 2 lands immediately after and `promtool check
   config` is re-run at Story 5/6).
3. `scrape_configs`:
   - `job_name: graph-loader` — `docker_sd_configs` against
     `unix:///var/run/docker.sock`, fallback port 9464, `refresh_interval: 2s`,
     `filters: [{name: label, values: ["graph_loader.kind"]}]`.
   - `relabel_configs` in the exact order from the story: (1) drop literal
     control ports 7700/7701 by `__meta_docker_port_private`; (2) drop
     containers with `graph_loader.metrics_enabled == "0"`; (3) keep only
     `graph-loader-net`; (4) promote `graph_loader_kind`, `run_id`,
     `node_label`, `edge_type`, `writer_id`, `replica_id` docker labels to
     series labels `kind`, `run_id`, `node_label`, `edge_type`, `writer_id`,
     `replica_id`; (5) promote container name to `container_name`.
   - `job_name: prometheus` — static self-scrape target `localhost:9090`.

## Validation
- `docker run --rm -v "$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 promtool check config /etc/prometheus/prometheus.yml`
  (once Story 2's alert files exist, since `rule_files` is referenced; until
  then this is expected to fail cleanly on the missing glob — accepted as
  transient within this one commit's window, re-validated at Story 2's
  commit and again at Story 5/6).
- `make lint` unaffected (no `src/`/`tests/` files touched).

## Commit
One commit: `feat(monitoring): Prometheus Docker SD config (Story 1)`.
