# Run status: where each field comes from

Every progress field of a run status document is `{value, observed_at, source}` (plus `lower_bound: true` where noted)
or `null`.

* `null` means **not known**: the snapshot is missing or unreadable, the observation is older than three status
  intervals, the quantity was never observed, or a poll failed. A field is never `0` or a stale value in its place.
* A field that is **absent** from a type entry is **not applicable** in that mode (the table says which).
* `observed_at` is when the value was observed (for a gauge, its own `*_observed_at_seconds` companion; for a counter
  total, the newest snapshot that contributed). It is never the modification time of a file used to judge freshness.
* A counter total is the sum of every incarnation's last snapshot, so a restarted role keeps its predecessor's writes.
  It carries `lower_bound: true` when a contributing incarnation ended without writing its final snapshot (for example
  `SIGKILL`) or has no readable snapshot.

The same sources are used in both runtimes (Docker and process). Roles write
`<timings>/<execution_id>/<incarnation>/metrics.prom` every status interval (`GRAPH_LOADER_STATUS_INTERVAL_S`, default
5 s) and `final.json` after their last snapshot, whether or not the HTTP metrics endpoint is enabled.

## Node labels (`kind: node`)

| Field | Bulk | Stream | Source |
|---|---|---|---|
| `records_written` | yes | yes | node loaders' `graph_loader_records_consumed_total`, summed over replicas and incarnations. At-least-once write count, not distinct records. |
| `ready` | yes | absent | node monitor's label-ready message (CLI). `null` until it arrives. |
| `lag` | yes | yes | node loader's `graph_loader_consumer_lag_records{kind="node"}`: cached high watermark minus the committed next offset (or the consumer position before the first commit), observed every 5 s with `graph_loader_consumer_lag_observed_at_seconds`. Summed over the running replicas, each of which must have a fresh observation. |
| `rejected` | yes | yes | `graph_loader_rejected_total{kind="node"}`: malformed records routed to the rejection sink or DLQ (observed events, at-least-once). |
| `retry_*`, `dlq_lag`, `parked_size`, `plan_chunks_*` | absent | absent | not applicable to nodes. |

## Relationship types (`kind: edge`)

| Field | Classic / lease, bulk | Classic / lease, stream | Planned bulk | Source |
|---|---|---|---|---|
| `records_written` | yes | yes | yes | edge loaders' and planned writers' `graph_loader_records_consumed_total`. Retry-consumer writes are **not** included. |
| `lag` | yes | yes | absent | CLI lag poll of the relationship monitor (`graph_loader_consumer_lag_records` computation); `null` after a failed poll, never the previous value. |
| `rejected` | yes | yes | absent: per type n/a for planned (the planner counts rejected records only as a run total, not per relationship type; see `totals.rejected`) | `graph_loader_rejected_total{kind="edge"}`. |
| `retry_written`, `retry_redlq`, `retry_parked` | absent | yes, when a retry consumer runs | absent | retry consumer's `graph_loader_retry_outcome_total{outcome=written\|re_dlq\|parked}` (at-least-once outcome counts). |
| `dlq_lag` | absent | yes, when a retry consumer runs | absent | `graph_loader_dlq_backlog{queue="dlq"}`: DLQ high watermark minus the retry consumer's committed offset, with `graph_loader_dlq_backlog_observed_at_seconds` (a failed computation keeps the old gauge value but not the companion, so the field becomes `null` once the observation is older than 180 s). |
| `parked_size` | absent | yes, when a retry consumer runs | absent | `graph_loader_dlq_backlog{queue="parked"}`: physical size of the parking-lot topic, not unresolved logical records. |
| `plan_chunks_done`, `plan_chunks_total` | absent | absent | yes | dispatcher `STATUS` `by_type`, polled by the CLI. |

## Run totals (`totals`)

| Field | Meaning |
|---|---|
| `records_written` | sum over all node and relationship entries; `null` if any entry is unknown; `lower_bound` if any entry is. |
| `rejected` | sum of the entries' `rejected`, except in a planned run, where it is the planner's exact total (`null` until planning has finished). |
| `plan_chunks_done`, `plan_chunks_total` | the dispatcher's run-level chunk counts (planned bulk only). |

## Roles and the run

* `roles[]`: state, incarnation, restart count, exit code, `exit_reason` (`oom`, `signal N`, `execution-finished`) and
  last error come from the runtime handles (`State.Status`, `ExitCode`, `OOMKilled`) polled with the telemetry.
* A process-runtime cgroup OOM that cannot be attributed to one child is an `execution.oom` event, and counts as a role
  failure before a stop request.
* `telemetry.available` is `false`, with a reason, when running roles are past the start-up grace period and none has
  written a readable snapshot (for example a read-only timing directory); every snapshot-derived field is then `null`.
