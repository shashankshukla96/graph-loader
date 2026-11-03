# Run status stores: JSON files and the SQL database

Every `graph-loader start` records **what it is doing** as a status document and a gap-free list of events, whichever
runtime runs the roles. You choose where they are kept with `--status-store` (or `GRAPH_LOADER_STATUS_STORE`):

| Store | Where | Needs |
| --- | --- | --- |
| `json` (default) | `status.json` and `events.<first seq>.jsonl` in the run's attempt directory | nothing beyond the base install |
| `sql` | a database (PostgreSQL is the proven target) | `pip install "neo4j-graph-loader[postgres]"` and a database URL |

Read the result with `graph-loader status --run` (a human summary), `--json` (the document), `--watch`, and `--list`
(see [CLI reference](CLI.md#status)). What every progress field means and where it comes from is in
[STATUS_FIELDS.md](STATUS_FIELDS.md); the contract of the document and of each event is the pair of JSON Schemas in
[status-schemas/](status-schemas/) (`status.schema.json`, `event.schema.json`; `schema_version` is 1, and later
versions only add fields).

## The JSON store

Under the attempt directory (`stage=runtime directory=...` in the log; `--run-dir BASE` chooses the base):

| File | Content |
| --- | --- |
| `status.json` | the current status document, replaced atomically (at most once a second while it changes, and once more at the end) |
| `events.<first seq>.jsonl` | one event per line, `fsync`ed; `seq` starts at 1 and has no gaps. A new file starts when the current one reaches 8 MB, and only the newest 8 are kept (`GRAPH_LOADER_STATUS_EVENT_FILE_MB`, `GRAPH_LOADER_STATUS_EVENT_FILES`); a reader reports deleted history as a `gap` event |
| `status-locator.json` | which store holds this run and where; never contains a password |

A damaged or unwritable store never affects the load: the store is detached, the execution is flagged
`history_incomplete: true`, and one log line says so.

## What a document and its events say

A document has `execution_id` (one `graph-loader start`, 32 hex digits), the optional reusable `run_id`, the
`state` (`starting`, `running`, `stopping`, then one of `succeeded`, `failed`, `stopped`), `outcome` (exit code, reason,
error), timestamps and `heartbeat_at`, `mode` (`bulk`/`stream`), `runtime` (`docker`/`process`), `activities` and
`phases`, the `roles` (one entry per role **incarnation**, with exit code and exit reason, for example `oom` or
`signal 9`), the node and relationship `types` with their progress fields, run `totals`, `telemetry` and
`history_incomplete`. Event types: `execution.started`, `execution.run_id`, `execution.state`, `execution.finished`,
`execution.oom`, `activity.state`, `phase.started`, `phase.finished`, `role.started`, `role.exited`, `role.oom`,
`error` and `gap`. A running execution whose heartbeat is older than `--stale-after` (default 30 s) is reported
stale; if its recorded process is gone, dead.

```text
$ graph-loader status --run --run-dir ./runs
execution a76bc5b7d78c42acaef9da7ae506157f  run 03c3cfef99a84ab29e0334b43171a86d  bulk/process
state     succeeded
started   2026-11-03T20:02:59.909Z   updated 2026-11-03T20:03:14.123Z   finished 2026-11-03T20:03:14.108Z
outcome   exit code 0
roles     5: 5 exited
  edge EMPLOYED_BY              written 3,000, rejected ?, chunks 12/12
  node Employer                 written 6, rejected 0, lag ?, ready yes
  node Worker                   written 1,500, rejected 0, lag ?, ready yes
  total                          written 4,506, rejected 0, chunks 12/12
```

`?` is "not known" (`null` in the document); a field is never `0` in its place.

## The SQL store

```bash
python -m pip install "neo4j-graph-loader[postgres]==0.1.0a1"     # SQLAlchemy 2 and psycopg 3
export GRAPH_LOADER_STATUS_DB_URL='postgresql+psycopg://USER:PASSWORD@HOST:5432/DBNAME'
graph-loader status-store migrate                                 # once; idempotent
graph-loader start --mode bulk --config schema.yaml --status-store sql --run-dir ./runs
graph-loader status --list                                        # runs of the database
graph-loader status --run --execution-id <id>                     # one of them
```

* **The URL is never a command-line argument.** It is read from `GRAPH_LOADER_STATUS_DB_URL` (or a `.pgpass`,
  `PGPASSWORD`, service file). Only the CLI connects to the database; the roles never receive the URL. It is
  stripped of user information and password-like parameters before it is written to `status-locator.json`, and
  scrubbed from every log line and error message; status errors are redacted the same way. A reader that starts from
  a run directory finds the store from the locator but takes the password from its own environment.
* `pip install "neo4j-graph-loader[sql]"` installs SQLAlchemy only (for another dialect you install its driver
  yourself); `[postgres]` adds `psycopg[binary]`. A base install has neither, and `--status-store sql` then says which
  extra is missing. PostgreSQL is the store that was exercised end to end; SQLite works for tests and small local
  use and has no schemas.
* **Migrations.** `graph-loader status-store migrate [--schema NAME]` creates or upgrades the schema (PostgreSQL
  schema `graph_loader`, or `GRAPH_LOADER_STATUS_SCHEMA`). `start` refuses an older or newer schema and says so;
  `--status-store-auto-migrate` (or `GRAPH_LOADER_STATUS_AUTO_MIGRATE=1`) lets `start` create or upgrade it. Give the
  role that runs `start` the right to create the schema once; day-to-day it needs `SELECT`, `INSERT` and `UPDATE`
  on the tables. The Grafana run-history dashboard needs only `SELECT`.
* **The load never waits for the database.** Every event is written and `fsync`ed to a local outbox
  (`<attempt>/status-outbox/`, bounded by `GRAPH_LOADER_STATUS_OUTBOX_MB`, default 64 MB) before it is queued for
  delivery, which copies it to the database in order, **at least once**; the database ignores a repeated
  `(execution_id, seq)` with the same content. A database outage therefore only delays the history. If the outbox
  overflows, the oldest segment is dropped after a durable gap record, delivered to the database as an event of type
  `gap`, and the execution is flagged `history_incomplete`. If the outbox cannot be written at all, the journal
  degrades to a bounded in-memory queue, says so, and writes `status-degraded.json`.
* **After a kill or an outage:** `graph-loader status-store flush --run-dir <attempt or base> [--wait SECONDS]`
  delivers what a run left in its outbox (the next `start` also recovers it). It serializes with a live writer
  through `status-outbox.lock`.
* **Retention.** `graph-loader status-store prune --older-than 30d` deletes **finished** executions (terminal state)
  that finished longer ago than that, with all their rows; running executions are never deleted.
* A running execution is also written as it progresses: a snapshot goes to the database in one transaction and only if
  its `revision` is higher than the stored one, so a stale writer cannot overwrite a newer snapshot.

### Tables (all in the configured schema)

There are no foreign keys: a failed write of one table can never block another, and `prune` deletes children first.
Join on `execution_id`.

| Table | Primary key | Content |
| --- | --- | --- |
| `schema_version` | `version` | one row: the schema version (1) and `applied_at` |
| `executions` | `execution_id` | `run_id`, `state`, `mode`, `runtime`, `host`, `pid`, `versions` (JSON), `started_at`, `updated_at`, `heartbeat_at`, `finished_at`, `outcome` (JSON), `exit_code`, `revision`, `history_incomplete`, `telemetry` (JSON), `totals` (JSON). Indexes on `run_id` and `started_at` |
| `execution_activities` | `execution_id, name` | `state`, `started_at`, `finished_at`, `updated_at` of each overlapping activity (`planning`, `nodes`, `edges`, `draining`, `stream`) |
| `execution_phases` | `execution_id, phase_id` | `ordinal`, `kind`, `idx`, `edge_type`, `state`, `started_at`, `finished_at` |
| `execution_labels` | `execution_id, name` | run labels (`value`) |
| `execution_types` | `execution_id, kind, name` | one row per node label / relationship type: `ordinal` and `fields` (JSON: its progress fields, each `{value, observed_at, source}` or `null`) |
| `execution_roles` | `execution_id, incarnation` | one row per role incarnation: `ordinal`, `role`, `name`, `replica`, `restart_count`, `state`, `container_id`, `pid`, `started_at`, `finished_at`, `exit_code`, `exit_reason`, `last_error` (JSON) |
| `execution_events` | `execution_id, seq` | `event_type`, `ts`, `payload` (the event as JSON) and `payload_hash` (SHA-256) |

Example queries:

```text
-- failed executions of the last day
SELECT execution_id, run_id, started_at, outcome
FROM graph_loader.executions WHERE state = 'failed' AND started_at > now() - interval '1 day' ORDER BY started_at DESC;

-- the roles of one execution, with the reason each exited
SELECT role, name, replica, restart_count, state, exit_code, exit_reason
FROM graph_loader.execution_roles WHERE execution_id = '<id>' ORDER BY ordinal;

-- executions whose history is incomplete (an outbox gap or a degraded journal)
SELECT execution_id, started_at FROM graph_loader.executions WHERE history_incomplete;
```

The Grafana [run-history dashboard](MONITORING.md#10-grafana-dashboards-and-the-run-history-dashboard) is these
queries over a PostgreSQL datasource.
