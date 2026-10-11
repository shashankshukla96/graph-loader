# IMDb bulk-load example

Back to the [project README](../../README.md).

This example turns the official IMDb non-commercial datasets into a bounded
IMDb graph sample, publishes node and relationship records to Kafka, and
invokes this project's public bulk loader. Relationship rows use the same
schema-declared endpoint envelope as the production edge loader.

The downloaded and extracted official TSVs are never changed.  A subset's
`source/` directory is a filtered copy with the same column schema and values;
`graph/` contains graph-oriented TSVs derived from that copy.

## Prerequisites

Install project dependencies and the optional Pandas dependency used only by
the local subset builder:

```bash
.venv/bin/pip install -r requirements.txt -r examples/imdb/requirements.txt
```

Start the local Kafka and Neo4j services as described in the repository root
README before publishing data.

## Build a bounded sample

```bash
# Download the official compressed files once.
.venv/bin/python examples/imdb/download_imdb.py

# Extract unchanged TSVs to examples/imdb/data/extracted/.
.venv/bin/python examples/imdb/extract_tsv.py

# Start from ten titles, follow two Title <-> Person credit hops, and show progress.
.venv/bin/python examples/imdb/build_subset.py --titles 10 --depth 2 --verbose
```

`--titles N` selects `N` eligible seed titles. `--offset M` skips the first `M`
eligible titles before selecting seeds, so `--titles 100 --offset 0` selects
the first 100 and `--titles 100 --offset 100` selects the next 100. Eligibility
respects `--include-adult`. Offset zero keeps the existing
`titles-N-depth-D/` name; other offsets use `titles-N-offset-M-depth-D/`.
The manifest records `seed_offset`.
Build all offset batches from the same extracted IMDb snapshot; a refreshed
source can change which titles occupy those positions.

```bash
python examples/imdb/build_subset.py --titles 100 --offset 0 --depth 4
python examples/imdb/build_subset.py --titles 100 --offset 100 --depth 4
```

`--depth` controls the
alternating breadth-first traversal: depth 1 finds their people; depth 2 finds
titles connected to those people; and so on.  The defaults cap the result at
`N * 10` titles and `N * 50` people.  Increase `--max-titles` and
`--max-people` explicitly when a larger demo is intended.

The builder uses Pandas' C parser in 500,000-row chunks, so it never loads a
multi-gigabyte IMDb file into memory.  `--verbose` reports every chunk,
throughput, ordered-key early exits, per-file copy totals, and final counts.
IMDb's title-keyed exports are ordered by ID; for small early samples the
builder stops a title-keyed scan after it passes the last needed title.

For the command above, use this output directory:

```text
examples/imdb/data/subsets/titles-10-depth-2/
├── source/                  # filtered official-schema TSV copies
├── graph/nodes/             # files sent to Kafka in this example
├── graph/relationships/     # relationship records published to Kafka
└── manifest.json            # traversal budgets, selected counts, records per graph file
```

### Convert the entire dataset

`--all` converts every eligible title and every person instead of a traversal:

```bash
python examples/imdb/build_subset.py --all --include-adult --verbose   # -> data/subsets/all-with-adult/
python examples/imdb/build_subset.py --all                             # -> data/subsets/all/ (no adult titles)
```

The graph files have the same columns, formats, derivation and dedup rules as a
subset's: the result is what a subset would contain if its traversal had
selected every title and person. Title eligibility is the seed rule (a title
type and primary title, and `isAdult = 0` unless `--include-adult`). A
relationship is written only when both endpoints are nodes, so credits for
people or titles missing from the basics files, episodes of an unselected
series, and known-for or alternative titles of excluded titles are dropped,
exactly as in a subset. The publisher and planner work on it unchanged
(`--subset-dir examples/imdb/data/subsets/all-with-adult`).

`--all` writes no `source/` copies (they would duplicate the extracted TSVs);
the graph is derived from `data/extracted/` directly. Its manifest records
`mode`, `include_adult`, the selected counts and the records per graph file.
It cannot be combined with `--titles`, `--offset` or the budgets.

The conversion first collects the selected title and person IDs, then streams
each source file independently. `--jobs` (default 4 with `--all`, 1 otherwise)
derives that many files concurrently in forked processes; the output is
byte-identical for any `--jobs`. ID sets and deduplicated node keys are held
as 128-bit digests in sorted NumPy arrays (16 bytes per key), so the whole
conversion stays at a few GB of RAM. Measured on this host (8 cores): about
5 minutes (310 s) and a 4.5 GiB peak RSS summed across all processes for `--all --include-adult`,
producing 16 GB of TSVs (94M nodes, 467M relationship records).

## Quick start: the fastest load

Once a subset exists (see above) and Kafka and Neo4j are running, this is the
recommended way to load it. It uses planned mode, which has zero Neo4j lock
waits:

```bash
./scripts/cleanup_imdb_local_load.sh           # clear this example's Kafka input topics
python examples/imdb/publish_and_load_nodes.py \
  --subset-dir examples/imdb/data/subsets/titles-40000-depth-4 \
  --partitions 4 --consumer-workers 4 --publish-workers 4 \
  --edge-plan --report-dir examples/imdb/reports
```

The command publishes the node records, plans every relationship while the
nodes load, and writes the relationships with a pool of writers. No two
writers ever touch the same node at the same time. It prints and saves a
Markdown timing report. Add `--skip-image-build` when the loader images are
already current.

These are measurements from single runs on this repository's host (Neo4j
pinned to 8 cores, Kafka to 4, loaders on the rest), not guarantees; your
numbers will differ. "End-to-end" includes reading the TSVs and publishing
to Kafka.

| Subset | Input records | Graph created | Mode | End-to-end | Lock waiting |
| --- | ---: | --- | --- | ---: | ---: |
| `titles-20000-depth-4` | 11.0M | 2.15M nodes, 8.4M relationships | `--edge-plan` | about 60 s | 0 ms |
| `titles-20000-depth-4` | 11.0M | same | `--edge-classic --bulk-edge-overlap shared --edge-consumers 4` | about 87 s | about 82% of edge-query time |
| `titles-40000-depth-4` | 23.3M | 4.06M nodes, 17.8M relationships | `--edge-plan` | 107.8 s | 0 ms |
| `titles-40000-depth-4` | 23.3M | same | `--edge-plan --plan-source kafka` | 122.4 s | 0 ms |
| `titles-40000-depth-4` | 23.3M | same | `--edge-classic --bulk-edge-overlap shared --edge-consumers 4` | 166.5 s | about 82% |

All modes produce the same graph. The 40k-seed planned and classic loads
match on every label's and type's count and property checksum. The number
of relationships created is lower than the number of input records, because
repeated records for the same pair `MERGE` into one relationship. For how
planned mode works and every option, see the
[library usage guide](../../docs/USAGE.md).

## Publish nodes and run a bulk load

Clear this example's Kafka input topics before a fresh load:

```bash
./scripts/cleanup_imdb_local_load.sh
```

The cleanup reads `KAFKA_BOOTSTRAP_SERVERS` from the environment or project
`.env`, so it also works when Kafka runs outside local Docker Compose. Use
`--dry-run` to inspect which IMDb topics it would delete. It keeps Neo4j data
and the loader's shared control and clock topics.
Node upserts and relationship merges make a fresh run safe against IMDb graph
records already in Neo4j. The publisher checks that its Kafka input topics are
empty before sending any records, so a failed run cannot silently append a
second copy of the subset.
For incremental loads, clear the IMDb Kafka input topics and node consumer
groups before each `publish_and_load_nodes.py` run, then point it at the next
offset subset. Keep the Neo4j graph: repeated nodes and relationships are
upserted, and traversal around different seed batches can overlap.

For 100 seed titles at depth 2, one Kafka partition and one consumer per node
type, with four parallel Kafka publishers:

```bash
.venv/bin/python examples/imdb/publish_and_load_nodes.py \
  --subset-dir examples/imdb/data/subsets/titles-100-depth-2 \
  --partitions 1 \
  --consumer-workers 1 \
  --publish-workers 4 \
  --bulk-timeout-seconds 600
```

```bash
.venv/bin/python examples/imdb/publish_and_load_nodes.py \
  --subset-dir examples/imdb/data/subsets/titles-10-depth-2 \
  --partitions 4
```

For the measured one-container/four-writer IMDb configuration (classic
loaders, so `--edge-classic`; `--edge-consumers` and `--edge-writers` apply to the
classic and lease loaders only):

```bash
./scripts/cleanup_imdb_local_load.sh
python examples/imdb/publish_and_load_nodes.py \
  --subset-dir examples/imdb/data/subsets/titles-100-depth-4 \
  --partitions 4 --consumer-workers 4 --publish-workers 4 \
  --edge-classic --edge-consumers 1 --edge-writers 4 \
  --skip-image-build --report-dir examples/imdb/reports
```

The IMDb schema uses a 2,000-record relationship outer flush, based on a
10k-seed comparison (its report is not committed to this repository).
Use `--edge-batch-size 500` to override it for a comparison. Node batches
default to `loading.unwind_batch_size` (500); use `--node-batch-size 750` to
override them independently. Node batches accept 1–10,000 records and edge
batches 1–1,000,000 (classic only; lease runs use `--bucket-lease-outer-batch-size`). Each relationship writer lane retains its separate
`mix_and_batch.batch_size` limit of 1,000. The timing report records both
effective outer batch sizes.

### Stopping a run

`publish_and_load_nodes.py` forwards SIGINT/SIGTERM to the `python -m src.cli
start` child it launches and waits for the CLI's own graceful stop (about
5-10 s) before exiting with the child's exit code. A second signal is
forwarded too; as the CLI documents, it changes nothing and does not force
the stop. If the child is still running 60 s after the first signal it is
killed.

### Bucket leases (node-disjoint relationship writes)

`--edge-leases` starts a run-scoped lease coordinator container (the edge
image running `src.orchestrator.lease_server`) and makes every relationship
transaction lease the nodes it touches: each node label has
`--lease-buckets` (default 16) stable hash buckets, and labels whose node
topic holds at most `--lease-keyed-max-nodes` (default 1,000) records -- IMDb
genres, title types, professions, regions and languages -- get one resource
per node, so hub nodes such as `Drama` and `Comedy` never block each other.
Concurrently running transactions of all edge types and containers are then
node-disjoint and never wait on Neo4j locks, so all types (including the
self-referencing `EPISODE_OF`) run concurrently (`--bulk-edge-overlap shared`
is implied) and the `apoc.lock.nodes` step is skipped. Each edge outer flush
(default 32,000 records in lease mode; `--edge-batch-size` overrides it) is
split into (source resource, target resource) cells; writers take the
largest leasable cells, merge small cells that share a leased resource into
one transaction of up to 200 rows (`GRAPH_LOADER_LEASE_MERGE_ROWS`), and cap
each transaction at `mix_and_batch.batch_size`. If the coordinator is
unreachable or dies, the edge loaders fail and the run fails; nothing is
written without a lease. The report records the lease settings and the
coordinator's final counters (grants, waits, maximum concurrent leases).

```bash
python examples/imdb/publish_and_load_nodes.py \
  --subset-dir examples/imdb/data/subsets/titles-100-depth-4 \
  --partitions 4 --consumer-workers 4 --edge-consumers 4 --edge-writers 4 \
  --edge-leases --report-dir examples/imdb/reports
```

### Variant: a backfill topic for one relationship type (not the default)

A relationship type can be fed by more than one topic. For example, a late backfill of genre assignments can
arrive on a second topic that must override the first. Declare the type with `topics:` and a conflict rule in a
copy of the schema (the default `graph_schema.yaml` and the default IMDb run are unchanged):

```yaml
  - type: HAS_GENRE
    topics: [imdb-has-genre, imdb-has-genre-backfill]
    topic_conflict: precedence          # the backfill topic (listed last) wins, property by property
    nodes: {source: Title, target: Genre}
    source_key_property: tconst
    target_key_property: name
    properties: {title_id: {type: string, required: true}}
```

`precedence` runs in bulk modes only (planned, which is the default, `--edge-leases` and `--edge-classic`); it is
refused with `--mode stream`. The other choices are `order_by: <property>` (newest value wins, every mode) and
`topic_conflict: arrival` (nondeterministic). The publisher in this example writes only `imdb-has-genre`; the
backfill topic is yours to fill with the same record shape, and **it must exist before the load starts, even if
it is empty** (create it with `kafka-topics --create`, or publish one record): a declared topic that is missing
makes the bulk boundary capture fail. With `precedence` in `--edge-leases` or
`--edge-classic`, the report shows one row per topic under the type (`substage-HAS_GENRE.t<rank>.json`), and each
rerun needs a fresh `--run-id`. See [`../multi_topic/README.md`](../multi_topic/README.md) for a complete example.

### Planned edges (one global plan, one writer pool)

`--edge-plan` (exclusive with `--edge-leases`) uses the library's planned bulk
mode (see the [library usage guide](../../docs/USAGE.md)).
Two plan sources produce the same plan:

* `--plan-source files` (default, fastest for IMDb) plans in the publisher with
  the library producer API (`src.loader.edge_planner`; this example only adds
  `TsvEdgeSource`, see `planned_edges.py`) and never publishes the per-type
  edge topics.
* `--plan-source kafka` publishes the per-type edge topics exactly like the
  normal mode and starts the loader with `--edge-plan` alone: the loader plans
  from the topics while the nodes load. Unset `--plan-buckets`,
  `--plan-hub-degree` and `--plan-chunk-rows` are derived by the loader.

Measured on a 4-core scratch host with the 20k subset (8.9M relationship
rows): from the files, node publishing took 5.1 s and the plan was ready
10.2 s after start. Via Kafka, publishing nodes and edges took 7.0 s and
planning another 12.7 s (806k records/s), so the plan was ready about 19.7 s
after start. On the full host with the 40k subset (19.2M relationship rows),
`files` took 107.8 s end-to-end and `kafka` took 122.4 s. That is why `files`
stays the default here. Both sources give the identical graph.

With `--plan-source files`, only the node topics are published
record by record. Meanwhile `--plan-workers` planner processes (default
`--publish-workers`) read every relationship TSV, map each endpoint to a
resource (`--plan-buckets` hash buckets per label; every node of total
degree >= `--plan-hub-degree` over all types gets its own resource; both
are derived from the writers and the relationship volume unless set), group rows into units (edge type, source resource, target
resource), cut units into chunks of at most `--plan-chunk-rows` (derived
from the writers and the volume, at most 5000 by default) rows, and produce one Kafka message per chunk to `--plan-topic`
(`imdb-edge-chunks`, `max.message.bytes` 16 MiB). The plan file (every
chunk's type, resources, rows, rank and Kafka offset) is written beside the
timing logs. The bulk CLI starts as soon as the nodes are published, so
planning overlaps node loading. It then runs a dispatcher container and
`--plan-writers` (default: Neo4j available processors + 2, probed; 10 if the
probe fails) writer containers for all edge types.
Chunks are granted highest rank first (busiest resource first), only when
both endpoint labels are ready and no running chunk shares a resource. Each
chunk is one MERGE transaction, and concurrent transactions are node-disjoint.
The run succeeds only when the dispatcher's completion ledger holds every
chunk exactly once with its planned rows. Each planner process stays within
`--plan-memory-mb` (default: a quarter of the available RAM over the planner
processes) and spills to `--plan-spill-dir` (default: beside the plan) beyond
it, so the full dataset plans with bounded memory; see "Large inputs" in
[docs/USAGE.md](../../docs/USAGE.md). The ledger is also written, fsync'd,
as `edge-plan.json.ledger.json`. Any writer failure or disconnect fails the
run. MERGE makes a full replay safe.

```bash
python examples/imdb/publish_and_load_nodes.py \
  --subset-dir examples/imdb/data/subsets/titles-20000-depth-4 \
  --partitions 4 --consumer-workers 4 --edge-plan --plan-writers 10 \
  --report-dir examples/imdb/reports
```

`--edge-consumers 1 --edge-writers 4` is the default for the classic and lease
loaders (planned mode uses `--plan-writers` instead); specify both explicitly
when comparing classic or lease runs. Remove `--skip-image-build` after changing loader source.

Add `--publish-only` to inspect Kafka production without starting the bulk
loader. The script creates every declared `imdb-*` node and relationship topic,
produces node JSON records plus canonical relationship events, flushes Kafka,
and then starts the project CLI in bulk mode. The CLI checks each node type's
fixed Kafka boundary, stops and drains its loaders, and makes that label
available to dependent relationships. It starts a relationship when both
endpoint labels are ready and no running relationship uses either label.
The relationship releases those labels after its own boundary, drain
acknowledgement, and zero-lag check. Self-referencing types run in separate
isolated phases after the shared types. All of this describes the classic
label-serialized mode, which now needs `--edge-classic` (it is the one mode that
can wait on Neo4j locks, and the only one that honors `--bulk-edge-overlap` and
`--max-concurrent-bulk-edges`). With no edge-mode flag the runner follows the
CLI's bulk default: planned mode, planned from the per-type edge topics
(equivalent to `--edge-plan --plan-source kafka`), with zero lock waits.
`--edge-plan` (producer-side planning by default), `--edge-leases` and
`--edge-classic` are mutually exclusive and schedule relationships as described
in their sections; `--edge-classic --bulk-edge-overlap shared` is the shared
overlap comparison mode. Migration: a command that used to run classic by
default now needs `--edge-classic` to keep doing so.

For a load run, the producer workers and node and edge image builds start together;
the consumer fleet starts only after both finish.

`--partitions` controls every generated node and relationship topic's partition count. Existing
topics are expanded when needed; Kafka cannot reduce partitions, so a smaller
request fails clearly. Records are keyed by each node type's schema key so they
are deterministically distributed across those partitions. `--consumer-workers`
controls containers per node type and defaults to `--partitions`, allowing one
consumer per partition. In classic and lease modes the IMDb run uses one Kafka consumer per
relationship type and four concurrent Neo4j writer threads per type by default (the four
Mix-and-Batch lanes exist only in classic; `--edge-writers` sets an unused `lane_count` in lease mode). Use `--edge-writers` to change the writer count and
`--edge-consumers` to change the consumer containers per relationship type.
`--publish-workers` separately controls the publisher worker processes shared by
all node and relationship topics. Large topics are split into key-hash shards
(`--publish-shards`, default automatic; `1` disables splitting) so every record
for a key is still produced by one producer in file order, and the largest work
is scheduled first. `--publish-compression` (default `lz4`) and
`--publish-linger-ms` (default `50`) tune the Kafka producers.
Every run writes a Markdown timing report to
`<subset-dir>/reports/` (or `--report-dir`) with topic setup, per-topic
Kafka-publication, image-build, consumer/Neo4j lifecycle, and total timings.
Full load reports also show successful Neo4j node and relationship write batches,
records, total write time, mean batch time, and maximum batch time per graph type.
Edge timings measure each flush, which can contain concurrent lane writes;
the summed timings are not end-to-end wall-clock time.
Raw write timing logs remain in an `imdb-write-timings-*` directory next to the
report so Docker's mounted log files are not deleted while still open.
Use `--skip-image-build` when the node and edge images are already
current; the report records that the image-build step was not run.
The bulk timeout defaults to 600 seconds per monitored stage. This example is
bulk only; `--mode stream` uses the bucket-lease fleet by default (the clock
and rotation path is the deprecated `--stream-clock-rotation`).

The publisher loads the project `.env` without overriding exported variables.
For Kafka and Neo4j on another machine, set `NEO4J_URI` and
`KAFKA_BOOTSTRAP_SERVERS` for the host process, plus `LOADER_NEO4J_URI` and
`LOADER_KAFKA_BOOTSTRAP_SERVERS` for Docker loader containers. With both loader
overrides set, the publisher uses Docker's `bridge` network by default. Use
`--network` if your Docker setup requires another network.

Kafka must advertise a broker address reachable from both the host process
and loader containers; advertising `localhost` from the remote machine will
cause metadata requests to redirect clients back to their own machine. If the
broker machine uses this repository's Compose file, set `KAFKA_EXTERNAL_HOST`
to its reachable address in that machine's `.env` and recreate the Kafka
service so the advertised listener changes.

The example retains IMDb's `primary_name` and also emits `name` for people.
That alias is compatible with the local project's existing non-null
`:Person.name` constraint.

See [model.md](model.md) for the source-to-graph mapping.
