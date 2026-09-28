# IMDb bulk-load example

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
└── manifest.json            # traversal budgets and selected counts
```

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

For the measured one-container/four-writer IMDb configuration:

```bash
./scripts/cleanup_imdb_local_load.sh
python examples/imdb/publish_and_load_nodes.py \
  --subset-dir examples/imdb/data/subsets/titles-1000-depth-4 \
  --partitions 4 --consumer-workers 4 --publish-workers 4 \
  --edge-consumers 1 --edge-writers 4 \
  --skip-image-build --report-dir examples/imdb/reports
```

The IMDb schema uses a 2,000-record relationship outer flush, based on the
10k-seed comparison in `reports/edge-outer-batch-benchmark/comparison.md`.
Use `--edge-batch-size 500` to override it for a comparison. Node batches
default to `loading.unwind_batch_size` (500); use `--node-batch-size 750` to
override them independently. Both options accept 1–10,000 records. Each
relationship writer lane retains its separate `mix_and_batch.batch_size` limit
of 1,000. The timing report records both effective outer batch sizes.

`--edge-consumers 1 --edge-writers 4` is the default; specify both explicitly
when comparing runs. Remove `--skip-image-build` after changing loader source.

Add `--publish-only` to inspect Kafka production without starting the bulk
loader. The script creates every declared `imdb-*` node and relationship topic,
produces node JSON records plus canonical relationship events, flushes Kafka,
and then starts the project CLI in bulk mode. The CLI checks each node type's
fixed Kafka boundary, stops and drains its loaders, and makes that label
available to dependent relationships. It starts a relationship when both
endpoint labels are ready and no running relationship uses either label.
The relationship releases those labels after its own boundary, drain
acknowledgement, and zero-lag check. Self-referencing types run in separate
isolated phases after the shared types.

For a load run, the producer workers and node and edge image builds start together;
the consumer fleet starts only after both finish.

`--partitions` controls every generated node and relationship topic's partition count. Existing
topics are expanded when needed; Kafka cannot reduce partitions, so a smaller
request fails clearly. Records are keyed by each node type's schema key so they
are deterministically distributed across those partitions. `--consumer-workers`
controls containers per node type and defaults to `--partitions`, allowing one
consumer per partition. The IMDb run uses one Kafka consumer per relationship
type and four concurrent Neo4j writer threads with four Mix-and-Batch lanes
per type by default. Use `--edge-writers` to change the writer count and
`--edge-consumers` to change the consumer containers per relationship type.
`--publish-workers` separately controls concurrent topic-publisher threads.
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
The bulk timeout defaults to 600 seconds per monitored stage. Streaming mode
continues to use the relationship clock and rotating leases.

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
