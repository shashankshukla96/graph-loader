# IMDb bulk-load example

This example turns the official IMDb non-commercial datasets into a bounded
IMDb graph sample, publishes **node records only** to Kafka, and invokes this
project's bulk loader.  Relationship TSVs are generated locally as a model
preview; they are deliberately not published until relationship loading is
available.

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

`--titles N` selects the first `N` eligible titles.  `--depth` controls the
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
├── graph/relationships/     # generated model preview; not loaded yet
└── manifest.json            # traversal budgets and selected counts
```

## Publish nodes and run a bulk load

```bash
.venv/bin/python examples/imdb/publish_and_load_nodes.py \
  --subset-dir examples/imdb/data/subsets/titles-10-depth-2 \
  --partitions 4
```

Add `--publish-only` to inspect Kafka production without starting the bulk
loader.  The script creates the `imdb-*` node topics from `graph_schema.yaml`,
produces one JSON record per node TSV row, flushes Kafka, and then starts the
project CLI in bulk mode.  For a load run, the producer workers and node-loader
image build start together; the consumer fleet starts only after both finish.
It does not publish the relationship TSVs.

`--partitions` controls every generated node topic's partition count. Existing
topics are expanded when needed; Kafka cannot reduce partitions, so a smaller
request fails clearly. Records are keyed by each node type's schema key so they
are deterministically distributed across those partitions. `--consumer-workers`
controls containers per node type and defaults to `--partitions`, allowing one
consumer per partition. `--publish-workers` separately controls concurrent
topic-publisher threads. Every run writes a Markdown timing report to
`<subset-dir>/reports/` (or `--report-dir`) with topic setup, per-topic
Kafka-publication, image-build, consumer/Neo4j lifecycle, and total timings.
Use `--skip-image-build` when `graph-loader-node:latest` is already current;
the report records that the image-build step was not run.

The example retains IMDb's `primary_name` and also emits `name` for people.
That alias is compatible with the local project's existing non-null
`:Person.name` constraint.

See [model.md](model.md) for the source-to-graph mapping.
