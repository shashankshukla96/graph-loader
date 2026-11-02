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

Install the library and the Pandas dependency used only by the subset builder:

```bash
pip install neo4j-graph-loader -r examples/imdb/requirements.txt
```

The example files are:

| File | Purpose |
| --- | --- |
| `download_imdb.py`, `extract_tsv.py` | Fetch the official compressed TSVs and extract them unchanged |
| `build_subset.py` | Build a bounded subset (or the whole dataset) as graph TSVs plus a manifest |
| `graph_schema.yaml`, `model.md` | The graph schema and the source-to-graph mapping |
| `load_imdb.py` | Publish a subset to Kafka and run the bulk load with the installed package |

Kafka and Neo4j must be running before you load (for a local stack, see the
repository root README).

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
exactly as in a subset. `load_imdb.py` loads it like any subset
(`examples/imdb/data/subsets/all-with-adult`).

`--all` writes no `source/` copies (they would duplicate the extracted TSVs);
the graph is derived from `data/extracted/` directly. Its manifest records
`mode`, `include_adult`, the selected counts and the records per graph file.
It cannot be combined with `--titles`, `--offset` or the budgets.

The conversion first collects the selected title and person IDs, then streams
each source file independently. `--jobs` (default 4 with `--all`, 1 otherwise)
derives files concurrently using explicit spawn; output remains byte-identical
for any `--jobs`. ID sets use 128-bit digests in sorted NumPy arrays (16 bytes per
key). Spawn copies those sets into each worker; use `--jobs 1` to avoid those
copies. See the spawn memory notes below. The previous fork implementation was
measured on this 8-core host at 310 seconds and 4.5 GiB summed peak RSS for
`--all --include-adult`, producing 16 GB of TSVs (94M nodes, 467M relationships).

## Load a subset

`load_imdb.py` loads a subset with the installed package only: it imports the
public `neo4j_graph_loader.schema` module and runs the installed `graph-loader`
executable, so it works from `pip install neo4j-graph-loader` without a
checkout. It needs a reachable Kafka broker, Neo4j 5 Enterprise with the target
database already created, and the four loader images available to Docker (see
the [Docker execution guide](../../docs/DOCKER_EXECUTION.md)).

```bash
export NEO4J_URI=bolt://localhost:7687 NEO4J_USER=neo4j NEO4J_PASSWORD=...
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python examples/imdb/load_imdb.py examples/imdb/graph_schema.yaml \
  examples/imdb/data/subsets/titles-10000-depth-4 --neo4j-database imdb --delete-topics
```

The script:

1. validates the schema and writes a copy whose topics and consumer group carry
   a unique prefix (`imdb-load-<UTC time>-`, or `--topic-prefix`), so a run owns
   its topics and refuses to start if any already exist;
2. creates those topics (`--partitions`, default 4) and publishes every node
   and relationship TSV of the subset in parallel (`--publish-workers`), largest
   files first;
3. runs `graph-loader start --mode bulk` with planned relationships (the bulk
   default: one global plan, node-disjoint concurrent writes, zero lock waits);
4. prints the time of each phase and keeps the prefixed schema, the CLI log and
   a `summary.json` under `--run-dir` (default `runs/`).

Topics are kept unless you pass `--delete-topics`; `python
examples/imdb/load_imdb.py --cleanup runs/imdb-load-<time>` later deletes
exactly the topics and consumer groups that run recorded. Options the script
does not define are passed to `graph-loader start` (for example
`--edge-classic`, or `--run-id`). Node upserts and relationship `MERGE` make a
repeated load into the same database safe.

**Kafka and Neo4j on another machine.** The loader containers resolve addresses
from inside Docker. When they differ from the host's, pass
`--loader-neo4j-uri` and `--loader-kafka-bootstrap`, and pick their Docker
network with `--network` (default `graph-loader-net`, the project's Compose
network). Kafka must advertise an address reachable from both the host process
and the containers; advertising `localhost` from a remote machine redirects
clients back to their own machine.

**Measured** (one run, 2026-11-01, not a guarantee): `titles-165000-depth-4`,
from a Mac publishing to Kafka 7.7.0 and Neo4j 5.26 Enterprise on a Linux host
with NVMe storage, 8 partitions per topic. Publishing 74.9M records took 141 s;
the load took 803 s and created 14,483,774 nodes and 70,210,506 relationships,
with every label and type matching the subset manifest and no duplicate
relationships. Neo4j's CPU was the bottleneck in the relationship phase. The
relationship count is lower than the input records because repeated records for
the same pair `MERGE` into one relationship.

### Variant: a backfill topic for one relationship type (not the default)

A relationship type can be fed by more than one topic. For example, a late backfill of genre assignments can
arrive on a second topic that must override the first. Declare the type with `topics:` and a conflict rule in a
copy of the schema (the default `graph_schema.yaml` is unchanged):

```yaml
  - type: HAS_GENRE
    topics: [imdb-has-genre, imdb-has-genre-backfill]
    topic_conflict: precedence          # the backfill topic (listed last) wins, property by property
    nodes: {source: Title, target: Genre}
    source_key_property: tconst
    target_key_property: name
    properties: {title_id: {type: string, required: true}}
```

`precedence` runs in bulk modes only (planned, the default, and `--edge-classic`); it is refused with
`--mode stream`. The other choices are `order_by: <property>` (newest value wins, every mode) and
`topic_conflict: arrival` (nondeterministic). `load_imdb.py` publishes one file per type, so the backfill topic
is yours to fill with the same record shape, and **it must exist before the load starts, even if it is empty**: a
declared topic that is missing makes the bulk boundary capture fail. See
[`../multi_topic/README.md`](../multi_topic/README.md) for a complete example.

The example retains IMDb's `primary_name` and also emits `name` for people.
That alias is compatible with the local project's existing non-null
`:Person.name` constraint.

See [model.md](model.md) for the source-to-graph mapping.

### Spawn memory and embedding notes

Spawn serializes the selected ID sets and converter configuration into each
worker. Budget a separate copy per active process plus serialization buffers,
including the selected people result returned to the parent. Use `--jobs 1`
when those copies exceed available memory. Programmatic parallel selectors and
converters must be importable and picklable; local closures remain usable with
`jobs=1`. The caller-selected process method of an embedding application is
preserved. The historical fork timing/RSS figures above do not measure spawn's
copy/serialization costs. Tiny local TSV tests validate the spawn change; no
full IMDb conversion or new production performance/memory measurement is claimed.
