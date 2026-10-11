# Social/commerce example: the library contract on a non-IMDb model

Back to the [project README](../../README.md).

A synthetic graph that exercises the cases a real model tends to hit. It
proves the loader needs nothing but a YAML schema and records on the declared
topics. Nothing in `src/` knows about this model.

**What it covers**

* **Keys:** integer (`Person.person_id`, including negative values), string
  (`Company`, `Product`, `City`) and date (`Day`).
* **A self-referencing type with self loops:** `FRIEND_OF`, Person → Person.
* **Two types between the same labels:** `LIVES_IN` and `VISITED`, both
  Person → City.
* **Hub-heavy small labels:** about 25 cities and about 60 days receive a
  large share of the relationships.
* **A label without relationships:** `Country`.
* **Repeated `(source, target)` pairs** whose later records overwrite some
  properties and keep others (last write wins per property).
* **Property types:** string, integer, float, date and datetime.

Files:

* [`graph_schema.yaml`](graph_schema.yaml): the whole model definition.
* [`social_graph.py`](social_graph.py): a deterministic generator, a reference
  producer (it publishes per the library contract) and an exact verifier.

## Run it

Set the connection variables from [`../README.md`](../README.md#common-setup), then:

```bash
python examples/social/social_graph.py publish --persons 20000        # create topics, publish nodes + relationships
python -m src.cli start --mode bulk --config examples/social/graph_schema.yaml   # planned mode is the default; builds images on every start unless --skip-image-build
python examples/social/social_graph.py verify --persons 20000         # exact comparison with the expected graph
```

`--persons` scales the dataset, at about 17 relationships per person, and
`--seed` changes it. Use the same values for `publish` and `verify`.

## Subcommands

| Command | What it does |
| --- | --- |
| `publish [--persons N] [--seed S] [--bootstrap HOST:PORT] [--partitions P]` | Creates the `social-*` topics (small labels get one partition, the rest `P`, default 4) and publishes every record. Node records are keyed by the node key and relationship records by the source key. |
| `verify [--persons N] [--seed S] [--neo4j-uri ...] [--neo4j-user ...] [--neo4j-password ...] [--database DB] [--against-database OTHER]` | Recomputes the expected graph from the generator, without any library code, and compares every node, relationship and property with Neo4j. `--against-database` also compares with a second load, for example a classic-mode run. |
| `expect [--persons N] [--seed S]` | Prints the expected graph's counts without touching Neo4j. |
| `reset [--bootstrap HOST:PORT]` | Deletes the example's topics only. Consumer groups survive: also delete `social-example-<Label>`, the bulk run-scoped relationship groups `social-example-<type>-<run id>.bulk.*`, and, after a stream run, the stable groups `social-example-<type>` and `social-example-retry-consumer`. Or change `loading.consumer_group_id` before loading the same topics again. |

## Comparing modes

Load the same topics twice into different databases and compare the two graphs:

```bash
# create both databases first (names allow only letters, digits, dots and dashes):
# docker exec graph-loader-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "CREATE DATABASE socialplanned IF NOT EXISTS WAIT"
# (and the same for socialclassic)
NEO4J_DATABASE=socialplanned python -m src.cli start --mode bulk \
  --config examples/social/graph_schema.yaml      # default = planned (same as --edge-plan)
# node loaders commit offsets under social-example-<Label>: delete those groups
# (or change loading.consumer_group_id) before loading the same topics again
NEO4J_DATABASE=socialclassic python -m src.cli start --mode bulk \
  --config examples/social/graph_schema.yaml --edge-classic --bulk-edge-overlap shared
python examples/social/social_graph.py verify --persons N --database socialplanned --against-database socialclassic
```

Pass `verify` the same `--persons` (and `--seed`) you gave `publish`. It
defaults to 20,000, so after a smaller publish it reports `exact_match: false`.

Every `start` above rebuilds the node and edge images and retags `:latest`.
Add `--skip-image-build` when the images are already built.

`tests/test_social_planned_integration.py` automates the checks: planned
equals expected equals classic, and an invalid record fails the planned run
with its topic, partition and offset. It is marked `integration` and runs when
the `GRAPH_LOADER_IT_*` variables point at a scratch Kafka and Neo4j.

## Streaming it

The same schema runs in stream mode:

```bash
python -m src.cli start --mode stream --config examples/social/graph_schema.yaml   # runs until SIGINT or SIGTERM
```

* Stream consumer groups are stable: `social-example-<type>` for each
  relationship type, independent of `--run-id`, so a restart resumes them. A
  bulk run's node groups `social-example-<Label>` are shared with stream, so a
  stream started after a bulk run continues from the node offsets the bulk run
  committed. Bulk relationship offsets are not carried over. Bulk commits them
  under run-scoped groups (`social-example-<type>-<run id>.bulk.<n>.<hash>`),
  so a stream started after a bulk run re-reads every relationship topic from
  the start. That costs time but changes nothing (`MERGE`). Those run-scoped
  groups also survive `reset`; delete them by hand.
* Stop with SIGINT or SIGTERM (Ctrl-C); the CLI then stops everything it
  launched and exits 0.
* The schema has no `dead_letter` block, so an edge whose endpoint node has not
  been loaded yet stops the fleet. Publish all nodes before the relationship
  records.

## Validated results

* **20,000 persons** (323,252 relationship records, 13,940 repeated pairs
  whose final properties differ from their first record): the planned load
  matched the expected graph exactly, with every property and no duplicates.
  The classic-mode load matched as well, and the two graphs are identical.
* **150,000 persons** (2.43M relationship records): the planned load matched
  exactly. The plan was ready before the first node label finished loading,
  so planning added no wall time.

## Using it as a template

1. Copy `graph_schema.yaml`, then rename the labels, types, key properties and
   topics, and declare your properties with their types.
2. Make your producer emit the same record shapes: the `publish` function in
   `social_graph.py` is about 40 lines and shows the exact keys and JSON.
3. Run the same `start` command with your schema. The loader derives the
   scheduling itself; see the [library usage guide](../../docs/USAGE.md).
