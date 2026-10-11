# Examples

Back to: [README](../README.md) | [Usage guide](../docs/USAGE.md) | [CLI reference](../docs/CLI.md) | [Architecture](../docs/ARCHITECTURE.md) | [Changelog](../CHANGELOG.md)

Each example is an ordinary user of the graph-loader library. It supplies a
YAML schema and publishes records to the topics that schema declares; nothing
in `src/` knows about either model. Use them as templates for your own graph.
The full reference is the [library usage guide](../docs/USAGE.md).
To run a load end to end, see [How to run a bulk (initial) load](../docs/BULK_LOAD.md) and [How to load incrementally](../docs/INCREMENTAL_LOAD.md).

| Example | What it shows | Data |
| --- | --- | --- |
| [`imdb/`](imdb/README.md) | A real, large graph end to end: download, subset, publish, load, timing report. The benchmark for every performance number in this repository. | Official IMDb non-commercial datasets (downloaded by the example) |
| [`social/`](social/README.md) | The library contract on a synthetic model with edge cases (integer, string and date keys, self-referencing edges, several types between the same labels, hub nodes, repeated pairs), plus an exact verification against an independently computed graph | Generated deterministically from a seed |
| [`multi_topic/`](multi_topic/README.md) | Relationship types fed by **two topics each**: `order_by` (newest value wins, every mode) and `topic_conflict: precedence` (later-listed topic wins, bulk only), with an exact verification. Also shows which modes allow which rule, and (variant `graph_schema_multi_pair.yaml`) one type over **two label pairs** (`LIKES` Customer->Post and Customer->Comment, colliding ids) | Generated deterministically from a seed |

## Common setup

```bash
.venv/bin/pip install -r requirements.txt             # library
docker compose up -d neo4j kafka                      # local services (pinned CPUs, see ../docs/USAGE.md)
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092 NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j NEO4J_PASSWORD=...
export LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 LOADER_NEO4J_URI=bolt://neo4j:7687
export NEO4J_DATABASE=mygraph                         # without it the load targets the server default, `neo4j`
docker exec graph-loader-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
  "CREATE DATABASE mygraph IF NOT EXISTS WAIT"
# database names: ASCII letters, digits, dots and dashes only (no underscores)
```

## The pattern every example follows

1. **Schema:** a `graph_schema.yaml` declares node labels (key and property
   types), relationship types (endpoint labels and properties) and one Kafka
   topic per label and type.
2. **Publish:** node records are JSON property maps, keyed by the node key.
   Relationship records use the envelope
   `{"source": {...key...}, "target": {...key...}, "properties": {...}}`,
   keyed by the source key.
3. **Load:** `python -m src.cli start --mode bulk --config graph_schema.yaml`
   (planned mode is the bulk default; `--edge-classic` selects the old
   APOC-locked loaders). The loader derives the schedule, parallelism and lock-free write plan by
   itself.
4. **Verify:** compare the graph with what you expect (the social example
   shows how to check exactly).

To add your own model, copy `social/graph_schema.yaml`, change the labels,
types, keys and topics, and point your producer at those topics. Nothing else
needs to change.
