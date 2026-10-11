# Multi-topic example: two topics per relationship type

Back to the [project README](../../README.md).

A small synthetic graph (customers rating and wishlisting products) where **each relationship type is fed
by two Kafka topics**. It shows the two conflict rules that a multi-topic type must choose between, and
proves them with an exact, independently computed verification.

Kafka orders records only inside one partition, so when the same `(source, target)` pair shows up in both
topics of a type, the loader needs a rule. A type with two or more topics must declare one; there is no
silent default. Types with one topic (`topic:`) are unchanged.

| Type | Topics (in order) | Rule | Result for a pair present in both topics |
| --- | --- | --- | --- |
| `RATED` | `mt-rated-app`, `mt-rated-import` | `order_by: rated_at` | The newest `rated_at` wins, whichever topic carried it. A tie keeps the stored value. Deterministic in every mode, **including stream** |
| `WISHLISTED` | `mt-wishlist-base`, `mt-wishlist-corrections` | `topic_conflict: precedence` | The later-listed topic wins, property by property (`SET r += props`): the corrections topic replaces `priority` and, when it carries one, `note`; it keeps the base `note` when it omits it |

A third rule, `topic_conflict: arrival` (whichever record arrives last wins), is allowed in every mode but
nondeterministic and logs a warning at load. It is not used here.

## Which modes allow which rule

| Rule | Planned (bulk default) | `--edge-leases` | `--edge-classic` | `--mode stream` |
| --- | --- | --- | --- | --- |
| `order_by` | yes | yes | yes | yes |
| `precedence` | yes (the plan orders the chunks by topic index) | yes (the topics run as sequential sub-stages) | yes (same) | **refused**, at schema load when `loading.mode: stream` is written and again at `start` |
| `arrival` | yes | yes | yes | yes |

This schema has `loading.mode: bulk` because `WISHLISTED` uses `precedence`. To try stream mode, delete
`WISHLISTED` (or give it `order_by`) and set `mode: stream`.

## What the generator exercises

* About 30% of the pairs of each type are published to **both** topics.
* For `RATED`, the newer `rated_at` sits in the **earlier** topic for half of those pairs and in the **later**
  topic for the other half, so neither "first topic wins" nor "last topic wins" can pass. Timestamps are unique,
  so no tie handling is involved.
* For `WISHLISTED`, some corrections omit `note` (the base value must survive) and some corrections target a
  pair the base topic usually does not have (a random product, so occasionally one it does).
* Within each type the **second topic is published first**: the result must not depend on publish order.

Files:

* [`graph_schema.yaml`](graph_schema.yaml): the model, with the rule of each type explained inline.
* [`multi_topic_graph.py`](multi_topic_graph.py): deterministic generator, reference producer and exact verifier.

## Run it

Set the connection variables from [`../README.md`](../README.md#common-setup), then:

```bash
python examples/multi_topic/multi_topic_graph.py publish --customers 3000
python -m src.cli start --mode bulk --config examples/multi_topic/graph_schema.yaml      # planned mode
python examples/multi_topic/multi_topic_graph.py verify --customers 3000
```

`--customers` scales the dataset (3 ratings and 2 wishlist entries per customer) and `--seed` changes it; use
the same values for `publish` and `verify`. The same graph results from `--edge-leases` and `--edge-classic`.
`reset` deletes the example's topics.

## Things to know when running a multi-topic type

* **Consumer groups.** Non-precedence types use one consumer group for all their topics. A `precedence` type in
  `--edge-leases` or `--edge-classic` runs one sub-stage per topic and so leaves **one consumer group per topic**
  (here two for `WISHLISTED`). Use a **fresh `--run-id`** for every rerun, or delete those groups first; the
  node loaders' groups (`multi-topic-example-<Label>`) must also be deleted before loading the same topics again.
* **Late records.** In a bulk run, records appended to any topic of the type after the run captured its boundary
  fail the stage instead of being silently skipped.
* **Empty partitions.** A partition that has never held a record (`low == high` at the boundary) does not block
  completion, so a small topic next to a busy one is fine.
* **Dead-lettering.** The main schema does not enable `dead_letter`; the dead-letter variant below does. A
  multi-topic type has one DLQ for the whole type (`<type>.dlq`); see [`docs/USAGE.md`](../../docs/USAGE.md)
  section 3, "Dead-letter queues across topics".

## Dead-letter variant (stream, one DLQ per type)

[`graph_schema_dead_letter.yaml`](graph_schema_dead_letter.yaml) runs `RATED` in stream mode with `dead_letter`
enabled. Publishing the relationships **before** the nodes sends every rating of both topics to the type's one
dead-letter queue, `RATED.dlq` (created at `start` with as many partitions as the wider of the two topics), and
the retry consumer writes them once the customers and products arrive:

```bash
python examples/multi_topic/multi_topic_graph.py publish --customers 1000 --only edges   # also creates the topics
python -m src.cli start --mode stream --config examples/multi_topic/graph_schema_dead_letter.yaml &
python examples/multi_topic/multi_topic_graph.py publish --customers 1000 --only nodes
python examples/multi_topic/multi_topic_graph.py verify --customers 1000 --types RATED  # repeat until exact_match
```

Every envelope in `RATED.dlq` keeps its `original_topic`, so a replay can pick one topic and republishes each
record to the topic it came from:

```bash
python -m src.cli replay-dlq --config examples/multi_topic/graph_schema_dead_letter.yaml \
  --edge-type RATED --source-topic mt-rated-import --dry-run           # count one topic's records
python -m src.cli replay-dlq --config examples/multi_topic/graph_schema_dead_letter.yaml \
  --edge-type RATED --parked --source-topic mt-rated-app               # parked rows of one topic
```

`matched` counts distinct records (the retry consumer re-publishes a record once per attempt; a replay sends each
record once, from its first copy). `order_by` protects the newer `rated_at` whatever a replay sends.

Two things to watch while it runs:

* **Retry backoff and DLQ size.** Until the nodes arrive, the earliest outstanding rating of each pair is retried
  with backoff (1 s doubling to 10 s here) and re-published to `RATED.dlq` per attempt; later ratings of the same
  pair wait in the retry consumer. Sizing rule: [`docs/USAGE.md`](../../docs/USAGE.md), "DLQ sizing".
* **Parked-pair index admission** (types without `order_by`, for example `topic_conflict: arrival`). After every
  partition assignment a live edge loader reads nothing from its new partitions until its copy of the type's
  parked-pair index (`<type>.dlq.parked-pairs`) has caught up. It normally takes a fraction of a second. While it
  waits it logs `reason=parked_pair_index_lagging` every 30 s after 10 s, and
  `graph_loader_parked_pair_index_wait_seconds{kind="edge",name=<type>}` shows the seconds held (0 once
  admitted). A value that keeps growing means the index topic cannot be read: the partitions stay paused rather
  than risk writing a later row of a parked pair first.

## Variant: one relationship type over several label pairs

[`graph_schema_multi_pair.yaml`](graph_schema_multi_pair.yaml) has ONE relationship type, `LIKES`, that connects
two label pairs. Each topic binds its own pair ([`docs/USAGE.md`](../../docs/USAGE.md), "Several label pairs per type"):

| Topic | Pair | Notes |
| --- | --- | --- |
| `mt-likes-posts` | Customer -> Post | `order_by: liked_at` decides between the two Post topics |
| `mt-likes-posts-import` | Customer -> Post | 30% of its pairs are also in `mt-likes-posts`, the newer value in either |
| `mt-likes-comments` | Customer -> Comment | Comment ids equal Post ids; half of the customers like Post `i` and Comment `i` |

Customer 0 likes every post and every comment, a hub over both pairs. The records carry only keys and
properties (`{"target": {"comment_id": 7}}` on `mt-likes-comments`): the pair comes from the topic, never from the
record, so Comment 7 and Post 7 can never be confused.

```bash
python examples/multi_topic/multi_topic_graph.py publish --variant multi-pair --customers 3000
python -m src.cli start --mode bulk --config examples/multi_topic/graph_schema_multi_pair.yaml
python examples/multi_topic/multi_topic_graph.py verify --variant multi-pair --customers 3000
python examples/multi_topic/multi_topic_graph.py reset --variant multi-pair   # delete the variant's topics
```

`verify` compares the graph per labelled pair (Customer->Post and Customer->Comment separately) and reports any
`LIKES` relationship between other labels as `cross_pair`. Every mode supports the type: planned (the bulk
default; the plan file lists `types.LIKES.pairs`), `--edge-leases`, `--edge-classic` and `--mode stream` (the lease
fleet; `--stream-clock-rotation` refuses it). Metrics stay `{kind, name="LIKES"}`, with one lag series per topic.

## Validated results

Local stack, 3,000 customers (20,106 records: 11,695 `RATED` over 9,000 distinct pairs with 2,695 in both
topics; 8,411 `WISHLISTED` over 6,636 pairs with 1,775 in both topics):

| Mode | Graph | Neo4j query log |
| --- | --- | --- |
| planned (`edge_mode=planned reason=default`) | `exact_match: true` | not measured for this run (zero lock waits by construction) |
| `--edge-leases` | `exact_match: true` | 337 entries in the run window, all `waiting: 0` |

`tests/test_multi_topic_example.py` covers the schema, the generator, the expected graph and a planner replay of
the precedence type without Kafka or Neo4j. `tests/integration/test_multi_topic_relationship.py` runs the rules
in every mode against real Neo4j and Kafka and asserts zero lock waits.
