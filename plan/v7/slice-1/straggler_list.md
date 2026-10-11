# Slice 1 straggler list: single-topic assumptions on relationship types

Generated at S1 by grepping `\.topic\b` in `src/` and `examples/`. After S1,
`EdgeConfig.topic` raises `MultiTopicEdgeError` for multi-topic types, so each
site below fails loudly on a 2-topic type. Node `.topic`, coordination topic and
Kafka message `.topic()` uses are not listed. Line numbers are at commit
4382960 plus S1 (src/models/schema.py is the only file S1 changed in src/).

The grep covered `edge*`, `config` and `cfg` receivers. Sites that read the
topic under other variable names (for example `e.topic` or `spec.topic`) raise
the same error at runtime; S2-S5 should run their 2-topic fixtures to find them.

DLQ sites (S4) should move to `edge_dead_letter_topic_name(edge)`,
`edge_parked_topic_name(edge)` and `edge_parked_pair_index_topic_name(edge)`
from `src.models.schema`, instead of passing `edge.topic` to the entity-topic
helpers.

Raw-dict `topic` reads in `relationship_bulk_monitor.py:260-262`,
`bulk_monitor.py:322` and `edge_plan.py:552` concern boundary and plan payloads,
not schema, and are out of scope for this list.

## S2 (stream and bulk classic/lease, boundary, lag)
* `src/loader/edge_loader.py:273` `self._topic = topic or edge_config.topic` (subscription)
* `src/loader/edge_loader.py:1667` `entity_topic=edge_config.topic` (DLQ producer, S4 too)
* `src/loader/edge_loader.py:1694` `parked_pair_index_topic_name(edge_config.topic, ...)` (S4)
* `src/orchestrator/relationship_bulk_monitor.py:124` `_topic_by_edge_type`
* `src/orchestrator/relationship_bulk_monitor.py:202-205` boundary resolution
* `src/cli.py:519, 616, 955, 1167, 1421` metric/log label `topic=edge.topic`
* `src/cli.py:1617` topic preflight list (`[edge.topic for edge in ...]`)

## S3 (planned mode)
* `src/loader/edge_planner.py:1487` `sorted({edge.topic for edge in edges})`
* `src/loader/edge_planner.py:1509` boundary lookup `boundary[edge.topic]`
* `src/loader/edge_planner.py:1518, 1529, 1534` `KafkaReadTask(edge, edge.topic, ...)`
* `src/loader/edge_planner.py:1638` `per_topic.get(edge.topic, 0)`

## S4 (DLQ, retry consumer, replay)
* `src/cli.py:1404, 1406` DLQ enablement filter
* `src/cli.py:1701` `(edge.topic, dead_letter)` pairs for replay/topic creation
* `src/cli.py:1739, 1742, 1749` DLQ, parked and parked-pairs names
* `src/loader/edge_execution.py:1929` `dead_letter_topic_name(edge_config.topic, ...)`
* `src/cli.py:2385` `source_topic = entity.topic` in replay-dlq for edges
* `src/loader/retry_consumer.py:1447, 1505, 1508, 1536` DLQ, parked, entity_topic, index topic
  (retry_consumer.py, parked_pair_index.py, dead_letter.py are being fixed by another
  pass at S1 time; S4 must rebase on that work first)
