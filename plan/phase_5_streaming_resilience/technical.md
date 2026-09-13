# Phase 5: Technical Details

## Components & Architecture (Python, Kafka, Neo4j)

### 1. Streaming Cypher (Global Locking)
- Streaming needs strict row-level feedback for DLQ. 
```cypher
UNWIND $rows AS row
OPTIONAL MATCH (s:Src {id: row.s_id})
OPTIONAL MATCH (t:Tgt {id: row.t_id})
WITH row, s, t
WHERE s IS NOT NULL AND t IS NOT NULL
WITH collect({s: s, t: t, row: row}) AS rels, collect(s) + collect(t) AS all_nodes
CALL {
    WITH all_nodes
    UNWIND all_nodes AS n
    WITH DISTINCT n AS dist_n
    ORDER BY id(dist_n)
    RETURN collect(dist_n) AS sorted_nodes
}
CALL apoc.lock.nodes(sorted_nodes)
UNWIND rels AS rel
WITH rel.s AS s, rel.t AS t, rel.row AS row
MERGE (s)-[r:REL]->(t)
SET r += row.props
RETURN row.msg_id AS msg_id
```
- Python drops unmatched `msg_id`s into the DLQ.

### 2. Kafka Rebalance Listener (Proper Handoff)
- **The Fix:** If `on_revoke` fires, the Main Thread blocks (with a timeout slightly less than `max.poll.interval.ms`) to let the Neo4j worker thread finish its current `session.run()`. 
- **CRITICAL:** If the worker successfully finishes before the timeout, `on_revoke` MUST explicitly **commit the Kafka offsets** for those partitions before exiting. This guarantees the new partition owner does not double-process the batch. If it times out, offsets are not committed, and the next owner handles the idempotent retry safely.

### 3. DLQ Producer & Retry Logic
- Failed records go to `<topic>.dlq`.
- Python Retry Consumer utilizes `pause()`/`resume()` to honor `_retry_after` timestamps without blocking the `poll()` loop.
