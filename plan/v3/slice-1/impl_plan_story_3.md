# Implementation Plan — Phase 3 / Slice 1 / Story 3

## Story

**Consume Relationship Topics With Durable Offsets**

## Scope and non-goals

This story makes the sequential relationship writer operable as a direct Kafka
loader. It does not add relationship containers to the fleet/orchestrator,
parallel lanes, control-topic acknowledgements, retry/DLQ delivery, or
relationship scheduling; those belong to later slices/Phase 5.

## Files

| Action | Path | Purpose |
| --- | --- | --- |
| Modify | `src/loader/node_loader.py` | Extend the existing fsync-backed `RejectionSink` compatibly so it can record either `node_label` or `edge_type`. |
| Modify | `src/loader/edge_loader.py` | Add `PendingEdgeRecord`, `EdgeLoader`, CLI parser/entrypoint, parsing and offset-safety mechanics. |
| Modify | `tests/test_node_loader.py` | Prove the existing node rejection JSON remains backward compatible. |
| Modify | `tests/test_edge_loader.py` | Prove edge batching, durable rejection, commitment ordering/failures, rebalance safety, and CLI cleanup. |
| Modify | `README.md` | Document the direct relationship-loader command and canonical event envelope. |

## Shared rejection record

`RejectionSink.append()` will accept one explicit entity discriminator:

```python
def append(
    self, *, topic: str, partition: int, offset: int, reason: str,
    node_label: str | None = None, edge_type: str | None = None,
) -> None: ...
```

Exactly one is required. The fsync operation is unchanged. Existing node calls
continue to emit `node_label` with their current JSON shape; edge calls emit
`edge_type`, enabling a future Phase 5 DLQ router to identify record class.

## EdgeLoader API and mechanics

```python
@dataclass(frozen=True)
class PendingEdgeRecord:
    record: EdgeRecord
    topic: str
    partition: int
    offset: int

class EdgeLoader:
    def __init__(
        self, consumer: Consumer, writer: EdgeWriter, edge_config: EdgeConfig,
        topic: str | None = None, event_logger: logging.Logger = logger,
        loading_config: LoadingConfig | None = None,
        rejection_sink: RejectionSink | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None: ...
    def run(self, *, max_messages: int | None = None) -> int: ...
    def close(self) -> None: ...
```

The implementation will adapt the proven NodeLoader runtime mechanics rather
than a simplistic `process_message` loop:

1. Subscribe to exactly the selected edge topic with `on_assign`/`on_revoke`.
   Track assigned partitions. A revoke with pending/unresolved work sets a
   fail-closed rebalance failure; never commit an unowned partition.
2. `poll` a message, decode strict JSON (reject `NaN`/`Infinity`), require a
   top-level object, and normalize it through `normalize_edge_record`.
3. For a valid record, add `PendingEdgeRecord` to an in-memory batch and keep
   a `PartitionLedger(next_offset, resolved_offsets)` per topic/partition.
4. On configured batch size, idle timer, message cap, or clean shutdown:
   pause assignments, call `EdgeWriter.write_batch`, poll callbacks/revalidate
   ownership, mark written offsets resolved, synchronously commit each
   partition's longest contiguous resolved prefix, then resume assignments.
5. For malformed JSON/envelope only: append/fsync rejection metadata with
   `edge_type`, then mark the exact offset resolved and contiguously commit it.
   If rejection logging or commit fails, stop without advancing it.
6. `MissingRelationshipEndpointError`, Neo4j errors, Kafka broker errors, and
   all write failures never enter the resolved ledger and return a non-zero
   outcome. Thus their offset remains uncommitted for retry on restart.

The loader will **not** use a Kafka producer or publish control events: it is
a direct, single-loader CLI for Slice 1, and fleet lifecycle control is
intentionally deferred to Slice 4.

## CLI and cleanup

Add `build_parser()` and `main(argv=None)` in `edge_loader.py` with:

```text
--config config/graph_schema.yaml
--edge-type WORKS_AT        (required)
--topic optional override
--mode bulk|stream
--replica-id ID             (accepted for operational parity; unused in this direct loader)
--max-messages POSITIVE_INT
```

`main` loads the schema, selects exactly one edge by type, creates consumer
with `enable.auto.commit=False`, creates the Neo4j driver using the established
credential helper, runs the loader, and closes consumer/driver in `finally`.
It rejects unknown edge types or non-positive caps before resources are leaked.
`--mode` is parsed for consistency and documented, but scheduling behavior is
owned by later fleet work.

README will show a command such as:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
python -m src.loader.edge_loader --edge-type WORKS_AT --max-messages 1
```

and the canonical `source` / `target` / `properties` envelope.

## Tests

- `enable.auto.commit=False`, selected topic only, `EdgeWriter.write_batch`
  precedes a synchronous `TopicPartition(offset + 1)` commit;
- two partitions and non-zero offsets commit independently; a gap blocks a
  later offset until safely resolved;
- malformed JSON, non-object JSON, and invalid envelope append/fsync a JSONL
  rejection with `edge_type` before any commit; sink failure leaves it
  uncommitted;
- missing source/target and generic writer/Neo4j failure issue no commit and
  stop before a later message can be committed;
- batch-size, idle, and max-message final flushing; self-referencing `KNOWS`
  works through the same path;
- revoke/ownership safety and consumer close;
- CLI unknown edge type, invalid cap, Kafka configuration, and cleanup paths;
- existing node rejection sink JSON remains unchanged.

Run:

```bash
.venv/bin/python -m pytest tests/test_edge_loader.py tests/test_node_loader.py -v
```

## Rollback / cleanup

No offset is committed until Neo4j reports a durable write or malformed input
is on disk and fsync'd. A failed process naturally restarts from any unresolved
Kafka position. `main` closes both client objects in `finally`; no containers,
topics, or graph data are created by this story.
