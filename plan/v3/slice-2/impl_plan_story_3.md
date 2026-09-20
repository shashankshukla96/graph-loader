# Implementation Plan — Slice 2 / Story 3

Add pure `LaneBatcher(edge_config, *, clock: Callable[[], float])` in
`src/loader/mix_and_batch.py`, with `add(routed) -> None`, `drain_full()`,
`drain_expired()`, and `drain_all()` returning
`tuple[tuple[RoutedEdgeRecord, ...], ...]`, lanes ascending and records FIFO.
`drain_full` emits every exact batch-size prefix and preserves partial tails.
Expiry is measured from each lane's oldest record and resets only once empty;
it uses that lane's forward/reverse slot duration and emits one FIFO batch.
`drain_all` emits each remaining lane once. Reject different directions in a
nonempty same lane. No consumer, writer, Neo4j, thread, retry, or offset commit
is imported or invoked. Test multiple full batches/tails, exact expiry threshold,
deterministic lane order, and distinct provenance exactly once across drains.
