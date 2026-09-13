# Phase 5 (Epic): Real-Time Streaming & Resilience

## Business Objective
Transition the pipeline from a "batch/bulk load" model to a continuous, always-on streaming model that handles data arriving out of order.

## Value Proposition
- **Near Real-Time Graph:** As events hit Kafka, they appear in Neo4j within seconds.
- **Fault Tolerance:** If a node record is delayed and its edge arrives first, the edge is not lost. It is gracefully retried.
- **Self-Healing:** System automatically recovers from transient network or database errors.

## Features (High-Level)
1. **Streaming Mode:** A continuous run command (--mode=stream) where all containers run indefinitely.
2. **Dead Letter Queue (DLQ):** A safety net for records that fail processing (e.g., missing nodes).
3. **Retry Consumer:** A background process that periodically re-attempts edges parked in the DLQ.

## Acceptance Criteria
- [ ] Edge loaders successfully detect when a `MERGE` fails due to a missing source or target node (edge-before-node race condition).
- [ ] Unmatched edges are forwarded to a Dead Letter Queue (DLQ) topic specific to that edge type (e.g., `graph.edges.works_at.dlq`).
- [ ] A retry consumer processes the DLQ, respects the exponential `_retry_after` delay, and drops messages that exceed the maximum retry count.
- [ ] The `--mode stream` command runs edge loaders continuously, seamlessly handling real-time data influx.
