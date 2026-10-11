# Implementation Plan: Story 2 — Boundary Capture and Bulk Monitor Service

## Scope

Implement the run-scoped monitor only. It will consume lifecycle acknowledgements,
capture a stable `(topic, partition) -> end_offset` boundary after coverage is proven,
detect non-quiescent input and crashed containers, and compare committed offsets through
that boundary. CLI launch/drain orchestration remains Story 3.

## Files

### Create

- `src/orchestrator/bulk_monitor.py` — monitor, typed boundary model, and explicit
  monitor failure classes.
- `tests/test_bulk_monitor.py` — fully mocked AdminClient, temporary watermark/control
  consumers, Docker service, and deterministic clock tests.

### Modify

- `src/orchestrator/__init__.py` — export the monitor only if package conventions warrant it.
- `src/orchestrator/docker_service.py` — add deterministic `replica_id` and `run_id` labels
  only when required by Story 3; Story 2's monitor itself receives exact tracked containers.

## Interfaces

```python
Partition = tuple[str, int]

class BulkMonitor:
    def __init__(
        self,
        schema: GraphSchema,
        run_id: str,
        expected_replicas: set[tuple[str, str]],
        tracked_containers: Mapping[tuple[str, str], docker.models.containers.Container],
        *,
        bootstrap_servers: str,
        admin_client: AdminClient | None = None,
        watermark_consumer: Consumer | None = None,
        control_consumer: Consumer | None = None,
        poll_interval_seconds: float = 0.25,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None: ...

    def wait_for_assignment_coverage(self, timeout_seconds: float) -> None: ...
    def capture_boundary(self) -> dict[Partition, int]: ...
    def wait_for_completion(self, timeout_seconds: float) -> None: ...
    def wait_for_drain_complete(self, timeout_seconds: float) -> None: ...
    def verify_zero_lag(self) -> None: ...
    def close(self) -> None: ...
```

Define `BulkMonitorError`, `AssignmentCoverageError`, `BulkTimeoutError`,
`QuiescentViolationError`, `LoaderCrashError`, and `OffsetResolutionError` so the CLI can
report a precise reason and affected node label.

## Design

### 1. Expected schema topology

- Build `group_by_node_label = {node.label: f"{schema.loading.consumer_group_id}-{node.label}"}`.
- Reject a schema where two node labels declare the same Kafka topic. Slice 4 maps each
  qualified partition to exactly one node consumer group, so duplicate node topics are an
  ambiguous configuration and must raise a clear `BulkMonitorError` before monitoring begins.
- Call `AdminClient.list_topics()` and derive every expected topic partition from the
  selected node topics. A missing topic/metadata error is fatal.
- Use `TopicPartition(topic, partition)` for broker and offset requests. Never compare
  bare partition integers across topics.

### 2. Run-scoped control state

- Create a dedicated control consumer with an isolated monitor group id
  `graph-loader-monitor-{run_id}`, `enable.auto.commit=False`, and `auto.offset.reset=earliest`.
- Decode only valid JSON objects whose `run_id` equals the active run. Ignore all older
  runs, unknown replicas, malformed messages, and stale events for a replica whose
  `assignment_epoch` is lower than its latest accepted epoch.
- Track the latest acknowledgement for **every** expected `(node_label, replica_id)`.
  An `ASSIGNMENT` event must carry unique topic-qualified pairs; `IDLE_SURPLUS` proves that
  one expected replica owns nothing but contributes no partition coverage.
- `wait_for_assignment_coverage()` succeeds only when every expected replica has an
  acknowledgement and the union of the latest `ASSIGNMENT.assigned_partitions` equals the
  complete expected partition set exactly once. During an in-flight rebalance, duplicates or
  missing partitions are incomplete transient state: keep consuming until a coherent mapping
  appears or timeout. Unknown replicas, malformed topic/partition values, and impossible
  duplicate pairs inside one `ASSIGNMENT` event fail immediately; timeout is `BulkTimeoutError`.
  Invoke `_check_container_health()` on every coverage-poll iteration so a crashed replica is
  attributed as `LoaderCrashError` rather than surfacing later as an assignment timeout.

### 3. Boundary and quiescence

- `capture_boundary()` is callable only after assignment coverage succeeds. Query each
  expected partition with `watermark_consumer.get_watermark_offsets(TopicPartition(...))`
  and retain the high watermark. `None`, broker errors, or an invalid high watermark raise
  `OffsetResolutionError`.
- On every completion iteration, re-read every high watermark. Any high watermark greater
  than its captured value raises `QuiescentViolationError`. A lower watermark is also a
  failure because the captured input boundary is no longer reliable.

### 4. Durable progress and health

- Import `ConsumerGroupTopicPartitions` and `TopicPartition` from `confluent_kafka` (not
  `confluent_kafka.admin`). For each node group, build
  `request = ConsumerGroupTopicPartitions(group_id, [TopicPartition(topic, partition), ...])`,
  call `admin.list_consumer_group_offsets([request], require_stable=True)`, then unpack
  `result = futures[group_id].result()` and `result.topic_partitions`. The installed 2.15.1 API
  permits one group per request, so issue one call per node group. Treat missing, invalid, or
  negative committed offsets as unresolved — never as zero lag.
- A partition is complete only when its owning node group's committed offset is at least its
  captured high watermark. Boundary progress must be rechecked after every control poll.
  Invoke `_check_container_health()` on every completion-poll iteration too.
- Inspect only `tracked_containers`, keyed by `(node_label, replica_id)` and supplied by Story
  3 for this exact run. Reload each container before examining it; any exited or failed loader
  raises `LoaderCrashError` with its node label and replica id. Story 3 must label containers
  with both `run_id` and `replica_id`; no unscoped Docker list is allowed.
- Because active loaders use Docker's `remove=True`, a `docker.errors.NotFound` from `reload()`
  before Story 3 has intentionally initiated drain is also a `LoaderCrashError`, attributed from
  the tracked mapping's `(node_label, replica_id)`. It must never be reported as an anonymous
  Docker error or mistaken for successful completion.

### 5. Drain and final verification

- `wait_for_drain_complete()` consumes the same filtered control stream until every
  non-idle expected replica has a `DRAIN_COMPLETE` acknowledgement at or after its current
  assignment epoch. It uses the same bounded timeout semantics. During this post-stop phase,
  a tracked container exit or `NotFound` is acceptable only after that exact replica has
  durably acknowledged `DRAIN_COMPLETE`; disappearance before its acknowledgement raises
  attributed `LoaderCrashError` immediately.
- `verify_zero_lag()` re-reads watermarks and committed offsets after drain. It fails on any
  nonzero lag, any changed high watermark, or loss of assignment coverage.
- `close()` always closes monitor-owned Kafka consumers; cleanup errors are logged and do
  not mask the monitor's original failure.

## Tests

- Topic metadata expands a multi-partition, multi-node schema into qualified expected pairs.
- Control records from older run ids and stale epochs are ignored; transient duplicate/missing
  assignments keep polling during rebalance, unknown/malformed assignments fail, and an
  acknowledged idle surplus replica is accepted only alongside full assignment coverage.
- A crash during assignment coverage is attributed immediately; drain accepts container
  disappearance only after that replica's `DRAIN_COMPLETE` acknowledgement and rejects it
  before the acknowledgement.
- Boundary capture records exact high watermarks and rejects unavailable/invalid offsets.
- Completion accepts commits exactly at the boundary and treats missing/invalid offsets as
  incomplete; it rejects late arrivals and watermark regression.
- Docker crash detection attributes the correct run-scoped node replica.
- A removed-on-crash container (`reload()` raises `NotFound`) is attributed from the tracked
  replica key and fails as `LoaderCrashError`.
- Duplicate configured node topics are rejected during monitor topology construction rather
  than producing ambiguous partition-to-group progress checks.
- Offset tests assert the exact `ConsumerGroupTopicPartitions` request-list, future resolution,
  and `result.topic_partitions` unpacking used by confluent-kafka 2.15.1.
- Drain completion requires every non-idle replica and final zero-lag verification detects
  remaining lag or a late arrival.
- Every timeout path uses the injected monotonic clock/sleeper; no live broker or Docker
  daemon is required for unit tests.

## Cleanup

- The monitor owns and closes only the temporary watermark/control consumers it creates.
- It never stops containers; Story 3 owns stopping the exact tracked fleet after monitor
  success or failure.
