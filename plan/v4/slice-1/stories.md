# Phase 4 / Slice 1: Establish a Run-Scoped Global Batch Clock

## Slice Summary

This slice establishes a durable coordination contract, not loader admission or
fleet scheduling. It gives Phase 4 loaders a single run-scoped, time-bounded
view of the current slot and makes a clock failure explicit. Existing Phase 3
offset, locking, and relationship lifecycle behavior remains unchanged.

## Story 1: Declare Safe Global Clock Configuration

**As a** Pipeline Operator,
**I want** validated clock settings in the graph schema,
**So that** slot coordination has bounded and reproducible behavior.

### Technical Context

- Modify `src/models/schema.py` with a `CoordinationConfig` nested under
  `LoadingConfig`: `topic` default `graph.loader.coordination`, `bucket_count`
  (1–4096), `slot_duration_ms` (positive, bounded), and
  `lease_timeout_ms` (positive and at least `slot_duration_ms`). Reject bools
  where integers are required and blank topics.
- Update `config/graph_schema.yaml` with documented defaults. The default
  bucket count must be compatible with Phase 3 `mix_and_batch.lane_count`, but
  this story does not yet gate records.
- Extend `tests/test_schema_validation.py` for defaults, numeric bounds,
  boolean rejection, blank topic, and lease/slot ordering.

### Acceptance Criteria

- [ ] Valid schemas expose a complete immutable clock configuration.
- [ ] Unsafe duration, bucket, or topic values fail schema loading clearly.
- [ ] Existing configurations without a coordination section remain valid.

### Definition of Done

- [ ] Focused schema tests pass.
- [ ] Defaults are documented in canonical YAML.
- [ ] Reviewed and merged.

### Dependencies

Depends on: Phase 3 Slice 4. Blocks: Stories 2–3. **Estimate: 5 points.**

## Story 2: Define and Validate the Clock Lease Protocol

**As a** Relationship Loader,
**I want** a validated clock-slot message with run, epoch, ownership, and
expiry information,
**So that** I can reject stale, foreign, or unsafe coordination state.

### Technical Context

- Create `src/orchestrator/coordination.py` with `COORDINATION_TOPIC` support,
  frozen `ClockLease`, `ClockProtocolError`, and:

  ```python
  def encode_clock_lease(lease: ClockLease) -> bytes: ...
  def decode_clock_lease(payload: bytes, *, expected_run_id: str,
                         now_ms: int) -> ClockLease | None: ...
  ```

- The JSON payload contains `run_id`, `epoch`, `slot_id`, `issued_at_ms`,
  `expires_at_ms`, `active_edge_types`, and `bucket_owners`. `bucket_owners`
  maps every integer bucket in `[0, bucket_count)` to one active edge type;
  canonical sorted encoding is required so test comparisons and audit events
  are deterministic.
- Foreign run messages return `None`; malformed, duplicate/missing bucket,
  invalid identifier, non-monotonic time, expired lease, or invalid ownership
  raises `ClockProtocolError`. This story does not decide rotation policy.
- Create `tests/test_coordination.py` for round-trip/canonical payloads,
  foreign-run filtering, expiry, and every validation branch.

### Acceptance Criteria

- [ ] A valid lease identifies exactly one current run/epoch/slot.
- [ ] Expired or malformed ownership is never accepted as current state.
- [ ] Protocol parsing never exposes raw Kafka payload values in errors.

### Definition of Done

- [ ] Protocol unit tests pass.
- [ ] Public types and errors are documented.
- [ ] Reviewed and merged.

### Dependencies

Depends on: Story 1. Blocks: Story 3 and Phase 4 Slice 2. **Estimate: 8 points.**

## Story 3: Publish a Durable, Bounded Clock Stream

**As a** Pipeline Operator,
**I want** a service that publishes acknowledged monotonically advancing slots
and stops when its fleet is unhealthy,
**So that** loaders never act on an unproven or stalled clock.

### Technical Context

- Add `GlobalBatchClock` to `src/orchestrator/coordination.py`:

  ```python
  class GlobalBatchClock:
      def publish_initial(self) -> ClockLease: ...
      def advance(self) -> ClockLease: ...
      def run(self, shutdown_requested: Event) -> int: ...
  ```

- Constructor accepts run id, active non-self-referencing edge types,
  `CoordinationConfig`, Kafka `Producer`, injectable monotonic/wall clocks,
  sleeper, and an injectable Docker health probe. It starts at epoch/slot zero,
  issues a lease bounded by `lease_timeout_ms`, and increments epoch and slot
  modulo `bucket_count` only after `produce` delivery and `flush` succeed.
- The initial ownership may assign all buckets to the lexically first active
  type; fairness/rotation is intentionally Story/Slice 3 work. Empty or
  self-referencing-only active sets are rejected here.
- On producer callback/flush error, expired advance deadline, or unhealthy
  tracked clock container, raise/return a named clock failure without advancing
  local epoch. Docker health uses exact tracked objects, mirroring Phase 3
  monitors—not broad container discovery.
- Extend `tests/test_coordination.py` with fake clocks/producers/probes proving
  initial publish, monotonic rotation, no advance after delivery failure, lease
  timing, health failure attribution, graceful shutdown, and producer close.

### Acceptance Criteria

- [ ] Initial and subsequent slots are Kafka-acknowledged before exposure.
- [ ] Epochs never advance after a failed publish or failed health probe.
- [ ] Every lease has a finite expiry and deterministic bucket map.
- [ ] The service exits cleanly when asked to stop.

### Definition of Done

- [ ] Focused clock tests pass.
- [ ] No Docker/Kafka resource is leaked on construction or failure.
- [ ] Operator-facing logs include run and epoch without payload data.
- [ ] Reviewed and merged.

### Dependencies

Depends on: Story 2. Blocks: Phase 4 Slices 2–4. **Estimate: 8 points.**

## Story Map

```mermaid
flowchart LR
  S1["1. Clock configuration"] --> S2["2. Lease protocol"] --> S3["3. Durable clock service"]
```

## Total Estimate

**21 points** — one sprint. First Mate should begin with Story 1.
