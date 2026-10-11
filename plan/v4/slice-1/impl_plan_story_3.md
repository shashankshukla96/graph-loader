# Implementation Plan — Phase 4 / Slice 1 / Story 3

## Scope

Implement a run-scoped, Kafka-acknowledged clock publisher. It does not launch
containers or gate relationship records; later slices consume its leases.

## Files

- Modify `src/orchestrator/coordination.py`: add `ClockPublishError`,
  `ClockHealthError`, and `GlobalBatchClock`.
- Modify `tests/test_coordination.py`: publisher lifecycle tests.

## API and behavior

```python
class GlobalBatchClock:
    def __init__(self, *, run_id: str, edges: Sequence[EdgeConfig],
                 config: CoordinationConfig, producer: Producer,
                 health_probe: Callable[[], None], wall_clock_ms: Callable[[], int],
                 sleeper: Callable[[float], None]) -> None: ...
    def publish_initial(self) -> ClockLease: ...
    def advance(self) -> ClockLease: ...
    def run(self, shutdown_requested: Event) -> int: ...
```

Reject blank run id, empty/duplicate edge types, and any
`edge.nodes.is_self_referencing` declaration. Derive sorted active type names
from validated EdgeConfig values. The initial lease owns every configured bucket
by the first lexical type. `publish_initial` creates epoch/slot zero exactly
once (later calls return that existing lease);
`advance` creates `(epoch + 1, (slot + 1) % bucket_count)`. Both call exact
health probe first, encode the complete lease, `produce` to config.topic keyed
by run id, and require delivery callback plus `flush() == 0` before storing
the lease. Delivery requires exactly one successful callback and `flush() == 0`;
callback error, missing callback, producer exception, or flush failure raise
`ClockPublishError` and leave state unchanged. Health exceptions are wrapped as
`ClockHealthError`. Before advance, reject `wall_clock_ms() >= current expiry`;
reject noninteger/bool/regressing wall clocks. Lease expiry is
`issued + lease_timeout_ms`.
`CoordinationConfig` requires `lease_timeout_ms > slot_duration_ms`, providing
a renewal margin; `advance()` before `publish_initial()` raises
`ClockPublishError` without producing or changing state.

`run` publishes initial once, sleeps slot duration while not shutdown, and
advances; it returns 1 for clock errors and 0 for graceful shutdown. It never
catches KeyboardInterrupt as success. Producer ownership is caller-owned: the
service never closes it, and callers flush in their own `finally`.

## Tests

- Initial canonical publish, run key/topic, callback/flush acknowledgement,
  and all-bucket deterministic map.
- Monotonic epoch/slot and bounded expiry across advances.
- Probe, produce, flush, callback/missing-callback, expiry deadline, and wall-clock regression failures leave
  current lease unchanged.
- Graceful run shutdown and failure return code; no raw payload in errors.

Run `.venv/bin/python -m pytest tests/test_coordination.py -v`.

## Rollback

Pure mocks only; no worker or Docker lifecycle behavior changes.
