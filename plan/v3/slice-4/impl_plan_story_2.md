# Implementation Plan — Phase 3 / Slice 4 / Story 2

## Story

**Give Edge Loaders the Run-Scoped Bulk Lifecycle**

## Scope and non-goals

This story extends only `EdgeLoader` with Phase 2's control protocol for
finite bulk runs. It does not launch Docker containers, monitor offsets,
calculate stages, or change CLI orchestration. Stream-mode direct edge loading
remains free of control-producer side effects.

## Files

| Action | Path | Purpose |
| --- | --- | --- |
| Modify | `src/loader/edge_loader.py` | Optional control producer, assignment/drain protocol, parser options, and signal-safe lifecycle. |
| Modify | `tests/test_edge_loader.py` | Control delivery and no-false-drain safety tests. |

## Constructor and command contract

Extend `EdgeLoader.__init__` with optional keyword dependencies:

```python
producer: Producer | None = None,
run_id: str = "default_run",
replica_id: str = "0",
```

Add `_assignment_epoch` and `_pending_assignment_events`. Add parser
`--run-id` (default `default_run`) while preserving `--replica-id`. In `main`,
create and flush `Producer({"bootstrap.servers": kafka_bootstrap_servers})`
only for `--mode bulk`, then pass it with `run_id` and `replica_id`. Stream
mode retains a `None` producer and therefore makes no control-topic calls.

## Control protocol

Import `CONTROL_TOPIC` from `src.loader.control` and existing
`ControlDeliveryError` from `src.loader.node_loader`; this is safe because the
edge loader already imports `PartitionLedger` and `RejectionSink` from node
code, and node code never imports edge code.

Add `_publish_control(message_type, data, *, assignment_epoch=None)` and
`_publish_pending_assignments()`. A control event is keyed by run id and
contains `run_id`, `edge_type`, `replica_id`, `type`, and `assignment_epoch`.
An `ASSIGNMENT` also carries topic-qualified `assigned_partitions` objects.
The delivery callback and `Producer.flush()` must both succeed. An absent
producer when a control event is requested raises `ControlDeliveryError`, so
a bulk run fails closed rather than proceeding without evidence.

`_on_assign` must assign Kafka ownership, clear pause state, increment the
epoch, and queue its assignment only when producer control is enabled. The
run loop publishes queued epochs after Kafka callbacks. Empty assignment emits
`IDLE_SURPLUS`; nonempty assignment emits `ASSIGNMENT`.

`_on_revoke` must invalidate the previously announced ownership: advance the
epoch, remove the revoked partitions, and queue a current zero/remaining
assignment snapshot for durable publication when control is enabled. Any later
`DRAIN_COMPLETE` uses this newest epoch. This prevents a monitor from treating
an old assignment as current while a rebalance is in progress.

## Drain path

On SIGTERM: flush pending relationship work; process callback-prefetched
messages before declaring drain; reject unresolved offset gaps; only then
synchronously publish `DRAIN_COMPLETE` with the latest epoch and return zero.
Use this same finalization on a finite `max_messages` limit. With no producer,
preserve the old stream return behavior. A failed write, commit, rebalance, or
control publish returns nonzero and must never emit false drain completion.
`run()` catches `ControlDeliveryError`, logs only edge type and replica context,
and returns `1` for both assignment-publish and final-drain-publish failures.

## Tests

Extend `tests/test_edge_loader.py` with mock consumer/producer coverage for:

- assignment payload, idle surplus, epoch advancement, and run/type scoping;
- producer flush/delivery failure raising `ControlDeliveryError`;
- SIGTERM after buffered valid work writes, commits, then sends one drain ack;
- failed write, failed offset commit, or unresolved work emitting no drain ack;
- no control calls from direct stream mode; and
- bulk `main` creating/passing/flushing the producer with `--run-id`.

Also cover revoke epoch invalidation/current snapshot publishing and confirm
both assignment-control failure and final-drain-control failure return `1`
without a false successful completion.

Run:

```bash
.venv/bin/python -m pytest tests/test_edge_loader.py tests/test_node_loader.py -v
```

## Rollback / cleanup

No external state is produced by unit tests. If this fails, remove only the
optional edge lifecycle branches; do not alter Phase 2's node protocol.
