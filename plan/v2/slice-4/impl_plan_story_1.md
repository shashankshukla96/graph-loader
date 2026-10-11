# Implementation Plan: Story 1 - Loader Lifecycle Acknowledgements and Graceful Drain

## Files to Modify
- `src/loader/node_loader.py`
- `tests/test_node_loader.py`

## Plan

### `src/loader/node_loader.py`
1. **CLI Argument Updates:**
   - Update `build_parser()` to accept `--run-id` and `--replica-id`.
2. **Signal Handling in `main()`:**
   - Create a shared `threading.Event` for shutdown. Register a `SIGTERM` handler only after argument validation; it sets that event and never raises from inside a Neo4j write or Kafka offset commit.
   - Pass the event to `NodeLoader`. The consumer loop checks it only between message processing iterations, so an in-flight write and synchronous commit complete before the loader publishes `DRAIN_COMPLETE` and exits `0`.
   - Restore the prior SIGTERM handler from an enclosing `finally`, including when startup fails after registration. Parser/validation failures occur before registration.
3. **Dependency Injection:**
   - In `main()`, instantiate a `Producer` connected to `KAFKA_BOOTSTRAP_SERVERS`.
   - Pass the `Producer`, `run_id`, and `replica_id` into the `NodeLoader` constructor.
4. **`NodeLoader` Class Updates:**
   - Update `__init__` to accept `producer`, `run_id`, and `replica_id`.
   - Add a private method `_publish_control(message_type: str, data: dict, flush: bool = True)`:
     - Constructs JSON payload with `run_id`, `node_label`, `replica_id`, `type=message_type`, `assignment_epoch`, and merges `data`.
     - `self._producer.produce("__graph_loader_control", key=self._run_id, value=json.dumps(payload))`
     - Require an injected producer whenever a control event is published; raise a dedicated control-delivery error otherwise.
     - If `flush` is True, call `self._producer.flush()` and raise a dedicated control-delivery error when its returned undelivered-message count is non-zero. Capture delivery callback errors and propagate `produce()`/`flush()` errors so a control acknowledgement is never treated as durable when it is not.
   - Add `self._assignment_epoch = 0`.
   - Implement `_on_assign(consumer, partitions)` callback:
     - Call `consumer.assign(partitions)` so the callback preserves Confluent Kafka's default assignment behavior.
     - `self._assignment_epoch += 1`
     - Extract fully-qualified `{"topic": partition.topic, "partition": partition.partition}` assignment entries, using the actual overridden topic.
     - We CANNOT block with `flush()` inside `on_assign`. Queue an immutable assignment event `(epoch, assignments)` and handle each queued event in FIFO order in the main `run()` loop so rapid rebalances cannot overwrite a prior epoch.
   - Update `self._consumer.subscribe()` to pass `on_assign=self._on_assign`.
5. **Main `run()` Loop Updates:**
   - At the start of every loop iteration, drain queued assignment events and then check the shutdown event before the next `poll()`. This lets both busy and idle loaders acknowledge termination without interrupting an in-flight write/commit.
   - Publish `ASSIGNMENT` with topic-qualified assignments (or `IDLE_SURPLUS` when the assignment is empty), synchronously flush, and only remove the event after successful delivery.
   - When the shutdown event is set, *note that synchronous Slice 1 writes have no pending batch to flush*, publish `DRAIN_COMPLETE` only after the prior message's write/commit has resolved, then return `0`.

### `tests/test_node_loader.py`
1. **Unit Tests:**
   - Test `_publish_control` correctly formats the payload and flushes the `Producer`.
   - Test `_on_assign` increments the epoch and flags the pending publish, which is then published in `run()`.
   - Test `on_assign` invokes `consumer.assign()` and preserves topic-qualified partition entries.
   - Test `produce()` errors and a non-zero `flush()` return prevent acknowledgement success.
   - Test multiple rapid assignment callbacks publish each epoch in FIFO order.
   - Test a shutdown event raised during a write and during a commit does not interrupt the operation; the committed record is followed by a durably flushed `DRAIN_COMPLETE` and exit `0`.
   - Test an idle loader observes the shutdown event before another poll and publishes `DRAIN_COMPLETE`.
   - Test parser rejection leaves the original SIGTERM handler installed.
   - Test that publishing without a producer and a delivery callback error both fail closed.
   - Update existing mocked tests in `test_node_loader_main_uses_config_and_closes_resources` to mock `Producer` and handle new parser arguments.
