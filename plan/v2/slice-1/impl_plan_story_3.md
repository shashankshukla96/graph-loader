# Implementation Plan — Story 3: Run a Direct Kafka-to-Neo4j Node Loader

## Scope and Boundaries

Implement only Story 3. Add a direct module command that selects one schema node label, subscribes only to its topic, writes messages synchronously through `NodeWriter`, and commits only after a successful write. To prevent offset loss before Slice 2's ordered disposition logic, any malformed, Kafka-error, or write-failed message logs context and stops the run non-zero before another message can commit. Do not add batching, retries, DLQ publishing, rebalance callbacks, Docker SDK orchestration, or changes to fleet behavior.

## Files

### Modify

- `src/loader/node_loader.py` — add `NodeLoader`, CLI parser/main, JSON decoding, config selection, and direct dependency factories.
- `src/cli.py` — centralize Neo4j username lookup to prefer `NEO4J_USERNAME` and fall back to legacy `NEO4J_USER`.
- `tests/test_node_loader.py` — unit tests for Kafka configuration, ordering, fail-closed behavior, CLI arguments, and cleanup.
- `tests/test_cli.py` — credential-precedence coverage for the existing CLI helper.
- `tests/conftest.py` — add explicitly requested Kafka Testcontainers fixtures, UUID topic creation, and bounded deletion.
- `tests/test_node_loader_integration.py` — add marked Kafka+Neo4j end-to-end tests using temporary YAML and a unique group id.
- `README.md` — document direct loader prerequisites, `NEO4J_USERNAME` (with fallback), Kafka variables, command, and intentional limitations.

## Design

### 1. Credential precedence

Add a small `get_neo4j_credentials()` helper in `src/cli.py`, returning URI, username, password with `NEO4J_USERNAME` preferred and `NEO4J_USER` as compatibility fallback. Refactor `handle_start()` to call it. `node_loader.py` imports this helper, avoiding a second credential contract.

### 2. Fail-closed loader

Add to `src/loader/node_loader.py`:

```python
class NodeLoader:
    """Consume one configured node topic and synchronously write records."""

    def __init__(self, consumer: Consumer, writer: NodeWriter,
                 node_config: NodeConfig, logger: logging.Logger) -> None: ...
    def process_message(self, message: Message) -> bool: ...
    def run(self, *, max_messages: int | None = None) -> int: ...
    def close(self) -> None: ...

def build_parser() -> argparse.ArgumentParser: ...
def main(argv: Sequence[str] | None = None) -> int: ...
```

- Kafka config: `bootstrap.servers` from `KAFKA_BOOTSTRAP_SERVERS` defaulting to `localhost:9092`; `group.id` from `schema.loading.consumer_group_id`; `enable.auto.commit=False`; `auto.offset.reset="earliest"`; `max.poll.interval.ms=schema.loading.max_poll_interval_ms`; and `session.timeout.ms=schema.loading.session_timeout_ms`.
- `NodeLoader.__init__()` owns `consumer.subscribe([node_config.topic])`. `main()` constructs and stores the consumer, then the driver and `NodeWriter`, then constructs `NodeLoader`; its `finally` closes every constructed resource if subscription/initialization fails or `run()` ends. `main()` calls `get_neo4j_driver(*get_neo4j_credentials())`, builds `NodeWriter`, and calls `run()` only after subscription succeeds.
- `process_message()` handles a `Message.error()` as a logged failure. For normal messages, decode UTF-8 and `json.loads(..., parse_constant=_reject_json_constant)`; require a top-level object; normalize; write; then call `consumer.commit(message=message, asynchronous=False)`. Return `True` only after commit.
- Any decode/validation/write/commit/Kafka error logs node label, topic, partition, offset, and safe reason, returns `False`, and `run()` immediately returns `1`. It must not poll again or commit a later message. JSON lists, scalars, and `null` are rejected as non-object payloads through the same fail-closed path. A successful capped run returns `0`; keyboard interrupt logs and returns non-zero without committing a new message.
- `--max-messages` is a positive integer test/development cap; parser rejects zero/negative values. No cap means continuous consumption.

### 3. Integration fixtures and temporary schema

Extend `tests/conftest.py` with a session Kafka Testcontainers fixture plus function fixtures that create UUID-suffixed one-partition topics and delete them with bounded polling. Integration tests explicitly request those fixtures and remain `@pytest.mark.integration`.

For every Kafka integration test, write a temporary YAML copied from the canonical schema but with the selected node topic replaced by its UUID topic and `loading.consumer_group_id` set to a UUID-suffixed group. Invoke `main()` with that temporary schema and a finite `--max-messages`; never use the developer’s Compose topics or consumer group.

## Tests

- Mocked tests prove subscription is exactly `[selected_topic]`, exact consumer configuration (including both `max.poll.interval.ms` and `session.timeout.ms`), `write → synchronous commit` ordering, malformed/nonstandard JSON and decoded list/scalar/null rejection, validation/write/Kafka failures halt before a later message, `--max-messages`, unknown node label, credential precedence, and consumer/driver `finally` cleanup. A subscription failure proves both the already-created consumer and driver close, and the loader never enters `run()`.
- Update `tests/test_cli.py` for username precedence and legacy fallback.
- Marked Testcontainers integration tests produce valid Person and Company messages to isolated topics, invoke the loader per label, verify nodes/replay idempotency, and prove subscriptions use only selected topics.

Run:

```bash
.venv/bin/python -m pytest tests/test_cli.py tests/test_node_loader.py -v
.venv/bin/python -m pytest tests/test_node_loader_integration.py -v -m integration
```

## Rollback and Cleanup

- Kafka fixtures delete only UUID topics created for the current test; driver/consumer close in `finally`.
- If Docker is unavailable, marked integration tests skip with an actionable reason; unit tests remain mandatory.
- If implementation is abandoned, revert only Story 3 changes and retain approved Stories 1–2. Do not reset unrelated worktree changes.
