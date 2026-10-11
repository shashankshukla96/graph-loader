# Story 2 implementation plan: lock-wait and deadlock-recovery assertion helpers

File: `tests/stress/lock_wait.py` (new). Unit tests:
`tests/test_lock_wait_helpers.py` (new, no Docker, plus one
`@pytest.mark.integration` test in the same file using its own
function-scoped `Neo4jContainer`, per the story's own note that the
session-scoped `neo4j_driver` fixture must not be mutated).

## Corrections vs. the story doc's sketch (verified empirically, see below)

- Neo4j 5.21-enterprise's query-log defaults are ALREADY
  `db.logs.query.enabled=VERBOSE` and `db.logs.query.threshold=0s`
  (confirmed via `SHOW SETTINGS` against a throwaway `neo4j:5.21-enterprise`
  container: both showed as their own `defaultValue`). The one setting that
  is NOT already this way is `db.logs.query.parameter_logging_enabled`
  (default `true`) which MUST be set `false` for every stress container, or
  every batch's full parameter list (5000 rows at a time) is logged. Docker
  env var mapping confirmed by testing: `NEO4J_db_logs_query_enabled`,
  `NEO4J_db_logs_query_threshold`, and
  `NEO4J_db_logs_query_parameter__logging__enabled` (double underscore
  escapes the literal `_` in `parameter_logging_enabled`, same convention
  `docker-compose.yaml` already uses for `server_memory_heap_initial__size`).
  No "unrecognized setting" warning appeared in `docker logs` with these
  three set, and `SHOW SETTINGS` reflected the requested values exactly.
- The query log's line format is
  `"<n> ms: (planning: X, waiting: Y) - ..."` — confirmed against real
  container output, including a genuinely induced lock wait
  (`... waiting: 2579) ...`). There is **no** `"ms"` token directly after
  the `waiting:` number (only the *outer* elapsed time has one). The
  regex is therefore `r"waiting:\s*(\d+)\)"`, not the story sketch's
  `r"waiting:\s*(\d+)\s*ms"` (which would never match and would make every
  "zero lock wait" assertion vacuously true — exactly the failure mode
  Residual Risk 3 in `stories.md` warns about).
- APOC is not installed by default on a plain `neo4j:5.21-enterprise`
  container (`apoc.lock.nodes`/`apoc.util.sleep` are both "unknown
  procedure/function" until `NEO4J_PLUGINS='["apoc"]'` plus
  `NEO4J_dbms_security_procedures_unrestricted="apoc.*"` and
  `NEO4J_dbms_security_procedures_allowlist="apoc.*"` are set). Confirmed by
  direct docker testing. This only matters for tests that use
  `ApocLockedEdgeWriter(explicit_locks=True)` (the classic/shared-overlap
  default) or call `apoc.util.sleep` directly to manufacture contention;
  traced `build_apoc_locked_upsert_query`: planned mode
  (`explicit_locks=False, sort_rows=False`) and bulk-lease mode
  (`explicit_locks=False, sort_rows=True`) never call any `apoc.*`
  procedure, so Stories 3-5 and 7's planned-mode/lease-mode containers do
  **not** need APOC — only Story 2's own contention AC test and Story 6's
  shared-overlap test do.

## `tests/stress/lock_wait.py` contents

- `with_verbose_query_log(container) -> Neo4jContainer`: applies the three
  `with_env` calls above; must be called before `.start()`.
- `with_apoc(container) -> Neo4jContainer`: applies the three APOC env vars
  above; also before `.start()`. New helper beyond the story's literal
  sketch (justified above) — reused by Story 6.
- `_WAIT_RE = re.compile(r"waiting:\s*(\d+)\)")`.
- `total_lock_wait_ms(container) -> int`: `exec_run(["cat", "/logs/query.log"])`
  via `container.get_wrapped_container()`, decode utf-8 (`errors="replace"`),
  sum every match.
- `any_lock_wait(container) -> int | None`: first nonzero match, else `None`.
- `assert_zero_lock_wait(container) -> None`: raises `AssertionError` with the
  measured total on failure.
- `poll_show_transactions(driver, stop: threading.Event, out: list, interval_s=0.05)`:
  background-thread debugging aid only (documented as not the pass/fail
  signal); appends `{status, activeLockCount, currentQuery}` rows whose
  `status` contains `"Blocked"`.
- `count_retry_log_lines(caplog, edge_type: str | None = None) -> int`:
  counts `caplog.records` whose logger name is
  `"src.loader.edge_execution"` and message starts with
  `"stage=edge-write-retry"`, optionally filtered by `record.args[0] ==
  edge_type` (the retry log call is `logger.warning("stage=edge-write-retry
  edge=%s ...", self._edge_type, ...)`, so `record.args[0]` is the edge
  type argument — confirmed against `edge_execution.py:211-214`).
- `assert_no_unhandled_deadlock(write_call: Callable[[], int]) -> int`: calls
  `write_call()` with no `try/except` around it (so any exception propagates
  completely unmodified — this is the whole point of the helper, not an
  accident of a missing except clause), asserts the return value is a
  non-negative `int` (the attempt count contract of `write_rows`), and
  returns it.
- `try_scrape_metric(url: str, name: str) -> float | None`: bare
  `urllib.request.urlopen(url, timeout=2)` GET; on `URLError`/`OSError`
  return `None`; else parse the Prometheus text-exposition body for every
  line matching `^{name}(\{[^}]*\})?\s+([0-9eE+\-.]+)` and return the sum of
  matched values, or `None` if no line matches. Never imports
  `prometheus_client`.

## Unit tests (no Docker)

- `count_retry_log_lines`: construct a fake logger and use `caplog` to
  capture `logger.warning("stage=edge-write-retry edge=%s records=%s
  attempt=%s/%s delay_s=%.3f code=%s", "KNOWS", 10, 1, 3, 0.1, "Deadlock")`
  lines directly against the real `src.loader.edge_execution` logger name
  (matches the AC's framing "a fake writer's `_sleep`-stubbed retry loop" —
  implemented by constructing a real `ApocLockedEdgeWriter` with a mocked
  `Driver` whose `_write_rows` raises `TransientError` a controlled number
  of times then succeeds, i.e. driving the real retry path, not a
  hand-rolled log line, to also prove the log line's exact shape).
- `try_scrape_metric` returns `None` against an unused local port (no
  server listening) — no exception escapes.
- `assert_no_unhandled_deadlock` re-raises unchanged when `write_call`
  raises (a plain `RuntimeError`), and returns the value when it returns an
  int normally.

## Integration test (real Docker, own container)

- Two threads: one holds a transaction that does
  `MATCH (n:Probe {id:1}) SET n.x = 1` then `CALL apoc.util.sleep(2000)`
  before committing; the other, started ~0.3s later, runs
  `MATCH (n:Probe {id:1}) SET n.y = 1`. Assert
  `total_lock_wait_ms(container) > 0` (proves the helper detects a real
  wait — this is the non-negotiable AC from Residual Risk 3).
- A second, uncontended run on the same container (a plain `MERGE` on a
  fresh node) then `assert_zero_lock_wait(container)` passes.
- Own `Neo4jContainer("neo4j:5.21-enterprise").with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT",
  "yes")`, `with_apoc`, `with_verbose_query_log` applied before `.start()`;
  torn down in a `finally`.
