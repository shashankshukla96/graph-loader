# Slice 6: metrics, monitoring, examples, docs, integration test, compatibility

Depends on S2-S5. Files: `src/utils/metrics.py` (docstrings/pre-touch only), `monitoring/**` (dashboards, alerts, alert_tests),
`docs/MONITORING.md`, `docs/USAGE.md`, `examples/social/**`, `examples/imdb/**`, `examples/README.md`,
`tests/integration/test_multi_topic_relationship.py` (new), `tests/test_validate_monitoring.py`, `tests/test_social_example.py`.

## Story 6.1 Metrics cardinality
No new labels. `name=<type>`; lag keeps `topic`. Document the series bound (series per type <= topics of that type; total <= schema topics) and that `precedence` sub-stages do not add labels. `dlq_backlog{kind,name,queue}` unchanged (per-type DLQ).
Acceptance: test that a schema with 3 types x 4 topics yields exactly 12 lag series and no per-record label; `tests/test_metrics.py` label-set assertions unchanged.

## Story 6.2 Dashboards and alerts
"Lag by topic" panel already shows `{{kind}}/{{name}}/{{topic}}`; add a per-type "lag by topic" breakdown row and a "slowest topic of a type" stat (`max by (name) (...)`). Lag alerts already carry `topic`; add nothing new unless a per-topic stall alert is wanted (skip). Add an alert note: a stuck single topic keeps the whole type above zero lag (by design, zero-lag is over all topics). DLQ alerts unchanged (per-type). Update `alert_tests` only if an expression changes; `test_validate_monitoring.py` must stay green.
Acceptance: dashboard JSON validates; alert rule tests pass.

## Story 6.3 Examples
social: add a `VISITED_EVENTS`-style second topic to an existing type, e.g. `LIVES_IN` fed by `social-lives-in` and `social-lives-in-corrections` with `order_by: since` (date), plus a generator path in `social_graph.py` that publishes overlapping pairs; keep the unmodified single-topic types. IMDb: `HAS_GENRE` (or `ACTED_IN`-style) gets a second topic for a backfill file with `topic_conflict: precedence` (bulk/planned demo) in a documented variant schema, NOT the default one, so the default IMDb run is unchanged.
Acceptance: `tests/test_social_example.py` validates the new schema; example READMEs explain the conflict rule and which modes allow it.

## Story 6.4 Docs
USAGE.md: section 3 field table (`topics`, `topic`, `topic_conflict`), "Validation" list, new subsection "Multiple topics per relationship type" (the mode x rule table, recommendation, property-merge semantics, ties), DLQ naming for multi-topic (`<type>.dlq`, rename hazard), replay-dlq `--source-topic`, sections 5/6 (planned topic order), limitations (section 12: stream cannot do precedence; arrival nondeterministic; ties across topics are arrival order), MONITORING.md cardinality note.
Acceptance: every YAML snippet in the new docs loads via `load_schema` in a docs-snippet test (extend `test_dev_environment.py` or add a snippet test).

## Story 6.5 Zero-lock-wait integration test (Docker, nightly marker)
`tests/integration/test_multi_topic_relationship.py`: Neo4j 5.26 + Kafka testcontainers. Type `LIKES` with topics `likes-a`, `likes-b`, `order_by: updated_at`; 20k pairs with 30% overlap, hub node included; overlapping pairs carry newer value in the EARLIER topic for half and in the LATER topic for half. Run (1) stream lease fleet, (2) bulk lease, (3) bulk classic, (4) planned. Assert: final property == max(updated_at) for every pair in all four modes; relationship count == distinct pairs; Neo4j query log (existing `test_lock_wait_helpers`/`test_lock_wait_integration` helpers) shows zero lock-wait time; DLQ empty. Separate precedence case in modes 2-4 (not stream). Add arrival case asserting only count and absence of lock waits (not values).
Acceptance: passes in nightly job; skips cleanly without Docker.

## Story 6.6 Compatibility suite
Acceptance: the whole existing unit suite and all existing examples pass untouched; a schema file using only `topic:` produces identical DLQ names, group ids, plan files and metrics series as before (golden comparison recorded before S1 merges).
