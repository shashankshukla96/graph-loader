# Plan — Slice 3 Story 1: Execution Modes

Modify `schema.py` with `EdgeExecutionConfig(mode: Literal["python_apoc",
"native_disjoint"]="python_apoc", worker_count=Field(1, ge=1, le=64))` and
`EdgeConfig.execution: EdgeExecutionConfig = Field(default_factory=...)`; reject
boolean worker counts and update canonical YAML/default tests. Worker count is the
bounded Python executor size in Story4 and native server concurrency in Story3.
Add `src/loader/edge_execution.py`: `ExecutionModeError`,
`ServerCapabilities(neo4j_version, cypher_25)`, `probe_server_capabilities(driver)`,
and `require_execution_mode`. The probe opens/closes a session, executes and
consumes `CYPHER 25 RETURN 1 AS supported`, maps Neo4j client syntax/unsupported
errors to `cypher_25=False`, but propagates connectivity/auth failures; version is
diagnostic only. Later integration calls guard before EdgeLoader polls, so native
refusal is fail-closed/no commits. Test default/invalid config, probe success,
unsupported, propagated errors/session closing, and guard output containing
mode/version. No writes/threads/Kafka in this story.
