# Neo4j Graph Loader — development build

This local development distribution includes the migrated runtime modules under
`neo4j_graph_loader`: schema models/loading, planning, loaders, orchestration and
metrics. It must not be uploaded to a package index.

After installation, use `graph-loader --help` or
`python -m neo4j_graph_loader --help`; both support `--version`. Component
commands use the distinctive namespace, for example
`python -m neo4j_graph_loader.loader.node_loader --help`. The old `src.*` imports
and commands are removed without compatibility aliases. The supported public modules are `neo4j_graph_loader.schema` and
`neo4j_graph_loader.planning`; see the source repository
[public API guide](https://github.com/shashankshukla96/graph-loader/blob/main/docs/LIBRARY_API.md)
for source shapes, Kafka requirements, ownership and cancellation limits.
Other module APIs remain internal. `graph-loader schema-example --name social`
prints a bundled sample; `graph-loader validate-schema --config schema.yaml`
validates a caller file without services. Omit `--config` to use the bundled
canonical schema. Neither command nor imports load `.env` files implicitly. Starts keep generated state in a private OS temporary attempt directory;
`--run-dir BASE` selects its parent. Explicit plan/spill/timing paths retain
priority. Attempts persist after shutdown for diagnostics and worker ownership.
See the [CLI guide](https://github.com/shashankshukla96/graph-loader/blob/main/docs/CLI_RUNTIME.md).
Full CLI orchestration still expects the repository as its image build context and a local Docker
service; checkout-free image delivery is also later work.

Python 3.12 is the development baseline. Native development checks and local
image smoke checks do not establish the future supported platform/service
matrix. All eight declared runtime dependencies are mandatory, including the
matched Neo4j driver/Rust extension; no optional extras are provided.

See the [source repository](https://github.com/shashankshukla96/graph-loader)
for development and migration instructions. The initial release target is
`0.1.0a1`; development builds use `0.1.0a1.dev0` from
`neo4j_graph_loader._version`. Candidate freeze and publication require separate
validation and authorization.

Source is licensed under Apache-2.0. Report issues at the
[project issue tracker](https://github.com/shashankshukla96/graph-loader/issues).
