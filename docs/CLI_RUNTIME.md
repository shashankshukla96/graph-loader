# Installed command, schemas and runtime state

Install the distribution, then run `graph-loader --help` or
`python -m neo4j_graph_loader --help` from any directory. The existing
`python -m neo4j_graph_loader.cli` form has the same parser and exit codes.
`--version` reports the installed package version. Imports and commands never
load `.env` implicitly; supply process environment settings explicitly.

`graph-loader schema-example --name social > schema.yaml` copies the bundled
social sample to a caller-selected file. The default name is `default`, the
canonical schema. `graph-loader validate-schema --config schema.yaml` validates
that file without Docker, Kafka or Neo4j. `validate-schema --sample social`
validates the bundled social sample. These are example schemas; adapt their
entities and topics before ingestion. Exactly these two small YAML resources
ship in wheel and sdist; no input datasets ship.

An explicit `--config PATH` is resolved from the caller's directory. Omitting
it uses the bundled canonical schema, regardless of any `config/` directory in
the caller's working directory. Materialized resources remain available through
command shutdown. Installed files and caller schemas are never rewritten.

Each `graph-loader start` creates a private persistent `graph-loader-*` attempt
directory under the OS temporary directory. `start --run-dir BASE` creates a
unique child under BASE instead, creating the base if necessary. The absolute
attempt path is logged as `stage=runtime directory=...`; save it for diagnostics.
The attempt directory has mode0700. It contains:

| Path | Purpose |
| --- | --- |
| `runtime_schema.yaml` | Private0600 copy used for this attempt and container binds |
| `rejections/` | Node, classic edge, planner and retry rejection output |
| `plans/` | Default edge plan and adjacent ledger/progress/failure/lock files |
| `spill/` | Default planner spill base; planner owns its children |
| `timings/` | CLI/container timings, metrics snapshots and coordinator logs |

`--plan-dir` and `--plan-spill-dir` override those output defaults.
`GRAPH_LOADER_TIMING_DIR` overrides the timing directory. Existing caller
permissions are preserved and actual write access is checked before ingestion;
recognized existing timing outputs are opened without truncation for preflight.
The external `--edge-plan PATH` remains an input, with its ledger beside it; its
producer may create the input later. Explicit shared output directories require
caller coordination. Planner output locks still reject overlapping owners.
Separate default attempts never share state, including repeated explicit run IDs;
existing Docker names and Kafka group/topic identities are unchanged, so reuse
of a run ID does not authorize concurrent fleets with that identity.

Attempts are retained after both success and failure. No automatic directory
removal occurs: diagnostics or a worker that has not reaped may still own files.
After all owned workers and containers have stopped, the caller may remove their
attempt directory. Keep explicit shared outputs and foreign locks under their
own ownership policy. Read-only working directories and installations work when
the chosen temporary/output storage is writable; path errors return exit1 before
ingestion dispatch. Cheap invalid numeric/mode flags fail before state creation.

Container launchers bind absolute host schema/plan paths read-only and rejection/
timing directories read-write at `/app/runtime_schema.yaml`, `/app/edge-plan.json`,
`/app/rejections` and `/app/write-timings`. Host bind paths must be visible to the
Docker daemon: use a local daemon or shared filesystem with matching host paths.
E05 does not translate files to remote Docker hosts.

Full CLI orchestration still builds images from the current checkout unless
`--skip-image-build` is supplied with compatible local images/explicit environment
image overrides. Checkout-free image selection and registry delivery remain E06.
Help, version, schema commands and runtime state preparation are checkout-independent;
this does not claim a complete checkout-free service ingestion proof.
