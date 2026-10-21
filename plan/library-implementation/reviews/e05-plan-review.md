# E05 independent execution-plan review — 2026-10-21

**Verdict: accepted for implementation. No blocking plan finding.**

This is a separately initialized review of the E05 plan before product changes.
Actual HEAD is `14fe4934976f62f421086e541e6bc00dbc1ceec8`; observed pending
changes were the tracker and new execution plan. The review read the
[execution plan](../execution-e05.md), [handoff](../handoff-e05.md),
[canonical E05](../epics/e05.md), [decisions](../decisions.md), and
[ordering matrix](../ordering-matrix.md), then inspected implementation and
packaging rather than relying on previous approval labels. No applicable
AGENTS.md was found. Only this review report was written. No private settings,
remote IMDb service, Docker resource cleanup, source modification, commit, or
publication was used.

## CLI and helper scope

`cli.main(args_list=None)` already supplies the required no-argument-compatible
call signature. `build_parser()` currently requires a subcommand and derives
its program name from invocation; preserving no-argument parser failure while
fixing the program name is compatible with S1. The existing CLI-module main
guard is inert when imported; the proposed package-module guard must remain so
because the artifact verifier imports every Python module. One approved console
entry and version from the existing version authority match D01/D07.

The two bounded schema-only helpers directly provide E05's installed example
and validation proof. They do not expand ingestion semantics, scheduling,
ordering, public embedded API, or E06 image selection. The exact reviewed
sample choices are viable: the fresh interpreter
`var/e05-validation-20261021/dev/bin/python -I` successfully loaded
`config/graph_schema.yaml` (12,350 bytes, two node/two edge types) and
`examples/social/graph_schema.yaml` (3,547 bytes, six node/six edge types)
through the existing `load_schema`. That control made no service call.
Keeping explicit filesystem paths separate from omitted bundled defaults,
and holding `as_file` contexts through command shutdown, meets S2.

## Runtime ownership, lifetime, and Docker paths

The planned persistent exclusive attempt directory addresses current concrete
checkout-relative outputs without taking ownership of caller overrides. I
inspected `orchestrator/planned_edges.py`: the default plan currently falls
back to `var/edge-plans/<run_id>`, rejections use imported `REJECTION_DIR`, the
completion ledger is adjacent to the plan, and stage/summary/log diagnostics
read the timing directory. Copying schema bytes into owned state avoids
mutating caller/package files or binding a short-lived materialization.
Retaining state after failure is consistent with E04's potentially live owners.

All Docker roles were inspected in `orchestrator/docker_service.py`: node and
edge currently mount schema and rejections; clock mounts schema; planned writers
mount schema and plan; dispatcher mounts plan; lease server has timing output.
Timing mounts currently read global process environment through
`_mount_write_timing_directory`. Retry currently mounts schema/timings but has
no rejection mount, while `loader/retry_consumer.py:3154` hardcodes
`var/rejections/retry-consumer.jsonl`. The plan explicitly includes this
fallback and all those roles. Per-instance context and absolute daemon-visible
bind sources with existing `/app` destinations can cover the paths without
changing scheduling or image choice. The documented local/shared daemon
filesystem constraint is appropriate to E05.

`utils/metrics.py:791–830` currently chooses the CLI periodic snapshot from
environment inside `start_metrics_server`; the final CLI write independently
calls `cli_snapshot_path`. `cli.py` also writes lease settings/logs and
relationship timing files, and `planned_edges.py` writes summary/ledger/log
diagnostics. The plan correctly requires explicit timing context across all of
these, including periodic and final snapshots. Merely changing Docker mounts or
the final snapshot would not satisfy its stated scope.

The attempt-directory separation guarantees filesystem independence, including
when two attempts use the same explicit run identity. It does not promise
simultaneous Kafka/container identity independence; the plan explicitly excludes
identity redesign and retains current collision/ownership guards. This is an
appropriate boundary, rather than an E06 or scheduling change.

## Early validation and packaging proof

`_handle_start` presently starts the metrics server before edge-mode/numeric
validation and schema loading; then service preflights, topic creation, and DDL
precede launch. Consequently the planned public `main` preflight must actually
run before entering that side-effect boundary. The plan states that ordering,
real writable probes, path priority, source immutability, service-forbidden
negative controls, and read-only-rootfs proof. Existing direct-handler tests
cannot alone establish it; the proposed actual-main controls address that gap.
Preflight must preserve existing explicit plan locks and avoid testing writable
access by truncating caller files; exclusive owned probe files are compatible
with the plan's ownership rule.

The strict verifier is physically at
`plan/library-implementation/tools/verify_e03_artifacts.py` (the handoff's
`tools/` is relative to that planning directory). It currently permits exactly
all Python modules plus five wheel metadata members, rejects entry points,
permits an exact sdist egg-info set, compares package bytes, imports every module
with socket/thread/subprocess side effects forbidden, and asserts clean
noneditable installs. `MANIFEST.in` currently globally excludes files before
explicit Python/metadata includes. Therefore S1/S2 must deliberately add the sole
entry-point metadata in both archive formats and only the two exact data members;
the plan requires precisely that rather than weakening the leak checks. The
baseline capture's broad data exclusion also needs only the two exact resource
exceptions, as the implementation owner separately identified.

No-Git sdist rebuild/member-byte comparison, two clean external noneditable
installs, missing-resource/archive omission controls, and read-only package/cwd
checks are sufficient planned evidence for E05. Immutable image checks and the
owned no-skip B3 preserve existing service behavior without claiming E07–E09
completion. Actual execution and fresh story/final reviews remain required;
this acceptance closes only the pre-implementation plan gate.
