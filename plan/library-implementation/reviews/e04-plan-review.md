# E04 independent plan checkpoint — 2026-10-18

Initial disposition: **changes requested: one actionable P2 validation gap**.
The bounded implementation design and story order are otherwise suitable for
E04. This report accepts neither future implementation nor unexecuted tests.

Reviewed actual entry HEAD `98cd267db5c70d3517a89ec68c1a4cedf2c254fd`.
Product source is unchanged at this checkpoint. The working changes inspected
are execution-e04.md, the pending tracker entry and e04-entry.json. This
reviewer writes only this report; no product edits, service work, commits or
pushes were performed. No private environment file or remote IMDb was accessed.

## P1 — prove the new closed codec's safety and resource bounds

**P2, execution-e04.md:29–39.** The new codec has meaningful concrete limits,
but the S5 test list only spells out transport framing/fault controls. It does
not explicitly require the codec type, callback-exclusion and boundary controls
that establish the safety of replacing arbitrary parent-side pickle decoding.
The approved cancellation contract separately requires maximum-frame
resource/codec validation. A transport oversize-header rejection alone does not
prove a bounded decoder or that valid current worker results round-trip.

Add these concrete S5 checks before implementing that step:

- Round-trip every actual post-submission command/reply shape, including scan
  task summaries, degree dictionaries with bytes keys, emit ResourceTable JSON,
  ChunkSpec tuples/lists and error strings. Include every supported hub scalar
  key type and numeric counter type that actual workers produce. Test the real
  maximum current counter (`2**22` uint32 slots, 16 MiB) and its 512 KiB packed
  candidate command; do not mistakenly apply the 1 MiB scalar bound to the
  counter buffer.
- Reject unsupported exact types/subclasses, object/unsupported array dtypes,
  unknown/malformed tags, inconsistent declared lengths/shapes and invalid
  mappings. Demonstrate that parent encoding/decoding cannot execute user
  `__reduce__`, constructors or property hooks. The initial task pickle remains
  explicitly child-only and excluded; no later response may fall back to pickle.
- Exercise valid boundary and over-boundary scalar/frame/counter/node/depth
  cases, huge integer encodings, and cumulative allocation accounting. Check
  limits before materializing oversized containers/scalars/buffers, rather than
  only after building the complete encoded output or decoded object graph.
  Use a substantial valid frame/resource case and checkpoint instrumentation to
  verify cancellation/deadline checks during long traversal, as already promised
  by execution-e04.md:33–34. No CPU-preemption promise is requested.

This is a missing planned proof, not an allegation that unimplemented codec
code already violates the contract. It can be repaired by specifying this S5
matrix and retaining it through the implementation review.

## Independently inspected and retained

Read the full handoff, execution plan, canonical E04 epic, cancellation contract,
decision brief/register, ordering matrix, test inventory and change tracker.
Read the linked fresh E03 acceptance/review records and independently verified
all 13 raw-record sizes and SHA-256 values from e03-revalidation-20261018.json.
The accepted prerequisite records contain the 3,387-pass suite and fresh exact
B3/build/install/image proofs. This reviewer did not rerun those executions.

Inspected actual `_planner_worker_main`, `_ProcessWorker`, `_InlineWorker`,
SourceTask.scan, counter/degrees/emit payload construction, ParallelEdgePlanner
start/gather/broadcast/finish/close, heartbeat and convenience/Kafka topic
ownership paths. Read relevant ownership/preflight, abort, external planner,
source, schema and multi-pair regressions. Current parent blocking Pipe I/O,
per-worker joins and unconditional spill/lock deletion are accurately identified
as work still required, rather than validated cancellation behavior.

The following design boundaries are sound and must remain intact:

- F1.S1 → F2.S1 → F2.S5 → F2.S3 → F2.S4 → F2.S2. S5 proves private transport;
  S3 independently proves public signal/timeout observation; S4 adds total
  cleanup/liveness/marker/resource proof. Real partial-large-frame and later
  command/stop backpressure fixtures retain their external hard supervisors.
- Process deadline anchors after all initial submissions; inline before source
  iteration, with per-record checks. One stored absolute deadline includes
  phase changes and the start/finish gap. Excluded callbacks do not reset it.
- A separate aggregate 10-second cleanup deadline covers signal-all,
  stop delivery, shared join/terminate/kill cutoffs and heartbeat/endpoints.
  Unreaped worker identities remain observable and live spill/output/topic
  ownership is retained. Errors and cleanup diagnostics remain available.
- The explicit schema/planning export list derives from approved D02. Concrete
  source identity, Kafka requirement, no expanded fleet/worker Python API,
  mandatory dependencies, external configuration and E05 console boundary are
  preserved. This checkpoint does not expand D04's advertised platform matrix;
  every later claimed platform/start-method/maximum-worker cell requires E08.
- Final fresh source/environment identities, complete archive/no-Git rebuild,
  separate installed origins, all affected images and actual B3 regression
  remain planned. Source edits invalidate their corresponding old evidence.
  No E05–E12 implementation or publication is in scope.

## Resolution status

P1 is open pending a revised S5 validation plan and independent re-review.

## Independent re-review — P1 closed

**Final disposition: approved for the E04 implementation sequence. No open
actionable plan finding remains.** Read the actual revised S5 section: it adds
the complete command/result round-trip matrix, maximum real counter/candidate
controls, exact-type/dtype/tag/shape/length rejection, parent callback exclusion,
and exact/over-limit frame/scalar/depth/node/cumulative allocation checks. Small
configurable bounds are supplemented by real maximum counter/frame controls.
The existing traversal interruption checks remain required. These are sufficient
planned proofs for P1; implementation and executed evidence remain separate
future gates. ResourceTable encodes date/datetime hub values into primitive JSON
key representations before later IPC, so this does not require adding arbitrary
Python date constructors to the private codec.

Re-reviewed execution-e04.md SHA-256: `2d1aea4863bba6e7159a35e0458c98d8ea7dce92a10565e79e4196e6254a33a3`.
`git diff --check` passed. Original finding and requested controls are retained
above; this approval does not claim those tests have already been implemented
or executed. Only this review report was edited by this reviewer.
