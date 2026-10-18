# Approved cancellation and timeout implementation contract

**D02 approved on 2026-10-16** through the user's approval of the complete
brief. This is the approved implementation target, not an implemented or
validated public API. Approval includes specified names, limits and the
10-second aggregate cleanup target; E04/E08 retain implementation/proof.
Original proposal wording below is retained as design context. See
[direct approval evidence](evidence/e01-user-decisions-20261016.md).
Ownership already proved by B1 is separate from this new work.

## Proposed supported surface

- Add keyword-only `cancel_event=None` (a caller-owned `threading.Event`-like
  `is_set()` signal, observed only by the parent) and `timeout_seconds=None`
  (positive finite seconds) to approved planner/convenience interfaces.
- Proposed exported errors are `EdgePlanCancelledError` and
  `EdgePlanTimeoutError`, both under the public `EdgePlanError` family; preserve
  causes/diagnostics for ordinary source failures. D02 chooses final names.
- The planner is one-shot. A different thread may set `cancel_event` while the
  caller is waiting; concurrent `start`/`finish`/`close` on the same planner is
  unsupported. Independent planner instances still follow filesystem ownership.
- Use one operation-wide absolute monotonic deadline across scan, hub, emit
  and parent result processing. Never reset it per gather or phase. In process
  mode record the clock anchor immediately after initial task serialization/
  submission completes (time between a separately completed `start()` and later `finish()` counts).
  In inline mode record it immediately before the first source iteration, since
  inline submission itself runs the scan synchronously. Inline scan time must
  count; check the same deadline between records and phases. It is not a CPU timer.
- Kafka topic administration, source construction/pickling, arbitrary blocking
  `next()`/client callbacks and OS-uninterruptible work cannot be interrupted by
  this coordinator deadline. Their own supported timeouts/limits must be explicit
  in API documentation. Do not describe this as a universal bound on an external
  call or reset the elapsed budget after excluded blocking work returns.

## Complete framed IPC: new behavior owned by E04.F2.S5

The proposed private transport is an owned socketpair/stream adapter with length
framing. The parent endpoint uses nonblocking incremental header/body reads and
nonblocking writes, waiting for readiness only through cancellation/deadline-aware
selector slices. A `poll()`/readiness success means some bytes may be available,
not that a complete frame has arrived. Never follow readiness with an unbounded
`Connection.recv()`/`send()` or `sendall()` on the coordinator after its deadline
begins. Track partial headers, partial bodies and write backpressure explicitly.
The child may use blocking I/O; it is the owned worker that cleanup can terminate.

Preserve current result types and external Kafka/plan formats. Set explicit
frame-length/resource limits, validate framing, and report oversize/corrupt/
truncated/EOF failures clearly. Document parent-side codec computation separately
from bounded I/O waits: check the elapsed deadline before and after codec work,
allow no undisclosed arbitrary user callbacks there, and validate maximum-frame
resource/codec behavior in the supported matrix. This is not CPU preemption.
If codec/transport discovery needs a larger design, split and re-review it.

The planner owns both endpoints, closes duplicate child descriptors after spawn,
and introduces no receiver helper thread/process. Later broadcasts and stop
messages use the same bounded write path. A worker refusing to read a stop frame
must lead to deadline-aware cleanup and out-of-band terminate/kill, not an
unbounded stop send. Initial task/source serialization/submission keeps its
already explicit exclusion; later IPC is not silently excluded from the deadline.
D02 approves this proposal or triggers a separately reviewed equivalent design;
D04/E08 must validate every advertised platform/transport.

S5 validates the private adapter with fake-clock/readiness controls and real-process
partial-frame/backpressure fixtures. These fixtures call the private transport
with a supplied signal/deadline; an external supervisor independently bounds the
test and owns child/endpoint cleanup. Public planner errors and full operation
cleanup are successor gates in S3 and S4, not prerequisites for completing S5.

## Process wait/cancellation integration: E04.F2.S3

Use E04.F2.S5 to keep the calling thread responsive throughout complete-message
transfer and parent gather/broadcast/result processing. Each at-most-100-ms I/O
wait slice, partial-transfer iteration and phase transition checks the signal
and the same stored absolute monotonic deadline. Stop scheduling work on
cancellation/expiry, enter owned cleanup and raise the approved public error.
Do not configure caller signals or global process start methods.

A real blocked child source is required but is not the only negative control.
A second real-process worker must begin a reply larger than the socket/pipe
buffer, expose partial frame readiness and stall before the whole frame arrives.
Exercise both cancellation and timeout through the public planner surface from
a separately supervised client. The test supervisor has its own hard deadline
and emergency fixture cleanup so a regression cannot hang the runner. S3 asserts
the approved public error and prompt coordinator signal/deadline observation,
using the existing abort/terminate path. Also exercise public cancellation and
timeout when a worker stops reading a later command. Keep pre-reply blocking,
phase-clock and inline-clock tests. S4 completes these same public cases with
the final total operation/cleanup bounds, liveness, markers and resource proofs;
private transport controls alone do not establish the public contract.

## Inline sources: an explicit limit

`records()`/`next()` run synchronously in the calling process. Check the signal
and deadline between records and phases; a source or callback that never returns
cannot be forcibly interrupted safely. Document and test cooperative inline
behavior; never promise bounded interruption of arbitrary blocking inline code.
Callers requiring interruption of `records()` must select supported process
execution with picklable source objects. Construction, pickling, Kafka/client
calls and OS-uninterruptible operations have their separately stated limits.

## Cleanup budget: new behavior owned by E04.F2.S4

Include partial-frame and full-send-buffer cases in this budget. Endpoint
closure and bounded stop delivery are owned by the same planner; there is no
receiver helper to abandon. Use a single elapsed cleanup budget (proposed 10 seconds, finalized in D02 and
E08 support evidence) across workers, not an unbounded/N-times-30-second join.
Signal all workers, use bounded joins, then terminate/kill where the supported
platform permits. The cleanup budget is separate from the expired planning deadline, not a reset
of it. A normal blocked Python worker must be reaped within the
budget plus the <=100 ms polling tolerance and documented test scheduling slack.
Validate representative and every advertised maximum worker count in E08.

If the OS cannot reap a worker within the budget, report its identity and
unreclaimed resources explicitly. Preserve potentially live spill/output
ownership; do not falsely claim successful cleanup or delete a live worker's
files. Do not delete caller-owned topics/resources. The public failure and its
cleanup diagnostics must remain observable. This is a bound on supported
coordinator waits and normal child cleanup, not a universal OS termination claim.

Required tests: blocked real child source, parent cancellation, elapsed timeout,
cooperative inline cancellation, normal source error, partial startup and a
termination-resistant fake process. Verify final values/errors, elapsed bound,
worker liveness, failure markers and ownership of retained/reclaimed files and
topics through the public surface. S4 owns these final assertions across the S3
public controls, including mid-frame replies, later-command/stop backpressure,
pre-reply blocked sources, source failures and partial startup. Cover both
cancellation and timeout where applicable. Preserve each test's independent
hard-deadline supervisor and emergency cleanup; do not silently remove a
negative control to fit a story estimate.

The cancellation tests must include a slow multi-phase operation to prove the
same absolute deadline survives gather/phase changes, and a cooperative inline
scan that expires while iterating. Process-mode tests identify the completed
initial submission as the one-time clock anchor.

For the inline-clock negative control, use ten individually returning records
that advance a fake monotonic clock on each record; assert timeout stops
consumption before all ten records. Check separately that time between process
`start()` completion and a later `finish()` counts against the stored deadline.
