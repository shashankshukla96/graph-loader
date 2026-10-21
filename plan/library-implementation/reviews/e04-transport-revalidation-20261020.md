# E04 transport independent revalidation — 2026-10-20

Reviewed HEAD `786260bec3cceb8ed14990896380640cb52115d2`, including the E04
implementation at `55dd4938188768090c10d851f8f7330706975ea5`, against the
canonical E04 stories, approved cancellation contract and D02/D04 decisions.
Prior review approval labels were not used as acceptance evidence.

**No actionable transport findings identified in the reviewed state.** This is
a scoped transport review, not acceptance of all E04 behavior or E08's platform,
start-method and maximum-worker matrix.

## Scope and inspected behavior

- `_planner_transport.py`: closed codec, frame magic/length checks, scalar and
  array bounds, cumulative blob accounting, depth/node limits, malformed-value
  failures, interruption checks, incremental reads/writes, selector waits,
  interrupted outbound-frame handling and endpoint close.
- `_ProcessWorker` and `_planner_worker_main` in `edge_planner.py`: initial
  excluded source pickle versus later closed-codec messages, parent deadline
  callback assignment after initial submission, child/parent duplicate closure,
  failed construction/start ownership, remote-error handling and bounded stop
  delivery through the same transport.
- `tests/test_planner_transport.py`, `tests/fixtures/e04_faults.py` and
  `plan/library-implementation/tools/run_e04_fault.py`: deterministic partial
  progress/deadline controls, codec controls, real-process fault activation,
  private/public partial replies and nonreading command/stop fixtures, and the
  supervisor's independent deadline and owned-process-group emergency cleanup.

The actual adapter makes both endpoints nonblocking and limits every individual
socket read/write attempt to 64 KiB. Partial progress loops check the supplied
control, and selector waits use that control's remaining absolute deadline with
100 ms maximum slices. Later parent sends, including stop, use this adapter;
there is no receiver/helper thread or process. Interrupted outbound frames
cannot be followed by an appended stop frame.

Parent reply decoding does not call pickle. Exact supported Python types and
numeric-array dtypes are accepted; unapproved object types are rejected without
formatting/reducing them. Limits bound the frame, aggregate blob payload,
individual scalars/arrays, nodes and depth. The documented aggregate blob cap
is not a hard resident-memory cap: framing buffers, object overhead and
conversion copies also consume memory. The documentation describes computation
as cooperative at traversal/operation boundaries, including before/after codec
work, rather than promising CPU preemption inside a conversion.

## Fresh executed checks

Using the existing installed development interpreter
`var/e04-validation-20261018/dev/bin/python`, with bytecode and pytest cache
writes disabled:

- `tests/test_planner_transport.py`: **55 passed in 2.28 seconds**. This includes
  the real-process private partial-large-reply and nonreading-peer faults, each
  exercised with timeout and cancellation through a separate supervisor. It
  also includes exact/over-limit 64 MiB frames, maximum actual counters,
  supported array endianness/widths, malformed frames, callback rejection,
  construction/start failure socket ownership and interrupted outbound frames.
- Independent deterministic malformed-payload probe: **50,000 random payloads**
  of 0–96 bytes, plus invalid UTF-8 and an unhashable nested tuple mapping key.
  Rejected payloads produced `FrameProtocolError`; **zero unexpected decoder
  exceptions** occurred.

No services, remote IMDb operations, product edits, commits or pushes were
performed. Only this review report was written. The parent revalidation owns
the full suite, public operation/cleanup acceptance, installed artifact/image
checks and the final E04 report; this review does not substitute private-adapter
tests for those successor/public gates.
