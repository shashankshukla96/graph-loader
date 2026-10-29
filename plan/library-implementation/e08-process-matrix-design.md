# E08 S2 remaining matrix design —2026-10-28

Prepared before the remaining harness changes. The [focused design review](reviews/e08-external-client-review-20261028.md)
accepts this bounded opt-in design for implementation, with the limits below.
This is not implemented matrix proof or S2 acceptance. The [execution plan](execution-e08.md)
and [S2 evidence](evidence/E08.F2.S1.md) retain every original gate.

Add an explicit process-envelope option to the maintained installed runner,
limited to its public-contracts lane and passed explicitly by support-matrix
tooling. Its existing interface/defaults and all original selections/assertions
remain. Use the same fresh noneditable target environment, frozen external
fixtures and actual E07 startup/origin/creation guard. Each sequential cell gets
new owned output and a separate provenance log. Change only the target-owned
guard configuration's log path, restore it in finally, and fail on restoration,
provenance, command, observation or cleanup error. Independently verify actual
case child identities and installed payload origins before recording success.

Normal spawn/inline cells measure1–8,16,32,64 workers. Full failure/cleanup cells
measure1,2,4,8,64: nonpicklable source, original source failure and actual exit37
under spawn; cooperative source failure under inline. Inline legitimately accepts
an unused nonpicklable attribute and cannot safely emulate abrupt process exit
or preempt a source that never returns. Preserve the caller-selected method.

Extend E04 fault fixtures only through explicit context/worker controls. Legacy
three-positional/default calls, original timers, assertions, first-received R1
controls and external supervisors remain unchanged. Framed process faults use
explicit spawn; inline has no process channel. Every new cell records requested
capacity separately from actual launched workers. A deliberate second-start
failure creates two actual workers even at a larger requested capacity. Mixed
peer/source faults require at least two workers; never label that as a proven
one-worker mixed case. The single-source failure covers one-worker errors.

New prepared transport cells may use importable worker wrappers that mark boot
readiness before delegating to the unchanged worker body. A fixture-only bounded
60-second readiness barrier before the original operation anchor separates cold
imports and all intended blocked/partial-reply peer activation from the actual
frame wait. Record the excluded
submission-to-boot/peer-activation interval. These prepared cells do not establish unmodified
end-to-end startup timeout behavior. Separate cold blocked-source timeout cells
at measured worker counts use the unmodified operation clock and do not require
the source fault to have activated before the deadline. All original clock/gap
controls remain; an N-lane fake clock advances5 per initial scan, then must prove
that cumulative anchor and the later expiry without resetting the deadline.

Only the new prepared cancellation path may wait for the operation anchor before
its original readiness window. It must also observe the fixture stop signal on
startup failure, join, and assert timer/thread death. Existing default timing
assertions and10-second aggregate cleanup remain. Public cleanup/liveness/lock/
spill assertions precede any fallback fixture reaping.

For the new high-count later-write fault, use a counter of1<<18 uint32 entries:
1MiB before packing,32KiB packed degrees. The initially proposed16K counter
would yield only2KiB packed degrees and would not reliably exceed the socket
buffer. Record actual SO_SNDBUF and encoded degrees-frame size, require the
frame to exceed that observed buffer, and require the original EVENT_WRITE/
degrees activation marker. Keep the old4M-entry default unchanged. Maintain
one operation budget over all real phases; never move its anchor between phases
to hide counter/read work.

Private high-count controls own N fault peers and exercise a fault channel with
blocked neighbors. Record that scope rather than claiming multiplexed receive.
Retain the original0.15-second control/0.75-second adapter assertion and use one
aggregate cleanup budget for all peers. New externally supervised cells may
have a separately recorded180-second overall preparation/execution bound;
original default20-second fault supervision remains intact.

Run actual native arm64/amd64 cells from fresh owned immutable contexts and
installed wheels, with original IDs/provenance and no checkout source mount.
Retain all failures and every unavailable required cell. Fresh complete S2
review and all final artifact/dual-wheel/image/live-service/R1/B3/adversarial/
delivery/equality gates remain required. No publication or later epic execution.

Partial-startup cleanup and cold deadlines can legitimately terminate children
after their audited interpreter startup but before application import. Retain
their actual empty import lists, with full prefix/site/payload/startup/parent/
creation/launch binding. Permit that scope only for the exact injected second-
start failure before the operation anchor, or an unprepared real0.8-second cold
timeout. Activated transport peers still require actual planning-module import
evidence. Never fabricate imports or count an unbound child as verified.


## Actual native exit-status race amendment

The first S2 arm64 minimum/current runs retain real worker-exit failures at
four/two workers: source0 commits its activation identity and calls os._exit37,
but channel EOF is received before multiprocessing has reaped the exit status,
so the public error honestly contains exit code None. Do not delay receipt,
reset clocks or change production cleanup to wait for diagnostic text. The
bounded fixture repair uses an actual SpawnContext subclass delegating Process
creation to the unchanged real spawn context and retaining those parent handles.
After the original finish/cleanup, require the activated source0 PID's actual
reaped exitcode37 and dead state. Accept only the original unexpected-exit error
with37 or transient None in its message, retain that original cause/marker and
record actual process exits. Exit38 and an unrelated boot-time37 remain failures.
No application byte, transport behavior, timing bound or worker body changes.
Focused reproduction/negative controls, fresh review and new immutable native
proof are required before acceptance; the first failed runs remain failed.
