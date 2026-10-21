# E04.F2.S4 independent cleanup review

Verdict: **changes requested**. Two actionable findings reproduced independently.
Review performed 2026-10-21 UTC against the actual dirty/untracked working tree
on `codex/pypi-library-checklist`, HEAD
`98cd267db5c70d3517a89ec68c1a4cedf2c254fd`. No implementation or test edits were
made by this reviewer. S1/S5/S3 changes were treated as prerequisites.

Read the handoff, canonical E04 story, cancellation contract, execution plan,
actual planner/transport, public cleanup/lifecycle/cancellation tests, fixture and
external supervisor. No `.env`, remote IMDb, Docker or foreign resources used.

Reviewed source SHA-256 before repairs:

- `edge_planner.py`: `a1ba369c9513b9fa3de2366c2fd34d77e9227e0774453182977765653a21746f`
- `_planner_transport.py`: `00b9f7fbe67149d61522bad11627b800cf1a7b379676e769ebb2a4f56446126b`
- `test_public_cleanup.py`: `400fb8006c9f652c7c6442657df020cd13a43cba715f64ec23e1f655e4217fcb`
- `e04_faults.py`: `74fd9b022012f954caf6533a8b5f71b4a57e968cdd3fa87c66c82ed8a4afd6e2`

## F1 — P1: heartbeat removal error skips worker cleanup and permits live-topic deletion

Locations: `src/neo4j_graph_loader/loader/edge_planner.py:1762` (unguarded
heartbeat stop), `:1338` (progress unlink), `:1754` (subsequent close becomes a
no-op), `:1780` (incomplete flag assigned too late), `:2250` (topic deletion).

`ParallelEdgePlanner.close()` records `_cleanup_started` before calling
`_heartbeat.stop(remove=True)`. If progress removal raises an `OSError`, cleanup
returns before signalling any worker or closing any endpoint. The flag remains
false and the permanent started guard prevents the convenience function's
`finally` from doing cleanup. `plan_and_publish_edges()` therefore deletes its
owned topic despite the potentially live producer. This violates S4's cleanup
and retained ownership rules; adding the error as an exception note does not
stop or account for the live workers.

Reproduced through the public convenience call, using a deterministic inert
process fake (same pattern as `test_public_cleanup.py`) and a `Path.unlink`
fault only for an existing `plan.json.progress`. Source reply was
`('error', 'ordinary source failure')`, explicit spawn context was supplied,
and `FakeCluster.admin` owned the topic. No actual child or service was created.
Observed output:

```text
error planner worker 0 failed: ordinary source failure
notes ['review injected progress unlink failure']
process_cleanup_actions []
worker_endpoint_closed False
topic_deleted ['chunks']
retained_lock True
```

The injected failure represents a normal permissions/filesystem removal error;
all other unlink operations proceeded normally. The public original error was
preserved, but its required cleanup did not execute.

Required repair: isolate best-effort housekeeping failures from stop/terminate/
kill and endpoint cleanup, continue attempting all owned workers under the same
budget, and conservatively account for unresolved cleanup before convenience
ownership decisions. Add regression assertions that a heartbeat/progress cleanup
failure cannot bypass child signalling or endpoint closure, cannot incorrectly
delete a topic while children remain unresolved, and remains observable without
hiding the original error. Audit endpoint and inline cleanup exceptions for the
same early-abort pattern.

## F2 — P2: outer finish overwrites partial-startup retained-resource diagnostics

Location: `src/neo4j_graph_loader/loader/edge_planner.py:1614`.

When startup fails inside `finish()`, `start()` first writes its failure marker,
runs cleanup, and appends unresolved worker IDs and retained paths. It attaches
the cleanup error as a note and re-raises. The enclosing `finish()` then calls
`marker.write_text(...)`, erasing the cleanup diagnostic. Its final close is
already a no-op. This loses the on-disk recovery information specifically needed
for unreaped partial startup, despite the exception note and live lock remaining.

Reproduced independently with public `producer_edge_planner(...).finish(path)`:
two process fakes, second `start()` sets PID then raises
`RuntimeError('second process started then failed')`; both processes remain
alive under terminate/kill and joins advance a fake shared cleanup clock.
Observed:

```text
cleanup elapsed: 10.0 (one aggregate budget)
cleanup_incomplete True
retained_pids [900001, 900002]
failed_marker 'RuntimeError: second process started then failed\n'
```

The exception note contained both worker IDs and retained spill/lock paths, but
the persisted `.failed` marker contained none of them. This contradicts S4's
failure marker/resource diagnostics and the API documentation's statement that
the marker retains diagnostics.

Required repair: preserve existing cleanup diagnostics when outer failure
handling persists the original startup error. Add the partial-startup plus
unreapable-worker combination to the fake public tests and assert original
error/cause/note, all PIDs, retained paths, lock/spill/topic retention and no fresh
cleanup budget. Current normal real startup assertion only checks the error
class in the marker; current unreapable controls generate failures after
startup, so neither detects this overwrite.

## Independent validation and scope

Executed with `var/e04-validation-20261018/dev/bin/python`:

```text
python -m pytest tests/test_public_cleanup.py tests/test_public_cancellation.py tests/test_planner_abort.py -q
42 passed in 12.57s

python -m pytest tests/test_planner_transport.py tests/test_public_lifecycle.py -q
63 passed in 2.21s
```

This ran the external-supervisor public blocked-source, partial-reply,
later-command backpressure, stop backpressure, source-error, second-process
startup failure and four-worker cases, plus timeout/cancellation variants and
unreapable fake retention controls. The normal cases established their existing
elapsed bound, child/endpoint death, heartbeat absence and released lock/spill.
The aggregate fake schedule is shared and signal-all occurs before joins. The
interrupted outbound-frame guard prevents appending another stop to a partial
frame; stop sends use one shared 100ms cutoff. Worker handles are owned before
`start()` and the started-second-worker normal fixture is reaped.

The passing suite does not close the two additional failure-path findings above.
No final S4 acceptance is given; repairs require focused retests and independent
re-review of their actual revised state.

## Repair re-review — 2026-10-21 UTC

Final S4 verdict: **accepted** for the revised state identified below. The initial
changes-requested report and reproductions above are preserved as history. F1
and F2 are closed by independently inspected and exercised repairs; no remaining
actionable S4 finding was identified. This is story acceptance, not whole-E04 or
E08 platform-matrix acceptance.

Revised SHA-256:

- `edge_planner.py`: `082defec8d930f0076c27e85db29621a75a515fa4411db033f3cf4f1119fe4bf`
- `_planner_transport.py`: `00b9f7fbe67149d61522bad11627b800cf1a7b379676e769ebb2a4f56446126b`
- `test_public_cleanup.py`: `c89b1c7f612f15c4548955c056a9d4f5c72d5be010cb173997a25f4f0777dcbd`
- `test_planner_transport.py`: `2dfebc03487394b9ef5719fc3d508019f91cefbb79c463985cbf4f341ca4c38c`
- `e04_faults.py`: `74fd9b022012f954caf6533a8b5f71b4a57e968cdd3fa87c66c82ed8a4afd6e2`

F1 repair: close sets its incomplete flag conservatively at entry. Heartbeat
stop/removal, individual endpoint closures, inline stop/close and owned file
removal failures cannot skip the remaining worker cleanup. Such failures retain
owned lock/spill/topic state, append recovery diagnostics, preserve the original
planning exception plus cleanup note, and do not obtain a fresh cleanup budget
on the convenience function's final close. Endpoint failures retain the affected
worker handle while remaining worker cleanup proceeds. A cleanup error is
observable on explicit close as well as through the original-error note.

F2 repair: outer `finish()` appends its original failure rather than truncating a
marker already containing startup cleanup diagnostics. The added unreapable
startup controls exercise factory calls and both convenience topic-ownership
modes and retain all started PIDs and resource paths.

The reviewer independently re-ran both original fault patterns through public
convenience calls, using newly written deterministic controls in ignored
`var/e04-validation-20261018/reviewer-s4-repair-controls.py`. Unlike the first
reproduction, progress-unlink failure now records termination for every worker,
closes all endpoints, keeps the owned topic and persists cleanup details. The
second-start-after-PID failure retains both PIDs in the marker, keeps the
lock/spill/topic and consumes exactly one aggregate 10-second fake-clock budget.
Repeated close performs no new actions. The same independent controls inject
both an inline stop error and inline close error in the first of two workers:
both stops and both closes are attempted in order, the second state closes, the
first is retained, and diagnostics preserve both errors. No actual child/service
was created by these additional controls.

Control script SHA-256:
`fc11d8abe04c7736a76fa064e1af9ff4aef4822b1b1d7c4fb053c841c92f0b8d`.
Its retained log is
`var/e04-validation-20261018/reviewer-s4-repair-controls.log`, SHA-256
`82917a5b799cbc500c4f053e8a257fad098bb1c2d89dfcd8b2dfe5785cb09817`.
All assertions passed, exit 0.

Fresh independent installed-dev checks:

```text
python -m pytest tests/test_public_cleanup.py tests/test_public_cancellation.py tests/test_planner_abort.py tests/test_planner_transport.py tests/test_public_lifecycle.py -q
111 passed in 14.58s

python -m pytest tests/test_planner_transport.py::test_interrupted_outbound_frame_cannot_be_followed_by_stop -q
1 passed in 0.07s
```

The focused suite includes the added progress/endpoint/spill fault controls and
unreapable startup combinations. The final single transport test was explicitly
run after the author reported adding it during re-review; it verifies an
interrupted two-byte header cannot be extended with a stop command. No runtime
code changed during this re-review. Source/fixture bounds, external supervisors,
normal child/endpoint/heartbeat liveness assertions and the shared signal-all /
join / terminate / kill schedule remain intact. No implementation or test edits
were made by this reviewer.
