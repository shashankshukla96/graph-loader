# Flaky `test_explicit_process_faults_prove_peer_activation_and_cleanup`

Classification: (c) environment-triggered race in the test harness, fixed test-side. Not a product defect (`src/` untouched).

## Root cause
- Assertion that fails: `assert not supervisor['timeout'] and not supervisor['emergency_group_cleanup']` (`tests/test_public_cleanup.py:23`).
- `emergency_group_cleanup` is set by `plan/library-implementation/tools/run_e04_fault.py:55-64`: right after `child.wait()` it does `os.killpg(client_pgid, SIGTERM)`, and any live group member counts as a leak.
- The client (`tests/fixtures/e04_faults.py`, spawn context) always owns one CPython `multiprocessing.resource_tracker` child in its session. On CPython builds without the `ResourceTracker.__del__` -> `_stop()` wait (the Linux host's 3.12.3; the macOS 3.12.14 has it), nothing waits for the tracker. It sees EOF only after the client died and exits a few ms later, so the supervisor's immediate probe finds it. This matches E11.F1.S3 (24/24 leftovers were exactly the resource tracker, draining within 24 ms).
- Any parametrization can lose the race (all four scenarios and both worker counts were seen failing); load only changes the odds. That is why the failing ids differ per run.

## Reproduction
- Stock macOS venv, CPython 3.12.14: 0/40 runs fail with 17 busy-loop stressors on 18 cores (`before-stress`), and 0/30 trials leave the tracker in the group. Not reproducible, because 3.12.14 waits for the tracker at exit.
- To emulate 3.12.3 I added a venv-only `.pth` (since removed) that sets `ResourceTracker.__del__` to a no-op. A standalone probe (client that spawns one process, exits; parent `killpg(pid, 0)` straight after `wait()`) then finds the group alive 30/30 times (median linger 3.2 ms, max 5.3 ms); with the explicit reap 0/30.
- Same emulation, real test, 17 stressors, 40 runs: before the fix 19/40 runs had at least one failing parametrization (25 of 320 parametrizations; 16 with the Linux assertion above, 9 with macOS' variant: `killpg` raises `PermissionError` on a group holding an exiting process). After the fix: 0/40 runs (0/320).

## Fix (`tests/fixtures/e04_faults.py`, +22 lines)
`reap_resource_tracker()` closes the tracker's pipe and waits (bounded, 5 s, polling `waitpid(WNOHANG)`) for it, called at the end of the client `__main__` after `client.json` is written. All fault children are already reaped by then. Version independent (only `_lock/_fd/_pid`, present since 3.8). No assertion weakened; `emergency_group_cleanup` still catches real leaks. Test ids unchanged (collect-only diff of the four affected files is empty).

## Commands
```
# repro loop (40 runs, 17 spinners started/killed by exact pid)
pytest -q -p no:randomly -p no:cacheprovider tests/test_public_cleanup.py -k test_explicit_process_faults -rf
# shared-fixture regression
pytest -q -p no:randomly tests/test_public_cleanup.py tests/test_planner_transport.py tests/test_public_cancellation.py   # 126 passed, with and without emulation
```

## Needs Linux confirmation
Yes: macOS 3.12.14 never reproduced the original failure; the 3.12.3 behaviour was emulated. Run the `public-contracts` lane from the installed wheel on the host (native 3.12.3) a few times.
