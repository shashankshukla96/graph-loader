# E04.F2.S5 independent private transport review

Reviewed 2026-10-18 against entry HEAD `98cd267` and the actual working tree.
Read the E04 handoff, execution plan, cancellation contract, canonical S5,
transport implementation, worker integration, tests, fixture/supervisor and S5
evidence. This review covers the private S5 gate only.

## Finding and independently verified repair

**S5-R1, P2 — constructor failures before process.start leaked owned endpoints.**
Original `src/neo4j_graph_loader/loader/edge_planner.py:1101-1108` allocated the
socketpair before RawArray, Process and FrameStream construction, with cleanup
beginning only around `process.start()`. An exception in any earlier construction
step bypassed both explicit socket closes. Retaining its traceback could retain
both descriptors. This violated S5 partial-construction endpoint ownership.

Reproduced independently using a real socketpair captured by a patched pair
factory and a context whose RawArray raises RuntimeError. Both socket filenos
remained open (`[3, 4]`) after constructor failure. This reproduction did not
start children and closed its own sockets afterwards.

**Closed after repair.** Actual revised constructor at lines 1102-1117 includes
RawArray, Process, FrameStream and start under one try, closes parent on failure
and closes child in finally. Independently reran the new four-stage regression:
RawArray/Process/FrameStream/start preserve the exact exception and both real
socket filenos are `-1`. Rechecked actual source, not only the test result.

## Executed verification

Fresh installed development interpreter:
`var/e04-validation-20261018/dev/bin/python`.

- Initial private transport test file: **50 passed in 1.63s**.
- Repaired private transport test file: **54 passed in 1.58s**.
- Repaired focused lane (`test_planner_transport.py`, `test_external_planner.py`,
  `test_planner_abort.py`, `test_public_api.py`, `test_public_lifecycle.py`):
  **98 passed in 3.13s**.
- Independent supplemental probes: 10,000 deterministic random malformed
  payloads plus explicit invalid UTF-8, unknown array dtype and unhashable
  direct/nested mapping keys rejected with FrameProtocolError; no uncontrolled
  decoder exceptions.
- Independent fake-selector probes exercised both read AND write with timeout
  AND cancellation. Absolute timeout ended at fake time 0.25; signal observed
  at 0.30 with signal first set after 0.25. Every wait was <=0.1 seconds and the
  exception type matched the requested control.
- Existing executed tests include all supported array endianness/widths, actual
  maximum 2^22 uint32 counter, exact 64MiB frame and over-limit rejection,
  scalar/depth/node/allocation bounds, unsupported callback/object types,
  short reads/writes, codec interruption checks and the four real-process
  partial-reply/nonreading-peer timeout/cancellation controls. Inspected the
  external supervisor's independently owned session/group emergency cleanup.
- `git diff --check`: passed.

No additional actionable S5 findings remain in the reviewed state. Parent later
traffic uses the closed binary codec; only the child bootstrap receives the
initial excluded pickle. Nonblocking header/body reads and writes check control
through partial progress and selector waits. No transport helper is introduced.

S3 public cancellation errors/one operation-wide clock and S4 aggregate cleanup
remain separate open successor gates. The current legacy per-worker joins do
not constitute a shared cleanup budget. This review does not accept platform
matrix, artifact/image, service or final E04 completion proof.

## Re-reviewed file identities (SHA-256)

- `_planner_transport.py`: `ac97ee9e1dd4c8838eeff7b3243dfd97794f3674a9d587bf1ed613390e93cfc8`
- `edge_planner.py`: `319e4fa6c7ff5d8d934fd3a67bd5d24f79ef088c2a01d5f0def1da94ad61488c`
- `tests/test_planner_transport.py`: `75f34e1ace2aa6962ae4a3d7916cabc451d655532cfa6b6c5aa8401709d52429`
- `tests/fixtures/e04_faults.py`: `fed2ec8b0d79ba184d2cfbee86b207d3665a6fd20599093d89b758d11258e37d`
- `tools/run_e04_fault.py`: `b854f756f41d3ae5fdcfe0b4c558d9ca1891189fb33d80dd3b639ac67b746fc0`

Only this review report was written by the reviewer; no product edits, secrets,
remote IMDb access, commits or pushes were performed.
