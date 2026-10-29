# Independent E08 Neo4j/Rust floor review — 2026-10-27

**Disposition: approve the narrow paired correction to `neo4j>=6.0,<7` and
`neo4j-rust-ext>=6.0,<7`, with exact minimum lane targets Neo4j 6.0.0 and Rust
extension 6.0.0.0. No actionable finding remains in this floor decision.** Keep
the approved PyYAML 6.0.1 correction and every other direct floor unchanged.
All eight runtime dependencies remain mandatory, with no extras or advertised
pure-driver fallback. This approval is not E08.F1.S1 acceptance, E08 completion,
server compatibility proof or support-matrix acceptance.

The reviewer started with a task-only brief and no preceding implementation
conversation. Reviewed D05 in [decisions](../decisions.md), the
[execution plan](../execution-e08.md), the earlier
[PyYAML floor review](e08-dependency-floor-review.md), dependency metadata,
application imports/taxonomy, existing tests and the retained evidence below.
Only this review was written. No dependency metadata, constraints, application
code or evidence was modified; no environment file, service or remote IMDb
access occurred. Upstream public documentation was read for corroboration.

## Actual failure and minimum candidate

- The original installed minimum lane remains red. The retained
  `var/e08-validation-20261026/cells-arm64-minimum-v2/cells/bare/observation/public-3.stderr.log`
  shows `ImportError: cannot import name 'ConnectionPoolError'` from the installed
  Neo4j 5.20.0 package, reached through `edge_loader` → `write_outage` →
  `write_errors`. Both `write_errors.py` and `retry_consumer.py` import that class.
  The successful earlier dependency-only installation therefore did not establish
  application compatibility; the earlier PyYAML-only review explicitly limited
  its approval to installability and did not run the project wheel or imports.
- The downloaded official PyPI distributions and URL/hash records in
  `driver-floor-discovery/result.json` and `later-5x.json` were inspected directly.
  The reviewer recomputed and matched every recorded archive hash. The sampled
  5.20.0–5.28.8 releases lack both `ConnectionPoolError` and
  `ConnectionAcquisitionTimeoutError`; 6.0.0 defines both, with acquisition timeout
  inheriting from pool error. The samples do not claim exhaustive patch testing.
  Existing regression fixtures also use `ClientError._hydrate_neo4j`, absent in
  the inspected 5.20.0–5.25.0 files and present in 5.26.0 and the inspected later
  files. Raising only to 5.26 would still leave the pool imports broken.
- This is a behavioral dependency, not merely a missing import name. In the
  retained 5.20.0 `_sync/io/_pool.py`, acquisition exhaustion raises generic
  `ClientError`; the retained 5.28.8 implementation does the same and includes
  a TODO to change it for 6.0. The 6.0.0 implementation raises the dedicated
  `ConnectionAcquisitionTimeoutError`. The existing classifier deliberately
  classifies typed pool failures as connectivity, which prevents row bisection
  and row accounting for infrastructure failure.
- `candidate-metadata.json` and the actual candidate `resolution.json` agree that
  `neo4j-rust-ext==6.0.0.0` requires `neo4j==6.0.0`. The matching extension publishes
  CPython 3.12 Linux arm64 and amd64 wheels. These are artifact availability
  observations; only the arm64 candidate was executed. The upstream
  [Rust extension release metadata](https://pypi.org/pypi/neo4j-rust-ext/6.0.0.0/json)
  also explains the matching driver-version convention.
- `linux-candidate-driver6-v1/result.json`, `install.log` and `resolution.json`
  retain an actual new bare `/tmp/base` virtual environment with default
  `system_site_packages=False`, on CPython 3.12.14 native Linux aarch64. Isolated
  pip jointly installs all eight exact direct targets and the existing strict
  project wheel. The other targets remain Kafka 2.5.0, Pydantic 2.7.0, PyYAML
  6.0.1, Docker 7.1.0, NumPy 1.26.0 and Prometheus client 0.20.0. `pip check`
  succeeds. All 57 application modules import; an actual 6.0.0 pool error
  classifies as connectivity; the driver uses the actual native Rust packer and
  successfully packs a value. The public example reports schema, planning,
  cancellation, deadline and resource-reclamation success.
- The candidate install log hash matches the receipt. The receipt exits 0,
  records no timeout or leftover owned processes/cleanup errors, and records
  the removed container as absent. The installed inventory and resolution report
  agree on the exact candidate versions. The installed project wheel still
  advertises the old 5.20 lower bounds: it is pre-edit discovery evidence, not
  proof of rebuilt final metadata or artifacts.

## Why the paired correction is justified

A broad `ClientError` alias would classify allow-listed row failures as
connectivity before reaching the row branch. Deleting or ignoring pool errors
would remove the explicit infrastructure classification. A local placeholder
class would never match the actual generic exception raised by 5.x. A message
adapter or private driver interception would require a separately specified,
tested compatibility contract; none is proposed or demonstrated here. Thus a
safe 5.x compatibility implementation has not been established. It is not
necessary to introduce one merely to preserve an inaccurate advertised floor.

The existing implementation already requires the 6.0 exception taxonomy. The
earliest demonstrated coherent pair supplying it is 6.0.0/6.0.0.0. Raising both
lower bounds to 6.0 is the smallest justified metadata correction in the
reviewed candidates while preserving existing error semantics and the `<7`
fences. D05 explicitly allows justified, independently reviewed minimum-floor
corrections during E08. No other runtime floor increase is supported by this
evidence.

## Required continuation and limits

Regenerate runtime requirements, select the exact paired minimum constraints,
rebuild strict artifacts and refresh their immutable inventory explicitly.
Preserve every original 5.20 failure and superseded receipt. Then rerun the bare
installed-client proof and the separate full frozen installed regression lanes
using the final artifacts, finite wheelhouse and exact recorded constraints.
The discovery probe does not replace those lanes, real Neo4j 5.26 Enterprise
service proof, warning/process/transport cells or final independent acceptance.

Neo4j's [upgrade documentation](https://neo4j.com/docs/python-manual/current/upgrade/)
describes 6.x server compatibility and breaking changes. The documented major
upgrade and successful import/example probe do not establish every application
path; the full minimum regression and service gates remain necessary. Native
Linux amd64 is unavailable and remains an explicit open release blocker under
D04. Arm64 success, wheel availability or labeled emulation cannot close it.
