# Reproducing E08 observations

The approved E08 targets are CPython3.12 on Linux arm64 and amd64. Native amd64 was unavailable for the first arm64 review. On2026-10-28
the user provided an SSH runner whose host and Docker daemon report Linux/x86_64;
both minimum/current native sets have completed fresh S1 and S2 observations,
including native-volume export verification. Independent S2 review verifies
all36 current observations,14,688 installed passes/four intentional core skips
and536 process cases across both architectures. Final E08 artifact, service and
delivery results are recorded separately in
[final validation](../plan/library-implementation/evidence/e08-final-validation.md).
No matrix or release acceptance follows from a tool reporting an observation.

The checkout-only tools keep runtime dependencies separate from test and authoring
tools. All eight runtime dependencies, including matched Neo4j/Rust acceleration,
are mandatory. There are no extras or pure-driver fallback. The minimum and dated
current direct inputs are in `config/support-matrix/`. Each target resolves those
inputs jointly with its test tools, then freezes every resolved dependency and
records upstream download URLs/hashes and locally built wheel hashes. Test tools
cannot upgrade the pinned runtime set.

The frozen source IDs were collected with pytest9.1.1. The runner pins that test
tool explicitly:9.2 changes long automatically generated parameter IDs, so otherwise
successful test calls fail the exact-ID receipt. This pin changes no application
dependency, test selection, assertion or skip allowance.

## Native runner preparation

Use a new output directory outside every Git checkout. A portable preparation
bundle contains only the explicit application wheel, frozen test/reference
closure, maintained tools, inventory and runtime pins. It contains no application
source tree, Git metadata, private environment, datasets or architecture wheels.
Its manifest freezes every included byte. Keep instructions and receipts beside
the bundle, so they do not change that manifest.

Create a fresh CPython 3.12 authoring environment with `psutil==7.2.2` and
`packaging==26.3`.
These tools belong to the controller, outside all target environments. Save exact
versions and installation reports. On the actual native target, run:

```sh
python -B /absolute/bundle/scripts/support_matrix.py prepare \
  --pins /absolute/bundle/config/support-matrix/minimum-runtime.txt \
  --resolution-constraints /absolute/bundle/config/support-matrix/minimum-linux-arm64-20261027.txt \
  --wheel /absolute/bundle/neo4j_graph_loader-0.1.0a1-py3-none-any.whl \
  --inventory /absolute/bundle/inventory.json \
  --output /absolute/new-preparation
```

Repeat separately with `current-runtime-20261026.txt`, the matching
`current-linux-arm64-20261027.txt` full lock and a new preparation directory.
Those full locks record versions resolved on arm64; they do not assert amd64 wheel
availability. The native runner resolves/builds its own architecture artifacts
and must retain any failure. The result includes a coherent full constraints file, pip resolution
report, finite wheelhouse manifest, build/download logs and supervised command
receipts. A pure-Python source build is recorded explicitly. Missing wheels or
failed builds remain failures requiring investigation.

Create a separate executable bundle using the preparation's `constraints.txt`,
`wheelhouse/` and `wheelhouse.json`, passed to the same tool's `bundle` command
with `--constraints`, `--wheelhouse` and `--wheelhouse-manifest`. Specify the
explicit wheel, inventory and a new output directory. The tool can run from the
preparation bundle, whose test/reference closure is already complete.

## Offline target observations

Use a finite test-only image containing the executable bundle and authoring
`psutil`/`packaging` installed from its verified wheelhouse. This image is not a
production image or published artifact. Record the base digest, complete build
context manifest and immutable local image ID. Run as an unprivileged user, with
networking disabled, a read-only root filesystem and only a new owned output
directory mounted writable. Never mount checkout source to rescue an import.

The native amd64 Docker Desktop host share allowed package writes despite chmod,
which correctly failed the unchanged resource probe. Use a new labeled native
Linux Docker volume for that target workspace, with a separate owned host export
bind. Initialize volume permissions in the test image before nonroot execution.
Retain the volume across container cleanup until snapshot verification/recovery;
a tmpfs-only workspace can erase partial evidence on a hard termination. Record
its name, owner label and creation identity before launch. Verify actual chmod
rejection and recover a deliberately interrupted artifact before the full run.
The maintained amd64 adapter exports receipts on ordinary success/failure, then
separately verifies every exported byte and actual installed package against the
read-only retained volume. Foreign volumes and global Docker settings are untouched.

Run `observe --bundle /absolute/executable-bundle --architecture arm64
--execution-mode native --output /absolute/new-observation` using the bundle's
`scripts/support_matrix.py`. Omit `--lane` for the bare runtime probe; then run
each of the eight explicit inventory lanes in a separate container/new output.
Use `amd64` only on that actual target, and label emulation `emulated`. Record
host/kernel, daemon and interpreter identities separately; a command-line label
alone is not native-host proof.

The bare target is a new noneditable virtualenv without system/user/development
packages. Before public probes it asserts absence of pytest, testcontainers,
pandas, dotenv, build, twine and psutil; checks all payload bytes and mandatory
dependencies; and exercises the matched active Rust codec. Public imports, CLI
parity, resources, schema and planner examples run with individual hard deadlines
and command receipts. Planner producer doubles establish local API behavior only.

Installed regression targets separately install the frozen test tools offline.
Their existing exact IDs, allowed exceptions, phase assertions and parent/child
origin/creation receipts remain mandatory. An unexpected distribution/version,
skip, missing test, child provenance violation or cleanup error fails the lane.

Use `scripts/matrix_docker.py` from an outer supervised controller for container
ownership. Its fresh CID and owner label bind cleanup to that launch; it retains
inspection/cleanup errors and original interruptions. The outer controller must
exit nonzero when any cell fails. Keep its receipts outside the child writable
mount. Never prune Docker or remove an unrelated object.

E08 process/start-method/transport/worker-bound proof, final artifact/image/service
retention gates and independent story/final reviews remain separate requirements.
Historical E07 totals and partially successful E08 observations do not close them.

## Explicit process envelope

The maintained runner now accepts `--process-envelope` only with an executed
`public-contracts` lane. Pass it explicitly to `support_matrix.py observe` with
`--lane public-contracts`, or to the prescribed `verify_installed.py` command.
It runs the frozen regression lane first, then134 separately supervised process
cases in that same fresh noneditable environment. Each case gets new output,
actual installed child-origin/startup/creation receipts and a separate provenance
log. The target-owned guard configuration is restored byte for byte, including
when a case fails. Ordinary invocations retain their existing selection/behavior.

Normal planning measures requested worker counts1–8,16,32,64 under the private
spawn context and cooperative inline mode. Failure/transport cleanup measures
1,2,4,8,64. These points do not establish timing behavior for unmeasured interior
counts or arbitrary larger API settings. The CLI still accepts1–64 and the API
still accepts positive counts without a new upper bound. Inline creates no
process workers, does not serialize an unused source attribute and cannot
preempt a source that never returns. Other process methods remain unadvertised.
External spawn clients need importable source/producer definitions and a guarded
`if __name__ == '__main__':` entry point. The library preserves the embedding
application's selected global method.

Prepared fault cases exclude a recorded boot/all-intended-peer-activation
interval before the original operation clock. Separate cold cases use the real
unmodified0.8-second deadline and may stop a child before application import;
retain that empty import list with its audited interpreter/startup/payload
identity. Activated fault peers require actual application-import receipts.
A second-start failure launches two actual workers; an early serialization
failure can launch one despite a larger request. Receipts distinguish requested
capacity from actual process workers. A channel EOF can precede a reaped exit
status, so external exit-failure proof binds the activated source's actual dead
process/exitcode37 after cleanup rather than requiring37 in transient error
text. Original public error/cause/marker text remains retained.

Fresh native minimum/current S2 runs are independently accepted in
[the full S2 review](../plan/library-implementation/reviews/e08-s2-full-review-20261028.md).
The tool's complete process-envelope receipt, or any historical S1/Mac result,
does not by itself close E08's final artifact/image/service/delivery gates.
