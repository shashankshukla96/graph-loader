# E02 execution checklist — 2026-10-16

Prerequisites: E00/E01 complete and independently accepted. Preserve the dirty
checkout at d53a92cfef652b7aae5aca6472736c10e851db42; no resets/stashes/pushes.
No applicable AGENTS.md found in ancestors/repository. E03–E12 are outside this
assignment. Artifacts are local development scaffolds, never upload candidates.

| Story | Acceptance / surfaces | Validation / review |
| --- | --- | --- |
| E02.F1.S1 | setuptools metadata, explicit new-package selection, standalone version, dedicated honest README/license, inert scaffold; legacy source untouched | Isolated wheel/sdist build, strict metadata/archive inventory, clean unrelated-cwd import, sdist rebuild; Agent checkpoint 1 before next story |
| E02.F1.S2 | pyproject authoritative eight unchanged runtime bounds; deterministic requirements generate/check; dev/IMDb/build tools outside base; matching Rust/driver | Parity negative control, isolated base resolver/install/pip check, no extras chosen; Agent checkpoint 2 |
| E02.F2.S1 | metadata/runtime/default metrics version equal; explicit deployment overrides; file-version sdist-safe/image policy | Fresh wheel/sdist-wheel imports, default/override metrics labels, affected/full checkout regression; Agent checkpoint 3 and final adversarial acceptance |

Risks: generic legacy `src` is a real package, so backend selection and sdist
allowlist must exclude it, plan/tests/private data. Checkout metrics cannot
silently require an editable install of the scaffold. Runtime changes invalidate
old source evidence; manifest must include every new input. Rust/driver pairing
is provided by extension metadata and must be resolver-verified. No tested
platform/mode promise follows from metadata. Existing fork warnings remain A5.

Build tools live in requirements-build.txt and are installed into a throwaway
build virtualenv; isolated backend selection is in pyproject. Package version
authority is `_version.py`; development 0.1.0a1.dev0 is distinct from E11's
approved initial release target 0.1.0a1. No Git/runtime metadata lookup.

Cleanup/rollback: own only newly created temporary build/install/resolve dirs;
leave inherited work and remote services untouched. Generated build/egg-info is
ignored, raw logs remain in owned /tmp evidence directories; safe summaries and
hashes go under evidence/. Revert a repair only by its own narrow change.
No service/IMDb run or publication is required by E02.

All three story checkpoints are independently approved in reviews/e02-checkpoints.md.
The final fresh baseline has 3,380 passes, four inherited skips, 49 deselected
and 19 inherited fork warnings; 147 focused version/metrics/runtime cases pass.
Canonical E02 statuses are complete. The fresh no-history adversarial review in reviews/e02-final-adversarial.md
approved the completed slice without actionable findings. E03 remains unstarted.
