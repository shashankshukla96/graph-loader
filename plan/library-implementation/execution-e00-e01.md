# E00/E01 execution checklist — 2026-10-16

Scope: this dirty checkout only; preserve all inherited tracked/untracked work.
HEAD at entry: `d53a92cfef652b7aae5aca6472736c10e851db42`; branch
`codex/pypi-library-checklist`. No applicable AGENTS.md found in repository or
ancestor directories. No E02–E12 implementation, push, publication or remote
IMDb access is authorized by this slice.

| Story | Prerequisite / acceptance | Work and affected files | Checks / evidence | State |
| --- | --- | --- | --- | --- |
| E00.F1.S1 | Current snapshot; safe tracked/untracked manifest, fixture mutation changes identity; new full baseline, each skip explained | Evidence tooling/records under this plan; retain prerequisite source/tests | Ruff, pip check, full unit/example suite, diff check; E00.F1.S1.md | Complete; independently accepted |
| E00.F2.S1 | E00.F1.S1; exact real-service DLQ case passes without skip, owned cleanup | Maintained fixture and integration test/conftest only if a defect is reproduced | Exact named integration command, graph/DLQ/parking/query-log assertions; E00.F2.S1.md | Complete; independently accepted |
| E01.F1.S1 | Read-only name/command checks; human approves identity, authorized owner/public contact | decisions.md and decision brief | Dated PyPI/TestPyPI/local evidence; E01.F1.S1.md | Complete; user approved D01 |
| E01.F1.S2 | E01.F1.S1; human approves D02/D03 including contracts and format table | Decision brief/register; current schema/planner/retry/group source | Explicit API/errors/types/CLI, ordering and compatibility choices; E01.F1.S2.md | Complete; user approved D02–D05 |
| E01.F2.S1 | E01.F1.S2; human approves D04/D05 | Dependency/eager import and support inventory | Actual requirements, installed versions, server/image prerequisites; E01.F2.S1.md | Complete; user approved D02–D05 |
| E01.F2.S2 | E01.F2.S1; human approves D06/D07 and owners | Image/backend/publication choices in brief/register | No workflows or publication; E01.F2.S2.md | Complete; delegated choices independently reviewed |

Commands from repository root:

```sh
.venv/bin/python -m ruff check src tests examples
.venv/bin/python -m pip check
.venv/bin/python -m pytest tests examples/imdb/tests -m "not integration and not smoke" -q
git diff --check
.venv/bin/python -m pytest tests/integration/test_multi_topic_relationship.py::test_dead_letter_arrival_stream_type_wide_dlq_drained_by_the_retry_consumer -m integration -v
```

Risks: Docker Desktop is installed but its daemon is initially stopped; service
startup may fail or skip and that is never B3 success. Unit forks retain A5
warnings. Availability is not name reservation. Proposed release choices cannot
be inferred from plan review or silence. A broader choice requires replan and
independent review before expanded work. Independent execution review follows
evidence capture, with correction/retest loops for findings.

Ownership/rollback: no resets, stashes, cleanup of inherited files or remote
resources. Testcontainers owns only newly created services and Ryuk resources;
the test owns its fleet, retry child, topics, lease server and temporary files.
Starting Docker does not authorize stopping unrelated containers. Store large
run logs outside the repository; commit only safe result summaries and manifest
metadata, never .env, credentials, ignored data or operational logs. Manifest
mutation proof changes a temporary fixture copy, never the real fixture. New
execution files can be removed individually; any necessary source repair must
be narrowly documented and preserve the inherited diff.

Final gate: E00 accepted by independent execution review 01; E01 has four complete stories: delegated GHCR registry and Shashank image/release ownership recorded; independent completion checkpoints verify the final records and validation. Approved policy remains unchanged. See evidence records and handoff-e02.md.
