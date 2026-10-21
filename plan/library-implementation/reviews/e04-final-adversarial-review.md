# E04 final independent adversarial review

Verdict: **accepted for the E04 implementation and final tested evidence**.
No actionable finding remains in the reviewed state. This fresh zero-history
review inspected the actual complete tracked/untracked E04 delta, rather than
using the six story acceptance labels as proof. Review completed 2026-10-20
against `codex/pypi-library-checklist`, entry/current implementation HEAD
`98cd267db5c70d3517a89ec68c1a4cedf2c254fd`, with the intended E04 working tree.
Only this report was added by the reviewer; implementation/tests were not edited.

The full suite and final safe evidence were initially pending, so acceptance
was withheld until their successful completion and independent identity checks.
Canonical status updates, final plan rendering/link checks, staged inspection
and local delivery/content-equality evidence follow this implementation review.
They must record the actual delivery hashes without inventing tested commits.

## Review scope and behavioral conclusions

Read the E04 handoff, canonical stories, D01–D07 and approved E01 API table,
cancellation contract, execution plan, full actual source/test/tool/documentation
delta and new files. Inspected ownership, start/finish/close, partial startup,
source/task binding, inline and process error causes, framing/codec, heartbeat,
metrics, import boundaries, artifact validator and fault supervisors.

The explicit public modules and finite exports match the approved schema and
planning surface, including cancellation errors. Reexports retain canonical
class identities and the source base remains concrete. The guide accurately
distinguishes convenience mapping inline defaults from factory/sequence spawn
defaults, mandatory Kafka/dependencies and future console/resources work.
Schema read/decode/YAML/model failures retain causes; direct models retain
Pydantic validation. Inline ordinary source failures retain the actual cause;
process failures retain remote type/message/traceback in a local diagnostic
cause without arbitrary exception unpickling.

Parent transport reads and writes are nonblocking complete-frame operations.
Header/body partial progress and later-command/stop backpressure check the
supplied signal and absolute deadline through at-most-100ms selector slices.
The parent never decodes initial pickles; only child bootstrap does so. Later
traffic uses exact approved builtins, bounded numeric arrays and the owned
ChunkSpec type, with explicit frame/scalar/array/node/depth/blob-allocation
limits and before/during/after codec checks. No receiver helper is introduced.
Interrupted outbound frames cannot acquire a trailing stop frame.

The process operation clock anchors after all initial submissions; inline
scanning anchors before first iteration. Gather/broadcast, phases, callbacks,
assembly/results and the start/finish gap share that clock. Cooperative inline
checks occur between returning records. Construction/submission, arbitrary
blocking caller/Kafka callbacks and OS-uninterruptible exclusions are explicit.

Cleanup uses one separate aggregate ten-second schedule across workers and
heartbeat waits, with all relevant workers signalled before joins and shared
stop/grace/terminate/kill cutoffs. Parent endpoints close even on interrupted
delivery. Housekeeping/endpoint/inline cleanup failures do not bypass remaining
worker cleanup. Unknown/live owners retain lock/spill/topic state and PID/path
diagnostics; outer finish appends rather than erasing startup cleanup evidence.
Repeated close does not silently grant another cleanup budget. Caller topics
and signals remain caller-owned. The original S5 endpoint-construction and S4
cleanup/marker findings and their reproductions remain in the story reports;
the actual repaired paths and regression assertions were inspected here.

Fresh public/CLI import controls preserve signals, logging, environment, threads
and start methods while forbidding connection/server/process creation. Repeated
public calls use distinct private metrics registries without initializing the
internal singleton. CLI's internal planner still receives its explicit metrics
owner. The conditioned E05 handoff preserves these contracts and does not start
E05 or authorize publication.

## Independent executed checks

Interpreter: `var/e04-validation-20261018/dev/bin/python` (fresh installed
CPython 3.12 environment). Independently executed:

```text
python -m pytest
  tests/test_public_cleanup.py::test_unreapable_fake_workers_share_one_budget_and_keep_live_owned_state
  tests/test_public_cleanup.py::test_cleanup_action_failure_still_reaps_children_and_retains_owned_topic
  tests/test_public_cancellation.py::test_public_process_waits_observe_errors_under_an_external_supervisor
  tests/test_planner_imports.py -q -rs
24 passed in 5.11s; exit 0
```

This includes timeout/cancellation for real blocked sources, partial replies and
later-command backpressure under their independent hard supervisors; fake
unreapable ownership combinations; progress/endpoint/spill cleanup failures;
fresh import and repeated-call metrics controls. No real-process fault was run
without its external supervisor/emergency owner.

Additional independent probes passed 1,000 deterministic nested codec round
trips (seed 91804) and four extra malformed payloads: nested unhashable mapping
key, invalid UTF-8, invalid array byte length and truncated ChunkSpec. All four
failed as FrameProtocolError. Identity probes verified actual current runtime
bytes against both artifact lanes and all four image inventories, actual
artifact hashes, every retained final-results log hash, all 259 current
allowlisted manifest inputs and byte equality of six raw/safe evidence copies.
The supplemental result is retained at ignored
`var/e04-validation-20261018/reviewer-final-probes.json`.

One initial reviewer evidence-inspection script used the nonexistent JSON key
`import_result` and exited with KeyError before running its remaining probes.
The corrected script used the actual `imports` schema and all probes passed;
this was a reviewer script error, not a runtime or evidence failure.

## Final proof and exact identities

Reviewed [final results](../evidence/e04-final-results.json),
[before manifest](../evidence/e04-final-before.json),
[final source manifest](../evidence/e04-final-source.json),
[artifacts](../evidence/e04-artifacts.json),
[artifact commands](../evidence/e04-artifact-commands.json),
[images](../evidence/e04-images.json),
[image commands](../evidence/e04-image-commands.json) and the actual raw logs.
All copied proof matches the raw retained results byte for byte. Identities:

- Full-suite frozen source: `fc8209594b422e25b9a2f49b69c5ca1be3388bee4a3a49230ac552a87fd93b9a`.
- Final source: `1d04f2e6ffa3ee09bcfec4d8e82a2621131cee1717c60f7fce54fd44e690b05c`.
- Environment: `921610492c278551bfe4b511a52ec637cf7799b784fdf6ee832a91482d97f349`.
- Final manifest file SHA-256: `67fa1340a07bee92a580944aac77b9c5aaee7962d4262cb62269015250f28479`.
- Current `edge_planner.py`: `e9dca64614546e35a244b8b1a03fbbca32e8bebb35a41ded367eca174bd1980b`.
- Current `_planner_transport.py`: `00b9f7fbe67149d61522bad11627b800cf1a7b379676e769ebb2a4f56446126b`.

The before/after manifest difference is solely `docs/LIBRARY_API.md`'s corrected
source-default wording. All runtime, build, test and fixture inputs are equal.
The final guide's SHA-256 is
`7cd357739c4facf05e1e0795ed359e0e6a93ae5aa58418edccc5a1a9c79c3757`.

The required full command over `tests examples/imdb/tests` with
`-m "not integration and not smoke" -q -rs` completed exit 0: **3,512 passed,
four inherited skips, 49 deselections, 19 known fork warnings in 187.36s**.
The skips retain their bash-specific SHELLOPTS, two subset-not-Git-checkout and
intentional multi-pair-example reasons. Fork warnings retain A5/E08 ownership.
The recorded Ruff, dependency check, requirements parity and whitespace checks
also passed. This is fresh current-source proof, not the E03 count.

Isolated original sdist/wheel, no-Git extracted-sdist wheel rebuild and strict
Twine passed. Every original/rebuilt wheel member is byte-equal; both wheel
archives themselves hash to
`d32faa92813f9d9f0de9e2749fe23c0a2df282c00ec96d787a82e94fc674bd08`.
The original sdist hashes to
`89b9e6e8e970906b0dbbdbcc5aa666635d1e529c3877aa26846bb8192d63e1bd`.
Both clean noneditable environments live outside the checkout and verify all
52 runtime module bytes/site-packages origins, metadata/dependencies/absence of
development tools and old namespace, eight component helps and self-contained
public success/error/cancel/deadline/import probes without path rescue.

All four newly built local image matrices passed 13 checks each, using their
immutable IDs with `--network none`, no host source mounts and no PYTHONPATH:

| Component | Immutable image ID |
| --- | --- |
| CLI | `sha256:ed13d74ff98b86cc05e8ff1803ce2f5048f0e1d86fce8e6f77f75f80bcf16a0e` |
| Node | `sha256:e2ce0493f6b93e5ce793c205a72ace5eb170976c71e0d31dcb7601c1385fb946` |
| Edge | `sha256:099c318e9994cbf762c5ac1f9ebb1e2975402cb9a429c9cf7cae3ef8a4c1f347` |
| Clock | `sha256:9e909ade689dbe3e96ac1527f2d6219c6b2d37c8331034dfe780810d8c89f992` |

These check all 52 module bytes/origins, eight component helps, default help,
pip check and both public/import fixtures. Original Docker-stopped build/B3
failures remain retained and labelled; successful retries replace no history.

Exact B3 used the explicit fresh interpreter and maintained owned supervisor:
`test_dead_letter_arrival_stream_type_wide_dlq_drained_by_the_retry_consumer`
**passed once, no skip, in 24.08s**; supervisor exit 0, no timeout, 24.662s,
empty before/after container inventory. Inspected its actual unchanged semantic
assertions: both source topics consumed into the shared type-wide DLQ before
nodes exist, exact final relationship properties, retry committed offsets equal
DLQ high watermarks, every input dead-lettered, >100 shared pairs, widest DLQ
partition count, no parked/per-topic DLQ state and actual zero lock-wait writes.
The test captures owned logs before fixture cleanup. No IMDb benchmark was used
as a substitute, and no remote IMDb/foreign locks/resources/global prune was used.

## Limits and delivery follow-through

Acceptance is for this exact E04 state. Installed smoke probes do not establish
E07's full installed regression lane. Native macOS arm64 and local Linux arm64
images do not establish E08's architecture/start-method/max-worker matrix or
E09's complete service matrix. No console executable, packaged resources,
prebuilt registry delivery, upload, push, tag or release is claimed or authorized.
Existing callback/submission/OS exclusions and unresolved-owner manual recovery
remain explicit. Final documentation/status/render/link and local delivery
changes should retain per-file equality with these tested runtime inputs; any
runtime/test/build correction requires its applicable retest/review proof.

## Independent delivery closure recheck — 2026-10-20

**Accepted for local delivery.** Rechecked the 56 staged intended E04 files,
canonical status changes, resolution index, final evidence and conditioned E05
handoff. E00–E04 are complete; all six E04 stories are complete; E05–E12 remain
unstarted. The staged inventory contains no private environment, runtime data,
logs or ignored validation artifacts. No unstaged implementation edits existed
before this report append. The pending implementation commit and subsequent
tracker/content-equality proof remain accurately described as pending.

Independently compared every reviewed and delivered manifest row, ignoring only
the expected untracked-to-index-tracked metadata changes. The sole content
difference is one removed ASCII trailing space on `docs/LIBRARY_API.md:133`.
Reinserting that exact byte into the delivered guide reproduces the earlier
reviewed hash `7cd357739c4facf05e1e0795ed359e0e6a93ae5aa58418edccc5a1a9c79c3757`.
The normalized guide hashes to
`fa3da8eb0011cf43cd7a12a796181b96ff71318be12b606cb078d46e22a7d75b`.
The initial staged whitespace exit 2 is retained in the final-results
normalization entry; the corrected staged whitespace check passed exit 0.
Every runtime/build/test/fixture byte and executable mode remains unchanged,
so the passing code/artifact/image/service evidence remains applicable.

All 259 current input contents/sizes/modes and their staged bytes match the
[delivery source manifest](../evidence/e04-delivery-source.json), source identity
`cefa79c72880b3b4c7ba06931b3d5b38bf3141839a9d06e65336dac3f28a4e85`, with
the unchanged environment identity recorded above. Inspected the successful
renderer evidence for 13 epics, 26 features, 54 stories and 48 coverage items.
An independent current local-link check covered 87 Markdown files and 453 links
with none missing before this append. Rerun final links/staged whitespace after
staging this report append and any delivery bookkeeping; no implementation
correction or further code-suite rerun is required by this closure review.
