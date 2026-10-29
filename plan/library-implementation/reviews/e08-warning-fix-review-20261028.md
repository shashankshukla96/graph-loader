# Independent E08 warning-repair review — 2026-10-28

**Disposition: accept the bounded example warning repair. No actionable finding remains in the reviewed four-file change. This is not acceptance of E08.F2.S1, the full process/native support envelope, E08 or delivery.**

The reviewer received a focused task brief, inspected the current diff against HEAD `4b246f3ab953bd9f7d266a682ac9fff39bd32032`, and read the approved [execution plan](../execution-e08.md), [S1 acceptance](e08-s1-global-review-20261028.md) and [S2 work-in-progress evidence](../evidence/E08.F2.S1.md). No applicable AGENTS.md was found in the checkout or ancestor chain. Scope is `examples/imdb/build_subset.py`, its two affected test modules and `examples/imdb/README.md`. Only this report and new ignored audit outputs were written; no source change, SSH, Docker operation, service/data access, private environment-file read, staging or commit was performed.

## Implementation and failure review

The builder now uses explicit private `multiprocessing.get_context("spawn")` contexts for person selection and source conversion. Importable top-level selectors and `functools.partial` replace the fork-dependent local tasks. The normal CLI entry remains guarded. No global start-method setter, warning filter, runtime dependency, thread receiver or application planner/transport implementation change was introduced.

The parent-selected `CHUNK_ROWS` is passed to the person-selection task and each source-conversion task. The title selection remains in the parent and reads its current configuration. Each converter, column tuple and output-kind tuple is passed explicitly from the current `SOURCES` entry, rather than depending on a spawned interpreter inheriting patched module globals. The existing serial path continues to accept local closures; parallel callers must supply importable, picklable functions/configuration as documented.

The injected crew failure remains an actual `ValueError("broken crew chunk")` raised by the converter in a worker. Moving that converter to module scope makes it serializable. The original `pytest.raises(RuntimeError, match="(?s)title.crew.tsv.*broken crew chunk")` remains unchanged, so a startup/pickling error cannot satisfy its intended source-error assertion. Worker exceptions still return their traceback to the parent, which fails closed. Early parent title-selection failures terminate/join the selection worker. Failed process startup closes both pipe ends; normal result collection closes the receiver and joins the child; source-derivation cleanup closes remaining receivers and terminates/joins remaining tracked workers. This inspection and the successful tests do not establish new deadlines for arbitrary hung converters or faulted transports; those are outside this bounded repair.

Output-producing converters, hashing, deduplication, graph file definitions and traversal rules are unchanged. The existing byte-for-byte serial/parallel reference test still exercises chunk size 2, and all prior collision, JSON/quoting, endpoint filtering and manifest assertions remain. The sole planner-test edit changes its explicit context from fork to spawn while preserving its success, source-failure and marker assertions.

## Independent preservation and execution checks

An AST comparison against HEAD confirms all 53 original subset-test assertions and all 62 original planner-test assertions are retained in order. All original test functions remain. The subset module adds exactly one test and three assertions; the planner module adds no assertions. Independently recomputed all 59 application payload hashes against the current inventory with no mismatch. The retained `inventory-before-warning-fix.json` and current inventory are exactly equal; the newly edited example source has deliberately not been refrozen during S2 work in progress. This review does not turn that stale source inventory into final equality proof.

The added real-child test records the custom crew converter's observed chunk lengths and child start method, then invokes the normal converter. It proves multiple nonempty chunks of at most two rows, actual spawn execution, successful passage of the custom converter, and preservation of the caller's initialized global start method. The isolated run also succeeds, so that test does not depend on an earlier parallel test. Explicit alternate/uninitialized caller-method controls remain part of the broader S2 work.

Inspected the retained runner: the audit hook is installed before importing pytest; a live benign parent thread spans pytest execution; the existing external process-group supervisor owns the hard timeout and cleanup; plugin autoload is disabled. The audit blocks the private root dotenv file and the identified IMDb host. Independently verified every referenced log hash below, zero exits, no timeout, no cleanup errors/leftover-owned receipt entries, no blocked audit attempts, and a terminated fixture thread.

| Retained run under `var/e08-process-20261028/` | Actual observation |
| --- | --- |
| `warnings-before-v1` | 26 passed, 19 deprecation warnings: 15 subset and 4 planner |
| `warnings-after-v1` | 27 passed with `-W error::DeprecationWarning` |
| `warnings-after-v2` | 27 passed with the same warning error gate |
| `spawn-config-isolated-v1` | 1 passed, 26 intentionally deselected by the focused selector |
| Independent `warning-fix-review-v1` | 27 passed in 6.54 seconds with the warning error gate |

The independent execution used the existing audited helper with a new output label:

```text
var/e08-validation-20261026/dev/bin/python -B var/e08-process-20261028/run_warning_checks.py warning-fix-review-v1 -W error::DeprecationWarning
```

`git diff --check` passes for all four reviewed files. No claim of full checkout, installed or native retesting follows from this local focused run.

## Copy costs and remaining gates

The README correctly replaces the former shared-fork memory claim with explicit per-worker ID/configuration copies plus serialization buffers and the `jobs=1` option. The 310-second/4.5-GiB figures are clearly attributed to the historical fork implementation. No new full-IMDb performance or memory measurement is claimed. Spawn also serializes the selected people result back to the parent; the generic serialization-buffer caveat applies to that selection phase as well. Tiny TSV tests cannot establish production-scale capacity.

The reviewed source snapshot is:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `examples/imdb/build_subset.py` | 41111 | `0e09e397924ffb21e38eea58e70a1920ab6b4e1ed24269370acb8215003f46fe` |
| `examples/imdb/tests/test_build_subset.py` | 26670 | `c9f61ca0b63b93a4bd26b333cc711efd922412ef3a6f3f8477dcc6952ccc0934` |
| `examples/imdb/tests/test_planned_edges.py` | 26759 | `dd99603c20c8a000819a14470b405f3b7f4d253c0ddfa4c9e3aca27e2059b17c` |
| `examples/imdb/README.md` | 22826 | `dc53661511b6edaf944bd5a131c7c0730bf3a459b5c7004ac8ccf83e6311f20a` |

S2's installed external-source spawn/inline clients, explicit pickling failures, worker identities, caller-method controls, framed transport/fault/cleanup cells, worker envelope and required native observations remain open. Fresh full S2 acceptance and all final inventory, checkout, archive, dual-wheel, installed, image, live-service, R1, B3, adversarial-review and delivery gates remain required under the approved execution plan. This focused acceptance supplies no publication, push, tag, service or later-epic authorization.

## Documentation-only ID-preservation follow-up — 2026-10-28

**Disposition: accept the README reflow and relocation of detailed spawn notes. The bounded code acceptance above remains unchanged.** The retained `var/e08-process-20261028/readme-before-id-repair.md` exactly matches the previously reviewed README's 22826 bytes and SHA-256 `dc53661511b6edaf944bd5a131c7c0730bf3a459b5c7004ac8ccf83e6311f20a`. Compared that snapshot and HEAD with the current README: the introductory conversion paragraph returns to its original line count, and the detailed memory/embedding caveats move to the document end. Every original fenced-block line number is restored, including the unchanged YAML block at line 253. The spawn copying/buffer costs, serial alternative, picklability restriction, caller-method preservation, historical-fork attribution and absence of production-scale measurements remain explicit. The note now also explicitly accounts for the selected people result serialized back to the parent.

The current README is **22969 bytes**, SHA-256 **`c3e75b692df29cb2a10bf1caaadabaf252b4baa3686895738f656f783b2af0ab`**; this supersedes only its earlier snapshot row. Independently rehashed the builder and both test modules: their sizes/hashes still exactly match the accepted rows above. No executable retest was necessary for this prose-only follow-up.

Independently compared the retained collection records with the 3841-ID pre-repair inventory. `source-collection-v1.json` records the unintended replacement of `tests/test_docs_yaml_snippets.py::test_yaml_snippet_loads[examples/imdb/README.md:253]` by the line-263 ID. The collection command itself exited zero; its resulting ID set correctly fails the preservation requirement. The original collection, log, audit and command receipt remain retained. `source-collection-v2.json` restores every previous ID in order and adds exactly 15 IDs (the accepted child-configuration test and 14 external-client cases), for 3856 total. Verified both collection log hashes, successful collection exits, no timeout or cleanup error and audits installed before pytest with no blocked attempts. Current inventory retains every prior per-ID disposition and the complete original source/assertion baseline. This updates the earlier work-in-progress observation that inventory had not yet been refrozen; it is not final tested/committed/current equality proof.

The additional external-client implementation and its behavior were not reviewed in this follow-up. Collection preservation is not their runtime acceptance, full S2 acceptance or closure of the remaining native/transport/final gates.
