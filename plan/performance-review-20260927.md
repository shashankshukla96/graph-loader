# IMDb throughput review — 2026-09-27

## Update after independent executor and endpoint-preflight experiments — 2026-09-28

Both candidates were tested separately against the 2,000-row edge baseline on the same fresh 10k-seed subset, with four successful ABBA runs per candidate and graph-count verification after every load. [Persistent executor reuse](../examples/imdb/reports/edge-executor-reuse-benchmark/comparison.md) averaged **377.717 s** versus **374.145 s** baseline. The 3.572 s (1.0%) slower mean is smaller than run-to-run variation, so there is **no demonstrated improvement**; that code was removed. [Aggregate endpoint preflight](../examples/imdb/reports/edge-aggregate-preflight-benchmark/comparison.md) averaged **334.263 s** versus **375.600 s** baseline, a **41.337 s (11.0%) load-time reduction** and 12.4% higher input throughput. All 14 edge stages improved on average, and the final graph matched in every run. That change is retained in source and the latest edge image. The preflight still rejects any batch with missing or ambiguous endpoints before a relationship write commits.

The checked-in IMDb edge outer batch remains 2,000. The 4,000-row batch gave a separate 4.1% gain, but its combination with aggregate preflight has not been measured. The current measured improvement is the aggregate query at 2,000 rows. Larger and incremental loads remain to be checked before extrapolating these timings.

The retained configuration processes approximately 17,583 input records per loading second on this subset (5,877,378 / 334.263). Person-related mean edge lifecycles still sum to about 273.372 s, or 81.8% of total loading lifecycle; these types remain serialized with one another. The user reports 550 tests passed and four skipped. Follow-up inspection checked the comparison reports and aggregate query/result checks, without rerunning the suite or a load. The four busy SMB deletion files were left untouched.

## Update after the 2,000-versus-4,000 edge batch comparison

The [four-run ABBA comparison](../examples/imdb/reports/edge-outer-batch-2000-vs-4000/comparison.md) held the node outer batch at 500, each edge writer lane transaction limit at 1,000, one edge consumer/four writers, the same images, and the same fresh 10k-seed input. Each trial began with an empty Neo4j graph and cleared IMDb Kafka input topics, succeeded, and produced the expected final graph. Loading lifecycle averaged **381.737 s at 2,000** and **365.969 s at 4,000**: **15.768 s (4.1%) less time**, equivalent to **4.3% higher input throughput**. End-to-end time fell 3.7%. Edge flushes fell from 2,323 to 1,165 while node flushes remained 2,515. All seven Person-related edge types improved on average, and their serialized mean lifecycle sum fell from 309.7 s to 287.2 s.

The 4,000-row setting was consistently faster in this subset, but the additional gain is modest relative to the earlier 500-to-2,000 change. **Keep 2,000 as the checked-in IMDb default** pending validation on a larger or incremental workload. The independent executor and endpoint-validation experiments appear above. These runs do not isolate Neo4j server time from client and Kafka work.

## Update after the edge-only outer batch comparison

The [four-run ABBA comparison](../examples/imdb/reports/edge-outer-batch-benchmark/comparison.md) held node batches at 500, one edge consumer/four writers, the 1,000-row lane limit, updated node prefetch, Rust extension, and the same fresh 10k-seed graph input. Edge outer batches of 2,000 averaged **371.592 s** loading lifecycle versus **574.664 s** at 500, a **35.3% wall-time reduction** across two successful runs per setting. End-to-end time averaged 402.945 s versus 606.046 s. Edge flushes fell from 9,274 to 2,323, and all 13 shared edge lifecycle timings improved in both repetitions. Interrupted attempts during host sleep and a report write failure were excluded.

An independent `loading.edge_unwind_batch_size` setting and `--edge-batch-size` CLI override now isolate edge tuning from node batches. The IMDb example defaults to 2,000; other schemas retain the existing `unwind_batch_size` fallback. Per-type lifecycle logging now includes the isolated `EPISODE_OF` path for future reports. The 4,000-row follow-up and current recommendation appear above.

Recalculation from the four reports gives approximately 10,228 versus 15,817 input records per loading second, a 54.6% throughput increase (using the displayed millisecond timings). The seven Person-related lifecycles average 517.388 s at 500 and 305.059 s at 2,000. Since they cannot overlap each other, their remaining duration is about 82.1% of the 2,000-row loading lifecycle. This reinforces the priority of optimizing the remaining edge path. These lifecycle measurements include startup and drain, unlike the successful-write sums in the original audit. The user reports 546 tests passed and four skipped; this follow-up review inspected code and reports without rerunning tests or changing the loaded graph.

## Update after the controlled node-prefetch comparison

The [10k-seed A/B comparison](../examples/imdb/reports/node-prefetch-benchmark/comparison.md) supersedes the original priority assigned to node prefetch below. The code now keeps prefetch active for ordinary node writes and pauses on the retry path. The user reports all 82 focused tests passing; this follow-up review did not rerun them.

Both successful runs used identical inputs (1,243,246 nodes and 4,634,132 relationships), empty graph/input-topic starting states, unchanged edge images, and Rust extension 6.3.1.0. Loading lifecycle time changed from 612.388 s to 619.274 s: 6.886 s (1.1%) slower. This demonstrates **no end-to-end speedup in this pair**, and one run per setting does not establish a repeatable regression. The failed initial reset/load is excluded as documented in the comparison.

The local fetch-stall finding remains valid, but it did not establish the pipeline's critical bottleneck. My original recommendation ranked that fix too strongly as an end-to-end throughput opportunity. Keep the tested behavior as the baseline and move performance investigation to edges.

The raw successful-write logs also show a substantial edge timing shift: KNOWN_FOR processed the same 201,376 records in 403 flushes, but summed flush time rose from 25.210 s to 61.342 s. Median flush time rose from 62.829 ms to 151.174 ms, and p95 from 76.469 ms to 201.386 ms. This is not explained by one isolated slow batch. Across all seven Person-related types, non-overlapping successful flush intervals total 442.523 s in the baseline and 476.497 s in the treatment (72.3% and 76.9% of lifecycle time). Changed overlap/contention or different server conditions are hypotheses; these logs cannot distinguish them or prove a hidden node-stage improvement. Summed timing deltas must not be subtracted from lifecycle time to estimate causal savings.

At that stage, the proposed next experiment was an edge-only outer batch comparison, requiring a separate setting and per-type lifecycle measurements. That experiment and instrumentation are now implemented. Executor reuse and aggregate preflight have also been tested independently; their results and retention decisions appear at the top of this review.

The sections below retain the original audit evidence. Hardware, image, and code descriptions refer to that initial inspection unless explicitly updated above.

## Scope and evidence

Reviewed the three requested reports, their successful-write logs, current loader/orchestrator code, and the existing edge parallelism comparison. Performed small read-only Kafka and Neo4j probes. No graph data, topic data, or production consumer offsets were changed. No full load was rerun, and no implementation files were edited.

The working tree already contained changes to edge execution, schema initialization, and their tests. Findings refer to the current working tree; historical reports do not record source or image digests, so exact historical implementation equivalence cannot be established.

| Run (UTC) | Outcome | Published graph records | End to end | Input records/s | Loading lifecycle records/s |
| --- | --- | ---: | ---: | ---: | ---: |
| 00:42:29 | Failed | 11,033,996 | 583.275 s | Not a completed-load rate | Not comparable |
| 02:13:45 | Succeeded | 13,728,667 | 1,456.732 s | 9,424 | 9,901 |
| 02:45:33 | Succeeded | 16,158,488 | 1,692.057 s | 9,550 | 10,038 |

All three specify four Kafka partitions, four node consumers per label, one edge consumer per type, and four edge writer threads. None measures the reported eight-consumer configuration. The two successful datasets differ, so they are throughput observations rather than a controlled configuration comparison. Counts are input records processed, not necessarily distinct relationships created by MERGE.

Current local hardware: Apple M5 Max, 18 physical/logical cores, 36 GiB RAM. Docker Desktop reports 18 CPUs, aarch64, and 7.75 GiB RAM. The edge image is arm64. These observations show CPU visibility and native architecture; they do not establish historical CPU saturation or memory pressure. No workload was running during inspection.

Current remote Neo4j identifies as 5.26.31. All 18 inspected indexes are ONLINE (14 RANGE, two TEXT, two LOOKUP); index state alone does not prove optimal query plans. Twenty warm read-only RETURN 1 queries from the host had median elapsed time 3.071 ms. This includes driver/server work and is not a raw network RTT or loaded-server measurement.

## 1. Remove unconditional Kafka pause/resume in ordinary node flushes

At the original inspection, `src/loader/node_loader.py:572` paused every assigned partition before writing a batch and resumed after committing its offsets. The ordinary edge bulk path already avoided this pattern (`src/loader/edge_loader.py:1075`). This node behavior has since been changed and benchmarked; see the update above.

Reproduced the effect with manually assigned partition 0 of `imdb-alternative-title`: six batches of 500 messages per trial, a simulated 50 ms processing interval between reads, excluding the first read from the median. Each trial used a unique diagnostic group, disabled auto commit and auto offset storage, never subscribed, and never committed. The container probe ran in the current node image on Docker's default bridge; the named graph-loader network was absent. The first attempted container was removed.

| Probe | Continuous fetch, median next 500 | Pause/process/resume, median next 500 |
| --- | ---: | ---: |
| Host, first pair | 0.20 ms | 947.80 ms |
| Host, repeated pair | 0.23 ms | 946.00 ms |
| Current node image | 0.23 ms | 951.64 ms |

This demonstrates an avoidable delay in the consumption pattern, not an end-to-end speedup estimate. The synchronous node poll owner cannot process a second batch while writing the first. Keep broker prefetch active, preserve ownership/rebalance checks and commit-after-durability, and retain polling during retries. Verify shutdown, rejection durability, retry, and revocation behavior with the existing tests.

This can improve both node completion and the time when dependent edge types become eligible. AlternativeTitle has 1.707 million records and 3,416 batches across four replicas in the latest run, making repeated per-batch stalls a substantial concern. Do not multiply this diagnostic delay by all batches and claim the result as wall-clock savings: replicas and edge work overlap.

## 2. Increase outer edge batch size before adding consumers

The example sets `loading.unwind_batch_size: 500`. Its runtime schema sets both lane count and worker count to four. Every outer flush routes those 500 records into source-hash/direction lanes and immediately calls `drain_all()`.

Consequently, a four-lane flush commonly produces roughly 125 records per transaction when one direction dominates, and can split across up to eight directional lanes. Actual occupancy depends on endpoint values and ordering. The per-lane batch limit defaults to 1,000 but cannot create larger transactions from an outer buffer of only 500. Raising only the per-lane limit will not address this.

In the latest run there were 25,628 successful edge flushes. Each occupied lane executes an explicit transaction with endpoint preflight, another endpoint lookup for the locked MERGE, and commit. Small transactions multiply remote requests and local Bolt encoding/decoding.

Benchmark outer sizes 500, 2,000, and 4,000 at one consumer/four writers, retaining a 1,000-row lane limit initially. Treat 2,000 as the first candidate, not a proven optimum. Larger batches can increase lock overlap and transaction duration, especially for shared targets. The outer setting currently also changes node batches; introduce separate node/edge settings if independent tuning is required.

## 3. Reduce work done for every edge transaction

In `src/loader/edge_execution.py`:

- `LaneExecutionCoordinator.execute()` creates and shuts down a ThreadPoolExecutor on every flush: 25,628 pool lifecycles for the latest successful run. Reuse a bounded pool for the coordinator lifetime, with explicit shutdown and unchanged lane ordering/failure handling.
- `_preflight_query()` returns one result row per edge, and the writer materializes all results in Python. For the latest run that is 12.812 million preflight result rows. Return an aggregate validation result instead, preserving the requirement that every input resolves exactly one source and one target.
- A later optimization can combine validation and locked writing to avoid duplicate endpoint lookups and sending row parameters twice. It must still fail and roll back the entire transaction for missing/ambiguous endpoints; silently dropping unmatched rows is unacceptable. Benchmark the query plan and transaction behavior before adopting it.
- Specify a configurable target database in sessions. Neo4j recommends this to avoid default-database discovery work; actual benefit depends on driver/protocol caching.

The originally inspected node image used Neo4j Python driver 6.3.1 without the Rust extension. Both subsequent node-prefetch benchmark variants include Rust extension 6.3.1.0, so its effect cannot be inferred from that comparison. Neo4j recommends it for faster driver processing; advertised driver speedups are not predictions for this complete pipeline. See [Neo4j driver performance guidance](https://neo4j.com/docs/python-manual/current/performance/).

## 4. Tune concurrency for endpoint contention

The lanes hash the source endpoint; they do not guarantee disjoint target endpoints. `apoc.lock.nodes()` explicitly obtains write locks on every deduplicated endpoint in each transaction. More lanes or consumers can therefore compete for the same target locks. Neo4j documents these locks in [APOC lock documentation](https://neo4j.com/docs/apoc/current/overview/apoc.lock/apoc.lock.nodes/).

The latest dataset has only ten TitleType, 28 Genre, 45 Profession, 80 Language, and 173 Region nodes. These are natural contention candidates. HAS_TITLE_TYPE's mean outer-flush time was 141.0 ms despite only 200,000 input records. This is consistent with contention but does not measure lock wait directly.

Benchmark one or two writers for these shared-target types and four writers for larger endpoint spaces. The example currently applies one global writer/consumer setting to every edge; allow per-edge overrides rather than applying eight everywhere. Do not remove explicit locks merely to obtain a faster benchmark; any replacement needs a correctness argument and concurrency tests.

Existing evidence in `examples/imdb/reports/edge-benchmark/comparison.md`: on the same smaller dataset with A/B/B/A ordering, one consumer/four writers averaged 171.708 s versus 192.936 s for four consumers/one writer. The former took 11% less time. This supports the current baseline, but does not quantify the missing eight-consumer run or guarantee the result at larger scale.

## 5. Bulk scheduling limits available parallelism

Phase 4's streaming clock is not active in these finite bulk runs. `_run_completion_driven_bulk_edges()` in `src/cli.py` reserves whole endpoint labels and releases them after drain/zero-lag verification; edge containers are launched with `slot_gating=False`.

All seven Person-related types therefore execute without overlapping one another. Their recorded successful flush durations sum to:

- Latest run: 1,176.909 s (19.62 minutes), 73.1% of the 1,609.725 s loading lifecycle.
- Previous successful run: 1,043.928 s, 75.3% of its 1,386.589 s lifecycle.

These are client-observed flush intervals, including routing/coordinator/driver work, not pure Neo4j server execution. Because these types cannot overlap, their sum is a meaningful serialized component. Summing all edge types would overcount concurrent work.

In the latest run CREDITED_ON, ACTED_IN, and PLAYED alone account for 802.734 s of these serialized flushes. Focus edge profiling there. Label-level scheduling makes the number of host cores a poor predictor of throughput.

Longer term, bulk-specific scheduling using proven disjoint endpoint buckets could overlap types sharing a label. This is a larger coordination change: preserve leases, in-flight barriers, offset safety, and isolation. Do not simply enable conflicting types or change the streaming slot duration. The existing native-disjoint mode is capability-gated and should not be selected as a configuration-only fix for this Neo4j 5.26 server.

## Benchmark sequence and required measurements

Use an identical subset and database starting state for each comparison; separate initial imports from MERGE replays. Record code/image digests, dataset checksum, effective settings, and remote server configuration. Repeat promising comparisons in alternating order.

1. Updated baseline: four partitions, four node consumers per large label, one edge consumer, four writers, node outer batch 500, edge outer batch 2,000, ordinary node prefetch enabled, Rust extension installed, aggregate endpoint validation enabled, and a new executor per flush.
2. Retain the added per-type lifecycle timestamps; measure lane transaction sizes, commit duration, and remote lock/CPU behavior alongside subsequent experiments.
3. If continuing batch tuning, compare 2,000 and 4,000 with aggregate validation enabled in both variants, holding node batches at 500 and the lane transaction limit at 1,000. Repeat in ABBA order; only this combination remains unmeasured. Do not assume the separate 4.1% and 11.0% gains add or multiply.
4. Validate the retained baseline on a larger subset and an incremental/replay workload, measuring those scenarios separately. Executor reuse showed no gain and was removed; aggregate validation is retained. Revisit either experiment only if new profiling identifies a reason.
5. Compare one/two writers on shared-target types.
6. Revisit additional edge consumers only after measuring the remaining bottleneck.

Capture timestamps and durations for node/edge launch, readiness, Kafka fetch, decode/validation, lane occupancy, each transaction, offset commit, and drain. Edge lifecycle timestamps are now available; the older successful-flush timing files alone cannot reconstruct idle gaps or attribute all elapsed time. Measure local container CPU/RSS and Docker VM pressure concurrently with remote Neo4j CPU, lock wait, GC, page-cache behavior, transaction-log I/O, and Kafka request latency. Idle-machine measurements cannot establish historical utilization.

Forty node containers are launched with the current global replica setting, including four each for tiny lookup labels. A per-label replica policy (one for tiny labels, four for large labels) could reduce startup and memory overhead. The example currently overwrites all node replica values; per-label policy needs a runner change. Docker's current 7.75 GiB allocation is worth monitoring, but increasing memory is not yet a measured remedy.

Publication accounts for only 4.7–4.8% of elapsed time in the original successful runs, so publisher tuning has a small end-to-end ceiling. Following the node-prefetch A/B, the immediate priorities are edge transaction overhead and measurements that explain per-type timing changes.
