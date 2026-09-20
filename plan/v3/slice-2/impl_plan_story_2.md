# Implementation Plan — Phase 3 / Slice 2 / Story 2

## Scope

Implement pure deterministic lane routing in `src/loader/mix_and_batch.py`.

## API and algorithm

Add `MixAndBatchPartitioner(edge_config: EdgeConfig)` and
`route(record: EdgeRecord, *, topic: str, partition: int, offset: int) -> RoutedEdgeRecord`.

Create canonical typed endpoint tokens by serializing the two-item list
`[type(value).__qualname__, repr(value)]` with `json.dumps(..., ensure_ascii=True,
separators=(",", ":"))`, then UTF-8 encoding it. Hash with
`hashlib.blake2b(digest_size=16)`, convert using
`int.from_bytes(digest, byteorder="big", signed=False)`, and modulo schema
`lane_count`; never call Python `hash()`. Compare the
tokens lexicographically: `forward` when source <= target, otherwise `reverse`.
Set `lane_id = direction_bit * lane_count + source_bucket`; retain target bucket
only for diagnostics. No I/O, consumer, writer, offset commit, or logging occurs.

## Tests

Prove repeatability across instances, lane/bucket ranges, exact one routed record,
forward/reverse separation, self-edge forward behavior, and exact metadata
preservation. Directly compare canonical tokens/digests for `1` versus `'1'`
(rather than lane IDs, which may collide modulo lane count). Run
`.venv/bin/python -m pytest tests/test_mix_and_batch.py -v`.

## Rollback

Pure in-memory code only; no external side effects exist.
