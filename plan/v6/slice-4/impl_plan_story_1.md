# Story 1 implementation plan: star-topology dataset generator

Files:
- `tests/stress/__init__.py` (new, empty package marker)
- `tests/stress/star_topology_data.py` (new)
- `tests/test_star_topology_data.py` (new, unit, no Docker)

## `star_topology_data.py`

Mirrors `examples/social/social_graph.py`'s shape but scoped to one hub:

- `Dataset` dataclass: `nodes: dict[str, list[dict]]`, `edges: dict[str, list[tuple[object, object, dict]]]`
  (identical shape to `social_graph.Dataset`).
- `schema_dict(topology: str) -> dict`: builds the loading/nodes/edges dict in
  the exact YAML shape `load_schema` accepts (verified against
  `examples/social/graph_schema.yaml` and `src/models/schema.py`'s
  `NodeConfig`/`EdgeConfig`/`SourceTargetConfig` fields):
  - `target_hub`: nodes `Hub` (key `h`, string), `Leaf` (key `leaf_id`,
    integer); edge `POINTS_AT` (source Leaf, target Hub), property `seq: integer`.
  - `source_hub`: nodes `Hub`, `Leaf`; edge `POINTS_FROM` (source Hub, target
    Leaf), property `seq`.
  - `multi_type_hub`: nodes `Hub`, `Leaf`; edges `LIKES`/`FOLLOWS`/`RATED`/
    `VIEWED`/`SHARED` (all source Leaf, target Hub), each with property `seq`.
  - `loading.mode: bulk`.
- `generate(hub_edges: int, topology: str, seed: int = 13, *, repeat_fraction: float = 0.01) -> Dataset`:
  deterministic `random.Random(seed)`; one `Hub` node `{"h": "H"}`; `hub_edges`
  distinct `Leaf` nodes with integer keys `0..hub_edges-1` (shuffled key
  order is not needed here — the identifiers themselves are already
  disjoint); for `target_hub`/`source_hub`, one edge record per leaf with
  `seq` incrementing per source leaf (starting at 0); a `repeat_fraction`
  (~1%) of leaves get a *second* record with the same pair and a higher
  `seq`, to make last-write-wins collapse checkable (Story 4/6's
  `_distinct_pairs`). For `multi_type_hub`, split `hub_edges` across the five
  types via `hub_edges // 5` plus remainder distributed to the first types so
  every type gets at least 1 record even at `hub_edges=1000`.
- `expected_graph(dataset) -> tuple[dict, dict]`: same `_canonical`/last-
  write-wins merge convention as `social_graph.expected_graph` (`merged.update(...)`,
  never `merged = ...`).
- `last_write_evidence(dataset) -> dict`: same shape as `social_graph.last_write_evidence`.
- `wire_value`/`node_record`/`edge_record`: same wire-format helpers as
  `social_graph.py` (dates/datetimes as ISO strings; here only int/str keys
  are used, so this is mostly a straight port).
- `publish(dataset, bootstrap, *, edge_topics: dict[str, str], partitions=4, create=True) -> dict`:
  imports `confluent_kafka` INSIDE the function body only (never at module
  scope) — this is the AC that guarantees `tests.stress.star_topology_data`
  imports with `confluent_kafka`/`neo4j` removed from `sys.modules`.
  Publishes only edge records (node seeding is direct Cypher per Decision 5,
  not this helper) to each edge type's topic, keyed by source id (string).
- `distinct_pairs(dataset, edge_type) -> set[tuple]`: reduces one edge type's
  rows to unique `(source, target)` pairs — used by Stories 4-7 for the exact
  final-count assertion.

No Neo4j/Kafka import at module scope anywhere in the file.

## Unit test file

`tests/test_star_topology_data.py`, no markers (runs in the fast suite):
- AC: `generate(1000, "target_hub")` → exactly 1000 `POINTS_AT` records, 1 Hub node.
- AC: `generate(1000, "source_hub")` → exactly 1000 `POINTS_FROM` records, same source key throughout.
- AC: `generate(1000, "multi_type_hub")` → 1000 records total across 5 types, every type >= 1.
- AC: last-write-wins collapse — generate with a known `repeat_fraction`,
  assert `len(expected_graph(...)[1]["POINTS_AT"])  == len(distinct_pairs(...))`
  and that this is strictly less than the raw record count.
- AC: import-safety — a test that pops `confluent_kafka`/`neo4j` out of
  `sys.modules` (restoring afterward) and re-imports the already-imported
  module's `publish` is not called, only that importing the module itself
  doesn't need those packages: implemented via `importlib.reload` under
  `unittest.mock.patch.dict(sys.modules, {"confluent_kafka": None, "neo4j": None})`. A
  `None` entry in `sys.modules` makes any `import confluent_kafka` inside the
  module raise `ImportError` at that statement — proving the module itself
  doesn't import it at load time.
- AC: every function above is unit-tested at small scale (hundreds).
