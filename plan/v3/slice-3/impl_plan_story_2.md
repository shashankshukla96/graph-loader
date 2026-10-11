# Plan — Slice 3 Story 2: APOC Locked Batches

Extend `edge_execution.py` with `ApocLockedEdgeWriter(driver, edge_config, *,
missing_endpoint_error_factory: Callable[[str], Exception])`, `write(record)`,
and `write_batch(records)`; row shape is `{source_key,target_key,properties}`.
It uses type-only EdgeRecord imports; Story4's EdgeLoader integration passes its
existing `MissingRelationshipEndpointError` as the explicit factory, avoiding an
import cycle while retaining the Slice 1 public failure type. One transaction
preflights every row then runs this
verbatim schema-derived query:
```cypher
UNWIND $rows AS row
MATCH (s:`<source>` {`<source-key>`: row.source_key})
MATCH (t:`<target>` {`<target-key>`: row.target_key})
WITH collect({s:s,t:t,props:row.properties}) AS rels, collect(s)+collect(t) AS all_nodes
CALL { WITH all_nodes UNWIND all_nodes AS n WITH DISTINCT n AS dist_n ORDER BY id(dist_n) RETURN collect(dist_n) AS sorted_nodes }
CALL apoc.lock.nodes(sorted_nodes)
UNWIND rels AS rel MERGE (rel.s)-[r:`<type>`]->(rel.t) SET r += rel.props
```
Both preflight and lock/MERGE consume before commit; missing endpoints means no
APOC query/commit. Driver stays caller-owned. Test two shared-endpoint rows,
global distinct ordered lock set query, consume-before-commit, preflight failure,
driver failure, injected missing-endpoint exception type, and two edge types.
