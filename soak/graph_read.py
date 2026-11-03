"""Paginated, unbounded-row-count reads of the scratch graph, sorted so they can be merged against the model.

Nodes are read by key range; relationships by a range of source keys with their relationships expanded (so a hot customer is one
page, never a cut in the middle of its pairs). Temporal values are normalised to UTC instants and ISO dates on the way out.
"""
from __future__ import annotations

from . import spec

PAGE = 2000


class Neo4jGraph:
    def __init__(self, driver, database, *, page=PAGE, timeout=120):
        # ``driver`` may be a zero-argument callable returning the current driver: a Neo4j restart replaces the stack's driver.
        self._driver, self.database, self.page, self.timeout = driver, database, page, timeout

    @property
    def driver(self):
        return self._driver() if callable(self._driver) else self._driver

    def _read(self, query, **params):
        from neo4j import Query

        # An auto-commit read: a Query object (server-side timeout) is only accepted by ``session.run``.
        with self.driver.session(database=self.database, default_access_mode='READ') as session:
            return [dict(record) for record in session.run(Query(query, timeout=self.timeout), **params)]

    def nodes(self, label):
        """(key, normalised properties) in key order. A repeated key is yielded twice so the caller can report it."""
        node = spec.NODES[label]
        last = ''
        while True:
            rows = self._read(f'MATCH (n:{label}) WHERE n.{node.key} > $last RETURN n.{node.key} AS k, properties(n) AS p '
                              'ORDER BY k LIMIT $n', last=last, n=self.page)
            if not rows:
                return
            for row in rows:
                yield row['k'], normalise_observed(node.types, row['p'])
            if len(rows) < self.page:
                return
            last = rows[-1]['k']

    def relationships(self, rel, a_label, b_label):
        """(source key, target key, normalised properties) in (source, target) order."""
        rspec = spec.RELS[rel]
        a_key, b_key = spec.key_of(a_label), spec.key_of(b_label)
        last = ''
        while True:
            sources = self._read(f'MATCH (a:{a_label}) WHERE a.{a_key} > $last RETURN a.{a_key} AS k ORDER BY k LIMIT $n',
                                 last=last, n=self.page)
            if not sources:
                return
            upper = sources[-1]['k'] if len(sources) == self.page else None
            bound = upper if upper is not None else sources[-1]['k']
            rows = self._read(f'MATCH (a:{a_label})-[r:{rel}]->(b:{b_label}) WHERE a.{a_key} > $last AND a.{a_key} <= $upper '
                              f'RETURN a.{a_key} AS a, b.{b_key} AS b, properties(r) AS p ORDER BY a, b', last=last, upper=bound)
            for row in rows:
                yield row['a'], row['b'], normalise_observed(rspec.types, row['p'])
            if upper is None:
                return
            last = upper

    def counts(self):
        out = {}
        for label in spec.NODES:
            out[label] = self._read(f'MATCH (n:{label}) RETURN count(n) AS c')[0]['c']
        for rel in spec.RELS:
            out[rel] = self._read(f'MATCH ()-[r:{rel}]->() RETURN count(r) AS c')[0]['c']
        return out


def normalise_observed(types: dict, props: dict) -> dict:
    """Observed property values in the model's canonical form; an undeclared property is kept (and so compares different)."""
    out = {}
    for name, value in props.items():
        out[name] = spec.normalise(types[name], value) if name in types else value
    return out
