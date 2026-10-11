"""Shared v7-S7 fixtures: one relationship type (``LIKES``) over two label pairs.

``LIKES`` reads ``likes-posts`` and ``likes-posts-backfill`` (Person->Post, the same pair, so a conflict rule
applies to it) and ``likes-comments`` (Person->Comment). Post and Comment ids can collide on purpose.
"""
from __future__ import annotations

import copy
import re
import threading

from src.models.schema import GraphSchema

POSTS, BACKFILL, COMMENTS = "likes-posts", "likes-posts-backfill", "likes-comments"


def likes_edge(*, order_by: bool = True, topic_conflict: str | None = None, dead_letter: dict | None = None,
               topics: tuple[str, ...] = (POSTS, BACKFILL, COMMENTS), replicas: int = 1,
               execution: dict | None = None) -> dict:
    entries = {
        POSTS: {"name": POSTS, "nodes": {"source": "Person", "target": "Post"},
                "source_key_property": "person_id", "target_key_property": "post_id"},
        BACKFILL: {"name": BACKFILL, "nodes": {"source": "Person", "target": "Post"},
                   "source_key_property": "person_id", "target_key_property": "post_id"},
        COMMENTS: {"name": COMMENTS, "nodes": {"source": "Person", "target": "Comment"},
                   "source_key_property": "person_id", "target_key_property": "comment_id"},
    }
    edge: dict = {
        "type": "LIKES",
        "topics": [copy.deepcopy(entries[topic]) for topic in topics],
        "properties": {"liked_at": {"type": "datetime", "required": True}, "note": {"type": "string"}},
        "replicas": replicas,
    }
    if order_by:
        edge["order_by"] = "liked_at"
    if topic_conflict is not None:
        edge["topic_conflict"] = topic_conflict
    if dead_letter is not None:
        edge["dead_letter"] = dead_letter
    if execution is not None:
        edge["execution"] = execution
    return edge


def nodes(*, comment_key_type: str = "integer") -> list[dict]:
    return [
        {"label": "Person", "topic": "persons", "key_property": "person_id",
         "properties": {"person_id": {"type": "integer", "required": True, "constraint": {"type": "uniqueness"}}}},
        {"label": "Post", "topic": "posts", "key_property": "post_id",
         "properties": {"post_id": {"type": "integer", "required": True, "constraint": {"type": "uniqueness"}}}},
        {"label": "Comment", "topic": "comments", "key_property": "comment_id",
         "properties": {"comment_id": {"type": comment_key_type, "required": True,
                                       "constraint": {"type": "uniqueness"}}}},
    ]


def likes_schema_dict(*, comment_key_type: str = "integer", extra_edges: list[dict] | None = None,
                      loading: dict | None = None, **edge_kwargs) -> dict:
    data = {"nodes": nodes(comment_key_type=comment_key_type),
            "edges": [likes_edge(**edge_kwargs), *(extra_edges or [])]}
    if loading is not None:
        data["loading"] = loading
    return data


def likes_schema(**kwargs) -> GraphSchema:
    return GraphSchema.model_validate(likes_schema_dict(**kwargs))


def likes(**kwargs):
    """The validated multi-pair ``LIKES`` edge."""
    return next(edge for edge in likes_schema(**kwargs).edges if edge.type == "LIKES")


def record(topic: str, person: object, other: object, liked_at: str = "2024-01-01T00:00:00+00:00",
           **properties) -> dict:
    """A wire record of ``topic``: the target key property is the one ``topic`` binds."""
    target_property = "comment_id" if topic == COMMENTS else "post_id"
    return {"source": {"person_id": person}, "target": {target_property: other},
            "properties": {"liked_at": liked_at, **properties}}


# ── An in-memory stand-in for Neo4j that understands the relationship writer's queries ──────────

_ENDPOINT = re.compile(r"MATCH \((s|t):`(\w+)` \{`(\w+)`: row\.(?:source|target)_key\}\)")
_REL = re.compile(r"MERGE \(s\)-\[r:`(\w+)`\]->\(t\)")


def query_pair(query: str) -> tuple[str, str]:
    """``(source label, target label)`` a writer query matches."""
    found = {alias: label for alias, label, _key in _ENDPOINT.findall(query)}
    return found["s"], found["t"]


class GraphStore:
    """Nodes per label (by key), relationships keyed ``(type, S, sk, T, tk)``, and a log of every
    transaction: ``{"pairs": {(S, T), ...}, "rows": [...], "committed": bool, "writes": bool}``."""

    def __init__(self, nodes: dict[str, set] | None = None) -> None:
        self.nodes: dict[str, set] = {label: set(keys) for label, keys in (nodes or {}).items()}
        self.rels: dict[tuple, dict] = {}
        self.transactions: list[dict] = []
        self.lock = threading.Lock()
        # Optional lease bookkeeping: ``resource_of(label, key)`` gives a node's lease token; every
        # open write transaction's tokens are tracked and an overlap between two open transactions
        # (two concurrent holders of one resource, i.e. a possible Neo4j lock wait) is recorded.
        self.resource_of = None
        self.open_tokens: dict[int, set] = {}
        self.overlaps: list = []
        self.max_open_pairs: set = set()
        self.before_write = None  # optional hook(pair) called before a write is staged
        self._open_logs: set = set()

    def driver(self) -> "FakeDriver":
        return FakeDriver(self)

    def committed_writes(self) -> list[dict]:
        return [tx for tx in self.transactions if tx["committed"] and tx["writes"]]

    def _resolves(self, label: str, key: object) -> int:
        keys = self.nodes.get(label, set())
        return int(any(type(k) is type(key) and k == key for k in keys))


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def consume(self):
        return None


class FakeTransaction:
    def __init__(self, store: GraphStore) -> None:
        self._store = store
        self._staged: list = []
        self._log = {"pairs": set(), "rows": [], "committed": False, "writes": False}
        with store.lock:
            store.transactions.append(self._log)
            store._open_logs.add(id(self._log))

    def run(self, query: str, rows: list[dict], order_by: str | None = None, **_params) -> _Result:
        store = self._store
        source, target = query_pair(query)
        self._log["pairs"].add((source, target))
        if "RETURN row_index, source_matches" in query:
            return _Result([{"row_index": i, "source_matches": store._resolves(source, row["source_key"]),
                             "target_matches": store._resolves(target, row["target_key"])}
                            for i, row in enumerate(rows)])
        if "MERGE" not in query:  # aggregate preflight
            invalid = sum(1 for row in rows if not (store._resolves(source, row["source_key"])
                                                   and store._resolves(target, row["target_key"])))
            return _Result([{"checked_rows": len(rows), "invalid_rows": invalid}])
        rel_type = _REL.search(query).group(1)
        if store.resource_of is not None:
            tokens = {store.resource_of(source, row["source_key"]) for row in rows} | {
                store.resource_of(target, row["target_key"]) for row in rows}
            with store.lock:
                for other_id, other in store.open_tokens.items():
                    if other_id != id(self) and tokens & other:
                        store.overlaps.append(sorted(tokens & other))
                store.open_tokens.setdefault(id(self), set()).update(tokens)
                self._log["tokens"] = set(tokens)
                open_pairs = {tuple(tx["pairs"])[0] for tx in store.transactions
                              if id(tx) in store._open_logs and tx["pairs"]} | {(source, target)}
                if len(open_pairs) > len(store.max_open_pairs):
                    store.max_open_pairs = open_pairs
        if store.before_write is not None:
            store.before_write((source, target))
        resolved = [row for row in rows if store._resolves(source, row["source_key"])
                    and store._resolves(target, row["target_key"])]
        invalid = len(rows) - len(resolved)
        guarded = "CALL (rels, all_nodes, invalid_rows)" in query
        writes = resolved if (invalid == 0 or not guarded) else []
        self._log["rows"].extend(writes)
        self._log["writes"] = self._log["writes"] or bool(writes)
        self._staged.extend((rel_type, source, target, row, order_by) for row in writes)
        out = {"checked_rows": len(resolved), "invalid_rows": invalid, "merged_rows": len(writes)}
        if "$order_by" in query:
            out["skipped_as_not_newer_rows"] = 0
        return _Result([out])

    def commit(self) -> None:
        store = self._store
        with store.lock:
            for rel_type, source, target, row, order_by in self._staged:
                key = (rel_type, source, row["source_key"], target, row["target_key"])
                current = store.rels.setdefault(key, {})
                incoming = row["properties"]
                if order_by is not None and incoming.get(order_by) is not None \
                        and current.get(order_by) is not None and not incoming[order_by] > current[order_by]:
                    continue
                current.update(incoming)
            self._log["committed"] = True
        self._staged = []

    def _close(self) -> None:
        with self._store.lock:
            self._store.open_tokens.pop(id(self), None)
            self._store._open_logs.discard(id(self._log))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._staged = []
        self._close()
        return False


class FakeSession:
    def __init__(self, store: GraphStore) -> None:
        self._store = store

    def begin_transaction(self, **_kwargs) -> FakeTransaction:
        return FakeTransaction(self._store)

    def run(self, query: str, **params) -> _Result:
        tx = FakeTransaction(self._store)
        result = tx.run(query, **params)
        tx.commit()
        tx._close()
        return result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeDriver:
    def __init__(self, store: GraphStore) -> None:
        self._store = store

    def session(self, **_kwargs) -> FakeSession:
        return FakeSession(self._store)
