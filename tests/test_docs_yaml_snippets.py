"""v7-S6 story 6.4: every ```yaml snippet in the user-facing docs loads through ``load_schema``.

A snippet is classified by its shape and completed with the minimum it lacks, so a doc cannot show a schema
(or an edge, loading or dead_letter fragment) the loader would refuse:

* a mapping with ``nodes`` and ``edges``: a whole schema, loaded as written;
* a list of edges (or a mapping with only ``edges``): spliced into a schema whose node labels are derived from the
  edges' ``nodes`` and key properties, and (v7-S7) from every ``topics`` mapping entry that binds its own pair;
* a mapping with only ``loading``: merged into a minimal schema;
* a mapping with only ``dead_letter``: attached to a minimal single-topic edge.

Any other shape fails the test, so a new kind of snippet must extend this classifier on purpose.
"""
from __future__ import annotations

from pathlib import Path
import re

import pytest
import yaml

from src.utils.schema_loader import load_schema

ROOT = Path(__file__).resolve().parents[1]
DOCS = [
    "README.md", "docs/USAGE.md", "docs/MONITORING.md", "docs/BULK_LOAD.md", "docs/INCREMENTAL_LOAD.md",
    "examples/README.md", "examples/imdb/README.md", "examples/social/README.md", "examples/multi_topic/README.md",
]
FENCE = re.compile(r"```yaml\n(.*?)```", re.S)


def _snippets() -> list[tuple[str, int, str]]:
    found = []
    for relative in DOCS:
        text = (ROOT / relative).read_text(encoding="utf-8")
        for match in FENCE.finditer(text):
            found.append((relative, text.count("\n", 0, match.start()) + 1, match.group(1)))
    return found


SNIPPETS = _snippets()


def _node(label: str, key: str) -> dict:
    return {"label": label, "topic": f"{label.lower()}-events", "key_property": key,
            "properties": {key: {"type": "string", "required": True}}}


def _pairs_of(edge: dict) -> list[dict]:
    """The top-level pair (if any) and every ``topics`` mapping entry's pair (v7-S7)."""
    pairs = [edge] if "nodes" in edge else []
    pairs += [entry for entry in edge.get("topics") or () if isinstance(entry, dict)]
    return pairs


def _schema_for_edges(edges: list[dict], loading: dict | None = None) -> dict:
    nodes: dict[str, dict] = {}
    for edge in edges:
        for pair in _pairs_of(edge):
            for side, key_name in (("source", "source_key_property"), ("target", "target_key_property")):
                label, key = pair["nodes"][side], pair[key_name]
                nodes.setdefault(label, _node(label, key))
    return {"loading": loading or {}, "nodes": list(nodes.values()), "edges": edges}


def _complete(data: object) -> dict:
    if isinstance(data, list) and data and all(isinstance(item, dict) and "type" in item and _pairs_of(item)
                                               for item in data):
        return _schema_for_edges(data)
    if isinstance(data, dict):
        keys = set(data)
        if {"nodes", "edges"} <= keys:
            return data
        if keys == {"edges"}:
            return _schema_for_edges(data["edges"])
        if keys == {"loading"}:
            return {"loading": data["loading"], "nodes": [_node("Thing", "id")], "edges": []}
        if keys == {"dead_letter"}:
            edge = {"type": "LINKS", "topic": "links-events", "nodes": {"source": "Thing", "target": "Thing"},
                    "source_key_property": "id", "target_key_property": "id", "dead_letter": data["dead_letter"],
                    "properties": {"since": {"type": "date"}}}
            return _schema_for_edges([edge])
    raise AssertionError(f"unclassified yaml snippet shape: {type(data).__name__} {sorted(data) if isinstance(data, dict) else ''}")


def test_the_docs_contain_yaml_snippets() -> None:
    assert len(SNIPPETS) >= 7
    assert any("topics:" in text for _, _, text in SNIPPETS), "the multi-topic docs must keep a snippet"


@pytest.mark.parametrize("relative,line,text", SNIPPETS, ids=[f"{r}:{n}" for r, n, _ in SNIPPETS])
def test_yaml_snippet_loads(relative, line, text, tmp_path) -> None:
    schema = _complete(yaml.safe_load(text))
    path = tmp_path / "snippet.yaml"
    path.write_text(yaml.safe_dump(schema), encoding="utf-8")
    load_schema(path)


def test_the_docs_show_a_type_with_several_label_pairs() -> None:
    assert any("name: likes-comments" in text for _, _, text in SNIPPETS)


def test_a_snippet_with_an_unknown_topic_entry_key_fails(tmp_path) -> None:
    """The classifier completes a snippet; it must not hide an invalid mapping entry."""
    bad = [{"type": "LIKES", "properties": {"at": {"type": "date"}}, "order_by": "at",
            "topics": [{"name": "a", "nodes": {"source": "A", "target": "B"}, "source_key_property": "id",
                        "target_key_property": "id", "partitions": 3},
                       {"name": "c", "nodes": {"source": "A", "target": "C"}, "source_key_property": "id",
                        "target_key_property": "id"}]}]
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(_complete(bad)), encoding="utf-8")
    with pytest.raises(Exception, match=r"unknown key\(s\) \['partitions'\]"):
        load_schema(path)
