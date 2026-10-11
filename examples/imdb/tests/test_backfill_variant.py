"""v7-S6 story 6.3: the README's IMDb multi-topic variant loads, and the default schema is untouched."""
from __future__ import annotations

from pathlib import Path
import re

import yaml

from src.utils.schema_loader import load_schema

IMDB = Path(__file__).resolve().parents[1]


def _readme_variant() -> dict:
    text = (IMDB / "README.md").read_text(encoding="utf-8")
    section = text.split("### Variant: a backfill topic", 1)[1].split("\n### ", 1)[0]
    snippet = re.search(r"```yaml\n(.*?)```", section, re.S).group(1)
    return yaml.safe_load(snippet)[0]


def test_default_imdb_schema_stays_single_topic() -> None:
    schema = load_schema(IMDB / "graph_schema.yaml")
    assert all(not edge.is_multi_topic for edge in schema.edges)
    assert next(e for e in schema.edges if e.type == "HAS_GENRE").topics == ["imdb-has-genre"]


def test_readme_backfill_variant_replaces_has_genre_and_loads(tmp_path: Path) -> None:
    data = yaml.safe_load((IMDB / "graph_schema.yaml").read_text(encoding="utf-8"))
    variant = _readme_variant()
    data["edges"] = [variant if edge["type"] == "HAS_GENRE" else edge for edge in data["edges"]]
    path = tmp_path / "graph_schema_backfill.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    schema = load_schema(path)
    edge = next(e for e in schema.edges if e.type == "HAS_GENRE")
    assert edge.topics == ["imdb-has-genre", "imdb-has-genre-backfill"]
    assert edge.conflict_rule == "precedence"
    assert edge.precedence_rank("imdb-has-genre-backfill") == 1
