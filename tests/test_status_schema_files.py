"""E15.F1.S1/S3: the published JSON Schema files under docs/status-schemas equal the Python source (checkout only: they read
the docs tree, so the file runs from the checkout and not from an installed wheel)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from neo4j_graph_loader.status import ValidationError, schemas
from neo4j_graph_loader.status.validate import validate

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "docs" / "status-schemas"
STRICT_EVENT = schemas.STRICT_EVENT_SCHEMA


def test_generated_schema_files_equal_the_python_source():
    for name, schema in schemas.schemas().items():
        assert json.loads((SCHEMA_DIR / name).read_text()) == schema, name
        assert (SCHEMA_DIR / name).read_text() == schemas.render(schema)


def test_the_published_schema_files_still_equal_the_source():
    for name, schema in schemas.schemas().items():
        assert json.loads((Path(__file__).resolve().parents[1] / "docs" / "status-schemas" / name).read_text()) == schema
    with pytest.raises(ValidationError):
        validate({"seq": 1}, STRICT_EVENT)
