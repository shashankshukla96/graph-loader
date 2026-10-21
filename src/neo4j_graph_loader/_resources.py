"""Finite bundled schemas; materialized paths belong to the enclosing command."""
from contextlib import contextmanager
from importlib.resources import as_file, files
from pathlib import Path

SCHEMAS = {"default": "graph_schema.yaml", "social": "social_schema.yaml"}


@contextmanager
def schema_path(config: str | None = None, *, sample: str = "default"):
    if config is not None:
        yield Path(config)
    else:
        resource = files("neo4j_graph_loader").joinpath("data", SCHEMAS[sample])
        with as_file(resource) as path:
            yield path
