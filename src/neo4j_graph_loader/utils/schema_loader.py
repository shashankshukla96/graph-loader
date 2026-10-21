"""
src/neo4j_graph_loader/utils/schema_loader.py
──────────────────────────
Utility for loading and validating the graph schema YAML file.

All pipeline components must obtain the schema exclusively through
``load_schema()``. This ensures a single parse + validation point and
gives callers a fully typed ``GraphSchema`` object.

Runtime file consumers use this function. Public callers may also construct or
validate ``neo4j_graph_loader.schema.GraphSchema`` directly; that path retains
Pydantic ValidationError rather than the file-loading SchemaLoadError.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from neo4j_graph_loader.models.schema import GraphSchema


class SchemaLoadError(Exception):
    """Raised when the schema YAML cannot be read or fails Pydantic validation.

    Read, decoding, YAML and Pydantic exceptions
    are chained via ``raise ... from exc`` so callers can inspect the
    root cause via ``e.__cause__``.
    """


def load_schema(path: str | os.PathLike) -> GraphSchema:
    """Load, parse, and validate the graph schema YAML file.

    Parameters
    ----------
    path:
        Absolute or relative file-system path to the YAML schema file
        (e.g. ``"config/graph_schema.yaml"``).

    Returns
    -------
    GraphSchema
        A fully validated, typed schema object.

    Raises
    ------
    SchemaLoadError
        If the file does not exist, cannot be parsed as YAML, has a
        non-mapping root (including empty files), or fails Pydantic
        validation. The original exception is chained via ``raise ... from``
        so callers can inspect the root cause.

    Examples
    --------
    >>> schema = load_schema("config/graph_schema.yaml")
    >>> print(len(schema.nodes))
    2
    """
    try:
        resolved = Path(path).resolve()
    except (OSError, RuntimeError) as exc:
        raise SchemaLoadError(f"Failed to resolve schema path: {exc}") from exc

    # ── 1. File existence check ───────────────────────────────────────────────
    try:
        is_file = resolved.is_file()
    except OSError as exc:
        raise SchemaLoadError(f"Failed to inspect schema at {resolved}: {exc}") from exc
    if not is_file:
        raise SchemaLoadError(f"Schema file not found: {resolved}") from FileNotFoundError(str(resolved))

    # ── 2. YAML parse ─────────────────────────────────────────────────────────
    try:
        with resolved.open("r", encoding="utf-8") as fh:
            raw: dict = yaml.safe_load(fh)  # typed — keeps mypy/Pyright happy downstream
    except (OSError, UnicodeError) as exc:
        raise SchemaLoadError(f"Failed to read schema at {resolved}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise SchemaLoadError(
            f"Failed to parse YAML at {resolved}: {exc}"
        ) from exc

    # ── 3. Root type guard ────────────────────────────────────────────────────
    # yaml.safe_load returns None for empty files, a list for YAML sequences,
    # or a scalar for bare values — all are invalid schema roots.
    if not isinstance(raw, dict):
        raise SchemaLoadError(
            f"Schema file must be a YAML mapping at the root level, "
            f"got: {type(raw).__name__}"
        ) from ValueError("schema root must be a mapping")

    # ── 4. Pydantic validation ────────────────────────────────────────────────
    try:
        return GraphSchema.model_validate(raw)
    except ValidationError as exc:
        raise SchemaLoadError(
            f"Schema validation failed for {resolved}:\n{exc}"
        ) from exc
