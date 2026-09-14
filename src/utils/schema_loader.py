"""
src/utils/schema_loader.py
──────────────────────────
Utility for loading and validating the graph schema YAML file.

All pipeline components must obtain the schema exclusively through
``load_schema()``. This ensures a single parse + validation point and
gives callers a fully typed ``GraphSchema`` object.

Do **not** import ``GraphSchema`` directly and call
``GraphSchema.model_validate()`` from application code or tests —
use this module instead.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from src.models.schema import GraphSchema


class SchemaLoadError(Exception):
    """Raised when the schema YAML cannot be read or fails Pydantic validation.

    The original exception (``yaml.YAMLError`` or ``pydantic.ValidationError``)
    is always chained via ``raise ... from exc`` so callers can inspect the
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
    resolved = Path(path).resolve()

    # ── 1. File existence check ───────────────────────────────────────────────
    if not resolved.is_file():
        raise SchemaLoadError(f"Schema file not found: {resolved}")

    # ── 2. YAML parse ─────────────────────────────────────────────────────────
    try:
        with resolved.open("r", encoding="utf-8") as fh:
            raw: dict = yaml.safe_load(fh)  # typed — keeps mypy/Pyright happy downstream
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
        )

    # ── 4. Pydantic validation ────────────────────────────────────────────────
    try:
        return GraphSchema.model_validate(raw)
    except ValidationError as exc:
        raise SchemaLoadError(
            f"Schema validation failed for {resolved}:\n{exc}"
        ) from exc
