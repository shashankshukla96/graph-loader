"""Shared scalar normalization for schema-declared graph records.

The node and relationship loaders deliberately share this implementation so
their Neo4j-compatible value semantics cannot drift.  Callers supply the
entity/field context and an error factory to retain their public exception
types.
"""
from __future__ import annotations

from datetime import date, datetime
import math
from typing import Callable

from src.models.schema import PropertyConfig


RecordErrorFactory = Callable[[str], Exception]


def normalize_property_value(
    value: object,
    property_config: PropertyConfig,
    *,
    entity_description: str,
    property_name: str,
    error_factory: RecordErrorFactory,
) -> object:
    """Return a Neo4j-driver-compatible scalar or raise the supplied error.

    ``entity_description`` and ``property_name`` are interpolated only into
    errors; raw input values are intentionally never included in messages.
    """

    def fail(reason: str) -> None:
        raise error_factory(
            f"{entity_description} property '{property_name}' {reason}"
        )

    property_type = property_config.type
    if property_type == "string":
        if not isinstance(value, str):
            fail("must be a string")
        return value

    if property_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            fail("must be an integer")
        return value

    if property_type == "float":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            fail("must be a finite number")
        try:
            numeric_value = float(value)
        except (OverflowError, ValueError):
            fail("must be a finite number")
        if not math.isfinite(numeric_value):
            fail("must be a finite number")
        return numeric_value

    if property_type == "date":
        if not isinstance(value, str):
            fail("must be an ISO-8601 date")
        try:
            return date.fromisoformat(value)
        except ValueError:
            fail("must be an ISO-8601 date")

    if property_type == "datetime":
        if not isinstance(value, str):
            fail("must be an ISO-8601 datetime")
        if len(value) < 11 or value[10] != "T":
            fail("must use a 'T' datetime separator")
        normalized_value = f"{value[:-1]}+00:00" if value.endswith("Z") else value
        try:
            parsed_value = datetime.fromisoformat(normalized_value)
        except ValueError:
            fail("must be an ISO-8601 datetime")
        if parsed_value.tzinfo is None or parsed_value.utcoffset() is None:
            fail("must include a timezone offset")
        return parsed_value

    fail(f"has unsupported type '{property_type}'")
    raise AssertionError("unreachable")
