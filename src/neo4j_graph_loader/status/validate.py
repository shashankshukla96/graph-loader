"""A small JSON Schema validator for the subset the status schemas use (no third-party dependency).

Supported keywords: ``$ref`` (local ``#/$defs/...``), ``type``, ``enum``, ``const``, ``pattern``, ``minLength``,
``minimum``, ``not``, ``required``, ``properties``, ``additionalProperties`` (false or a schema), ``items``, ``anyOf``,
``oneOf``. Anything else in a schema is ignored; tests cross-check this validator against the ``jsonschema``
package, where it is installed, on the same fixtures.
"""
from __future__ import annotations

import re
from typing import Any


class ValidationError(ValueError):
    """The instance does not satisfy the schema; ``path`` locates the first offending member."""

    def __init__(self, path: str, message: str):
        super().__init__(f"{path or '$'}: {message}")
        self.path = path
        self.message = message


def _is_type(value: Any, name: str) -> bool:
    if name == "null":
        return value is None
    if name == "boolean":
        return isinstance(value, bool)
    if name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if name == "string":
        return isinstance(value, str)
    if name == "array":
        return isinstance(value, list)
    if name == "object":
        return isinstance(value, dict)
    raise ValueError(f"unsupported schema type {name!r}")


def _resolve(root: dict, reference: str) -> dict:
    if not reference.startswith("#/"):
        raise ValueError(f"unsupported $ref {reference!r}")
    node: Any = root
    for part in reference[2:].split("/"):
        node = node[part]
    return node


def _check(schema: dict, value: Any, root: dict, path: str) -> None:
    if "$ref" in schema:
        _check(_resolve(root, schema["$ref"]), value, root, path)
        return
    if "const" in schema and (value != schema["const"] or type(value) is not type(schema["const"])):
        raise ValidationError(path, f"must equal {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        raise ValidationError(path, f"must be one of {schema['enum']!r}")
    if "type" in schema:
        names = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if not any(_is_type(value, name) for name in names):
            raise ValidationError(path, f"must be of type {'/'.join(names)}")
    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            raise ValidationError(path, f"must be at least {schema['minLength']} characters")
        if "pattern" in schema and re.search(schema["pattern"], value) is None:
            raise ValidationError(path, f"must match {schema['pattern']}")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            raise ValidationError(path, f"must be at least {schema['minimum']}")
    if "not" in schema:
        try:
            _check(schema["not"], value, root, path)
        except ValidationError:
            pass
        else:
            raise ValidationError(path, "must not match the excluded schema")
    if "anyOf" in schema:
        _any_of(schema["anyOf"], value, root, path, exactly_one=False)
    if "oneOf" in schema:
        _any_of(schema["oneOf"], value, root, path, exactly_one=True)
    if isinstance(value, dict):
        for name in schema.get("required", ()):
            if name not in value:
                raise ValidationError(path, f"missing required member {name!r}")
        properties = schema.get("properties", {})
        extra = schema.get("additionalProperties", True)
        for name, member in value.items():
            child = f"{path}.{name}" if path else name
            if name in properties:
                _check(properties[name], member, root, child)
            elif extra is False:
                raise ValidationError(child, "member is not allowed")
            elif isinstance(extra, dict):
                _check(extra, member, root, child)
    if isinstance(value, list) and "items" in schema:
        for index, item in enumerate(value):
            _check(schema["items"], item, root, f"{path}[{index}]")


def _any_of(options: list, value: Any, root: dict, path: str, *, exactly_one: bool) -> None:
    matches, errors = 0, []
    for option in options:
        try:
            _check(option, value, root, path)
        except ValidationError as exc:
            errors.append((option, exc))
        else:
            matches += 1
    if matches == 0:
        # Explain with the alternative that matters: the one a discriminating `type` member selects, else the first
        # that is not just the `null` of a nullable member, else the last one tried.
        real = [(option, exc) for option, exc in errors if option != {"type": "null"}]
        chosen = (real or errors)[0][1]
        if isinstance(value, dict) and "type" in value:
            for option, exc in errors:
                resolved = _resolve(root, option["$ref"]) if "$ref" in option else option
                if resolved.get("properties", {}).get("type", {}).get("const") == value["type"]:
                    chosen = exc
                    break
        suffix = " (no alternative matches)" if len(real or errors) > 1 else ""
        raise ValidationError(chosen.path, chosen.message + suffix)
    if exactly_one and matches != 1:
        raise ValidationError(path, f"matches {matches} alternatives; exactly one is required")


def validate(instance: Any, schema: dict) -> None:
    """Raise :class:`ValidationError` when ``instance`` does not satisfy ``schema``."""
    _check(schema, instance, schema, "")
