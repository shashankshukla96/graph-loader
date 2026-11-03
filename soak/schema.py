"""Render soak/schema.yaml for one owned run: substitute the owner token, optionally change replica counts."""
from __future__ import annotations

from pathlib import Path
import re

import yaml

from .spec import NAMESPACE, TEMPLATE_TOKEN

TEMPLATE = Path(__file__).with_name('schema.yaml')
TOKEN_RE = re.compile(r'[0-9a-f]{32}\Z')


def render(token: str, *, replicas: dict | None = None, mode: str | None = None, overrides: dict | None = None) -> str:
    """The schema text for ``token``. ``replicas`` maps an entity name (label or type) to its replica count;
    ``mode`` rewrites ``loading.mode`` (``stream`` is the template's value; the bulk handover run uses ``bulk``)."""
    if TOKEN_RE.match(token) is None:
        raise ValueError('owner token must be 32 lowercase hex characters')
    text = TEMPLATE.read_text().replace(f'e13-{TEMPLATE_TOKEN}', f'{NAMESPACE}-{token}').replace(TEMPLATE_TOKEN, token)
    if not (replicas or mode or overrides):
        return text
    document = yaml.safe_load(text)
    for entity in (*document['nodes'], *document['edges']):
        name = entity.get('label') or entity.get('type')
        if replicas and name in replicas:
            entity['replicas'] = int(replicas[name])
    if mode:
        document['loading']['mode'] = mode
    for path, value in (overrides or {}).items():
        target = document
        parts = path.split('.')
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = value
    return yaml.safe_dump(document, sort_keys=False)


def replica_plan(text: str) -> dict:
    """{entity: replicas} of a rendered schema (nodes and relationship types)."""
    document = yaml.safe_load(text)
    return {(e.get('label') or e.get('type')): int(e.get('replicas', 1)) for e in (*document['nodes'], *document['edges'])}


def application_containers(text: str, *, retry_consumers: int = 1) -> int:
    """Containers the CLI starts for a schema: every loader replica, the retry consumer(s) and the lease server."""
    return sum(replica_plan(text).values()) + retry_consumers + 1
