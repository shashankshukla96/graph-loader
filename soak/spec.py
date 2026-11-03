"""The soak's graph: one table that the schema renderer, generator, oracle, checker and graph reader all share.

Topic names here are suffixes; a run prefixes them with ``e13-<token>-`` (``topic_name``).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import os

# Every owned name is ``<namespace>-<token>-...``. E13 runs use ``e13``; E14's host-process and host-service soaks set SOAK_NAMESPACE=e14.
NAMESPACE = os.environ.get('SOAK_NAMESPACE', 'e13')

TEMPLATE_TOKEN = '0' * 32  # the literal token soak/schema.yaml carries; a run substitutes its own
RETRY_ATTEMPTS = 11        # retry.base 1 s, max 60 s: attempts at 1,2,4,8,16,32,60,60,60,60,60 s after the first DLQ publish
RETRY_WINDOW_S = 304       # B4: base + sum over k=2..N of min(base * 2^(k-2), max) = 304 s for N = 11


def topic_name(token: str, suffix: str) -> str:
    return f'{NAMESPACE}-{token}-{suffix}'


@dataclass(frozen=True)
class NodeSpec:
    label: str
    key: str
    topic: str
    partitions: int
    replicas: int
    types: dict          # property -> string | integer | float | datetime
    required: frozenset  # properties a valid record must carry non-null
    optional: tuple      # the properties that may be null or absent


@dataclass(frozen=True)
class TopicSpec:
    suffix: str
    partitions: int
    source: str
    target: str


@dataclass(frozen=True)
class RelSpec:
    type: str
    topics: tuple
    replicas: int
    types: dict
    required: frozenset
    order_by: str | None

    def pair_topics(self, source: str, target: str):
        return [t for t in self.topics if (t.source, t.target) == (source, target)]

    @property
    def pairs(self):
        return sorted({(t.source, t.target) for t in self.topics})


NODES = {
    'Customer': NodeSpec('Customer', 'customer_id', 'customers', 6, 2,
                         {'customer_id': 'string', 'rid': 'string', 'name': 'string', 'tier': 'string', 'city': 'string',
                          'updated_at': 'datetime'},
                         frozenset({'customer_id', 'rid', 'name'}), ('tier', 'city', 'updated_at')),
    'Product': NodeSpec('Product', 'product_id', 'products', 3, 1,
                        {'product_id': 'string', 'rid': 'string', 'title': 'string', 'price': 'float', 'category': 'string'},
                        frozenset({'product_id', 'rid', 'title', 'price'}), ('category',)),
    'Store': NodeSpec('Store', 'store_id', 'stores', 1, 1,
                      {'store_id': 'string', 'rid': 'string', 'name': 'string'},
                      frozenset({'store_id', 'rid', 'name'}), ()),
}

RELS = {
    'PURCHASED': RelSpec('PURCHASED', (TopicSpec('purchases-web', 6, 'Customer', 'Product'),
                                       TopicSpec('purchases-app', 3, 'Customer', 'Product')), 2,
                         {'rid': 'string', 'qty': 'integer', 'amount': 'float', 'version': 'integer', 'at': 'datetime'},
                         frozenset({'rid', 'qty', 'amount', 'version', 'at'}), 'version'),
    'STOCKS': RelSpec('STOCKS', (TopicSpec('stock', 6, 'Store', 'Product'),), 1,
                      {'rid': 'string', 'quantity': 'integer', 'updated_at': 'datetime'},
                      frozenset({'rid', 'quantity', 'updated_at'}), 'updated_at'),
    'FOLLOWS': RelSpec('FOLLOWS', (TopicSpec('follows', 3, 'Customer', 'Customer'),), 1,
                       {'rid': 'string', 'since': 'date', 'strength': 'integer'},
                       frozenset({'rid'}), None),
    'VIEWED': RelSpec('VIEWED', (TopicSpec('views-product', 3, 'Customer', 'Product'),
                                 TopicSpec('views-store', 1, 'Customer', 'Store')), 1,
                      {'rid': 'string', 'seen_at': 'datetime', 'device': 'string'},
                      frozenset({'rid', 'seen_at', 'device'}), 'seen_at'),
}


def key_of(label: str) -> str:
    """The key property of a label; VIEWED's two pairs therefore carry different target key names."""
    return NODES[label].key


def dlq_topic(token: str, rel: str) -> str:
    return topic_name(token, f'dlq-{rel.lower()}')


def source_topics(token: str):
    """Every source topic of the run with its partition count and the entity that reads it: {topic: (partitions, kind, name)}."""
    result = {}
    for node in NODES.values():
        result[topic_name(token, node.topic)] = (node.partitions, 'node', node.label)
    for rel in RELS.values():
        for topic in rel.topics:
            result[topic_name(token, topic.suffix)] = (topic.partitions, 'rel', rel.type)
    return result


def dlq_topics(token: str):
    """{rel type: (dlq topic, parked topic, partitions)} — the DLQ has as many partitions as the type's widest topic."""
    return {rel.type: (dlq_topic(token, rel.type), dlq_topic(token, rel.type) + '.parked', max(t.partitions for t in rel.topics))
            for rel in RELS.values()}


# ---- value normalisation: payload text and Neo4j values meet in one canonical form -------------------------------------

def parse_instant(text: str) -> datetime:
    """An ISO-8601 datetime with an offset, as a UTC instant. A naive value is a harness error, not a guess."""
    value = datetime.fromisoformat(text.replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError(f'datetime without an offset: {text!r}')
    return value.astimezone(timezone.utc)


def canonical_instant(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def normalise(kind: str, value):
    """Canonical comparable form of a declared property value (``None`` stays ``None``)."""
    if value is None:
        return None
    if kind == 'datetime':
        if isinstance(value, str):
            return canonical_instant(parse_instant(value))
        if hasattr(value, 'to_native'):  # neo4j.time.DateTime
            return canonical_instant(value.to_native())
        return canonical_instant(value)
    if kind == 'date':
        if isinstance(value, str):
            return date.fromisoformat(value).isoformat()
        if hasattr(value, 'iso_format'):  # neo4j.time.Date
            return value.iso_format()
        return value.isoformat()
    if kind == 'integer':
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f'not an integer: {value!r}')
        return value
    if kind == 'float':
        return float(value)
    return value


def normalise_props(types: dict, props: dict) -> dict:
    """Drop nulls (the product never writes them), then normalise each declared property."""
    return {name: normalise(types[name], value) for name, value in props.items() if value is not None and name in types}


def order_key(spec: RelSpec, props: dict):
    """The sort value of an ``order_by`` property: an instant for datetimes, the number itself otherwise."""
    kind = spec.types[spec.order_by]
    value = props[spec.order_by]
    return parse_instant(value) if kind == 'datetime' and isinstance(value, str) else value
