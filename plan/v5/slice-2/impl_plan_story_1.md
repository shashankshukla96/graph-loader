# Story 1 implementation plan: Dead-Letter Envelope and Wire Codec

## Files

- New: `src/loader/dead_letter.py`
- New: `tests/test_dead_letter.py`

## Design

`src/loader/dead_letter.py` (this story's slice — later stories append to this
module rather than creating siblings):

```python
from __future__ import annotations
import base64
from dataclasses import dataclass
import json
from typing import Literal

FAILURE_CLASSES = ("malformed", "missing_endpoint", "ambiguous_endpoint")
SCHEMA_VERSION = 1

class DeadLetterEnvelopeError(ValueError): ...

@dataclass(frozen=True)
class LoaderIdentity:
    run_id: str
    replica_id: str
    kind: Literal["node", "edge"]

@dataclass(frozen=True)
class DeadLetterEnvelope:
    schema_version: int
    failure_class: str
    entity_kind: str
    entity_name: str
    original_topic: str
    original_partition: int
    original_offset: int
    key_bytes: bytes | None
    value_bytes: bytes | None
    reason_code: str
    error_text: str
    loader_identity: LoaderIdentity
    first_failure_at_ms: int
    attempt_count: int
    dlq_routed_at_ms: int
```

`encode_envelope(envelope: DeadLetterEnvelope) -> bytes`:
- Validates every field's type/shape (delegates to a private `_validate`
  helper shared with `decode_envelope`, mirroring `coordination.py`'s
  `_validated`/`_integer` precedent) — raises `DeadLetterEnvelopeError` on any
  violation so a caller can never publish a malformed envelope.
- Serializes to a canonical JSON dict: `key_bytes`/`value_bytes` become
  `null` or base64 `str` (`base64.b64encode(...).decode("ascii")`).
  `loader_identity` becomes a nested object. `json.dumps(..., sort_keys=True,
  separators=(",", ":")).encode("utf-8")` — canonical, deterministic, no
  whitespace, matching `encode_clock_lease`.

`decode_envelope(payload: bytes) -> DeadLetterEnvelope`:
- `json.loads` with `object_pairs_hook` rejecting duplicate keys (same
  precedent as `decode_clock_lease`). Any `UnicodeDecodeError`/
  `json.JSONDecodeError`/non-dict top level -> `DeadLetterEnvelopeError`.
- Requires an exact field set (missing or extra -> error).
- Validates: `schema_version == 1` (unknown version errors — this slice does
  not support migrating old envelopes); `failure_class in FAILURE_CLASSES`;
  `entity_kind in {"node", "edge"}`; `original_partition >= 0`,
  `original_offset >= 0`; `first_failure_at_ms >= 0`, `dlq_routed_at_ms >= 0`,
  `attempt_count >= 0` (all plain non-negative ints, reject bool per the
  `coordination.py` `_integer` precedent); base64 fields decode via
  `base64.b64decode(..., validate=True)` inside a `try/except
  (binascii.Error, ValueError)` -> `DeadLetterEnvelopeError` on truncated/
  invalid base64; `loader_identity` sub-object has exactly `run_id`,
  `replica_id`, `kind` with `kind in {"node", "edge"}`.
- Returns a fully constructed `DeadLetterEnvelope`.

No Kafka import in this module yet (Story 3 adds producer code below the
codec, in the same file, per the story's stated shared-module intent).

## Tests (`tests/test_dead_letter.py`)

1. Round-trip: build an envelope with non-UTF-8 `value_bytes` (e.g.
   `b"\xff\xfe\x00bad"`), `key_bytes=None`, encode then decode; assert
   equality field-by-field including `key_bytes is None` and exact
   `value_bytes` bytes.
2. Round-trip with empty `value_bytes=b""` and non-`None` `key_bytes`.
3. Deterministic encoding: encode the same envelope twice, assert identical
   bytes.
4. Envelope is valid JSON: `json.loads(encode_envelope(env))` succeeds and is
   a `dict`.
5. `decode_envelope` rejects (each as its own test, `pytest.raises
   (DeadLetterEnvelopeError)`):
   - unknown `schema_version` (e.g. 2 or 0),
   - unrecognized `failure_class` (e.g. "bogus"),
   - negative `original_offset` / `original_partition`,
   - truncated/invalid base64 in `value_bytes`,
   - not valid JSON at all,
   - a JSON array instead of an object,
   - a missing required field,
   - an unexpected extra field,
   - duplicate top-level JSON keys,
   - bad `loader_identity.kind`.
6. `failure_class` literal restriction: encoding an envelope constructed with
   a bogus `failure_class` string also raises (encode validates, not just
   decode) — protects against a future call site passing a typo.

## Dependencies / sequencing

No dependencies. This lands first; every later story imports from this
module.
