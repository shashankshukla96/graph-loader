"""Private prebuilt-image contract; no checkout builds or service startup."""
from __future__ import annotations

import logging
import os
import re
import threading

from docker.errors import ImageNotFound

from neo4j_graph_loader._version import __version__

logger = logging.getLogger(__name__)
NAMESPACE = 'ghcr.io/shashankshukla96'
ROLES = ('cli', 'node', 'edge', 'clock')
CONTRACT = '1'


def image_reference(role: str) -> str:
    if role not in ROLES:
        raise ValueError('unknown image component')
    return os.environ.get(f'GRAPH_LOADER_{role.upper()}_IMAGE') or f'{NAMESPACE}/graph-loader-{role}:{__version__}'


class ImageSelectionError(RuntimeError):
    """Image could not be resolved with the required compatibility metadata."""


class ImageSelection:
    def __init__(self, client, *, local_only=False):
        self.client = client
        self.local_only = local_only
        self.references = {role: image_reference(role) for role in ROLES}
        self.selected = {}
        self._lock = threading.RLock()

    def resolve(self, role: str, reference: str | None = None) -> str:
        if role not in ROLES:
            raise ValueError('unknown image component')
        with self._lock:
            requested = reference if reference is not None else self.references[role]
            if role in self.selected:
                previous, identity = self.selected[role]
                if requested != previous:
                    raise ImageSelectionError(f'{role} image is already pinned; use a new DockerService to change it')
                return identity
            # References are operator input, not credential-bearing URLs. Never
            # echo them or Docker exception bodies into compatibility diagnostics.
            if not isinstance(requested, str) or '://' in requested or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}', requested):
                raise ImageSelectionError(f'invalid {role} image reference; use an image tag or digest, not a URL')
            try:
                try:
                    image = self.client.images.get(requested)
                except ImageNotFound:
                    if self.local_only:
                        raise ImageSelectionError(f'{role} image is missing locally; preload a matching image or omit --skip-image-build') from None
                    image = self.client.images.pull(requested)
                labels = image.attrs.get('Config', {}).get('Labels') or {}
                expected = {'org.opencontainers.image.version': __version__,
                            'io.graph-loader.component': role, 'io.graph-loader.image-contract': CONTRACT}
                if any(labels.get(key) != value for key, value in expected.items()):
                    raise ImageSelectionError(f'incompatible {role} image: require package version {__version__}, component {role}, image-contract {CONTRACT}')
                identity = image.id
                if not isinstance(identity, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', identity):
                    raise ImageSelectionError(f'{role} image has no immutable local identity')
            except ImageSelectionError:
                raise
            except Exception as exc:
                raise ImageSelectionError(f'cannot resolve {role} image; check Docker daemon access, local image availability and registry access') from exc
            self.references[role] = requested
            self.selected[role] = requested, identity
            logger.info('stage=image component=%s version=%s id=%s', role, __version__, identity)
            return identity
