"""Private prebuilt-image contract; no checkout builds or service startup."""
from __future__ import annotations

import logging
import os
import re
import threading

from docker.errors import ImageNotFound

from neo4j_graph_loader._version import __version__

logger = logging.getLogger(__name__)
NAMESPACE = 'ghcr.io/neburyx'
ROLES = ('cli', 'node', 'edge', 'clock')
CONTRACT = '1'
REGISTRY_ENV = 'GRAPH_LOADER_IMAGE_REGISTRY'

# A registry prefix is ``host[:port](/path-component)*``: no scheme, no userinfo, no tag or digest, no whitespace, no empty or
# uppercase path component. It is a different grammar from a full image reference, so it is checked on its own.
_HOST = r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*'
_PATH_COMPONENT = r'[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*'
_REGISTRY_PREFIX = re.compile(rf'{_HOST}(?::[0-9]{{1,5}})?(?:/{_PATH_COMPONENT})*')
# A full reference: a name with an optional tag, optionally pinned by a digest. A bare ``@`` is never allowed: before the digest
# it would be userinfo (``user:password@host/image``), which is not a credential channel.
_REFERENCE_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}')
_REFERENCE_DIGEST = re.compile(r'[A-Za-z][A-Za-z0-9]*(?:[+._-][A-Za-z][A-Za-z0-9]*)*:[0-9a-fA-F]{32,}')


class ImageSelectionError(RuntimeError):
    """Image could not be resolved with the required compatibility metadata."""


def registry_prefix(value: str) -> str:
    """The validated ``GRAPH_LOADER_IMAGE_REGISTRY`` prefix; the value is never echoed (it may have been typed with credentials)."""
    if not isinstance(value, str) or len(value) > 255 or not _REGISTRY_PREFIX.fullmatch(value):
        raise ImageSelectionError(f'invalid {REGISTRY_ENV}: use host[:port] and lowercase path components, '
                                  'with no scheme, credentials, tag, digest, spaces or trailing slash')
    return value


def valid_reference(reference) -> bool:
    """A full image reference: ``name[:tag][@digest]`` with no URL scheme and no userinfo."""
    if not isinstance(reference, str) or '://' in reference or len(reference) > 512:
        return False
    name, separator, digest = reference.partition('@')
    if not _REFERENCE_NAME.fullmatch(name):
        return False
    return not separator or _REFERENCE_DIGEST.fullmatch(digest) is not None


def image_reference(role: str) -> str:
    if role not in ROLES:
        raise ValueError('unknown image component')
    explicit = os.environ.get(f'GRAPH_LOADER_{role.upper()}_IMAGE')
    if explicit:
        return explicit
    registry = os.environ.get(REGISTRY_ENV)
    prefix = registry_prefix(registry) if registry else NAMESPACE
    return f'{prefix}/graph-loader-{role}:{__version__}'


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
            if not valid_reference(requested):
                raise ImageSelectionError(f'invalid {role} image reference; use an image tag or digest, not a URL or credentials')
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
