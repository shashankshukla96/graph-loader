"""Explicit compatible-image responses for controlled launcher clients."""
import hashlib
from types import SimpleNamespace

from neo4j_graph_loader._version import __version__

IMAGE_IDS = {role: 'sha256:' + hashlib.sha256(role.encode()).hexdigest() for role in ('cli', 'node', 'edge', 'clock')}


def configure_images(client):
    def get(reference):
        role = next(role for role in IMAGE_IDS if f'graph-loader-{role}:' in reference)
        return SimpleNamespace(id=IMAGE_IDS[role], attrs={'Config': {'Labels': {
            'org.opencontainers.image.version': __version__, 'io.graph-loader.component': role,
            'io.graph-loader.image-contract': '1'}}})
    client.images.get.side_effect = get
    return client
