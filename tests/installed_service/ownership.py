"""One ownership policy for every cleanup path of the live service lane.

A container carrying the run's owner label is owned. One carrying another
owner's label is never owned, whatever network it is on. One carrying no owner
label (the installed CLI does not label what it creates) is owned only when it
is attached to a network the run created and whose own owner label was verified, it was created
from one of the candidate images when those are known, and it did not exist before the run.
``scripts/service_lane.py`` applies the same rules outside the pytest process;
``tests/test_service_lane.py`` compares the two over a shared matrix.
"""
from __future__ import annotations

OWNER_LABEL = 'io.graph-loader.e09-owner'


def labelled(labels, owner):
    return (labels or {}).get(OWNER_LABEL) == owner


def conflicting(labels, owner):
    """True when the resource names a different owner."""
    value = (labels or {}).get(OWNER_LABEL)
    return value is not None and value != owner


def container_owned(attrs, owner, trusted_networks, images=None):
    label = (attrs['Config'].get('Labels') or {}).get(OWNER_LABEL)
    if label is not None:
        return label == owner
    if images is not None and attrs.get('Image') not in images:
        return False
    return any(n['NetworkID'] in trusted_networks for n in attrs['NetworkSettings']['Networks'].values())
