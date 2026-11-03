"""The memory-budget inequality for the largest scheduled fleet (E13 "Memory").

The frozen CLI sets no memory limit on the containers it launches, so loader memory is bounded by measurement and by the container
count, not by limits:

    Neo4j limit + Kafka limit + 1.5 x (sum of measured peaks of the largest fleet) + harness peak
        <=  host MemAvailable at start - headroom

The largest fleet is every application container of the schema after the ``PURCHASED`` scaling fault (the lease server and the
retry consumer included), plus one overlapping replacement container starting while its predecessor drains, plus the CLI.
"""
from __future__ import annotations

GiB = 1024 ** 3
FACTOR = 1.5
HEADROOM = 2 * GiB


def largest_fleet_peaks(peaks: dict, replicas_now: dict, replicas_after: dict, *, cli_peak: int) -> dict:
    """Estimate the largest fleet's per-container peaks from the measured ones.

    ``peaks`` maps a component key (``node-loader:Customer``, ``edge-loader:PURCHASED``, ``retry-consumer``, ``bucket-lease-server``)
    to the highest peak RSS of any container incarnation of that kind. ``replicas_now``/``replicas_after`` give replica counts of
    entities before and after the scaling fault; extra replicas are charged the measured peak of their kind. One overlapping
    replacement is charged the single largest peak. Returns the itemised sum.
    """
    rows = {}
    for key, peak in peaks.items():
        kind, _, entity = key.partition(':')
        count = 1
        if entity:
            count = max(replicas_now.get(entity, 1), replicas_after.get(entity, 1))
        rows[key] = {'peak': peak, 'count': count, 'bytes': peak * count}
    total = sum(row['bytes'] for row in rows.values())
    overlap = max((row['peak'] for row in rows.values()), default=0)
    return {'rows': rows, 'sum_peaks': total, 'overlapping_replacement': overlap, 'cli': cli_peak,
            'sum_with_overlap_and_cli': total + overlap + cli_peak}


def evaluate(*, neo4j_limit, kafka_limit, fleet, harness_peak, mem_available, headroom=HEADROOM, factor=FACTOR) -> dict:
    need = neo4j_limit + kafka_limit + factor * fleet['sum_with_overlap_and_cli'] + harness_peak
    allowed = mem_available - headroom
    return {'neo4j_limit': neo4j_limit, 'kafka_limit': kafka_limit, 'factor': factor, 'fleet_sum': fleet['sum_with_overlap_and_cli'],
            'harness_peak': harness_peak, 'mem_available_at_start': mem_available, 'headroom': headroom,
            'required': int(need), 'allowed': int(allowed), 'ok': need <= allowed, 'margin': int(allowed - need)}


def max_fleet_sum(neo4j_limit, kafka_limit, harness_peak, mem_available, headroom=HEADROOM, factor=FACTOR) -> int:
    """The largest ``sum_with_overlap_and_cli`` the inequality admits (what the S2/S3 plan must stay under)."""
    return int((mem_available - headroom - neo4j_limit - kafka_limit - harness_peak) / factor)
