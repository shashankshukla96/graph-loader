"""Pre-launch checks and the one-time offset migration for stable stream groups.

A stream fleet's relationship groups are ``<prefix>-<type>`` and its retry
group is ``<prefix>-retry-consumer`` (``src.utils.consumer_groups``). Before
the fleet launches, :func:`prepare_stream_groups`:

1. refuses to start while any of those groups still has active members.
   That is a second stream fleet of the same schema and ``consumer_group_id``
   (it would split the partitions with this one and bypass its lease server),
   or containers of an earlier run that are still alive. A crashed run's
   members expire after the session timeout, so the check waits that long
   before it refuses.
2. migrates offsets once: for each stable group that has no committed offset
   on its topics, if ``--run-id`` names an earlier run whose run-scoped group
   (``<prefix>-<type>-<run id>``, ``graph-loader-retry-consumer-<run id>``)
   has commits, those commits are copied with
   ``AdminClient.alter_consumer_group_offsets`` (the group is empty, so Kafka
   accepts it) and a WARNING names both groups. Without ``--run-id`` there is
   no reliable legacy id; nothing is guessed and an INFO says the fleet
   resumes from the stable group.

Bulk runs never call this module.
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from typing import Callable, Iterable, Sequence

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_SECONDS = 10.0
_POLL_SECONDS = 1.0
# A broker that just started answers group requests with a retriable error
# (COORDINATOR_LOAD_IN_PROGRESS, NOT_COORDINATOR) until __consumer_offsets is loaded.
_RETRIABLE_BUDGET_SECONDS = 60.0


def _retriable(exc: BaseException) -> bool:
    """A Kafka error flagged retriable, or a group-coordinator error (librdkafka does not flag those)."""
    from confluent_kafka import KafkaError

    error = exc.args[0] if getattr(exc, "args", None) else None
    retriable = getattr(error, "retriable", None)
    if callable(retriable) and retriable():
        return True
    code = getattr(error, "code", None)
    return callable(code) and code() in (
        KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS, KafkaError.COORDINATOR_NOT_AVAILABLE,
    )


def _with_retries(call: Callable[[], object], *, what: str, clock: Callable[[], float] = time.monotonic,
                  sleep: Callable[[float], None] = time.sleep, budget_seconds: float = _RETRIABLE_BUDGET_SECONDS):
    """Run ``call``, retrying a retriable Kafka error for up to ``budget_seconds``."""
    deadline = clock() + budget_seconds
    while True:
        try:
            return call()
        except Exception as exc:
            if not _retriable(exc) or clock() >= deadline:
                raise
            logger.info("stage=consumer-groups reason=retriable_kafka_error request=%s error=%s", what, exc)
            sleep(_POLL_SECONDS)


class StreamGroupError(RuntimeError):
    """A stable stream group cannot be used safely right now."""


class StreamGroupWaitStopped(StreamGroupError):
    """A stop was requested while waiting for a stable group's members to expire; nothing was started."""


@dataclass(frozen=True)
class StreamGroup:
    """One stable stream group, the topics it reads and its legacy name (if known)."""

    group: str
    topics: tuple[str, ...]
    legacy_group: str | None = None


def _partitions(admin, topics: Iterable[str]) -> list[tuple[str, int]]:
    metadata = _with_retries(lambda: admin.list_topics(timeout=_REQUEST_TIMEOUT_SECONDS), what="list_topics")
    result: list[tuple[str, int]] = []
    for topic in sorted(set(topics)):
        info = metadata.topics.get(topic)
        if info is None or getattr(info, "error", None) is not None:
            raise StreamGroupError(f"topic {topic} metadata unavailable")
        result.extend((topic, partition) for partition in sorted(info.partitions))
    return result


def committed_offsets(admin, group: str, partitions: Sequence[tuple[str, int]]) -> dict[tuple[str, int], int]:
    """The committed offsets of ``group`` on ``partitions``; uncommitted partitions are omitted.

    A partition reported with an error raises :class:`StreamGroupError`: treating it as
    uncommitted could let the migration overwrite real commits with older legacy offsets.
    An offset below 0 without an error (-1001) means "no commit".
    """
    if not partitions:
        return {}
    def fetch():
        request = ConsumerGroupTopicPartitions(
            group, [TopicPartition(topic, partition) for topic, partition in partitions],
        )
        return admin.list_consumer_group_offsets([request])[group].result(timeout=_REQUEST_TIMEOUT_SECONDS)

    result = _with_retries(fetch, what=f"list_consumer_group_offsets {group}")
    errors = [f"{tp.topic}[{tp.partition}]: {tp.error}" for tp in result.topic_partitions
              if getattr(tp, "error", None) is not None]
    if errors:
        raise StreamGroupError(f"committed offsets of group {group} could not be read: {'; '.join(errors)}")
    return {
        (tp.topic, tp.partition): tp.offset
        for tp in result.topic_partitions
        if isinstance(tp.offset, int) and tp.offset >= 0
    }


def _watermarks(admin, partitions: Sequence[tuple[str, int]]) -> dict[tuple[str, int], tuple[int, int]]:
    """``{(topic, partition): (low, high)}`` from the broker."""
    from confluent_kafka.admin import OffsetSpec

    def fetch(spec):
        def call():
            futures = admin.list_offsets(
                {TopicPartition(topic, partition): spec for topic, partition in partitions},
                request_timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            return {(tp.topic, tp.partition): future.result(timeout=_REQUEST_TIMEOUT_SECONDS).offset
                    for tp, future in futures.items()}
        return _with_retries(call, what="list_offsets")

    lows, highs = fetch(OffsetSpec.earliest()), fetch(OffsetSpec.latest())
    return {key: (lows[key], highs[key]) for key in partitions}


def _active_members(admin, groups: Sequence[str]) -> dict[str, list[str]]:
    """``{group: [member descriptions]}`` for every group that still has members."""
    if not groups:
        return {}
    def describe():
        futures = admin.describe_consumer_groups(list(groups), request_timeout=_REQUEST_TIMEOUT_SECONDS)
        return {group: future.result(timeout=_REQUEST_TIMEOUT_SECONDS) for group, future in futures.items()}

    active: dict[str, list[str]] = {}
    for group, description in _with_retries(describe, what="describe_consumer_groups").items():
        members = list(getattr(description, "members", None) or ())
        if members:
            active[group] = [
                f"{getattr(member, 'client_id', '?')}@{getattr(member, 'host', '?')}" for member in members
            ]
    return active


def wait_until_groups_empty(
    admin, groups: Sequence[str], *, timeout_seconds: float,
    clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
    stop_requested: Callable[[], bool] | None = None,
) -> None:
    """Return once no group has members; raise :class:`StreamGroupError` after ``timeout_seconds``.

    Best effort: two CLIs starting at the same moment can both see empty groups. The
    stream monitor's member check (``RelationshipBulkMonitor``) catches that after launch.

    With the classic group protocol a member leaves when it closes, or when the broker
    stops hearing from it for ``session.timeout.ms``; a member that died during a
    rebalance can stay until the rebalance times out (up to ``max.poll.interval.ms``).
    Under KIP-848 (``group.protocol=consumer``) the broker's
    ``group.consumer.session.timeout.ms`` applies instead of the client setting.
    """
    deadline = clock() + timeout_seconds
    first = True
    while True:
        active = _active_members(admin, groups)
        if not active:
            return
        if first:
            logger.warning(
                "stage=consumer-groups reason=groups_have_members groups=%s waiting_s=%.0f "
                "detail=waiting for the members of an earlier run to expire",
                ",".join(sorted(active)), timeout_seconds,
            )
            first = False
        details = "; ".join(f"{group}: {', '.join(members)}" for group, members in sorted(active.items()))
        if stop_requested is not None and stop_requested():
            raise StreamGroupWaitStopped(
                f"stop requested while waiting for the members of stable stream group(s) to expire ({details})"
            )
        if clock() >= deadline:
            raise StreamGroupError(
                "stable stream consumer group(s) still have active members after "
                f"{timeout_seconds:.0f}s ({details}). Another stream fleet with the same schema and "
                "loading.consumer_group_id is running, or containers of an earlier run are still alive. "
                "Stop them first, or give this deployment its own loading.consumer_group_id. A member that "
                "died during a rebalance can stay up to max_poll_interval_ms plus session_timeout_ms; to wait "
                "that long, start with --stream-group-member-wait-seconds"
            )
        sleep(min(_POLL_SECONDS, max(0.0, deadline - clock())))


def migrate_legacy_offsets(admin, stream_group: StreamGroup, *, explicit_run_id: str | None) -> bool:
    """Copy a legacy run-scoped group's commits into an uncommitted stable group.

    Returns whether offsets were copied. Never overwrites a stable group that
    already has a commit on any of its partitions.
    """
    partitions = _partitions(admin, stream_group.topics)
    current = committed_offsets(admin, stream_group.group, partitions)
    if current:
        logger.info(
            "stage=consumer-groups group=%s resume=committed partitions_committed=%s/%s",
            stream_group.group, len(current), len(partitions),
        )
        return False
    if explicit_run_id is None or stream_group.legacy_group is None:
        logger.info(
            "stage=consumer-groups group=%s resume=stable-group reason=no_committed_offsets "
            "detail=no --run-id, so no run-scoped group of an earlier release is migrated; "
            "the group starts from auto.offset.reset=earliest",
            stream_group.group,
        )
        return False
    legacy_all = committed_offsets(admin, stream_group.legacy_group, partitions)
    marks = _watermarks(admin, sorted(legacy_all)) if legacy_all else {}
    legacy = {key: offset for key, offset in legacy_all.items() if marks[key][0] <= offset <= marks[key][1]}
    skipped = {key: offset for key, offset in legacy_all.items() if key not in legacy}
    if skipped:
        logger.warning(
            "stage=consumer-groups reason=legacy_offsets_out_of_range from_group=%s to_group=%s not_copied=%s "
            "detail=outside the partition's current [low, high]; those partitions start from auto.offset.reset",
            stream_group.legacy_group, stream_group.group,
            ", ".join(
                f"{topic}[{partition}]={offset} range=[{marks[(topic, partition)][0]}, {marks[(topic, partition)][1]}]"
                for (topic, partition), offset in sorted(skipped.items())
            ),
        )
    if not legacy:
        logger.info(
            "stage=consumer-groups group=%s resume=stable-group legacy_group=%s reason=legacy_group_has_no_commits",
            stream_group.group, stream_group.legacy_group,
        )
        return False
    def alter():
        request = ConsumerGroupTopicPartitions(
            stream_group.group,
            [TopicPartition(topic, partition, offset) for (topic, partition), offset in sorted(legacy.items())],
        )
        return admin.alter_consumer_group_offsets([request])[stream_group.group].result(
            timeout=_REQUEST_TIMEOUT_SECONDS,
        )

    result = _with_retries(alter, what=f"alter_consumer_group_offsets {stream_group.group}")
    failed = [
        f"{tp.topic}[{tp.partition}]: {tp.error}" for tp in getattr(result, "topic_partitions", ()) or ()
        if getattr(tp, "error", None) is not None
    ]
    if failed:
        raise StreamGroupError(
            f"copying offsets from {stream_group.legacy_group} to {stream_group.group} failed: {'; '.join(failed)}"
        )
    logger.warning(
        "stage=consumer-groups reason=migrated_legacy_offsets from_group=%s to_group=%s partitions=%s/%s "
        "offsets=%s detail=stream groups are now stable; the run-scoped group is no longer read and can be deleted",
        stream_group.legacy_group, stream_group.group, len(legacy), len(partitions),
        ",".join(f"{topic}[{partition}]={offset}" for (topic, partition), offset in sorted(legacy.items())),
    )
    return True


def prepare_stream_groups(
    admin, groups: Sequence[StreamGroup], *, explicit_run_id: str | None, member_timeout_seconds: float,
    clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
    stop_requested: Callable[[], bool] | None = None,
) -> list[str]:
    """Refuse active groups, then migrate legacy offsets; returns the groups that were migrated."""
    names = [group.group for group in groups]
    if len(set(names)) != len(names):
        raise StreamGroupError(f"stream consumer group names collide: {sorted(names)}")
    wait_until_groups_empty(
        admin, names, timeout_seconds=member_timeout_seconds, clock=clock, sleep=sleep,
        stop_requested=stop_requested,
    )
    return [group.group for group in groups if migrate_legacy_offsets(admin, group, explicit_run_id=explicit_run_id)]
