"""Lease-fenced Neo4j transactions: a leased write never outlives its lease deadline.

A lease grant at client time ``G`` gives the writer the deadline ``D = G + T``
(``loading.bucket_lease.tx_timeout_ms``). The lease server's fence assumes
every transaction of that grant is over by ``D``: a restarted server grants
nothing for ``Q = T + margin`` after it starts, and a disconnected owner's
resources stay held for ``T + margin`` after the disconnect. So no other
writer can be granted the same nodes before ``D + margin``.

Neo4j counts a transaction's timeout from the moment the server begins it
(BEGIN), not from the first statement and not from the moment the client chose
the value (verified on Neo4j 5 enterprise: a transaction begun with a 3 s
timeout and left idle for 4 s fails its first RUN with
``TransactionTimedOutClientConfiguration``). Bolt carries the timeout in the
BEGIN message, so it cannot be set after the transaction has begun. Picking
``D - now`` before ``begin_transaction()`` is therefore not enough: connection
acquisition and the BEGIN round trip (``x``) are added on top, and Neo4j's
transaction monitor notices an expired transaction up to one check interval
(``db.transaction.monitor.check.interval``, 2 s by default) late. The
transaction could then last until ``D + x + 2 s``, past the fence.

:func:`begin_fenced_transaction` closes that window:

* Just before BEGIN, at ``t0``, the timeout is
  ``D - t0 - TX_MONITOR_SLACK_SECONDS - BEGIN_ALLOWANCE_SECONDS``.
* ``begin_transaction()`` of an explicit transaction returns only after the
  server acknowledged BEGIN (the driver does not pipeline it; verified for the
  neo4j driver < 7, which requirements.txt pins), so the server began the
  transaction at some ``ts <= t1``, the time it returned.
* If ``t1 - t0 > BEGIN_ALLOWANCE_SECONDS``, the transaction is rolled back
  before any statement runs (it holds no locks) and BEGIN is tried again with
  the smaller budget left.
* Otherwise the transaction's timeout expires by ``ts + timeout <= D - S``, and
  Neo4j's monitor marks it terminated by ``D - S + lag <= D`` (the slack ``S`` is
  3 s: the 2 s default check interval plus 1 s for the terminated transaction
  to notice). No statement of it can start after ``D``. A COMMIT already in
  progress at that moment still completes; the lease server's margin
  (``restart_quiet_margin_ms``, at least 2 s beyond ``T``) covers it.

When the budget left is at most ``MIN_TX_TIMEOUT_SECONDS``, nothing is begun:
:class:`LeaseDeadlineExceeded` is raised. Nothing of the call was written, so
the lease coordinator re-queues the cell and writes it under a fresh grant.

With a pooled connection ``t1 - t0`` is well under a millisecond, so the normal
path is one BEGIN, as before, with a timeout ``S + A`` (4 s) shorter. Leased
writes need ``T >= MIN_LEASED_TX_TIMEOUT_MS`` (checked by the CLI for stream
mode and bulk ``--edge-leases``, :func:`require_fenced_budget`). Writes without
a lease deadline (planned writers, classic loaders, bulk node loaders) keep
their flat timeout and are not affected.
"""
from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import Callable, Iterator

from src.loader.lease_client import LeaseCoordinatorError

logger = logging.getLogger(__name__)

# Neo4j's default db.transaction.monitor.check.interval (2 s) plus 1 s of headroom
# for the terminated transaction to notice: an expired transaction is ended at
# most this long after its timeout.
TX_MONITOR_SLACK_SECONDS = 3.0
# Longest accepted time from just before begin_transaction() (connection
# acquisition included) to the server's BEGIN acknowledgement.
BEGIN_ALLOWANCE_SECONDS = 1.0
# Never begin a leased transaction with less than this Neo4j timeout.
MIN_TX_TIMEOUT_SECONDS = 1.0
# The smallest loading.bucket_lease.tx_timeout_ms that leaves a leased write any budget.
MIN_LEASED_TX_TIMEOUT_MS = int((TX_MONITOR_SLACK_SECONDS + BEGIN_ALLOWANCE_SECONDS + MIN_TX_TIMEOUT_SECONDS) * 1000) + 1000


class LeaseDeadlineExceeded(LeaseCoordinatorError):
    """Too little of the lease deadline is left to begin a fenced transaction.

    Raised before any statement of the write ran, so nothing of the call was
    written: the coordinator re-queues the cell and writes it under a fresh grant.
    """


def require_fenced_budget(tx_timeout_ms: int, feature: str) -> None:
    """Refuse a lease-mode run whose ``tx_timeout_ms`` leaves no fenced budget.

    A lease-mode check only, not a schema rule: planned and classic bulk use
    ``tx_timeout_ms`` as a flat timeout and keep loading any value the schema allows.
    """
    if tx_timeout_ms < MIN_LEASED_TX_TIMEOUT_MS:
        raise ValueError(
            f"{feature} requires loading.bucket_lease.tx_timeout_ms >= {MIN_LEASED_TX_TIMEOUT_MS} "
            f"(got {tx_timeout_ms}): a leased transaction's Neo4j timeout is T minus "
            f"{TX_MONITOR_SLACK_SECONDS + BEGIN_ALLOWANCE_SECONDS:g} s (transaction-monitor slack and BEGIN "
            f"allowance) and must stay above {MIN_TX_TIMEOUT_SECONDS:g} s"
        )


def fenced_timeout(deadline: float, now: float) -> float:
    """The Neo4j timeout a transaction begun at ``now`` may have under ``deadline``."""
    return deadline - now - TX_MONITOR_SLACK_SECONDS - BEGIN_ALLOWANCE_SECONDS


def check_deadline(deadline: float, now: float, what: str) -> float:
    """Return :func:`fenced_timeout`, or raise :class:`LeaseDeadlineExceeded` when it is too small."""
    budget = fenced_timeout(deadline, now)
    if budget <= MIN_TX_TIMEOUT_SECONDS:
        raise LeaseDeadlineExceeded(f"{what} lease deadline exceeded before transaction; aborting instead of writing")
    return budget


@contextmanager
def begin_fenced_transaction(
    session, *, deadline: float | None, tx_timeout_seconds: float | None, clock: Callable[[], float],
    what: str, metadata: dict | None = None,
) -> Iterator[object]:
    """Begin a transaction that Neo4j ends by ``deadline`` at the latest (module docstring).

    ``deadline=None`` (no lease): one plain BEGIN with ``tx_timeout_seconds``, unchanged.
    """
    kwargs: dict = {"metadata": metadata} if metadata else {}
    if deadline is None:
        if tx_timeout_seconds is not None:
            kwargs["timeout"] = tx_timeout_seconds
        with session.begin_transaction(**kwargs) as transaction:
            yield transaction
        return
    while True:
        started = clock()
        timeout = check_deadline(deadline, started, what)
        if tx_timeout_seconds is not None:
            timeout = min(timeout, tx_timeout_seconds)
        late = None
        with session.begin_transaction(**kwargs, timeout=timeout) as transaction:
            begun = clock()
            if begun - started <= BEGIN_ALLOWANCE_SECONDS:
                yield transaction
                return
            # Leaving the block without commit rolls back; no statement ran, so no lock was taken.
            late = begun - started
        logger.info(
            "stage=lease-fence %s reason=late_transaction_begin begin_s=%.3f allowance_s=%.1f "
            "detail=rolled back before any statement; retrying with the budget left",
            what, late, BEGIN_ALLOWANCE_SECONDS,
        )
