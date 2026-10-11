"""The shared Neo4j write-error taxonomy.

The retry consumer defined these classes first (P5-S4, owner decision D1).
They are factored out here so that the live stream loaders' outage ride-out
(:mod:`src.loader.write_outage`) uses exactly the same classes. Moving the
code here did not change it: ``src.loader.retry_consumer`` re-exports every
name under its old private alias, and the classification table test in
``tests/test_retry_consumer.py`` still pins it.

The classes, by what a failed write says about the rows it carried:

* ``"connectivity"``: nothing. Neo4j or the lease server is unavailable;
  a connection pool or acquisition timeout; a transient error;
  authentication, database or cluster state; a read-only database; a drain
  abandon.
* ``"server"``: global memory-pool pressure.
* ``"capacity"``: a transaction timeout or the per-transaction memory limit.
* ``"row"``: an allow-listed, row-specific Neo4j code.
* ``"unknown"``: anything else.
"""
from __future__ import annotations

from neo4j.exceptions import ClientError, ConnectionPoolError, ServiceUnavailable, SessionExpired, TransientError

from src.loader.lease_client import LeaseCoordinatorError

ROW_ERROR_CODES = frozenset({
    "Neo.ClientError.Schema.ConstraintValidationFailed",
    "Neo.ClientError.Schema.ConstraintViolation",
    "Neo.ClientError.Schema.ConstraintVerificationFailed",
    "Neo.ClientError.Statement.TypeError",
    "Neo.ClientError.Statement.ArgumentError",
    "Neo.ClientError.Statement.ArithmeticError",
    "Neo.ClientError.Statement.EntityNotFound",
    "Neo.ClientError.Statement.PropertyNotFound",
})
"""The only Neo4j codes that say, on their own, that the *row* is what Neo4j
rejects. Every other ``ClientError`` (a missing procedure, a syntax error after
a server change ...) is ``unknown``: it may be a server-side condition, so it is
never counted against a row without further evidence."""

SERVER_CODE_PREFIXES = ("Neo.TransientError.General.MemoryPoolOutOfMemoryError",)
"""Memory pressure on the global transaction pool: smaller transactions may
fit, so the slice is bisected. One huge row can exhaust the pool by itself, so a
row that fails alone with it is counted under the evidence rule like
``capacity`` (orchestrator decision under D1, superseding round 4's F1 "never
count"): only once a same-type WRITTEN came after its first uncounted failure."""

CAPACITY_CODE_PREFIXES = (
    "Neo.ClientError.Transaction.TransactionTimedOut",
    "Neo.TransientError.General.TransactionMemoryLimit",
)
"""A transaction that timed out or hit the per-transaction memory limit: one
huge row can cause it, so it is bisected like a row error, but it is only
counted against a row under the evidence rule (D1)."""

CONNECTIVITY_CODE_PREFIXES = (
    "Neo.ClientError.Security.", "Neo.ClientError.Database.", "Neo.ClientError.Cluster.",
    "Neo.ClientError.General.WriteOnReadOnlyAccessDatabase",
    "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase",
    "Neo.ClientError.General.ReadOnly",
)

FATAL_CODE_PREFIXES = ("Neo.ClientError.Security.", "Neo.ClientError.Database.DatabaseNotFound")
"""Connectivity-class codes that do not heal by waiting: authentication and a
missing database. A continuous run of them for ``FATAL_OUTAGE_SECONDS`` is
fatal (owner decision D2)."""

FATAL_OUTAGE_SECONDS = 600.0
"""Continuous fatal-class errors (authentication, a missing database) for this
long make the process exit non-zero; the stream supervisor then stops the fleet
(owner decision D2)."""

FATAL_GAP_SECONDS = 60.0
"""A fatal-class run is continuous only if its errors are at most this far
apart (the edge backoff caps at 5 s, so a real run is far denser)."""


def classify_write_error(exc: BaseException) -> str:
    """Classify a failed write by what it says about the rows.

    * ``"connectivity"``: nothing (Neo4j or the lease server unavailable, a
      pool/acquisition timeout, a transient error, authentication, database or
      cluster state, a read-only database, a drain abandon). Never bisected,
      never counted.
    * ``"server"``: global memory-pool pressure. Bisected, counted only under
      the evidence rule.
    * ``"capacity"``: a transaction timeout or per-transaction memory limit.
      Bisected, counted only under the evidence rule.
    * ``"row"``: an allow-listed row-specific Neo4j code. Bisected and counted.
    * ``"unknown"``: anything else. Bisected, counted only under the evidence
      rule.
    """
    from src.loader.edge_execution import LeaseDrainAbandoned

    code = getattr(exc, "code", None) or ""
    if code.startswith(CAPACITY_CODE_PREFIXES):
        return "capacity"
    if code.startswith(SERVER_CODE_PREFIXES):
        return "server"
    if isinstance(exc, (
        ServiceUnavailable, SessionExpired, ConnectionPoolError, LeaseCoordinatorError, LeaseDrainAbandoned,
        TimeoutError, OSError,
    )) or code.startswith(CONNECTIVITY_CODE_PREFIXES) or isinstance(exc, TransientError):
        return "connectivity"
    if isinstance(exc, ClientError) and code in ROW_ERROR_CODES:
        return "row"
    return "unknown"


def is_fatal_error(exc: BaseException | None) -> bool:
    """Whether ``exc`` carries a fatal-class Neo4j code (authentication, a missing database)."""
    return exc is not None and (getattr(exc, "code", None) or "").startswith(FATAL_CODE_PREFIXES)


def is_neo4j_coded(exc: BaseException | None) -> bool:
    """Whether Neo4j itself classified ``exc`` (a ``Neo.*`` status code)."""
    return exc is not None and (getattr(exc, "code", None) or "").startswith("Neo.")
