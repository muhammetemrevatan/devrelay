import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from math import isfinite
from secrets import compare_digest
from uuid import UUID

from devrelay.application.contracts.claims import (
    ClaimReplayConflict,
    ClaimRequestIdentity,
    WorkerClaimRequest,
)
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerHeartbeatIntent,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.domain.jobs.values import _utc, _uuid

MAXIMUM_LEASE_SECONDS = 300
MAXIMUM_LEASE_REVISION = (1 << 63) - 1


class HeartbeatRejected(Exception):
    """The known assignment cannot accept this heartbeat input."""


class HeartbeatAdmissionRejected(Exception):
    """New heartbeat identities exceed a bounded durable admission policy."""


@dataclass(frozen=True, slots=True, kw_only=True)
class HeartbeatLimits:
    lease_seconds: int = 120
    minimum_interval_seconds: int = 5
    attempt_receipts: int = 4096
    worker_receipts: int = 100_000
    global_receipts: int = 1_000_000

    def __post_init__(self) -> None:
        for name, minimum, maximum in (
            ("lease_seconds", 30, MAXIMUM_LEASE_SECONDS),
            ("minimum_interval_seconds", 1, 10),
            ("attempt_receipts", 1, 1_000_000),
            ("worker_receipts", 1, 1_000_000),
            ("global_receipts", 1, 10_000_000),
        ):
            value = getattr(self, name)
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError("Heartbeat limits require bounded positive integers.")


def _lease_revision(value: int) -> None:
    if type(value) is not int or not 1 <= value <= MAXIMUM_LEASE_REVISION:
        raise ValueError("Lease revision requires a bounded positive integer.")


def _initial_version(value: int) -> None:
    if type(value) is not int or value != 1:
        raise ValueError("Heartbeat protocol and generation must be one.")


@dataclass(frozen=True, slots=True, kw_only=True)
class LeaseAuthorityObservation:
    """Current nonsecret lease evidence, separate from immutable claim anchors.

    This observation is neither durable execution authority nor launch permission.
    Credential scope/expiry and the worker's conservative deadline remain gates.
    """

    reference: AssignmentReference
    lease_revision: int
    observed_at: datetime
    updated_at: datetime
    current_expires_at: datetime
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.reference, AssignmentReference):
            raise ValueError("Lease authority requires original assignment anchors.")
        _lease_revision(self.lease_revision)
        for name in ("observed_at", "updated_at", "current_expires_at", "revoked_at"):
            value = getattr(self, name)
            if value is None and name != "revoked_at":
                raise ValueError("Lease authority requires UTC timestamps.")
            if value is not None:
                if not isinstance(value, datetime):
                    raise ValueError("Lease authority requires UTC timestamps.")
                object.__setattr__(self, name, _utc(value))
        if (
            not self.reference.issued_at <= self.updated_at <= self.observed_at
            or self.current_expires_at < self.reference.expires_at
            or (
                self.lease_revision == 1
                and (
                    self.current_expires_at != self.reference.expires_at
                    or self.updated_at != self.reference.issued_at
                )
            )
            or self.current_expires_at - self.updated_at
            > timedelta(seconds=MAXIMUM_LEASE_SECONDS)
            or (self.revoked_at is None and self.current_expires_at <= self.updated_at)
            or (
                self.revoked_at is not None
                and not self.updated_at <= self.revoked_at <= self.observed_at
            )
        ):
            raise ValueError("Lease authority timestamps are inconsistent.")


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class HeartbeatRequest:
    """Proof-bound operation input; no worker-supplied identity grants authority."""

    claim: WorkerClaimRequest = field(repr=False)
    heartbeat_request_id: UUID
    attempt_id: UUID
    generation: int
    expected_lease_revision: int
    protocol_version: int = 1

    def __post_init__(self) -> None:
        _uuid(self.heartbeat_request_id)
        _uuid(self.attempt_id)
        _initial_version(self.generation)
        _initial_version(self.protocol_version)
        _lease_revision(self.expected_lease_revision)
        if (
            not isinstance(self.claim, WorkerClaimRequest)
            or self.claim.protocol_version != self.protocol_version
        ):
            raise ValueError("Heartbeat requires the original supported claim input.")

    def identify(self, authorization: WorkerAuthorization) -> HeartbeatIdentity:
        return HeartbeatIdentity(
            claim=self.claim.identify(authorization),
            heartbeat_request_id=self.heartbeat_request_id,
            attempt_id=self.attempt_id,
            generation=self.generation,
            expected_lease_revision=self.expected_lease_revision,
            protocol_version=self.protocol_version,
        )


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class HeartbeatIdentity:
    """Scoped versioned fingerprint; this is not a permanent heartbeat receipt."""

    claim: ClaimRequestIdentity = field(repr=False)
    heartbeat_request_id: UUID
    attempt_id: UUID
    generation: int
    expected_lease_revision: int
    protocol_version: int = 1
    fingerprint: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _uuid(self.heartbeat_request_id)
        _uuid(self.attempt_id)
        _initial_version(self.generation)
        _initial_version(self.protocol_version)
        _lease_revision(self.expected_lease_revision)
        if (
            not isinstance(self.claim, ClaimRequestIdentity)
            or self.claim.protocol_version != self.protocol_version
        ):
            raise ValueError("Heartbeat identity requires the original scoped claim.")
        payload = {
            "version": 1,
            "operation": "heartbeat",
            "route": f"/api/v1/worker/attempts/{self.attempt_id}/heartbeats",
            "worker_id": str(self.claim.worker_id),
            "heartbeat_request_id": str(self.heartbeat_request_id),
            "claim_request_id": str(self.claim.claim_request_id),
            "claim_fingerprint": self.claim.fingerprint,
            "attempt_id": str(self.attempt_id),
            "generation": self.generation,
            "expected_lease_revision": self.expected_lease_revision,
            "protocol_version": self.protocol_version,
        }
        object.__setattr__(
            self,
            "fingerprint",
            hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                    "ascii"
                )
            ).hexdigest(),
        )

    def require_replay(self, fingerprint: str) -> None:
        if (
            not isinstance(fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
            or not compare_digest(self.fingerprint, fingerprint)
        ):
            raise ClaimReplayConflict("Heartbeat conflicts with original input.")


@dataclass(frozen=True, slots=True, kw_only=True)
class HeartbeatResult:
    heartbeat_request_id: UUID
    authority: LeaseAuthorityObservation
    renewed: bool
    stop_required: bool

    def __post_init__(self) -> None:
        _uuid(self.heartbeat_request_id)
        if (
            not isinstance(self.authority, LeaseAuthorityObservation)
            or type(self.renewed) is not bool
            or type(self.stop_required) is not bool
        ):
            raise ValueError("Heartbeat result requires typed authority metadata.")
        if (
            (
                self.authority.revoked_at is not None
                or self.authority.observed_at >= self.authority.current_expires_at
            )
            and not self.stop_required
        ) or (self.renewed and self.stop_required):
            raise ValueError("Stopped heartbeat authority cannot renew or execute.")

    def require_correlation(
        self, request: HeartbeatRequest, authorization: WorkerAuthorization
    ) -> None:
        """Validate transport evidence without granting execution permission.

        A shorter rotated credential need not change an existing lease deadline.
        Consumers cap local authority by that current credential even when the
        unrenewed observation retains a later server deadline.
        """
        if not isinstance(request, HeartbeatRequest) or not isinstance(
            authorization, WorkerAuthorization
        ):
            raise ValueError("Heartbeat correlation requires typed request and scope.")
        reference = self.authority.reference
        if (
            self.heartbeat_request_id != request.heartbeat_request_id
            or reference.attempt_id != request.attempt_id
            or reference.generation != request.generation
            or reference.worker_id != authorization.worker_id
            or reference.repository_id not in authorization.allowed_repository_ids
            or self.authority.lease_revision < request.expected_lease_revision
            or (
                not self.stop_required
                and self.authority.observed_at >= authorization.credential_expires_at
            )
            or (
                self.renewed
                and (
                    self.authority.lease_revision != request.expected_lease_revision + 1
                    or self.authority.current_expires_at
                    > authorization.credential_expires_at
                )
            )
        ):
            raise ValueError("Heartbeat response does not match current request scope.")


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerHeartbeatExchange:
    """Fresh process-local timing evidence; never persist or treat as launch permission."""

    intent: WorkerHeartbeatIntent
    result: HeartbeatResult
    request_started_at: float
    received_at: float

    def __post_init__(self) -> None:
        if (
            not isinstance(self.intent, WorkerHeartbeatIntent)
            or not isinstance(self.result, HeartbeatResult)
            or self.result.heartbeat_request_id != self.intent.heartbeat_request_id
            or self.result.authority.reference.attempt_id != self.intent.attempt_id
            or self.result.authority.reference.generation != self.intent.generation
        ):
            raise ValueError("Heartbeat exchange requires correlated typed evidence.")
        for name in ("request_started_at", "received_at"):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not 0 <= value <= 2**53 - 1
                or not isfinite(value)
            ):
                raise ValueError(
                    "Heartbeat exchange requires bounded monotonic readings."
                )
            object.__setattr__(self, name, float(value))
        if self.received_at < self.request_started_at:
            raise ValueError("Heartbeat receipt cannot precede request start.")
