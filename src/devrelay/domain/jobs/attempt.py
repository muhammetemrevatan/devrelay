from dataclasses import dataclass, replace
from datetime import datetime
from uuid import UUID

from devrelay.domain.jobs.values import (
    AttemptStatus,
    InvalidJobEvidence,
    InvalidJobTransition,
    StaleAttempt,
    _utc,
    _uuid,
)


class LeaseExpired(StaleAttempt):
    """The assignment has reached its authority deadline."""


class LeaseRevoked(StaleAttempt):
    """The control service has withdrawn assignment authority."""


def _positive(value: int) -> None:
    if type(value) is not int or value <= 0:
        raise InvalidJobEvidence("Attempt numbers and generations must be positive.")


@dataclass(frozen=True, slots=True, kw_only=True)
class Lease:
    """Nonsecret authority metadata; authentication and proof checks are external.

    Supply trusted control timestamps, eventually sampled from DB time after
    locking. This value neither reads a clock nor proves process containment.
    """

    attempt_id: UUID
    worker_id: UUID
    generation: int
    issued_at: datetime
    updated_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        _uuid(self.attempt_id)
        _uuid(self.worker_id)
        _positive(self.generation)
        for name in ("issued_at", "updated_at", "expires_at", "revoked_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value))
        if self.updated_at < self.issued_at or self.expires_at <= self.issued_at:
            raise InvalidJobEvidence("Lease timestamps are inconsistent.")
        if self.revoked_at is None:
            if self.updated_at >= self.expires_at:
                raise InvalidJobEvidence("Live lease updates must precede expiry.")
        elif not self.issued_at <= self.revoked_at == self.updated_at:
            raise InvalidJobEvidence("Revocation must be the final lease update.")

    def _time(self, now: datetime) -> datetime:
        timestamp = _utc(now)
        if timestamp < self.updated_at:
            raise InvalidJobEvidence("Lease timestamps cannot move backwards.")
        return timestamp

    def require_authority(
        self, *, worker_id: UUID, generation: int, now: datetime
    ) -> None:
        _uuid(worker_id)
        _positive(generation)
        if worker_id != self.worker_id or generation != self.generation:
            raise StaleAttempt("The assigned worker and generation are required.")
        timestamp = self._time(now)
        if self.revoked_at is not None:
            raise LeaseRevoked("Assignment authority has been revoked.")
        if timestamp >= self.expires_at:
            raise LeaseExpired("Assignment authority has expired.")

    def renew(
        self,
        *,
        worker_id: UUID,
        generation: int,
        expires_at: datetime,
        now: datetime,
    ) -> Lease:
        self.require_authority(worker_id=worker_id, generation=generation, now=now)
        deadline = _utc(expires_at)
        if deadline <= self.expires_at:
            raise InvalidJobEvidence("Renewal must extend the current deadline.")
        return replace(self, updated_at=_utc(now), expires_at=deadline)

    def revoke(self, *, now: datetime) -> Lease:
        timestamp = self._time(now)
        if self.revoked_at is not None:
            raise LeaseRevoked("Assignment authority has been revoked.")
        return replace(self, updated_at=timestamp, revoked_at=timestamp)


_EXECUTING = frozenset(
    (AttemptStatus.RUNNING, AttemptStatus.TESTING, AttemptStatus.PUBLISHING_PR)
)
_ENDED = frozenset(
    (AttemptStatus.SUCCEEDED, AttemptStatus.FAILED, AttemptStatus.CANCELLED)
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionAttempt:
    """One assignment, retaining its reservation until trusted completion.

    Application coordinates this value with Job in one versioned transaction.
    Expiry/revocation alone cannot establish that a worker or child process died.
    """

    id: UUID
    job_id: UUID
    number: int
    lease: Lease
    updated_at: datetime
    status: AttemptStatus = AttemptStatus.CLAIMED
    acknowledged_at: datetime | None = None
    ended_at: datetime | None = None

    def __post_init__(self) -> None:
        _uuid(self.id)
        _uuid(self.job_id)
        _positive(self.number)
        if not isinstance(self.lease, Lease) or self.lease.attempt_id != self.id:
            raise InvalidJobEvidence("Lease must belong to this attempt.")
        if not isinstance(self.status, AttemptStatus):
            raise InvalidJobEvidence("Attempt status must be typed.")
        for name in ("updated_at", "acknowledged_at", "ended_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value))
        if self.updated_at < self.lease.updated_at:
            raise InvalidJobEvidence("Attempt cannot precede its lease update.")
        if self.acknowledged_at is not None:
            if (
                not self.lease.issued_at <= self.acknowledged_at <= self.updated_at
                or self.acknowledged_at >= self.lease.expires_at
                or (
                    self.lease.revoked_at is not None
                    and self.acknowledged_at > self.lease.revoked_at
                )
            ):
                raise InvalidJobEvidence("Acknowledgement timestamps are inconsistent.")
            if self.status is AttemptStatus.CLAIMED:
                raise InvalidJobEvidence("Acknowledged work cannot remain claimed.")
        elif self.status in _EXECUTING or self.status is AttemptStatus.SUCCEEDED:
            raise InvalidJobEvidence("Execution requires assignment acknowledgement.")
        if (self.status in _ENDED) != (self.ended_at is not None):
            raise InvalidJobEvidence("Completed attempts require a recorded end.")
        if self.ended_at is not None:
            if (
                not self.lease.issued_at <= self.ended_at == self.updated_at
                or self.lease.revoked_at is None
                or self.lease.revoked_at > self.ended_at
                or (
                    self.acknowledged_at is not None
                    and self.acknowledged_at > self.ended_at
                )
            ):
                raise InvalidJobEvidence("Completion must retain revoked authority.")
        if (
            self.status is AttemptStatus.LEASE_EXPIRED
            and self.updated_at < self.lease.expires_at
        ):
            raise InvalidJobEvidence("An expired attempt must reach its deadline.")

    @property
    def reserves_execution(self) -> bool:
        return self.ended_at is None

    def _time(self, now: datetime) -> datetime:
        timestamp = _utc(now)
        if timestamp < self.updated_at:
            raise InvalidJobEvidence("Attempt timestamps cannot move backwards.")
        return timestamp

    def _require_open(self) -> None:
        if self.ended_at is not None:
            raise InvalidJobTransition("The attempt has already ended.")

    def acknowledge(
        self, *, worker_id: UUID, generation: int, now: datetime
    ) -> ExecutionAttempt:
        if self.status is not AttemptStatus.CLAIMED:
            raise InvalidJobTransition("Only a claimed attempt can be acknowledged.")
        timestamp = self._time(now)
        self.lease.require_authority(
            worker_id=worker_id, generation=generation, now=timestamp
        )
        return replace(
            self,
            status=AttemptStatus.RUNNING,
            acknowledged_at=timestamp,
            updated_at=timestamp,
        )

    def renew(
        self,
        *,
        worker_id: UUID,
        generation: int,
        expires_at: datetime,
        now: datetime,
    ) -> ExecutionAttempt:
        self._require_open()
        timestamp = self._time(now)
        lease = self.lease.renew(
            worker_id=worker_id,
            generation=generation,
            expires_at=expires_at,
            now=timestamp,
        )
        return replace(self, lease=lease, updated_at=timestamp)

    def revoke(self, *, now: datetime) -> ExecutionAttempt:
        self._require_open()
        timestamp = self._time(now)
        return replace(
            self, lease=self.lease.revoke(now=timestamp), updated_at=timestamp
        )

    def expire(self, *, now: datetime) -> ExecutionAttempt:
        self._require_open()
        if self.status is AttemptStatus.LEASE_EXPIRED:
            raise InvalidJobTransition("Lease expiry has already been recorded.")
        timestamp = self._time(now)
        if timestamp < self.lease.expires_at:
            raise InvalidJobTransition("The lease deadline has not been reached.")
        return replace(self, status=AttemptStatus.LEASE_EXPIRED, updated_at=timestamp)

    def confirm_stopped(
        self, *, stopped_attempt_id: UUID, outcome: AttemptStatus, now: datetime
    ) -> ExecutionAttempt:
        """Control calls this only after independently verifying containment."""
        self._require_open()
        _uuid(stopped_attempt_id)
        if stopped_attempt_id != self.id:
            raise StaleAttempt("Containment must name the current attempt.")
        if not isinstance(outcome, AttemptStatus) or outcome not in (
            AttemptStatus.FAILED,
            AttemptStatus.CANCELLED,
        ):
            raise InvalidJobTransition("Stopped work must fail or be cancelled.")
        if (
            self.lease.revoked_at is None
            and self.status is not AttemptStatus.LEASE_EXPIRED
        ):
            raise InvalidJobTransition("Withdraw authority before confirming a stop.")
        timestamp = self._time(now)
        lease = (
            self.lease.revoke(now=timestamp)
            if self.lease.revoked_at is None
            else self.lease
        )
        return replace(
            self, lease=lease, status=outcome, updated_at=timestamp, ended_at=timestamp
        )
