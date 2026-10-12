from dataclasses import dataclass
from datetime import datetime
from math import isfinite

from devrelay.application.contracts.heartbeats import LeaseAuthorityObservation
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.contracts.workers import WorkerAuthorization

_MAX_MONOTONIC_SECONDS = 2**53 - 1


class LeaseDeadlineRejected(ValueError):
    """Invalid or incoherent deadline input; execution must fail closed."""


def _monotonic(value: float) -> float:
    if (
        type(value) not in (int, float)
        or not 0 <= value <= _MAX_MONOTONIC_SECONDS
        or not isfinite(value)
    ):
        raise LeaseDeadlineRejected(
            "Deadline timestamps require bounded finite numbers."
        )
    return float(value)


def _margin(value: float) -> float:
    value = _monotonic(value)
    if not 0 < value <= 300:
        raise LeaseDeadlineRejected("Stop margin must be positive and bounded.")
    return value


def _budget(
    authority: LeaseAuthorityObservation,
    authorization: WorkerAuthorization,
    *,
    request_started_at: float,
    received_at: float,
    margin: float,
    stop_required: bool,
    observed_at_floor: datetime | None = None,
) -> tuple[float, bool]:
    if (
        not isinstance(authority, LeaseAuthorityObservation)
        or not isinstance(authorization, WorkerAuthorization)
        or authorization.worker_id != authority.reference.worker_id
        or authority.reference.repository_id not in authorization.allowed_repository_ids
    ):
        raise LeaseDeadlineRejected(
            "Deadline observation requires matching current scope."
        )
    if type(stop_required) is not bool:
        raise LeaseDeadlineRejected("Stop instruction must be a boolean.")
    request_started_at, received_at = (
        _monotonic(request_started_at),
        _monotonic(received_at),
    )
    if received_at < request_started_at:
        raise LeaseDeadlineRejected("Monotonic receipt cannot precede request start.")
    expiry = min(authority.current_expires_at, authorization.credential_expires_at)
    observed_at = authority.observed_at
    if observed_at_floor is not None:
        observed_at = max(observed_at, observed_at_floor)
    remaining = (expiry - observed_at).total_seconds()
    # Anchoring to request start spends round-trip time and requires no wall clock.
    stop_at = request_started_at + remaining - margin
    stopped = (
        stop_required or authority.revoked_at is not None or stop_at <= received_at
    )
    return (min(received_at, max(0.0, stop_at)) if stopped else stop_at), stopped


@dataclass(frozen=True, slots=True, kw_only=True)
class LeaseDeadline:
    """Process-local stop budget from trusted control evidence, never launch authority.

    It must not be restored from a journal or reused across process restarts.
    Callers supply monotonic readings and stop independently on validation failure.
    The clock must account for suspend, or callers must invalidate on resume;
    deterministic deadline arithmetic does not certify native suspend handling.
    """

    authority: LeaseAuthorityObservation
    authority_request_started_at: float
    request_started_at: float
    received_at: float
    stop_at: float
    stop_margin_seconds: float
    stopped: bool

    def __post_init__(self) -> None:
        if not isinstance(self.authority, LeaseAuthorityObservation):
            raise LeaseDeadlineRejected(
                "Deadline requires a typed authority observation."
            )
        for name in (
            "authority_request_started_at",
            "request_started_at",
            "received_at",
            "stop_at",
        ):
            object.__setattr__(self, name, _monotonic(getattr(self, name)))
        object.__setattr__(
            self, "stop_margin_seconds", _margin(self.stop_margin_seconds)
        )
        lease_cap = (
            self.authority_request_started_at
            + (
                self.authority.current_expires_at - self.authority.observed_at
            ).total_seconds()
            - self.stop_margin_seconds
        )
        if (
            type(self.stopped) is not bool
            or self.received_at < self.request_started_at
            or self.authority_request_started_at > self.request_started_at
            or self.stopped != (self.stop_at <= self.received_at)
            or (self.authority.revoked_at is not None and not self.stopped)
            or (not self.stopped and self.stop_at > lease_cap)
        ):
            raise LeaseDeadlineRejected("Deadline state is inconsistent.")

    @property
    def reference(self) -> AssignmentReference:
        return self.authority.reference

    @property
    def lease_revision(self) -> int:
        return self.authority.lease_revision

    @classmethod
    def from_observation(
        cls,
        authority: LeaseAuthorityObservation,
        authorization: WorkerAuthorization,
        *,
        request_started_at: float,
        received_at: float,
        stop_margin_seconds: float,
        stop_required: bool = False,
    ) -> LeaseDeadline:
        margin = _margin(stop_margin_seconds)
        stop_at, stopped = _budget(
            authority,
            authorization,
            request_started_at=request_started_at,
            received_at=received_at,
            margin=margin,
            stop_required=stop_required,
        )
        return cls(
            authority=authority,
            authority_request_started_at=request_started_at,
            request_started_at=request_started_at,
            received_at=received_at,
            stop_at=stop_at,
            stop_margin_seconds=margin,
            stopped=stopped,
        )

    def remaining_seconds(self, now: float) -> float:
        now = _monotonic(now)
        if now < self.received_at:
            raise LeaseDeadlineRejected("Monotonic observation cannot move backwards.")
        return 0.0 if self.stopped else max(0.0, self.stop_at - now)

    def must_stop(self, now: float) -> bool:
        return self.remaining_seconds(now) == 0.0

    def observe(
        self,
        authority: LeaseAuthorityObservation,
        authorization: WorkerAuthorization,
        *,
        request_started_at: float,
        received_at: float,
        stop_required: bool = False,
    ) -> LeaseDeadline:
        candidate, stopped = _budget(
            authority,
            authorization,
            request_started_at=request_started_at,
            received_at=received_at,
            margin=self.stop_margin_seconds,
            stop_required=stop_required,
            observed_at_floor=self.authority.observed_at,
        )
        if authority.reference != self.reference:
            raise LeaseDeadlineRejected("Deadline update changed original assignment.")
        if received_at < self.received_at:
            raise LeaseDeadlineRejected("Monotonic receipt cannot move backwards.")
        newer = authority.lease_revision > self.lease_revision
        same = authority.lease_revision == self.lease_revision
        if newer and (
            authority.observed_at < self.authority.observed_at
            or authority.updated_at < self.authority.updated_at
            or authority.current_expires_at < self.authority.current_expires_at
        ):
            raise LeaseDeadlineRejected(
                "Newer lease observation cannot regress authority."
            )
        if same and (
            authority.updated_at != self.authority.updated_at
            or authority.current_expires_at != self.authority.current_expires_at
            or authority.revoked_at != self.authority.revoked_at
        ):
            raise LeaseDeadlineRejected(
                "One lease revision cannot describe changed authority."
            )
        may_extend = newer and request_started_at >= self.request_started_at
        stop_at = candidate if may_extend else min(self.stop_at, candidate)
        credential_stop_at = (
            self.authority_request_started_at
            + (
                authorization.credential_expires_at - self.authority.observed_at
            ).total_seconds()
            - self.stop_margin_seconds
        )
        stop_at = min(stop_at, credential_stop_at)
        stopped = stopped or self.must_stop(received_at) or stop_at <= received_at
        if stopped:
            stop_at = max(0.0, min(stop_at, received_at))
        latest = self.authority
        authority_request_started_at = self.authority_request_started_at
        if newer or (same and authority.observed_at > latest.observed_at):
            latest = authority
            authority_request_started_at = request_started_at
        return LeaseDeadline(
            authority=latest,
            authority_request_started_at=authority_request_started_at,
            request_started_at=max(request_started_at, self.request_started_at),
            received_at=received_at,
            stop_at=stop_at,
            stop_margin_seconds=self.stop_margin_seconds,
            stopped=stopped,
        )
