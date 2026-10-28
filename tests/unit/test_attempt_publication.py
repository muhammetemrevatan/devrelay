from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import pytest

from devrelay.domain.jobs.attempt import (
    ExecutionAttempt,
    Lease,
    LeaseExpired,
    LeaseRevoked,
)
from devrelay.domain.jobs.values import (
    AttemptStatus,
    InvalidJobEvidence,
    InvalidJobTransition,
    StaleAttempt,
)

_NOW = datetime(2026, 10, 18, 12, tzinfo=UTC)
_ATTEMPT = UUID("11111111-1111-4111-8111-111111111111")
_WORKER = UUID("22222222-2222-4222-8222-222222222222")
_JOB = UUID("33333333-3333-4333-8333-333333333333")
_OTHER = UUID("44444444-4444-4444-8444-444444444444")
_EXPIRY = _NOW + timedelta(seconds=60)


def running() -> ExecutionAttempt:
    claimed = ExecutionAttempt(
        id=_ATTEMPT,
        job_id=_JOB,
        number=1,
        lease=Lease(
            attempt_id=_ATTEMPT,
            worker_id=_WORKER,
            generation=1,
            issued_at=_NOW,
            updated_at=_NOW,
            expires_at=_EXPIRY,
        ),
        updated_at=_NOW,
    )
    return claimed.acknowledge(worker_id=_WORKER, generation=1, now=_NOW)


def accept(attempt: ExecutionAttempt, *, now: datetime = _NOW) -> ExecutionAttempt:
    return attempt.accept_coding_result(
        worker_id=_WORKER, generation=1, result_attempt_id=_ATTEMPT, now=now
    )


def finish(attempt: ExecutionAttempt, *, now: datetime = _NOW) -> ExecutionAttempt:
    return attempt.complete_publication(
        worker_id=_WORKER,
        generation=1,
        published_attempt_id=_ATTEMPT,
        stopped_attempt_id=_ATTEMPT,
        now=now,
    )


def test_acceptance_preserves_original_assignment_and_open_authority() -> None:
    original = running()
    timestamp = _NOW + timedelta(seconds=2)

    published = accept(original, now=timestamp)

    assert published.status is AttemptStatus.PUBLISHING_PR
    assert published.updated_at == timestamp
    assert published.lease is original.lease
    assert published.acknowledged_at == original.acknowledged_at
    assert (published.id, published.job_id, published.number) == (_ATTEMPT, _JOB, 1)
    assert published.ended_at is None
    assert published.reserves_execution
    assert original.status is AttemptStatus.RUNNING
    assert original.updated_at == _NOW


def test_verified_publication_revokes_once_and_ends_only_the_attempt() -> None:
    publishing = accept(running())
    timestamp = _NOW + timedelta(seconds=3)

    completed = finish(publishing, now=timestamp)

    assert completed.status is AttemptStatus.SUCCEEDED
    assert completed.updated_at == completed.ended_at == timestamp
    assert completed.lease.revoked_at == completed.lease.updated_at == timestamp
    assert completed.lease.issued_at == _NOW
    assert completed.lease.expires_at == _EXPIRY
    assert completed.acknowledged_at == publishing.acknowledged_at
    assert (completed.id, completed.job_id, completed.number) == (_ATTEMPT, _JOB, 1)
    assert not completed.reserves_execution
    assert publishing.status is AttemptStatus.PUBLISHING_PR
    assert publishing.ended_at is publishing.lease.revoked_at is None
    with pytest.raises(InvalidJobTransition):
        finish(completed, now=timestamp)
    with pytest.raises(InvalidJobTransition):
        accept(completed, now=timestamp)
    with pytest.raises(LeaseRevoked):
        completed.lease.revoke(now=timestamp)


def test_runtime_current_renewed_lease_accepts_and_completes_after_original_expiry() -> (
    None
):
    historical = running()
    renewal = _NOW + timedelta(seconds=50)
    current_expiry = _NOW + timedelta(seconds=170)
    # Runtime aggregate constructs this from the separately locked authority row.
    current = replace(
        historical,
        lease=replace(historical.lease, updated_at=renewal, expires_at=current_expiry),
        updated_at=renewal,
    )

    publishing = accept(current, now=_EXPIRY + timedelta(seconds=1))
    completed = finish(publishing, now=_EXPIRY + timedelta(seconds=2))

    assert completed.status is AttemptStatus.SUCCEEDED
    assert completed.lease.expires_at == current_expiry
    assert historical.lease.expires_at == _EXPIRY
    assert historical.lease.updated_at == _NOW
    with pytest.raises(LeaseExpired):
        accept(historical, now=_EXPIRY + timedelta(seconds=1))


@pytest.mark.parametrize("operation", ["accept", "complete"])
@pytest.mark.parametrize("worker,generation", [(_OTHER, 1), (_WORKER, 2)])
def test_publication_requires_current_worker_and_generation(
    operation: str, worker: UUID, generation: int
) -> None:
    original = running()
    with pytest.raises(StaleAttempt):
        if operation == "accept":
            original.accept_coding_result(
                worker_id=worker,
                generation=generation,
                result_attempt_id=_ATTEMPT,
                now=_NOW,
            )
        else:
            accept(original).complete_publication(
                worker_id=worker,
                generation=generation,
                published_attempt_id=_ATTEMPT,
                stopped_attempt_id=_ATTEMPT,
                now=_NOW,
            )
    assert original.lease.revoked_at is original.ended_at is None


@pytest.mark.parametrize("operation", ["accept", "complete"])
@pytest.mark.parametrize("offset", [0, 1])
def test_normal_publication_rejects_exact_deadline_and_later(
    operation: str, offset: int
) -> None:
    attempt = running()
    action = accept if operation == "accept" else finish
    if operation == "complete":
        attempt = accept(attempt)
    with pytest.raises(LeaseExpired):
        action(attempt, now=_EXPIRY + timedelta(seconds=offset))
    assert attempt.lease.revoked_at is attempt.ended_at is None


@pytest.mark.parametrize("operation", ["accept", "complete"])
def test_normal_publication_cannot_reuse_revoked_authority(operation: str) -> None:
    attempt = running()
    action = accept if operation == "accept" else finish
    if operation == "complete":
        attempt = accept(attempt)
    revoked = attempt.revoke(now=_NOW)
    with pytest.raises(LeaseRevoked):
        action(revoked, now=_NOW)
    assert revoked.ended_at is None


@pytest.mark.parametrize("operation", ["accept", "complete"])
@pytest.mark.parametrize("clock", ["phase", "renewal"])
def test_publication_rejects_time_before_current_phase_or_renewal(
    operation: str, clock: str
) -> None:
    attempt = running()
    if operation == "complete":
        attempt = accept(attempt)
    timestamp = _NOW + timedelta(seconds=10)
    if clock == "phase":
        attempt = replace(attempt, updated_at=timestamp)
    else:
        attempt = replace(
            attempt,
            updated_at=timestamp,
            lease=replace(attempt.lease, updated_at=timestamp),
        )
    action = accept if operation == "accept" else finish
    with pytest.raises(InvalidJobEvidence):
        action(attempt, now=timestamp - timedelta(microseconds=1))
    assert attempt.lease.revoked_at is attempt.ended_at is None


@pytest.mark.parametrize("association", ["result", "publication", "stop"])
@pytest.mark.parametrize("foreign", [_OTHER, cast(UUID, True)])
def test_foreign_or_untyped_association_ids_cannot_complete_other_attempts(
    association: str, foreign: UUID
) -> None:
    with pytest.raises(
        StaleAttempt if isinstance(foreign, UUID) else InvalidJobEvidence
    ):
        if association == "result":
            running().accept_coding_result(
                worker_id=_WORKER,
                generation=1,
                result_attempt_id=foreign,
                now=_NOW,
            )
        else:
            accept(running()).complete_publication(
                worker_id=_WORKER,
                generation=1,
                published_attempt_id=foreign
                if association == "publication"
                else _ATTEMPT,
                stopped_attempt_id=foreign if association == "stop" else _ATTEMPT,
                now=_NOW,
            )


@pytest.mark.parametrize("operation", ["accept", "complete"])
@pytest.mark.parametrize(
    "status",
    [AttemptStatus.CLAIMED, AttemptStatus.TESTING, AttemptStatus.LEASE_EXPIRED],
)
def test_only_the_exact_expected_open_phase_can_advance(
    operation: str, status: AttemptStatus
) -> None:
    attempt = running()
    if status is AttemptStatus.CLAIMED:
        attempt = replace(attempt, status=status, acknowledged_at=None)
    elif status is AttemptStatus.LEASE_EXPIRED:
        attempt = attempt.expire(now=_EXPIRY)
    else:
        attempt = replace(attempt, status=status)
    with pytest.raises(InvalidJobTransition):
        (accept if operation == "accept" else finish)(attempt, now=attempt.updated_at)


def test_publication_cannot_skip_coding_acceptance_or_repeat_it() -> None:
    with pytest.raises(InvalidJobTransition):
        finish(running())
    with pytest.raises(InvalidJobTransition):
        accept(accept(running()))
