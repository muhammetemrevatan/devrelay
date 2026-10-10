from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from typing import cast
from uuid import UUID

import pytest

from devrelay.domain.jobs.attempt import (
    ExecutionAttempt,
    Lease,
    LeaseExpired,
    LeaseRevoked,
)
from devrelay.domain.jobs.job import Job
from devrelay.domain.jobs.values import (
    AttemptStatus,
    FailureCategory,
    InvalidJobEvidence,
    InvalidJobTransition,
    JobStatus,
    StaleAttempt,
)

NOW = datetime(2026, 10, 10, 10, tzinfo=UTC)
ATTEMPT_ID = UUID("11111111-1111-4111-8111-111111111111")
WORKER_ID = UUID("22222222-2222-4222-8222-222222222222")
JOB_ID = UUID("33333333-3333-4333-8333-333333333333")
OTHER_ID = UUID("44444444-4444-4444-8444-444444444444")
DEADLINE = NOW + timedelta(seconds=60)


@pytest.fixture
def lease() -> Lease:
    return Lease(
        attempt_id=ATTEMPT_ID,
        worker_id=WORKER_ID,
        generation=7,
        issued_at=NOW,
        updated_at=NOW,
        expires_at=DEADLINE,
    )


@pytest.fixture
def attempt(lease: Lease) -> ExecutionAttempt:
    return ExecutionAttempt(
        id=ATTEMPT_ID, job_id=JOB_ID, number=2, lease=lease, updated_at=NOW
    )


def running(attempt: ExecutionAttempt) -> ExecutionAttempt:
    return attempt.acknowledge(worker_id=WORKER_ID, generation=7, now=NOW)


def test_acknowledgement_preserves_assignment_and_input(
    attempt: ExecutionAttempt,
) -> None:
    timestamp = NOW + timedelta(seconds=1)

    result = attempt.acknowledge(worker_id=WORKER_ID, generation=7, now=timestamp)

    assert result.status is AttemptStatus.RUNNING
    assert result.acknowledged_at == result.updated_at == timestamp
    assert result.id == attempt.id
    assert result.job_id == attempt.job_id
    assert result.number == attempt.number
    assert result.lease == attempt.lease
    assert result.reserves_execution
    assert attempt.status is AttemptStatus.CLAIMED
    assert attempt.acknowledged_at is None


@pytest.mark.parametrize("worker_id,generation", [(OTHER_ID, 7), (WORKER_ID, 6)])
def test_foreign_worker_or_generation_cannot_acknowledge(
    attempt: ExecutionAttempt, worker_id: UUID, generation: int
) -> None:
    with pytest.raises(StaleAttempt):
        attempt.acknowledge(worker_id=worker_id, generation=generation, now=NOW)

    assert attempt.status is AttemptStatus.CLAIMED


@pytest.mark.parametrize("offset", [0, 1])
def test_acknowledgement_rejects_exact_or_past_deadline(
    attempt: ExecutionAttempt, offset: int
) -> None:
    timestamp = DEADLINE + timedelta(seconds=offset)

    with pytest.raises(LeaseExpired):
        attempt.acknowledge(worker_id=WORKER_ID, generation=7, now=timestamp)


def test_duplicate_acknowledgement_requires_application_replay(
    attempt: ExecutionAttempt,
) -> None:
    acknowledged = running(attempt)

    with pytest.raises(InvalidJobTransition):
        acknowledged.acknowledge(worker_id=WORKER_ID, generation=7, now=NOW)


@pytest.mark.parametrize("acknowledged", [False, True])
def test_renewal_extends_lease_without_reassigning_or_restarting(
    attempt: ExecutionAttempt, acknowledged: bool
) -> None:
    original = running(attempt) if acknowledged else attempt
    timestamp = NOW + timedelta(seconds=30)
    deadline = timestamp + timedelta(seconds=60)

    result = original.renew(
        worker_id=WORKER_ID, generation=7, expires_at=deadline, now=timestamp
    )

    assert result.lease.expires_at == deadline
    assert result.lease.updated_at == result.updated_at == timestamp
    assert result.lease.generation == original.lease.generation
    assert result.lease.worker_id == original.lease.worker_id
    assert result.id == original.id
    assert result.number == original.number
    assert result.status == original.status
    assert result.acknowledged_at == original.acknowledged_at
    assert result.reserves_execution
    assert original.lease.expires_at == DEADLINE


@pytest.mark.parametrize("worker_id,generation", [(OTHER_ID, 7), (WORKER_ID, 8)])
def test_renewal_rejects_foreign_authority(
    attempt: ExecutionAttempt, worker_id: UUID, generation: int
) -> None:
    with pytest.raises(StaleAttempt):
        attempt.renew(
            worker_id=worker_id,
            generation=generation,
            expires_at=DEADLINE + timedelta(seconds=60),
            now=NOW,
        )


@pytest.mark.parametrize("offset", [-1, 0])
def test_renewal_cannot_shorten_or_replay_deadline(
    attempt: ExecutionAttempt, offset: int
) -> None:
    deadline = DEADLINE + timedelta(seconds=offset)

    with pytest.raises(InvalidJobEvidence):
        attempt.renew(worker_id=WORKER_ID, generation=7, expires_at=deadline, now=NOW)


@pytest.mark.parametrize("offset", [0, 1])
def test_late_renewal_cannot_resurrect_lease(
    attempt: ExecutionAttempt, offset: int
) -> None:
    timestamp = DEADLINE + timedelta(seconds=offset)

    with pytest.raises(LeaseExpired):
        attempt.renew(
            worker_id=WORKER_ID,
            generation=7,
            expires_at=timestamp + timedelta(seconds=60),
            now=timestamp,
        )


def test_revocation_retains_reservation(attempt: ExecutionAttempt) -> None:
    timestamp = NOW + timedelta(seconds=1)

    result = attempt.revoke(now=timestamp)

    assert result.lease.revoked_at == timestamp
    assert result.lease.expires_at == DEADLINE
    assert result.status is AttemptStatus.CLAIMED
    assert result.ended_at is None
    assert result.reserves_execution
    assert attempt.lease.revoked_at is None


@pytest.mark.parametrize("operation", ["acknowledge", "renew"])
def test_revoked_authority_rejects_worker_messages(
    attempt: ExecutionAttempt, operation: str
) -> None:
    revoked = attempt.revoke(now=NOW)

    with pytest.raises(LeaseRevoked):
        if operation == "acknowledge":
            revoked.acknowledge(worker_id=WORKER_ID, generation=7, now=NOW)
        else:
            revoked.renew(
                worker_id=WORKER_ID,
                generation=7,
                expires_at=DEADLINE + timedelta(seconds=60),
                now=NOW,
            )


def test_duplicate_revocation_does_not_change_authority(
    attempt: ExecutionAttempt,
) -> None:
    revoked = attempt.revoke(now=NOW)

    with pytest.raises(LeaseRevoked):
        revoked.revoke(now=NOW + timedelta(seconds=1))


@pytest.mark.parametrize("acknowledged", [False, True])
def test_expiry_retains_reservation(
    attempt: ExecutionAttempt, acknowledged: bool
) -> None:
    original = running(attempt) if acknowledged else attempt

    expired = original.expire(now=DEADLINE)

    assert expired.status is AttemptStatus.LEASE_EXPIRED
    assert expired.ended_at is None
    assert expired.reserves_execution
    assert expired.lease == original.lease
    assert expired.acknowledged_at == original.acknowledged_at


def test_early_expiry_is_rejected(attempt: ExecutionAttempt) -> None:
    with pytest.raises(InvalidJobTransition):
        attempt.expire(now=DEADLINE - timedelta(microseconds=1))


def test_expired_attempt_cannot_be_renewed(attempt: ExecutionAttempt) -> None:
    expired = attempt.expire(now=DEADLINE)

    with pytest.raises(LeaseExpired):
        expired.renew(
            worker_id=WORKER_ID,
            generation=7,
            expires_at=DEADLINE + timedelta(seconds=60),
            now=DEADLINE,
        )


def test_duplicate_expiry_requires_application_replay(
    attempt: ExecutionAttempt,
) -> None:
    expired = attempt.expire(now=DEADLINE)

    with pytest.raises(InvalidJobTransition):
        expired.expire(now=DEADLINE)


@pytest.mark.parametrize("outcome", [AttemptStatus.FAILED, AttemptStatus.CANCELLED])
@pytest.mark.parametrize("expired", [False, True])
def test_confirmed_stop_releases_only_matching_contained_attempt(
    attempt: ExecutionAttempt, outcome: AttemptStatus, expired: bool
) -> None:
    reserved = attempt.expire(now=DEADLINE) if expired else attempt.revoke(now=NOW)
    timestamp = DEADLINE + timedelta(seconds=1)

    result = reserved.confirm_stopped(
        stopped_attempt_id=ATTEMPT_ID, outcome=outcome, now=timestamp
    )

    assert result.status is outcome
    assert result.ended_at == timestamp
    assert result.lease.revoked_at is not None
    assert not result.reserves_execution
    assert reserved.reserves_execution


def test_stop_confirmation_rejects_foreign_attempt(attempt: ExecutionAttempt) -> None:
    reserved = attempt.revoke(now=NOW)

    with pytest.raises(StaleAttempt):
        reserved.confirm_stopped(
            stopped_attempt_id=OTHER_ID, outcome=AttemptStatus.CANCELLED, now=NOW
        )


def test_stop_confirmation_requires_withdrawn_authority(
    attempt: ExecutionAttempt,
) -> None:
    with pytest.raises(InvalidJobTransition):
        attempt.confirm_stopped(
            stopped_attempt_id=ATTEMPT_ID, outcome=AttemptStatus.CANCELLED, now=NOW
        )


@pytest.mark.parametrize(
    "outcome",
    [AttemptStatus.SUCCEEDED, AttemptStatus.RUNNING, cast(AttemptStatus, "failed")],
)
def test_stop_confirmation_cannot_report_publication_success_or_raw_status(
    attempt: ExecutionAttempt, outcome: AttemptStatus
) -> None:
    reserved = attempt.revoke(now=NOW)

    with pytest.raises(InvalidJobTransition):
        reserved.confirm_stopped(
            stopped_attempt_id=ATTEMPT_ID, outcome=outcome, now=NOW
        )


@pytest.mark.parametrize("operation", ["renew", "revoke", "expire", "confirm_stopped"])
def test_completed_attempt_rejects_further_commands(
    attempt: ExecutionAttempt, operation: str
) -> None:
    ended = attempt.revoke(now=NOW).confirm_stopped(
        stopped_attempt_id=ATTEMPT_ID, outcome=AttemptStatus.CANCELLED, now=NOW
    )

    with pytest.raises(InvalidJobTransition):
        if operation == "renew":
            ended.renew(
                worker_id=WORKER_ID,
                generation=7,
                expires_at=DEADLINE + timedelta(seconds=60),
                now=NOW,
            )
        elif operation == "revoke":
            ended.revoke(now=NOW)
        elif operation == "expire":
            ended.expire(now=DEADLINE)
        else:
            ended.confirm_stopped(
                stopped_attempt_id=ATTEMPT_ID, outcome=AttemptStatus.CANCELLED, now=NOW
            )


@pytest.mark.parametrize("value", [0, -1, True, cast(int, "1")])
def test_invalid_generation_is_rejected(lease: Lease, value: int) -> None:
    with pytest.raises(InvalidJobEvidence):
        replace(lease, generation=value)


@pytest.mark.parametrize("value", [0, -1, True, cast(int, "1")])
def test_invalid_attempt_number_is_rejected(
    attempt: ExecutionAttempt, value: int
) -> None:
    with pytest.raises(InvalidJobEvidence):
        replace(attempt, number=value)


@pytest.mark.parametrize("field", ["attempt_id", "worker_id"])
def test_invalid_lease_identity_is_rejected(lease: Lease, field: str) -> None:
    invalid = cast(UUID, "invalid")

    with pytest.raises(InvalidJobEvidence):
        if field == "attempt_id":
            replace(lease, attempt_id=invalid)
        else:
            replace(lease, worker_id=invalid)


def test_mismatched_lease_attempt_is_rejected(attempt: ExecutionAttempt) -> None:
    foreign_lease = replace(attempt.lease, attempt_id=OTHER_ID)

    with pytest.raises(InvalidJobEvidence):
        replace(attempt, lease=foreign_lease)


@pytest.mark.parametrize(
    "field", ["issued_at", "updated_at", "expires_at", "revoked_at"]
)
def test_naive_lease_timestamps_are_rejected(lease: Lease, field: str) -> None:
    naive = NOW.replace(tzinfo=None)

    with pytest.raises(InvalidJobEvidence):
        if field == "issued_at":
            replace(lease, issued_at=naive)
        elif field == "updated_at":
            replace(lease, updated_at=naive)
        elif field == "expires_at":
            replace(lease, expires_at=naive)
        else:
            replace(lease, revoked_at=naive)


@pytest.mark.parametrize("field", ["updated_at", "acknowledged_at", "ended_at"])
def test_naive_attempt_timestamps_are_rejected(
    attempt: ExecutionAttempt, field: str
) -> None:
    naive = NOW.replace(tzinfo=None)

    with pytest.raises(InvalidJobEvidence):
        if field == "updated_at":
            replace(attempt, updated_at=naive)
        elif field == "acknowledged_at":
            replace(attempt, acknowledged_at=naive)
        else:
            replace(attempt, ended_at=naive)


def test_timestamps_normalize_to_utc(lease: Lease) -> None:
    zone = timezone(timedelta(hours=3))
    localized = replace(
        lease,
        issued_at=NOW.astimezone(zone),
        updated_at=NOW.astimezone(zone),
        expires_at=DEADLINE.astimezone(zone),
    )

    result = ExecutionAttempt(
        id=ATTEMPT_ID,
        job_id=JOB_ID,
        number=1,
        lease=localized,
        updated_at=NOW.astimezone(zone),
    )

    assert result.updated_at.tzinfo is UTC
    assert result.lease.issued_at.tzinfo is UTC
    assert result.lease.expires_at == DEADLINE


def test_backward_clock_is_rejected(attempt: ExecutionAttempt) -> None:
    acknowledged = attempt.acknowledge(
        worker_id=WORKER_ID, generation=7, now=NOW + timedelta(seconds=2)
    )

    with pytest.raises(InvalidJobEvidence):
        acknowledged.renew(
            worker_id=WORKER_ID,
            generation=7,
            expires_at=DEADLINE + timedelta(seconds=60),
            now=NOW + timedelta(seconds=1),
        )


@pytest.mark.parametrize("field", ["generation", "expires_at"])
def test_lease_is_immutable(lease: Lease, field: str) -> None:
    with pytest.raises(FrozenInstanceError):
        setattr(lease, field, 0)


@pytest.mark.parametrize("field", ["status", "job_id"])
def test_attempt_is_immutable(attempt: ExecutionAttempt, field: str) -> None:
    with pytest.raises(FrozenInstanceError):
        setattr(attempt, field, None)


@pytest.mark.parametrize(
    "case",
    ["zero_duration", "backward_update", "late_live_update", "revocation_not_final"],
)
def test_inconsistent_lease_timestamps_are_rejected(lease: Lease, case: str) -> None:
    with pytest.raises(InvalidJobEvidence):
        if case == "zero_duration":
            replace(lease, expires_at=NOW)
        elif case == "backward_update":
            replace(lease, updated_at=NOW - timedelta(seconds=1))
        elif case == "late_live_update":
            replace(lease, updated_at=DEADLINE)
        else:
            replace(lease, revoked_at=NOW, updated_at=NOW + timedelta(seconds=1))


def test_control_can_revoke_after_expiry_without_implying_containment(
    lease: Lease,
) -> None:
    timestamp = DEADLINE + timedelta(seconds=1)

    result = lease.revoke(now=timestamp)

    assert result.revoked_at == timestamp
    assert result.expires_at == DEADLINE


@pytest.mark.parametrize(
    "status",
    [AttemptStatus.RUNNING, AttemptStatus.TESTING, AttemptStatus.PUBLISHING_PR],
)
def test_restored_execution_requires_acknowledgement(
    attempt: ExecutionAttempt, status: AttemptStatus
) -> None:
    with pytest.raises(InvalidJobEvidence):
        replace(attempt, status=status)


@pytest.mark.parametrize(
    "case",
    ["before_claim", "after_update", "at_expiry", "after_revocation", "still_claimed"],
)
def test_invalid_restored_acknowledgements_are_rejected(
    attempt: ExecutionAttempt, case: str
) -> None:
    revoked_lease = attempt.lease.revoke(now=NOW)

    with pytest.raises(InvalidJobEvidence):
        if case == "before_claim":
            replace(
                attempt,
                status=AttemptStatus.RUNNING,
                acknowledged_at=NOW - timedelta(seconds=1),
            )
        elif case == "after_update":
            replace(
                attempt,
                status=AttemptStatus.RUNNING,
                acknowledged_at=NOW + timedelta(seconds=1),
            )
        elif case == "at_expiry":
            replace(
                attempt,
                status=AttemptStatus.RUNNING,
                acknowledged_at=DEADLINE,
                updated_at=DEADLINE,
            )
        elif case == "after_revocation":
            replace(
                attempt,
                lease=revoked_lease,
                status=AttemptStatus.RUNNING,
                acknowledged_at=NOW + timedelta(seconds=1),
                updated_at=NOW + timedelta(seconds=1),
            )
        else:
            replace(attempt, acknowledged_at=NOW)


def test_raw_attempt_status_is_rejected(attempt: ExecutionAttempt) -> None:
    with pytest.raises(InvalidJobEvidence):
        replace(attempt, status=cast(AttemptStatus, "claimed"))


@pytest.mark.parametrize(
    "status", [AttemptStatus.SUCCEEDED, AttemptStatus.FAILED, AttemptStatus.CANCELLED]
)
def test_restored_terminal_attempt_requires_recorded_end(
    attempt: ExecutionAttempt, status: AttemptStatus
) -> None:
    acknowledged = running(attempt)

    with pytest.raises(InvalidJobEvidence):
        replace(acknowledged, status=status)


def test_restored_terminal_attempt_requires_revoked_authority(
    attempt: ExecutionAttempt,
) -> None:
    with pytest.raises(InvalidJobEvidence):
        replace(attempt, status=AttemptStatus.FAILED, ended_at=NOW)


def test_unfinished_attempt_cannot_have_end_timestamp(
    attempt: ExecutionAttempt,
) -> None:
    with pytest.raises(InvalidJobEvidence):
        replace(attempt, ended_at=NOW)


def test_restored_expiry_cannot_precede_deadline(attempt: ExecutionAttempt) -> None:
    with pytest.raises(InvalidJobEvidence):
        replace(attempt, status=AttemptStatus.LEASE_EXPIRED)


def test_attempt_update_cannot_precede_lease_update(attempt: ExecutionAttempt) -> None:
    renewed_lease = attempt.lease.renew(
        worker_id=WORKER_ID,
        generation=7,
        now=NOW + timedelta(seconds=1),
        expires_at=DEADLINE + timedelta(seconds=60),
    )

    with pytest.raises(InvalidJobEvidence):
        replace(attempt, lease=renewed_lease)


def test_non_uuid_attempt_identity_is_rejected(attempt: ExecutionAttempt) -> None:
    with pytest.raises(InvalidJobEvidence):
        replace(attempt, id=cast(UUID, "invalid"))


def test_job_and_attempt_retain_reservations_during_lease_recovery(
    attempt: ExecutionAttempt,
) -> None:
    job = Job(
        id=JOB_ID,
        source_task_id=OTHER_ID,
        snapshot_id=OTHER_ID,
        repository_id=OTHER_ID,
        authorized_branch="devrelay/job-1",
        target_branch="master",
        base_revision="a" * 40,
        policy_version="v1",
        required_check_ids=("tests",),
        max_repair_cycles=1,
        created_at=NOW,
        updated_at=NOW,
    ).claim(ATTEMPT_ID, now=NOW)
    assigned = replace(attempt, number=job.last_attempt_number)
    expired = assigned.expire(now=DEADLINE)

    result = job.require_recovery(
        FailureCategory.LEASE_LOST, "Worker authority expired.", now=DEADLINE
    )

    assert result.status is JobStatus.RECOVERY_REQUIRED
    assert result.active_attempt_id == expired.id
    assert result.last_attempt_number == expired.number
    assert result.reserves_execution
    assert expired.reserves_execution
