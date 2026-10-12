from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.infrastructure.persistence.claim_models import ExecutionAttemptRecord
from devrelay.infrastructure.persistence.heartbeat_models import (
    AttemptLeaseAuthorityRecord,
    HeartbeatReceiptRecord,
)
from devrelay.infrastructure.persistence.job_mapping import _validate_lease_authority

NOW = datetime(2026, 10, 12, tzinfo=UTC)
REPOSITORY = UUID(int=5)


def attempt() -> ExecutionAttemptRecord:
    return ExecutionAttemptRecord(
        id=UUID(int=1),
        job_id=UUID(int=2),
        worker_id=UUID(int=3),
        claim_request_id=UUID(int=4),
        attempt_number=1,
        generation=1,
        status="running",
        issued_at=NOW,
        updated_at=NOW + timedelta(seconds=5),
        expires_at=NOW + timedelta(seconds=120),
        acknowledged_at=NOW + timedelta(seconds=5),
        revoked_at=None,
        ended_at=None,
    )


def authority(*, initial: bool = False) -> AttemptLeaseAuthorityRecord:
    return AttemptLeaseAuthorityRecord(
        attempt_id=UUID(int=1),
        job_id=UUID(int=2),
        worker_id=UUID(int=3),
        claim_request_id=UUID(int=4),
        generation=1,
        lease_revision=1 if initial else 2,
        updated_at=NOW if initial else NOW + timedelta(seconds=30),
        current_expires_at=NOW + timedelta(seconds=120 if initial else 150),
        revoked_at=None,
        receipt_count=0 if initial else 1,
        next_heartbeat_at=NOW if initial else NOW + timedelta(seconds=60),
    )


def receipt() -> HeartbeatReceiptRecord:
    return HeartbeatReceiptRecord(
        worker_id=UUID(int=3),
        request_id=UUID(int=6),
        attempt_id=UUID(int=1),
        job_id=UUID(int=2),
        repository_id=REPOSITORY,
        claim_request_id=UUID(int=4),
        generation=1,
        protocol_version=1,
        fingerprint_version=1,
        fingerprint="a" * 64,
        expected_lease_revision=1,
        lease_revision=2,
        updated_at=NOW + timedelta(seconds=30),
        current_expires_at=NOW + timedelta(seconds=150),
        renewed=True,
        recorded_at=NOW + timedelta(seconds=30),
        attempt_ordinal=1,
        worker_ordinal=1,
        global_ordinal=1,
    )


def test_renewal_projection_preserves_original_attempt_and_acknowledgement() -> None:
    original, current, latest = attempt(), authority(), receipt()
    expiry, acknowledgement = original.expires_at, original.acknowledged_at

    _validate_lease_authority(
        original, current, latest, repository_id=REPOSITORY, claimed=False
    )

    assert original.expires_at == expiry
    assert original.updated_at == acknowledgement
    assert current.current_expires_at > expiry


@pytest.mark.parametrize("claimed", [False, True])
def test_initial_projection_requires_no_heartbeat_history(claimed: bool) -> None:
    original, current = attempt(), authority(initial=True)

    _validate_lease_authority(
        original, current, None, repository_id=REPOSITORY, claimed=claimed
    )

    assert current.lease_revision == 1
    assert current.receipt_count == 0


@pytest.mark.parametrize("missing", ["authority", "receipt"])
def test_active_projection_fails_closed_with_missing_lease_history(
    missing: str,
) -> None:
    original = attempt()
    current = None if missing == "authority" else authority()
    latest = None if missing == "receipt" else receipt()

    with pytest.raises(JobPersistenceError):
        _validate_lease_authority(
            original, current, latest, repository_id=REPOSITORY, claimed=False
        )


@pytest.mark.parametrize(
    "field", ["attempt_id", "job_id", "worker_id", "claim_request_id"]
)
def test_projection_rejects_another_assignment_authority(field: str) -> None:
    original, current, latest = attempt(), authority(), receipt()
    setattr(current, field, UUID(int=99))

    with pytest.raises(JobPersistenceError):
        _validate_lease_authority(
            original, current, latest, repository_id=REPOSITORY, claimed=False
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("lease_revision", True),
        ("lease_revision", 0),
        ("lease_revision", 3),
        ("receipt_count", -1),
        ("receipt_count", True),
        ("revoked_at", NOW),
        ("current_expires_at", NOW + timedelta(seconds=151)),
        ("updated_at", NOW + timedelta(seconds=31)),
        ("next_heartbeat_at", NOW + timedelta(seconds=29)),
        ("next_heartbeat_at", NOW + timedelta(seconds=30)),
    ],
)
def test_projection_rejects_lease_history_corruption(field: str, value: Any) -> None:
    original, current, latest = attempt(), authority(), receipt()
    setattr(current, field, value)

    with pytest.raises(JobPersistenceError):
        _validate_lease_authority(
            original, current, latest, repository_id=REPOSITORY, claimed=False
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("repository_id", UUID(int=99)),
        ("claim_request_id", UUID(int=99)),
        ("attempt_ordinal", 2),
        ("global_ordinal", 0),
        ("worker_ordinal", True),
        ("generation", True),
        ("lease_revision", True),
        ("lease_revision", 2.0),
        ("expected_lease_revision", True),
        ("attempt_ordinal", True),
        ("global_ordinal", True),
        ("protocol_version", 2),
        ("fingerprint", "A" * 64),
        ("expected_lease_revision", 2),
        ("renewed", False),
        ("recorded_at", NOW + timedelta(seconds=31)),
    ],
)
def test_projection_rejects_forged_latest_receipt(field: str, value: Any) -> None:
    original, current, latest = attempt(), authority(), receipt()
    setattr(latest, field, value)

    with pytest.raises(JobPersistenceError):
        _validate_lease_authority(
            original, current, latest, repository_id=REPOSITORY, claimed=False
        )


def test_unrenewed_heartbeat_preserves_original_revision_and_expiry() -> None:
    original, current, latest = attempt(), authority(initial=True), receipt()
    current.receipt_count = 1
    current.next_heartbeat_at = NOW + timedelta(seconds=60)
    latest.lease_revision = 1
    latest.updated_at = NOW
    latest.current_expires_at = original.expires_at
    latest.renewed = False

    _validate_lease_authority(
        original, current, latest, repository_id=REPOSITORY, claimed=False
    )

    assert current.lease_revision == 1
    assert original.expires_at == current.current_expires_at


def test_claimed_projection_cannot_contain_renewed_authority() -> None:
    original, current, latest = attempt(), authority(), receipt()

    with pytest.raises(JobPersistenceError):
        _validate_lease_authority(
            original, current, latest, repository_id=REPOSITORY, claimed=True
        )


@pytest.mark.parametrize("acknowledged", [None, NOW + timedelta(seconds=31)])
def test_projection_rejects_heartbeat_without_prior_acknowledgement(
    acknowledged: datetime | None,
) -> None:
    original, current, latest = attempt(), authority(), receipt()
    original.acknowledged_at = acknowledged
    original.updated_at = acknowledged or NOW

    with pytest.raises(JobPersistenceError):
        _validate_lease_authority(
            original, current, latest, repository_id=REPOSITORY, claimed=False
        )


def test_projection_rejects_bool_revision_even_for_unrenewed_original_authority() -> (
    None
):
    original, current, latest = attempt(), authority(initial=True), receipt()
    current.receipt_count = 1
    current.next_heartbeat_at = NOW + timedelta(seconds=60)
    latest.lease_revision = True
    latest.updated_at = NOW
    latest.current_expires_at = original.expires_at
    latest.renewed = False

    with pytest.raises(JobPersistenceError):
        _validate_lease_authority(
            original, current, latest, repository_id=REPOSITORY, claimed=False
        )
