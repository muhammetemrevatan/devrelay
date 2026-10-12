from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

from devrelay.application.contracts.heartbeats import LeaseAuthorityObservation
from devrelay.application.contracts.worker_recovery import AssignmentReference
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.lease_deadlines import LeaseDeadline, LeaseDeadlineRejected

NOW = datetime(2026, 10, 12, tzinfo=UTC)


def observation() -> LeaseAuthorityObservation:
    reference = AssignmentReference(
        worker_id=UUID(int=1),
        job_id=UUID(int=2),
        repository_id=UUID(int=3),
        attempt_id=UUID(int=4),
        generation=1,
        issued_at=NOW,
        expires_at=NOW + timedelta(seconds=120),
    )
    return LeaseAuthorityObservation(
        reference=reference,
        lease_revision=1,
        observed_at=NOW + timedelta(seconds=20),
        updated_at=NOW,
        current_expires_at=reference.expires_at,
    )


def authorization() -> WorkerAuthorization:
    return WorkerAuthorization(
        worker_id=UUID(int=1),
        allowed_repository_ids=frozenset({UUID(int=3)}),
        credential_expires_at=NOW + timedelta(seconds=300),
    )


def deadline() -> LeaseDeadline:
    return LeaseDeadline.from_observation(
        observation(),
        authorization(),
        request_started_at=100,
        received_at=102,
        stop_margin_seconds=5,
    )


def renewal() -> LeaseAuthorityObservation:
    return replace(
        observation(),
        lease_revision=2,
        observed_at=NOW + timedelta(seconds=30),
        updated_at=NOW + timedelta(seconds=30),
        current_expires_at=NOW + timedelta(seconds=150),
    )


def test_request_start_anchor_spends_transport_delay_and_stop_margin() -> None:
    authority, scope = observation(), authorization()

    result = LeaseDeadline.from_observation(
        authority,
        scope,
        request_started_at=100,
        received_at=110,
        stop_margin_seconds=5,
    )

    assert result.stop_at == 195
    assert result.remaining_seconds(110) == 85
    assert result.reference == authority.reference
    assert not result.must_stop(194.999)
    assert result.must_stop(195)
    assert result.must_stop(196)


def test_credential_expiry_caps_budget_using_server_observation() -> None:
    scope = replace(authorization(), credential_expires_at=NOW + timedelta(seconds=60))

    result = LeaseDeadline.from_observation(
        observation(),
        scope,
        request_started_at=100,
        received_at=102,
        stop_margin_seconds=5,
    )

    assert result.stop_at == 135
    assert result.remaining_seconds(102) == 33


@pytest.mark.parametrize("received_at", [195, 196, 400])
def test_exhausted_transport_budget_requires_stop(received_at: float) -> None:
    authority, scope = observation(), authorization()

    result = LeaseDeadline.from_observation(
        authority,
        scope,
        request_started_at=100,
        received_at=received_at,
        stop_margin_seconds=5,
    )

    assert result.stopped
    assert result.must_stop(received_at)
    assert result.remaining_seconds(received_at) == 0


@pytest.mark.parametrize("expiry", [NOW, NOW + timedelta(seconds=20)])
def test_expired_credential_never_grants_budget(expiry: datetime) -> None:
    scope = replace(authorization(), credential_expires_at=expiry)

    result = LeaseDeadline.from_observation(
        observation(),
        scope,
        request_started_at=100,
        received_at=102,
        stop_margin_seconds=5,
    )

    assert result.must_stop(102)


def test_expired_lease_never_grants_budget() -> None:
    authority = replace(observation(), observed_at=NOW + timedelta(seconds=120))

    result = LeaseDeadline.from_observation(
        authority,
        authorization(),
        request_started_at=100,
        received_at=102,
        stop_margin_seconds=5,
    )

    assert result.must_stop(102)


@pytest.mark.parametrize("revoked", [False, True])
def test_control_stop_or_revocation_overrides_remaining_budget(revoked: bool) -> None:
    authority = observation()
    if revoked:
        authority = replace(
            authority, lease_revision=2, revoked_at=authority.updated_at
        )

    result = LeaseDeadline.from_observation(
        authority,
        authorization(),
        request_started_at=100,
        received_at=102,
        stop_margin_seconds=5,
        stop_required=not revoked,
    )

    assert result.must_stop(102)


def test_fresh_higher_revision_can_extend_unexpired_lineage() -> None:
    previous, authority = deadline(), renewal()

    result = previous.observe(
        authority,
        authorization(),
        request_started_at=110,
        received_at=112,
    )

    assert result.stop_at == 225
    assert result.lease_revision == 2
    assert result.reference == previous.reference
    assert previous.stop_at == 195


def test_same_revision_reply_can_only_shorten_budget() -> None:
    previous = deadline()
    authority = replace(observation(), observed_at=NOW + timedelta(seconds=31))

    result = previous.observe(
        authority,
        authorization(),
        request_started_at=110,
        received_at=112,
    )

    assert result.stop_at == 194
    assert result.lease_revision == 1


def test_same_revision_reply_cannot_extend_budget() -> None:
    previous, authority = deadline(), observation()

    result = previous.observe(
        authority,
        authorization(),
        request_started_at=110,
        received_at=112,
    )

    assert result.stop_at == previous.stop_at


def test_stale_revision_cannot_extend_newer_budget_or_regress_watermarks() -> None:
    previous = deadline().observe(
        renewal(), authorization(), request_started_at=110, received_at=112
    )

    result = previous.observe(
        observation(),
        authorization(),
        request_started_at=150,
        received_at=152,
    )

    assert result.stop_at == previous.stop_at
    assert result.authority == previous.authority
    assert result.request_started_at == 150


def test_stale_revision_may_conservatively_shorten_budget() -> None:
    previous = deadline().observe(
        renewal(), authorization(), request_started_at=110, received_at=112
    )

    result = previous.observe(
        observation(),
        authorization(),
        request_started_at=111,
        received_at=113,
    )

    assert result.stop_at == 196
    assert result.lease_revision == 2


def test_response_from_earlier_request_never_extends_newer_request_deadline() -> None:
    previous = deadline()

    result = previous.observe(
        renewal(),
        authorization(),
        request_started_at=99,
        received_at=112,
    )

    assert result.stop_at == previous.stop_at
    assert result.lease_revision == 2
    assert result.request_started_at == previous.request_started_at


@pytest.mark.parametrize("received_at", [195, 196])
def test_renewal_arriving_at_prior_deadline_cannot_resurrect_lineage(
    received_at: float,
) -> None:
    previous = deadline()

    result = previous.observe(
        renewal(),
        authorization(),
        request_started_at=190,
        received_at=received_at,
    )

    assert result.stopped
    assert result.must_stop(received_at)


def test_control_stop_latches_across_later_renewal() -> None:
    previous = deadline().observe(
        observation(),
        authorization(),
        request_started_at=105,
        received_at=106,
        stop_required=True,
    )

    result = previous.observe(
        renewal(), authorization(), request_started_at=110, received_at=112
    )

    assert result.must_stop(112)


def test_new_reply_respects_reduced_current_credential_authority() -> None:
    previous = deadline()
    scope = replace(authorization(), credential_expires_at=NOW + timedelta(seconds=50))

    result = previous.observe(renewal(), scope, request_started_at=110, received_at=112)

    assert result.stop_at == 125


@pytest.mark.parametrize("field", ["updated_at", "current_expires_at", "revoked_at"])
def test_same_revision_authority_mutation_is_rejected(field: str) -> None:
    prior_authority = renewal()
    previous = LeaseDeadline.from_observation(
        prior_authority,
        authorization(),
        request_started_at=100,
        received_at=102,
        stop_margin_seconds=5,
    )
    changes: dict[str, Any] = {
        "updated_at": NOW + timedelta(seconds=29),
        "current_expires_at": NOW + timedelta(seconds=151),
        "revoked_at": prior_authority.updated_at,
    }
    authority = replace(prior_authority, **{field: changes[field]})

    with pytest.raises(LeaseDeadlineRejected, match="One lease revision"):
        previous.observe(
            authority, authorization(), request_started_at=110, received_at=112
        )


@pytest.mark.parametrize("field", ["observed_at", "updated_at", "current_expires_at"])
def test_new_revision_cannot_regress_accepted_authority(field: str) -> None:
    prior_authority = replace(renewal(), updated_at=NOW + timedelta(seconds=25))
    previous = LeaseDeadline.from_observation(
        prior_authority,
        authorization(),
        request_started_at=100,
        received_at=102,
        stop_margin_seconds=5,
    )
    changes: dict[str, Any] = {
        "observed_at": NOW + timedelta(seconds=29),
        "updated_at": NOW + timedelta(seconds=24),
        "current_expires_at": NOW + timedelta(seconds=149),
    }
    authority = replace(prior_authority, lease_revision=3, **{field: changes[field]})

    with pytest.raises(LeaseDeadlineRejected, match="cannot regress"):
        previous.observe(
            authority, authorization(), request_started_at=110, received_at=112
        )


@pytest.mark.parametrize(
    "field",
    ["worker_id", "job_id", "repository_id", "attempt_id", "issued_at", "expires_at"],
)
def test_update_cannot_repin_original_assignment(field: str) -> None:
    previous = deadline()
    change: UUID | datetime = (
        NOW + timedelta(seconds=1 if field == "issued_at" else 121)
        if field in {"issued_at", "expires_at"}
        else UUID(int=99)
    )
    changes: dict[str, Any] = {field: change}
    reference = replace(previous.reference, **changes)
    authority = replace(renewal(), reference=reference)

    with pytest.raises(LeaseDeadlineRejected):
        previous.observe(
            authority, authorization(), request_started_at=110, received_at=112
        )


@pytest.mark.parametrize("foreign_worker", [False, True])
def test_observation_requires_current_authenticated_scope(foreign_worker: bool) -> None:
    scope = authorization()
    scope = (
        replace(scope, worker_id=UUID(int=99))
        if foreign_worker
        else replace(scope, allowed_repository_ids=frozenset({UUID(int=99)}))
    )

    with pytest.raises(LeaseDeadlineRejected, match="matching current scope"):
        LeaseDeadline.from_observation(
            observation(),
            scope,
            request_started_at=100,
            received_at=102,
            stop_margin_seconds=5,
        )


@pytest.mark.parametrize(
    "value", [True, False, float("nan"), float("inf"), -1.0, 2**53, 10**1000]
)
@pytest.mark.parametrize("field", ["request_started_at", "received_at"])
def test_untrusted_monotonic_input_fails_closed(field: str, value: Any) -> None:
    times: dict[str, Any] = {"request_started_at": 100, "received_at": 102}
    times[field] = value

    with pytest.raises(LeaseDeadlineRejected):
        LeaseDeadline.from_observation(
            observation(), authorization(), stop_margin_seconds=5, **times
        )


@pytest.mark.parametrize("value", [True, 0, -1, 301, float("nan"), float("inf")])
def test_stop_margin_must_be_positive_finite_and_bounded(value: Any) -> None:
    authority, scope = observation(), authorization()

    with pytest.raises(LeaseDeadlineRejected):
        LeaseDeadline.from_observation(
            authority,
            scope,
            request_started_at=100,
            received_at=102,
            stop_margin_seconds=value,
        )


def test_margin_exhausting_budget_requires_immediate_stop() -> None:
    authority, scope = observation(), authorization()

    result = LeaseDeadline.from_observation(
        authority,
        scope,
        request_started_at=100,
        received_at=102,
        stop_margin_seconds=100,
    )

    assert result.must_stop(102)


def test_receipt_before_request_start_is_rejected() -> None:
    authority, scope = observation(), authorization()

    with pytest.raises(LeaseDeadlineRejected, match="precede request"):
        LeaseDeadline.from_observation(
            authority,
            scope,
            request_started_at=100,
            received_at=99,
            stop_margin_seconds=5,
        )


def test_regressing_receipt_is_rejected() -> None:
    previous = deadline()

    with pytest.raises(LeaseDeadlineRejected, match="move backwards"):
        previous.observe(
            renewal(), authorization(), request_started_at=100, received_at=101
        )


@pytest.mark.parametrize("now", [101, True, float("nan"), float("inf"), -1])
def test_invalid_or_regressing_local_reading_is_rejected(now: Any) -> None:
    previous = deadline()

    with pytest.raises(LeaseDeadlineRejected):
        previous.must_stop(now)


def test_nonboolean_stop_instruction_is_rejected() -> None:
    authority, scope = observation(), authorization()
    malformed_stop: Any = 1

    with pytest.raises(LeaseDeadlineRejected, match="boolean"):
        LeaseDeadline.from_observation(
            authority,
            scope,
            request_started_at=100,
            received_at=102,
            stop_margin_seconds=5,
            stop_required=malformed_stop,
        )


def test_deadline_is_immutable() -> None:
    result = deadline()

    with pytest.raises(FrozenInstanceError):
        result.stop_at = 1000  # type: ignore[misc]  # Verify frozen value behavior.


def test_direct_construction_cannot_bypass_conservative_lease_cap() -> None:
    authority = observation()

    with pytest.raises(LeaseDeadlineRejected, match="inconsistent"):
        LeaseDeadline(
            authority=authority,
            authority_request_started_at=100,
            request_started_at=100,
            received_at=102,
            stop_at=10000,
            stop_margin_seconds=5,
            stopped=False,
        )


def test_stale_reply_cannot_hide_credential_expiry_at_known_server_time() -> None:
    prior_authority = replace(renewal(), observed_at=NOW + timedelta(seconds=70))
    previous = LeaseDeadline.from_observation(
        prior_authority,
        authorization(),
        request_started_at=100,
        received_at=102,
        stop_margin_seconds=5,
    )
    scope = replace(authorization(), credential_expires_at=NOW + timedelta(seconds=50))

    result = previous.observe(
        observation(), scope, request_started_at=110, received_at=112
    )

    assert result.must_stop(112)


def test_direct_construction_cannot_claim_revoked_deadline_is_live() -> None:
    authority = replace(observation(), lease_revision=2, revoked_at=NOW)

    with pytest.raises(LeaseDeadlineRejected):
        LeaseDeadline(
            authority=authority,
            authority_request_started_at=100,
            request_started_at=100,
            received_at=102,
            stop_at=190,
            stop_margin_seconds=5,
            stopped=False,
        )


@pytest.mark.parametrize("prior_stale_replies", [False, True])
def test_stale_reply_cannot_reanchor_a_shorter_credential_clock_budget(
    prior_stale_replies: bool,
) -> None:
    latest = replace(
        renewal(),
        observed_at=NOW + timedelta(seconds=100),
        updated_at=NOW + timedelta(seconds=100),
        current_expires_at=NOW + timedelta(seconds=200),
    )
    previous = LeaseDeadline.from_observation(
        latest,
        authorization(),
        request_started_at=100,
        received_at=101,
        stop_margin_seconds=1,
    )
    stale = replace(observation(), observed_at=NOW + timedelta(seconds=90))
    if prior_stale_replies:
        previous = previous.observe(
            stale, authorization(), request_started_at=102, received_at=103
        )
    scope = replace(authorization(), credential_expires_at=NOW + timedelta(seconds=105))

    result = previous.observe(stale, scope, request_started_at=110, received_at=111)

    assert result.must_stop(111)
    assert result.authority_request_started_at == 100
