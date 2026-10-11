from dataclasses import replace
from typing import Any, cast
from uuid import uuid4

import pytest

from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.worker_recovery import (
    WorkerRecoveryPhase,
    WorkerRecoveryState,
)
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
    changed_intent,
)


@pytest.mark.parametrize(
    "field,value",
    [
        ("acknowledgement_request_id", "invalid"),
        ("claim_request_id", "invalid"),
        ("attempt_id", "invalid"),
        ("snapshot_id", "invalid"),
        ("generation", True),
        ("generation", 2),
        ("expected_workflow_version", True),
        ("expected_workflow_version", 0),
        ("protocol_version", True),
        ("protocol_version", 2),
        ("definition_digest_version", True),
        ("definition_digest_version", 2),
        ("definition_digest", "A" * 64),
        ("definition_digest", "a" * 63),
        ("definition_digest", 1),
        ("base_revision", "invalid"),
    ],
)
def test_intent_rejects_unsupported_or_malformed_pins(
    field: str, value: object
) -> None:
    request, delivery = acknowledgement_inputs()
    intent = acknowledgement_intent(delivery)

    with pytest.raises(ValueError):
        # Deliberately malformed runtime input bypasses the static contract.
        replace(intent, **cast(Any, {field: value}))


@pytest.mark.parametrize(
    "field",
    [
        "claim_request_id",
        "attempt_id",
        "snapshot_id",
        "base_revision",
        "definition_digest",
    ],
)
def test_original_intent_rejects_changed_content(field: str) -> None:
    request, delivery = acknowledgement_inputs()
    intent = acknowledgement_intent(delivery)
    candidate = changed_intent(intent, field)

    with pytest.raises(ClaimReplayConflict):
        intent.require_same_input(candidate)


def test_recovery_phase_requires_correlated_acknowledgement() -> None:
    request, delivery = acknowledgement_inputs()
    candidate = replace(acknowledgement_intent(delivery), attempt_id=uuid4())

    with pytest.raises(ValueError):
        WorkerRecoveryState(
            phase=WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
            request=request,
            assignment=delivery.outcome,
            acknowledgement=candidate,
        )


def test_assigned_phase_cannot_hide_pending_acknowledgement() -> None:
    request, delivery = acknowledgement_inputs()
    intent = acknowledgement_intent(delivery)

    with pytest.raises(ValueError):
        WorkerRecoveryState(
            phase=WorkerRecoveryPhase.ASSIGNED,
            request=request,
            assignment=delivery.outcome,
            acknowledgement=intent,
        )
