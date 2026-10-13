from dataclasses import FrozenInstanceError, replace
from typing import Any
from uuid import UUID

import pytest

from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.worker_recovery import WorkerRecoveryPhase
from tests.acknowledgement_fixtures import changed_intent
from tests.unit.test_worker_execution_preparation import context, policy


def intent() -> WorkerExecutionIntent:
    acknowledgement = context().journal.state.acknowledgement
    assert acknowledgement is not None
    return WorkerExecutionIntent(
        execution_id=UUID(int=99), acknowledgement=acknowledgement, policy=policy()
    )


def test_exact_preparation_preserves_first_execution_identity() -> None:
    original = intent()
    candidate = replace(original, execution_id=UUID(int=100))

    original.require_same_input(candidate)

    assert original.execution_id == UUID(int=99)


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
def test_execution_replay_cannot_repin_original_acknowledgement(field: str) -> None:
    original = intent()
    candidate = replace(
        original, acknowledgement=changed_intent(original.acknowledgement, field)
    )

    with pytest.raises(ClaimReplayConflict):
        original.require_same_input(candidate)


@pytest.mark.parametrize(
    "digest", ["a" * 63, "a" * 65, "A" * 64, "z" * 64, " a" * 32, None, 1, True]
)
def test_policy_digest_requires_canonical_lowercase_sha256(digest: Any) -> None:
    with pytest.raises(ValueError):
        ExecutionPolicy(
            runtime=ExecutionRuntime.CODEX,
            isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
            policy_digest=digest,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("runtime", "codex"),
        ("runtime", None),
        ("isolation", "windows_restricted"),
        ("isolation", True),
    ],
)
def test_policy_does_not_coerce_runtime_or_isolation(field: str, value: Any) -> None:
    values: dict[str, Any] = {
        "runtime": ExecutionRuntime.CODEX,
        "isolation": ExecutionIsolation.WINDOWS_RESTRICTED,
        "policy_digest": "a" * 64,
    }
    values[field] = value

    with pytest.raises(ValueError):
        ExecutionPolicy(**values)


@pytest.mark.parametrize("field", ["execution_id", "acknowledgement", "policy"])
def test_execution_intent_requires_typed_identity_and_pins(field: str) -> None:
    original = intent()
    changes: dict[str, Any] = {field: "untrusted-value"}

    with pytest.raises(ValueError):
        replace(original, **changes)


def test_execution_intent_and_policy_are_immutable() -> None:
    original = intent()

    with pytest.raises(FrozenInstanceError):
        original.execution_id = UUID(int=100)  # type: ignore[misc]  # Verify frozen pins.


def test_recovery_state_accepts_execution_only_with_its_original_completed_ack() -> (
    None
):
    state = context().journal.state
    assert state.acknowledgement is not None
    recorded = WorkerExecutionIntent(
        execution_id=UUID(int=99),
        acknowledgement=state.acknowledgement,
        policy=policy(),
    )

    recovered = replace(state, execution=recorded)

    assert recovered.execution is recorded
    assert recovered.phase is WorkerRecoveryPhase.ACKNOWLEDGED


def test_recovery_state_rejects_execution_before_completed_acknowledgement() -> None:
    state = context().journal.state
    assert state.acknowledgement is not None
    recorded = WorkerExecutionIntent(
        execution_id=UUID(int=99),
        acknowledgement=state.acknowledgement,
        policy=policy(),
    )

    with pytest.raises(ValueError):
        replace(
            state,
            execution=recorded,
            phase=WorkerRecoveryPhase.ACKNOWLEDGEMENT_PENDING,
            completion=None,
        )


def test_recovery_state_rejects_execution_with_different_original_acknowledgement() -> (
    None
):
    state = context().journal.state
    assert state.acknowledgement is not None
    recorded = WorkerExecutionIntent(
        execution_id=UUID(int=99),
        acknowledgement=replace(
            state.acknowledgement, acknowledgement_request_id=UUID(int=100)
        ),
        policy=policy(),
    )

    with pytest.raises(ValueError):
        replace(state, execution=recorded)


def test_recovery_state_rejects_malformed_execution_intent() -> None:
    state = context().journal.state
    malformed: Any = "untrusted-value"

    with pytest.raises(ValueError):
        replace(state, execution=malformed)
