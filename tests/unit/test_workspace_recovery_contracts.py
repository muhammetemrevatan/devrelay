from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest

from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspaceDirectoryIdentity,
    WorkspaceLocation,
    WorkspacePreparationDecision,
    WorkspaceRecovery,
)
from tests.workspace_fixtures import workspace_binding


def intent() -> WorkerWorkspaceIntent:
    return WorkerWorkspaceIntent(
        workspace_request_id=uuid4(),
        execution=workspace_binding().execution.intent,
        location=WorkspaceLocation(
            root_directory="C:\\workspaces",
            policy_digest="a" * 64,
            identity=WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=1),
        ),
    )


@pytest.mark.parametrize(
    "path",
    [
        "C:\\",
        "C:relative",
        "C:\\root\\..\\other",
        "C:\\root\\",
        "C:\\root.",
        "C:\\NUL",
        "C:\\root:stream",
        "\\\\server\\share",
        "C:\\root~1",
        "C:\\root\x00",
    ],
)
def test_location_rejects_uncanonical_persisted_root(path: str) -> None:
    original = intent().location

    with pytest.raises(ValueError, match="canonical local path"):
        replace(original, root_directory=path)


@pytest.mark.parametrize("digest", ["", "A" * 64, "x" * 64, "a" * 63])
def test_location_requires_canonical_policy_digest(digest: str) -> None:
    original = intent().location

    with pytest.raises(ValueError, match="bounded observed pins"):
        replace(original, policy_digest=digest)


def test_replay_ignores_only_proposed_workspace_request_nonce() -> None:
    original = intent()
    repeated = replace(original, workspace_request_id=uuid4())

    original.require_same_input(repeated)

    assert original.workspace_request_id != repeated.workspace_request_id
    assert original.location is repeated.location


@pytest.mark.parametrize(
    "field,value",
    [
        ("root_directory", "C:\\other"),
        ("policy_digest", "b" * 64),
        ("identity", WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=2)),
    ],
)
def test_replay_rejects_repinning_root_or_policy(field: str, value: Any) -> None:
    original = intent()
    changed = replace(original, location=replace(original.location, **{field: value}))

    with pytest.raises(ClaimReplayConflict):
        original.require_same_input(changed)


@pytest.mark.parametrize("created", [0, 1, None, "true"])
def test_commit_decision_requires_exact_boolean(created: Any) -> None:
    original = intent()

    with pytest.raises(ValueError, match="typed commit decision"):
        WorkspacePreparationDecision(intent=original, created=created)


@pytest.mark.parametrize(
    "identity",
    [
        WorkspaceDirectoryIdentity(volume_serial_number=1, file_index=1),
        WorkspaceDirectoryIdentity(volume_serial_number=2, file_index=2),
    ],
)
def test_completion_requires_distinct_same_volume_child(
    identity: WorkspaceDirectoryIdentity,
) -> None:
    original = intent()

    with pytest.raises(ValueError, match="distinct same-volume child"):
        WorkerWorkspaceCompletion(intent=original, identity=identity)


def test_recovery_retains_uint64_child_as_historical_observation() -> None:
    original = intent()
    completion = WorkerWorkspaceCompletion(
        intent=original,
        identity=WorkspaceDirectoryIdentity(
            volume_serial_number=1, file_index=2**64 - 1
        ),
    )

    recovery = WorkspaceRecovery(intent=original, completion=completion)

    assert recovery.completion is completion
    assert recovery.intent is original
    assert not recovery.history_unknown
    assert original.location.root_directory not in repr(recovery)


def test_unknown_legacy_history_has_no_restored_preparation_or_completion() -> None:
    recovery = WorkspaceRecovery(intent=None, completion=None, history_unknown=True)

    assert recovery.history_unknown
    assert recovery.intent is None and recovery.completion is None


def test_unknown_history_cannot_also_claim_prepared_workspace() -> None:
    original = intent()

    with pytest.raises(ValueError, match="historical pins"):
        WorkspaceRecovery(intent=original, completion=None, history_unknown=True)
