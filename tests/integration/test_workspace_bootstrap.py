import asyncio
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerRecoveryState,
)
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
    WorkspacePreparationDecision,
)
from devrelay.application.contracts.workspaces import WorkspaceReservation
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_workspaces import ReserveAssignedWorkspace
from devrelay.bootstrap.workspaces import open_worker_workspace
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.workspace_fixtures import workspace_inputs

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Windows native APIs")


def test_native_factory_consumes_pinned_reservation_and_preserves_directory(
    tmp_path: Path,
) -> None:
    state, delivery, binding = workspace_inputs()
    root = tmp_path / "workspaces"
    root.mkdir()
    protected = tmp_path / "private"
    protected.mkdir()
    policy = WindowsWorkspacePolicy(
        root_directory=str(root), protected_directories=(str(protected),)
    )
    guards: list[AssignmentReference] = []

    class Journal:
        def __init__(self) -> None:
            self.state = state

        async def load_state(self) -> WorkerRecoveryState:
            return self.state

        async def get_or_create_workspace(
            self, request: WorkerClaimRequest, candidate: WorkerWorkspaceIntent
        ) -> WorkspacePreparationDecision:
            assert self.state.workspace_intent is None
            self.state = replace(self.state, workspace_intent=candidate)
            return WorkspacePreparationDecision(intent=candidate, created=True)

        async def record_workspace_completion(
            self, request: WorkerClaimRequest, completion: WorkerWorkspaceCompletion
        ) -> WorkerWorkspaceCompletion:
            assert completion.intent == self.state.workspace_intent
            self.state = replace(self.state, workspace_completion=completion)
            return completion

    class Guard:
        def require_current_assignment(self, reference: AssignmentReference) -> None:
            assert reference == binding.execution.reference
            guards.append(reference)

    journal = Journal()

    async def exercise() -> WorkspaceReservation:
        async with open_worker_workspace(binding, policy) as scope:
            service = ReserveAssignedWorkspace(journal, Guard(), scope)
            result = await service.execute(delivery)
            assert journal.state.workspace_intent is not None
            with pytest.raises(WorkspaceUnavailable):
                await scope.reserve(journal.state.workspace_intent.location)
            return result

    result = asyncio.run(exercise())

    assert result.binding == binding
    assert len(guards) >= 4 and all(
        reference == binding.execution.reference for reference in guards
    )
    assert journal.state.workspace_completion is not None
    assert journal.state.workspace_completion.identity == result.identity
    directory = root / result.directory_name
    assert directory.is_dir()
    assert list(directory.iterdir()) == []
    assert list(protected.iterdir()) == []
