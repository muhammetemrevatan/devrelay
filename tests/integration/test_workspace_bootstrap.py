import asyncio
import sys
from pathlib import Path

import pytest

from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.worker_recovery import (
    AssignmentReference,
    WorkerRecoveryState,
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
        async def load_state(self) -> WorkerRecoveryState:
            return state

        async def get_or_create_execution(
            self, request: WorkerClaimRequest, candidate: WorkerExecutionIntent
        ) -> WorkerExecutionIntent:
            raise AssertionError("Workspace cannot alter execution intent.")

    class Guard:
        def require_current_assignment(self, reference: AssignmentReference) -> None:
            assert reference == binding.execution.reference
            guards.append(reference)

    async def exercise() -> WorkspaceReservation:
        async with open_worker_workspace(binding, policy) as scope:
            service = ReserveAssignedWorkspace(Journal(), Guard(), scope)
            result = await service.execute(delivery)
            with pytest.raises(WorkspaceUnavailable):
                await scope.reserve()
            return result

    result = asyncio.run(exercise())

    assert result.binding == binding
    assert guards == [binding.execution.reference] * 2
    directory = root / result.directory_name
    assert directory.is_dir()
    assert list(directory.iterdir()) == []
    assert list(protected.iterdir()) == []
