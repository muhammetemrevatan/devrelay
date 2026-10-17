import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar, cast

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseMaterializationPolicy,
)
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.worker_recovery import WorkerRecoveryState
from devrelay.application.contracts.workspace_recovery import WorkspaceLocation
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.materialize_base import _consume_completion, _wait_for_tick
from devrelay.application.ports.editable_workspace import EditableWorkspacePort
from devrelay.application.ports.worker_workspace_journal import WorkerWorkspaceJournal
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_executions import ExecutionAuthorityGuard
from devrelay.application.worker_workspaces import (
    _historical_workspace,
    _validate_original_input,
)

_Value = TypeVar("_Value")
_DEFAULT_POLICY = BaseMaterializationPolicy()


class MaterializedWorkspaceOperation:
    """Shared original-pin and live-authority guards for materialized operations."""

    def __init__(
        self,
        journal: WorkerWorkspaceJournal,
        supervisor: ExecutionAuthorityGuard,
        workspace: EditableWorkspacePort,
        policy: BaseMaterializationPolicy = _DEFAULT_POLICY,
    ) -> None:
        if not isinstance(policy, BaseMaterializationPolicy):
            raise ValueError("Editable handoff requires a typed timing policy.")
        policy.__post_init__()
        self._journal, self._supervisor, self._workspace = (
            journal,
            supervisor,
            workspace,
        )
        self._policy = policy
        self._seal_original = workspace.seal
        self._used = False
        self._operation: asyncio.Future[object] | None = None
        self._materialization: BaseMaterialization | None = None
        self._location: WorkspaceLocation | None = None
        try:
            self._expected = workspace.binding
        except Exception:
            raise WorkspaceUnavailable(
                "Live workspace binding is unavailable."
            ) from None
        if not isinstance(self._expected, WorkspaceBinding):
            raise WorkspaceUnavailable("Live workspace binding is unavailable.")

    def _require_authority(self) -> None:
        self._fence()
        self._supervisor.require_current_assignment(self._expected.execution.reference)
        self._fence()

    def _fence(self) -> None:
        materialization = self._materialization
        try:
            matches = (
                materialization is not None
                and self._workspace.binding == self._expected
                and self._workspace.materialization is materialization
                and self._workspace.reservation is materialization.reservation
                and self._workspace.location == self._location
                and materialization.reservation.binding == self._expected
            )
        except Exception:
            raise WorkspaceUnavailable("Live editable scope is unavailable.") from None
        if not matches:
            raise WorkspaceUnavailable("Editable handoff changed original live scope.")

    def _validate(
        self, state: WorkerRecoveryState, delivery: AssignmentDelivery
    ) -> None:
        _validate_original_input(state, delivery)
        history = _historical_workspace(state)
        materialization = self._materialization
        if (
            history is None
            or history.history_unknown
            or history.intent is None
            or history.completion is None
            or materialization is None
        ):
            raise WorkspaceUnavailable(
                "Completed live workspace preparation is required."
            )
        materialization.__post_init__()
        if (
            state.execution != self._expected.execution.intent
            or state.assignment is None
            or state.assignment.assignment != self._expected.execution.reference
            or delivery.definition != self._expected.definition
            or history.intent.location != self._location
            or history.completion.identity != materialization.reservation.identity
        ):
            raise ClaimReplayConflict(
                "Editable handoff changed original execution pins."
            )

    async def _await(
        self, operation: Callable[[], Awaitable[_Value]], total_deadline: float
    ) -> _Value:
        self._require_authority()
        loop = asyncio.get_running_loop()
        deadline = min(
            total_deadline, loop.time() + self._policy.operation_timeout_seconds
        )
        task = asyncio.ensure_future(operation())
        self._operation = cast(asyncio.Future[object], task)
        self._operation.add_done_callback(_consume_completion)
        while True:
            self._require_authority()
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise WorkspaceUnavailable("Editable handoff exceeded its wait budget.")
            if task.done():
                self._operation = None
                return task.result()
            await _wait_for_tick(
                cast(asyncio.Future[object], task),
                min(remaining, self._policy.poll_interval_seconds),
            )


class HandoffMaterializedBase(MaterializedWorkspaceOperation):
    """Make one verified live base editable without granting launch or replay.

    Rejected late releases leave uncertain mutable resources; sealing cannot
    restore immutability, certify process stop or release the coding reservation.
    """

    async def execute(
        self, delivery: AssignmentDelivery, materialization: BaseMaterialization
    ) -> EditableWorkspace:
        if self._used:
            raise WorkspaceUnavailable("Editable handoff has already been requested.")
        self._used = True
        try:
            if not isinstance(materialization, BaseMaterialization):
                raise WorkspaceUnavailable("A live materialization is required.")
            self._materialization = materialization
            self._location = self._workspace.location
            if not isinstance(self._location, WorkspaceLocation):
                raise WorkspaceUnavailable("A retained workspace root is required.")
            self._require_authority()
            total_deadline = (
                asyncio.get_running_loop().time() + self._policy.total_timeout_seconds
            )
            state = await self._await(self._journal.load_state, total_deadline)
            await self._await(
                lambda: asyncio.to_thread(self._validate, state, delivery),
                total_deadline,
            )
            result = await self._await(
                lambda: self._workspace.handoff(materialization), total_deadline
            )
            if (
                not isinstance(result, EditableWorkspace)
                or result.materialization is not materialization
                or result.location != self._location
            ):
                raise WorkspaceUnavailable(
                    "Editable handoff changed verified evidence."
                )
            self._require_authority()
            return result
        except BaseException:
            try:
                self._seal_original()
            except Exception:
                pass
            if self._operation is not None:
                self._operation.cancel()
            raise
