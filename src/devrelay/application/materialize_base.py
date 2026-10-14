import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar, cast

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseMaterializationPolicy,
    BaseSourceRequest,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.worker_recovery import WorkerRecoveryState
from devrelay.application.contracts.workspace_recovery import WorkspaceLocation
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceReservation,
)
from devrelay.application.ports.base_materialization import BaseMaterializationPort
from devrelay.application.ports.base_source import BaseSourcePort, InvalidBaseSourceData
from devrelay.application.ports.worker_workspace_journal import WorkerWorkspaceJournal
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_executions import ExecutionAuthorityGuard
from devrelay.application.worker_workspaces import (
    _historical_workspace,
    _validate_original_input,
)

_Value = TypeVar("_Value")
_DEFAULT_POLICY = BaseMaterializationPolicy()


async def _wait_for_tick(task: asyncio.Future[object], timeout: float) -> None:
    await asyncio.wait({task}, timeout=timeout)


def _consume_completion(task: asyncio.Future[object]) -> None:
    if not task.cancelled():
        task.exception()


def _validate_snapshot(
    snapshot: BaseSourceSnapshot, request: BaseSourceRequest
) -> tuple[str, int, int]:
    try:
        if not isinstance(snapshot, BaseSourceSnapshot) or snapshot.request != request:
            raise ValueError("Changed source input.")
        snapshot.validate()
        return (
            snapshot.fingerprint(),
            len(snapshot.files),
            sum(len(item.content) for item in snapshot.files),
        )
    except Exception:
        raise InvalidBaseSourceData("Base source manifest is unavailable.") from None


class MaterializeAssignedBase:
    """Export once into a live scope; historical directory evidence grants no use.

    The shared authority guard reads its own suspend-inclusive clock. Event-loop
    timing bounds waits only. Failure seals the captured original scope before
    cancelling the wrapper; that cancellation does not prove native I/O settled.
    No directory cleanup, journal mutation, replay or reservation release follows.
    """

    def __init__(
        self,
        journal: WorkerWorkspaceJournal,
        source: BaseSourcePort,
        workspace: BaseMaterializationPort,
        supervisor: ExecutionAuthorityGuard,
        policy: BaseMaterializationPolicy = _DEFAULT_POLICY,
    ) -> None:
        if not isinstance(policy, BaseMaterializationPolicy):
            raise ValueError("Base materialization requires a typed timing policy.")
        policy.__post_init__()
        self._journal, self._source, self._workspace = journal, source, workspace
        self._supervisor, self._policy = supervisor, policy
        self._seal_original = workspace.seal
        self._used = False
        self._sealed = False
        self._operation: asyncio.Future[object] | None = None
        self._reservation: WorkspaceReservation | None = None
        self._location: WorkspaceLocation | None = None
        try:
            self._expected = workspace.binding
        except Exception:
            raise WorkspaceUnavailable(
                "Live workspace binding is unavailable."
            ) from None
        if not isinstance(self._expected, WorkspaceBinding):
            raise WorkspaceUnavailable("Live workspace binding is unavailable.")

    def _fence(self) -> None:
        try:
            matches = (
                self._workspace.binding == self._expected
                and self._workspace.reservation is self._reservation
                and self._workspace.location == self._location
            )
        except Exception:
            raise WorkspaceUnavailable("Live workspace scope is unavailable.") from None
        if not matches:
            raise WorkspaceUnavailable("Live workspace scope changed original input.")

    def _require_authority(self) -> None:
        self._fence()
        self._supervisor.require_current_assignment(self._expected.execution.reference)
        self._fence()

    def _seal(self) -> None:
        if not self._sealed:
            self._sealed = True
            try:
                self._seal_original()
            except Exception:
                # Preserve the original classified failure/cancellation. Resources
                # remain uncertain; failed sealing supplies no completed evidence.
                pass

    async def _await(
        self, operation: Callable[[], Awaitable[_Value]], total_deadline: float
    ) -> _Value:
        self._require_authority()
        loop = asyncio.get_running_loop()
        deadline = min(
            total_deadline, loop.time() + self._policy.operation_timeout_seconds
        )
        task = asyncio.ensure_future(operation())
        owned = cast(asyncio.Future[object], task)
        self._operation = owned
        owned.add_done_callback(_consume_completion)
        while True:
            self._require_authority()
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise WorkspaceUnavailable(
                    "Base materialization exceeded its wait budget."
                )
            if task.done():
                self._operation = None
                return task.result()
            await _wait_for_tick(
                owned, min(remaining, self._policy.poll_interval_seconds)
            )

    def _validate_state(
        self, state: WorkerRecoveryState, delivery: AssignmentDelivery
    ) -> None:
        _validate_original_input(state, delivery)
        history = _historical_workspace(state)
        reservation = self._reservation
        if (
            history is None
            or history.history_unknown
            or history.intent is None
            or history.completion is None
            or not isinstance(reservation, WorkspaceReservation)
        ):
            raise WorkspaceUnavailable(
                "Completed live workspace preparation is required."
            )
        if (
            state.execution != self._expected.execution.intent
            or state.assignment is None
            or state.assignment.assignment != self._expected.execution.reference
            or delivery.definition != self._expected.definition
            or history.intent.location != self._location
            or history.completion.identity != reservation.identity
            or reservation.binding != self._expected
        ):
            raise ClaimReplayConflict(
                "Base materialization changed original workspace pins."
            )

    async def execute(
        self, delivery: AssignmentDelivery, reservation: WorkspaceReservation
    ) -> BaseMaterialization:
        if self._used:
            raise WorkspaceUnavailable(
                "Base materialization has already been requested."
            )
        self._used = True
        try:
            if not isinstance(reservation, WorkspaceReservation):
                raise WorkspaceUnavailable("A live workspace reservation is required.")
            self._reservation = reservation
            self._location = self._workspace.location
            if not isinstance(self._location, WorkspaceLocation):
                raise WorkspaceUnavailable(
                    "A retained live workspace root is required."
                )
            self._require_authority()
            total_deadline = (
                asyncio.get_running_loop().time() + self._policy.total_timeout_seconds
            )
            state = await self._await(self._journal.load_state, total_deadline)
            await self._await(
                lambda: asyncio.to_thread(self._validate_state, state, delivery),
                total_deadline,
            )
            verified = await self._await(
                lambda: cast(Awaitable[object], self._workspace.verify(reservation)),
                total_deadline,
            )
            if verified is not None:
                raise WorkspaceUnavailable(
                    "Live workspace verification is unavailable."
                )
            definition = self._expected.definition
            request = BaseSourceRequest(
                repository_id=definition.repository_id,
                provider_repository_id=definition.provider_repository_id,
                repository_slug=definition.repository_slug,
                base_revision=definition.base_revision,
            )
            snapshot = await self._await(
                lambda: self._source.read(request), total_deadline
            )
            digest, count, size = await self._await(
                lambda: asyncio.to_thread(_validate_snapshot, snapshot, request),
                total_deadline,
            )
            result = await self._await(
                lambda: self._workspace.materialize(reservation, snapshot),
                total_deadline,
            )
            if (
                not isinstance(result, BaseMaterialization)
                or result.reservation is not reservation
                or result.source != request
                or result.root_tree_sha != snapshot.root_tree_sha
                or result.manifest_digest != digest
                or type(result.file_count) is not int
                or result.file_count != count
                or type(result.total_bytes) is not int
                or result.total_bytes != size
            ):
                raise WorkspaceUnavailable(
                    "Base materialization changed verified evidence."
                )
            self._require_authority()
            return result
        except BaseException:
            self._seal()
            if self._operation is not None:
                self._operation.cancel()
            raise
