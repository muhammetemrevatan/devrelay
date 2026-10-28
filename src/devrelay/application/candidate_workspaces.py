import asyncio

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseMaterializationPolicy,
)
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.contracts.workspace_recovery import WorkspaceLocation
from devrelay.application.editable_workspaces import MaterializedWorkspaceOperation
from devrelay.application.ports.candidate_workspace import CandidateWorkspacePort
from devrelay.application.ports.worker_workspace_journal import WorkerWorkspaceJournal
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_executions import ExecutionAuthorityGuard

_DEFAULT_POLICY = BaseMaterializationPolicy()


class PrepareCandidateCapture(MaterializedWorkspaceOperation):
    """Opt in to retaining the verified base before making source files editable."""

    def __init__(
        self,
        journal: WorkerWorkspaceJournal,
        supervisor: ExecutionAuthorityGuard,
        workspace: CandidateWorkspacePort,
        policy: BaseMaterializationPolicy = _DEFAULT_POLICY,
    ) -> None:
        super().__init__(journal, supervisor, workspace, policy)
        self._candidates = workspace

    def _validate_source(self, source: GitBaseSourceSnapshot) -> None:
        source.validate()
        materialization = self._materialization
        if (
            materialization is None
            or source.snapshot.request != materialization.source
            or source.snapshot.root_tree_sha != materialization.root_tree_sha
            or source.snapshot.fingerprint() != materialization.manifest_digest
        ):
            raise WorkspaceUnavailable(
                "Candidate source changed original materialization."
            )

    async def execute(
        self, delivery: AssignmentDelivery, materialization: BaseMaterialization
    ) -> GitWorkspacePreparation:
        if self._used:
            raise WorkspaceUnavailable("Candidate preparation already started.")
        self._used = True
        try:
            self._materialization = materialization
            self._location = self._workspace.location
            if not isinstance(self._location, WorkspaceLocation):
                raise WorkspaceUnavailable("A retained workspace root is required.")
            self._require_authority()
            preparation = self._candidates.git_preparation
            snapshot = self._candidates.source_snapshot
            if (
                not isinstance(preparation, GitWorkspacePreparation)
                or preparation.materialization is not materialization
                or preparation.branch != self._expected.definition.authorized_branch
                or snapshot is None
                or snapshot.raw_commit is None
                or self._candidates.editable is not None
                or self._candidates.candidate_capture_preparation is not None
            ):
                raise WorkspaceUnavailable("Original protected Git source is required.")
            raw_commit = snapshot.raw_commit
            deadline = (
                asyncio.get_running_loop().time() + self._policy.total_timeout_seconds
            )
            state = await self._await(self._journal.load_state, deadline)
            await self._await(
                lambda: asyncio.to_thread(self._validate, state, delivery), deadline
            )
            source = await self._await(
                lambda: asyncio.to_thread(
                    GitBaseSourceSnapshot, snapshot=snapshot, raw_commit=raw_commit
                ),
                deadline,
            )
            await self._await(
                lambda: asyncio.to_thread(self._validate_source, source), deadline
            )
            result = await self._await(
                lambda: self._candidates.prepare_candidate_capture(source), deadline
            )
            if (
                result is not preparation
                or self._candidates.candidate_capture_preparation is not preparation
                or self._candidates.git_preparation is not preparation
                or self._candidates.editable is not None
            ):
                raise WorkspaceUnavailable("Candidate preparation changed live scope.")
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
