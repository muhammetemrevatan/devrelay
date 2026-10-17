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
from devrelay.application.ports.git_workspace import GitWorkspacePort
from devrelay.application.ports.worker_workspace_journal import WorkerWorkspaceJournal
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_executions import ExecutionAuthorityGuard

_DEFAULT_POLICY = BaseMaterializationPolicy()


class PrepareMaterializedGit(MaterializedWorkspaceOperation):
    """Create exact-base metadata once while the literal source remains guarded."""

    def __init__(
        self,
        journal: WorkerWorkspaceJournal,
        supervisor: ExecutionAuthorityGuard,
        workspace: GitWorkspacePort,
        policy: BaseMaterializationPolicy = _DEFAULT_POLICY,
    ) -> None:
        super().__init__(journal, supervisor, workspace, policy)
        self._git_workspace = workspace

    def _validate_source(self, source: GitBaseSourceSnapshot) -> None:
        if not isinstance(source, GitBaseSourceSnapshot):
            raise WorkspaceUnavailable("Original Git source is required.")
        source.validate()
        materialization = self._materialization
        if materialization is None or (
            source.snapshot.request != materialization.source
            or source.snapshot.fingerprint() != materialization.manifest_digest
            or source.snapshot.root_tree_sha != materialization.root_tree_sha
        ):
            raise WorkspaceUnavailable("Git source changed original materialization.")

    async def execute(
        self,
        delivery: AssignmentDelivery,
        materialization: BaseMaterialization,
        source: GitBaseSourceSnapshot | None = None,
    ) -> GitWorkspacePreparation:
        if self._used:
            raise WorkspaceUnavailable("Git metadata preparation already started.")
        self._used = True
        try:
            if not isinstance(materialization, BaseMaterialization):
                raise WorkspaceUnavailable("A live materialization is required.")
            self._materialization = materialization
            self._location = self._workspace.location
            if not isinstance(self._location, WorkspaceLocation):
                raise WorkspaceUnavailable("A retained workspace root is required.")
            self._require_authority()
            deadline = (
                asyncio.get_running_loop().time() + self._policy.total_timeout_seconds
            )
            state = await self._await(self._journal.load_state, deadline)
            await self._await(
                lambda: asyncio.to_thread(self._validate, state, delivery), deadline
            )
            if source is None:
                snapshot = self._git_workspace.source_snapshot
                if snapshot is None or snapshot.raw_commit is None:
                    raise WorkspaceUnavailable(
                        "Original Git commit delivery is required."
                    )
                raw_commit = snapshot.raw_commit
                source = await self._await(
                    lambda: asyncio.to_thread(
                        GitBaseSourceSnapshot,
                        snapshot=snapshot,
                        raw_commit=raw_commit,
                    ),
                    deadline,
                )
            if not isinstance(source, GitBaseSourceSnapshot):
                raise WorkspaceUnavailable("Original Git commit source is unavailable.")
            original_source: GitBaseSourceSnapshot = source
            await self._await(
                lambda: asyncio.to_thread(self._validate_source, original_source),
                deadline,
            )
            branch = self._expected.definition.authorized_branch
            result = await self._await(
                lambda: self._git_workspace.prepare_git_metadata(
                    original_source, materialization, branch
                ),
                deadline,
            )
            if (
                not isinstance(result, GitWorkspacePreparation)
                or result.materialization is not materialization
                or result.branch != branch
            ):
                raise WorkspaceUnavailable("Git metadata changed original pins.")
            result.__post_init__()
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
