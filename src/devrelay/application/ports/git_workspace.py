from typing import Protocol

from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.ports.editable_workspace import EditableWorkspacePort


class GitWorkspacePort(EditableWorkspacePort, Protocol):
    @property
    def source_snapshot(self) -> BaseSourceSnapshot | None: ...

    async def prepare_git_metadata(
        self,
        source: GitBaseSourceSnapshot,
        materialization: BaseMaterialization,
        branch: str,
    ) -> GitWorkspacePreparation: ...
