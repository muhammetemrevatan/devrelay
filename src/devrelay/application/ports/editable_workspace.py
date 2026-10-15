from typing import Protocol

from devrelay.application.contracts.base_sources import BaseMaterialization
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.ports.base_materialization import BaseMaterializationPort


class EditableWorkspacePort(BaseMaterializationPort, Protocol):
    @property
    def materialization(self) -> BaseMaterialization | None: ...

    async def handoff(
        self, materialization: BaseMaterialization
    ) -> EditableWorkspace: ...
