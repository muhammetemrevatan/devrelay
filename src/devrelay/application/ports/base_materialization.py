from typing import Protocol

from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
    BaseSourceSnapshot,
)
from devrelay.application.contracts.workspace_recovery import WorkspaceLocation
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceReservation,
)


class BaseMaterializationPort(Protocol):
    @property
    def binding(self) -> WorkspaceBinding: ...

    @property
    def reservation(self) -> WorkspaceReservation | None: ...

    @property
    def location(self) -> WorkspaceLocation | None: ...

    def seal(self) -> None:
        """Latch this live scope's writes synchronously without waiting for native I/O."""
        ...

    async def verify(self, reservation: WorkspaceReservation) -> None: ...

    async def materialize(
        self, reservation: WorkspaceReservation, snapshot: BaseSourceSnapshot
    ) -> BaseMaterialization: ...
