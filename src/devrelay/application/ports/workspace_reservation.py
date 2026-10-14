from typing import Protocol

from devrelay.application.contracts.workspace_recovery import WorkspaceLocation
from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceReservation,
)


class WorkspaceUnavailable(Exception):
    """A directory reservation is unavailable or uncertain; never retry blindly."""


class WorkspaceReservationPort(Protocol):
    @property
    def binding(self) -> WorkspaceBinding: ...

    async def prepare(self) -> WorkspaceLocation:
        """Observe and retain root before committing any creation intent."""
        ...

    async def reserve(self, location: WorkspaceLocation) -> WorkspaceReservation:
        """Create once only; cancellation may leave a directory behind."""
        ...
