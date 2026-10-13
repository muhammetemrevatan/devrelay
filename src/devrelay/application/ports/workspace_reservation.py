from typing import Protocol

from devrelay.application.contracts.workspaces import (
    WorkspaceBinding,
    WorkspaceReservation,
)


class WorkspaceUnavailable(Exception):
    """A directory reservation is unavailable or uncertain; never retry blindly."""


class WorkspaceReservationPort(Protocol):
    @property
    def binding(self) -> WorkspaceBinding: ...

    async def reserve(self) -> WorkspaceReservation:
        """Create once only; cancellation may leave a directory behind."""
        ...
