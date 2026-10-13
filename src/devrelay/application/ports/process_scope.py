from typing import Protocol

from devrelay.application.contracts.processes import ExecutionProcessBinding


class ProcessScopeUnavailable(RuntimeError):
    """Scope management failed; associated-process stop cannot be assumed."""


class ProcessScope(Protocol):
    @property
    def binding(self) -> ExecutionProcessBinding: ...

    def seal(self) -> None:
        """Latch attachment rejection before any awaited termination operation."""
        ...

    async def terminate(self) -> None: ...

    async def active_processes(self) -> int:
        """Fresh native accounting for associated processes, not parent exit."""
        ...
