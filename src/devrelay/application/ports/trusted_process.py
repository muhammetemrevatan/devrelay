from typing import Protocol

from devrelay.application.contracts.trusted_processes import (
    TrustedProcessObservation,
    TrustedProcessSpec,
)
from devrelay.application.ports.process_scope import ProcessScope


class TrustedProcessScope(ProcessScope, Protocol):
    async def prepare(self, spec: TrustedProcessSpec) -> None:
        """Create one suspended host-owned process and attach before returning."""
        ...

    async def resume(self) -> None:
        """Resume exactly once; sealed scopes reject preparation and resume."""
        ...

    async def observe(self) -> TrustedProcessObservation:
        """Return bounded nonblocking pipe reads and root exit observation."""
        ...
