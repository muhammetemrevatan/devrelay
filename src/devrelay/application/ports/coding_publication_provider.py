from collections.abc import Callable
from typing import Protocol

from devrelay.application.contracts.coding_publications import (
    ProviderPublication,
    PublicationObservation,
    PublicationPlan,
    PublicationWrite,
)


class CodingPublicationProvider(Protocol):
    async def write(
        self,
        plan: PublicationPlan,
        write: PublicationWrite,
        *,
        before_write: Callable[[], None],
    ) -> PublicationObservation: ...

    async def observe(
        self, plan: PublicationPlan, write: PublicationWrite
    ) -> PublicationObservation | None: ...

    async def verify(
        self, plan: PublicationPlan, *, pull_request_number: int
    ) -> ProviderPublication: ...
