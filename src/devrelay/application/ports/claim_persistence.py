from typing import Protocol

from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.claims import ClaimRequestIdentity
from devrelay.application.contracts.eligibility import PreparedJobClaim
from devrelay.application.contracts.workers import WorkerAuthorization


class ClaimPreparer(Protocol):
    async def execute(
        self, authorization: WorkerAuthorization
    ) -> PreparedJobClaim | None: ...


class ClaimStore(Protocol):
    async def recover(
        self, identity: ClaimRequestIdentity, authorization: WorkerAuthorization
    ) -> ClaimResult | None: ...

    async def check_admission(self, authorization: WorkerAuthorization) -> None: ...

    async def claim(
        self,
        identity: ClaimRequestIdentity,
        authorization: WorkerAuthorization,
        observation: PreparedJobClaim | None,
    ) -> ClaimResult: ...
