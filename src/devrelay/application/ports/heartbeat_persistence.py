from typing import Protocol

from devrelay.application.contracts.heartbeats import HeartbeatIdentity, HeartbeatResult
from devrelay.application.contracts.workers import WorkerAuthorization


class HeartbeatStore(Protocol):
    async def heartbeat(
        self, identity: HeartbeatIdentity, authorization: WorkerAuthorization
    ) -> HeartbeatResult:
        """Fence renewal and retain bounded operation identity in one transaction.

        Exact replay never renews: return renewed=False with a fresh current
        authority/stop observation, rather than historical launch authority.
        """
        ...
