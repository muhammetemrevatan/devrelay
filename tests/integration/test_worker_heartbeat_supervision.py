import asyncio
import sys
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from time import monotonic

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.worker_heartbeats import HeartbeatPolicy, HeartbeatStopReason
from devrelay.config.control import ControlSettings
from tests.integration.test_worker_heartbeat_http_recovery import (
    HeartbeatResponses,
    heartbeat_storage,
    prepared_worker,
    require_original_anchors,
)


class HeldHeartbeat(HeartbeatResponses):
    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        super().__init__(inner)
        self.committed = asyncio.Event()
        self.release = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await super().handle_async_request(request)
        if request.url.path.endswith("/heartbeats"):
            assert response.status_code == 200
            self.committed.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                await response.aclose()
                raise
        return response


@pytest.mark.postgres
@pytest.mark.parametrize("native", [True, False])
def test_supervision_timeout_preserves_committed_input_for_fresh_restart_replay(
    database: Callable[[], AbstractAsyncContextManager[AsyncEngine]],
    worker_configuration: ControlSettings,
    tmp_path: Path,
    native: bool,
) -> None:
    if native and sys.platform != "win32":
        pytest.skip("Requires native Windows DPAPI/ACL and GetTickCount64.")
    policy = HeartbeatPolicy(
        exchange_timeout_seconds=1,
        monitor_interval_seconds=0.05,
        maximum_exchanges=1,
    )

    async def scenario() -> None:
        async with database() as engine:
            async with prepared_worker(
                engine, worker_configuration, tmp_path, native=native, clock=monotonic
            ) as harness:
                initial_transport = harness.transport()
                held = HeldHeartbeat(initial_transport.inner)
                async with harness.open_worker(held) as worker:
                    supervisor = worker.heartbeat_supervisor(policy=policy)
                    running = asyncio.create_task(supervisor.run())
                    await asyncio.wait_for(held.committed.wait(), timeout=5)

                    stopped = await running
                    pending = await worker.recover()
                after_timeout = await heartbeat_storage(engine)
                replay = harness.transport()
                async with harness.open_worker(replay) as restarted:
                    observer = restarted.heartbeat_supervisor(
                        policy=HeartbeatPolicy(maximum_exchanges=1)
                    )
                    recovered_stop = await observer.run()
                    recovered = await restarted.recover()
                after_replay = await heartbeat_storage(engine)

                assert stopped.reason is HeartbeatStopReason.EXCHANGE_TIMEOUT
                assert stopped.exchange_settled and supervisor.stop_required
                assert supervisor.deadline is None
                assert pending.pending_heartbeat is not None
                assert after_timeout == (2, 1, 1)
                assert after_replay == after_timeout
                assert held.inputs == replay.inputs
                assert recovered.last_heartbeat == pending.pending_heartbeat
                assert recovered.pending_heartbeat is None
                assert recovered_stop.reason is HeartbeatStopReason.BUDGET_EXHAUSTED
                assert observer.deadline is not None
                assert observer.deadline.lease_revision == 2
                require_original_anchors(pending, harness.initial)
                require_original_anchors(recovered, harness.initial)

    asyncio.run(
        scenario(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )
