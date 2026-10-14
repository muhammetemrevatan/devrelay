import asyncio
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import SecretStr

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
)
from devrelay.application.contracts.heartbeats import (
    HeartbeatResult,
    LeaseAuthorityObservation,
    WorkerHeartbeatExchange,
)
from devrelay.application.contracts.processes import ExecutionProcessBinding
from devrelay.application.contracts.worker_recovery import (
    AcknowledgementCompletion,
    WorkerHeartbeatIntent,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.worker_heartbeats import HeartbeatPolicy, HeartbeatSupervisor
from devrelay.bootstrap.worker import open_worker_journal
from devrelay.bootstrap.workspaces import open_worker_workspace
from devrelay.config.worker import WorkerSettings
from devrelay.infrastructure.worker_state.clock import WindowsLeaseClock
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.acknowledgement_fixtures import acknowledgement_inputs
from tests.integration.test_worker_journal import TOKEN

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="Windows private journal and directory handles"
)


@asynccontextmanager
async def fixture_supervisor(
    delivery: AssignmentDelivery,
) -> AsyncIterator[HeartbeatSupervisor]:
    reference = delivery.outcome.assignment
    assert reference is not None
    clock = await asyncio.to_thread(WindowsLeaseClock)
    heartbeat = WorkerHeartbeatIntent(
        heartbeat_request_id=uuid4(),
        claim_request_id=delivery.outcome.claim_request_id,
        attempt_id=reference.attempt_id,
        generation=1,
        expected_lease_revision=1,
    )

    async def observed_control_reply() -> WorkerHeartbeatExchange:
        started = clock()
        return WorkerHeartbeatExchange(
            intent=heartbeat,
            result=HeartbeatResult(
                heartbeat_request_id=heartbeat.heartbeat_request_id,
                authority=LeaseAuthorityObservation(
                    reference=reference,
                    lease_revision=2,
                    observed_at=reference.issued_at + timedelta(seconds=20),
                    updated_at=reference.issued_at + timedelta(seconds=20),
                    current_expires_at=reference.expires_at + timedelta(seconds=30),
                ),
                renewed=True,
                stop_required=False,
            ),
            request_started_at=started,
            received_at=clock(),
        )

    supervisor = HeartbeatSupervisor(
        observed_control_reply,
        WorkerAuthorization(
            worker_id=reference.worker_id,
            allowed_repository_ids=frozenset({reference.repository_id}),
            credential_expires_at=reference.issued_at + timedelta(hours=1),
        ),
        clock,
        HeartbeatPolicy(),
    )
    running = asyncio.create_task(supervisor.run())
    try:
        for _ in range(200):
            if supervisor.deadline is not None:
                break
            await asyncio.sleep(0.005)
        supervisor.require_current_assignment(reference)
        yield supervisor
    finally:
        supervisor.request_stop()
        await asyncio.wait_for(running, timeout=5)


def test_native_protected_workspace_reopens_only_historical_completion(
    tmp_path: Path,
) -> None:
    _, original = acknowledgement_inputs()
    root = tmp_path / "workspaces"
    root.mkdir()
    directory = tmp_path / "private-state"
    settings = WorkerSettings(
        control_url="https://control.example/",
        auth_token=SecretStr(TOKEN),
        workspace_root=root,
    )

    async def exercise() -> None:
        async with open_worker_journal(
            settings, worker_id=original.outcome.worker_id, state_directory=directory
        ) as worker:
            request = await worker.prepare()
            outcome = replace(
                original.outcome, claim_request_id=request.claim_request_id
            )
            delivery = replace(original, outcome=outcome)
            await worker.record_outcome(request, outcome)
            acknowledgement = await worker.prepare_acknowledgement(delivery)
            await worker.record_acknowledgement(
                request,
                AcknowledgementCompletion(
                    intent=acknowledgement,
                    acknowledged_at=outcome.recorded_at + timedelta(seconds=1),
                ),
            )
            reference = outcome.assignment
            assert reference is not None
            async with fixture_supervisor(delivery) as supervisor:
                execution = await worker.prepare_execution(
                    delivery,
                    supervisor,
                    ExecutionPolicy(
                        runtime=ExecutionRuntime.CODEX,
                        isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
                        policy_digest="a" * 64,
                    ),
                )
                binding = await asyncio.to_thread(
                    WorkspaceBinding,
                    execution=ExecutionProcessBinding(
                        intent=execution, reference=reference
                    ),
                    definition=delivery.definition,
                )
                policy = WindowsWorkspacePolicy(
                    root_directory=str(root), protected_directories=(str(directory),)
                )
                async with open_worker_workspace(binding, policy) as workspace:
                    result = await worker.reserve_workspace(
                        delivery, supervisor, workspace
                    )
            historical = await worker.recover_workspace(delivery)
            assert historical is not None and historical.completion is not None
            assert historical.completion.identity == result.identity
            assert historical.intent is not None
            proof = request.proof.reveal().encode("ascii")
        async with open_worker_journal(
            settings, worker_id=outcome.worker_id, state_directory=directory
        ) as reopened:
            recovered = await reopened.recover_workspace(delivery)
            assert recovered == historical
            assert recovered is not None and not recovered.history_unknown
        assert list((root / result.directory_name).iterdir()) == []
        assert all(proof not in path.read_bytes() for path in directory.iterdir())

    asyncio.run(exercise())
