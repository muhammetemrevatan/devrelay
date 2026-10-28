import asyncio
from dataclasses import replace
from datetime import timedelta
from typing import cast

import pytest

from devrelay.application.contracts.claims import ClaimReplayConflict
from devrelay.application.contracts.git_sources import GitWorkspacePreparation
from devrelay.application.contracts.worker_recovery import WorkerRecoveryState
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.application.ports.candidate_workspace import CandidateWorkspacePort
from devrelay.application.ports.worker_journal import WorkerStateUnavailable
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from tests.unit.test_candidate_workspaces import Context, context
from tests.unit.test_worker_assignment_fetch import Remote
from tests.unit.test_worker_base_materialization import Journal, active_supervisor
from tests.unit.test_worker_execution_authority import Clock
from tests.unit.test_worker_heartbeat_exchange import Endpoint


def client(
    setup: Context, *, configured: bool = True
) -> tuple[RequestWorkerClaim, PrepareWorkerClaim, Journal, Endpoint]:
    reference = setup.workspace.binding.execution.reference
    authorization = WorkerAuthorization(
        worker_id=reference.worker_id,
        allowed_repository_ids=frozenset({reference.repository_id}),
        credential_expires_at=reference.issued_at + timedelta(hours=1),
    )
    journal = Journal(cast(WorkerRecoveryState, setup.journal.state), setup.calls)
    remote = Remote(authorization, setup.delivery, setup.calls)
    endpoint = Endpoint(authorization, journal, remote, setup.calls)
    preparation = PrepareWorkerClaim(
        journal,
        heartbeat_journal=journal,
        workspace_journal=journal if configured else None,
    )
    worker = RequestWorkerClaim(
        preparation,
        endpoint,
        authorization,
        heartbeat_gateway=endpoint,
        monotonic_clock=Clock(),
    )
    return worker, preparation, journal, endpoint


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_public_candidate_preparation_retains_exact_pins_and_separate_handoff_admission(
    facade: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    worker, preparation, journal, _ = client(setup)
    state = journal.state
    workspace = cast(CandidateWorkspacePort, setup.workspace)

    async def scenario() -> tuple[GitWorkspacePreparation, object]:
        async with active_supervisor(worker, monkeypatch) as supervisor:
            if facade == "prepare":
                result = await preparation.prepare_candidate_capture(
                    setup.delivery, supervisor, workspace, setup.materialization
                )
                editable = await preparation.handoff_materialized_base(
                    setup.delivery, supervisor, workspace, setup.materialization
                )
            else:
                result = await worker.prepare_candidate_capture(
                    setup.delivery, workspace, setup.materialization
                )
                editable = await worker.handoff_materialized_base(
                    setup.delivery, workspace, setup.materialization
                )
            return result, editable.materialization

    result, materialization = asyncio.run(scenario())

    assert result is setup.workspace.git_preparation
    assert materialization is setup.materialization
    assert journal.state.workspace_intent is state.workspace_intent
    assert journal.state.workspace_completion is state.workspace_completion
    assert journal.state.execution is state.execution
    assert setup.calls.count("candidate") == setup.calls.count("handoff") == 1
    assert setup.workspace.seals == 0


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_public_preparation_without_journal_does_not_touch_workspace(
    facade: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    worker, preparation, _, _ = client(setup, configured=False)
    workspace = cast(CandidateWorkspacePort, setup.workspace)

    async def scenario() -> None:
        async with active_supervisor(worker, monkeypatch) as supervisor:
            with pytest.raises(WorkerStateUnavailable, match="Workspace recovery"):
                if facade == "prepare":
                    await preparation.prepare_candidate_capture(
                        setup.delivery, supervisor, workspace, setup.materialization
                    )
                else:
                    await worker.prepare_candidate_capture(
                        setup.delivery, workspace, setup.materialization
                    )

    asyncio.run(scenario())

    assert "candidate" not in setup.calls
    assert setup.workspace.seals == 0


@pytest.mark.parametrize("facade", ["prepare", "request"])
def test_public_preparation_cannot_retry_with_a_new_scope_after_rejection(
    facade: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context()
    other = context()
    worker, preparation, _, _ = client(setup)

    async def scenario() -> None:
        async with active_supervisor(worker, monkeypatch) as supervisor:

            async def invoke(candidate: Context) -> GitWorkspacePreparation:
                if facade == "prepare":
                    return await preparation.prepare_candidate_capture(
                        candidate.delivery,
                        supervisor,
                        cast(CandidateWorkspacePort, candidate.workspace),
                        candidate.materialization,
                    )
                return await worker.prepare_candidate_capture(
                    candidate.delivery,
                    cast(CandidateWorkspacePort, candidate.workspace),
                    candidate.materialization,
                )

            setup.workspace.git_preparation = None

            with pytest.raises(WorkspaceUnavailable):
                await invoke(setup)
            with pytest.raises(WorkspaceUnavailable, match="already started"):
                await invoke(other)

    asyncio.run(scenario())

    assert setup.workspace.seals == 1
    assert "candidate" not in setup.calls and "candidate" not in other.calls
    assert other.workspace.seals == 0


def test_request_without_current_supervision_cannot_retain_source() -> None:
    setup = context()
    worker, _, _, _ = client(setup)

    with pytest.raises(LeaseDeadlineRejected):
        asyncio.run(
            worker.prepare_candidate_capture(
                setup.delivery,
                cast(CandidateWorkspacePort, setup.workspace),
                setup.materialization,
            )
        )

    assert "candidate" not in setup.calls
    assert setup.workspace.seals == 1


def test_changed_definition_cannot_pass_public_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context()
    worker, _, _, _ = client(setup)
    delivery = replace(
        setup.delivery,
        definition=replace(setup.delivery.definition, policy_version="other-policy"),
    )

    async def scenario() -> None:
        async with active_supervisor(worker, monkeypatch):
            with pytest.raises(ClaimReplayConflict):
                await worker.prepare_candidate_capture(
                    delivery,
                    cast(CandidateWorkspacePort, setup.workspace),
                    setup.materialization,
                )

    asyncio.run(scenario())

    assert "candidate" not in setup.calls
    assert setup.workspace.seals == 1
