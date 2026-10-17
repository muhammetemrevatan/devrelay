import asyncio
from dataclasses import dataclass, replace
from typing import Any
from uuid import UUID

import pytest

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.base_sources import (
    BaseMaterialization,
)
from devrelay.application.contracts.git_sources import (
    GitBaseSourceSnapshot,
    GitWorkspacePreparation,
)
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
)
from devrelay.application.git_workspaces import PrepareMaterializedGit
from devrelay.application.ports.workspace_reservation import WorkspaceUnavailable
from tests.git_source_fixtures import git_workspace_inputs
from tests.unit.test_editable_workspaces import Workspace as EditableWorkspace
from tests.unit.test_materialize_base import Journal
from tests.unit.test_materialize_base import Workspace as BaseWorkspace
from tests.unit.test_worker_workspace_reservation import Guard


class Workspace(EditableWorkspace):
    source_snapshot: Any
    returned: Any = None

    async def prepare_git_metadata(
        self,
        source: GitBaseSourceSnapshot,
        materialization: BaseMaterialization,
        branch: str,
    ) -> GitWorkspacePreparation:
        self.calls.append("git")
        source.validate()
        assert materialization is self.materialization
        assert source.snapshot is self.source_snapshot
        assert branch == self.binding.definition.authorized_branch
        return self.returned or GitWorkspacePreparation(
            materialization=materialization, branch=branch, metadata_digest="d" * 64
        )


@dataclass
class Context:
    service: PrepareMaterializedGit
    workspace: Workspace
    journal: Journal
    delivery: AssignmentDelivery
    materialization: BaseMaterialization
    source: GitBaseSourceSnapshot
    calls: list[str]


def context() -> Context:
    state, delivery, binding, fixture = git_workspace_inputs()
    calls: list[str] = []
    original = BaseWorkspace(binding, calls)
    intent = WorkerWorkspaceIntent(
        workspace_request_id=UUID(int=901),
        execution=binding.execution.intent,
        location=original.location,
    )
    state = replace(
        state,
        workspace_intent=intent,
        workspace_completion=WorkerWorkspaceCompletion(
            intent=intent, identity=original.reservation.identity
        ),
    )
    snapshot = fixture.snapshot
    assert snapshot.raw_commit is not None
    source = GitBaseSourceSnapshot(snapshot=snapshot, raw_commit=snapshot.raw_commit)
    result = BaseMaterialization(
        reservation=original.reservation,
        source=snapshot.request,
        root_tree_sha=snapshot.root_tree_sha,
        manifest_digest=snapshot.fingerprint(),
        file_count=len(snapshot.files),
        total_bytes=sum(len(item.content) for item in snapshot.files),
    )
    workspace = Workspace(original, result, calls)
    workspace.source_snapshot = snapshot
    journal = Journal(state, calls)
    service = PrepareMaterializedGit(
        journal,
        Guard(binding.execution.reference, calls),
        workspace,  # type: ignore[arg-type]
    )
    return Context(service, workspace, journal, delivery, result, source, calls)


@pytest.mark.parametrize("cached", [False, True])
def test_git_preparation_preserves_original_pins_and_journal(cached: bool) -> None:
    setup = context()
    original_state = setup.journal.state

    result = asyncio.run(
        setup.service.execute(
            setup.delivery, setup.materialization, None if cached else setup.source
        )
    )

    assert result.materialization is setup.materialization
    assert result.branch == setup.delivery.definition.authorized_branch
    assert setup.journal.state is original_state and setup.journal.writes == 0
    assert [call for call in setup.calls if call != "authority"] == ["journal", "git"]


@pytest.mark.parametrize(
    "mutation",
    [
        "foreign_materialization",
        "missing_source",
        "literal_source",
        "missing_history",
        "changed_definition",
        "wrong_reply_branch",
        "wrong_reply_materialization",
    ],
)
def test_git_preparation_rejects_changed_or_missing_evidence(mutation: str) -> None:
    setup = context()
    result = setup.materialization
    delivery = setup.delivery
    match mutation:
        case "foreign_materialization":
            result = replace(result)
        case "missing_source":
            setup.workspace.source_snapshot = None
        case "literal_source":
            setup.workspace.source_snapshot = replace(
                setup.source.snapshot, raw_commit=None
            )
        case "missing_history":
            setup.journal.state = replace(
                setup.journal.state, workspace_completion=None
            )
        case "changed_definition":
            delivery = replace(
                delivery,
                definition=replace(delivery.definition, policy_version="different"),
            )
        case "wrong_reply_branch":
            setup.workspace.returned = GitWorkspacePreparation(
                materialization=result, branch="wrong/branch", metadata_digest="f" * 64
            )
        case "wrong_reply_materialization":
            setup.workspace.returned = GitWorkspacePreparation(
                materialization=replace(result),
                branch=delivery.definition.authorized_branch,
                metadata_digest="f" * 64,
            )

    with pytest.raises((WorkspaceUnavailable, ValueError)):
        asyncio.run(setup.service.execute(delivery, result))

    assert setup.workspace.seals == 1 and setup.journal.writes == 0


def test_git_preparation_latches_before_first_await() -> None:
    setup = context()

    async def exercise() -> None:
        first = asyncio.create_task(
            setup.service.execute(setup.delivery, setup.materialization)
        )
        await asyncio.sleep(0)
        try:
            with pytest.raises(WorkspaceUnavailable, match="already started"):
                await setup.service.execute(setup.delivery, setup.materialization)
        finally:
            await first

    asyncio.run(exercise())

    assert setup.calls.count("git") == 1


def test_git_preparation_cancel_seals_original_before_native_wrapper_cancel() -> None:
    setup = context()
    cancelled_after_seal: list[bool] = []

    async def exercise() -> None:
        entered = asyncio.Event()

        async def held(*args: Any) -> GitWorkspacePreparation:
            entered.set()
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                cancelled_after_seal.append(setup.workspace.seals > 0)
                raise
            raise AssertionError("Unreachable fixture completion.")

        setup.workspace.prepare_git_metadata = held  # type: ignore[method-assign, assignment]
        pending = asyncio.create_task(
            setup.service.execute(setup.delivery, setup.materialization)
        )
        await entered.wait()

        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await asyncio.sleep(0)

    asyncio.run(exercise())

    assert cancelled_after_seal == [True]
    assert setup.journal.writes == 0
