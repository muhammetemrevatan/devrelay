"""Actual native candidate lifecycle; no sandbox, model or untrusted command."""

import asyncio
import sys
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest

from devrelay.application.candidate_workspaces import PrepareCandidateCapture
from devrelay.application.contracts.frozen_candidates import FrozenCandidate
from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot
from devrelay.application.contracts.workspace_recovery import (
    WorkerWorkspaceCompletion,
    WorkerWorkspaceIntent,
)
from devrelay.application.editable_workspaces import HandoffMaterializedBase
from devrelay.application.git_workspaces import PrepareMaterializedGit
from devrelay.bootstrap.workspaces import open_worker_workspace
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.git_source_fixtures import git_workspace_inputs
from tests.integration.test_windows_candidates import fixture_git
from tests.unit.test_candidate_objects import commit
from tests.unit.test_materialize_base import Journal
from tests.unit.test_worker_workspace_reservation import Guard

pytestmark = pytest.mark.skipif(
    sys.platform != "win32", reason="Actual Windows candidate scope and handles."
)


def test_materialized_git_to_candidate_and_repair_preserves_original_parent() -> None:
    state, delivery, binding, fixture = git_workspace_inputs()
    calls: list[str] = []
    guard = Guard(binding.execution.reference, calls)
    artifacts = Path(__file__).resolve().parents[4] / "artifacts"

    async def scenario(root: Path) -> tuple[FrozenCandidate, FrozenCandidate, Journal]:
        policy = WindowsWorkspacePolicy(
            root_directory=str(root),
            protected_directories=(str(root.parent / "private"),),
        )
        async with open_worker_workspace(binding, policy) as scope:
            location = await scope.prepare()
            reservation = await scope.reserve(location)
            intent = WorkerWorkspaceIntent(
                workspace_request_id=uuid4(),
                execution=binding.execution.intent,
                location=location,
            )
            journal = Journal(
                replace(
                    state,
                    workspace_intent=intent,
                    workspace_completion=WorkerWorkspaceCompletion(
                        intent=intent, identity=reservation.identity
                    ),
                ),
                calls,
            )
            materialization = await scope.materialize(reservation, fixture.snapshot)
            assert fixture.snapshot.raw_commit is not None
            source = GitBaseSourceSnapshot(
                snapshot=fixture.snapshot, raw_commit=fixture.snapshot.raw_commit
            )
            preparation = await PrepareMaterializedGit(journal, guard, scope).execute(
                delivery, materialization, source
            )
            retained = await PrepareCandidateCapture(journal, guard, scope).execute(
                delivery, materialization
            )
            editable = await HandoffMaterializedBase(journal, guard, scope).execute(
                delivery, materialization
            )
            target = Path(location.root_directory) / reservation.directory_name
            await asyncio.to_thread(
                (target / "README.md").write_bytes, b"First controlled fixture edit\n"
            )

            first = await scope.capture_candidate(
                retained, editable, capture_id=uuid4(), cycle=0, commit=commit()
            )
            await scope.verify_candidate(first)
            parent = await asyncio.to_thread(
                fixture_git, first, "cat-file", "commit", "HEAD^"
            )
            status = await asyncio.to_thread(
                fixture_git, first, "status", "--porcelain=v1", "-z"
            )
            released = await scope.release_candidate(first)
            await asyncio.to_thread(
                (target / "README.md").write_bytes,
                b"Repaired controlled fixture edit\n",
            )
            second = await scope.capture_candidate(
                preparation, released, capture_id=uuid4(), cycle=1, commit=commit()
            )
            await scope.verify_candidate(second)

            assert retained is preparation
            assert scope.source_snapshot is None
            assert scope.editable is editable is released
            assert parent == source.raw_commit
            assert status == b""
            assert (
                first.snapshot.request
                == second.snapshot.request
                == source.snapshot.request
            )
            assert second.snapshot.revision != first.snapshot.revision
            assert second.git_directory != first.git_directory
            assert journal.writes == 0
            return first, second, journal

    with TemporaryDirectory(prefix="dr-flow-", dir=artifacts) as directory:
        root = Path(directory) / "w"
        root.mkdir()

        first, second, journal = asyncio.run(scenario(root))

        assert Path(first.git_directory).is_dir()
        assert Path(second.git_directory).is_dir()
        assert journal.state.execution is state.execution
        (Path(second.worktree) / "README.md").write_bytes(
            b"Owner cleanup released handles\n"
        )
