import asyncio
from typing import cast

import httpx
import pytest

from devrelay.application.contracts.base_sources import BaseSourceRequest
from devrelay.application.ports.base_source import (
    BaseSourceAccessDenied,
    InvalidBaseSourceData,
)
from devrelay.infrastructure.github.base_source import GitHubBaseSource
from devrelay.infrastructure.github.repository_source import RepositoryBinding
from tests.git_source_fixtures import git_workspace_inputs


@pytest.mark.parametrize("changed", [False, True])
def test_raw_commit_read_is_bracketed_by_numeric_repository_checks(
    changed: bool,
) -> None:
    _, _, binding, fixture = git_workspace_inputs()
    snapshot = fixture.snapshot
    calls: list[str] = []
    metadata_reads = 0

    class CommitReader:
        async def read(
            self, request: BaseSourceRequest, *, root_tree_sha: str
        ) -> bytes:
            calls.append("raw-commit")
            assert (
                request == snapshot.request and root_tree_sha == snapshot.root_tree_sha
            )
            assert snapshot.raw_commit is not None
            return snapshot.raw_commit

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal metadata_reads
        calls.append(request.url.path)
        if request.url.path == "/repos/" + snapshot.request.repository_slug:
            metadata_reads += 1
            if changed and metadata_reads == 2:
                return httpx.Response(
                    200,
                    json={
                        "id": 999,
                        "full_name": snapshot.request.repository_slug,
                        "archived": False,
                        "disabled": False,
                    },
                )
        return fixture.respond(request)

    async def exercise() -> None:
        async with httpx.AsyncClient(
            base_url="https://api.github.com/",
            headers={"Authorization": "Bearer fixture-read-token"},
            trust_env=False,
            follow_redirects=False,
            transport=httpx.MockTransport(respond),
        ) as client:
            source = GitHubBaseSource(
                client,
                bindings=(
                    RepositoryBinding(
                        repository_id=binding.definition.repository_id,
                        github_id=binding.definition.provider_repository_id,
                        slug=binding.definition.repository_slug,
                        target_branch="master",
                        coding_branch_prefix="devrelay/jobs",
                    ),
                ),
                commit_source=CommitReader(),
            )

            if changed:
                with pytest.raises(BaseSourceAccessDenied):
                    await source.read(snapshot.request)
            else:
                recovered = await source.read(snapshot.request)
                assert recovered == snapshot

    asyncio.run(exercise())

    assert metadata_reads == 2
    assert calls[-2] == "raw-commit"
    assert calls[0] == calls[-1] == "/repos/" + snapshot.request.repository_slug


@pytest.mark.parametrize(
    "raw", [None, "wrong-type", bytearray(b"wrong"), b"", b"changed"]
)
def test_invalid_raw_commit_never_becomes_a_literal_fallback(raw: object) -> None:
    _, _, binding, fixture = git_workspace_inputs()

    class Changed:
        async def read(
            self, request: BaseSourceRequest, *, root_tree_sha: str
        ) -> bytes:
            return cast(bytes, raw)

    async def exercise() -> None:
        async with httpx.AsyncClient(
            base_url="https://api.github.com/",
            headers={"Authorization": "Bearer fixture-read-token"},
            trust_env=False,
            follow_redirects=False,
            transport=httpx.MockTransport(fixture.respond),
        ) as client:
            source = GitHubBaseSource(
                client,
                bindings=(
                    RepositoryBinding(
                        repository_id=binding.definition.repository_id,
                        github_id=binding.definition.provider_repository_id,
                        slug=binding.definition.repository_slug,
                        target_branch="master",
                        coding_branch_prefix="devrelay/jobs",
                    ),
                ),
                commit_source=Changed(),
            )

            with pytest.raises(InvalidBaseSourceData):
                await source.read(fixture.snapshot.request)

    asyncio.run(exercise())
