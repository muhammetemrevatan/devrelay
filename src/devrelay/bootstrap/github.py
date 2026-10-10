from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from devrelay.application.ports.job_persistence import JobBranchReader
from devrelay.config.github import GitHubSettings
from devrelay.config.repositories import RepositorySettings
from devrelay.infrastructure.github.repository_source import (
    GitHubRepositorySource,
    RepositoryBinding,
)


@asynccontextmanager
async def open_github_repository_source(
    settings: GitHubSettings,
    repositories: tuple[RepositorySettings, ...],
    branches: JobBranchReader,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[GitHubRepositorySource]:
    numeric_ids = {
        item.repository_id: item.github_id for item in settings.repository_ids
    }
    if set(numeric_ids) != {item.repository_id for item in repositories}:
        raise ValueError("GitHub bindings must match configured repository policy.")
    bindings = tuple(
        RepositoryBinding(
            repository_id=repo.repository_id,
            github_id=numeric_ids[repo.repository_id],
            slug=repo.slug,
            target_branch=repo.target_branch,
            coding_branch_prefix=repo.coding_branch_prefix,
        )
        for repo in repositories
        if repo.enabled
    )
    async with httpx.AsyncClient(
        base_url="https://api.github.com/",
        headers={
            "Authorization": f"Bearer {settings.api_token.get_secret_value()}",
            "Accept": "application/vnd.github+json",
            "Accept-Encoding": "identity",
            "X-GitHub-Api-Version": "2026-03-10",
        },
        timeout=httpx.Timeout(settings.request_timeout_seconds),
        limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
        follow_redirects=False,
        trust_env=False,
        transport=transport,
    ) as client:
        yield GitHubRepositorySource(
            client,
            branches,
            bindings=bindings,
            request_timeout_seconds=settings.request_timeout_seconds,
            observation_timeout_seconds=settings.observation_timeout_seconds,
            max_response_bytes=settings.max_response_bytes,
            max_pull_request_pages=settings.max_pull_request_pages,
        )
