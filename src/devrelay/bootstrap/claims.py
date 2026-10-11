from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.claim_eligibility import PrepareJobClaim
from devrelay.bootstrap.github import open_github_repository_source
from devrelay.bootstrap.jira import open_jira_task_source
from devrelay.bootstrap.jobs import repository_policy
from devrelay.bootstrap.persistence import job_branch_reader, queued_job_reader
from devrelay.config.github import GitHubSettings
from devrelay.config.jira import JiraSettings
from devrelay.config.repositories import RepositorySettings


@asynccontextmanager
async def open_claim_preparer(
    jira: JiraSettings,
    github: GitHubSettings,
    repositories: tuple[RepositorySettings, ...],
    engine: AsyncEngine,
    *,
    jira_transport: httpx.AsyncBaseTransport | None = None,
    github_transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[PrepareJobClaim]:
    """Control-only composition; never registered as worker execution or an API."""
    identities = {item.repository_id: item.github_id for item in github.repository_ids}
    if set(identities) != {item.repository_id for item in repositories}:
        raise ValueError("Repository identity configuration does not match policy.")
    policies = tuple(
        repository_policy(
            item, jira, provider_repository_id=identities[item.repository_id]
        )
        for item in repositories
    )
    async with open_github_repository_source(
        github, repositories, job_branch_reader(engine), transport=github_transport
    ) as observer:
        async with open_jira_task_source(jira, transport=jira_transport) as source:
            yield PrepareJobClaim(queued_job_reader(engine), source, observer, policies)
