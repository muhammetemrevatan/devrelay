from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.claim_eligibility import PrepareJobClaim
from devrelay.application.claims import ClaimJob
from devrelay.application.contracts.assignments import ClaimLimits
from devrelay.bootstrap.github import open_github_repository_source
from devrelay.bootstrap.jira import open_jira_task_source
from devrelay.bootstrap.jobs import repository_policy
from devrelay.bootstrap.persistence import (
    claim_store,
    job_branch_reader,
    queued_job_reader,
)
from devrelay.bootstrap.security import lease_proof_guard
from devrelay.config.control import ControlSettings
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


@asynccontextmanager
async def open_job_claimer(
    settings: ControlSettings,
    engine: AsyncEngine,
    *,
    limits: ClaimLimits | None = None,
    jira_transport: httpx.AsyncBaseTransport | None = None,
    github_transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[ClaimJob]:
    """Explicit control composition; no route, polling or runtime registration."""
    if settings.jira is None or settings.github is None or not settings.workers:
        raise ValueError("Claims require provider policies and enrolled workers.")
    credentials = tuple(worker.auth_token for worker in settings.workers) + (
        settings.jira.api_token,
        settings.github.api_token,
    )
    if settings.task_import is not None:
        credentials += (settings.task_import.auth_token,)
    if settings.job_submission is not None:
        credentials += (settings.job_submission.auth_token,)
    if settings.database is not None:
        credentials += (settings.database.password,)
    identities = {
        item.repository_id: item.github_id for item in settings.github.repository_ids
    }
    policies = tuple(
        repository_policy(
            item, settings.jira, provider_repository_id=identities[item.repository_id]
        )
        for item in settings.repositories
    )
    async with open_claim_preparer(
        settings.jira,
        settings.github,
        settings.repositories,
        engine,
        jira_transport=jira_transport,
        github_transport=github_transport,
    ) as preparer:
        yield ClaimJob(
            claim_store(engine, policies, limits),
            preparer,
            lease_proof_guard(credentials),
        )
