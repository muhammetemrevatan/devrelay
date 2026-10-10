from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from uuid import UUID

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.contracts.submissions import CheckPolicy, RepositoryPolicy
from devrelay.application.jobs import PrepareJobSubmission, SubmitJob
from devrelay.application.ports.repository_source import RepositorySubmissionSource
from devrelay.application.tasks import TaskImportPolicy
from devrelay.bootstrap.github import open_github_repository_source
from devrelay.bootstrap.jira import open_jira_task_source
from devrelay.bootstrap.persistence import (
    job_branch_reader,
    job_submission_reader,
    job_submission_unit_of_work_factory,
    task_snapshot_reader,
)
from devrelay.config.github import GitHubSettings
from devrelay.config.jira import JiraSettings
from devrelay.config.repositories import RepositorySettings


def repository_policy(
    settings: RepositorySettings, jira: JiraSettings, *, provider_repository_id: int
) -> RepositoryPolicy:
    if not settings.allowed_project_keys <= jira.allowed_project_keys:
        raise ValueError("Repository source scope exceeds configured Jira scope.")
    return RepositoryPolicy(
        repository_id=settings.repository_id,
        provider_repository_id=provider_repository_id,
        slug=settings.slug,
        enabled=settings.enabled,
        target_branch=settings.target_branch,
        coding_branch_prefix=settings.coding_branch_prefix,
        version=settings.policy_version,
        task_scope=TaskImportPolicy(
            provider="jira",
            site_id=str(jira.cloud_id),
            allowed_scope_keys=settings.allowed_project_keys,
            readiness_label=settings.readiness_label,
        ),
        checks=tuple(
            CheckPolicy(
                id=check.id,
                argv=check.argv,
                working_directory=check.working_directory,
                timeout_seconds=check.timeout_seconds,
            )
            for check in settings.checks
        ),
        max_repair_cycles=settings.max_repair_cycles,
    )


@asynccontextmanager
async def open_job_preparer(
    jira: JiraSettings,
    repositories: tuple[RepositorySettings, ...],
    engine: AsyncEngine,
    *,
    repository_ids: Mapping[UUID, int],
    transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[PrepareJobSubmission]:
    if set(repository_ids) != {settings.repository_id for settings in repositories}:
        raise ValueError("Repository identity configuration does not match policy.")
    policies = tuple(
        repository_policy(
            settings,
            jira,
            provider_repository_id=repository_ids[settings.repository_id],
        )
        for settings in repositories
    )
    async with open_jira_task_source(jira, transport=transport) as source:
        yield PrepareJobSubmission(task_snapshot_reader(engine), source, policies)


@asynccontextmanager
async def open_job_submitter(
    jira: JiraSettings,
    repositories: tuple[RepositorySettings, ...],
    engine: AsyncEngine,
    repository_source: RepositorySubmissionSource,
    *,
    repository_ids: Mapping[UUID, int],
    transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[SubmitJob]:
    """Require an explicit trusted Git observer; never supply a permissive default."""
    async with open_job_preparer(
        jira, repositories, engine, repository_ids=repository_ids, transport=transport
    ) as preparer:
        yield SubmitJob(
            preparer,
            job_submission_reader(engine),
            job_submission_unit_of_work_factory(engine),
            repository_source,
        )


@asynccontextmanager
async def open_github_job_submitter(
    jira: JiraSettings,
    github: GitHubSettings,
    repositories: tuple[RepositorySettings, ...],
    engine: AsyncEngine,
    *,
    jira_transport: httpx.AsyncBaseTransport | None = None,
    github_transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[SubmitJob]:
    """Wire a verified observer to persistent queued Job submission."""
    async with open_github_repository_source(
        github, repositories, job_branch_reader(engine), transport=github_transport
    ) as observer:
        async with open_job_submitter(
            jira,
            repositories,
            engine,
            observer,
            repository_ids={
                item.repository_id: item.github_id for item in github.repository_ids
            },
            transport=jira_transport,
        ) as submitter:
            yield submitter
