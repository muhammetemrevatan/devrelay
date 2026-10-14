from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.acknowledgements import AcknowledgeAssignedJob
from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.base_source_delivery import ReadAssignedBase
from devrelay.application.claim_eligibility import PrepareJobClaim
from devrelay.application.claims import ClaimJob
from devrelay.application.contracts.assignments import ClaimLimits
from devrelay.application.contracts.base_source_delivery import BASE_DELIVERY_LIMITS
from devrelay.application.contracts.claims import LeaseProof
from devrelay.application.contracts.heartbeats import HeartbeatLimits
from devrelay.application.contracts.submissions import RepositoryPolicy
from devrelay.application.heartbeats import HeartbeatAssignedJob
from devrelay.bootstrap.github import (
    open_github_base_source,
    open_github_repository_source,
)
from devrelay.bootstrap.jira import open_jira_task_source
from devrelay.bootstrap.jobs import repository_policy
from devrelay.bootstrap.persistence import (
    acknowledgement_store,
    claim_store,
    heartbeat_store,
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
    """Control-only bounded provider observations; no worker execution resources."""
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


def _claim_configuration(
    settings: ControlSettings,
) -> tuple[tuple[RepositoryPolicy, ...], Callable[[LeaseProof], None]]:
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
    return policies, lease_proof_guard(credentials)


def job_acknowledger(
    settings: ControlSettings, engine: AsyncEngine
) -> AcknowledgeAssignedJob:
    """Trusted control use case only; no startup I/O or execution."""
    if settings.worker_claims is None:
        raise ValueError("Acknowledgements require explicitly enabled claims.")
    policies, proof_guard = _claim_configuration(settings)
    return AcknowledgeAssignedJob(
        ReadAssignedJob(claim_store(engine, policies), proof_guard),
        acknowledgement_store(engine, policies),
    )


def job_heartbeater(
    settings: ControlSettings,
    engine: AsyncEngine,
    *,
    limits: HeartbeatLimits | None = None,
) -> HeartbeatAssignedJob:
    """Explicit control renewal composition; no HTTP, scheduling or execution."""
    if settings.worker_claims is None:
        raise ValueError("Heartbeats require explicitly enabled claims.")
    policies, proof_guard = _claim_configuration(settings)
    return HeartbeatAssignedJob(heartbeat_store(engine, policies, limits), proof_guard)


@asynccontextmanager
async def open_job_base_reader(
    settings: ControlSettings,
    engine: AsyncEngine,
    *,
    github_transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[ReadAssignedBase]:
    """Read-only broker resource; the provider credential never leaves control."""
    if settings.worker_base_sources is None or settings.worker_claims is None:
        raise ValueError("Base delivery requires explicitly enabled source and claims.")
    if settings.github is None:
        raise ValueError("Base delivery requires configured GitHub identities.")
    policies, proof_guard = _claim_configuration(settings)
    async with open_github_base_source(
        settings.github,
        settings.repositories,
        limits=BASE_DELIVERY_LIMITS,
        transport=github_transport,
    ) as source:
        yield ReadAssignedBase(claim_store(engine, policies), source, proof_guard)


@asynccontextmanager
async def open_job_claimer(
    settings: ControlSettings,
    engine: AsyncEngine,
    *,
    limits: ClaimLimits | None = None,
    jira_transport: httpx.AsyncBaseTransport | None = None,
    github_transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[ClaimJob]:
    """Control claim composition; no worker polling or execution resources."""
    if settings.jira is None or settings.github is None:
        raise ValueError("Claims require provider policies.")
    policies, proof_guard = _claim_configuration(settings)
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
            proof_guard,
        )
