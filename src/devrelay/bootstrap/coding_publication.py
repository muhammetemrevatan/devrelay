import asyncio
import sys
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from math import isfinite

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.contracts.coding_publications import PublicationPolicy
from devrelay.application.publish_coding_result import PublishCodingResult
from devrelay.bootstrap.claims import _claim_configuration
from devrelay.bootstrap.github import _repository_bindings
from devrelay.bootstrap.persistence import coding_result_store
from devrelay.config.control import ControlSettings
from devrelay.infrastructure.github.coding_publication import (
    HttpGitHubCodingPublicationProvider,
)


def _publication_clock() -> Callable[[], float]:
    if sys.platform == "win32":
        from devrelay.infrastructure.worker_state.clock import WindowsLeaseClock

        return WindowsLeaseClock()
    clock_id = getattr(time, "CLOCK_BOOTTIME", None)
    read = getattr(time, "clock_gettime", None)
    if sys.platform != "linux" or type(clock_id) is not int or not callable(read):
        raise ValueError("Publication requires a suspend-inclusive monotonic clock.")
    previous = -1.0

    def current() -> float:
        nonlocal previous
        value = read(clock_id)
        if (
            type(value) not in (int, float)
            or not isfinite(value)
            or not previous <= value <= 2**53 - 1
        ):
            raise ValueError("Publication monotonic clock is unavailable.")
        previous = value
        return float(value)

    current()
    return current


@asynccontextmanager
async def open_coding_publisher(
    settings: ControlSettings,
    engine: AsyncEngine,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    clock: Callable[[], float] | None = None,
) -> AsyncIterator[PublishCodingResult]:
    """Explicit broker consumer; opening it performs no provider request."""
    publication = settings.coding_publication
    if (
        publication is None
        or settings.github is None
        or settings.worker_coding_results is None
    ):
        raise ValueError(
            "Publication requires explicitly enabled broker configuration."
        )
    policy = PublicationPolicy(
        **publication.model_dump(exclude={"api_token", "allowed_repository_ids"})
    )
    policies, _ = _claim_configuration(settings)
    allowed = publication.allowed_repository_ids
    policies = tuple(item for item in policies if item.repository_id in allowed)
    bindings = tuple(
        item
        for item in _repository_bindings(settings.github, settings.repositories)
        if item.repository_id in allowed
    )
    monotonic_clock = (
        clock if clock is not None else await asyncio.to_thread(_publication_clock)
    )
    limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
    async with httpx.AsyncClient(
        base_url="https://api.github.com/",
        headers={
            "Authorization": f"Bearer {publication.api_token.get_secret_value()}",
            "Accept": "application/vnd.github+json",
            "Accept-Encoding": "identity",
            "X-GitHub-Api-Version": "2026-03-10",
        },
        timeout=httpx.Timeout(policy.request_timeout_seconds),
        limits=limits,
        follow_redirects=False,
        trust_env=False,
        transport=transport
        if transport is not None
        else httpx.AsyncHTTPTransport(retries=0, limits=limits, trust_env=False),
    ) as client:
        provider = HttpGitHubCodingPublicationProvider(
            client, bindings=bindings, policy=policy, monotonic=monotonic_clock
        )
        service = PublishCodingResult(
            coding_result_store(engine, policies),
            provider,
            allowed_repository_ids=allowed,
            policy=policy,
            monotonic=monotonic_clock,
        )
        try:
            yield service
        finally:
            try:
                await service.aclose()
            finally:
                await provider.aclose()
