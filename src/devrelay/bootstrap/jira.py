from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from devrelay.config.jira import JiraSettings, JiraTokenMode
from devrelay.infrastructure.jira.task_source import JiraTaskSource


@asynccontextmanager
async def open_jira_task_source(
    settings: JiraSettings, *, transport: httpx.AsyncBaseTransport | None = None
) -> AsyncIterator[JiraTaskSource]:
    base_url = str(settings.site_url)
    if settings.token_mode is JiraTokenMode.SCOPED:
        base_url = f"https://api.atlassian.com/ex/jira/{settings.cloud_id}/"
    async with httpx.AsyncClient(
        base_url=base_url,
        auth=httpx.BasicAuth(settings.email, settings.api_token.get_secret_value()),
        timeout=httpx.Timeout(settings.request_timeout_seconds),
        limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
        follow_redirects=False,
        trust_env=False,
        transport=transport,
    ) as client:
        yield JiraTaskSource(
            client,
            site_id=str(settings.cloud_id),
            allowed_project_keys=settings.allowed_project_keys,
            acceptance_criteria_field=settings.acceptance_criteria_field,
            request_timeout_seconds=settings.request_timeout_seconds,
            max_response_bytes=settings.max_response_bytes,
        )
