from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.tasks import ImportTask, TaskImportPolicy
from devrelay.bootstrap.jira import open_jira_task_source
from devrelay.bootstrap.persistence import task_unit_of_work_factory
from devrelay.config.jira import JiraSettings


@asynccontextmanager
async def open_task_importer(
    settings: JiraSettings,
    engine: AsyncEngine,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[ImportTask]:
    async with open_jira_task_source(settings, transport=transport) as source:
        yield ImportTask(
            source,
            task_unit_of_work_factory(engine),
            TaskImportPolicy(
                provider="jira",
                site_id=str(settings.cloud_id),
                allowed_scope_keys=settings.allowed_project_keys,
            ),
        )
