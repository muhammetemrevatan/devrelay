import asyncio
import re
from datetime import UTC, datetime
from uuid import uuid4

import httpx
from pydantic import JsonValue, TypeAdapter, ValidationError

from devrelay.application.contracts.tasks import TaskRead
from devrelay.application.ports.task_source import (
    InvalidTaskSourceData,
    TaskNotFound,
    TaskSourceAccessDenied,
    TaskSourceScopeDenied,
    TaskSourceUnavailable,
)
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot
from devrelay.infrastructure.jira.adf import (
    custom_field_criteria,
    description_criteria,
    render_document,
)
from devrelay.infrastructure.jira.models import JiraIssue

_JSON = TypeAdapter(dict[str, JsonValue])


class JiraTaskSource:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        site_id: str,
        allowed_project_keys: frozenset[str],
        acceptance_criteria_field: str | None = None,
        request_timeout_seconds: float = 15,
        max_response_bytes: int = 1_048_576,
    ) -> None:
        self._client = client
        self._site_id = site_id
        self._projects = allowed_project_keys
        self._criteria_field = acceptance_criteria_field
        self._timeout = request_timeout_seconds
        self._max_bytes = max_response_bytes

    async def read_by_key(self, display_key: str) -> TaskRead:
        if re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}-[0-9]{1,32}", display_key) is None:
            raise InvalidTaskSourceData("Task lookup requires a valid display key.")
        if display_key.rsplit("-", 1)[0] not in self._projects:
            raise TaskSourceScopeDenied("Task is outside the configured Jira scope.")
        return await self._read(display_key)

    async def fetch_snapshot(self, identity: TaskIdentity) -> TaskSnapshot:
        return (await self.read_by_identity(identity)).snapshot

    async def read_by_identity(self, identity: TaskIdentity) -> TaskRead:
        if identity.provider != "jira" or identity.site_id != self._site_id:
            raise TaskSourceScopeDenied("Task is outside the configured Jira site.")
        if re.fullmatch(r"[0-9]{1,256}", identity.external_id) is None:
            raise InvalidTaskSourceData(
                "Jira task identity requires an opaque numeric ID."
            )
        result = await self._read(identity.external_id)
        if result.snapshot.identity != identity:
            raise InvalidTaskSourceData("Jira returned a different task identity.")
        return result

    async def _read(self, issue: str) -> TaskRead:
        fields = ["summary", "description", "updated", "project", "labels"]
        if self._criteria_field is not None:
            fields.append(self._criteria_field)
        try:
            async with asyncio.timeout(self._timeout):
                async with self._client.stream(
                    "GET",
                    f"rest/api/3/issue/{issue}",
                    params={"fields": ",".join(fields), "updateHistory": "false"},
                    headers={
                        "Accept": "application/json",
                        "Accept-Encoding": "identity",
                    },
                    follow_redirects=False,
                    timeout=self._timeout,
                ) as response:
                    self._check_status(response.status_code)
                    if response.headers.get("content-type", "").split(";")[
                        0
                    ].strip().lower() != ("application/json"):
                        raise InvalidTaskSourceData("Jira response must contain JSON.")
                    if response.headers.get(
                        "content-encoding", "identity"
                    ).lower() not in ("", "identity"):
                        raise InvalidTaskSourceData(
                            "Jira response must use identity encoding."
                        )
                    body = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=4_096):
                        if len(body) + len(chunk) > self._max_bytes:
                            raise InvalidTaskSourceData(
                                "Jira response exceeds byte limits."
                            )
                        body.extend(chunk)
        except TimeoutError, httpx.RequestError:
            raise TaskSourceUnavailable("Jira task read failed or timed out.") from None
        try:
            payload = _JSON.validate_json(body)
            parsed = JiraIssue.model_validate(payload)
            return self._map(parsed, payload)
        except ValidationError, ValueError, TypeError, RecursionError:
            raise InvalidTaskSourceData("Jira task data failed validation.") from None

    @staticmethod
    def _check_status(status: int) -> None:
        if status in (401, 403):
            raise TaskSourceAccessDenied("Jira denied task access.")
        if status == 404:
            raise TaskNotFound("Jira task was not found or is not visible.")
        if status in (408, 429) or 500 <= status <= 599:
            raise TaskSourceUnavailable("Jira is temporarily unavailable.")
        if status != 200:
            raise InvalidTaskSourceData("Jira returned an unsupported response status.")

    def _map(self, issue: JiraIssue, payload: dict[str, JsonValue]) -> TaskRead:
        if issue.fields.project.key not in self._projects:
            raise TaskSourceScopeDenied("Task is outside the configured Jira scope.")
        if issue.key.rsplit("-", 1)[0] != issue.fields.project.key:
            raise InvalidTaskSourceData("Jira returned inconsistent project metadata.")
        description = render_document(issue.fields.description)
        if self._criteria_field is None:
            criteria = description_criteria(issue.fields.description)
        else:
            raw_fields = payload.get("fields")
            if (
                not isinstance(raw_fields, dict)
                or self._criteria_field not in raw_fields
            ):
                raise InvalidTaskSourceData(
                    "Configured Jira criteria field is missing."
                )
            criteria = custom_field_criteria(raw_fields[self._criteria_field])
        snapshot = TaskSnapshot(
            id=uuid4(),
            identity=TaskIdentity(
                provider="jira", site_id=self._site_id, external_id=issue.id
            ),
            display_key=issue.key,
            title=issue.fields.summary,
            description=description,
            acceptance_criteria=criteria,
            captured_at=datetime.now(UTC),
            source_updated_at=(
                datetime.fromisoformat(issue.fields.updated)
                if issue.fields.updated is not None
                else None
            ),
        )
        return TaskRead(
            snapshot=snapshot,
            scope_key=issue.fields.project.key,
            labels=frozenset(issue.fields.labels),
        )
