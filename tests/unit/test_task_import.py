import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from types import TracebackType
from typing import Self
from uuid import uuid4

import pytest

from devrelay.application.contracts.tasks import SavedTaskSnapshot, TaskRead
from devrelay.application.ports.task_persistence import TaskPersistenceError
from devrelay.application.ports.task_source import TaskSourceUnavailable
from devrelay.application.tasks import (
    ImportTask,
    RequirementGap,
    RequirementsStatus,
    TaskImportPolicy,
    TaskImportRejected,
)
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot


def task_read() -> TaskRead:
    return TaskRead(
        snapshot=TaskSnapshot(
            id=uuid4(),
            identity=TaskIdentity(
                provider="jira", site_id="site-1", external_id="10005"
            ),
            display_key="DRY-5",
            title="Import a task",
            description="Preserve requirements.",
            acceptance_criteria=("Save once.",),
            captured_at=datetime.now(UTC),
        ),
        scope_key="DRY",
        labels=frozenset({"agent-ready"}),
    )


class Source:
    def __init__(self, observed: TaskRead, events: list[str]) -> None:
        self.observed = observed
        self.events = events

    async def read_by_key(self, display_key: str) -> TaskRead:
        self.events.append("read")
        return self.observed

    async def fetch_snapshot(self, identity: TaskIdentity) -> TaskSnapshot:
        raise AssertionError("Import must use the selected key lookup.")


class Unit:
    def __init__(self, events: list[str], failure: str | None = None) -> None:
        self.events = events
        self.failure = failure

    @property
    def tasks(self) -> Self:
        return self

    async def save_snapshot(self, snapshot: TaskSnapshot) -> SavedTaskSnapshot:
        self.events.append("save")
        if self.failure == "save":
            raise TaskPersistenceError("Storage failed.")
        return SavedTaskSnapshot(
            source_task_id=uuid4(), snapshot=snapshot, created=True
        )

    async def __aenter__(self) -> Self:
        self.events.append("begin")
        return self

    async def commit(self) -> None:
        self.events.append("commit")
        if self.failure == "commit":
            raise TaskPersistenceError("Commit failed.")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.events.append("rollback" if exc_type else "close")


def importer(
    observed: TaskRead, events: list[str], failure: str | None = None
) -> ImportTask:
    def factory() -> Unit:
        events.append("construct")
        return Unit(events, failure)

    return ImportTask(
        Source(observed, events),
        factory,
        TaskImportPolicy(
            provider="jira", site_id="site-1", allowed_scope_keys=frozenset({"DRY"})
        ),
    )


def test_source_read_precedes_transaction_and_success_follows_commit() -> None:
    events: list[str] = []
    service = importer(task_read(), events)

    result = asyncio.run(service.execute("DRY-5"))

    assert events == ["read", "construct", "begin", "save", "commit", "close"]
    assert result.requirements_status is RequirementsStatus.COMPLETE
    assert result.missing_requirements == ()


def test_incomplete_requirements_are_captured_without_job_authorization() -> None:
    observed = task_read()
    observed = replace(
        observed,
        snapshot=replace(observed.snapshot, description="", acceptance_criteria=()),
        labels=frozenset(),
    )
    service = importer(observed, [])

    result = asyncio.run(service.execute("DRY-5"))

    assert result.saved.created
    assert result.requirements_status is RequirementsStatus.INCOMPLETE
    assert result.missing_requirements == tuple(RequirementGap)


@pytest.mark.parametrize("lookup", ["", " ", " DRY-5", "x" * 129])
def test_invalid_lookup_does_not_open_resources(lookup: str) -> None:
    events: list[str] = []
    service = importer(task_read(), events)

    with pytest.raises(TaskImportRejected):
        asyncio.run(service.execute(lookup))

    assert events == []


@pytest.mark.parametrize("scope", ["OTHER", ""])
def test_disallowed_returned_scope_does_not_open_transaction(scope: str) -> None:
    events: list[str] = []
    service = importer(replace(task_read(), scope_key=scope), events)

    with pytest.raises(TaskImportRejected):
        asyncio.run(service.execute("DRY-5"))

    assert events == ["read"]


@pytest.mark.parametrize("failure", ["save", "commit"])
def test_failed_storage_cannot_return_success(failure: str) -> None:
    events: list[str] = []
    service = importer(task_read(), events, failure)

    with pytest.raises(TaskPersistenceError):
        asyncio.run(service.execute("DRY-5"))

    assert events[-1] == "rollback"
    assert ("commit" in events) == (failure == "commit")


@pytest.mark.parametrize(
    "identity",
    [
        TaskIdentity(provider="other", site_id="site-1", external_id="10005"),
        TaskIdentity(provider="jira", site_id="other-site", external_id="10005"),
    ],
)
def test_disallowed_identity_does_not_open_transaction(identity: TaskIdentity) -> None:
    observed = task_read()
    observed = replace(observed, snapshot=replace(observed.snapshot, identity=identity))
    events: list[str] = []
    service = importer(observed, events)

    with pytest.raises(TaskImportRejected):
        asyncio.run(service.execute("DRY-5"))

    assert events == ["read"]


def test_source_failure_does_not_open_transaction() -> None:
    class UnavailableSource(Source):
        async def read_by_key(self, display_key: str) -> TaskRead:
            raise TaskSourceUnavailable("Source is unavailable.")

    events: list[str] = []
    service = ImportTask(
        UnavailableSource(task_read(), events),
        lambda: Unit(events),
        TaskImportPolicy(
            provider="jira", site_id="site-1", allowed_scope_keys=frozenset({"DRY"})
        ),
    )
    with pytest.raises(TaskSourceUnavailable):
        asyncio.run(service.execute("DRY-5"))

    assert events == []
