import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from devrelay.application.accepted_coding_results import AcceptedCodingResultReader
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultStatus,
    TerminalCodingResult,
)
from devrelay.application.contracts.submissions import RepositoryPolicy
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.coding_result_persistence import CodingPublicationStore
from devrelay.application.publish_coding_result import PublishCodingResult
from devrelay.bootstrap import accepted_coding_results as bootstrap
from devrelay.config.control import ControlSettings
from tests.unit.test_coding_publication_configuration import enabled

NOW = datetime(2026, 10, 10, tzinfo=UTC)


def test_authorization_uses_current_enrollment_and_shared_publication_scope(
    worker_configuration: ControlSettings,
) -> None:
    settings = enabled(worker_configuration)
    registration = settings.workers[0]

    authorization = bootstrap.publication_authorization(
        settings, registration.worker_id, now=NOW
    )

    assert authorization == WorkerAuthorization(
        worker_id=registration.worker_id,
        allowed_repository_ids=registration.allowed_repository_ids,
        credential_expires_at=registration.expires_at,
    )
    assert registration.auth_token.get_secret_value() not in repr(authorization)


@pytest.mark.parametrize(
    "change",
    ["unknown", "disabled", "expired", "disjoint", "disabled_repository"],
)
def test_authorization_rejects_inactive_or_disjoint_enrollment(
    worker_configuration: ControlSettings, change: str
) -> None:
    settings = enabled(worker_configuration)
    registration = settings.workers[0]
    worker_id = registration.worker_id
    if change == "unknown":
        worker_id = uuid4()
    elif change == "disabled":
        registration = registration.model_copy(update={"enabled": False})
    elif change == "expired":
        registration = registration.model_copy(update={"expires_at": NOW})
    elif change == "disjoint":
        registration = registration.model_copy(
            update={"allowed_repository_ids": frozenset({uuid4()})}
        )
    else:
        settings = settings.model_copy(
            update={
                "repositories": (
                    settings.repositories[0].model_copy(update={"enabled": False}),
                )
            }
        )
    settings = settings.model_copy(update={"workers": (registration,)})

    with pytest.raises(ValueError):
        bootstrap.publication_authorization(settings, worker_id, now=NOW)


@pytest.mark.parametrize(
    "missing",
    ["database", "worker_claims", "worker_coding_results", "coding_publication"],
)
def test_disabled_configuration_fails_before_opening_resources(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    missing: str,
) -> None:
    settings = enabled(worker_configuration).model_copy(update={missing: None})

    def unexpected(configuration: object) -> None:
        raise AssertionError("Disabled publication must not open resources.")

    monkeypatch.setattr(bootstrap, "open_database_engine", unexpected)

    with pytest.raises(ValueError):
        asyncio.run(
            bootstrap.publish_accepted_result(
                settings, uuid4(), settings.workers[0].worker_id, now=lambda: NOW
            )
        )


def test_authorization_requires_an_aware_clock(
    worker_configuration: ControlSettings,
) -> None:
    settings = enabled(worker_configuration)

    with pytest.raises(ValueError):
        bootstrap.publication_authorization(
            settings, settings.workers[0].worker_id, now=NOW.replace(tzinfo=None)
        )


def test_authorization_narrows_enrollment_to_the_separate_publication_scope(
    worker_configuration: ControlSettings,
) -> None:
    settings = enabled(worker_configuration)
    registration = settings.workers[0]
    other = uuid4()
    settings = settings.model_copy(
        update={
            "workers": (
                registration.model_copy(
                    update={
                        "allowed_repository_ids": registration.allowed_repository_ids
                        | {other}
                    }
                ),
            ),
            "repositories": settings.repositories
            + (settings.repositories[0].model_copy(update={"repository_id": other}),),
        }
    )

    authorization = bootstrap.publication_authorization(
        settings, registration.worker_id, now=NOW
    )

    assert authorization.allowed_repository_ids == registration.allowed_repository_ids


@pytest.mark.parametrize("mode", ["publish", "reconcile"])
@pytest.mark.parametrize("outcome", ["success", "failure", "cancelled", "expired"])
def test_explicit_dispatch_composes_one_engine_and_closes_owned_resources(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    mode: str,
) -> None:
    settings = enabled(worker_configuration)
    registration = settings.workers[0]
    result_id = uuid4()
    engine = cast(AsyncEngine, object())
    store = cast(CodingPublicationStore, object())
    publisher = cast(PublishCodingResult, object())
    reader = cast(AcceptedCodingResultReader, object())
    terminal = cast(TerminalCodingResult, object())
    status = cast(CodingResultStatus, object())
    events: list[str] = []
    observed: list[tuple[UUID, WorkerAuthorization]] = []
    methods: list[str] = []
    instants = iter(
        [
            NOW,
            registration.expires_at
            if outcome == "expired"
            else NOW + timedelta(seconds=1),
        ]
    )

    @asynccontextmanager
    async def database(configuration: object) -> AsyncIterator[AsyncEngine]:
        assert configuration is settings.database
        events.append("database open")
        try:
            yield engine
        finally:
            events.append("database close")

    @asynccontextmanager
    async def publication(
        configuration: ControlSettings, received: AsyncEngine
    ) -> AsyncIterator[PublishCodingResult]:
        assert configuration is settings and received is engine
        events.append("publisher open")
        try:
            yield publisher
        finally:
            events.append("publisher close")

    def storage(
        received: AsyncEngine, policies: tuple[RepositoryPolicy, ...]
    ) -> CodingPublicationStore:
        assert received is engine
        assert frozenset(item.repository_id for item in policies) == (
            registration.allowed_repository_ids
        )
        return store

    def accepted_reader(
        sessions: object,
        policies: tuple[RepositoryPolicy, ...],
        publication_store: CodingPublicationStore,
    ) -> AcceptedCodingResultReader:
        assert publication_store is store
        assert frozenset(item.repository_id for item in policies) == (
            registration.allowed_repository_ids
        )
        return reader

    class Dispatcher:
        def __init__(
            self,
            received_reader: AcceptedCodingResultReader,
            received: PublishCodingResult,
        ) -> None:
            assert received_reader is reader and received is publisher

        async def execute(
            self, received_id: UUID, authorization: WorkerAuthorization
        ) -> TerminalCodingResult:
            methods.append("publish")
            observed.append((received_id, authorization))
            if outcome == "failure":
                raise RuntimeError("test-only failure")
            if outcome == "cancelled":
                raise asyncio.CancelledError
            return terminal

        async def reconcile(
            self, received_id: UUID, authorization: WorkerAuthorization
        ) -> CodingResultStatus | None:
            methods.append("reconcile")
            observed.append((received_id, authorization))
            if outcome == "failure":
                raise RuntimeError("test-only failure")
            if outcome == "cancelled":
                raise asyncio.CancelledError
            return status

    monkeypatch.setattr(bootstrap, "open_database_engine", database)
    monkeypatch.setattr(bootstrap, "open_coding_publisher", publication)
    monkeypatch.setattr(bootstrap, "coding_result_store", storage)
    monkeypatch.setattr(
        bootstrap, "SqlAlchemyAcceptedCodingResultReader", accepted_reader
    )
    monkeypatch.setattr(bootstrap, "DispatchAcceptedCodingResult", Dispatcher)
    operation = (
        bootstrap.publish_accepted_result(
            settings, result_id, registration.worker_id, now=lambda: next(instants)
        )
        if mode == "publish"
        else bootstrap.reconcile_accepted_result(
            settings, result_id, registration.worker_id, now=lambda: next(instants)
        )
    )

    if outcome == "success":
        result = asyncio.run(operation)
    else:
        error = {
            "failure": RuntimeError,
            "cancelled": asyncio.CancelledError,
            "expired": ValueError,
        }[outcome]
        with pytest.raises(error):
            asyncio.run(operation)
        result = None

    expected = terminal if mode == "publish" else status
    assert result is (expected if outcome == "success" else None)
    assert events == [
        "database open",
        "publisher open",
        "publisher close",
        "database close",
    ]
    if outcome == "expired":
        assert observed == []
        assert methods == []
    else:
        assert methods == [mode]
        assert observed == [
            (
                result_id,
                WorkerAuthorization(
                    worker_id=registration.worker_id,
                    allowed_repository_ids=registration.allowed_repository_ids,
                    credential_expires_at=registration.expires_at,
                ),
            )
        ]
