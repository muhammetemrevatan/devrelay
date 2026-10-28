"""The explicit consumer delegates durable publication ownership, never execution."""

import asyncio
from dataclasses import replace
from uuid import UUID

import pytest

from devrelay.application.accepted_coding_results import DispatchAcceptedCodingResult
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultRejected,
    CodingResultUnavailable,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from tests.unit.test_plan_coding_publication import context
from tests.unit.test_publish_coding_result import Clock, Provider, Store, consumer


class Reader:
    def __init__(self, prepared: PreparedCodingResult | None) -> None:
        self.prepared = prepared
        self.calls: list[tuple[UUID, WorkerAuthorization]] = []

    async def read(
        self, result_id: UUID, authorization: WorkerAuthorization
    ) -> PreparedCodingResult | None:
        self.calls.append((result_id, authorization))
        return self.prepared


@pytest.mark.parametrize("repairs", [0, 5])
def test_explicit_known_accepted_uuid_publishes_and_terminal_replay_does_not_write(
    repairs: int,
) -> None:
    async def scenario() -> None:
        value = context(repairs=repairs)
        store, reader = Store(value), Reader(value.prepared)
        provider = Provider(value, store.events)
        publisher = consumer(value, store, provider, Clock())
        service = DispatchAcceptedCodingResult(reader, publisher)

        terminal = await service.execute(value.receipt.result_id, value.authorization)
        count = len(provider.writes)
        replay = await service.execute(value.receipt.result_id, value.authorization)

        assert count > 0 and replay == terminal
        assert len(provider.writes) == count
        assert reader.calls == [(value.receipt.result_id, value.authorization)] * 2
        await publisher.aclose()

    asyncio.run(scenario())


def test_missing_accepted_uuid_never_reaches_provider_or_claims() -> None:
    async def scenario() -> None:
        value = context()
        store, reader = Store(value), Reader(None)
        provider = Provider(value, store.events)
        publisher = consumer(value, store, provider, Clock())
        service = DispatchAcceptedCodingResult(reader, publisher)

        with pytest.raises(CodingResultRejected):
            await service.execute(value.receipt.result_id, value.authorization)
        assert store.events == [] and provider.writes == []
        await publisher.aclose()

    asyncio.run(scenario())


def test_historical_pending_owner_cannot_be_restarted_by_explicit_dispatch() -> None:
    async def scenario() -> None:
        value = context()
        store, reader = Store(value), Reader(value.prepared)
        provider = Provider(value, store.events)
        publisher = consumer(value, store, provider, Clock())
        await store.begin_publication(
            value.prepared, value.authorization, invocation_id=UUID(int=42)
        )
        service = DispatchAcceptedCodingResult(reader, publisher)

        with pytest.raises(CodingResultUnavailable):
            await service.execute(value.receipt.result_id, value.authorization)
        assert provider.writes == []
        await publisher.aclose()

    asyncio.run(scenario())


def test_reader_error_keeps_publisher_unused() -> None:
    class RejectedReader(Reader):
        async def read(
            self, result_id: UUID, authorization: WorkerAuthorization
        ) -> PreparedCodingResult | None:
            raise CodingResultUnavailable("Closed.")

    async def scenario() -> None:
        value = context()
        store = Store(value)
        provider = Provider(value, store.events)
        publisher = consumer(value, store, provider, Clock())
        service = DispatchAcceptedCodingResult(
            RejectedReader(value.prepared), publisher
        )

        with pytest.raises(CodingResultUnavailable):
            await service.execute(value.receipt.result_id, replace(value.authorization))
        assert store.events == [] and provider.writes == []
        await publisher.aclose()

    asyncio.run(scenario())


def test_reconcile_unknown_accepted_uuid_has_no_provider_effect() -> None:
    async def scenario() -> None:
        value = context()
        store, reader = Store(value), Reader(None)
        provider = Provider(value, store.events)
        publisher = consumer(value, store, provider, Clock())
        service = DispatchAcceptedCodingResult(reader, publisher)

        result = await service.reconcile(value.receipt.result_id, value.authorization)

        assert result is None
        assert store.events == [] and provider.writes == []
        await publisher.aclose()

    asyncio.run(scenario())
