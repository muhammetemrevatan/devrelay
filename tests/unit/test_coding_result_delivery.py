import asyncio
import threading
from datetime import timedelta
from uuid import UUID

import pytest

import devrelay.application.coding_result_delivery as module
from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.coding_result_delivery import AcceptWorkerCodingResult
from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    AssignmentDelivery,
    AssignmentUnavailable,
)
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingResultAcceptance,
    CodingResultReceipt,
    CodingResultRejected,
    CodingResultStatus,
    CodingResultUnavailable,
)
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.domain.jobs.attempt import LeaseExpired
from tests.unit.test_coding_result_preparation import Context, context, prepare


class Reader(ReadAssignedJob):
    def __init__(self, setup: Context) -> None:
        self.setup = setup
        from devrelay.application.contracts.worker_recovery import (
            AssignmentReference,
            WorkerClaimOutcome,
        )

        issued_at = setup.definition.created_at + timedelta(seconds=1)
        self.delivery = AssignmentDelivery(
            definition=setup.definition,
            lease_authority_active=False,
            outcome=WorkerClaimOutcome(
                worker_id=setup.authorization.worker_id,
                claim_request_id=setup.request.claim.claim_request_id,
                recorded_at=issued_at,
                assignment=AssignmentReference(
                    worker_id=setup.authorization.worker_id,
                    job_id=setup.definition.job_id,
                    repository_id=setup.definition.repository_id,
                    attempt_id=setup.request.intent.execution.acknowledgement.attempt_id,
                    generation=1,
                    issued_at=issued_at,
                    expires_at=issued_at + timedelta(minutes=2),
                ),
            ),
        )
        self.calls: list[tuple[UUID, WorkerClaimRequest, WorkerAuthorization]] = []
        self.error = False
        self.wait: asyncio.Event | None = None

    async def execute(
        self,
        attempt_id: UUID,
        request: WorkerClaimRequest,
        authorization: WorkerAuthorization,
    ) -> AssignmentDelivery:
        self.calls.append((attempt_id, request, authorization))
        if self.error:
            raise AssignmentUnavailable("Unknown original assignment.")
        if self.wait is not None:
            await self.wait.wait()
        return self.delivery


class Store:
    def __init__(self) -> None:
        self.accepted: list[PreparedCodingResult] = []
        self.recovered: list[PreparedCodingResult] = []
        self.status: CodingResultStatus | None = None
        self.wait: asyncio.Event | None = None

    async def accept(
        self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
    ) -> CodingResultAcceptance:
        self.accepted.append(prepared)
        identity, definition = prepared.identity, prepared.definition
        intent = identity.intent
        receipt = CodingResultReceipt(
            result_id=UUID(int=909),
            worker_id=authorization.worker_id,
            request_id=intent.result_request_id,
            claim_request_id=identity.claim.claim_request_id,
            job_id=definition.job_id,
            attempt_id=intent.execution.acknowledgement.attempt_id,
            repository_id=definition.repository_id,
            execution_id=intent.execution.execution_id,
            definition_digest=definition.fingerprint(),
            request_fingerprint=identity.fingerprint,
            candidate_revision=intent.result.final_candidate.revision,
            repair_cycles=len(intent.result.cycles) - 1,
            publishing_workflow_version=4 + 2 * (len(intent.result.cycles) - 1),
            recorded_at=definition.created_at + timedelta(seconds=10),
        )
        self.status = CodingResultStatus(
            receipt=receipt, state=CodingPublicationState.ACCEPTED, operation_version=0
        )
        if self.wait is not None:
            await self.wait.wait()
        return CodingResultAcceptance(receipt=receipt, recovered=False)

    async def recover_result(
        self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
    ) -> CodingResultStatus | None:
        assert authorization.worker_id == prepared.identity.claim.worker_id
        self.recovered.append(prepared)
        return self.status


@pytest.mark.parametrize("repairs", [0, 2, 5])
def test_consumed_original_assignment_and_report_are_prepared_before_atomic_store(
    repairs: int,
) -> None:
    async def scenario() -> None:
        setup, store = context(repairs), Store()
        reader = Reader(setup)
        operation = AcceptWorkerCodingResult(reader, store)
        result = await operation.execute(setup.request, setup.authorization)
        assert reader.calls == [
            (
                setup.request.intent.execution.acknowledgement.attempt_id,
                setup.request.claim,
                setup.authorization,
            )
        ]
        assert len(store.accepted) == 1
        captured = store.accepted[0]
        assert captured.identity.intent is not setup.request.intent
        assert captured.payload_bytes == prepare(setup).payload_bytes
        result.receipt.require_input(captured.identity, job_id=setup.definition.job_id)
        assert result.receipt.repair_cycles == repairs
        assert (
            await operation.recover(setup.request, setup.authorization) == store.status
        )
        assert len(store.accepted) == len(store.recovered) == 1
        await operation.aclose()

    asyncio.run(scenario())


def test_unknown_assignment_never_prepares_or_accepts_new_result() -> None:
    async def scenario() -> None:
        setup, store = context(), Store()
        reader = Reader(setup)
        reader.error = True
        operation = AcceptWorkerCodingResult(reader, store)
        with pytest.raises(AssignmentUnavailable):
            await operation.execute(setup.request, setup.authorization)
        assert not store.accepted

    asyncio.run(scenario())


def test_recovery_of_absent_result_does_not_create_it() -> None:
    async def scenario() -> None:
        setup, store = context(), Store()
        operation = AcceptWorkerCodingResult(Reader(setup), store)
        assert await operation.recover(setup.request, setup.authorization) is None
        assert not store.accepted and len(store.recovered) == 1

    asyncio.run(scenario())


def test_expired_assignment_is_an_application_rejection_not_an_internal_error() -> None:
    class Expired(Store):
        async def accept(
            self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
        ) -> CodingResultAcceptance:
            raise LeaseExpired("Internal lease metadata is not an API error body.")

    async def scenario() -> None:
        setup = context()
        operation = AcceptWorkerCodingResult(Reader(setup), Expired())
        with pytest.raises(CodingResultRejected) as captured:
            await operation.execute(setup.request, setup.authorization)
        assert "Internal lease" not in str(captured.value)

    asyncio.run(scenario())


def test_invalid_full_candidate_never_reaches_result_storage() -> None:
    async def scenario() -> None:
        setup, store = context(), Store()
        object.__setattr__(
            setup.request.intent.result.final_candidate.files[0], "content", b"changed"
        )
        operation = AcceptWorkerCodingResult(Reader(setup), store)
        with pytest.raises(ValueError):
            await operation.execute(setup.request, setup.authorization)
        assert not store.accepted

    asyncio.run(scenario())


def test_cancelled_encoder_remains_admitted_until_actual_thread_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = threading.Event(), threading.Event()
    original = prepare_coding_result

    def blocked(
        request: WorkerCodingResultRequest,
        authorization: WorkerAuthorization,
        definition: AssignedJobDefinition,
    ) -> PreparedCodingResult:
        entered.set()
        assert release.wait(5)
        return original(request, authorization, definition)

    monkeypatch.setattr(module, "prepare_coding_result", blocked)

    async def scenario() -> None:
        setup, store = context(), Store()
        reader = Reader(setup)
        operation = AcceptWorkerCodingResult(reader, store)
        task = asyncio.create_task(
            operation.execute(setup.request, setup.authorization)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(CodingResultUnavailable):
                await operation.execute(setup.request, setup.authorization)
            assert len(reader.calls) == 1 and not store.accepted
            closing = asyncio.create_task(operation.aclose())
            await asyncio.sleep(0)
            assert not closing.done()
            release.set()
            await closing
            with pytest.raises(CodingResultUnavailable):
                await operation.recover(setup.request, setup.authorization)
            assert not store.accepted and not store.recovered
        finally:
            release.set()

    asyncio.run(scenario())


def test_close_while_assignment_read_is_pending_prevents_encoder_and_acceptance() -> (
    None
):
    async def scenario() -> None:
        setup, store = context(), Store()
        reader = Reader(setup)
        reader.wait = asyncio.Event()
        operation = AcceptWorkerCodingResult(reader, store)
        task = asyncio.create_task(
            operation.execute(setup.request, setup.authorization)
        )
        await asyncio.sleep(0)
        assert len(reader.calls) == 1
        await operation.aclose()
        reader.wait.set()
        with pytest.raises(CodingResultUnavailable):
            await task
        assert not store.accepted

    asyncio.run(scenario())


def test_cancel_after_possible_acceptance_is_recovered_without_another_acceptance() -> (
    None
):
    async def scenario() -> None:
        setup, store = context(), Store()
        store.wait = asyncio.Event()
        operation = AcceptWorkerCodingResult(Reader(setup), store)
        task = asyncio.create_task(
            operation.execute(setup.request, setup.authorization)
        )
        while not store.accepted:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(CodingResultUnavailable):
            await operation.recover(setup.request, setup.authorization)
        closing = asyncio.create_task(operation.aclose())
        await asyncio.sleep(0)
        assert not closing.done()
        store.wait.set()
        await closing
        fresh = AcceptWorkerCodingResult(Reader(setup), store)
        assert await fresh.recover(setup.request, setup.authorization) == store.status
        assert len(store.accepted) == len(store.recovered) == 1

    asyncio.run(scenario())
