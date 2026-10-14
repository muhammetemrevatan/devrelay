import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from threading import Event
from uuid import UUID, uuid4

import pytest

from devrelay.application import base_source_delivery
from devrelay.application.base_source_delivery import ReadAssignedBase
from devrelay.application.contracts.assignment_delivery import (
    AssignmentDelivery,
    AssignmentUnavailable,
    InvalidAssignmentDefinition,
)
from devrelay.application.contracts.assignments import ClaimResult, NoWorkReason
from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    BaseSourceRequest,
    BaseSourceSnapshot,
    git_blob_sha,
)
from devrelay.application.contracts.claims import (
    ClaimRequestIdentity,
    InvalidLeaseProof,
    LeaseProof,
    WorkerClaimRequest,
)
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.ports.base_source import InvalidBaseSourceData
from devrelay.domain.jobs.values import AttemptStatus
from tests.assignment_fixtures import NOW, assigned_result


class Store:
    def __init__(self, result: ClaimResult | None) -> None:
        self.result = result
        self.calls: list[ClaimRequestIdentity] = []

    async def recover(
        self, identity: ClaimRequestIdentity, authorization: WorkerAuthorization
    ) -> ClaimResult | None:
        self.calls.append(identity)
        return self.result


class Source:
    def __init__(self) -> None:
        self.calls: list[BaseSourceRequest] = []
        self.changed_request: BaseSourceRequest | None = None
        self.file_bytes = b"Literal base bytes\x00\xff"
        self.after_read: Callable[[], None] = lambda: None

    async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot:
        self.calls.append(request)
        self.after_read()
        return BaseSourceSnapshot(
            request=self.changed_request or request,
            root_tree_sha="c" * 40,
            files=(
                BaseSourceFile(
                    path="README.md",
                    mode=BaseFileMode.REGULAR,
                    content=self.file_bytes,
                    blob_sha=git_blob_sha(self.file_bytes),
                ),
            ),
        )


def setup() -> tuple[Store, Source, WorkerClaimRequest, WorkerAuthorization, UUID]:
    authorization = WorkerAuthorization(
        worker_id=uuid4(),
        allowed_repository_ids=frozenset({uuid4()}),
        credential_expires_at=NOW + timedelta(hours=1),
    )
    result = assigned_result(authorization)
    assert result.attempt is not None
    result = replace(
        result,
        attempt=result.attempt.acknowledge(
            worker_id=authorization.worker_id,
            generation=1,
            now=NOW + timedelta(seconds=1),
        ),
    )
    assert result.attempt is not None
    return (
        Store(result),
        Source(),
        WorkerClaimRequest(claim_request_id=uuid4(), proof=LeaseProof.generate()),
        authorization,
        result.attempt.id,
    )


def test_base_export_preserves_original_pins_and_fresh_reads_without_mutation() -> None:
    store, source, request, authorization, attempt_id = setup()
    original = store.result
    reader = ReadAssignedBase(store, source, lambda proof: None)

    async def scenario() -> None:
        exported = await reader.execute(attempt_id, request, authorization)
        await reader.require_current(attempt_id, request, authorization, exported)
        assert exported.snapshot.files[0].content == source.file_bytes
        assert exported.reference.attempt_id == attempt_id
        assert exported.claim_request_id == request.claim_request_id
        assert exported.snapshot.request == source.calls[0]
        assert request.proof.reveal() not in repr(exported)

    asyncio.run(scenario())

    assert len(store.calls) == 6 and len(source.calls) == 1
    for identity in store.calls:
        identity.require_replay(request.identify(authorization))
    assert store.result is original


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "no_work",
        "attempt",
        "claimed",
        "testing",
        "expired",
        "scope",
        "worker",
    ],
)
def test_ineligible_receipt_never_reaches_provider(change: str) -> None:
    store, source, request, authorization, attempt_id = setup()
    result = store.result
    assert result is not None and result.attempt is not None
    if change == "missing":
        store.result = None
    elif change == "no_work":
        store.result = ClaimResult(
            recorded_at=NOW, recovered=True, no_work=NoWorkReason.EMPTY
        )
    elif change == "attempt":
        attempt_id = uuid4()
    elif change == "claimed":
        store.result = replace(
            result,
            attempt=replace(
                result.attempt, status=AttemptStatus.CLAIMED, acknowledged_at=None
            ),
        )
    elif change == "testing":
        store.result = replace(
            result, attempt=replace(result.attempt, status=AttemptStatus.TESTING)
        )
    elif change == "expired":
        store.result = replace(result, lease_authority_active=False)
    elif change == "scope":
        authorization = replace(
            authorization, allowed_repository_ids=frozenset({uuid4()})
        )
    else:
        authorization = replace(authorization, worker_id=uuid4())
    reader = ReadAssignedBase(store, source, lambda proof: None)

    with pytest.raises((AssignmentUnavailable, InvalidAssignmentDefinition)):
        asyncio.run(reader.execute(attempt_id, request, authorization))

    assert source.calls == []


def test_credential_reuse_guard_runs_before_storage_and_provider() -> None:
    store, source, request, authorization, attempt_id = setup()

    def guard(proof: LeaseProof) -> None:
        raise InvalidLeaseProof("Independent proof is required.")

    reader = ReadAssignedBase(store, source, guard)

    with pytest.raises(InvalidLeaseProof):
        asyncio.run(reader.execute(attempt_id, request, authorization))

    assert store.calls == [] and source.calls == []


@pytest.mark.parametrize("change", ["repository", "numeric", "slug", "base"])
def test_provider_cannot_change_source_pins(change: str) -> None:
    store, source, request, authorization, attempt_id = setup()
    result = store.result
    assert result is not None and result.stored is not None
    original = BaseSourceRequest(
        repository_id=result.stored.job.repository_id,
        provider_repository_id=123,
        repository_slug="example/devrelay",
        base_revision="a" * 40,
    )
    if change == "repository":
        source.changed_request = replace(original, repository_id=uuid4())
    elif change == "numeric":
        source.changed_request = replace(original, provider_repository_id=456)
    elif change == "slug":
        source.changed_request = replace(original, repository_slug="example/other")
    else:
        source.changed_request = replace(original, base_revision="b" * 40)
    reader = ReadAssignedBase(store, source, lambda proof: None)

    with pytest.raises(InvalidBaseSourceData):
        asyncio.run(reader.execute(attempt_id, request, authorization))

    assert len(source.calls) == 1 and len(store.calls) == 2


@pytest.mark.parametrize("point", ["fetch", "encoding"])
@pytest.mark.parametrize(
    "change", ["authority", "base", "numeric", "slug", "definition", "reference"]
)
def test_fresh_recovery_fences_provider_and_serialization_waits(
    point: str, change: str
) -> None:
    store, source, request, authorization, attempt_id = setup()
    original = store.result
    assert (
        original is not None
        and original.attempt is not None
        and original.stored is not None
    )

    def change_receipt() -> None:
        assert original.attempt is not None and original.stored is not None
        if change == "authority":
            store.result = replace(original, lease_authority_active=False)
        elif change == "reference":
            # An unrelated original Attempt is never a replacement for this route.
            new_id = uuid4()
            store.result = replace(
                original,
                attempt=replace(
                    original.attempt,
                    id=new_id,
                    lease=replace(original.attempt.lease, attempt_id=new_id),
                ),
            )
        else:
            stored = original.stored
            if change == "base":
                stored = replace(
                    stored, job=replace(stored.job, base_revision="b" * 40)
                )
            elif change in ("numeric", "slug"):
                policy = (
                    replace(stored.intent.policy, provider_repository_id=456)
                    if change == "numeric"
                    else replace(stored.intent.policy, slug="example/other")
                )
                stored = replace(stored, intent=replace(stored.intent, policy=policy))
            else:
                snapshot = replace(
                    stored.intent.stored.snapshot, description="Different requirements"
                )
                stored = replace(
                    stored,
                    intent=replace(
                        stored.intent,
                        stored=replace(stored.intent.stored, snapshot=snapshot),
                    ),
                )
            store.result = replace(original, stored=stored)

    if point == "fetch":
        source.after_read = change_receipt
    reader = ReadAssignedBase(store, source, lambda proof: None)

    async def scenario() -> None:
        if point == "fetch":
            with pytest.raises(AssignmentUnavailable):
                await reader.execute(attempt_id, request, authorization)
        else:
            exported = await reader.execute(attempt_id, request, authorization)
            change_receipt()
            with pytest.raises(AssignmentUnavailable):
                await reader.require_current(
                    attempt_id, request, authorization, exported
                )

    asyncio.run(scenario())

    assert len(source.calls) == 1


def test_heartbeat_progress_does_not_repin_original_delivery() -> None:
    store, source, request, authorization, attempt_id = setup()
    original = store.result
    assert original is not None and original.attempt is not None

    def progress() -> None:
        assert original.attempt is not None
        store.result = replace(
            original,
            attempt=replace(original.attempt, updated_at=NOW + timedelta(seconds=2)),
        )

    source.after_read = progress
    reader = ReadAssignedBase(store, source, lambda proof: None)

    exported = asyncio.run(reader.execute(attempt_id, request, authorization))

    assert exported.reference.expires_at == original.attempt.lease.expires_at
    assert len(store.calls) == 4


def test_channel_file_limit_rejects_valid_but_oversized_provider_export() -> None:
    store, source, request, authorization, attempt_id = setup()
    source.file_bytes = b"x" * (2 * 1024**2 + 1)
    reader = ReadAssignedBase(store, source, lambda proof: None)

    with pytest.raises(InvalidBaseSourceData):
        asyncio.run(reader.execute(attempt_id, request, authorization))

    assert len(store.calls) == 2


def test_cancelled_fetch_keeps_original_receipt_and_stops_disclosure() -> None:
    store, _, request, authorization, attempt_id = setup()
    original = store.result

    async def scenario() -> None:
        entered = asyncio.Event()

        class WaitingSource:
            async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot:
                entered.set()
                await asyncio.Future[None]()
                raise AssertionError("Canceled source cannot disclose bytes.")

        reader = ReadAssignedBase(store, WaitingSource(), lambda proof: None)
        task = asyncio.create_task(reader.execute(attempt_id, request, authorization))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert store.result is original and len(store.calls) == 2


@pytest.mark.parametrize("point", ["initial", "fetch", "encoding"])
@pytest.mark.parametrize(
    "change", ["authority", "base", "numeric", "definition", "number"]
)
def test_authority_is_recovered_after_suspended_fingerprint_work(
    point: str, change: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, source, request, authorization, attempt_id = setup()
    original = store.result
    assert original is not None
    entered, release = Event(), Event()
    original_inputs = base_source_delivery._inputs
    calls = 0
    pause_call = {"initial": 1, "fetch": 2, "encoding": 3}[point]

    def paused_inputs(delivery: AssignmentDelivery) -> tuple[BaseSourceRequest, str]:
        nonlocal calls
        calls += 1
        inputs = original_inputs(delivery)
        if calls == pause_call:
            entered.set()
            assert release.wait(5)
        return inputs

    monkeypatch.setattr(base_source_delivery, "_inputs", paused_inputs)
    reader = ReadAssignedBase(store, source, lambda proof: None)

    async def scenario() -> None:
        task: asyncio.Task[object]
        if point == "encoding":
            exported = await reader.execute(attempt_id, request, authorization)
            task = asyncio.create_task(
                reader.require_current(attempt_id, request, authorization, exported)
            )
        else:
            task = asyncio.create_task(
                reader.execute(attempt_id, request, authorization)
            )
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            assert original.stored is not None and original.attempt is not None
            stored = original.stored
            if change == "authority":
                store.result = replace(original, lease_authority_active=False)
            elif change == "number":
                store.result = replace(
                    original, attempt=replace(original.attempt, number=2)
                )
            else:
                if change == "base":
                    stored = replace(
                        stored, job=replace(stored.job, base_revision="b" * 40)
                    )
                elif change == "numeric":
                    stored = replace(
                        stored,
                        intent=replace(
                            stored.intent,
                            policy=replace(
                                stored.intent.policy, provider_repository_id=456
                            ),
                        ),
                    )
                else:
                    stored = replace(
                        stored,
                        intent=replace(
                            stored.intent,
                            stored=replace(
                                stored.intent.stored,
                                snapshot=replace(
                                    stored.intent.stored.snapshot,
                                    description="Changed during CPU work",
                                ),
                            ),
                        ),
                    )
                store.result = replace(original, stored=stored)
        finally:
            release.set()
        with pytest.raises(AssignmentUnavailable):
            await task

    asyncio.run(scenario())

    assert len(source.calls) == (0 if point == "initial" else 1)
