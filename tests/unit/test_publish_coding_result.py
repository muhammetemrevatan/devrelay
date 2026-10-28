"""Outbox/provider ordering and cancellation fixtures; no network/database I/O."""

import asyncio
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from devrelay.application.contracts.coding_publications import (
    ProviderPublication,
    PublicationHistory,
    PublicationObservation,
    PublicationPlan,
    PublicationPolicy,
    PublicationWrite,
    PublicationWriteKind,
)
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingPublicationStep,
    CodingResultAcceptance,
    CodingResultStatus,
    CodingResultUnavailable,
    CodingWritePermit,
    TerminalCodingResult,
    VerifiedCodingPublication,
)
from devrelay.application.contracts.eligibility import WorkerAuthorityExpired
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.publish_coding_result import PublishCodingResult
from devrelay.domain.jobs.values import PullRequestReference
from tests.unit.test_plan_coding_publication import Context, context, plan


class Clock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


class Store:
    def __init__(self, value: Context) -> None:
        self.value = value
        self.events: list[str] = []
        self.history: PublicationHistory | None = PublicationHistory(
            status=CodingResultStatus(
                receipt=value.receipt,
                state=CodingPublicationState.ACCEPTED,
                operation_version=0,
            ),
            invocation_id=None,
        )
        self.hook: Callable[[str], None] = lambda _: None
        self.created = True
        self.lease_seconds = 60.0
        self.fail_observe = False
        self.observations: list[str] = []

    async def accept(
        self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
    ) -> CodingResultAcceptance:
        raise AssertionError("Publisher cannot accept a new worker report.")

    async def recover_result(
        self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
    ) -> CodingResultStatus | None:
        raise AssertionError("Publisher reads durable owner history.")

    async def read_publication(
        self, prepared: PreparedCodingResult, authorization: WorkerAuthorization
    ) -> PublicationHistory | None:
        self.events.append("read")
        self.hook("read")
        await asyncio.sleep(0)
        return self.history

    async def begin_publication(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        invocation_id: UUID,
    ) -> CodingResultStatus:
        self.events.append("begin")
        assert (
            self.history is not None
            and self.history.status.state is CodingPublicationState.ACCEPTED
        )
        status = replace(
            self.history.status,
            state=CodingPublicationState.PUBLISHING,
            operation_version=1,
        )
        self.history = PublicationHistory(
            status=status,
            invocation_id=invocation_id,
            last_event_kind="publishing_started",
        )
        self.hook("begin")
        return status

    async def plan_write(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
    ) -> CodingWritePermit:
        self.events.append("plan:" + step.step_kind)
        assert (
            self.history is not None
            and self.history.invocation_id == step.invocation_id
        )
        assert self.history.status.operation_version == step.expected_operation_version
        status = replace(
            self.history.status, operation_version=step.expected_operation_version + 1
        )
        self.history = PublicationHistory(
            status=status,
            invocation_id=step.invocation_id,
            last_step=step,
            last_event_kind="write_planned",
        )
        self.hook("planned")
        return CodingWritePermit(
            step=step,
            operation_version=status.operation_version,
            created=self.created,
            authorized_at=self.value.recorded_at if self.created else None,
            authorized_until=self.value.recorded_at
            + timedelta(seconds=self.lease_seconds)
            if self.created
            else None,
        )

    async def observe_write(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
        observation_digest: str,
    ) -> CodingResultStatus:
        self.events.append("observed:" + step.step_kind)
        if self.fail_observe:
            raise RuntimeError("storage detail must stay private")
        assert self.history is not None and self.history.last_step == step
        self.observations.append(observation_digest)
        status = replace(
            self.history.status,
            operation_version=self.history.status.operation_version + 1,
        )
        self.history = replace(
            self.history,
            status=status,
            last_observation_digest=observation_digest,
            last_event_kind="write_observed",
        )
        self.hook("observed")
        return status

    async def mark_uncertain(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        step: CodingPublicationStep,
    ) -> CodingResultStatus:
        self.events.append("uncertain")
        assert self.history is not None and self.history.last_step == step
        if self.history.last_event_kind == "write_observed":
            raise CodingResultUnavailable("Already observed.")
        status = replace(
            self.history.status,
            state=CodingPublicationState.UNCERTAIN,
            operation_version=self.history.status.operation_version + 1,
        )
        self.history = replace(self.history, status=status, last_event_kind="uncertain")
        return status

    async def finalize_publication(
        self,
        prepared: PreparedCodingResult,
        authorization: WorkerAuthorization,
        *,
        publication: VerifiedCodingPublication,
    ) -> TerminalCodingResult:
        self.events.append("finalize")
        assert (
            self.history is not None
            and self.history.status.state is CodingPublicationState.PUBLISHING
        )
        assert publication.invocation_id == self.history.invocation_id
        assert (
            publication.expected_operation_version
            == self.history.status.operation_version
        )
        terminal = TerminalCodingResult(
            receipt=self.value.receipt,
            reference=publication.reference,
            finalized_at=self.value.recorded_at + timedelta(seconds=20),
            workflow_version=self.value.receipt.publishing_workflow_version + 1,
            release_ordinal=1,
        )
        self.history = replace(
            self.history,
            status=CodingResultStatus(
                receipt=terminal.receipt,
                state=CodingPublicationState.FINALIZED,
                operation_version=self.history.status.operation_version + 1,
                terminal=terminal,
            ),
            last_event_kind="finalized",
            last_step=None,
            last_observation_digest=None,
        )
        return terminal


class Provider:
    def __init__(self, value: Context, events: list[str]) -> None:
        self.value, self.events = value, events
        self.writes: list[PublicationWrite] = []
        self.observed: list[PublicationWrite] = []
        self.before = asyncio.Event()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.block_before = self.block_after = False
        self.fail_write = self.fail_verify = self.wrong_verified = False
        self.skip_callback = self.double_callback = False
        self.not_found = False
        self.wrong_observation = False
        self.hook: Callable[[str], None] = lambda _: None

    def observation(self, write: PublicationWrite) -> PublicationObservation:
        return PublicationObservation(
            write_id=write.write_id,
            input_digest="f" * 64 if self.wrong_observation else write.input_digest,
            observation_digest="a" * 64,
            pull_request_number=7
            if write.kind is PublicationWriteKind.PULL_REQUEST
            else None,
        )

    async def write(
        self,
        plan: PublicationPlan,
        write: PublicationWrite,
        *,
        before_write: Callable[[], None],
    ) -> PublicationObservation:
        self.before.set()
        if self.block_before:
            await self.release.wait()
        if not self.skip_callback:
            before_write()
            if self.double_callback:
                before_write()
        self.events.append("post:" + write.kind.value)
        self.writes.append(write)
        self.started.set()
        if self.block_after:
            await self.release.wait()
        if self.fail_write:
            raise RuntimeError("provider credential must never be echoed")
        self.hook("response")
        return self.observation(write)

    async def observe(
        self, plan: PublicationPlan, write: PublicationWrite
    ) -> PublicationObservation | None:
        self.events.append("get:" + write.kind.value)
        self.observed.append(write)
        return None if self.not_found else self.observation(write)

    async def verify(
        self, plan: PublicationPlan, *, pull_request_number: int
    ) -> ProviderPublication:
        self.events.append("verify")
        if self.fail_verify:
            raise RuntimeError("provider private verification detail")
        return ProviderPublication(
            reference=PullRequestReference(
                repository_id=plan.repository_id,
                number=pull_request_number,
                source_branch=plan.source_branch,
                target_branch=plan.target_branch,
                head_revision="f" * 40 if self.wrong_verified else plan.head_revision,
            ),
            provider_repository_id=plan.provider_repository_id,
            observed_at=self.value.recorded_at + timedelta(seconds=10),
        )


def consumer(
    value: Context,
    store: Store,
    provider: Provider,
    clock: Clock,
    *,
    policy: PublicationPolicy | None = None,
) -> PublishCodingResult:
    return PublishCodingResult(
        store,
        provider,
        allowed_repository_ids=frozenset({value.prepared.definition.repository_id}),
        policy=policy or PublicationPolicy(),
        monotonic=clock,
        now=lambda: value.recorded_at + timedelta(seconds=10),
    )


@pytest.mark.parametrize("repairs", [0, 2, 5])
def test_complete_consumer_plans_each_write_before_post_and_finalizes_only_verified_pr(
    repairs: int,
) -> None:
    async def scenario() -> None:
        value = context(repairs=repairs)
        store, clock = Store(value), Clock()
        provider = Provider(value, store.events)
        service = consumer(value, store, provider, clock)
        terminal = await service.execute(value.prepared, value.authorization)
        assert terminal.reference.head_revision == value.receipt.candidate_revision
        assert (
            store.history is not None
            and store.history.status.state is CodingPublicationState.FINALIZED
        )
        for write in provider.writes:
            assert (
                store.events.index("plan:" + write.kind.value)
                < store.events.index("post:" + write.kind.value)
                < store.events.index("observed:" + write.kind.value)
            )
        assert store.events[-2:] == ["verify", "finalize"]
        previous = len(provider.writes)
        assert await service.execute(value.prepared, value.authorization) == terminal
        assert len(provider.writes) == previous
        await service.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize("budget", ["writes", "requests"])
def test_whole_plan_budget_failure_precedes_begin_and_any_provider_call(
    budget: str,
) -> None:
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        provider = Provider(value, store.events)
        policy = (
            PublicationPolicy(max_writes=3)
            if budget == "writes"
            else PublicationPolicy(max_requests=16)
        )
        with pytest.raises(ValueError):
            await consumer(value, store, provider, clock, policy=policy).execute(
                value.prepared, value.authorization
            )
        assert store.events == ["read"] and not provider.writes

    asyncio.run(scenario())


def test_configured_broker_repository_scope_fences_before_storage_or_provider() -> None:
    async def scenario() -> None:
        value = context()
        store = Store(value)
        provider = Provider(value, store.events)
        service = PublishCodingResult(
            store,
            provider,
            allowed_repository_ids=frozenset({uuid4()}),
            monotonic=Clock(),
        )
        with pytest.raises(ValueError):
            await service.execute(value.prepared, value.authorization)
        assert not store.events and not provider.writes

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "fault",
    [
        "recovered",
        "deadline",
        "missing_callback",
        "double_callback",
        "provider",
        "observation",
        "storage",
    ],
)
def test_no_failed_or_recovered_permit_can_produce_another_write(fault: str) -> None:
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        provider = Provider(value, store.events)
        if fault == "recovered":
            store.created = False
        elif fault == "deadline":
            store.lease_seconds = 1
            store.hook = lambda event: (
                setattr(clock, "value", 102.0) if event == "planned" else None
            )
        elif fault == "missing_callback":
            provider.skip_callback = True
        elif fault == "double_callback":
            provider.double_callback = True
        elif fault == "provider":
            provider.fail_write = True
        elif fault == "observation":
            provider.wrong_observation = True
        else:
            store.fail_observe = True
        with pytest.raises(CodingResultUnavailable) as error:
            await consumer(value, store, provider, clock).execute(
                value.prepared, value.authorization
            )
        assert "credential" not in str(error.value) and "detail" not in str(error.value)
        assert "finalize" not in store.events and len(provider.writes) <= 1
        if fault in ("recovered", "deadline", "double_callback"):
            assert not provider.writes
        assert store.history is not None
        if fault != "recovered":
            assert store.history.status.state is CodingPublicationState.UNCERTAIN

    asyncio.run(scenario())


@pytest.mark.parametrize("fault", ["head", "provider"])
def test_verification_failure_keeps_observed_publishing_reservation(fault: str) -> None:
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        provider = Provider(value, store.events)
        provider.wrong_verified = fault == "head"
        provider.fail_verify = fault == "provider"
        with pytest.raises((ValueError, CodingResultUnavailable)):
            await consumer(value, store, provider, clock).execute(
                value.prepared, value.authorization
            )
        assert "finalize" not in store.events and "uncertain" not in store.events
        assert (
            store.history is not None
            and store.history.status.state is CodingPublicationState.PUBLISHING
        )
        assert store.history.last_event_kind == "write_observed"

    asyncio.run(scenario())


def historical(value: Context, store: Store, *, uncertain: bool) -> None:
    first = plan(value).writes[0]
    step = CodingPublicationStep(
        result_id=value.receipt.result_id,
        invocation_id=uuid4(),
        expected_operation_version=1,
        step_id=first.write_id,
        step_kind=first.kind.value,
        input_digest=first.input_digest,
    )
    store.history = PublicationHistory(
        status=CodingResultStatus(
            receipt=value.receipt,
            state=CodingPublicationState.UNCERTAIN
            if uncertain
            else CodingPublicationState.PUBLISHING,
            operation_version=3 if uncertain else 2,
        ),
        invocation_id=step.invocation_id,
        last_step=step,
        last_event_kind="uncertain" if uncertain else "write_planned",
    )


@pytest.mark.parametrize("uncertain", [False, True])
def test_reconcile_only_appends_original_observation_and_never_resumes_or_finalizes(
    uncertain: bool,
) -> None:
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        historical(value, store, uncertain=uncertain)
        original = store.history
        provider = Provider(value, store.events)
        service = consumer(value, store, provider, clock)
        result = await service.reconcile(value.prepared, value.authorization)
        assert result is not None and original is not None
        assert result.state is original.status.state
        assert result.operation_version == original.status.operation_version + 1
        assert (
            store.history is not None and store.history.last_step == original.last_step
        )
        assert await service.reconcile(value.prepared, value.authorization) == result
        assert len(store.observations) == 1
        with pytest.raises(CodingResultUnavailable):
            await service.execute(value.prepared, value.authorization)
        assert (
            not provider.writes
            and "begin" not in store.events
            and "finalize" not in store.events
        )

    asyncio.run(scenario())


def test_reconcile_not_found_never_repeats_an_ambiguous_post() -> None:
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        historical(value, store, uncertain=True)
        assert store.history is not None
        expected = store.history.status
        provider = Provider(value, store.events)
        provider.not_found = True
        assert (
            await consumer(value, store, provider, clock).reconcile(
                value.prepared, value.authorization
            )
            == expected
        )
        assert not provider.writes and store.events == ["read", "get:blob", "read"]

    asyncio.run(scenario())


@pytest.mark.parametrize("observation", ["missing", "recorded", "new"])
def test_reconcile_rechecks_auth_after_provider_and_keeps_late_bookkeeping(
    observation: str,
) -> None:
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        historical(value, store, uncertain=True)
        assert store.history is not None
        if observation == "recorded":
            store.history = replace(
                store.history,
                status=replace(store.history.status, operation_version=4),
                last_event_kind="write_observed",
                last_observation_digest="a" * 64,
            )
        provider = Provider(value, store.events)
        provider.not_found = observation == "missing"
        reads = 0

        def expire_after_provider(stage: str) -> None:
            nonlocal reads
            if stage == "read":
                reads += 1
                if reads == 2:
                    raise WorkerAuthorityExpired("Current worker credential expired.")

        store.hook = expire_after_provider
        service = consumer(value, store, provider, clock)

        with pytest.raises(WorkerAuthorityExpired):
            await service.reconcile(value.prepared, value.authorization)

        assert reads == 2 and provider.writes == []
        assert len(store.observations) == int(observation == "new")
        assert store.history.status.state is CodingPublicationState.UNCERTAIN
        assert "begin" not in store.events and "finalize" not in store.events
        await service.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize("timing", ["before", "after"])
def test_cancelled_provider_wait_fences_new_posts_but_keeps_late_bookkeeping(
    timing: str,
) -> None:
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        provider = Provider(value, store.events)
        provider.block_before, provider.block_after = (
            timing == "before",
            timing == "after",
        )
        service = consumer(value, store, provider, clock)
        pending = asyncio.create_task(
            service.execute(value.prepared, value.authorization)
        )
        await asyncio.wait_for(
            (provider.before if timing == "before" else provider.started).wait(), 2
        )
        pending.cancel()
        provider.release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await service.aclose()
        assert store.history is not None and "finalize" not in store.events
        if timing == "before":
            assert (
                not provider.writes
                and store.history.status.state is CodingPublicationState.UNCERTAIN
            )
        else:
            assert len(provider.writes) == 1 and len(store.observations) == 1
            assert store.history.last_event_kind == "write_observed"

    asyncio.run(scenario())


def test_close_repeated_cancellation_drains_original_provider_response_before_returning() -> (
    None
):
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        provider = Provider(value, store.events)
        provider.block_after = True
        service = consumer(value, store, provider, clock)
        running = asyncio.create_task(
            service.execute(value.prepared, value.authorization)
        )
        await asyncio.wait_for(provider.started.wait(), 2)
        closing = asyncio.create_task(service.aclose())
        await asyncio.sleep(0)
        for _ in range(2):
            closing.cancel()
            await asyncio.sleep(0)
            assert not closing.done()
        with pytest.raises(CodingResultUnavailable):
            await service.reconcile(value.prepared, value.authorization)
        provider.release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(closing, 2)
        with pytest.raises(CodingResultUnavailable):
            await running
        assert len(store.observations) == 1 and len(provider.writes) == 1
        assert "finalize" not in store.events

    asyncio.run(scenario())


def test_suspend_time_after_actual_write_records_response_but_starts_no_next_write() -> (
    None
):
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        provider = Provider(value, store.events)
        provider.hook = lambda _: setattr(clock, "value", 1000.0)
        with pytest.raises(CodingResultUnavailable):
            await consumer(value, store, provider, clock).execute(
                value.prepared, value.authorization
            )
        assert len(provider.writes) == len(store.observations) == 1
        assert (
            store.history is not None
            and store.history.last_event_kind == "write_observed"
        )
        assert "finalize" not in store.events

    asyncio.run(scenario())


def test_caller_metadata_mutation_during_read_does_not_change_owned_publication() -> (
    None
):
    async def scenario() -> None:
        value = context()
        original_title = value.prepared.definition.snapshot.title
        store, clock = Store(value), Clock()
        provider = Provider(value, store.events)

        def mutate(event: str) -> None:
            if event == "read":
                object.__setattr__(
                    value.prepared.definition.snapshot, "title", "Changed caller title"
                )

        store.hook = mutate
        await consumer(value, store, provider, clock).execute(
            value.prepared, value.authorization
        )
        title = json.loads(provider.writes[-1].body)["title"]
        assert original_title in title and "Changed caller title" not in title

    asyncio.run(scenario())


@pytest.mark.parametrize("clock_value", [99.0, float("nan"), float("inf"), True])
def test_invalid_or_regressing_dispatch_clock_cannot_authorize_a_post(
    clock_value: float,
) -> None:
    async def scenario() -> None:
        value = context()
        store, clock = Store(value), Clock()
        provider = Provider(value, store.events)
        store.hook = lambda event: (
            setattr(clock, "value", clock_value) if event == "planned" else None
        )
        with pytest.raises(CodingResultUnavailable):
            await consumer(value, store, provider, clock).execute(
                value.prepared, value.authorization
            )
        assert not provider.writes and "uncertain" in store.events

    asyncio.run(scenario())
