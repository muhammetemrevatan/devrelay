import asyncio
import threading
from dataclasses import dataclass, field
from typing import Any

import pytest

import devrelay.bootstrap.trusted_processes as bootstrap
from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessResourceLimits,
)
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessObservation,
    TrustedProcessSpec,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from tests.unit.test_process_bootstrap import binding, limits
from tests.unit.test_trusted_process_contracts import spec


@dataclass
class State:
    binding: ExecutionProcessBinding = field(default_factory=binding)
    calls: list[tuple[str, int]] = field(default_factory=list)
    block: str | None = None
    entered: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)
    settled: threading.Event = field(default_factory=threading.Event)
    sealed: bool = False
    active: int = 0
    fail_bridge_initialize: bool = False
    fail_bridge_close: bool = False
    fail_job_initialize: bool = False

    def record(self, name: str) -> None:
        self.calls.append((name, threading.get_ident()))

    def operation(self, name: str) -> None:
        self.record(name)
        if self.block == name:
            self.entered.set()
            if not self.release.wait(5):
                raise AssertionError("Fixture native operation was not released.")
            self.record(name + "-settled")
            self.settled.set()


def install(monkeypatch: pytest.MonkeyPatch, state: State) -> None:
    class Job:
        def __init__(
            self,
            supplied: ExecutionProcessBinding,
            supplied_limits: ProcessResourceLimits,
        ) -> None:
            assert supplied is state.binding and supplied_limits == limits()
            self.binding = supplied
            state.operation("job-initialize")
            if state.fail_job_initialize:
                raise ProcessScopeUnavailable("Fixture native platform unavailable.")

        def seal(self) -> None:
            state.sealed = True

        def close(self) -> None:
            state.record("job-close")

    class Native:
        def __init__(self, job: Job) -> None:
            self.binding = job.binding
            state.operation("bridge-initialize")
            if state.fail_bridge_initialize:
                raise ProcessScopeUnavailable("Fixture bridge initialization failed.")

        def seal(self) -> None:
            state.sealed = True

        def prepare(self, command: TrustedProcessSpec) -> None:
            assert command == spec()
            if state.sealed:
                raise ProcessScopeUnavailable("Fixture scope is sealed.")
            state.operation("prepare")
            state.active = 1

        def resume(self) -> None:
            if state.sealed:
                raise ProcessScopeUnavailable("Fixture scope is sealed.")
            state.operation("resume")

        def observe(self) -> TrustedProcessObservation:
            state.operation("observe")
            return TrustedProcessObservation(
                exit_code=None,
                stdout=b"fixture",
                stderr=b"",
                stdout_eof=False,
                stderr_eof=False,
            )

        def terminate(self) -> None:
            assert state.sealed
            state.operation("terminate")
            state.active = 0

        def active_processes(self) -> int:
            state.operation("query")
            return state.active

        def close(self) -> None:
            assert state.sealed
            state.record("bridge-close")
            if state.fail_bridge_close:
                raise ProcessScopeUnavailable("Fixture bridge cleanup failed.")

    monkeypatch.setattr(bootstrap, "WindowsJobObject", Job)
    monkeypatch.setattr(bootstrap, "WindowsTrustedProcess", Native)


def names(state: State) -> list[str]:
    return [name for name, _ in state.calls]


def test_process_lifecycle_and_fifo_cleanup_have_one_native_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = State()
    install(monkeypatch, state)

    async def scenario() -> None:
        async with bootstrap.open_trusted_process_scope(
            state.binding, limits()
        ) as scope:
            assert scope.binding is state.binding
            await scope.prepare(spec())
            await scope.resume()
            assert (await scope.observe()).stdout == b"fixture"
            assert await scope.active_processes() == 1
            await scope.terminate()
            assert await scope.active_processes() == 0

    asyncio.run(scenario())

    assert names(state) == [
        "job-initialize",
        "bridge-initialize",
        "prepare",
        "resume",
        "observe",
        "query",
        "terminate",
        "query",
        "bridge-close",
        "job-close",
    ]
    assert len({owner for _, owner in state.calls}) == 1
    assert threading.get_ident() != state.calls[0][1]


@pytest.mark.parametrize("operation", ["prepare", "resume", "observe", "query"])
def test_cancelled_wait_keeps_actual_native_lane_until_settlement_and_fifo_close(
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = State(block=operation)
    install(monkeypatch, state)

    async def scenario() -> None:
        try:
            async with bootstrap.open_trusted_process_scope(
                state.binding, limits()
            ) as scope:
                if operation != "prepare":
                    await scope.prepare(spec())
                if operation == "prepare":
                    pending: asyncio.Task[Any] = asyncio.create_task(
                        scope.prepare(spec())
                    )
                elif operation == "resume":
                    pending = asyncio.create_task(scope.resume())
                elif operation == "observe":
                    pending = asyncio.create_task(scope.observe())
                else:
                    pending = asyncio.create_task(scope.active_processes())
                assert await asyncio.to_thread(state.entered.wait, 2)
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
                with pytest.raises(ProcessScopeUnavailable):
                    await scope.active_processes()
                with pytest.raises(ProcessScopeUnavailable):
                    await scope.terminate()
                assert "bridge-close" not in names(state)
                state.release.set()
                assert await asyncio.to_thread(state.settled.wait, 2)
                await asyncio.sleep(0)
                if operation in ("prepare", "resume"):
                    with pytest.raises(ProcessScopeUnavailable):
                        await scope.prepare(spec())
                    with pytest.raises(ProcessScopeUnavailable):
                        await scope.resume()
        finally:
            state.release.set()

    asyncio.run(scenario())

    assert names(state)[-2:] == ["bridge-close", "job-close"]
    assert names(state).count(operation) == 1
    assert names(state).index(operation + "-settled") < names(state).index(
        "bridge-close"
    )
    assert len({owner for _, owner in state.calls}) == 1


@pytest.mark.parametrize("operation", ["prepare", "resume"])
def test_cancelled_prepare_or_resume_never_adopts_late_success_as_new_admission(
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = State(block=operation)
    install(monkeypatch, state)

    async def scenario() -> None:
        try:
            async with bootstrap.open_trusted_process_scope(
                state.binding, limits()
            ) as scope:
                if operation == "resume":
                    await scope.prepare(spec())
                pending = asyncio.create_task(
                    scope.prepare(spec()) if operation == "prepare" else scope.resume()
                )
                assert await asyncio.to_thread(state.entered.wait, 2)
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
                assert not state.sealed
                state.release.set()
                assert await asyncio.to_thread(state.settled.wait, 2)
                await asyncio.sleep(0)
                with pytest.raises(ProcessScopeUnavailable):
                    await scope.prepare(spec())
                with pytest.raises(ProcessScopeUnavailable):
                    await scope.resume()
                assert not state.sealed
        finally:
            state.release.set()

    asyncio.run(scenario())

    assert names(state).count("prepare") == 1
    assert names(state).count("resume") == (1 if operation == "resume" else 0)


def test_successful_prepare_and_resume_each_consume_one_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = State()
    install(monkeypatch, state)

    async def scenario() -> None:
        async with bootstrap.open_trusted_process_scope(
            state.binding, limits()
        ) as scope:
            with pytest.raises(ProcessScopeUnavailable):
                await scope.resume()
            await scope.prepare(spec())
            with pytest.raises(ProcessScopeUnavailable):
                await scope.prepare(spec())
            await scope.resume()
            with pytest.raises(ProcessScopeUnavailable):
                await scope.resume()

    asyncio.run(scenario())

    assert names(state).count("prepare") == names(state).count("resume") == 1


@pytest.mark.parametrize("phase", ["job-initialize", "bridge-initialize"])
def test_cancellation_during_initialization_closes_eventual_resources_without_yielding(
    phase: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = State(block=phase)
    install(monkeypatch, state)
    yielded = False

    async def scenario() -> None:
        async def initialize() -> None:
            nonlocal yielded
            async with bootstrap.open_trusted_process_scope(state.binding, limits()):
                yielded = True

        opening = asyncio.create_task(initialize())
        try:
            assert await asyncio.to_thread(state.entered.wait, 2)
            opening.cancel()
            state.release.set()
            with pytest.raises(asyncio.CancelledError):
                await opening
        finally:
            state.release.set()

    asyncio.run(scenario())

    assert not yielded
    assert names(state)[-2:] == ["bridge-close", "job-close"]
    assert len({owner for _, owner in state.calls}) == 1


def test_bridge_initialization_failure_still_closes_created_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = State(fail_bridge_initialize=True)
    install(monkeypatch, state)

    async def scenario() -> None:
        with pytest.raises(ProcessScopeUnavailable, match="initialization failed"):
            async with bootstrap.open_trusted_process_scope(state.binding, limits()):
                raise AssertionError("Failed setup cannot yield a process scope.")

    asyncio.run(scenario())

    assert names(state) == ["job-initialize", "bridge-initialize", "job-close"]


def test_bridge_cleanup_failure_does_not_skip_job_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = State(fail_bridge_close=True)
    install(monkeypatch, state)

    async def scenario() -> None:
        with pytest.raises(ProcessScopeUnavailable, match="cleanup failed"):
            async with bootstrap.open_trusted_process_scope(state.binding, limits()):
                pass

    asyncio.run(scenario())

    assert names(state)[-2:] == ["bridge-close", "job-close"]


def test_unavailable_native_platform_does_not_yield_or_create_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = State(fail_job_initialize=True)
    install(monkeypatch, state)

    async def scenario() -> None:
        with pytest.raises(ProcessScopeUnavailable, match="platform unavailable"):
            async with bootstrap.open_trusted_process_scope(state.binding, limits()):
                raise AssertionError("Native platform failure must not yield a scope.")

    asyncio.run(scenario())

    assert names(state) == ["job-initialize"]


def test_sealed_or_closed_scope_cannot_start_or_resume_another_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = State()
    install(monkeypatch, state)

    async def scenario() -> None:
        async with bootstrap.open_trusted_process_scope(
            state.binding, limits()
        ) as scope:
            scope.seal()
            with pytest.raises(ProcessScopeUnavailable):
                await scope.prepare(spec())
            with pytest.raises(ProcessScopeUnavailable):
                await scope.resume()
        with pytest.raises(ProcessScopeUnavailable):
            await scope.active_processes()

    asyncio.run(scenario())

    assert names(state) == [
        "job-initialize",
        "bridge-initialize",
        "bridge-close",
        "job-close",
    ]
