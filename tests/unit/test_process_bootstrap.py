import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from uuid import UUID

import pytest

from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessResourceLimits,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.bootstrap import processes as bootstrap
from tests.unit.test_worker_execution_preparation import context, policy


def binding() -> ExecutionProcessBinding:
    setup = context()
    acknowledgement = setup.journal.state.acknowledgement
    reference = setup.delivery.outcome.assignment
    assert acknowledgement is not None and reference is not None
    return ExecutionProcessBinding(
        intent=WorkerExecutionIntent(
            execution_id=UUID(int=100),
            acknowledgement=acknowledgement,
            policy=policy(),
        ),
        reference=reference,
    )


def limits() -> ProcessResourceLimits:
    return ProcessResourceLimits(
        cpu_percent=25, committed_memory_bytes=512 * 1024**2, maximum_processes=4
    )


@dataclass
class Native:
    binding: ExecutionProcessBinding
    calls: list[tuple[str, int]] = field(default_factory=list)
    sealed: bool = False
    count: int = 2
    query: Callable[[], int] | None = None
    on_close: Callable[[], None] = lambda: None

    def seal(self) -> None:
        self.sealed = True

    def attach_suspended_process(self, process_handle: int) -> None:
        if self.sealed:
            raise ProcessScopeUnavailable("Test scope is sealed.")
        self.calls.append(("attach", threading.get_ident()))

    def terminate(self) -> None:
        self.calls.append(("terminate", threading.get_ident()))
        assert self.sealed
        self.count = 0

    def active_processes(self) -> int:
        self.calls.append(("query", threading.get_ident()))
        return self.count if self.query is None else self.query()

    def close(self) -> None:
        self.calls.append(("close", threading.get_ident()))
        assert self.sealed
        self.on_close()


def test_native_operations_and_cleanup_remain_on_one_owning_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = binding()
    native = Native(expected)
    main_thread = threading.get_ident()

    def create(
        supplied: ExecutionProcessBinding, supplied_limits: ProcessResourceLimits
    ) -> Native:
        assert supplied == expected and supplied_limits == limits()
        native.calls.append(("create", threading.get_ident()))
        return native

    monkeypatch.setattr(bootstrap, "WindowsJobObject", create)

    async def exercise() -> tuple[int, int, ExecutionProcessBinding]:
        async with bootstrap.open_worker_process_scope(expected, limits()) as scope:
            await scope.attach_suspended_process(1)
            before = await scope.active_processes()
            await scope.terminate()
            after = await scope.active_processes()
            return before, after, scope.binding

    before, after, supplied = asyncio.run(exercise())

    assert before == 2 and after == 0 and supplied == expected
    assert [name for name, _ in native.calls] == [
        "create",
        "attach",
        "query",
        "terminate",
        "query",
        "close",
    ]
    assert len({owner for _, owner in native.calls}) == 1
    assert native.calls[0][1] != main_thread


def test_cancelled_native_query_blocks_new_operations_until_actual_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = binding()
    native = Native(expected)
    entered, release = threading.Event(), threading.Event()

    def held_query() -> int:
        entered.set()
        if not release.wait(5):
            raise ProcessScopeUnavailable("Test timeout.")
        return 2

    native.query = held_query
    monkeypatch.setattr(bootstrap, "WindowsJobObject", lambda *args: native)

    async def exercise() -> None:
        async with bootstrap.open_worker_process_scope(expected, limits()) as scope:
            pending = asyncio.create_task(scope.active_processes())
            assert await asyncio.to_thread(entered.wait, 5)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            try:
                with pytest.raises(ProcessScopeUnavailable, match="unsettled"):
                    await scope.terminate()
            finally:
                release.set()

    asyncio.run(exercise())

    assert [name for name, _ in native.calls] == ["query", "close"]
    assert native.sealed


def test_seal_rejects_late_attachment_and_closed_scope_has_no_empty_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = binding()
    native = Native(expected)
    monkeypatch.setattr(bootstrap, "WindowsJobObject", lambda *args: native)

    async def exercise() -> None:
        async with bootstrap.open_worker_process_scope(expected, limits()) as scope:
            scope.seal()
            with pytest.raises(ProcessScopeUnavailable):
                await scope.attach_suspended_process(1)
        with pytest.raises(ProcessScopeUnavailable):
            await scope.active_processes()

    asyncio.run(exercise())

    assert [name for name, _ in native.calls] == ["close"]


def test_cancellation_during_initialization_closes_eventual_scope_without_yielding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = binding()
    native = Native(expected)
    entered, release = threading.Event(), threading.Event()
    yielded = False

    def create(*args: object) -> Native:
        entered.set()
        if not release.wait(5):
            raise ProcessScopeUnavailable("Test timeout.")
        native.calls.append(("create", threading.get_ident()))
        return native

    monkeypatch.setattr(bootstrap, "WindowsJobObject", create)

    async def exercise() -> None:
        async def open_scope() -> None:
            nonlocal yielded
            async with bootstrap.open_worker_process_scope(expected, limits()):
                yielded = True

        pending = asyncio.create_task(open_scope())
        assert await asyncio.to_thread(entered.wait, 5)
        pending.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending

    asyncio.run(exercise())

    assert not yielded and native.sealed
    assert [name for name, _ in native.calls] == ["create", "close"]
    assert native.calls[0][1] == native.calls[1][1]
