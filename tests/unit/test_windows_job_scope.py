import ctypes
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any
from uuid import uuid4

import pytest

from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessResourceLimits,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.infrastructure.worker_processes import windows
from tests.acknowledgement_fixtures import (
    acknowledgement_inputs,
    acknowledgement_intent,
)


def binding() -> ExecutionProcessBinding:
    _, delivery = acknowledgement_inputs()
    assert delivery.outcome.assignment
    return ExecutionProcessBinding(
        intent=WorkerExecutionIntent(
            execution_id=uuid4(),
            acknowledgement=acknowledgement_intent(delivery),
            policy=ExecutionPolicy(
                runtime=ExecutionRuntime.CODEX,
                isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
                policy_digest="a" * 64,
            ),
        ),
        reference=delivery.outcome.assignment,
    )


LIMITS = ProcessResourceLimits(
    cpu_percent=20, committed_memory_bytes=512 * 1024**2, maximum_processes=8
)


class Function:
    def __init__(self, body: Callable[..., Any]) -> None:
        self.body = body
        self.argtypes: list[object] = []
        self.restype: object = None

    def __call__(self, *args: Any) -> Any:
        return self.body(*args)


class Kernel:
    def __init__(self) -> None:
        self.handle = 2**40 + 3 if ctypes.sizeof(ctypes.c_void_p) == 8 else 123
        self.created: list[tuple[object, object]] = []
        self.closed: list[int] = []
        self.assigned: list[tuple[int, int]] = []
        self.terminations: list[int] = []
        self.saved: dict[int, bytes] = {}
        self.fail = ""
        self.mismatch = ""
        self.member = 1
        self.active = 0
        self.on_assign: Callable[[], None] = lambda: None
        self.CreateJobObjectW = Function(self.create)
        self.SetHandleInformation = Function(
            lambda *args: int(self.fail != "set_handle")
        )
        self.GetHandleInformation = Function(self.get_handle)
        self.SetInformationJobObject = Function(self.set_information)
        self.QueryInformationJobObject = Function(self.query)
        self.AssignProcessToJobObject = Function(self.assign)
        self.IsProcessInJob = Function(self.membership)
        self.TerminateJobObject = Function(self.terminate)
        self.CloseHandle = Function(self.close)

    def create(self, security: object, name: object) -> int:
        self.created.append((security, name))
        return 0 if self.fail == "create" else self.handle

    def get_handle(self, handle: int, output: Any) -> int:
        output._obj.value = int(self.mismatch == "inherit")
        return int(self.fail != "get_handle")

    def set_information(self, handle: int, kind: int, data: Any, size: int) -> int:
        self.saved[kind] = ctypes.string_at(data, size)
        return int(self.fail != f"set_{kind}")

    def query(
        self, handle: int, kind: int, output: Any, size: int, returned: Any
    ) -> int:
        if self.fail == f"query_{kind}":
            return 0
        if kind == 1:
            output._obj.ActiveProcesses = self.active
        else:
            ctypes.memmove(output, self.saved[kind], size)
        returned._obj.value = size - 1 if self.mismatch == "length" else size
        if self.mismatch == "flags" and kind == 9:
            output._obj.BasicLimitInformation.LimitFlags |= 0x800
        elif self.mismatch == "memory" and kind == 9:
            output._obj.JobMemoryLimit -= 1
        elif self.mismatch == "processes" and kind == 9:
            output._obj.BasicLimitInformation.ActiveProcessLimit -= 1
        elif self.mismatch == "cpu_flags" and kind == 15:
            output._obj.ControlFlags = 1
        elif self.mismatch == "cpu_rate" and kind == 15:
            output._obj.CpuRate -= 1
        return 1

    def assign(self, job: int, process: int) -> int:
        self.assigned.append((job, process))
        self.on_assign()
        return int(self.fail != "assign")

    def membership(self, process: int, job: int, output: Any) -> int:
        output._obj.value = self.member
        return int(self.fail != "membership")

    def terminate(self, handle: int, code: int) -> int:
        self.terminations.append(handle)
        return int(self.fail != "terminate")

    def close(self, handle: int) -> int:
        self.closed.append(handle)
        return int(self.fail != "close")


@pytest.fixture
def kernel(monkeypatch: pytest.MonkeyPatch) -> Kernel:
    native = Kernel()
    monkeypatch.setattr(windows, "_load_kernel", lambda: native)
    return native


def test_private_job_sets_and_verifies_required_caps_without_breakaway(
    kernel: Kernel,
) -> None:
    association = binding()

    scope = windows.WindowsJobObject(association, LIMITS)

    extended = windows._ExtendedLimits.from_buffer_copy(kernel.saved[9])
    cpu = windows._CpuLimits.from_buffer_copy(kernel.saved[15])

    assert scope.binding is association
    assert kernel.created == [(None, None)]
    assert extended.BasicLimitInformation.LimitFlags == 0x2208
    assert extended.BasicLimitInformation.ActiveProcessLimit == LIMITS.maximum_processes
    assert extended.JobMemoryLimit == LIMITS.committed_memory_bytes
    assert cpu.ControlFlags == 5 and cpu.CpuRate == LIMITS.cpu_percent * 100
    assert kernel.CreateJobObjectW.restype is ctypes.c_void_p
    assert kernel.AssignProcessToJobObject.argtypes == [
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]


@pytest.mark.parametrize(
    "failure",
    ["create", "set_handle", "get_handle", "set_9", "set_15", "query_9", "query_15"],
)
def test_failed_setup_closes_owned_job_and_never_falls_back(
    kernel: Kernel, failure: str
) -> None:
    kernel.fail = failure
    association = binding()

    with pytest.raises(ProcessScopeUnavailable) as error:
        windows.WindowsJobObject(association, LIMITS)

    assert str(error.value) == "Windows process scope is unavailable."
    assert kernel.closed == ([] if failure == "create" else [kernel.handle])
    assert kernel.assigned == []


@pytest.mark.parametrize(
    "mismatch",
    ["inherit", "flags", "memory", "processes", "cpu_flags", "cpu_rate", "length"],
)
def test_effective_cap_or_handle_mismatch_fails_closed(
    kernel: Kernel, mismatch: str
) -> None:
    kernel.mismatch = mismatch
    association = binding()

    with pytest.raises(ProcessScopeUnavailable):
        windows.WindowsJobObject(association, LIMITS)

    assert kernel.closed == [kernel.handle] and kernel.assigned == []


def test_borrowed_handle_is_assigned_and_verified_without_closing_or_resuming(
    kernel: Kernel,
) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)
    process_handle = kernel.handle + 1

    scope.attach_suspended_process(process_handle)

    assert kernel.assigned == [(kernel.handle, process_handle)]
    assert kernel.closed == []


@pytest.mark.parametrize("value", [0, -1, True, 1.5, 2**64 - 1, 2**64, "123"])
def test_invalid_borrowed_handle_never_reaches_assignment(
    kernel: Kernel, value: Any
) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)

    with pytest.raises(ProcessScopeUnavailable):
        scope.attach_suspended_process(value)

    assert kernel.assigned == [] and kernel.closed == []


@pytest.mark.parametrize(
    "failure", ["assign", "membership", "not_member", "seal_during_assign"]
)
def test_attachment_failure_seals_scope_without_owning_borrowed_process(
    kernel: Kernel, failure: str
) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)
    kernel.fail = failure
    kernel.member = 0 if failure == "not_member" else 1
    if failure == "seal_during_assign":
        kernel.on_assign = scope.seal

    with pytest.raises(ProcessScopeUnavailable):
        scope.attach_suspended_process(123)

    with pytest.raises(ProcessScopeUnavailable):
        scope.attach_suspended_process(124)
    assert len(kernel.assigned) == 1 and kernel.closed == []


@pytest.mark.parametrize("action", ["seal", "terminate", "close"])
def test_terminal_boundary_rejects_late_attachment(kernel: Kernel, action: str) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)
    getattr(scope, action)()

    with pytest.raises(ProcessScopeUnavailable):
        scope.attach_suspended_process(123)

    assert kernel.assigned == []


def test_seal_never_waits_for_blocked_native_assignment_and_rejects_its_completion(
    kernel: Kernel,
) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)
    entered, release = Event(), Event()

    def blocked_assignment() -> None:
        entered.set()
        if not release.wait(5):
            raise RuntimeError("Fixture assignment timeout.")

    def attach() -> bool:
        try:
            scope.attach_suspended_process(123)
        except ProcessScopeUnavailable:
            return False
        return True

    kernel.on_assign = blocked_assignment
    with ThreadPoolExecutor(max_workers=1) as executor:
        operation = executor.submit(attach)
        assert entered.wait(5)
        try:
            scope.seal()
        finally:
            release.set()
        completed = operation.result(timeout=5)

    assert completed is False
    assert kernel.assigned == [(kernel.handle, 123)]
    assert kernel.closed == []


def test_terminate_is_idempotent_and_accounting_remains_fresh(kernel: Kernel) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)
    kernel.active = 2
    scope.terminate()

    scope.terminate()
    count = scope.active_processes()

    assert kernel.terminations == [kernel.handle]
    assert count == 2


def test_failed_termination_does_not_claim_success_and_can_retry(
    kernel: Kernel,
) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)
    kernel.fail = "terminate"

    with pytest.raises(ProcessScopeUnavailable):
        scope.terminate()

    kernel.fail = ""
    scope.terminate()
    assert kernel.terminations == [kernel.handle, kernel.handle]


def test_failed_query_never_returns_false_empty(kernel: Kernel) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)
    kernel.fail = "query_1"

    with pytest.raises(ProcessScopeUnavailable):
        scope.active_processes()


def test_closed_job_cannot_be_observed_as_empty(kernel: Kernel) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)
    scope.close()

    with pytest.raises(ProcessScopeUnavailable):
        scope.active_processes()

    scope.close()
    assert kernel.closed == [kernel.handle]


def test_close_failure_retains_handle_for_owned_cleanup_retry(kernel: Kernel) -> None:
    scope = windows.WindowsJobObject(binding(), LIMITS)
    kernel.fail = "close"

    with pytest.raises(ProcessScopeUnavailable):
        scope.close()

    kernel.fail = ""
    scope.close()
    assert kernel.closed == [kernel.handle, kernel.handle]


def test_native_structures_preserve_windows_widths_and_alignment() -> None:
    pointer_bytes = ctypes.sizeof(ctypes.c_void_p)

    sizes = tuple(
        ctypes.sizeof(kind)
        for kind in (
            windows._Accounting,
            windows._CpuLimits,
            windows._BasicLimits,
            windows._ExtendedLimits,
        )
    )

    assert ctypes.sizeof(windows._DWORD) == ctypes.sizeof(windows._BOOL) == 4
    assert sizes[:2] == (48, 8)
    if pointer_bytes == 8:
        assert sizes[2:] == (64, 144)
        assert windows._BasicLimits.Affinity.offset == 48


def test_non_windows_platform_has_no_native_load_or_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(
        ctypes, "WinDLL", lambda *args, **kwargs: calls.append("load"), raising=False
    )
    association = binding()

    with pytest.raises(ProcessScopeUnavailable):
        windows.WindowsJobObject(association, LIMITS)

    assert calls == []


def test_system_only_loader_flags_and_missing_native_function_are_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def loader(*args: Any, **kwargs: Any) -> object:
        calls.append((args, kwargs))
        return object()

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(ctypes, "WinDLL", loader, raising=False)
    association = binding()

    with pytest.raises(ProcessScopeUnavailable) as error:
        windows.WindowsJobObject(association, LIMITS)

    assert calls == [(("Kernel32.dll",), {"use_last_error": True, "winmode": 0x800})]
    assert str(error.value) == "Windows process scope is unavailable."
    assert error.value.__cause__ is None
