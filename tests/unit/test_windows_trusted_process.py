import ctypes
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event
from typing import Any, cast

import pytest

from devrelay.application.contracts.trusted_processes import (
    TRUSTED_PROCESS_READ_SIZE,
    TrustedProcessSpec,
)
from devrelay.application.ports.process_scope import ProcessScopeUnavailable
from devrelay.infrastructure.worker_processes import trusted_windows as native
from devrelay.infrastructure.worker_processes.windows import WindowsJobObject
from tests.unit.test_windows_job_scope import Function, binding


class Scope:
    def __init__(self) -> None:
        self.binding = binding()
        self.sealed = False
        self.assigned: list[int] = []
        self.terminations = 0
        self.fail = ""

    def seal(self) -> None:
        self.sealed = True

    def attach_suspended_process(self, handle: int) -> None:
        if self.sealed or self.fail == "attach":
            raise ProcessScopeUnavailable("Fixture attachment failure.")
        self.assigned.append(handle)

    def terminate(self) -> None:
        self.terminations += 1
        if self.fail == "terminate":
            raise ProcessScopeUnavailable("Fixture termination failure.")

    def active_processes(self) -> int:
        return 1


class Kernel:
    def __init__(self) -> None:
        self.fail = ""
        self.error = 0
        self.flags: dict[int, int] = {}
        self.next_handle = 10
        self.pipes: list[int] = []
        self.contents: dict[int, bytes] = {}
        self.eof: set[int] = set()
        self.closed: list[int] = []
        self.inherited: tuple[int, ...] = ()
        self.command: tuple[object, ...] | None = None
        self.startup: object = None
        self.environment = ""
        self.resumes = 0
        self.resume_count = 1
        self.wait = 258
        self.exit_code = 0
        self.direct_terminations: list[int] = []
        self.attribute_deletions = 0
        self.CreateFileW = Function(self.open)
        self.CreatePipe = Function(self.pipe)
        self.GetHandleInformation = Function(self.information)
        self.SetHandleInformation = Function(self.set_information)
        self.InitializeProcThreadAttributeList = Function(self.initialize)
        self.UpdateProcThreadAttribute = Function(self.update)
        self.DeleteProcThreadAttributeList = Function(self.delete)
        self.CreateProcessW = Function(self.create)
        self.ResumeThread = Function(self.resume)
        self.PeekNamedPipe = Function(self.peek)
        self.ReadFile = Function(self.read)
        self.WaitForSingleObject = Function(
            lambda handle, timeout: 0xFFFFFFFF if self.fail == "wait" else self.wait
        )
        self.GetExitCodeProcess = Function(self.exit)
        self.TerminateProcess = Function(self.terminate)
        self.CloseHandle = Function(self.close)
        self.GetLastError = Function(lambda: self.error)

    def handle(self, inherit: bool) -> int:
        handle = self.next_handle
        self.next_handle += 1
        self.flags[handle] = int(inherit)
        return handle

    def open(self, *args: Any) -> int:
        return native._INVALID_HANDLE if self.fail == "nul" else self.handle(True)

    def pipe(self, read: Any, write: Any, security: Any, size: int) -> int:
        if self.fail == "pipe":
            return 0
        read._obj.value, write._obj.value = self.handle(True), self.handle(True)
        self.pipes.append(read._obj.value)
        self.contents[read._obj.value] = b""
        return 1

    def information(self, handle: int, output: Any) -> int:
        output._obj.value = self.flags[handle]
        return int(self.fail != "information")

    def set_information(self, handle: int, mask: int, flags: int) -> int:
        if self.fail != "inheritance":
            self.flags[handle] = flags
        return int(self.fail != "set_information")

    def initialize(self, attributes: Any, count: int, flags: int, size: Any) -> int:
        if attributes is None:
            size._obj.value = 64
            self.error = 122
            return 0
        return int(self.fail != "initialize")

    def update(
        self,
        attributes: Any,
        flags: int,
        attribute: int,
        handles: Any,
        size: int,
        previous: Any,
        returned: Any,
    ) -> int:
        assert attribute == 0x20002
        self.inherited = tuple(handles)
        return int(self.fail != "update")

    def delete(self, attributes: object) -> None:
        self.attribute_deletions += 1

    def create(
        self,
        executable: str,
        command: Any,
        process_security: Any,
        thread_security: Any,
        inherit: bool,
        flags: int,
        environment: Any,
        cwd: str,
        startup: Any,
        process: Any,
    ) -> int:
        self.command = (
            executable,
            command.value,
            process_security,
            thread_security,
            inherit,
            flags,
            cwd,
        )
        self.environment = environment[:]
        self.startup = startup._obj.startup
        if self.fail == "create":
            return 0
        process._obj.process, process._obj.thread = (
            self.handle(False),
            self.handle(False),
        )
        return 1

    def resume(self, thread: int) -> int:
        self.resumes += 1
        return self.resume_count

    def peek(
        self, handle: int, buffer: Any, count: int, read: Any, available: Any, left: Any
    ) -> int:
        if self.fail == "peek" or handle in self.eof:
            self.error = 5 if self.fail else 109
            return 0
        available._obj.value = len(self.contents[handle])
        return 1

    def read(
        self, handle: int, buffer: Any, count: int, output: Any, overlapped: object
    ) -> int:
        data = self.contents[handle][:count]
        self.contents[handle] = self.contents[handle][count:]
        ctypes.memmove(buffer, data, len(data))
        output._obj.value = len(data) if self.fail != "read_count" else count + 1
        return int(self.fail != "read")

    def exit(self, handle: int, output: Any) -> int:
        output._obj.value = self.exit_code
        return int(self.fail != "exit")

    def terminate(self, handle: int, code: int) -> int:
        self.direct_terminations.append(handle)
        if self.fail != "terminate":
            self.wait = 0
        return int(self.fail != "terminate")

    def close(self, handle: int) -> int:
        self.closed.append(handle)
        return int(self.fail != "close")


def specification() -> TrustedProcessSpec:
    return TrustedProcessSpec(
        executable="C:\\tools\\python.exe",
        arguments=("-I", "-c", "print('fixture')"),
        working_directory="C:\\workspace",
        environment=(("SystemRoot", "C:\\Windows"),),
    )


@pytest.fixture
def setup(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[native.WindowsTrustedProcess, Kernel, Scope]:
    kernel, scope = Kernel(), Scope()
    monkeypatch.setattr(native, "_load_kernel", lambda: kernel)
    return native.WindowsTrustedProcess(cast(WindowsJobObject, scope)), kernel, scope


def test_prepare_pins_suspended_process_and_only_three_explicit_child_handles(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, scope = setup
    spec = specification()

    process.prepare(spec)

    assert kernel.command == (
        spec.executable,
        "C:\\tools\\python.exe -I -c print('fixture')",
        None,
        None,
        True,
        0x08080404,
        spec.working_directory,
    )
    assert "SystemRoot=C:\\Windows\x00\x00" in kernel.environment
    assert len(kernel.inherited) == 3 and all(
        kernel.flags[handle] == 1 for handle in kernel.inherited
    )
    assert scope.assigned == [process._process] and kernel.resumes == 0
    assert all(kernel.flags[handle] == 0 for handle in process._handles)
    assert kernel.closed == list(kernel.inherited) and kernel.attribute_deletions == 1
    process.resume()
    assert kernel.resumes == 1
    with pytest.raises(ProcessScopeUnavailable):
        process.resume()
    assert kernel.resumes == 1


@pytest.mark.parametrize(
    "failure",
    [
        "nul",
        "pipe",
        "information",
        "set_information",
        "inheritance",
        "initialize",
        "update",
        "create",
    ],
)
def test_prepare_failure_is_sanitized_no_resume_and_closes_owned_handles(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope], failure: str
) -> None:
    process, kernel, scope = setup
    kernel.fail = failure

    with pytest.raises(
        ProcessScopeUnavailable, match="^Trusted Windows process is unavailable\\.$"
    ):
        process.prepare(specification())

    assert kernel.resumes == 0 and process._handles == []
    with pytest.raises(ProcessScopeUnavailable):
        process.prepare(specification())


def test_attachment_failure_kills_suspended_owned_root_without_resume(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, scope = setup
    scope.fail = "attach"

    with pytest.raises(ProcessScopeUnavailable):
        process.prepare(specification())

    assert kernel.resumes == 0 and kernel.direct_terminations == [process._process]
    assert process._handles == []


@pytest.mark.parametrize("count", [0, 2, 0xFFFFFFFF])
def test_resume_requires_exact_suspension_count_one(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope], count: int
) -> None:
    process, kernel, scope = setup
    process.prepare(specification())
    kernel.resume_count = count

    with pytest.raises(ProcessScopeUnavailable):
        process.resume()

    assert kernel.resumes == 1 and scope.sealed


def test_observe_reads_at_most_one_bounded_chunk_from_each_pipe(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, _ = setup
    process.prepare(specification())
    kernel.contents[kernel.pipes[0]] = b"o" * (TRUSTED_PROCESS_READ_SIZE + 1)
    kernel.contents[kernel.pipes[1]] = b"e" * (TRUSTED_PROCESS_READ_SIZE + 2)

    observation = process.observe()

    assert observation.stdout == b"o" * TRUSTED_PROCESS_READ_SIZE
    assert observation.stderr == b"e" * TRUSTED_PROCESS_READ_SIZE
    assert (
        observation.exit_code is None
        and not observation.stdout_eof
        and not observation.stderr_eof
    )
    assert process.observe().stdout == b"o"


def test_genuine_exit259_and_pipe_eof_are_independent(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, _ = setup
    process.prepare(specification())
    kernel.wait, kernel.exit_code = 0, 259
    kernel.eof.add(kernel.pipes[0])

    observation = process.observe()

    assert observation.exit_code == 259 and observation.stdout_eof
    assert not observation.stderr_eof and observation.stderr == b""


@pytest.mark.parametrize("failure", ["peek", "read", "read_count", "wait", "exit"])
def test_observation_failure_is_sanitized_and_seals_resume(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope], failure: str
) -> None:
    process, kernel, scope = setup
    process.prepare(specification())
    kernel.contents[kernel.pipes[0]] = b"data"
    kernel.wait, kernel.fail = 0, failure

    with pytest.raises(ProcessScopeUnavailable):
        process.observe()

    assert scope.sealed
    with pytest.raises(ProcessScopeUnavailable):
        process.resume()


def test_changed_job_binding_blocks_resume_and_observation(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, scope = setup
    process.prepare(specification())
    scope.binding = replace(
        scope.binding,
        intent=replace(
            scope.binding.intent,
            policy=replace(scope.binding.intent.policy, policy_digest="b" * 64),
        ),
    )

    with pytest.raises(ProcessScopeUnavailable):
        process.resume()

    assert kernel.resumes == 0


def test_close_preserves_job_ownership_and_is_idempotent(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, scope = setup
    process.prepare(specification())
    process.resume()

    process.close()
    calls = list(kernel.closed)
    process.close()

    assert kernel.closed == calls and process._handles == []
    assert scope.terminations == 1 and scope.sealed
    assert kernel.direct_terminations == [process._process]


def test_changed_binding_cleanup_never_terminates_job_but_still_stops_owned_root(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, scope = setup
    process.prepare(specification())
    scope.binding = binding()

    with pytest.raises(ProcessScopeUnavailable):
        process.close()

    assert scope.terminations == 0
    assert kernel.direct_terminations == [process._process] and process._handles == []


def test_supplier_swap_never_redirects_seal_or_job_termination(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, original = setup
    process.prepare(specification())
    replacement = Scope()
    process._scope = cast(WindowsJobObject, replacement)

    with pytest.raises(ProcessScopeUnavailable):
        process.close()

    assert original.sealed and not replacement.sealed
    assert original.terminations == 0 and replacement.terminations == 0
    assert kernel.direct_terminations == [process._process]


def test_job_termination_failure_preserves_uncertainty_despite_owned_root_cleanup(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, scope = setup
    process.prepare(specification())
    scope.fail = "terminate"

    with pytest.raises(ProcessScopeUnavailable):
        process.close()

    assert scope.terminations == 1 and kernel.direct_terminations == [process._process]
    assert process._handles == []


def test_close_failure_retains_owned_handle_inventory_for_later_cleanup(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, _ = setup
    process.prepare(specification())
    original = tuple(process._handles)
    kernel.fail = "close"

    with pytest.raises(ProcessScopeUnavailable):
        process.close()

    assert tuple(process._handles) == original
    kernel.fail = ""
    process.close()
    assert process._handles == []


def test_presealed_process_never_allocates_or_creates_child(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, _ = setup
    process.seal()

    with pytest.raises(ProcessScopeUnavailable):
        process.prepare(specification())

    assert kernel.command is None and kernel.resumes == 0 and kernel.next_handle == 10


def test_seal_during_native_create_is_nonblocking_and_prevents_attach_resume(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, scope = setup
    entered, release = Event(), Event()
    original = kernel.create

    def create(*args: Any) -> int:
        result = original(*args)
        entered.set()
        if not release.wait(5):
            raise RuntimeError("Fixture process creation did not settle.")
        return result

    kernel.CreateProcessW.body = create
    with ThreadPoolExecutor(max_workers=1) as executor:
        operation = executor.submit(process.prepare, specification())
        assert entered.wait(5)
        try:
            process.seal()
            assert scope.sealed and kernel.resumes == 0
        finally:
            release.set()
        with pytest.raises(ProcessScopeUnavailable):
            operation.result(timeout=5)

    assert scope.assigned == [] and kernel.resumes == 0 and process._handles == []


def test_command_line_utf16_budget_fails_before_native_allocation(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, _ = setup
    spec = specification()
    object.__setattr__(spec, "arguments", ("x" * 8192,) * 5)

    with pytest.raises(ProcessScopeUnavailable):
        process.prepare(spec)

    assert kernel.command is None and kernel.next_handle == 10


def test_unknown_root_stop_retains_process_handles_for_later_owned_cleanup(
    setup: tuple[native.WindowsTrustedProcess, Kernel, Scope],
) -> None:
    process, kernel, _ = setup
    process.prepare(specification())
    owned_process_handles = (process._process, process._thread)
    kernel.fail = "terminate"

    with pytest.raises(ProcessScopeUnavailable):
        process.close()

    assert tuple(process._handles) == owned_process_handles
    kernel.fail = ""
    process.close()
    assert process._handles == []
