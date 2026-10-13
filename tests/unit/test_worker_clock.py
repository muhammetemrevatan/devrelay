import ctypes
import sys
from collections.abc import Callable
from typing import Any

import pytest

from devrelay.application.lease_deadlines import LeaseDeadlineRejected
from devrelay.infrastructure.worker_state import clock

_FAILURE = "Windows lease clock is unavailable."


def fake_counter(monkeypatch: pytest.MonkeyPatch, values: list[Any]) -> None:
    readings = iter(values)
    monkeypatch.setattr(clock, "_load_counter", lambda: lambda: next(readings))


@pytest.mark.parametrize(
    "milliseconds", [0, 1, 16, 1000, 123456789, (2**53 - 1) * 1000]
)
def test_clock_converts_bounded_uint64_milliseconds_to_seconds(
    monkeypatch: pytest.MonkeyPatch, milliseconds: int
) -> None:
    fake_counter(monkeypatch, [milliseconds])
    timer = clock.WindowsLeaseClock()

    actual = timer()

    assert type(actual) is float
    assert actual == milliseconds / 1000


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        -1,
        1.0,
        "1000",
        None,
        float("nan"),
        float("inf"),
        2**64,
        (2**53 - 1) * 1000 + 1,
        10**1000,
    ],
)
def test_clock_rejects_invalid_native_readings_without_leaking_values(
    monkeypatch: pytest.MonkeyPatch, value: Any
) -> None:
    fake_counter(monkeypatch, [value])
    timer = clock.WindowsLeaseClock()

    with pytest.raises(
        LeaseDeadlineRejected, match="^Windows lease clock is unavailable\\.$"
    ) as error:
        timer()

    assert str(error.value) == _FAILURE
    assert error.value.__cause__ is None


def test_equal_ticks_are_valid_at_native_timer_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_counter(monkeypatch, [1000, 1000])
    timer = clock.WindowsLeaseClock()
    first = timer()

    repeated = timer()

    assert first == repeated == 1.0


@pytest.mark.parametrize(
    "first,second", [(1000, 999), ((2**53 - 1) * 1000, (2**53 - 1) * 1000 - 1)]
)
def test_backwards_raw_ticks_fail_even_if_float_rounding_hides_the_difference(
    monkeypatch: pytest.MonkeyPatch, first: int, second: int
) -> None:
    fake_counter(monkeypatch, [first, second])
    timer = clock.WindowsLeaseClock()
    timer()

    with pytest.raises(
        LeaseDeadlineRejected, match="^Windows lease clock is unavailable\\.$"
    ):
        timer()


def test_forward_jump_spends_elapsed_sleep_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_counter(monkeypatch, [1000, 301000])
    timer = clock.WindowsLeaseClock()
    before = timer()

    after = timer()

    assert after - before == 300.0


def test_native_call_failure_is_sanitized(monkeypatch: pytest.MonkeyPatch) -> None:
    def failure() -> int:
        raise OSError("sensitive native details")

    monkeypatch.setattr(clock, "_load_counter", lambda: failure)
    timer = clock.WindowsLeaseClock()

    with pytest.raises(LeaseDeadlineRejected) as error:
        timer()

    assert str(error.value) == _FAILURE
    assert error.value.__cause__ is None


def test_loader_failure_is_sanitized(monkeypatch: pytest.MonkeyPatch) -> None:
    def failure() -> Callable[[], int]:
        raise OSError("sensitive library path")

    monkeypatch.setattr(clock, "_load_counter", failure)

    with pytest.raises(LeaseDeadlineRejected) as error:
        clock.WindowsLeaseClock()

    assert str(error.value) == _FAILURE
    assert error.value.__cause__ is None


def test_non_windows_constructor_fails_without_loading_dll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def loader(*args: object, **kwargs: object) -> None:
        calls.append("native_load")

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(ctypes, "WinDLL", loader, raising=False)

    with pytest.raises(LeaseDeadlineRejected) as error:
        clock.WindowsLeaseClock()

    assert str(error.value) == _FAILURE
    assert calls == []


def test_missing_windows_loader_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.delattr(ctypes, "WinDLL", raising=False)

    with pytest.raises(LeaseDeadlineRejected) as error:
        clock.WindowsLeaseClock()

    assert str(error.value) == _FAILURE


def test_native_function_binding_uses_only_system32_and_uint64_no_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Counter:
        argtypes: list[object] | None = None
        restype: object = None

        def __call__(self) -> int:
            return 1000

    class Kernel:
        GetTickCount64 = Counter()

    loads: list[tuple[str, dict[str, object]]] = []

    def loader(name: str, **kwargs: object) -> Kernel:
        loads.append((name, kwargs))
        return Kernel()

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(ctypes, "WinDLL", loader, raising=False)

    timer = clock.WindowsLeaseClock()

    assert loads == [("Kernel32.dll", {"use_last_error": True, "winmode": 0x800})]
    assert Kernel.GetTickCount64.argtypes == []
    assert Kernel.GetTickCount64.restype is ctypes.c_ulonglong
    assert timer._last_milliseconds is None


def test_missing_native_function_fails_with_fixed_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(
        ctypes, "WinDLL", lambda *args, **kwargs: object(), raising=False
    )

    with pytest.raises(LeaseDeadlineRejected) as error:
        clock.WindowsLeaseClock()

    assert str(error.value) == _FAILURE
