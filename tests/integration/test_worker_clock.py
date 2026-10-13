import sys
from math import isfinite

import pytest

from devrelay.infrastructure.worker_state.clock import WindowsLeaseClock


@pytest.mark.skipif(
    sys.platform != "win32", reason="Requires native Windows GetTickCount64."
)
def test_native_windows_clock_returns_bounded_nondecreasing_elapsed_seconds() -> None:
    timer = WindowsLeaseClock()

    readings = [timer() for _ in range(32)]

    assert all(
        type(value) is float and isfinite(value) and 0 <= value <= 2**53 - 1
        for value in readings
    )
    assert readings == sorted(readings)


@pytest.mark.skipif(
    sys.platform != "win32", reason="Requires native Windows GetTickCount64."
)
def test_native_instances_share_system_uptime_without_process_relative_reset() -> None:
    first, second = WindowsLeaseClock(), WindowsLeaseClock()
    before = first()

    observed = second()

    assert observed >= before
    assert observed > 0
