import ctypes
import sys
from collections.abc import Callable
from math import isfinite
from typing import cast

from devrelay.application.lease_deadlines import LeaseDeadlineRejected

_FAILURE = "Windows lease clock is unavailable."
_MAX_SECONDS = 2**53 - 1
_MAX_MILLISECONDS = min(2**64 - 1, _MAX_SECONDS * 1000)
_LOAD_LIBRARY_SEARCH_SYSTEM32 = 0x00000800


def _load_counter() -> Callable[[], int]:
    loader = getattr(ctypes, "WinDLL", None)
    if sys.platform != "win32" or loader is None:
        raise LeaseDeadlineRejected(_FAILURE)
    kernel = cast(
        ctypes.CDLL,
        loader(
            "Kernel32.dll",
            use_last_error=True,
            winmode=_LOAD_LIBRARY_SEARCH_SYSTEM32,
        ),
    )
    counter = kernel.GetTickCount64
    counter.argtypes = []
    counter.restype = ctypes.c_ulonglong
    return cast(Callable[[], int], counter)


class WindowsLeaseClock:
    """Elapsed seconds including sleep/hibernate, at the Windows system tick resolution.

    GetTickCount64 is unaffected by wall-clock adjustments. Native tests verify
    the binding and readings; they do not certify a forced suspend/resume cycle.
    Read this process-local clock on the worker's orchestration thread; its native
    loader may be initialized off-loop.
    """

    def __init__(self) -> None:
        self._last_milliseconds: int | None = None
        try:
            self._counter = _load_counter()
        except Exception:
            raise LeaseDeadlineRejected(_FAILURE) from None

    def __call__(self) -> float:
        try:
            milliseconds = self._counter()
            if (
                type(milliseconds) is not int
                or not 0 <= milliseconds <= _MAX_MILLISECONDS
                or (
                    self._last_milliseconds is not None
                    and milliseconds < self._last_milliseconds
                )
            ):
                raise LeaseDeadlineRejected(_FAILURE)
            seconds = milliseconds / 1000
            if not isfinite(seconds) or not 0 <= seconds <= _MAX_SECONDS:
                raise LeaseDeadlineRejected(_FAILURE)
        except Exception:
            raise LeaseDeadlineRejected(_FAILURE) from None
        self._last_milliseconds = milliseconds
        return seconds
