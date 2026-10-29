import ctypes
import json
import runpy
from pathlib import Path
from typing import Any

import pytest


@pytest.mark.parametrize(
    "responses, expected",
    [
        ([(0, 0), (0, 0)], {"version": 1, "available": True, "errno": 0}),
        ([(-1, 1)], {"version": 1, "available": False, "errno": 1}),
        ([(0, 0), (-1, 13)], {"version": 1, "available": False, "errno": 13}),
    ],
)
def test_kernel_observation_is_independent_and_never_a_readiness_receipt(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    responses: list[tuple[int, int]],
    expected: dict[str, object],
) -> None:
    module = runpy.run_path(
        str(Path(__file__).resolve().parents[2] / "worker-runtime" / "entry.py")
    )
    calls: list[int] = []

    class Unshare:
        argtypes: Any = None
        restype: Any = None

        def __call__(self, flags: int) -> int:
            calls.append(flags)
            result, error = responses[len(calls) - 1]
            ctypes.set_errno(error)
            return result

    class Library:
        unshare = Unshare()

    monkeypatch.setattr(module["ctypes"], "CDLL", lambda *args, **kwargs: Library())
    assert module["probe"]({"phase": "kernel_namespace"}) == 0
    value = json.loads(capsys.readouterr().out)
    assert value == expected
    assert "accepted" not in value and "ready" not in value
    assert calls == [0x10000000, 0x60020000][: len(responses)]


@pytest.mark.parametrize("result,error", [(-1, 12), (-1, 0), (2, 0)])
def test_unknown_syscall_failure_is_not_classified_as_expected_denial(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    result: int,
    error: int,
) -> None:
    module = runpy.run_path(
        str(Path(__file__).resolve().parents[2] / "worker-runtime" / "entry.py")
    )

    class Unshare:
        argtypes: Any = None
        restype: Any = None

        def __call__(self, flags: int) -> int:
            ctypes.set_errno(error)
            return result

    class Library:
        unshare = Unshare()

    monkeypatch.setattr(module["ctypes"], "CDLL", lambda *args, **kwargs: Library())
    with pytest.raises(ValueError, match="Unexpected namespace"):
        module["probe"]({"phase": "kernel_namespace"})
    assert not capsys.readouterr().out
