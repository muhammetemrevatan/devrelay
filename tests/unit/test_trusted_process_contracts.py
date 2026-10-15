from dataclasses import replace
from typing import Any
from uuid import UUID

import pytest

from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.trusted_processes import (
    TRUSTED_PROCESS_READ_SIZE,
    TrustedProcessObservation,
    TrustedProcessPolicy,
    TrustedProcessResult,
    TrustedProcessSpec,
    TrustedProcessStopReason,
)


def spec(**changes: Any) -> TrustedProcessSpec:
    values: dict[str, Any] = {
        "executable": r"C:\trusted\fixture.exe",
        "arguments": ("literal argument", "", "--fixture"),
        "working_directory": r"C:\workspaces\fixture",
        "environment": (("FIXTURE", "literal value"),),
    }
    values.update(changes)
    return TrustedProcessSpec(**values)


@pytest.mark.parametrize("field", ["executable", "working_directory"])
@pytest.mark.parametrize(
    "value",
    [
        "",
        "relative.exe",
        r"C:relative.exe",
        r"\rooted.exe",
        r"\\server\share\fixture.exe",
        r"\\?\C:\fixture.exe",
        r"C:\trusted\..\fixture.exe",
        r"C:\trusted\.\fixture.exe",
        r"C:\trusted\fixture.exe:stream",
        r"C:\trusted\*.exe",
        "C:\\fixture\x00.exe",
        "C:\\" + "x" * 16379,
    ],
)
def test_process_paths_require_bounded_explicit_local_windows_locations(
    field: str,
    value: str,
) -> None:
    with pytest.raises(ValueError):
        spec(**{field: value})


@pytest.mark.parametrize(
    "arguments",
    [["fixture"], ("x",) * 65, ("x" * 8193,), ("nul\x00argument",), (1,)],
)
def test_process_arguments_reject_unbounded_or_implicit_values(arguments: Any) -> None:
    with pytest.raises(ValueError):
        spec(arguments=arguments)


@pytest.mark.parametrize(
    "environment",
    [
        [("KEY", "value")],
        (("KEY", "value"),) * 65,
        (("Key", "first"), ("KEY", "second")),
        (("", "value"),),
        (("A=B", "value"),),
        (("A\x00B", "value"),),
        (("KEY", "value\x00suffix"),),
        (("KEY", 1),),
        ((1, "value"),),
        (("KEY",),),
        (["KEY", "value"],),
    ],
)
def test_process_environment_requires_explicit_unique_bounded_pairs(
    environment: Any,
) -> None:
    with pytest.raises(ValueError):
        spec(environment=environment)


def test_environment_budget_counts_utf16_units_and_final_terminator() -> None:
    accepted = spec(environment=(("K", "x" * 32763),))

    assert accepted.environment[0][1] == "x" * 32763
    with pytest.raises(ValueError):
        spec(environment=(("K", "x" * 32764),))
    with pytest.raises(ValueError):
        spec(environment=(("K", "\U0001f680" * 16382),))


@pytest.mark.parametrize(
    "arguments",
    [("x" * 8192,) * 4, ("\U0001f680" * 8192,) * 2, ('"' * 8192,) * 2],
)
def test_total_command_budget_includes_utf16_encoding_and_windows_quoting(
    arguments: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match="Windows limit"):
        spec(arguments=arguments)


def test_empty_environment_is_explicit_and_literal_arguments_are_preserved() -> None:
    arguments = ("", "space inside", 'literal"quote', r"trailing\\", "$(fixture)")
    command = spec(arguments=arguments, environment=())

    assert command.arguments is arguments
    assert command.environment == ()


def test_process_spec_and_output_repr_do_not_disclose_command_or_capture() -> None:
    command = spec(
        arguments=("fixture-sensitive-argument",),
        environment=(("TOKEN", "fixture-sensitive-environment"),),
    )
    observation = TrustedProcessObservation(
        exit_code=None,
        stdout=b"fixture-sensitive-stdout",
        stderr=b"fixture-sensitive-stderr",
        stdout_eof=False,
        stderr_eof=False,
    )
    result = TrustedProcessResult(
        execution_id=UUID(int=12),
        reason=TrustedProcessStopReason.EXITED,
        exit_code=0,
        stdout=observation.stdout,
        stderr=observation.stderr,
        stop_status=ProcessStopStatus.EMPTY_VERIFIED,
    )

    for value in (command, observation, result):
        assert "fixture-sensitive" not in repr(value)
    assert command.executable not in repr(command)
    assert command.working_directory not in repr(command)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("exit_code", True),
        ("exit_code", -1),
        ("exit_code", 2**32),
        ("stdout", bytearray(b"value")),
        ("stderr", "value"),
        ("stdout", b"x" * (TRUSTED_PROCESS_READ_SIZE + 1)),
        ("stderr", b"x" * (TRUSTED_PROCESS_READ_SIZE + 1)),
        ("stdout_eof", 1),
        ("stderr_eof", 0),
    ],
)
def test_observation_is_typed_bounded_native_evidence(field: str, value: Any) -> None:
    valid = TrustedProcessObservation(
        exit_code=0,
        stdout=b"",
        stderr=b"",
        stdout_eof=True,
        stderr_eof=True,
    )

    with pytest.raises(ValueError):
        replace(valid, **{field: value})


@pytest.mark.parametrize(
    "field",
    [
        "maximum_runtime_seconds",
        "operation_timeout_seconds",
        "stop_timeout_seconds",
        "monitor_interval_seconds",
    ],
)
@pytest.mark.parametrize("value", [True, 0, -1, float("nan"), float("inf"), 10**1000])
def test_process_timing_policy_requires_finite_typed_bounds(
    field: str, value: Any
) -> None:
    with pytest.raises(ValueError):
        TrustedProcessPolicy(**{field: value})


@pytest.mark.parametrize("field", ["maximum_observations", "maximum_output_bytes"])
@pytest.mark.parametrize("value", [True, 0, -1, 1.0, 10**1000])
def test_process_counters_require_bounded_positive_integers(
    field: str, value: Any
) -> None:
    with pytest.raises(ValueError):
        TrustedProcessPolicy(**{field: value})


def test_result_enforces_one_shared_stdout_stderr_budget() -> None:
    result = TrustedProcessResult(
        execution_id=UUID(int=12),
        reason=TrustedProcessStopReason.OUTPUT_LIMIT,
        exit_code=None,
        stdout=b"x" * (1024 * 1024 - 1),
        stderr=b"y",
        stop_status=ProcessStopStatus.UNKNOWN,
    )

    with pytest.raises(ValueError):
        replace(result, stderr=b"yz")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("execution_id", "id"),
        ("reason", "exited"),
        ("stop_status", "unknown"),
        ("exit_code", False),
        ("exit_code", -1),
        ("exit_code", 2**32),
        ("stdout", bytearray()),
        ("stderr", ""),
    ],
)
def test_results_do_not_coerce_untyped_process_evidence(field: str, value: Any) -> None:
    valid = TrustedProcessResult(
        execution_id=UUID(int=12),
        reason=TrustedProcessStopReason.EXITED,
        exit_code=0,
        stdout=b"",
        stderr=b"",
        stop_status=ProcessStopStatus.EMPTY_VERIFIED,
    )

    with pytest.raises(ValueError):
        replace(valid, **{field: value})
