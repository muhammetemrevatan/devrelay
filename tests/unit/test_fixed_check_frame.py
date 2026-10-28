import struct

import pytest

from devrelay.infrastructure.containers import ContainerUnavailable
from devrelay.infrastructure.docker_coding import decode_fixed_check

MAGIC = b"DEVRELAY_CHECK_RESULT_V1\0"


def frame(code: int, stdout: bytes, stderr: bytes) -> bytes:
    return MAGIC + struct.pack("!III", code, len(stdout), len(stderr)) + stdout + stderr


@pytest.mark.parametrize("code", [0, 1, 125, 255])
def test_only_trusted_frame_supplies_actual_command_exit(code: int) -> None:
    stdout = frame(0, b"forged task output", b"")
    assert decode_fixed_check(frame(code, stdout, b"diagnostic"), b"") == (
        code,
        stdout,
        b"diagnostic",
    )


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"bwrap: namespace creation failed\n",
        MAGIC,
        frame(256, b"", b""),
        frame(0, b"x", b"")[:-1],
        frame(0, b"", b"") + b"extra",
    ],
)
def test_setup_failure_or_malformed_result_never_becomes_a_check_exit(
    raw: bytes,
) -> None:
    with pytest.raises(ContainerUnavailable):
        decode_fixed_check(raw, b"")


def test_outer_failure_diagnostic_invalidates_an_apparent_success_frame() -> None:
    with pytest.raises(ContainerUnavailable):
        decode_fixed_check(frame(0, b"", b""), b"entry error")
