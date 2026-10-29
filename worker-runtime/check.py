"""Execute the fixed check against the actual readonly candidate package."""

import ctypes
import json
import os
import subprocess
import sys
from pathlib import Path

spec = json.loads(os.environ.pop("DEVRELAY_FIXED_CHECK"))
launch_nonce = spec.pop("launch_nonce")
# The configured command and build backend must not inspect or alter the shim's
# private settlement nonce through same-UID ptrace/process_vm_readv access.
prctl = ctypes.CDLL(None, use_errno=True).prctl
prctl.argtypes = [
    ctypes.c_int,
    ctypes.c_ulong,
    ctypes.c_ulong,
    ctypes.c_ulong,
    ctypes.c_ulong,
]
prctl.restype = ctypes.c_int
if prctl(4, 0, 0, 0, 0) != 0 or prctl(3, 0, 0, 0, 0) != 0:
    raise SystemExit(125)
environment = dict(os.environ)
if (Path("/workspace") / "pyproject.toml").is_file():
    result = subprocess.run(
        [
            "/usr/local/bin/uv",
            "pip",
            "install",
            "--python",
            "/opt/deps/.venv/bin/python",
            "--offline",
            "--no-deps",
            "--no-build-isolation",
            "--target",
            "/tmp/candidate-site",
            "/workspace",
        ],
        stdin=subprocess.DEVNULL,
        env=environment,
        check=False,
    )
    if result.returncode:
        raise SystemExit(125)
    environment["PYTHONPATH"] = "/tmp/candidate-site"
else:
    environment["PYTHONPATH"] = "/workspace/src"
os.chdir("/workspace" + ("/" + spec["cwd"] if spec["cwd"] != "." else ""))
try:
    process = subprocess.Popen(
        spec["argv"],
        env=environment,
        stdin=subprocess.DEVNULL,
        close_fds=True,
    )
except OSError:
    raise SystemExit(125) from None
code = process.wait()
# Removed from the command environment. The nonce and read-side of this pipe
# remain unavailable to the command; only the trusted shim emits settlement.
sys.stdout.buffer.write(
    b"DEVRELAY_CHECK_SETTLED_V1:"
    + launch_nonce.encode("ascii")
    + b":"
    + str(code).encode("ascii")
    + b"\n"
)
sys.stdout.buffer.flush()
raise SystemExit(code if code >= 0 else 125)
