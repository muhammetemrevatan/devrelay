"""Fixed container entry; trusted input and OS policy stay outside task control."""

import base64
import hashlib
import json
import os
import re
import secrets
import socket
import struct
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any
from uuid import UUID

MAX_INPUT = 1024 * 1024
MAX_OUTPUT = 1024 * 1024


def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in values:
        if key in result:
            raise ValueError("Duplicate execution input field.")
        result[key] = value
    return result


def read_input() -> dict[str, Any]:
    count = os.environ.get("DEVRELAY_INPUT_COUNT", "")
    if not count.isdecimal() or not 1 <= int(count) <= 44:
        raise ValueError("Invalid execution input count.")
    names = [f"DEVRELAY_INPUT_{index:03d}" for index in range(int(count))]
    allowed = {*names, "DEVRELAY_INPUT_COUNT", "DEVRELAY_INPUT_SHA256"}
    if {key for key in os.environ if key.startswith("DEVRELAY_INPUT_")} != allowed:
        raise ValueError("Unexpected execution input chunks.")
    raw = base64.b64decode("".join(os.environ[name] for name in names), validate=True)
    if (
        not 1 <= len(raw) <= MAX_INPUT
        or hashlib.sha256(raw).hexdigest() != os.environ["DEVRELAY_INPUT_SHA256"]
    ):
        raise ValueError("Execution input differs from its digest.")
    for name in allowed:
        del os.environ[name]
    value = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(
            ValueError("Invalid JSON number.")
        ),
    )
    common = {
        "version",
        "mode",
        "execution_id",
        "invocation_id",
        "cycle",
        "timeout_seconds",
    }
    additions = {
        "probe": {"phase"},
        "code": {"prompt", "model"},
        "check": {"argv", "cwd"},
    }
    mode = value.get("mode")
    if (
        mode not in additions
        or set(value) != common | additions[mode]
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError("Invalid execution input schema.")
    for name in ("execution_id", "invocation_id"):
        if str(UUID(value[name])) != value[name]:
            raise ValueError("Invalid execution input identifier.")
    if (
        type(value["cycle"]) is not int
        or not 0 <= value["cycle"] <= 5
        or type(value["timeout_seconds"]) not in (int, float)
        or not 0 < value["timeout_seconds"] <= 3600
    ):
        raise ValueError("Invalid execution budget.")
    return value


def overrides(phase: str) -> list[str]:
    filesystem = {
        ":root": "deny",
        ":minimal": "read",
        "/usr": "read",
        "/lib": "read",
        "/lib64": "read",
        "/etc": "read",
        "/opt": "read",
        "/tmp": "write",
        "/workspace": "write" if phase == "code" else "read",
        "/runtime/auth": "deny",
        "/runtime/private": "deny",
        "/proc": "deny",
    }
    values = {
        "default_permissions": '"devrelay"',
        "permissions.devrelay.extends": json.dumps(
            ":workspace" if phase == "code" else ":read-only"
        ),
        "permissions.devrelay.filesystem": "{"
        + ",".join(
            json.dumps(key) + "=" + json.dumps(value)
            for key, value in filesystem.items()
        )
        + "}",
        "permissions.devrelay.network.enabled": "false",
        'projects."/workspace".trust_level': '"untrusted"',
        "features.hooks": "false",
        "features.plugins": "false",
        "features.remote_plugin": "false",
        "features.multi_agent": "false",
        "features.goals": "false",
        "features.js_repl": "false",
        "features.skill_mcp_dependency_install": "false",
        "apps._default.enabled": "false",
        "web_search": '"disabled"',
        "project_doc_max_bytes": "0",
        "shell_environment_policy.inherit": '"none"',
    }
    return [part for key, value in values.items() for part in ("-c", key + "=" + value)]


def environment() -> dict[str, str]:
    return {
        "PATH": "/opt/deps/.venv/bin:/usr/local/bin:/usr/bin:/bin",
        "HOME": "/tmp/home",
        "CODEX_HOME": "/runtime/auth",
        "TMPDIR": "/tmp",
        "PYTHONUNBUFFERED": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "UV_CACHE_DIR": "/tmp/uv-cache",
        "UV_PYTHON_DOWNLOADS": "never",
        "LANG": "C.UTF-8",
    }


def bounded(
    command: list[str],
    *,
    timeout: float,
    data: bytes | None = None,
    extra_env: dict[str, str] | None = None,
    maximum_output_bytes: int = MAX_OUTPUT,
) -> tuple[int, bytes, bytes]:
    env = environment()
    env.update(extra_env or {})
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        close_fds=True,
    )
    output = [bytearray(), bytearray()]
    failed: list[str] = []
    lock = threading.Lock()

    def consume(index: int) -> None:
        stream = process.stdout if index == 0 else process.stderr
        assert stream is not None
        while chunk := stream.read(16384):
            with lock:
                if sum(map(len, output)) + len(chunk) > maximum_output_bytes:
                    failed.append("output")
                    process.kill()
                    return
                output[index].extend(chunk)

    readers = [
        threading.Thread(target=consume, args=(index,), daemon=True) for index in (0, 1)
    ]
    for reader in readers:
        reader.start()
    try:
        if data is not None:
            assert process.stdin is not None
            process.stdin.write(data)
            process.stdin.close()
        code = process.wait(timeout=timeout)
        for reader in readers:
            reader.join(timeout=2)
        if failed or any(reader.is_alive() for reader in readers):
            raise ValueError("Execution output is incomplete or oversized.")
        return code, bytes(output[0]), bytes(output[1])
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2)


CANARY = r"""
import errno,json,os,socket,sys
from pathlib import Path
result={"uid":os.getuid(),"cwd":os.getcwd()}
for label in ("private","auth"):
 p=Path('/runtime/'+label+'/canary.txt')
 for operation in ('read','write'):
  try:
   if operation=='read':p.read_bytes()
   else:p.write_bytes(b'FAIL')
  except OSError as e:result[label+'_'+operation]=e.errno in (errno.EACCES,errno.EPERM,errno.ENOENT,errno.EROFS)
  else:result[label+'_'+operation]=False
try:Path('/workspace/positive.txt').write_text('positive');result['workspace_write']=True
except OSError:result['workspace_write']=False
try:socket.create_connection(('127.0.0.1',int(sys.argv[1])),timeout=1).close();result['network_denied']=False
except OSError:result['network_denied']=True
try:Path('/proc/'+sys.argv[2]+'/environ').read_bytes();result['parent_process_denied']=False
except OSError:result['parent_process_denied']=True
print(json.dumps(result))
"""


def run_fixed_check(
    argv: list[str], cwd: str, timeout: float
) -> tuple[int, bytes, bytes]:
    launch_nonce = secrets.token_hex(32)
    check = json.dumps(
        {"argv": argv, "cwd": cwd, "launch_nonce": launch_nonce}, separators=(",", ":")
    )
    if len(check.encode()) > 32768:
        raise ValueError("Fixed check exceeds the runtime profile limit.")
    extra = [
        "-c",
        "shell_environment_policy.set={"
        + ",".join(
            json.dumps(key) + "=" + json.dumps(value)
            for key, value in {
                **{
                    key: value
                    for key, value in environment().items()
                    if key != "CODEX_HOME"
                },
                "DEVRELAY_FIXED_CHECK": check,
            }.items()
        )
        + "}",
    ]
    command = [
        "codex",
        *overrides("check"),
        *extra,
        "sandbox",
        "--permission-profile",
        "devrelay",
        "--cd",
        "/workspace",
        "--",
        "/opt/deps/.venv/bin/python",
        "/opt/devrelay/check.py",
    ]
    code, stdout, stderr = bounded(
        command,
        timeout=timeout,
        extra_env={"DEVRELAY_FIXED_CHECK": check},
        maximum_output_bytes=MAX_OUTPUT - 256,
    )
    prefix = b"DEVRELAY_CHECK_SETTLED_V1:" + launch_nonce.encode("ascii") + b":"
    receipts = re.findall(re.escape(prefix) + rb"(-?[0-9]{1,3})\n", stdout)
    if (
        len(receipts) != 1
        or not 0 <= int(receipts[0]) <= 255
        or int(receipts[0]) != code
    ):
        raise ValueError("The fixed check did not reach verified command settlement.")
    return code, stdout.replace(prefix + receipts[0] + b"\n", b"", 1), stderr


def probe(spec: dict[str, Any]) -> int:
    from probe import hostile_project

    marker = hostile_project()
    phase = spec["phase"]
    if phase not in ("code", "check"):
        raise ValueError("Invalid probe phase.")
    for kind in ("private", "auth"):
        Path("/runtime/" + kind + "/canary.txt").write_bytes(b"NONSECRET_CANARY")
    Path("/workspace/positive.txt").write_text("original")
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(4)
    port = server.getsockname()[1]
    socket.create_connection(("127.0.0.1", port), timeout=1).close()
    command = [
        "codex",
        *overrides(phase),
        "sandbox",
        "--permission-profile",
        "devrelay",
        "--cd",
        "/workspace",
        "--",
        "/opt/deps/.venv/bin/python",
        "-c",
        CANARY,
        str(port),
        str(os.getpid()),
    ]
    try:
        if phase == "check":
            code, stdout, stderr = run_fixed_check(
                command[-5:], ".", min(spec["timeout_seconds"], 60)
            )
        else:
            code, stdout, stderr = bounded(
                command, timeout=min(spec["timeout_seconds"], 60)
            )
    finally:
        server.close()
    if code:
        sys.stderr.buffer.write(stderr)
        return 125
    result = json.loads(stdout)
    checks = {
        name: result.get(name) is True
        for name in (
            "private_read",
            "private_write",
            "auth_read",
            "auth_write",
            "network_denied",
            "parent_process_denied",
        )
    }
    checks["workspace_policy"] = result.get("workspace_write") is (phase == "code")
    checks["positive_read"] = result.get("cwd") == "/workspace"
    checks["files_unchanged"] = all(
        Path("/runtime/" + kind + "/canary.txt").read_bytes() == b"NONSECRET_CANARY"
        for kind in ("private", "auth")
    )
    checks["project_config_ignored"] = not marker.exists()
    if phase == "check":
        checks["check_command_admission"] = True
    events = ""
    if phase == "code":
        from probe import harness_probe

        harness, events = harness_probe(overrides, bounded, CANARY)
        checks.update(harness)
    print(
        json.dumps(
            {
                "version": 1,
                "phase": phase,
                "accepted": all(checks.values()),
                "checks": checks,
                "events": events,
            }
        )
    )
    return 0 if all(checks.values()) else 125


def main() -> int:
    spec = read_input()
    if spec["mode"] == "probe":
        return probe(spec)
    if spec["mode"] == "code":
        if (
            type(spec["prompt"]) is not str
            or len(spec["prompt"].encode("utf-8")) > MAX_INPUT
            or (
                spec["model"] is not None
                and (
                    type(spec["model"]) is not str
                    or re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", spec["model"])
                    is None
                )
            )
        ):
            raise ValueError("Invalid coding invocation.")
        command = [
            "codex",
            "-a",
            "never",
            "exec",
            "--strict-config",
            "--ignore-user-config",
            "--ignore-rules",
            "--ephemeral",
            "--skip-git-repo-check",
            "--json",
            "--color",
            "never",
            *overrides("code"),
        ]
        if spec["model"] is not None:
            command.extend(["-m", spec["model"]])
        command.append("-")
        code, stdout, stderr = bounded(
            command,
            timeout=spec["timeout_seconds"],
            data=spec["prompt"].encode("utf-8"),
        )
    else:
        argv, cwd = spec["argv"], spec["cwd"]
        if (
            type(argv) is not list
            or not 1 <= len(argv) <= 64
            or any(
                type(arg) is not str
                or not arg
                or "\0" in arg
                or len(arg.encode()) > 4096
                for arg in argv
            )
            or type(cwd) is not str
            or (
                cwd != "."
                and (
                    re.fullmatch(r"[a-zA-Z0-9_./-]+", cwd) is None
                    or any(part in ("", ".", "..") for part in cwd.split("/"))
                )
            )
        ):
            raise ValueError("Invalid fixed check.")
        code, stdout, stderr = run_fixed_check(argv, cwd, spec["timeout_seconds"])
        sys.stdout.buffer.write(
            b"DEVRELAY_CHECK_RESULT_V1\0"
            + struct.pack("!III", code, len(stdout), len(stderr))
            + stdout
            + stderr
        )
        return 0
    sys.stdout.buffer.write(stdout)
    sys.stderr.buffer.write(stderr)
    return code if 0 <= code <= 255 else 125


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        print("The configured execution profile did not complete.", file=sys.stderr)
        raise SystemExit(125) from None
