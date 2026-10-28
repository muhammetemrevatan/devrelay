"""Bounded local Docker HTTP transport and ownership-fenced container lifecycle."""

import asyncio
import base64
import hashlib
import json
import math
import re
import struct
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Literal
from uuid import UUID

import requests

MAX_INPUT_BYTES = 1024 * 1024
MAX_OUTPUT_BYTES = 1024 * 1024
_FAILURE = "The owned execution container is unavailable."


class ContainerUnavailable(Exception):
    """No verified settlement or permission to retry an uncertain operation."""


def _pairs(values: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in values:
        if key in result:
            raise ValueError(_FAILURE)
        result[key] = value
    return result


def decode_json(raw: bytes) -> Any:
    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError(_FAILURE)),
        )
    except ValueError, UnicodeError, RecursionError:
        raise ContainerUnavailable(_FAILURE) from None


@dataclass(frozen=True, slots=True)
class DockerResponse:
    status: int
    body: bytes


class DockerEngine:
    """Use SDK socket adapters without loading Docker config, auth or environment."""

    def __init__(
        self, endpoint: str = "npipe:////./pipe/dockerDesktopLinuxEngine"
    ) -> None:
        if endpoint not in (
            "npipe:////./pipe/dockerDesktopLinuxEngine",
            "npipe:////./pipe/docker_engine",
            "unix:///var/run/docker.sock",
        ):
            raise ValueError("An explicit local Docker endpoint is required.")
        self._endpoint = endpoint
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="docker")
        self._session: requests.Session | None = None
        self._closed = False
        self._pending: asyncio.Future[DockerResponse] | None = None
        self._last_failure_type: str | None = None
        self._close_task: asyncio.Task[None] | None = None

    def _connect(self) -> requests.Session:
        if self._session is None:
            session = requests.Session()
            session.trust_env = False
            if self._endpoint.startswith("npipe:") and sys.platform == "win32":
                from docker.transport import (
                    NpipeHTTPAdapter,
                )

                adapter = NpipeHTTPAdapter(
                    self._endpoint, timeout=5, pool_connections=1, max_pool_size=1
                )
            elif self._endpoint.startswith("unix:") and sys.platform != "win32":
                from docker.transport import (
                    UnixHTTPAdapter,
                )

                adapter = UnixHTTPAdapter(
                    self._endpoint.replace("unix:", "http+unix:"),
                    timeout=5,
                    pool_connections=1,
                    max_pool_size=1,
                )
            else:
                session.close()
                raise ContainerUnavailable(_FAILURE)
            session.mount("http+docker://", adapter)
            self._session = session
        return self._session

    def _request(
        self, method: str, path: str, body: bytes | None, maximum_bytes: int
    ) -> DockerResponse:
        started = time.monotonic()
        try:
            with self._connect().request(
                method,
                "http+docker://localhost/v1.47" + path,
                data=body,
                headers={"Content-Type": "application/json"},
                timeout=(5, 30 if path.startswith("/containers/create?") else 15),
                stream=True,
                allow_redirects=False,
            ) as response:
                length = response.headers.get("Content-Length")
                if length is not None and (
                    not length.isascii()
                    or not length.isdecimal()
                    or int(length) > maximum_bytes
                ):
                    raise ContainerUnavailable(_FAILURE)
                chunks = bytearray()
                for chunk in response.iter_content(16384):
                    if (
                        time.monotonic() - started >= 30
                        or len(chunks) + len(chunk) > maximum_bytes
                    ):
                        raise ContainerUnavailable(_FAILURE)
                    chunks.extend(chunk)
                return DockerResponse(response.status_code, bytes(chunks))
        except (requests.RequestException, OSError, ValueError) as error:
            self._last_failure_type = type(error).__name__
            raise ContainerUnavailable(_FAILURE) from None

    async def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        *,
        maximum_bytes: int = 4 * 1024**2,
    ) -> DockerResponse:
        if self._closed or self._pending is not None:
            raise ContainerUnavailable(_FAILURE)
        allowed = {
            "GET": r"/(?:info|version|images/sha256:[0-9a-f]{64}/json|volumes/[a-z][a-z0-9_-]{0,62}|containers/[0-9a-f]{64}/(?:json|logs\?stdout=1&stderr=1&follow=0))",
            "POST": r"/containers/(?:create\?name=devrelay-[0-9a-f-]{36}|[0-9a-f]{64}/(?:start|kill\?signal=KILL))",
            "DELETE": r"/containers/[0-9a-f]{64}",
        }
        if method not in allowed or not re.fullmatch(allowed[method], path):
            raise ContainerUnavailable(_FAILURE)
        if not 1 <= maximum_bytes <= 8 * 1024**2 or (
            body is not None and len(body) > 2 * 1024**2
        ):
            raise ContainerUnavailable(_FAILURE)
        future = asyncio.get_running_loop().run_in_executor(
            self._executor, self._request, method, path, body, maximum_bytes
        )
        self._pending = future
        try:
            return await asyncio.shield(future)
        finally:
            if future.done():
                self._pending = None
            else:

                def settled(done: asyncio.Future[DockerResponse]) -> None:
                    if not done.cancelled():
                        done.exception()
                    if self._pending is done:
                        self._pending = None

                future.add_done_callback(settled)

    async def aclose(self) -> None:
        self._closed = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        loop = asyncio.get_running_loop()

        def close() -> None:
            if self._session is not None:
                self._session.close()

        future = loop.run_in_executor(self._executor, close)
        try:
            await asyncio.shield(future)
        finally:
            await asyncio.to_thread(self._executor.shutdown, wait=True)


@dataclass(frozen=True, slots=True, kw_only=True)
class ContainerSpec:
    image: str
    execution_id: UUID
    invocation_id: UUID
    mode: Literal["probe", "code", "check"]
    workspace: str | None = None
    auth_volume: str | None = None

    def __post_init__(self) -> None:
        if (
            re.fullmatch(r"sha256:[0-9a-f]{64}", self.image) is None
            or not isinstance(self.execution_id, UUID)
            or not isinstance(self.invocation_id, UUID)
            or self.mode not in ("probe", "code", "check")
        ):
            raise ValueError(_FAILURE)
        if self.mode == "probe":
            if self.workspace is not None or self.auth_volume is not None:
                raise ValueError(_FAILURE)
        elif (
            self.workspace is None
            or not (
                PureWindowsPath(self.workspace).is_absolute()
                or PurePosixPath(self.workspace).is_absolute()
            )
            or ".." in self.workspace.replace("\\", "/").split("/")
            or self.workspace.replace("\\", "/").split("/")[-1]
            != str(self.execution_id)
            or any(char in self.workspace for char in ("\x00", "\r", "\n"))
        ):
            raise ValueError(_FAILURE)
        if self.auth_volume is not None and (
            self.mode != "code"
            or re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", self.auth_volume) is None
        ):
            raise ValueError(_FAILURE)


@dataclass(frozen=True, slots=True)
class ContainerResult:
    container_id: str
    exit_code: int
    stdout: bytes
    stderr: bytes


def input_environment(payload: bytes) -> list[str]:
    if not 1 <= len(payload) <= MAX_INPUT_BYTES:
        raise ContainerUnavailable("Execution input exceeds the image profile limit.")
    encoded = base64.b64encode(payload).decode("ascii")
    chunks = [encoded[index : index + 32768] for index in range(0, len(encoded), 32768)]
    return [
        f"DEVRELAY_INPUT_COUNT={len(chunks)}",
        f"DEVRELAY_INPUT_SHA256={hashlib.sha256(payload).hexdigest()}",
        *(f"DEVRELAY_INPUT_{index:03d}={chunk}" for index, chunk in enumerate(chunks)),
    ]


def decode_logs(raw: bytes) -> tuple[bytes, bytes]:
    streams = {1: bytearray(), 2: bytearray()}
    offset = 0
    while offset < len(raw):
        if len(raw) - offset < 8:
            raise ContainerUnavailable(_FAILURE)
        stream, reserved, length = (
            raw[offset],
            raw[offset + 1 : offset + 4],
            struct.unpack(">I", raw[offset + 4 : offset + 8])[0],
        )
        offset += 8
        if (
            stream not in streams
            or reserved != b"\0\0\0"
            or length > MAX_OUTPUT_BYTES
            or offset + length > len(raw)
        ):
            raise ContainerUnavailable(_FAILURE)
        streams[stream].extend(raw[offset : offset + length])
        offset += length
        if sum(len(value) for value in streams.values()) > MAX_OUTPUT_BYTES:
            raise ContainerUnavailable("Execution output exceeded its finite limit.")
    return bytes(streams[1]), bytes(streams[2])


class OwnedContainer:
    def __init__(
        self,
        engine: DockerEngine,
        spec: ContainerSpec,
        require_current: Callable[[], None],
        clock: Callable[[], float],
    ) -> None:
        spec.__post_init__()
        self._engine, self._spec, self._guard, self._clock = (
            engine,
            spec,
            require_current,
            clock,
        )
        self._id: str | None = None
        self._used = self._sealed = False
        self._labels = {
            "devrelay.kind": spec.mode,
            "devrelay.execution": str(spec.execution_id),
            "devrelay.invocation": str(spec.invocation_id),
        }
        self._config: dict[str, Any] | None = None
        self._task: asyncio.Task[ContainerResult] | None = None
        self._removed = False
        self._deadline: float | None = None
        self._loop_deadline: float | None = None
        self._last_clock: float | None = None
        self._close_task: asyncio.Task[None] | None = None

    def seal(self) -> None:
        self._sealed = True

    def _require(self) -> None:
        task = asyncio.current_task()
        if self._sealed or (task is not None and task.cancelling()):
            raise ContainerUnavailable(_FAILURE)
        self._guard()
        now = self._clock()
        if (
            type(now) not in (int, float)
            or not math.isfinite(now)
            or now < 0
            or (self._last_clock is not None and now < self._last_clock)
        ):
            raise ContainerUnavailable(_FAILURE)
        self._last_clock = now
        if (self._deadline is not None and now >= self._deadline) or (
            self._loop_deadline is not None
            and asyncio.get_running_loop().time() >= self._loop_deadline
        ):
            raise ContainerUnavailable(
                "The execution container exceeded its time budget."
            )

    def _create_config(self, payload: bytes) -> dict[str, Any]:
        spec = self._spec
        mounts: list[dict[str, Any]] = []
        tmpfs = {
            "/tmp": "rw,nosuid,nodev,size=268435456,mode=1777",
            "/runtime/private": "rw,nosuid,nodev,size=1048576,uid=10001,gid=10001,mode=700",
        }
        if spec.workspace is not None:
            mounts.append(
                {
                    "Type": "bind",
                    "Source": spec.workspace,
                    "Target": "/workspace",
                    "ReadOnly": spec.mode == "check",
                    "BindOptions": {"Propagation": "rprivate"},
                }
            )
        else:
            tmpfs["/workspace"] = (
                "rw,nosuid,nodev,size=8388608,uid=10001,gid=10001,mode=700"
            )
        if spec.auth_volume is not None:
            mounts.append(
                {
                    "Type": "volume",
                    "Source": spec.auth_volume,
                    "Target": "/runtime/auth",
                    "ReadOnly": False,
                }
            )
        else:
            tmpfs["/runtime/auth"] = (
                "rw,nosuid,nodev,size=67108864,uid=10001,gid=10001,mode=700"
            )
        return {
            "Image": spec.image,
            "User": "10001:10001",
            "WorkingDir": "/workspace",
            "Entrypoint": ["/opt/deps/.venv/bin/python", "/opt/devrelay/entry.py"],
            "Cmd": [],
            "Env": input_environment(payload),
            "Labels": self._labels,
            "Tty": False,
            "OpenStdin": False,
            "Healthcheck": {"Test": ["NONE"]},
            "HostConfig": {
                "NetworkMode": "bridge" if spec.mode == "code" else "none",
                "ReadonlyRootfs": True,
                "Privileged": False,
                "CapDrop": ["ALL"],
                "SecurityOpt": ["no-new-privileges"],
                "Init": True,
                "Memory": 2 * 1024**3,
                "MemorySwap": 2 * 1024**3,
                "NanoCpus": 2_000_000_000,
                "PidsLimit": 128,
                "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
                "AutoRemove": False,
                "Mounts": mounts,
                "Tmpfs": tmpfs,
                "LogConfig": {
                    "Type": "local",
                    "Config": {"max-size": "2m", "max-file": "1", "compress": "false"},
                },
            },
        }

    async def _inspect(self) -> dict[str, Any]:
        if self._id is None:
            raise ContainerUnavailable(_FAILURE)
        response = await self._engine.request("GET", f"/containers/{self._id}/json")
        value = decode_json(response.body)
        if (
            response.status != 200
            or not isinstance(value, dict)
            or value.get("Id") != self._id
            or value.get("Image") != self._spec.image
        ):
            raise ContainerUnavailable(_FAILURE)
        config, host = value.get("Config", {}), value.get("HostConfig", {})
        expected = self._config
        if (
            expected is None
            or not isinstance(config, dict)
            or not isinstance(host, dict)
            or config.get("Labels") != self._labels
            or config.get("Cmd") not in (None, [])
            or config.get("Healthcheck") != {"Test": ["NONE"]}
        ):
            raise ContainerUnavailable(_FAILURE)
        for key in ("User", "WorkingDir", "Entrypoint", "Tty", "OpenStdin"):
            if config.get(key) != expected[key]:
                raise ContainerUnavailable(_FAILURE)
        expected_env = expected["Env"]
        actual_env = config.get("Env")
        if (
            not isinstance(actual_env, list)
            or any(type(item) is not str for item in actual_env)
            or [item for item in actual_env if item.startswith("DEVRELAY_INPUT_")]
            != expected_env
        ):
            raise ContainerUnavailable(_FAILURE)
        for key in (
            "NetworkMode",
            "ReadonlyRootfs",
            "Privileged",
            "CapDrop",
            "SecurityOpt",
            "Init",
            "Memory",
            "MemorySwap",
            "NanoCpus",
            "PidsLimit",
            "AutoRemove",
            "RestartPolicy",
            "Tmpfs",
            "LogConfig",
        ):
            if host.get(key) != expected["HostConfig"][key]:
                raise ContainerUnavailable(_FAILURE)
        for key in (
            "CapAdd",
            "Binds",
            "Devices",
            "ExtraHosts",
            "PortBindings",
            "VolumesFrom",
            "DeviceRequests",
            "DeviceCgroupRules",
            "GroupAdd",
            "Sysctls",
            "PidMode",
            "UTSMode",
            "UsernsMode",
            "CgroupParent",
        ):
            if host.get(key):
                raise ContainerUnavailable(_FAILURE)
        if host.get("IpcMode") != "private" or host.get("CgroupnsMode") != "private":
            raise ContainerUnavailable(_FAILURE)
        actual_mounts = value.get("Mounts")
        if not isinstance(actual_mounts, list) or len(actual_mounts) != len(
            expected["HostConfig"]["Mounts"]
        ):
            raise ContainerUnavailable(_FAILURE)
        if any(not isinstance(item, dict) for item in actual_mounts):
            raise ContainerUnavailable(_FAILURE)
        by_target = {item.get("Destination"): item for item in actual_mounts}
        if len(by_target) != len(actual_mounts):
            raise ContainerUnavailable(_FAILURE)
        for wanted in expected["HostConfig"]["Mounts"]:
            actual = by_target.get(wanted["Target"], {})
            source = (
                actual.get("Name")
                if wanted["Type"] == "volume"
                else actual.get("Source")
            )
            if (
                actual.get("Type") != wanted["Type"]
                or source != wanted["Source"]
                or actual.get("Destination") != wanted["Target"]
                or actual.get("RW") is not (not wanted["ReadOnly"])
                or (
                    wanted["Type"] == "bind" and actual.get("Propagation") != "rprivate"
                )
            ):
                raise ContainerUnavailable(_FAILURE)
        return value

    async def _run(self, payload: bytes, timeout_seconds: float) -> ContainerResult:
        self._require()
        assert self._last_clock is not None
        self._deadline = self._last_clock + timeout_seconds
        self._loop_deadline = asyncio.get_running_loop().time() + timeout_seconds
        self._config = self._create_config(payload)
        response = await self._engine.request(
            "POST",
            f"/containers/create?name=devrelay-{self._spec.invocation_id}",
            json.dumps(self._config, separators=(",", ":")).encode(),
        )
        if response.status != 201:
            raise ContainerUnavailable(_FAILURE)
        value = decode_json(response.body)
        original_id = value.get("Id") if isinstance(value, dict) else None
        if (
            not isinstance(original_id, str)
            or re.fullmatch(r"[0-9a-f]{64}", original_id) is None
        ):
            raise ContainerUnavailable(_FAILURE)
        self._id = original_id
        self._require()
        await self._inspect()
        self._require()
        response = await self._engine.request("POST", f"/containers/{self._id}/start")
        if response.status != 204:
            raise ContainerUnavailable(_FAILURE)
        while True:
            self._require()
            state = (await self._inspect()).get("State", {})
            self._require()
            if (
                state.get("Running") is False
                and type(state.get("Pid")) is int
                and state.get("Pid") == 0
                and state.get("Status") == "exited"
                and state.get("Dead") is False
                and state.get("Restarting") is False
            ):
                code = state.get("ExitCode")
                if (
                    type(code) is not int
                    or not 0 <= code <= 255
                    or state.get("OOMKilled") is not False
                    or state.get("Error")
                ):
                    raise ContainerUnavailable(_FAILURE)
                break
            await asyncio.sleep(0.1)
        response = await self._engine.request(
            "GET",
            f"/containers/{self._id}/logs?stdout=1&stderr=1&follow=0",
            maximum_bytes=2 * MAX_OUTPUT_BYTES,
        )
        if response.status != 200:
            raise ContainerUnavailable(_FAILURE)
        stdout, stderr = decode_logs(response.body)
        final = (await self._inspect()).get("State", {})
        self._require()
        if final != state:
            raise ContainerUnavailable(_FAILURE)
        response = await self._engine.request("DELETE", f"/containers/{self._id}")
        if response.status != 204:
            raise ContainerUnavailable(_FAILURE)
        self._removed = True
        return ContainerResult(self._id, code, stdout, stderr)

    async def run(
        self, payload: bytes, *, timeout_seconds: float = 300
    ) -> ContainerResult:
        if (
            self._used
            or type(timeout_seconds) not in (int, float)
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 3600
        ):
            raise ContainerUnavailable(_FAILURE)
        self._used = True
        self._task = asyncio.create_task(self._run(payload, timeout_seconds))
        try:
            return await asyncio.shield(self._task)
        except BaseException:
            self.seal()
            # The owning close drains the original creator/start operation first.
            raise

    async def aclose(self) -> None:
        self.seal()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        if self._task is not None:
            try:
                await asyncio.shield(self._task)
            except Exception, asyncio.CancelledError:
                pass
        if self._id is not None and not self._removed:
            value = await self._inspect()
            state = value.get("State", {})
            if state.get("Running") is True:
                response = await self._engine.request(
                    "POST", f"/containers/{self._id}/kill?signal=KILL"
                )
                if response.status not in (204, 409):
                    raise ContainerUnavailable(_FAILURE)
                for _ in range(30):
                    state = (await self._inspect()).get("State", {})
                    if state.get("Running") is False and state.get("Pid") == 0:
                        break
                    await asyncio.sleep(0.1)
                else:
                    raise ContainerUnavailable(_FAILURE)
            if (
                state.get("Running") is not False
                or type(state.get("Pid")) is not int
                or state.get("Pid") != 0
                or state.get("Dead") is not False
                or state.get("Restarting") is not False
                or state.get("Status") not in ("created", "exited")
            ):
                raise ContainerUnavailable(_FAILURE)
            response = await self._engine.request("DELETE", f"/containers/{self._id}")
            if response.status not in (204, 404):
                raise ContainerUnavailable(_FAILURE)
            self._removed = True
