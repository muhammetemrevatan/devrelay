import asyncio
import json
import struct
from collections.abc import Callable
from copy import deepcopy
from time import monotonic
from typing import Any, Literal, cast
from uuid import uuid4

import pytest

from devrelay.infrastructure.containers import (
    ContainerSpec,
    ContainerUnavailable,
    DockerEngine,
    DockerResponse,
    OwnedContainer,
    decode_json,
    decode_logs,
    input_environment,
)

IMAGE = "sha256:" + "a" * 64
CID = "b" * 64


class Engine:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.config: dict[str, Any] = {}
        self.running = False
        self.mutate: Callable[[dict[str, Any]], None] = lambda value: None
        self.created = asyncio.Event()
        self.release: asyncio.Event | None = None
        self.exit = True
        self.fail_create = False

    async def request(
        self, method: str, path: str, body: bytes | None = None, **kwargs: object
    ) -> DockerResponse:
        self.calls.append((method, path))
        if path.startswith("/containers/create?"):
            assert body is not None
            self.config = json.loads(body)
            self.created.set()
            if self.release is not None:
                await self.release.wait()
            if self.fail_create:
                raise ContainerUnavailable("Unknown create reply.")
            return DockerResponse(201, json.dumps({"Id": CID}).encode())
        if path.endswith("/start"):
            self.running = True
            return DockerResponse(204, b"")
        if path.endswith("/kill?signal=KILL"):
            self.running = False
            return DockerResponse(204, b"")
        if method == "DELETE":
            assert not self.running
            return DockerResponse(204, b"")
        if "/logs?" in path:
            return DockerResponse(200, struct.pack("!B3sI", 1, b"\0" * 3, 4) + b"done")
        assert path == "/containers/" + CID + "/json"
        if self.exit:
            self.running = False
        host = deepcopy(self.config["HostConfig"])
        host.update(IpcMode="private", CgroupnsMode="private")
        value = {
            "Id": CID,
            "Image": IMAGE,
            "Config": deepcopy(self.config),
            "HostConfig": host,
            "Mounts": [
                {
                    "Type": mount["Type"],
                    "Source": mount["Source"],
                    "Name": mount["Source"],
                    "Destination": mount["Target"],
                    "RW": not mount["ReadOnly"],
                    "Propagation": "rprivate",
                }
                for mount in reversed(host["Mounts"])
            ],
            "State": {
                "Status": "exited" if self.exit else "running",
                "Running": not self.exit and self.running,
                "Pid": 0 if self.exit or not self.running else 10,
                "Dead": False,
                "Restarting": False,
                "ExitCode": 0,
                "OOMKilled": False,
                "Error": "",
            },
        }
        self.mutate(value)
        return DockerResponse(200, json.dumps(value).encode())


def owned(
    engine: Engine, *, mode: Literal["probe", "code", "check"] = "probe"
) -> OwnedContainer:
    execution = uuid4()
    return OwnedContainer(
        cast(DockerEngine, engine),
        ContainerSpec(
            image=IMAGE,
            execution_id=execution,
            invocation_id=uuid4(),
            mode=mode,
            workspace="C:/work/" + str(execution) if mode != "probe" else None,
            auth_volume="dedicated-auth" if mode == "code" else None,
        ),
        lambda: None,
        monotonic,
    )


@pytest.mark.parametrize("mode", ["probe", "code", "check"])
def test_original_id_settlement_and_mount_order(
    mode: Literal["probe", "code", "check"],
) -> None:
    async def scenario() -> None:
        engine = Engine()
        scope = owned(engine, mode=mode)
        result = await scope.run(b"{}")
        assert result.container_id == CID and result.stdout == b"done"
        await scope.aclose()
        assert engine.calls[-1] == ("DELETE", "/containers/" + CID)
        assert sum(method == "DELETE" for method, _ in engine.calls) == 1
        with pytest.raises(ContainerUnavailable):
            await scope.run(b"{}")

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(Id="c" * 64),
        lambda v: v.update(Image="sha256:" + "c" * 64),
        lambda v: v["HostConfig"].update(Privileged=True),
        lambda v: v["HostConfig"].update(PidMode="host"),
        lambda v: v["HostConfig"].update(IpcMode="host"),
        lambda v: v["HostConfig"].update(CapAdd=["SYS_ADMIN"]),
        lambda v: v["Config"].update(Entrypoint=["/bin/sh"]),
        lambda v: v["Config"].update(Env=["DEVRELAY_INPUT_COUNT=1"]),
        lambda v: v["Config"]["Labels"].update(unowned="1"),
        lambda v: v["Mounts"].append(
            {
                "Type": "bind",
                "Source": "C:\\Users",
                "Destination": "/profile",
                "RW": True,
            }
        ),
    ],
)
def test_changed_owner_or_boundary_never_starts_or_destructively_adopts(
    mutation: Any,
) -> None:
    async def scenario() -> None:
        engine = Engine()
        engine.mutate = mutation
        scope = owned(engine)
        with pytest.raises(ContainerUnavailable):
            await scope.run(b"{}")
        with pytest.raises(ContainerUnavailable):
            await scope.aclose()
        assert not any(
            method == "DELETE" or path.endswith("/start") or "/kill?" in path
            for method, path in engine.calls
        )

    asyncio.run(scenario())


def test_cancelled_create_retains_original_reply_and_never_starts() -> None:
    async def scenario() -> None:
        engine = Engine()
        engine.release = asyncio.Event()
        scope = owned(engine)
        task = asyncio.create_task(scope.run(b"{}"))
        await engine.created.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        closing = asyncio.create_task(scope.aclose())
        await asyncio.sleep(0)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        engine.release.set()
        await scope.aclose()
        assert not any(path.endswith("/start") for _, path in engine.calls)
        assert engine.calls[-1] == ("DELETE", "/containers/" + CID)

    asyncio.run(scenario())


def test_unknown_creation_never_lists_adopts_or_retries() -> None:
    async def scenario() -> None:
        engine = Engine()
        engine.fail_create = True
        scope = owned(engine)
        with pytest.raises(ContainerUnavailable):
            await scope.run(b"{}")
        await scope.aclose()
        assert len(engine.calls) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "path",
    [
        "/containers/json",
        "/auth",
        "/images/create",
        "/containers/name/kill?signal=KILL",
        "/containers/" + CID + "/exec",
    ],
)
def test_local_transport_rejects_discovery_auth_and_unbound_operations(
    path: str,
) -> None:
    async def scenario() -> None:
        engine = DockerEngine()
        try:
            with pytest.raises(ContainerUnavailable):
                await engine.request("POST", path)
        finally:
            await engine.aclose()

    asyncio.run(scenario())


@pytest.mark.parametrize("raw", [b'{"x":1,"x":2}', b'{"x":NaN}', b"\xff"])
def test_strict_docker_json(raw: bytes) -> None:
    with pytest.raises(ContainerUnavailable):
        decode_json(raw)


def test_log_framing_and_total_input_are_bounded() -> None:
    with pytest.raises(ContainerUnavailable):
        decode_logs(struct.pack("!B3sI", 1, b"\0" * 3, 9) + b"x")
    with pytest.raises(ContainerUnavailable):
        input_environment(b"x" * (1024**2 + 1))
    assert len(input_environment(b"x" * 65536)) > 3
