import base64
import hashlib
from dataclasses import dataclass

import httpx

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    BaseSourceRequest,
    BaseSourceSnapshot,
    git_blob_sha,
)
from devrelay.application.contracts.workspaces import WorkspaceBinding


@dataclass(frozen=True)
class BaseFixture:
    snapshot: BaseSourceSnapshot
    trees: dict[str, dict[str, object]]
    blobs: dict[str, dict[str, object]]

    def respond(self, request: httpx.Request) -> httpx.Response:
        pins = self.snapshot.request
        root = f"/repos/{pins.repository_slug}"
        if request.method != "GET" or request.url.host != "api.github.com":
            raise AssertionError("Fixture received an unexpected provider operation.")
        if request.url.path == root:
            return httpx.Response(
                200,
                json={
                    "id": pins.provider_repository_id,
                    "full_name": pins.repository_slug,
                    "archived": False,
                    "disabled": False,
                },
            )
        if request.url.path == root + "/git/commits/" + pins.base_revision:
            return httpx.Response(
                200,
                json={
                    "sha": pins.base_revision,
                    "tree": {"sha": self.snapshot.root_tree_sha},
                },
            )
        if request.url.path.startswith(root + "/git/trees/"):
            return httpx.Response(
                200, json=self.trees[request.url.path.rsplit("/", 1)[1]]
            )
        if request.url.path.startswith(root + "/git/blobs/"):
            return httpx.Response(
                200, json=self.blobs[request.url.path.rsplit("/", 1)[1]]
            )
        raise AssertionError("Fixture received an unexpected provider path.")


def source_fixture(binding: WorkspaceBinding) -> BaseFixture:
    definition = binding.definition
    pins = BaseSourceRequest(
        repository_id=definition.repository_id,
        provider_repository_id=definition.provider_repository_id,
        repository_slug=definition.repository_slug,
        base_revision=definition.base_revision,
    )
    files = tuple(
        BaseSourceFile(
            path=path,
            blob_sha=git_blob_sha(content),
            mode=BaseFileMode.REGULAR,
            content=content,
        )
        for path, content in (
            ("README.md", b"Fixture source\n"),
            ("data/payload.bin", b"\x00\xffbinary\r\n"),
            ("src/app.py", b"print('fixture')\n"),
        )
    )
    blobs = {
        item.blob_sha: {
            "sha": item.blob_sha,
            "encoding": "base64",
            "size": len(item.content),
            "content": base64.b64encode(item.content).decode("ascii"),
        }
        for item in files
    }
    trees: dict[str, dict[str, object]] = {}

    def tree(prefix: str) -> str:
        entries: list[dict[str, object]] = []
        children = {
            item.path[len(prefix) :].split("/", 1)[0]
            for item in files
            if item.path.startswith(prefix)
        }
        for child in children:
            path = prefix + child
            matched = next((item for item in files if item.path == path), None)
            if matched is None:
                entries.append(
                    {
                        "path": child,
                        "mode": "040000",
                        "type": "tree",
                        "sha": tree(path + "/"),
                    }
                )
            else:
                entries.append(
                    {
                        "path": child,
                        "mode": matched.mode.value,
                        "type": "blob",
                        "sha": matched.blob_sha,
                        "size": len(matched.content),
                    }
                )
        entries.sort(
            key=lambda item: (
                str(item["path"]).encode("utf-8")
                + (b"/" if item["type"] == "tree" else b"")
            )
        )
        body = b"".join(
            (b"40000" if item["type"] == "tree" else str(item["mode"]).encode("ascii"))
            + b" "
            + str(item["path"]).encode("utf-8")
            + b"\x00"
            + bytes.fromhex(str(item["sha"]))
            for item in entries
        )
        sha = hashlib.sha1(
            b"tree " + str(len(body)).encode("ascii") + b"\x00" + body,
            usedforsecurity=False,
        ).hexdigest()
        trees[sha] = {"sha": sha, "truncated": False, "tree": entries}
        return sha

    snapshot = BaseSourceSnapshot(request=pins, root_tree_sha=tree(""), files=files)
    return BaseFixture(snapshot=snapshot, trees=trees, blobs=blobs)
