import asyncio
import base64
import hashlib
import json
import re
from dataclasses import dataclass
from typing import cast

import httpx

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    BaseSourceLimits,
    BaseSourceRequest,
    BaseSourceSnapshot,
    canonical_base_path,
    git_blob_sha,
)
from devrelay.application.ports.base_source import (
    BaseSourceAccessDenied,
    BaseSourceUnavailable,
    InvalidBaseSourceData,
)
from devrelay.application.ports.git_commit_source import GitCommitSource
from devrelay.infrastructure.github.repository_source import RepositoryBinding

_DEFAULT_LIMITS = BaseSourceLimits()
_API = httpx.URL("https://api.github.com/")
_INVALID = "GitHub base source failed bounded validation."


def _sha(value: object) -> str:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise ValueError(_INVALID)
    return value


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError(_INVALID)
        result[key] = value
    return result


def _constant(value: str) -> object:
    raise ValueError(_INVALID)


def _object(value: object) -> dict[str, object]:
    if type(value) is not dict:
        raise ValueError(_INVALID)
    return cast(dict[str, object], value)


def _json(body: bytes) -> dict[str, object]:
    return _object(json.loads(body, object_pairs_hook=_pairs, parse_constant=_constant))


def _size(value: object, maximum: int) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError(_INVALID)
    return value


@dataclass(frozen=True, slots=True)
class _TreeEntry:
    name: str
    sha: str
    mode: str
    size: int | None


def _tree(
    body: bytes, expected: str, limits: BaseSourceLimits
) -> tuple[_TreeEntry, ...]:
    data = _json(body)
    raw = data.get("tree")
    if (
        _sha(data.get("sha")) != expected
        or data.get("truncated") is not False
        or type(raw) is not list
        or len(raw) > limits.max_entries
    ):
        raise ValueError(_INVALID)
    entries: list[_TreeEntry] = []
    names: set[str] = set()
    for value in raw:
        item = _object(value)
        name = item.get("path")
        if type(name) is not str or "/" in name or name in names:
            raise ValueError(_INVALID)
        canonical_base_path(name)
        names.add(name)
        mode = item.get("mode")
        if mode == "040000" and item.get("type") == "tree":
            size = None
        elif mode in ("100644", "100755") and item.get("type") == "blob":
            size = _size(item.get("size"), limits.max_file_bytes)
        else:
            raise ValueError(_INVALID)
        entries.append(_TreeEntry(name, _sha(item.get("sha")), mode, size))
    # Git compares directory basenames as if suffixed with '/', and stores 40000.
    entries.sort(
        key=lambda item: (item.name + ("/" if item.size is None else "")).encode(
            "utf-8"
        )
    )
    content = b"".join(
        ("40000" if item.size is None else item.mode).encode("ascii")
        + b" "
        + item.name.encode("utf-8")
        + b"\x00"
        + bytes.fromhex(item.sha)
        for item in entries
    )
    actual = hashlib.sha1(
        b"tree " + str(len(content)).encode("ascii") + b"\x00" + content,
        usedforsecurity=False,
    ).hexdigest()
    if actual != expected:
        raise ValueError(_INVALID)
    return tuple(entries)


def _blob(body: bytes, expected: str, maximum: int) -> bytes:
    data = _json(body)
    size = _size(data.get("size"), maximum)
    encoded = data.get("content")
    if (
        _sha(data.get("sha")) != expected
        or data.get("encoding") != "base64"
        or type(encoded) is not str
        or not encoded.isascii()
    ):
        raise ValueError(_INVALID)
    compact = encoded.replace("\n", "").replace("\r", "")
    content = base64.b64decode(compact, validate=True)
    if (
        len(content) != size
        or base64.b64encode(content).decode("ascii") != compact
        or git_blob_sha(content) != expected
        or content.split(b"\n", 1)[0].rstrip(b"\r")
        == b"version https://git-lfs.github.com/spec/v1"
    ):
        raise ValueError(_INVALID)
    return content


class GitHubBaseSource:
    """Trusted broker-side plain-file export; no worker credentials or Git process.

    Commit correlation trusts the authenticated provider response. Tree and blob
    IDs are independently checked, but no raw-commit/signature proof is claimed.
    The caller owns the explicitly configured authenticated client and its lifetime.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        bindings: tuple[RepositoryBinding, ...],
        limits: BaseSourceLimits = _DEFAULT_LIMITS,
        commit_source: GitCommitSource | None = None,
    ) -> None:
        if (
            not isinstance(client, httpx.AsyncClient)
            or client.base_url != _API
            or client.trust_env
            or not client.headers.get("authorization", "").startswith("Bearer ")
            or type(bindings) is not tuple
            or not 1 <= len(bindings) <= 32
            or not isinstance(limits, BaseSourceLimits)
        ):
            raise ValueError(
                "GitHub base source requires explicit bounded configuration."
            )
        limits.__post_init__()
        for binding in bindings:
            if not isinstance(binding, RepositoryBinding):
                raise ValueError(
                    "GitHub base source requires configured repository scope."
                )
            BaseSourceRequest(
                repository_id=binding.repository_id,
                provider_repository_id=binding.github_id,
                repository_slug=binding.slug,
                base_revision="0" * 40,
            )
        if len({item.repository_id for item in bindings}) != len(bindings) or len(
            {item.github_id for item in bindings}
        ) != len(bindings):
            raise ValueError("GitHub base source bindings must be unique.")
        self._client = client
        self._bindings = {item.repository_id: item for item in bindings}
        self._limits = limits
        self._commit_source = commit_source

    async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot:
        try:
            if not isinstance(request, BaseSourceRequest):
                raise ValueError(_INVALID)
            request.__post_init__()
            binding = self._bindings.get(request.repository_id)
            if (
                binding is None
                or binding.github_id != request.provider_repository_id
                or binding.slug.casefold() != request.repository_slug.casefold()
            ):
                raise BaseSourceAccessDenied("Base source is outside configured scope.")
            async with asyncio.timeout(self._limits.total_timeout_seconds):
                return await _Export(
                    self._client, binding, self._limits, self._commit_source
                ).read(request)
        except TimeoutError, httpx.RequestError:
            raise BaseSourceUnavailable(
                "GitHub base source failed or timed out."
            ) from None
        except ValueError, TypeError, RecursionError, OverflowError:
            raise InvalidBaseSourceData(_INVALID) from None


class _Export:
    def __init__(
        self,
        client: httpx.AsyncClient,
        binding: RepositoryBinding,
        limits: BaseSourceLimits,
        commit_source: GitCommitSource | None = None,
    ) -> None:
        self.client, self.binding, self.limits = client, binding, limits
        self.commit_source = commit_source
        self.requests = self.response_bytes = self.entries = self.total_bytes = 0
        self.files: list[BaseSourceFile] = []
        self.paths: dict[str, str] = {}
        self.trees: dict[str, tuple[_TreeEntry, ...]] = {}
        self.blobs: dict[str, bytes] = {}
        self.path = f"repos/{binding.slug}"

    async def read(self, request: BaseSourceRequest) -> BaseSourceSnapshot:
        await self._repository()
        commit = await asyncio.to_thread(
            _json, await self._get(f"{self.path}/git/commits/{request.base_revision}")
        )
        if _sha(commit.get("sha")) != request.base_revision:
            raise ValueError(_INVALID)
        tree_sha = _sha(_object(commit.get("tree")).get("sha"))
        await self._walk(tree_sha, "", frozenset())
        raw_commit = (
            None
            if self.commit_source is None
            else await self.commit_source.read(request, root_tree_sha=tree_sha)
        )
        if self.commit_source is not None and type(raw_commit) is not bytes:
            raise ValueError(_INVALID)
        snapshot = await asyncio.to_thread(
            self._snapshot, request, tree_sha, raw_commit
        )
        await self._repository()
        return snapshot

    def _snapshot(
        self, request: BaseSourceRequest, tree_sha: str, raw_commit: bytes | None
    ) -> BaseSourceSnapshot:
        return BaseSourceSnapshot(
            request=request,
            root_tree_sha=tree_sha,
            files=tuple(sorted(self.files, key=lambda item: item.path.encode("utf-8"))),
            raw_commit=raw_commit,
        )

    async def _repository(self) -> None:
        metadata = await asyncio.to_thread(_json, await self._get(self.path))
        slug = metadata.get("full_name")
        if (
            type(metadata.get("id")) is not int
            or metadata["id"] != self.binding.github_id
            or type(slug) is not str
            or slug.casefold() != self.binding.slug.casefold()
            or metadata.get("archived") is not False
            or metadata.get("disabled") is not False
        ):
            raise BaseSourceAccessDenied(
                "GitHub repository identity does not match scope."
            )

    async def _walk(self, sha: str, prefix: str, ancestors: frozenset[str]) -> None:
        if sha in ancestors:
            raise ValueError(_INVALID)
        if sha not in self.trees:
            self.trees[sha] = await asyncio.to_thread(
                _tree, await self._get(f"{self.path}/git/trees/{sha}"), sha, self.limits
            )
        entries = self.trees[sha]
        if prefix and not entries:
            raise ValueError(_INVALID)
        self.entries += len(entries)
        if self.entries > self.limits.max_entries:
            raise ValueError(_INVALID)
        for entry in entries:
            path = prefix + entry.name
            await asyncio.to_thread(canonical_base_path, path)
            if len(path.split("/")) > self.limits.max_depth:
                raise ValueError(_INVALID)
            key = path.casefold()
            if key in self.paths:
                raise ValueError(_INVALID)
            self.paths[key] = path
            if entry.size is None:
                await self._walk(entry.sha, path + "/", ancestors | {sha})
            else:
                if (
                    len(self.files) >= self.limits.max_files
                    or self.total_bytes + entry.size > self.limits.max_total_bytes
                ):
                    raise ValueError(_INVALID)
                if entry.sha not in self.blobs:
                    self.blobs[entry.sha] = await asyncio.to_thread(
                        _blob,
                        await self._get(f"{self.path}/git/blobs/{entry.sha}"),
                        entry.sha,
                        self.limits.max_file_bytes,
                    )
                content = self.blobs[entry.sha]
                if len(content) != entry.size:
                    raise ValueError(_INVALID)
                self.files.append(
                    await asyncio.to_thread(
                        BaseSourceFile,
                        path=path,
                        blob_sha=entry.sha,
                        mode=BaseFileMode(entry.mode),
                        content=content,
                    )
                )
                self.total_bytes += len(content)

    async def _get(self, path: str) -> bytes:
        if self.requests >= self.limits.max_requests:
            raise ValueError(_INVALID)
        self.requests += 1
        url = _API.join(path)
        async with asyncio.timeout(self.limits.request_timeout_seconds):
            async with self.client.stream(
                "GET",
                url,
                timeout=self.limits.request_timeout_seconds,
                follow_redirects=False,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Accept-Encoding": "identity",
                },
            ) as response:
                status = response.status_code
                if (
                    status in (408, 429)
                    or 500 <= status <= 599
                    or (
                        status == 403
                        and (
                            response.headers.get("x-ratelimit-remaining") == "0"
                            or "retry-after" in response.headers
                        )
                    )
                ):
                    raise BaseSourceUnavailable(
                        "GitHub base source is temporarily unavailable."
                    )
                if status in (401, 403, 404):
                    raise BaseSourceAccessDenied("GitHub base source access failed.")
                if status != 200 or response.url != url:
                    raise ValueError(_INVALID)
                if response.headers.get("content-type", "").split(";", 1)[
                    0
                ].strip().lower() not in (
                    "application/json",
                    "application/vnd.github+json",
                ) or response.headers.get(
                    "content-encoding", "identity"
                ).lower() not in ("", "identity"):
                    raise ValueError(_INVALID)
                body = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=4096):
                    self.response_bytes += len(chunk)
                    if (
                        len(body) + len(chunk) > self.limits.max_response_bytes
                        or self.response_bytes > self.limits.max_total_response_bytes
                    ):
                        raise ValueError(_INVALID)
                    body.extend(chunk)
                return bytes(body)
