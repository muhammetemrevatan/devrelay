import asyncio
import base64
import gzip
import hashlib
import traceback
from collections.abc import AsyncIterator, Callable
from dataclasses import replace
from typing import Any, cast
from uuid import UUID

import httpx
import pytest

from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceLimits,
    BaseSourceRequest,
    BaseSourceSnapshot,
    git_blob_sha,
)
from devrelay.application.ports.base_source import (
    BaseSourceAccessDenied,
    BaseSourceUnavailable,
    InvalidBaseSourceData,
)
from devrelay.infrastructure.github.base_source import GitHubBaseSource
from devrelay.infrastructure.github.repository_source import RepositoryBinding

TOKEN = "test-only-base-source-credential"
REPO_ID = UUID("11111111-2222-4333-8444-555555555555")
BASE = "a" * 40
DEFAULT_LIMITS = BaseSourceLimits()
PREFIX = "/repos/example/project"
BINDING = RepositoryBinding(
    repository_id=REPO_ID,
    github_id=123,
    slug="example/project",
    target_branch="master",
    coding_branch_prefix="devrelay/jobs",
)


def request() -> BaseSourceRequest:
    return BaseSourceRequest(
        repository_id=REPO_ID,
        provider_repository_id=123,
        repository_slug=BINDING.slug,
        base_revision=BASE,
    )


def blob(content: bytes) -> dict[str, object]:
    return {
        "sha": git_blob_sha(content),
        "encoding": "base64",
        "size": len(content),
        "content": base64.b64encode(content).decode("ascii"),
        "url": "https://untrusted.example/" + TOKEN,
    }


def file_entry(name: str, content: bytes, mode: str = "100644") -> dict[str, object]:
    return {
        "path": name,
        "mode": mode,
        "type": "blob",
        "size": len(content),
        "sha": git_blob_sha(content),
    }


def tree(entries: list[dict[str, object]]) -> dict[str, object]:
    ordered = sorted(
        entries,
        key=lambda entry: (
            str(entry["path"]) + ("/" if entry["type"] == "tree" else "")
        ).encode("utf-8"),
    )
    raw = b"".join(
        ("40000" if entry["type"] == "tree" else str(entry["mode"])).encode()
        + b" "
        + str(entry["path"]).encode()
        + b"\0"
        + bytes.fromhex(str(entry["sha"]))
        for entry in ordered
    )
    sha = hashlib.sha1(
        b"tree " + str(len(raw)).encode() + b"\0" + raw, usedforsecurity=False
    ).hexdigest()
    return {
        "sha": sha,
        "tree": entries,
        "truncated": False,
        "url": "https://untrusted.example/" + TOKEN,
    }


class Fixture:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.responses: dict[str, Any] = {}
        self.metadata_reads = 0
        self.metadata = {
            "id": 123,
            "full_name": BINDING.slug,
            "archived": False,
            "disabled": False,
        }
        self.final_metadata: dict[str, object] | None = None
        self.add_blob(b"hello\n")
        self.add_blob(b"\x00\xffbinary")
        child = tree([file_entry("run.bin", b"\x00\xffbinary", "100755")])
        self.add_tree(child)
        self.root = tree(
            [
                {"path": "foo", "mode": "040000", "type": "tree", "sha": child["sha"]},
                file_entry("foo.c", b"hello\n"),
            ]
        )
        self.add_tree(self.root)
        self.responses[PREFIX + "/git/commits/" + BASE] = {
            "sha": BASE,
            "tree": {"sha": self.root["sha"], "url": "https://invalid/"},
        }

    def add_blob(self, content: bytes) -> None:
        self.responses[PREFIX + "/git/blobs/" + git_blob_sha(content)] = blob(content)

    def add_tree(self, value: dict[str, object]) -> None:
        self.responses[PREFIX + "/git/trees/" + str(value["sha"])] = value

    def set_root(self, entries: list[dict[str, object]]) -> None:
        self.root = tree(entries)
        self.add_tree(self.root)
        self.responses[PREFIX + "/git/commits/" + BASE] = {
            "sha": BASE,
            "tree": {"sha": self.root["sha"]},
        }

    def respond(self, incoming: httpx.Request) -> httpx.Response:
        self.requests.append(incoming)
        if incoming.url.path == PREFIX:
            self.metadata_reads += 1
            metadata = self.final_metadata if self.metadata_reads > 1 else None
            return httpx.Response(200, json=metadata or self.metadata)
        value = self.responses[incoming.url.path]
        if isinstance(value, httpx.Response):
            return value
        if type(value) is bytes:
            return httpx.Response(
                200, content=value, headers={"content-type": "application/json"}
            )
        return httpx.Response(200, json=value)


def read(
    fixture: Fixture,
    *,
    limits: BaseSourceLimits = DEFAULT_LIMITS,
    pinned: BaseSourceRequest | None = None,
    respond: Callable[[httpx.Request], httpx.Response] | None = None,
) -> BaseSourceSnapshot:
    async def exercise() -> BaseSourceSnapshot:
        async with httpx.AsyncClient(
            base_url="https://api.github.com/",
            trust_env=False,
            headers={"Authorization": "Bearer " + TOKEN},
            transport=httpx.MockTransport(respond or fixture.respond),
        ) as client:
            source = GitHubBaseSource(client, bindings=(BINDING,), limits=limits)
            return await source.read(pinned or request())

    return asyncio.run(exercise())


def test_exact_base_binary_files_use_verified_tree_order_and_fixed_get_paths() -> None:
    fixture = Fixture()

    result = read(fixture)

    assert result.request == request()
    assert result.root_tree_sha == fixture.root["sha"]
    assert [(file.path, file.mode, file.content) for file in result.files] == [
        ("foo.c", BaseFileMode.REGULAR, b"hello\n"),
        ("foo/run.bin", BaseFileMode.EXECUTABLE, b"\x00\xffbinary"),
    ]
    assert result.files[0].blob_sha == "ce013625030ba8dba906f756967f9e9ca394464a"
    assert TOKEN not in repr(result)
    assert len(fixture.requests) == 7 and fixture.metadata_reads == 2
    assert all(incoming.method == "GET" for incoming in fixture.requests)
    assert all(incoming.url.host == "api.github.com" for incoming in fixture.requests)
    assert all(not incoming.url.query for incoming in fixture.requests)
    assert all(
        incoming.headers["authorization"] == "Bearer " + TOKEN
        for incoming in fixture.requests
    )
    assert all(
        incoming.headers["accept-encoding"] == "identity"
        for incoming in fixture.requests
    )


@pytest.mark.parametrize("change", ["uuid", "numeric", "slug", "sha64"])
def test_unsupported_or_unconfigured_input_never_reaches_provider(change: str) -> None:
    fixture = Fixture()
    pinned = request()
    if change == "sha64":
        object.__setattr__(pinned, "base_revision", "a" * 64)
    else:
        if change == "uuid":
            pinned = replace(pinned, repository_id=UUID(int=99))
        elif change == "numeric":
            pinned = replace(pinned, provider_repository_id=999)
        else:
            pinned = replace(pinned, repository_slug="other/project")

    with pytest.raises(
        InvalidBaseSourceData if change == "sha64" else BaseSourceAccessDenied
    ):
        read(fixture, pinned=pinned)

    assert fixture.requests == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", 999),
        ("id", True),
        ("full_name", "other/repository"),
        ("archived", True),
        ("disabled", True),
    ],
)
def test_numeric_identity_is_rechecked_after_content_export(
    field: str, value: Any
) -> None:
    fixture = Fixture()
    fixture.final_metadata = fixture.metadata | {field: value}

    with pytest.raises(BaseSourceAccessDenied):
        read(fixture)

    assert fixture.metadata_reads == 2


@pytest.mark.parametrize(
    "damage",
    [
        "commit_sha",
        "tree_sha",
        "tree_hash",
        "truncated",
        "tree_type",
        "tree_mode",
        "duplicate",
        "nested_path",
        "symlink",
        "submodule",
    ],
)
def test_commit_and_tree_corruption_is_not_exported(damage: str) -> None:
    fixture = Fixture()
    root = fixture.root
    entries = cast(list[dict[str, object]], root["tree"])
    if damage == "commit_sha":
        fixture.responses[PREFIX + "/git/commits/" + BASE]["sha"] = "b" * 40
    elif damage == "tree_sha":
        root["sha"] = "b" * 40
    elif damage == "tree_hash":
        entries[1]["sha"] = "b" * 40
    elif damage == "truncated":
        root["truncated"] = True
    elif damage == "duplicate":
        entries.append(entries[0])
    elif damage == "nested_path":
        entries[0]["path"] = "nested/path"
    elif damage == "tree_type":
        entries[0]["type"] = "blob"
    else:
        entries[0]["mode"] = {
            "tree_mode": "40000",
            "symlink": "120000",
            "submodule": "160000",
        }[damage]

    with pytest.raises(InvalidBaseSourceData):
        read(fixture)

    assert fixture.metadata_reads == 1


@pytest.mark.parametrize(
    "damage",
    [
        "sha",
        "hash",
        "size",
        "bool_size",
        "encoding",
        "base64",
        "padding",
        "noncanonical",
        "whitespace",
        "unicode",
    ],
)
def test_malformed_or_inconsistent_blob_is_not_exported(damage: str) -> None:
    fixture = Fixture()
    value = fixture.responses[PREFIX + "/git/blobs/" + git_blob_sha(b"hello\n")]
    changes: dict[str, dict[str, object]] = {
        "sha": {"sha": "b" * 40},
        "hash": {"content": "YmFk"},
        "size": {"size": 999},
        "bool_size": {"size": True},
        "encoding": {"encoding": "utf-8"},
        "base64": {"content": "!!!"},
        "padding": {"content": "aGVsbG8K="},
        "noncanonical": {"content": "Zh=="},
        "whitespace": {"content": " aGVsbG8K"},
        "unicode": {"content": "é"},
    }
    value.update(changes[damage])

    with pytest.raises(InvalidBaseSourceData):
        read(fixture)

    assert fixture.metadata_reads == 1


def test_line_wrapped_base64_is_decoded_without_changing_file_bytes() -> None:
    fixture = Fixture()
    value = fixture.responses[PREFIX + "/git/blobs/" + git_blob_sha(b"hello\n")]
    value["content"] = "aGVs\r\nbG8K\n"

    result = read(fixture)

    assert result.files[0].content == b"hello\n"


@pytest.mark.parametrize(
    "path",
    [
        "../escape",
        ".git",
        "C:escape",
        "NUL.txt",
        "a:stream",
        "trailing.",
        "UPPER\\escape",
        "PROGRA~1",
        "e\u0301",
        "bad\x00",
    ],
)
def test_ambiguous_windows_names_are_rejected_before_export(path: str) -> None:
    fixture = Fixture()
    fixture.set_root([file_entry(path, b"hello\n")])

    with pytest.raises(InvalidBaseSourceData):
        read(fixture)

    assert fixture.metadata_reads == 1


def test_case_aliases_in_a_valid_git_tree_are_rejected() -> None:
    fixture = Fixture()
    fixture.set_root([file_entry("Foo", b"hello\n"), file_entry("foo", b"hello\n")])

    with pytest.raises(InvalidBaseSourceData):
        read(fixture)

    assert fixture.metadata_reads == 1


def test_lfs_pointer_is_rejected_without_secondary_provider_requests() -> None:
    fixture = Fixture()
    content = b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 10\n"
    fixture.add_blob(content)
    fixture.set_root([file_entry("large.bin", content)])

    with pytest.raises(InvalidBaseSourceData):
        read(fixture)

    assert all("lfs" not in incoming.url.path for incoming in fixture.requests)


def test_empty_root_tree_is_legitimate_and_correlated() -> None:
    fixture = Fixture()
    fixture.set_root([])

    result = read(fixture)

    assert result.files == ()
    assert result.root_tree_sha == "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
    assert len(fixture.requests) == 4


def test_empty_child_tree_cannot_be_silently_omitted_from_manifest() -> None:
    fixture = Fixture()
    empty = tree([])
    fixture.add_tree(empty)
    fixture.set_root(
        [{"path": "missing-dir", "type": "tree", "mode": "040000", "sha": empty["sha"]}]
    )

    with pytest.raises(InvalidBaseSourceData):
        read(fixture)

    assert fixture.metadata_reads == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_files", 1),
        ("max_entries", 1),
        ("max_depth", 1),
        ("max_file_bytes", 1),
        ("max_total_bytes", 1),
        ("max_requests", 3),
        ("max_response_bytes", 8),
        ("max_total_response_bytes", 8),
    ],
)
def test_every_configured_budget_fails_closed(field: str, value: int) -> None:
    fixture = Fixture()
    limits = replace(BaseSourceLimits(), **{field: value})

    with pytest.raises(InvalidBaseSourceData):
        read(fixture, limits=limits)

    assert fixture.metadata_reads == 1
    if field == "max_requests":
        assert len(fixture.requests) == value


@pytest.mark.parametrize(
    "status,exception",
    [
        (302, InvalidBaseSourceData),
        (401, BaseSourceAccessDenied),
        (403, BaseSourceAccessDenied),
        (404, BaseSourceAccessDenied),
        (408, BaseSourceUnavailable),
        (429, BaseSourceUnavailable),
        (503, BaseSourceUnavailable),
    ],
)
def test_http_failures_are_classified_and_never_retried(
    status: int, exception: type[Exception]
) -> None:
    fixture = Fixture()
    requests: list[httpx.Request] = []
    response = httpx.Response(
        status, headers={"location": "https://invalid/" + TOKEN}, text=TOKEN
    )

    def respond(incoming: httpx.Request) -> httpx.Response:
        requests.append(incoming)
        return response

    with pytest.raises(exception) as error:
        read(fixture, respond=respond)

    assert len(requests) == 1
    assert TOKEN not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize(
    "field,value", [("content-type", "text/html"), ("content-encoding", "gzip")]
)
def test_unsupported_response_encoding_is_rejected(field: str, value: str) -> None:
    fixture = Fixture()
    response = httpx.Response(
        200,
        content=gzip.compress(b"{}") if field == "content-encoding" else b"{}",
        headers={"content-type": "application/json", field: value},
    )

    with pytest.raises(InvalidBaseSourceData):
        read(fixture, respond=lambda incoming: response)


@pytest.mark.parametrize(
    "body", [b'{"id":123,"id":999}', b'{"id":NaN}', b"[]", b"not JSON"]
)
def test_ambiguous_json_is_not_interpreted_as_repository_identity(body: bytes) -> None:
    fixture = Fixture()
    response = httpx.Response(
        200, content=body, headers={"content-type": "application/json"}
    )

    with pytest.raises(InvalidBaseSourceData):
        read(fixture, respond=lambda incoming: response)


@pytest.mark.parametrize("timeout", ["request", "total"])
def test_mock_transport_is_bounded_by_explicit_async_deadlines(timeout: str) -> None:
    fixture = Fixture()
    limits = (
        replace(DEFAULT_LIMITS, request_timeout_seconds=0.05)
        if timeout == "request"
        else replace(DEFAULT_LIMITS, total_timeout_seconds=0.05)
    )

    async def slow(incoming: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1)
        return fixture.respond(incoming)

    async def exercise() -> None:
        async with httpx.AsyncClient(
            base_url="https://api.github.com/",
            trust_env=False,
            headers={"Authorization": "Bearer " + TOKEN},
            transport=httpx.MockTransport(slow),
        ) as client:
            source = GitHubBaseSource(client, bindings=(BINDING,), limits=limits)
            with pytest.raises(BaseSourceUnavailable):
                await source.read(request())

    asyncio.run(exercise())


@pytest.mark.parametrize("change", ["origin", "proxy", "auth", "duplicate"])
def test_misconfigured_client_or_scope_never_constructs_adapter(change: str) -> None:
    async def exercise() -> None:
        async with httpx.AsyncClient(
            base_url="https://invalid.example/"
            if change == "origin"
            else "https://api.github.com/",
            trust_env=change == "proxy",
            headers={} if change == "auth" else {"Authorization": "Bearer " + TOKEN},
        ) as client:
            bindings = (BINDING, BINDING) if change == "duplicate" else (BINDING,)
            with pytest.raises(ValueError):
                GitHubBaseSource(client, bindings=bindings)

    asyncio.run(exercise())


def test_initial_repository_mismatch_blocks_commit_and_blob_requests() -> None:
    fixture = Fixture()
    fixture.metadata["id"] = 999

    with pytest.raises(BaseSourceAccessDenied):
        read(fixture)

    assert [incoming.url.path for incoming in fixture.requests] == [PREFIX]


def test_repeated_blob_ids_are_read_once_but_count_every_exported_file() -> None:
    fixture = Fixture()
    fixture.set_root([file_entry("a", b"hello\n"), file_entry("b", b"hello\n")])

    result = read(fixture)

    assert [(item.path, item.content) for item in result.files] == [
        ("a", b"hello\n"),
        ("b", b"hello\n"),
    ]
    assert (
        len(
            [
                incoming
                for incoming in fixture.requests
                if "/git/blobs/" in incoming.url.path
            ]
        )
        == 1
    )


def test_repeated_blob_content_does_not_bypass_logical_byte_budget() -> None:
    fixture = Fixture()
    fixture.set_root([file_entry("a", b"hello\n"), file_entry("b", b"hello\n")])
    limits = replace(DEFAULT_LIMITS, max_total_bytes=6)

    with pytest.raises(InvalidBaseSourceData):
        read(fixture, limits=limits)

    assert fixture.metadata_reads == 1


@pytest.mark.parametrize(
    "headers",
    [
        {"x-ratelimit-remaining": "0"},
        {"retry-after": "3"},
    ],
)
def test_rate_limited_forbidden_response_is_temporarily_unavailable(
    headers: dict[str, str],
) -> None:
    fixture = Fixture()
    response = httpx.Response(403, headers=headers, text=TOKEN)

    with pytest.raises(BaseSourceUnavailable) as error:
        read(fixture, respond=lambda incoming: response)

    assert TOKEN not in "".join(traceback.format_exception(error.value))


class BoundedStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.emitted = 0
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for _ in range(10):
            self.emitted += 1
            yield b"x" * 4096

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.parametrize("budget", ["response", "cumulative"])
def test_streamed_bodies_stop_at_byte_budget_and_close_response(budget: str) -> None:
    fixture = Fixture()
    stream = BoundedStream()
    limits = (
        replace(DEFAULT_LIMITS, max_response_bytes=4096)
        if budget == "response"
        else replace(DEFAULT_LIMITS, max_total_response_bytes=4096)
    )
    response = httpx.Response(
        200, stream=stream, headers={"content-type": "application/json"}
    )

    with pytest.raises(InvalidBaseSourceData):
        read(fixture, limits=limits, respond=lambda incoming: response)

    assert stream.emitted == 2
    assert stream.closed


def test_compressed_stream_is_rejected_before_consuming_body() -> None:
    fixture = Fixture()
    stream = BoundedStream()
    response = httpx.Response(
        200,
        stream=stream,
        headers={
            "content-type": "application/json",
            "content-encoding": "gzip",
        },
    )

    with pytest.raises(InvalidBaseSourceData):
        read(fixture, respond=lambda incoming: response)

    assert stream.emitted == 0
    assert stream.closed


def test_transport_error_is_sanitized_without_retrying() -> None:
    fixture = Fixture()
    requests: list[httpx.Request] = []

    def fail(incoming: httpx.Request) -> httpx.Response:
        requests.append(incoming)
        raise httpx.ConnectError(TOKEN, request=incoming)

    with pytest.raises(BaseSourceUnavailable) as error:
        read(fixture, respond=fail)

    assert len(requests) == 1
    assert TOKEN not in "".join(traceback.format_exception(error.value))


def test_cancellation_propagates_and_does_not_start_another_request() -> None:
    async def exercise() -> None:
        started = asyncio.Event()
        calls: list[httpx.Request] = []

        async def blocked(incoming: httpx.Request) -> httpx.Response:
            calls.append(incoming)
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("The canceled response must never be returned.")

        async with httpx.AsyncClient(
            base_url="https://api.github.com/",
            trust_env=False,
            headers={"Authorization": "Bearer " + TOKEN},
            transport=httpx.MockTransport(blocked),
        ) as client:
            source = GitHubBaseSource(client, bindings=(BINDING,))
            task = asyncio.create_task(source.read(request()))
            await started.wait()

            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            assert len(calls) == 1

    asyncio.run(exercise())
