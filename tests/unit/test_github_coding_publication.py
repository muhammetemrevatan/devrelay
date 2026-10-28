import asyncio
import base64
import copy
import json
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
import pytest

import devrelay.infrastructure.github.coding_publication as adapter
from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    git_blob_sha,
)
from devrelay.application.contracts.candidates import parse_candidate_commit
from devrelay.application.contracts.coding_publications import (
    PublicationPlan,
    PublicationPolicy,
    PublicationWrite,
    PublicationWriteKind,
)
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultRejected,
    CodingResultUnavailable,
)
from devrelay.application.contracts.git_objects import GitObject, build_git_tree
from devrelay.infrastructure.github.coding_publication import (
    HttpGitHubCodingPublicationProvider,
)
from devrelay.infrastructure.github.repository_source import RepositoryBinding
from tests.unit.test_coding_results import context

TOKEN = "fixture_only_publisher_0123456789abcdefghijklmnop"
NOW = datetime(2026, 10, 20, tzinfo=UTC)


def canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode()


def plan(
    *,
    policy: PublicationPolicy | None = None,
    files: tuple[BaseSourceFile, ...] | None = None,
) -> PublicationPlan:
    candidate = context().result.final_candidate
    if files is not None:
        original = parse_candidate_commit(
            candidate.raw_commit,
            parent=candidate.request.base_revision,
            tree=candidate.root_tree_sha,
        )
        tree = build_git_tree(files)
        changed = GitObject(
            kind="commit",
            data=original.raw_commit(
                parent=candidate.request.base_revision, tree=tree.root_tree_sha
            ),
        )
        candidate = replace(
            candidate,
            files=files,
            root_tree_sha=tree.root_tree_sha,
            raw_commit=changed.data,
            revision=changed.sha,
        )
    objects = build_git_tree(candidate.files).objects
    commit = GitObject(kind="commit", data=candidate.raw_commit)
    spec = parse_candidate_commit(
        commit.data,
        parent=candidate.request.base_revision,
        tree=candidate.root_tree_sha,
    )
    prefix = "repos/" + candidate.request.repository_slug + "/"
    writes: list[PublicationWrite] = []

    def write(
        kind: PublicationWriteKind, path: str, body: object, sha: str | None
    ) -> None:
        writes.append(
            PublicationWrite(
                write_id=uuid4(),
                kind=kind,
                path=prefix + path,
                body=canonical(body),
                expected_sha=sha,
            )
        )

    def entries(item: GitObject) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        offset = 0
        while offset < len(item.data):
            space = item.data.index(b" ", offset)
            nul = item.data.index(b"\0", space + 1)
            mode = item.data[offset:space].decode()
            result.append(
                {
                    "path": item.data[space + 1 : nul].decode(),
                    "mode": "040000" if mode == "40000" else mode,
                    "type": "tree" if mode == "40000" else "blob",
                    "sha": item.data[nul + 1 : nul + 21].hex(),
                }
            )
            offset = nul + 21
        return result

    by_sha = {item.sha: item for item in objects}
    ordered = [item for item in objects if item.kind == "blob"]
    visited: set[str] = set()

    def visit(sha: str) -> None:
        if sha in visited:
            return
        visited.add(sha)
        item = by_sha[sha]
        for entry in entries(item):
            if entry["type"] == "tree":
                visit(entry["sha"])
        ordered.append(item)

    visit(candidate.root_tree_sha)
    for item in ordered:
        if item.kind == "blob":
            write(
                PublicationWriteKind.BLOB,
                "git/blobs",
                {"content": base64.b64encode(item.data).decode(), "encoding": "base64"},
                item.sha,
            )
        else:
            write(
                PublicationWriteKind.TREE,
                "git/trees",
                {"tree": entries(item)},
                item.sha,
            )
    date = (
        datetime.fromtimestamp(spec.timestamp, UTC).isoformat().replace("+00:00", "Z")
    )
    write(
        PublicationWriteKind.COMMIT,
        "git/commits",
        {
            "message": spec.message,
            "tree": candidate.root_tree_sha,
            "parents": [candidate.request.base_revision],
            "author": {
                "name": spec.author.name,
                "email": spec.author.email,
                "date": date,
            },
            "committer": {
                "name": spec.committer.name,
                "email": spec.committer.email,
                "date": date,
            },
        },
        candidate.revision,
    )
    write(
        PublicationWriteKind.BRANCH,
        "git/refs",
        {"ref": "refs/heads/" + candidate.authorized_branch, "sha": candidate.revision},
        candidate.revision,
    )
    write(
        PublicationWriteKind.PULL_REQUEST,
        "pulls",
        {
            "head": candidate.authorized_branch,
            "base": "main",
            "title": "Apply fixture",
            "body": "Fixture publication",
            "draft": False,
            "maintainer_can_modify": False,
        },
        None,
    )
    return PublicationPlan(
        result_id=uuid4(),
        repository_id=candidate.request.repository_id,
        provider_repository_id=candidate.request.provider_repository_id,
        repository_slug=candidate.request.repository_slug,
        source_branch=candidate.authorized_branch,
        target_branch="main",
        head_revision=candidate.revision,
        base_revision=candidate.request.base_revision,
        objects=(*objects, commit),
        writes=tuple(writes),
        policy=policy or PublicationPolicy(),
    )


class GitHub:
    def __init__(self, value: PublicationPlan, *, token: str = TOKEN) -> None:
        self.plan = value
        self.token = token
        self.requests: list[httpx.Request] = []
        self.values: dict[str, object] = {}
        self.branch: object | None = None
        self.pull_requests: list[object] = []
        self.hook: Callable[[httpx.Request, httpx.Response], httpx.Response] = (
            lambda _, value: value
        )

    def repo(self) -> dict[str, object]:
        return {
            "id": self.plan.provider_repository_id,
            "full_name": self.plan.repository_slug,
            "fork": False,
            "archived": False,
            "disabled": False,
        }

    def ref(self) -> dict[str, object]:
        return {
            "ref": "refs/heads/" + self.plan.source_branch,
            "object": {"type": "commit", "sha": self.plan.head_revision},
        }

    def pr(self) -> dict[str, object]:
        return {
            "number": 42,
            "state": "open",
            "draft": False,
            "merged": False,
            "merged_at": None,
            "head": {
                "ref": self.plan.source_branch,
                "sha": self.plan.head_revision,
                "repo": self.repo(),
            },
            "base": {
                "ref": self.plan.target_branch,
                "sha": self.plan.base_revision,
                "repo": self.repo(),
            },
        }

    def object(self, write: PublicationWrite) -> object:
        body = json.loads(write.body)
        result: dict[str, object] = {"sha": write.expected_sha, **body}
        if write.kind is PublicationWriteKind.BLOB:
            result["size"] = len(base64.b64decode(body["content"]))
        elif write.kind is PublicationWriteKind.TREE:
            result["truncated"] = False
        else:
            result["tree"] = {"sha": body["tree"]}
            result["parents"] = [{"sha": sha} for sha in body["parents"]]
            result["verification"] = {
                "verified": False,
                "reason": "unsigned",
                "signature": None,
                "payload": None,
                "verified_at": None,
            }
        return result

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.url.scheme == "https" and request.url.host == "api.github.com"
        assert request.headers["authorization"] == "Bearer " + self.token
        prefix = "/repos/" + self.plan.repository_slug
        path = request.url.path
        status = 200
        result: object
        if request.method == "POST":
            command = next(
                item
                for item in self.plan.writes
                if "/" + item.path == path and request.content == item.body
            )
            assert request.content == command.body
            status = 201
            if command.kind is PublicationWriteKind.BRANCH:
                self.branch = self.ref()
                result = self.branch
            elif command.kind is PublicationWriteKind.PULL_REQUEST:
                self.pull_requests.append(self.pr())
                result = self.pull_requests[0]
            else:
                self.values[path + "/" + str(command.expected_sha)] = self.object(
                    command
                )
                result = {"sha": command.expected_sha}
        else:
            assert request.method == "GET"
            if path == prefix:
                result = self.repo()
            elif path == prefix + "/pulls":
                assert (
                    request.url.params["head"]
                    == self.plan.repository_slug.split("/")[0]
                    + ":"
                    + self.plan.source_branch
                )
                assert request.url.params["base"] == self.plan.target_branch
                result = self.pull_requests
            elif path == prefix + "/pulls/42" and self.pull_requests:
                result = self.pull_requests[0]
            elif (
                path.startswith(prefix + "/git/ref/heads/") and self.branch is not None
            ):
                result = self.branch
            elif path in self.values:
                result = self.values[path]
            else:
                status, result = 404, {"message": "not found"}
        return self.hook(request, httpx.Response(status, json=result))

    def provider(
        self,
        *,
        clock: Callable[[], float] | None = None,
        binding_prefix: str | None = None,
        **changes: Any,
    ) -> HttpGitHubCodingPublicationProvider:
        options: dict[str, Any] = dict(
            base_url="https://api.github.com/",
            headers={
                "Authorization": "Bearer " + self.token,
                "X-GitHub-Api-Version": "2026-03-10",
            },
            transport=httpx.MockTransport(self),
            trust_env=False,
            follow_redirects=False,
        )
        options.update(changes)
        connection = httpx.AsyncClient(**options)
        binding = RepositoryBinding(
            repository_id=self.plan.repository_id,
            github_id=self.plan.provider_repository_id,
            slug=self.plan.repository_slug,
            target_branch=self.plan.target_branch,
            coding_branch_prefix=self.plan.source_branch.rsplit("/", 1)[0]
            if binding_prefix is None
            else binding_prefix,
        )
        provider_options: dict[str, Any] = {} if clock is None else {"monotonic": clock}
        return HttpGitHubCodingPublicationProvider(
            connection,
            bindings=(binding,),
            policy=self.plan.policy,
            now=lambda: NOW,
            **provider_options,
        )


def test_full_exact_objects_create_only_branch_pr_and_independent_final_verification() -> (
    None
):
    value = plan()
    remote = GitHub(value)
    guards: list[int] = []

    async def scenario() -> None:
        provider = remote.provider()
        try:
            for command in value.writes:

                def guard() -> None:
                    guards.append(len(remote.requests))

                observed = await provider.write(value, command, before_write=guard)
                assert (
                    observed.write_id == command.write_id
                    and observed.input_digest == command.input_digest
                )
            verified = await provider.verify(value, pull_request_number=42)
            assert verified.reference.head_revision == value.head_revision
            assert verified.reference.target_branch == value.target_branch
            assert (
                verified.provider_repository_id == value.provider_repository_id
                and verified.observed_at == NOW
            )
            assert observed.pull_request_number == 42
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    posts = [
        index
        for index, request in enumerate(remote.requests)
        if request.method == "POST"
    ]
    assert guards == posts and len(posts) == len(value.writes)


def test_nested_multiple_blob_tree_writes_read_back_exact_child_first_objects() -> None:
    files = tuple(
        BaseSourceFile(path=path, mode=mode, blob_sha=git_blob_sha(data), content=data)
        for path, mode, data in (
            ("README.md", BaseFileMode.REGULAR, b"root\n"),
            ("src/deep/first.py", BaseFileMode.REGULAR, b"nested\n"),
            ("src/deep/second.py", BaseFileMode.REGULAR, b"nested\n"),
            ("tools/run.sh", BaseFileMode.EXECUTABLE, b"#!/bin/sh\nexit 0\n"),
        )
    )
    value = plan(files=files)
    remote = GitHub(value, token="different_fixture_write_token_0123456789abcdefgh")

    async def scenario() -> None:
        provider = remote.provider()
        try:
            for command in value.writes:
                await provider.write(value, command, before_write=lambda: None)
            verified = await provider.verify(value, pull_request_number=42)
            assert verified.reference.head_revision == value.head_revision
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    posts = [request for request in remote.requests if request.method == "POST"]
    assert len(posts) == len(value.writes)
    assert (
        sum(command.kind is PublicationWriteKind.BLOB for command in value.writes) == 3
    )
    assert (
        sum(command.kind is PublicationWriteKind.TREE for command in value.writes) == 4
    )


@pytest.mark.parametrize("kind", list(PublicationWriteKind))
def test_readonly_observation_reconciles_exact_effect_without_post(
    kind: PublicationWriteKind,
) -> None:
    value = plan()
    remote = GitHub(value)
    command = next(item for item in value.writes if item.kind is kind)
    if kind is PublicationWriteKind.BRANCH:
        remote.branch = remote.ref()
    elif kind is PublicationWriteKind.PULL_REQUEST:
        remote.pull_requests = [remote.pr()]
    else:
        remote.values["/" + command.path + "/" + str(command.expected_sha)] = (
            remote.object(command)
        )

    async def scenario() -> None:
        provider = remote.provider()
        try:
            first = await provider.observe(value, command)
            assert first is not None and first == await provider.observe(value, command)
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert all(request.method == "GET" for request in remote.requests)


@pytest.mark.parametrize("kind", list(PublicationWriteKind))
def test_missing_observation_never_creates_resource(kind: PublicationWriteKind) -> None:
    value = plan()
    remote = GitHub(value)
    command = next(item for item in value.writes if item.kind is kind)

    async def scenario() -> None:
        provider = remote.provider()
        try:
            assert await provider.observe(value, command) is None
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert all(request.method == "GET" for request in remote.requests)


@pytest.mark.parametrize(
    "kind", [PublicationWriteKind.BRANCH, PublicationWriteKind.PULL_REQUEST]
)
def test_existing_even_matching_resource_is_not_a_new_write_right(
    kind: PublicationWriteKind,
) -> None:
    value = plan()
    remote = GitHub(value)
    command = next(item for item in value.writes if item.kind is kind)
    remote.branch = remote.ref()
    remote.pull_requests = [remote.pr()]

    async def scenario() -> None:
        provider = remote.provider()
        try:
            with pytest.raises(CodingResultRejected):
                await provider.write(value, command, before_write=lambda: None)
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert all(request.method == "GET" for request in remote.requests)


@pytest.mark.parametrize(
    "failure",
    [
        "repo",
        "returned_sha",
        "blob_bytes",
        "tree_mode",
        "commit_date",
        "commit_signed",
        "branch",
        "pr_head",
        "pr_base",
        "pr_repo",
        "pr_draft",
        "pr_merged",
        "duplicate_pr",
    ],
)
def test_wrong_provider_identity_or_independently_read_content_is_rejected(
    failure: str,
) -> None:
    value = plan()
    remote = GitHub(value)
    kind = PublicationWriteKind.BLOB
    if failure.startswith("tree"):
        kind = PublicationWriteKind.TREE
    elif failure.startswith("commit"):
        kind = PublicationWriteKind.COMMIT
    elif failure == "branch":
        kind = PublicationWriteKind.BRANCH
    elif failure.startswith("pr_") or failure == "duplicate_pr":
        kind = PublicationWriteKind.PULL_REQUEST
    command = next(item for item in value.writes if item.kind is kind)

    def hook(request: httpx.Request, response: httpx.Response) -> httpx.Response:
        data = response.json()
        if failure == "repo" and request.url.path == "/repos/" + value.repository_slug:
            data["id"] = True
        elif failure == "returned_sha" and request.method == "POST":
            data["sha"] = "f" * 40
        elif request.method == "GET" and request.url.path.endswith(
            str(command.expected_sha)
        ):
            if failure == "blob_bytes":
                data["content"] = base64.b64encode(b"changed").decode()
            elif failure == "tree_mode":
                data["tree"][0]["mode"] = "100755"
            elif failure == "commit_date":
                data["author"]["date"] = "2024-01-01T00:00:00Z"
            elif failure == "commit_signed":
                data["verification"]["verified"] = True
        elif (
            failure == "branch"
            and request.method == "GET"
            and "/git/ref/" in request.url.path
            and response.status_code == 200
        ):
            data["object"]["sha"] = "f" * 40
        elif failure.startswith("pr_") and request.url.path.endswith("/pulls/42"):
            if failure == "pr_head":
                data["head"]["sha"] = "f" * 40
            elif failure == "pr_base":
                data["base"]["ref"] = "elsewhere"
            elif failure == "pr_repo":
                data["head"]["repo"]["id"] += 1
            elif failure == "pr_draft":
                data["draft"] = True
            elif failure == "pr_merged":
                data["merged"] = True
        return httpx.Response(response.status_code, json=data)

    remote.hook = hook

    async def scenario() -> None:
        provider = remote.provider()
        try:
            if failure == "duplicate_pr":
                remote.pull_requests = [remote.pr(), remote.pr()]
                with pytest.raises(CodingResultRejected):
                    await provider.observe(value, command)
            else:
                with pytest.raises(CodingResultRejected):
                    await provider.write(value, command, before_write=lambda: None)
        finally:
            await provider.aclose()

    asyncio.run(scenario())


def test_lost_post_response_is_never_retried_and_exact_object_can_be_read_only_reconciled() -> (
    None
):
    value = plan()
    remote = GitHub(value)
    command = value.writes[0]

    def lost(request: httpx.Request, response: httpx.Response) -> httpx.Response:
        if request.method == "POST":
            raise httpx.ReadTimeout("private " + TOKEN, request=request)
        return response

    remote.hook = lost

    async def scenario() -> None:
        provider = remote.provider()
        try:
            with pytest.raises(CodingResultUnavailable) as error:
                await provider.write(value, command, before_write=lambda: None)
            assert TOKEN not in str(error.value) and error.value.__suppress_context__
            with pytest.raises(CodingResultRejected):
                await provider.write(value, command, before_write=lambda: None)
            assert await provider.observe(value, command) is not None
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert sum(request.method == "POST" for request in remote.requests) == 1


@pytest.mark.parametrize("visible", [False, True])
def test_lost_pr_response_empty_or_matching_read_never_authorizes_second_post(
    visible: bool,
) -> None:
    value = plan()
    remote = GitHub(value)
    command = value.writes[-1]

    def lost(request: httpx.Request, response: httpx.Response) -> httpx.Response:
        if request.method == "POST":
            raise httpx.ReadTimeout("fixture lost response", request=request)
        return response

    remote.hook = lost

    async def scenario() -> None:
        provider = remote.provider()
        try:
            with pytest.raises(CodingResultUnavailable):
                await provider.write(value, command, before_write=lambda: None)
            if not visible:
                remote.pull_requests.clear()
            observed = await provider.observe(value, command)
            assert (observed is not None) == visible
            with pytest.raises(CodingResultRejected):
                await provider.write(value, command, before_write=lambda: None)
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert sum(request.method == "POST" for request in remote.requests) == 1


def test_total_response_bytes_cannot_reset_between_readonly_calls() -> None:
    value = plan(
        policy=replace(
            PublicationPolicy(), max_response_bytes=1024, max_total_response_bytes=4096
        )
    )
    remote = GitHub(value)

    def padded(request: httpx.Request, response: httpx.Response) -> httpx.Response:
        if request.url.path == "/repos/" + value.repository_slug:
            return httpx.Response(
                200, json={**remote.repo(), "harmless_documented_metadata": "x" * 700}
            )
        return response

    remote.hook = padded

    async def scenario() -> None:
        provider = remote.provider()
        completed = 0
        try:
            with pytest.raises(CodingResultUnavailable):
                for _ in range(8):
                    assert await provider.observe(value, value.writes[0]) is None
                    completed += 1
            assert completed >= 1
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert 3 < len(remote.requests) < 24
    assert all(request.method == "GET" for request in remote.requests)


def test_utf8_surrogate_in_ignored_metadata_array_is_rejected() -> None:
    value = plan()
    remote = GitHub(value)
    remote.hook = lambda request, response: httpx.Response(
        200,
        content=canonical({**remote.repo(), "ignored": ["\ud800"]}),
        headers={"content-type": "application/json"},
    )

    async def scenario() -> None:
        provider = remote.provider()
        try:
            with pytest.raises(CodingResultRejected):
                await provider.observe(value, value.writes[0])
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert len(remote.requests) == 1


@pytest.mark.parametrize("trailing_slash", [False, True])
def test_sibling_coding_branch_prefix_never_authorizes_any_provider_io(
    trailing_slash: bool,
) -> None:
    value = plan()
    prefix, tail = value.source_branch.rsplit("/", 1)
    sibling = prefix + "-evil/" + tail
    commands = list(value.writes)
    branch = json.loads(commands[-2].body)
    branch["ref"] = "refs/heads/" + sibling
    commands[-2] = replace(commands[-2], body=canonical(branch))
    pr = json.loads(commands[-1].body)
    pr["head"] = sibling
    commands[-1] = replace(commands[-1], body=canonical(pr))
    value = replace(value, source_branch=sibling, writes=tuple(commands))
    remote = GitHub(value)

    async def scenario() -> None:
        provider = remote.provider(
            binding_prefix=prefix + ("/" if trailing_slash else "")
        )
        try:
            with pytest.raises(CodingResultRejected):
                await provider.write(value, value.writes[0], before_write=lambda: None)
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert remote.requests == []


@pytest.mark.parametrize(
    "failure", ["redirect", "auth", "size", "duplicate", "nonfinite", "float"]
)
def test_bounded_http_and_json_rejections_never_retry(failure: str) -> None:
    policy = replace(PublicationPolicy(), max_response_bytes=1024)
    value = plan(policy=policy)
    remote = GitHub(value)

    def bad(request: httpx.Request, response: httpx.Response) -> httpx.Response:
        if failure == "redirect":
            return httpx.Response(307, headers={"location": "https://other.invalid/"})
        if failure == "auth":
            return httpx.Response(403, json={"message": TOKEN})
        if failure == "size":
            return httpx.Response(
                200, content=b" " * 1025, headers={"content-type": "application/json"}
            )
        if failure == "duplicate":
            return httpx.Response(
                200,
                content=b'{"id":1,"id":2}',
                headers={"content-type": "application/json"},
            )
        if failure == "nonfinite":
            return httpx.Response(
                200, content=b'{"id":NaN}', headers={"content-type": "application/json"}
            )
        return httpx.Response(
            200, content=b'{"id":1.0}', headers={"content-type": "application/json"}
        )

    remote.hook = bad

    async def scenario() -> None:
        provider = remote.provider()
        try:
            with pytest.raises(
                (CodingResultRejected, CodingResultUnavailable)
            ) as error:
                await provider.write(value, value.writes[0], before_write=lambda: None)
            assert TOKEN not in str(error.value)
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert len(remote.requests) == 1


@pytest.mark.parametrize(
    "failure", ["path", "branch", "object", "unknown", "noncanonical", "external_write"]
)
def test_forged_plan_commands_cannot_write_outside_exact_object_and_branch_pins(
    failure: str,
) -> None:
    value = plan()
    commands = list(value.writes)
    selected = 0
    if failure == "branch":
        selected = -2
        body = json.loads(commands[selected].body)
        body["ref"] = "refs/heads/main"
        commands[selected] = replace(commands[selected], body=canonical(body))
    elif failure == "path":
        commands[0] = replace(commands[0], path="repos/other/repository/git/blobs")
    elif failure == "object":
        body = json.loads(commands[0].body)
        body["content"] = base64.b64encode(b"forged").decode()
        commands[0] = replace(commands[0], body=canonical(body))
    elif failure == "unknown":
        body = json.loads(commands[0].body)
        body["unexpected"] = True
        commands[0] = replace(commands[0], body=canonical(body))
    elif failure == "noncanonical":
        commands[0] = replace(commands[0], body=commands[0].body + b" ")
    if failure == "path":
        object.__setattr__(value, "writes", tuple(commands))
    else:
        value = replace(value, writes=tuple(commands))
    remote = GitHub(value)
    command = commands[selected]
    if failure == "external_write":
        command = replace(command, write_id=uuid4())

    async def scenario() -> None:
        provider = remote.provider()
        try:
            with pytest.raises(CodingResultRejected):
                await provider.write(value, command, before_write=lambda: None)
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert remote.requests == []


def test_final_callback_refusal_happens_after_all_preflight_awaits_without_post() -> (
    None
):
    value = plan()
    remote = GitHub(value)
    calls: list[str] = []

    class Stopped(RuntimeError):
        pass

    def guard() -> None:
        assert len(remote.requests) == 2  # repository identity and missing ref
        calls.append("guard")
        raise Stopped

    async def scenario() -> None:
        provider = remote.provider()
        try:
            with pytest.raises(Stopped):
                await provider.write(value, value.writes[-2], before_write=guard)
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert calls == ["guard"] and all(
        request.method == "GET" for request in remote.requests
    )


@pytest.mark.parametrize("stage", ["_prepare", "_repo", "_object_response"])
def test_cancelled_offloop_validation_retains_admission_and_close_waits_real_future(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    value = plan()
    remote = GitHub(value)
    entered, release = threading.Event(), threading.Event()
    original = getattr(adapter, stage)

    def held(*args: Any) -> Any:
        entered.set()
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(adapter, stage, held)

    async def scenario() -> None:
        provider = remote.provider()
        pending = asyncio.create_task(
            provider.write(value, value.writes[0], before_write=lambda: None)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            with pytest.raises(CodingResultUnavailable):
                await provider.observe(value, value.writes[0])
            closing = asyncio.create_task(provider.aclose())
            await asyncio.sleep(0)
            assert not closing.done()
            release.set()
            await asyncio.wait_for(closing, 2)
        finally:
            release.set()
            await provider.aclose()

    asyncio.run(scenario())
    assert (
        len(remote.requests)
        == {"_prepare": 0, "_repo": 1, "_object_response": 3}[stage]
    )


def test_request_budget_is_retained_across_repeated_readonly_operations() -> None:
    value = plan(policy=replace(PublicationPolicy(), max_requests=28))
    remote = GitHub(value)

    async def scenario() -> None:
        provider = remote.provider()
        try:
            for _ in range(9):
                assert await provider.observe(value, value.writes[0]) is None
            with pytest.raises(CodingResultUnavailable):
                await provider.observe(value, value.writes[0])
            assert len(remote.requests) == 28
            with pytest.raises(CodingResultUnavailable):
                await provider.observe(copy.deepcopy(value), value.writes[0])
        finally:
            await provider.aclose()

    asyncio.run(scenario())
    assert len(remote.requests) == 28


@pytest.mark.parametrize(
    "change", ["url", "version", "redirects", "proxy", "cookies", "auth"]
)
def test_client_requires_fixed_version_destination_and_credential_channel(
    change: str,
) -> None:
    remote = GitHub(plan())
    options: dict[str, Any] = {}
    if change == "url":
        options["base_url"] = "https://elsewhere.invalid/"
    elif change == "version":
        options["headers"] = {
            "Authorization": "Bearer " + TOKEN,
            "X-GitHub-Api-Version": "2022-11-28",
        }
    elif change == "redirects":
        options["follow_redirects"] = True
    elif change == "proxy":
        options["trust_env"] = True
    elif change == "cookies":
        options["cookies"] = {"private": "fixture"}
    else:
        options["auth"] = ("fixture", "fixture")
    with pytest.raises(CodingResultRejected):
        remote.provider(**options)
    assert remote.requests == []


@pytest.mark.parametrize("expired", [91.0, float("nan"), float("inf"), -1.0])
def test_retained_total_clock_cannot_reset_or_regress_between_operations(
    expired: float,
) -> None:
    value = plan()
    remote = GitHub(value)
    current = [0.0]

    async def scenario() -> None:
        provider = remote.provider(clock=lambda: current[0])
        try:
            assert await provider.observe(value, value.writes[0]) is None
            requests = len(remote.requests)
            current[0] = expired
            with pytest.raises(CodingResultUnavailable):
                await provider.observe(copy.deepcopy(value), value.writes[0])
            assert len(remote.requests) == requests
        finally:
            await provider.aclose()

    asyncio.run(scenario())


def test_input_graph_is_owned_before_first_encoder_await(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = plan()
    remote = GitHub(copy.deepcopy(value))
    original = adapter._prepare
    entered, release = threading.Event(), threading.Event()

    def held(*args: Any) -> Any:
        entered.set()
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(adapter, "_prepare", held)

    async def scenario() -> None:
        provider = remote.provider()
        pending = asyncio.create_task(
            provider.write(value, value.writes[0], before_write=lambda: None)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            object.__setattr__(value, "source_branch", "main")
            object.__setattr__(value.writes[0], "body", b'{"unexpected":true}')
            release.set()
            observed = await pending
            assert observed.input_digest == remote.plan.writes[0].input_digest
        finally:
            release.set()
            await provider.aclose()

    asyncio.run(scenario())
    posts = [item for item in remote.requests if item.method == "POST"]
    assert len(posts) == 1 and posts[0].content == remote.plan.writes[0].body


def test_close_during_encoding_stops_new_mutation_but_drains_real_encoder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = plan()
    remote = GitHub(value)
    original = adapter._prepare
    entered, release = threading.Event(), threading.Event()

    def held(*args: Any) -> Any:
        entered.set()
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(adapter, "_prepare", held)

    async def scenario() -> None:
        provider = remote.provider()
        pending = asyncio.create_task(
            provider.write(value, value.writes[0], before_write=lambda: None)
        )
        closing: asyncio.Task[None] | None = None
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            closing = asyncio.create_task(provider.aclose())
            await asyncio.sleep(0)
            assert not closing.done()
            release.set()
            with pytest.raises(CodingResultUnavailable):
                await pending
            await closing
        finally:
            release.set()
            await asyncio.gather(pending, return_exceptions=True)
            if closing is not None:
                await closing
            await provider.aclose()

    asyncio.run(scenario())
    assert all(item.method == "GET" for item in remote.requests)
