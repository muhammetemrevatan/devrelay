import asyncio
import traceback
from collections.abc import AsyncIterator, Callable
from dataclasses import replace
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from devrelay.application.contracts.submissions import (
    JobBranchAssociation,
    RepositorySubmissionObservation,
)
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.ports.repository_source import (
    InvalidRepositorySourceData,
    RepositorySourceAccessDenied,
    RepositorySourceError,
    RepositorySourceUnavailable,
)
from devrelay.bootstrap.github import open_github_repository_source
from devrelay.config.github import GitHubRepositoryId, GitHubSettings
from devrelay.config.repositories import CheckSettings, RepositorySettings

REPO_ID = UUID("11111111-2222-3333-4444-555555555555")
SOURCE_ID = UUID("22222222-2222-3333-4444-555555555555")
JOB_ID = UUID("33333333-2222-3333-4444-555555555555")
TOKEN = "test-only-github-token-never-disclose"
BRANCH = f"devrelay/jobs/{JOB_ID.hex}"


def repository(**changes: object) -> RepositorySettings:
    values: dict[str, object] = {
        "repository_id": REPO_ID,
        "slug": "example/devrelay",
        "enabled": True,
        "target_branch": "main",
        "coding_branch_prefix": "devrelay/jobs",
        "policy_version": "v1",
        "allowed_project_keys": ["DRY"],
        "checks": (
            CheckSettings(
                id="pytest", argv=("uv", "run", "pytest"), timeout_seconds=300
            ),
        ),
        "max_repair_cycles": 2,
    }
    return RepositorySettings.model_validate(values | changes)


def settings(**changes: object) -> GitHubSettings:
    values: dict[str, object] = {
        "api_token": SecretStr(TOKEN),
        "repository_ids": (GitHubRepositoryId(repository_id=REPO_ID, github_id=123),),
    }
    return GitHubSettings.model_validate(values | changes)


def metadata() -> dict[str, object]:
    return {
        "id": 123,
        "full_name": "example/devrelay",
        "archived": False,
        "disabled": False,
    }


def reference() -> dict[str, object]:
    return {"ref": "refs/heads/main", "object": {"type": "commit", "sha": "a" * 40}}


def pull_request(
    branch: str = BRANCH, *, number: int = 7, repo_id: int = 123, base: str = "main"
) -> dict[str, object]:
    return {
        "number": number,
        "state": "open",
        "title": TOKEN,
        "body": TOKEN,
        "head": {
            "ref": branch,
            "sha": "b" * 40,
            "repo": {"id": repo_id, "full_name": "example/devrelay"},
        },
        "base": {
            "ref": base,
            "sha": "a" * 40,
            "repo": {"id": 123, "full_name": "example/devrelay"},
        },
    }


def association() -> JobBranchAssociation:
    return JobBranchAssociation(
        job_id=JOB_ID,
        repository_id=REPO_ID,
        source_task_id=SOURCE_ID,
        authorized_branch=BRANCH,
    )


class Branches:
    def __init__(self, rows: tuple[JobBranchAssociation, ...] = ()) -> None:
        self.rows = rows
        self.calls: list[tuple[UUID, tuple[UUID, ...]]] = []

    async def find_by_ids(
        self, *, repository_id: UUID, job_ids: tuple[UUID, ...]
    ) -> tuple[JobBranchAssociation, ...]:
        self.calls.append((repository_id, job_ids))
        return self.rows


def handler(
    page: list[dict[str, object]], requests: list[httpx.Request]
) -> Callable[[httpx.Request], httpx.Response]:
    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/pulls"):
            return httpx.Response(200, json=page)
        if "/git/ref/" in request.url.path:
            return httpx.Response(200, json=reference())
        return httpx.Response(200, json=metadata())

    return respond


def observe(
    respond: Callable[[httpx.Request], httpx.Response],
    branches: Branches | None = None,
    *,
    configuration: GitHubSettings | None = None,
    policy: RepositorySettings | None = None,
    source_task_id: UUID = SOURCE_ID,
) -> RepositorySubmissionObservation:
    async def scenario() -> RepositorySubmissionObservation:
        async with open_github_repository_source(
            configuration or settings(),
            (policy or repository(),),
            branches or Branches(),
            transport=httpx.MockTransport(respond),
        ) as source:
            return await source.observe(
                repository_id=REPO_ID,
                target_branch=(policy or repository()).target_branch,
                source_task_id=source_task_id,
            )

    return asyncio.run(scenario())


def test_repository_head_is_verified_with_read_only_bounded_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://invalid-proxy.example.test")
    requests: list[httpx.Request] = []
    branches = Branches()
    result = observe(handler([], requests), branches)
    assert result.head.repository_id == REPO_ID
    assert result.head.revision == "a" * 40
    assert result.unresolved_pull_request is None
    assert branches.calls == []
    assert len(requests) == 4
    for request in requests:
        assert request.method == "GET"
        assert request.url.host == "api.github.com"
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        assert request.headers["x-github-api-version"] == "2026-03-10"
        assert request.headers["accept-encoding"] == "identity"
    assert dict(requests[2].url.params) == {
        "state": "open",
        "sort": "created",
        "direction": "asc",
        "per_page": "100",
        "page": "1",
    }


@pytest.mark.parametrize("target", ["main", "release/v1.0"])
def test_only_configured_literal_branch_is_read(target: str) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if "/git/ref/" in request.url.path:
            return httpx.Response(
                200, json={**reference(), "ref": f"refs/heads/{target}"}
            )
        return handler([], [])(request)

    assert (
        observe(respond, policy=repository(target_branch=target)).head.target_branch
        == target
    )
    assert requests[1].url.raw_path.endswith(target.replace("/", "%2F").encode())


def test_stored_branch_association_blocks_even_after_prefix_change() -> None:
    branches = Branches((association(),))
    result = observe(
        handler([pull_request(base="release/previous")], []),
        branches,
        policy=repository(coding_branch_prefix="new/jobs"),
    )
    assert result.unresolved_pull_request is not None
    assert result.unresolved_pull_request.number == 7
    assert result.unresolved_pull_request.target_branch == "release/previous"
    assert branches.calls == [(REPO_ID, (JOB_ID,))]


def test_other_task_and_unrelated_pr_text_do_not_authorize_or_block() -> None:
    page = [pull_request(), pull_request("human/change", number=8)]
    result = observe(
        handler(page, []), Branches((replace(association(), source_task_id=uuid4()),))
    )
    assert result.unresolved_pull_request is None


@pytest.mark.parametrize(
    "rows, branch",
    [
        ((), BRANCH),
        ((), "devrelay/jobs/not-a-job"),
        ((replace(association(), repository_id=uuid4()),), BRANCH),
        ((replace(association(), authorized_branch="other/branch"),), BRANCH),
        ((association(), association()), BRANCH),
    ],
)
def test_unknown_or_inconsistent_managed_association_fails_closed(
    rows: tuple[JobBranchAssociation, ...], branch: str
) -> None:
    with pytest.raises(InvalidRepositorySourceData):
        observe(handler([pull_request(branch)], []), Branches(rows))


def test_fork_head_cannot_claim_a_recorded_job_branch() -> None:
    with pytest.raises(InvalidRepositorySourceData):
        observe(handler([pull_request(repo_id=456)], []), Branches((association(),)))


def test_ambiguous_associated_pull_requests_fail_closed() -> None:
    with pytest.raises(InvalidRepositorySourceData):
        observe(
            handler([pull_request(number=7), pull_request(number=8)], []),
            Branches((association(),)),
        )


def test_duplicate_pull_requests_are_not_silently_collapsed() -> None:
    with pytest.raises(InvalidRepositorySourceData):
        observe(handler([pull_request(), pull_request()], []))


@pytest.mark.parametrize(
    "change",
    [
        {"id": 456},
        {"id": True},
        {"full_name": "other/repo"},
        {"archived": True},
        {"disabled": True},
    ],
)
def test_repository_identity_and_availability_must_match(
    change: dict[str, object],
) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=metadata() | change)

    with pytest.raises((RepositorySourceAccessDenied, InvalidRepositorySourceData)):
        observe(respond)
    assert len(requests) == 1


def test_repository_identity_is_rechecked_after_observation() -> None:
    requests: list[httpx.Request] = []
    normal = handler([], requests)

    def respond(request: httpx.Request) -> httpx.Response:
        if len(requests) == 3:
            return httpx.Response(200, json=metadata() | {"id": 456})
        return normal(request)

    with pytest.raises(RepositorySourceAccessDenied):
        observe(respond)


@pytest.mark.parametrize(
    "status, expected",
    [
        (401, RepositorySourceAccessDenied),
        (403, RepositorySourceAccessDenied),
        (404, RepositorySourceAccessDenied),
        (408, RepositorySourceUnavailable),
        (429, RepositorySourceUnavailable),
        (500, RepositorySourceUnavailable),
        (503, RepositorySourceUnavailable),
        (301, InvalidRepositorySourceData),
        (302, InvalidRepositorySourceData),
        (409, InvalidRepositorySourceData),
        (422, InvalidRepositorySourceData),
    ],
)
def test_status_errors_are_sanitized_without_following_remote_urls(
    status: int, expected: type[RepositorySourceError]
) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            status,
            json={"message": TOKEN},
            headers={"location": f"https://evil.example.test/{TOKEN}"},
        )

    with pytest.raises(expected) as captured:
        observe(respond)
    assert TOKEN not in "".join(traceback.format_exception(captured.value))
    assert len(requests) == 1


@pytest.mark.parametrize(
    "headers", [{"retry-after": "60"}, {"x-ratelimit-remaining": "0"}]
)
def test_rate_limited_403_is_transient(headers: dict[str, str]) -> None:
    with pytest.raises(RepositorySourceUnavailable):
        observe(
            lambda request: httpx.Response(
                403, json={"message": TOKEN}, headers=headers
            )
        )


@pytest.mark.parametrize("error_type", [httpx.ConnectError, httpx.ReadTimeout])
def test_network_exception_values_are_not_exposed(
    error_type: type[httpx.RequestError],
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        raise error_type(TOKEN, request=request)

    with pytest.raises(RepositorySourceUnavailable) as captured:
        observe(respond)
    assert TOKEN not in "".join(traceback.format_exception(captured.value))


@pytest.mark.parametrize(
    "body, headers",
    [
        (b"not-json", {"content-type": "application/json"}),
        (b"{}", {"content-type": "text/html"}),
        (b"{}", {"content-type": "application/json", "content-encoding": "br"}),
        (b"x" * 1025, {"content-type": "application/json"}),
    ],
)
def test_response_validation_and_byte_limits_fail_closed(
    body: bytes, headers: dict[str, str]
) -> None:
    with pytest.raises(InvalidRepositorySourceData):
        observe(
            lambda request: httpx.Response(200, content=body, headers=headers),
            configuration=settings(max_response_bytes=1024),
        )


def test_full_last_page_is_not_assumed_to_be_complete() -> None:
    page = [pull_request("human/change", number=index + 1) for index in range(100)]
    with pytest.raises(InvalidRepositorySourceData):
        observe(handler(page, []), configuration=settings(max_pull_request_pages=1))


def test_pagination_uses_fixed_endpoint_and_bounded_queries() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/pulls"):
            page = (
                [pull_request("human/change", number=index + 1) for index in range(100)]
                if request.url.params["page"] == "1"
                else [pull_request(number=101)]
            )
            return httpx.Response(
                200,
                json=page,
                headers={"link": f'<https://evil.example.test/{TOKEN}>; rel="next"'},
            )
        return handler([], [])(request)

    branches = Branches((association(),))
    assert observe(respond, branches).unresolved_pull_request is not None
    assert len(requests) == 5
    assert branches.calls == [(REPO_ID, (JOB_ID,))]
    assert all(request.url.host == "api.github.com" for request in requests)


def test_disabled_repository_causes_no_network_read() -> None:
    with pytest.raises(RepositorySourceAccessDenied):
        observe(
            lambda request: pytest.fail("Disabled repository must not access GitHub."),
            policy=repository(enabled=False),
        )


def test_total_observation_timeout_and_cancellation_are_preserved() -> None:
    async def scenario() -> None:
        entered = asyncio.Event()

        async def respond(request: httpx.Request) -> httpx.Response:
            entered.set()
            await asyncio.Event().wait()
            raise AssertionError("Unreachable.")

        async with open_github_repository_source(
            settings(observation_timeout_seconds=0.05),
            (repository(),),
            Branches(),
            transport=httpx.MockTransport(respond),
        ) as source:
            with pytest.raises(RepositorySourceUnavailable):
                await source.observe(
                    repository_id=REPO_ID,
                    target_branch="main",
                    source_task_id=SOURCE_ID,
                )
            entered.clear()
            task = asyncio.create_task(
                source.observe(
                    repository_id=REPO_ID,
                    target_branch="main",
                    source_task_id=SOURCE_ID,
                )
            )
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "change",
    [
        {"ref": "refs/heads/other"},
        {"object": {"type": "tag", "sha": "a" * 40}},
        {"object": {"type": "commit", "sha": "short"}},
        {"object": {"type": "commit", "sha": "A" * 40}},
    ],
)
def test_branch_reference_must_be_an_exact_full_commit(
    change: dict[str, object],
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if "/git/ref/" in request.url.path:
            return httpx.Response(200, json=reference() | change)
        return handler([], [])(request)

    with pytest.raises(InvalidRepositorySourceData):
        observe(respond)


@pytest.mark.parametrize(
    "change",
    [
        {"number": True},
        {"state": "closed"},
        {
            "base": {
                "ref": "main",
                "sha": "a" * 40,
                "repo": {"id": 456, "full_name": "other/repo"},
            }
        },
        {"head": {"ref": BRANCH, "sha": "b" * 40, "repo": None}},
    ],
)
def test_inconsistent_pull_request_evidence_is_rejected(
    change: dict[str, object],
) -> None:
    with pytest.raises(InvalidRepositorySourceData):
        observe(handler([pull_request() | change], []), Branches((association(),)))


def test_page_size_is_bounded_before_any_storage_lookup() -> None:
    branches = Branches()
    page = [pull_request("human/change", number=index + 1) for index in range(101)]
    with pytest.raises(InvalidRepositorySourceData):
        observe(handler(page, []), branches)
    assert branches.calls == []


def test_oversized_stream_is_closed_without_consuming_all_bytes() -> None:
    class Oversized(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.closed = False
            self.chunks = 0

        async def __aiter__(self) -> AsyncIterator[bytes]:
            for _ in range(10):
                self.chunks += 1
                yield b"x" * 4096

        async def aclose(self) -> None:
            self.closed = True

    stream = Oversized()
    with pytest.raises(InvalidRepositorySourceData):
        observe(
            lambda request: httpx.Response(
                200, stream=stream, headers={"content-type": "application/json"}
            ),
            configuration=settings(max_response_bytes=1024),
        )
    assert stream.closed
    assert stream.chunks == 1


def test_unconfigured_repository_and_branch_are_rejected_before_io() -> None:
    async def scenario() -> None:
        async with open_github_repository_source(
            settings(),
            (repository(),),
            Branches(),
            transport=httpx.MockTransport(
                lambda request: pytest.fail(
                    "Unauthorized scope must not access GitHub."
                )
            ),
        ) as source:
            for repository_id, target in ((uuid4(), "main"), (REPO_ID, "other")):
                with pytest.raises(RepositorySourceAccessDenied):
                    await source.observe(
                        repository_id=repository_id,
                        target_branch=target,
                        source_task_id=SOURCE_ID,
                    )

    asyncio.run(scenario())


def test_storage_failure_is_not_interpreted_as_no_associated_pr() -> None:
    class FailedBranches(Branches):
        async def find_by_ids(
            self, *, repository_id: UUID, job_ids: tuple[UUID, ...]
        ) -> tuple[JobBranchAssociation, ...]:
            raise JobPersistenceError("Test storage failure.")

    with pytest.raises(JobPersistenceError):
        observe(handler([pull_request()], []), FailedBranches())


@pytest.mark.parametrize("github_id", [True, 0, -1, "123", 2**63])
def test_numeric_repository_identity_is_strict(github_id: object) -> None:
    with pytest.raises(ValidationError):
        GitHubRepositoryId.model_validate(
            {"repository_id": REPO_ID, "github_id": github_id}
        )


@pytest.mark.parametrize("token", ["", " padded", "line\nbreak", "ümlaut", "x" * 513])
def test_credentials_are_bounded_and_validation_does_not_print_them(token: str) -> None:
    with pytest.raises(ValidationError):
        settings(api_token=token)
