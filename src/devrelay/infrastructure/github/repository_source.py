import asyncio
import re
from dataclasses import dataclass
from urllib.parse import quote
from uuid import UUID

import httpx
from pydantic import ValidationError

from devrelay.application.contracts.submissions import (
    RepositoryHead,
    RepositorySubmissionObservation,
)
from devrelay.application.ports.job_persistence import JobBranchReader
from devrelay.application.ports.repository_source import (
    InvalidRepositorySourceData,
    RepositorySourceAccessDenied,
    RepositorySourceUnavailable,
)
from devrelay.domain.jobs.values import PullRequestReference
from devrelay.infrastructure.github.models import (
    PULL_REQUEST_PAGE,
    GitReference,
    PullRequest,
    RepositoryIdentity,
    RepositoryMetadata,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class RepositoryBinding:
    repository_id: UUID
    github_id: int
    slug: str
    target_branch: str
    coding_branch_prefix: str


class GitHubRepositorySource:
    def __init__(
        self,
        client: httpx.AsyncClient,
        branches: JobBranchReader,
        *,
        bindings: tuple[RepositoryBinding, ...],
        request_timeout_seconds: float,
        observation_timeout_seconds: float,
        max_response_bytes: int,
        max_pull_request_pages: int,
    ) -> None:
        self._client = client
        self._branches = branches
        self._bindings = {binding.repository_id: binding for binding in bindings}
        self._request_timeout = request_timeout_seconds
        self._observation_timeout = observation_timeout_seconds
        self._max_bytes = max_response_bytes
        self._max_pages = max_pull_request_pages

    async def observe(
        self,
        *,
        repository_id: UUID,
        target_branch: str,
        source_task_id: UUID,
    ) -> RepositorySubmissionObservation:
        binding = self._bindings.get(repository_id)
        if binding is None or binding.target_branch != target_branch:
            raise RepositorySourceAccessDenied(
                "Repository observation is outside configured scope."
            )
        try:
            async with asyncio.timeout(self._observation_timeout):
                return await self._observe(binding, source_task_id)
        except TimeoutError, httpx.RequestError:
            raise RepositorySourceUnavailable(
                "GitHub observation failed or timed out."
            ) from None
        except ValidationError, ValueError, TypeError, RecursionError:
            raise InvalidRepositorySourceData(
                "GitHub observation failed validation."
            ) from None

    async def _observe(
        self,
        binding: RepositoryBinding,
        source_task_id: UUID,
    ) -> RepositorySubmissionObservation:
        path = f"repos/{binding.slug}"
        await self._verify_repository(path, binding)
        reference = GitReference.model_validate_json(
            await self._get(
                f"{path}/git/ref/heads/{quote(binding.target_branch, safe='')}"
            )
        )
        if reference.ref != f"refs/heads/{binding.target_branch}":
            raise InvalidRepositorySourceData(
                "GitHub returned a different branch reference."
            )
        blocking: PullRequestReference | None = None
        seen: set[int] = set()
        for page_number in range(1, self._max_pages + 1):
            page = PULL_REQUEST_PAGE.validate_json(
                await self._get(
                    f"{path}/pulls",
                    params={
                        "state": "open",
                        "sort": "created",
                        "direction": "asc",
                        "per_page": "100",
                        "page": str(page_number),
                    },
                )
            )
            if any(pr.number in seen for pr in page) or len(
                {pr.number for pr in page}
            ) != len(page):
                raise InvalidRepositorySourceData(
                    "GitHub pagination returned duplicate pull requests."
                )
            seen.update(pr.number for pr in page)
            associated = await self._reconcile_page(binding, source_task_id, page)
            if associated is not None:
                if blocking is not None:
                    raise InvalidRepositorySourceData(
                        "Task has ambiguous open pull request associations."
                    )
                blocking = associated
            if len(page) < 100:
                break
        else:
            raise InvalidRepositorySourceData(
                "GitHub pull request observation exceeds page limits."
            )
        # GitHub reads are non-atomic. Recheck the slug/ID after ref and PR reads.
        await self._verify_repository(path, binding)
        return RepositorySubmissionObservation(
            head=RepositoryHead(
                repository_id=binding.repository_id,
                provider_repository_id=binding.github_id,
                target_branch=binding.target_branch,
                revision=reference.object.sha,
            ),
            unresolved_pull_request=blocking,
        )

    @staticmethod
    def _matches(
        identity: RepositoryIdentity | None, binding: RepositoryBinding
    ) -> bool:
        return (
            identity is not None
            and identity.id == binding.github_id
            and identity.full_name.casefold() == binding.slug.casefold()
        )

    async def _verify_repository(self, path: str, binding: RepositoryBinding) -> None:
        metadata = RepositoryMetadata.model_validate_json(await self._get(path))
        if (
            not self._matches(metadata, binding)
            or metadata.archived
            or metadata.disabled
        ):
            raise RepositorySourceAccessDenied(
                "GitHub repository identity or availability does not match policy."
            )

    async def _reconcile_page(
        self,
        binding: RepositoryBinding,
        source_task_id: UUID,
        page: list[PullRequest],
    ) -> PullRequestReference | None:
        candidates: dict[int, UUID] = {}
        for pr in page:
            if not self._matches(pr.base.repo, binding):
                raise InvalidRepositorySourceData(
                    "GitHub pull request base belongs to another repository."
                )
            tail = pr.head.ref.rsplit("/", 1)[-1]
            if re.fullmatch(r"[0-9a-f]{32}", tail):
                candidates[pr.number] = UUID(hex=tail)
        job_ids = tuple(dict.fromkeys(candidates.values()))
        associations = (
            await self._branches.find_by_ids(
                repository_id=binding.repository_id, job_ids=job_ids
            )
            if job_ids
            else ()
        )
        by_id = {association.job_id: association for association in associations}
        if len(by_id) != len(associations) or any(
            association.repository_id != binding.repository_id
            or association.provider_repository_id != binding.github_id
            or association.job_id not in job_ids
            for association in associations
        ):
            raise InvalidRepositorySourceData(
                "Stored branch lookup returned inconsistent scope."
            )
        blocking: PullRequestReference | None = None
        for pr in page:
            association = (
                by_id.get(candidates[pr.number]) if pr.number in candidates else None
            )
            owned = (
                association is not None and association.authorized_branch == pr.head.ref
            )
            managed = pr.head.ref.startswith(binding.coding_branch_prefix + "/")
            if managed and not owned:
                raise InvalidRepositorySourceData(
                    "Managed pull request branch has no verified stored Job association."
                )
            if not owned:
                continue
            if not self._matches(pr.head.repo, binding):
                raise InvalidRepositorySourceData(
                    "Job pull request head belongs to an unverified repository."
                )
            if association is not None and association.source_task_id == source_task_id:
                if blocking is not None:
                    raise InvalidRepositorySourceData(
                        "Task has ambiguous open pull request associations."
                    )
                blocking = PullRequestReference(
                    repository_id=binding.repository_id,
                    number=pr.number,
                    source_branch=pr.head.ref,
                    target_branch=pr.base.ref,
                    head_revision=pr.head.sha,
                )
        return blocking

    async def _get(self, path: str, *, params: dict[str, str] | None = None) -> bytes:
        async with self._client.stream(
            "GET",
            path,
            params=params,
            timeout=self._request_timeout,
            follow_redirects=False,
        ) as response:
            status = response.status_code
            if (
                status == 429
                or status == 408
                or 500 <= status <= 599
                or (
                    status == 403
                    and (
                        response.headers.get("x-ratelimit-remaining") == "0"
                        or "retry-after" in response.headers
                    )
                )
            ):
                raise RepositorySourceUnavailable(
                    "GitHub is temporarily unavailable or rate limited."
                )
            if status in (401, 403, 404):
                raise RepositorySourceAccessDenied("GitHub repository access failed.")
            if status != 200:
                raise InvalidRepositorySourceData(
                    "GitHub returned an unsupported response status."
                )
            if response.headers.get("content-type", "").split(";", 1)[
                0
            ].strip().lower() not in (
                "application/json",
                "application/vnd.github+json",
            ) or response.headers.get("content-encoding", "identity").lower() not in (
                "",
                "identity",
            ):
                raise InvalidRepositorySourceData(
                    "GitHub response must contain uncompressed JSON."
                )
            body = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=4096):
                if len(body) + len(chunk) > self._max_bytes:
                    raise InvalidRepositorySourceData(
                        "GitHub response exceeds byte limits."
                    )
                body.extend(chunk)
            return bytes(body)
