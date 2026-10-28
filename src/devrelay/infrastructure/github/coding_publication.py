"""Bounded broker-only GitHub writes; recovered observations never authorize POST."""

import asyncio
import base64
import copy
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field, fields, is_dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from math import isfinite
from time import monotonic as _system_monotonic
from typing import cast
from urllib.parse import quote, urlencode
from uuid import UUID

import httpx

from devrelay.application.contracts.candidates import parse_candidate_commit
from devrelay.application.contracts.coding_publications import (
    ProviderPublication,
    PublicationObservation,
    PublicationPlan,
    PublicationPolicy,
    PublicationWrite,
    PublicationWriteKind,
)
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultRejected,
    CodingResultUnavailable,
)
from devrelay.application.contracts.git_objects import GitObject
from devrelay.domain.jobs.values import PullRequestReference
from devrelay.infrastructure.github.repository_source import RepositoryBinding

_API = httpx.URL("https://api.github.com/")
_INVALID = "GitHub publication failed bounded validation."
_UNAVAILABLE = "GitHub publication is unavailable or unresolved."
_ERRORS = (
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    UnicodeError,
    RecursionError,
    OverflowError,
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("ascii")


def _shape(body: bytes) -> None:
    index = depth = containers = commas = 0
    while index < len(body):
        byte = body[index]
        if byte == 34:
            end = index + 1
            while True:
                end = body.find(b'"', end)
                if end < 0:
                    raise ValueError(_INVALID)
                cursor = end - 1
                while cursor > index and body[cursor] == 92:
                    cursor -= 1
                if (end - cursor - 1) % 2 == 0:
                    break
                end += 1
            index = end
        elif byte in (123, 91):
            depth += 1
            containers += 1
            if depth > 16 or containers > 16384:
                raise ValueError(_INVALID)
        elif byte in (125, 93):
            depth -= 1
            if depth < 0:
                raise ValueError(_INVALID)
        elif byte == 44:
            commas += 1
            if commas > 65536:
                raise ValueError(_INVALID)
        index += 1
    if depth:
        raise ValueError(_INVALID)


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError(_INVALID)
        key.encode("utf-8")
        if isinstance(value, str):
            value.encode("utf-8")
        result[key] = value
    return result


def _reject(value: str) -> object:
    raise ValueError(_INVALID)


def _integer(value: object, *, minimum: int = 0, maximum: int = 2**63 - 1) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(_INVALID)
    return value


def _parse_int(value: str) -> int:
    if len(value) > 20:
        raise ValueError(_INVALID)
    return int(value)


def _json(body: bytes) -> object:
    _shape(body)
    value = json.loads(
        body.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_int=_parse_int,
        parse_float=_reject,
        parse_constant=_reject,
    )
    pending = [value]
    while pending:
        item = pending.pop()
        if type(item) is str:
            item.encode("utf-8")
        elif type(item) is dict:
            pending.extend(item.values())
        elif type(item) is list:
            pending.extend(item)
    return value


def _object(value: object, *, keys: set[str] | None = None) -> dict[str, object]:
    if type(value) is not dict or (keys is not None and set(value) != keys):
        raise ValueError(_INVALID)
    return cast(dict[str, object], value)


def _text(value: object, maximum: int = 65536) -> str:
    if type(value) is not str or len(value.encode("utf-8")) > maximum:
        raise ValueError(_INVALID)
    return value


def _sha(value: object) -> str:
    value = _text(value, 40)
    if re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise ValueError(_INVALID)
    return value


def _date(value: object) -> int:
    value = _text(value, 30)
    parsed = datetime.fromisoformat(value)
    if (
        parsed.utcoffset() != timedelta(0)
        or parsed.microsecond
        or parsed.isoformat().replace("+00:00", "Z") != value
    ):
        raise ValueError(_INVALID)
    return int(parsed.timestamp())


def _tree_raw(value: object, *, request: bool) -> bytes:
    if type(value) is not list or len(value) > 4096:
        raise ValueError(_INVALID)
    entries: list[tuple[bytes, bytes]] = []
    names: set[str] = set()
    for raw in value:
        item = _object(raw, keys={"path", "mode", "type", "sha"} if request else None)
        name, mode = _text(item["path"], 1024), _text(item["mode"], 6)
        if (
            not name
            or name in (".", "..")
            or "/" in name
            or "\x00" in name
            or name in names
        ):
            raise ValueError(_INVALID)
        names.add(name)
        if (mode, item["type"]) not in (
            ("040000", "tree"),
            ("100644", "blob"),
            ("100755", "blob"),
        ):
            raise ValueError(_INVALID)
        sha = _sha(item["sha"])
        encoded = name.encode("utf-8")
        entries.append(
            (
                encoded + (b"/" if mode == "040000" else b""),
                mode.lstrip("0").encode("ascii")
                + b" "
                + encoded
                + b"\0"
                + bytes.fromhex(sha),
            )
        )
    entries.sort()
    return b"".join(raw for _, raw in entries)


def _commit_raw(
    value: dict[str, object], plan: PublicationPlan, *, request: bool
) -> bytes:
    if request:
        if set(value) != {"tree", "parents", "message", "author", "committer"}:
            raise ValueError(_INVALID)
        tree = _sha(value["tree"])
        if value["parents"] != [plan.base_revision]:
            raise ValueError(_INVALID)
    else:
        tree = _sha(_object(value["tree"])["sha"])
        parents = value["parents"]
        if (
            type(parents) is not list
            or len(parents) != 1
            or _object(parents[0])["sha"] != plan.base_revision
        ):
            raise ValueError(_INVALID)
        verification = _object(value["verification"])
        if (
            verification.get("verified") is not False
            or verification.get("reason") != "unsigned"
            or verification["signature"] is not None
            or verification["payload"] is not None
        ):
            raise ValueError(_INVALID)
    lines = [
        b"tree " + tree.encode("ascii"),
        b"parent " + plan.base_revision.encode("ascii"),
    ]
    for name in ("author", "committer"):
        identity = _object(
            value[name], keys={"name", "email", "date"} if request else None
        )
        lines.append(
            name.encode("ascii")
            + b" "
            + _text(identity["name"], 256).encode("utf-8")
            + b" <"
            + _text(identity["email"], 254).encode("utf-8")
            + b"> "
            + str(_date(identity["date"])).encode("ascii")
            + b" +0000"
        )
    raw = b"\n".join(lines) + b"\n\n" + _text(value["message"], 16384).encode("utf-8")
    parse_candidate_commit(raw, parent=plan.base_revision, tree=tree)
    return raw


def _snapshot(
    plan: PublicationPlan, write: PublicationWrite | None
) -> tuple[PublicationPlan, PublicationWrite | None]:
    """Copy the bounded frozen graph before yielding; hashes run on the executor."""
    pending: list[tuple[object, int]] = [(plan, 0), (write, 0)]
    count = byte_count = 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if depth > 12 or count > 32768:
            raise CodingResultRejected(_INVALID)
        if type(item) is bytes:
            byte_count += len(item)
        elif type(item) is str:
            byte_count += 4 * len(item)
        elif item is None or type(item) in (int, float, UUID) or isinstance(item, Enum):
            pass
        elif type(item) is tuple and len(item) <= 8192:
            pending.extend((child, depth + 1) for child in item)
        elif is_dataclass(item) and type(item) in (
            PublicationPlan,
            PublicationWrite,
            PublicationPolicy,
            GitObject,
        ):
            pending.extend(
                (getattr(item, value.name), depth + 1) for value in fields(item)
            )
        else:
            raise CodingResultRejected(_INVALID)
        if byte_count > 64 * 1024**2:
            raise CodingResultRejected(_INVALID)
    return copy.deepcopy((plan, write))


@dataclass(frozen=True, slots=True, repr=False)
class _Plan:
    value: PublicationPlan
    fingerprint: str
    objects: dict[str, GitObject]


def _prepare(
    plan: PublicationPlan,
    write: PublicationWrite | None,
    policy: PublicationPolicy,
    bindings: tuple[RepositoryBinding, ...],
) -> _Plan:
    try:
        plan.__post_init__()
        if plan.policy != policy:
            raise ValueError(_INVALID)
        binding = next(
            item for item in bindings if item.repository_id == plan.repository_id
        )
        if (binding.github_id, binding.slug, binding.target_branch) != (
            plan.provider_repository_id,
            plan.repository_slug,
            plan.target_branch,
        ) or not plan.source_branch.startswith(
            binding.coding_branch_prefix.rstrip("/") + "/"
        ):
            raise ValueError(_INVALID)
        objects = {item.sha: item for item in plan.objects}
        if (
            len(objects) != len(plan.objects)
            or objects.get(plan.head_revision, GitObject(kind="blob", data=b"")).kind
            != "commit"
        ):
            raise ValueError(_INVALID)
        seen: set[str] = set()
        base = "repos/" + plan.repository_slug + "/"
        for command in plan.writes:
            body = _object(_json(command.body))
            if _canonical(body) != command.body:
                raise ValueError(_INVALID)
            kind = command.kind
            path = base + (
                "pulls"
                if kind is PublicationWriteKind.PULL_REQUEST
                else "git/"
                + ("refs" if kind is PublicationWriteKind.BRANCH else kind.value + "s")
            )
            if command.path != path:
                raise ValueError(_INVALID)
            if kind in (
                PublicationWriteKind.BLOB,
                PublicationWriteKind.TREE,
                PublicationWriteKind.COMMIT,
            ):
                item = objects[cast(str, command.expected_sha)]
                if item.kind != kind.value or item.sha in seen:
                    raise ValueError(_INVALID)
                if kind is PublicationWriteKind.BLOB:
                    if (
                        set(body) != {"content", "encoding"}
                        or body["encoding"] != "base64"
                    ):
                        raise ValueError(_INVALID)
                    encoded = _text(body["content"], 3 * 1024**2)
                    raw = base64.b64decode(encoded, validate=True)
                    if base64.b64encode(raw).decode("ascii") != encoded:
                        raise ValueError(_INVALID)
                elif kind is PublicationWriteKind.TREE:
                    if set(body) != {"tree"}:
                        raise ValueError(_INVALID)
                    raw = _tree_raw(body["tree"], request=True)
                    for child in cast(list[dict[str, object]], body["tree"]):
                        if child["sha"] not in seen:
                            raise ValueError(_INVALID)
                else:
                    raw = _commit_raw(body, plan, request=True)
                    if (
                        command.expected_sha != plan.head_revision
                        or body["tree"] not in seen
                    ):
                        raise ValueError(_INVALID)
                if raw != item.data:
                    raise ValueError(_INVALID)
                seen.add(item.sha)
            elif kind is PublicationWriteKind.BRANCH:
                if (
                    body
                    != {
                        "ref": "refs/heads/" + plan.source_branch,
                        "sha": plan.head_revision,
                    }
                    or command.expected_sha != plan.head_revision
                ):
                    raise ValueError(_INVALID)
            else:
                if (
                    set(body)
                    != {
                        "title",
                        "body",
                        "head",
                        "base",
                        "draft",
                        "maintainer_can_modify",
                    }
                    or body["head"] != plan.source_branch
                    or body["base"] != plan.target_branch
                    or body["draft"] is not False
                    or body["maintainer_can_modify"] is not False
                ):
                    raise ValueError(_INVALID)
                if (
                    not _text(body["title"], 256).strip()
                    or len(_text(body["body"], 16384)) > 16384
                ):
                    raise ValueError(_INVALID)
        if seen != set(objects) or (
            write is not None and not any(write == item for item in plan.writes)
        ):
            raise ValueError(_INVALID)
        fingerprint = hashlib.sha256(
            _canonical(
                {
                    "result": str(plan.result_id),
                    "repository": str(plan.repository_id),
                    "provider": plan.provider_repository_id,
                    "slug": plan.repository_slug,
                    "source": plan.source_branch,
                    "target": plan.target_branch,
                    "head": plan.head_revision,
                    "base": plan.base_revision,
                    "writes": [
                        [str(item.write_id), item.input_digest] for item in plan.writes
                    ],
                }
            )
        ).hexdigest()
        return _Plan(plan, fingerprint, objects)
    except (*_ERRORS, StopIteration):
        raise CodingResultRejected(_INVALID) from None


def _repo(value: object, plan: PublicationPlan) -> None:
    data = _object(value)
    if (
        _integer(data["id"], minimum=1) != plan.provider_repository_id
        or _text(data["full_name"], 256).casefold() != plan.repository_slug.casefold()
        or data.get("archived") is not False
        or data.get("disabled") is not False
        or data.get("fork") is not False
    ):
        raise ValueError(_INVALID)


def _ref(value: object, plan: PublicationPlan) -> None:
    data = _object(value)
    target = _object(data["object"])
    if (
        data["ref"] != "refs/heads/" + plan.source_branch
        or target["type"] != "commit"
        or _sha(target["sha"]) != plan.head_revision
    ):
        raise ValueError(_INVALID)


def _pr(value: object, plan: PublicationPlan, number: int | None = None) -> int:
    data = _object(value)
    observed = _integer(data["number"], minimum=1)
    if number is not None and observed != number:
        raise ValueError(_INVALID)
    if (
        data["state"] != "open"
        or data["draft"] is not False
        or data.get("merged", False) is not False
        or (number is not None and data.get("merged") is not False)
        or data.get("merged_at") is not None
    ):
        raise ValueError(_INVALID)
    for label, branch in (("head", plan.source_branch), ("base", plan.target_branch)):
        association = _object(data[label])
        repo = _object(association["repo"])
        if (
            association["ref"] != branch
            or _integer(repo["id"], minimum=1) != plan.provider_repository_id
            or _text(repo["full_name"], 256).casefold()
            != plan.repository_slug.casefold()
            or repo.get("fork") is not False
        ):
            raise ValueError(_INVALID)
        if label == "head" and _sha(association["sha"]) != plan.head_revision:
            raise ValueError(_INVALID)
        _sha(association["sha"])
    return observed


def _object_response(data: object, captured: _Plan, write: PublicationWrite) -> None:
    plan = captured.value
    value = _object(data)
    if _sha(value["sha"]) != write.expected_sha:
        raise ValueError(_INVALID)
    item = captured.objects[write.expected_sha]
    if write.kind is PublicationWriteKind.BLOB:
        if value["encoding"] != "base64":
            raise ValueError(_INVALID)
        encoded = _text(value["content"], 3 * 1024**2)
        # GitHub wraps base64 at line boundaries; no other whitespace is accepted.
        compact = encoded.replace("\n", "")
        raw = base64.b64decode(compact, validate=True)
        if base64.b64encode(raw).decode("ascii") != compact or _integer(
            value["size"], maximum=2 * 1024**2
        ) != len(raw):
            raise ValueError(_INVALID)
    elif write.kind is PublicationWriteKind.TREE:
        if value["truncated"] is not False:
            raise ValueError(_INVALID)
        raw = _tree_raw(value["tree"], request=False)
    else:
        raw = _commit_raw(value, plan, request=False)
    if (
        raw != item.data
        or GitObject(kind=item.kind, data=raw).sha != write.expected_sha
    ):
        raise ValueError(_INVALID)


def _observation(
    write: PublicationWrite, number: int | None = None
) -> PublicationObservation:
    digest = hashlib.sha256(
        _canonical(
            {
                "write_id": str(write.write_id),
                "input_digest": write.input_digest,
                "sha": write.expected_sha,
                "pull_request_number": number,
            }
        )
    ).hexdigest()
    return PublicationObservation(
        write_id=write.write_id,
        input_digest=write.input_digest,
        observation_digest=digest,
        pull_request_number=number,
    )


@dataclass(slots=True, repr=False)
class _Budget:
    fingerprint: str | None
    started: float
    requests: int = 0
    sent: int = 0
    received: int = 0
    attempted: set[UUID] = field(default_factory=set)


def _consume[T](future: asyncio.Future[T]) -> None:
    if not future.cancelled():
        future.exception()


class HttpGitHubCodingPublicationProvider:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        bindings: tuple[RepositoryBinding, ...],
        policy: PublicationPolicy,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = _system_monotonic,
    ) -> None:
        policy.__post_init__()
        self._client, self._bindings, self._policy, self._now = (
            client,
            tuple(replace(item) for item in bindings),
            replace(policy),
            now,
        )
        self._headers = tuple(client.headers.multi_items())
        self._monotonic = monotonic
        self._last_tick: float | None = None
        self._check_client()
        if (
            not bindings
            or len(bindings) > 32
            or len({item.repository_id for item in bindings}) != len(bindings)
        ):
            raise ValueError(_INVALID)
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="devrelay-publication"
        )
        self._pending: Future[object] | None = None
        self._busy = self._closed = False
        self._idle = asyncio.Event()
        self._idle.set()
        self._budgets: dict[UUID, _Budget] = {}

    def _check_client(self) -> None:
        client = self._client
        authorization = client.headers.get_list("authorization")
        if (
            client.base_url != _API
            or client.auth is not None
            or client.params
            or client.cookies
            or client.trust_env
            or client.follow_redirects
            or tuple(client.headers.multi_items()) != self._headers
            or len(authorization) != 1
            or re.fullmatch(r"Bearer [!-~]{1,512}", authorization[0]) is None
            or client.headers.get_list("x-github-api-version") != ["2026-03-10"]
        ):
            raise CodingResultRejected(_INVALID)

    async def _cpu[T](self, operation: Callable[[], T]) -> T:
        if self._pending is not None and not self._pending.done():
            raise CodingResultUnavailable(_UNAVAILABLE)
        pending = self._executor.submit(operation)
        self._pending = cast(Future[object], pending)
        wrapped = asyncio.wrap_future(pending)
        wrapped.add_done_callback(_consume)
        return await asyncio.shield(wrapped)

    def _remaining(self, budget: _Budget) -> float:
        remaining = self._policy.total_timeout_seconds - (self._tick() - budget.started)
        if remaining <= self._policy.dispatch_margin_seconds:
            raise CodingResultUnavailable(_UNAVAILABLE)
        return remaining

    def _tick(self) -> float:
        try:
            value = self._monotonic()
            if (
                type(value) not in (int, float)
                or not isfinite(value)
                or (self._last_tick is not None and value < self._last_tick)
            ):
                raise ValueError
            self._last_tick = float(value)
            return float(value)
        except Exception:
            raise CodingResultUnavailable(_UNAVAILABLE) from None

    async def _run[T](
        self,
        plan: PublicationPlan,
        write: PublicationWrite | None,
        operation: Callable[[_Plan, _Budget, PublicationWrite | None], Awaitable[T]],
    ) -> T:
        if (
            self._closed
            or self._busy
            or (self._pending is not None and not self._pending.done())
        ):
            raise CodingResultUnavailable(_UNAVAILABLE)
        owned, command = _snapshot(plan, write)
        if type(owned.result_id) is not UUID:
            raise CodingResultRejected(_INVALID)
        budget = self._budgets.get(owned.result_id)
        if budget is None:
            if len(self._budgets) >= 32:
                raise CodingResultUnavailable(_UNAVAILABLE)
            budget = self._budgets[owned.result_id] = _Budget(None, self._tick())
        self._remaining(budget)
        self._busy = True
        self._idle.clear()
        try:
            async with asyncio.timeout(self._policy.total_timeout_seconds):
                captured = await self._cpu(
                    lambda: _prepare(owned, command, self._policy, self._bindings)
                )
                if budget.fingerprint is None:
                    budget.fingerprint = captured.fingerprint
                if budget.fingerprint != captured.fingerprint:
                    raise CodingResultRejected(_INVALID)
                async with asyncio.timeout(self._remaining(budget)):
                    return await operation(captured, budget, command)
        except TimeoutError, httpx.RequestError:
            raise CodingResultUnavailable(_UNAVAILABLE) from None
        except _ERRORS:
            raise CodingResultRejected(_INVALID) from None
        finally:
            self._busy = False
            self._idle.set()
            if self._pending is not None and self._pending.done():
                self._pending = None

    async def _request(
        self,
        budget: _Budget,
        method: str,
        path: str,
        *,
        body: bytes = b"",
        missing: bool = False,
        before_write: Callable[[], None] | None = None,
    ) -> object | None:
        self._check_client()
        remaining = self._remaining(budget)
        if (
            method not in ("GET", "POST")
            or (method == "POST" and self._closed)
            or budget.requests >= self._policy.max_requests
            or budget.sent + len(body) > self._policy.max_request_bytes
        ):
            raise CodingResultUnavailable(_UNAVAILABLE)
        target = _API.join(path)
        if (
            target.scheme != "https"
            or target.host != "api.github.com"
            or target.port not in (None, 443)
        ):
            raise CodingResultRejected(_INVALID)
        budget.requests += 1
        budget.sent += len(body)
        timeout = httpx.Timeout(min(remaining, self._policy.request_timeout_seconds))
        headers = {
            "Accept": "application/vnd.github+json",
            "Accept-Encoding": "identity",
            "Content-Type": "application/json",
        }
        if before_write is not None:
            before_write()
        async with self._client.stream(
            method,
            target,
            content=body,
            headers=headers,
            timeout=timeout,
            follow_redirects=False,
        ) as response:
            self._check_client()
            self._remaining(budget)
            if response.request.url != target:
                raise CodingResultRejected(_INVALID)
            if missing and response.status_code == 404:
                return None
            if response.status_code != (201 if method == "POST" else 200):
                raise CodingResultUnavailable(_UNAVAILABLE)
            if (
                len(response.headers.get_list("content-type")) != 1
                or response.headers.get("content-type", "")
                .split(";", 1)[0]
                .strip()
                .lower()
                not in ("application/json", "application/vnd.github+json")
                or len(response.headers.get_list("content-encoding")) > 1
                or response.headers.get("content-encoding", "identity") != "identity"
            ):
                raise CodingResultRejected(_INVALID)
            lengths = response.headers.get_list("content-length")
            if len(lengths) > 1 or (
                lengths
                and (
                    not lengths[0].isascii()
                    or not lengths[0].isdecimal()
                    or len(lengths[0]) > 10
                    or int(lengths[0]) > self._policy.max_response_bytes
                )
            ):
                raise CodingResultRejected(_INVALID)
            content = bytearray()

            def retain(chunk: bytes) -> None:
                budget.received += len(chunk)
                if (
                    len(content) + len(chunk) > self._policy.max_response_bytes
                    or budget.received > self._policy.max_total_response_bytes
                ):
                    raise CodingResultUnavailable(_UNAVAILABLE)
                content.extend(chunk)

            if response.is_stream_consumed:
                retain(response.content)
            else:
                async for chunk in response.aiter_raw():
                    self._check_client()
                    self._remaining(budget)
                    retain(chunk)
            if (
                len(content) > self._policy.max_response_bytes
                or budget.received > self._policy.max_total_response_bytes
                or (lengths and len(content) != int(lengths[0]))
            ):
                raise CodingResultRejected(_INVALID)
            result = await self._cpu(lambda: _json(bytes(content)))
            self._remaining(budget)
            return result

    async def _repository(self, plan: PublicationPlan, budget: _Budget) -> None:
        response = await self._request(budget, "GET", "repos/" + plan.repository_slug)
        await self._cpu(lambda: _repo(response, plan))

    @staticmethod
    def _read_path(plan: PublicationPlan, write: PublicationWrite) -> str:
        prefix = "repos/" + plan.repository_slug + "/"
        if write.kind is PublicationWriteKind.BRANCH:
            return prefix + "git/ref/heads/" + quote(plan.source_branch, safe="")
        return write.path + "/" + cast(str, write.expected_sha)

    async def _pull_requests(
        self, plan: PublicationPlan, budget: _Budget
    ) -> list[object]:
        query = urlencode(
            {
                "state": "open",
                "head": plan.repository_slug.split("/")[0] + ":" + plan.source_branch,
                "base": plan.target_branch,
                "per_page": "100",
                "page": "1",
            }
        )
        result = await self._request(
            budget, "GET", "repos/" + plan.repository_slug + "/pulls?" + query
        )
        if type(result) is not list or len(result) >= 100:
            raise CodingResultRejected(_INVALID)
        # A full page may hide an additional match; never infer uniqueness from it.
        return result

    async def _observe(
        self, captured: _Plan, budget: _Budget, write: PublicationWrite
    ) -> PublicationObservation | None:
        plan = captured.value
        if write.kind is PublicationWriteKind.PULL_REQUEST:
            values = await self._pull_requests(plan, budget)
            if not values:
                return None
            if len(values) != 1:
                raise CodingResultRejected(_INVALID)
            number = await self._cpu(lambda: _pr(values[0], plan))
            detail = await self._request(
                budget, "GET", f"repos/{plan.repository_slug}/pulls/{number}"
            )
            await self._cpu(lambda: _pr(detail, plan, number))
            return _observation(write, number)
        value = await self._request(
            budget, "GET", self._read_path(plan, write), missing=True
        )
        if value is None:
            return None
        if write.kind is PublicationWriteKind.BRANCH:
            await self._cpu(lambda: _ref(value, plan))
        else:
            await self._cpu(lambda: _object_response(value, captured, write))
        return _observation(write)

    async def write(
        self,
        plan: PublicationPlan,
        write: PublicationWrite,
        *,
        before_write: Callable[[], None],
    ) -> PublicationObservation:
        async def operation(
            captured: _Plan, budget: _Budget, command: PublicationWrite | None
        ) -> PublicationObservation:
            assert command is not None
            if command.write_id in budget.attempted:
                raise CodingResultRejected(_INVALID)
            await self._repository(captured.value, budget)
            if command.kind is PublicationWriteKind.BRANCH:
                existing = await self._request(
                    budget,
                    "GET",
                    self._read_path(captured.value, command),
                    missing=True,
                )
                if existing is not None:
                    raise CodingResultRejected(_INVALID)
            elif command.kind is PublicationWriteKind.PULL_REQUEST:
                if await self._pull_requests(captured.value, budget):
                    raise CodingResultRejected(_INVALID)
            budget.attempted.add(command.write_id)
            returned = await self._request(
                budget,
                "POST",
                command.path,
                body=command.body,
                before_write=before_write,
            )
            observation: PublicationObservation | None
            if command.kind is PublicationWriteKind.PULL_REQUEST:
                number = await self._cpu(lambda: _pr(returned, captured.value))
                detail = await self._request(
                    budget,
                    "GET",
                    f"repos/{captured.value.repository_slug}/pulls/{number}",
                )
                await self._cpu(lambda: _pr(detail, captured.value, number))
                observation = _observation(command, number)
            elif command.kind is PublicationWriteKind.BRANCH:
                await self._cpu(lambda: _ref(returned, captured.value))
                observation = await self._observe(captured, budget, command)
            else:
                await self._cpu(lambda: self._returned_sha(returned, command))
                observation = await self._observe(captured, budget, command)
            if observation is None:
                raise CodingResultUnavailable(_UNAVAILABLE)
            await self._repository(captured.value, budget)
            return observation

        return await self._run(plan, write, operation)

    @staticmethod
    def _returned_sha(value: object, write: PublicationWrite) -> None:
        if _sha(_object(value)["sha"]) != write.expected_sha:
            raise ValueError(_INVALID)

    async def observe(
        self, plan: PublicationPlan, write: PublicationWrite
    ) -> PublicationObservation | None:
        async def operation(
            captured: _Plan, budget: _Budget, command: PublicationWrite | None
        ) -> PublicationObservation | None:
            assert command is not None
            await self._repository(captured.value, budget)
            observation = await self._observe(captured, budget, command)
            await self._repository(captured.value, budget)
            return observation

        return await self._run(plan, write, operation)

    async def verify(
        self, plan: PublicationPlan, *, pull_request_number: int
    ) -> ProviderPublication:
        if type(pull_request_number) is not int or not 1 <= pull_request_number < 2**63:
            raise CodingResultRejected(_INVALID)

        async def operation(
            captured: _Plan, budget: _Budget, command: PublicationWrite | None
        ) -> ProviderPublication:
            value = captured.value
            await self._repository(value, budget)
            branch = value.writes[-2]
            found = await self._request(budget, "GET", self._read_path(value, branch))
            await self._cpu(lambda: _ref(found, value))
            detail = await self._request(
                budget,
                "GET",
                f"repos/{value.repository_slug}/pulls/{pull_request_number}",
            )
            await self._cpu(lambda: _pr(detail, value, pull_request_number))
            await self._repository(value, budget)
            observed_at = self._now()
            if not isinstance(
                observed_at, datetime
            ) or observed_at.utcoffset() != timedelta(0):
                raise CodingResultRejected(_INVALID)
            return ProviderPublication(
                reference=PullRequestReference(
                    repository_id=value.repository_id,
                    number=pull_request_number,
                    source_branch=value.source_branch,
                    target_branch=value.target_branch,
                    head_revision=value.head_revision,
                ),
                provider_repository_id=value.provider_repository_id,
                observed_at=observed_at,
            )

        return await self._run(plan, None, operation)

    async def aclose(self) -> None:
        self._closed = True
        await self._idle.wait()
        if self._pending is not None:
            wrapped = asyncio.wrap_future(self._pending)
            wrapped.add_done_callback(_consume)
            try:
                await asyncio.shield(wrapped)
            except Exception:
                pass
            self._pending = None
        await asyncio.to_thread(
            self._executor.shutdown, wait=True, cancel_futures=False
        )
