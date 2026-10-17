import asyncio
import base64
import hashlib
import os
import stat
import subprocess
from collections.abc import Iterator
from contextlib import nullcontext
from dataclasses import dataclass, replace
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any

import pytest

import devrelay.infrastructure.github.git_commit_source as git_source
from devrelay.application.contracts.base_sources import BaseSourceRequest
from devrelay.application.ports.base_source import (
    BaseSourceAccessDenied,
    BaseSourceUnavailable,
    InvalidBaseSourceData,
)
from devrelay.infrastructure.github.git_commit_source import (
    BrokerGitPolicy,
    GitHubGitCommitSource,
)
from devrelay.infrastructure.github.repository_source import RepositoryBinding
from tests.workspace_fixtures import workspace_binding

TOKEN = "fixture_only_read_token_" + "x" * 40
TREE = "a" * 40
TRACE = b"packet:          git< version 2\npacket:          git< fetch=shallow wait-for-done filter\n"


def raw_commit(
    *, signed: bool = False, extra: bool = False, parent: bool = True, tree: str = TREE
) -> bytes:
    return (
        b"tree "
        + tree.encode()
        + b"\n"
        + (b"parent " + b"b" * 40 + b"\n" if parent else b"")
        + b"author Fixture <fixture@example.invalid> 1700000000 +0330\n"
        + b"committer Fixture <fixture@example.invalid> 1700000001 -0500\n"
        + (
            b"gpgsig -----BEGIN PGP SIGNATURE-----\n fixture opaque signature\n -----END PGP SIGNATURE-----\n"
            if signed
            else b""
        )
        + (
            b"encoding ISO-8859-1\nmergetag object "
            + b"c" * 40
            + b"\n type commit\n tag fixture\n \n signed tag content\n"
            if extra
            else b""
        )
        + b"\nOriginal fixture message\n"
    )


def commit_sha(raw: bytes) -> str:
    return hashlib.sha1(
        b"commit " + str(len(raw)).encode() + b"\0" + raw, usedforsecurity=False
    ).hexdigest()


def policy(tmp_path: Path) -> BrokerGitPolicy:
    executable = tmp_path / "git"
    executable.write_bytes(b"fixture executable bytes")
    executable.chmod(0o700)
    helpers = tmp_path / "helpers"
    helpers.mkdir()
    (helpers / "git-remote-https").write_bytes(b"fixture helper bytes")
    (helpers / "git-remote-https").chmod(0o700)
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    return BrokerGitPolicy(
        executable=executable,
        executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
        version="git version 2.45.1",
        exec_directory=helpers,
        https_helper_sha256=hashlib.sha256(
            (helpers / "git-remote-https").read_bytes()
        ).hexdigest(),
        private_directory=private,
    )


class Commands:
    def __init__(self, raw: bytes, pins: BrokerGitPolicy) -> None:
        self.raw, self.policy = raw, pins
        self.root: Path | None = None
        self.cancelled = Event()
        self.settled = True
        self.calls: list[tuple[tuple[str, ...], dict[str, str]]] = []
        self.damage: str | None = None
        self.entered, self.release = Event(), Event()
        self.block = False
        self.on_disk: list[bytes] = []
        self.inventory: bytes | None = None
        self.inventory_stderr = b""
        self.empty_tree_output = git_source._Output(0, b"", b"")

    def run(
        self,
        arguments: tuple[str, ...],
        environment: dict[str, str],
        *,
        maximum_stdout: int = 65536,
    ) -> git_source._Output:
        assert self.root is not None
        self.calls.append((arguments, environment))
        if self.cancelled.is_set():
            raise ValueError("Fixture cancellation.")
        sha = commit_sha(self.raw)
        if arguments == ("--version",):
            version = (
                "git version other" if self.damage == "version" else self.policy.version
            )
            return git_source._Output(0, (version + "\n").encode(), b"")
        if arguments[0] == "init":
            repo = self.root / "repo"
            (repo / "objects/info").mkdir(parents=True)
            (repo / "config").write_bytes(b"[core]\n bare = true\n")
            return git_source._Output(0, b"", b"")
        if arguments[0] == "ls-remote":
            self.entered.set()
            if self.block:
                assert self.release.wait(5)
            trace = TRACE
            if self.damage == "probe_filter":
                trace = TRACE.replace(b" filter", b"")
            elif self.damage == "probe_v2":
                trace = TRACE.replace(b"version 2", b"version 1")
            return git_source._Output(0, b"", trace)
        if "fetch" in arguments:
            if self.damage == "status":
                return git_source._Output(128, b"", TOKEN.encode())
            repo = self.root / "repo"
            if b"\nparent " in self.raw and self.damage != "shallow_missing":
                (repo / "shallow").write_bytes(
                    ("f" * 40 if self.damage == "shallow_wrong" else sha).encode()
                    + b"\n"
                )
            if self.damage == "alternates":
                (repo / "objects/info/alternates").write_bytes(b"/must-not-read\n")
            if self.damage == "disk":
                (repo / "excess").write_bytes(
                    b"x" * (self.policy.maximum_disk_bytes + 1)
                )
            trace = (
                TRACE
                + b"packet:          git> filter tree:0\npacket:          git> deepen 1\npacket:          git> want "
                + sha.encode()
                + b"\n"
            )
            if self.damage == "fetch_filter":
                trace = trace.replace(b" filter\n", b"\n")
            elif self.damage == "filter_request":
                trace = trace.replace(b"> filter tree:0", b"> filter blob:none")
            elif self.damage == "deepen_request":
                trace = trace.replace(b"> deepen 1", b"> deepen 2")
            elif self.damage == "want_request":
                trace = trace.replace(b"> want " + sha.encode(), b"> want " + b"f" * 40)
            elif self.damage == "warning":
                trace += b"warning: filtering not recognized by server, ignoring\n"
            return git_source._Output(0, b"", trace)
        if "-s" in arguments:
            value = (
                b"999999\n"
                if self.damage == "size"
                else str(len(self.raw)).encode() + b"\n"
            )
            return git_source._Output(0, value, b"")
        if "commit" in arguments:
            value = self.raw + b"changed" if self.damage == "raw" else self.raw
            return git_source._Output(0, value, b"")
        if "tree" in arguments:
            assert arguments[-1] == git_source._EMPTY_TREE_SHA
            assert maximum_stdout == 1
            return self.empty_tree_output
        if "--batch-all-objects" in arguments:
            value = f"{sha} commit {len(self.raw)}\n".encode()
            if self.damage == "extra_objects":
                value += b"f" * 40 + b" blob 999\n"
            elif self.damage == "extra_empty_tree":
                value += b"4b825dc642cb6eb9a060e54bf8d69288fbee4904 tree 0\n"
            if self.damage == "settlement":
                self.settled = False
            self.on_disk = [
                item.read_bytes() for item in self.root.rglob("*") if item.is_file()
            ]
            return git_source._Output(
                0,
                self.inventory if self.inventory is not None else value,
                self.inventory_stderr,
            )
        raise AssertionError("Unexpected fixture command.")


@dataclass
class Context:
    source: GitHubGitCommitSource
    commands: Commands
    request: BaseSourceRequest
    policy: BrokerGitPolicy
    raw: bytes


def context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, raw: bytes | None = None
) -> Context:
    pins = policy(tmp_path)
    content = raw or raw_commit()
    definition = workspace_binding().definition
    request = BaseSourceRequest(
        repository_id=definition.repository_id,
        provider_repository_id=definition.provider_repository_id,
        repository_slug=definition.repository_slug,
        base_revision=commit_sha(content),
    )
    binding = RepositoryBinding(
        repository_id=request.repository_id,
        github_id=request.provider_repository_id,
        slug=request.repository_slug,
        target_branch="master",
        coding_branch_prefix="agent/",
    )
    commands = Commands(content, pins)

    def native(selected: BrokerGitPolicy, root: Path, cancelled: Event) -> Commands:
        assert selected is pins
        commands.root, commands.cancelled = root, cancelled
        return commands

    monkeypatch.setattr(git_source, "_LinuxCommands", native)
    monkeypatch.setattr(git_source, "_private_lock", lambda root: nullcontext())
    source = GitHubGitCommitSource(policy=pins, bindings=(binding,), token=TOKEN)
    return Context(source, commands, request, pins, content)


@pytest.mark.parametrize(
    "signed,extra,parent",
    [
        (False, False, True),
        (True, False, True),
        (False, True, True),
        (False, False, False),
    ],
)
def test_fixed_source_reads_original_unsigned_signed_and_extra_header_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    signed: bool,
    extra: bool,
    parent: bool,
) -> None:
    setup = context(
        tmp_path, monkeypatch, raw=raw_commit(signed=signed, extra=extra, parent=parent)
    )

    result = asyncio.run(setup.source.read(setup.request, root_tree_sha=TREE))

    assert result == setup.raw
    assert list(setup.policy.private_directory.iterdir()) == []
    commands = [args for args, _ in setup.commands.calls]
    fetched = next(args for args in commands if "fetch" in args)
    assert fetched[-2:] == (
        "https://github.com/" + setup.request.repository_slug + ".git",
        setup.request.base_revision,
    )
    assert {
        "--depth=1",
        "--filter=tree:0",
        "--no-tags",
        "--no-write-fetch-head",
        "--no-recurse-submodules",
        "--no-auto-maintenance",
    } <= set(fetched)
    assert not any(
        "checkout" in args or "clone" in args or "push" in args for args in commands
    )


@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("commit_first", [False, True])
@pytest.mark.parametrize("notice", [b"", git_source._PROMISOR_NOTICE])
def test_empty_root_accepts_only_commit_or_exact_sorted_commit_and_empty_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    present: bool,
    commit_first: bool,
    notice: bytes,
) -> None:
    empty_sha = git_source._EMPTY_TREE_SHA
    raw = raw_commit(tree=empty_sha)
    # Exercise either side of the fixed tree hash in Git's documented hash order.
    for nonce in range(1000):
        candidate = raw + str(nonce).encode()
        if (commit_sha(candidate) < empty_sha) == commit_first:
            raw = candidate
            break
    else:
        raise AssertionError("No fixture commit on requested side of empty tree hash.")
    setup = context(tmp_path, monkeypatch, raw=raw)
    commit_row = f"{setup.request.base_revision} commit {len(raw)}\n".encode()
    tree_row = f"{empty_sha} tree 0\n".encode()
    setup.commands.inventory = (
        b"".join(sorted((commit_row, tree_row))) if present else commit_row
    )
    setup.commands.inventory_stderr = notice

    result = asyncio.run(setup.source.read(setup.request, root_tree_sha=empty_sha))

    tree_calls = [args for args, _ in setup.commands.calls if "tree" in args]
    assert result == raw
    assert list(setup.policy.private_directory.iterdir()) == []
    assert len(tree_calls) == int(present)


@pytest.mark.parametrize(
    "damage",
    [
        "missing_commit",
        "duplicate_tree",
        "duplicate_commit",
        "unsorted",
        "other_tree",
        "wrong_type",
        "wrong_size",
        "extra_blob",
        "extra_commit",
        "notice_changed",
        "tree_bytes",
        "tree_stderr",
        "tree_status",
    ],
)
def test_empty_root_rejects_unexpected_inventory_or_unverified_raw_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    empty_sha = git_source._EMPTY_TREE_SHA
    setup = context(tmp_path, monkeypatch, raw=raw_commit(tree=empty_sha))
    commit_row = f"{setup.request.base_revision} commit {len(setup.raw)}\n".encode()
    tree_row = f"{empty_sha} tree 0\n".encode()
    inventory = b"".join(sorted((commit_row, tree_row)))
    if damage == "missing_commit":
        inventory = tree_row
    elif damage == "duplicate_tree":
        inventory = b"".join(sorted((commit_row, tree_row, tree_row)))
    elif damage == "duplicate_commit":
        inventory = b"".join(sorted((commit_row, commit_row, tree_row)))
    elif damage == "unsorted":
        inventory = b"".join(reversed(sorted((commit_row, tree_row))))
    elif damage == "other_tree":
        inventory = inventory.replace(empty_sha.encode(), b"f" * 40)
    elif damage == "wrong_type":
        inventory = inventory.replace(b" tree 0\n", b" blob 0\n")
    elif damage == "wrong_size":
        inventory = inventory.replace(b" tree 0\n", b" tree 1\n")
    elif damage == "extra_blob":
        inventory += b"f" * 40 + b" blob 1\n"
    elif damage == "extra_commit":
        inventory += b"f" * 40 + b" commit 100\n"
    elif damage == "notice_changed":
        setup.commands.inventory_stderr = (
            git_source._PROMISOR_NOTICE + b"warning: extra\n"
        )
    elif damage == "tree_bytes":
        setup.commands.empty_tree_output = git_source._Output(0, b"x", b"")
    elif damage == "tree_stderr":
        setup.commands.empty_tree_output = git_source._Output(
            0, b"", b"warning: extra\n"
        )
    elif damage == "tree_status":
        setup.commands.empty_tree_output = git_source._Output(1, b"", b"")
    setup.commands.inventory = inventory

    with pytest.raises(BaseSourceUnavailable):
        asyncio.run(setup.source.read(setup.request, root_tree_sha=empty_sha))

    assert any(
        item.name.startswith("git-read-")
        for item in setup.policy.private_directory.iterdir()
    )
    if not damage.startswith("tree_"):
        assert not any("tree" in args for args, _ in setup.commands.calls)


def test_token_is_transient_scoped_env_only_with_no_ambient_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context(tmp_path, monkeypatch)
    monkeypatch.setenv("HTTP_PROXY", "http://untrusted.invalid")
    monkeypatch.setenv("GIT_CONFIG_VALUE_999", "must-not-inherit")
    monkeypatch.setenv("HOME", "must-not-inherit")

    asyncio.run(setup.source.read(setup.request, root_tree_sha=TREE))

    encoded = base64.b64encode(("x-access-token:" + TOKEN).encode()).decode()
    for args, environment in setup.commands.calls:
        assert TOKEN not in repr(args) and encoded not in repr(args)
        assert (
            "HTTP_PROXY" not in environment
            and "GIT_CONFIG_VALUE_999" not in environment
        )
        assert "must-not-inherit" not in repr(environment)
        values = {
            environment[f"GIT_CONFIG_KEY_{i}"]: environment[f"GIT_CONFIG_VALUE_{i}"]
            for i in range(int(environment["GIT_CONFIG_COUNT"]))
        }
        assert values["credential.helper"] == ""
        assert (
            values["http.followRedirects"] == "false"
            and values["http.sslVerify"] == "true"
        )
        assert values["http.proxy"] == "" and values["protocol.allow"] == "never"
        assert (
            environment["GIT_CONFIG_NOSYSTEM"] == "1"
            and environment["GIT_NO_LAZY_FETCH"] == "1"
        )
        secrets = [
            value for key, value in values.items() if key.endswith(".extraHeader")
        ]
        if "fetch" in args or args[0] == "ls-remote":
            assert secrets == ["Authorization: Basic " + encoded]
            assert values["protocol.https.allow"] == "always"
        else:
            assert secrets == [] and values["protocol.https.allow"] == "never"
    assert all(
        TOKEN.encode() not in item and encoded.encode() not in item
        for item in setup.commands.on_disk
    )


@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "probe_filter",
        "probe_v2",
        "status",
        "fetch_filter",
        "filter_request",
        "deepen_request",
        "want_request",
        "warning",
        "size",
        "raw",
        "extra_objects",
        "extra_empty_tree",
        "shallow_missing",
        "shallow_wrong",
        "alternates",
        "settlement",
        "disk",
    ],
)
def test_changed_capabilities_objects_limits_or_stop_preserve_partial_and_block_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    setup = context(tmp_path, monkeypatch)
    setup.commands.damage = damage

    with pytest.raises(BaseSourceUnavailable) as caught:
        asyncio.run(setup.source.read(setup.request, root_tree_sha=TREE))
    previous = len(setup.commands.calls)
    with pytest.raises(BaseSourceUnavailable):
        asyncio.run(setup.source.read(setup.request, root_tree_sha=TREE))

    assert TOKEN not in str(caught.value)
    assert len(setup.commands.calls) == previous
    assert any(
        item.name.startswith("git-read-")
        for item in setup.policy.private_directory.iterdir()
    )
    if damage in ("probe_filter", "probe_v2"):
        assert not any("fetch" in args for args, _ in setup.commands.calls)


@pytest.mark.parametrize(
    "field,value",
    [("provider_repository_id", 999), ("repository_slug", "other/repository")],
)
def test_wrong_repository_is_denied_before_any_process_or_private_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: Any,
) -> None:
    setup = context(tmp_path, monkeypatch)

    with pytest.raises(BaseSourceAccessDenied):
        asyncio.run(
            setup.source.read(
                replace(setup.request, **{field: value}), root_tree_sha=TREE
            )
        )

    assert (
        setup.commands.calls == []
        and list(setup.policy.private_directory.iterdir()) == []
    )


def test_wrong_tree_is_rejected_after_actual_raw_commit_hash_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context(tmp_path, monkeypatch)

    with pytest.raises(BaseSourceUnavailable):
        asyncio.run(setup.source.read(setup.request, root_tree_sha="f" * 40))

    assert setup.commands.calls


@pytest.mark.parametrize("tree", [None, True, "not-an-object", "A" * 40])
def test_invalid_tree_fails_before_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tree: Any
) -> None:
    setup = context(tmp_path, monkeypatch)

    with pytest.raises(InvalidBaseSourceData):
        asyncio.run(setup.source.read(setup.request, root_tree_sha=tree))

    assert setup.commands.calls == []


@pytest.mark.parametrize("component", ["git", "helper"])
def test_changed_administrator_executable_identity_is_not_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, component: str
) -> None:
    setup = context(tmp_path, monkeypatch)
    selected = (
        setup.policy.executable
        if component == "git"
        else setup.policy.exec_directory / "git-remote-https"
    )
    selected.write_bytes(b"replacement executable")

    with pytest.raises(BaseSourceUnavailable):
        asyncio.run(setup.source.read(setup.request, root_tree_sha=TREE))

    assert setup.commands.calls == []


def test_cancellation_keeps_actual_thread_admission_until_it_settles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup = context(tmp_path, monkeypatch)
    setup.commands.block = True

    async def scenario() -> None:
        pending = asyncio.create_task(
            setup.source.read(setup.request, root_tree_sha=TREE)
        )
        assert await asyncio.to_thread(setup.commands.entered.wait, 3)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        with pytest.raises(BaseSourceUnavailable):
            await setup.source.read(setup.request, root_tree_sha=TREE)
        setup.commands.release.set()
        assert setup.source._pending is not None
        await asyncio.gather(setup.source._pending, return_exceptions=True)
        with pytest.raises(BaseSourceUnavailable):
            await setup.source.read(setup.request, root_tree_sha=TREE)

    try:
        asyncio.run(scenario())
    finally:
        setup.commands.release.set()

    assert setup.commands.cancelled.is_set()
    assert not any("fetch" in args for args, _ in setup.commands.calls)


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), 10**400, -1])
def test_policy_rejects_invalid_time_budgets(tmp_path: Path, value: Any) -> None:
    selected = policy(tmp_path)

    with pytest.raises(ValueError):
        replace(selected, total_timeout_seconds=value)


@pytest.mark.parametrize("cancelled", [False, True])
def test_cancelled_or_expired_total_budget_cannot_start_another_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cancelled: bool,
) -> None:
    commands = object.__new__(git_source._LinuxCommands)
    commands.policy = policy(tmp_path)
    commands.root = commands.policy.private_directory
    commands.cancelled = Event()
    commands.deadline = 0 if not cancelled else float("inf")
    if cancelled:
        commands.cancelled.set()

    def forbidden_launch(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Expired or cancelled acquisition must not launch.")

    monkeypatch.setattr(subprocess, "Popen", forbidden_launch)

    with pytest.raises(ValueError):
        commands.run(("--version",), {})


class Scan:
    def __init__(self, entries: Iterator[Any]) -> None:
        self.entries = entries

    def __enter__(self) -> Iterator[Any]:
        return self.entries

    def __exit__(self, *args: object) -> None:
        pass


@pytest.mark.parametrize("operation,limit", [("private", 32), ("disk", 4096)])
def test_directory_scan_stops_before_materializing_unbounded_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    limit: int,
) -> None:
    observed = 0
    information = SimpleNamespace(st_mode=stat.S_IFREG, st_size=0)

    def entries() -> Iterator[Any]:
        nonlocal observed
        while True:
            observed += 1
            yield SimpleNamespace(
                name="ordinary",
                path=str(tmp_path / "ordinary"),
                stat=lambda **kwargs: information,
            )

    monkeypatch.setattr(os, "scandir", lambda root: Scan(entries()))

    with pytest.raises(ValueError):
        if operation == "private":
            git_source._check_private_entries(tmp_path)
        else:
            git_source._disk(tmp_path, 1024)

    assert observed == limit + 1


def test_disk_scan_ignores_only_disappeared_entries_and_still_checks_rest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def disappeared(**kwargs: Any) -> Any:
        raise FileNotFoundError("Expected Git temporary rename.")

    information = SimpleNamespace(st_mode=stat.S_IFREG, st_size=4)
    values = [
        SimpleNamespace(name="gone", path=str(tmp_path / "gone"), stat=disappeared),
        SimpleNamespace(
            name="remaining",
            path=str(tmp_path / "remaining"),
            stat=lambda **kwargs: information,
        ),
    ]
    monkeypatch.setattr(os, "scandir", lambda root: Scan(iter(values)))

    git_source._disk(tmp_path, 4)
    with pytest.raises(ValueError):
        git_source._disk(tmp_path, 3)


def test_private_root_scan_stops_at_first_retained_acquisition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def entries() -> Iterator[Any]:
        yield SimpleNamespace(name="git-read-retained")
        raise AssertionError(
            "A retained acquisition must stop enumeration immediately."
        )

    monkeypatch.setattr(os, "scandir", lambda root: Scan(entries()))

    with pytest.raises(ValueError):
        git_source._check_private_entries(tmp_path)


@pytest.mark.parametrize("failure", [FileNotFoundError, PermissionError])
def test_disk_scan_directory_disappear_is_tolerated_but_access_denial_is_not(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: type[OSError],
) -> None:
    information = SimpleNamespace(st_mode=stat.S_IFDIR)
    child = tmp_path / "temporary"
    visited: list[Path] = []

    def scan(root: Path) -> Scan:
        visited.append(root)
        if root == child:
            raise failure("Controlled temporary directory race.")
        return Scan(
            iter([SimpleNamespace(path=str(child), stat=lambda **kwargs: information)])
        )

    monkeypatch.setattr(os, "scandir", scan)

    if failure is FileNotFoundError:
        git_source._disk(tmp_path, 1)
    else:
        with pytest.raises(PermissionError):
            git_source._disk(tmp_path, 1)

    assert visited == [tmp_path, child]


@pytest.mark.parametrize("damage", ["oversized", "not_regular"])
def test_executable_fd_size_and_type_are_checked_before_reading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    selected = policy(tmp_path)
    information = selected.executable.stat()
    values = {
        name: getattr(information, name) for name in ("st_mode", "st_size", "st_uid")
    }
    values.update(
        {"st_size": 64 * 1024**2 + 1}
        if damage == "oversized"
        else {"st_mode": stat.S_IFIFO | 0o700}
    )
    reads: list[int] = []

    class UnreadableFile:
        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def fileno(self) -> int:
            return 123

        def read(self, maximum: int) -> bytes:
            reads.append(maximum)
            raise AssertionError(
                "Invalid descriptor metadata must fail before reading."
            )

    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: UnreadableFile())
    monkeypatch.setattr(os, "fstat", lambda descriptor: SimpleNamespace(**values))

    with pytest.raises(ValueError):
        git_source._digest(selected.executable)

    assert reads == []


@pytest.mark.parametrize("location", ["descriptor", "named_path"])
def test_executable_identity_is_rechecked_after_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    location: str,
) -> None:
    selected = policy(tmp_path)
    original_fstat, original_stat = os.fstat, Path.stat
    observations = 0

    def changed(information: Any) -> Any:
        fields = (
            "st_dev",
            "st_ino",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
            "st_mode",
            "st_uid",
            "st_gid",
        )
        values = {name: getattr(information, name) for name in fields}
        values["st_ino"] += 1
        return SimpleNamespace(**values)

    def fstat(descriptor: int) -> Any:
        nonlocal observations
        observations += 1
        information = original_fstat(descriptor)
        return (
            changed(information)
            if location == "descriptor" and observations == 2
            else information
        )

    def named_stat(path: Path, *args: Any, **kwargs: Any) -> Any:
        information = original_stat(path, *args, **kwargs)
        return (
            changed(information)
            if location == "named_path" and path == selected.executable
            else information
        )

    monkeypatch.setattr(os, "fstat", fstat)
    monkeypatch.setattr(Path, "stat", named_stat)

    with pytest.raises(ValueError):
        git_source._digest(selected.executable)

    assert observations == 2


def test_executable_growth_during_hash_is_rejected_before_unbounded_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = policy(tmp_path)
    original_open = Path.open
    changed = False

    class GrowingFile:
        def __init__(self, source: Any) -> None:
            self.source = source

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: object) -> None:
            self.source.close()

        def fileno(self) -> int:
            return int(self.source.fileno())

        def read(self, maximum: int) -> bytes:
            nonlocal changed
            result = bytes(self.source.read(maximum))
            if not changed:
                changed = True
                with original_open(selected.executable, "ab") as destination:
                    destination.write(b"growth")
            return result

    def open_fixture(path: Path, *args: Any, **kwargs: Any) -> Any:
        source = original_open(path, *args, **kwargs)
        return (
            GrowingFile(source)
            if path == selected.executable and args == ("rb",)
            else source
        )

    monkeypatch.setattr(Path, "open", open_fixture)

    with pytest.raises(ValueError):
        git_source._digest(selected.executable)

    assert changed


def test_exact_promisor_inventory_notice_is_supported_without_relaxing_fetch_warnings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = context(tmp_path, monkeypatch)
    original = setup.commands.run

    def run(
        arguments: tuple[str, ...],
        environment: dict[str, str],
        *,
        maximum_stdout: int = 65536,
    ) -> git_source._Output:
        result = original(arguments, environment, maximum_stdout=maximum_stdout)
        if "--batch-all-objects" in arguments:
            return git_source._Output(
                result.code, result.stdout, git_source._PROMISOR_NOTICE
            )
        return result

    monkeypatch.setattr(setup.commands, "run", run)

    assert (
        asyncio.run(setup.source.read(setup.request, root_tree_sha=TREE)) == setup.raw
    )
