"""Explicit Linux broker Git reads; never a hostile-command sandbox or worker API."""

import asyncio
import base64
import hashlib
import importlib
import os
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from math import isfinite
from pathlib import Path
from threading import Event
from typing import Any
from uuid import uuid4

from devrelay.application.contracts.base_sources import BaseSourceRequest
from devrelay.application.contracts.git_sources import (
    MAX_GIT_COMMIT_BYTES,
    validate_git_commit,
)
from devrelay.application.ports.base_source import (
    BaseSourceAccessDenied,
    BaseSourceUnavailable,
    InvalidBaseSourceData,
)
from devrelay.infrastructure.github.repository_source import RepositoryBinding

_FAILURE = "Broker Git commit acquisition is unavailable."
_PROMISOR_NOTICE = (
    b"warning: This repository uses promisor remotes. Some objects may not be loaded.\n"
)
_EMPTY_TREE_SHA = hashlib.sha1(b"tree 0\0", usedforsecurity=False).hexdigest()
_SETUP = (
    "import os,resource,sys;"
    "resource.setrlimit(resource.RLIMIT_CPU,(10,10));"
    "resource.setrlimit(resource.RLIMIT_AS,(536870912,536870912));"
    "resource.setrlimit(resource.RLIMIT_FSIZE,(8388608,8388608));"
    "resource.setrlimit(resource.RLIMIT_NOFILE,(64,64));"
    "os.execve(sys.argv[1],sys.argv[1:],dict(os.environ))"
)


def _native(module: object, name: str) -> Any:
    # Resolve Linux-only features lazily so Windows imports remain harmless.
    return getattr(module, name)


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerGitPolicy:
    executable: Path
    executable_sha256: str
    version: str
    exec_directory: Path
    https_helper_sha256: str
    private_directory: Path
    operation_timeout_seconds: float = 10
    total_timeout_seconds: float = 30
    stop_timeout_seconds: float = 2
    maximum_disk_bytes: int = 16 * 1024**2
    maximum_retained_bytes: int = 64 * 1024**2
    maximum_processes: int = 8

    def __post_init__(self) -> None:
        if (
            any(
                not isinstance(value, Path) or not value.is_absolute()
                for value in (
                    self.executable,
                    self.exec_directory,
                    self.private_directory,
                )
            )
            or any(
                type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None
                for value in (
                    self.executable_sha256,
                    self.https_helper_sha256,
                )
            )
            or type(self.version) is not str
            or re.fullmatch(r"git version [A-Za-z0-9. -]{1,80}", self.version) is None
        ):
            raise ValueError(
                "Broker Git requires explicit executable and private-root pins."
            )
        for value, maximum in (
            (self.operation_timeout_seconds, 30),
            (self.total_timeout_seconds, 60),
            (self.stop_timeout_seconds, 5),
        ):
            if (
                type(value) not in (int, float)
                or not 0.05 <= value <= maximum
                or not isfinite(value)
            ):
                raise ValueError("Broker Git time limits must be finite and bounded.")
        for value, minimum, maximum in (
            (self.maximum_disk_bytes, 1024**2, 32 * 1024**2),
            (self.maximum_retained_bytes, 1024**2, 256 * 1024**2),
            (self.maximum_processes, 1, 8),
        ):
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError("Broker Git resource budgets must be finite.")


@dataclass(frozen=True, slots=True)
class _Output:
    code: int
    stdout: bytes = field(repr=False)
    stderr: bytes = field(repr=False)


def _digest(path: Path) -> str:
    maximum = 64 * 1024**2
    digest = hashlib.sha256()
    with path.open("rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= maximum:
            raise ValueError(_FAILURE)
        if sys.platform == "linux" and (
            before.st_uid not in (0, os.geteuid())
            or before.st_mode & 0o022
            or not before.st_mode & 0o111
        ):
            raise ValueError(_FAILURE)
        total = 0
        while chunk := source.read(min(65536, maximum + 1 - total)):
            total += len(chunk)
            if total > before.st_size or total > maximum:
                raise ValueError(_FAILURE)
            digest.update(chunk)
        after, named = os.fstat(source.fileno()), path.stat()
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
        # Windows named-stat ctime can expose birth time while fstat exposes change
        # time. The production Linux backend compares all fields against both.
        named_fields = (
            fields
            if sys.platform == "linux"
            else tuple(field for field in fields if field != "st_ctime_ns")
        )
        if (
            total != before.st_size
            or any(getattr(before, field) != getattr(after, field) for field in fields)
            or any(
                getattr(before, field) != getattr(named, field)
                for field in named_fields
            )
        ):
            raise ValueError(_FAILURE)
    return digest.hexdigest()


def _verify_profile(policy: BrokerGitPolicy) -> None:
    if sys.platform == "linux":
        for directory in (policy.executable.parent, policy.exec_directory):
            information = directory.stat()
            if (
                not stat.S_ISDIR(information.st_mode)
                or information.st_uid not in (0, os.geteuid())
                or information.st_mode & 0o022
            ):
                raise ValueError(_FAILURE)
    if (
        _digest(policy.executable) != policy.executable_sha256
        or _digest(policy.exec_directory / "git-remote-https")
        != policy.https_helper_sha256
    ):
        raise ValueError(_FAILURE)


def _disk(root: Path, maximum: int) -> None:
    size = entries = 0
    pending = [root]
    while pending:
        try:
            with os.scandir(pending.pop()) as children:
                for item in children:
                    entries += 1
                    if entries > 4096:
                        raise ValueError(_FAILURE)
                    try:
                        information = item.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if stat.S_ISDIR(information.st_mode):
                        pending.append(Path(item.path))
                    elif stat.S_ISREG(information.st_mode):
                        size += information.st_size
                    else:
                        raise ValueError(_FAILURE)
                    if size > maximum:
                        raise ValueError(_FAILURE)
        except FileNotFoundError:
            continue


def _check_private_entries(root: Path) -> None:
    with os.scandir(root) as entries:
        for count, item in enumerate(entries, 1):
            if count > 32 or item.name.startswith("git-read-"):
                raise ValueError(_FAILURE)


def _capabilities(trace: bytes) -> None:
    lines = [
        match.group(1) for match in re.finditer(rb"packet:\s+\S+< ([^\r\n]+)", trace)
    ]
    features = [line[6:].split() for line in lines if line.startswith(b"fetch=")]
    if b"version 2" not in lines or not any(
        b"shallow" in item and b"filter" in item for item in features
    ):
        raise ValueError(_FAILURE)


@contextmanager
def _private_lock(root: Path) -> Iterator[None]:
    if sys.platform != "linux" or not hasattr(os, "O_NOFOLLOW"):
        raise BaseSourceUnavailable(
            "Broker Git requires its tested Linux process backend."
        )
    native = importlib.import_module("fcntl")
    handle = os.open(
        root / ".broker-git.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
    )
    try:
        information = os.fstat(handle)
        if (
            not stat.S_ISREG(information.st_mode)
            or information.st_uid != os.geteuid()
            or information.st_mode & 0o077
        ):
            raise ValueError(_FAILURE)
        native.flock(handle, native.LOCK_EX | native.LOCK_NB)
        try:
            yield
        finally:
            native.flock(handle, native.LOCK_UN)
    finally:
        os.close(handle)


class _LinuxCommands:
    """Bound trusted Git groups, not arbitrary commands or escaped descendants.

    The fixed trusted broker interpreter sets inherited rlimits before exec; no
    Python preexec callback runs inside the multithreaded control process. The
    leader remains unreaped until group termination, avoiding destructive reuse
    of a reaped leader's numeric group ID. Unverified group absence fails closed.
    """

    policy: BrokerGitPolicy
    root: Path
    cancelled: Event
    deadline: float
    settled: bool

    def __init__(self, policy: BrokerGitPolicy, root: Path, cancelled: Event) -> None:
        if (
            sys.platform != "linux"
            or not hasattr(os, "waitid")
            or not hasattr(os, "WNOWAIT")
            or not Path("/proc/self/stat").is_file()
        ):
            raise BaseSourceUnavailable(
                "Broker Git requires its tested Linux process backend."
            )
        self.policy, self.root, self.cancelled = policy, root, cancelled
        self.deadline = time.monotonic() + policy.total_timeout_seconds
        self.settled = True

    def _group_size(self, group: int) -> None:
        count = scanned = 0
        with os.scandir("/proc") as processes:
            for entry in processes:
                scanned += 1
                if scanned > 65536:
                    raise ValueError(_FAILURE)
                if not entry.name.isdecimal():
                    continue
                try:
                    with (Path(entry.path) / "stat").open("rb") as source:
                        content = source.read(4097)
                except FileNotFoundError, ProcessLookupError:
                    continue
                if len(content) > 4096:
                    raise ValueError(_FAILURE)
                # comm may contain spaces/parentheses; fields follow its last ')'.
                fields = content.rsplit(b")", 1)[1].split()
                if int(fields[2]) == group:
                    count += 1
                if count > self.policy.maximum_processes:
                    raise ValueError(_FAILURE)

    def _stop(self, process: subprocess.Popen[bytes]) -> None:
        # Do not poll()/wait() before signalling: keep the leader identity pinned.
        try:
            _native(os, "killpg")(process.pid, _native(signal, "SIGKILL"))
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + self.policy.stop_timeout_seconds
        process.wait(timeout=max(0.001, deadline - time.monotonic()))
        while True:
            try:
                _native(os, "killpg")(process.pid, 0)
            except ProcessLookupError:
                self.settled = True
                return
            if time.monotonic() >= deadline:
                raise ValueError(_FAILURE)
            time.sleep(0.01)

    def run(
        self,
        arguments: tuple[str, ...],
        environment: dict[str, str],
        *,
        maximum_stdout: int = 65536,
    ) -> _Output:
        _verify_profile(self.policy)
        if self.cancelled.is_set() or time.monotonic() >= self.deadline:
            raise ValueError(_FAILURE)
        deadline = min(
            self.deadline, time.monotonic() + self.policy.operation_timeout_seconds
        )
        output, error = bytearray(), bytearray()
        process = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-c",
                _SETUP,
                str(self.policy.executable),
                *arguments,
            ],
            cwd=self.root,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            close_fds=True,
        )
        self.settled = False
        assert process.stdout is not None and process.stderr is not None
        streams = (process.stdout, process.stderr)
        status: Any = None
        try:
            with selectors.DefaultSelector() as selector:
                for stream in streams:
                    os.set_blocking(stream.fileno(), False)
                    selector.register(stream, selectors.EVENT_READ)
                while selector.get_map() or status is None:
                    if self.cancelled.is_set() or time.monotonic() >= deadline:
                        raise ValueError(_FAILURE)
                    _disk(self.root, self.policy.maximum_disk_bytes)
                    self._group_size(process.pid)
                    status = _native(os, "waitid")(
                        _native(os, "P_PID"),
                        process.pid,
                        _native(os, "WEXITED")
                        | _native(os, "WNOHANG")
                        | _native(os, "WNOWAIT"),
                    )
                    for key, _ in selector.select(0.01):
                        chunk = os.read(key.fd, 4096)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        destination = output if key.fileobj is process.stdout else error
                        maximum = maximum_stdout if destination is output else 65536
                        if len(destination) + len(chunk) > maximum:
                            raise ValueError(_FAILURE)
                        destination.extend(chunk)
                assert status is not None
                code = (
                    status.si_status
                    if status.si_code == _native(os, "CLD_EXITED")
                    else -status.si_status
                )
            return _Output(code, bytes(output), bytes(error))
        finally:
            try:
                self._stop(process)
            finally:
                for stream in streams:
                    stream.close()


class GitHubGitCommitSource:
    """Read one original commit inside the trusted broker only.

    The composing GitHub source brackets this reader with numeric repository
    observations. Authentication remains transient in trusted child environments.
    Failure retains its private partial directory and blocks further acquisition
    until operator cleanup. No implicit OS installation or fallback.
    """

    def __init__(
        self,
        *,
        policy: BrokerGitPolicy,
        bindings: tuple[RepositoryBinding, ...],
        token: str,
    ) -> None:
        if (
            not isinstance(policy, BrokerGitPolicy)
            or type(bindings) is not tuple
            or not 1 <= len(bindings) <= 32
        ):
            raise ValueError("Broker Git requires explicit bounded configuration.")
        policy.__post_init__()
        if (
            type(token) is not str
            or re.fullmatch(r"[A-Za-z0-9_-]{20,256}", token) is None
        ):
            raise ValueError("Broker Git requires a separate bounded read credential.")
        for binding in bindings:
            if not isinstance(binding, RepositoryBinding):
                raise ValueError("Broker Git requires typed repository bindings.")
            BaseSourceRequest(
                repository_id=binding.repository_id,
                provider_repository_id=binding.github_id,
                repository_slug=binding.slug,
                base_revision="0" * 40,
            )
        if len({item.repository_id for item in bindings}) != len(bindings) or len(
            {item.github_id for item in bindings}
        ) != len(bindings):
            raise ValueError("Broker Git requires unique repository bindings.")
        self._policy, self._bindings, self._token = (
            policy,
            {item.repository_id: item for item in bindings},
            token,
        )
        self._pending: asyncio.Future[bytes] | None = None

    def _environment(
        self, root: Path, url: str, *, online: bool, trace: bool = False
    ) -> dict[str, str]:
        values: tuple[tuple[str, str], ...] = (
            ("protocol.allow", "never"),
            ("protocol.https.allow", "always" if online else "never"),
            ("protocol.version", "2"),
            ("http.sslVerify", "true"),
            ("http.followRedirects", "false"),
            ("http.proxy", ""),
            ("http.cookieFile", ""),
            ("http.saveCookies", "false"),
            ("credential.helper", ""),
            ("credential.interactive", "false"),
            ("credential.useHttpPath", "true"),
            ("core.hooksPath", str(root / "empty")),
            ("init.templateDir", str(root / "empty")),
            ("init.defaultBranch", "devrelay-base"),
            ("core.fsmonitor", "false"),
            ("gc.auto", "0"),
            ("maintenance.auto", "false"),
            ("fetch.writeCommitGraph", "false"),
            ("fetch.parallel", "1"),
            ("pack.threads", "1"),
            ("index.threads", "1"),
            ("submodule.recurse", "false"),
            ("fetch.recurseSubmodules", "false"),
            ("promisor.acceptFromServer", "none"),
            ("promisor.storeFields", ""),
        )
        if online:
            secret = base64.b64encode(
                ("x-access-token:" + self._token).encode("ascii")
            ).decode("ascii")
            values += (
                ("http." + url + ".extraHeader", "Authorization: Basic " + secret),
            )
        environment = {
            "PATH": str(self._policy.exec_directory),
            "HOME": str(root / "home"),
            "XDG_CONFIG_HOME": str(root / "home"),
            "TMPDIR": str(root),
            "LC_ALL": "C",
            "GIT_EXEC_PATH": str(self._policy.exec_directory),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ASKPASS": "",
            "SSH_ASKPASS": "",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_NO_LAZY_FETCH": "1",
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_CONFIG_COUNT": str(len(values)),
            "GIT_ALLOW_PROTOCOL": "https" if online else "",
            "GIT_TRACE_REDACT": "1",
        }
        for index, (key, value) in enumerate(values):
            environment[f"GIT_CONFIG_KEY_{index}"] = key
            environment[f"GIT_CONFIG_VALUE_{index}"] = value
        if trace:
            environment["GIT_TRACE_PACKET"] = "1"
        return environment

    def _read(self, request: BaseSourceRequest, tree: str, cancelled: Event) -> bytes:
        parent = self._policy.private_directory
        if parent.is_symlink() or not parent.is_dir():
            raise ValueError(_FAILURE)
        if sys.platform == "linux":
            information = parent.stat()
            if information.st_uid != os.geteuid() or information.st_mode & 0o077:
                raise ValueError(_FAILURE)
        with _private_lock(parent):
            _check_private_entries(parent)
            return self._read_locked(request, tree, cancelled)

    def _read_locked(
        self, request: BaseSourceRequest, tree: str, cancelled: Event
    ) -> bytes:
        policy = self._policy
        _verify_profile(policy)
        parent = policy.private_directory
        if parent.is_symlink() or not parent.is_dir():
            raise ValueError(_FAILURE)
        if sys.platform == "linux":
            information = parent.stat()
            if information.st_uid != os.geteuid() or information.st_mode & 0o077:
                raise ValueError(_FAILURE)
        _disk(parent, policy.maximum_retained_bytes)
        root = parent / ("git-read-" + uuid4().hex)
        root.mkdir(mode=0o700)
        for name in ("home", "empty"):
            (root / name).mkdir(mode=0o700)
        commands = _LinuxCommands(policy, root, cancelled)
        binding = self._bindings[request.repository_id]
        url = "https://github.com/" + binding.slug + ".git"
        offline = self._environment(root, url, online=False)

        def command(
            arguments: tuple[str, ...],
            *,
            online: bool = False,
            trace: bool = False,
            maximum_stdout: int = 65536,
        ) -> _Output:
            if cancelled.is_set():
                raise ValueError(_FAILURE)
            output = commands.run(
                arguments,
                self._environment(root, url, online=online, trace=trace)
                if online
                else offline,
                maximum_stdout=maximum_stdout,
            )
            if output.code != 0:
                raise ValueError(_FAILURE)
            return output

        version = command(("--version",))
        if version.stdout != (policy.version + "\n").encode("ascii") or version.stderr:
            raise ValueError(_FAILURE)
        initialized = command(
            (
                "init",
                "--quiet",
                "--bare",
                "--object-format=sha1",
                "--template=" + str(root / "empty"),
                str(root / "repo"),
            )
        )
        if initialized.stderr:
            raise ValueError(_FAILURE)
        probe = command(
            ("ls-remote", "--refs", url, "refs/__devrelay_capability_only__"),
            online=True,
            trace=True,
        )
        _capabilities(probe.stderr)
        fetched = command(
            (
                "--git-dir=" + str(root / "repo"),
                "fetch",
                "--quiet",
                "--depth=1",
                "--filter=tree:0",
                "--no-tags",
                "--no-write-fetch-head",
                "--no-recurse-submodules",
                "--no-auto-maintenance",
                "--no-write-commit-graph",
                url,
                request.base_revision,
            ),
            online=True,
            trace=True,
        )
        _capabilities(fetched.stderr)
        if (
            b"warning:" in fetched.stderr.lower()
            or b"error:" in fetched.stderr.lower()
            or b"> filter tree:0" not in fetched.stderr
            or b"> deepen 1" not in fetched.stderr
            or b"> want " + request.base_revision.encode("ascii") not in fetched.stderr
        ):
            raise ValueError(_FAILURE)
        prefix = ("--git-dir=" + str(root / "repo"),)
        size = command(prefix + ("cat-file", "-s", request.base_revision))
        if size.stderr or re.fullmatch(rb"[1-9][0-9]{0,5}\n", size.stdout) is None:
            raise ValueError(_FAILURE)
        length = int(size.stdout)
        if length > MAX_GIT_COMMIT_BYTES:
            raise ValueError(_FAILURE)
        raw = command(
            prefix + ("cat-file", "commit", request.base_revision),
            maximum_stdout=MAX_GIT_COMMIT_BYTES,
        )
        if raw.stderr or len(raw.stdout) != length:
            raise ValueError(_FAILURE)
        validate_git_commit(raw.stdout, request.base_revision, tree)
        objects = command(
            prefix
            + (
                "cat-file",
                "--batch-all-objects",
                "--batch-check=%(objectname) %(objecttype) %(objectsize)",
            )
        )
        commit_row = f"{request.base_revision} commit {length}\n".encode("ascii")
        # Git 2.55 can include the zero-byte root tree despite tree:0. Accept only
        # this exact expected object, then independently read/hash its bytes.
        # --batch-all-objects defaults to ascending object-name order.
        empty_root_present = tree == _EMPTY_TREE_SHA and objects.stdout == b"".join(
            sorted((commit_row, f"{_EMPTY_TREE_SHA} tree 0\n".encode("ascii")))
        )
        if objects.stderr not in (
            b"",
            _PROMISOR_NOTICE,
        ) or (objects.stdout != commit_row and not empty_root_present):
            raise ValueError(_FAILURE)
        if empty_root_present:
            empty_root = command(prefix + ("cat-file", "tree", tree), maximum_stdout=1)
            if (
                empty_root.stderr
                or empty_root.stdout != b""
                or hashlib.sha1(
                    b"tree "
                    + str(len(empty_root.stdout)).encode("ascii")
                    + b"\0"
                    + empty_root.stdout,
                    usedforsecurity=False,
                ).hexdigest()
                != tree
            ):
                raise ValueError(_FAILURE)
        repository = root / "repo"
        shallow = repository / "shallow"
        parents = any(
            line.startswith(b"parent ")
            for line in raw.stdout.partition(b"\n\n")[0].split(b"\n")
        )
        if shallow.exists():
            if shallow.read_bytes() != request.base_revision.encode("ascii") + b"\n":
                raise ValueError(_FAILURE)
        elif parents:
            raise ValueError(_FAILURE)
        if (
            (repository / "objects/info/alternates").exists()
            or not commands.settled
            or cancelled.is_set()
        ):
            raise ValueError(_FAILURE)
        _disk(root, policy.maximum_disk_bytes)
        # Only successful, fully settled, created-here private resources are removed.
        if root.resolve().parent != parent.resolve() or root.is_symlink():
            raise ValueError(_FAILURE)
        shutil.rmtree(root)
        return raw.stdout

    async def read(self, request: BaseSourceRequest, *, root_tree_sha: str) -> bytes:
        if not isinstance(request, BaseSourceRequest):
            raise InvalidBaseSourceData(_FAILURE)
        try:
            request.__post_init__()
        except ValueError, TypeError:
            raise InvalidBaseSourceData(_FAILURE) from None
        binding = self._bindings.get(request.repository_id)
        if (
            binding is None
            or binding.github_id != request.provider_repository_id
            or binding.slug.casefold() != request.repository_slug.casefold()
        ):
            raise BaseSourceAccessDenied(
                "Broker Git source is outside configured scope."
            )
        if (
            type(root_tree_sha) is not str
            or re.fullmatch(r"[0-9a-f]{40}", root_tree_sha) is None
        ):
            raise InvalidBaseSourceData(_FAILURE)
        if self._pending is not None and not self._pending.done():
            raise BaseSourceUnavailable(_FAILURE)
        cancelled = Event()
        task = asyncio.create_task(
            asyncio.to_thread(self._read, request, root_tree_sha, cancelled)
        )
        self._pending = task
        task.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        try:
            await asyncio.wait({task})
            return task.result()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        except Exception:
            raise BaseSourceUnavailable(_FAILURE) from None
