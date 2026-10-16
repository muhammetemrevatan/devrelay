"""Actual Linux Git/process fixtures; no provider network or real authentication."""

import asyncio
import base64
import hashlib
import os
import shutil
import ssl
import subprocess
import sys
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Event, Thread
from uuid import UUID

import pytest

import devrelay.infrastructure.github.git_commit_source as git_source
from devrelay.application.contracts.base_sources import BaseSourceRequest
from devrelay.application.ports.base_source import BaseSourceUnavailable
from devrelay.infrastructure.github.git_commit_source import (
    BrokerGitPolicy,
    GitHubGitCommitSource,
)
from devrelay.infrastructure.github.repository_source import RepositoryBinding

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Actual broker Git backend requires Linux."
)
TOKEN = "fixture_only_no_provider_auth_" + "x" * 32


def local_git(
    executable: Path, arguments: tuple[str, ...], *, stdin: bytes | None = None
) -> bytes:
    result = subprocess.run(
        [str(executable), *arguments],
        input=stdin,
        capture_output=True,
        check=True,
        timeout=10,
        env={
            "PATH": str(executable.parent),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "HOME": str(executable.parent),
        },
    )
    return result.stdout


def profile(tmp_path: Path) -> BrokerGitPolicy:
    selected = shutil.which(
        "git"
    )  # Fixture discovery only; production never searches PATH.
    if selected is None:
        pytest.skip("The controlled Linux fixture image must explicitly provide Git.")
    executable = Path(selected).resolve()
    helpers = Path(local_git(executable, ("--exec-path",)).decode().strip())
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    return BrokerGitPolicy(
        executable=executable,
        executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
        version=local_git(executable, ("--version",)).decode().strip(),
        exec_directory=helpers,
        https_helper_sha256=hashlib.sha256(
            (helpers / "git-remote-https").read_bytes()
        ).hexdigest(),
        private_directory=private,
    )


def object_sha(raw: bytes) -> str:
    return hashlib.sha1(
        b"commit " + str(len(raw)).encode() + b"\0" + raw, usedforsecurity=False
    ).hexdigest()


@pytest.mark.parametrize("variant", ["unsigned", "signed", "encoding", "mergetag"])
def test_actual_git_preserves_original_commit_bytes_from_local_filtered_shallow_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
) -> None:
    pins = profile(tmp_path)
    remote = tmp_path / "remote.git"
    template = tmp_path / "empty-template"
    template.mkdir()
    local_git(
        pins.executable,
        ("init", "--bare", "--quiet", "--template=" + str(template), str(remote)),
    )
    prefix = ("--git-dir=" + str(remote),)
    tree = (
        local_git(
            pins.executable,
            prefix + ("hash-object", "-t", "tree", "-w", "--stdin"),
            stdin=b"",
        )
        .decode()
        .strip()
    )
    parent_raw = (
        b"tree "
        + tree.encode()
        + b"\nauthor Fixture <fixture@example.invalid> 1700000000 +0330\n"
        b"committer Fixture <fixture@example.invalid> 1700000000 -0500\n\nParent fixture\n"
    )
    parent = (
        local_git(
            pins.executable,
            prefix + ("hash-object", "-t", "commit", "-w", "--stdin"),
            stdin=parent_raw,
        )
        .decode()
        .strip()
    )
    extra = b""
    if variant == "signed":
        extra = b"gpgsig -----BEGIN PGP SIGNATURE-----\n fixture opaque signature\n -----END PGP SIGNATURE-----\n"
    elif variant == "encoding":
        extra = b"encoding ISO-8859-1\n"
    elif variant == "mergetag":
        extra = (
            b"mergetag object "
            + parent.encode()
            + b"\n type commit\n tag fixture\n tagger Fixture <fixture@example.invalid> 1700000000 +0000\n \n fixture signed tag payload\n"
        )
    raw = (
        b"tree "
        + tree.encode()
        + b"\nparent "
        + parent.encode()
        + b"\nauthor Fixture <fixture@example.invalid> 1700000001 +0330\n"
        + b"committer Fixture <fixture@example.invalid> 1700000002 -0500\n"
        + extra
        + b"\nOriginal fixture message "
        + (b"caf\xe9" if variant == "encoding" else b"bytes")
        + b"\n"
    )
    sha = (
        local_git(
            pins.executable,
            prefix + ("hash-object", "-t", "commit", "-w", "--stdin"),
            stdin=raw,
        )
        .decode()
        .strip()
    )
    local_git(pins.executable, prefix + ("update-ref", "refs/heads/fixture", sha))
    local_git(pins.executable, prefix + ("config", "uploadpack.allowFilter", "true"))
    local_git(
        pins.executable, prefix + ("config", "uploadpack.allowAnySHA1InWant", "true")
    )
    binding = RepositoryBinding(
        repository_id=UUID(int=701),
        github_id=9001,
        slug="fixture/remote",
        target_branch="master",
        coding_branch_prefix="agent/",
    )
    request = BaseSourceRequest(
        repository_id=binding.repository_id,
        provider_repository_id=binding.github_id,
        repository_slug=binding.slug,
        base_revision=sha,
    )
    outbound: list[tuple[str, ...]] = []

    class LocalFixtureCommands(git_source._LinuxCommands):
        def run(
            self,
            arguments: tuple[str, ...],
            environment: dict[str, str],
            *,
            maximum_stdout: int = 65536,
        ) -> git_source._Output:
            outbound.append(arguments)
            expected_url = "https://github.com/fixture/remote.git"
            if expected_url in arguments:
                # Only this controlled fixture substitutes the provider transport.
                arguments = tuple(
                    remote.as_uri() if item == expected_url else item
                    for item in arguments
                )
                environment = environment.copy()
                count = int(environment["GIT_CONFIG_COUNT"])
                environment[f"GIT_CONFIG_KEY_{count}"] = "protocol.file.allow"
                environment[f"GIT_CONFIG_VALUE_{count}"] = "always"
                environment["GIT_CONFIG_COUNT"] = str(count + 1)
                environment["GIT_ALLOW_PROTOCOL"] = "file"
            result = super().run(arguments, environment, maximum_stdout=maximum_stdout)
            return result

    monkeypatch.setattr(git_source, "_LinuxCommands", LocalFixtureCommands)
    source = GitHubGitCommitSource(policy=pins, bindings=(binding,), token=TOKEN)

    try:
        actual = asyncio.run(source.read(request, root_tree_sha=tree))
    except BaseSourceUnavailable:
        assert source._pending is not None
        raise AssertionError(
            "Controlled local Git fixture failed."
        ) from source._pending.exception()

    assert actual == raw and object_sha(actual) == sha
    assert not any(
        path.name.startswith("git-read-") for path in pins.private_directory.iterdir()
    )
    assert all(TOKEN not in repr(arguments) for arguments in outbound)
    assert (remote / "objects").is_dir()


def test_actual_linux_output_budget_stops_group_and_confirms_absence(
    tmp_path: Path,
) -> None:
    pins = profile(tmp_path)
    commands = git_source._LinuxCommands(pins, pins.private_directory, Event())
    environment = {
        "PATH": str(pins.exec_directory),
        "HOME": str(pins.private_directory),
    }

    with pytest.raises(ValueError):
        commands.run(("--version",), environment, maximum_stdout=1)

    assert commands.settled


def test_actual_linux_setup_applies_limits_and_runtime_cancel_stops_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pins = replace(profile(tmp_path), operation_timeout_seconds=0.1)
    commands = git_source._LinuxCommands(pins, pins.private_directory, Event())
    # Trusted fixed test canary replaces only the launch-setup body, never repo code.
    monkeypatch.setattr(
        git_source,
        "_SETUP",
        git_source._SETUP.rsplit("os.execve", 1)[0] + "import time;time.sleep(10)",
    )

    with pytest.raises(ValueError):
        commands.run(
            ("--version",),
            {"PATH": str(pins.exec_directory), "HOME": str(pins.private_directory)},
        )

    assert commands.settled


def test_actual_linux_private_root_lock_rejects_concurrent_acquisition(
    tmp_path: Path,
) -> None:
    pins = profile(tmp_path)

    with git_source._private_lock(pins.private_directory):
        with pytest.raises(BlockingIOError):
            with git_source._private_lock(pins.private_directory):
                raise AssertionError("The second lock must not be admitted.")


@pytest.mark.parametrize("response", ["challenge", "redirect", "untrusted_tls"])
def test_actual_https_credential_channel_challenge_redirect_and_tls_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    response: str,
) -> None:
    pins = profile(tmp_path)
    openssl = shutil.which("openssl")
    assert openssl is not None, "The explicit Linux Git fixture image requires openssl."
    certificate, key = tmp_path / "fixture.crt", tmp_path / "fixture.key"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
        ],
        capture_output=True,
        check=True,
        timeout=10,
    )
    received: list[tuple[str, str | None]] = []

    class Origin(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            received.append((self.path, self.headers.get("Authorization")))
            if response == "redirect":
                self.send_response(302)
                self.send_header(
                    "Location",
                    f"https://localhost:{server.server_port}/unexpected-redirect",
                )
            else:
                self.send_response(401)
                self.send_header("WWW-Authenticate", 'Basic realm="fixture"')
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Origin)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(certificate, key)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    github_url = "https://github.com/fixture/remote.git"
    local_url = f"https://localhost:{server.server_port}/fixture/remote.git"

    class HTTPSFixtureCommands(git_source._LinuxCommands):
        def run(
            self,
            arguments: tuple[str, ...],
            environment: dict[str, str],
            *,
            maximum_stdout: int = 65536,
        ) -> git_source._Output:
            if github_url in arguments:
                arguments = tuple(
                    local_url if item == github_url else item for item in arguments
                )
                environment = environment.copy()
                count = int(environment["GIT_CONFIG_COUNT"])
                for index in range(count):
                    name = f"GIT_CONFIG_KEY_{index}"
                    if environment[name] == "http." + github_url + ".extraHeader":
                        environment[name] = "http." + local_url + ".extraHeader"
                if response != "untrusted_tls":
                    environment[f"GIT_CONFIG_KEY_{count}"] = "http.sslCAInfo"
                    environment[f"GIT_CONFIG_VALUE_{count}"] = str(certificate)
                    environment["GIT_CONFIG_COUNT"] = str(count + 1)
            return super().run(arguments, environment, maximum_stdout=maximum_stdout)

    monkeypatch.setattr(git_source, "_LinuxCommands", HTTPSFixtureCommands)
    binding = RepositoryBinding(
        repository_id=UUID(int=711),
        github_id=9002,
        slug="fixture/remote",
        target_branch="master",
        coding_branch_prefix="agent/",
    )
    request = BaseSourceRequest(
        repository_id=binding.repository_id,
        provider_repository_id=binding.github_id,
        repository_slug=binding.slug,
        base_revision="a" * 40,
    )
    source = GitHubGitCommitSource(policy=pins, bindings=(binding,), token=TOKEN)
    try:
        with pytest.raises(BaseSourceUnavailable) as caught:
            asyncio.run(source.read(request, root_tree_sha="b" * 40))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert TOKEN not in str(caught.value)
    if response == "untrusted_tls":
        assert received == []
    else:
        encoded = base64.b64encode(("x-access-token:" + TOKEN).encode()).decode()
        assert received == [
            (
                "/fixture/remote.git/info/refs?service=git-upload-pack",
                "Basic " + encoded,
            )
        ]
    assert not any(
        TOKEN.encode() in item.read_bytes()
        for item in pins.private_directory.rglob("*")
        if item.is_file()
    )
