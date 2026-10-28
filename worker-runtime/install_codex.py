"""Install one independently checksum-pinned official Linux Codex bundle."""

import hashlib
import json
import os
import subprocess
import tarfile
import time
import urllib.request
from pathlib import Path

VERSION = "0.164.0"
DIGEST = "3ec55053109608d5038f7e50750f7d25f768dfd740fef071aa710e2559b63aac"
URL = f"https://github.com/openai/codex/releases/download/rust-v{VERSION}/codex-package-x86_64-unknown-linux-musl.tar.gz"
SIZE = 162338756
CACHE_DIRECTORY = Path("/var/cache/devrelay-codex")


def install() -> None:
    # This directory is a locked BuildKit cache, never copied into the runtime.
    cache = CACHE_DIRECTORY
    cache.mkdir(parents=True, exist_ok=True)
    archive = cache / (DIGEST + ".tar.gz")
    digest = hashlib.sha256()
    size = archive.stat().st_size if archive.exists() else 0
    if size > SIZE:
        archive.unlink()
        raise ValueError("Cached Codex bundle exceeds its pinned size.")
    started = time.monotonic()
    reported = 0
    if size < SIZE:
        request = urllib.request.Request(
            URL, headers={"Range": f"bytes={size}-"} if size else {}
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            if size and response.status != 206:
                size = 0
            elif response.status == 206 and not response.headers.get(
                "Content-Range", ""
            ).startswith(f"bytes {size}-"):
                raise ValueError(
                    "Codex bundle range does not match its retained bytes."
                )
            reported = size
            with archive.open("ab" if size else "wb") as target:
                while chunk := response.read(1024 * 1024):
                    size += len(chunk)
                    if size > 180 * 1024**2 or time.monotonic() - started > 600:
                        raise ValueError(
                            "Codex bundle exceeds its finite download budget."
                        )
                    target.write(chunk)
                    if size - reported >= 16 * 1024**2:
                        print(
                            f"Codex release download: {size // 1024**2} MiB received",
                            flush=True,
                        )
                        reported = size
    with archive.open("rb") as retained:
        while chunk := retained.read(1024 * 1024):
            digest.update(chunk)
    if digest.hexdigest() != DIGEST or size != SIZE:
        archive.unlink()
        raise ValueError("Codex bundle differs from its pinned release.")
    print("Pinned Codex bundle SHA256 verified.", flush=True)
    destination = Path("/opt/codex")
    destination.mkdir()
    with tarfile.open(archive) as source:
        source.extractall(destination, filter="data")
    for name in ("codex", "bwrap", "rg"):
        matches = [path for path in destination.rglob(name) if path.is_file()]
        if len(matches) != 1:
            raise ValueError("Codex bundle does not contain one expected executable.")
        matches[0].chmod(0o755)
        os.symlink(matches[0], Path("/usr/local/bin") / name)
    version = (
        subprocess.check_output(["/usr/local/bin/codex", "--version"], timeout=10)
        .decode()
        .strip()
    )
    if version != f"codex-cli {VERSION}":
        raise ValueError("Codex executable version does not match its bundle.")
    Path("/opt/devrelay/manifest.json").write_text(
        json.dumps({"version": 1, "codex_version": version, "bundle_sha256": DIGEST})
        + "\n"
    )


if __name__ == "__main__":
    install()
