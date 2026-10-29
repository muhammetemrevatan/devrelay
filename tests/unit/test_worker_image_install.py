"""Build-download cache fixtures never contact a server or execute a bundle."""

import hashlib
import io
import runpy
from pathlib import Path
from typing import Any

import pytest


class VerifiedArchiveReached(Exception):
    pass


@pytest.mark.parametrize(
    "partial, status", [(b"", 200), (b"x", 206), (b"x", 200), (b"xyz", None)]
)
def test_cached_or_resumed_bytes_must_match_pinned_bundle_before_extract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    partial: bytes,
    status: int | None,
) -> None:
    module = runpy.run_path(
        str(Path(__file__).resolve().parents[2] / "worker-runtime" / "install_codex.py")
    )
    install = module["install"]
    values = install.__globals__
    digest = hashlib.sha256(b"xyz").hexdigest()
    values.update(SIZE=3, DIGEST=digest, CACHE_DIRECTORY=tmp_path)
    archive = tmp_path / (digest + ".tar.gz")
    archive.write_bytes(partial)
    captured: list[str | None] = []

    class Response(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(b"yz" if status == 206 else b"xyz")
            self.status = status
            self.headers = {"Content-Range": "bytes 1-2/3"}

    def fetch(request: Any, **kwargs: object) -> Response:
        assert status is not None
        captured.append(request.get_header("Range"))
        return Response()

    def verified(*args: object, **kwargs: object) -> None:
        assert archive.read_bytes() == b"xyz"
        raise VerifiedArchiveReached

    monkeypatch.setattr(values["urllib"].request, "urlopen", fetch)
    monkeypatch.setattr(values["tarfile"], "open", verified)
    # Stop before the build-only destination or any executable is touched.
    monkeypatch.setattr(values["Path"], "mkdir", lambda *args, **kwargs: None)
    with pytest.raises(VerifiedArchiveReached):
        install()
    assert captured == ([] if status is None else ["bytes=1-" if partial else None])


def test_poisoned_complete_cache_is_removed_without_extract_or_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = runpy.run_path(
        str(Path(__file__).resolve().parents[2] / "worker-runtime" / "install_codex.py")
    )
    install = module["install"]
    values = install.__globals__
    digest = hashlib.sha256(b"xyz").hexdigest()
    values.update(SIZE=3, DIGEST=digest, CACHE_DIRECTORY=tmp_path)
    archive = tmp_path / (digest + ".tar.gz")
    archive.write_bytes(b"bad")

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError(
            "Unverified bytes must not reach network/extraction/execution."
        )

    monkeypatch.setattr(values["urllib"].request, "urlopen", unexpected)
    monkeypatch.setattr(values["tarfile"], "open", unexpected)
    with pytest.raises(ValueError, match="differs from its pinned release"):
        install()
    assert not archive.exists()
