import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, field
from enum import StrEnum
from math import isfinite
from uuid import UUID

from devrelay.application.contracts.workspace_recovery import canonical_workspace_path
from devrelay.application.contracts.workspaces import WorkspaceReservation

MAX_BASE_FILES = 1024
MAX_BASE_FILE_BYTES = 4 * 1024**2
MAX_BASE_TOTAL_BYTES = 32 * 1024**2
MAX_BASE_DEPTH = 16


def _sha(value: object) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise ValueError("Base source requires canonical Git SHA-1 object IDs.")


def canonical_base_path(value: object) -> str:
    if (
        type(value) is not str
        or not value
        or "\\" in value
        or len(value.encode("utf-8", errors="surrogatepass")) > 240
        or unicodedata.normalize("NFC", value) != value
    ):
        raise ValueError("Base source requires a canonical relative file path.")
    parts = value.split("/")
    if len(parts) > MAX_BASE_DEPTH or any(part.casefold() == ".git" for part in parts):
        raise ValueError("Base source path exceeds its allowed namespace.")
    canonical_workspace_path("C:\\" + value.replace("/", "\\"))
    return value


def git_blob_sha(content: bytes) -> str:
    # Git object correlation uses the repository's SHA-1 format, not a signature.
    return hashlib.sha1(
        b"blob " + str(len(content)).encode("ascii") + b"\x00" + content,
        usedforsecurity=False,
    ).hexdigest()


class BaseFileMode(StrEnum):
    REGULAR = "100644"
    EXECUTABLE = "100755"


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseSourceRequest:
    repository_id: UUID
    provider_repository_id: int
    repository_slug: str
    base_revision: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.repository_id, UUID)
            or type(self.provider_repository_id) is not int
            or not 1 <= self.provider_repository_id < 2**63
            or type(self.repository_slug) is not str
            or re.fullmatch(
                r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})/[A-Za-z0-9_][A-Za-z0-9_.-]{0,99}",
                self.repository_slug,
            )
            is None
            or self.repository_slug.endswith(".git")
        ):
            raise ValueError("Base source requires pinned repository identity.")
        _sha(self.base_revision)


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseSourceFile:
    path: str
    blob_sha: str
    mode: BaseFileMode
    content: bytes = field(repr=False)

    def __post_init__(self) -> None:
        canonical_base_path(self.path)
        _sha(self.blob_sha)
        if (
            not isinstance(self.mode, BaseFileMode)
            or type(self.content) is not bytes
            or len(self.content) > MAX_BASE_FILE_BYTES
            or git_blob_sha(self.content) != self.blob_sha
            or self.content.split(b"\n", 1)[0].rstrip(b"\r")
            == b"version https://git-lfs.github.com/spec/v1"
        ):
            raise ValueError("Base file requires bounded verified ordinary blob bytes.")


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseSourceSnapshot:
    request: BaseSourceRequest
    root_tree_sha: str
    files: tuple[BaseSourceFile, ...] = field(repr=False)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.request, BaseSourceRequest):
            raise ValueError("Base snapshot requires pinned repository input.")
        self.request.__post_init__()
        _sha(self.root_tree_sha)
        if type(self.files) is not tuple or len(self.files) > MAX_BASE_FILES:
            raise ValueError("Base snapshot exceeds bounded file count.")
        names: dict[str, str] = {}
        files: set[str] = set()
        directories: set[str] = set()
        total = 0
        previous: bytes | None = None
        for item in self.files:
            if not isinstance(item, BaseSourceFile):
                raise ValueError("Base snapshot requires ordinary files.")
            if type(item.content) is not bytes:
                raise ValueError("Base snapshot requires bounded bytes.")
            total += len(item.content)
            if total > MAX_BASE_TOTAL_BYTES:
                raise ValueError("Base snapshot exceeds byte limits.")
            item.__post_init__()
            encoded = item.path.encode("utf-8")
            if previous is not None and encoded <= previous:
                raise ValueError("Base snapshot requires unique canonical ordering.")
            previous = encoded
            parts = item.path.split("/")
            for index in range(1, len(parts) + 1):
                path = "/".join(parts[:index])
                key = path.casefold()
                if key in names and names[key] != path:
                    raise ValueError("Base snapshot has ambiguous path aliases.")
                names[key] = path
                if index < len(parts):
                    directories.add(key)
            files.add(item.path.casefold())
        if files & directories or total > MAX_BASE_TOTAL_BYTES:
            raise ValueError(
                "Base snapshot has conflicting paths or exceeds byte limits."
            )

    def fingerprint(self) -> str:
        payload = {
            "version": 1,
            "repository_id": str(self.request.repository_id),
            "provider_repository_id": self.request.provider_repository_id,
            "repository_slug": self.request.repository_slug,
            "base_revision": self.request.base_revision,
            "root_tree_sha": self.root_tree_sha,
            "files": [
                {
                    "path": item.path,
                    "mode": item.mode.value,
                    "blob_sha": item.blob_sha,
                    "size": len(item.content),
                    "content_sha256": hashlib.sha256(item.content).hexdigest(),
                }
                for item in self.files
            ],
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseSourceLimits:
    max_files: int = 512
    max_entries: int = 2048
    max_depth: int = 16
    max_file_bytes: int = 2 * 1024**2
    max_total_bytes: int = MAX_BASE_TOTAL_BYTES
    max_requests: int = 1024
    max_response_bytes: int = 4 * 1024**2
    max_total_response_bytes: int = 64 * 1024**2
    request_timeout_seconds: float = 5
    total_timeout_seconds: float = 60

    def __post_init__(self) -> None:
        for name, maximum in (
            ("max_files", MAX_BASE_FILES),
            ("max_entries", 4096),
            ("max_depth", MAX_BASE_DEPTH),
            ("max_file_bytes", MAX_BASE_FILE_BYTES),
            ("max_total_bytes", MAX_BASE_TOTAL_BYTES),
            ("max_requests", 4096),
            ("max_response_bytes", 8 * 1024**2),
            ("max_total_response_bytes", 128 * 1024**2),
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(
                    "Base source limits require bounded positive integers."
                )
        for value in (self.request_timeout_seconds, self.total_timeout_seconds):
            if (
                type(value) not in (int, float)
                or not 0.05 <= value <= 120
                or not isfinite(value)
            ):
                raise ValueError("Base source time limits must be finite and bounded.")


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseMaterializationPolicy:
    poll_interval_seconds: float = 0.1
    operation_timeout_seconds: float = 90
    total_timeout_seconds: float = 180

    def __post_init__(self) -> None:
        for name, minimum, maximum in (
            ("poll_interval_seconds", 0.05, 0.5),
            ("operation_timeout_seconds", 0.05, 120),
            ("total_timeout_seconds", 0.05, 300),
        ):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not minimum <= value <= maximum
                or not isfinite(value)
            ):
                raise ValueError(
                    "Materialization timing requires finite bounded values."
                )


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseMaterialization:
    """Same-process file export evidence; no Git checkout, launch or recovery rights."""

    reservation: WorkspaceReservation
    source: BaseSourceRequest
    root_tree_sha: str
    manifest_digest: str
    file_count: int
    total_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.reservation, WorkspaceReservation) or not isinstance(
            self.source, BaseSourceRequest
        ):
            raise ValueError(
                "Materialization requires live reservation and source pins."
            )
        definition = self.reservation.binding.definition
        if (
            self.source.repository_id != definition.repository_id
            or self.source.provider_repository_id != definition.provider_repository_id
            or self.source.repository_slug != definition.repository_slug
            or self.source.base_revision != definition.base_revision
            or type(self.manifest_digest) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.manifest_digest) is None
            or type(self.file_count) is not int
            or not 0 <= self.file_count <= MAX_BASE_FILES
            or type(self.total_bytes) is not int
            or not 0 <= self.total_bytes <= MAX_BASE_TOTAL_BYTES
        ):
            raise ValueError("Materialization changed source pins or bounded evidence.")
        _sha(self.root_tree_sha)
