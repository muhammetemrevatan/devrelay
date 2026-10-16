import hashlib
import re

MAX_GIT_COMMIT_BYTES = 256 * 1024


def validate_git_commit(
    raw_commit: bytes, expected_sha: str, root_tree_sha: str
) -> None:
    """Verify original object bytes, not a reconstructed commit or signature."""
    if (
        type(raw_commit) is not bytes
        or not 1 <= len(raw_commit) <= MAX_GIT_COMMIT_BYTES
        or type(expected_sha) is not str
        or re.fullmatch(r"[0-9a-f]{40}", expected_sha) is None
        or type(root_tree_sha) is not str
        or re.fullmatch(r"[0-9a-f]{40}", root_tree_sha) is None
    ):
        raise ValueError("Git base requires bounded original commit bytes.")
    actual = hashlib.sha1(
        b"commit " + str(len(raw_commit)).encode("ascii") + b"\x00" + raw_commit,
        usedforsecurity=False,
    ).hexdigest()
    header, delimiter, _ = raw_commit.partition(b"\n\n")
    lines = header.split(b"\n")
    if (
        actual != expected_sha
        or not delimiter
        or b"\x00" in header
        or lines[0] != b"tree " + root_tree_sha.encode("ascii")
        or sum(line.startswith(b"tree ") for line in lines) != 1
        or sum(line.startswith(b"author ") for line in lines) != 1
        or sum(line.startswith(b"committer ") for line in lines) != 1
    ):
        raise ValueError("Git commit differs from accepted base or root tree.")
    for line in lines[1:]:
        if not line or (not line.startswith(b" ") and b" " not in line):
            raise ValueError("Git commit headers are malformed.")
        if (
            line.startswith(b"parent ")
            and re.fullmatch(b"parent [0-9a-f]{40}", line) is None
        ):
            raise ValueError("Git commit parent object identity is malformed.")
