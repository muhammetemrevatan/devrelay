from collections.abc import Callable
from hashlib import sha256
from secrets import compare_digest

from pydantic import SecretStr


def token_verifier(token: SecretStr) -> Callable[[str], bool]:
    expected = sha256(token.get_secret_value().encode("utf-8")).digest()

    def verify(presented: str) -> bool:
        return compare_digest(expected, sha256(presented.encode("utf-8")).digest())

    return verify
