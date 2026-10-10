import re

from pydantic import SecretStr


def validate_bearer_token(value: SecretStr) -> SecretStr:
    token = value.get_secret_value()
    if re.fullmatch(r"[A-Za-z0-9_-]{43,128}", token) is None or token.startswith(
        "replace-with-"
    ):
        raise ValueError("Authentication requires a generated URL-safe token.")
    return value
