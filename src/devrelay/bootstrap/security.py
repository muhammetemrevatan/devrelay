from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from secrets import compare_digest

from pydantic import SecretStr

from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.config.worker_authentication import WorkerRegistrationSettings


def token_verifier(token: SecretStr) -> Callable[[str], bool]:
    expected = sha256(token.get_secret_value().encode("utf-8")).digest()

    def verify(presented: str) -> bool:
        return compare_digest(expected, sha256(presented.encode("utf-8")).digest())

    return verify


def worker_authenticator(
    registrations: tuple[WorkerRegistrationSettings, ...],
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Callable[[str], WorkerAuthorization | None]:
    entries = tuple(
        (
            token_verifier(registration.auth_token),
            WorkerAuthorization(
                worker_id=registration.worker_id,
                allowed_repository_ids=registration.allowed_repository_ids,
                credential_expires_at=registration.expires_at,
            ),
            registration.enabled,
        )
        for registration in registrations
    )

    def authenticate(presented: str) -> WorkerAuthorization | None:
        authenticated = None
        instant = now()
        if instant.utcoffset() is None:
            raise ValueError("Worker authentication clock must be timezone-aware.")
        for verify, authorization, enabled in entries:
            matches = verify(presented)
            if matches and enabled and instant < authorization.credential_expires_at:
                authenticated = authorization
        return authenticated

    return authenticate
