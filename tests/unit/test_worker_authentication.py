from collections.abc import Callable
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import SecretStr

from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.bootstrap import security
from devrelay.bootstrap.security import worker_authenticator
from devrelay.config.control import ControlSettings

NOW = datetime(2026, 10, 10, tzinfo=UTC)
TOKEN = "test_only_worker_token_0123456789abcdefghijklmnop"


@pytest.mark.parametrize("position", [0, 2, None])
def test_authentication_checks_all_bounded_registrations(
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    position: int | None,
) -> None:
    registration = worker_configuration.workers[0]
    registrations = tuple(
        registration.model_copy(
            update={"worker_id": uuid4(), "auth_token": SecretStr(TOKEN + str(index))}
        )
        for index in range(3)
    )
    comparisons: list[str] = []
    verifier = security.token_verifier

    def tracked_verifier(token: SecretStr) -> Callable[[str], bool]:
        verify = verifier(token)

        def tracked(presented: str) -> bool:
            comparisons.append(token.get_secret_value())
            return verify(presented)

        return tracked

    monkeypatch.setattr(security, "token_verifier", tracked_verifier)
    authenticate = worker_authenticator(registrations, now=lambda: NOW)
    presented = TOKEN + str(position) if position is not None else "wrong"

    authorization = authenticate(presented)

    assert comparisons == [
        value.auth_token.get_secret_value() for value in registrations
    ]
    assert (authorization is not None) == (position is not None)
    if authorization is not None and position is not None:
        assert authorization.worker_id == registrations[position].worker_id


def test_authenticated_scope_comes_from_enrollment(
    worker_configuration: ControlSettings,
) -> None:
    registration = worker_configuration.workers[0]
    authenticate = worker_authenticator(worker_configuration.workers, now=lambda: NOW)

    authorization = authenticate(TOKEN)

    assert authorization == WorkerAuthorization(
        worker_id=registration.worker_id,
        allowed_repository_ids=registration.allowed_repository_ids,
        credential_expires_at=registration.expires_at,
    )
    assert TOKEN not in repr(authorization)
    assert TOKEN not in repr(authenticate)


@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_credential_expiry_is_checked_on_each_authentication(
    worker_configuration: ControlSettings, offset: int
) -> None:
    expiry = worker_configuration.workers[0].expires_at
    current_time = expiry + timedelta(seconds=offset)
    authenticate = worker_authenticator(
        worker_configuration.workers, now=lambda: current_time
    )

    authorization = authenticate(TOKEN)

    assert (authorization is not None) == (offset < 0)


def test_disabled_worker_cannot_authenticate(
    worker_configuration: ControlSettings,
) -> None:
    disabled = worker_configuration.workers[0].model_copy(update={"enabled": False})
    authenticate = worker_authenticator((disabled,), now=lambda: NOW)

    authorization = authenticate(TOKEN)

    assert authorization is None


@pytest.mark.parametrize(
    "token", ["", "wrong", TOKEN.upper(), TOKEN + "x", "\N{SNOWMAN}"]
)
def test_unknown_credentials_return_no_authority(
    worker_configuration: ControlSettings, token: str
) -> None:
    authenticate = worker_authenticator(worker_configuration.workers, now=lambda: NOW)

    authorization = authenticate(token)

    assert authorization is None


def test_naive_authentication_clock_is_an_internal_failure(
    worker_configuration: ControlSettings,
) -> None:
    authenticate = worker_authenticator(
        worker_configuration.workers, now=lambda: datetime(2026, 10, 10)
    )

    with pytest.raises(ValueError, match="timezone-aware"):
        authenticate(TOKEN)


def test_worker_authority_is_immutable(worker_configuration: ControlSettings) -> None:
    registration = worker_configuration.workers[0]
    authorization = WorkerAuthorization(
        worker_id=registration.worker_id,
        allowed_repository_ids=registration.allowed_repository_ids,
        credential_expires_at=registration.expires_at,
    )

    with pytest.raises(FrozenInstanceError):
        authorization.worker_id = uuid4()  # type: ignore[misc]  # Deliberate mutation attempt.


@pytest.mark.parametrize("field", ["naive_expiry", "non_utc", "empty_scope"])
def test_worker_authority_rejects_invalid_scope_or_time(
    worker_configuration: ControlSettings, field: str
) -> None:
    registration = worker_configuration.workers[0]
    authorization = WorkerAuthorization(
        worker_id=registration.worker_id,
        allowed_repository_ids=registration.allowed_repository_ids,
        credential_expires_at=registration.expires_at,
    )
    expiry = registration.expires_at
    scope = registration.allowed_repository_ids
    if field == "naive_expiry":
        expiry = datetime(2026, 10, 10)
    elif field == "non_utc":
        expiry = datetime(2026, 10, 10, tzinfo=timezone(timedelta(hours=3)))
    else:
        scope = frozenset()

    with pytest.raises(ValueError):
        replace(
            authorization, credential_expires_at=expiry, allowed_repository_ids=scope
        )
