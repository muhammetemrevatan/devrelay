import base64
from uuid import uuid4

import pytest
from pydantic import SecretStr, ValidationError

from devrelay.application.contracts.claims import InvalidLeaseProof, LeaseProof
from devrelay.bootstrap.claims import _claim_configuration
from devrelay.config.coding_publication import CodingPublicationSettings
from devrelay.config.control import ControlSettings

TOKEN = base64.urlsafe_b64encode(b"p" * 32).decode().rstrip("=")


def enabled(original: ControlSettings) -> ControlSettings:
    values = original.model_dump()
    values["worker_claims"] = {}
    values["worker_coding_results"] = {}
    values["coding_publication"] = {
        "api_token": TOKEN,
        "allowed_repository_ids": frozenset({original.repositories[0].repository_id}),
    }
    return ControlSettings.model_validate(values)


def test_publication_is_separately_disabled_by_default() -> None:
    settings = ControlSettings()

    assert settings.coding_publication is None


@pytest.mark.parametrize("missing", ["worker_claims", "worker_coding_results"])
def test_publication_cannot_bypass_result_coordination(
    worker_configuration: ControlSettings, missing: str
) -> None:
    values = enabled(worker_configuration).model_dump()
    values[missing] = None

    with pytest.raises(ValidationError):
        ControlSettings.model_validate(values)


@pytest.mark.parametrize("credential", ["github", "jira", "database", "worker"])
def test_publication_requires_a_different_server_credential(
    worker_configuration: ControlSettings, credential: str
) -> None:
    settings = enabled(worker_configuration)
    assert (
        settings.github is not None
        and settings.jira is not None
        and settings.database is not None
    )
    assert settings.coding_publication is not None
    secrets = {
        "github": settings.github.api_token,
        "jira": settings.jira.api_token,
        "database": settings.database.password,
        "worker": settings.workers[0].auth_token,
    }
    values = settings.model_dump()
    values["coding_publication"] = settings.coding_publication.model_copy(
        update={"api_token": secrets[credential]}
    )

    with pytest.raises(ValidationError, match="separate server credential"):
        ControlSettings.model_validate(values)


@pytest.mark.parametrize("disabled", [False, True])
def test_publication_scope_requires_enabled_bound_repositories(
    worker_configuration: ControlSettings, disabled: bool
) -> None:
    settings = enabled(worker_configuration)
    assert settings.coding_publication is not None
    values = settings.model_dump()
    if disabled:
        values["repositories"] = (
            settings.repositories[0].model_copy(update={"enabled": False}),
        )
    else:
        values["coding_publication"] = settings.coding_publication.model_copy(
            update={"allowed_repository_ids": frozenset({uuid4()})}
        )

    with pytest.raises(ValidationError):
        ControlSettings.model_validate(values)


@pytest.mark.parametrize(
    "change",
    [
        {"api_token": " "},
        {"api_token": "token\nvalue"},
        {"api_token": "nonascii-ş"},
        {"max_writes": 2},
        {"max_writes": True},
        {"max_writes": 513},
        {"max_requests": 4097},
        {"max_request_bytes": 33 * 1024**2},
        {"max_response_bytes": 5 * 1024**2},
        {"max_total_response_bytes": 65 * 1024**2},
        {"total_timeout_seconds": float("inf")},
        {"request_timeout_seconds": float("nan")},
        {"dispatch_margin_seconds": 0},
        {"total_timeout_seconds": 2, "request_timeout_seconds": 3},
        {"unknown": True},
    ],
)
def test_publication_budgets_and_credential_shape_fail_closed(
    change: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "api_token": TOKEN,
        "allowed_repository_ids": frozenset({uuid4()}),
        **change,
    }

    with pytest.raises(ValidationError):
        CodingPublicationSettings.model_validate(values)


def test_server_publication_secret_cannot_be_reused_as_lease_proof(
    worker_configuration: ControlSettings,
) -> None:
    settings = enabled(worker_configuration)
    _, guard = _claim_configuration(settings)

    with pytest.raises(InvalidLeaseProof):
        guard(LeaseProof(TOKEN))


def test_publication_settings_do_not_expose_token_in_repr() -> None:
    value = CodingPublicationSettings(
        api_token=SecretStr(TOKEN), allowed_repository_ids=frozenset({uuid4()})
    )

    assert TOKEN not in repr(value)
