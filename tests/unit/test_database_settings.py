import pytest
from pydantic import SecretStr, ValidationError

from devrelay.bootstrap.persistence import database_url
from devrelay.config.control import ControlSettings
from devrelay.config.database import DatabaseSettings


def test_url_preserves_special_characters_without_interpolation() -> None:
    password = "test-only:@/ ?%password"
    settings = DatabaseSettings(
        host="127.0.0.1",
        database="devrelay_test",
        username="test_user",
        password=SecretStr(password),
    )

    url = database_url(settings)

    assert url.password == password
    assert password not in str(url)
    assert password not in repr(settings)


def test_database_configuration_is_optional() -> None:
    settings = ControlSettings()

    assert settings.database is None


def test_partial_database_environment_fails_without_disclosing_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEVRELAY_CONTROL_DATABASE__PASSWORD", "test-only-secret")

    with pytest.raises(ValidationError) as error:
        ControlSettings()

    assert "test-only-secret" not in str(error.value)


@pytest.mark.parametrize(
    "schema", ["public;drop schema public", "Upper", "two schemas", "x" * 64]
)
def test_schema_is_a_bounded_identifier(schema: str) -> None:
    with pytest.raises(ValidationError):
        DatabaseSettings(
            host="127.0.0.1",
            database="devrelay_test",
            username="test_user",
            password=SecretStr("test-only"),
            schema_name=schema,
        )
