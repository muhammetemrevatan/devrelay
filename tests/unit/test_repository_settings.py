import json
from uuid import uuid4

import pytest
from pydantic import SecretStr, ValidationError

from devrelay.bootstrap.jobs import repository_policy
from devrelay.config.control import ControlSettings
from devrelay.config.database import DatabaseSettings
from devrelay.config.github import GitHubRepositoryId, GitHubSettings
from devrelay.config.jira import JiraSettings
from devrelay.config.repositories import CheckSettings, RepositorySettings


def check() -> CheckSettings:
    return CheckSettings(
        id="pytest", argv=("uv", "run", "--locked", "pytest"), timeout_seconds=300
    )


def repository() -> RepositorySettings:
    return RepositorySettings(
        repository_id=uuid4(),
        slug="example/devrelay",
        target_branch="main",
        coding_branch_prefix="devrelay/jobs",
        policy_version="v1",
        allowed_project_keys=frozenset({"DRY"}),
        checks=(check(),),
        max_repair_cycles=2,
    )


def jira() -> JiraSettings:
    return JiraSettings(
        site_url="https://example.atlassian.net",
        cloud_id=uuid4(),
        email="reader@example.test",
        api_token=SecretStr("test-only-jira-token"),
        allowed_project_keys=frozenset({"DRY"}),
    )


def database() -> DatabaseSettings:
    return DatabaseSettings(
        host="127.0.0.1",
        database="devrelay_test",
        username="devrelay_test",
        password=SecretStr("test-only-database-token"),
    )


def test_configuration_defaults_to_no_repository_admission() -> None:
    settings = ControlSettings()

    assert settings.repositories == ()


def test_repository_policy_defaults_to_disabled_without_credentials() -> None:
    repo, source = repository(), jira()

    policy = repository_policy(repo, source)

    assert not repo.enabled
    assert not policy.enabled
    assert policy.checks[0].argv == repo.checks[0].argv
    assert policy.checks[0].working_directory == "."
    assert not hasattr(policy, "api_token")


@pytest.mark.parametrize(
    "branch",
    [
        "-main",
        "refs/heads/main",
        "main..next",
        "main.lock",
        ".hidden/main",
        "main//next",
        "main/",
        "/main",
        "main.",
        "@",
        "@{-1}",
        "main~1",
        "main^",
        "main:next",
        "main?",
        "main*",
        "main[1",
        "main\\next",
        "main next",
        "main\n",
        "main\x7f",
    ],
)
def test_nonliteral_git_branches_are_rejected(branch: str) -> None:
    data = {**repository().model_dump(), "target_branch": branch}

    with pytest.raises(ValidationError):
        RepositorySettings.model_validate(data)


@pytest.mark.parametrize("branch", ["main", "release/v1.0", "feature_task", "hotfix-1"])
def test_literal_branches_are_accepted(branch: str) -> None:
    data = {**repository().model_dump(), "target_branch": branch}

    settings = RepositorySettings.model_validate(data)

    assert settings.target_branch == branch


@pytest.mark.parametrize(
    "directory",
    [
        "../tests",
        "tests/../src",
        "/tmp",
        "C:/tmp",
        "tests\\unit",
        "tests//unit",
        "tests/.",
    ],
)
def test_check_directories_cannot_be_absolute_or_traverse(directory: str) -> None:
    data = {**check().model_dump(), "working_directory": directory}

    with pytest.raises(ValidationError):
        CheckSettings.model_validate(data)


@pytest.mark.parametrize(
    "argv",
    [
        (),
        ("",),
        (" uv",),
        ("uv", "x\x00y"),
        ("uv", "x\ny"),
        ("uv", "x" * 4097),
        ("uv",) * 65,
    ],
)
def test_invalid_or_unbounded_check_argv_is_rejected(argv: tuple[str, ...]) -> None:
    data = {**check().model_dump(), "argv": argv}

    with pytest.raises(ValidationError):
        CheckSettings.model_validate(data)


@pytest.mark.parametrize("timeout", [0, 3601, True])
def test_check_timeout_must_be_explicit_bounded_integer(timeout: int) -> None:
    data = {**check().model_dump(), "timeout_seconds": timeout}

    with pytest.raises(ValidationError):
        CheckSettings.model_validate(data)


@pytest.mark.parametrize("reason", ["duplicate_checks", "unbounded_budget"])
def test_duplicate_check_ids_and_unbounded_budget_are_rejected(reason: str) -> None:
    data = repository().model_dump()
    if reason == "duplicate_checks":
        data["checks"] = (check(), check())
    else:
        data["max_repair_cycles"] = 6

    with pytest.raises(ValidationError):
        RepositorySettings.model_validate(data)


def test_repositories_require_complete_integration_settings() -> None:
    repo = repository()

    with pytest.raises(ValidationError):
        ControlSettings(repositories=(repo,))


@pytest.mark.parametrize("reason", ["duplicate_id", "duplicate_slug", "foreign_scope"])
def test_control_validates_unique_repository_and_source_scope(reason: str) -> None:
    repo = repository()
    if reason == "duplicate_id":
        repos: tuple[RepositorySettings, ...] = (repo, repo)
    elif reason == "duplicate_slug":
        repos = (
            repo,
            repo.model_copy(
                update={"repository_id": uuid4(), "slug": repo.slug.upper()}
            ),
        )
    else:
        repos = (
            repo.model_copy(update={"allowed_project_keys": frozenset({"OTHER"})}),
        )
    source, storage = jira(), database()

    with pytest.raises(ValidationError):
        ControlSettings(jira=source, database=storage, repositories=repos)


def test_repository_policy_rejects_foreign_source_scope() -> None:
    repo = repository().model_copy(
        update={"allowed_project_keys": frozenset({"OTHER"})}
    )
    source = jira()

    with pytest.raises(ValueError):
        repository_policy(repo, source)


def test_repository_json_environment_is_loaded_without_enabling_an_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = repository()
    monkeypatch.setenv(
        "DEVRELAY_CONTROL_REPOSITORIES", json.dumps([repo.model_dump(mode="json")])
    )
    settings = ControlSettings(jira=jira(), database=database())
    assert settings.repositories == (repo,)
    assert settings.task_import is None


def github(repo: RepositorySettings) -> GitHubSettings:
    return GitHubSettings(
        api_token=SecretStr("test-only-github-token"),
        repository_ids=(
            GitHubRepositoryId(repository_id=repo.repository_id, github_id=123),
        ),
    )


def test_github_configuration_is_optional() -> None:
    settings = ControlSettings()

    assert settings.github is None


def test_matching_github_identity_does_not_enable_import_api() -> None:
    repo = repository()
    gh, source, storage = github(repo), jira(), database()

    configuration = ControlSettings(
        jira=source, database=storage, repositories=(repo,), github=gh
    )

    assert configuration.github == gh
    assert configuration.task_import is None


@pytest.mark.parametrize(
    "reason", ["foreign_repository", "jira_secret", "database_secret"]
)
def test_github_requires_matching_identity_and_separate_credentials(
    reason: str,
) -> None:
    repo = repository()
    gh, source, storage = github(repo), jira(), database()
    if reason == "foreign_repository":
        invalid = gh.model_copy(
            update={
                "repository_ids": (
                    GitHubRepositoryId(repository_id=uuid4(), github_id=123),
                )
            }
        )
    elif reason == "jira_secret":
        invalid = gh.model_copy(update={"api_token": source.api_token})
    else:
        invalid = gh.model_copy(update={"api_token": storage.password})

    with pytest.raises(ValidationError):
        ControlSettings(
            jira=source, database=storage, repositories=(repo,), github=invalid
        )


def test_duplicate_github_repository_bindings_are_rejected() -> None:
    gh = github(repository())
    bindings = gh.repository_ids * 2

    with pytest.raises(ValidationError):
        GitHubSettings(api_token=gh.api_token, repository_ids=bindings)
