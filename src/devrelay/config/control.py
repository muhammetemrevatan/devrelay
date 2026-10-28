import json
from enum import StrEnum

from pydantic import Field, IPvAnyAddress, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from devrelay.config.broker_git import BrokerGitSettings
from devrelay.config.coding_publication import CodingPublicationSettings
from devrelay.config.database import DatabaseSettings
from devrelay.config.github import GitHubSettings
from devrelay.config.jira import JiraSettings
from devrelay.config.job_submission import JobSubmissionSettings
from devrelay.config.logging import LogLevel
from devrelay.config.repositories import RepositorySettings
from devrelay.config.task_import import TaskImportSettings
from devrelay.config.worker_authentication import WorkerRegistrationSettings
from devrelay.config.worker_base_sources import WorkerBaseSourcesSettings
from devrelay.config.worker_claims import WorkerClaimsSettings
from devrelay.config.worker_coding_results import WorkerCodingResultsSettings


class RuntimeEnvironment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class ControlSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DEVRELAY_CONTROL_",
        extra="ignore",
        frozen=True,
        hide_input_in_errors=True,
        env_nested_delimiter="__",
    )

    environment: RuntimeEnvironment = RuntimeEnvironment.DEVELOPMENT
    host: IPvAnyAddress = Field(default="127.0.0.1", validate_default=True)
    port: int = Field(default=8000, ge=1, le=65535)
    log_level: LogLevel = LogLevel.INFO
    jira: JiraSettings | None = None
    github: GitHubSettings | None = None
    database: DatabaseSettings | None = None
    task_import: TaskImportSettings | None = None
    job_submission: JobSubmissionSettings | None = None
    repositories: tuple[RepositorySettings, ...] = Field(default=(), max_length=32)
    workers: tuple[WorkerRegistrationSettings, ...] = Field(default=(), max_length=32)
    worker_claims: WorkerClaimsSettings | None = None
    worker_base_sources: WorkerBaseSourcesSettings | None = None
    worker_coding_results: WorkerCodingResultsSettings | None = None
    coding_publication: CodingPublicationSettings | None = None
    broker_git: BrokerGitSettings | None = None

    @model_validator(mode="after")
    def require_publication_scope(self) -> ControlSettings:
        publication = self.coding_publication
        if publication is None:
            return self
        if self.worker_coding_results is None or self.github is None:
            raise ValueError(
                "Publication requires explicitly enabled result coordination."
            )
        enabled = {repo.repository_id for repo in self.repositories if repo.enabled}
        if not publication.allowed_repository_ids <= enabled:
            raise ValueError(
                "Publication scope must contain only enabled repository policies."
            )
        other_secrets = (
            self.github.api_token,
            self.jira.api_token if self.jira else None,
            self.database.password if self.database else None,
            self.task_import.auth_token if self.task_import else None,
            self.job_submission.auth_token if self.job_submission else None,
            *(worker.auth_token for worker in self.workers),
        )
        if any(
            publication.api_token == secret
            for secret in other_secrets
            if secret is not None
        ):
            raise ValueError("Publication requires a separate server credential.")
        return self

    @model_validator(mode="after")
    def require_base_source_claims(self) -> ControlSettings:
        if self.worker_coding_results is not None and self.worker_claims is None:
            raise ValueError("Worker coding results require explicitly enabled claims.")
        if self.broker_git is not None and self.worker_base_sources is None:
            raise ValueError("Broker Git requires explicitly enabled base delivery.")
        if self.worker_base_sources is not None and self.worker_claims is None:
            raise ValueError("Worker base sources require explicitly enabled claims.")
        return self

    @model_validator(mode="after")
    def require_claim_integrations(self) -> ControlSettings:
        if self.worker_claims is not None and (
            not self.workers
            or self.database is None
            or self.jira is None
            or self.github is None
            or not any(worker.enabled for worker in self.workers)
        ):
            raise ValueError("Worker claims require integrations and enrolled workers.")
        if self.worker_claims is not None and any(
            len(
                json.dumps(
                    [check.model_dump(mode="json") for check in repository.checks],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            > 512 * 1024
            for repository in self.repositories
            if repository.enabled
        ):
            raise ValueError(
                "Worker claim check policy exceeds assignment delivery bounds."
            )
        return self

    @model_validator(mode="after")
    def require_worker_scope(self) -> ControlSettings:
        if not self.workers:
            return self
        if self.github is None:
            raise ValueError(
                "Worker registration requires configured GitHub identities."
            )
        enabled = {repo.repository_id for repo in self.repositories if repo.enabled}
        if any(not worker.allowed_repository_ids <= enabled for worker in self.workers):
            raise ValueError(
                "Worker scope must contain only enabled repository policies."
            )
        if len({worker.worker_id for worker in self.workers}) != len(self.workers):
            raise ValueError("Worker registration requires unique identities.")
        tokens = {worker.auth_token.get_secret_value() for worker in self.workers}
        if len(tokens) != len(self.workers):
            raise ValueError("Worker registration requires separate credentials.")
        other_secrets = (
            self.jira.api_token if self.jira else None,
            self.database.password if self.database else None,
            self.github.api_token,
            self.task_import.auth_token if self.task_import else None,
            self.job_submission.auth_token if self.job_submission else None,
        )
        if any(
            secret.get_secret_value() in tokens
            for secret in other_secrets
            if secret is not None
        ):
            raise ValueError("Worker authentication requires a separate credential.")
        return self

    @model_validator(mode="after")
    def require_submission_integrations(self) -> ControlSettings:
        submission = self.job_submission
        if submission is None:
            return self
        if self.jira is None or self.database is None or self.github is None:
            raise ValueError(
                "Job submission requires complete Jira, database and GitHub settings."
            )
        enabled = {repo.repository_id for repo in self.repositories if repo.enabled}
        if not submission.allowed_repository_ids <= enabled:
            raise ValueError(
                "Submission scope must contain only enabled repository policies."
            )
        other_secrets = (
            self.jira.api_token,
            self.database.password,
            self.github.api_token,
            self.task_import.auth_token if self.task_import else None,
        )
        if any(
            submission.auth_token == secret
            for secret in other_secrets
            if secret is not None
        ):
            raise ValueError(
                "Submission authentication requires a separate credential."
            )
        return self

    @model_validator(mode="after")
    def require_github_scope(self) -> ControlSettings:
        if self.github is not None:
            if {item.repository_id for item in self.github.repository_ids} != {
                item.repository_id for item in self.repositories
            }:
                raise ValueError(
                    "GitHub bindings must match configured repository policy."
                )
            other_secrets = (
                self.jira.api_token if self.jira else None,
                self.database.password if self.database else None,
                self.task_import.auth_token if self.task_import else None,
            )
            if any(
                self.github.api_token == secret
                for secret in other_secrets
                if secret is not None
            ):
                raise ValueError("GitHub observation requires a separate credential.")
        return self

    @model_validator(mode="after")
    def require_repository_scope(self) -> ControlSettings:
        if self.repositories:
            if self.jira is None or self.database is None:
                raise ValueError(
                    "Repository policy requires Jira and database settings."
                )
            if len({repo.repository_id for repo in self.repositories}) != len(
                self.repositories
            ) or len({repo.slug.casefold() for repo in self.repositories}) != len(
                self.repositories
            ):
                raise ValueError("Repository identities and slugs must be unique.")
            if any(
                not repo.allowed_project_keys <= self.jira.allowed_project_keys
                for repo in self.repositories
            ):
                raise ValueError(
                    "Repository source scope must be inside configured Jira scope."
                )
        return self

    @model_validator(mode="after")
    def require_import_integrations(self) -> ControlSettings:
        if self.task_import is not None and (
            self.jira is None or self.database is None
        ):
            raise ValueError(
                "Task import requires complete Jira and database settings."
            )
        if self.task_import is not None and self.jira is not None:
            if self.task_import.auth_token == self.jira.api_token:
                raise ValueError(
                    "Import authentication must use a separate credential."
                )
        if self.task_import is not None and self.database is not None:
            if self.task_import.auth_token == self.database.password:
                raise ValueError(
                    "Import authentication must use a separate credential."
                )
        return self
