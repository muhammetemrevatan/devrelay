from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from devrelay.application.contracts.tasks import SavedTaskSnapshot, TaskRead
from devrelay.application.ports.task_persistence import TaskUnitOfWork
from devrelay.application.ports.task_source import TaskSource


class RequirementGap(StrEnum):
    READINESS_LABEL = "readiness_label"
    DESCRIPTION = "description"
    ACCEPTANCE_CRITERIA = "acceptance_criteria"


class RequirementsStatus(StrEnum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"


class TaskImportRejected(Exception):
    """The task lookup or returned source identity violates import scope."""


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskImportPolicy:
    provider: str
    site_id: str
    allowed_scope_keys: frozenset[str]
    readiness_label: str = "agent-ready"

    def __post_init__(self) -> None:
        if not self.provider.strip() or not self.site_id.strip():
            raise ValueError(
                "Task import requires explicit provider and site identity."
            )
        if not self.allowed_scope_keys or any(
            not key.strip() for key in self.allowed_scope_keys
        ):
            raise ValueError("Task import requires explicit nonempty scope keys.")
        if not self.readiness_label.strip():
            raise ValueError("Task import requires an explicit readiness label.")

    def check_scope(self, task: TaskRead) -> None:
        identity = task.snapshot.identity
        if (
            identity.provider != self.provider
            or identity.site_id != self.site_id
            or task.scope_key not in self.allowed_scope_keys
        ):
            raise TaskImportRejected("Task is outside the configured import scope.")


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskImportResult:
    saved: SavedTaskSnapshot
    observed_labels: frozenset[str]
    requirements_status: RequirementsStatus
    missing_requirements: tuple[RequirementGap, ...]


def missing_requirements(
    observed: TaskRead, readiness_label: str
) -> tuple[RequirementGap, ...]:
    """Shared completeness rule; this observation never grants execution authority."""
    gaps: list[RequirementGap] = []
    if readiness_label not in observed.labels:
        gaps.append(RequirementGap.READINESS_LABEL)
    if not observed.snapshot.description.strip():
        gaps.append(RequirementGap.DESCRIPTION)
    if not observed.snapshot.acceptance_criteria:
        gaps.append(RequirementGap.ACCEPTANCE_CRITERIA)
    return tuple(gaps)


class ImportTask:
    def __init__(
        self,
        source: TaskSource,
        unit_of_work: Callable[[], TaskUnitOfWork],
        policy: TaskImportPolicy,
    ) -> None:
        self._source = source
        self._unit_of_work = unit_of_work
        self._policy = policy

    async def execute(self, display_key: str) -> TaskImportResult:
        if (
            not display_key.strip()
            or len(display_key) > 128
            or display_key != display_key.strip()
        ):
            raise TaskImportRejected(
                "Task lookup requires a bounded nonblank display key."
            )
        observed = await self._source.read_by_key(display_key)
        self._policy.check_scope(observed)
        gaps = missing_requirements(observed, self._policy.readiness_label)
        async with self._unit_of_work() as unit:
            saved = await unit.tasks.save_snapshot(observed.snapshot)
            await unit.commit()
        return TaskImportResult(
            saved=saved,
            observed_labels=observed.labels,
            requirements_status=(
                RequirementsStatus.INCOMPLETE if gaps else RequirementsStatus.COMPLETE
            ),
            missing_requirements=gaps,
        )
