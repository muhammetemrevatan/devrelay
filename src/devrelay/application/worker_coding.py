"""One live coding/check/repair sequence; recovery metadata cannot launch it."""

import asyncio
import unicodedata
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial
from math import isfinite
from typing import Protocol, TypeVar, cast
from uuid import uuid4

from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.candidates import CandidateCommitSpec
from devrelay.application.contracts.coding_results import (
    MAX_CHECK_DIAGNOSTIC_BYTES,
    CodingCheckEvidence,
    CodingCycleEvidence,
    SettledProcessEvidence,
    SuccessfulCodingResult,
    check_policy_digest,
)
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.executions import WorkerExecutionIntent
from devrelay.application.contracts.frozen_candidates import FrozenCandidate
from devrelay.application.contracts.git_sources import GitWorkspacePreparation
from devrelay.application.contracts.processes import ExecutionProcessBinding
from devrelay.application.contracts.submissions import CheckPolicy
from devrelay.application.contracts.trusted_processes import TrustedProcessResult
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.materialize_base import _consume_completion, _wait_for_tick
from devrelay.application.ports.candidate_workspace import CandidateWorkspacePort
from devrelay.application.worker_executions import ExecutionAuthorityGuard

_Value = TypeVar("_Value")
_ERROR = "The original live coding execution is unavailable."


class CodingRunUnavailable(Exception):
    """No successful result; reservation and uncertain resources remain retained."""


class CodingRuntime(Protocol):
    """Trusted isolated runtime, not the harmless host-command runner.

    Each invocation must independently check current authority and its actual
    isolation boundary immediately before process admission. Check commands come
    only from the accepted fixed policy and use enforced read-only candidate
    access. A settled result reports actual exit, complete pipes and fresh empty
    native process accounting; model messages cannot supply those observations.
    The owning bootstrap scope must stop and drain native work after sealing.
    """

    @property
    def binding(self) -> ExecutionProcessBinding: ...

    def seal(self) -> None: ...

    async def code(
        self,
        delivery: AssignmentDelivery,
        editable: EditableWorkspace,
        *,
        cycle: int,
        previous: CodingCycleEvidence | None,
    ) -> TrustedProcessResult: ...

    async def check(
        self, candidate: FrozenCandidate, check: CheckPolicy
    ) -> TrustedProcessResult: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class CodingRunPolicy:
    maximum_runtime_seconds: float = 3600
    coding_timeout_seconds: float = 900
    workspace_timeout_seconds: float = 90
    monitor_interval_seconds: float = 0.05
    maximum_observations: int = 100_000

    def __post_init__(self) -> None:
        for name, lower, upper in (
            ("maximum_runtime_seconds", 0.1, 21_600),
            ("coding_timeout_seconds", 0.1, 3600),
            ("workspace_timeout_seconds", 0.1, 300),
            ("monitor_interval_seconds", 0.01, 1),
        ):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not isfinite(value)
                or not lower <= value <= upper
            ):
                raise ValueError("Coding execution requires finite time budgets.")
        if (
            type(self.maximum_observations) is not int
            or not 1 <= self.maximum_observations <= 100_000
        ):
            raise ValueError("Coding execution requires finite observation bounds.")


_DEFAULT_POLICY = CodingRunPolicy()


def _diagnostic(result: TrustedProcessResult) -> str:
    raw = (result.stderr or result.stdout)[-MAX_CHECK_DIAGNOSTIC_BYTES:]
    text = "".join(
        char
        for char in raw.decode("utf-8", "replace")
        if char in "\n\t" or not unicodedata.category(char).startswith("C")
    )
    return text.encode("utf-8")[-MAX_CHECK_DIAGNOSTIC_BYTES:].decode("utf-8", "ignore")


class RunAssignedCoding:
    """Consume original retained capabilities once, returning historical evidence.

    This does not submit a result, release a reservation, recover a launch or
    certify native isolation. Failed/unknown/cancelled operations seal the
    captured originals before wrapper cancellation. Their bootstrap owners must
    retain pending native operations through actual stop and resource cleanup.
    """

    def __init__(
        self,
        workspace: CandidateWorkspacePort,
        runtime: CodingRuntime,
        guard: ExecutionAuthorityGuard,
        *,
        monotonic_clock: Callable[[], float],
        policy: CodingRunPolicy = _DEFAULT_POLICY,
    ) -> None:
        if not callable(monotonic_clock) or not isinstance(policy, CodingRunPolicy):
            raise ValueError("Coding requires an explicit clock and bounded policy.")
        policy.__post_init__()
        self._workspace, self._runtime, self._guard = workspace, runtime, guard
        self._clock, self._policy = monotonic_clock, policy
        self._expected = workspace.binding
        self._runtime_binding = runtime.binding
        if not isinstance(self._expected, WorkspaceBinding) or not isinstance(
            self._runtime_binding, ExecutionProcessBinding
        ):
            raise CodingRunUnavailable(_ERROR)
        self._seal_workspace, self._seal_runtime = workspace.seal, runtime.seal
        self._used = False
        self._pending: asyncio.Future[object] | None = None
        self._preparation: GitWorkspacePreparation | None = None
        self._editable: EditableWorkspace | None = None
        self._last_clock: float | None = None
        self._deadline: float | None = None
        self._loop_deadline: float | None = None
        self._observations = 0

    def _fence(self) -> None:
        preparation, editable = self._preparation, self._editable
        if (
            preparation is None
            or editable is None
            or self._workspace.binding != self._expected
            or self._runtime.binding != self._runtime_binding
            or self._runtime_binding != self._expected.execution
            or self._workspace.git_preparation is not preparation
            or self._workspace.candidate_capture_preparation is not preparation
            or self._workspace.editable is not editable
            or self._workspace.materialization is not preparation.materialization
            or preparation.materialization is not editable.materialization
            or self._workspace.reservation is not editable.materialization.reservation
            or self._workspace.location != editable.location
            or editable.materialization.reservation.binding != self._expected
        ):
            raise CodingRunUnavailable(_ERROR)

    def _now(self) -> float:
        value = self._clock()
        if (
            type(value) not in (int, float)
            or not isfinite(value)
            or value < 0
            or (self._last_clock is not None and value < self._last_clock)
        ):
            raise CodingRunUnavailable("The coding execution clock is unavailable.")
        self._last_clock = value
        return value

    def _require(self) -> float:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError
        self._fence()
        self._guard.require_current_assignment(self._expected.execution.reference)
        now = self._now()
        if self._deadline is not None and now >= self._deadline:
            raise CodingRunUnavailable("The coding execution exceeded its time budget.")
        if (
            self._loop_deadline is not None
            and asyncio.get_running_loop().time() >= self._loop_deadline
        ):
            raise CodingRunUnavailable("The coding execution exceeded its wait budget.")
        self._fence()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError
        return now

    async def _await(
        self, operation: Callable[[], Awaitable[_Value]], timeout: float
    ) -> _Value:
        started = self._require()
        loop_deadline = asyncio.get_running_loop().time() + timeout
        deadline = started + timeout
        task = asyncio.ensure_future(operation())
        self._pending = cast(asyncio.Future[object], task)
        self._pending.add_done_callback(_consume_completion)
        while True:
            now = self._require()
            self._observations += 1
            if self._observations > self._policy.maximum_observations:
                raise CodingRunUnavailable("Coding observation bounds were exhausted.")
            remaining = min(
                deadline - now, loop_deadline - asyncio.get_running_loop().time()
            )
            if remaining <= 0:
                raise CodingRunUnavailable(
                    "A coding operation exceeded its time budget."
                )
            if task.done():
                value = task.result()
                self._pending = None
                self._require()
                return value
            await _wait_for_tick(
                cast(asyncio.Future[object], task),
                min(remaining, self._policy.monitor_interval_seconds),
            )

    def _validate_input(
        self,
        delivery: AssignmentDelivery,
        execution: WorkerExecutionIntent,
        preparation: GitWorkspacePreparation,
        editable: EditableWorkspace,
        commit: CandidateCommitSpec,
    ) -> tuple[str, str]:
        delivery.__post_init__()
        delivery.outcome.__post_init__()
        delivery.definition.__post_init__()
        self._expected.__post_init__()
        execution.__post_init__()
        execution.acknowledgement.__post_init__()
        execution.policy.__post_init__()
        preparation.__post_init__()
        editable.__post_init__()
        commit.__post_init__()
        reference = delivery.outcome.assignment
        digest = delivery.definition.fingerprint()
        if (
            execution != self._expected.execution.intent
            or reference != self._expected.execution.reference
            or delivery.definition != self._expected.definition
            or execution.acknowledgement.claim_request_id
            != delivery.outcome.claim_request_id
            or execution.acknowledgement.definition_digest != digest
            or execution.acknowledgement.snapshot_id != delivery.definition.snapshot.id
            or execution.acknowledgement.base_revision
            != delivery.definition.base_revision
            or preparation.branch != delivery.definition.authorized_branch
        ):
            raise CodingRunUnavailable(_ERROR)
        return digest, check_policy_digest(delivery.definition)

    def _validate_candidate(
        self,
        candidate: FrozenCandidate,
        capture_id: object,
        cycle: int,
        commit: CandidateCommitSpec,
    ) -> str:
        if not isinstance(candidate, FrozenCandidate):
            raise CodingRunUnavailable(
                "The runtime did not produce a frozen candidate."
            )
        candidate.__post_init__()
        if (
            candidate.capture_id != capture_id
            or candidate.preparation is not self._preparation
            or candidate.editable is not self._editable
            or candidate.snapshot.execution_id
            != self._expected.execution.intent.execution_id
            or candidate.snapshot.cycle != cycle
            or candidate.snapshot.raw_commit
            != commit.raw_commit(
                parent=self._expected.definition.base_revision,
                tree=candidate.snapshot.root_tree_sha,
            )
        ):
            raise CodingRunUnavailable("The frozen candidate changed original pins.")
        return candidate.snapshot.fingerprint()

    def _settlement(
        self, result: TrustedProcessResult, *, coding: bool
    ) -> SettledProcessEvidence:
        try:
            evidence = SettledProcessEvidence.from_result(result)
        except ValueError, TypeError, AttributeError:
            raise CodingRunUnavailable(
                "The process did not settle with a verified exit."
            ) from None
        if evidence.execution_id != self._expected.execution.intent.execution_id or (
            coding and evidence.exit_code != 0
        ):
            raise CodingRunUnavailable(
                "The coding stage did not produce a settled success."
            )
        return evidence

    async def execute(
        self,
        delivery: AssignmentDelivery,
        execution: WorkerExecutionIntent,
        preparation: GitWorkspacePreparation,
        editable: EditableWorkspace,
        *,
        commit: CandidateCommitSpec,
    ) -> SuccessfulCodingResult:
        if self._used:
            raise CodingRunUnavailable(
                "This coding execution has already been requested."
            )
        self._used = True
        try:
            if (
                not isinstance(delivery, AssignmentDelivery)
                or not isinstance(execution, WorkerExecutionIntent)
                or not isinstance(preparation, GitWorkspacePreparation)
                or not isinstance(editable, EditableWorkspace)
                or not isinstance(commit, CandidateCommitSpec)
            ):
                raise CodingRunUnavailable(_ERROR)
            self._preparation, self._editable = preparation, editable
            started = self._require()
            self._deadline = started + self._policy.maximum_runtime_seconds
            self._loop_deadline = (
                asyncio.get_running_loop().time() + self._policy.maximum_runtime_seconds
            )
            definition_digest, checks_digest = await self._await(
                lambda: asyncio.to_thread(
                    self._validate_input,
                    delivery,
                    execution,
                    preparation,
                    editable,
                    commit,
                ),
                self._policy.workspace_timeout_seconds,
            )
            cycles: list[CodingCycleEvidence] = []
            previous: CodingCycleEvidence | None = None
            for cycle in range(delivery.definition.max_repair_cycles + 1):
                coding = await self._await(
                    partial(
                        self._runtime.code,
                        delivery,
                        editable,
                        cycle=cycle,
                        previous=previous,
                    ),
                    self._policy.coding_timeout_seconds,
                )
                settled = self._settlement(coding, coding=True)
                capture_id = uuid4()
                frozen = await self._await(
                    partial(
                        self._workspace.capture_candidate,
                        preparation,
                        editable,
                        capture_id=capture_id,
                        cycle=cycle,
                        commit=commit,
                    ),
                    self._policy.workspace_timeout_seconds,
                )
                candidate_digest = await self._await(
                    partial(
                        asyncio.to_thread,
                        self._validate_candidate,
                        frozen,
                        capture_id,
                        cycle,
                        commit,
                    ),
                    self._policy.workspace_timeout_seconds,
                )
                checks: list[CodingCheckEvidence] = []
                for check in delivery.definition.checks:
                    await self._await(
                        partial(self._workspace.verify_candidate, frozen),
                        self._policy.workspace_timeout_seconds,
                    )
                    result = await self._await(
                        partial(self._runtime.check, frozen, check),
                        check.timeout_seconds,
                    )
                    evidence = self._settlement(result, coding=False)
                    await self._await(
                        partial(self._workspace.verify_candidate, frozen),
                        self._policy.workspace_timeout_seconds,
                    )
                    checks.append(
                        CodingCheckEvidence(
                            check_id=check.id,
                            revision=frozen.snapshot.revision,
                            candidate_digest=candidate_digest,
                            settlement=evidence,
                            diagnostic_tail=_diagnostic(result),
                        )
                    )
                previous = CodingCycleEvidence(
                    cycle=cycle,
                    revision=frozen.snapshot.revision,
                    root_tree_sha=frozen.snapshot.root_tree_sha,
                    candidate_digest=candidate_digest,
                    raw_commit=frozen.snapshot.raw_commit,
                    coding=settled,
                    checks=tuple(checks),
                )
                cycles.append(previous)
                if previous.passed:
                    report = await self._await(
                        partial(
                            asyncio.to_thread,
                            self._report,
                            delivery,
                            execution,
                            definition_digest,
                            checks_digest,
                            tuple(cycles),
                            frozen,
                        ),
                        self._policy.workspace_timeout_seconds,
                    )
                    self._seal_runtime()
                    self._require()
                    return report
                if cycle >= delivery.definition.max_repair_cycles:
                    raise CodingRunUnavailable(
                        "The configured coding repair budget was exhausted."
                    )
                released = await self._await(
                    partial(self._workspace.release_candidate, frozen),
                    self._policy.workspace_timeout_seconds,
                )
                if released is not editable:
                    raise CodingRunUnavailable(
                        "Repair changed the original editable workspace."
                    )
            raise CodingRunUnavailable(_ERROR)
        except BaseException:
            for seal in (self._seal_runtime, self._seal_workspace):
                try:
                    seal()
                except Exception:
                    pass
            if self._pending is not None:
                self._pending.cancel()
            raise

    @staticmethod
    def _report(
        delivery: AssignmentDelivery,
        execution: WorkerExecutionIntent,
        definition_digest: str,
        checks_digest: str,
        cycles: tuple[CodingCycleEvidence, ...],
        candidate: FrozenCandidate,
    ) -> SuccessfulCodingResult:
        report = SuccessfulCodingResult(
            execution_id=execution.execution_id,
            definition_digest=definition_digest,
            check_policy_digest=checks_digest,
            cycles=cycles,
            final_candidate=candidate.snapshot,
        )
        report.validate(delivery.definition)
        return report
