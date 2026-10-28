"""Bounded historical coding evidence, never native or current lease authority."""

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, field
from uuid import UUID

from devrelay.application.contracts.assignment_delivery import (
    AssignedJobDefinition,
    check_payload,
)
from devrelay.application.contracts.candidates import (
    MAX_CANDIDATE_MESSAGE_BYTES,
    CandidateSnapshot,
    parse_candidate_commit,
)
from devrelay.application.contracts.git_objects import GitObject
from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessResult,
    TrustedProcessStopReason,
)
from devrelay.domain.jobs.values import CheckResult, VerificationOutcome

MAX_CODING_CYCLES = 6
MAX_CODING_CHECKS = 32
MAX_CHECK_DIAGNOSTIC_BYTES = 4096
MAX_CODING_DIAGNOSTIC_BYTES = (
    MAX_CODING_CYCLES * MAX_CODING_CHECKS * MAX_CHECK_DIAGNOSTIC_BYTES
)
_MAX_PROCESS_OUTPUT_BYTES = 1024 * 1024


def _digest(value: object, length: int = 64) -> None:
    if (
        type(value) is not str
        or re.fullmatch(r"[0-9a-f]{" + str(length) + "}", value) is None
    ):
        raise ValueError("Coding evidence requires canonical object digests.")


def _fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def check_policy_digest(definition: AssignedJobDefinition) -> str:
    """Pin exact ordered fixed commands, directories and time budgets."""
    if not isinstance(definition, AssignedJobDefinition):
        raise ValueError("Coding evidence requires a typed original definition.")
    definition.__post_init__()
    definition.fingerprint()
    return _fingerprint(
        ("devrelay.coding-check-policy.v1", check_payload(definition.checks))
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class SettledProcessEvidence:
    """Historical observation, not a current process scope or sandbox capability.

    The trusted coordinator compacts its actual runner result through from_result;
    transport reconstruction must authenticate that coordinator independently.
    Output hashes and counts remain diagnostic, never check success evidence.
    """

    execution_id: UUID
    reason: TrustedProcessStopReason
    exit_code: int
    stop_status: ProcessStopStatus
    stdout_digest: str
    stderr_digest: str
    stdout_bytes: int
    stderr_bytes: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.execution_id, UUID)
            or self.reason is not TrustedProcessStopReason.EXITED
            or self.stop_status is not ProcessStopStatus.EMPTY_VERIFIED
            or type(self.exit_code) is not int
            or not 0 <= self.exit_code <= 0xFFFFFFFF
            or type(self.stdout_bytes) is not int
            or type(self.stderr_bytes) is not int
            or not 0 <= self.stdout_bytes <= _MAX_PROCESS_OUTPUT_BYTES
            or not 0 <= self.stderr_bytes <= _MAX_PROCESS_OUTPUT_BYTES
            or self.stdout_bytes + self.stderr_bytes > _MAX_PROCESS_OUTPUT_BYTES
        ):
            raise ValueError("Coding process evidence requires a settled native exit.")
        for digest, count in (
            (self.stdout_digest, self.stdout_bytes),
            (self.stderr_digest, self.stderr_bytes),
        ):
            _digest(digest)
            if count == 0 and digest != hashlib.sha256(b"").hexdigest():
                raise ValueError("Empty process output has a fixed diagnostic digest.")

    @classmethod
    def from_result(cls, result: TrustedProcessResult) -> SettledProcessEvidence:
        if not isinstance(result, TrustedProcessResult):
            raise ValueError("Coding evidence requires the trusted runner result.")
        result.__post_init__()
        if result.exit_code is None:
            raise ValueError("Coding evidence cannot invent an absent process exit.")
        return cls(
            execution_id=result.execution_id,
            reason=result.reason,
            exit_code=result.exit_code,
            stop_status=result.stop_status,
            stdout_digest=hashlib.sha256(result.stdout).hexdigest(),
            stderr_digest=hashlib.sha256(result.stderr).hexdigest(),
            stdout_bytes=len(result.stdout),
            stderr_bytes=len(result.stderr),
        )

    def _payload(self) -> tuple[object, ...]:
        self.__post_init__()
        return (
            str(self.execution_id),
            self.reason.value,
            self.exit_code,
            self.stop_status.value,
            self.stdout_digest,
            self.stderr_digest,
            self.stdout_bytes,
            self.stderr_bytes,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class CodingCheckEvidence:
    check_id: str
    revision: str
    candidate_digest: str
    settlement: SettledProcessEvidence
    diagnostic_tail: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.check_id) is not str
            or not 1 <= len(self.check_id) <= 128
            or re.fullmatch(r"[a-z][a-z0-9_-]*", self.check_id) is None
            or not isinstance(self.settlement, SettledProcessEvidence)
            or type(self.diagnostic_tail) is not str
            or len(self.diagnostic_tail.encode("utf-8")) > MAX_CHECK_DIAGNOSTIC_BYTES
            or any(
                unicodedata.category(char).startswith("C") and char not in "\n\t"
                for char in self.diagnostic_tail
            )
        ):
            raise ValueError(
                "Coding checks require bounded sanitized fixed-check evidence."
            )
        _digest(self.revision, 40)
        _digest(self.candidate_digest)
        self.settlement.__post_init__()

    def _payload(self) -> tuple[object, ...]:
        self.__post_init__()
        return (
            self.check_id,
            self.revision,
            self.candidate_digest,
            self.settlement._payload(),
            self.diagnostic_tail,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class CodingCycleEvidence:
    """Retain prior commit identity and checks without retaining prior file bytes."""

    cycle: int
    revision: str
    root_tree_sha: str
    candidate_digest: str
    raw_commit: bytes = field(repr=False)
    coding: SettledProcessEvidence
    checks: tuple[CodingCheckEvidence, ...] = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.cycle) is not int
            or not 0 <= self.cycle < MAX_CODING_CYCLES
            or type(self.raw_commit) is not bytes
            or not 1 <= len(self.raw_commit) <= MAX_CANDIDATE_MESSAGE_BYTES + 2048
            or not isinstance(self.coding, SettledProcessEvidence)
            or type(self.checks) is not tuple
            or not 1 <= len(self.checks) <= MAX_CODING_CHECKS
            or any(not isinstance(check, CodingCheckEvidence) for check in self.checks)
        ):
            raise ValueError("Coding cycles require bounded immutable evidence.")
        _digest(self.revision, 40)
        _digest(self.root_tree_sha, 40)
        _digest(self.candidate_digest)
        self.coding.__post_init__()
        if self.coding.exit_code != 0:
            raise ValueError(
                "Successful coding evidence requires a settled coding stage."
            )
        if GitObject(kind="commit", data=self.raw_commit).sha != self.revision:
            raise ValueError("Cycle revision differs from its actual raw commit.")
        for check in self.checks:
            check.__post_init__()
            if (
                check.revision != self.revision
                or check.candidate_digest != self.candidate_digest
                or check.settlement.execution_id != self.coding.execution_id
            ):
                raise ValueError(
                    "Every check must belong to this exact frozen candidate."
                )
        if len({check.check_id for check in self.checks}) != len(self.checks):
            raise ValueError(
                "Each fixed check must be observed exactly once per cycle."
            )

    @property
    def passed(self) -> bool:
        self.__post_init__()
        return all(check.settlement.exit_code == 0 for check in self.checks)

    def domain_checks(self) -> tuple[CheckResult, ...]:
        if not self.passed:
            raise ValueError(
                "Only fully passed fixed checks become domain success evidence."
            )
        return tuple(
            CheckResult(
                check_id=check.check_id,
                revision=self.revision,
                outcome=VerificationOutcome.PASSED,
                exit_code=check.settlement.exit_code,
            )
            for check in self.checks
        )

    def _payload(self) -> tuple[object, ...]:
        self.__post_init__()
        return (
            self.cycle,
            self.revision,
            self.root_tree_sha,
            self.candidate_digest,
            hashlib.sha256(self.raw_commit).hexdigest(),
            self.coding._payload(),
            tuple(check._payload() for check in self.checks),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class SuccessfulCodingResult:
    """One historical successful coding report, not Job delivery completion.

    Native source/metadata verification and the configured readonly check policy
    remain responsibilities of the live composed coordinator. No boolean in this
    report supplies those capabilities or restores them after restart.
    """

    execution_id: UUID
    definition_digest: str
    check_policy_digest: str
    cycles: tuple[CodingCycleEvidence, ...] = field(repr=False)
    final_candidate: CandidateSnapshot = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.execution_id, UUID)
            or type(self.cycles) is not tuple
            or not 1 <= len(self.cycles) <= MAX_CODING_CYCLES
            or any(not isinstance(cycle, CodingCycleEvidence) for cycle in self.cycles)
            or not isinstance(self.final_candidate, CandidateSnapshot)
        ):
            raise ValueError(
                "Successful coding requires bounded cycle and final evidence."
            )
        _digest(self.definition_digest)
        _digest(self.check_policy_digest)
        self.final_candidate.validate()
        diagnostics = 0
        for index, cycle in enumerate(self.cycles):
            cycle.__post_init__()
            if (
                cycle.cycle != index
                or cycle.coding.execution_id != self.execution_id
                or cycle.passed != (index == len(self.cycles) - 1)
            ):
                raise ValueError(
                    "Coding history requires contiguous failed-then-passed cycles."
                )
            diagnostics += sum(
                len(check.diagnostic_tail.encode("utf-8")) for check in cycle.checks
            )
        if diagnostics > MAX_CODING_DIAGNOSTIC_BYTES:
            raise ValueError("Coding diagnostics exceed the aggregate evidence budget.")
        final = self.cycles[-1]
        candidate = self.final_candidate
        if (
            candidate.execution_id != self.execution_id
            or candidate.cycle != final.cycle
            or candidate.revision != final.revision
            or candidate.root_tree_sha != final.root_tree_sha
            or candidate.raw_commit != final.raw_commit
            or candidate.fingerprint() != final.candidate_digest
        ):
            raise ValueError(
                "Final complete candidate differs from the checked candidate."
            )

    def validate(self, definition: AssignedJobDefinition) -> None:
        self.__post_init__()
        if (
            not isinstance(definition, AssignedJobDefinition)
            or definition.fingerprint() != self.definition_digest
            or check_policy_digest(definition) != self.check_policy_digest
            or len(self.cycles) - 1 > definition.max_repair_cycles
        ):
            raise ValueError(
                "Coding result changed original definition or repair policy."
            )
        candidate = self.final_candidate
        request = candidate.request
        if (
            request.repository_id != definition.repository_id
            or request.provider_repository_id != definition.provider_repository_id
            or request.repository_slug != definition.repository_slug
            or request.base_revision != definition.base_revision
            or candidate.authorized_branch != definition.authorized_branch
        ):
            raise ValueError(
                "Coding result changed original repository, base or branch."
            )
        check_ids = tuple(check.id for check in definition.checks)
        for cycle in self.cycles:
            parse_candidate_commit(
                cycle.raw_commit,
                parent=definition.base_revision,
                tree=cycle.root_tree_sha,
            )
            if tuple(check.check_id for check in cycle.checks) != check_ids:
                raise ValueError(
                    "Every cycle requires the exact ordered configured checks."
                )

    def fingerprint(self) -> str:
        self.__post_init__()
        return _fingerprint(
            (
                "devrelay.successful-coding-result.v1",
                str(self.execution_id),
                self.definition_digest,
                self.check_policy_digest,
                tuple(cycle._payload() for cycle in self.cycles),
                self.final_candidate.fingerprint(),
            )
        )
