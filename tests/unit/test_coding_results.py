import hashlib
from dataclasses import FrozenInstanceError, dataclass, fields, replace
from typing import Any
from uuid import UUID

import pytest

from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    git_blob_sha,
)
from devrelay.application.contracts.candidates import (
    CandidateCommitIdentity,
    CandidateCommitSpec,
    CandidateSnapshot,
)
from devrelay.application.contracts.coding_results import (
    MAX_CHECK_DIAGNOSTIC_BYTES,
    CodingCheckEvidence,
    CodingCycleEvidence,
    SettledProcessEvidence,
    SuccessfulCodingResult,
    check_policy_digest,
)
from devrelay.application.contracts.git_objects import GitObject, build_git_tree
from devrelay.application.contracts.processes import ProcessStopStatus
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessResult,
    TrustedProcessStopReason,
)
from devrelay.domain.jobs.values import VerificationOutcome
from tests.git_source_fixtures import git_workspace_inputs

_EXECUTION = UUID("a1111111-1111-4111-8111-111111111111")
_OTHER_EXECUTION = UUID("b1111111-1111-4111-8111-111111111111")


def process(exit_code: int = 0, **changes: Any) -> TrustedProcessResult:
    values: dict[str, Any] = {
        "execution_id": _EXECUTION,
        "reason": TrustedProcessStopReason.EXITED,
        "exit_code": exit_code,
        "stdout": b"model says success; not check evidence",
        "stderr": b"private runtime diagnostic",
        "stop_status": ProcessStopStatus.EMPTY_VERIFIED,
    }
    values.update(changes)
    return TrustedProcessResult(**values)


@dataclass
class Context:
    definition: AssignedJobDefinition
    result: SuccessfulCodingResult


def context(
    repairs: int = 0,
    *,
    repeated_revision: bool = False,
    definition: AssignedJobDefinition | None = None,
    execution_id: UUID = _EXECUTION,
) -> Context:
    _, delivery, _, source = git_workspace_inputs()
    if definition is None:
        definition = replace(
            delivery.definition,
            max_repair_cycles=5,
            checks=(
                *delivery.definition.checks,
                replace(delivery.definition.checks[0], id="lint"),
            ),
        )
    source_request = replace(
        source.snapshot.request,
        repository_id=definition.repository_id,
        provider_repository_id=definition.provider_repository_id,
        repository_slug=definition.repository_slug,
        base_revision=definition.base_revision,
    )
    cycles: list[CodingCycleEvidence] = []
    candidate: CandidateSnapshot | None = None
    human = CandidateCommitIdentity(name="Fixture Human", email="human@example.invalid")
    for index in range(repairs + 1):
        data = (
            b"Final changed content\n"
            if repeated_revision
            else f"Cycle {index}\n".encode()
        )
        files_ = (
            BaseSourceFile(
                path="changed.txt",
                blob_sha=git_blob_sha(data),
                mode=BaseFileMode.REGULAR,
                content=data,
            ),
        )
        tree = build_git_tree(files_).root_tree_sha
        raw = CandidateCommitSpec(
            author=human,
            committer=human,
            timestamp=1700000000,
            message="Apply candidate\n",
        ).raw_commit(parent=definition.base_revision, tree=tree)
        candidate = CandidateSnapshot(
            execution_id=execution_id,
            cycle=index,
            request=source_request,
            authorized_branch=definition.authorized_branch,
            root_tree_sha=tree,
            revision=GitObject(kind="commit", data=raw).sha,
            raw_commit=raw,
            files=files_,
        )
        checks = tuple(
            CodingCheckEvidence(
                check_id=check.id,
                revision=candidate.revision,
                candidate_digest=candidate.fingerprint(),
                settlement=SettledProcessEvidence.from_result(
                    process(
                        1 if index < repairs and position == 0 else 0,
                        execution_id=execution_id,
                    )
                ),
                diagnostic_tail="Sanitized check tail\n",
            )
            for position, check in enumerate(definition.checks)
        )
        cycles.append(
            CodingCycleEvidence(
                cycle=index,
                revision=candidate.revision,
                root_tree_sha=tree,
                candidate_digest=candidate.fingerprint(),
                raw_commit=raw,
                coding=SettledProcessEvidence.from_result(
                    process(execution_id=execution_id)
                ),
                checks=checks,
            )
        )
    assert candidate is not None
    return Context(
        definition,
        SuccessfulCodingResult(
            execution_id=execution_id,
            definition_digest=definition.fingerprint(),
            check_policy_digest=check_policy_digest(definition),
            cycles=tuple(cycles),
            final_candidate=candidate,
        ),
    )


@pytest.mark.parametrize("repairs", [0, 1, 5])
def test_success_binds_full_definition_actual_candidate_and_contiguous_checks(
    repairs: int,
) -> None:
    setup = context(repairs)

    setup.result.validate(setup.definition)

    assert tuple(cycle.cycle for cycle in setup.result.cycles) == tuple(
        range(repairs + 1)
    )
    assert [cycle.passed for cycle in setup.result.cycles] == [False] * repairs + [True]
    assert len(setup.result.fingerprint()) == 64
    assert setup.result.cycles[-1].revision == setup.result.final_candidate.revision
    checks = setup.result.cycles[-1].domain_checks()
    assert tuple(check.check_id for check in checks) == tuple(
        check.id for check in setup.definition.checks
    )
    assert all(
        check.revision == setup.result.final_candidate.revision
        and check.outcome is VerificationOutcome.PASSED
        and check.exit_code == 0
        for check in checks
    )


def test_repeated_revision_still_requires_fresh_cycle_specific_check_binding() -> None:
    setup = context(2, repeated_revision=True)

    setup.result.validate(setup.definition)

    assert len({cycle.revision for cycle in setup.result.cycles}) == 1
    assert len({cycle.candidate_digest for cycle in setup.result.cycles}) == 3
    prior = setup.result.cycles[0].checks[0]
    with pytest.raises(ValueError, match="exact frozen candidate"):
        replace(
            setup.result.cycles[1], checks=(prior, *setup.result.cycles[1].checks[1:])
        )


def test_process_compaction_discards_raw_output_and_preserves_diagnostic_hashes() -> (
    None
):
    raw = process()

    compact = SettledProcessEvidence.from_result(raw)

    assert compact.stdout_digest == hashlib.sha256(raw.stdout).hexdigest()
    assert compact.stderr_digest == hashlib.sha256(raw.stderr).hexdigest()
    assert compact.stdout_bytes == len(raw.stdout)
    assert compact.stderr_bytes == len(raw.stderr)
    assert "stdout" not in {item.name for item in fields(compact)}
    assert "stderr" not in {item.name for item in fields(compact)}
    assert raw.stdout.decode() not in repr(compact)
    assert raw.stderr.decode() not in repr(compact)


@pytest.mark.parametrize("reason", list(TrustedProcessStopReason)[1:])
def test_timeout_cancel_or_launch_failure_cannot_be_an_ordinary_exit(
    reason: TrustedProcessStopReason,
) -> None:
    with pytest.raises(ValueError, match="settled native exit"):
        SettledProcessEvidence.from_result(process(reason=reason))


@pytest.mark.parametrize(
    "changes",
    [
        {"stop_status": ProcessStopStatus.UNKNOWN},
        {"exit_code": None},
    ],
)
def test_missing_exit_or_fresh_empty_evidence_cannot_support_success(
    changes: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        SettledProcessEvidence.from_result(process(**changes))


@pytest.mark.parametrize("exit_code", [1, 259, 0xFFFFFFFF])
def test_actual_nonzero_exit_supports_failure_but_never_domain_success(
    exit_code: int,
) -> None:
    cycle = context().result.cycles[0]
    failed = replace(
        cycle,
        checks=(
            replace(
                cycle.checks[0],
                settlement=SettledProcessEvidence.from_result(process(exit_code)),
            ),
            *cycle.checks[1:],
        ),
    )

    assert not failed.passed
    with pytest.raises(ValueError, match="fully passed"):
        failed.domain_checks()


@pytest.mark.parametrize(
    "changes",
    [
        {"reason": "exited"},
        {"stop_status": "empty_verified"},
        {"exit_code": True},
        {"exit_code": -1},
        {"exit_code": 2**32},
        {"execution_id": "not-a-uuid"},
        {"stdout_digest": "F" * 64},
        {"stderr_digest": "a" * 63},
        {"stdout_bytes": True},
        {"stdout_bytes": -1},
        {"stdout_bytes": 1024**2},
        {"stdout_bytes": 0},
    ],
)
def test_compact_settlement_revalidates_typed_native_bounds(
    changes: dict[str, Any],
) -> None:
    compact = SettledProcessEvidence.from_result(process())

    with pytest.raises(ValueError):
        replace(compact, **changes)


def test_empty_outputs_have_exact_fixed_digest() -> None:
    compact = SettledProcessEvidence.from_result(process(stdout=b"", stderr=b""))
    assert compact.stdout_bytes == compact.stderr_bytes == 0
    assert (
        compact.stdout_digest
        == compact.stderr_digest
        == hashlib.sha256(b"").hexdigest()
    )


@pytest.mark.parametrize(
    "tail", ["\x00secret", "\x1b[31mred", "\rreturn", "\u202esecret", "é" * 2049]
)
def test_diagnostic_tail_rejects_controls_and_utf8_overflow(tail: str) -> None:
    check = context().result.cycles[0].checks[0]

    with pytest.raises(ValueError):
        replace(check, diagnostic_tail=tail)


def test_bounded_diagnostics_are_hidden_and_do_not_determine_check_success() -> None:
    cycle = context().result.cycles[0]
    check = replace(cycle.checks[0], diagnostic_tail="failed" * 682 + "!!!!")
    assert len(check.diagnostic_tail.encode()) == MAX_CHECK_DIAGNOSTIC_BYTES
    assert check.diagnostic_tail not in repr(check)
    assert replace(cycle, checks=(check, *cycle.checks[1:])).passed
    failed = replace(
        check,
        settlement=SettledProcessEvidence.from_result(process(1)),
        diagnostic_tail="SUCCESS",
    )
    assert not replace(cycle, checks=(failed, *cycle.checks[1:])).passed


@pytest.mark.parametrize(
    "mutation",
    [
        "empty",
        "list",
        "duplicate",
        "foreign_execution",
        "wrong_revision",
        "wrong_digest",
    ],
)
def test_cycle_checks_require_exact_candidate_and_execution(mutation: str) -> None:
    cycle = context().result.cycles[0]
    checks: Any = cycle.checks
    if mutation == "empty":
        checks = ()
    elif mutation == "list":
        checks = list(checks)
    elif mutation == "duplicate":
        checks = (checks[0], checks[0])
    elif mutation == "foreign_execution":
        checks = (
            replace(
                checks[0],
                settlement=replace(checks[0].settlement, execution_id=_OTHER_EXECUTION),
            ),
            *checks[1:],
        )
    elif mutation == "wrong_revision":
        checks = (replace(checks[0], revision="f" * 40), *checks[1:])
    else:
        checks = (replace(checks[0], candidate_digest="e" * 64), *checks[1:])

    with pytest.raises(ValueError):
        replace(cycle, checks=checks)


@pytest.mark.parametrize(
    "mutation",
    [
        "gap",
        "empty",
        "tuple_type",
        "earlier_passed",
        "final_failed",
        "execution",
        "final_snapshot",
        "raw_commit",
    ],
)
def test_success_rejects_incomplete_or_mismatched_history(mutation: str) -> None:
    result = context(1).result
    changes: dict[str, Any] = {}
    match mutation:
        case "gap":
            changes["cycles"] = (replace(result.cycles[0], cycle=1), result.cycles[1])
        case "empty":
            changes["cycles"] = ()
        case "tuple_type":
            changes["cycles"] = list(result.cycles)
        case "earlier_passed":
            first = result.cycles[0]
            checks = tuple(
                replace(check, settlement=replace(check.settlement, exit_code=0))
                for check in first.checks
            )
            changes["cycles"] = (replace(first, checks=checks), result.cycles[1])
        case "final_failed":
            last = result.cycles[1]
            checks = (
                replace(
                    last.checks[0],
                    settlement=replace(last.checks[0].settlement, exit_code=1),
                ),
                *last.checks[1:],
            )
            changes["cycles"] = (result.cycles[0], replace(last, checks=checks))
        case "execution":
            changes["execution_id"] = _OTHER_EXECUTION
        case "final_snapshot":
            changes["final_candidate"] = replace(result.final_candidate, cycle=0)
        case "raw_commit":
            corrupt = replace(result.cycles[1])
            object.__setattr__(corrupt, "raw_commit", corrupt.raw_commit + b"changed")
            changes["cycles"] = (result.cycles[0], corrupt)

    with pytest.raises(ValueError):
        replace(result, **changes)


def test_coding_stage_nonzero_is_not_a_supported_check_repair_report() -> None:
    cycle = context().result.cycles[0]
    with pytest.raises(ValueError, match="settled coding stage"):
        replace(cycle, coding=SettledProcessEvidence.from_result(process(1)))


@pytest.mark.parametrize(
    "mutation",
    [
        "reorder",
        "missing",
        "extra",
        "policy",
        "definition",
        "budget",
        "base",
        "branch",
        "repository",
    ],
)
def test_definition_validation_rejects_wrong_policy_and_original_scope(
    mutation: str,
) -> None:
    setup = context(1)
    result, definition = setup.result, setup.definition
    match mutation:
        case "reorder":
            definition = replace(definition, checks=tuple(reversed(definition.checks)))
        case "missing":
            definition = replace(definition, checks=definition.checks[:1])
        case "extra":
            definition = replace(
                definition,
                checks=(
                    *definition.checks,
                    replace(definition.checks[0], id="additional"),
                ),
            )
        case "policy":
            definition = replace(
                definition,
                checks=(
                    replace(definition.checks[0], timeout_seconds=99),
                    *definition.checks[1:],
                ),
            )
        case "definition":
            definition = replace(definition, policy_version="other")
        case "budget":
            definition = replace(definition, max_repair_cycles=0)
        case "base":
            definition = replace(definition, base_revision="f" * 40)
        case "branch":
            definition = replace(definition, authorized_branch="agent/other")
        case "repository":
            definition = replace(definition, repository_slug="other/project")
    # Re-pin outer digests to exercise structural correlation beyond their hash.
    if mutation not in {"definition", "policy"}:
        result = replace(
            result,
            definition_digest=definition.fingerprint(),
            check_policy_digest=check_policy_digest(definition),
        )

    with pytest.raises(ValueError):
        result.validate(definition)


def test_old_cycle_commit_must_retain_exact_original_base_parent() -> None:
    setup = context(1)
    first = setup.result.cycles[0]
    raw = first.raw_commit.replace(
        setup.definition.base_revision.encode(), b"f" * 40, 1
    )
    revision = GitObject(kind="commit", data=raw).sha
    altered = replace(
        first,
        raw_commit=raw,
        revision=revision,
        checks=tuple(replace(check, revision=revision) for check in first.checks),
    )
    result = replace(setup.result, cycles=(altered, setup.result.cycles[1]))

    with pytest.raises(ValueError, match="canonical unsigned"):
        result.validate(setup.definition)


def test_historical_cycles_do_not_retain_old_source_bytes_or_process_output() -> None:
    result = context(2).result
    assert "files" not in {item.name for item in fields(result.cycles[0])}
    assert "stdout" not in {item.name for item in fields(result.cycles[0].coding)}
    assert "timestamp" not in {item.name for item in fields(result)}
    assert result.final_candidate.files[0].content not in repr(result).encode()
    assert result.cycles[0].raw_commit not in repr(result.cycles[0]).encode()
    with pytest.raises(FrozenInstanceError):
        result.definition_digest = "f" * 64  # type: ignore[misc]


def test_cycle_and_check_count_limits_are_enforced_before_folding() -> None:
    result = context().result
    first = result.cycles[0]
    with pytest.raises(ValueError, match="bounded immutable"):
        replace(first, cycle=6)
    with pytest.raises(ValueError, match="bounded immutable"):
        replace(
            first,
            checks=tuple(
                replace(first.checks[0], check_id=f"check_{index}")
                for index in range(33)
            ),
        )
    with pytest.raises(ValueError, match="bounded cycle"):
        replace(result, cycles=(first,) * 7)


@pytest.mark.parametrize("field", ["argv", "working_directory", "timeout_seconds"])
def test_check_policy_digest_binds_actual_fixed_command_configuration(
    field: str,
) -> None:
    definition = context().definition
    changes: dict[str, Any] = {
        "argv": ("python", "-m", "another_check"),
        "working_directory": "src",
        "timeout_seconds": 99,
    }
    changed = replace(
        definition,
        checks=(
            replace(definition.checks[0], **{field: changes[field]}),
            *definition.checks[1:],
        ),
    )
    assert check_policy_digest(changed) != check_policy_digest(definition)
    assert check_policy_digest(
        replace(definition, checks=tuple(reversed(definition.checks)))
    ) != check_policy_digest(definition)


def test_boundary_revalidation_detects_mutated_nested_settlement() -> None:
    setup = context()
    object.__setattr__(
        setup.result.cycles[0].checks[0].settlement,
        "stop_status",
        ProcessStopStatus.UNKNOWN,
    )

    with pytest.raises(ValueError, match="settled native"):
        setup.result.validate(setup.definition)
    with pytest.raises(ValueError, match="settled native"):
        setup.result.fingerprint()


def test_fingerprint_binds_diagnostics_and_full_final_content_without_repr_leaks() -> (
    None
):
    setup = context()
    result = setup.result
    last = result.cycles[0]
    changes = replace(last.checks[0], diagnostic_tail="Different bounded tail")
    altered = replace(
        result, cycles=(replace(last, checks=(changes, *last.checks[1:])),)
    )

    assert altered.fingerprint() != result.fingerprint()
    assert replace(result).fingerprint() == result.fingerprint()
    assert "Different bounded tail" not in repr(altered)
    corrupt = replace(result.final_candidate)
    object.__setattr__(corrupt.files[0], "content", b"Unhashed changed bytes")
    with pytest.raises(ValueError):
        replace(result, final_candidate=corrupt).fingerprint()
