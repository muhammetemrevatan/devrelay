from dataclasses import replace
from typing import Any
from uuid import UUID

import pytest

from devrelay.application.contracts.coding_result_receipts import (
    CodingPublicationState,
    CodingPublicationStep,
    CodingResultStatus,
    CodingWritePermit,
)
from devrelay.infrastructure.persistence.result_store import _capture, _snapshot
from tests.unit.test_coding_result_preparation import context, prepare
from tests.unit.test_coding_result_receipts import context as receipt_context
from tests.unit.test_coding_result_receipts import terminal


def step() -> CodingPublicationStep:
    return CodingPublicationStep(
        result_id=UUID(int=1),
        invocation_id=UUID(int=2),
        expected_operation_version=1,
        step_id=UUID(int=3),
        step_kind="blob:" + "a" * 40,
        input_digest="b" * 64,
    )


@pytest.mark.parametrize("value", [True, 0, -1, 2045, 2046, 2047, 1.0, "1"])
def test_write_admission_reserves_history_for_observation_and_terminal_event(
    value: Any,
) -> None:
    with pytest.raises(ValueError):
        replace(step(), expected_operation_version=value)


def test_last_valid_step_leaves_room_for_both_post_write_events() -> None:
    last = replace(step(), expected_operation_version=2044)
    permit = CodingWritePermit(step=last, operation_version=2045, created=True)
    assert permit.operation_version + 2 == 2047
    assert not replace(permit, created=False).created


@pytest.mark.parametrize(
    "value", ["", "../branch", " ref", "a\nb", "A", "x" * 129, None, True]
)
def test_step_kind_is_bounded_stable_non_payload_key(value: Any) -> None:
    with pytest.raises(ValueError):
        replace(step(), step_kind=value)


def test_terminal_status_requires_prior_operation_history() -> None:
    setup = receipt_context()
    with pytest.raises(ValueError):
        CodingResultStatus(
            receipt=setup.receipt,
            state=CodingPublicationState.FINALIZED,
            operation_version=1,
            terminal=terminal(setup),
        )


def test_store_snapshot_owns_metadata_before_offloop_decode_and_validation() -> None:
    setup = context(1)
    prepared = prepare(setup)
    captured_input = _snapshot(prepared)
    expected = prepared.identity.fingerprint
    object.__setattr__(prepared.definition.snapshot, "title", "changed caller")
    object.__setattr__(prepared.identity.claim.verifier, "digest", b"y" * 32)
    object.__setattr__(prepared.cycle_checks[0][0], "exit_code", 0)
    object.__setattr__(
        prepared.identity.intent.result.final_candidate.files[0],
        "content",
        b"changed source",
    )
    result = _capture(captured_input)
    assert result.identity.fingerprint == expected
    assert result.definition.snapshot.title != "changed caller"
    assert result.identity.claim.verifier.digest != b"y" * 32
    assert result.cycle_checks[0][0].exit_code != 0
    assert (
        result.identity.intent.result.final_candidate.files[0].content
        != b"changed source"
    )
    assert "payload_bytes" not in repr(captured_input)
