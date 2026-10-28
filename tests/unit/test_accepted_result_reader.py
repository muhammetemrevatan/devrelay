"""Corrupt durable pins and canonical bytes never become dispatch evidence."""

from dataclasses import replace
from typing import Any, cast
from uuid import uuid4

import pytest

from devrelay.infrastructure.persistence.accepted_result_reader import (
    _AcceptedInput,
    _prepare,
)
from devrelay.infrastructure.persistence.job_mapping import AcceptedResultHistory
from tests.unit.test_plan_coding_publication import context


def accepted_input() -> _AcceptedInput:
    value = context(repairs=5)
    receipt, prepared = value.receipt, value.prepared
    return _AcceptedInput(
        AcceptedResultHistory(
            id=receipt.result_id,
            request_id=receipt.request_id,
            worker_id=receipt.worker_id,
            attempt_id=receipt.attempt_id,
            job_id=receipt.job_id,
            claim_request_id=receipt.claim_request_id,
            repository_id=receipt.repository_id,
            snapshot_id=prepared.definition.snapshot.id,
            execution_id=receipt.execution_id,
            generation=1,
            definition_digest=receipt.definition_digest,
            claim_fingerprint=prepared.identity.claim.fingerprint,
            fingerprint=prepared.identity.fingerprint,
            protocol_version=1,
            fingerprint_version=1,
            expected_workflow_version=2,
            publishing_workflow_version=receipt.publishing_workflow_version,
            candidate_revision=receipt.candidate_revision,
            repair_cycles=receipt.repair_cycles,
            recorded_at=value.recorded_at,
            payload_sha256=prepared.payload_sha256,
        ),
        prepared.definition,
        prepared.identity.claim,
        prepared.payload_bytes,
    )


@pytest.mark.parametrize(
    "field",
    [
        "worker_id",
        "attempt_id",
        "job_id",
        "claim_request_id",
        "repository_id",
        "snapshot_id",
        "execution_id",
        "request_id",
    ],
)
def test_joined_metadata_cannot_rebind_original_accepted_report(field: str) -> None:
    source = accepted_input()
    corrupted = source.metadata._replace(**cast(Any, {field: uuid4()}))

    with pytest.raises(ValueError):
        _prepare(replace(source, metadata=corrupted))


@pytest.mark.parametrize(
    "field", ["payload_sha256", "fingerprint", "claim_fingerprint", "definition_digest"]
)
def test_corrupt_digest_fails_original_canonical_preflight(field: str) -> None:
    source = accepted_input()

    with pytest.raises(ValueError):
        _prepare(
            replace(
                source,
                metadata=source.metadata._replace(**cast(Any, {field: "f" * 64})),
            )
        )


def test_changed_payload_is_rejected() -> None:
    source = accepted_input()

    with pytest.raises(ValueError):
        _prepare(replace(source, payload=source.payload + b" "))


def test_wrong_verifier_cannot_preserve_original_claim_fingerprint() -> None:
    source = accepted_input()
    verifier = replace(source.claim.verifier, digest=b"x" * 32)

    with pytest.raises(ValueError):
        replace(source.claim, verifier=verifier)


def test_original_proof_free_claim_retains_all_six_cycles() -> None:
    source = accepted_input()

    prepared = _prepare(source)

    prepared.identity.claim.require_replay(source.claim)
    assert len(prepared.cycle_checks) == 6
