"""Bounded proof-free durable publication input, with fresh final authentication."""

import asyncio
import hashlib
from dataclasses import dataclass, replace
from secrets import compare_digest
from uuid import UUID

from sqlalchemy import String, cast, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Bundle

from devrelay.application.coding_result_preparation import compile_coding_checks
from devrelay.application.contracts.assignment_delivery import AssignedJobDefinition
from devrelay.application.contracts.claims import (
    ClaimRequestIdentity,
    LeaseProofVerifier,
)
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import (
    CodingResultUnavailable,
)
from devrelay.application.contracts.coding_result_requests import (
    MAX_CODING_RESULT_REQUEST_BYTES,
    CodingResultIdentity,
)
from devrelay.application.contracts.coding_result_wire import (
    decode_coding_result_intent,
)
from devrelay.application.contracts.submissions import RepositoryPolicy
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.jobs import JobSubmissionConflict
from devrelay.application.ports.coding_result_persistence import CodingPublicationStore
from devrelay.application.ports.job_persistence import JobPersistenceError
from devrelay.application.ports.task_persistence import TaskPersistenceError
from devrelay.infrastructure.persistence.claim_models import ClaimReceiptRecord
from devrelay.infrastructure.persistence.claim_store import SqlAlchemyClaimStore
from devrelay.infrastructure.persistence.job_mapping import AcceptedResultHistory
from devrelay.infrastructure.persistence.job_models import JobRecord
from devrelay.infrastructure.persistence.job_repository import read_submission
from devrelay.infrastructure.persistence.result_models import AttemptResultRecord

_FAILURE = "Accepted coding result is unavailable."


@dataclass(frozen=True, slots=True, repr=False)
class _AcceptedInput:
    metadata: AcceptedResultHistory
    definition: AssignedJobDefinition
    claim: ClaimRequestIdentity
    payload: bytes


def _prepare(source: _AcceptedInput) -> PreparedCodingResult:
    """Hash and validate complete owned input outside the SQL transaction/loop."""
    metadata, payload = source.metadata, source.payload
    if (
        type(payload) is not bytes
        or not 1 <= len(payload) <= MAX_CODING_RESULT_REQUEST_BYTES
        or not compare_digest(
            hashlib.sha256(payload).hexdigest(), metadata.payload_sha256
        )
    ):
        raise ValueError(_FAILURE)
    intent = decode_coding_result_intent(payload)
    intent.result.validate(source.definition)
    identity = CodingResultIdentity(intent=intent, claim=source.claim)
    identity.require_replay(metadata.fingerprint)
    execution = intent.execution
    acknowledgement = execution.acknowledgement
    if (
        metadata.worker_id != source.claim.worker_id
        or metadata.claim_request_id != source.claim.claim_request_id
        or metadata.claim_fingerprint != source.claim.fingerprint
        or metadata.request_id != intent.result_request_id
        or metadata.job_id != source.definition.job_id
        or metadata.repository_id != source.definition.repository_id
        or metadata.snapshot_id != source.definition.snapshot.id
        or metadata.definition_digest != source.definition.fingerprint()
        or metadata.attempt_id != acknowledgement.attempt_id
        or metadata.generation != acknowledgement.generation
        or metadata.execution_id != execution.execution_id
        or metadata.candidate_revision != intent.result.final_candidate.revision
        or metadata.repair_cycles != len(intent.result.cycles) - 1
        or metadata.publishing_workflow_version != 4 + 2 * metadata.repair_cycles
        or (
            metadata.protocol_version,
            metadata.fingerprint_version,
            metadata.expected_workflow_version,
        )
        != (1, 1, 2)
    ):
        raise ValueError(_FAILURE)
    return PreparedCodingResult(
        identity=identity,
        definition=source.definition,
        payload_bytes=payload,
        payload_sha256=metadata.payload_sha256,
        cycle_checks=compile_coding_checks(intent.result),
    )


class SqlAlchemyAcceptedCodingResultReader(SqlAlchemyClaimStore):
    """Read original accepted evidence without taking over pending operations.

    Metadata admission precedes payload disclosure. All SQL is a fixed number of
    bounded queries, with no provider I/O; the existing store performs the final
    authenticated lock/correlation read after expensive canonical validation.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        policies: tuple[RepositoryPolicy, ...],
        publication_store: CodingPublicationStore,
    ) -> None:
        super().__init__(sessions, policies)
        self._publication_store = publication_store

    async def read(
        self, result_id: UUID, authorization: WorkerAuthorization
    ) -> PreparedCodingResult | None:
        if not isinstance(result_id, UUID) or not isinstance(
            authorization, WorkerAuthorization
        ):
            raise CodingResultUnavailable(_FAILURE)
        captured = replace(authorization)
        try:
            async with self._sessions() as session, session.begin():
                await self._timeouts(session)
                scope = self._authorize(captured, await self._clock(session))
                row = (
                    await session.execute(
                        select(
                            Bundle(
                                "metadata",
                                *(
                                    getattr(AttemptResultRecord, name)
                                    for name in AcceptedResultHistory._fields
                                ),
                            ),
                            func.octet_length(AttemptResultRecord.payload_bytes),
                            cast(
                                func.pg_typeof(AttemptResultRecord.payload_bytes),
                                String,
                            ),
                            ClaimReceiptRecord,
                            JobRecord.submission_key,
                        )
                        .outerjoin(
                            ClaimReceiptRecord,
                            (
                                ClaimReceiptRecord.worker_id
                                == AttemptResultRecord.worker_id
                            )
                            & (
                                ClaimReceiptRecord.claim_request_id
                                == AttemptResultRecord.claim_request_id
                            ),
                        )
                        .outerjoin(
                            JobRecord, JobRecord.id == AttemptResultRecord.job_id
                        )
                        .where(
                            AttemptResultRecord.id == result_id,
                            AttemptResultRecord.worker_id == captured.worker_id,
                            AttemptResultRecord.repository_id.in_(scope),
                        )
                    )
                ).one_or_none()
                if row is None:
                    self._authorize(captured, await self._clock(session))
                    return None
                raw, length, payload_type, claim, key = row
                metadata = AcceptedResultHistory(*raw)
                if (
                    type(length) is not int
                    or not 1 <= length <= MAX_CODING_RESULT_REQUEST_BYTES
                    or payload_type != "bytea"
                    or claim is None
                    or key is None
                    or claim.outcome != "assigned"
                    or claim.job_id != metadata.job_id
                    or claim.attempt_id != metadata.attempt_id
                    or (
                        claim.protocol_version,
                        claim.fingerprint_version,
                        claim.verifier_version,
                    )
                    != (1, 1, 1)
                ):
                    raise ValueError(_FAILURE)
                identity = ClaimRequestIdentity(
                    worker_id=claim.worker_id,
                    claim_request_id=claim.claim_request_id,
                    protocol_version=1,
                    fingerprint=claim.fingerprint,
                    verifier=LeaseProofVerifier(
                        worker_id=claim.worker_id,
                        claim_request_id=claim.claim_request_id,
                        digest=claim.verifier,
                    ),
                )
                stored = await read_submission(session, key)
                if stored is None:
                    raise ValueError(_FAILURE)
                job, policy = stored.job, stored.intent.policy
                configured = self._policies[metadata.repository_id]
                if (
                    policy.provider_repository_id is None
                    or configured.provider_repository_id
                    != policy.provider_repository_id
                    or configured.slug.lower() != policy.slug.lower()
                    or job.id != metadata.job_id
                ):
                    raise ValueError(_FAILURE)
                definition = AssignedJobDefinition(
                    job_id=job.id,
                    source_task_id=job.source_task_id,
                    repository_id=job.repository_id,
                    provider_repository_id=policy.provider_repository_id,
                    repository_slug=policy.slug,
                    snapshot=stored.intent.stored.snapshot,
                    authorized_branch=job.authorized_branch,
                    target_branch=job.target_branch,
                    base_revision=job.base_revision,
                    policy_version=job.policy_version,
                    checks=policy.checks,
                    max_repair_cycles=job.max_repair_cycles,
                    created_at=job.created_at,
                )
                self._authorize(captured, await self._clock(session))
                payload = (
                    await session.execute(
                        select(AttemptResultRecord.payload_bytes).where(
                            AttemptResultRecord.id == result_id,
                            AttemptResultRecord.worker_id == captured.worker_id,
                            func.octet_length(AttemptResultRecord.payload_bytes)
                            == length,
                            func.octet_length(AttemptResultRecord.payload_bytes)
                            <= MAX_CODING_RESULT_REQUEST_BYTES,
                            AttemptResultRecord.payload_sha256
                            == metadata.payload_sha256,
                        )
                    )
                ).scalar_one()
                source = _AcceptedInput(metadata, definition, identity, payload)
                self._authorize(captured, await self._clock(session))
            prepared = await asyncio.to_thread(_prepare, source)
            history = await self._publication_store.read_publication(prepared, captured)
            if history is None or history.status.receipt.result_id != result_id:
                raise ValueError(_FAILURE)
            return prepared
        except (
            SQLAlchemyError,
            ValueError,
            TypeError,
            AttributeError,
            KeyError,
            OverflowError,
            JobPersistenceError,
            TaskPersistenceError,
            JobSubmissionConflict,
        ):
            raise CodingResultUnavailable(_FAILURE) from None
