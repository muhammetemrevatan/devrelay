import base64
import binascii
import json
import re
from dataclasses import dataclass, field
from hashlib import sha256
from secrets import compare_digest, token_urlsafe
from typing import Literal
from uuid import UUID

from devrelay.application.contracts.workers import WorkerAuthorization


class InvalidLeaseProof(ValueError):
    """The proof is malformed or reuses a known credential."""


class InvalidClaimRequest(ValueError):
    """The request identity or protocol is not supported."""


class ClaimReplayConflict(ValueError):
    """An operation identity was reused with different normalized input."""


def _identity(value: UUID) -> None:
    if not isinstance(value, UUID):
        raise InvalidClaimRequest("Claim identity must be a UUID.")


def _version(value: int) -> None:
    if type(value) is not int or value != 1:
        raise InvalidClaimRequest("Claim protocol or verifier version is unsupported.")


def _decode_proof(value: str) -> bytes:
    if (
        not isinstance(value, str)
        or len(value) != 43
        or re.fullmatch(r"[A-Za-z0-9_-]{43}", value) is None
        or value.startswith("replace-with-")
    ):
        raise InvalidLeaseProof("Lease proof requires a canonical 32-byte secret.")
    try:
        decoded = base64.b64decode(value + "=", altchars=b"-_", validate=True)
    except binascii.Error:
        raise InvalidLeaseProof(
            "Lease proof requires a canonical 32-byte secret."
        ) from None
    if (
        len(decoded) != 32
        or base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=") != value
    ):
        raise InvalidLeaseProof("Lease proof requires a canonical 32-byte secret.")
    return decoded


class LeaseProof:
    """Opaque secret with explicit transport/journal access and redacted display.

    Never serialize whole request objects into receipts, responses or logs.
    Syntax validation does not establish entropy or durable journal storage.
    """

    __slots__ = ("_encoded",)
    _encoded: str

    def __init__(self, value: str) -> None:
        _decode_proof(value)
        object.__setattr__(self, "_encoded", value)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("Lease proofs are immutable.")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("Lease proofs are immutable.")

    def __repr__(self) -> str:
        return "LeaseProof('**********')"

    def __str__(self) -> str:
        return "**********"

    @classmethod
    def generate(cls) -> LeaseProof:
        """Generate ephemeral input; journal it durably before sending a claim."""
        return cls(token_urlsafe(32))

    def reveal(self) -> str:
        """Use only at the protected journal or authenticated transport boundary."""
        return self._encoded


def _proof_digest(worker_id: UUID, request_id: UUID, proof: LeaseProof) -> bytes:
    _identity(worker_id)
    _identity(request_id)
    if not isinstance(proof, LeaseProof):
        raise InvalidLeaseProof("A typed lease proof is required.")
    return sha256(
        b"devrelay.lease-proof.v1\x00"
        + worker_id.bytes
        + request_id.bytes
        + _decode_proof(proof.reveal())
    ).digest()


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class LeaseProofVerifier:
    """Persistable scoped verifier; compare through matches, never object equality."""

    worker_id: UUID
    claim_request_id: UUID
    digest: bytes = field(repr=False)
    version: Literal[1] = 1

    def __post_init__(self) -> None:
        _identity(self.worker_id)
        _identity(self.claim_request_id)
        _version(self.version)
        if type(self.digest) is not bytes or len(self.digest) != 32:
            raise InvalidLeaseProof("Lease verifier requires a fixed-size digest.")

    @classmethod
    def from_proof(
        cls, *, worker_id: UUID, claim_request_id: UUID, proof: LeaseProof
    ) -> LeaseProofVerifier:
        return cls(
            worker_id=worker_id,
            claim_request_id=claim_request_id,
            digest=_proof_digest(worker_id, claim_request_id, proof),
        )

    def matches(
        self, proof: LeaseProof, *, worker_id: UUID, claim_request_id: UUID
    ) -> bool:
        presented = _proof_digest(worker_id, claim_request_id, proof)
        verified = compare_digest(self.digest, presented)
        return (
            verified
            and worker_id == self.worker_id
            and claim_request_id == self.claim_request_id
        )


def _request_fingerprint(
    worker_id: UUID,
    request_id: UUID,
    protocol_version: int,
    verifier: LeaseProofVerifier,
) -> str:
    normalized = {
        "version": 1,
        "operation": "claim",
        "route": "/api/v1/worker/claims",
        "worker_id": str(worker_id),
        "claim_request_id": str(request_id),
        "protocol_version": protocol_version,
        "proof_verifier_version": verifier.version,
        "proof_verifier": verifier.digest.hex(),
    }
    return sha256(
        json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class ClaimRequestIdentity:
    """Secret-free receipt input; compare replay through require_replay."""

    worker_id: UUID
    claim_request_id: UUID
    protocol_version: Literal[1]
    verifier: LeaseProofVerifier = field(repr=False)
    fingerprint: str = field(repr=False)
    fingerprint_version: Literal[1] = 1

    def __post_init__(self) -> None:
        _identity(self.worker_id)
        _identity(self.claim_request_id)
        _version(self.protocol_version)
        _version(self.fingerprint_version)
        if (
            not isinstance(self.verifier, LeaseProofVerifier)
            or self.verifier.worker_id != self.worker_id
            or self.verifier.claim_request_id != self.claim_request_id
        ):
            raise InvalidClaimRequest("Claim verifier must match the receipt identity.")
        expected = _request_fingerprint(
            self.worker_id, self.claim_request_id, self.protocol_version, self.verifier
        )
        if (
            not isinstance(self.fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", self.fingerprint) is None
            or not compare_digest(expected, self.fingerprint)
        ):
            raise InvalidClaimRequest("Claim fingerprint is inconsistent.")

    def require_replay(self, presented: ClaimRequestIdentity) -> None:
        if not isinstance(presented, ClaimRequestIdentity):
            raise ClaimReplayConflict("Claim replay conflicts with the original input.")
        matches = compare_digest(self.fingerprint, presented.fingerprint)
        if (
            not matches
            or self.worker_id != presented.worker_id
            or self.claim_request_id != presented.claim_request_id
            or self.protocol_version != presented.protocol_version
            or self.fingerprint_version != presented.fingerprint_version
        ):
            raise ClaimReplayConflict("Claim replay conflicts with the original input.")


@dataclass(frozen=True, slots=True, kw_only=True, eq=False)
class WorkerClaimRequest:
    """Client input; trusted authentication alone supplies worker identity/scope."""

    claim_request_id: UUID
    proof: LeaseProof = field(repr=False)
    protocol_version: Literal[1] = 1

    def __post_init__(self) -> None:
        _identity(self.claim_request_id)
        _version(self.protocol_version)
        if not isinstance(self.proof, LeaseProof):
            raise InvalidLeaseProof("A typed lease proof is required.")

    def identify(self, authorization: WorkerAuthorization) -> ClaimRequestIdentity:
        """Normalize authenticated input; DB-time authority checks remain external."""
        if not isinstance(authorization, WorkerAuthorization):
            raise InvalidClaimRequest("Trusted worker authorization is required.")
        verifier = LeaseProofVerifier.from_proof(
            worker_id=authorization.worker_id,
            claim_request_id=self.claim_request_id,
            proof=self.proof,
        )
        return ClaimRequestIdentity(
            worker_id=authorization.worker_id,
            claim_request_id=self.claim_request_id,
            protocol_version=self.protocol_version,
            verifier=verifier,
            fingerprint=_request_fingerprint(
                authorization.worker_id,
                self.claim_request_id,
                self.protocol_version,
                verifier,
            ),
        )
