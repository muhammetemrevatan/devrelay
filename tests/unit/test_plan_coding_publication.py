"""Local canonical object/wire fixtures, not live GitHub byte compatibility."""

import base64
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid5

import pytest

from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.base_sources import (
    BaseFileMode,
    BaseSourceFile,
    git_blob_sha,
)
from devrelay.application.contracts.candidates import (
    CandidateCommitIdentity,
    CandidateCommitSpec,
)
from devrelay.application.contracts.coding_publications import (
    PublicationPlan,
    PublicationPolicy,
    PublicationWriteKind,
)
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import CodingResultReceipt
from devrelay.application.contracts.git_objects import GitObject
from devrelay.application.contracts.workers import WorkerAuthorization
from devrelay.application.plan_coding_publication import build_publication_plan
from tests.unit.test_coding_result_preparation import context as preparation_context
from tests.unit.test_coding_result_requests import Context as WireContext
from tests.unit.test_coding_result_wire import with_files


@dataclass
class Context:
    prepared: PreparedCodingResult
    authorization: WorkerAuthorization
    receipt: CodingResultReceipt
    recorded_at: datetime


def context(
    files: tuple[BaseSourceFile, ...] | None = None, *, repairs: int = 0
) -> Context:
    value = preparation_context(repairs)
    if files is not None:
        value.request = with_files(
            WireContext(value.request, value.authorization), files
        )
    prepared = prepare_coding_result(
        value.request, value.authorization, value.definition
    )
    intent = prepared.identity.intent
    accepted_at = value.definition.created_at + timedelta(seconds=6)
    receipt = CodingResultReceipt(
        result_id=UUID(int=6001),
        worker_id=value.authorization.worker_id,
        request_id=intent.result_request_id,
        claim_request_id=value.request.claim.claim_request_id,
        job_id=value.definition.job_id,
        attempt_id=intent.execution.acknowledgement.attempt_id,
        repository_id=value.definition.repository_id,
        execution_id=intent.execution.execution_id,
        definition_digest=value.definition.fingerprint(),
        request_fingerprint=prepared.identity.fingerprint,
        candidate_revision=intent.result.final_candidate.revision,
        repair_cycles=len(intent.result.cycles) - 1,
        publishing_workflow_version=4 + 2 * (len(intent.result.cycles) - 1),
        recorded_at=accepted_at,
    )
    return Context(prepared, value.authorization, receipt, accepted_at)


def file(
    path: str, content: bytes = b"fixture\n", *, executable: bool = False
) -> BaseSourceFile:
    return BaseSourceFile(
        path=path,
        content=content,
        blob_sha=git_blob_sha(content),
        mode=BaseFileMode.EXECUTABLE if executable else BaseFileMode.REGULAR,
    )


def plan(value: Context, policy: PublicationPolicy | None = None) -> PublicationPlan:
    return build_publication_plan(
        value.prepared, value.receipt, policy or PublicationPolicy()
    )


def test_objects_and_child_first_tree_requests_reconstruct_the_exact_checked_sha() -> (
    None
):
    value = context(
        (
            file("a.txt", b"same\n"),
            file("nested/deeper/tool.sh", b"echo fixture\n", executable=True),
            file("nested/other.txt", b"same\n"),
        )
    )
    result = plan(value)
    seen: set[str] = set()
    for write in result.writes[:-3]:
        body = json.loads(write.body)
        assert write.body == json.dumps(
            body, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("ascii")
        if write.kind is PublicationWriteKind.BLOB:
            raw = base64.b64decode(body["content"], validate=True)
            assert body["encoding"] == "base64"
            item = GitObject(kind="blob", data=raw)
        else:
            raw_tree = bytearray()
            assert set(body) == {"tree"}
            for entry in body["tree"]:
                assert entry["sha"] in seen
                mode = "40000" if entry["type"] == "tree" else entry["mode"]
                raw_tree.extend(
                    mode.encode()
                    + b" "
                    + entry["path"].encode("utf-8")
                    + b"\0"
                    + bytes.fromhex(entry["sha"])
                )
            item = GitObject(kind="tree", data=bytes(raw_tree))
        assert item.sha == write.expected_sha
        assert item in result.objects
        seen.add(item.sha)
    assert (
        len(
            [
                write
                for write in result.writes
                if write.kind is PublicationWriteKind.BLOB
            ]
        )
        == 2
    )
    candidate = value.prepared.identity.intent.result.final_candidate
    assert (
        result.head_revision == candidate.revision and candidate.root_tree_sha in seen
    )
    commit = json.loads(result.writes[-3].body)
    assert commit["parents"] == [result.base_revision]
    assert commit["tree"] == candidate.root_tree_sha
    stamp = int(datetime.fromisoformat(commit["author"]["date"]).timestamp())
    assert commit["author"]["date"] == datetime.fromtimestamp(stamp, UTC).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")
    spec = CandidateCommitSpec(
        author=CandidateCommitIdentity(
            name=commit["author"]["name"], email=commit["author"]["email"]
        ),
        committer=CandidateCommitIdentity(
            name=commit["committer"]["name"], email=commit["committer"]["email"]
        ),
        timestamp=stamp,
        message=commit["message"],
    )
    assert (
        spec.raw_commit(parent=result.base_revision, tree=commit["tree"])
        == candidate.raw_commit
    )


@pytest.mark.parametrize("repairs", [0, 2, 5])
def test_branch_and_pr_are_create_only_with_stable_result_scoped_ids(
    repairs: int,
) -> None:
    value = context(repairs=repairs)
    first, second = plan(value), plan(value)
    assert first == second
    for write in first.writes[:-2]:
        assert write.write_id == uuid5(
            first.result_id, write.kind.value + ":" + str(write.expected_sha)
        )
    branch, pr = first.writes[-2:]
    assert json.loads(branch.body) == {
        "ref": "refs/heads/" + first.source_branch,
        "sha": first.head_revision,
    }
    assert branch.path.endswith("/git/refs") and pr.path.endswith("/pulls")
    assert branch.write_id == uuid5(first.result_id, "branch:" + first.source_branch)
    assert pr.write_id == uuid5(
        first.result_id,
        "pull_request:" + first.source_branch + ":" + first.target_branch,
    )
    body = json.loads(pr.body)
    assert body["head"] == first.source_branch and body["base"] == first.target_branch
    assert body["draft"] is False and body["maintainer_can_modify"] is False
    assert len(body["title"]) <= 200
    assert str(first.result_id) in body["body"] and first.head_revision in body["body"]
    assert all(
        b"proof" not in write.body and b"claim_fingerprint" not in write.body
        for write in first.writes
    )


@pytest.mark.parametrize(
    "policy",
    [
        PublicationPolicy(max_writes=3),
        PublicationPolicy(max_requests=16),
        PublicationPolicy(max_request_bytes=1024),
    ],
)
def test_whole_plan_budgets_reject_before_a_plan_can_be_consumed(
    policy: PublicationPolicy,
) -> None:
    value = context((file("large.txt", b"x" * 2048),))
    with pytest.raises(ValueError):
        plan(value, policy)


def test_tree_object_count_is_included_in_whole_plan_budget() -> None:
    value = context(
        tuple(
            file(f"dir{index:03}/file.txt", str(index).encode()) for index in range(80)
        )
    )
    with pytest.raises(ValueError):
        plan(value)


def test_empty_candidate_keeps_the_real_empty_root_tree_object() -> None:
    value = context(())
    result = plan(value)
    assert result.writes[0].kind is PublicationWriteKind.TREE
    assert json.loads(result.writes[0].body) == {"tree": []}
    assert result.writes[0].expected_sha == GitObject(kind="tree", data=b"").sha


@pytest.mark.parametrize("corruption", ["payload", "receipt", "snapshot"])
def test_mutated_prepared_or_receipt_input_cannot_be_planned(corruption: str) -> None:
    value = context()
    if corruption == "payload":
        object.__setattr__(
            value.prepared, "payload_bytes", value.prepared.payload_bytes + b" "
        )
    elif corruption == "receipt":
        value.receipt = replace(value.receipt, candidate_revision="f" * 40)
    else:
        object.__setattr__(
            value.prepared.definition.snapshot, "title", "Mutated source title"
        )
    with pytest.raises(ValueError):
        plan(value)
