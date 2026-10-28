"""Rebuild a complete create-only publication plan before external writes."""

import base64
import hashlib
import hmac
import json
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid5

from devrelay.application.coding_result_preparation import compile_coding_checks
from devrelay.application.contracts.candidates import parse_candidate_commit
from devrelay.application.contracts.coding_publications import (
    PublicationPlan,
    PublicationPolicy,
    PublicationWrite,
    PublicationWriteKind,
)
from devrelay.application.contracts.coding_result_acceptance import PreparedCodingResult
from devrelay.application.contracts.coding_result_receipts import CodingResultReceipt
from devrelay.application.contracts.coding_result_requests import CodingResultIdentity
from devrelay.application.contracts.coding_result_wire import (
    decode_coding_result_intent,
    encode_coding_result_intent,
)
from devrelay.application.contracts.git_objects import GitObject, build_git_tree


def _body(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("ascii")


def _tree_entries(item: GitObject) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    offset = 0
    while offset < len(item.data):
        space = item.data.index(b" ", offset)
        nul = item.data.index(b"\0", space + 1)
        mode = item.data[offset:space].decode("ascii")
        name = item.data[space + 1 : nul].decode("utf-8")
        sha = item.data[nul + 1 : nul + 21]
        if mode not in ("40000", "100644", "100755") or len(sha) != 20:
            raise ValueError("Publication tree contains an unsupported Git entry.")
        entries.append(
            {
                "path": name,
                "mode": "040000" if mode == "40000" else mode,
                "type": "tree" if mode == "40000" else "blob",
                "sha": sha.hex(),
            }
        )
        offset = nul + 21
    return entries


def _write(
    result_id: UUID,
    kind: PublicationWriteKind,
    path: str,
    value: object,
    *,
    sha: str | None = None,
    identity: str | None = None,
) -> PublicationWrite:
    return PublicationWrite(
        write_id=uuid5(result_id, kind.value + ":" + (identity or sha or "")),
        kind=kind,
        path=path,
        body=_body(value),
        expected_sha=sha,
    )


def build_publication_plan(
    prepared: PreparedCodingResult,
    receipt: CodingResultReceipt,
    policy: PublicationPolicy,
) -> PublicationPlan:
    """Synchronous full-byte validation; schedule outside SQL and the event loop.

    The full write/request/event budget is validated before returning a plan.
    REST byte compatibility still requires the controlled provider fixture;
    returned/read-back SHAs may never replace this checked candidate SHA.
    """
    if not isinstance(prepared, PreparedCodingResult) or not isinstance(
        policy, PublicationPolicy
    ):
        raise ValueError("Publication requires prepared original input and policy.")
    prepared.__post_init__()
    policy.__post_init__()
    payload = prepared.payload_bytes
    if not hmac.compare_digest(
        hashlib.sha256(payload).hexdigest(), prepared.payload_sha256
    ):
        raise ValueError("Publication payload changed after preparation.")
    intent = decode_coding_result_intent(payload)
    if encode_coding_result_intent(intent) != payload:
        raise ValueError("Publication requires canonical retained result bytes.")
    identity = CodingResultIdentity(intent=intent, claim=prepared.identity.claim)
    identity.require_replay(prepared.identity.fingerprint)
    definition = replace(
        prepared.definition,
        snapshot=replace(
            prepared.definition.snapshot,
            identity=replace(prepared.definition.snapshot.identity),
        ),
        checks=tuple(replace(check) for check in prepared.definition.checks),
    )
    intent.result.validate(definition)
    if compile_coding_checks(intent.result) != prepared.cycle_checks:
        raise ValueError("Publication checks changed the retained result.")
    receipt = replace(receipt)
    receipt.require_input(identity, job_id=definition.job_id)
    candidate = intent.result.final_candidate
    tree = build_git_tree(candidate.files)
    if tree.root_tree_sha != candidate.root_tree_sha:
        raise ValueError("Publication tree changed the checked candidate.")
    by_sha = {item.sha: item for item in tree.objects}
    ordered = sorted(
        (item for item in tree.objects if item.kind == "blob"),
        key=lambda item: item.sha,
    )
    visited: set[str] = set()
    tree_bodies: dict[str, list[dict[str, str]]] = {}

    def visit(sha: str) -> None:
        if sha in visited:
            return
        visited.add(sha)
        item = by_sha[sha]
        entries = _tree_entries(item)
        for entry in entries:
            child = by_sha[entry["sha"]]
            if child.kind != entry["type"]:
                raise ValueError("Publication tree changed its exact child object.")
            if child.kind == "tree":
                visit(entry["sha"])
        tree_bodies[sha] = entries
        ordered.append(item)

    visit(candidate.root_tree_sha)
    prefix = f"repos/{definition.repository_slug}/"
    writes: list[PublicationWrite] = []
    for item in ordered:
        if item.kind == "blob":
            value: object = {
                "content": base64.b64encode(item.data).decode("ascii"),
                "encoding": "base64",
            }
        else:
            value = {"tree": tree_bodies[item.sha]}
        writes.append(
            _write(
                receipt.result_id,
                PublicationWriteKind(item.kind),
                prefix + "git/" + item.kind + "s",
                value,
                sha=item.sha,
            )
        )
    spec = parse_candidate_commit(
        candidate.raw_commit,
        parent=candidate.request.base_revision,
        tree=candidate.root_tree_sha,
    )
    date = (
        datetime.fromtimestamp(spec.timestamp, UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    commit = GitObject(kind="commit", data=candidate.raw_commit)
    if commit.sha != receipt.candidate_revision:
        raise ValueError("Publication commit differs from the accepted candidate.")
    ordered.append(commit)
    writes.append(
        _write(
            receipt.result_id,
            PublicationWriteKind.COMMIT,
            prefix + "git/commits",
            {
                "message": spec.message,
                "tree": candidate.root_tree_sha,
                "parents": [candidate.request.base_revision],
                "author": {
                    "name": spec.author.name,
                    "email": spec.author.email,
                    "date": date,
                },
                "committer": {
                    "name": spec.committer.name,
                    "email": spec.committer.email,
                    "date": date,
                },
            },
            sha=candidate.revision,
        )
    )
    writes.append(
        _write(
            receipt.result_id,
            PublicationWriteKind.BRANCH,
            prefix + "git/refs",
            {
                "ref": "refs/heads/" + definition.authorized_branch,
                "sha": candidate.revision,
            },
            sha=candidate.revision,
            identity=definition.authorized_branch,
        )
    )
    # Snapshot text is labelled display data, never a provider/agent instruction.
    label = " ".join(definition.snapshot.display_key.split())[:128]
    title = " ".join(f"Implement {label}: {definition.snapshot.title}".split())[:200]
    description = (
        "DevRelay coding result ready for human review.\n\n"
        f"Task: {label}\nJob: {definition.job_id}\n"
        f"Result: {receipt.result_id}\nCandidate: {candidate.revision}\n"
    )
    writes.append(
        _write(
            receipt.result_id,
            PublicationWriteKind.PULL_REQUEST,
            prefix + "pulls",
            {
                "title": title,
                "body": description,
                "head": definition.authorized_branch,
                "base": definition.target_branch,
                "draft": False,
                "maintainer_can_modify": False,
            },
            identity=definition.authorized_branch + ":" + definition.target_branch,
        )
    )
    return PublicationPlan(
        result_id=receipt.result_id,
        repository_id=definition.repository_id,
        provider_repository_id=definition.provider_repository_id,
        repository_slug=definition.repository_slug,
        source_branch=definition.authorized_branch,
        target_branch=definition.target_branch,
        head_revision=candidate.revision,
        base_revision=candidate.request.base_revision,
        objects=tuple(ordered),
        writes=tuple(writes),
        policy=replace(policy),
    )
