"""Native candidates from the coding driver through real result delivery/storage.

Coding/check invocations and GitHub responses are controlled fixtures. No process,
Codex/model, credential provisioning or sandbox certification occurs here.
"""

import asyncio
import sys
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from typing import cast
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from devrelay.api.dependencies import ClaimOperation
from devrelay.application.acknowledgements import AcknowledgeAssignedJob
from devrelay.application.assignment_delivery import ReadAssignedJob
from devrelay.application.candidate_workspaces import PrepareCandidateCapture
from devrelay.application.coding_result_preparation import prepare_coding_result
from devrelay.application.contracts.acknowledgements import (
    AcknowledgementResult,
    WorkerAcknowledgementRequest,
)
from devrelay.application.contracts.assignment_delivery import AssignmentDelivery
from devrelay.application.contracts.assignments import ClaimResult
from devrelay.application.contracts.claims import WorkerClaimRequest
from devrelay.application.contracts.coding_publications import PublicationPolicy
from devrelay.application.contracts.coding_result_requests import (
    WorkerCodingResultRequest,
)
from devrelay.application.contracts.coding_results import CodingCycleEvidence
from devrelay.application.contracts.editable_workspaces import EditableWorkspace
from devrelay.application.contracts.executions import (
    ExecutionIsolation,
    ExecutionPolicy,
    ExecutionRuntime,
    WorkerExecutionIntent,
)
from devrelay.application.contracts.frozen_candidates import FrozenCandidate
from devrelay.application.contracts.git_sources import GitBaseSourceSnapshot
from devrelay.application.contracts.processes import (
    ExecutionProcessBinding,
    ProcessStopStatus,
)
from devrelay.application.contracts.submissions import (
    CheckPolicy,
    PreparedJobSubmission,
    RepositoryHead,
    RepositoryPolicy,
)
from devrelay.application.contracts.tasks import StoredTaskSnapshot
from devrelay.application.contracts.trusted_processes import (
    TrustedProcessResult,
    TrustedProcessStopReason,
)
from devrelay.application.contracts.worker_recovery import AcknowledgementCompletion
from devrelay.application.contracts.workers import (
    WorkerAuthorization,
    WorkerJournalScope,
)
from devrelay.application.contracts.workspaces import WorkspaceBinding
from devrelay.application.editable_workspaces import HandoffMaterializedBase
from devrelay.application.git_workspaces import PrepareMaterializedGit
from devrelay.application.plan_coding_publication import build_publication_plan
from devrelay.application.ports.control_gateway import ControlGateway
from devrelay.application.tasks import TaskImportPolicy
from devrelay.application.worker_claims import PrepareWorkerClaim, RequestWorkerClaim
from devrelay.application.worker_coding import RunAssignedCoding
from devrelay.application.worker_workspaces import ReserveAssignedWorkspace
from devrelay.bootstrap import control
from devrelay.bootstrap.coding_publication import open_coding_publisher
from devrelay.bootstrap.control import create_app
from devrelay.bootstrap.persistence import (
    acknowledgement_store,
    claim_store,
    job_submission_unit_of_work_factory,
    task_unit_of_work_factory,
)
from devrelay.bootstrap.workspaces import open_worker_workspace
from devrelay.config.control import ControlSettings
from devrelay.config.github import GitHubRepositoryId
from devrelay.config.repositories import CheckSettings
from devrelay.config.worker_authentication import WorkerRegistrationSettings
from devrelay.domain.tasks import TaskIdentity, TaskSnapshot
from devrelay.infrastructure.control.coding_results import (
    open_http_control_coding_results,
)
from devrelay.infrastructure.persistence.result_store import SqlAlchemyResultStore
from devrelay.infrastructure.worker_state.journal import (
    AsyncClaimJournal,
    SqliteClaimStore,
)
from devrelay.infrastructure.workspace.windows import WindowsWorkspacePolicy
from tests.acknowledgement_fixtures import acknowledgement_intent
from tests.git_source_fixtures import git_source_fixture, git_workspace_inputs
from tests.integration.test_assignment_acknowledgement import Assigned
from tests.integration.test_atomic_claim_persistence import (
    authority,
    observe,
    run,
)
from tests.integration.test_atomic_claim_persistence import (
    request as claim_request,
)
from tests.integration.test_coding_publication_flow import (
    settings as publication_settings,
)
from tests.integration.test_coding_result_http import TOKEN
from tests.integration.test_coding_result_store import (
    Context,
    DatabaseFactory,
    projection,
)
from tests.integration.test_worker_journal import TestProtector, TestStorage
from tests.unit.test_candidate_objects import commit
from tests.unit.test_coding_publication_configuration import TOKEN as PUBLICATION_TOKEN
from tests.unit.test_github_coding_publication import GitHub
from tests.unit.test_worker_coding_results import Guard

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        sys.platform != "win32", reason="Actual Windows candidate scope and handles."
    ),
]


def _control_settings(
    original: ControlSettings, assigned: Assigned, delivery: AssignmentDelivery
) -> ControlSettings:
    """Configure from accepted inputs before any coding report exists."""
    definition = delivery.definition
    values = original.model_dump()
    assert original.github is not None
    values["github"] = original.github.model_copy(
        update={
            "repository_ids": (
                GitHubRepositoryId(
                    repository_id=definition.repository_id,
                    github_id=definition.provider_repository_id,
                ),
            ),
        }
    )
    values["repositories"] = (
        original.repositories[0].model_copy(
            update={
                "repository_id": definition.repository_id,
                "checks": tuple(
                    CheckSettings(
                        id=check.id,
                        argv=check.argv,
                        working_directory=check.working_directory,
                        timeout_seconds=check.timeout_seconds,
                    )
                    for check in definition.checks
                ),
            }
        ),
    )
    values["workers"] = (
        WorkerRegistrationSettings(
            worker_id=assigned.authorization.worker_id,
            auth_token=SecretStr(TOKEN),
            enabled=True,
            expires_at=assigned.authorization.credential_expires_at,
            allowed_repository_ids=frozenset({definition.repository_id}),
        ),
    )
    values["worker_claims"] = {}
    values["worker_coding_results"] = {}
    return ControlSettings.model_validate(values)


async def _assigned_real_base(engine: AsyncEngine) -> Assigned:
    # The existing save_job fixture deliberately uses an opaque SHA. This flow
    # needs independently materializable raw base bytes before the first edit.
    _, _, _, source = git_workspace_inputs()
    now = datetime.now(UTC)
    snapshot = TaskSnapshot(
        id=uuid4(),
        identity=TaskIdentity(
            provider="jira", site_id="test-site", external_id=uuid4().hex
        ),
        display_key="DRY-5",
        title="Exercise the controlled coding driver",
        description="Edit the fixture and pass every fixed check.",
        acceptance_criteria=("Publish the exact final checked candidate.",),
        captured_at=now,
    )
    async with task_unit_of_work_factory(engine)() as unit:
        saved = await unit.tasks.save_snapshot(snapshot)
        await unit.commit()
    policy = RepositoryPolicy(
        repository_id=uuid4(),
        provider_repository_id=123,
        slug="example/devrelay",
        enabled=True,
        target_branch="master",
        coding_branch_prefix="devrelay/jobs",
        version="v1",
        task_scope=TaskImportPolicy(
            provider="jira", site_id="test-site", allowed_scope_keys=frozenset({"DRY"})
        ),
        checks=tuple(
            CheckPolicy(
                id=f"check_{index}",
                argv=("pytest",),
                working_directory=".",
                timeout_seconds=30,
            )
            for index in range(2)
        ),
        max_repair_cycles=2,
    )
    intent = PreparedJobSubmission(
        stored=StoredTaskSnapshot(
            source_task_id=saved.source_task_id, snapshot=saved.snapshot
        ),
        policy=policy,
        actor_id="test-operator",
    )
    candidate = intent.build_candidate(
        job_id=uuid4(),
        head=RepositoryHead(
            repository_id=policy.repository_id,
            provider_repository_id=123,
            target_branch=policy.target_branch,
            revision=source.snapshot.request.base_revision,
        ),
        now=now,
    )
    async with job_submission_unit_of_work_factory(engine)() as unit:
        admitted = await unit.jobs.admit(intent, candidate)
        await unit.commit()
    stored = admitted.stored
    authorization, pending = authority(stored), claim_request()
    claims = claim_store(engine, (policy,))
    claimed = await claims.claim(
        pending.identify(authorization), authorization, observe(stored, authorization)
    )
    delivery = AssignmentDelivery.from_result(
        claimed,
        worker_id=authorization.worker_id,
        claim_request_id=pending.claim_request_id,
    )
    return Assigned(
        service=AcknowledgeAssignedJob(
            ReadAssignedJob(claims, lambda _: None),
            acknowledgement_store(engine, (policy,)),
        ),
        request=WorkerAcknowledgementRequest(
            intent=acknowledgement_intent(delivery), claim=pending
        ),
        authorization=authorization,
        claims=claims,
        stored=stored,
    )


@asynccontextmanager
async def _journal(
    directory: Path,
    setup: Assigned,
    delivery: AssignmentDelivery,
    execution: WorkerExecutionIntent,
    acknowledged: AcknowledgementResult,
) -> AsyncIterator[AsyncClaimJournal]:
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="coding-flow-state")
    loop = asyncio.get_running_loop()
    scope = WorkerJournalScope(
        worker_id=setup.authorization.worker_id, control_url="http://127.0.0.1/"
    )

    def initialize() -> SqliteClaimStore:
        store = SqliteClaimStore(directory, scope, TestProtector(), TestStorage())
        request = setup.request.claim
        store.get_or_create_pending(request)
        store.record_outcome(request, delivery.outcome)
        store.get_or_create_acknowledgement(request, execution.acknowledgement)
        store.record_acknowledgement(
            request,
            AcknowledgementCompletion.from_result(
                acknowledged, delivery.outcome, execution.acknowledgement
            ),
        )
        store.get_or_create_execution(request, execution)
        return store

    store = await loop.run_in_executor(executor, initialize)
    journal = AsyncClaimJournal(store, executor)
    try:
        yield journal
    finally:
        journal.stop_admission()
        await loop.run_in_executor(executor, store.close)
        await asyncio.to_thread(executor.shutdown, wait=True)


class _ControlledRuntime:
    """Harmless fixture operations, not native process or isolation evidence."""

    def __init__(self, binding: ExecutionProcessBinding, repairs: int) -> None:
        self.binding = binding
        self.repairs = repairs
        self.previous: list[CodingCycleEvidence | None] = []
        self.checks: list[tuple[int, str]] = []
        self.sealed = False

    def seal(self) -> None:
        self.sealed = True

    def _settled(self, exit_code: int) -> TrustedProcessResult:
        return TrustedProcessResult(
            execution_id=self.binding.intent.execution_id,
            reason=TrustedProcessStopReason.EXITED,
            exit_code=exit_code,
            stdout=b"Controlled fixture observation\n",
            stderr=b"",
            stop_status=ProcessStopStatus.EMPTY_VERIFIED,
        )

    async def code(
        self,
        delivery: AssignmentDelivery,
        editable: EditableWorkspace,
        *,
        cycle: int,
        previous: CodingCycleEvidence | None,
    ) -> TrustedProcessResult:
        assert not self.sealed
        assert (
            delivery.definition
            == editable.materialization.reservation.binding.definition
        )
        self.previous.append(previous)
        target = (
            Path(editable.location.root_directory)
            / editable.materialization.reservation.directory_name
            / "README.md"
        )
        await asyncio.to_thread(
            target.write_bytes, f"Controlled native candidate cycle {cycle}\n".encode()
        )
        return self._settled(0)

    async def check(
        self, candidate: FrozenCandidate, check: CheckPolicy
    ) -> TrustedProcessResult:
        assert not self.sealed
        self.checks.append((candidate.snapshot.cycle, check.id))
        target = Path(candidate.worktree) / "README.md"
        observed = await asyncio.to_thread(target.read_bytes)
        assert observed == (
            f"Controlled native candidate cycle {candidate.snapshot.cycle}\n".encode()
        )
        # Exercise the actual source handles retained by the native capture.
        # This does not certify an entire runtime's readonly policy.
        with pytest.raises(PermissionError):
            await asyncio.to_thread(target.write_bytes, b"Forbidden fixture mutation\n")
        failed = candidate.snapshot.cycle < self.repairs and check.id == "check_0"
        return self._settled(int(failed))


@pytest.mark.parametrize("repairs", [0, 1])
def test_coding_driver_native_candidate_to_http_publication_and_terminal_recovery(
    database: DatabaseFactory,
    worker_configuration: ControlSettings,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repairs: int,
) -> None:
    async def scenario(root: Path) -> None:
        async with database() as engine:
            assigned = await _assigned_real_base(engine)
            acknowledged = await assigned.service.execute(
                assigned.request, assigned.authorization
            )
            delivery = await ReadAssignedJob(assigned.claims, lambda _: None).execute(
                assigned.request.intent.attempt_id,
                assigned.request.claim,
                assigned.authorization,
            )
            reference = delivery.outcome.assignment
            assert reference is not None
            guard = Guard(reference, [])
            execution = WorkerExecutionIntent(
                execution_id=uuid4(),
                acknowledgement=assigned.request.intent,
                policy=ExecutionPolicy(
                    runtime=ExecutionRuntime.CODEX,
                    isolation=ExecutionIsolation.WINDOWS_RESTRICTED,
                    policy_digest="a" * 64,
                ),
            )
            binding = WorkspaceBinding(
                execution=ExecutionProcessBinding(
                    intent=execution, reference=reference
                ),
                definition=delivery.definition,
            )
            fixture = git_source_fixture(binding)
            assert fixture.snapshot.raw_commit is not None
            assert (
                fixture.snapshot.request.base_revision
                == delivery.definition.base_revision
            )
            source = GitBaseSourceSnapshot(
                snapshot=fixture.snapshot, raw_commit=fixture.snapshot.raw_commit
            )
            policy = WindowsWorkspacePolicy(
                root_directory=str(root), protected_directories=(str(tmp_path),)
            )
            async with (
                _journal(
                    tmp_path, assigned, delivery, execution, acknowledged
                ) as local,
                open_worker_workspace(binding, policy) as workspace,
            ):
                reservation = await ReserveAssignedWorkspace(
                    local, guard, workspace
                ).execute(delivery)
                materialization = await workspace.materialize(
                    reservation, fixture.snapshot
                )
                preparation = await PrepareMaterializedGit(
                    local, guard, workspace
                ).execute(delivery, materialization, source)
                retained = await PrepareCandidateCapture(
                    local, guard, workspace
                ).execute(delivery, materialization)
                editable = await HandoffMaterializedBase(
                    local, guard, workspace
                ).execute(delivery, materialization)
                runtime = _ControlledRuntime(binding.execution, repairs)
                runner = RunAssignedCoding(
                    workspace, runtime, guard, monotonic_clock=monotonic
                )

                async def claim(
                    request: WorkerClaimRequest, authorization: WorkerAuthorization
                ) -> ClaimResult:
                    raise AssertionError("Result delivery cannot claim another job.")

                @asynccontextmanager
                async def claims() -> AsyncIterator[ClaimOperation]:
                    yield claim

                @asynccontextmanager
                async def existing_database(
                    settings: object,
                ) -> AsyncIterator[AsyncEngine]:
                    yield engine

                monkeypatch.setattr(control, "open_database_engine", existing_database)
                app = create_app(
                    _control_settings(worker_configuration, assigned, delivery),
                    claim_resources=claims,
                )
                async with (
                    app.router.lifespan_context(app),
                    open_http_control_coding_results(
                        "http://127.0.0.1/",
                        TOKEN,
                        transport=httpx.ASGITransport(app=app),
                    ) as gateway,
                ):
                    worker = RequestWorkerClaim(
                        PrepareWorkerClaim(local, result_journal=local),
                        cast(ControlGateway, object()),
                        assigned.authorization,
                        monotonic_clock=monotonic,
                        coding_result_gateway=gateway,
                    )
                    # Reuse the already authenticated fixture authority. No
                    # renewal or claim transport runs during the composed call.
                    monkeypatch.setattr(worker, "heartbeat_supervisor", lambda: guard)
                    try:
                        accepted = await worker.run_coding_result(
                            delivery,
                            runner,
                            execution,
                            retained,
                            editable,
                            commit=commit(),
                        )
                        after_acceptance = await local.load_result_state()
                        assert after_acceptance.intent is not None
                        pending = WorkerCodingResultRequest(
                            intent=after_acceptance.intent, claim=assigned.request.claim
                        )
                        report = pending.intent.result
                        prepared = await asyncio.to_thread(
                            prepare_coding_result,
                            pending,
                            assigned.authorization,
                            delivery.definition,
                        )
                        setup = Context(
                            assigned,
                            pending,
                            prepared,
                            SqlAlchemyResultStore(
                                async_sessionmaker(
                                    engine, expire_on_commit=False, autoflush=False
                                ),
                                (assigned.stored.intent.policy,),
                            ),
                        )
                        plan = await asyncio.to_thread(
                            build_publication_plan,
                            prepared,
                            accepted.receipt,
                            PublicationPolicy(),
                        )
                        github = GitHub(plan, token=PUBLICATION_TOKEN)
                        async with open_coding_publisher(
                            publication_settings(worker_configuration, setup),
                            engine,
                            transport=httpx.MockTransport(github),
                        ) as publisher:
                            terminal = await publisher.execute(
                                prepared, assigned.authorization
                            )
                            replay = await publisher.execute(
                                prepared, assigned.authorization
                            )
                        guard.active = False
                        recovered = await worker.recover_coding_result()
                        durable = await local.load_result_state()
                    finally:
                        await worker.close_coding_results()
                current = await projection(engine)

                assert retained is preparation
                assert runtime.sealed
                assert len(runtime.previous) == len(report.cycles) == repairs + 1
                assert runtime.previous[0] is None
                assert all(
                    cycle is not None and not cycle.passed
                    for cycle in runtime.previous[1:]
                )
                assert runtime.checks == [
                    (cycle, check.id)
                    for cycle in range(repairs + 1)
                    for check in delivery.definition.checks
                ]
                assert not accepted.recovered
                assert replay == terminal
                assert (
                    terminal.reference.head_revision == report.final_candidate.revision
                )
                assert recovered is not None and recovered.terminal == terminal
                assert (
                    durable.intent == pending.intent
                    and durable.terminal_status == recovered
                )
                assert current["repair_cycles"] == repairs
                assert current["workflow_version"] == 5 + 2 * repairs
                assert (
                    current["results"]
                    == current["operations"]
                    == current["release_count"]
                    == 1
                )
                assert current["reserved_attempt_id"] is None
                assert sum(
                    request.method == "POST" for request in github.requests
                ) == len(plan.writes)

    artifacts = Path(__file__).resolve().parents[4] / "artifacts"
    with TemporaryDirectory(prefix="dr-code-flow-", dir=artifacts) as directory:
        root = Path(directory) / "w"
        root.mkdir()
        run(scenario(root))
