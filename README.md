# DevRelay

DevRelay connects Jira tasks to coding agents, pull requests, and deployments,
while keeping code review and merge decisions with the developer.

## Status

The project is in early development. The current scaffold provides a control
HTTP liveness endpoint, separate validated control/worker configuration,
immutable task snapshots, a read-only Jira Cloud adapter tested with simulated
responses, task import with PostgreSQL storage and Alembic migrations, and an
authenticated import endpoint that is disabled by default.
The delivery domain also validates checks, review, release CI and deployment
completion, with explicit failure, retry and cancellation rules.
Job submission validates current task readiness against an allowed repository
and configured check policy, then atomically records the complete queued definition.
Repeated submissions resolve the original Job; conflicting policies/requirements
cannot create another unresolved delivery for that task/repository.
A read-only GitHub observer verifies configured repository IDs and target commits,
and reconciles open pull requests with stored Job/task branch associations.
Its transport is fixture-tested; no live account connection is implied.
An optional Job submission API uses a separate operator token and explicit
repository scope to record queued Jobs. It is disabled by default.
Optional configured worker authentication verifies a separate expiring token
and returns only the authenticated worker's identity and repository scope.
The domain models assignment acknowledgement, lease renewal/revocation and expiry.
Expired or revoked assignments retain their reservation until a trusted stop is
confirmed. Durable initial claims now serialize assignment through one coding slot;
lease expiry retains that slot and the unresolved Job guard.
The Windows recovery adapter protects pending claim proofs with user-scoped DPAPI
and commits them to a private local SQLite journal before returning a request.
It restores the original request after restart and excludes a second worker process.
The local journal also commits correlated no-work completion or stable assignment
references. No-work permits a new request; an assignment retains the original
protected proof and blocks a new poll identity, including after restart or lease
expiry. Repeated replies cannot replace or renew it. Journal version 4 upgrades
valid version-1 pending, version-2 recovery and version-3 intent records without changing their
ciphertext, original request identity or assignment anchors;
damaged records fail closed. Recorded assignment metadata is authenticated with a
local proof-bound integrity tag. Acknowledgement completion retains historical
evidence without launch authority. An opt-in claim
API and bounded worker HTTP adapter now connect durable claims to local recovery.
An assigned worker can also retrieve its complete immutable task/check inputs
using the original protected claim proof. Payload reads never claim another Job.
Explicit one-task Linux execution and scoped result publication are implemented.
Live account/model acceptance, automatic polling, terminal claim rotation and
deployment automation remain outstanding.

Control-side claim preparation can now read one scoped queued candidate and
recheck Jira requirements, current repository policy, numeric repository identity,
pinned target revision and conflicting pull requests before assignment. Provider
reads run after the database session is released; the observation expires after
30 seconds. This is an eligibility observation, not a reservation or permission
to execute. An explicit control use case commits the Attempt, claimed workflow,
history event, slot and immutable request receipt in one PostgreSQL transaction.
Exact replay recovers the original assignment or no-work result, skips provider
reads and never renews a lease. Current scope/credential expiry and observation
freshness are checked with database time after locks. New polls have a durable
30-second boundary and finite receipt/enrollment limits; quota exhaustion blocks
new requests while preserving recovery. Proofs remain worker-owned; the control
stores scoped verification digests rather than recoverable secrets.
Migration 0006 supports only initial claims and refuses downgrade after claim
history exists. Apply migrations explicitly with old writers stopped.
Control acknowledgement now validates accepted inputs and atomically advances
Attempt/workflow/history with an immutable replay receipt. An authenticated HTTP
exchange records completion in the worker journal before returning. Bounded
heartbeat exchange, input recovery and explicit bounded supervision are implemented;
containment and agent execution remain pending.

## Planned workflow

1. Prepare a Jira task with a description and acceptance criteria.
2. Add the `agent-ready` label when it is ready for execution.
3. The developer explicitly submits it for an allowed repository; DevRelay assigns
   the authorized job to a local coding worker.
4. The worker makes changes, runs checks, and opens a pull request.
5. The developer reviews and merges the pull request.
6. Successful CI triggers deployment through Dokploy and verifies health.

The control service and PostgreSQL run on the VPS. The coding worker runs
locally; jobs wait while it is offline.

## Local setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run
from the repository root:

```shell
uv sync --locked
uv run devrelay-control
```

The project pins Python 3.14.8. uv can provision the pinned interpreter if needed.
The control service defaults to `127.0.0.1:8000`.

```shell
curl http://127.0.0.1:8000/health/live
```

The response `{"status":"ok"}` reports process liveness only. It does not assert
database connectivity or job-processing readiness.

Environment variables use separate `DEVRELAY_CONTROL_` and `DEVRELAY_WORKER_`
prefixes. To load a local environment file explicitly:

```shell
uv run devrelay-control --env-file .env
```

`.env.example` contains examples; actual credentials must never be committed.
Worker configuration can be validated with:

```shell
uv run devrelay-worker --check-config --env-file .env
```

This command does not enroll a worker, verify a server credential, contact the
control service or execute jobs. HTTPS is required for non-loopback control URLs.
Local HTTP is allowed for loopback access through a protected tunnel.

## Jira reads

The reader resolves a selected task key or reads a stable task ID within one
configured Jira Cloud site and project allowlist. It captures readiness labels;
the reader itself never executes tasks or changes Jira.

Optional Jira settings use `DEVRELAY_CONTROL_JIRA__` followed by the setting name.
The commented group in `.env.example` shows the required site URL, cloud ID,
account email, API token, token mode and allowed project keys. Keep tokens in
local/deployment secrets and verify the cloud ID matches the configured site.
Scoped tokens use the Atlassian API gateway; unscoped tokens use the site URL.
Jira and database resources are opened only when the import API is explicitly
configured. Startup creates no schema and does not read Jira or verify live
integration credentials.

By default, acceptance criteria come from the list immediately after an
`Acceptance criteria` heading or paragraph in the description. Configure
`ACCEPTANCE_CRITERIA_FIELD` to use an explicit custom field instead; its supported
forms are text lines, a string list, or supported ADF blocks. A configured field
must be present in the response. Missing criteria are captured for review and
do not make a task eligible for execution.

Descriptions are mapped to bounded text with lists and references preserved.
Unsupported rich content produces a validation error rather than dropping
requirements. Reads have total I/O deadlines and response limits; they do not
retry automatically. Live account validation remains a separate setup step.

## Authenticated task import

Configure all three optional groups: `DEVRELAY_CONTROL_JIRA__`,
`DEVRELAY_CONTROL_DATABASE__`, and `DEVRELAY_CONTROL_TASK_IMPORT__AUTH_TOKEN`.
The route is absent unless import authentication is configured; incomplete
integration settings fail startup validation. Generate the operator token locally:

```shell
uv run --locked python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Store it in local/deployment secrets. Use a separate token for this operation;
it grants task import only and does not enroll workers, execute agents or modify
Jira. It must not be shared with coding agents or reused as a Jira/DB credential.
To rotate or revoke it, replace/remove the setting and restart the service.

Send the token only in the Authorization header:

```http
POST /api/v1/tasks/import
Authorization: Bearer <import-token>
Content-Type: application/json

{"task_key":"DRY-5"}
```

The response contains task/snapshot IDs, the content fingerprint and requirement
diagnostics. New content returns 201; repeated content returns 200 with the
original snapshot. Missing/wrong credentials return 401, disallowed scope 403,
and a missing task 404. Source failures return 502/503; storage failures return
503, without vendor errors or secrets in the response. Request validation errors
return 422 without reflecting input. Import never starts a coding job.

Bodies must be uncompressed JSON and at most 1 KiB, including chunked requests.
Body reads have a default five-second deadline. At most four imports per process
are admitted by default (configurable from one to four); additional requests get
503 with Retry-After, without waiting in an unbounded queue. These limits do not
replace network/proxy rate limiting.

Use HTTPS for remote access or HTTP only through a protected loopback SSH/VPN
tunnel. Keep the server/firewall access restrictions when deploying; a bearer
token does not encrypt traffic. The control CLI disables URL access logging so
accidentally supplied query credentials are not written by that logger. Tokens
in query parameters, cookies or request bodies are never accepted.

## Authenticated Job submission

Configure `DEVRELAY_CONTROL_JOB_SUBMISSION__AUTH_TOKEN`, `ACTOR_ID`, and
`ALLOWED_REPOSITORY_IDS`, plus complete Jira, database, GitHub observation and
repository policy settings. The allowed UUIDs must refer to enabled policies.
Generate a new token using the local command above; do not reuse the import,
Jira, GitHub or database credential. The actor and repository permissions are
server-owned settings, not request fields. Rotate/revoke the token through
configuration and restart. This initial credential has no automatic expiry or
multi-user account management and must not be given to a coding agent.

```http
POST /api/v1/jobs
Authorization: Bearer <submission-token>
Content-Type: application/json

{"snapshot_id":"<imported-snapshot-uuid>","repository_id":"<allowed-repository-uuid>"}
```

Only the two UUID fields are accepted. The control service resolves the stored
snapshot, checks caller/repository scope and current Jira requirements/readiness,
then verifies the configured GitHub repository, target head and open task PRs.
Branches, checks, policy, actor and base revision cannot be supplied by the client.

A new durable queued Job returns 201. An authorized exact replay returns 200
and the original Job without new provider reads. The response contains `job_id`,
`source_task_id`, `snapshot_id`, `repository_id`, `status` and `created`.
Both paths use `Cache-Control: no-store`. Missing/wrong credentials return 401,
disallowed scope 403, and a missing snapshot 404. Changed requirements, missing
readiness or conflicting deliveries return 409; provider/storage failures return
502/503. Input rejection and unexpected failures use the same sanitized error
envelope as task import.

Bodies must be uncompressed JSON and at most 1 KiB, including chunked bodies.
Authentication precedes body reads; reads have a default five-second deadline.
At most four submissions per process are admitted by default (configurable from
one to four); excess requests receive 503 with `Retry-After`. Import and submission
have separate admission limits and share one bounded control database pool.
Use HTTPS or a protected loopback tunnel and preserve network access restrictions.

The route is absent unless submission authentication is explicitly configured.
Startup opens configured resources without provider reads, schema migration or
database readiness checks. Submission records queued definitions only.
Fresh pre-execution reconciliation,
dispatch and execution remain later steps.

## Configured worker identity

Worker identity verification is disabled by default. Set
`DEVRELAY_CONTROL_WORKERS` to an explicit JSON array of registrations containing
`worker_id`, a separately generated `auth_token`, an aware `expires_at`, `enabled`
and `allowed_repository_ids`. There may be at most 32 registrations, each scoped
to 1..32 enabled repository policies with complete GitHub numeric configuration.
Worker IDs and tokens must be unique; operator and integration credentials cannot
be reused. Registrations default to disabled.

Generate each worker token locally using `secrets.token_urlsafe(32)`, as in the
operator setup command. Tokens must be 43..128 URL-safe characters; placeholders
are rejected. Set the same token in the local `DEVRELAY_WORKER_AUTH_TOKEN` secret.
`devrelay-worker --check-config` validates local configuration only and makes no
control request. Keep transport on HTTPS or a protected loopback tunnel.

```http
GET /api/v1/worker/identity
Authorization: Bearer <worker-token>
```

The endpoint derives the worker UUID from the token and returns only its own
repository scope, credential expiry and protocol version 1, with
`Cache-Control: no-store`. Body/query/cookie identity or tokens grant no authority.
Wrong, expired and disabled credentials receive the same sanitized 401 response.
Expiry is checked against control-process UTC time on every request. Expired
registrations can load so the service can restart, but cannot authenticate.

To rotate or revoke a token or change scope, replace/remove/disable its
registration and restart every control process. An old process retains its
loaded policy until restarted. No registration or token issuance API is exposed.
The identity endpoint creates no session, lease or assignment and opens no
database or provider resources when claims/import/submission are disabled.
Claims independently verify authority and expiry using database time.
Heartbeat fencing and explicit maintenance are implemented. Execution and agent
isolation remain pending.

Neutral claim contracts provide redacted lease-proof values, worker/request-bound
verifiers and versioned replay fingerprints. These helpers do not expose a claim
endpoint or persist assignments. The protected Windows journal prepares and
recovers the original pending or assigned request; the optional API delegates
assignment to atomic PostgreSQL coordination.

The journal composition requires an explicit absolute directory on a local fixed
Windows drive, outside repository checkouts and agent workspaces. Its parent must
exist; the adapter creates the private child directory with access restricted to
the current Windows identity and SYSTEM. Existing insecure directories, missing
journals and damaged records fail closed. Proofs are encrypted before SQLite
writes, including WAL storage, and disk operations run on a dedicated thread.
There is no plaintext or non-Windows fallback. DPAPI protects data at rest;
code running as the same Windows identity can decrypt it, so agent isolation
remains a separate requirement. This adapter is not wired to worker polling or
the CLI. Local acknowledgement intent is implemented as described below;
control acknowledgement is implemented, while launch intent remains pending.

## Worker claim exchange

Claims are disabled by default, independently of the identity endpoint. Set
`DEVRELAY_CONTROL_WORKER_CLAIMS={}` only with complete Jira/database/GitHub
configuration and an enabled worker registration. The optional settings
`max_concurrent_requests` (1..4, default 4) and `body_timeout_seconds` (0.1..15,
default 5) bound admission/body reads. Composition shares the control database
pool and closes provider resources on shutdown; startup performs no claim,
provider read or migration.

`POST /api/v1/worker/claims` uses the separate worker bearer token. Its JSON body
contains only `claim_request_id`, `protocol_version: 1` and the original canonical
lease `proof`. Authentication precedes bounded JSON reads (1 KiB). Body-supplied
worker/repository identities are rejected. Responses carry the authenticated
worker/request IDs, recorded timestamp, stable assignment references or a typed
no-work reason, `recovered` and `lease_authority_active`. They never return the
proof, token or verification digest. Active metadata is not permission to launch;
this initial endpoint provides recovery references, not task/check execution
payloads. The separate assignment endpoint below retrieves those accepted inputs.

Exact replay recovers the original receipt without another provider read or lease
renewal. Conflicting input receives 409; denied scope receives 403; durable
poll/receipt-budget restrictions receive 429. Body/transport/storage errors use
the shared sanitized envelope. Keep the original local request after any failure;
quota exhaustion requires operator attention, not automatic receipt deletion.

Worker bootstrap `open_worker_claim_client` verifies control identity before
opening its protected journal and exposes a single explicit exchange. It uses
verified TLS (plain HTTP only on loopback), disables environment proxies and
redirects, and bounds each response to 16 KiB and each request to 60 seconds.
It validates the complete response, correlation, scope, protocol and initial
lease bounds before committing local completion. There are no automatic retries
or polling; concurrent exchanges are rejected. A lost response/restart retains
the original request/proof; assigned recovery never creates another poll, even
after expiry. This composition performs no claim until explicitly invoked and
does not expose an execution CLI.

An assigned worker may call
`POST /api/v1/worker/attempts/{attempt_id}/assignment` with the same request UUID,
protocol and proof. This read-only endpoint recovers an existing receipt only;
unknown/no-work requests cannot claim work. Current credential/repository scope
and the original proof are required. The response includes the immutable task
snapshot, GitHub numeric identity/slug, authorized and target branches, pinned base
revision, full check argv/directories/timeouts and repair budget. It omits operator
audit data and all service credentials. Task text remains untrusted data and never
supplies commands. A definition fingerprint covers the accepted content and pins;
the worker recomputes it and checks the original journal anchors before returning.

Assignment responses are bounded to 2 MiB. When claims are enabled, the complete
serialized check policy for each enabled repository must fit 512 KiB; larger
policies are rejected during configuration validation. These limits accommodate
the maximum task fields, including JSON escaping. Unsupported oversized historical
definitions fail closed without returning a partial payload or releasing a slot.
Serialization/validation runs off the async event loop. Payload reads preserve the
original request, receipt, expiry and journal phase. Expired assignments remain
readable with renewed current authentication and inactive lease metadata. Retrieval
does not recheck live providers, acknowledge work, renew authority or launch an
agent. Heartbeat and containment still precede execution.

## Worker acknowledgement intent

The composed worker client exposes `prepare_acknowledgement()` to retrieve and
validate an active assigned definition, then durably pin its snapshot UUID, base
revision and complete definition digest under one acknowledgement request UUID.
It also records the original claim/Attempt identity, generation, protocol and
expected initial workflow version. The original protected proof and lease anchors
remain in the journal. Task text and check payloads are not copied into recovery
storage.

The journal commits this input and the `acknowledgement_pending` phase
atomically. Proof-bound integrity links the intent to its assignment; missing or
changed pins and attempts to erase the intent by regressing the phase fail closed.
After restart, preparation returns the same recorded input without HTTP, a new
identity or renewed authority. Duplicate claim responses preserve pending intent.
Later payload reads must still match its accepted definition digest.

Valid version-1 through version-6 journals upgrade to version 7 in one transaction, preserving protected
proof bytes, original identities, assignment anchors and no-work markers. Damaged
state or an incomplete upgrade retains the old schema and fails closed. SQLite,
DPAPI and integrity checks remain on the dedicated journal thread.

This step prepares local input only. The control now implements the atomic
claimed-to-running transition and acknowledgement receipt described below.
Its HTTP exchange and worker response completion are described below.
Contained process stopping and runtime integration
are required before launching an agent; the CLI remains a
configuration validator.

## Worker acknowledgement exchange

With worker claims enabled, `POST /api/v1/worker/attempts/{attempt_id}/acknowledgements`
accepts an `intent` object containing the recorded acknowledgement/claim/Attempt
UUIDs, generation, snapshot UUID, base revision, full definition digest and the
protocol/digest/expected workflow versions (all initially 1), plus the original
`proof`. It uses the worker bearer token and shares claim admission, authentication
before body reads, the 1-KiB UTF-8 JSON limit, deadline and sanitized error boundary.
The body Attempt must match the path. Unknown fields, duplicate keys and coerced
versions are rejected. Responses wrap the nonsecret `AcknowledgementResult` in
`result`, preserve original expiry and report current recovery/authority metadata.

The explicit worker `acknowledge()` call commits intent before sending HTTP,
validates the returned original pins, identity, scope and lease anchors, then
atomically records `acknowledged` completion before returning. Version 4 retains
the protected claim and intent; the bounded completion row stores only the first
acknowledgement timestamp and workflow version 2. Proof-bound integrity detects
completion tampering, erasure and phase regression. Current active/recovered flags
are never persisted as authority. Repeated calls recover the same control receipt
and original expiry; a lost response, local write failure or cancellation preserves
exact input for explicit recovery. There are no automatic retries or execution.

## Heartbeat authority foundation

Neutral heartbeat contracts now distinguish immutable claim/assignment anchors
from current lease expiry and revision. Proof-bound operation fingerprints and
strict response correlation retain the original worker/Attempt/generation scope.
The pure worker deadline policy caps the remaining budget by current credential
expiry, anchors it to monotonic request start and subtracts a stop margin, so
network delay consumes available time. Stale or out-of-order replies cannot extend
the deadline; a stopped execution lineage cannot revive through a late response.

Deadline state is process-local and must never be restored as authority from the
journal. The Windows clock and explicit supervision described below account for
elapsed sleep time. Actual descendant containment and execution restart recovery
remain separate integration gates. The deadline policy
performs no polling, process launch or stopping.

## Atomic control heartbeat persistence

`HeartbeatAssignedJob` renews only an authenticated, proof-bound, acknowledged
assignment through atomic PostgreSQL coordination. Current expiry and lease
revision live separately from original claim/acknowledgement anchors. Concurrent
requests fence the expected revision; retained request identities reject changed
input and exact replay reports current authority without renewing again.

Renewal uses fresh database time after locks and caps expiry by current credential
authority. Durable rate and finite receipt budgets bound new operations while
preserving accepted recovery. Expiry, cancellation or budget exhaustion retains
the coding reservation. Current readers validate joined authority/history while
original recovery values remain unchanged. Migration `0008_heartbeat_authorities`
requires stopped writers during rollout and refuses to discard accepted heartbeat
history on downgrade. The optional HTTP exchange described below delegates to
this persistence use case. Agent execution remains pending.

## Worker heartbeat exchange

With worker claims enabled, `POST /api/v1/worker/attempts/{attempt_id}/heartbeats`
accepts the original `claim_request_id` and `proof`, a `heartbeat_request_id`,
`generation`, `expected_lease_revision` and `protocol_version`. Generation and
protocol start at 1; the expected revision is a positive bounded integer. The
Attempt comes from the path. Authentication precedes the shared bounded JSON
reader and admission boundary. Responses wrap a nonsecret `HeartbeatResult` in
`result`, with protocol version 1, current authority and a stop indication.
Rejected input returns 409; new rate or receipt admission exhaustion returns 429.
Accepted exact retries bypass new admission and never renew twice.

The explicit worker `heartbeat()` call stores input before HTTP and records
validated completion before returning process-local request/receipt timing.
SQLite version 7 keeps one pending input and one last completed input, bound to
the original protected proof and acknowledgement completion. These records contain
request identities and expected revisions only. Current expiry, returned revision,
observation clocks, active flags and execution deadlines are never stored as
authority. Upgrades preserve original proof ciphertext, identity and lease anchors.

Pending input always takes priority. After restart, the first call replays the
last completed input to discover current authority; a subsequent explicit call
can create a new input using the observed revision. `heartbeat(renew=False)`
replays recorded input only and refuses an initial request. Network loss, local
write failure and cancellation retain exact recovery input. A completion committed
during cancellation also forces discovery before another renewal. The transport
verifies the full original assignment reference, current scope and credentials,
strict bounded responses and verified TLS. One-shot exchanges perform no automatic
retry or polling; optional heartbeat maintenance is described below.

## Explicit heartbeat supervision

The composed worker client exposes `heartbeat_supervisor()`. Constructing it
starts no task or network request. An explicit `await supervisor.run()` maintains
only the acknowledged assignment and returns a typed terminal stop result.
`wait_stopped()` reports the latched stop before exchange cancellation cleanup;
`request_stop()` requests shutdown. A client retains one supervisor and its policy,
and a stopped instance cannot start again. Manual heartbeat calls after its stop
can only observe a completed receipt. Pending input with unknown server acceptance
is retained for fresh client recovery, without another request from the stopped
client. A request already in flight when stop is signalled can still commit.

Initial pending or last input is replayed for a fresh observation. Only validated
exchange evidence initializes the process-local deadline. Renewals wait after
previous completion, without catch-up bursts or retries after failure. Neutral
`HeartbeatPolicy` separately bounds cadence (11–60 seconds), operation timeout
(1–30 seconds), stop margin (3–30 seconds), watchdog interval (0.05–1 second),
cancellation cleanup (0.1–5 seconds) and exchange count (1–4,096). These settings
do not enable claim polling or reinterpret `poll_interval_seconds`.

The watchdog checks the existing deadline while HTTP or journal completion is
pending. Expired authority, late replies, control stop, errors, clock regression,
timeout, cancellation and the finite exchange budget stop maintenance. Late
completion cannot revive the deadline. Bounded cleanup reports whether the
async exchange task settled. Control or journal executor work may still commit
after cancellation; this does not prove an agent or its descendants stopped.

Bootstrap loads the Windows clock off the event loop and shares one instance
between HTTP timing and supervision. It uses system-only `GetTickCount64`, rejects
missing APIs and backward values, and has no ordinary-clock fallback. Microsoft
documents [elapsed time including sleep and hibernation](https://learn.microsoft.com/en-us/windows/win32/sysinfo/interrupt-time)
and [approximately 10–16 ms resolution](https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-gettickcount64).
Native tests verify the binding; controlled clock jumps test delayed wake-ups.
Tests do not suspend the host. Direct application composition must explicitly
provide a clock with these elapsed-time semantics before creating a supervisor.
The explicit Docker consumer below uses this supervisor; heartbeat evidence alone
does not establish process containment or sandbox isolation.

## Durable worker execution preparation

The composed worker client exposes explicit `prepare_execution(delivery, policy)`.
It verifies the delivered definition against the original durable acknowledgement
and pins one execution UUID with the Codex runtime, selected isolation backend
and trusted policy digest. A new intent requires fresh matching authority
from the retained heartbeat supervisor, checked with its shared clock before and
after journal commit. Preparation never starts supervision or a subprocess.

SQLite version 7 binds both execution input and its presence to the protected
original claim, acknowledgement and heartbeat inputs. Valid older journals upgrade
atomically; changed or missing records fail closed. Duplicate claim/acknowledgement
responses and heartbeat maintenance preserve the execution UUID and policy.

Recovery returns the original instance and pins even after restart or stop. It is
historical evidence, never permission to launch or proof that an earlier process
did not start. Cancellation or expiry during commit can leave a recoverable intent.
A changed definition or policy cannot replace it. The Docker consumer below adds
effective isolation, Git workspace preparation and original-container settlement
before accepting a real command result.

## Windows process-scope management

`ManageExecutionProcesses` manages an existing trusted scope bound to the original
execution intent and assignment. It monitors fresh heartbeat authority and native
associated-process accounting within finite runtime, operation, observation and
stop budgets. It creates no process and never turns recovered intent into launch
permission. Stop seals attachments before termination. Only fresh accounting of
zero associated processes after termination yields `empty_verified`; errors,
timeouts and uncertain operations yield `unknown`, with reservations unchanged.

The Windows adapter creates an unnamed, noninheritable Job Object with explicitly
configured CPU hard cap, committed-memory limit and process-count limit. It reads
back the applied limits and fails if a required setting is unsupported. A trusted
caller may attach a caller-owned suspended process handle; creation, identity,
resume and failed-launch cleanup belong to the later launch driver. Native work
runs on one owned executor. Cancellation retains the native future and blocks new
operation admission until it settles. Handle cleanup supplies no empty evidence.

Parent exit and a successful termination call alone do not establish empty state.
Job Object results cover associated processes; they do not certify secret isolation,
escaped-process termination or restart reconciliation. The memory limit concerns
committed virtual memory, and nested CPU caps can be relative to a parent Job.
Tests exercise harmless Windows processes only. Codex execution, identity/sandbox
setup and check commands remain disabled.

## Windows workspace reservation

The composed client exposes explicit `reserve_workspace(delivery, workspace)` and
`recover_workspace(delivery)` operations. `ReserveAssignedWorkspace` validates the
original durable execution and full accepted definition. It observes and retains
the native root, then commits one protected workspace intent before creation.
Only the call that newly committed the intent may create the UUID child. Exact
replay, concurrent losers, existing workspace history and changed bindings cannot
create another directory. Fresh heartbeat guards fence preparation and the final
return; a valid child observation is recorded historically before that last guard.

The Windows adapter requires an existing, explicitly configured root controlled
by the coordinator, with canonical protected-directory paths supplied separately.
It rejects overlapping namespaces, ambiguous DOS paths, existing targets and
reparse ancestors. It retains noninheritable directory handles without delete
sharing, checks final paths and native identities, and creates only the execution
UUID child. Native filesystem work and handle cleanup run on one owned executor;
cleanup closes handles and preserves any created directory.

Journal version 7 preserves the canonical root path, workspace-policy digest and
native root identity plus an optional immutable child completion. The protected
proof binds contents, presence and the legacy uncertainty flag together with the
original execution/ACK/heartbeat history. Cancelled or lost operations can leave
pending or completed evidence; neither permits another creation, root replacement,
directory deletion or automatic reuse. Valid older journals upgrade atomically.
An execution already present in a legacy journal retains unknown workspace history
and blocks creation because migration cannot prove whether an earlier reservation
occurred. No automatic reset of that uncertainty is provided.

`RecoverAssignedWorkspace` returns historical input/completion only, without
filesystem handles, live authority or a usable reservation. Fresh filesystem and
process reconciliation remain required before reuse or execution. These records
provide no Git/base provenance, denied-read sandbox, hostile same-user mutation
protection or proof that work has stopped. Protected paths are trusted namespace
configuration, not ACL verification. Codex launch and the worker CLI remain disabled.

## Pinned base file materialization

The composed worker exposes explicit `materialize_base(delivery, workspace,
reservation, source)` after protected workspace preparation. It requires the
actual same-process reservation, original ACK/execution/full definition and
completed version-7 workspace history. Historical recovery cannot supply live
handles or authorize reuse. Heartbeat supervision remains independent while the
operation checks its retained authority before and after each awaited step.
The retained client admits one materialization request before any source read;
duplicate or concurrent calls fail without disrupting the original operation.

`GitHubBaseSource` is a trusted broker-side reader. It constructs bounded,
fixed-host GETs for the accepted full SHA-1 commit, nonrecursive trees and blobs,
checks configured numeric repository identity before and after export, and
verifies raw Git tree and blob hashes. Commit identity relies on the authenticated
provider response; raw-commit/signature verification is not provided. Redirects,
truncated trees, symlinks, submodules, LFS pointers and exhausted limits fail closed.
Provider credentials stay with trusted broker resources. A scoped authenticated
broker-to-worker delivery route is available behind the separate source gate below;
integration tests compose provider fixtures with real Windows journal/filesystem
resources. Real provider/account verification remains a separate deployment step.

The complete manifest is checked before writes for bounded paths/bytes, DOS path
ambiguities, Unicode casefold collisions, file/directory conflicts and accepted
check directories. The native adapter uses one admission, CREATE_NEW files,
retained noninheritable handles, native identities, bounded byte reads/writes and
exact final directory enumeration. Native creation also refuses filesystem aliases
not caught by the portable manifest checks. Cancellation, lease loss or failure
seals further writes; partial files remain and cannot be retried or adopted.
Cleanup closes handles after native work settles without deleting directories.

This exports literal file bytes, including binary files. It creates no `.git`
metadata, runs no hooks/filters/checks and supplies neither a full Git checkout nor
durable materialization recovery. The configured root is a trusted namespace,
not hostile same-user isolation. Git metadata/reconciliation,
credential isolation and Codex launch remain gates before agent execution.

## Broker source delivery

`DEVRELAY_CONTROL_WORKER_BASE_SOURCES` explicitly enables
`POST /api/v1/worker/attempts/{attempt_id}/base-source` and requires worker claims.
It is disabled by default. Requests use the enrolled worker token and original
claim UUID/proof; they cannot select a repository, commit, blob URL or branch.
The reader requires an acknowledged running Attempt and active current authority,
derives full source pins from its immutable definition, and rechecks proof, scope,
authority and original pins after provider work and response encoding. Database
sessions close before network waits. Reads never claim, renew or release work.

The source response carries original assignment and definition pins with a verified
manifest and canonical base64 file bytes. Limits are 512 files, 2 MiB per file,
8 MiB decoded total and 12 MiB JSON. Structural JSON allocation, paths, blob hashes
and manifest digest are bounded and checked. One source admission is independent
of claim/heartbeat admission; a canceled response retains encoder admission until
its actual executor work settles. This is point-in-time authorization, not a
transfer of a server lease deadline to the worker.

The composed worker can call `materialize_base(delivery, workspace, reservation)`
to use this channel. It binds the protected original input only after live workspace
validation and admits one export before any fetch. Bootstrap owns a separate HTTP
client so a long source response leaves the heartbeat connection free. Both clients
use only the worker credential, verified HTTPS or the configured loopback tunnel,
without redirects or environment proxies. GitHub credentials stay control-side.
Current worker credentials and complete response pins are checked again before
return, while retained heartbeat authority guards receipt and native staging.

Fixture tests cover HTTP round trips into actual Windows protected workspaces,
real separate loopback connections during heartbeat renewal, and disposable
PostgreSQL expiry/recovery without changing durable state. No live integrations,
automatic polling, Codex/check execution, Git history or restart reuse are enabled.

## Editable workspace handoff

After `materialize_base`, the composed worker can explicitly call
`handoff_materialized_base(delivery, workspace, materialization)` to make the live
workspace editable. The original export remains protected until that separate
one-shot transition. Reconstructed or historical materialization values cannot
authorize it; the exact successful result and retained native scope are required.

The use case validates original ACK/execution/full definition and completed
workspace history, checking retained heartbeat authority around each awaited step.
Before releasing any guard, the native adapter rechecks source pins, exact bytes,
directory contents and native identities. It closes the materialized file and
descendant-directory handles while retaining the UUID root and its ancestors.
Retained directories request explicit read access with no delete sharing; native
tests verify that root and parent rename remain blocked. Missing directory read
permission fails closed.

Files can then be edited, replaced or deleted and child directories renamed within
the still-open scope. Their earlier source-byte observations are historical once
editing begins. Success releases cached source-byte buffers. Cancellation, expiry
or partial handle-release failure seals the scope, preserves remaining resources
and returns no usable handoff result. Sealing cannot undo already-released guards.
Cleanup preserves filesystem contents, and uncertain work never releases capacity.

This is an explicit same-process transition without journal/schema changes. It
creates no Git metadata, process, sandbox or restart-reuse permission. The explicit
Docker consumer below composes the separate preparation and launch boundaries.

## Verified shallow Git workspaces

Optional `DEVRELAY_CONTROL_BROKER_GIT` enables original commit acquisition alongside
the existing broker file export. It requires source delivery and explicit Linux
Git executable/version/SHA256, HTTPS helper SHA256, helper directory and private
directory settings. The broker uses the configured read-only provider credential,
fixed repository URL, verified TLS, depth-one tree-filtered reads and finite process,
output, memory, file and disk budgets. Ambient configuration, credential helpers, prompts,
redirects and alternate transports are excluded. Repository numeric identity is
rechecked after acquisition. Failed or uncertain `git-read-*` directories are
retained and block another acquisition until trusted operator reconciliation.

Build the optional control image with `docker build --target broker-git -t
devrelay-control:git .`. This profile uses tested Git 2.47.3 from the pinned Debian
package and records exact executable/helper identities in
`/app/broker-git-profile.json`. Supply that JSON as the optional setting and mount
its dedicated writable private directory, owned by UID10001 with mode0700. The
default image target and source protocol remain unchanged when Git is disabled.
Image construction does not enable integrations or run migrations.

Source protocol v2 adds bounded original commit bytes. The worker verifies their
Git object SHA and matching root tree; it does not reconstruct or authenticate a
signature. After file materialization, explicit `prepare_git_base` validates the
original ACK/execution/workspace pins and current heartbeat authority, then creates
native metadata once: verified loose objects, authorized branch/HEAD, shallow
boundary and a binary index. Existing `.git` is rejected. Provider credentials and
Git network operations stay on the broker; metadata preparation runs no Git process
on the worker.

Before editable handoff, source bytes, metadata digest and native identities are
rechecked. The initial index describes literal exported bytes: trusted Git attributes
disable repository text/filter/encoding conversions. Symlinks, submodules, LFS,
conversion-dependent and history-dependent profiles are unsupported. Parent history
is absent beyond the accepted shallow boundary. Cancellation/expiry preserves
uncertain resources and grants no restart, launch or capacity-release authority.
Generated references must also fit the canonical Windows metadata path budget
and the actual workspace path length; larger domain-valid references fail before
metadata creation. Configure a short coding-branch prefix and workspace root.

## Managed trusted host processes

`RunTrustedProcess` provides an explicit process bridge for host-owned commands
and harmless native fixtures. It prepares one hidden suspended Windows process,
attaches it to the existing resource-limited Job Object, rechecks fresh assignment
authority and resumes once. Executable, arguments, working directory and the
complete child environment are explicit. Only the three child standard-I/O
handles are inherited; coordinator environment and private handles are excluded.

The runner collects bounded binary stdout/stderr chunks and a genuine root exit
code. Runtime, operation, observation and combined output budgets stop the entire
associated job. Root exit alone does not prove descendant termination or pipe
closure. Every terminal path seals launch admission before termination, and only
fresh post-termination accounting can produce `empty_verified`. Unsettled native
work, invalid clocks and failed stop observations retain `unknown` evidence.
Cancellation remains cancellation after bounded cleanup; it does not grant retry
or release a reservation. Owning-thread cleanup closes process resources before
the Job Object and is separate from verified containment evidence.

This bridge does not establish a restricted execution identity, denied secret
reads or network isolation. It is not composed into worker startup, recovery or
Codex/check execution. The Docker consumer below supplies its own isolated runtime
and never uses this harmless host-command bridge for agent/check execution.

## Control acknowledgement persistence

`AcknowledgeAssignedJob` recovers only the authenticated original assignment,
validates its snapshot, base revision and complete definition digest off the
database transaction, and delegates to bounded coordination storage. Bootstrap
composes this use case only with explicitly enabled worker claims and the existing
independent-proof credential guard; constructing it opens no connection or provider
request. The optional acknowledgement route delegates to this use case.

Migration 0007 adds one immutable acknowledgement receipt per Attempt and permits
only the initial claimed/version-1 to running/version-2 transition. Storage locks
slot, worker, source task, workflow and Attempt in order, then rechecks scope,
credential expiry, original proof, generation, workflow version and lease deadline
using fresh database time. Attempt, workflow, history and receipt commit together.
Credential expiry is checked again before disclosure; a failed write rolls back
the entire transition.

Exact repeats return the original acknowledgement timestamp and accepted pins.
Recovery after lease expiry requires current authentication and reports inactive
authority; it never renews, replaces or releases the assignment. Original claim,
assignment input and submission status reads remain available after acknowledgement.
Definitions, check policy, claim receipt, generation, expiry and coding reservation
are preserved. Running records acceptance of the assignment; it does not prove an
agent process started or grant launch permission.

The receipt's unique Attempt key bounds its count by accepted assignments, without
new polling counters or automatic deletion. Downgrade rejects acknowledged history;
claim-only data can downgrade and upgrade without losing its original records.
Apply schema/code changes explicitly with old writers stopped. Migrations do not
run at startup, and no live rollout is included.

## GitHub observation

Optional `DEVRELAY_CONTROL_GITHUB__` settings require a separate read-only token
and an explicit `REPOSITORY_IDS` array mapping configured DevRelay repository UUIDs
to independently verified GitHub numeric IDs. Configure the whole group together;
the commented `.env.example` values are placeholders. Contents, Pull requests and
Metadata read permissions are sufficient for the implemented GitHub reads.

The observer uses GET only against `api.github.com`, verifies slug/numeric identity
before and after observation, reads the exact target reference, and scans bounded
pages of open PRs. PR title/body/labels do not establish task ownership. Matching
requires the stored Job UUID, exact authorized branch, task and repository scope;
fork claims, unknown managed branches and incomplete/ambiguous observations fail
closed. Each relevant page uses one bounded database lookup, with its session
released before the next network request. Requests and the complete observation
have time/byte/page limits; redirects and provider-supplied pagination URLs are
not followed. No remote write, merge or automatic retry is performed.

The optional authenticated Job API uses this submission composition.
GitHub configuration alone opens no client at HTTP
startup and enables no new route. Numeric identity is verified against operator
configuration per observation and pinned durably for new submissions. Stored
PR associations must have the same numeric ID. Remote reads are non-atomic;
fresh pre-execution reconciliation remains required before worker dispatch.

## Task storage

The import use case captures one selected task within an explicit site/project
allowlist. It reads Jira before opening the storage transaction and commits the
stable task identity and immutable content snapshot together. Repeated content
returns the original snapshot; changed requirements create a new snapshot.
Readiness labels, description and acceptance criteria produce a requirements
diagnostic only. Import does not create or authorize a coding job.

Database settings use `DEVRELAY_CONTROL_DATABASE__` followed by the setting name.
The optional group in `.env.example` shows the required connection values.
The HTTP service opens the engine only for the configured import API and never
runs migrations at startup. Schema changes require an explicit operator command
from the checkout
or source distribution:

```shell
uv run --locked python -m alembic -x env_file=.env upgrade head
```

The control image includes the migration history; its equivalent command is
`python -m alembic upgrade head` with database settings supplied at runtime.
The wheel contains application code; operational migrations are included in the
source distribution and control image.

## Job preparation and storage

The application preparation use case loads a canonical stored snapshot, releases
its database session, and reads fresh Jira content and labels by stable task ID.
It requires explicit caller repository scope, an enabled repository policy,
matching source scope, `agent-ready`, description and acceptance criteria.
Changed requirements must be imported again. A renamed task key alone does not
create a different task or replace the pinned snapshot.

Optional `DEVRELAY_CONTROL_REPOSITORIES` configuration is a JSON array. Each entry
defines a control-owned repository UUID, GitHub owner/name, target branch, coding
branch prefix, source project allowlist, policy version, required check argv and
timeouts, and repair budget. Entries default to disabled; no repository is allowed
by default. This group requires complete Jira and database configuration.
The UUID is a DevRelay policy identity, not a GitHub numeric repository ID.
The commented example in `.env.example` is illustrative, not a live configuration.

Check commands come from operator configuration, never task text. Preparation
does not run them. Directory validation is a syntax guard; execution isolation,
path containment and restricted subprocess environments remain future work.

Default submission identity binds stable task identity, content fingerprint,
repository and generation one. Rule evaluation returns the original Job for an
identical recorded intent, rejects conflicting policy, and blocks a new delivery
while the task/repository has an unresolved Job or associated PR. The submission
use case resolves accepted exact replay before fresh provider checks. It preserves
the original operator, branch, base commit and full check policy on retry, even
when readiness has subsequently changed. Policy edits conflict with existing intent.

For new admission, Jira and trusted Git/PR observations finish before the write
transaction. Storage locks the stable task row, repeats replay/conflict checks,
and commits repository identity, numeric binding, Job definition and ordered checks
together. A local repository UUID and GitHub numeric ID have a one-to-one immutable
binding. Configuration changes cannot rebind an accepted repository or hide a
conflicting replay; renames and transfers require explicit reconciliation.
Database uniqueness and a partial task/repository guard prevent duplicate queued
deliveries. Full check argv, directories, timeouts, scope, repair limits and
operator identity are stored; snapshots and definitions are immutable.

Migration 0003 pins new version-2 definitions to the verified GitHub numeric ID.
Existing version-1 definitions retain their original fingerprints and a null
numeric ID; migration does not infer identity from today's configuration. They
remain readable, but bound policy cannot replay them as verified work. Legacy
Jobs require trusted reconciliation before any future execution. Downgrade is
rejected once a numeric binding has been stored, preserving accepted identity.

The submission migrations preserve queued definitions. Subsequent workflow/claim
migrations coordinate current state separately. Intentional replay generations
and execution transitions require later migrations. The submission lock is
separate from the global coding concurrency slot.

Migration 0004 separates the accepted definition from its current workflow
projection. Every submission atomically creates its definition, complete checks,
queued workflow and version-0 creation event. Existing bound and unbound
definitions are backfilled without changing fingerprints or guessing numeric
repository identity. The unresolved task/repository guard belongs to the workflow;
submission responses use its materialized status. Missing or inconsistent
workflow/history fails closed. Migration 0004 supports queued creation only;
migration 0006 adds initial claims. These migrations do not enable worker execution.

Schema and code must be rolled out together: stop control writers, explicitly
migrate, deploy the matching code, then restart. Old writers that omit workflow
creation cannot commit. Startup never runs migrations. A queued-only downgrade
preserves definitions; advanced or inconsistent workflow history blocks it.
Composition requires an
explicit trusted observer; the optional GitHub implementation supplies verified
read-only observations and never a permissive default.
Import authentication grants import only.

Preparation/candidate values alone do not start work. The explicit Docker consumer
below requires separate runtime enablement; submission records a durable Job and opt-in claims
reserve its initial assignment.
Repository configuration alone exposes no new route or provider adapter.

## Successful coding result coordination

Result delivery has a separate default-disabled `worker_coding_results` gate and
requires explicitly enabled worker claims. It exposes authenticated
`POST /api/v1/worker/attempts/{attempt_id}/results` and
`POST /api/v1/worker/attempts/{attempt_id}/results/recovery`. The latter consumes
the same original request; it does not create missing results or a new assignment.
The bounded v1 request includes the original proof/ACK/execution pins, contiguous
settled coding/check cycles and only the final candidate's complete source bytes.
The strict codec rejects duplicate/unknown fields, noncanonical identities and
oversized structures before source decoding. Result bodies are capped at 16 MiB;
the ordinary claim/ACK/heartbeat body limit remains unchanged.

Acceptance returns 202 with a nonsecret receipt after the transaction commits.
The immutable proof-free report, every workflow event and the publication intent
commit together. Fixed checks must match the accepted policy and exact candidate
SHA. Failed local cycles consume the configured repair budget; the final cycle
must pass all checks. Exact retry returns the original receipt; changed input or
another result for the Attempt conflicts. Acceptance retains the coding slot and
ends at `PUBLISHING_PR`. Cancellation retains actual decoder/preparation/storage
work until settlement; no late value becomes a new operation's input.

Broker-only coordination methods record one invocation and each provider write
plan before any external mutation. Only a newly committed plan permits its one
write; replay never permits repeating it. Outcomes of already planned writes can
be recorded after expiry without granting further work rights. Uncertainty is
sticky and retains the reservation, including after a late observation. Normal
finalization requires fresh authority and independently verified exact PR/repo/
branch/head association. It atomically records `AWAITING_REVIEW`, closes the
coding Attempt, revokes authority and releases only the original reservation once.
Historical claim/ACK/heartbeat/result reads remain available under current scope,
including after another Attempt reserves the slot. Coding completion does not
mean merged, deployed or healthy delivery.

Migration 0009 must be deployed with matching stopped writers; startup never
migrates. It preserves immutable definitions and original claim/ACK/lease anchors,
retains heartbeat update fields separately from revocation, and refuses to discard
advanced result history on downgrade. Explicit broker publication is separately
configured below; the admitted Codex/check runtime remains separate consumer
work. The result gate does not enable automatic GitHub writes or execution.

The worker separately opts in with `DEVRELAY_WORKER_CODING_RESULTS_ENABLED=true`.
Its explicit `submit_coding_result` method requires the retained heartbeat
supervisor and original completed execution/workspace. It validates the complete
report off the event loop and commits one canonical proof-free report and request
UUID to the protected private SQLite journal before its first HTTP submission.
A later submit with existing input first recovers that original result; it never
silently replaces the report or changes the accepted target branch. The final
authority check runs after encoding, immediately before the outbound submission.
Result traffic has its own connection, so a slow result response does not occupy
the heartbeat connection. Opening the worker does not submit or recover a result.

SQLite v8 binds report contents and presence, the target branch, acceptance and
terminal receipts to the original protected proof and execution/workspace state.
Valid v1–v7 upgrades are atomic. An older journal with an execution already
present retains an explicit unknown result-history flag and cannot create a new
report without reconciliation. Receipt metadata is strictly correlated to the
original request, candidate SHA, repository, execution and both PR branches.

`recover_coding_result` reads and persists historical acceptance/terminal metadata
under current worker authentication without constructing a heartbeat supervisor
or reviving execution authority. Canceled submissions retain unresolved work;
once HTTP has started, correlated response bookkeeping settles before normal
resource closure. A terminal receipt preserves the original claim and workspace
history. It does not reset the worker, prove native process settlement, delete a
workspace or admit another job; verified settlement and bounded historical claim
rotation remain separate consumer work.

## Scoped GitHub publication

The optional control `coding_publication` setting requires enabled result
coordination, a separate server-only `api_token` and explicit
`allowed_repository_ids`. Its scope must be inside enabled, numerically bound
repository policies. The write credential cannot equal a read, operator, database
or worker credential, and the claim proof reuse guard includes it. It is never
sent to the local worker, task workspace or agent process. The GitHub credential
must be provisioned separately for the selected repositories with the required
contents and pull-request write permissions; configuration does not grant those
permissions. See GitHub's [Git commit API](https://docs.github.com/en/rest/git/commits?apiVersion=2026-03-10)
and [pull request API](https://docs.github.com/en/rest/pulls/pulls?apiVersion=2026-03-10).

The control lifespan owns one explicit `coding_result_publisher`, sharing its
database pool with result coordination and using a separate bounded GitHub client.
Opening it performs no provider request. Trusted broker code explicitly calls
`execute(prepared, authorization)` or read-only `reconcile(prepared, authorization)`;
no worker publication endpoint, automatic startup task or polling loop is added.
Task PRs are left for human review and merge.

The trusted control command can load and publish one already accepted result:

```shell
devrelay-control --env-file control.env --publish-result RESULT_UUID --worker-id WORKER_UUID
```

Replace both UUID placeholders with the durable result and its enrolled worker.
Run this command only in the trusted broker environment holding the separate
publication credential. Both flags are required together, and this mode exits
after that result instead of starting HTTP. It requires an enabled, unexpired
worker enrollment and the intersection of its repository scope with the enabled
publication policy. The command reconstructs the original proof-free report and
immutable assignment from storage; it never requests or recreates a worker lease
proof. Storage revalidates current authentication and history after bounded
off-loop report validation and again before publication writes. It does not
claim another job, renew authority, launch an agent or migrate the database.
Failures print a fixed credential-free message; inspect and reconcile retained
publication history before retrying. Repeating the command cannot grant another
write for an existing uncertain or interrupted publication owner.

Use the separate observation-only command for an accepted result whose provider
response was lost or whose publication was interrupted:

```shell
devrelay-control --env-file control.env --reconcile-result RESULT_UUID --worker-id WORKER_UUID
```

The two result commands are mutually exclusive and require the same current
enrollment and broker configuration. Reconciliation reads the exact retained
provider effect and may append its correlated observation to control history.
It never creates another provider write or publication owner, clears uncertainty,
finalizes an uncertain result or releases its coding reservation. The output
reports the current publication state or unavailable result; a recorded terminal
result includes only its correlated PR number and checked head. Uncertain work
still requires trusted resolution before another coding job can advance.

The publisher validates the complete plan before recording an owner or calling
GitHub. It reconstructs exact blobs, child-first trees and the canonical unsigned
commit, preserving the checked SHA and original parent. Write IDs and request
digests are stable. Configured write, event, request, byte and time budgets reject
an oversized plan before its first provider request. The fixed GitHub API client
does not follow redirects, use environment proxies or retry mutations.

Each write requires its newly committed DB plan and a fresh dispatch window.
Historical permits carry no dispatch timestamps. The broker conservatively
subtracts the full SQL wait and safety margin from the DB window and checks a
suspend-inclusive clock immediately before HTTP. Close and cancellation also
fence that final boundary. Started provider work and its bookkeeping settle
before normal resource closure. The provider independently reads back object
bytes/hashes, exact ref and same-repository PR source/target/head association;
returned SHAs never replace the checked candidate SHA.

Branches and PRs are created once, with no update, force, delete or merge operation.
A lost mutation response retains its original planned input and coding reservation.
Read-only reconciliation may observe that exact effect and append its historical
observation; uncertainty stays sticky and grants neither a new POST nor normal
finalization. An empty PR list after a lost response is not retry permission.
Successful normal publication atomically records `AWAITING_REVIEW` and releases
the original slot once. An uncertain or interrupted publisher still needs explicit
trusted recovery before normal processing can advance.

Controlled GitHub transport fixtures and disposable PostgreSQL verify this flow,
including repair history and response loss for each write kind. They do not
certify live GitHub byte compatibility or provision a real write credential. The
live account/model acceptance, known-stopped failure handling and verified local
settlement/claim rotation remain separate work. Publication configuration alone
does not start coding or deployment.

## Windows worker with Linux execution

The explicit `--run-one` path claims at most one task, acknowledges it, maintains
heartbeats, prepares its broker-delivered Git base and runs bounded coding/check
cycles in disposable Docker Desktop Linux containers. Windows retains the private
journal and native candidate handles. Only the task UUID directory is shared;
checks receive a read-only mount. Each container uses a read-only root, non-root
user, no added capabilities, no-new-privileges, 2 CPUs, 2 GiB and 128 PIDs.
The pinned Codex Linux sandbox stays enabled inside the container.
The host must permit its unprivileged user namespaces; an incompatible kernel or
AppArmor policy fails readiness. An independent, account-free syscall diagnostic
distinguishes explicit namespace denial from application failure. CI verifies
refusal on restricted hosts; positive sandbox tests run only on capable hosts.
The complete positive suite is also tested on Windows Docker Desktop. No CI host
policy or container privileges are relaxed to make an incompatible host pass.

Journal version 9 preserves version-8 proof, execution, workspace and result bytes
while adding the `docker_linux` isolation value; it never repins an old execution.

Build the image and run account-free diagnostics first (PowerShell):

```powershell
docker build -f Dockerfile.worker -t devrelay-worker:local .
$workerImage = docker image inspect --format '{{.Id}}' devrelay-worker:local
devrelay-worker --check-runtime --runtime-image $workerImage
```

Diagnostics use fake files and a scripted loopback Responses server, with no
authentication or model request. They test private/auth read and write denial,
network and parent-process denial, writable coding versus read-only checks,
project-configuration rejection, command/patch/image tools and bounded JSONL
completion. Passing diagnostics is not evidence of account/model compatibility.

`DEVRELAY_WORKER_DOCKER_RUNTIME` accepts an explicit JSON object containing
`image_id` (the immutable `sha256:` ID), optional `auth_volume`, optional `model`,
`model_calls_enabled` (default false), `coding_timeout_seconds` (default 900) and
`maximum_runtime_seconds` (default 3600). Only the configured local Docker pipe
or Unix socket is accepted. No Docker configuration, auth file or environment
proxy is loaded. Set `DEVRELAY_WORKER_CODING_RESULTS_ENABLED=true`, an absolute
workspace root and `DEVRELAY_WORKER_COMMIT_AUTHOR_NAME` / `_EMAIL` before running.
The author identity must be the configured human identity.

Real execution additionally requires separately provisioned authentication in a
dedicated Docker volume labelled `devrelay.auth=1` and `devrelay.image=<image_id>`,
and explicit `model_calls_enabled=true`. The worker never copies desktop auth,
creates a login, provisions a volume or enables model access automatically. The
auth volume is mounted only for coding and denied to sandboxed tools; checks
receive no authentication volume and no network. Real login/model acceptance is
still outstanding.

```powershell
devrelay-worker --run-one --env-file C:\DevRelay\worker.env --state-directory C:\DevRelayPrivate\worker
```

The first image supports Linux Python checks beginning with `python`, `python3`,
`pytest`, `ruff`, `mypy` or `lint-imports`. Check arguments are used exactly as
accepted by control. Configure cache output under `/tmp` or disable it, such as
`pytest -p no:cacheprovider` and `ruff --no-cache`. Windows commands and `uv run`
are rejected rather than translated. Dependencies come from the image; candidate
packages are built offline into temporary storage and imported from that build.
The trusted check shim reports actual command settlement separately from sandbox
startup. Missing executables, namespace failures and signals never become invented
check exit codes; a genuine exit 125 remains an ordinary failed command.

Containers are observed and removed by their original IDs before a candidate is
captured or a check result is accepted. Cancellation seals admission and drains
owned work. A missing/changed owner, lost mutation reply, expired lease or failed
isolation check retains the coding reservation. Restarting the worker can recover
an existing result but never launches an existing execution again. Automatic
failure release, local terminal archival and next-claim rotation remain deferred;
do not clear the journal or reuse a reserved workspace to work around them.

## Container

The Dockerfile builds the control service with locked runtime dependencies,
installs the package without editable source references, and runs as user 10001.

```shell
docker build -t devrelay-control:local .
docker run --rm -p 127.0.0.1:8000:8000 devrelay-control:local
```

The container listens on port 8000 and defaults to production settings with
interactive API documentation disabled. Pass environment variables at runtime;
do not bake credentials into the image. The Docker health check reports process
liveness only. For Dokploy, select the Dockerfile build and container port 8000.
Schema migrations are an explicit operation; deployment automation is deferred.
The local coding worker remains a separate process on the developer's computer.

## Development checks

```shell
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked python -m mypy
uv run --locked lint-imports
uv run --locked python -m pytest
uv build
```

The CI workflow runs these checks with a PostgreSQL 18 service. Locally, PostgreSQL
tests skip unless `DEVRELAY_TEST_POSTGRES_PORT` identifies a disposable server on
`127.0.0.1`. That server must have database/user `devrelay_test` and the test-only
password `devrelay-test-only-password`. Each test migrates and removes its own
generated schema; it never uses the control service's database configuration.
