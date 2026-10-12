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
evidence without launch authority; the worker CLI still validates configuration only. An opt-in claim
API and bounded worker HTTP adapter now connect durable claims to local recovery.
An assigned worker can also retrieve its complete immutable task/check inputs
using the original protected claim proof. Payload reads never claim another Job.
Live account validation, automatic polling, agent execution, PR publication and
deployment automation are not implemented yet.

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
heartbeat exchange and input recovery are implemented; scheduling, containment
and agent execution remain pending.

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
Heartbeat fencing, execution and agent isolation remain pending.

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

Valid version-1 through version-4 journals upgrade to version 5 in one transaction, preserving protected
proof bytes, original identities, assignment anchors and no-work markers. Damaged
state or an incomplete upgrade retains the old schema and fails closed. SQLite,
DPAPI and integrity checks remain on the dedicated journal thread.

This step prepares local input only. The control now implements the atomic
claimed-to-running transition and acknowledgement receipt described below.
Its HTTP exchange and worker response completion are described below.
Heartbeat scheduling, containment and runtime integration
are also required before launching an agent; the CLI remains a
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
journal. Native suspend handling, actual descendant containment and heartbeat
scheduling remain separate integration gates. The deadline policy
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
this persistence use case. Scheduling and agent execution remain pending.

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
SQLite version 5 keeps one pending input and one last completed input, bound to
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
strict bounded responses and verified TLS. There is no automatic retry or polling.

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
migration 0006 adds initial claims, while worker execution remains disabled.

Schema and code must be rolled out together: stop control writers, explicitly
migrate, deploy the matching code, then restart. Old writers that omit workflow
creation cannot commit. Startup never runs migrations. A queued-only downgrade
preserves definitions; advanced or inconsistent workflow history blocks it.
Composition requires an
explicit trusted observer; the optional GitHub implementation supplies verified
read-only observations and never a permissive default.
Import authentication grants import only.

Worker execution is not implemented. Preparation/candidate values
alone do not start work; submission records a durable Job and opt-in claims
reserve its initial assignment.
Repository configuration alone exposes no new route or provider adapter.

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
