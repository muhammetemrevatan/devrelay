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
Live account validation, worker dispatch, agent execution, PR publication and
deployment automation are not implemented yet.

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
database or provider resources. Future claim requests must independently verify
authority and expiry using database time. Worker claims, heartbeat fencing,
assignment recovery, execution and agent isolation remain pending.

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

The migrations support queued definitions only. Worker claims, lease/state
updates, durable lifecycle history and intentional replay generations require
later migrations. This submission lock is not the global coding concurrency slot.
Composition requires an
explicit trusted observer; the optional GitHub implementation supplies verified
read-only observations and never a permissive default.
Import authentication grants import only.

Worker dispatch is not implemented. Preparation/candidate values
alone do not start work; only the submission use case records a durable Job.
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
