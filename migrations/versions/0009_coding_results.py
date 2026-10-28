"""Accept successful coding results and finalize verified publication atomically.

Normal successful profile only. Unknown effects retain the dispatch reservation.
Live rollout requires stopped old writers; no migration runs at application startup.
"""

import importlib

from alembic import op

revision = "0009_coding_results"
down_revision = "0008_heartbeat_authorities"
branch_labels = None
depends_on = None

_TABLES = r"""
CREATE TABLE attempt_results (
	id UUID NOT NULL,
	request_id UUID NOT NULL,
	worker_id UUID NOT NULL,
	attempt_id UUID NOT NULL,
	job_id UUID NOT NULL,
	claim_request_id UUID NOT NULL,
	repository_id UUID NOT NULL,
	snapshot_id UUID NOT NULL,
	execution_id UUID NOT NULL,
	generation BIGINT NOT NULL,
	definition_digest VARCHAR(64) NOT NULL,
	claim_fingerprint VARCHAR(64) NOT NULL,
	fingerprint VARCHAR(64) NOT NULL,
	protocol_version SMALLINT NOT NULL,
	fingerprint_version SMALLINT NOT NULL,
	expected_workflow_version BIGINT NOT NULL,
	publishing_workflow_version BIGINT NOT NULL,
	candidate_revision VARCHAR(40) NOT NULL,
	repair_cycles BIGINT NOT NULL,
	recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
	payload_bytes BYTEA NOT NULL,
	payload_sha256 VARCHAR(64) NOT NULL,
	CONSTRAINT pk_attempt_results PRIMARY KEY (id),
	CONSTRAINT uq_attempt_results_request UNIQUE (worker_id, request_id),
	CONSTRAINT uq_attempt_results_attempt UNIQUE (attempt_id),
	CONSTRAINT uq_attempt_results_job UNIQUE (id, job_id),
	CONSTRAINT fk_attempt_results_attempt_id_execution_attempts FOREIGN KEY(attempt_id, job_id, worker_id, claim_request_id) REFERENCES execution_attempts (id, job_id, worker_id, claim_request_id) DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT ck_attempt_results_versions CHECK (generation = 1 AND protocol_version = 1 AND fingerprint_version = 1 AND expected_workflow_version = 2 AND repair_cycles BETWEEN 0 AND 5 AND publishing_workflow_version = 4 + 2 * repair_cycles),
	CONSTRAINT ck_attempt_results_payload CHECK (definition_digest ~ '^[0-9a-f]{64}$' AND claim_fingerprint ~ '^[0-9a-f]{64}$' AND fingerprint ~ '^[0-9a-f]{64}$' AND candidate_revision ~ '^[0-9a-f]{40}$' AND payload_sha256 ~ '^[0-9a-f]{64}$' AND octet_length(payload_bytes) BETWEEN 1 AND 16777216 AND encode(sha256(payload_bytes), 'hex') = payload_sha256)
);
CREATE TABLE delivery_operations (
	result_id UUID NOT NULL,
	version BIGINT NOT NULL,
	state VARCHAR(16) NOT NULL,
	invocation_id UUID,
	event_count BIGINT NOT NULL,
	pr_number BIGINT,
	pr_repository_id UUID,
	pr_provider_repository_id BIGINT,
	pr_branch VARCHAR(256),
	pr_target_branch VARCHAR(256),
	pr_head_revision VARCHAR(40),
	completed_at TIMESTAMP WITH TIME ZONE,
	final_workflow_version BIGINT,
	release_ordinal BIGINT,
	CONSTRAINT pk_delivery_operations PRIMARY KEY (result_id),
	CONSTRAINT fk_delivery_operations_result_id_attempt_results FOREIGN KEY(result_id) REFERENCES attempt_results (id),
	CONSTRAINT uq_delivery_operations_release UNIQUE (release_ordinal),
	CONSTRAINT uq_delivery_operations_release_pair UNIQUE (result_id, release_ordinal),
	CONSTRAINT ck_delivery_operations_stage CHECK (version BETWEEN 0 AND 2047 AND event_count = version + 1 AND state IN ('accepted', 'publishing', 'uncertain', 'finalized') AND ((state = 'accepted' AND version = 0 AND invocation_id IS NULL) OR (state <> 'accepted' AND version > 0 AND invocation_id IS NOT NULL))),
	CONSTRAINT ck_delivery_operations_terminal CHECK ((state <> 'finalized' AND pr_number IS NULL AND pr_repository_id IS NULL AND pr_provider_repository_id IS NULL AND pr_branch IS NULL AND pr_target_branch IS NULL AND pr_head_revision IS NULL AND completed_at IS NULL AND final_workflow_version IS NULL AND release_ordinal IS NULL) OR (state = 'finalized' AND pr_number IS NOT NULL AND pr_number > 0 AND pr_repository_id IS NOT NULL AND pr_provider_repository_id IS NOT NULL AND pr_provider_repository_id > 0 AND pr_branch IS NOT NULL AND length(pr_branch) BETWEEN 1 AND 256 AND pr_target_branch IS NOT NULL AND length(pr_target_branch) BETWEEN 1 AND 256 AND pr_branch <> pr_target_branch AND pr_head_revision IS NOT NULL AND pr_head_revision ~ '^[0-9a-f]{40}$' AND completed_at IS NOT NULL AND final_workflow_version IS NOT NULL AND final_workflow_version BETWEEN 5 AND 15 AND release_ordinal IS NOT NULL AND release_ordinal > 0))
);
CREATE TABLE delivery_operation_events (
	result_id UUID NOT NULL,
	sequence BIGINT NOT NULL,
	kind VARCHAR(32) NOT NULL,
	invocation_id UUID,
	step_id UUID,
	step_kind VARCHAR(128),
	input_digest VARCHAR(64),
	observation_digest VARCHAR(64),
	occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
	CONSTRAINT pk_delivery_operation_events PRIMARY KEY (result_id, sequence),
	CONSTRAINT fk_delivery_operation_events_result_id_delivery_operations FOREIGN KEY(result_id) REFERENCES delivery_operations (result_id) DEFERRABLE INITIALLY DEFERRED,
	CONSTRAINT ck_delivery_operation_events_stage CHECK (sequence BETWEEN 0 AND 2047 AND kind IN ('accepted', 'publishing_started', 'write_planned', 'write_observed', 'uncertain', 'finalized') AND ((sequence = 0 AND kind = 'accepted' AND invocation_id IS NULL) OR (sequence > 0 AND kind <> 'accepted' AND invocation_id IS NOT NULL))),
	CONSTRAINT ck_delivery_operation_events_evidence CHECK ((kind IN ('accepted', 'publishing_started', 'finalized') AND step_id IS NULL AND step_kind IS NULL AND input_digest IS NULL AND observation_digest IS NULL) OR (kind IN ('write_planned', 'write_observed') AND step_id IS NOT NULL AND step_kind IS NOT NULL AND length(step_kind) BETWEEN 1 AND 128 AND input_digest IS NOT NULL AND input_digest ~ '^[0-9a-f]{64}$' AND ((kind = 'write_planned' AND observation_digest IS NULL) OR (kind = 'write_observed' AND observation_digest IS NOT NULL AND observation_digest ~ '^[0-9a-f]{64}$'))) OR (kind = 'uncertain' AND step_id IS NOT NULL AND step_kind IS NOT NULL AND length(step_kind) BETWEEN 1 AND 128 AND input_digest IS NOT NULL AND input_digest ~ '^[0-9a-f]{64}$' AND observation_digest IS NULL))
);
CREATE UNIQUE INDEX uq_delivery_events_observed_step ON delivery_operation_events (result_id, step_id) WHERE kind = 'write_observed';
CREATE UNIQUE INDEX uq_delivery_events_planned_step ON delivery_operation_events (result_id, step_id) WHERE kind = 'write_planned';
ALTER TABLE delivery_operations ADD CONSTRAINT fk_delivery_operations_result_id_delivery_operation_events FOREIGN KEY(result_id, version) REFERENCES delivery_operation_events (result_id, sequence) DEFERRABLE INITIALLY DEFERRED;
"""

_SCHEMA = r"""

ALTER TABLE job_workflows ADD COLUMN repair_cycles bigint NOT NULL DEFAULT 0,
 ADD COLUMN candidate_revision varchar(40), ADD COLUMN accepted_result_id uuid,
 DROP CONSTRAINT ck_job_workflows_claim_state;
ALTER TABLE job_workflow_events ADD COLUMN result_id uuid, ADD COLUMN cycle bigint,
 ADD COLUMN candidate_revision varchar(40), DROP CONSTRAINT ck_job_workflow_events_claim_state;
ALTER TABLE dispatch_slots ADD COLUMN release_count bigint NOT NULL DEFAULT 0,
 ADD COLUMN last_released_result_id uuid, DROP CONSTRAINT dispatch_slots_check;
ALTER TABLE execution_attempts DROP CONSTRAINT ck_execution_attempts_ack_state;
ALTER TABLE attempt_lease_authorities DROP CONSTRAINT attempt_lease_authorities_revoked_at_check;
ALTER TABLE job_workflows ADD CONSTRAINT ck_job_workflows_claim_state CHECK (blocks_new_submission IS TRUE AND updated_at >= created_at AND repair_cycles BETWEEN 0 AND 5 AND ((status = 'queued' AND workflow_version = 0 AND updated_at = created_at AND active_attempt_id IS NULL AND last_attempt_id IS NULL AND last_attempt_number = 0 AND repair_cycles = 0 AND candidate_revision IS NULL AND accepted_result_id IS NULL) OR (last_attempt_id IS NOT NULL AND last_attempt_number = 1 AND ((status = 'claimed' AND workflow_version = 1 AND active_attempt_id IS NOT NULL AND active_attempt_id = last_attempt_id AND repair_cycles = 0 AND candidate_revision IS NULL AND accepted_result_id IS NULL) OR (status = 'running' AND workflow_version = 2 + 2 * repair_cycles AND active_attempt_id IS NOT NULL AND active_attempt_id = last_attempt_id AND candidate_revision IS NULL AND ((repair_cycles = 0 AND accepted_result_id IS NULL) OR (repair_cycles > 0 AND accepted_result_id IS NOT NULL))) OR (status IN ('testing', 'publishing_pr', 'awaiting_review') AND accepted_result_id IS NOT NULL AND candidate_revision IS NOT NULL AND candidate_revision ~ '^[0-9a-f]{40}$' AND ((status = 'testing' AND workflow_version = 3 + 2 * repair_cycles AND active_attempt_id IS NOT NULL AND active_attempt_id = last_attempt_id) OR (status = 'publishing_pr' AND workflow_version = 4 + 2 * repair_cycles AND active_attempt_id IS NOT NULL AND active_attempt_id = last_attempt_id) OR (status = 'awaiting_review' AND workflow_version = 5 + 2 * repair_cycles AND active_attempt_id IS NULL)))))));
ALTER TABLE job_workflows ADD CONSTRAINT fk_workflow_result FOREIGN KEY(accepted_result_id, job_id) REFERENCES attempt_results (id, job_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE job_workflow_events ADD CONSTRAINT ck_job_workflow_events_claim_state CHECK (((workflow_version = 0 AND status = 'queued' AND event_type = 'submitted' AND attempt_id IS NULL AND previous_version IS NULL AND previous_status IS NULL) OR (workflow_version = 1 AND status = 'claimed' AND event_type = 'claimed' AND attempt_id IS NOT NULL AND previous_version = 0 AND previous_version IS NOT NULL AND previous_status = 'queued' AND previous_status IS NOT NULL) OR (workflow_version = 2 AND status = 'running' AND event_type = 'acknowledged' AND attempt_id IS NOT NULL AND previous_version = 1 AND previous_version IS NOT NULL AND previous_status = 'claimed' AND previous_status IS NOT NULL)) AND result_id IS NULL AND cycle IS NULL AND candidate_revision IS NULL OR (workflow_version > 2 AND attempt_id IS NOT NULL AND result_id IS NOT NULL AND cycle IS NOT NULL AND cycle BETWEEN 0 AND 5 AND previous_version IS NOT NULL AND previous_version = workflow_version - 1 AND previous_status IS NOT NULL AND ((status = 'testing' AND workflow_version = 3 + 2 * cycle AND event_type = 'tested' AND previous_status = 'running' AND candidate_revision IS NOT NULL AND candidate_revision ~ '^[0-9a-f]{40}$') OR (status = 'running' AND workflow_version = 2 + 2 * cycle AND cycle > 0 AND event_type = 'repair_requested' AND previous_status = 'testing' AND candidate_revision IS NULL) OR (status = 'publishing_pr' AND workflow_version = 4 + 2 * cycle AND event_type = 'checks_accepted' AND previous_status = 'testing' AND candidate_revision IS NOT NULL AND candidate_revision ~ '^[0-9a-f]{40}$') OR (status = 'awaiting_review' AND workflow_version = 5 + 2 * cycle AND event_type = 'published' AND previous_status = 'publishing_pr' AND candidate_revision IS NOT NULL AND candidate_revision ~ '^[0-9a-f]{40}$'))));
ALTER TABLE job_workflow_events ADD CONSTRAINT fk_workflow_event_previous FOREIGN KEY(job_id, previous_version, previous_status) REFERENCES job_workflow_events (job_id, workflow_version, status) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE job_workflow_events ADD CONSTRAINT fk_workflow_event_result FOREIGN KEY(result_id, job_id) REFERENCES attempt_results (id, job_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE dispatch_slots ADD CONSTRAINT ck_dispatch_slots_release CHECK ((release_count = 0 AND last_released_result_id IS NULL) OR (release_count > 0 AND last_released_result_id IS NOT NULL));
ALTER TABLE dispatch_slots ADD CONSTRAINT ck_dispatch_slots_version CHECK (version = receipt_count + release_count);
ALTER TABLE dispatch_slots ADD CONSTRAINT fk_dispatch_last_release FOREIGN KEY(last_released_result_id, release_count) REFERENCES delivery_operations (result_id, release_ordinal) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE execution_attempts ADD CONSTRAINT ck_execution_attempts_ack_state CHECK (expires_at > issued_at AND ((status = 'claimed' AND updated_at = issued_at AND acknowledged_at IS NULL AND revoked_at IS NULL AND ended_at IS NULL) OR (acknowledged_at IS NOT NULL AND acknowledged_at >= issued_at AND acknowledged_at < expires_at AND updated_at >= acknowledged_at AND ((status IN ('running', 'publishing_pr') AND revoked_at IS NULL AND ended_at IS NULL) OR (status = 'succeeded' AND revoked_at IS NOT NULL AND ended_at IS NOT NULL AND revoked_at = ended_at AND ended_at = updated_at AND ended_at >= acknowledged_at)))));
ALTER TABLE attempt_lease_authorities ADD CONSTRAINT ck_lease_authority_revocation CHECK (revoked_at IS NULL OR revoked_at >= updated_at);
ALTER TABLE dispatch_slots ADD CONSTRAINT ck_dispatch_release_count CHECK (release_count >= 0 AND release_count <= receipt_count);
"""

_GUARDS = r"""
CREATE OR REPLACE FUNCTION devrelay_guard_claim_transition()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  IF TG_OP <> 'UPDATE' THEN
    RAISE EXCEPTION 'Coordination records cannot be deleted.' USING ERRCODE = '23514';
  END IF;
  IF TG_TABLE_NAME = 'job_workflows' THEN
    IF NEW.workflow_version <> OLD.workflow_version + 1 OR NEW.updated_at < OLD.updated_at
      OR (NEW.job_id, NEW.source_task_id, NEW.repository_id, NEW.created_at, NEW.blocks_new_submission)
        IS DISTINCT FROM (OLD.job_id, OLD.source_task_id, OLD.repository_id, OLD.created_at, OLD.blocks_new_submission)
      OR NOT (
        (OLD.status = 'queued' AND NEW.status = 'claimed')
        OR (OLD.status = 'claimed' AND NEW.status = 'running' AND NEW.workflow_version = 2
          AND (NEW.active_attempt_id, NEW.last_attempt_id, NEW.last_attempt_number, NEW.repair_cycles,
               NEW.candidate_revision, NEW.accepted_result_id)
            IS NOT DISTINCT FROM (OLD.active_attempt_id, OLD.last_attempt_id, OLD.last_attempt_number,
               OLD.repair_cycles, OLD.candidate_revision, OLD.accepted_result_id))
        OR (OLD.status = 'running' AND NEW.status = 'testing'
          AND NEW.repair_cycles = OLD.repair_cycles
          AND (OLD.accepted_result_id IS NULL OR NEW.accepted_result_id = OLD.accepted_result_id)
          AND (NEW.active_attempt_id, NEW.last_attempt_id, NEW.last_attempt_number)
            IS NOT DISTINCT FROM (OLD.active_attempt_id, OLD.last_attempt_id, OLD.last_attempt_number))
        OR (OLD.status = 'testing' AND NEW.status = 'running'
          AND NEW.repair_cycles = OLD.repair_cycles + 1
          AND (NEW.active_attempt_id, NEW.last_attempt_id, NEW.last_attempt_number, NEW.accepted_result_id)
            IS NOT DISTINCT FROM (OLD.active_attempt_id, OLD.last_attempt_id, OLD.last_attempt_number, OLD.accepted_result_id))
        OR (OLD.status = 'testing' AND NEW.status = 'publishing_pr'
          AND (NEW.active_attempt_id, NEW.last_attempt_id, NEW.last_attempt_number, NEW.repair_cycles,
               NEW.candidate_revision, NEW.accepted_result_id)
            IS NOT DISTINCT FROM (OLD.active_attempt_id, OLD.last_attempt_id, OLD.last_attempt_number,
               OLD.repair_cycles, OLD.candidate_revision, OLD.accepted_result_id))
        OR (OLD.status = 'publishing_pr' AND NEW.status = 'awaiting_review' AND NEW.active_attempt_id IS NULL
          AND (NEW.last_attempt_id, NEW.last_attempt_number, NEW.repair_cycles, NEW.candidate_revision, NEW.accepted_result_id)
            IS NOT DISTINCT FROM (OLD.last_attempt_id, OLD.last_attempt_number, OLD.repair_cycles,
               OLD.candidate_revision, OLD.accepted_result_id))
      )
    THEN RAISE EXCEPTION 'Unsupported workflow transition.' USING ERRCODE = '23514'; END IF;
  ELSIF TG_TABLE_NAME = 'worker_coordination' THEN
    IF NEW.heartbeat_receipt_count <> OLD.heartbeat_receipt_count THEN
      IF NEW.heartbeat_receipt_count <> OLD.heartbeat_receipt_count + 1
        OR (NEW.worker_id, NEW.enrolled_at, NEW.next_poll_at, NEW.receipt_count, NEW.version)
          IS DISTINCT FROM (OLD.worker_id, OLD.enrolled_at, OLD.next_poll_at, OLD.receipt_count, OLD.version)
      THEN RAISE EXCEPTION 'Heartbeat counters must advance alone.' USING ERRCODE = '23514'; END IF;
    ELSIF (NEW.worker_id, NEW.enrolled_at) IS DISTINCT FROM (OLD.worker_id, OLD.enrolled_at)
      OR NEW.receipt_count <> OLD.receipt_count + 1 OR NEW.version <> OLD.version + 1
      OR NEW.next_poll_at < OLD.next_poll_at
    THEN RAISE EXCEPTION 'Worker coordination must advance monotonically.' USING ERRCODE = '23514'; END IF;
  ELSE
    IF NEW.heartbeat_receipt_count <> OLD.heartbeat_receipt_count THEN
      IF NEW.heartbeat_receipt_count <> OLD.heartbeat_receipt_count + 1
        OR (NEW.name, NEW.reserved_attempt_id, NEW.receipt_count, NEW.worker_count, NEW.version,
            NEW.release_count, NEW.last_released_result_id)
          IS DISTINCT FROM (OLD.name, OLD.reserved_attempt_id, OLD.receipt_count, OLD.worker_count,
            OLD.version, OLD.release_count, OLD.last_released_result_id)
      THEN RAISE EXCEPTION 'Heartbeat counters must advance alone.' USING ERRCODE = '23514'; END IF;
    ELSIF NEW.release_count <> OLD.release_count THEN
      IF NEW.release_count <> OLD.release_count + 1 OR NEW.version <> OLD.version + 1
        OR OLD.reserved_attempt_id IS NULL OR NEW.reserved_attempt_id IS NOT NULL
        OR NEW.last_released_result_id IS NULL
        OR (NEW.name, NEW.receipt_count, NEW.worker_count)
          IS DISTINCT FROM (OLD.name, OLD.receipt_count, OLD.worker_count)
      THEN RAISE EXCEPTION 'Dispatch release requires exactly one terminal operation.' USING ERRCODE = '23514'; END IF;
    ELSE
      IF NEW.name <> OLD.name OR NEW.receipt_count <> OLD.receipt_count + 1
        OR NEW.version <> OLD.version + 1 OR NEW.worker_count < OLD.worker_count
        OR NEW.worker_count > OLD.worker_count + 1
        OR NEW.last_released_result_id IS DISTINCT FROM OLD.last_released_result_id
        OR (OLD.reserved_attempt_id IS NOT NULL AND NEW.reserved_attempt_id IS DISTINCT FROM OLD.reserved_attempt_id)
      THEN RAISE EXCEPTION 'Dispatch reservation and counters cannot reset.' USING ERRCODE = '23514'; END IF;
    END IF;
  END IF;
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION devrelay_guard_attempt_acknowledgement()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  IF TG_OP = 'INSERT' AND NEW.status = 'claimed' THEN RETURN NEW; END IF;
  IF TG_OP <> 'UPDATE' THEN
    RAISE EXCEPTION 'Attempts require an initial claim and cannot be deleted.' USING ERRCODE = '23514';
  END IF;
  IF NEW.updated_at < OLD.updated_at
    OR (NEW.id, NEW.job_id, NEW.worker_id, NEW.claim_request_id, NEW.attempt_number,
        NEW.generation, NEW.issued_at, NEW.expires_at)
      IS DISTINCT FROM (OLD.id, OLD.job_id, OLD.worker_id, OLD.claim_request_id,
        OLD.attempt_number, OLD.generation, OLD.issued_at, OLD.expires_at)
    OR NOT (
      (OLD.status = 'claimed' AND NEW.status = 'running')
      OR (OLD.status = 'running' AND NEW.status = 'publishing_pr'
        AND NEW.acknowledged_at IS NOT DISTINCT FROM OLD.acknowledged_at)
      OR (OLD.status = 'publishing_pr' AND NEW.status = 'succeeded'
        AND NEW.acknowledged_at IS NOT DISTINCT FROM OLD.acknowledged_at)
    )
  THEN RAISE EXCEPTION 'Unsupported attempt transition.' USING ERRCODE = '23514'; END IF;
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION devrelay_guard_lease_authority()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'Lease authority cannot be deleted.' USING ERRCODE = '23514';
  ELSIF TG_OP = 'INSERT' THEN
    IF NEW.lease_revision <> 1 OR NEW.receipt_count <> 0 OR NEW.revoked_at IS NOT NULL THEN
      RAISE EXCEPTION 'Authority must begin at its original assignment.' USING ERRCODE = '23514';
    END IF;
  ELSE
    IF (NEW.attempt_id, NEW.job_id, NEW.worker_id, NEW.claim_request_id, NEW.generation)
      IS DISTINCT FROM (OLD.attempt_id, OLD.job_id, OLD.worker_id, OLD.claim_request_id, OLD.generation)
      OR OLD.revoked_at IS NOT NULL
    THEN RAISE EXCEPTION 'Authority ownership and revocation cannot reset.' USING ERRCODE = '23514'; END IF;
    IF NEW.revoked_at IS NOT NULL THEN
      IF (NEW.lease_revision, NEW.updated_at, NEW.current_expires_at, NEW.receipt_count, NEW.next_heartbeat_at)
        IS DISTINCT FROM (OLD.lease_revision, OLD.updated_at, OLD.current_expires_at, OLD.receipt_count, OLD.next_heartbeat_at)
      THEN RAISE EXCEPTION 'Revocation preserves the last renewal.' USING ERRCODE = '23514'; END IF;
    ELSIF NEW.receipt_count <> OLD.receipt_count + 1 OR NEW.next_heartbeat_at <= OLD.next_heartbeat_at
      OR NOT ((NEW.lease_revision = OLD.lease_revision AND NEW.updated_at = OLD.updated_at
        AND NEW.current_expires_at = OLD.current_expires_at)
        OR (NEW.lease_revision = OLD.lease_revision + 1 AND NEW.updated_at >= OLD.updated_at
          AND NEW.current_expires_at > OLD.current_expires_at))
    THEN RAISE EXCEPTION 'Authority must advance with one heartbeat.' USING ERRCODE = '23514'; END IF;
  END IF;
  RETURN NEW;
END $$;

CREATE FUNCTION devrelay_guard_delivery_operation()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.state <> 'accepted' OR NEW.version <> 0 THEN
      RAISE EXCEPTION 'Publication begins with accepted intent.' USING ERRCODE = '23514'; END IF;
  ELSIF TG_OP <> 'UPDATE' OR OLD.state = 'finalized' OR NEW.result_id <> OLD.result_id
    OR NEW.version <> OLD.version + 1 OR NEW.event_count <> OLD.event_count + 1
    OR (OLD.invocation_id IS NOT NULL AND NEW.invocation_id IS DISTINCT FROM OLD.invocation_id)
    OR NOT ((OLD.state = 'accepted' AND NEW.state = 'publishing')
      OR (OLD.state = 'publishing' AND NEW.state IN ('publishing', 'uncertain', 'finalized'))
      OR (OLD.state = 'uncertain' AND NEW.state = 'uncertain'))
  THEN RAISE EXCEPTION 'Unsupported publication operation transition.' USING ERRCODE = '23514'; END IF;
  RETURN NEW;
END $$;
"""

_LINKS = r"""
CREATE FUNCTION devrelay_require_attempt_links(identity uuid)
RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$ BEGIN
  IF identity IS NULL THEN RETURN; END IF;
  IF NOT EXISTS (
    SELECT 1 FROM execution_attempts a
    JOIN job_workflows w ON w.job_id = a.job_id AND w.last_attempt_id = a.id
      AND w.last_attempt_number = a.attempt_number
    JOIN jobs j ON j.id = a.job_id AND j.provider_repository_id IS NOT NULL AND j.definition_version = 2
    JOIN job_workflow_events e ON e.job_id = w.job_id AND e.workflow_version = w.workflow_version
      AND e.status = w.status AND e.attempt_id = a.id AND e.occurred_at = w.updated_at
    JOIN job_workflow_events c ON c.job_id = a.job_id AND c.workflow_version = 1
      AND c.attempt_id = a.id AND c.occurred_at = a.issued_at AND c.status = 'claimed'
    JOIN worker_claim_receipts q ON q.worker_id = a.worker_id AND q.claim_request_id = a.claim_request_id
      AND q.attempt_id = a.id AND q.job_id = a.job_id AND q.outcome = 'assigned' AND q.recorded_at = a.issued_at
    JOIN attempt_lease_authorities l ON l.attempt_id = a.id
    JOIN dispatch_slots s ON s.name = 'coding'
    LEFT JOIN worker_acknowledgement_receipts k ON k.attempt_id = a.id
    LEFT JOIN job_workflow_events ack ON ack.job_id = a.job_id AND ack.workflow_version = 2
    LEFT JOIN attempt_results r ON r.id = w.accepted_result_id AND r.attempt_id = a.id
    LEFT JOIN delivery_operations o ON o.result_id = r.id
    WHERE a.id = identity AND a.updated_at = w.updated_at AND (
      (a.status = 'claimed' AND w.status = 'claimed' AND w.workflow_version = 1
        AND w.active_attempt_id = a.id AND s.reserved_attempt_id = a.id
        AND k.attempt_id IS NULL AND l.revoked_at IS NULL)
      OR (k.worker_id = a.worker_id AND k.job_id = a.job_id AND k.claim_request_id = a.claim_request_id
        AND k.generation = a.generation AND k.snapshot_id = j.snapshot_id AND k.base_revision = j.base_revision
        AND k.repository_id = j.repository_id AND k.acknowledged_at = a.acknowledged_at
        AND ack.status = 'running' AND ack.event_type = 'acknowledged' AND ack.attempt_id = a.id
        AND ack.occurred_at = a.acknowledged_at AND (
          (a.status = 'running' AND w.status = 'running' AND w.workflow_version = 2
            AND a.updated_at = a.acknowledged_at AND w.active_attempt_id = a.id
            AND s.reserved_attempt_id = a.id AND l.revoked_at IS NULL)
          OR (a.status = 'publishing_pr' AND w.status = 'publishing_pr'
            AND w.workflow_version = r.publishing_workflow_version AND w.repair_cycles = r.repair_cycles
            AND w.candidate_revision = r.candidate_revision AND a.updated_at = r.recorded_at
            AND w.active_attempt_id = a.id AND s.reserved_attempt_id = a.id
            AND l.revoked_at IS NULL AND o.state IN ('accepted', 'publishing', 'uncertain'))
          OR (a.status = 'succeeded' AND w.status = 'awaiting_review' AND w.active_attempt_id IS NULL
            AND w.workflow_version = r.publishing_workflow_version + 1
            AND w.repair_cycles = r.repair_cycles AND w.candidate_revision = r.candidate_revision
            AND o.state = 'finalized' AND o.final_workflow_version = w.workflow_version
            AND o.completed_at = a.ended_at AND a.revoked_at = a.ended_at AND l.revoked_at = a.ended_at
            AND a.ended_at = w.updated_at AND a.ended_at < l.current_expires_at
            AND s.reserved_attempt_id IS DISTINCT FROM a.id
            AND o.release_ordinal <= s.release_count)
        ))
    )
  ) THEN RAISE EXCEPTION 'Claim ownership must commit together.' USING ERRCODE = '23514'; END IF;
END $$;

CREATE OR REPLACE FUNCTION devrelay_validate_claim_links()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE identity uuid;
BEGIN
  IF TG_TABLE_NAME = 'execution_attempts' THEN identity := NEW.id;
  ELSIF TG_TABLE_NAME = 'job_workflows' THEN identity := NEW.last_attempt_id;
  ELSIF TG_TABLE_NAME = 'dispatch_slots' THEN
    identity := NEW.reserved_attempt_id;
    IF TG_OP = 'UPDATE' AND NEW.release_count <> OLD.release_count THEN
      IF NOT EXISTS (SELECT 1 FROM attempt_results r JOIN delivery_operations o ON o.result_id = r.id
        WHERE r.id = NEW.last_released_result_id AND r.attempt_id = OLD.reserved_attempt_id
          AND o.state = 'finalized' AND o.release_ordinal = NEW.release_count)
      THEN RAISE EXCEPTION 'Dispatch release requires its exact finalized result.' USING ERRCODE = '23514'; END IF;
      PERFORM devrelay_require_attempt_links(OLD.reserved_attempt_id);
    END IF;
  ELSE identity := NEW.attempt_id; END IF;
  PERFORM devrelay_require_attempt_links(identity);
  RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION devrelay_validate_initial_workflow()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE identity uuid;
BEGIN
  IF TG_TABLE_NAME = 'jobs' THEN identity := NEW.id; ELSE identity := NEW.job_id; END IF;
  IF NOT EXISTS (
    SELECT 1 FROM jobs j JOIN job_workflows w ON w.job_id = j.id
    JOIN job_workflow_events initial ON initial.job_id = j.id AND initial.workflow_version = 0
    JOIN job_workflow_events e ON e.job_id = w.job_id AND e.workflow_version = w.workflow_version
    WHERE j.id = identity AND w.created_at = j.created_at AND initial.occurred_at = j.created_at
      AND e.status = w.status AND e.occurred_at = w.updated_at
      AND e.attempt_id IS NOT DISTINCT FROM w.last_attempt_id
      AND e.result_id IS NOT DISTINCT FROM w.accepted_result_id
      AND (w.workflow_version <= 2 OR (e.cycle = w.repair_cycles
        AND e.candidate_revision IS NOT DISTINCT FROM w.candidate_revision))
  ) THEN RAISE EXCEPTION 'Workflow and history must match the definition.' USING ERRCODE = '23514'; END IF;
  IF TG_TABLE_NAME = 'job_workflow_events' THEN
    IF NOT EXISTS (SELECT 1 FROM job_workflows w
      LEFT JOIN job_workflow_events p ON p.job_id = NEW.job_id AND p.workflow_version = NEW.previous_version
      LEFT JOIN attempt_results r ON r.id = NEW.result_id
      LEFT JOIN delivery_operations o ON o.result_id = r.id
      WHERE w.job_id = NEW.job_id AND NEW.workflow_version <= w.workflow_version
        AND (NEW.workflow_version = 0 OR (p.status = NEW.previous_status AND p.occurred_at <= NEW.occurred_at))
        AND (NEW.workflow_version <= 2 OR (r.id = w.accepted_result_id AND r.attempt_id = NEW.attempt_id
          AND NEW.cycle <= r.repair_cycles AND ((NEW.status <> 'awaiting_review' AND NEW.occurred_at = r.recorded_at)
            OR (NEW.status = 'awaiting_review' AND NEW.occurred_at = o.completed_at)))))
    THEN RAISE EXCEPTION 'Workflow event chain is inconsistent.' USING ERRCODE = '23514'; END IF;
  END IF;
  RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION devrelay_validate_lease_authority()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE identity uuid;
BEGIN
  IF TG_TABLE_NAME = 'execution_attempts' THEN identity := NEW.id; ELSE identity := NEW.attempt_id; END IF;
  IF NOT EXISTS (
    SELECT 1 FROM attempt_lease_authorities l
    JOIN execution_attempts a ON a.id = l.attempt_id AND a.job_id = l.job_id
      AND a.worker_id = l.worker_id AND a.claim_request_id = l.claim_request_id AND a.generation = l.generation
    LEFT JOIN worker_heartbeat_receipts h ON h.attempt_id = l.attempt_id AND h.attempt_ordinal = l.receipt_count
    WHERE l.attempt_id = identity AND l.updated_at >= a.issued_at AND l.current_expires_at >= a.expires_at
      AND (l.lease_revision <> 1 OR (l.updated_at = a.issued_at AND l.current_expires_at = a.expires_at))
      AND l.revoked_at IS NOT DISTINCT FROM a.revoked_at
      AND ((l.receipt_count = 0 AND l.lease_revision = 1 AND l.next_heartbeat_at = a.issued_at AND h.request_id IS NULL)
        OR (l.receipt_count > 0 AND h.worker_id = l.worker_id AND h.claim_request_id = l.claim_request_id
          AND h.job_id = l.job_id AND h.generation = l.generation AND h.lease_revision = l.lease_revision
          AND h.updated_at = l.updated_at AND h.current_expires_at = l.current_expires_at AND l.next_heartbeat_at > h.recorded_at))
  ) THEN RAISE EXCEPTION 'Lease authority history is inconsistent.' USING ERRCODE = '23514'; END IF;
  IF TG_TABLE_NAME = 'attempt_lease_authorities' AND TG_OP = 'UPDATE' THEN
    IF NEW.revoked_at IS NOT NULL THEN
      PERFORM devrelay_require_attempt_links(NEW.attempt_id);
    ELSIF NOT EXISTS (SELECT 1 FROM worker_heartbeat_receipts h WHERE h.attempt_id = NEW.attempt_id
      AND h.attempt_ordinal = NEW.receipt_count AND h.expected_lease_revision = OLD.lease_revision
      AND h.lease_revision = NEW.lease_revision AND h.updated_at = NEW.updated_at
      AND h.current_expires_at = NEW.current_expires_at AND h.recorded_at >= OLD.next_heartbeat_at
      AND h.recorded_at < OLD.current_expires_at)
    THEN RAISE EXCEPTION 'Authority and heartbeat must commit together.' USING ERRCODE = '23514'; END IF;
  END IF;
  RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION devrelay_validate_heartbeat_receipt()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM execution_attempts a JOIN jobs j ON j.id = a.job_id AND j.repository_id = NEW.repository_id
    JOIN job_workflow_events ack ON ack.job_id = a.job_id AND ack.workflow_version = 2
      AND ack.status = 'running' AND ack.attempt_id = a.id AND ack.occurred_at = a.acknowledged_at
    JOIN attempt_lease_authorities l ON l.attempt_id = a.id AND l.receipt_count >= NEW.attempt_ordinal
    JOIN worker_coordination c ON c.worker_id = a.worker_id AND c.heartbeat_receipt_count >= NEW.worker_ordinal
    JOIN dispatch_slots s ON s.name = 'coding' AND s.heartbeat_receipt_count >= NEW.global_ordinal
    LEFT JOIN worker_heartbeat_receipts p ON p.attempt_id = a.id AND p.attempt_ordinal = NEW.attempt_ordinal - 1
    WHERE a.id = NEW.attempt_id AND a.job_id = NEW.job_id AND a.worker_id = NEW.worker_id
      AND a.claim_request_id = NEW.claim_request_id AND a.generation = NEW.generation
      AND NEW.recorded_at >= a.acknowledged_at AND (a.revoked_at IS NULL OR NEW.recorded_at < a.revoked_at)
      AND ((NEW.attempt_ordinal = 1 AND NEW.expected_lease_revision = 1 AND NEW.recorded_at < a.expires_at
        AND ((NEW.renewed AND NEW.current_expires_at > a.expires_at)
          OR (NOT NEW.renewed AND NEW.current_expires_at = a.expires_at AND NEW.updated_at = a.issued_at)))
        OR (NEW.attempt_ordinal > 1 AND NEW.expected_lease_revision = p.lease_revision
          AND NEW.recorded_at >= p.recorded_at AND NEW.recorded_at < p.current_expires_at
          AND ((NEW.renewed AND NEW.current_expires_at > p.current_expires_at)
            OR (NOT NEW.renewed AND NEW.current_expires_at = p.current_expires_at AND NEW.updated_at = p.updated_at))))
  ) THEN RAISE EXCEPTION 'Heartbeat receipt history is inconsistent.' USING ERRCODE = '23514'; END IF;
  RETURN NULL;
END $$;
"""

_RESULT_LINKS = r"""
CREATE FUNCTION devrelay_validate_coding_result()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE identity uuid;
BEGIN
  identity := NEW.id;
  IF NOT EXISTS (
    SELECT 1 FROM attempt_results r JOIN jobs j ON j.id = r.job_id
    JOIN execution_attempts a ON a.id = r.attempt_id AND a.job_id = r.job_id
      AND a.worker_id = r.worker_id AND a.claim_request_id = r.claim_request_id AND a.generation = r.generation
    JOIN worker_claim_receipts c ON c.worker_id = r.worker_id AND c.claim_request_id = r.claim_request_id
      AND c.fingerprint = r.claim_fingerprint AND c.attempt_id = a.id
    JOIN worker_acknowledgement_receipts k ON k.attempt_id = a.id
      AND k.definition_digest = r.definition_digest
    JOIN attempt_lease_authorities l ON l.attempt_id = a.id
    JOIN job_workflows w ON w.job_id = r.job_id AND w.accepted_result_id = r.id
    JOIN job_workflow_events accepted ON accepted.job_id = r.job_id
      AND accepted.workflow_version = r.publishing_workflow_version
    JOIN delivery_operations o ON o.result_id = r.id
    WHERE r.id = identity AND r.repository_id = j.repository_id AND r.snapshot_id = j.snapshot_id
      AND r.repair_cycles <= j.max_repair_cycles AND r.recorded_at >= a.acknowledged_at
      AND r.recorded_at < l.current_expires_at AND (l.revoked_at IS NULL OR r.recorded_at <= l.revoked_at)
      AND accepted.result_id = r.id AND accepted.attempt_id = a.id
      AND accepted.status = 'publishing_pr' AND accepted.event_type = 'checks_accepted'
      AND accepted.cycle = r.repair_cycles AND accepted.candidate_revision = r.candidate_revision
      AND accepted.occurred_at = r.recorded_at
  ) THEN RAISE EXCEPTION 'Coding result and publication intent must commit together.' USING ERRCODE = '23514'; END IF;
  PERFORM devrelay_require_attempt_links(NEW.attempt_id);
  RETURN NULL;
END $$;

CREATE FUNCTION devrelay_validate_delivery_operation()
RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE identity uuid;
BEGIN
  identity := NEW.result_id;
  IF NOT EXISTS (
    SELECT 1 FROM delivery_operations o JOIN attempt_results r ON r.id = o.result_id
    JOIN delivery_operation_events e ON e.result_id = o.result_id AND e.sequence = o.version
    JOIN job_workflows w ON w.accepted_result_id = r.id AND w.job_id = r.job_id
    JOIN jobs j ON j.id = r.job_id
    JOIN dispatch_slots s ON s.name = 'coding'
    WHERE o.result_id = identity AND e.invocation_id IS NOT DISTINCT FROM o.invocation_id
      AND ((o.state = 'accepted' AND e.kind = 'accepted' AND e.occurred_at = r.recorded_at)
        OR (o.state = 'publishing' AND e.kind IN ('publishing_started', 'write_planned', 'write_observed'))
        OR (o.state = 'uncertain' AND e.kind IN ('uncertain', 'write_observed'))
        OR (o.state = 'finalized' AND e.kind = 'finalized' AND e.occurred_at = o.completed_at
          AND o.final_workflow_version = r.publishing_workflow_version + 1
          AND w.status = 'awaiting_review' AND w.workflow_version = o.final_workflow_version
          AND o.pr_repository_id = r.repository_id AND o.pr_provider_repository_id = j.provider_repository_id
          AND o.pr_branch = j.authorized_branch AND o.pr_target_branch = j.target_branch
          AND o.pr_head_revision = r.candidate_revision AND o.completed_at >= r.recorded_at
          AND o.release_ordinal <= s.release_count
          AND (o.release_ordinal <> s.release_count OR s.last_released_result_id = r.id)))
  ) THEN RAISE EXCEPTION 'Publication projection and event witness are inconsistent.' USING ERRCODE = '23514'; END IF;
  IF TG_TABLE_NAME = 'delivery_operation_events' THEN
    IF NOT EXISTS (
      SELECT 1 FROM delivery_operations o JOIN attempt_results r ON r.id = o.result_id
      LEFT JOIN delivery_operation_events p ON p.result_id = NEW.result_id AND p.sequence = NEW.sequence - 1
      WHERE o.result_id = NEW.result_id AND NEW.sequence <= o.version
        AND NEW.occurred_at >= r.recorded_at
        AND NEW.invocation_id IS NOT DISTINCT FROM CASE WHEN NEW.sequence = 0 THEN NULL ELSE o.invocation_id END
        AND ((NEW.sequence = 0 AND NEW.kind = 'accepted' AND NEW.occurred_at = r.recorded_at)
          OR (p.occurred_at <= NEW.occurred_at AND (
            (NEW.sequence = 1 AND NEW.kind = 'publishing_started' AND p.kind = 'accepted')
            OR (NEW.kind = 'write_planned' AND p.kind IN ('publishing_started', 'write_observed')
              AND NOT EXISTS (SELECT 1 FROM delivery_operation_events u
                WHERE u.result_id = NEW.result_id AND u.kind = 'uncertain' AND u.sequence < NEW.sequence))
            OR (NEW.kind IN ('write_observed', 'uncertain') AND p.kind IN ('write_planned', 'uncertain')
              AND NEW.step_id = p.step_id AND NEW.step_kind = p.step_kind AND NEW.input_digest = p.input_digest
              AND (NEW.kind <> 'uncertain' OR p.kind = 'write_planned'))
            OR (NEW.kind = 'finalized' AND p.kind = 'write_observed'
              AND NOT EXISTS (SELECT 1 FROM delivery_operation_events u
                WHERE u.result_id = NEW.result_id AND u.kind = 'uncertain' AND u.sequence < NEW.sequence))
          )))
    ) THEN RAISE EXCEPTION 'Publication event chain is inconsistent.' USING ERRCODE = '23514'; END IF;
  END IF;
  PERFORM devrelay_require_attempt_links((SELECT attempt_id FROM attempt_results WHERE id = identity));
  RETURN NULL;
END $$;
"""


def upgrade() -> None:
    op.execute(
        "LOCK TABLE dispatch_slots, worker_coordination, job_workflows, "
        "job_workflow_events, execution_attempts, worker_claim_receipts, "
        "worker_acknowledgement_receipts, attempt_lease_authorities, "
        "worker_heartbeat_receipts IN ACCESS EXCLUSIVE MODE"
    )
    # Earlier revisions can backfill authority rows in this same transaction.
    # Validate their queued FK/history events before altering those relations.
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
    op.execute("SET CONSTRAINTS ALL DEFERRED")
    op.execute(_TABLES)
    op.execute(_SCHEMA)
    op.execute(_GUARDS)
    op.execute(_LINKS)
    op.execute(_RESULT_LINKS)
    op.execute("""
      CREATE TRIGGER attempt_results_no_mutation BEFORE UPDATE OR DELETE ON attempt_results
        FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation();
      CREATE TRIGGER delivery_events_no_mutation BEFORE UPDATE OR DELETE ON delivery_operation_events
        FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation();
      CREATE TRIGGER delivery_operation_transition BEFORE INSERT OR UPDATE OR DELETE ON delivery_operations
        FOR EACH ROW EXECUTE FUNCTION devrelay_guard_delivery_operation();
      CREATE CONSTRAINT TRIGGER coding_result_links AFTER INSERT ON attempt_results
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_coding_result();
      CREATE CONSTRAINT TRIGGER delivery_operation_links AFTER INSERT OR UPDATE ON delivery_operations
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_delivery_operation();
      CREATE CONSTRAINT TRIGGER delivery_event_links AFTER INSERT ON delivery_operation_events
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_delivery_operation();
    """)


def downgrade() -> None:
    op.execute(
        "LOCK TABLE dispatch_slots, worker_coordination, job_workflows, job_workflow_events, "
        "execution_attempts, worker_claim_receipts, worker_acknowledgement_receipts, "
        "attempt_lease_authorities, worker_heartbeat_receipts, attempt_results, "
        "delivery_operations, delivery_operation_events IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
    op.execute("SET CONSTRAINTS ALL DEFERRED")
    op.execute("""
      DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM attempt_results) OR EXISTS (SELECT 1 FROM delivery_operations)
          OR EXISTS (SELECT 1 FROM delivery_operation_events)
          OR EXISTS (SELECT 1 FROM job_workflows WHERE workflow_version > 2
            OR repair_cycles <> 0 OR candidate_revision IS NOT NULL OR accepted_result_id IS NOT NULL)
          OR EXISTS (SELECT 1 FROM job_workflow_events WHERE workflow_version > 2
            OR result_id IS NOT NULL OR cycle IS NOT NULL OR candidate_revision IS NOT NULL)
          OR EXISTS (SELECT 1 FROM dispatch_slots WHERE release_count <> 0 OR last_released_result_id IS NOT NULL)
          OR EXISTS (SELECT 1 FROM execution_attempts WHERE status NOT IN ('claimed', 'running')
            OR revoked_at IS NOT NULL OR ended_at IS NOT NULL)
          OR EXISTS (SELECT 1 FROM attempt_lease_authorities WHERE revoked_at IS NOT NULL)
        THEN RAISE EXCEPTION 'Cannot discard accepted coding/publication history.' USING ERRCODE = '23514'; END IF;
      END $$;
      DROP TRIGGER coding_result_links ON attempt_results;
      DROP TRIGGER delivery_operation_links ON delivery_operations;
      DROP TRIGGER delivery_event_links ON delivery_operation_events;
      DROP FUNCTION devrelay_validate_coding_result();
      DROP FUNCTION devrelay_validate_delivery_operation();
      DROP TRIGGER delivery_operation_transition ON delivery_operations;
      DROP FUNCTION devrelay_guard_delivery_operation();
      ALTER TABLE job_workflows DROP CONSTRAINT fk_workflow_result, DROP CONSTRAINT ck_job_workflows_claim_state;
      ALTER TABLE job_workflow_events DROP CONSTRAINT fk_workflow_event_result,
        DROP CONSTRAINT fk_workflow_event_previous, DROP CONSTRAINT ck_job_workflow_events_claim_state;
      ALTER TABLE dispatch_slots DROP CONSTRAINT fk_dispatch_last_release,
        DROP CONSTRAINT ck_dispatch_slots_release, DROP CONSTRAINT ck_dispatch_slots_version,
        DROP CONSTRAINT ck_dispatch_release_count;
      ALTER TABLE execution_attempts DROP CONSTRAINT ck_execution_attempts_ack_state;
      ALTER TABLE attempt_lease_authorities DROP CONSTRAINT ck_lease_authority_revocation;
    """)
    legacy7 = importlib.import_module(
        "migrations.versions.0007_assignment_acknowledgements"
    )
    legacy8 = importlib.import_module("migrations.versions.0008_heartbeat_authorities")
    op.execute(legacy8._coordination_guard(True))
    op.execute(legacy7._claim_links(True))
    op.execute(_LEGACY_FUNCTIONS)
    op.execute("DROP FUNCTION devrelay_require_attempt_links(uuid)")
    op.execute("""
      ALTER TABLE job_workflows DROP COLUMN repair_cycles, DROP COLUMN candidate_revision, DROP COLUMN accepted_result_id;
      ALTER TABLE job_workflow_events DROP COLUMN result_id, DROP COLUMN cycle, DROP COLUMN candidate_revision;
      ALTER TABLE dispatch_slots DROP COLUMN release_count, DROP COLUMN last_released_result_id,
        ADD CONSTRAINT dispatch_slots_check CHECK (version = receipt_count);
      ALTER TABLE attempt_lease_authorities ADD CONSTRAINT attempt_lease_authorities_revoked_at_check CHECK (revoked_at IS NULL);
      ALTER TABLE delivery_operations DROP CONSTRAINT fk_delivery_operations_result_id_delivery_operation_events;
      DROP TABLE delivery_operation_events;
      DROP TABLE delivery_operations;
      DROP TABLE attempt_results;
      ALTER TABLE execution_attempts ADD CONSTRAINT ck_execution_attempts_ack_state CHECK (
        expires_at > issued_at AND revoked_at IS NULL AND ended_at IS NULL
        AND ((status = 'claimed' AND updated_at = issued_at AND acknowledged_at IS NULL)
          OR (status = 'running' AND acknowledged_at IS NOT NULL AND updated_at = acknowledged_at
            AND acknowledged_at >= issued_at AND acknowledged_at < expires_at)));
    """)
    op.execute(
        "ALTER TABLE job_workflows ADD CONSTRAINT ck_job_workflows_claim_state CHECK ("
        + legacy7._WORKFLOW
        + legacy7._RUNNING
        + "))"
    )
    op.execute(
        "ALTER TABLE job_workflow_events ADD CONSTRAINT ck_job_workflow_events_claim_state CHECK ("
        + legacy7._EVENT
        + legacy7._ACK_EVENT
        + ")"
    )


# Exact previous migration bodies; no current application model dependency.
_LEGACY_FUNCTIONS = r"""
CREATE OR REPLACE FUNCTION devrelay_validate_initial_workflow()
        RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
        DECLARE identity uuid;
        BEGIN
          IF TG_TABLE_NAME = 'jobs' THEN identity := NEW.id;
          ELSE identity := NEW.job_id; END IF;
          IF NOT EXISTS (
            SELECT 1 FROM jobs j
            JOIN job_workflows w ON w.job_id = j.id
            JOIN job_workflow_events initial ON initial.job_id = j.id AND initial.workflow_version = 0
            JOIN job_workflow_events e ON e.job_id = w.job_id
              AND e.workflow_version = w.workflow_version AND e.status = w.status
            WHERE j.id = identity AND w.created_at = j.created_at
              AND initial.occurred_at = j.created_at
              AND e.occurred_at = w.updated_at
              AND e.attempt_id IS NOT DISTINCT FROM w.active_attempt_id
          ) THEN
            RAISE EXCEPTION 'Workflow and history must match the definition.' USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END $$;

CREATE OR REPLACE FUNCTION devrelay_guard_attempt_acknowledgement()
      RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF TG_OP = 'INSERT' AND NEW.status = 'claimed' THEN RETURN NEW; END IF;
        IF TG_OP <> 'UPDATE' THEN RAISE EXCEPTION 'Attempts require initial claim and cannot be deleted.' USING ERRCODE = '23514'; END IF;
        IF OLD.status <> 'claimed' OR NEW.status <> 'running'
          OR (NEW.id, NEW.job_id, NEW.worker_id, NEW.claim_request_id, NEW.attempt_number,
              NEW.generation, NEW.issued_at, NEW.expires_at, NEW.revoked_at, NEW.ended_at)
             IS DISTINCT FROM (OLD.id, OLD.job_id, OLD.worker_id, OLD.claim_request_id, OLD.attempt_number,
              OLD.generation, OLD.issued_at, OLD.expires_at, OLD.revoked_at, OLD.ended_at)
        THEN RAISE EXCEPTION 'Only initial acknowledgement may update an attempt.' USING ERRCODE = '23514'; END IF;
        RETURN NEW;
      END $$;

CREATE OR REPLACE FUNCTION devrelay_guard_lease_authority()
      RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'Lease authority cannot be deleted.' USING ERRCODE = '23514';
        ELSIF TG_OP = 'INSERT' THEN
          IF NEW.lease_revision <> 1 OR NEW.receipt_count <> 0 THEN
            RAISE EXCEPTION 'Lease authority must begin at its original assignment.' USING ERRCODE = '23514';
          END IF;
        ELSE
          IF (NEW.attempt_id, NEW.job_id, NEW.worker_id, NEW.claim_request_id, NEW.generation, NEW.revoked_at)
            IS DISTINCT FROM (OLD.attempt_id, OLD.job_id, OLD.worker_id, OLD.claim_request_id, OLD.generation, OLD.revoked_at)
            OR NEW.receipt_count <> OLD.receipt_count + 1
            OR NEW.next_heartbeat_at <= OLD.next_heartbeat_at
            OR NOT ((NEW.lease_revision = OLD.lease_revision AND NEW.updated_at = OLD.updated_at
                     AND NEW.current_expires_at = OLD.current_expires_at)
              OR (NEW.lease_revision = OLD.lease_revision + 1 AND NEW.updated_at >= OLD.updated_at
                  AND NEW.current_expires_at > OLD.current_expires_at))
          THEN RAISE EXCEPTION 'Lease authority must advance with one heartbeat.' USING ERRCODE = '23514'; END IF;
        END IF;
        RETURN NEW;
      END $$;

CREATE OR REPLACE FUNCTION devrelay_validate_lease_authority()
      RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
      DECLARE identity uuid;
      BEGIN
        IF TG_TABLE_NAME = 'execution_attempts' THEN identity := NEW.id;
        ELSE identity := NEW.attempt_id; END IF;
        IF NOT EXISTS (
          SELECT 1 FROM attempt_lease_authorities l
          JOIN execution_attempts a ON a.id = l.attempt_id AND a.job_id = l.job_id
            AND a.worker_id = l.worker_id AND a.claim_request_id = l.claim_request_id AND a.generation = l.generation
          LEFT JOIN worker_heartbeat_receipts h ON h.attempt_id = l.attempt_id AND h.attempt_ordinal = l.receipt_count
          WHERE l.attempt_id = identity AND l.updated_at >= a.issued_at AND l.current_expires_at >= a.expires_at
            AND (l.lease_revision <> 1 OR (l.updated_at = a.issued_at AND l.current_expires_at = a.expires_at))
            AND ((l.receipt_count = 0 AND l.lease_revision = 1 AND l.next_heartbeat_at = a.issued_at AND h.request_id IS NULL)
              OR (l.receipt_count > 0 AND h.worker_id = l.worker_id AND h.claim_request_id = l.claim_request_id
                  AND h.job_id = l.job_id AND h.generation = l.generation AND h.lease_revision = l.lease_revision
                  AND h.updated_at = l.updated_at AND h.current_expires_at = l.current_expires_at
                  AND l.next_heartbeat_at > h.recorded_at))
        ) THEN RAISE EXCEPTION 'Lease authority history is inconsistent.' USING ERRCODE = '23514'; END IF;
        IF TG_TABLE_NAME = 'attempt_lease_authorities' AND TG_OP = 'UPDATE' THEN
          IF NOT EXISTS (
            SELECT 1 FROM worker_heartbeat_receipts h WHERE h.attempt_id = NEW.attempt_id
              AND h.attempt_ordinal = NEW.receipt_count AND h.expected_lease_revision = OLD.lease_revision
              AND h.lease_revision = NEW.lease_revision AND h.updated_at = NEW.updated_at
              AND h.current_expires_at = NEW.current_expires_at
              AND h.recorded_at >= OLD.next_heartbeat_at AND h.recorded_at < OLD.current_expires_at
          ) THEN RAISE EXCEPTION 'Authority and heartbeat must commit together.' USING ERRCODE = '23514'; END IF;
        END IF;
        RETURN NULL;
      END $$;

CREATE OR REPLACE FUNCTION devrelay_validate_heartbeat_receipt()
      RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM execution_attempts a
          JOIN jobs j ON j.id = a.job_id AND j.repository_id = NEW.repository_id
          JOIN job_workflows w ON w.job_id = a.job_id AND w.active_attempt_id = a.id
            AND w.status = 'running' AND w.workflow_version = 2
          JOIN attempt_lease_authorities l ON l.attempt_id = a.id AND l.receipt_count >= NEW.attempt_ordinal
          JOIN worker_coordination c ON c.worker_id = a.worker_id AND c.heartbeat_receipt_count >= NEW.worker_ordinal
          JOIN dispatch_slots s ON s.name = 'coding' AND s.reserved_attempt_id = a.id
            AND s.heartbeat_receipt_count >= NEW.global_ordinal
          LEFT JOIN worker_heartbeat_receipts p ON p.attempt_id = a.id AND p.attempt_ordinal = NEW.attempt_ordinal - 1
          WHERE a.id = NEW.attempt_id AND a.job_id = NEW.job_id AND a.worker_id = NEW.worker_id
            AND a.claim_request_id = NEW.claim_request_id AND a.generation = NEW.generation AND a.status = 'running'
            AND NEW.recorded_at >= a.acknowledged_at
            AND ((NEW.attempt_ordinal = 1 AND NEW.expected_lease_revision = 1
                  AND NEW.recorded_at < a.expires_at
                  AND ((NEW.renewed AND NEW.current_expires_at > a.expires_at)
                    OR (NOT NEW.renewed AND NEW.current_expires_at = a.expires_at AND NEW.updated_at = a.issued_at)))
              OR (NEW.attempt_ordinal > 1 AND NEW.expected_lease_revision = p.lease_revision
                  AND NEW.recorded_at >= p.recorded_at AND NEW.recorded_at < p.current_expires_at
                  AND ((NEW.renewed AND NEW.current_expires_at > p.current_expires_at)
                    OR (NOT NEW.renewed AND NEW.current_expires_at = p.current_expires_at AND NEW.updated_at = p.updated_at))))
        ) THEN RAISE EXCEPTION 'Heartbeat receipt history is inconsistent.' USING ERRCODE = '23514'; END IF;
        RETURN NULL;
      END $$;
"""
