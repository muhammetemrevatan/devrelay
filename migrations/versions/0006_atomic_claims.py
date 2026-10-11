"""Persist initial claims without changing accepted Job definitions."""

from alembic import op

revision = "0006_atomic_claims"
down_revision = "0005_queued_candidate_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE job_workflows, job_workflow_events IN ACCESS EXCLUSIVE MODE")
    op.execute("""
        CREATE TABLE worker_coordination (
          worker_id uuid PRIMARY KEY,
          enrolled_at timestamptz NOT NULL,
          next_poll_at timestamptz NOT NULL,
          receipt_count bigint NOT NULL CHECK (receipt_count > 0),
          version bigint NOT NULL CHECK (version = receipt_count),
          CHECK (next_poll_at >= enrolled_at)
        );
        CREATE TABLE execution_attempts (
          id uuid PRIMARY KEY,
          job_id uuid NOT NULL REFERENCES job_workflows(job_id),
          worker_id uuid NOT NULL REFERENCES worker_coordination(worker_id),
          claim_request_id uuid NOT NULL,
          attempt_number bigint NOT NULL CHECK (attempt_number = 1),
          generation bigint NOT NULL CHECK (generation = 1),
          status varchar(32) NOT NULL CHECK (status = 'claimed'),
          issued_at timestamptz NOT NULL,
          updated_at timestamptz NOT NULL,
          expires_at timestamptz NOT NULL,
          acknowledged_at timestamptz,
          revoked_at timestamptz,
          ended_at timestamptz,
          CHECK (updated_at = issued_at AND expires_at > issued_at),
          CHECK (acknowledged_at IS NULL AND revoked_at IS NULL AND ended_at IS NULL),
          UNIQUE (job_id, attempt_number),
          UNIQUE (id, job_id),
          UNIQUE (id, job_id, worker_id, claim_request_id)
        );
        CREATE UNIQUE INDEX uq_execution_attempts_open_job
          ON execution_attempts(job_id) WHERE ended_at IS NULL;
        CREATE UNIQUE INDEX uq_execution_attempts_open_worker
          ON execution_attempts(worker_id) WHERE ended_at IS NULL;
        CREATE TABLE dispatch_slots (
          name varchar(16) PRIMARY KEY CHECK (name = 'coding'),
          reserved_attempt_id uuid REFERENCES execution_attempts(id),
          receipt_count bigint NOT NULL CHECK (receipt_count >= 0),
          worker_count bigint NOT NULL CHECK (worker_count BETWEEN 0 AND 256),
          version bigint NOT NULL CHECK (version = receipt_count),
          CHECK (worker_count <= receipt_count)
        );
        INSERT INTO dispatch_slots VALUES ('coding', NULL, 0, 0, 0);
        CREATE TABLE worker_claim_receipts (
          worker_id uuid NOT NULL REFERENCES worker_coordination(worker_id),
          claim_request_id uuid NOT NULL,
          protocol_version smallint NOT NULL CHECK (protocol_version = 1),
          fingerprint_version smallint NOT NULL CHECK (fingerprint_version = 1),
          verifier_version smallint NOT NULL CHECK (verifier_version = 1),
          fingerprint varchar(64) NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
          verifier bytea NOT NULL CHECK (octet_length(verifier) = 32),
          outcome varchar(16) NOT NULL CHECK (outcome IN ('assigned', 'no_work')),
          no_work_reason varchar(16),
          job_id uuid,
          attempt_id uuid,
          recorded_at timestamptz NOT NULL,
          PRIMARY KEY (worker_id, claim_request_id),
          CHECK ((outcome = 'no_work' AND job_id IS NULL AND attempt_id IS NULL
                   AND no_work_reason IS NOT NULL
                   AND no_work_reason IN ('empty', 'occupied', 'changed', 'stale'))
            OR (outcome = 'assigned' AND job_id IS NOT NULL AND attempt_id IS NOT NULL
                   AND no_work_reason IS NULL)),
          FOREIGN KEY (attempt_id, job_id, worker_id, claim_request_id)
            REFERENCES execution_attempts(id, job_id, worker_id, claim_request_id)
            DEFERRABLE INITIALLY DEFERRED
        );
        ALTER TABLE execution_attempts ADD CONSTRAINT fk_attempt_original_claim
          FOREIGN KEY (worker_id, claim_request_id)
          REFERENCES worker_claim_receipts(worker_id, claim_request_id)
          DEFERRABLE INITIALLY DEFERRED;
        ALTER TABLE job_workflows
          ADD COLUMN active_attempt_id uuid,
          ADD COLUMN last_attempt_id uuid,
          ADD COLUMN last_attempt_number bigint NOT NULL DEFAULT 0,
          ADD CONSTRAINT fk_workflow_active_attempt FOREIGN KEY (active_attempt_id, job_id)
            REFERENCES execution_attempts(id, job_id) DEFERRABLE INITIALLY DEFERRED,
          ADD CONSTRAINT fk_workflow_last_attempt FOREIGN KEY (last_attempt_id, job_id)
            REFERENCES execution_attempts(id, job_id) DEFERRABLE INITIALLY DEFERRED;
        ALTER TABLE job_workflows DROP CONSTRAINT ck_job_workflows_initial_state;
        ALTER TABLE job_workflows DROP CONSTRAINT ck_job_workflows_initial_chronology;
        ALTER TABLE job_workflows ADD CONSTRAINT ck_job_workflows_claim_state CHECK (
          blocks_new_submission IS TRUE AND updated_at >= created_at AND
          ((status = 'queued' AND workflow_version = 0 AND updated_at = created_at
             AND active_attempt_id IS NULL AND last_attempt_id IS NULL
             AND last_attempt_number = 0)
           OR (status = 'claimed' AND workflow_version = 1 AND active_attempt_id IS NOT NULL
             AND last_attempt_id IS NOT NULL AND active_attempt_id = last_attempt_id
             AND last_attempt_number = 1))
        );
        ALTER TABLE job_workflow_events
          ADD COLUMN attempt_id uuid,
          ADD COLUMN previous_version bigint,
          ADD COLUMN previous_status varchar(32),
          ADD CONSTRAINT fk_workflow_event_attempt FOREIGN KEY (attempt_id, job_id)
            REFERENCES execution_attempts(id, job_id) DEFERRABLE INITIALLY DEFERRED;
        ALTER TABLE job_workflow_events DROP CONSTRAINT ck_job_workflow_events_initial_state;
        ALTER TABLE job_workflow_events ADD CONSTRAINT ck_job_workflow_events_claim_state CHECK (
          (workflow_version = 0 AND status = 'queued' AND event_type = 'submitted'
             AND attempt_id IS NULL AND previous_version IS NULL AND previous_status IS NULL)
          OR (workflow_version = 1 AND status = 'claimed' AND event_type = 'claimed'
             AND attempt_id IS NOT NULL AND previous_version IS NOT NULL
             AND previous_version = 0 AND previous_status IS NOT NULL AND previous_status = 'queued')
        );
        DROP TRIGGER job_workflows_no_mutation ON job_workflows;
    """)
    op.execute("""
        CREATE FUNCTION devrelay_guard_claim_transition()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'Coordination records cannot be deleted.' USING ERRCODE = '23514';
          END IF;
          IF TG_TABLE_NAME = 'job_workflows' THEN
            IF OLD.status <> 'queued' OR NEW.status <> 'claimed'
              OR NEW.workflow_version <> OLD.workflow_version + 1
              OR (NEW.job_id, NEW.source_task_id, NEW.repository_id, NEW.created_at)
                 IS DISTINCT FROM (OLD.job_id, OLD.source_task_id, OLD.repository_id, OLD.created_at)
            THEN RAISE EXCEPTION 'Unsupported workflow transition.' USING ERRCODE = '23514'; END IF;
          ELSIF TG_TABLE_NAME = 'worker_coordination' THEN
            IF (NEW.worker_id, NEW.enrolled_at) IS DISTINCT FROM (OLD.worker_id, OLD.enrolled_at)
              OR NEW.receipt_count <> OLD.receipt_count + 1 OR NEW.version <> OLD.version + 1
              OR NEW.next_poll_at < OLD.next_poll_at
            THEN RAISE EXCEPTION 'Worker coordination must advance monotonically.' USING ERRCODE = '23514'; END IF;
          ELSE
            IF NEW.name <> OLD.name OR NEW.receipt_count <> OLD.receipt_count + 1
              OR NEW.version <> OLD.version + 1 OR NEW.worker_count < OLD.worker_count
              OR NEW.worker_count > OLD.worker_count + 1
              OR (OLD.reserved_attempt_id IS NOT NULL
                  AND NEW.reserved_attempt_id IS DISTINCT FROM OLD.reserved_attempt_id)
            THEN RAISE EXCEPTION 'Dispatch reservation and counters cannot reset.' USING ERRCODE = '23514'; END IF;
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER job_workflows_claim_transition BEFORE UPDATE OR DELETE ON job_workflows
          FOR EACH ROW EXECUTE FUNCTION devrelay_guard_claim_transition();
        CREATE TRIGGER worker_coordination_advance BEFORE UPDATE OR DELETE ON worker_coordination
          FOR EACH ROW EXECUTE FUNCTION devrelay_guard_claim_transition();
        CREATE TRIGGER dispatch_slots_advance BEFORE UPDATE OR DELETE ON dispatch_slots
          FOR EACH ROW EXECUTE FUNCTION devrelay_guard_claim_transition();
        CREATE TRIGGER execution_attempts_no_mutation BEFORE UPDATE OR DELETE ON execution_attempts
          FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation();
        CREATE TRIGGER worker_claim_receipts_no_mutation BEFORE UPDATE OR DELETE ON worker_claim_receipts
          FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation();
    """)
    op.execute("""
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
        CREATE CONSTRAINT TRIGGER job_workflows_updated_history AFTER UPDATE ON job_workflows
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
          EXECUTE FUNCTION devrelay_validate_initial_workflow();
        CREATE FUNCTION devrelay_validate_claim_links()
        RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
        DECLARE identity uuid;
        BEGIN
          IF TG_TABLE_NAME = 'execution_attempts' THEN identity := NEW.id;
          ELSIF TG_TABLE_NAME = 'job_workflows' THEN identity := NEW.active_attempt_id;
          ELSIF TG_TABLE_NAME = 'dispatch_slots' THEN identity := NEW.reserved_attempt_id;
          ELSE identity := NEW.attempt_id; END IF;
          IF identity IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM execution_attempts a
            JOIN job_workflows w ON w.job_id = a.job_id AND w.active_attempt_id = a.id
              AND w.last_attempt_id = a.id AND w.last_attempt_number = a.attempt_number
              AND w.status = 'claimed' AND w.updated_at = a.issued_at
            JOIN job_workflow_events e ON e.job_id = w.job_id AND e.workflow_version = w.workflow_version
              AND e.attempt_id = a.id AND e.occurred_at = a.issued_at
            JOIN worker_claim_receipts r ON r.worker_id = a.worker_id
              AND r.claim_request_id = a.claim_request_id AND r.attempt_id = a.id
              AND r.job_id = a.job_id AND r.outcome = 'assigned' AND r.recorded_at = a.issued_at
            JOIN dispatch_slots s ON s.name = 'coding' AND s.reserved_attempt_id = a.id
            JOIN jobs j ON j.id = a.job_id AND j.provider_repository_id IS NOT NULL
              AND j.definition_version = 2
            WHERE a.id = identity
          ) THEN RAISE EXCEPTION 'Claim ownership must commit together.' USING ERRCODE = '23514'; END IF;
          RETURN NULL;
        END $$;
    """)
    for table, operation in (
        ("execution_attempts", "INSERT"),
        ("worker_claim_receipts", "INSERT"),
        ("job_workflows", "UPDATE"),
        ("dispatch_slots", "UPDATE"),
    ):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER {table}_claim_links AFTER {operation} ON {table} "
            "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_claim_links()"
        )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE dispatch_slots, worker_coordination, execution_attempts, worker_claim_receipts, job_workflows IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM worker_claim_receipts)
            OR EXISTS (SELECT 1 FROM execution_attempts)
            OR EXISTS (SELECT 1 FROM worker_coordination)
            OR EXISTS (SELECT 1 FROM dispatch_slots WHERE receipt_count <> 0 OR worker_count <> 0 OR reserved_attempt_id IS NOT NULL)
            OR EXISTS (SELECT 1 FROM job_workflows WHERE workflow_version <> 0)
            OR EXISTS (SELECT 1 FROM job_workflow_events WHERE workflow_version <> 0)
            OR EXISTS (
              SELECT 1 FROM jobs j LEFT JOIN job_workflows w ON w.job_id = j.id
              LEFT JOIN job_workflow_events e ON e.job_id = w.job_id AND e.workflow_version = 0
              WHERE w.job_id IS NULL OR e.job_id IS NULL OR w.status <> 'queued'
                OR w.created_at <> j.created_at OR w.updated_at <> j.created_at
                OR e.occurred_at <> w.updated_at
            )
          THEN RAISE EXCEPTION 'Cannot discard durable claim history.' USING ERRCODE = '23514'; END IF;
        END $$;
        DROP TRIGGER job_workflows_claim_links ON job_workflows;
        DROP TRIGGER job_workflows_updated_history ON job_workflows;
        DROP TRIGGER job_workflows_claim_transition ON job_workflows;
        ALTER TABLE job_workflows DROP CONSTRAINT fk_workflow_active_attempt,
          DROP CONSTRAINT fk_workflow_last_attempt, DROP CONSTRAINT ck_job_workflows_claim_state,
          DROP COLUMN active_attempt_id, DROP COLUMN last_attempt_id, DROP COLUMN last_attempt_number;
        ALTER TABLE job_workflows ADD CONSTRAINT ck_job_workflows_initial_state
          CHECK (status = 'queued' AND workflow_version = 0 AND blocks_new_submission IS TRUE);
        ALTER TABLE job_workflows ADD CONSTRAINT ck_job_workflows_initial_chronology CHECK (created_at = updated_at);
        ALTER TABLE job_workflow_events DROP CONSTRAINT fk_workflow_event_attempt,
          DROP CONSTRAINT ck_job_workflow_events_claim_state,
          DROP COLUMN attempt_id, DROP COLUMN previous_version, DROP COLUMN previous_status;
        ALTER TABLE job_workflow_events ADD CONSTRAINT ck_job_workflow_events_initial_state
          CHECK (workflow_version = 0 AND status = 'queued' AND event_type = 'submitted');
        CREATE TRIGGER job_workflows_no_mutation BEFORE UPDATE OR DELETE ON job_workflows
          FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation();
        ALTER TABLE execution_attempts DROP CONSTRAINT fk_attempt_original_claim;
        DROP TABLE worker_claim_receipts;
        DROP TABLE dispatch_slots;
        DROP TABLE execution_attempts;
        DROP TABLE worker_coordination;
        DROP FUNCTION devrelay_guard_claim_transition();
        DROP FUNCTION devrelay_validate_claim_links();
        CREATE OR REPLACE FUNCTION devrelay_validate_initial_workflow()
        RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
        DECLARE identity uuid;
        BEGIN
          IF TG_TABLE_NAME = 'jobs' THEN identity := NEW.id; ELSE identity := NEW.job_id; END IF;
          IF NOT EXISTS (
            SELECT 1 FROM jobs j JOIN job_workflows w ON w.job_id = j.id
            JOIN job_workflow_events e ON e.job_id = w.job_id
              AND e.workflow_version = w.workflow_version AND e.status = w.status
            WHERE j.id = identity AND w.created_at = j.created_at
              AND w.updated_at = j.created_at AND e.occurred_at = w.updated_at
          ) THEN RAISE EXCEPTION 'Initial workflow and history must match the definition.' USING ERRCODE = '23514'; END IF;
          RETURN NULL;
        END $$;
    """)
