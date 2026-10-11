"""Persist initial acknowledgement beside immutable claim/definition history."""

from alembic import op

revision = "0007_assignment_acknowledgements"
down_revision = "0006_atomic_claims"
branch_labels = None
depends_on = None

_WORKFLOW = """
blocks_new_submission IS TRUE AND updated_at >= created_at AND
((status = 'queued' AND workflow_version = 0 AND updated_at = created_at
  AND active_attempt_id IS NULL AND last_attempt_id IS NULL AND last_attempt_number = 0)
 OR (status = 'claimed' AND workflow_version = 1 AND active_attempt_id IS NOT NULL
  AND last_attempt_id IS NOT NULL AND active_attempt_id = last_attempt_id AND last_attempt_number = 1)
"""
_RUNNING = """
 OR (status = 'running' AND workflow_version = 2 AND active_attempt_id IS NOT NULL
  AND last_attempt_id IS NOT NULL AND active_attempt_id = last_attempt_id AND last_attempt_number = 1)
"""
_EVENT = """
(workflow_version = 0 AND status = 'queued' AND event_type = 'submitted'
 AND attempt_id IS NULL AND previous_version IS NULL AND previous_status IS NULL)
OR (workflow_version = 1 AND status = 'claimed' AND event_type = 'claimed'
 AND attempt_id IS NOT NULL AND previous_version IS NOT NULL AND previous_version = 0
 AND previous_status IS NOT NULL AND previous_status = 'queued')
"""
_ACK_EVENT = """
OR (workflow_version = 2 AND status = 'running' AND event_type = 'acknowledged'
 AND attempt_id IS NOT NULL AND previous_version IS NOT NULL AND previous_version = 1
 AND previous_status IS NOT NULL AND previous_status = 'claimed')
"""


def _workflow_guard(acknowledgements: bool) -> str:
    acknowledgement = (
        """
      OR (OLD.status = 'claimed' AND NEW.status = 'running'
          AND OLD.workflow_version = 1 AND NEW.workflow_version = 2
          AND (NEW.active_attempt_id, NEW.last_attempt_id, NEW.last_attempt_number)
              IS NOT DISTINCT FROM (OLD.active_attempt_id, OLD.last_attempt_id, OLD.last_attempt_number))
    """
        if acknowledgements
        else ""
    )
    return f"""
    CREATE OR REPLACE FUNCTION devrelay_guard_claim_transition()
    RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'Coordination records cannot be deleted.' USING ERRCODE = '23514';
      END IF;
      IF TG_TABLE_NAME = 'job_workflows' THEN
        IF NOT ((OLD.status = 'queued' AND NEW.status = 'claimed'
                 AND NEW.workflow_version = OLD.workflow_version + 1) {acknowledgement})
          OR NEW.updated_at < OLD.updated_at
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
          OR (OLD.reserved_attempt_id IS NOT NULL AND NEW.reserved_attempt_id IS DISTINCT FROM OLD.reserved_attempt_id)
        THEN RAISE EXCEPTION 'Dispatch reservation and counters cannot reset.' USING ERRCODE = '23514'; END IF;
      END IF;
      RETURN NEW;
    END $$;
    """


def _claim_links(acknowledgements: bool) -> str:
    joined = (
        "LEFT JOIN worker_acknowledgement_receipts q ON q.attempt_id = a.id"
        if acknowledgements
        else ""
    )
    state = (
        """
      AND w.status = a.status AND w.updated_at = a.updated_at
      AND ((a.status = 'claimed' AND w.workflow_version = 1 AND q.attempt_id IS NULL)
        OR (a.status = 'running' AND w.workflow_version = 2
          AND q.worker_id = a.worker_id AND q.job_id = a.job_id
          AND q.claim_request_id = a.claim_request_id AND q.generation = a.generation
          AND q.snapshot_id = j.snapshot_id AND q.base_revision = j.base_revision
          AND q.acknowledged_at = a.acknowledged_at AND q.acknowledged_at = a.updated_at
          AND q.repository_id = j.repository_id))
    """
        if acknowledgements
        else "AND w.status = 'claimed' AND w.updated_at = a.issued_at"
    )
    return f"""
    CREATE OR REPLACE FUNCTION devrelay_validate_claim_links()
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
        JOIN job_workflow_events e ON e.job_id = w.job_id AND e.workflow_version = w.workflow_version
          AND e.attempt_id = a.id AND e.occurred_at = a.updated_at AND e.status = a.status
        JOIN job_workflow_events claimed ON claimed.job_id = a.job_id AND claimed.workflow_version = 1
          AND claimed.attempt_id = a.id AND claimed.occurred_at = a.issued_at AND claimed.status = 'claimed'
        JOIN worker_claim_receipts r ON r.worker_id = a.worker_id AND r.claim_request_id = a.claim_request_id
          AND r.attempt_id = a.id AND r.job_id = a.job_id AND r.outcome = 'assigned' AND r.recorded_at = a.issued_at
        JOIN dispatch_slots s ON s.name = 'coding' AND s.reserved_attempt_id = a.id
        JOIN jobs j ON j.id = a.job_id AND j.provider_repository_id IS NOT NULL AND j.definition_version = 2
        {joined}
        WHERE a.id = identity {state}
      ) THEN RAISE EXCEPTION 'Claim ownership must commit together.' USING ERRCODE = '23514'; END IF;
      RETURN NULL;
    END $$;
    """


def _workflow_constraints(acknowledgements: bool) -> None:
    op.execute("ALTER TABLE job_workflows DROP CONSTRAINT ck_job_workflows_claim_state")
    op.execute(
        "ALTER TABLE job_workflow_events DROP CONSTRAINT ck_job_workflow_events_claim_state"
    )
    op.execute(
        "ALTER TABLE job_workflows ADD CONSTRAINT ck_job_workflows_claim_state CHECK ("
        + _WORKFLOW
        + (_RUNNING if acknowledgements else "")
        + "))"
    )
    op.execute(
        "ALTER TABLE job_workflow_events ADD CONSTRAINT ck_job_workflow_events_claim_state CHECK ("
        + _EVENT
        + (_ACK_EVENT if acknowledgements else "")
        + ")"
    )


def upgrade() -> None:
    op.execute(
        "LOCK TABLE dispatch_slots, worker_coordination, job_workflows, job_workflow_events, execution_attempts, worker_claim_receipts IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""
      CREATE TABLE worker_acknowledgement_receipts (
        worker_id uuid NOT NULL,
        request_id uuid NOT NULL,
        attempt_id uuid NOT NULL UNIQUE,
        job_id uuid NOT NULL,
        repository_id uuid NOT NULL,
        claim_request_id uuid NOT NULL,
        generation bigint NOT NULL CHECK (generation = 1),
        snapshot_id uuid NOT NULL,
        base_revision varchar(64) NOT NULL CHECK (base_revision ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'),
        definition_digest varchar(64) NOT NULL CHECK (definition_digest ~ '^[0-9a-f]{64}$'),
        definition_digest_version smallint NOT NULL CHECK (definition_digest_version = 1),
        protocol_version smallint NOT NULL CHECK (protocol_version = 1),
        fingerprint_version smallint NOT NULL CHECK (fingerprint_version = 1),
        fingerprint varchar(64) NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
        expected_workflow_version bigint NOT NULL CHECK (expected_workflow_version = 1),
        workflow_version bigint NOT NULL CHECK (workflow_version = 2),
        acknowledged_at timestamptz NOT NULL,
        PRIMARY KEY (worker_id, request_id),
        FOREIGN KEY (attempt_id, job_id, worker_id, claim_request_id)
          REFERENCES execution_attempts(id, job_id, worker_id, claim_request_id) DEFERRABLE INITIALLY DEFERRED,
        FOREIGN KEY (job_id, workflow_version)
          REFERENCES job_workflow_events(job_id, workflow_version) DEFERRABLE INITIALLY DEFERRED
      );
      CREATE TRIGGER worker_acknowledgements_no_mutation BEFORE UPDATE OR DELETE ON worker_acknowledgement_receipts
        FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation();
      ALTER TABLE execution_attempts DROP CONSTRAINT execution_attempts_status_check,
        DROP CONSTRAINT execution_attempts_check, DROP CONSTRAINT execution_attempts_check1;
      ALTER TABLE execution_attempts ADD CONSTRAINT ck_execution_attempts_ack_state CHECK (
        expires_at > issued_at AND revoked_at IS NULL AND ended_at IS NULL
        AND ((status = 'claimed' AND updated_at = issued_at AND acknowledged_at IS NULL)
          OR (status = 'running' AND acknowledged_at IS NOT NULL AND updated_at = acknowledged_at
            AND acknowledged_at >= issued_at AND acknowledged_at < expires_at))
      );
      DROP TRIGGER execution_attempts_no_mutation ON execution_attempts;
      CREATE FUNCTION devrelay_guard_attempt_acknowledgement()
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
      CREATE TRIGGER execution_attempts_ack_transition BEFORE INSERT OR UPDATE OR DELETE ON execution_attempts
        FOR EACH ROW EXECUTE FUNCTION devrelay_guard_attempt_acknowledgement();
    """)
    _workflow_constraints(True)
    op.execute(_workflow_guard(True))
    op.execute(_claim_links(True))
    for table, trigger, operation in (
        ("execution_attempts", "execution_attempts_ack_links", "UPDATE"),
        ("worker_acknowledgement_receipts", "worker_acknowledgements_links", "INSERT"),
    ):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER {trigger} AFTER {operation} ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_claim_links()"
        )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE dispatch_slots, worker_coordination, job_workflows, job_workflow_events, execution_attempts, worker_acknowledgement_receipts IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""
      DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM worker_acknowledgement_receipts)
          OR EXISTS (SELECT 1 FROM execution_attempts WHERE status <> 'claimed')
          OR EXISTS (SELECT 1 FROM job_workflows WHERE workflow_version > 1)
          OR EXISTS (SELECT 1 FROM job_workflow_events WHERE workflow_version > 1)
        THEN RAISE EXCEPTION 'Cannot discard durable acknowledgement history.' USING ERRCODE = '23514'; END IF;
      END $$;
      DROP TRIGGER execution_attempts_ack_links ON execution_attempts;
      DROP TRIGGER execution_attempts_ack_transition ON execution_attempts;
      ALTER TABLE execution_attempts DROP CONSTRAINT ck_execution_attempts_ack_state;
      ALTER TABLE execution_attempts ADD CONSTRAINT execution_attempts_status_check CHECK (status = 'claimed'),
        ADD CONSTRAINT execution_attempts_check CHECK (updated_at = issued_at AND expires_at > issued_at),
        ADD CONSTRAINT execution_attempts_check1 CHECK (acknowledged_at IS NULL AND revoked_at IS NULL AND ended_at IS NULL);
      CREATE TRIGGER execution_attempts_no_mutation BEFORE UPDATE OR DELETE ON execution_attempts
        FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation();
      DROP FUNCTION devrelay_guard_attempt_acknowledgement();
    """)
    _workflow_constraints(False)
    op.execute(_workflow_guard(False))
    op.execute(_claim_links(False))
    op.execute("DROP TABLE worker_acknowledgement_receipts")
