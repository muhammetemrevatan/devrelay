"""Separate renewable authority from immutable assignment and acknowledgement."""

from alembic import op

revision = "0008_heartbeat_authorities"
down_revision = "0007_assignment_acknowledgements"
branch_labels = None
depends_on = None


def _coordination_guard(heartbeats: bool) -> str:
    worker_heartbeat = (
        """
      IF NEW.heartbeat_receipt_count <> OLD.heartbeat_receipt_count THEN
        IF NEW.heartbeat_receipt_count <> OLD.heartbeat_receipt_count + 1
          OR (NEW.worker_id, NEW.enrolled_at, NEW.next_poll_at, NEW.receipt_count, NEW.version)
            IS DISTINCT FROM (OLD.worker_id, OLD.enrolled_at, OLD.next_poll_at, OLD.receipt_count, OLD.version)
        THEN RAISE EXCEPTION 'Heartbeat counters must advance alone.' USING ERRCODE = '23514'; END IF;
        RETURN NEW;
      END IF;
    """
        if heartbeats
        else ""
    )
    slot_heartbeat = (
        """
      IF NEW.heartbeat_receipt_count <> OLD.heartbeat_receipt_count THEN
        IF NEW.heartbeat_receipt_count <> OLD.heartbeat_receipt_count + 1
          OR (NEW.name, NEW.reserved_attempt_id, NEW.receipt_count, NEW.worker_count, NEW.version)
            IS DISTINCT FROM (OLD.name, OLD.reserved_attempt_id, OLD.receipt_count, OLD.worker_count, OLD.version)
        THEN RAISE EXCEPTION 'Heartbeat counters must advance alone.' USING ERRCODE = '23514'; END IF;
        RETURN NEW;
      END IF;
    """
        if heartbeats
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
                 AND NEW.workflow_version = OLD.workflow_version + 1)
          OR (OLD.status = 'claimed' AND NEW.status = 'running'
              AND OLD.workflow_version = 1 AND NEW.workflow_version = 2
              AND (NEW.active_attempt_id, NEW.last_attempt_id, NEW.last_attempt_number)
                  IS NOT DISTINCT FROM (OLD.active_attempt_id, OLD.last_attempt_id, OLD.last_attempt_number)))
          OR NEW.updated_at < OLD.updated_at
          OR (NEW.job_id, NEW.source_task_id, NEW.repository_id, NEW.created_at)
             IS DISTINCT FROM (OLD.job_id, OLD.source_task_id, OLD.repository_id, OLD.created_at)
        THEN RAISE EXCEPTION 'Unsupported workflow transition.' USING ERRCODE = '23514'; END IF;
      ELSIF TG_TABLE_NAME = 'worker_coordination' THEN
        {worker_heartbeat}
        IF (NEW.worker_id, NEW.enrolled_at) IS DISTINCT FROM (OLD.worker_id, OLD.enrolled_at)
          OR NEW.receipt_count <> OLD.receipt_count + 1 OR NEW.version <> OLD.version + 1
          OR NEW.next_poll_at < OLD.next_poll_at
        THEN RAISE EXCEPTION 'Worker coordination must advance monotonically.' USING ERRCODE = '23514'; END IF;
      ELSE
        {slot_heartbeat}
        IF NEW.name <> OLD.name OR NEW.receipt_count <> OLD.receipt_count + 1
          OR NEW.version <> OLD.version + 1 OR NEW.worker_count < OLD.worker_count
          OR NEW.worker_count > OLD.worker_count + 1
          OR (OLD.reserved_attempt_id IS NOT NULL AND NEW.reserved_attempt_id IS DISTINCT FROM OLD.reserved_attempt_id)
        THEN RAISE EXCEPTION 'Dispatch reservation and counters cannot reset.' USING ERRCODE = '23514'; END IF;
      END IF;
      RETURN NEW;
    END $$;
    """


def upgrade() -> None:
    op.execute(
        "LOCK TABLE dispatch_slots, worker_coordination, job_workflows, job_workflow_events, execution_attempts, worker_claim_receipts, worker_acknowledgement_receipts IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""
      ALTER TABLE worker_coordination ADD COLUMN heartbeat_receipt_count bigint NOT NULL DEFAULT 0
        CHECK (heartbeat_receipt_count >= 0);
      ALTER TABLE dispatch_slots ADD COLUMN heartbeat_receipt_count bigint NOT NULL DEFAULT 0
        CHECK (heartbeat_receipt_count >= 0);
      CREATE TABLE attempt_lease_authorities (
        attempt_id uuid PRIMARY KEY,
        job_id uuid NOT NULL,
        worker_id uuid NOT NULL,
        claim_request_id uuid NOT NULL,
        generation bigint NOT NULL CHECK (generation = 1),
        lease_revision bigint NOT NULL CHECK (lease_revision > 0),
        updated_at timestamptz NOT NULL,
        current_expires_at timestamptz NOT NULL,
        revoked_at timestamptz CHECK (revoked_at IS NULL),
        receipt_count bigint NOT NULL CHECK (receipt_count >= 0),
        next_heartbeat_at timestamptz NOT NULL,
        CHECK (current_expires_at > updated_at
          AND current_expires_at <= updated_at + interval '300 seconds'
          AND next_heartbeat_at >= updated_at AND lease_revision <= receipt_count + 1),
        FOREIGN KEY (attempt_id, job_id, worker_id, claim_request_id)
          REFERENCES execution_attempts(id, job_id, worker_id, claim_request_id)
          DEFERRABLE INITIALLY DEFERRED
      );
      CREATE TABLE worker_heartbeat_receipts (
        worker_id uuid NOT NULL,
        request_id uuid NOT NULL,
        attempt_id uuid NOT NULL,
        job_id uuid NOT NULL,
        repository_id uuid NOT NULL,
        claim_request_id uuid NOT NULL,
        generation bigint NOT NULL CHECK (generation = 1),
        protocol_version smallint NOT NULL CHECK (protocol_version = 1),
        fingerprint_version smallint NOT NULL CHECK (fingerprint_version = 1),
        fingerprint varchar(64) NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
        expected_lease_revision bigint NOT NULL CHECK (expected_lease_revision > 0),
        lease_revision bigint NOT NULL CHECK (lease_revision > 0),
        updated_at timestamptz NOT NULL,
        current_expires_at timestamptz NOT NULL,
        renewed boolean NOT NULL,
        recorded_at timestamptz NOT NULL,
        attempt_ordinal bigint NOT NULL CHECK (attempt_ordinal > 0),
        worker_ordinal bigint NOT NULL CHECK (worker_ordinal > 0),
        global_ordinal bigint NOT NULL CHECK (global_ordinal > 0),
        PRIMARY KEY (worker_id, request_id),
        UNIQUE (attempt_id, attempt_ordinal),
        UNIQUE (worker_id, worker_ordinal),
        UNIQUE (global_ordinal),
        CHECK ((renewed AND lease_revision = expected_lease_revision + 1 AND updated_at = recorded_at)
          OR (NOT renewed AND lease_revision = expected_lease_revision)),
        CHECK (updated_at <= recorded_at AND current_expires_at > recorded_at
          AND current_expires_at <= updated_at + interval '300 seconds'),
        FOREIGN KEY (attempt_id, job_id, worker_id, claim_request_id)
          REFERENCES execution_attempts(id, job_id, worker_id, claim_request_id)
          DEFERRABLE INITIALLY DEFERRED
      );
      INSERT INTO attempt_lease_authorities
        SELECT id, job_id, worker_id, claim_request_id, generation, 1,
          issued_at, expires_at, NULL, 0, issued_at FROM execution_attempts;
      CREATE TRIGGER worker_heartbeats_no_mutation BEFORE UPDATE OR DELETE ON worker_heartbeat_receipts
        FOR EACH ROW EXECUTE FUNCTION devrelay_reject_submission_mutation();
      CREATE FUNCTION devrelay_guard_lease_authority()
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
      CREATE TRIGGER lease_authority_transition BEFORE INSERT OR UPDATE OR DELETE ON attempt_lease_authorities
        FOR EACH ROW EXECUTE FUNCTION devrelay_guard_lease_authority();
      CREATE FUNCTION devrelay_guard_initial_heartbeat_count()
      RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.heartbeat_receipt_count <> 0 THEN
          RAISE EXCEPTION 'Heartbeat counters must begin at zero.' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER worker_initial_heartbeat_count BEFORE INSERT ON worker_coordination
        FOR EACH ROW EXECUTE FUNCTION devrelay_guard_initial_heartbeat_count();
    """)
    op.execute(_coordination_guard(True))
    op.execute("""
      CREATE FUNCTION devrelay_validate_lease_authority()
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
      CREATE FUNCTION devrelay_validate_heartbeat_receipt()
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
      CREATE FUNCTION devrelay_validate_heartbeat_counter()
      RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$ BEGIN
        IF NEW.heartbeat_receipt_count = OLD.heartbeat_receipt_count THEN RETURN NULL; END IF;
        IF TG_TABLE_NAME = 'worker_coordination' THEN
          IF NOT EXISTS (SELECT 1 FROM worker_heartbeat_receipts WHERE worker_id = NEW.worker_id
              AND worker_ordinal = NEW.heartbeat_receipt_count) THEN
            RAISE EXCEPTION 'Worker heartbeat counter requires its receipt.' USING ERRCODE = '23514'; END IF;
        ELSE
          IF NOT EXISTS (SELECT 1 FROM worker_heartbeat_receipts WHERE global_ordinal = NEW.heartbeat_receipt_count) THEN
            RAISE EXCEPTION 'Global heartbeat counter requires its receipt.' USING ERRCODE = '23514'; END IF;
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER lease_authority_links AFTER INSERT OR UPDATE ON attempt_lease_authorities
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_lease_authority();
      CREATE CONSTRAINT TRIGGER execution_attempts_authority_links AFTER INSERT OR UPDATE ON execution_attempts
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_lease_authority();
      CREATE CONSTRAINT TRIGGER heartbeat_receipt_links AFTER INSERT ON worker_heartbeat_receipts
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_heartbeat_receipt();
      CREATE CONSTRAINT TRIGGER worker_heartbeat_counter_links AFTER UPDATE ON worker_coordination
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_heartbeat_counter();
      CREATE CONSTRAINT TRIGGER global_heartbeat_counter_links AFTER UPDATE ON dispatch_slots
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION devrelay_validate_heartbeat_counter();
    """)


def downgrade() -> None:
    op.execute(
        "LOCK TABLE dispatch_slots, worker_coordination, execution_attempts, attempt_lease_authorities, worker_heartbeat_receipts IN ACCESS EXCLUSIVE MODE"
    )
    op.execute("""
      DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM worker_heartbeat_receipts)
          OR EXISTS (SELECT 1 FROM worker_coordination WHERE heartbeat_receipt_count <> 0)
          OR EXISTS (SELECT 1 FROM dispatch_slots WHERE heartbeat_receipt_count <> 0)
          OR EXISTS (SELECT 1 FROM execution_attempts a LEFT JOIN attempt_lease_authorities l ON l.attempt_id = a.id
            WHERE l.attempt_id IS NULL OR l.lease_revision <> 1 OR l.receipt_count <> 0
              OR l.updated_at <> a.issued_at OR l.current_expires_at <> a.expires_at
              OR l.next_heartbeat_at <> a.issued_at OR l.revoked_at IS NOT NULL)
        THEN RAISE EXCEPTION 'Cannot discard durable heartbeat history.' USING ERRCODE = '23514'; END IF;
      END $$;
      DROP TRIGGER global_heartbeat_counter_links ON dispatch_slots;
      DROP TRIGGER worker_heartbeat_counter_links ON worker_coordination;
      DROP TRIGGER worker_initial_heartbeat_count ON worker_coordination;
      DROP TRIGGER execution_attempts_authority_links ON execution_attempts;
      DROP TRIGGER lease_authority_links ON attempt_lease_authorities;
      DROP TRIGGER heartbeat_receipt_links ON worker_heartbeat_receipts;
      DROP FUNCTION devrelay_validate_heartbeat_counter();
      DROP FUNCTION devrelay_validate_heartbeat_receipt();
      DROP FUNCTION devrelay_validate_lease_authority();
      DROP FUNCTION devrelay_guard_initial_heartbeat_count();
      DROP TABLE worker_heartbeat_receipts;
      DROP TABLE attempt_lease_authorities;
      DROP FUNCTION devrelay_guard_lease_authority();
    """)
    op.execute(_coordination_guard(False))
    op.execute("""
      ALTER TABLE worker_coordination DROP COLUMN heartbeat_receipt_count;
      ALTER TABLE dispatch_slots DROP COLUMN heartbeat_receipt_count;
    """)
