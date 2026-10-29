//! Deterministic row refusals must not pin an active job at its checkpoint.

use super::*;
use crate::db::MAX_MUTATION_JOB_STEP_ROWS_UPDATED;

fn seed_constrained_rows(session: &DbSession<JournaledTestCanister>, rejected_key: u64) {
    session
        .execute_admin_sql_ddl(&format!(
            "ALTER TABLE IdentityRow ADD CONSTRAINT payload_guard \
             CHECK (id <> {rejected_key} OR payload <= 10) \
             EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2"
        ))
        .unwrap();
    session
        .execute_trusted_dynamic_mutation_batch(
            (0..rejected_key)
                .map(|_| DynamicMutation::Insert {
                    entity: ENTITY_NAME.to_string(),
                    patch: dynamic_payload_patch(0),
                })
                .collect(),
        )
        .unwrap();
}

fn request(job_id: MutationJobId, sequence: u64) -> MutationJobAdvanceRequest {
    MutationJobAdvanceRequest::new(
        job_id,
        sequence,
        MutationJobIdempotencyKey::new(format!("constraint-{sequence}")).unwrap(),
    )
}

#[test]
fn constraint_rejection_discards_the_entire_uncommitted_page() {
    let (session, _root) = initialize_journaled_with_root();
    seed_constrained_rows(&session, 2);
    let refusal = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(2),
            patch: dynamic_payload_patch(42),
        })
        .unwrap_err();
    assert!(matches!(
        refusal.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ConstraintViolation,
        }),
    ));
    let job_id = MutationJobId::try_from_bytes([101; 32]).unwrap();
    let initial = session
        .start_trusted_sql_mutation_job(job_id, "UPDATE IdentityRow SET payload = 42 WHERE id > 0")
        .unwrap();
    let request = request(job_id, initial.sequence);
    let terminal = session.advance_trusted_mutation_job(&request).unwrap();
    assert_eq!(
        terminal.status,
        MutationJobStatus::RestartRequired(MutationJobRestartReason::TargetRowRejected),
    );
    assert_eq!(terminal.rows_updated, 0);
    assert_eq!(terminal.rows_updated_total, 0);
    assert_dynamic_payload(&session, 1, 0);
    assert_dynamic_payload(&session, 2, 0);
    assert_eq!(
        session.advance_trusted_mutation_job(&request),
        Ok(terminal.clone())
    );
    session
        .acknowledge_mutation_job(job_id, terminal.committed_sequence)
        .unwrap();
    assert_eq!(
        session.mutation_job_state(job_id),
        Err(MutationJobError::NotFound)
    );
}

#[test]
fn constraint_rejection_after_progress_is_terminal_replayable_and_acknowledgeable() {
    let (session, _root) = initialize_journaled_with_root();
    let rejected_key = MAX_MUTATION_JOB_STEP_ROWS_UPDATED + 1;
    seed_constrained_rows(&session, rejected_key);
    let job_id = MutationJobId::try_from_bytes([102; 32]).unwrap();
    let initial = session
        .start_trusted_sql_mutation_job(job_id, "UPDATE IdentityRow SET payload = 42 WHERE id > 0")
        .unwrap();
    let forward = session
        .advance_trusted_mutation_job(&request(job_id, initial.sequence))
        .unwrap();
    assert_eq!(forward.status, MutationJobStatus::Active);
    assert_eq!(forward.rows_updated, MAX_MUTATION_JOB_STEP_ROWS_UPDATED);
    let failed_request = request(job_id, forward.committed_sequence);
    let terminal = session
        .advance_trusted_mutation_job(&failed_request)
        .unwrap();
    assert_eq!(
        terminal.status,
        MutationJobStatus::RestartRequired(MutationJobRestartReason::TargetRowRejected),
    );
    assert_eq!(terminal.rows_updated, 0);
    assert_eq!(terminal.rows_updated_total, forward.rows_updated_total);
    assert_eq!(terminal.keys_scanned_total, forward.keys_scanned_total);
    assert_eq!(terminal.committed_sequence, forward.committed_sequence + 1);
    assert_dynamic_payload(&session, 1, 42);
    assert_dynamic_payload(&session, rejected_key - 1, 42);
    assert_dynamic_payload(&session, rejected_key, 0);
    assert_eq!(
        session.advance_trusted_mutation_job(&failed_request),
        Ok(terminal.clone())
    );
    assert_eq!(
        session.advance_trusted_mutation_job(&request(job_id, terminal.committed_sequence)),
        Err(MutationJobError::RestartRequired(
            MutationJobRestartReason::TargetRowRejected
        )),
    );
    session
        .acknowledge_mutation_job(job_id, terminal.committed_sequence)
        .unwrap();
    assert_eq!(
        session.mutation_job_state(job_id),
        Err(MutationJobError::NotFound)
    );
}
