//! Catalog availability must not become persisted evidence of schema drift.

use super::*;
use crate::db::{
    ReadSetRevisionProof,
    integrity::with_resumable_progress_store,
    schema::{apply_schema_migration_record_op, schema_migration_record_lifecycle_ops_for_tests},
};

fn retained_job(job_id: MutationJobId) -> MutationJobRecord {
    with_mutation_progress_store::<JournaledTestCanister, _>(|store| store.load_mutation(job_id))
        .expect("retained job should remain readable")
}

// Both phases consume the same catalog authority; Verify starts from a nonmatching page.
fn pending_jobs(
    session: &DbSession<JournaledTestCanister>,
) -> Vec<(MutationJobAdvanceRequest, MutationJobRecord)> {
    [
        (61_u8, "UPDATE IdentityRow SET payload = 42 WHERE id = 1"),
        (62_u8, "UPDATE IdentityRow SET payload = 42 WHERE id = 999"),
    ]
    .into_iter()
    .map(|(identity, sql)| {
        let job_id = MutationJobId::try_from_bytes([identity; 32]).unwrap();
        let mut state = session.start_trusted_sql_mutation_job(job_id, sql).unwrap();
        if identity == 62 {
            let forward = MutationJobAdvanceRequest::new(
                job_id,
                state.sequence,
                MutationJobIdempotencyKey::new("forward").unwrap(),
            );
            let receipt = session.advance_trusted_mutation_job(&forward).unwrap();
            assert_eq!(receipt.phase, MutationJobPhase::Verify);
            state = session.mutation_job_state(job_id).unwrap();
        }
        (
            MutationJobAdvanceRequest::new(
                job_id,
                state.sequence,
                MutationJobIdempotencyKey::new("retry-unchanged").unwrap(),
            ),
            retained_job(job_id),
        )
    })
    .collect()
}

fn assert_unavailable_preserves_jobs(
    session: &DbSession<JournaledTestCanister>,
    jobs: &[(MutationJobAdvanceRequest, MutationJobRecord)],
) {
    for (request, before) in jobs {
        assert_eq!(
            session.advance_trusted_mutation_job(request),
            Err(MutationJobError::TargetQueryFailed),
        );
        assert_eq!(retained_job(request.job_id), *before);
    }
}

fn assert_same_requests_resume(
    session: &DbSession<JournaledTestCanister>,
    jobs: &[(MutationJobAdvanceRequest, MutationJobRecord)],
) {
    assert_dynamic_payload(session, 1, 41);
    for (request, before) in jobs {
        let receipt = session.advance_trusted_mutation_job(request).unwrap();
        assert_eq!(receipt.committed_sequence, before.state().sequence + 1);
        assert!(!matches!(
            receipt.status,
            MutationJobStatus::RestartRequired(_)
        ));
        assert_eq!(session.advance_trusted_mutation_job(request), Ok(receipt));
    }
    assert_dynamic_payload(session, 1, 42);
}

#[test]
fn migration_gate_preserves_forward_and_verify_for_retry() {
    let (session, _root) = initialize_journaled_with_root();
    assert_eq!(insert_exact_key_fixture(&session, 41), 1);
    let jobs = pending_jobs(&session);
    let (prepared, validating, aborted) =
        schema_migration_record_lifecycle_ops_for_tests().unwrap();
    apply_schema_migration_record_op(&prepared).unwrap();
    apply_schema_migration_record_op(&validating).unwrap();

    assert_unavailable_preserves_jobs(&session, &jobs);

    apply_schema_migration_record_op(&aborted).unwrap();
    assert_same_requests_resume(&session, &jobs);
}

#[test]
fn recovery_wait_preserves_forward_and_verify_for_retry() {
    let (session, _root) = initialize_journaled_with_root();
    assert_eq!(insert_exact_key_fixture(&session, 41), 1);
    let jobs = pending_jobs(&session);
    forget_recovered_domain_for_tests(&session.db).unwrap();

    assert_unavailable_preserves_jobs(&session, &jobs);

    drive_journaled_recovery_to_completion(&session);
    assert_same_requests_resume(&session, &jobs);
}

#[test]
fn corrupt_catalog_preserves_forward_and_verify_progress() {
    let (session, _root) = initialize_journaled_with_root();
    assert_eq!(insert_exact_key_fixture(&session, 41), 1);
    let jobs = pending_jobs(&session);
    JOURNALED_SCHEMA_STORE.with(|schema| {
        schema
            .borrow_mut()
            .corrupt_current_accepted_schema_root_for_tests()
            .unwrap();
    });
    let error = session
        .find_accepted_schema_catalog_context_for_entity_source_key(ENTITY_SOURCE)
        .expect_err("corruption must fail inspection rather than prove a missing entity");
    assert_eq!(error.class(), ErrorClass::Corruption);

    assert_unavailable_preserves_jobs(&session, &jobs);
}

fn publish_successor(session: &DbSession<JournaledTestCanister>, retain_entity: bool) {
    let (snapshots, bindings) = if retain_entity {
        (
            BTreeMap::from([(ENTITY_TAG, identity_snapshot(JOURNALED_STORE_PATH, false))]),
            BTreeMap::from([
                ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
                ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
            ]),
        )
    } else {
        (BTreeMap::new(), BTreeMap::new())
    };
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        JOURNALED_STORE_PATH,
        AcceptedSchemaRevision::new(2),
        snapshots,
        bindings,
    );
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    crate::db::commit::publish_accepted_schema_candidate(
        JOURNALED_STORE_PATH,
        store,
        AcceptedSchemaRevision::INITIAL,
        &candidate,
    )
    .unwrap();
}

fn assert_schema_drift_terminalizes_jobs(
    session: &DbSession<JournaledTestCanister>,
    jobs: &[(MutationJobAdvanceRequest, MutationJobRecord)],
) {
    for (request, before) in jobs {
        let receipt = session.advance_trusted_mutation_job(request).unwrap();
        assert_eq!(
            receipt.status,
            MutationJobStatus::RestartRequired(MutationJobRestartReason::AcceptedSchemaChanged),
        );
        assert_eq!(receipt.committed_sequence, before.state().sequence + 1);
        assert_eq!(receipt.rows_updated, 0);
        assert_eq!(session.advance_trusted_mutation_job(request), Ok(receipt));
    }
}

#[test]
fn changed_catalog_terminalizes_forward_and_verify() {
    let (session, _root) = initialize_journaled_with_root();
    assert_eq!(insert_exact_key_fixture(&session, 41), 1);
    let jobs = pending_jobs(&session);
    publish_successor(&session, true);

    assert_schema_drift_terminalizes_jobs(&session, &jobs);
    assert_dynamic_payload(&session, 1, 41);
}

#[test]
fn missing_entity_terminalizes_forward_and_verify() {
    let (session, _root) = initialize_journaled_with_root();
    // Removal uses an empty domain; there is no row cleanup to bypass.
    let jobs = pending_jobs(&session);
    publish_successor(&session, false);
    assert!(
        session
            .find_accepted_schema_catalog_context_for_entity_source_key(ENTITY_SOURCE)
            .unwrap()
            .is_none(),
    );

    assert_schema_drift_terminalizes_jobs(&session, &jobs);
}

#[test]
fn pending_marker_prevents_unadvanced_cancellation() {
    let (session, _root) = initialize_journaled_with_root();
    assert_eq!(insert_exact_key_fixture(&session, 41), 1);
    let job_id = MutationJobId::try_from_bytes([71; 32]).unwrap();
    let state = session
        .start_trusted_sql_mutation_job(job_id, "UPDATE IdentityRow SET payload = 42 WHERE id = 1")
        .unwrap();
    let request = MutationJobAdvanceRequest::new(
        job_id,
        state.sequence,
        MutationJobIdempotencyKey::new("interrupted-first-page").unwrap(),
    );
    let before = retained_job(job_id);
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::MarkerPersisted);
    assert!(session.advance_trusted_mutation_job(&request).is_err());
    assert_eq!(retained_job(job_id), before);

    assert_eq!(
        session.cancel_unadvanced_mutation_job(job_id, 0),
        Err(MutationJobError::TargetQueryFailed),
    );
    assert_eq!(retained_job(job_id), before);

    drive_journaled_recovery_to_completion(&session);
    let receipt = session.advance_trusted_mutation_job(&request).unwrap();
    assert_eq!(receipt.committed_sequence, 1);
    assert_eq!(receipt.rows_updated, 1);
    assert_eq!(receipt.phase, MutationJobPhase::Verify);
    assert_dynamic_payload(&session, 1, 42);
}

fn completed_mutation_job(session: &DbSession<JournaledTestCanister>) -> MutationJobRecord {
    let job_id = MutationJobId::try_from_bytes([65; 32]).unwrap();
    session
        .start_trusted_sql_mutation_job(
            job_id,
            "UPDATE IdentityRow SET payload = 42 WHERE id = 999",
        )
        .unwrap();
    for sequence in 0..2 {
        let request = MutationJobAdvanceRequest::new(
            job_id,
            sequence,
            MutationJobIdempotencyKey::new(format!("complete-{sequence}")).unwrap(),
        );
        session.advance_trusted_mutation_job(&request).unwrap();
    }
    let record = retained_job(job_id);
    assert_eq!(record.state().status, MutationJobStatus::Completed);
    record
}

fn assert_pending_mutation_commands(
    session: &DbSession<JournaledTestCanister>,
    jobs: &[(MutationJobAdvanceRequest, MutationJobRecord)],
    completed: &MutationJobRecord,
) {
    assert_eq!(
        session.cancel_unadvanced_mutation_job(jobs[0].0.job_id, 0),
        Err(MutationJobError::TargetQueryFailed),
    );
    assert_eq!(
        session.acknowledge_mutation_job(completed.state().job_id, completed.state().sequence),
        Err(MutationJobError::TargetQueryFailed),
    );
    assert_eq!(retained_job(completed.state().job_id), *completed);
    let fresh = MutationJobId::try_from_bytes([63; 32]).unwrap();
    assert_eq!(
        session.start_trusted_sql_mutation_job(
            fresh,
            "UPDATE IdentityRow SET payload = 43 WHERE id = 1"
        ),
        Err(MutationJobError::TargetQueryFailed),
    );
    assert_eq!(
        session.mutation_job_state(fresh),
        Err(MutationJobError::NotFound)
    );
    for (request, before) in jobs {
        let next = MutationJobAdvanceRequest::new(
            request.job_id,
            before.state().sequence,
            MutationJobIdempotencyKey::new("pending-next-page").unwrap(),
        );
        // Pending recovery must win before either phase can persist a budget restart.
        assert_eq!(
            advance_with_exhausted_mutation_predicate_budget(session, &next),
            Err(MutationJobError::TargetQueryFailed),
        );
        assert_eq!(retained_job(request.job_id), *before);
        assert_eq!(
            session.mutation_job_state(request.job_id).unwrap(),
            *before.state()
        );
    }
}

fn assert_pending_resumable_commands(
    session: &DbSession<JournaledTestCanister>,
    proof: &ReadSetRevisionProof,
    active: ResumableJobId,
    completed: ResumableJobId,
) {
    let before = with_resumable_progress_store::<JournaledTestCanister, _>(|store| {
        Ok((
            store.load_resumable(active)?,
            store.load_resumable(completed)?,
        ))
    })
    .unwrap();
    assert_eq!(
        session.resumable_job_state(active).unwrap(),
        *before.0.state()
    );
    assert_eq!(
        session.acknowledge_resumable_job(completed, before.1.state().sequence),
        Err(ResumableJobError::Internal),
    );
    let fresh = ResumableJobId::try_from_bytes([79; 32]).unwrap();
    assert_eq!(
        session.start_resumable_job(fresh, proof.clone(), Vec::new()),
        Err(ResumableJobError::Internal),
    );
    assert_eq!(
        session.resumable_job_state(fresh),
        Err(ResumableJobError::NotFound)
    );
    let request = ResumableJobAdvanceRequest::new(
        active,
        0,
        ResumableJobIdempotencyKey::new("pending-application-page").unwrap(),
    );
    assert!(matches!(
        session.compare_proof_and_advance::<()>(&request, |_| {
            panic!("pending recovery must reject before calling application work")
        }),
        Err(CompareProofAndAdvanceError::Protocol(
            ResumableJobError::Internal
        )),
    ));
    let after = with_resumable_progress_store::<JournaledTestCanister, _>(|store| {
        Ok((
            store.load_resumable(active)?,
            store.load_resumable(completed)?,
        ))
    })
    .unwrap();
    assert_eq!(after, before);
}

fn assert_pending_marker_commands(interruption: MutationCommitInterruption) {
    let (session, _root) = initialize_journaled_with_root();
    assert_eq!(insert_exact_key_fixture(&session, 41), 1);
    let jobs = pending_jobs(&session);
    let completed = completed_mutation_job(&session);
    let proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .unwrap();
    let active_application = ResumableJobId::try_from_bytes([77; 32]).unwrap();
    let completed_application = ResumableJobId::try_from_bytes([78; 32]).unwrap();
    for job_id in [active_application, completed_application] {
        session
            .start_resumable_job(job_id, proof.clone(), Vec::new())
            .unwrap();
    }
    let completion_request = ResumableJobAdvanceRequest::new(
        completed_application,
        0,
        ResumableJobIdempotencyKey::new("complete-application-job").unwrap(),
    );
    session
        .compare_proof_and_advance::<()>(&completion_request, |_| {
            Ok(ResumableJobAdvance::new(None, vec![1], Vec::new()).unwrap())
        })
        .unwrap();

    interrupt_next_mutation_commit_for_tests(interruption);
    assert!(session.advance_trusted_mutation_job(&jobs[0].0).is_err());
    let retained = jobs
        .iter()
        .map(|(request, _)| (request.clone(), retained_job(request.job_id)))
        .collect::<Vec<_>>();
    let inventory = session.progress_job_inventory().unwrap();
    assert_pending_mutation_commands(&session, &retained, &completed);
    assert_pending_resumable_commands(&session, &proof, active_application, completed_application);
    assert_eq!(session.progress_job_inventory().unwrap(), inventory);
    if let Some(receipt) = retained[0].1.exact_replay(&jobs[0].0).unwrap() {
        assert_eq!(
            session.advance_trusted_mutation_job(&jobs[0].0),
            Ok(receipt.clone())
        );
        assert_eq!(retained_job(jobs[0].0.job_id), retained[0].1);
    }

    drive_journaled_recovery_to_completion(&session);
    let recovered = session.advance_trusted_mutation_job(&jobs[0].0).unwrap();
    assert_eq!(recovered.committed_sequence, 1);
    assert_eq!(recovered.rows_updated, 1);
    assert_dynamic_payload(&session, 1, 42);
    // After recovery, both acknowledgement families and initial cancellation work.
    session
        .acknowledge_mutation_job(completed.state().job_id, completed.state().sequence)
        .unwrap();
    session
        .acknowledge_resumable_job(completed_application, 1)
        .unwrap();
    let fresh = MutationJobId::try_from_bytes([63; 32]).unwrap();
    session
        .start_trusted_sql_mutation_job(fresh, "UPDATE IdentityRow SET payload = 43 WHERE id = 1")
        .unwrap();
    assert_eq!(session.cancel_unadvanced_mutation_job(fresh, 0), Ok(()));
    assert_eq!(session.cancel_unadvanced_mutation_job(fresh, 0), Ok(()));
    let fresh_application = ResumableJobId::try_from_bytes([79; 32]).unwrap();
    let current_proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .unwrap();
    session
        .start_resumable_job(fresh_application, current_proof, Vec::new())
        .unwrap();
}

#[test]
fn marker_only_rejects_progress_writes_and_recovers() {
    assert_pending_marker_commands(MutationCommitInterruption::MarkerPersisted);
}

#[test]
fn marker_created_by_application_work_preserves_resumable_progress() {
    let (session, _root) = initialize_journaled_with_root();
    assert_eq!(insert_exact_key_fixture(&session, 41), 1);
    let jobs = pending_jobs(&session);
    let proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .unwrap();
    let job_id = ResumableJobId::try_from_bytes([80; 32]).unwrap();
    session.start_resumable_job(job_id, proof, vec![7]).unwrap();
    let before = with_resumable_progress_store::<JournaledTestCanister, _>(|store| {
        store.load_resumable(job_id)
    })
    .unwrap();
    let request = ResumableJobAdvanceRequest::new(
        job_id,
        0,
        ResumableJobIdempotencyKey::new("application-retains-marker").unwrap(),
    );
    let result = session.compare_proof_and_advance::<()>(&request, |_| {
        interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::RowsPublished);
        assert!(session.advance_trusted_mutation_job(&jobs[0].0).is_err());
        Ok(ResumableJobAdvance::new(None, vec![8], Vec::new()).unwrap())
    });
    assert!(matches!(
        result,
        Err(CompareProofAndAdvanceError::Protocol(
            ResumableJobError::Internal
        )),
    ));
    let retained = with_resumable_progress_store::<JournaledTestCanister, _>(|store| {
        store.load_resumable(job_id)
    })
    .unwrap();
    assert_eq!(retained, before);

    drive_journaled_recovery_to_completion(&session);
    assert_dynamic_payload(&session, 1, 42);
    let receipt = session
        .compare_proof_and_advance::<()>(&request, |_| {
            panic!("recovered source drift must reject before application work")
        })
        .unwrap();
    assert_eq!(receipt.status, ResumableJobAdvanceStatus::Invalidated);
    let state = session.resumable_job_state(job_id).unwrap();
    assert_eq!(state.sequence, 1);
    assert_eq!(state.application_state, vec![7]);
    session.acknowledge_resumable_job(job_id, 1).unwrap();
}

#[test]
fn published_journal_rejects_progress_writes_and_recovers() {
    assert_pending_marker_commands(MutationCommitInterruption::JournalPublished);
}

#[test]
fn published_rows_reject_progress_writes_and_recover() {
    assert_pending_marker_commands(MutationCommitInterruption::RowsPublished);
}

#[test]
fn published_progress_rejects_new_writes_but_preserves_replay_and_recovery() {
    assert_pending_marker_commands(MutationCommitInterruption::ProgressReplaced);
}
