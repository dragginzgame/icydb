//! Bulk attempts retain journal, identity and publication ownership.

use super::*;
use crate::db::DynamicTypedBatchAttempt;
use std::sync::Arc;

#[test]
fn bulk_attempts_keep_journal_backlog_and_no_effect_identity_refusals() {
    let session = initialize_journaled();
    let binding = Arc::new(exact_key_binding(&session));
    let request = || vec![(binding.clone(), typed_payload_insert(&binding, 7))];
    let before = crate::db::commit::database_control_proof_identity().unwrap();
    assert!(matches!(
        session
            .attempt_trusted_typed_mutation_batch_shared(request(), 1, usize::MAX)
            .unwrap(),
        Some(DynamicTypedBatchAttempt::PrecommitByteRefusal(_))
    ));
    assert_eq!(
        crate::db::commit::database_control_proof_identity().unwrap(),
        before
    );

    for id in 1..=64 {
        let Some(DynamicTypedBatchAttempt::Committed(results)) = session
            .attempt_trusted_typed_mutation_batch_shared(request(), usize::MAX, usize::MAX)
            .unwrap()
        else {
            panic!("admitted attempt should commit")
        };
        assert_eq!(results[0].rows, vec![expected_dynamic_row(id, 7)]);
    }
    let tail_before =
        JOURNALED_TAIL_STORE.with(|tail| tail.borrow().current_tail_control().unwrap());
    assert_eq!(tail_before.batch_count(), 64);
    let sequence = crate::db::commit::next_database_commit_sequence().unwrap();
    let error = session
        .attempt_trusted_typed_mutation_batch_shared(request(), usize::MAX, usize::MAX)
        .expect_err("backlog refusal must never authorize splitting");
    assert_exact_batch_backlog_pressure(&error, tail_before, sequence);
    for folded in 1..=64 {
        assert_eq!(
            session.db.drive_startup_recovery_page().unwrap(),
            folded == 64
        );
    }
    let Some(DynamicTypedBatchAttempt::Committed(results)) = session
        .attempt_trusted_typed_mutation_batch_shared(request(), usize::MAX, usize::MAX)
        .unwrap()
    else {
        panic!("drained journal should admit one batch")
    };
    assert_eq!(results[0].rows, vec![expected_dynamic_row(65, 7)]);
}

#[test]
fn bulk_attempt_after_publication_is_an_error_without_retry_authority() {
    let session = initialize_journaled();
    let binding = Arc::new(exact_key_binding(&session));
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::RowsPublished);
    let interrupted = session.attempt_trusted_typed_mutation_batch_shared(
        vec![(binding.clone(), typed_payload_insert(&binding, 10))],
        usize::MAX,
        usize::MAX,
    );
    assert!(
        interrupted.is_err(),
        "a retained marker is never a safe byte refusal"
    );
    drive_journaled_recovery_to_completion(&session);
    let Some(DynamicTypedBatchAttempt::Committed(results)) = session
        .attempt_trusted_typed_mutation_batch_shared(
            vec![(binding.clone(), typed_payload_insert(&binding, 20))],
            usize::MAX,
            usize::MAX,
        )
        .unwrap()
    else {
        panic!("recovered journal should admit a new batch")
    };
    assert_eq!(results[0].rows, vec![expected_dynamic_row(2, 20)]);
}
