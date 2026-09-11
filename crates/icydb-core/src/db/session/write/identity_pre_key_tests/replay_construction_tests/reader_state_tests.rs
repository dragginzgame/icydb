//! Qualify unique-value handoffs across maintained recovery reader states.

use super::*;

#[test]
fn direct_unique_handoff_recovers_from_predecessor_state() {
    let session = initialize_with_snapshot(identity_snapshot(STORE_PATH, true));
    seed_unique_owner(&session);
    recover_unique_handoff(
        &session,
        10,
        Some(MutationCommitInterruption::MarkerPersisted),
        None,
    );
}

#[test]
fn direct_unique_handoff_recovers_partial_publication_and_stage_loss() {
    let session = initialize_with_snapshot(identity_snapshot(STORE_PATH, true));
    seed_unique_owner(&session);
    recover_unique_handoff(
        &session,
        10,
        Some(MutationCommitInterruption::RowPrefixPublished),
        Some(0),
    );
}

#[test]
fn direct_unique_handoff_recovers_from_already_applied_state() {
    let session = initialize_with_snapshot(identity_snapshot(STORE_PATH, true));
    seed_unique_owner(&session);
    recover_unique_handoff(
        &session,
        10,
        Some(MutationCommitInterruption::RowsPublished),
        None,
    );
}

#[test]
fn journaled_unique_handoff_converges_online() {
    let session = initialize_journaled_with_unique_payload();
    seed_unique_owner(&session);
    drive_journaled_recovery_to_completion(&session);
    recover_unique_handoff(&session, 10, None, None);
}

#[test]
fn journaled_unique_handoff_recovers_partial_live_publication() {
    let session = initialize_journaled_with_unique_payload();
    seed_unique_owner(&session);
    drive_journaled_recovery_to_completion(&session);
    recover_unique_handoff(
        &session,
        10,
        Some(MutationCommitInterruption::RowPrefixPublished),
        None,
    );
}

#[test]
fn journaled_unique_handoff_folds_its_predecessor_before_replay_and_restart() {
    let session = initialize_journaled_with_unique_payload();
    seed_unique_owner(&session);
    drive_journaled_recovery_to_completion(&session);
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
            patch: dynamic_payload_patch(20),
        }])
        .expect("the logical predecessor should commit without requiring an immediate fold");
    assert_dynamic_payload(&session, 1, 20);
    assert!(JOURNALED_TAIL_STORE.with(|tail| tail.borrow().has_stored_batch()));

    // Canonical storage still precedes this commit's logical predecessor.
    // Restart after Replay and both folds; their durable watermarks must keep
    // the unique handoff intact while the retained marker is verified.
    recover_unique_handoff(
        &session,
        20,
        Some(MutationCommitInterruption::MarkerPersisted),
        Some(2),
    );
}

fn seed_unique_owner<C: CanisterKind>(session: &DbSession<C>) {
    let inserted = session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(10)])
        .expect("the unique owner should commit");
    assert_eq!(inserted.rows, vec![expected_dynamic_row(1, 10)]);
}

fn recover_unique_handoff<C: CanisterKind>(
    session: &DbSession<C>,
    previous: u64,
    interruption: Option<MutationCommitInterruption>,
    restart_after_page: Option<usize>,
) {
    if let Some(interruption) = interruption {
        interrupt_next_mutation_commit_for_tests(interruption);
    }
    let result = session.execute_trusted_dynamic_mutation_batch(vec![
        DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
            patch: dynamic_payload_patch(30),
        },
        DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(previous),
        },
    ]);
    if interruption.is_some() {
        let error = result.expect_err("the selected publication boundary should interrupt");
        assert_eq!(error.class(), ErrorClass::InvariantViolation);
        forget_recovered_domain_for_tests(&session.db)
            .expect("startup should reconstruct volatile recovery ownership");
    } else {
        result.expect("the unique handoff should commit");
    }

    let mut complete = false;
    for page in 0..8 {
        complete = session
            .db
            .drive_startup_recovery_page()
            .expect("the admitted handoff should recover");
        if restart_after_page == Some(page) {
            assert!(
                !complete,
                "stage loss must happen before marker verification"
            );
            forget_recovered_domain_for_tests(&session.db)
                .expect("a lost recovery stage should restart safely");
        }
        if complete {
            break;
        }
        assert!(session.db.ensure_recovered_state().is_err());
    }
    assert!(complete, "the bounded fixture should finish recovery");
    assert_dynamic_payload(session, 1, 30);
    assert_dynamic_payload(session, 2, previous);
    assert!(
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![dynamic_payload_patch(previous)]
            )
            .is_err(),
        "the recovered unique membership must reject a duplicate"
    );
    let inserted = session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(40)])
        .expect("a distinct value should remain insertable");
    assert_eq!(inserted.rows, vec![expected_dynamic_row(3, 40)]);
}
