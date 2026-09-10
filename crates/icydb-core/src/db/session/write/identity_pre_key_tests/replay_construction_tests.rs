//! Recovery qualification for shared prepared-row index transitions.

mod preparation_context_tests;

use super::*;

#[test]
fn heap_identity_marker_recovery_restores_the_indexed_row() {
    let session = initialize();
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::MarkerPersisted);
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(100)])
        .expect_err("the identity insert should interrupt after marker persistence");
    forget_recovered_domain_for_tests(&session.db)
        .expect("recovery should reconstruct its volatile ownership");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("the direct identity marker should recover")
    );
    assert!(session.db.ensure_recovered_state().is_err());
    assert_eq!(INDEX_STORE.with(|store| store.borrow().len()), 1);
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("marker effects should verify")
    );
    assert_dynamic_payload(&session, 1, 100);
}

#[test]
fn heap_identity_mixed_batch_recovers_after_marker_persistence() {
    assert_heap_identity_mixed_batch_recovers(MutationCommitInterruption::MarkerPersisted);
}

#[test]
fn heap_identity_mixed_batch_recovers_after_partial_row_publication() {
    assert_heap_identity_mixed_batch_recovers(MutationCommitInterruption::RowPrefixPublished);
}

#[test]
fn heap_identity_mixed_batch_recovers_after_all_rows_publish() {
    assert_heap_identity_mixed_batch_recovers(MutationCommitInterruption::RowsPublished);
}

fn assert_heap_identity_mixed_batch_recovers(interruption: MutationCommitInterruption) {
    let session = initialize_with_snapshot(identity_snapshot(STORE_PATH, true));
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![dynamic_payload_patch(100), dynamic_payload_patch(200)],
        )
        .expect("the direct unique-index fixture should commit");
    interrupt_next_mutation_commit_for_tests(interruption);
    session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
            },
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(2),
                patch: dynamic_payload_patch(300),
            },
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(100),
            },
        ])
        .expect_err("the identity-bound mixed batch should interrupt");
    forget_recovered_domain_for_tests(&session.db)
        .expect("recovery should reconstruct its volatile ownership");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("the direct batch should recover")
    );
    assert!(session.db.ensure_recovered_state().is_err());
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("the direct batch should verify")
    );
    assert_dynamic_payload(&session, 2, 300);
    assert_dynamic_payload(&session, 3, 100);
    assert_eq!(DATA_STORE.with(|store| store.borrow().len()), 2);
    assert_eq!(INDEX_STORE.with(|store| store.borrow().len()), 2);
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("a repeated recovery call should be idempotent")
    );

    let inserted = session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(200)])
        .expect("the updated row should release its old unique value");
    assert_eq!(inserted.rows, vec![expected_dynamic_row(4, 200)]);
}

#[test]
fn online_fold_preserves_updated_and_deleted_unique_keys() {
    assert_fold_preserves_updated_and_deleted_unique_keys(false);
}

#[test]
fn startup_fold_preserves_updated_and_deleted_unique_keys() {
    assert_fold_preserves_updated_and_deleted_unique_keys(true);
}

fn assert_fold_preserves_updated_and_deleted_unique_keys(interrupt: bool) {
    let session = initialize_journaled_with_unique_payload();
    session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(100),
            },
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(200),
            },
        ])
        .expect("initial unique rows should commit");
    drive_journaled_recovery_to_completion(&session);

    if interrupt {
        interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::RowsPublished);
    }
    let result = session.execute_trusted_dynamic_mutation_batch(vec![
        DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
            patch: dynamic_payload_patch(300),
        },
        DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(2),
        },
    ]);
    if interrupt {
        result.expect_err("row publication should interrupt before marker clear");
        forget_recovered_domain_for_tests(&session.db)
            .expect("startup should reconstruct volatile recovery state");
    } else {
        result.expect("the mixed transition should commit before online folding");
    }
    drive_journaled_recovery_to_completion(&session);

    let updated = session
        .execute_trusted_live_page(
            &DynamicQuery::new(ENTITY_NAME)
                .filter(crate::db::FieldRef::new("payload").eq(300_u64))
                .select(["payload"])
                .limit(1),
            None,
        )
        .expect("the new secondary key should select the updated row");
    assert_eq!(updated.rows, vec![vec![OutputValue::nat64(300)]]);

    // Reusing both old unique values proves that update and deletion
    // retired their index entries, not merely that missing rows are filtered.
    session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(100),
            },
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(200),
            },
        ])
        .expect("both retired unique values should be reusable");
    drive_journaled_recovery_to_completion(&session);
}

#[test]
fn recovery_preserves_unchanged_secondary_keys_after_a_relation_only_update() {
    let session = initialize_journaled_multi_entity();
    session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(100),
            },
            DynamicMutation::Insert {
                entity: SECOND_ENTITY_NAME.to_string(),
                patch: related_dynamic_payload_patch(1_100, 1),
            },
        ])
        .expect("related rows should commit together");
    drive_journaled_recovery_to_completion(&session);

    // Only the relation changes. The ordinary index planner can retain the
    // payload key; recovery must preserve it while removing the relation edge.
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::RowsPublished);
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Update {
            entity: SECOND_ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
            patch: DynamicStructuralPatch::new(vec![(
                "target_id".to_string(),
                DynamicWriteCell::Value(InputValue::null()),
            )]),
        }])
        .expect_err("the committed relation update should interrupt before marker clear");
    forget_recovered_domain_for_tests(&session.db)
        .expect("the interruption should reset volatile recovery state");
    drive_journaled_recovery_to_completion(&session);

    let recovered = session
        .execute_trusted_live_page(
            &DynamicQuery::new(SECOND_ENTITY_NAME)
                .filter(crate::db::FieldRef::new("payload").eq(1_100_u64))
                .select(["payload", "target_id"])
                .limit(1),
            None,
        )
        .expect("the unchanged payload index should still select the recovered row");
    assert_eq!(
        recovered.rows,
        vec![vec![OutputValue::nat64(1_100), OutputValue::null()]],
    );
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
        }])
        .expect("the recovered relation removal should release its former target");
}

#[test]
fn recovery_verification_failure_retains_marker_and_admission_barrier() {
    let session = initialize();
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::MarkerPersisted);
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![dynamic_payload_patch(100), dynamic_payload_patch(200)],
        )
        .expect_err("marker persistence should interrupt");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("replay should publish indexes")
    );
    assert_eq!(INDEX_STORE.with(|store| store.borrow().len()), 2);
    // Final verification must inspect stored effects in its own call. It must
    // not replay the marker again and hide a missing derived effect.
    // Reverse marker traversal verifies row 2 first. Remove only row 1's key
    // so the later failure occurs after successful reuse of the entity setup.
    INDEX_STORE.with(|store| {
        let mut store = store.borrow_mut();
        let mut first_key = None;
        store
            .visit_entries(|key, _| {
                first_key = Some(key.clone());
                Ok::<_, InternalError>(IndexStoreVisit::Stop)
            })
            .expect("the ordered payload index should be readable");
        store.remove(&first_key.expect("the first payload key should exist"));
    });
    assert_eq!(INDEX_STORE.with(|store| store.borrow().len()), 1);
    for _ in 0..2 {
        let error = session
            .db
            .drive_startup_recovery_page()
            .expect_err("missing index must reject verification");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert!(session.db.ensure_recovered_state().is_err());
        assert!(
            crate::db::commit::retained_commit_marker_measurement_for_tests()
                .expect("marker inspection should succeed")
                .is_some()
        );
    }
}

#[test]
fn recovery_restarts_replay_after_losing_its_volatile_stage() {
    let session = initialize();
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::MarkerPersisted);
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(100)])
        .expect_err("marker persistence should interrupt");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("replay should complete")
    );
    forget_recovered_domain_for_tests(&session.db).expect("volatile stage should be forgotten");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("replay should restart idempotently")
    );
    assert!(session.db.ensure_recovered_state().is_err());
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("verification should finish")
    );
    assert_dynamic_payload(&session, 1, 100);
    let inserted = session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(200)])
        .expect("identity range should not be consumed twice");
    assert_eq!(inserted.rows, vec![expected_dynamic_row(2, 200)]);
}

#[test]
fn recovery_restarts_after_a_completed_fold_without_losing_committed_progress() {
    let session = initialize_journaled();
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::MarkerPersisted);
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(100)])
        .expect_err("marker persistence should interrupt");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("replay should append the marker batch")
    );
    assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 0);
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("the complete batch should fold")
    );
    assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 1);
    let watermark = JOURNALED_TAIL_STORE.with(|tail| {
        tail.borrow()
            .fold_watermark()
            .expect("watermark should decode")
    });
    forget_recovered_domain_for_tests(&session.db).expect("volatile stage should be forgotten");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("replay should retain the completed fold")
    );
    assert_eq!(
        JOURNALED_TAIL_STORE.with(|tail| tail
            .borrow()
            .fold_watermark()
            .expect("watermark should decode")),
        watermark
    );
    assert!(session.db.ensure_recovered_state().is_err());
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("the retained marker should verify")
    );
    assert_dynamic_payload(&session, 1, 100);
    assert_eq!(JOURNALED_INDEX_STORE.with(|store| store.borrow().len()), 1);
}
