//! Row commits preserve store-wide index visibility until its lifecycle owner completes.

use super::*;
use crate::db::index::IndexState;

fn qualify_write_readiness<C: CanisterKind>(session: &DbSession<C>, store_path: &'static str) {
    let store = session.db.store_handle(store_path).unwrap();
    assert_eq!(store.index_state(), IndexState::Ready);
    let mut expected = Vec::new();
    for state in [IndexState::Ready, IndexState::Building] {
        if state == IndexState::Building {
            store.mark_index_building().unwrap();
        }
        let revision = store.access_state_revision().unwrap();
        let id = insert_exact_key_fixture(session, 10);
        assert_eq!(
            store.index_state(),
            state,
            "inserting a row cannot certify a whole index"
        );
        assert_eq!(store.access_state_revision().unwrap(), revision);
        for mutation in [
            DynamicMutation::Update {
                entity: ENTITY_NAME.into(),
                key: InputValue::nat64(id),
                patch: dynamic_payload_patch(20),
            },
            DynamicMutation::Replace {
                entity: ENTITY_NAME.into(),
                key: InputValue::nat64(id),
                patch: dynamic_payload_patch(30),
            },
            DynamicMutation::Delete {
                entity: ENTITY_NAME.into(),
                key: InputValue::nat64(id),
            },
        ] {
            let result = session.execute_trusted_dynamic_mutation(&mutation).unwrap();
            assert_eq!(result.affected_rows, 1);
            assert_eq!(store.index_state(), state);
            assert_eq!(store.access_state_revision().unwrap(), revision);
        }
        let binding = exact_key_binding(session);
        let results = session
            .execute_trusted_typed_mutation_batch(vec![
                (binding.clone(), typed_payload_insert(&binding, 40)),
                (binding.clone(), typed_payload_insert(&binding, 50)),
            ])
            .unwrap()
            .unwrap();
        assert_eq!(results.len(), 2);
        expected.extend(batch_rows(&results));
        assert_eq!(store.index_state(), state);
        assert_eq!(store.access_state_revision().unwrap(), revision);
    }

    let building_revision = store.access_state_revision().unwrap();
    forget_recovered_domain_for_tests(&session.db).unwrap();
    let mut complete = false;
    for _ in 0..32 {
        if session.db.drive_startup_recovery_page().unwrap() {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "the recovery owner must complete the bounded fixture"
    );
    assert_eq!(store.index_state(), IndexState::Ready);
    if store.journal_tail_store().is_some() {
        // The explicit Building transition already invalidated prior proofs.
        // Reconstructing journaled visibility must retain that durable revision.
        assert_eq!(store.access_state_revision().unwrap(), building_revision);
    } else {
        assert!(store.access_state_revision().unwrap() > building_revision);
    }
    let query = DynamicQuery::new(ENTITY_NAME);
    let mut cursor = None;
    let mut rows = Vec::new();
    for _ in 0..8 {
        let page = session
            .execute_trusted_live_page(&query, cursor.as_deref())
            .unwrap();
        rows.extend(page.rows);
        cursor = page.continuation;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none(), "the bounded fixture must exhaust");
    assert_eq!(expected.len(), 4);
    assert_eq!(rows, expected);
}

#[test]
fn successful_row_writes_leave_heap_and_journaled_index_readiness_unchanged() {
    qualify_write_readiness(&initialize(), STORE_PATH);
    qualify_write_readiness(&initialize_journaled(), JOURNALED_STORE_PATH);
}

#[test]
fn retained_marker_recovery_owns_index_readiness_after_an_interrupted_write() {
    let session = initialize_journaled();
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    store.mark_index_building().unwrap();
    let revision = store.access_state_revision().unwrap();
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::RowsPublished);
    assert!(
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: ENTITY_NAME.into(),
                patch: dynamic_payload_patch(7),
            })
            .is_err()
    );
    assert_eq!(store.index_state(), IndexState::Building);
    assert_eq!(store.access_state_revision().unwrap(), revision);
    assert!(
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: ENTITY_NAME.into(),
                patch: dynamic_payload_patch(8),
            })
            .is_err()
    );

    drive_journaled_recovery_to_completion(&session);
    assert_eq!(store.index_state(), IndexState::Ready);
    assert_eq!(store.access_state_revision().unwrap(), revision);
    let page = session
        .execute_trusted_live_page(&DynamicQuery::new(ENTITY_NAME), None)
        .unwrap();
    assert_eq!(page.rows, vec![expected_dynamic_row(1, 7)]);
}

#[test]
fn atomic_progress_writes_preserve_readiness_until_recovery_completes() {
    let session = initialize_journaled();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    store.mark_index_building().unwrap();
    let revision = store.access_state_revision().unwrap();
    let (before, after, operation) = atomic_progress_fixture(92);
    with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
        store.insert_mutation(&before)
    })
    .unwrap();
    session
        .execute_accepted_structural_update_with_mutation_progress(
            &catalog,
            batch(&[7]),
            Timestamp::from_millis(17),
            operation,
        )
        .unwrap();
    assert_eq!(store.index_state(), IndexState::Building);
    assert_eq!(store.access_state_revision().unwrap(), revision);
    assert_eq!(
        with_mutation_progress_store::<JournaledTestCanister, _>(
            |store| store.load_mutation(before.state().job_id)
        )
        .unwrap(),
        after
    );

    forget_recovered_domain_for_tests(&session.db).unwrap();
    drive_journaled_recovery_to_completion(&session);
    assert_eq!(store.index_state(), IndexState::Ready);
    assert_eq!(store.access_state_revision().unwrap(), revision);
    let page = session
        .execute_trusted_live_page(&DynamicQuery::new(ENTITY_NAME), None)
        .unwrap();
    assert_eq!(page.rows, vec![expected_dynamic_row(1, 7)]);
}
