//! Delete protection checks each final source against the complete delete set.

use super::*;

fn seed_source(target_count: u64) -> DbSession<JournaledTestCanister> {
    let session = initialize_nested();
    session
        .execute_trusted_dynamic_mutation_batch(
            (1..=target_count)
                .map(|id| DynamicMutation::Insert {
                    entity: ENTITY_NAME.into(),
                    patch: dynamic_payload_patch(id),
                })
                .collect(),
        )
        .unwrap();
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: SECOND_ENTITY_NAME.into(),
            patch: nested_patch(&(1..=target_count).collect::<Vec<_>>()),
        })
        .unwrap();
    drive_journaled_recovery_to_completion(&session);
    session
}

fn update_and_delete(patch: DynamicStructuralPatch, deleted: &[u64]) -> Vec<DynamicMutation> {
    let mut batch = vec![DynamicMutation::Update {
        entity: SECOND_ENTITY_NAME.into(),
        key: InputValue::nat64(1),
        patch,
    }];
    batch.extend(deleted.iter().map(|id| DynamicMutation::Delete {
        entity: ENTITY_NAME.into(),
        key: InputValue::nat64(*id),
    }));
    batch
}

#[test]
fn update_away_and_many_target_deletes_share_one_source_projection() {
    for reverse in [false, true] {
        std::thread::spawn(move || {
            let session = seed_source(200);
            let kept = (81..=200).collect::<Vec<_>>();
            let mut batch = update_and_delete(nested_patch(&kept), &(1..=80).collect::<Vec<_>>());
            if reverse {
                batch.reverse();
            }
            session
                .execute_trusted_dynamic_mutation_batch(batch)
                .unwrap();
            assert_nested_state(&session, &kept);
            drive_journaled_recovery_to_completion(&session);
            assert_nested_state(&session, &kept);
            let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
            store.with_data(|data| {
                for id in 1..=200 {
                    let key = crate::db::data::DecodedDataStoreKey::new(
                        ENTITY_TAG,
                        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(id)),
                    )
                    .to_raw()
                    .unwrap();
                    assert_eq!(data.get(&key).is_some(), id > 80);
                }
            });
        })
        .join()
        .unwrap();
    }
}

#[test]
fn a_later_deleted_target_or_relation_still_rejects_atomically() {
    for later_relation in [false, true] {
        std::thread::spawn(move || {
            let session = seed_source(3);
            let rows = source_rows(&session);
            let edges = reverse_entries();
            let tail = JOURNALED_TAIL_STORE
                .with(|tail| tail.borrow().current_tail_control())
                .unwrap();
            let patch = if later_relation {
                DynamicStructuralPatch::new(vec![
                    (
                        "list".into(),
                        DynamicWriteCell::Value(InputValue::list(Vec::new())),
                    ),
                    (
                        "set".into(),
                        DynamicWriteCell::Value(InputValue::list(vec![InputValue::nat64(3)])),
                    ),
                    (
                        "map".into(),
                        DynamicWriteCell::Value(InputValue::map(Vec::new())),
                    ),
                ])
            } else {
                nested_patch(&[2, 3])
            };
            let error = session
                .execute_trusted_dynamic_mutation_batch(update_and_delete(patch, &[1, 3]))
                .unwrap_err();
            assert!(error.diagnostic_facts().contains(&(
                icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
                icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
            )));
            assert_eq!(source_rows(&session), rows);
            assert_eq!(reverse_entries(), edges);
            assert_eq!(
                JOURNALED_TAIL_STORE
                    .with(|tail| tail.borrow().current_tail_control())
                    .unwrap(),
                tail
            );
        })
        .join()
        .unwrap();
    }
}
