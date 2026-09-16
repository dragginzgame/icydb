//! Shared predicate normalization must preserve index effects through recovery.

use super::*;
use crate::db::sql_shared::MAX_SQL_EXPR_DEPTH;
use ic_memory::ic_stable_structures::Storable;

fn partial_index_snapshot(sql: &str) -> PersistedSchemaSnapshot {
    let base = identity_snapshot(STORE_PATH, false);
    let index = &base.indexes()[0];
    PersistedSchemaSnapshot::new_with_indexes(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        base.row_layout().clone(),
        base.fields().to_vec(),
        vec![PersistedIndexSnapshot::new(
            index.schema_id(),
            index.ordinal(),
            index.name().to_string(),
            index.store().to_string(),
            index.unique(),
            index.key().clone(),
            Some(sql.to_string()),
        )],
    )
}

#[test]
fn normalized_partial_indexes_replay_the_same_effects_as_uninterrupted_writes() {
    let cases = [
        (vec!["payload >= 10"; MAX_SQL_EXPR_DEPTH].join(" AND "), 3),
        // Reduced-SQL integers are Int64; equality/IN remain strict against
        // this Nat64 field. Ordered comparisons explicitly widen numerically.
        (
            (10..10 + MAX_SQL_EXPR_DEPTH)
                .map(|value| format!("payload = {value}"))
                .collect::<Vec<_>>()
                .join(" OR "),
            0,
        ),
        ("payload = 10 OR payload >= 200".to_string(), 1),
        (
            format!("{}payload < 10", "NOT ".repeat(MAX_SQL_EXPR_DEPTH - 1)),
            3,
        ),
        ("payload >= 10 AND payload < 10".to_string(), 0),
        ("payload >= 10 AND payload <= 10".to_string(), 1),
    ];
    for (sql, expected_count) in cases {
        let mut expected_entries = None;
        for interruption in [
            None,
            Some(MutationCommitInterruption::MarkerPersisted),
            Some(MutationCommitInterruption::RowPrefixPublished),
        ] {
            // The fixture's durable identity/checkpoint state is thread-local.
            // Each comparison needs an independent database, not just empty maps.
            let entries = std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        let session = initialize_with_snapshot(partial_index_snapshot(&sql));
                        if let Some(interruption) = interruption {
                            interrupt_next_mutation_commit_for_tests(interruption);
                        }
                        let result = session.execute_trusted_dynamic_insert_batch(
                            ENTITY_NAME,
                            [10, 20, 200]
                                .into_iter()
                                .map(dynamic_payload_patch)
                                .collect(),
                        );
                        if interruption.is_some() {
                            let error = result.unwrap_err();
                            assert_eq!(error.class(), ErrorClass::InvariantViolation, "{error:?}");
                            // Reconstruct the accepted schema from its persisted checkpoint
                            // as well as losing volatile row-replay state.
                            SCHEMA_STORE.with_borrow_mut(|store| *store = SchemaStore::init_heap());
                            forget_recovered_domain_for_tests(&session.db).unwrap();
                            let mut complete = false;
                            for _ in 0..8 {
                                complete = session.db.drive_startup_recovery_page().unwrap();
                                if complete {
                                    break;
                                }
                            }
                            assert!(complete, "admitted partial-index writes must recover");
                            assert!(session.db.drive_startup_recovery_page().unwrap());
                        } else {
                            assert_eq!(result.unwrap().rows.len(), 3);
                        }
                        for (id, payload) in [(1, 10), (2, 20), (3, 200)] {
                            assert_dynamic_payload(&session, id, payload);
                        }
                        INDEX_STORE.with_borrow(|store| {
                            let mut entries = Vec::new();
                            store
                                .visit_entries(|key, value| {
                                    entries.push((
                                        key.to_bytes().into_owned(),
                                        value.to_bytes().into_owned(),
                                    ));
                                    Ok::<_, InternalError>(IndexStoreVisit::Continue)
                                })
                                .unwrap();
                            entries
                        })
                    })
                    .join()
                    .unwrap()
            });
            assert_eq!(entries.len(), expected_count);
            if let Some(expected) = &expected_entries {
                assert_eq!(
                    &entries, expected,
                    "replay must preserve full index effects"
                );
            } else {
                expected_entries = Some(entries);
            }
        }
    }
}
