//! Journal folds preserve the completed portion of an exact-cardinality build.

use super::*;

fn page(handle: crate::db::registry::StoreHandle) -> CardinalityGenerationPageOutcome {
    handle
        .with_data(|data| {
            handle.with_index(|index| {
                handle.with_schema_mut(|schema| {
                    drive_cardinality_generation_page(data, index, schema, |schema| {
                        CardinalityBuildAuthority::derive(
                            schema,
                            database_incarnation_id()?,
                            handle.allocation_identities(),
                            JOURNALED_TAIL_STORE.with_borrow(JournalTailStore::fold_watermark)?,
                        )
                    })
                })
            })
        })
        .unwrap()
}

fn insert(session: &DbSession<JournaledTestCanister>, payload: u64) {
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(payload),
        })
        .unwrap();
    drive_journaled_recovery_to_completion(session);
}

#[test]
fn building_cardinality_finishes_with_a_write_between_every_page() {
    let session = initialize_journaled();
    insert(&session, 10);
    drive_journaled_cardinality_to_ready(&session);
    mark_journaled_cardinality_building();
    let handle = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    let initial = handle
        .with_schema(SchemaStore::cardinality_generation_header)
        .unwrap()
        .unwrap();
    insert(&session, 10); // Fold while the slot still awaits clearing.
    for rows in 2..10 {
        if page(handle) == CardinalityGenerationPageOutcome::PublishedReady {
            let (index_id, components) = journaled_user_index_prefix();
            reset_journaled_cardinality_projections();
            assert_journaled_cardinality(handle, index_id, &components, rows);
            assert_eq!(
                handle
                    .with_schema(SchemaStore::cardinality_generation_header)
                    .unwrap()
                    .unwrap()
                    .generation(),
                initial.generation()
            );
            return;
        }
        insert(&session, 10);
        let header = handle
            .with_schema(SchemaStore::cardinality_generation_header)
            .unwrap()
            .unwrap();
        let authority = handle
            .with_schema(|schema| {
                CardinalityBuildAuthority::derive(
                    schema,
                    database_incarnation_id()?,
                    handle.allocation_identities(),
                    JOURNALED_TAIL_STORE.with_borrow(JournalTailStore::fold_watermark)?,
                )
            })
            .unwrap();
        assert!(
            header.validate_source(authority.source()).is_ok(),
            "fold must advance the Building source without discarding its scan"
        );
    }
    panic!("busy store must finish its bounded build");
}

// Seed the exact result of a short source page through the ordinary preflighted
// publication boundary. This isolates checkpoint edges without thousands of rows.
fn partial_page(
    handle: StoreHandle,
    phase: crate::db::schema::cardinality_generation::CardinalityBuildPhase,
    checkpoint: crate::db::schema::cardinality_generation::CardinalityBuildCheckpoint,
    increments: &[(
        crate::db::schema::cardinality_generation::CardinalityCountDigest,
        u64,
    )],
) {
    use crate::db::schema::cardinality_generation::CardinalityBuildCursor;
    handle.with_schema_mut(|schema| {
        let header = schema.cardinality_generation_header().unwrap().unwrap();
        let before = schema.cardinality_build_cursor().unwrap().unwrap();
        let counts = schema
            .prepare_cardinality_count_increments(header.slot(), header.generation(), increments)
            .unwrap();
        let next = CardinalityBuildCursor::new(
            header.generation(),
            header.slot(),
            header.source(),
            phase,
            Some(checkpoint),
            before
                .totals()
                .checked_add_page(2, 100, 0, counts.new_count_keys())
                .unwrap(),
        )
        .unwrap();
        let page = schema
            .prepare_cardinality_build_page(header, &before, counts, &next)
            .unwrap();
        schema.apply_prepared_cardinality_build_page(page).unwrap();
    });
}

fn mutate(session: &DbSession<JournaledTestCanister>, key: u64, payload: Option<u64>) {
    let mutation = match payload {
        Some(payload) => DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(key),
            patch: dynamic_payload_patch(payload),
        },
        None => DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(key),
        },
    };
    session.execute_trusted_dynamic_mutation(&mutation).unwrap();
    drive_journaled_recovery_to_completion(session);
}

fn assert_count(
    handle: StoreHandle,
    digest: crate::db::schema::cardinality_generation::CardinalityCountDigest,
    expected: u64,
) {
    handle.with_schema(|schema| {
        let header = schema.cardinality_generation_header().unwrap().unwrap();
        assert_eq!(
            schema
                .cardinality_count(header.slot(), header.generation(), digest)
                .unwrap()
                .unwrap_or(0),
            expected
        );
    });
}

#[test]
fn building_cardinality_row_checkpoint_applies_only_completed_domain_changes() {
    use crate::db::{
        data::DecodedDataStoreKey,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::cardinality_generation::{
            CardinalityBuildCheckpoint, CardinalityBuildPhase, CardinalityCountDigest,
        },
    };
    let session = initialize_journaled();
    for _ in 0..4 {
        insert(&session, 10);
    }
    drive_journaled_cardinality_to_ready(&session);
    mark_journaled_cardinality_building();
    let handle = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    assert_eq!(
        page(handle),
        CardinalityGenerationPageOutcome::WorkRemaining
    );
    let digest = CardinalityCountDigest::for_entity(ENTITY_TAG);
    let key = DecodedDataStoreKey::new_primary_key_value(
        ENTITY_TAG,
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(2)),
    )
    .to_raw()
    .unwrap();
    partial_page(
        handle,
        CardinalityBuildPhase::Rows,
        CardinalityBuildCheckpoint::Row(key),
        &[(digest, 2)],
    );
    let before = handle
        .with_schema(SchemaStore::cardinality_build_cursor)
        .unwrap()
        .unwrap();
    mutate(&session, 1, Some(20)); // A retained row changes only its index keys.
    mutate(&session, 2, None); // At the inclusive completed checkpoint.
    mutate(&session, 4, None); // Ahead of the checkpoint: never counted yet.
    insert(&session, 10); // Also ahead: the later scan must count it exactly once.
    assert_count(handle, digest, 1);
    let after = handle
        .with_schema(SchemaStore::cardinality_build_cursor)
        .unwrap()
        .unwrap();
    assert_eq!(after.phase(), before.phase());
    assert_eq!(after.checkpoint(), before.checkpoint());
    assert_eq!(after.totals(), before.totals());
    drive_journaled_cardinality_to_ready(&session);
    assert_count(handle, digest, 3);
}

#[test]
fn building_cardinality_index_checkpoint_tracks_keys_moving_across_it() {
    use crate::db::schema::cardinality_generation::{
        CardinalityBuildCheckpoint, CardinalityBuildPhase, CardinalityCountDigest,
    };
    let session = initialize_journaled();
    for _ in 0..4 {
        insert(&session, 10);
    }
    drive_journaled_cardinality_to_ready(&session);
    mark_journaled_cardinality_building();
    let handle = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    assert_eq!(
        page(handle),
        CardinalityGenerationPageOutcome::WorkRemaining
    );
    assert_eq!(
        page(handle),
        CardinalityGenerationPageOutcome::WorkRemaining
    );
    let mut keys = Vec::new();
    handle
        .with_index(|index| {
            index.visit_entries(|key, _| {
                if IndexKey::try_from_raw(key).unwrap().key_kind() == IndexKeyKind::User {
                    keys.push(key.clone());
                }
                Ok::<_, InternalError>(IndexStoreVisit::Continue)
            })
        })
        .unwrap();
    assert_eq!(keys.len(), 4);
    let decoded = IndexKey::try_from_raw(&keys[1]).unwrap();
    let index_id = *decoded.index_id();
    let components = (0..decoded.component_count())
        .map(|i| decoded.component(i).unwrap().to_vec())
        .collect::<Vec<_>>();
    let digest = CardinalityCountDigest::for_user_index_prefix(index_id, &components).unwrap();
    partial_page(
        handle,
        CardinalityBuildPhase::Indexes,
        CardinalityBuildCheckpoint::Index(keys[1].clone()),
        &[(digest, 2)],
    );
    mutate(&session, 1, Some(20)); // Scanned key moves ahead: decrement only old key.
    mutate(&session, 4, Some(5)); // Unscanned key moves behind: increment only new key.
    mutate(&session, 2, None); // Delete exactly at the checkpoint.
    insert(&session, 10); // Appended key remains ahead and is counted by the scan.
    assert_count(handle, digest, 0);
    // Forget recovery state so persisted header/cursor authority is re-admitted.
    forget_recovered_domain_for_tests(&session.db).unwrap();
    drive_journaled_recovery_to_completion(&session);
    drive_journaled_cardinality_to_ready(&session);
    assert_count(handle, CardinalityCountDigest::for_entity(ENTITY_TAG), 4);
    assert_count(handle, digest, 2); // Keys 3 and 5 still have payload 10.
    let mut moved_prefixes = std::collections::BTreeSet::new();
    handle
        .with_index(|index| {
            index.visit_entries(|key, _| {
                let key = IndexKey::try_from_raw(key).unwrap();
                if key.key_kind() == IndexKeyKind::User {
                    let parts = (0..key.component_count())
                        .map(|i| key.component(i).unwrap().to_vec())
                        .collect::<Vec<_>>();
                    if parts != components {
                        moved_prefixes.insert(
                            CardinalityCountDigest::for_user_index_prefix(index_id, &parts)
                                .unwrap(),
                        );
                    }
                }
                Ok::<_, InternalError>(IndexStoreVisit::Continue)
            })
        })
        .unwrap();
    // Both moved-to prefixes have one row; their byte spelling is owned by
    // the index encoder, not this coverage test.
    assert_eq!(moved_prefixes.len(), 2);
    for digest in moved_prefixes {
        assert_count(handle, digest, 1);
    }
}

#[test]
fn building_cardinality_malformed_checkpoint_does_not_block_authoritative_fold() {
    use crate::db::{
        data::RawDataStoreKey,
        schema::cardinality_generation::{CardinalityBuildCheckpoint, CardinalityBuildPhase},
    };
    let session = initialize_journaled();
    insert(&session, 10);
    drive_journaled_cardinality_to_ready(&session);
    mark_journaled_cardinality_building();
    let handle = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    page(handle);
    partial_page(
        handle,
        CardinalityBuildPhase::Rows,
        CardinalityBuildCheckpoint::Row(RawDataStoreKey::from_persisted_bytes(vec![0xff])),
        &[],
    );
    let before = handle
        .with_schema(SchemaStore::cardinality_generation_control)
        .unwrap();
    insert(&session, 20);
    assert_eq!(
        handle
            .with_schema(SchemaStore::cardinality_generation_control)
            .unwrap(),
        before
    );
    assert_eq!(handle.exact_entity_count(ENTITY_TAG), None);
    assert_dynamic_payload(&session, 2, 20);
}
