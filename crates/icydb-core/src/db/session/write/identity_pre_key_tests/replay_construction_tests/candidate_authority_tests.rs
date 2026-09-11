//! Candidate ownership follows accepted live/canonical schema, not cached syntax.

use super::*;
use crate::db::{
    commit::{
        CommitPrepareContext, CommitPrepareMode, CommitRowOp,
        prepare_commit_context_from_catalog_selection, prepare_row_commit_with_context,
    },
    data::DecodedDataStoreKey,
    key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
    relation::RelationCommitBudget,
    schema::AcceptedSchemaFingerprint,
};
use ic_memory::ic_stable_structures::Storable;

const CANDIDATE_GENERATION: u64 = 9;

fn candidate_snapshot(
    store_path: &str,
    base_fingerprint: AcceptedSchemaFingerprint,
) -> PersistedSchemaSnapshot {
    let index_id = SchemaIndexId::new(2).unwrap();
    let candidate = PersistedIndexSnapshot::new_sql_ddl(
        index_id,
        2,
        "pending_unique_payload".to_string(),
        store_path.to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["payload".to_string()],
            AcceptedFieldKind::Nat64,
            false,
        )]),
        None,
    )
    .clone_with_schema_identity(index_id, 2, CANDIDATE_GENERATION);
    identity_snapshot(store_path, false)
        .with_added_unique_activation(candidate, base_fingerprint, CANDIDATE_GENERATION)
        .unwrap()
        .with_schema_version(SchemaVersion::new(2))
}

fn publish_candidate<C: CanisterKind>(session: &DbSession<C>, store_path: &'static str) {
    let store = session.db.store_handle(store_path).unwrap();
    let base_fingerprint = store
        .with_schema(SchemaStore::current_accepted_schema_root)
        .unwrap()
        .unwrap()
        .root()
        .fingerprint();
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        store_path,
        AcceptedSchemaRevision::new(2),
        BTreeMap::from([
            (ENTITY_TAG, candidate_snapshot(store_path, base_fingerprint)),
            (
                SECOND_ENTITY_TAG,
                identity_snapshot_for_entity(
                    store_path,
                    false,
                    false,
                    false,
                    SECOND_ENTITY_SOURCE,
                    SECOND_ENTITY_NAME,
                    None,
                ),
            ),
        ]),
        BTreeMap::from([
            ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
            ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
            (
                (SECOND_ENTITY_TAG, source_key(SECOND_ID_SOURCE)),
                FieldId::new(1),
            ),
            (
                (SECOND_ENTITY_TAG, source_key(SECOND_PAYLOAD_SOURCE)),
                FieldId::new(2),
            ),
        ]),
    );
    crate::db::commit::publish_accepted_schema_candidate(
        store_path,
        store,
        AcceptedSchemaRevision::INITIAL,
        &candidate,
    )
    .unwrap();
}

fn fingerprint(session: &DbSession<JournaledTestCanister>) -> [u8; 16] {
    session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap()
        .fingerprint()
}

fn prepared_delete_owners(
    session: &DbSession<JournaledTestCanister>,
    fingerprint: [u8; 16],
    mode: CommitPrepareMode,
) -> Vec<IndexId> {
    let context = session
        .db
        .accepted_runtime_entity_for_path(ENTITY_SOURCE)
        .unwrap()
        .prepare_commit_context(&session.db, fingerprint, mode)
        .unwrap();
    prepared_delete_owners_with_context(session, fingerprint, &context).unwrap()
}

fn prepared_delete_owners_with_context(
    session: &DbSession<JournaledTestCanister>,
    fingerprint: [u8; 16],
    context: &CommitPrepareContext,
) -> Result<Vec<IndexId>, InternalError> {
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    let key = DecodedDataStoreKey::new_primary_key_value(
        ENTITY_TAG,
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(1)),
    )
    .to_raw()
    .unwrap();
    let before = store.with_data(|data| data.get_canonical(&key).unwrap().into_bytes());
    let op = CommitRowOp::new(ENTITY_SOURCE, key, Some(before), None, fingerprint);
    // Only schema selection differs here: deleting a seeded row needs no
    // uniqueness witness. Real Replay/Fold readers are exercised below.
    let prepared = prepare_row_commit_with_context(
        &session.db,
        &op,
        context,
        &session.db,
        &store,
        &mut RelationCommitBudget::default(),
    )?;
    assert!(prepared.data_value.is_none());
    Ok(prepared
        .index_ops
        .iter()
        .map(|operation| {
            assert!(operation.value.is_none());
            *IndexKey::try_from_raw(&operation.key).unwrap().index_id()
        })
        .collect())
}

fn seed_row(session: &DbSession<JournaledTestCanister>) {
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(10)])
        .unwrap();
    drive_journaled_recovery_to_completion(session);
}

#[test]
fn candidate_delete_preparation_tracks_live_and_canonical_schema_ownership() {
    let session = initialize_journaled();
    seed_row(&session);
    let predecessor_fingerprint = fingerprint(&session);
    let predecessor = prepared_delete_owners(
        &session,
        predecessor_fingerprint,
        CommitPrepareMode::RecoveryReplay,
    );
    publish_candidate(&session, JOURNALED_STORE_PATH);
    let candidate_fingerprint = fingerprint(&session);
    assert_ne!(candidate_fingerprint, predecessor_fingerprint);
    let candidate_id = IndexId::new_with_generation(ENTITY_TAG, 2, CANDIDATE_GENERATION);
    let live = prepared_delete_owners(
        &session,
        candidate_fingerprint,
        CommitPrepareMode::NormalWrite,
    );
    assert!(live.contains(&candidate_id));
    assert!(!predecessor.contains(&candidate_id));
    assert_eq!(
        prepared_delete_owners(
            &session,
            predecessor_fingerprint,
            CommitPrepareMode::RecoveryReplay
        ),
        predecessor,
        "unfolded live activation must not replace canonical predecessor authority",
    );

    let selection = session
        .db
        .store_handle(JOURNALED_STORE_PATH)
        .unwrap()
        .with_schema(|schema| {
            schema.current_accepted_catalog_selection(
                ENTITY_TAG,
                ENTITY_SOURCE,
                JOURNALED_STORE_PATH,
            )
        })
        .unwrap()
        .unwrap();
    let qualification = prepare_commit_context_from_catalog_selection(
        &session.db,
        &selection,
        candidate_fingerprint,
        CommitPrepareMode::RecoveryReplay,
    )
    .unwrap();
    assert_eq!(
        prepared_delete_owners_with_context(&session, candidate_fingerprint, &qualification)
            .unwrap(),
        live,
        "explicit logical predecessor must not be replaced by lagging canonical schema",
    );
    // Drop the operation-local context before advancing accepted authority.
    drop(qualification);

    drive_journaled_recovery_to_completion(&session);
    assert_eq!(
        prepared_delete_owners(
            &session,
            candidate_fingerprint,
            CommitPrepareMode::RecoveryReplay
        ),
        live,
        "a fresh recovery context must use the folded candidate generation",
    );
}

#[test]
fn explicit_selection_preserves_entity_and_record_fingerprint_checks() {
    let session = initialize_journaled();
    seed_row(&session);
    publish_candidate(&session, JOURNALED_STORE_PATH);
    let fingerprint = fingerprint(&session);
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    for (tag, path, expected) in [
        (
            SECOND_ENTITY_TAG,
            SECOND_ENTITY_SOURCE,
            ErrorClass::Corruption,
        ),
        (ENTITY_TAG, ENTITY_SOURCE, ErrorClass::Unsupported),
    ] {
        let selection = store
            .with_schema(|schema| {
                schema.current_accepted_catalog_selection(tag, path, JOURNALED_STORE_PATH)
            })
            .unwrap()
            .unwrap();
        let context = prepare_commit_context_from_catalog_selection(
            &session.db,
            &selection,
            fingerprint,
            CommitPrepareMode::RecoveryReplay,
        )
        .unwrap();
        assert_eq!(context.entity_tag(), tag);
        let mut recorded_fingerprint = fingerprint;
        if tag == ENTITY_TAG {
            recorded_fingerprint[0] ^= 1;
        }
        let error = prepared_delete_owners_with_context(&session, recorded_fingerprint, &context)
            .unwrap_err();
        assert_eq!(error.class(), expected);
    }
}

#[test]
fn candidate_schema_folds_before_an_interrupted_delete_and_survives_stage_loss() {
    let session = initialize_journaled();
    seed_row(&session);
    publish_candidate(&session, JOURNALED_STORE_PATH);
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::MarkerPersisted);
    let error = session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
        }])
        .expect_err("delete should interrupt at the marker boundary");
    assert_eq!(error.class(), ErrorClass::InvariantViolation);
    forget_recovered_domain_for_tests(&session.db).unwrap();
    assert!(!session.db.drive_startup_recovery_page().unwrap());
    forget_recovered_domain_for_tests(&session.db).unwrap();
    drive_journaled_recovery_to_completion(&session);
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    assert_eq!(store.with_data(DataStore::len), 0);
    let canonical = store
        .with_schema(SchemaStore::current_canonical_accepted_schema_bundle)
        .unwrap()
        .unwrap();
    let snapshot = canonical.entity_snapshots().get(&ENTITY_TAG).unwrap();
    assert_eq!(snapshot.candidate_indexes().len(), 1);
    assert_eq!(
        snapshot.candidate_indexes()[0].physical_generation(),
        CANDIDATE_GENERATION
    );
    assert_eq!(
        snapshot.constraint_activations()[0].activation_epoch(),
        CANDIDATE_GENERATION
    );
    assert!(session.db.ensure_recovered_state().is_ok());
}

#[test]
fn direct_recovery_restores_candidate_authority_from_the_live_schema_checkpoint() {
    let session = initialize();
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(10)])
        .unwrap();
    publish_candidate(&session, STORE_PATH);
    let accepted_before = SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .unwrap()
        .unwrap();
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::MarkerPersisted);
    // Heap-only deletes are not durable row payloads. A same-store identity
    // insert binds both rows into the maintained direct identity commit.
    let error = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
            },
            DynamicMutation::Insert {
                entity: SECOND_ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(20),
            },
        ])
        .expect_err("delete should interrupt before its row effect is published");
    assert_eq!(error.class(), ErrorClass::InvariantViolation);

    // Lose only the volatile accepted catalog and stage. Startup must restore
    // the exact checkpoint, including its candidate, before preparing the row.
    SCHEMA_STORE.with_borrow_mut(|store| *store = SchemaStore::init_heap());
    forget_recovered_domain_for_tests(&session.db).unwrap();
    assert!(!session.db.drive_startup_recovery_page().unwrap());
    assert!(session.db.ensure_recovered_state().is_err());
    let restored = SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .unwrap()
        .unwrap();
    assert_eq!(restored, accepted_before);
    let row_key = |tag| {
        DecodedDataStoreKey::new_primary_key_value(
            tag,
            &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(1)),
        )
        .to_raw()
        .unwrap()
    };
    DATA_STORE.with_borrow(|store| {
        assert!(store.get_canonical(&row_key(ENTITY_TAG)).is_none());
        assert!(store.get_canonical(&row_key(SECOND_ENTITY_TAG)).is_some());
        assert_eq!(store.len(), 1);
    });
    assert_eq!(INDEX_STORE.with_borrow(IndexStore::len), 1);
    assert!(session.db.drive_startup_recovery_page().unwrap());
    assert!(session.db.ensure_recovered_state().is_ok());
}
