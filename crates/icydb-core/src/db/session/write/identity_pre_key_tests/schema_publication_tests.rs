//! Populated SQL index publication through interrupted marker and journal recovery.
//! Reuses accepted-catalog session fixtures; compares complete effects with a live run.

use super::*;
use crate::db::{
    commit::{
        SchemaPublicationInterruption, interrupt_next_schema_publication_for_tests,
        retained_commit_marker_measurement_for_tests,
    },
    data::StoreVisit,
    index::IndexState,
    schema::enum_catalog::AcceptedSchemaRootSelection,
};
use ic_memory::ic_stable_structures::Storable;

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, Eq, PartialEq)]
struct RecoveredPublication {
    schema_root: AcceptedSchemaRootSelection,
    rows: Entries,
    indexes: Entries,
}

// Text payloads exercise both field and expression indexes through ordinary DDL.
fn text_snapshot() -> PersistedSchemaSnapshot {
    let base = identity_snapshot(JOURNALED_STORE_PATH, false);
    let kind = AcceptedFieldKind::Text { max_len: None };
    PersistedSchemaSnapshot::new_with_indexes(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        FieldId::new(1),
        base.row_layout().clone(),
        vec![
            base.fields()[0].clone(),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "payload".to_string(),
                SchemaFieldSlot::new(1),
                kind.clone(),
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "by_payload".to_string(),
            JOURNALED_STORE_PATH.to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["payload".to_string()],
                kind,
                false,
            )]),
            None,
        )],
    )
}

fn populated_session() -> DbSession<JournaledTestCanister> {
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, &root);
    drive_journaled_recovery_to_completion(&session);
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        JOURNALED_STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([
            (ENTITY_TAG, text_snapshot()),
            (
                SECOND_ENTITY_TAG,
                identity_snapshot_for_entity(
                    JOURNALED_STORE_PATH,
                    false,
                    false,
                    false,
                    SECOND_ENTITY_SOURCE,
                    SECOND_ENTITY_NAME,
                    Some(ENTITY_SOURCE),
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
            (
                (SECOND_ENTITY_TAG, source_key(SECOND_TARGET_SOURCE)),
                FieldId::new(3),
            ),
        ]),
    );
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    crate::db::commit::publish_accepted_schema_candidate(
        JOURNALED_STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            ["Alpha", "BETA"]
                .into_iter()
                .map(|text| {
                    DynamicStructuralPatch::new(vec![(
                        "payload".to_string(),
                        DynamicWriteCell::Value(InputValue::text(text.to_string())),
                    )])
                })
                .collect(),
        )
        .unwrap();
    session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Insert {
            entity: SECOND_ENTITY_NAME.to_string(),
            patch: related_dynamic_payload_patch(100, 1),
        }])
        .unwrap();
    drive_journaled_recovery_to_completion(&session);
    session
}

fn rows(store: StoreHandle) -> Entries {
    let mut entries = Vec::new();
    store.with_data(|data| {
        data.visit_entries(|key, row| {
            entries.push((key.to_bytes().into_owned(), row.as_bytes().to_vec()));
            Ok::<_, InternalError>(StoreVisit::Continue)
        })
        .unwrap();
    });
    entries
}

fn indexes(store: StoreHandle, unrelated_only: bool) -> Entries {
    let mut entries = Vec::new();
    store.with_index(|index| {
        index
            .visit_entries(|key, value| {
                let decoded = IndexKey::try_from_raw(key).unwrap();
                if !unrelated_only
                    || decoded.key_kind() != IndexKeyKind::User
                    || decoded.index_id().entity_tag() != ENTITY_TAG
                {
                    entries.push((key.to_bytes().into_owned(), value.to_bytes().into_owned()));
                }
                Ok::<_, InternalError>(IndexStoreVisit::Continue)
            })
            .unwrap();
    });
    entries
}

fn assert_pending(session: &DbSession<JournaledTestCanister>) {
    let error = session.db.ensure_recovered_state().unwrap_err();
    assert_eq!(
        error.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );
    assert!(
        retained_commit_marker_measurement_for_tests()
            .unwrap()
            .is_some()
    );
}

// Restart with the same marker after Replay and again after durable Fold. This
// proves retained-authority idempotence, not just a no-op after marker deletion.
fn recover_with_stage_loss(session: &DbSession<JournaledTestCanister>) {
    forget_recovered_domain_for_tests(&session.db).unwrap();
    assert_pending(session);
    assert!(!session.db.drive_startup_recovery_page().unwrap());
    assert_pending(session);
    forget_recovered_domain_for_tests(&session.db).unwrap();
    assert!(!session.db.drive_startup_recovery_page().unwrap());
    assert!(!session.db.drive_startup_recovery_page().unwrap());
    assert_pending(session);
    let watermark = JOURNALED_TAIL_STORE.with_borrow(|tail| tail.fold_watermark().unwrap());
    assert!(!JOURNALED_TAIL_STORE.with_borrow(JournalTailStore::has_stored_batch));
    forget_recovered_domain_for_tests(&session.db).unwrap();
    assert!(!session.db.drive_startup_recovery_page().unwrap());
    assert_pending(session);
    assert_eq!(
        JOURNALED_TAIL_STORE.with_borrow(|tail| tail.fold_watermark().unwrap()),
        watermark,
    );
    assert!(session.db.drive_startup_recovery_page().unwrap());
    assert!(session.db.drive_startup_recovery_page().unwrap());
}

fn publication_case(
    sql: &'static str,
    interruption: Option<SchemaPublicationInterruption>,
) -> RecoveredPublication {
    let session = populated_session();
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    let before_rows = rows(store);
    let unrelated = indexes(store, true);
    // One other entity's user key and its reverse edge are real maintained data.
    assert_eq!(unrelated.len(), 2);
    let before_indexes = indexes(store, false);
    let before_schema = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .unwrap()
        .unwrap();
    if let Some(cut) = interruption {
        interrupt_next_schema_publication_for_tests(cut);
    }
    let result = session.execute_admin_sql_ddl(sql);
    if let Some(cut) = interruption {
        let error = result.expect_err("the real publication path must reach the armed cut");
        assert_eq!(
            error.diagnostic().class(),
            icydb_diagnostic_code::ErrorClass::InvariantViolation
        );
        assert_pending(&session);
        if cut == SchemaPublicationInterruption::MarkerPersisted {
            assert_eq!(indexes(store, false), before_indexes);
            assert_eq!(
                store
                    .with_schema(SchemaStore::current_accepted_schema_bundle)
                    .unwrap()
                    .unwrap(),
                before_schema
            );
        } else {
            assert_eq!(store.index_state(), IndexState::Building);
            assert_ne!(indexes(store, false), before_indexes);
        }
        recover_with_stage_loss(&session);
    } else {
        result.expect("uninterrupted publication should succeed");
        drive_journaled_recovery_to_completion(&session);
    }
    assert!(session.db.ensure_recovered_state().is_ok());
    assert_eq!(store.index_state(), IndexState::Ready);
    assert!(
        retained_commit_marker_measurement_for_tests()
            .unwrap()
            .is_none()
    );
    assert!(!JOURNALED_TAIL_STORE.with_borrow(JournalTailStore::has_stored_batch));
    assert_eq!(rows(store), before_rows);
    assert_eq!(indexes(store, true), unrelated);
    let (schema_root, schema) = store
        .with_schema(SchemaStore::current_canonical_accepted_schema_authority)
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .with_schema(SchemaStore::current_accepted_schema_bundle)
            .unwrap()
            .unwrap(),
        schema
    );
    assert_eq!(schema.revision(), AcceptedSchemaRevision::new(2));
    assert_eq!(schema.entity_snapshots()[&ENTITY_TAG].indexes().len(), 2);
    let indexes = indexes(store, false);
    assert_eq!(indexes.len(), before_indexes.len() + 2);
    RecoveredPublication {
        schema_root,
        rows: before_rows,
        indexes,
    }
}

fn assert_publication_recovery(sql: &'static str) {
    let mut expected = None;
    for interruption in [
        None,
        Some(SchemaPublicationInterruption::MarkerPersisted),
        Some(SchemaPublicationInterruption::IndexDeleted),
        Some(SchemaPublicationInterruption::IndexInserted),
    ] {
        // Each comparison must start with independent stable and volatile TLS.
        let actual = std::thread::spawn(move || publication_case(sql, interruption))
            .join()
            .unwrap();
        if let Some(expected) = &expected {
            assert_eq!(
                &actual, expected,
                "interrupted publication must match uninterrupted state"
            );
        } else {
            expected = Some(actual);
        }
    }
}

#[test]
fn populated_field_index_publication_recovers_exact_domain_after_interruption() {
    assert_publication_recovery(
        "CREATE INDEX by_id_payload ON IdentityRow (id, payload) EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
    );
}

#[test]
fn populated_expression_index_publication_recovers_exact_domain_after_interruption() {
    assert_publication_recovery(
        "CREATE INDEX by_lower_payload ON IdentityRow (LOWER(payload)) EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2",
    );
}
