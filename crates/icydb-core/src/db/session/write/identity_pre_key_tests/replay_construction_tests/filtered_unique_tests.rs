//! Filtered unique ownership transfers retain the same meaning during recovery.

use super::*;
use crate::db::{
    FieldRef,
    key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
    schema::{
        PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot, PersistedIndexKeyItemSnapshot,
    },
};
use icydb_diagnostic_code::{DiagnosticConstraintKind, DiagnosticFactTag, ErrorCode};

fn filtered_snapshot(expression: bool) -> PersistedSchemaSnapshot {
    let base = identity_snapshot(JOURNALED_STORE_PATH, true);
    let text = AcceptedFieldKind::Text { max_len: None };
    let fields = vec![
        base.fields()[0].clone(),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "payload".into(),
            SchemaFieldSlot::new(1),
            text.clone(),
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(3),
            "active".into(),
            SchemaFieldSlot::new(2),
            AcceptedFieldKind::Bool,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Bool),
        ),
    ];
    let path = PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["payload".into()],
        text.clone(),
        false,
    );
    let key = if expression {
        PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(Box::new(
            PersistedIndexExpressionSnapshot::new(
                PersistedIndexExpressionOp::Lower,
                path,
                text.clone(),
                text,
                "expr:v1:LOWER(payload)".into(),
            ),
        ))])
    } else {
        PersistedIndexKeySnapshot::FieldPath(vec![path])
    };
    PersistedSchemaSnapshot::new_with_indexes(
        base.version(),
        base.entity_path().into(),
        base.entity_name().into(),
        base.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields.clone(),
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "active_payload".into(),
            JOURNALED_STORE_PATH.into(),
            true,
            key,
            Some(crate::db::schema::AcceptedIndexPredicate::bind_test_sql(
                "active = true",
                &fields,
            )),
        )],
    )
}

fn initialize_filtered(expression: bool) -> DbSession<JournaledTestCanister> {
    let session = DbSession::<JournaledTestCanister>::new(
        &JOURNALED_STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session.db.drive_startup_recovery_page().unwrap();
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        JOURNALED_STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(ENTITY_TAG, filtered_snapshot(expression))]),
        BTreeMap::from([
            ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
            ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
            ((ENTITY_TAG, source_key("active")), FieldId::new(3)),
        ]),
    );
    crate::db::commit::publish_accepted_schema_candidate(
        JOURNALED_STORE_PATH,
        session.db.store_handle(JOURNALED_STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
    session
}

fn active_patch(active: bool) -> DynamicStructuralPatch {
    DynamicStructuralPatch::new(vec![(
        "active".into(),
        DynamicWriteCell::Value(InputValue::boolean(active)),
    )])
}

fn set_active(id: u64, active: bool) -> DynamicMutation {
    DynamicMutation::Update {
        entity: ENTITY_NAME.into(),
        key: InputValue::nat64(id),
        patch: active_patch(active),
    }
}

fn assert_owner(session: &DbSession<JournaledTestCanister>, owner: u64) {
    for id in [1, 2] {
        let page = session
            .execute_trusted_live_page(
                &DynamicQuery::new(ENTITY_NAME)
                    .filter(FieldRef::new("id").eq(id))
                    .select(["active"]),
                None,
            )
            .unwrap();
        assert_eq!(page.rows, vec![vec![OutputValue::boolean(id == owner)]]);
        assert!(page.continuation.is_none());
    }
    JOURNALED_INDEX_STORE.with_borrow(|store| {
        let mut keys = Vec::new();
        store
            .visit_entries(|key, _| {
                keys.push(
                    IndexKey::try_from_raw(key)
                        .unwrap()
                        .primary_key_value()
                        .unwrap(),
                );
                Ok::<_, InternalError>(IndexStoreVisit::Continue)
            })
            .unwrap();
        assert_eq!(
            keys,
            vec![PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(owner))]
        );
    });
}

fn assert_transfer(expression: bool, restart: bool, entering_first: bool) {
    let session = initialize_filtered(expression);
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            [true, false]
                .into_iter()
                .map(|active| {
                    DynamicStructuralPatch::new(vec![
                        (
                            "payload".into(),
                            DynamicWriteCell::Value(InputValue::text(
                                if expression && !active {
                                    "shared"
                                } else {
                                    "Shared"
                                }
                                .into(),
                            )),
                        ),
                        (
                            "active".into(),
                            DynamicWriteCell::Value(InputValue::boolean(active)),
                        ),
                    ])
                })
                .collect(),
        )
        .unwrap();
    drive_journaled_recovery_to_completion(&session);
    assert_owner(&session, 1);

    let mut transfer = vec![set_active(1, false), set_active(2, true)];
    if entering_first {
        transfer.reverse();
    }
    session
        .execute_trusted_dynamic_mutation_batch(transfer)
        .unwrap();
    assert_owner(&session, 2);
    if restart {
        forget_recovered_domain_for_tests(&session.db).unwrap();
    }
    drive_journaled_recovery_to_completion(&session);
    assert_owner(&session, 2);
    assert!(session.db.drive_startup_recovery_page().unwrap());

    // A real final-image collision must still reject atomically; membership
    // only releases a value when the previous owner actually leaves the filter.
    let error = session
        .execute_trusted_dynamic_mutation_batch(vec![set_active(1, true), set_active(2, true)])
        .unwrap_err();
    assert_eq!(
        error.diagnostic().error_code(),
        ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
    );
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::ConstraintKind,
        DiagnosticConstraintKind::Unique.raw(),
    )));
    assert_owner(&session, 2);

    // Transfer back in the opposite operation order, then recover again. This
    // checks continued writes as well as the first successful recovery pass.
    let mut transfer = vec![set_active(2, false), set_active(1, true)];
    if !entering_first {
        transfer.reverse();
    }
    session
        .execute_trusted_dynamic_mutation_batch(transfer)
        .unwrap();
    forget_recovered_domain_for_tests(&session.db).unwrap();
    drive_journaled_recovery_to_completion(&session);
    assert_owner(&session, 1);
}

fn assert_both_orders(expression: bool, restart: bool) {
    for entering_first in [false, true] {
        // Each order needs fresh thread-local stable memory and recovery state.
        std::thread::spawn(move || assert_transfer(expression, restart, entering_first))
            .join()
            .unwrap();
    }
}

#[test]
fn filtered_unique_field_path_transfer_folds_online() {
    assert_both_orders(false, false);
}

#[test]
fn filtered_unique_field_path_transfer_recovers_after_restart() {
    assert_both_orders(false, true);
}

#[test]
fn filtered_unique_expression_transfer_folds_online() {
    assert_both_orders(true, false);
}

#[test]
fn filtered_unique_expression_transfer_recovers_after_restart() {
    assert_both_orders(true, true);
}
