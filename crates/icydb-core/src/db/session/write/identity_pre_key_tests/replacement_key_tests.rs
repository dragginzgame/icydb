//! Replacement preserves existing generated keys without making them authorable.

use super::*;
use crate::db::schema::FieldWriteManagement;
use icydb_diagnostic_code::{DiagnosticFactTag as Tag, DiagnosticMutationOperation, ErrorCode};

fn replacement_snapshot(
    kind: AcceptedFieldKind,
    policy: SchemaFieldWritePolicy,
    composite: bool,
) -> PersistedSchemaSnapshot {
    let codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
    let fields = vec![
        PersistedFieldSnapshot::new_initial_with_write_policy(
            FieldId::new(1),
            "id".into(),
            SchemaFieldSlot::new(0),
            kind,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            policy,
            FieldStorageDecode::ByKind,
            codec,
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "payload".into(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
    ];
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY_SOURCE.into(),
        ENTITY_NAME.into(),
        if composite {
            vec![FieldId::new(2), FieldId::new(1)]
        } else {
            vec![FieldId::new(1)]
        },
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    )
}

fn assert_owned_replacement_error(
    error: &InternalError,
    session: &DbSession<TestCanister>,
    position: u64,
) {
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), ErrorOrigin::Executor);
    assert_eq!(
        error.diagnostic().error_code(),
        ErrorCode::RUNTIME_BOUNDARY_MUTATION_DATABASE_OWNED_FIELD_EXPLICIT
    );
    assert_mutation_facts(
        error,
        session,
        vec![
            (Tag::EntityTag, ENTITY_TAG.value()),
            (Tag::FieldId, 1),
            (
                Tag::MutationOperation,
                DiagnosticMutationOperation::Replace.raw(),
            ),
            (Tag::BatchPosition, position),
        ],
    );
}

fn non_primary_generation_snapshot(
    kind: AcceptedFieldKind,
    generation: FieldInsertGeneration,
) -> PersistedSchemaSnapshot {
    let mut fields = replacement_snapshot(
        AcceptedFieldKind::Nat64,
        SchemaFieldWritePolicy::from_model_policies(None, None),
        false,
    )
    .fields()
    .to_vec();
    let codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
    fields.push(PersistedFieldSnapshot::new_initial_with_write_policy(
        FieldId::new(3),
        "generated".into(),
        SchemaFieldSlot::new(2),
        kind,
        vec![],
        false,
        SchemaInsertDefault::None,
        SchemaFieldWritePolicy::from_model_policies(Some(generation), None),
        FieldStorageDecode::ByKind,
        codec,
    ));
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY_SOURCE.into(),
        ENTITY_NAME.into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    )
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one matrix pairs ULID/timestamp ownership across typed/structural single/batch routes and authorship rejection"
)]
fn replacement_routes_preserve_non_primary_insert_generation() {
    for (kind, generation) in [
        (AcceptedFieldKind::Ulid, FieldInsertGeneration::Ulid),
        (
            AcceptedFieldKind::Timestamp,
            FieldInsertGeneration::Timestamp,
        ),
    ] {
        for typed in [false, true] {
            for batch in [false, true] {
                let kind = kind.clone();
                std::thread::spawn(move || {
                    let session =
                        initialize_with_snapshot(non_primary_generation_snapshot(kind, generation));
                    let binding = exact_key_binding(&session);
                    let inserted = session
                        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                            entity: ENTITY_NAME.into(),
                            patch: DynamicStructuralPatch::new(vec![
                                ("id".into(), DynamicWriteCell::Value(InputValue::nat64(1))),
                                (
                                    "payload".into(),
                                    DynamicWriteCell::Value(InputValue::nat64(10)),
                                ),
                            ]),
                        })
                        .unwrap();
                    let replace = |id, payload| {
                        if typed {
                            let mutation = DynamicTypedMutation::Replace {
                                key: InputValue::nat64(id),
                                patch: binding
                                    .bind_write_ordinals(vec![(
                                        1,
                                        DynamicWriteCell::Value(InputValue::nat64(payload)),
                                    )])
                                    .unwrap(),
                            };
                            if batch {
                                session
                                    .execute_trusted_same_entity_typed_mutation_batch(
                                        &binding,
                                        vec![mutation],
                                    )
                                    .unwrap()
                                    .unwrap()
                            } else {
                                session
                                    .execute_trusted_typed_mutation(&binding, mutation)
                                    .unwrap()
                                    .unwrap()
                            }
                        } else {
                            let mutation = DynamicMutation::Replace {
                                entity: ENTITY_NAME.into(),
                                key: InputValue::nat64(id),
                                patch: dynamic_payload_patch(payload),
                            };
                            if batch {
                                session
                                    .execute_trusted_dynamic_mutation_batch(vec![mutation])
                                    .unwrap()
                                    .remove(0)
                            } else {
                                session.execute_trusted_dynamic_mutation(&mutation).unwrap()
                            }
                        }
                    };
                    let unchanged = replace(1, 10);
                    assert_eq!(unchanged.affected_rows, 0);
                    assert_eq!(unchanged.rows, inserted.rows);
                    let changed = replace(1, 20);
                    assert_eq!(changed.affected_rows, 1);
                    assert_eq!(changed.rows[0][2], inserted.rows[0][2]);
                    assert_eq!(changed.rows[0][1], OutputValue::nat64(20));
                    let created = replace(2, 30);
                    assert_eq!(created.affected_rows, 1);
                    assert_eq!(created.rows[0][0], OutputValue::nat64(2));
                    assert_eq!(created.rows[0][1], OutputValue::nat64(30));
                    assert!(matches!(
                        (generation, created.rows[0][2].as_public()),
                        (
                            FieldInsertGeneration::Ulid,
                            crate::value::PublicValue::Ulid(_)
                        ) | (
                            FieldInsertGeneration::Timestamp,
                            crate::value::PublicValue::Timestamp(_)
                        )
                    ));
                    for cell in [
                        DynamicWriteCell::Null,
                        DynamicWriteCell::Default,
                        DynamicWriteCell::Value(InputValue::from_public(
                            inserted.rows[0][2].clone().into_public(),
                        )),
                    ] {
                        assert!(
                            session
                                .execute_trusted_dynamic_mutation(&DynamicMutation::Replace {
                                    entity: ENTITY_NAME.into(),
                                    key: InputValue::nat64(1),
                                    patch: DynamicStructuralPatch::new(vec![
                                        (
                                            "payload".into(),
                                            DynamicWriteCell::Value(InputValue::nat64(20))
                                        ),
                                        ("generated".into(), cell),
                                    ]),
                                })
                                .is_err()
                        );
                    }
                })
                .join()
                .expect("isolated replacement route should preserve generated fields");
            }
        }
    }
}

#[test]
fn missing_replacements_reject_database_owned_primary_keys() {
    let ulid = crate::types::Ulid::from_u128(7);
    let timestamp = Timestamp::from_millis(7);
    for (kind, generation, management, chosen) in [
        (
            AcceptedFieldKind::Ulid,
            Some(FieldInsertGeneration::Ulid),
            None,
            InputValue::ulid(ulid),
        ),
        (
            AcceptedFieldKind::Timestamp,
            Some(FieldInsertGeneration::Timestamp),
            None,
            InputValue::timestamp(timestamp),
        ),
        (
            AcceptedFieldKind::Nat64,
            Some(FieldInsertGeneration::Identity),
            None,
            InputValue::nat64(99),
        ),
        (
            AcceptedFieldKind::Timestamp,
            None,
            Some(FieldWriteManagement::CreatedAt),
            InputValue::timestamp(timestamp),
        ),
        (
            AcceptedFieldKind::Timestamp,
            None,
            Some(FieldWriteManagement::UpdatedAt),
            InputValue::timestamp(timestamp),
        ),
    ] {
        for composite in [false, true] {
            if composite && generation == Some(FieldInsertGeneration::Identity) {
                continue; // Identity is restricted to a sole primary-key field.
            }
            let kind = kind.clone();
            let chosen = chosen.clone();
            // Each schema gets fresh thread-local database control and checkpoint state.
            std::thread::spawn(move || {
                let session = initialize_with_snapshot(replacement_snapshot(
                    kind,
                    SchemaFieldWritePolicy::from_model_policies(generation, management),
                    composite,
                ));
                let key = if composite {
                    InputValue::list(vec![InputValue::nat64(10), chosen.clone()])
                } else {
                    chosen.clone()
                };
                let error = session
                    .execute_trusted_dynamic_mutation(&DynamicMutation::Replace {
                        entity: ENTITY_NAME.into(),
                        key,
                        patch: dynamic_payload_patch(10),
                    })
                    .expect_err("a missing row cannot preserve a database-owned key");
                assert_owned_replacement_error(&error, &session, 0);
                assert_eq!(DATA_STORE.with(|store| store.borrow().len()), 0);

                let inserted = session
                    .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                        entity: ENTITY_NAME.into(),
                        patch: dynamic_payload_patch(10),
                    })
                    .expect("omission still generates the primary key");
                let id = inserted.rows[0][0].clone().into_public().into();
                let key = if composite {
                    InputValue::list(vec![InputValue::nat64(10), id])
                } else {
                    id
                };
                let replaced = session
                    .execute_trusted_dynamic_mutation(&DynamicMutation::Replace {
                        entity: ENTITY_NAME.into(),
                        key,
                        patch: dynamic_payload_patch(10),
                    })
                    .expect("an existing row keeps its database-owned key");
                assert_eq!(replaced.rows, inserted.rows);
                assert_eq!(DATA_STORE.with(|store| store.borrow().len()), 1);
            })
            .join()
            .expect("isolated replacement policy case should pass");
        }
    }
}

#[test]
fn generated_replacement_batch_rejection_preserves_earlier_rows() {
    const DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
        ENTITY_SOURCE,
        &[ID_SOURCE],
        &[
            TypedFieldDescriptor::new(ID_SOURCE, TypedFieldType::Scalar(ScalarType::Ulid), false),
            TypedFieldDescriptor::new(
                PAYLOAD_SOURCE,
                TypedFieldType::Scalar(ScalarType::Nat64),
                false,
            ),
        ],
    );
    let session = initialize_with_snapshot(replacement_snapshot(
        AcceptedFieldKind::Ulid,
        SchemaFieldWritePolicy::from_model_policies(Some(FieldInsertGeneration::Ulid), None),
        false,
    ));
    let inserted = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.into(),
            patch: dynamic_payload_patch(10),
        })
        .unwrap();
    let key = InputValue::from_public(inserted.rows[0][0].clone().into_public());
    let missing = InputValue::ulid(crate::types::Ulid::from_u128(7));
    assert_ne!(key, missing);
    let binding = session.issue_typed_entity_binding(&DESCRIPTOR).unwrap();
    for typed in [false, true] {
        let error = if typed {
            let patch = binding
                .bind_write_ordinals(vec![(1, DynamicWriteCell::Value(InputValue::nat64(20)))])
                .unwrap();
            session
                .execute_trusted_same_entity_typed_mutation_batch(
                    &binding,
                    vec![
                        DynamicTypedMutation::Replace {
                            key: key.clone(),
                            patch: patch.clone(),
                        },
                        DynamicTypedMutation::Replace {
                            key: missing.clone(),
                            patch,
                        },
                    ],
                )
                .unwrap_err()
        } else {
            session
                .execute_trusted_dynamic_mutation_batch(vec![
                    DynamicMutation::Replace {
                        entity: ENTITY_NAME.into(),
                        key: key.clone(),
                        patch: dynamic_payload_patch(20),
                    },
                    DynamicMutation::Replace {
                        entity: ENTITY_NAME.into(),
                        key: missing.clone(),
                        patch: dynamic_payload_patch(20),
                    },
                ])
                .unwrap_err()
        };
        assert_owned_replacement_error(&error, &session, 1);
        let rows = session
            .execute_trusted_live_page(&DynamicQuery::new(ENTITY_NAME), None)
            .unwrap()
            .rows;
        assert_eq!(
            rows, inserted.rows,
            "a later rejection must not publish the earlier replacement"
        );
    }
    let result = session
        .execute_trusted_typed_mutation(
            &binding,
            DynamicTypedMutation::Replace {
                key,
                patch: binding
                    .bind_write_ordinals(vec![(1, DynamicWriteCell::Value(InputValue::nat64(30)))])
                    .unwrap(),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(result.rows[0][0], inserted.rows[0][0]);
    assert_eq!(result.rows[0][1], OutputValue::nat64(30));
}

#[test]
fn missing_replacements_still_insert_caller_owned_primary_keys() {
    let session = initialize_with_snapshot(replacement_snapshot(
        AcceptedFieldKind::Nat64,
        SchemaFieldWritePolicy::from_model_policies(None, None),
        false,
    ));
    let dynamic = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Replace {
            entity: ENTITY_NAME.into(),
            key: InputValue::nat64(99),
            patch: dynamic_payload_patch(10),
        })
        .unwrap();
    assert_eq!(dynamic.rows, vec![expected_dynamic_row(99, 10)]);
    let binding = exact_key_binding(&session);
    let typed = session
        .execute_trusted_typed_mutation(
            &binding,
            DynamicTypedMutation::Replace {
                key: InputValue::nat64(100),
                patch: binding
                    .bind_write_ordinals(vec![(1, DynamicWriteCell::Value(InputValue::nat64(20)))])
                    .unwrap(),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(typed.rows, vec![expected_dynamic_row(100, 20)]);
}
