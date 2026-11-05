use super::*;
use crate::db::schema::{
    AcceptedConstraintCatalog, AcceptedFieldKind, AcceptedSchemaFingerprint,
    ConstraintActivationKind, ConstraintActivationSnapshot, ConstraintActivationState,
    ConstraintId, ConstraintIdAllocator, ConstraintOrigin, FieldId, FieldInsertGeneration,
    FieldStorageDecode, LeafCodec, PersistedFieldOrigin, PersistedRelationEdgeSnapshot,
    PersistedRelationPathStepSnapshot, RelationId, RowLayoutVersion, ScalarCodec, SchemaFieldSlot,
    SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaInsertDefault, SchemaRowLayout,
    SchemaVersion,
};

fn snapshot(generation: Option<FieldInsertGeneration>) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::transition::Identity".to_string(),
        "Identity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial_with_write_policy(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            SchemaFieldWritePolicy::from_model_policies(generation, None),
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )],
    )
}

#[test]
fn ddl_additions_reject_identity_policy_changes_and_no_op_snapshots() {
    let actual = snapshot(None);
    assert!(plan_sql_ddl_addition(&actual, &actual).is_none());
    for (actual, expected) in [
        (None, Some(FieldInsertGeneration::Identity)),
        (Some(FieldInsertGeneration::Identity), None),
    ] {
        assert!(plan_sql_ddl_addition(&snapshot(actual), &snapshot(expected)).is_none());
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep catalog admission and transition controls paired for each named contract and wire lane"
)]
fn named_canonical_historical_fills_admit_append_only_transitions() {
    use crate::{
        db::{
            data::{
                encode_canonical_value_storage_bytes,
                validate_default_payload_for_accepted_field_contract,
            },
            schema::{
                AcceptedFieldDecodeContract, TestEnumDefinition, TestEnumVariant,
                build_accepted_enum_catalog_for_tests, build_record_composite_catalog_for_tests,
            },
        },
        value::{Value, ValueEnum},
    };

    let enums = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        "tests::Status",
        vec![TestEnumVariant::unit("Active")],
    )])
    .unwrap();
    let type_id = enums.type_id("tests::Status").unwrap();
    let variant_id = enums
        .enum_type(type_id)
        .unwrap()
        .variant_id("Active")
        .unwrap();
    let enum_value = Value::Enum(ValueEnum::test_unit(type_id.get(), variant_id.get()));
    let (composites, record_id) = build_record_composite_catalog_for_tests(
        "tests::Profile".into(),
        "status".into(),
        AcceptedFieldKind::Enum { type_id },
        false,
        &enums,
    );
    for (kind, value) in [
        (AcceptedFieldKind::Enum { type_id }, enum_value.clone()),
        (
            AcceptedFieldKind::Composite { type_id: record_id },
            Value::Map(vec![(Value::Text("status".into()), enum_value)]),
        ),
    ] {
        let wire = encode_canonical_value_storage_bytes(&value).unwrap();
        for lane in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
            let contract = AcceptedFieldDecodeContract::new(
                "named",
                &kind,
                false,
                lane,
                LeafCodec::Structural,
            );
            validate_default_payload_for_accepted_field_contract(
                &enums,
                &composites,
                contract,
                &wire,
            )
            .unwrap();
            for unadmitted in [Value::Null, Value::Nat64(9)] {
                let payload = encode_canonical_value_storage_bytes(&unadmitted).unwrap();
                assert!(
                    validate_default_payload_for_accepted_field_contract(
                        &enums,
                        &composites,
                        contract,
                        &payload
                    )
                    .is_err()
                );
            }
            let actual = snapshot(None);
            let layout = RowLayoutVersion::INITIAL.checked_next().unwrap();
            let candidate = |payload: Vec<u8>| {
                let mut fields = actual.fields().to_vec();
                fields.push(PersistedFieldSnapshot::new_with_write_policy_and_origin(
                    FieldId::new(2),
                    "named".into(),
                    SchemaFieldSlot::new(1),
                    kind.clone(),
                    Vec::new(),
                    false,
                    layout,
                    SchemaInsertDefault::SlotPayload(payload.clone()),
                    SchemaHistoricalFill::SlotPayload(payload),
                    SchemaFieldWritePolicy::none(),
                    PersistedFieldOrigin::Generated,
                    lane,
                    LeafCodec::Structural,
                ));
                PersistedSchemaSnapshot::new(
                    actual.version(),
                    actual.entity_path().into(),
                    actual.entity_name().into(),
                    actual.primary_key_field_ids().to_vec(),
                    SchemaRowLayout::new(
                        layout,
                        RowLayoutVersion::INITIAL,
                        vec![
                            (FieldId::new(1), SchemaFieldSlot::new(0)),
                            (FieldId::new(2), SchemaFieldSlot::new(1)),
                        ],
                    ),
                    fields,
                )
            };
            for _ in 0..2 {
                let Some(plan) = plan_sql_ddl_addition(&actual, &candidate(wire.clone())) else {
                    panic!("canonical named fill must admit the append-only transition");
                };
                assert_eq!(plan, SchemaTransitionPlan::AppendOnlyFields);
            }
            let mut trailing = wire.clone();
            trailing.push(0);
            for malformed in [
                Vec::new(),
                wire[..wire.len() - 1].to_vec(),
                trailing,
                vec![0],
            ] {
                assert!(plan_sql_ddl_addition(&actual, &candidate(malformed)).is_none());
            }
        }
    }
}

#[test]
fn ddl_field_addition_rejects_unrelated_policy_or_activation_changes() {
    let actual = snapshot(None);
    let current_layout = RowLayoutVersion::INITIAL
        .checked_next()
        .expect("test layout should advance");
    let mut fields = actual.fields().to_vec();
    fields.push(PersistedFieldSnapshot::new_with_write_policy_and_origin(
        FieldId::new(2),
        "target_id".to_string(),
        SchemaFieldSlot::new(1),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        true,
        current_layout,
        SchemaInsertDefault::None,
        SchemaHistoricalFill::Null,
        SchemaFieldWritePolicy::none(),
        PersistedFieldOrigin::Generated,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    ));
    let relation = PersistedRelationEdgeSnapshot::new_nested(
        RelationId::new(1).expect("test relation identity should be non-zero"),
        "target_id".to_string(),
        "schema::transition::Target".to_string(),
        FieldId::new(2),
        vec![PersistedRelationPathStepSnapshot::OptionalSome],
    )
    .clone_with_physical_generation(7);
    let activation_id = ConstraintId::new(
        actual
            .constraint_catalog()
            .allocator()
            .high_water()
            .checked_add(1)
            .expect("test constraint identity should advance"),
    )
    .expect("test constraint identity should be non-zero");
    let activation = ConstraintActivationSnapshot::new(
        activation_id,
        relation.name().to_string(),
        ConstraintOrigin::Generated,
        ConstraintActivationKind::Relation {
            relation_id: relation.id(),
        },
        ConstraintActivationState::EnforcingNewWrites,
        AcceptedSchemaFingerprint::new([0x53; 32]),
        relation.physical_generation(),
    );
    let catalog = AcceptedConstraintCatalog::from_persisted_parts(
        ConstraintIdAllocator::new(activation_id.get()),
        actual.constraints().to_vec(),
        vec![activation],
    );
    let expected = PersistedSchemaSnapshot::new(
        actual.version(),
        actual.entity_path().to_string(),
        actual.entity_name().to_string(),
        actual.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            current_layout,
            RowLayoutVersion::INITIAL,
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        fields,
    );
    let plan = plan_sql_ddl_addition(&actual, &expected)
        .expect("the nullable field addition alone should admit");
    assert_eq!(plan, SchemaTransitionPlan::AppendOnlyFields);

    let mut changed_fields = expected.fields().to_vec();
    changed_fields[0] = snapshot(Some(FieldInsertGeneration::Identity)).fields()[0].clone();
    let changed_policy = expected.clone().with_fields(changed_fields);
    assert!(plan_sql_ddl_addition(&actual, &changed_policy).is_none());

    let with_activation = expected
        .with_constraint_catalog(catalog)
        .with_constraint_candidates(Vec::new(), vec![relation]);
    assert!(plan_sql_ddl_addition(&actual, &with_activation).is_none());
}
