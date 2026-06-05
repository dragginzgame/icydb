use crate::{
    db::{
        data::{
            decode_runtime_value_from_accepted_field_contract, encode_persisted_scalar_slot_payload,
        },
        schema::{
            AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
            PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, SchemaFieldDefault,
            SchemaFieldSlot, SchemaFieldWritePolicy, SchemaRowLayout, SchemaVersion,
            runtime::{
                AcceptedFieldAbsencePolicy, AcceptedRowDecodeContract,
                AcceptedRowLayoutRuntimeContract, AcceptedRowLayoutRuntimeField,
            },
        },
    },
    model::{
        entity::EntityModel,
        field::{
            FieldInsertGeneration, FieldKind, FieldModel, FieldStorageDecode, FieldWriteManagement,
            LeafCodec, ScalarCodec,
        },
        index::IndexModel,
    },
    testing::entity_model_from_static,
    value::Value,
};

static RUNTIME_ENTITY_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("nickname", FieldKind::Text { max_len: Some(32) }),
];
static RUNTIME_ENTITY_INDEXES: [&IndexModel; 0] = [];
static RUNTIME_ENTITY_MODEL: EntityModel = entity_model_from_static(
    "schema::tests::RuntimeEntity",
    "RuntimeEntity",
    &RUNTIME_ENTITY_FIELDS[0],
    0,
    &RUNTIME_ENTITY_FIELDS,
    &RUNTIME_ENTITY_INDEXES,
);

static WRITE_POLICY_ENTITY_FIELDS: [FieldModel; 3] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated_with_storage_decode_nullability_and_write_policies(
        "token",
        FieldKind::Ulid,
        FieldStorageDecode::ByKind,
        false,
        Some(FieldInsertGeneration::Ulid),
        None,
    ),
    FieldModel::generated_with_storage_decode_nullability_and_write_policies(
        "updated_at",
        FieldKind::Timestamp,
        FieldStorageDecode::ByKind,
        false,
        None,
        Some(FieldWriteManagement::UpdatedAt),
    ),
];
static WRITE_POLICY_ENTITY_INDEXES: [&IndexModel; 0] = [];
static WRITE_POLICY_ENTITY_MODEL: EntityModel = entity_model_from_static(
    "schema::tests::WritePolicyEntity",
    "WritePolicyEntity",
    &WRITE_POLICY_ENTITY_FIELDS[0],
    0,
    &WRITE_POLICY_ENTITY_FIELDS,
    &WRITE_POLICY_ENTITY_INDEXES,
);

fn accepted_schema_fixture() -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::tests::RuntimeEntity".to_string(),
        "RuntimeEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(9)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "nickname".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Text { max_len: Some(32) },
                Vec::new(),
                true,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    ))
}

fn generated_compatible_accepted_schema_fixture() -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::tests::RuntimeEntity".to_string(),
        "RuntimeEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "nickname".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Text { max_len: Some(32) },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    ))
}

fn accepted_composite_primary_key_schema_fixture() -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::tests::RuntimeEntity".to_string(),
        "RuntimeEntity".to_string(),
        vec![FieldId::new(1), FieldId::new(2)],
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "nickname".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Text { max_len: Some(32) },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    ))
}

fn accepted_schema_fixture_with_relation_edge() -> AcceptedSchemaSnapshot {
    let snapshot = accepted_schema_fixture()
        .persisted_snapshot()
        .clone()
        .with_relations(vec![PersistedRelationEdgeSnapshot::new(
            "nickname_owner".to_string(),
            "schema::tests::Owner".to_string(),
            vec![FieldId::new(2)],
        )]);

    AcceptedSchemaSnapshot::new(snapshot)
}

fn generated_slot_compatible_accepted_schema_with_nickname_decode(
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
) -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::tests::RuntimeEntity".to_string(),
        "RuntimeEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "nickname".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Text { max_len: Some(32) },
                Vec::new(),
                nullable,
                SchemaFieldDefault::None,
                storage_decode,
                leaf_codec,
            ),
        ],
    ))
}

fn write_policy_accepted_schema_fixture() -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::tests::WritePolicyEntity".to_string(),
        "WritePolicyEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(2),
                "token".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                SchemaFieldWritePolicy::from_model_policies(
                    Some(FieldInsertGeneration::Ulid),
                    None,
                ),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(3),
                "updated_at".to_string(),
                SchemaFieldSlot::new(2),
                PersistedFieldKind::Timestamp,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                SchemaFieldWritePolicy::from_model_policies(
                    None,
                    Some(FieldWriteManagement::UpdatedAt),
                ),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Timestamp),
            ),
        ],
    ))
}

#[test]
fn accepted_row_layout_runtime_contract_uses_row_layout_slot_authority() {
    let accepted = accepted_schema_fixture();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted runtime contract should build");

    assert_eq!(descriptor.version(), SchemaVersion::initial());
    assert_eq!(descriptor.required_slot_count(), 10);
    assert_eq!(descriptor.first_primary_key_name(), "id");
    assert_eq!(descriptor.primary_key_names(), ["id"]);
    assert_eq!(descriptor.first_primary_key_slot_index(), 0);
    assert_eq!(descriptor.primary_key_slot_indices(), [0]);
    assert_eq!(descriptor.fields().len(), 2);

    let nickname = descriptor
        .fields()
        .iter()
        .find(|field| field.name() == "nickname")
        .expect("nickname field should be present");
    assert_eq!(nickname.field_id(), FieldId::new(2));
    assert_eq!(nickname.slot(), SchemaFieldSlot::new(9));
    assert_eq!(
        nickname.absence_policy(),
        AcceptedFieldAbsencePolicy::NullIfMissing
    );
    assert_eq!(nickname.default(), &SchemaFieldDefault::None);
    let nickname_decode_contract = nickname.decode_contract();
    assert!(nickname_decode_contract.nullable());
    assert_eq!(
        nickname_decode_contract.storage_decode(),
        FieldStorageDecode::ByKind,
    );
    assert_eq!(
        nickname_decode_contract.leaf_codec(),
        LeafCodec::Scalar(ScalarCodec::Text),
    );
    std::assert_matches!(
        nickname.kind(),
        PersistedFieldKind::Text { max_len: Some(32) },
    );
    assert_eq!(
        descriptor
            .field_for_slot(SchemaFieldSlot::new(9))
            .expect("nickname should be indexed by accepted slot")
            .name(),
        "nickname",
    );
    assert_eq!(
        descriptor
            .field_for_id(FieldId::new(2))
            .expect("nickname should be indexed by durable field ID")
            .slot(),
        SchemaFieldSlot::new(9),
    );
    assert_eq!(
        descriptor
            .field_by_name("nickname")
            .expect("nickname should be indexed by persisted field name")
            .field_id(),
        FieldId::new(2),
    );
    assert_eq!(descriptor.field_slot_index_by_name("nickname"), Some(9));
    std::assert_matches!(
        descriptor.field_kind_by_name("nickname"),
        Some(PersistedFieldKind::Text { max_len: Some(32) }),
    );
    assert!(nickname.nested_leaves().is_empty());
    assert!(nickname.nullable());
}

#[test]
fn accepted_row_layout_runtime_contract_exposes_ordered_primary_key_fields() {
    let accepted = accepted_composite_primary_key_schema_fixture();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted composite primary-key schema should build contract");

    assert_eq!(descriptor.first_primary_key_name(), "id");
    assert_eq!(descriptor.primary_key_names(), ["id", "nickname"]);
    assert_eq!(descriptor.first_primary_key_slot_index(), 0);
    assert_eq!(descriptor.primary_key_slot_indices(), [0, 1]);
    let decode_contract = descriptor.row_decode_contract();
    assert_eq!(decode_contract.first_primary_key_slot_index(), 0);
    assert_eq!(decode_contract.primary_key_slot_indices(), [0, 1]);
    assert!(descriptor.is_primary_key_field_name("id"));
    assert!(descriptor.is_primary_key_field_name("nickname"));
    assert!(!descriptor.is_primary_key_field_name("missing"));
    assert_eq!(
        descriptor.primary_key_kinds(),
        [
            &PersistedFieldKind::Ulid,
            &PersistedFieldKind::Text { max_len: Some(32) },
        ],
    );
}

#[test]
fn accepted_row_decode_contract_owns_slot_indexed_field_contracts() {
    let accepted = accepted_schema_fixture();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted runtime contract should build");
    let contract = descriptor.row_decode_contract();
    let nickname = contract
        .field_for_slot(9)
        .expect("nickname field should be available by accepted row slot");

    assert_eq!(contract.required_slot_count(), 10);
    assert_eq!(contract.first_primary_key_slot_index(), 0);
    assert_eq!(contract.primary_key_slot_indices(), [0]);
    assert_eq!(nickname.field_name(), "nickname");
    assert!(
        contract.field_for_slot(1).is_none(),
        "accepted row decode contract should preserve row-layout gaps"
    );
    std::assert_matches!(
        nickname.kind(),
        PersistedFieldKind::Text { max_len: Some(32) },
    );
}

#[test]
fn accepted_row_decode_contract_owns_relation_edge_contracts() {
    let accepted = accepted_schema_fixture_with_relation_edge();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted runtime contract should build");

    assert_eq!(descriptor.relation_edges().len(), 1);
    assert_eq!(descriptor.relation_edges()[0].name(), "nickname_owner");
    assert_eq!(
        descriptor.relation_edges()[0].target_path(),
        "schema::tests::Owner"
    );
    assert_eq!(descriptor.relation_edges()[0].local_field_slots(), &[9]);

    let contract = descriptor.row_decode_contract();
    assert_eq!(contract.relation_edges().len(), 1);
    assert_eq!(contract.relation_edges()[0].name(), "nickname_owner");
    assert_eq!(contract.relation_edges()[0].local_field_slots(), &[9]);
}

#[test]
fn accepted_row_decode_contract_survives_descriptor_borrow_scope() {
    let contract: AcceptedRowDecodeContract = {
        let accepted = generated_compatible_accepted_schema_fixture();
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
            .expect("accepted runtime contract should build");

        descriptor.row_decode_contract()
    };
    assert_eq!(contract.first_primary_key_slot_index(), 0);
    assert_eq!(contract.primary_key_slot_indices(), [0]);
    let nickname_field = contract
        .field_for_slot(1)
        .expect("nickname field should survive as owned accepted contract");
    let raw_value = encode_persisted_scalar_slot_payload(&"Ada".to_string(), "nickname")
        .expect("owned accepted scalar fixture should encode");

    let value = decode_runtime_value_from_accepted_field_contract(
        nickname_field.decode_contract(),
        raw_value.as_slice(),
    )
    .expect("owned accepted field contract should decode outside descriptor borrow scope");

    assert_eq!(value, Value::Text("Ada".to_string()));
}

#[test]
fn accepted_row_layout_runtime_contract_projects_generated_compatibility_proof() {
    let accepted = generated_compatible_accepted_schema_fixture();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("generated-compatible schema should build contract");

    let proof = descriptor
        .generated_row_compatibility_proof_for_model(&RUNTIME_ENTITY_MODEL)
        .expect("matching generated model should produce row compatibility proof");

    assert_eq!(proof.required_slot_count(), 2);
    assert_eq!(proof.first_primary_key_slot_index(), 0);
}

#[test]
fn accepted_row_layout_runtime_contract_builds_descriptor_and_row_compatibility_proof() {
    let accepted = generated_compatible_accepted_schema_fixture();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted schema should build contract");
    let proof = descriptor
        .generated_row_compatibility_proof_for_model(&RUNTIME_ENTITY_MODEL)
        .expect("generated-compatible schema should build row compatibility proof");

    assert_eq!(descriptor.required_slot_count(), 2);
    assert_eq!(descriptor.first_primary_key_slot_index(), 0);
    assert_eq!(descriptor.first_primary_key_name(), "id");
    assert_eq!(descriptor.primary_key_names(), ["id"]);
    assert_eq!(
        descriptor.first_primary_key_kind(),
        &PersistedFieldKind::Ulid
    );
    assert_eq!(proof.required_slot_count(), 2);
    assert_eq!(proof.first_primary_key_slot_index(), 0);
    assert_eq!(
        descriptor.field_slot_index_by_name("nickname"),
        Some(1),
        "checked contract should retain accepted field lookup facts",
    );
    assert_eq!(
        descriptor
            .field_for_slot_index(1)
            .map(AcceptedRowLayoutRuntimeField::name),
        Some("nickname"),
        "checked contract should resolve accepted physical slots by index",
    );
    let nickname_field = descriptor
        .field_by_name("nickname")
        .expect("nickname should resolve accepted runtime contract field");
    assert_eq!(
        nickname_field.write_policy().insert_generation(),
        None,
        "generated-compatible contract should project accepted fields to write-policy facts",
    );
}

#[test]
fn accepted_row_layout_runtime_contract_rejects_primary_key_shape_drift() {
    let accepted = accepted_composite_primary_key_schema_fixture();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted composite primary-key schema should build contract");

    let err = descriptor
        .generated_row_compatibility_proof_for_model(&RUNTIME_ENTITY_MODEL)
        .expect_err("generated-compatible bridge should reject composite/scalar drift");

    assert!(
        err.message
            .contains("accepted row layout primary key is not generated-compatible"),
        "unexpected generated-compatible proof error: {}",
        err.message,
    );
}

#[test]
fn accepted_field_decode_contract_reports_persisted_scalar_field_name() {
    let accepted = generated_compatible_accepted_schema_fixture();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("accepted schema should build contract");
    let nickname_field = descriptor
        .field_by_name("nickname")
        .expect("nickname should resolve accepted runtime contract field");

    // Invalid UTF-8 inside a scalar text envelope should be attributed to
    // the accepted persisted field name, not to a generated placeholder.
    let invalid_text_scalar_payload = [0xFF, 0x01, 0xFF];
    let err = decode_runtime_value_from_accepted_field_contract(
        nickname_field.decode_contract(),
        invalid_text_scalar_payload.as_slice(),
    )
    .expect_err("invalid accepted scalar payload should fail closed");

    assert!(
        err.message.contains("field 'nickname'"),
        "accepted scalar decode should retain field ownership in diagnostics: {}",
        err.message,
    );
    assert!(
        !err.message.contains("accepted field"),
        "accepted scalar decode should not use the old placeholder field name: {}",
        err.message,
    );
}

#[test]
fn accepted_row_layout_runtime_contract_projects_persisted_write_policy() {
    let accepted = write_policy_accepted_schema_fixture();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("write-policy accepted schema should build contract");
    descriptor
        .generated_row_compatibility_proof_for_model(&WRITE_POLICY_ENTITY_MODEL)
        .expect("write-policy schema should remain generated-compatible");

    let token_field = descriptor
        .field_by_name("token")
        .expect("token should resolve accepted runtime contract field");
    let token_policy = token_field.write_policy();
    assert_eq!(
        token_policy.insert_generation(),
        Some(FieldInsertGeneration::Ulid)
    );
    assert_eq!(token_policy.write_management(), None);

    let updated_at_field = descriptor
        .field_by_name("updated_at")
        .expect("updated_at should resolve accepted runtime contract field");
    let updated_at_policy_from_field = updated_at_field.write_policy();
    assert_eq!(
        updated_at_policy_from_field.write_management(),
        Some(FieldWriteManagement::UpdatedAt),
        "contract-owned field projection should avoid name re-resolution",
    );
}

#[test]
fn accepted_row_layout_runtime_contract_rejects_non_generated_compatible_layout() {
    let accepted = accepted_schema_fixture();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("slot-expanded accepted schema should build contract");

    let err = descriptor
        .generated_row_compatibility_proof_for_model(&RUNTIME_ENTITY_MODEL)
        .expect_err("slot-expanded schema must not produce generated-compatible row proof");

    assert!(
        err.message
            .contains("accepted row layout slot is not generated-compatible"),
        "unexpected generated-compatible proof error: {}",
        err.message,
    );
}

#[test]
fn accepted_row_layout_runtime_contract_rejects_extra_generated_field_layout() {
    let mut snapshot = generated_compatible_accepted_schema_fixture()
        .persisted_snapshot()
        .clone();
    let mut fields = snapshot.fields().to_vec();
    fields.push(PersistedFieldSnapshot::new(
        FieldId::new(3),
        "generated_extra".to_string(),
        SchemaFieldSlot::new(2),
        PersistedFieldKind::Text { max_len: None },
        Vec::new(),
        true,
        SchemaFieldDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    ));
    snapshot = PersistedSchemaSnapshot::new(
        snapshot.version(),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        snapshot.first_primary_key_field_id(),
        SchemaRowLayout::new(
            snapshot.row_layout().version(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ],
        ),
        fields,
    );
    let accepted = AcceptedSchemaSnapshot::new(snapshot);
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("extra generated accepted schema should build contract");

    let err = descriptor
        .generated_row_compatibility_proof_for_model(&RUNTIME_ENTITY_MODEL)
        .expect_err("extra generated field must not produce generated-compatible row proof");

    assert!(
        err.message
            .contains("accepted row layout has generated field outside generated model"),
        "unexpected generated-compatible proof error: {}",
        err.message,
    );
}

#[test]
fn accepted_row_layout_runtime_contract_rejects_storage_decode_drift() {
    let accepted = generated_slot_compatible_accepted_schema_with_nickname_decode(
        false,
        FieldStorageDecode::Value,
        LeafCodec::Scalar(ScalarCodec::Text),
    );
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("slot-compatible accepted schema should build contract");

    let err = descriptor
        .generated_row_compatibility_proof_for_model(&RUNTIME_ENTITY_MODEL)
        .expect_err("storage decode drift must reject generated decoder bridge");

    assert!(
        err.message
            .contains("accepted row layout storage decode is not generated-compatible"),
        "unexpected generated-compatible storage decode error: {}",
        err.message,
    );
}

#[test]
fn accepted_row_layout_runtime_contract_rejects_leaf_codec_drift() {
    let accepted = generated_slot_compatible_accepted_schema_with_nickname_decode(
        false,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Blob),
    );
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("slot-compatible accepted schema should build contract");

    let err = descriptor
        .generated_row_compatibility_proof_for_model(&RUNTIME_ENTITY_MODEL)
        .expect_err("leaf codec drift must reject generated decoder bridge");

    assert!(
        err.message
            .contains("accepted row layout leaf codec is not generated-compatible"),
        "unexpected generated-compatible leaf codec error: {}",
        err.message,
    );
}

#[test]
fn accepted_row_layout_runtime_contract_rejects_nullability_drift() {
    let accepted = generated_slot_compatible_accepted_schema_with_nickname_decode(
        true,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    );
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect("slot-compatible accepted schema should build contract");

    let err = descriptor
        .generated_row_compatibility_proof_for_model(&RUNTIME_ENTITY_MODEL)
        .expect_err("nullability drift must reject generated decoder bridge");

    assert!(
        err.message
            .contains("accepted row layout nullability is not generated-compatible"),
        "unexpected generated-compatible nullability error: {}",
        err.message,
    );
}

#[test]
fn accepted_row_layout_runtime_contract_rejects_missing_layout_slot() {
    let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::tests::BrokenEntity".to_string(),
        "BrokenEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "nickname".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Text { max_len: None },
                Vec::new(),
                true,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    ));

    let err = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
        .expect_err("missing row-layout slot should fail closed");
    assert!(
        err.to_string().contains("missing slot for field_id=2"),
        "unexpected descriptor error: {err}",
    );
}
