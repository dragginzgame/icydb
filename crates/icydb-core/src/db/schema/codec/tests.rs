use super::{PersistedSchemaSnapshotWire, SCHEMA_SNAPSHOT_CODEC_VERSION, SchemaRowLayoutWire};
use crate::{
    db::schema::{
        FieldId, PersistedFieldKind, PersistedFieldOrigin, PersistedFieldSnapshot,
        PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
        PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot,
        SchemaFieldDefault, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaRowLayout,
        SchemaVersion, decode_persisted_schema_snapshot, encode_persisted_schema_snapshot,
    },
    model::field::{
        FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec, ScalarCodec,
    },
};
use candid::Encode;

#[test]
fn decode_persisted_schema_snapshot_rejects_obsolete_codec_without_version_inference() {
    let wire = PersistedSchemaSnapshotWire {
        codec_version: SCHEMA_SNAPSHOT_CODEC_VERSION.saturating_sub(1),
        version: SchemaVersion::initial().get(),
        entity_path: "entities::Obsolete".to_string(),
        entity_name: "Obsolete".to_string(),
        primary_key_field_ids: Vec::new(),
        row_layout: SchemaRowLayoutWire {
            version: SchemaVersion::initial().get(),
            field_to_slot: Vec::new(),
            retired_field_slots: Vec::new(),
        },
        fields: Vec::new(),
        indexes: Vec::new(),
        relations: Vec::new(),
    };
    let encoded =
        Encode!(&wire).expect("obsolete schema snapshot fixture should still Candid-encode");

    let err = decode_persisted_schema_snapshot(&encoded)
        .expect_err("obsolete schema snapshot codec should hard-cut");

    assert!(
        err.message()
            .contains("unsupported persisted schema snapshot codec version"),
        "obsolete schema snapshots should fail clearly before schema_version inference"
    );
}

#[test]
fn decode_persisted_schema_snapshot_rejects_zero_schema_version() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::new(0),
        "entities::ZeroVersion".to_string(),
        "ZeroVersion".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(SchemaVersion::new(0), Vec::new()),
        Vec::new(),
    );
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("version-zero schema snapshot should encode for decode-boundary coverage");

    let err = decode_persisted_schema_snapshot(&encoded)
        .expect_err("decode should reject version-zero schema snapshots");

    assert!(
        err.message()
            .contains("persisted schema snapshot schema_version must be positive"),
        "schema codec should hard-cut non-positive persisted schema versions"
    );
}

#[test]
fn decode_persisted_schema_snapshot_rejects_snapshot_layout_version_mismatch() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::new(2),
        "entities::Mismatch".to_string(),
        "Mismatch".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(SchemaVersion::initial(), Vec::new()),
        Vec::new(),
    );
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode for decode-boundary coverage");

    let err = decode_persisted_schema_snapshot(&encoded)
        .expect_err("decode should reject mismatched snapshot/layout versions");

    assert!(
        err.message()
            .contains("persisted schema snapshot row-layout version mismatch"),
        "schema codec should report the decoded version invariant"
    );
}

#[test]
fn persisted_schema_snapshot_round_trips_field_write_policy() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::WritePolicy".to_string(),
        "WritePolicy".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
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
                FieldId::new(2),
                "updated_at".to_string(),
                SchemaFieldSlot::new(1),
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
    );
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode persisted write policy");

    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode persisted write policy");

    assert_eq!(
        decoded.fields()[0].write_policy().insert_generation(),
        Some(FieldInsertGeneration::Ulid),
        "insert generation should survive schema snapshot round-trip",
    );
    assert_eq!(
        decoded.fields()[1].write_policy().write_management(),
        Some(FieldWriteManagement::UpdatedAt),
        "managed write policy should survive schema snapshot round-trip",
    );
}

#[test]
fn persisted_schema_snapshot_round_trips_field_origin() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::FieldOrigin".to_string(),
        "FieldOrigin".to_string(),
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
            PersistedFieldSnapshot::new_with_write_policy_and_origin(
                FieldId::new(2),
                "nickname".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Text { max_len: None },
                Vec::new(),
                true,
                SchemaFieldDefault::None,
                SchemaFieldWritePolicy::none(),
                PersistedFieldOrigin::SqlDdl,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    );
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode field origin");

    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode field origin");

    assert_eq!(
        decoded.fields()[0].origin(),
        PersistedFieldOrigin::Generated,
        "generated field origin should survive schema snapshot round-trip",
    );
    assert_eq!(
        decoded.fields()[1].origin(),
        PersistedFieldOrigin::SqlDdl,
        "DDL field origin should survive schema snapshot round-trip",
    );
}

#[test]
fn persisted_schema_snapshot_round_trips_encoded_default_payload() {
    let default_payload = vec![0x01, 0x02, 0x03];
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::DefaultPayload".to_string(),
        "DefaultPayload".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
        ),
        vec![PersistedFieldSnapshot::new_with_write_policy(
            FieldId::new(1),
            "score".to_string(),
            SchemaFieldSlot::new(0),
            PersistedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaFieldDefault::SlotPayload(default_payload.clone()),
            SchemaFieldWritePolicy::none(),
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )],
    );
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode persisted default payload");

    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode persisted default payload");

    assert_eq!(
        decoded.fields()[0].default().slot_payload(),
        Some(default_payload.as_slice())
    );
}

#[test]
fn persisted_schema_snapshot_round_trips_big_integer_max_bytes_contracts() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::BigNumbers".to_string(),
        "BigNumbers".to_string(),
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
                "signed".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::IntBig { max_bytes: 384 },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::StructuralFallback,
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "unsigned".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::NatBig { max_bytes: 512 },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::StructuralFallback,
            ),
        ],
    );

    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode bounded big integers");
    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode bounded big integers");

    assert_eq!(
        decoded.fields()[0].kind(),
        &PersistedFieldKind::IntBig { max_bytes: 384 },
    );
    assert_eq!(
        decoded.fields()[1].kind(),
        &PersistedFieldKind::NatBig { max_bytes: 512 },
    );
}

#[test]
fn persisted_schema_snapshot_round_trips_ordered_primary_key_field_ids() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::CompositeKeyed".to_string(),
        "CompositeKeyed".to_string(),
        vec![FieldId::new(1), FieldId::new(3)],
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
                "tenant_id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "name".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(3),
                "local_id".to_string(),
                SchemaFieldSlot::new(2),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
        ],
    );
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode ordered primary-key fields");

    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode ordered primary-key fields");

    assert_eq!(
        decoded.primary_key_field_ids(),
        &[FieldId::new(1), FieldId::new(3)],
        "accepted schema codec must preserve composite primary-key arity and order",
    );
    assert_eq!(
        decoded.first_primary_key_field_id(),
        FieldId::new(1),
        "first-primary-key-field helper remains explicitly first-component only",
    );
}

#[test]
fn persisted_schema_snapshot_round_trips_field_path_indexes() {
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "entities::Indexed".to_string(),
        "Indexed".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(2),
                "email".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            7,
            "idx_indexed__email".to_string(),
            "indexed::email".to_string(),
            true,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["email".to_string()],
                PersistedFieldKind::Text { max_len: None },
                false,
            )]),
            Some("email IS NOT NULL".to_string()),
        )],
    );
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode accepted index contracts");

    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode accepted index contracts");

    assert_eq!(decoded.indexes().len(), 1);
    let index = &decoded.indexes()[0];
    assert_eq!(index.ordinal(), 7);
    assert_eq!(index.name(), "idx_indexed__email");
    assert_eq!(index.store(), "indexed::email");
    assert!(index.unique());
    assert_eq!(index.predicate_sql(), Some("email IS NOT NULL"));
    assert_eq!(index.key().field_paths()[0].field_id(), FieldId::new(2));
    assert_eq!(index.key().field_paths()[0].slot(), SchemaFieldSlot::new(1));
    assert_eq!(index.key().field_paths()[0].path(), &["email".to_string()]);
}

#[test]
fn persisted_schema_snapshot_round_trips_relation_edges() {
    let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        SchemaVersion::initial(),
        "entities::Related".to_string(),
        "Related".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(2),
                "owner_id".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
        ],
        Vec::new(),
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        "owner".to_string(),
        "entities::Owner".to_string(),
        vec![FieldId::new(2)],
    )]);
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode accepted relation contracts");

    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode accepted relation contracts");

    assert_eq!(decoded.relations().len(), 1);
    let relation = &decoded.relations()[0];
    assert_eq!(relation.name(), "owner");
    assert_eq!(relation.target_path(), "entities::Owner");
    assert_eq!(relation.local_field_ids(), &[FieldId::new(2)]);
}

#[test]
fn persisted_schema_snapshot_round_trips_expression_indexes() {
    let source = PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["email".to_string()],
        PersistedFieldKind::Text { max_len: None },
        false,
    );
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "entities::ExpressionIndexed".to_string(),
        "ExpressionIndexed".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(2),
                "email".to_string(),
                SchemaFieldSlot::new(1),
                PersistedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            8,
            "idx_expression_indexed__lower_email".to_string(),
            "expression_indexed::lower_email".to_string(),
            true,
            PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
                Box::new(PersistedIndexExpressionSnapshot::new(
                    PersistedIndexExpressionOp::Lower,
                    source,
                    PersistedFieldKind::Text { max_len: None },
                    PersistedFieldKind::Text { max_len: None },
                    "expr:v1:LOWER(email)".to_string(),
                )),
            )]),
            None,
        )],
    );
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode accepted expression index contracts");

    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode accepted expression index contracts");

    let PersistedIndexKeySnapshot::Items(items) = decoded.indexes()[0].key() else {
        panic!("expression index should decode as explicit accepted key items");
    };
    let PersistedIndexKeyItemSnapshot::Expression(expression) = &items[0] else {
        panic!("expression key item should decode as an accepted expression");
    };
    assert_eq!(expression.op(), PersistedIndexExpressionOp::Lower);
    assert_eq!(expression.source().field_id(), FieldId::new(2));
    assert_eq!(expression.canonical_text(), "expr:v1:LOWER(email)");
}
