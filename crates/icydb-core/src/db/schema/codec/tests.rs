use crate::{
    db::schema::{
        AcceptedCheckExprV1, AcceptedConstraintCatalog, AcceptedConstraintKind,
        AcceptedConstraintSnapshot, AcceptedFieldKind, AcceptedSchemaFingerprint,
        ConstraintIdAllocator, ConstraintOrigin, FieldId, PersistedFieldOrigin,
        PersistedFieldSnapshot, PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
        PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId,
        RowLayoutVersion, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaHistoricalFill,
        SchemaIndexId, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
        decode_persisted_schema_snapshot, encode_persisted_schema_snapshot,
    },
    error::{ErrorClass, ErrorOrigin},
    model::field::{
        FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec, ScalarCodec,
    },
    types::EntityTag,
};

fn encode_unchecked_schema_fixture(snapshot: &PersistedSchemaSnapshot) -> Vec<u8> {
    candid::encode_one(super::PersistedSchemaSnapshotWire::from_snapshot(snapshot))
        .expect("unchecked schema wire fixture should encode")
}

#[test]
fn decode_persisted_schema_snapshot_rejects_future_codec_version() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::FutureCodec".to_string(),
        "FutureCodec".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(Vec::new()),
        Vec::new(),
    );
    let mut wire = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    wire.codec_version = super::SCHEMA_SNAPSHOT_CODEC_VERSION.saturating_add(1);
    let encoded = candid::encode_one(&wire).expect("future schema codec fixture should encode");

    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("future schema codec version must fail closed");

    assert_eq!(error.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(error.origin(), ErrorOrigin::Serialize);
}

#[test]
fn decode_persisted_schema_snapshot_rejects_wrong_contract_profile() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::WrongProfile".to_string(),
        "WrongProfile".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(Vec::new()),
        Vec::new(),
    );
    let mut wire = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    wire.contract_profile ^= 1;
    let encoded = candid::encode_one(&wire).expect("wrong schema profile fixture should encode");

    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("wrong schema contract profile must fail closed");

    assert_eq!(error.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(error.origin(), ErrorOrigin::Serialize);
}

#[test]
fn decode_persisted_schema_snapshot_rejects_zero_schema_version() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::new(0),
        "entities::ZeroVersion".to_string(),
        "ZeroVersion".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(Vec::new()),
        Vec::new(),
    );
    let encoded = encode_unchecked_schema_fixture(&snapshot);

    let err = decode_persisted_schema_snapshot(&encoded)
        .expect_err("decode should reject version-zero schema snapshots");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
        "schema codec should hard-cut non-positive persisted schema versions"
    );
}

#[test]
fn persisted_schema_snapshot_round_trips_temporal_layout_facts() {
    let snapshot = temporal_schema_snapshot();
    let catalog = AcceptedConstraintCatalog::from_persisted_parts(
        ConstraintIdAllocator::new(7),
        snapshot.constraints().to_vec(),
        Vec::new(),
    );
    let snapshot = snapshot.with_constraint_catalog(catalog);
    let current = snapshot.row_layout().current_version();
    let historical_payload = snapshot.fields()[1]
        .historical_fill()
        .slot_payload()
        .expect("temporal fixture should carry a historical payload")
        .to_vec();
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("temporal schema snapshot should encode");

    let decoded =
        decode_persisted_schema_snapshot(&encoded).expect("temporal schema snapshot should decode");

    assert_eq!(decoded.row_layout().current_version(), current);
    assert_eq!(
        decoded.row_layout().history_floor(),
        RowLayoutVersion::INITIAL
    );
    assert_eq!(decoded.fields()[1].introduced_in_layout(), current);
    assert_eq!(decoded.constraint_id_allocator().high_water(), 7);
    assert_eq!(decoded.constraints(), snapshot.constraints());
    assert_eq!(
        decoded.fields()[1].historical_fill().slot_payload(),
        Some(historical_payload.as_slice())
    );
}

#[test]
fn persisted_schema_snapshot_rejects_missing_structural_constraint() {
    let snapshot = temporal_schema_snapshot();
    let catalog = AcceptedConstraintCatalog::from_persisted_parts(
        snapshot.constraint_id_allocator(),
        snapshot.constraints()[1..].to_vec(),
        Vec::new(),
    );
    let malformed = snapshot.with_constraint_catalog(catalog);
    assert!(
        encode_persisted_schema_snapshot(&malformed).is_err(),
        "typed codec egress must reject an incomplete structural registry",
    );
    let encoded = encode_unchecked_schema_fixture(&malformed);

    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("missing primary-key constraint must fail closed");

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
    );
}

#[test]
fn persisted_schema_snapshot_rejects_orphan_structural_constraint_reference() {
    let snapshot = temporal_schema_snapshot();
    let mut constraints = snapshot.constraints().to_vec();
    let not_null_position = constraints
        .iter()
        .position(|constraint| matches!(constraint.kind(), AcceptedConstraintKind::NotNull { .. }))
        .expect("temporal fixture should contain a not-null constraint");
    let current = &constraints[not_null_position];
    constraints[not_null_position] = AcceptedConstraintSnapshot::new(
        current.id(),
        current.name().to_string(),
        current.origin(),
        AcceptedConstraintKind::NotNull {
            field_id: FieldId::new(999),
        },
    );
    let catalog = AcceptedConstraintCatalog::from_persisted_parts(
        snapshot.constraint_id_allocator(),
        constraints,
        Vec::new(),
    );
    let malformed = snapshot.with_constraint_catalog(catalog);
    assert!(
        encode_persisted_schema_snapshot(&malformed).is_err(),
        "typed codec egress must reject an orphan structural owner reference",
    );
    let encoded = encode_unchecked_schema_fixture(&malformed);

    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("orphan not-null constraint reference must fail closed");

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
    );
}

fn assert_structural_constraint_wire_rejects(wire: super::PersistedSchemaSnapshotWire) {
    let encoded = candid::encode_one(&wire).expect("malformed constraint wire should encode");
    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("malformed structural constraint catalog must fail closed");

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
    );
}

#[test]
fn persisted_schema_snapshot_rejects_malformed_constraint_identity_and_metadata() {
    let mut zero_id =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    zero_id.constraints[0].id = 0;
    assert_structural_constraint_wire_rejects(zero_id);

    let mut allocator_below_id =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    allocator_below_id.constraint_id_high_water = 1;
    assert_structural_constraint_wire_rejects(allocator_below_id);

    let mut duplicate_id =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    duplicate_id.constraints[1].id = duplicate_id.constraints[0].id;
    assert_structural_constraint_wire_rejects(duplicate_id);

    let mut duplicate_name =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    duplicate_name.constraints[1].name = duplicate_name.constraints[0].name.clone();
    assert_structural_constraint_wire_rejects(duplicate_name);

    let mut invalid_name =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    invalid_name.constraints[0].name = "invalid name".to_string();
    assert_structural_constraint_wire_rejects(invalid_name);

    let mut overlong_name =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    overlong_name.constraints[0].name = "a".repeat(257);
    assert_structural_constraint_wire_rejects(overlong_name);

    let mut wrong_origin =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    wrong_origin.constraints[1].origin = super::ConstraintOriginWire::SqlDdl;
    assert_structural_constraint_wire_rejects(wrong_origin);
}

fn snapshot_with_true_check() -> PersistedSchemaSnapshot {
    let snapshot = temporal_schema_snapshot();
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check(
            "score_policy".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
        )
        .expect("test check should allocate");
    snapshot.with_constraint_catalog(catalog)
}

#[test]
fn persisted_schema_snapshot_round_trips_current_check_expression() {
    let snapshot = snapshot_with_true_check();
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("current check expression should encode");
    let decoded =
        decode_persisted_schema_snapshot(&encoded).expect("current check expression should decode");

    assert_eq!(decoded, snapshot);
}

fn snapshot_with_check_activation() -> PersistedSchemaSnapshot {
    let snapshot = temporal_schema_snapshot();
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check_activation(
            "pending_score_policy".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            7,
        )
        .expect("test activation should reserve identity");
    snapshot.with_constraint_catalog(catalog)
}

#[test]
fn persisted_schema_snapshot_round_trips_current_check_activation() {
    let snapshot = snapshot_with_check_activation();
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("current check activation should encode");
    let decoded =
        decode_persisted_schema_snapshot(&encoded).expect("current check activation should decode");

    assert_eq!(decoded, snapshot);
    assert_eq!(decoded.constraint_activations().len(), 1);
}

#[test]
fn persisted_schema_snapshot_round_trips_planner_invisible_unique_candidate() {
    let snapshot = temporal_schema_snapshot();
    let candidate = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).expect("test index identity should be non-zero"),
        1,
        "unique_score".to_string(),
        "entities::Temporal::unique_score".to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["score".to_string()],
            AcceptedFieldKind::Nat64,
            false,
        )]),
        None,
    )
    .clone_with_schema_identity(
        SchemaIndexId::new(1).expect("test index identity should be non-zero"),
        1,
        8,
    );
    let snapshot = snapshot
        .with_added_unique_activation(candidate, AcceptedSchemaFingerprint::new([0xB7; 32]), 8)
        .expect("unique candidate activation should close");

    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("unique candidate activation should encode");
    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("unique candidate activation should decode");

    assert_eq!(decoded, snapshot);
    assert!(decoded.indexes().is_empty());
    assert_eq!(decoded.candidate_indexes().len(), 1);

    let mut missing_owner = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    missing_owner.candidate_indexes.clear();
    assert_structural_constraint_wire_rejects(missing_owner);
}

#[test]
fn persisted_schema_snapshot_rejects_stale_or_unbound_activation_identity() {
    let snapshot = snapshot_with_check_activation();

    let mut stale_fingerprint = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    stale_fingerprint.activations[0].fingerprint[0] ^= 1;
    assert_structural_constraint_wire_rejects(stale_fingerprint);

    let mut zero_epoch = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    zero_epoch.activations[0].activation_epoch = 0;
    assert_structural_constraint_wire_rejects(zero_epoch);

    let mut duplicate_name = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    duplicate_name.activations[0].name = duplicate_name.constraints[0].name.clone();
    assert_structural_constraint_wire_rejects(duplicate_name);
}

#[test]
fn persisted_schema_snapshot_rejects_invalid_check_field_and_noncanonical_boolean() {
    let snapshot = snapshot_with_true_check();
    let mut unknown_field = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    let Some(check) = unknown_field.constraints.last_mut() else {
        panic!("test check wire should exist");
    };
    check.kind = super::AcceptedConstraintKindWire::Check {
        expression: Box::new(super::AcceptedCheckExprV1Wire::IsNull(
            super::AcceptedCheckValueExprV1Wire::Field(999),
        )),
    };
    assert_structural_constraint_wire_rejects(unknown_field);

    let mut noncanonical = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    let Some(check) = noncanonical.constraints.last_mut() else {
        panic!("test check wire should exist");
    };
    check.kind = super::AcceptedConstraintKindWire::Check {
        expression: Box::new(super::AcceptedCheckExprV1Wire::And(vec![
            super::AcceptedCheckExprV1Wire::True,
        ])),
    };
    assert_structural_constraint_wire_rejects(noncanonical);
}

fn temporal_schema_snapshot() -> PersistedSchemaSnapshot {
    let current = RowLayoutVersion::INITIAL
        .checked_next()
        .expect("test layout should advance");
    let historical_payload = vec![0x10, 0x20];
    PersistedSchemaSnapshot::new(
        SchemaVersion::new(2),
        "entities::Temporal".to_string(),
        "Temporal".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            current,
            RowLayoutVersion::INITIAL,
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(2),
                "score".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                current,
                SchemaInsertDefault::SlotPayload(vec![0x30]),
                SchemaHistoricalFill::SlotPayload(historical_payload),
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ],
    )
}

fn assert_temporal_schema_wire_rejects(wire: super::PersistedSchemaSnapshotWire) {
    let encoded = candid::encode_one(&wire).expect("invalid temporal schema fixture should encode");
    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("invalid temporal schema metadata must fail closed");

    assert_eq!(error.class(), ErrorClass::Corruption);
}

#[test]
fn persisted_schema_snapshot_rejects_invalid_layout_version_ranges() {
    let mut zero_current =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    zero_current.row_layout.current_version = 0;
    assert_temporal_schema_wire_rejects(zero_current);

    let mut zero_floor =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    zero_floor.row_layout.history_floor = 0;
    assert_temporal_schema_wire_rejects(zero_floor);

    let mut inverted =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    inverted.row_layout.history_floor = inverted.row_layout.current_version.saturating_add(1);
    assert_temporal_schema_wire_rejects(inverted);

    let mut future_introduction =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    future_introduction.fields[1].introduced_in_layout = future_introduction
        .row_layout
        .current_version
        .saturating_add(1);
    assert_temporal_schema_wire_rejects(future_introduction);
}

#[test]
fn persisted_schema_snapshot_rejects_open_or_overclosed_history() {
    let mut open_history =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    open_history.fields[1].historical_fill = super::SchemaHistoricalFillWire::Reject;
    assert_temporal_schema_wire_rejects(open_history);

    let mut overclosed_history =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    overclosed_history.row_layout.history_floor = overclosed_history.row_layout.current_version;
    assert_temporal_schema_wire_rejects(overclosed_history);

    let mut invalid_null_fill =
        super::PersistedSchemaSnapshotWire::from_snapshot(&temporal_schema_snapshot());
    invalid_null_fill.fields[1].historical_fill = super::SchemaHistoricalFillWire::Null;
    assert_temporal_schema_wire_rejects(invalid_null_fill);
}

#[test]
fn decode_persisted_schema_snapshot_rejects_fragmented_field_identities() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::FragmentedFields".to_string(),
        "FragmentedFields".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(3), SchemaFieldSlot::new(2)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(3),
                "email".to_string(),
                SchemaFieldSlot::new(2),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
    );
    let encoded = encode_unchecked_schema_fixture(&snapshot);

    let err = decode_persisted_schema_snapshot(&encoded)
        .expect_err("decode should reject fragmented field IDs and slots");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
    );
}

#[test]
fn decode_persisted_schema_snapshot_rejects_fragmented_index_ordinals() {
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "entities::FragmentedIndexes".to_string(),
        "FragmentedIndexes".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "email".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(2).expect("test index identity should be non-zero"),
            2,
            "idx_fragmented_indexes__email".to_string(),
            "fragmented_indexes::email".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["email".to_string()],
                AcceptedFieldKind::Text { max_len: None },
                false,
            )]),
            None,
        )],
    );
    let encoded = encode_unchecked_schema_fixture(&snapshot);

    let err = decode_persisted_schema_snapshot(&encoded)
        .expect_err("decode should reject fragmented index ordinals");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
    );
}

#[test]
fn persisted_schema_snapshot_round_trips_field_write_policy() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::WritePolicy".to_string(),
        "WritePolicy".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::from_model_policies(
                    Some(FieldInsertGeneration::Ulid),
                    None,
                ),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(2),
                "updated_at".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Timestamp,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
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
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial_with_write_policy_and_origin(
                FieldId::new(2),
                "nickname".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                true,
                SchemaInsertDefault::None,
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
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial_with_write_policy(
            FieldId::new(1),
            "score".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::SlotPayload(default_payload.clone()),
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
        decoded.fields()[0].insert_default().slot_payload(),
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
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "signed".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::IntBig { max_bytes: 384 },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "unsigned".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::NatBig { max_bytes: 512 },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ),
        ],
    );

    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode bounded big integers");
    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode bounded big integers");

    assert_eq!(
        decoded.fields()[0].kind(),
        &AcceptedFieldKind::IntBig { max_bytes: 384 },
    );
    assert_eq!(
        decoded.fields()[1].kind(),
        &AcceptedFieldKind::NatBig { max_bytes: 512 },
    );
}

#[test]
fn persisted_schema_snapshot_round_trips_ordered_primary_key_field_ids() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::CompositeKeyed".to_string(),
        "CompositeKeyed".to_string(),
        vec![FieldId::new(1), FieldId::new(3)],
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
            (FieldId::new(3), SchemaFieldSlot::new(2)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "tenant_id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "name".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(3),
                "local_id".to_string(),
                SchemaFieldSlot::new(2),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
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
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(2),
                "email".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
            1,
            "idx_indexed__email".to_string(),
            "indexed::email".to_string(),
            true,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["email".to_string()],
                AcceptedFieldKind::Text { max_len: None },
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
    assert_eq!(index.schema_id().get(), 1);
    assert_eq!(index.ordinal(), 1);
    assert_eq!(index.name(), "idx_indexed__email");
    assert_eq!(index.store(), "indexed::email");
    assert!(index.unique());
    assert_eq!(index.predicate_sql(), Some("email IS NOT NULL"));
    assert_eq!(index.key().field_paths()[0].field_id(), FieldId::new(2));
    assert_eq!(index.key().field_paths()[0].slot(), SchemaFieldSlot::new(1));
    assert_eq!(index.key().field_paths()[0].path(), &["email".to_string()]);

    let mut zero_identity = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    zero_identity.indexes[0].schema_id = 0;
    let encoded = candid::encode_one(&zero_identity)
        .expect("zero logical index identity fixture should encode");
    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("zero logical index identity must fail closed");
    assert_eq!(error.class(), ErrorClass::Corruption);

    let duplicate = snapshot.indexes()[0]
        .clone_with_dense_identities(2, |field_id, slot| Some((field_id, slot)))
        .expect("test index should support physical ordinal compaction");
    let mut duplicate_identity = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    duplicate_identity
        .indexes
        .push(super::PersistedIndexSnapshotWire::from_index(&duplicate));
    let encoded = candid::encode_one(&duplicate_identity)
        .expect("duplicate logical index identity fixture should encode");
    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("duplicate logical index identity must fail closed");
    assert_eq!(error.class(), ErrorClass::Corruption);
}

#[test]
fn persisted_schema_snapshot_round_trips_relation_edges() {
    let relation_kind = AcceptedFieldKind::Relation {
        target_path: "entities::Owner".to_string(),
        target_entity_name: "Owner".to_string(),
        target_entity_tag: EntityTag::new(7),
        target_store_path: "stores::Owner".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Ulid),
    };
    let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        SchemaVersion::initial(),
        "entities::Related".to_string(),
        "Related".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(2),
                "owner_id".to_string(),
                SchemaFieldSlot::new(1),
                relation_kind.clone(),
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
        ],
        Vec::new(),
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        RelationId::new(1).expect("test relation identity should be non-zero"),
        "owner".to_string(),
        "entities::Owner".to_string(),
        vec![FieldId::new(2)],
    )]);
    let constraint_catalog = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .expect("test relation constraint catalog should build");
    let snapshot = snapshot.with_constraint_catalog(constraint_catalog);
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("schema snapshot should encode accepted relation contracts");

    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("schema snapshot should decode accepted relation contracts");

    assert_eq!(decoded.relations().len(), 1);
    let relation = &decoded.relations()[0];
    assert_eq!(relation.id().get(), 1);
    assert_eq!(relation.name(), "owner");
    assert_eq!(relation.target_path(), "entities::Owner");
    assert_eq!(relation.local_field_ids(), &[FieldId::new(2)]);
    assert_eq!(decoded.fields()[1].kind(), &relation_kind);

    let mut zero_identity = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    zero_identity.relations[0].relation_id = 0;
    let encoded = candid::encode_one(&zero_identity)
        .expect("zero logical relation identity fixture should encode");
    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("zero logical relation identity must fail closed");
    assert_eq!(error.class(), ErrorClass::Corruption);

    let duplicate = PersistedRelationEdgeSnapshot::new(
        relation.id(),
        "secondary_owner".to_string(),
        "entities::SecondaryOwner".to_string(),
        vec![FieldId::new(2)],
    );
    let mut duplicate_identity = super::PersistedSchemaSnapshotWire::from_snapshot(&snapshot);
    duplicate_identity
        .relations
        .push(super::PersistedRelationEdgeSnapshotWire::from_relation(
            &duplicate,
        ));
    let encoded = candid::encode_one(&duplicate_identity)
        .expect("duplicate logical relation identity fixture should encode");
    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("duplicate logical relation identity must fail closed");
    assert_eq!(error.class(), ErrorClass::Corruption);
}

#[test]
fn persisted_schema_snapshot_round_trips_expression_indexes() {
    let source = PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["email".to_string()],
        AcceptedFieldKind::Text { max_len: None },
        false,
    );
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "entities::ExpressionIndexed".to_string(),
        "ExpressionIndexed".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(2),
                "email".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
            1,
            "idx_expression_indexed__lower_email".to_string(),
            "expression_indexed::lower_email".to_string(),
            true,
            PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
                Box::new(PersistedIndexExpressionSnapshot::new(
                    PersistedIndexExpressionOp::Lower,
                    source,
                    AcceptedFieldKind::Text { max_len: None },
                    AcceptedFieldKind::Text { max_len: None },
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
