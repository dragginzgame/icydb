use std::mem::size_of;

use crate::{
    db::schema::{
        AcceptedCheckExprV1, AcceptedCheckLiteralV1, AcceptedCheckValueExprV1,
        AcceptedConstraintCatalog, AcceptedConstraintKind, AcceptedConstraintSnapshot,
        AcceptedFieldKind, AcceptedNamedTypeIdentity, AcceptedRuleOperation, AcceptedRuleTarget,
        AcceptedSchemaFingerprint, ConstraintActivationKind, ConstraintIdAllocator,
        ConstraintOrigin, FieldId, FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement,
        LeafCodec, MAX_ACCEPTED_RECURSIVE_DEPTH, MAX_SCHEMA_SNAPSHOT_BYTES, PersistedFieldOrigin,
        PersistedFieldSnapshot, PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
        PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId,
        RelationIdAllocator, RowLayoutVersion, ScalarCodec, SchemaFieldSlot,
        SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaIndexId, SchemaInsertDefault,
        SchemaRowLayout, SchemaVersion, accepted_schema_cache_fingerprint_for_persisted_snapshot,
        composite_catalog::CompositeTypeId, decode_persisted_schema_snapshot,
        encode_persisted_schema_snapshot,
    },
    error::{ErrorClass, ErrorOrigin},
    types::EntityTag,
};

fn encode_unchecked_schema_fixture(snapshot: &PersistedSchemaSnapshot) -> Vec<u8> {
    super::encode_unchecked_persisted_schema_snapshot_for_tests(snapshot)
        .expect("unchecked schema fixture should encode")
}

#[test]
fn persisted_schema_snapshot_codec_enforces_shared_byte_bound_before_decode() {
    super::reset_persisted_schema_snapshot_decode_count_for_tests();

    let oversized = vec![0_u8; MAX_SCHEMA_SNAPSHOT_BYTES as usize + 1];
    let error = decode_persisted_schema_snapshot(&oversized)
        .expect_err("oversized schema snapshot must reject before decoding");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
    assert_eq!(
        super::persisted_schema_snapshot_decode_count_for_tests(),
        0,
        "oversized bytes must not enter schema decoding",
    );

    let bounded_malformed = vec![0_u8; MAX_SCHEMA_SNAPSHOT_BYTES as usize];
    let error = decode_persisted_schema_snapshot(&bounded_malformed)
        .expect_err("bounded malformed bytes must still fail closed");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
    assert_eq!(
        super::persisted_schema_snapshot_decode_count_for_tests(),
        1,
        "the exact byte boundary may enter schema decoding",
    );

    let oversized_snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "x".repeat(MAX_SCHEMA_SNAPSHOT_BYTES as usize),
        "Oversized".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(Vec::new()),
        Vec::new(),
    );
    let error = super::encode_unchecked_persisted_schema_snapshot_for_tests(&oversized_snapshot)
        .expect_err("oversized schema snapshot must reject before emission");
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), ErrorOrigin::Store);
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
    let mut encoded = encode_unchecked_schema_fixture(&snapshot);
    encoded[super::SCHEMA_SNAPSHOT_MAGIC.len()] =
        super::SCHEMA_SNAPSHOT_FORMAT_VERSION.saturating_add(1);

    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("future schema codec version must fail closed");

    assert_eq!(error.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(error.origin(), ErrorOrigin::Serialize);
}

#[test]
fn decode_persisted_schema_snapshot_rejects_corrupt_format_magic() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::WrongProfile".to_string(),
        "WrongProfile".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(Vec::new()),
        Vec::new(),
    );
    let mut encoded = encode_unchecked_schema_fixture(&snapshot);
    encoded[0] ^= 0xff;

    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("corrupt schema format magic must fail closed");

    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
}

#[test]
fn persisted_schema_snapshot_rejects_unknown_tags_and_trailing_bytes() {
    let mut kind_reader = super::SnapshotReader::new(&[u8::MAX]);
    let kind_error = super::field::decode_kind(&mut kind_reader, 0)
        .expect_err("unknown field-kind tags must fail closed");
    assert_eq!(kind_error.class(), ErrorClass::Corruption);

    let mut encoded = encode_persisted_schema_snapshot(&temporal_schema_snapshot())
        .expect("current snapshot should encode");
    encoded.push(0);
    let trailing_error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("trailing snapshot bytes must fail closed");
    assert_eq!(trailing_error.class(), ErrorClass::Corruption);
}

#[test]
fn persisted_schema_snapshot_encoding_is_canonical() {
    const TEMPORAL_SCHEMA_V1: &[u8] = &[
        73, 67, 89, 85, 83, 78, 80, 0, 1, 0, 0, 0, 2, 0, 0, 0, 18, 101, 110, 116, 105, 116, 105,
        101, 115, 58, 58, 84, 101, 109, 112, 111, 114, 97, 108, 0, 0, 0, 8, 84, 101, 109, 112, 111,
        114, 97, 108, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0,
        0, 0, 0, 2, 0, 1, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 19, 95, 95, 105, 99, 121,
        100, 98, 95, 112, 114, 105, 109, 97, 114, 121, 95, 107, 101, 121, 1, 1, 0, 0, 0, 2, 0, 0,
        0, 18, 95, 95, 105, 99, 121, 100, 98, 95, 110, 111, 116, 95, 110, 117, 108, 108, 95, 49, 1,
        2, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 18, 95, 95, 105, 99, 121, 100, 98, 95, 110, 111, 116,
        95, 110, 117, 108, 108, 95, 50, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0,
        0, 2, 105, 100, 0, 0, 26, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 13, 0, 0, 0, 2,
        0, 0, 0, 5, 115, 99, 111, 114, 101, 0, 1, 23, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 1, 48,
        2, 0, 0, 0, 2, 16, 32, 0, 0, 1, 1, 1, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];

    let snapshot = temporal_schema_snapshot();
    let encoded =
        encode_persisted_schema_snapshot(&snapshot).expect("current snapshot should encode");
    assert!(encoded.starts_with(&super::SCHEMA_SNAPSHOT_MAGIC));
    assert_eq!(
        encoded[super::SCHEMA_SNAPSHOT_MAGIC.len()],
        super::SCHEMA_SNAPSHOT_FORMAT_VERSION,
    );
    assert_eq!(encoded, TEMPORAL_SCHEMA_V1);

    let decoded =
        decode_persisted_schema_snapshot(&encoded).expect("current snapshot should decode");
    let reencoded =
        encode_persisted_schema_snapshot(&decoded).expect("decoded snapshot should re-encode");
    assert_eq!(reencoded, encoded);
}

#[test]
fn persisted_schema_snapshot_rejects_truncation_and_deceptive_lengths() {
    let encoded = encode_persisted_schema_snapshot(&temporal_schema_snapshot())
        .expect("current snapshot should encode");

    for prefix_len in 0..encoded.len() {
        let error = decode_persisted_schema_snapshot(&encoded[..prefix_len])
            .expect_err("every truncated current snapshot must fail closed");
        assert!(matches!(
            error.class(),
            ErrorClass::Corruption | ErrorClass::IncompatiblePersistedFormat
        ));
    }

    let mut deceptive_length = encoded;
    let entity_path_length_offset =
        super::SCHEMA_SNAPSHOT_MAGIC.len() + size_of::<u8>() + size_of::<u32>();
    deceptive_length[entity_path_length_offset..entity_path_length_offset + size_of::<u32>()]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    let error = decode_persisted_schema_snapshot(&deceptive_length)
        .expect_err("a declared length above the accepted bound must fail before allocation");
    assert_eq!(error.class(), ErrorClass::Corruption);
}

#[test]
fn persisted_schema_snapshot_uses_exact_recursive_depth_boundaries() {
    fn list_kind(levels: usize) -> AcceptedFieldKind {
        let mut kind = AcceptedFieldKind::Nat64;
        for _ in 0..levels {
            kind = AcceptedFieldKind::List(Box::new(kind));
        }
        kind
    }

    let valid_kind = list_kind(MAX_ACCEPTED_RECURSIVE_DEPTH - 1);
    let mut writer = super::SnapshotWriter::new();
    super::field::encode_kind(&mut writer, &valid_kind, 0)
        .expect("the highest valid recursive field kind must encode");
    let valid_bytes = writer.finish().expect("valid kind bytes should finish");
    let mut reader = super::SnapshotReader::new(&valid_bytes);
    let decoded = super::field::decode_kind(&mut reader, 0)
        .expect("the highest valid recursive field kind must decode");
    reader
        .finish()
        .expect("valid kind should consume all bytes");
    assert_eq!(decoded, valid_kind);

    let invalid_kind = list_kind(MAX_ACCEPTED_RECURSIVE_DEPTH);
    let mut writer = super::SnapshotWriter::new();
    let error = super::field::encode_kind(&mut writer, &invalid_kind, 0)
        .expect_err("the first out-of-range recursive field kind must reject");
    assert_eq!(error.class(), ErrorClass::Unsupported);

    let mut invalid_bytes = vec![29; MAX_ACCEPTED_RECURSIVE_DEPTH];
    invalid_bytes.push(23);
    let mut reader = super::SnapshotReader::new(&invalid_bytes);
    let error = super::field::decode_kind(&mut reader, 0)
        .expect_err("persisted field recursion beyond the accepted bound must reject");
    assert_eq!(error.class(), ErrorClass::Corruption);
}

#[test]
fn persisted_schema_snapshot_uses_exact_check_expression_depth_boundaries() {
    let valid_not_count = usize::from(crate::db::schema::check::MAX_CHECK_EXPR_V1_DEPTH) - 1;
    let mut valid_bytes = vec![3; valid_not_count];
    valid_bytes.push(1);
    let mut reader = super::SnapshotReader::new(&valid_bytes);
    let mut nodes = 0;
    let _ = super::constraint::decode_check_expression(&mut reader, 0, &mut nodes)
        .expect("the highest valid check-expression depth must decode");
    reader
        .finish()
        .expect("valid check expression should consume all bytes");

    let invalid_not_count = usize::from(crate::db::schema::check::MAX_CHECK_EXPR_V1_DEPTH);
    let mut invalid_bytes = vec![3; invalid_not_count];
    invalid_bytes.push(1);
    let mut reader = super::SnapshotReader::new(&invalid_bytes);
    let mut nodes = 0;
    let error = super::constraint::decode_check_expression(&mut reader, 0, &mut nodes)
        .expect_err("the first out-of-range check-expression depth must reject");
    assert_eq!(error.class(), ErrorClass::Corruption);
}

fn single_field_contract_snapshot(
    kind: AcceptedFieldKind,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::ContractBoundary".to_string(),
        "ContractBoundary".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            kind,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            storage_decode,
            leaf_codec,
        )],
    )
}

#[test]
fn persisted_schema_snapshot_rejects_invalid_local_field_contracts() {
    let malformed = [
        single_field_contract_snapshot(
            AcceptedFieldKind::Decimal {
                scale: icydb_schema::Decimal::max_supported_scale().saturating_add(1),
            },
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        ),
        single_field_contract_snapshot(
            AcceptedFieldKind::IntBig { max_bytes: 0 },
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        ),
        single_field_contract_snapshot(
            AcceptedFieldKind::NatBig { max_bytes: 0 },
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        ),
        single_field_contract_snapshot(
            AcceptedFieldKind::Nat64,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        ),
    ];

    for snapshot in malformed {
        let error = encode_persisted_schema_snapshot(&snapshot)
            .expect_err("accepted codec egress must reject malformed field contracts");
        assert_eq!(error.class(), ErrorClass::InvariantViolation);

        let encoded = encode_unchecked_schema_fixture(&snapshot);
        let error = decode_persisted_schema_snapshot(&encoded)
            .expect_err("accepted codec ingress must reject malformed field contracts");
        assert_eq!(error.class(), ErrorClass::Corruption);
    }
}

#[test]
fn persisted_schema_snapshot_round_trips_u256_current_v1_contract() {
    let snapshot = single_field_contract_snapshot(
        AcceptedFieldKind::U256,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::U256),
    );
    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("current U256 schema snapshot should encode");

    assert_eq!(
        decode_persisted_schema_snapshot(&encoded)
            .expect("current U256 schema snapshot should decode"),
        snapshot,
    );
}

#[test]
fn persisted_schema_snapshot_rejects_noncanonical_check_literal_contract() {
    let snapshot = snapshot_with_true_check();
    let mut constraints = snapshot.constraints().to_vec();
    let position = constraints
        .iter()
        .position(|constraint| matches!(constraint.kind(), AcceptedConstraintKind::Check { .. }))
        .expect("test snapshot should contain a check constraint");
    let accepted = &constraints[position];
    constraints[position] = AcceptedConstraintSnapshot::new(
        accepted.id(),
        accepted.name().to_string(),
        accepted.origin(),
        AcceptedConstraintKind::Check {
            expression: Box::new(AcceptedCheckExprV1::IsNull(
                AcceptedCheckValueExprV1::Literal(AcceptedCheckLiteralV1::from_accepted_parts(
                    AcceptedFieldKind::Nat64,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Structural,
                    vec![1],
                )),
            )),
        },
    );
    let allocator = snapshot.constraint_id_allocator();
    let malformed = snapshot.with_constraint_catalog(
        AcceptedConstraintCatalog::from_persisted_parts(allocator, constraints, Vec::new()),
    );

    let error = encode_persisted_schema_snapshot(&malformed)
        .expect_err("accepted codec egress must reject a noncanonical check literal");
    assert_eq!(error.class(), ErrorClass::InvariantViolation);
    let encoded = encode_unchecked_schema_fixture(&malformed);
    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("accepted codec ingress must reject a noncanonical check literal");
    assert_eq!(error.class(), ErrorClass::Corruption);
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

fn snapshot_with_targeted_rule() -> PersistedSchemaSnapshot {
    let snapshot = temporal_schema_snapshot();
    let target_type =
        CompositeTypeId::new(7).expect("test targeted type identity should be non-zero");
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_targeted_rule(
            "nested_cardinality".to_string(),
            ConstraintOrigin::Generated,
            AcceptedRuleTarget::new(
                FieldId::new(2),
                AcceptedNamedTypeIdentity::Composite(target_type),
            ),
            AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 8 },
        )
        .expect("test targeted rule should allocate");
    snapshot.with_constraint_catalog(catalog)
}

#[test]
fn persisted_schema_snapshot_round_trips_current_targeted_rule() {
    let snapshot = snapshot_with_targeted_rule();
    let encoded =
        encode_persisted_schema_snapshot(&snapshot).expect("current targeted rule should encode");
    let decoded =
        decode_persisted_schema_snapshot(&encoded).expect("current targeted rule should decode");

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
fn persisted_schema_snapshot_round_trips_current_targeted_activation() {
    let snapshot = temporal_schema_snapshot();
    let target_type =
        CompositeTypeId::new(7).expect("test targeted type identity should be non-zero");
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_targeted_rule_activation(
            "pending_nested_cardinality".to_string(),
            ConstraintOrigin::Generated,
            AcceptedRuleTarget::new(
                FieldId::new(2),
                AcceptedNamedTypeIdentity::Composite(target_type),
            ),
            AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 8 },
            AcceptedSchemaFingerprint::new([0xA6; 32]),
            8,
        )
        .expect("targeted activation should reserve identity");
    let snapshot = snapshot.with_constraint_catalog(catalog);

    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("current targeted activation should encode");
    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("current targeted activation should decode");

    assert_eq!(decoded, snapshot);
    assert!(matches!(
        decoded.constraint_activations()[0].kind(),
        ConstraintActivationKind::TargetedRule { .. }
    ));
}

#[test]
fn persisted_schema_snapshot_round_trips_targeted_semantic_replacement() {
    let snapshot = snapshot_with_targeted_rule();
    let accepted = snapshot
        .constraints()
        .iter()
        .find(|constraint| {
            matches!(
                constraint.kind(),
                AcceptedConstraintKind::TargetedRule { .. }
            )
        })
        .expect("accepted targeted rule should exist");
    let AcceptedConstraintKind::TargetedRule { target, .. } = accepted.kind() else {
        panic!("accepted targeted rule should retain its kind");
    };
    let accepted_id = accepted.id();
    let target = *target;
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_replaced_targeted_rule_activation(
            accepted_id,
            target,
            AcceptedRuleOperation::LengthRangeInclusive { min: 2, max: 7 },
            AcceptedSchemaFingerprint::new([0xA7; 32]),
            9,
        )
        .expect("targeted semantic replacement should stage");
    let snapshot = snapshot.with_constraint_catalog(catalog);

    let encoded = encode_persisted_schema_snapshot(&snapshot)
        .expect("targeted semantic replacement should encode");
    let decoded = decode_persisted_schema_snapshot(&encoded)
        .expect("targeted semantic replacement should decode");

    assert_eq!(decoded, snapshot);
    assert_eq!(decoded.constraint_activations()[0].id(), accepted_id);
    assert_eq!(
        decoded
            .constraints()
            .last()
            .map(AcceptedConstraintSnapshot::id),
        Some(accepted_id),
    );
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
            PersistedFieldSnapshot::new_with_write_policy_and_origin(
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
                PersistedFieldOrigin::Generated,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ],
    )
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
fn persisted_schema_snapshot_round_trips_identity_generation() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::Identity".to_string(),
        "Identity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial_with_write_policy(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat128,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            SchemaFieldWritePolicy::from_model_policies(
                Some(FieldInsertGeneration::Identity),
                None,
            ),
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        )],
    );
    let encoded =
        encode_persisted_schema_snapshot(&snapshot).expect("identity schema should encode");
    let decoded =
        decode_persisted_schema_snapshot(&encoded).expect("identity schema should decode");

    assert_eq!(
        decoded.fields()[0].write_policy().insert_generation(),
        Some(FieldInsertGeneration::Identity),
    );

    let caller_owned = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "entities::Identity".to_string(),
        "Identity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat128,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        )],
    );
    assert_ne!(
        accepted_schema_cache_fingerprint_for_persisted_snapshot(&snapshot)
            .expect("identity fingerprint should derive"),
        accepted_schema_cache_fingerprint_for_persisted_snapshot(&caller_owned)
            .expect("caller-owned fingerprint should derive"),
        "identity policy must participate in accepted schema fingerprints",
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
}

#[test]
fn persisted_schema_snapshot_decode_hard_cuts_ambiguous_nullable_unique_state() {
    let snapshot = nullable_unique_codec_fixture(None);
    let encoded = encode_unchecked_schema_fixture(&snapshot);

    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("well-formed implicit nullable uniqueness must hard-cut");
    assert_eq!(error.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(error.origin(), ErrorOrigin::Serialize);

    let encode_error = encode_persisted_schema_snapshot(&snapshot)
        .expect_err("new code must not emit implicit nullable uniqueness");
    assert_eq!(encode_error.class(), ErrorClass::InvariantViolation);
    assert_eq!(encode_error.origin(), ErrorOrigin::Store);
}

#[test]
fn persisted_schema_snapshot_decode_hard_cuts_ambiguous_nullable_unique_candidate() {
    let active = nullable_unique_codec_fixture(None);
    let candidate = active.indexes()[0].clone().clone_with_schema_identity(
        SchemaIndexId::new(1).expect("test index identity should be non-zero"),
        1,
        9,
    );
    let base = PersistedSchemaSnapshot::new(
        active.version(),
        active.entity_path().to_string(),
        active.entity_name().to_string(),
        active.primary_key_field_ids().to_vec(),
        active.row_layout().clone(),
        active.fields().to_vec(),
    );
    let catalog = base
        .constraint_catalog()
        .clone()
        .with_added_unique_activation(&candidate, AcceptedSchemaFingerprint::new([0xC7; 32]), 9)
        .expect("raw candidate fixture should close structurally");
    let snapshot = base
        .with_constraint_catalog(catalog)
        .with_constraint_candidates(vec![candidate], Vec::new());
    let encoded = encode_unchecked_schema_fixture(&snapshot);

    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("well-formed implicit nullable unique candidate must hard-cut");
    assert_eq!(error.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(error.origin(), ErrorOrigin::Serialize);
}

#[test]
fn persisted_schema_snapshot_decode_keeps_nullable_unique_predicate_corruption_distinct() {
    let encoded =
        encode_unchecked_schema_fixture(&nullable_unique_codec_fixture(Some("email IS NOT")));

    let error = decode_persisted_schema_snapshot(&encoded)
        .expect_err("malformed nullable-unique predicate must remain corruption");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
}

fn nullable_unique_codec_fixture(predicate_sql: Option<&str>) -> PersistedSchemaSnapshot {
    PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "entities::NullableUnique".to_string(),
        "NullableUnique".to_string(),
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
                true,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
            1,
            "idx_nullable_unique__email".to_string(),
            "nullable_unique::email".to_string(),
            true,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["email".to_string()],
                AcceptedFieldKind::Text { max_len: None },
                true,
            )]),
            predicate_sql.map(str::to_string),
        )],
    )
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
    .with_relation_id_allocator(RelationIdAllocator::new(9))
    .with_relations(vec![PersistedRelationEdgeSnapshot::new_direct(
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
    assert_eq!(decoded.relation_id_allocator().high_water(), 9);
    assert_eq!(relation.source().direct_field_ids(), &[FieldId::new(2)]);
    assert_eq!(decoded.fields()[1].kind(), &relation_kind);

    let inconsistent = snapshot.with_relation_id_allocator(RelationIdAllocator::default());
    let inconsistent_bytes = encode_persisted_schema_snapshot(&inconsistent)
        .expect("test should encode the intentionally inconsistent allocator state");
    let error = decode_persisted_schema_snapshot(&inconsistent_bytes)
        .expect_err("decoder must reject a relation ID above the persisted high-water");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
}

#[test]
fn relation_decoder_rejects_immutable_predecessor_source_bytes_as_incompatible() {
    const PREDECESSOR_DIRECT_RELATION: &[u8] = &[
        0, 0, 0, 1, // relation ID
        0, 0, 0, 0, 0, 0, 0, 0, // physical generation
        0, 0, 0, 1, b'a', // name
        0, 0, 0, 1, b'b', // target path
        0, 0, 0, 1, // predecessor untagged local-field count
        0, 0, 0, 2, // local field ID
    ];
    let mut reader = super::SnapshotReader::new(PREDECESSOR_DIRECT_RELATION);

    let error = super::index::decode_relation(&mut reader)
        .expect_err("predecessor untagged relation source must fail closed");

    assert_eq!(error.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(error.origin(), ErrorOrigin::Serialize);
}

#[test]
fn relation_decoder_rejects_unknown_current_source_tag_as_corruption() {
    const UNKNOWN_SOURCE_RELATION: &[u8] = &[
        0, 0, 0, 1, // relation ID
        0, 0, 0, 0, 0, 0, 0, 0, // physical generation
        0, 0, 0, 1, b'a', // name
        0, 0, 0, 1, b'b', // target path
        2,    // unknown current source tag
        0, 0, 0, 1, // field count
        0, 0, 0, 2, // field ID
    ];
    let mut reader = super::SnapshotReader::new(UNKNOWN_SOURCE_RELATION);

    let error = super::index::decode_relation(&mut reader)
        .expect_err("unknown relation source tag must fail closed");

    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
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
