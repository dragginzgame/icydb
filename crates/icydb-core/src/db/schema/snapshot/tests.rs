use super::*;
use crate::model::field::ScalarCodec;

// Build a small accepted schema snapshot with deliberately non-generated
// slot values so accessor tests prove they read persisted schema facts.
fn accepted_schema_fixture() -> AcceptedSchemaSnapshot {
    accepted_schema_fixture_with_payload_slots(SchemaFieldSlot::new(7), SchemaFieldSlot::new(7))
}

// Build a deliberately inconsistent accepted wrapper for owner-local
// boundary tests. Production reconciliation rejects this shape, but the
// accessor must still prove which internal artifact owns slot answers.
fn accepted_schema_fixture_with_payload_slots(
    layout_slot: SchemaFieldSlot,
    field_slot: SchemaFieldSlot,
) -> AcceptedSchemaSnapshot {
    AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::snapshot::tests::Asset".to_string(),
        "Asset".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), layout_slot),
            ],
        ),
        vec![
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "payload".to_string(),
                field_slot,
                AcceptedFieldKind::Blob { max_len: None },
                vec![PersistedNestedLeafSnapshot::new(
                    vec!["thumbnail".to_string()],
                    AcceptedFieldKind::Blob { max_len: None },
                    false,
                )],
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Blob),
            ),
        ],
    ))
}

#[test]
fn accepted_schema_snapshot_exposes_schema_facts_without_raw_payload_access() {
    let snapshot = accepted_schema_fixture();

    assert_eq!(snapshot.entity_path(), "schema::snapshot::tests::Asset");
    assert_eq!(snapshot.entity_name(), "Asset");
    assert_eq!(snapshot.primary_key_field_names(), ["id"]);
    assert_eq!(
        snapshot.field_kind_by_name("id"),
        Some(&AcceptedFieldKind::Ulid)
    );
    assert_eq!(
        snapshot.field_kind_by_name("payload"),
        Some(&AcceptedFieldKind::Blob { max_len: None }),
    );
    assert_eq!(snapshot.field_kind_by_name("missing"), None);
}

#[test]
fn accepted_schema_snapshot_exposes_ordered_primary_key_field_names() {
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::snapshot::tests::Placement".to_string(),
        "Placement".to_string(),
        vec![FieldId::new(2), FieldId::new(1)],
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
                "entity_id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "battle_id".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ],
    ));

    assert_eq!(
        snapshot.primary_key_field_names(),
        ["battle_id", "entity_id"]
    );
}

#[test]
fn accepted_schema_snapshot_footprint_counts_field_and_nested_leaf_facts() {
    let snapshot = accepted_schema_fixture();
    let footprint = snapshot.footprint();

    assert_eq!(footprint.fields(), 2);
    assert_eq!(footprint.nested_leaf_facts(), 1);
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_invalid_metadata() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::snapshot::tests::Invalid".to_string(),
        "Invalid".to_string(),
        FieldId::new(99),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
        ),
        vec![PersistedFieldSnapshot::new(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
    );

    let err = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect_err("accepted schema construction should reject invalid metadata");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "accepted schema construction should report the integrity failure"
    );
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_zero_schema_version() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::new(0),
        "schema::snapshot::tests::ZeroVersion".to_string(),
        "ZeroVersion".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::new(0),
            vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
        ),
        vec![PersistedFieldSnapshot::new(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
    );

    let err = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect_err("accepted schema construction should reject version-zero metadata");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "accepted schema construction should hard-cut non-positive schema versions"
    );
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_duplicate_primary_key_fields() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::snapshot::tests::DuplicatePk".to_string(),
        "DuplicatePk".to_string(),
        vec![FieldId::new(1), FieldId::new(1)],
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
        ),
        vec![PersistedFieldSnapshot::new(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
    );

    let err = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect_err("accepted schema construction should reject duplicate primary-key ids");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "accepted schema construction should report duplicate primary-key fields"
    );
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_invalid_index_contract() {
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "schema::snapshot::tests::Indexed".to_string(),
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
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "email".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            1,
            "idx_indexed__email".to_string(),
            "indexed::email".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(7),
                vec!["email".to_string()],
                AcceptedFieldKind::Text { max_len: None },
                false,
            )]),
            None,
        )],
    );

    let err = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect_err("accepted schema construction should reject invalid index metadata");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "accepted schema construction should reject index slots that diverge from row layout"
    );
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_index_source_contract_drift() {
    let invalid_sources = [
        PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["renamed_email".to_string()],
            AcceptedFieldKind::Text { max_len: None },
            false,
        ),
        PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["email".to_string()],
            AcceptedFieldKind::Nat64,
            false,
        ),
        PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["email".to_string()],
            AcceptedFieldKind::Text { max_len: None },
            true,
        ),
    ];

    for source in invalid_sources {
        let snapshot = PersistedSchemaSnapshot::new_with_indexes(
            SchemaVersion::initial(),
            "schema::snapshot::tests::Indexed".to_string(),
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
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    AcceptedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "email".to_string(),
                    SchemaFieldSlot::new(1),
                    AcceptedFieldKind::Text { max_len: None },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
            vec![PersistedIndexSnapshot::new(
                1,
                "idx_indexed__email".to_string(),
                "indexed::email".to_string(),
                false,
                PersistedIndexKeySnapshot::FieldPath(vec![source]),
                None,
            )],
        );

        let error = AcceptedSchemaSnapshot::try_new(snapshot)
            .expect_err("accepted schema construction should reject index source drift");
        assert_eq!(
            error.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        );
    }
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_invalid_relation_contract() {
    let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        SchemaVersion::initial(),
        "schema::snapshot::tests::Related".to_string(),
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
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "owner_id".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
        ],
        Vec::new(),
    )
    .with_relations(vec![
        PersistedRelationEdgeSnapshot::new(
            "owner".to_string(),
            "schema::snapshot::tests::Owner".to_string(),
            vec![FieldId::new(2)],
        ),
        PersistedRelationEdgeSnapshot::new(
            "owner".to_string(),
            "schema::snapshot::tests::Owner".to_string(),
            vec![FieldId::new(2)],
        ),
    ]);

    let err = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect_err("accepted schema construction should reject invalid relation metadata");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "accepted schema construction should report invalid relation metadata"
    );
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_relation_missing_local_field() {
    let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        SchemaVersion::initial(),
        "schema::snapshot::tests::Related".to_string(),
        "Related".to_string(),
        FieldId::new(1),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
        ),
        vec![PersistedFieldSnapshot::new(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
        Vec::new(),
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        "owner".to_string(),
        "schema::snapshot::tests::Owner".to_string(),
        vec![FieldId::new(2)],
    )]);

    let err = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect_err("accepted schema construction should reject invalid relation metadata");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "accepted schema construction should report missing relation local fields"
    );
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_composite_relation_local_field() {
    let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        SchemaVersion::initial(),
        "schema::snapshot::tests::RelatedComposite".to_string(),
        "RelatedComposite".to_string(),
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
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "owner".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::test_composite(),
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::CatalogValue,
                LeafCodec::Structural,
            ),
        ],
        Vec::new(),
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        "owner".to_string(),
        "schema::snapshot::tests::Owner".to_string(),
        vec![FieldId::new(2)],
    )]);

    let error = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect_err("whole composites must not become relation local fields");

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
    );
}

#[test]
fn accepted_schema_snapshot_exposes_relation_edges() {
    let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        SchemaVersion::initial(),
        "schema::snapshot::tests::Related".to_string(),
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
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "owner_id".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
        ],
        Vec::new(),
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        "owner".to_string(),
        "schema::snapshot::tests::Owner".to_string(),
        vec![FieldId::new(2)],
    )]);

    let accepted = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect("relation metadata should pass source-local integrity checks");

    assert_eq!(accepted.persisted_snapshot().relations().len(), 1);
    assert_eq!(accepted.persisted_snapshot().relations()[0].name(), "owner");
    assert_eq!(
        accepted.persisted_snapshot().relations()[0].local_field_ids(),
        &[FieldId::new(2)]
    );
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_invalid_expression_index_contract() {
    let source = PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["email".to_string()],
        AcceptedFieldKind::Text { max_len: None },
        false,
    );
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "schema::snapshot::tests::ExpressionIndexed".to_string(),
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
            PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new(
                FieldId::new(2),
                "email".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            1,
            "idx_expression_indexed__lower_email".to_string(),
            "expression_indexed::lower_email".to_string(),
            false,
            PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
                Box::new(PersistedIndexExpressionSnapshot::new(
                    PersistedIndexExpressionOp::Lower,
                    source,
                    AcceptedFieldKind::Text { max_len: None },
                    AcceptedFieldKind::Date,
                    "expr:v1:LOWER(email)".to_string(),
                )),
            )]),
            None,
        )],
    );

    let err = AcceptedSchemaSnapshot::try_new(snapshot)
        .expect_err("accepted schema construction should reject invalid expression metadata");

    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "accepted schema construction should reject expression output-kind drift"
    );
}

#[test]
fn composite_members_select_the_canonical_recursive_wire() {
    let composite = AcceptedFieldKind::test_composite();
    let nested = AcceptedFieldKind::List(Box::new(composite.clone()));

    assert!(composite.requires_canonical_value_wire());
    assert!(nested.requires_canonical_value_wire());
    assert!(!AcceptedFieldKind::Nat64.requires_canonical_value_wire());
}
