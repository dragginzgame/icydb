use super::*;
use crate::db::schema::{NullableUniqueIndexContractError, ScalarCodec};

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
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), layout_slot),
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
                "payload".to_string(),
                field_slot,
                AcceptedFieldKind::Blob { max_len: None },
                vec![PersistedNestedLeafSnapshot::new(
                    vec!["thumbnail".to_string()],
                    AcceptedFieldKind::Blob { max_len: None },
                    false,
                )],
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Blob),
            ),
        ],
    ))
}

fn identity_schema_fixture(
    kind: AcceptedFieldKind,
    nullable: bool,
    insert_default: SchemaInsertDefault,
) -> PersistedSchemaSnapshot {
    let leaf_codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::snapshot::tests::Identity".to_string(),
        "Identity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial_with_write_policy(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            kind,
            Vec::new(),
            nullable,
            insert_default,
            SchemaFieldWritePolicy::from_model_policies(
                Some(FieldInsertGeneration::Identity),
                None,
            ),
            FieldStorageDecode::ByKind,
            leaf_codec,
        )],
    )
}

fn nullable_unique_schema_fixture(
    unique: bool,
    nullable_fields: &[&str],
    key_fields: &[&str],
    predicate_sql: Option<&str>,
) -> PersistedSchemaSnapshot {
    let mut fields = vec![PersistedFieldSnapshot::new_initial(
        FieldId::new(1),
        "id".to_string(),
        SchemaFieldSlot::new(0),
        AcceptedFieldKind::Ulid,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Ulid),
    )];
    for (offset, name) in ["email", "tenant"].into_iter().enumerate() {
        let id = FieldId::new(u32::try_from(offset + 2).expect("fixture field id should fit"));
        let slot =
            SchemaFieldSlot::new(u16::try_from(offset + 1).expect("fixture field slot should fit"));
        fields.push(PersistedFieldSnapshot::new_initial(
            id,
            name.to_string(),
            slot,
            AcceptedFieldKind::Text { max_len: None },
            Vec::new(),
            nullable_fields.contains(&name),
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        ));
    }
    let row_layout = SchemaRowLayout::initial(
        fields
            .iter()
            .map(|field| (field.id(), field.slot()))
            .collect(),
    );
    let key = key_fields
        .iter()
        .map(|name| {
            let field = fields
                .iter()
                .find(|field| field.name() == *name)
                .expect("fixture key field should exist");
            PersistedIndexFieldPathSnapshot::new(
                field.id(),
                field.slot(),
                vec![field.name().to_string()],
                field.kind().clone(),
                field.nullable(),
            )
        })
        .collect();
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "schema::snapshot::tests::NullableUnique".to_string(),
        "NullableUnique".to_string(),
        FieldId::new(1),
        row_layout,
        fields,
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
            1,
            "idx_nullable_unique".to_string(),
            "nullable_unique::value".to_string(),
            unique,
            PersistedIndexKeySnapshot::FieldPath(key),
            predicate_sql.map(str::to_string),
        )],
    );
    let catalog = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .expect("fixture constraint catalog should build");
    snapshot.with_constraint_catalog(catalog)
}

fn nested_nullable_unique_schema_fixture(
    root_nullable: bool,
    terminal_nullable: bool,
    predicate_sql: Option<&str>,
) -> PersistedSchemaSnapshot {
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "schema::snapshot::tests::NestedNullableUnique".to_string(),
        "NestedNullableUnique".to_string(),
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
                "profile".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Blob { max_len: None },
                vec![PersistedNestedLeafSnapshot::new(
                    vec!["email".to_string()],
                    AcceptedFieldKind::Text { max_len: None },
                    terminal_nullable,
                )],
                root_nullable,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Blob),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
            1,
            "idx_nested_nullable_unique".to_string(),
            "nested_nullable_unique::profile_email".to_string(),
            true,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["profile".to_string(), "email".to_string()],
                AcceptedFieldKind::Text { max_len: None },
                terminal_nullable,
            )]),
            predicate_sql.map(str::to_string),
        )],
    );
    let catalog = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .expect("fixture constraint catalog should build");
    snapshot.with_constraint_catalog(catalog)
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
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "entity_id".to_string(),
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
                "battle_id".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
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
#[cfg(feature = "sql")]
fn update_managed_unique_field_requires_global_write_validation() {
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "schema::snapshot::tests::ManagedUnique".to_string(),
        "ManagedUnique".to_string(),
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
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
            0,
            "idx_managed_unique__updated_at".to_string(),
            "managed_unique::updated_at".to_string(),
            true,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["updated_at".to_string()],
                AcceptedFieldKind::Timestamp,
                false,
            )]),
            None,
        )],
    );

    assert!(snapshot.update_management_requires_global_write_validation());
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_invalid_metadata() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::snapshot::tests::Invalid".to_string(),
        "Invalid".to_string(),
        FieldId::new(99),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
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
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
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
fn nullable_unique_acceptance_requires_every_exact_top_level_guard() {
    AcceptedSchemaSnapshot::try_new_with_acceptance(nullable_unique_schema_fixture(
        false,
        &["email"],
        &["email"],
        None,
    ))
    .expect("non-unique nullable membership remains unchanged");

    AcceptedSchemaSnapshot::try_new_with_acceptance(nullable_unique_schema_fixture(
        true,
        &[],
        &["email"],
        None,
    ))
    .expect("non-null unique source should not require a predicate");

    let missing = AcceptedSchemaSnapshot::try_new_with_acceptance(nullable_unique_schema_fixture(
        true,
        &["email"],
        &["email"],
        None,
    ))
    .expect_err("nullable unique source without a guard must reject");
    assert!(matches!(
        missing,
        SchemaSnapshotAcceptanceError::NullableUnique(
            NullableUniqueIndexContractError::MissingGuards { sources, .. }
        ) if sources == vec![vec!["email".to_string()]]
    ));

    for predicate in [
        "email IS NOT NULL",
        "email IS NOT NULL AND email IS NOT NULL",
        "email IS NOT NULL AND tenant = 'active'",
    ] {
        AcceptedSchemaSnapshot::try_new_with_acceptance(nullable_unique_schema_fixture(
            true,
            &["email"],
            &["email"],
            Some(predicate),
        ))
        .expect("an exact matching non-null conjunct should admit");
    }

    for predicate in [
        "email IS NULL",
        "NOT email IS NULL",
        "email = 'present'",
        "email IS NOT NULL OR tenant IS NOT NULL",
    ] {
        let error = AcceptedSchemaSnapshot::try_new_with_acceptance(
            nullable_unique_schema_fixture(true, &["email"], &["email"], Some(predicate)),
        )
        .expect_err("non-exact nullable guard must reject");
        assert!(matches!(
            error,
            SchemaSnapshotAcceptanceError::NullableUnique(
                NullableUniqueIndexContractError::MissingGuards { .. }
            )
        ));
    }
}

#[test]
fn nullable_unique_acceptance_is_composite_complete_and_bind_fail_closed() {
    let error = AcceptedSchemaSnapshot::try_new_with_acceptance(nullable_unique_schema_fixture(
        true,
        &["email", "tenant"],
        &["email", "tenant"],
        Some("email IS NOT NULL"),
    ))
    .expect_err("every nullable composite source must be guarded");
    assert!(matches!(
        error,
        SchemaSnapshotAcceptanceError::NullableUnique(
            NullableUniqueIndexContractError::MissingGuards { sources, .. }
        ) if sources == vec![vec!["tenant".to_string()]]
    ));

    AcceptedSchemaSnapshot::try_new_with_acceptance(nullable_unique_schema_fixture(
        true,
        &["email", "tenant"],
        &["email", "tenant"],
        Some("tenant IS NOT NULL AND email IS NOT NULL"),
    ))
    .expect("guard order should not alter composite coverage");

    for predicate in ["missing IS NOT NULL", "email IS NOT"] {
        let error = AcceptedSchemaSnapshot::try_new_with_acceptance(
            nullable_unique_schema_fixture(true, &["email"], &["email"], Some(predicate)),
        )
        .expect_err("malformed or unbound predicates must fail closed");
        assert_eq!(error, SchemaSnapshotAcceptanceError::Predicate);
    }
}

#[test]
fn nullable_unique_acceptance_rejects_unguardable_nested_omission() {
    let terminal = AcceptedSchemaSnapshot::try_new_with_acceptance(
        nested_nullable_unique_schema_fixture(false, true, Some("profile.email IS NOT NULL")),
    )
    .expect_err("dotted predicate text cannot bind as a nested guard");
    assert_eq!(terminal, SchemaSnapshotAcceptanceError::Predicate);

    let ancestor = AcceptedSchemaSnapshot::try_new_with_acceptance(
        nested_nullable_unique_schema_fixture(true, false, Some("profile IS NOT NULL")),
    )
    .expect_err("nullable nested ancestors are not physical omission proofs");
    assert!(matches!(
        ancestor,
        SchemaSnapshotAcceptanceError::NullableUnique(
            NullableUniqueIndexContractError::UnsupportedNullableAncestor { source, .. }
        ) if source == vec!["profile".to_string(), "email".to_string()]
    ));
}

#[test]
fn nullable_unique_acceptance_applies_to_expression_sources() {
    let base = nullable_unique_schema_fixture(true, &["email"], &["email"], None);
    let source = PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["email".to_string()],
        AcceptedFieldKind::Text { max_len: None },
        true,
    );
    let index = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).expect("test index identity should be non-zero"),
        1,
        "idx_lower_email".to_string(),
        "nullable_unique::lower_email".to_string(),
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
        Some("email IS NOT NULL".to_string()),
    );
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        base.row_layout().clone(),
        base.fields().to_vec(),
        vec![index],
    );
    let catalog = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .expect("fixture constraint catalog should build");

    AcceptedSchemaSnapshot::try_new_with_acceptance(snapshot.with_constraint_catalog(catalog))
        .expect("expression source should consume the same exact source guard");
}

#[test]
fn accepted_identity_policy_admits_every_exact_unsigned_width() {
    for kind in [
        AcceptedFieldKind::Nat8,
        AcceptedFieldKind::Nat16,
        AcceptedFieldKind::Nat32,
        AcceptedFieldKind::Nat64,
        AcceptedFieldKind::Nat128,
    ] {
        AcceptedSchemaSnapshot::try_new(identity_schema_fixture(
            kind,
            false,
            SchemaInsertDefault::None,
        ))
        .expect("exact unsigned identity schema should pass accepted integrity");
    }
}

#[test]
fn accepted_identity_policy_rejects_ineligible_persisted_shapes() {
    for snapshot in [
        identity_schema_fixture(AcceptedFieldKind::Int64, false, SchemaInsertDefault::None),
        identity_schema_fixture(AcceptedFieldKind::Nat64, true, SchemaInsertDefault::None),
        identity_schema_fixture(
            AcceptedFieldKind::Nat64,
            false,
            SchemaInsertDefault::SlotPayload(vec![1]),
        ),
    ] {
        AcceptedSchemaSnapshot::try_new(snapshot)
            .expect_err("ineligible persisted identity policy must reject");
    }

    let composite_primary_key = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::snapshot::tests::CompositeIdentity".to_string(),
        "CompositeIdentity".to_string(),
        vec![FieldId::new(1), FieldId::new(2)],
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            identity_schema_fixture(AcceptedFieldKind::Nat64, false, SchemaInsertDefault::None)
                .fields()[0]
                .clone(),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "partition".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
        ],
    );
    AcceptedSchemaSnapshot::try_new(composite_primary_key)
        .expect_err("identity on a composite primary key must reject");
}

#[test]
fn accepted_schema_snapshot_try_new_rejects_duplicate_primary_key_fields() {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema::snapshot::tests::DuplicatePk".to_string(),
        "DuplicatePk".to_string(),
        vec![FieldId::new(1), FieldId::new(1)],
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
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
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
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
                SchemaIndexId::new(1).expect("test index identity should be non-zero"),
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
                "owner_id".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
        ],
        Vec::new(),
    )
    .with_relations(vec![
        PersistedRelationEdgeSnapshot::new(
            RelationId::new(1).expect("test relation identity should be non-zero"),
            "owner".to_string(),
            "schema::snapshot::tests::Owner".to_string(),
            vec![FieldId::new(2)],
        ),
        PersistedRelationEdgeSnapshot::new(
            RelationId::new(2).expect("test relation identity should be non-zero"),
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
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
        Vec::new(),
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        RelationId::new(1).expect("test relation identity should be non-zero"),
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
                "owner".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::test_composite(),
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::CatalogValue,
                LeafCodec::Structural,
            ),
        ],
        Vec::new(),
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        RelationId::new(1).expect("test relation identity should be non-zero"),
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
                "owner_id".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
        ],
        Vec::new(),
    )
    .with_relations(vec![PersistedRelationEdgeSnapshot::new(
        RelationId::new(1).expect("test relation identity should be non-zero"),
        "owner".to_string(),
        "schema::snapshot::tests::Owner".to_string(),
        vec![FieldId::new(2)],
    )]);
    let catalog = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .expect("relation constraint catalog should build");
    let snapshot = snapshot.with_constraint_catalog(catalog);

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
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
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
