use crate::db::schema::{
    AcceptedFieldKind, FieldId, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
    PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
    PersistedRelationEdgeSnapshot, RelationId, ScalarCodec, SchemaFieldSlot, SchemaIndexId,
    SchemaInsertDefault, SchemaRowLayout,
    integrity::{
        schema_snapshot_index_integrity_detail, schema_snapshot_relation_integrity_detail,
    },
};

fn field_contract() -> (SchemaRowLayout, Vec<PersistedFieldSnapshot>) {
    (
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".into(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )],
    )
}

fn text_index_field_contract() -> (
    SchemaRowLayout,
    Vec<PersistedFieldSnapshot>,
    PersistedIndexFieldPathSnapshot,
) {
    let (_, mut fields) = field_contract();
    let text = AcceptedFieldKind::Text { max_len: None };
    fields.push(PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "value".into(),
        SchemaFieldSlot::new(1),
        text.clone(),
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        text.leaf_codec_for_storage(FieldStorageDecode::ByKind),
    ));
    let layout = SchemaRowLayout::initial(
        fields
            .iter()
            .map(|field| (field.id(), field.slot()))
            .collect(),
    );
    let source = PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["value".into()],
        text,
        false,
    );
    (layout, fields, source)
}

#[test]
fn index_width_is_enforced_for_accepted_and_candidate_key_forms() {
    use crate::{
        MAX_INDEX_FIELDS,
        db::schema::{
            AcceptedSchemaSnapshot, PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
            PersistedIndexKeyItemSnapshot, PersistedSchemaSnapshot, SchemaSnapshotAcceptanceError,
            SchemaVersion, decode_persisted_schema_snapshot, encode_persisted_schema_snapshot,
        },
    };
    let (layout, fields, source) = text_index_field_contract();
    let text = source.kind();
    for count in [0, 1, MAX_INDEX_FIELDS, MAX_INDEX_FIELDS + 1] {
        for form in 0..3 {
            let key = if form == 0 {
                PersistedIndexKeySnapshot::FieldPath(vec![source.clone(); count])
            } else {
                PersistedIndexKeySnapshot::Items(
                    (0..count)
                        .map(|offset| {
                            if form == 2 && offset % 2 == 0 {
                                PersistedIndexKeyItemSnapshot::Expression(Box::new(
                                    PersistedIndexExpressionSnapshot::new(
                                        PersistedIndexExpressionOp::Lower,
                                        source.clone(),
                                        text.clone(),
                                        text.clone(),
                                        "expr:v1:LOWER(value)".into(),
                                    ),
                                ))
                            } else {
                                PersistedIndexKeyItemSnapshot::FieldPath(source.clone())
                            }
                        })
                        .collect(),
                )
            };
            let index = PersistedIndexSnapshot::new(
                SchemaIndexId::new(1).unwrap(),
                1,
                "by_value".into(),
                "test::Store".into(),
                false,
                key,
                None,
            );
            let indexes = [index.clone()];
            let valid = (1..=MAX_INDEX_FIELDS).contains(&count);
            for split in 0..=1 {
                assert_eq!(
                    schema_snapshot_index_integrity_detail(
                        "test",
                        &layout,
                        &fields,
                        &indexes[..split],
                        &indexes[split..],
                    )
                    .is_none(),
                    valid
                );
            }
            let snapshot = PersistedSchemaSnapshot::new_with_indexes(
                SchemaVersion::initial(),
                "test::Entity".into(),
                "Entity".into(),
                FieldId::new(1),
                layout.clone(),
                fields.clone(),
                vec![index],
            );
            let accepted = AcceptedSchemaSnapshot::try_new_with_acceptance(snapshot.clone());
            if valid {
                assert!(accepted.is_ok());
                let encoded = encode_persisted_schema_snapshot(&snapshot).unwrap();
                assert_eq!(
                    decode_persisted_schema_snapshot(&encoded).unwrap(),
                    snapshot
                );
            } else {
                assert!(matches!(
                    accepted,
                    Err(SchemaSnapshotAcceptanceError::Structural)
                ));
                assert!(encode_persisted_schema_snapshot(&snapshot).is_err());
            }
        }
    }
}

#[test]
fn acceptance_errors_preserve_boundary_specific_classification() {
    use crate::{
        db::schema::{
            NullableUniqueIndexContractError, SchemaSnapshotAcceptanceError,
            constraint::AcceptedConstraintCatalogError,
        },
        error::InternalError,
    };

    let index_id = SchemaIndexId::new(1).unwrap();
    let cases = [
        (
            SchemaSnapshotAcceptanceError::Structural,
            InternalError::store_corruption(),
            InternalError::store_invariant(),
        ),
        (
            SchemaSnapshotAcceptanceError::Predicate,
            InternalError::store_corruption(),
            InternalError::store_unsupported(),
        ),
        (
            SchemaSnapshotAcceptanceError::NullableUnique(
                NullableUniqueIndexContractError::MissingGuards {
                    index_id,
                    index_name: "idx".into(),
                    sources: vec![vec!["value".into()]],
                },
            ),
            InternalError::serialize_incompatible_persisted_format(),
            InternalError::store_unsupported(),
        ),
        (
            SchemaSnapshotAcceptanceError::NullableUnique(
                NullableUniqueIndexContractError::UnsupportedNullableAncestor {
                    index_id,
                    index_name: "idx".into(),
                    source: vec!["nested".into(), "value".into()],
                },
            ),
            InternalError::serialize_incompatible_persisted_format(),
            InternalError::store_unsupported(),
        ),
    ];
    for (error, decoded, proposed) in cases {
        assert_eq!(
            error.clone().into_decode_error().diagnostic(),
            decoded.diagnostic(),
        );
        assert_eq!(
            error.clone().into_proposal_error().diagnostic(),
            proposed.diagnostic(),
        );
        assert_eq!(
            error.clone().into_invariant_error().diagnostic(),
            InternalError::store_invariant().diagnostic(),
        );
        assert_eq!(
            error.into_constraint_error(),
            AcceptedConstraintCatalogError::OwnerMismatch,
        );
    }
}

fn index(id: u32, ordinal: u16, name: &str, field_id: u32) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        SchemaIndexId::new(id).unwrap(),
        ordinal,
        name.into(),
        "test::Store".into(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(field_id),
            SchemaFieldSlot::new(0),
            vec!["id".into()],
            AcceptedFieldKind::Nat64,
            false,
        )]),
        None,
    )
}

fn relation(id: u32, name: &str, field_id: u32) -> PersistedRelationEdgeSnapshot {
    PersistedRelationEdgeSnapshot::new_direct(
        RelationId::new(id).unwrap(),
        name.into(),
        "test::Target".into(),
        vec![FieldId::new(field_id)],
    )
}

#[test]
fn index_integrity_keeps_one_ordinal_sequence_across_active_and_candidate_entries() {
    let (layout, fields) = field_contract();
    let indexes = [
        index(1, 1, "first", 1),
        index(2, 2, "second", 1),
        index(3, 3, "third", 1),
    ];
    for split in 0..=indexes.len() {
        assert_eq!(
            schema_snapshot_index_integrity_detail(
                "test",
                &layout,
                &fields,
                &indexes[..split],
                &indexes[split..],
            ),
            None,
            "valid entries must admit at split {split}"
        );
    }
    assert_eq!(
        schema_snapshot_index_integrity_detail("test", &layout, &fields, &[], &[]),
        None
    );
}

#[test]
fn index_integrity_rejects_conflicts_and_malformed_keys_on_either_side_of_the_split() {
    let (layout, fields) = field_contract();
    for invalid in [
        index(1, 2, "second", 1), // Duplicate stable identity.
        index(2, 2, "first", 1),  // Duplicate name.
        index(2, 1, "second", 1), // Duplicate ordinal.
        index(2, 3, "second", 1), // Gap in the combined ordinal sequence.
        index(2, 2, "second", 2), // Key references a field outside authority.
    ] {
        let indexes = [index(1, 1, "first", 1), invalid];
        for split in 0..=indexes.len() {
            assert_eq!(
                schema_snapshot_index_integrity_detail(
                    "test",
                    &layout,
                    &fields,
                    &indexes[..split],
                    &indexes[split..],
                ),
                Some(()),
                "invalid entries must reject at split {split}"
            );
        }
    }
}

#[test]
fn relation_integrity_admits_active_and_candidate_entries_without_an_ordinal_requirement() {
    let (layout, fields) = field_contract();
    let relations = [
        relation(3, "third", 1),
        relation(1, "first", 1),
        relation(2, "second", 1),
    ];
    for split in 0..=relations.len() {
        assert_eq!(
            schema_snapshot_relation_integrity_detail(
                "test",
                &layout,
                &fields,
                &relations[..split],
                &relations[split..],
            ),
            None,
            "valid entries must admit at split {split}"
        );
    }
    assert_eq!(
        schema_snapshot_relation_integrity_detail("test", &layout, &fields, &[], &[]),
        None
    );
}

#[test]
fn relation_integrity_rejects_conflicts_and_invalid_sources_on_either_side_of_the_split() {
    let (layout, fields) = field_contract();
    for invalid in [
        relation(1, "second", 1), // Duplicate stable identity.
        relation(2, "first", 1),  // Duplicate name.
        relation(2, "second", 2), // Source references a field outside authority.
    ] {
        let relations = [relation(1, "first", 1), invalid];
        for split in 0..=relations.len() {
            assert_eq!(
                schema_snapshot_relation_integrity_detail(
                    "test",
                    &layout,
                    &fields,
                    &relations[..split],
                    &relations[split..],
                ),
                Some(()),
                "invalid entries must reject at split {split}"
            );
        }
    }
}
